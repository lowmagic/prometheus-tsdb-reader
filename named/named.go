// Package named reads the named-query file and builds a named query's expressions from typed values, by adding
// them to each expression's syntax tree as matchers and printing it.
package named

import (
	"bytes"
	_ "embed" // The shipped query file.
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/nathancurry/prometheus-tsdb-reader/query"
)

//go:embed queries.json
var shipped []byte

// Shipped returns the query file the binary embeds.
func Shipped() []byte { return slices.Clone(shipped) }

// Match types: a flag's value becomes an = or an =~ matcher.
const (
	MatchExact = "exact"
	MatchRegex = "regex"
)

// File is a named-query file. Its JSON shape is interface.
type File struct {
	Flags   []Flag  `json:"flags"`
	Queries []Query `json:"queries"`
}

// Flag is a typed flag: its name on the command line, the label it matches and how.
type Flag struct {
	Name  string `json:"name"`
	Label string `json:"label"`
	Match string `json:"match"`
}

// Query is one named query: the flags it takes and its expressions, in output order.
type Query struct {
	Name        string       `json:"name"`
	Flags       []string     `json:"flags"`
	Expressions []Expression `json:"expressions"`
	Compare     *Compare     `json:"compare,omitempty"`
}

// Expression is one of a query's expressions. When Flags is set, it takes only those of the query's flags.
type Expression struct {
	Name   string   `json:"name"`
	PromQL string   `json:"promql"`
	Flags  []string `json:"flags,omitempty"`
}

// Compare names two expressions and the labels to pair their series on.
type Compare struct {
	Left  string   `json:"left"`
	Right string   `json:"right"`
	On    []string `json:"on"`
}

// Built is an expression as it runs: its name and its PromQL in the printer's form.
type Built struct {
	Name, PromQL string
}

// newParser is a parser with the engine's options, so a file's PromQL parses here as the engine will parse it.
func newParser() parser.Parser {
	return parser.NewParser(parser.Options{ExperimentalDurationExpr: true})
}

// Parse reads a query file and checks it: every name is unique, every flag a query or expression lists is
// declared and the query's, every expression parses, and a compare names two of the query's expressions.
func Parse(data []byte) (*File, error) {
	var f File
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("read the query file: %w", err)
	}
	if err := f.check(); err != nil {
		return nil, fmt.Errorf("the query file: %w", err)
	}
	return &f, nil
}

func (f *File) check() error {
	flags := map[string]bool{}
	for _, fl := range f.Flags {
		switch {
		case fl.Name == "" || flags[fl.Name]:
			return fmt.Errorf("flag %q is unnamed or declared twice", fl.Name)
		case !model.UTF8Validation.IsValidLabelName(fl.Label):
			return fmt.Errorf("flag %q matches %q, which isn't a label name", fl.Name, fl.Label)
		case fl.Match != MatchExact && fl.Match != MatchRegex:
			return fmt.Errorf("flag %q has match %q, want %q or %q", fl.Name, fl.Match, MatchExact, MatchRegex)
		}
		flags[fl.Name] = true
	}
	queries := map[string]bool{}
	p := newParser()
	for _, q := range f.Queries {
		if q.Name == "" || queries[q.Name] {
			return fmt.Errorf("query %q is unnamed or defined twice", q.Name)
		}
		queries[q.Name] = true
		if err := q.check(flags, p); err != nil {
			return fmt.Errorf("query %q: %w", q.Name, err)
		}
	}
	return nil
}

func (q *Query) check(declared map[string]bool, p parser.Parser) error {
	for _, name := range q.Flags {
		if !declared[name] {
			return fmt.Errorf("flag %q isn't declared", name)
		}
	}
	if len(q.Expressions) == 0 {
		return errors.New("no expressions")
	}
	names := map[string]bool{}
	for _, e := range q.Expressions {
		if e.Name == "" || names[e.Name] {
			return fmt.Errorf("expression %q is unnamed or defined twice", e.Name)
		}
		names[e.Name] = true
		for _, name := range e.Flags {
			if !slices.Contains(q.Flags, name) {
				return fmt.Errorf("expression %q takes flag %q, which the query doesn't", e.Name, name)
			}
		}
		if _, err := p.ParseExpr(e.PromQL); err != nil {
			return fmt.Errorf("expression %q: %w", e.Name, err)
		}
	}
	if c := q.Compare; c != nil {
		if !names[c.Left] || !names[c.Right] || c.Left == c.Right || len(c.On) == 0 {
			return fmt.Errorf("compare must name two of its expressions and the labels to pair on, not %+v", *c)
		}
	}
	return nil
}

// Names returns the file's query names, in file order.
func (f *File) Names() []string {
	names := make([]string, 0, len(f.Queries))
	for _, q := range f.Queries {
		names = append(names, q.Name)
	}
	return names
}

// Query returns the query called name.
func (f *File) Query(name string) (*Query, bool) {
	i := slices.IndexFunc(f.Queries, func(q Query) bool { return q.Name == name })
	if i < 0 {
		return nil, false
	}
	return &f.Queries[i], true
}

// FlagsOf returns the declarations of the flags q takes, in q's order.
func (f *File) FlagsOf(q *Query) []Flag {
	out := make([]Flag, 0, len(q.Flags))
	for _, name := range q.Flags {
		i := slices.IndexFunc(f.Flags, func(fl Flag) bool { return fl.Name == name })
		out = append(out, f.Flags[i])
	}
	return out
}

// Build returns q's expressions with one matcher added to every vector selector for each value set, keyed by
// flag name, that the expression takes, and every range selector's range set to window. It fails on a regex
// value that doesn't compile.
func (f *File) Build(q *Query, values map[string]string, window time.Duration) ([]Built, error) {
	declared := f.FlagsOf(q)
	p := newParser()
	out := make([]Built, 0, len(q.Expressions))
	for _, e := range q.Expressions {
		takes := q.Flags
		if len(e.Flags) > 0 {
			takes = e.Flags
		}
		var matchers []*labels.Matcher
		for _, fl := range declared {
			v, ok := values[fl.Name]
			if !ok || !slices.Contains(takes, fl.Name) {
				continue
			}
			typ := labels.MatchEqual
			if fl.Match == MatchRegex {
				typ = labels.MatchRegexp
			}
			m, err := labels.NewMatcher(typ, fl.Label, v)
			if err != nil {
				return nil, fmt.Errorf("--%s %q: %w", fl.Name, v, err)
			}
			matchers = append(matchers, m)
		}
		expr, err := p.ParseExpr(e.PromQL)
		if err != nil {
			return nil, fmt.Errorf("expression %q: %w", e.Name, err)
		}
		parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
			switch n := node.(type) {
			case *parser.VectorSelector:
				n.LabelMatchers = append(n.LabelMatchers, matchers...)
			case *parser.MatrixSelector:
				n.Range, n.RangeExpr = window, nil
			}
			return nil
		})
		out = append(out, Built{Name: e.Name, PromQL: expr.String()})
	}
	return out, nil
}

// Unpaired returns the label sets, on c.On, of left's series that no series of right shares, sorted.
func Unpaired(c *Compare, left, right []query.Series) [][]query.Label {
	key := func(s query.Series) []query.Label {
		var out []query.Label
		for _, name := range c.On {
			i := slices.IndexFunc(s.Labels, func(l query.Label) bool { return l.Name == name })
			if i >= 0 {
				out = append(out, s.Labels[i])
			}
		}
		return out
	}
	text := func(ls []query.Label) string {
		var b strings.Builder
		for _, l := range ls {
			fmt.Fprintf(&b, "%q=%q,", l.Name, l.Value)
		}
		return b.String()
	}
	paired := map[string]bool{}
	for _, s := range right {
		paired[text(key(s))] = true
	}
	unpaired := map[string][]query.Label{}
	for _, s := range left {
		if k := key(s); !paired[text(k)] {
			unpaired[text(k)] = k
		}
	}
	out := make([][]query.Label, 0, len(unpaired))
	for _, k := range slices.Sorted(maps.Keys(unpaired)) {
		out = append(out, unpaired[k])
	}
	return out
}
