package query

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// The reasons a result is empty, as OUTPUT-4 names them.
const (
	OutsideDumpRange = "outside-dump-range"
	NoMatchingSeries = "no-matching-series"
	NoSamplesInRange = "no-samples-in-range"
	EmptyEvaluation  = "empty-evaluation"
)

// selector is one vector selector in an expression: its text, its matchers, and the window of raw samples the
// engine reads for it.
type selector struct {
	text     string
	matchers []*labels.Matcher
	// mint and maxt bound the samples read, widened by the lookback delta, ranges and offsets.
	mint, maxt int64
	// from is the earliest time the selector is evaluated at: mint for a range selector, and for an instant
	// selector the time its lookback reaches mint from.
	from int64
}

// selectorsOf returns the statement's distinct vector selectors in the order they appear, each with the window
// the engine reads for it. A selector is named in the printer's form with its offset and @ modifier, since
// up and up offset 1d read different samples; one that appears twice reads the union of its windows.
func selectorsOf(stmt *parser.EvalStmt) []selector {
	var out []selector
	index := map[string]int{}
	parser.Inspect(stmt.Expr, func(node parser.Node, path []parser.Node) error {
		vs, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		sel := selector{text: vs.String(), matchers: vs.LabelMatchers}
		sel.mint, sel.maxt = window(stmt, vs, path)
		sel.from = sel.mint
		if _, ranged := parent(path).(*parser.MatrixSelector); !ranged {
			sel.from += stmt.LookbackDelta.Milliseconds() - 1
		}
		i, seen := index[sel.text]
		if !seen {
			index[sel.text] = len(out)
			out = append(out, sel)
			return nil
		}
		out[i].mint = min(out[i].mint, sel.mint)
		out[i].maxt = max(out[i].maxt, sel.maxt)
		out[i].from = min(out[i].from, sel.from)
		return nil
	})
	return out
}

// window returns the samples the engine reads for one selector: promql.FindMinMaxTime over an expression that
// holds only that selector and the range and subqueries around it, which are all FindMinMaxTime reads of a
// selector's path.
func window(stmt *parser.EvalStmt, vs *parser.VectorSelector, path []parser.Node) (mint, maxt int64) {
	var e parser.Expr = vs
	if m, ok := parent(path).(*parser.MatrixSelector); ok {
		e = &parser.MatrixSelector{VectorSelector: vs, Range: m.Range}
	}
	for i := len(path) - 1; i >= 0; i-- {
		if sq, ok := path[i].(*parser.SubqueryExpr); ok {
			around := *sq
			around.Expr = e
			e = &around
		}
	}
	one := *stmt
	one.Expr = e
	return promql.FindMinMaxTime(&one)
}

func parent(path []parser.Node) parser.Node {
	if len(path) == 0 {
		return nil
	}
	return path[len(path)-1]
}

// span is the union of the selectors' windows.
func span(sels []selector) (mint, maxt int64) {
	mint, maxt = math.MaxInt64, math.MinInt64
	for _, s := range sels {
		mint, maxt = min(mint, s.mint), max(maxt, s.maxt)
	}
	return mint, maxt
}

// The pass reads each series at probes points spread across its selector's window, up to gapsPerProbe gaps at
// each, so a change in the interval partway through the range still shows.
const (
	probes       = 4
	gapsPerProbe = 8
)

// IntervalVaries is the warning code for samples whose spacing changes across the window.
const IntervalVaries = "sample-interval-varies"

// scanned is what a pass over the selectors' raw samples found.
type scanned struct {
	// gaps counts, at each probe, the gaps between consecutive samples of a series, by length in milliseconds.
	gaps [probes]map[int64]int
}

// scan reads each selector's raw samples in its window at each probe.
func scan(ctx context.Context, q storage.Queryable, sels []selector) (_ scanned, err error) {
	var sc scanned
	for i := range sc.gaps {
		sc.gaps[i] = map[int64]int{}
	}
	err = eachSeries(ctx, q, sels, func(sel selector, it chunkenc.Iterator) (bool, error) {
		return false, sc.probe(it, sel.mint, sel.maxt)
	})
	return sc, err
}

// eachSeries calls f with an iterator over each series each selector matches in its window, until f returns
// true for that selector.
func eachSeries(ctx context.Context, q storage.Queryable, sels []selector,
	f func(selector, chunkenc.Iterator) (bool, error)) (err error) {
	if len(sels) == 0 {
		return nil
	}
	querier, err := q.Querier(span(sels))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, querier.Close()) }()

	var it chunkenc.Iterator
	for _, sel := range sels {
		set := querier.Select(ctx, false, &storage.SelectHints{Start: sel.mint, End: sel.maxt}, sel.matchers...)
		for set.Next() {
			it = set.At().Iterator(it)
			done, err := f(sel, it)
			if err != nil {
				return err
			}
			if done {
				break
			}
		}
		if err := set.Err(); err != nil {
			return err
		}
	}
	return nil
}

// probe reads one series' gaps at each probe point in [mint, maxt]. The gaps read at a probe may run past the
// next probe point, so samples further apart than a probe's share of the window still have their gap counted.
func (sc scanned) probe(it chunkenc.Iterator, mint, maxt int64) error {
	for i := range probes {
		from := mint + int64(i)*(maxt-mint)/probes
		until := maxt // A probe starts before the next probe point, or the window's end.
		if i+1 < probes {
			until = mint + int64(i+1)*(maxt-mint)/probes - 1
		}
		// Seek decodes forward to from, and does nothing when the iterator is already past it.
		if it.Seek(from) == chunkenc.ValNone || it.AtT() > maxt {
			break
		}
		prev := it.AtT()
		if prev > until {
			continue
		}
		for range gapsPerProbe {
			if it.Next() == chunkenc.ValNone {
				break
			}
			t := it.AtT()
			if t > maxt {
				break
			}
			sc.gaps[i][roundGap(t-prev)]++
			prev = t
		}
	}
	return it.Err()
}

// roundGap rounds a gap of a second or more to the second, so scrape jitter doesn't split one interval into
// many.
func roundGap(ms int64) int64 {
	if ms < 1000 {
		return ms
	}
	return (ms + 500) / 1000 * 1000
}

// interval returns the median of every gap read, and false when no series had two samples close together.
func (sc scanned) interval() (time.Duration, bool) {
	all := map[int64]int{}
	for _, g := range sc.gaps {
		for gap, n := range g {
			all[gap] += n
		}
	}
	return median(all)
}

// intervalWarning warns when the median gap differs between probes, which is when one sample interval
// misdescribes the window.
func (sc scanned) intervalWarning() (Warning, bool) {
	var medians []string
	distinct := map[time.Duration]bool{}
	for _, g := range sc.gaps {
		m, ok := median(g)
		if !ok {
			medians = append(medians, "none")
			continue
		}
		distinct[m] = true
		medians = append(medians, duration(m))
	}
	if len(distinct) < 2 {
		return Warning{}, false
	}
	return Warning{Code: IntervalVaries, Message: fmt.Sprintf(
		"the gap between samples changes across the window: %s at %d points from its start to its end; "+
			"sample_interval is the median of them", strings.Join(medians, ", "), probes)}, true
}

// median returns the median of gaps counted by length, and false when there are none.
func median(gaps map[int64]int) (time.Duration, bool) {
	total := 0
	for _, n := range gaps {
		total += n
	}
	if total == 0 {
		return 0, false
	}
	seen := 0
	for _, gap := range slices.Sorted(maps.Keys(gaps)) {
		seen += gaps[gap]
		if 2*seen >= total {
			return time.Duration(gap) * time.Millisecond, true
		}
	}
	return 0, false // Unreachable: the loop passes half of total.
}

// explain says why a result over [start, end] is empty. withoutSamples returns the selectors with no sample the
// result could show; it runs only when every selector matches a series somewhere in the dump.
func explain(ctx context.Context, d Dump, sels []selector, start, end int64,
	withoutSamples func() ([]selector, error)) (*Empty, error) {
	if len(sels) == 0 {
		return &Empty{Reason: EmptyEvaluation, Selectors: []string{}}, nil
	}
	if outsideDump(d, start, end) {
		return &Empty{Reason: OutsideDumpRange, Selectors: []string{}}, nil
	}
	unmatched, err := unmatchedSelectors(ctx, d, sels)
	if err != nil {
		return nil, err
	}
	if len(unmatched) > 0 {
		return &Empty{Reason: NoMatchingSeries, Selectors: texts(unmatched)}, nil
	}
	without, err := withoutSamples()
	if err != nil {
		return nil, err
	}
	if len(without) > 0 {
		return &Empty{Reason: NoSamplesInRange, Selectors: texts(without)}, nil
	}
	return &Empty{Reason: EmptyEvaluation, Selectors: []string{}}, nil
}

// outsideDump reports whether [start, end] misses the dump's range.
func outsideDump(d Dump, start, end int64) bool {
	return end < d.Start.UnixMilli() || start > d.End.UnixMilli()
}

// unsampled returns the selectors none of whose series has a sample the engine could show in the selector's
// window. A stale marker, which Prometheus writes when a target disappears, ends a series rather than being a
// sample: a range selector drops it, and an instant selector shows nothing while it's the newest sample in the
// lookback. So a sample counts when it isn't a stale marker and, for an instant selector, the sample after it
// comes after the selector's first evaluation, or there is none.
func unsampled(ctx context.Context, q storage.Queryable, sels []selector) ([]selector, error) {
	sampled := map[string]bool{}
	err := eachSeries(ctx, q, sels, func(sel selector, it chunkenc.Iterator) (bool, error) {
		live := false // The previous sample isn't a stale marker.
		for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
			t := it.AtT()
			if t > sel.maxt {
				break
			}
			if live && t > sel.from {
				break
			}
			live = !stale(it, typ)
		}
		if live {
			sampled[sel.text] = true
		}
		return live, it.Err()
	})
	if err != nil {
		return nil, err
	}
	var out []selector
	for _, sel := range sels {
		if !sampled[sel.text] {
			out = append(out, sel)
		}
	}
	return out, nil
}

// stale reports whether the iterator's sample is a stale marker.
func stale(it chunkenc.Iterator, typ chunkenc.ValueType) bool {
	switch typ {
	case chunkenc.ValFloat:
		_, f := it.At()
		return value.IsStaleNaN(f)
	case chunkenc.ValHistogram:
		_, h := it.AtHistogram(nil)
		return value.IsStaleNaN(h.Sum)
	case chunkenc.ValFloatHistogram:
		_, h := it.AtFloatHistogram(nil)
		return value.IsStaleNaN(h.Sum)
	}
	return false
}

// unmatchedSelectors returns the selectors that match no series anywhere in the dump, stopping at each one's
// first match.
func unmatchedSelectors(ctx context.Context, d Dump, sels []selector) (_ []selector, err error) {
	querier, err := d.Queryable.Querier(d.Start.UnixMilli(), d.End.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, querier.Close()) }()
	var out []selector
	for _, sel := range sels {
		hints := &storage.SelectHints{Start: d.Start.UnixMilli(), End: d.End.UnixMilli(), Func: "series"}
		set := querier.Select(ctx, false, hints, sel.matchers...)
		matched := set.Next()
		if err := set.Err(); err != nil {
			return nil, err
		}
		if !matched {
			out = append(out, sel)
		}
	}
	return out, nil
}

func texts(sels []selector) []string {
	out := make([]string, 0, len(sels))
	for _, s := range sels {
		out = append(out, s.text)
	}
	return out
}
