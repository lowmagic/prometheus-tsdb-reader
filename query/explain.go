package query

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
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

// selector is one vector selector in an expression, as text and as matchers.
type selector struct {
	text     string
	matchers []*labels.Matcher
}

// selectorsOf returns the expression's distinct vector selectors in the order they appear, without their
// offsets and @ modifiers, which the time window accounts for.
func selectorsOf(expr parser.Expr) []selector {
	var out []selector
	seen := map[string]bool{}
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		vs, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		bare := &parser.VectorSelector{Name: vs.Name, LabelMatchers: vs.LabelMatchers}
		if text := bare.String(); !seen[text] {
			seen[text] = true
			out = append(out, selector{text: text, matchers: vs.LabelMatchers})
		}
		return nil
	})
	return out
}

// The pass reads each series at probes points spread across the window, up to gapsPerProbe gaps at each, so
// its cost grows with the number of series rather than the length of the range, and a change in the interval
// partway through the range still shows.
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
	// withoutSamples are the selectors with no sample in the window.
	withoutSamples []selector
}

// scan reads the selectors' raw samples in [mint, maxt] at each probe.
func scan(ctx context.Context, q storage.Queryable, sels []selector, mint, maxt int64) (_ scanned, err error) {
	var sc scanned
	for i := range sc.gaps {
		sc.gaps[i] = map[int64]int{}
	}
	if len(sels) == 0 {
		return sc, nil
	}
	querier, err := q.Querier(mint, maxt)
	if err != nil {
		return scanned{}, err
	}
	defer func() { err = errors.Join(err, querier.Close()) }()

	var it chunkenc.Iterator
	for _, sel := range sels {
		sampled := false
		set := querier.Select(ctx, false, &storage.SelectHints{Start: mint, End: maxt}, sel.matchers...)
		for set.Next() {
			it = set.At().Iterator(it)
			s, err := sc.probe(it, mint, maxt)
			if err != nil {
				return scanned{}, err
			}
			sampled = sampled || s
		}
		if err := set.Err(); err != nil {
			return scanned{}, err
		}
		if !sampled {
			sc.withoutSamples = append(sc.withoutSamples, sel)
		}
	}
	return sc, nil
}

// probe reads one series' gaps at each probe point and reports whether it has a sample in [mint, maxt].
func (sc scanned) probe(it chunkenc.Iterator, mint, maxt int64) (sampled bool, _ error) {
	for i := range probes {
		from := mint + int64(i)*(maxt-mint)/probes
		until := maxt // The probe reads up to the next probe point, or to the window's end.
		if i+1 < probes {
			until = mint + int64(i+1)*(maxt-mint)/probes - 1
		}
		// Seek skips whole chunks without decoding them, and does nothing when the iterator is already past from.
		if it.Seek(from) == chunkenc.ValNone || it.AtT() > maxt {
			break
		}
		sampled = true
		prev := it.AtT()
		if prev > until {
			continue
		}
		for range gapsPerProbe {
			if it.Next() == chunkenc.ValNone {
				break
			}
			t := it.AtT()
			if t > until {
				break
			}
			sc.gaps[i][roundGap(t-prev)]++
			prev = t
		}
	}
	return sampled, it.Err()
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

// explain says why a result over the window [mint, maxt] is empty, given the selectors with no samples in it.
func explain(ctx context.Context, d Dump, sels []selector, mint, maxt int64, withoutSamples []selector) (*Empty, error) {
	if len(sels) == 0 {
		return &Empty{Reason: EmptyEvaluation, Selectors: []string{}}, nil
	}
	if maxt < d.Start.UnixMilli() || mint > d.End.UnixMilli() {
		return &Empty{Reason: OutsideDumpRange, Selectors: []string{}}, nil
	}
	unmatched, err := unmatchedSelectors(ctx, d, sels)
	if err != nil {
		return nil, err
	}
	if len(unmatched) > 0 {
		return &Empty{Reason: NoMatchingSeries, Selectors: texts(unmatched)}, nil
	}
	if len(withoutSamples) > 0 {
		return &Empty{Reason: NoSamplesInRange, Selectors: texts(withoutSamples)}, nil
	}
	return &Empty{Reason: EmptyEvaluation, Selectors: []string{}}, nil
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
