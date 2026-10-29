package query

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

// Listing is a series or label listing, the range it covers and why it's empty when it is.
type Listing struct {
	Start, End time.Time
	// Selectors are the selectors the listing was narrowed by, as given.
	Selectors []string
	// Series holds a series listing's label sets, in label order.
	Series [][]Label
	// Name is the label whose values a label listing holds; empty for a listing of label names.
	Name string
	// Labels holds a label listing's names or values, sorted.
	Labels []string
	// Empty says why the listing is empty, and is nil when it isn't.
	Empty *Empty
}

// Series lists the series matching any of the selectors with samples between start and end.
func (e *Engine) Series(ctx context.Context, d Dump, selectors []string, start, end Time) (_ *Listing, err error) {
	l, sels, err := e.listing(d, selectors, start, end)
	if err != nil {
		return nil, err
	}
	mint, maxt := l.Start.UnixMilli(), l.End.UnixMilli()
	querier, err := d.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, querier.Close()) }()

	hints := &storage.SelectHints{Start: mint, End: maxt, Func: "series"}
	sets := make([]storage.SeriesSet, 0, len(sels))
	for _, sel := range sels {
		sets = append(sets, querier.Select(ctx, true, hints, sel.matchers...))
	}
	set := storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
	for set.Next() {
		l.Series = append(l.Series, labelsOf(set.At().Labels()))
	}
	if err := set.Err(); err != nil {
		return nil, err
	}
	if len(l.Series) == 0 {
		// The listing is empty, so no selector matched a series in the range.
		l.Empty, err = explain(ctx, d, sels, mint, maxt, func() ([]selector, error) { return sels, nil })
	}
	return l, err
}

// Labels lists label names, or the values of the label name when it isn't empty, on series with samples
// between start and end, narrowed to the series matching any of the selectors when there are some.
func (e *Engine) Labels(ctx context.Context, d Dump, name string, selectors []string, start, end Time) (_ *Listing, err error) {
	l, sels, err := e.listing(d, selectors, start, end)
	if err != nil {
		return nil, err
	}
	l.Name = name
	mint, maxt := l.Start.UnixMilli(), l.End.UnixMilli()
	querier, err := d.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, querier.Close()) }()

	hints := &storage.LabelHints{}
	matcherSets := [][]*labels.Matcher{nil}
	if len(sels) > 0 {
		matcherSets = matcherSets[:0]
		for _, sel := range sels {
			matcherSets = append(matcherSets, sel.matchers)
		}
	}
	seen := map[string]bool{}
	for _, ms := range matcherSets {
		var found []string
		if name == "" {
			found, _, err = querier.LabelNames(ctx, hints, ms...)
		} else {
			found, _, err = querier.LabelValues(ctx, name, hints, ms...)
		}
		if err != nil {
			return nil, err
		}
		for _, f := range found {
			seen[f] = true
		}
	}
	l.Labels = make([]string, 0, len(seen))
	for f := range seen {
		// A block's index hands out strings that live only as long as the block is open.
		l.Labels = append(l.Labels, strings.Clone(f))
	}
	slices.Sort(l.Labels)

	if len(l.Labels) > 0 {
		return l, nil
	}
	// Only a series carrying the label has a value for it, so the reason is found for each selector, or with
	// none for every series, narrowed to those, and names the narrowed selectors.
	if name != "" {
		if len(sels) == 0 {
			sels = []selector{{}}
		}
		for i, sel := range sels {
			sels[i] = carrying(sel, name)
		}
	}
	if len(sels) == 0 {
		// Label names with no selector cover every series, so they're empty only where the dump has no samples.
		reason := NoSamplesInRange
		if outsideDump(d, mint, maxt) {
			reason = OutsideDumpRange
		}
		l.Empty = &Empty{Reason: reason, Selectors: []string{}}
		return l, nil
	}
	// The listing is empty, so no selector matched a series in the range.
	l.Empty, err = explain(ctx, d, sels, mint, maxt, func() ([]selector, error) { return sels, nil })
	return l, err
}

// carrying narrows a selector to the series that carry the label name, and names it in the printer's form, as
// up{mode!=""}.
func carrying(sel selector, name string) selector {
	ms := append(slices.Clip(sel.matchers), labels.MustNewMatcher(labels.MatchNotEqual, name, ""))
	vs := &parser.VectorSelector{LabelMatchers: ms}
	for _, m := range ms {
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual {
			vs.Name = m.Value
		}
	}
	return selector{text: vs.String(), matchers: ms}
}

// listing resolves a listing's range and parses its selectors.
func (e *Engine) listing(d Dump, selectors []string, start, end Time) (*Listing, []selector, error) {
	s, en, err := listingBounds(d, start, end)
	if err != nil {
		return nil, nil, err
	}
	sels := make([]selector, 0, len(selectors))
	for _, text := range selectors {
		ms, err := e.parser.ParseMetricSelector(text)
		if err != nil {
			return nil, nil, fmt.Errorf("parse selector %q: %w", text, err)
		}
		sels = append(sels, selector{text: text, matchers: ms})
	}
	return &Listing{Start: s, End: en, Selectors: selectors}, sels, nil
}
