package query

import (
	"cmp"
	"fmt"
	"slices"
)

// Limit names, as a truncation or a refused query names the limit that applied.
const (
	LimitSeries  = "series"
	LimitPoints  = "points"
	LimitSamples = "samples"
)

// Options bound what one query may load and return. A zero limit is no limit, and a zero MaxSamples is
// Prometheus's server default.
type Options struct {
	// MaxSeries is the most series a result or a series listing keeps.
	MaxSeries int
	// MaxPoints is the most points a range result keeps, across all its series.
	MaxPoints int
	// MaxSamples is the most samples one query may load, as Prometheus's --query.max-samples.
	MaxSamples int
}

// Truncation is a limit that cut a result: which limit, its value, and the series or points the full result had.
type Truncation struct {
	Limit string
	Value int
	Full  int
}

// LimitError is a query the engine refused for loading more samples than Options.MaxSamples allows.
type LimitError struct {
	Limit string
	Value int
	Err   error
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("the query loads more than the %d samples it may: %v", e.Value, e.Err)
}

func (e *LimitError) Unwrap() error { return e.Err }

// truncate cuts a vector or matrix result to opts's limits, keeping whole series, the first in label order.
func truncate(r *Result, opts Options) {
	if r.Type != Vector && r.Type != Matrix {
		return
	}
	keep := len(r.Series)
	limit, full := "", 0
	if opts.MaxSeries > 0 && keep > opts.MaxSeries {
		keep, limit, full = opts.MaxSeries, LimitSeries, len(r.Series)
	}
	// An instant result counts one point per series, so the point limit bounds only a range result.
	if r.Type == Matrix && opts.MaxPoints > 0 && r.Points() > opts.MaxPoints {
		points := 0
		for i, s := range r.Series[:keep] {
			points += len(s.Points)
			if points > opts.MaxPoints {
				keep, limit, full = i, LimitPoints, r.Points()
				break
			}
		}
	}
	if limit == "" {
		return
	}
	// The engine sorts a matrix by labels, and an instant result is sorted the same way before it's cut, so the
	// cut keeps the same series on every run.
	if r.Type == Vector {
		slices.SortStableFunc(r.Series, func(a, b Series) int { return compareLabels(a.Labels, b.Labels) })
	}
	r.Series = r.Series[:keep]
	value := opts.MaxSeries
	if limit == LimitPoints {
		value = opts.MaxPoints
	}
	r.Truncated = &Truncation{Limit: limit, Value: value, Full: full}
}

// truncateListing cuts a series listing, already in label order, to MaxSeries.
func (e *Engine) truncateListing(l *Listing) {
	if e.opts.MaxSeries <= 0 || len(l.Series) <= e.opts.MaxSeries {
		return
	}
	l.Truncated = &Truncation{Limit: LimitSeries, Value: e.opts.MaxSeries, Full: len(l.Series)}
	l.Series = l.Series[:e.opts.MaxSeries]
}

// compareLabels orders label sets as labels.Compare does.
func compareLabels(a, b []Label) int {
	for i := range min(len(a), len(b)) {
		if c := cmp.Compare(a[i].Name, b[i].Name); c != 0 {
			return c
		}
		if c := cmp.Compare(a[i].Value, b[i].Value); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(a), len(b))
}
