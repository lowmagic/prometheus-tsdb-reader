// Package query evaluates PromQL with Prometheus's own engine and returns the tool's own result types.
package query

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

// Prometheus's server defaults, so an expression answers here as it does in a server started with no flags.
const (
	defaultMaxSamples = 50_000_000
	// The global evaluation_interval, which sets the step of a subquery written without one.
	defaultEvaluationInterval = time.Minute
	// The engine needs a timeout. A run has no deadline of its own; an interrupt cancels it instead.
	noTimeout = 24 * time.Hour
)

// ValueType is a result's type, as the Prometheus HTTP API names it.
type ValueType string

// The result types.
const (
	Vector ValueType = "vector"
	Matrix ValueType = "matrix"
	Scalar ValueType = "scalar"
	String ValueType = "string"
)

// Result is an evaluated expression.
type Result struct {
	Type ValueType
	// Series holds a vector's or a matrix's series, in the engine's order.
	Series []Series
	// Scalar holds a scalar result.
	Scalar Point
	// String holds a string result.
	String StringPoint
	// Annotations are the engine's warnings and infos, sorted.
	Annotations []string
}

// Points counts the result's points: one per series in a vector, every point in a matrix.
func (r *Result) Points() int {
	switch r.Type {
	case Scalar, String:
		return 1
	}
	n := 0
	for _, s := range r.Series {
		n += len(s.Points)
	}
	return n
}

// Series is one series' labels and points. An instant query's series has one point.
type Series struct {
	Labels []Label
	Points []Point
}

// Label is one label pair.
type Label struct {
	Name, Value string
}

// Point is one sample: a float, or a native histogram when H isn't nil.
type Point struct {
	T time.Time
	F float64
	H *Histogram
}

// StringPoint is a string result's value.
type StringPoint struct {
	T time.Time
	V string
}

// Histogram is a native histogram sample, with only its populated buckets.
type Histogram struct {
	Count, Sum float64
	Buckets    []Bucket
}

// Bucket is one histogram bucket.
type Bucket struct {
	Lower, Upper                   float64
	LowerInclusive, UpperInclusive bool
	Count                          float64
}

// Engine evaluates PromQL as a Prometheus server started with default flags does.
type Engine struct {
	ng *promql.Engine
}

// NewEngine returns an engine configured with Prometheus's server defaults.
func NewEngine() *Engine {
	return &Engine{ng: promql.NewEngine(promql.EngineOpts{
		MaxSamples: defaultMaxSamples,
		Timeout:    noTimeout,
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return defaultEvaluationInterval.Milliseconds()
		},
		// The server always enables these two.
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		Parser:               parser.NewParser(parser.Options{ExperimentalDurationExpr: true}),
	})}
}

// Instant evaluates expr at t over q.
func (e *Engine) Instant(ctx context.Context, q storage.Queryable, expr string, t time.Time) (*Result, error) {
	qry, err := e.ng.NewInstantQuery(ctx, q, nil, expr, t)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", expr, err)
	}
	defer qry.Close()
	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, fmt.Errorf("evaluate %q: %w", expr, res.Err)
	}
	out, err := convert(res.Value)
	if err != nil {
		return nil, err
	}
	warnings, infos := res.Warnings.AsStrings(expr, 0, 0)
	out.Annotations = slices.Concat(warnings, infos)
	slices.Sort(out.Annotations)
	return out, nil
}

func convert(v parser.Value) (*Result, error) {
	switch v := v.(type) {
	case promql.Vector:
		r := &Result{Type: Vector, Series: make([]Series, 0, len(v))}
		for _, s := range v {
			r.Series = append(r.Series, Series{
				Labels: labelsOf(s.Metric),
				Points: []Point{pointOf(s.T, s.F, s.H)},
			})
		}
		return r, nil
	case promql.Matrix:
		r := &Result{Type: Matrix, Series: make([]Series, 0, len(v))}
		for _, s := range v {
			r.Series = append(r.Series, seriesOf(s))
		}
		return r, nil
	case promql.Scalar:
		return &Result{Type: Scalar, Scalar: pointOf(v.T, v.V, nil)}, nil
	case promql.String:
		return &Result{Type: String, String: StringPoint{T: timestamp.Time(v.T).UTC(), V: v.V}}, nil
	}
	return nil, fmt.Errorf("unexpected result type %T", v)
}

// seriesOf merges a matrix series' float and histogram points into time order, as one series can hold both.
func seriesOf(s promql.Series) Series {
	points := make([]Point, 0, len(s.Floats)+len(s.Histograms))
	for _, p := range s.Floats {
		points = append(points, pointOf(p.T, p.F, nil))
	}
	for _, p := range s.Histograms {
		points = append(points, pointOf(p.T, 0, p.H))
	}
	if len(s.Floats) > 0 && len(s.Histograms) > 0 {
		slices.SortStableFunc(points, func(a, b Point) int { return a.T.Compare(b.T) })
	}
	return Series{Labels: labelsOf(s.Metric), Points: points}
}

func labelsOf(ls labels.Labels) []Label {
	out := make([]Label, 0, ls.Len())
	ls.Range(func(l labels.Label) { out = append(out, Label{Name: l.Name, Value: l.Value}) })
	return out
}

func pointOf(t int64, f float64, h *histogram.FloatHistogram) Point {
	p := Point{T: timestamp.Time(t).UTC(), F: f}
	if h != nil {
		p.H = histogramOf(h)
	}
	return p
}

func histogramOf(h *histogram.FloatHistogram) *Histogram {
	out := &Histogram{Count: h.Count, Sum: h.Sum}
	it := h.AllBucketIterator()
	for it.Next() {
		b := it.At()
		if b.Count == 0 {
			continue
		}
		out.Buckets = append(out.Buckets, Bucket{
			Lower: b.Lower, Upper: b.Upper,
			LowerInclusive: b.LowerInclusive, UpperInclusive: b.UpperInclusive,
			Count: b.Count,
		})
	}
	return out
}
