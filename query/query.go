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
	// How far back an instant selector looks for a sample.
	lookbackDelta = 5 * time.Minute
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

// Result is an evaluated expression and what qualifies it.
type Result struct {
	Type ValueType
	// Series holds a vector's or a matrix's series, in the engine's order.
	Series []Series
	// Scalar holds a scalar result.
	Scalar Point
	// String holds a string result.
	String StringPoint

	// Time is an instant query's evaluation time.
	Time time.Time
	// Start, End and Step are a range query's.
	Start, End time.Time
	Step       time.Duration
	// SampleInterval is the median gap between the selectors' raw samples, when HasSampleInterval.
	SampleInterval    time.Duration
	HasSampleInterval bool
	// Empty says why the result is empty, and is nil when it isn't.
	Empty *Empty
	// Warnings are the step warnings, the sample interval's, then the engine's annotations, sorted, under the
	// code "promql".
	Warnings []Warning
}

// Empty is why a result is empty, and the selectors at fault.
type Empty struct {
	Reason    string
	Selectors []string
}

// Warning is something the reader should know about a result, with a code to match on.
type Warning struct {
	Code, Message string
}

// Dump is the data a query runs over and the range of samples it holds.
type Dump struct {
	Queryable  storage.Queryable
	Start, End time.Time
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
	ng     *promql.Engine
	parser parser.Parser
}

// NewEngine returns an engine configured with Prometheus's server defaults.
func NewEngine() *Engine {
	p := parser.NewParser(parser.Options{ExperimentalDurationExpr: true})
	return &Engine{parser: p, ng: promql.NewEngine(promql.EngineOpts{
		MaxSamples:    defaultMaxSamples,
		Timeout:       noTimeout,
		LookbackDelta: lookbackDelta,
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return defaultEvaluationInterval.Milliseconds()
		},
		// The server always enables these two.
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		Parser:               p,
	})}
}

// Instant evaluates expr over d at the time at names.
func (e *Engine) Instant(ctx context.Context, d Dump, expr string, at Time) (*Result, error) {
	t := at.At(d.End)
	qry, err := e.ng.NewInstantQuery(ctx, d.Queryable, nil, expr, t)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", expr, err)
	}
	res, err := execute(ctx, d, qry, expr)
	if err != nil {
		return nil, err
	}
	res.Time = t
	return res, nil
}

// Span is a range query's bounds and step. The zero Span is the 2h before the dump's end at STEP-1's step.
type Span struct {
	Start, End Time
	// Step is the step, or zero for the default.
	Step time.Duration
}

// Range evaluates expr over d across span.
func (e *Engine) Range(ctx context.Context, d Dump, expr string, span Span) (*Result, error) {
	start, end, err := bounds(d, span.Start, span.End)
	if err != nil {
		return nil, err
	}
	step := span.Step
	if step == 0 {
		step = defaultStep(start, end)
	}
	qry, err := e.ng.NewRangeQuery(ctx, d.Queryable, nil, expr, start, end, step)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", expr, err)
	}
	res, err := execute(ctx, d, qry, expr)
	if err != nil {
		return nil, err
	}
	res.Start, res.End, res.Step = start, end, step
	res.Warnings = append(stepWarnings(step, res.SampleInterval, res.HasSampleInterval), res.Warnings...)
	return res, nil
}

// execute runs a prepared query, then reads its selectors once more for the sample interval and, when the
// result is empty, the reason.
func execute(ctx context.Context, d Dump, qry promql.Query, expr string) (*Result, error) {
	defer qry.Close()
	// The engine rewrites the statement as it evaluates, as when it replaces a subquery with the series it
	// computed, so the selectors and their windows are read before it runs.
	stmt, ok := qry.Statement().(*parser.EvalStmt)
	if !ok {
		return nil, fmt.Errorf("unexpected statement %T", qry.Statement())
	}
	sels := selectorsOf(stmt)
	start, end := stmt.Start.UnixMilli(), stmt.End.UnixMilli()

	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, fmt.Errorf("evaluate %q: %w", expr, res.Err)
	}
	out, err := convert(res.Value)
	if err != nil {
		return nil, err
	}
	warnings, infos := res.Warnings.AsStrings(expr, 0, 0)
	annotations := slices.Concat(warnings, infos)
	slices.Sort(annotations)
	for _, a := range annotations {
		out.Warnings = append(out.Warnings, Warning{Code: "promql", Message: a})
	}

	sc, err := scan(ctx, d.Queryable, sels)
	if err != nil {
		return nil, err
	}
	out.SampleInterval, out.HasSampleInterval = sc.interval()
	if w, ok := sc.intervalWarning(); ok {
		out.Warnings = append([]Warning{w}, out.Warnings...)
	}
	if out.isEmpty() {
		out.Empty, err = explain(ctx, d, sels, start, end, func() ([]selector, error) {
			return unsampled(ctx, d.Queryable, sels)
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r *Result) isEmpty() bool {
	return (r.Type == Vector || r.Type == Matrix) && len(r.Series) == 0
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
