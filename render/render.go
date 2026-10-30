// Package render writes results as JSON lines: a summary record first, then one record per series.
package render

import (
	"encoding/json"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/prometheus/common/model"

	"github.com/junglestyle/prometheus-tsdb-reader/query"
)

// Summary is what qualifies every result: what ran, over what dump, with which engine, and what the dump is
// missing.
type Summary struct {
	Command       string
	PromQLVersion string
	Dump          Dump
	WAL           WAL
	Gaps          []Gap
}

// Dump is the dump's range and the TSDB root the run used.
type Dump struct {
	Start, End time.Time
	Root       string
}

// WAL is what the run did with the WAL, its size, and its newest sample when Sampled. A WAL that failed has an
// Error and the span it leaves Missing.
type WAL struct {
	State   string
	Bytes   int64
	Newest  time.Time
	Sampled bool
	Error   string
	Missing *Missing
}

// Missing is the span of samples a failed WAL leaves out. Start is unknown unless Started, and the span is
// open-ended unless Ended.
type Missing struct {
	Start   time.Time
	Started bool
	End     time.Time
	Ended   bool
}

// Block is one block as info lists it. End is exclusive. Start, End and Series are unknown unless Known. Error
// is why the block didn't open, or "".
type Block struct {
	Dir        string
	Known      bool
	Start, End time.Time
	Series     uint64
	Status     string
	Error      string
}

// Gap is a corrupt block's range, which the dump is missing. Start and End are unknown unless Known.
type Gap struct {
	Block      string
	Known      bool
	Start, End time.Time
	Error      string
}

// Info writes info's summary, then one record per block.
func Info(w io.Writer, s Summary, blocks []Block) error {
	enc := newEncoder(w)
	if err := enc.Encode(headOf(s)); err != nil {
		return err
	}
	for _, b := range blocks {
		rec := blockRecord{Kind: "block", Dir: b.Dir, Status: b.Status, Error: optional(b.Error)}
		if b.Known {
			rec.Start, rec.End, rec.Series = timePtr(b.Start), timePtr(b.End), &b.Series
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}

// Instant writes an instant query's summary, then its records.
func Instant(w io.Writer, s Summary, expr string, r *query.Result) error {
	enc := newEncoder(w)
	summary := instantSummary{summaryHead: headOf(s), Expression: expr, Time: timeOf(r.Time), resultTail: tailOf(r)}
	if err := enc.Encode(summary); err != nil {
		return err
	}
	return writeResult(enc, r)
}

// Range writes a range query's summary, then its records.
func Range(w io.Writer, s Summary, expr string, r *query.Result) error {
	enc := newEncoder(w)
	summary := rangeSummary{
		summaryHead: headOf(s),
		Expression:  expr,
		Start:       timeOf(r.Start),
		End:         timeOf(r.End),
		Step:        duration(r.Step),
		resultTail:  tailOf(r),
	}
	if err := enc.Encode(summary); err != nil {
		return err
	}
	return writeResult(enc, r)
}

// Series writes a series listing's summary, then one record per series.
func Series(w io.Writer, s Summary, l *query.Listing) error {
	enc := newEncoder(w)
	summary := seriesSummary{
		summaryHead: headOf(s),
		Selectors:   l.Selectors,
		Start:       timeOf(l.Start),
		End:         timeOf(l.End),
		Series:      len(l.Series),
		Truncated:   truncatedOf(l.Truncated),
		Empty:       emptyOf(l.Empty),
	}
	if err := enc.Encode(summary); err != nil {
		return err
	}
	for _, ls := range l.Series {
		if err := enc.Encode(seriesRecord{Kind: "series", Metric: metricOf(ls)}); err != nil {
			return err
		}
	}
	return nil
}

// Labels writes a label listing's summary, then one labels record.
func Labels(w io.Writer, s Summary, l *query.Listing) error {
	enc := newEncoder(w)
	selectors := l.Selectors
	if selectors == nil {
		selectors = []string{}
	}
	summary := labelsSummary{
		summaryHead: headOf(s),
		Selectors:   selectors,
		Start:       timeOf(l.Start),
		End:         timeOf(l.End),
		Empty:       emptyOf(l.Empty),
	}
	if err := enc.Encode(summary); err != nil {
		return err
	}
	rec := labelsRecord{Kind: "labels", Names: l.Labels}
	if l.Name != "" {
		rec = labelsRecord{Kind: "labels", Name: l.Name, Values: l.Labels}
	}
	return enc.Encode(rec)
}

func writeResult(enc *json.Encoder, r *query.Result) error {
	switch r.Type {
	case query.Scalar:
		return enc.Encode(valueRecord{Kind: "scalar", Value: samplePair(r.Scalar)})
	case query.String:
		return enc.Encode(valueRecord{Kind: "string", Value: pair{timeOf(r.String.T), r.String.V}})
	}
	for _, series := range r.Series {
		rec := seriesRecord{Kind: "series", Metric: metricOf(series.Labels)}
		if r.Type == query.Vector {
			p := samplePair(series.Points[0])
			rec.Value = &p
		} else {
			for _, p := range series.Points {
				rec.Values = append(rec.Values, samplePair(p))
			}
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}

// Error writes the record a failed run leaves as its only line on stdout.
func Error(w io.Writer, exit int, message string) error {
	return newEncoder(w).Encode(errorRecord{Kind: "error", Exit: exit, Message: message})
}

// LimitError writes the record a run a limit refused leaves as its only line on stdout, naming the limit and its
// value.
func LimitError(w io.Writer, exit int, message, limit string, value int) error {
	return newEncoder(w).Encode(errorRecord{Kind: "error", Exit: exit, Message: message, Limit: limit, Value: &value})
}

func newEncoder(w io.Writer) *json.Encoder {
	enc := json.NewEncoder(w)
	// PromQL is full of < and >, and a reader should see them as they are.
	enc.SetEscapeHTML(false)
	return enc
}

// summaryHead is the summary fields every command writes. Embedded, its fields come first in the record.
type summaryHead struct {
	Kind          string   `json:"kind"`
	Command       string   `json:"command"`
	PromQLVersion string   `json:"promql_version"`
	Dump          dumpRec  `json:"dump"`
	WAL           walRec   `json:"wal"`
	Gaps          []gapRec `json:"gaps"`
}

type instantSummary struct {
	summaryHead
	Expression string `json:"expression"`
	Time       timeOf `json:"time"`
	resultTail
}

type rangeSummary struct {
	summaryHead
	Expression string `json:"expression"`
	Start      timeOf `json:"start"`
	End        timeOf `json:"end"`
	Step       string `json:"step"`
	resultTail
}

// resultTail is the summary fields a query result ends with.
type resultTail struct {
	Series         int           `json:"series"`
	Points         int           `json:"points"`
	Truncated      *truncatedRec `json:"truncated"`
	Empty          *emptyRec     `json:"empty"`
	SampleInterval *string       `json:"sample_interval"`
	Warnings       []warning     `json:"warnings"`
}

type seriesSummary struct {
	summaryHead
	Selectors []string      `json:"selectors"`
	Start     timeOf        `json:"start"`
	End       timeOf        `json:"end"`
	Series    int           `json:"series"`
	Truncated *truncatedRec `json:"truncated"`
	Empty     *emptyRec     `json:"empty"`
}

type labelsSummary struct {
	summaryHead
	Selectors []string  `json:"selectors"`
	Start     timeOf    `json:"start"`
	End       timeOf    `json:"end"`
	Empty     *emptyRec `json:"empty"`
}

type truncatedRec struct {
	Limit string `json:"limit"`
	Value int    `json:"value"`
	Full  int    `json:"full"`
}

type emptyRec struct {
	Reason    string   `json:"reason"`
	Selectors []string `json:"selectors"`
}

type labelsRecord struct {
	Kind   string   `json:"kind"`
	Names  []string `json:"names,omitempty"`
	Name   string   `json:"name,omitempty"`
	Values []string `json:"values,omitempty"`
}

type dumpRec struct {
	Start timeOf `json:"start"`
	End   timeOf `json:"end"`
	Root  string `json:"root"`
}

type walRec struct {
	State   string      `json:"state"`
	Bytes   int64       `json:"bytes"`
	Newest  *timeOf     `json:"newest"`
	Error   *string     `json:"error,omitempty"`
	Missing *missingRec `json:"missing,omitempty"`
}

type missingRec struct {
	Start *timeOf `json:"start"`
	End   *timeOf `json:"end"`
}

type gapRec struct {
	Block string  `json:"block"`
	Start *timeOf `json:"start"`
	End   *timeOf `json:"end"`
	Error string  `json:"error"`
}

type blockRecord struct {
	Kind   string  `json:"kind"`
	Dir    string  `json:"dir"`
	Start  *timeOf `json:"start"`
	End    *timeOf `json:"end"`
	Series *uint64 `json:"series"`
	Status string  `json:"status"`
	// Error is the reason a block didn't open; null for one that did.
	Error *string `json:"error"`
}

type warning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type seriesRecord struct {
	Kind   string            `json:"kind"`
	Metric map[string]string `json:"metric"`
	Value  *pair             `json:"value,omitempty"`
	Values []pair            `json:"values,omitempty"`
}

type valueRecord struct {
	Kind  string `json:"kind"`
	Value pair   `json:"value"`
}

type errorRecord struct {
	Kind    string `json:"kind"`
	Exit    int    `json:"exit"`
	Message string `json:"message"`
	Limit   string `json:"limit,omitempty"`
	Value   *int   `json:"value,omitempty"`
}

func headOf(s Summary) summaryHead {
	wal := walRec{State: s.WAL.State, Bytes: s.WAL.Bytes, Error: optional(s.WAL.Error)}
	if s.WAL.Sampled {
		wal.Newest = timePtr(s.WAL.Newest)
	}
	if m := s.WAL.Missing; m != nil {
		wal.Missing = &missingRec{}
		if m.Started {
			wal.Missing.Start = timePtr(m.Start)
		}
		if m.Ended {
			wal.Missing.End = timePtr(m.End)
		}
	}
	gaps := make([]gapRec, 0, len(s.Gaps))
	for _, g := range s.Gaps {
		rec := gapRec{Block: g.Block, Error: g.Error}
		if g.Known {
			rec.Start, rec.End = timePtr(g.Start), timePtr(g.End)
		}
		gaps = append(gaps, rec)
	}
	return summaryHead{
		Kind:          "summary",
		Command:       s.Command,
		PromQLVersion: s.PromQLVersion,
		Dump:          dumpRec{Start: timeOf(s.Dump.Start), End: timeOf(s.Dump.End), Root: s.Dump.Root},
		WAL:           wal,
		Gaps:          gaps,
	}
}

// optional is a string that is null when empty.
func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func timePtr(t time.Time) *timeOf {
	out := timeOf(t)
	return &out
}

func tailOf(r *query.Result) resultTail {
	warnings := make([]warning, 0, len(r.Warnings))
	for _, w := range r.Warnings {
		warnings = append(warnings, warning{Code: w.Code, Message: w.Message})
	}
	tail := resultTail{
		Series:    len(r.Series),
		Points:    r.Points(),
		Truncated: truncatedOf(r.Truncated),
		Empty:     emptyOf(r.Empty),
		Warnings:  warnings,
	}
	if r.HasSampleInterval {
		interval := duration(r.SampleInterval)
		tail.SampleInterval = &interval
	}
	return tail
}

func truncatedOf(t *query.Truncation) *truncatedRec {
	if t == nil {
		return nil
	}
	return &truncatedRec{Limit: t.Limit, Value: t.Value, Full: t.Full}
}

func emptyOf(e *query.Empty) *emptyRec {
	if e == nil {
		return nil
	}
	return &emptyRec{Reason: e.Reason, Selectors: e.Selectors}
}

// duration writes a duration as Prometheus does, such as 1m or 1h30m.
func duration(d time.Duration) string { return model.Duration(d).String() }

func metricOf(ls []query.Label) map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

// timeOf is a time written as RFC 3339 in UTC, where the HTTP API writes Unix seconds.
type timeOf time.Time

func (t timeOf) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format(time.RFC3339Nano))
}

// pair is a [time, value] pair, as the HTTP API writes a sample.
type pair struct {
	T timeOf
	V any
}

func (p pair) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]any{p.T, p.V})
}

func samplePair(p query.Point) pair {
	if p.H != nil {
		return pair{timeOf(p.T), histogramOf(p.H)}
	}
	return pair{timeOf(p.T), formatFloat(p.F)}
}

// histogramRec is the HTTP API's histogram object. Each bucket is [boundaries, lower, upper, count], where
// boundaries is 0 for (lower, upper], 1 for [lower, upper), 2 for (lower, upper) and 3 for [lower, upper].
type histogramRec struct {
	Count   string   `json:"count"`
	Sum     string   `json:"sum"`
	Buckets [][4]any `json:"buckets,omitempty"`
}

func histogramOf(h *query.Histogram) histogramRec {
	rec := histogramRec{Count: formatFloat(h.Count), Sum: formatFloat(h.Sum)}
	for _, b := range h.Buckets {
		rec.Buckets = append(rec.Buckets, [4]any{
			boundaries(b.LowerInclusive, b.UpperInclusive),
			formatFloat(b.Lower), formatFloat(b.Upper), formatFloat(b.Count),
		})
	}
	return rec
}

func boundaries(lowerInclusive, upperInclusive bool) int {
	switch {
	case lowerInclusive && upperInclusive:
		return 3
	case lowerInclusive:
		return 1
	case upperInclusive:
		return 0
	}
	return 2
}

// formatFloat writes a sample value as the HTTP API does: a string, so NaN and ±Inf survive JSON, in plain
// notation unless the value is very small or very large.
func formatFloat(f float64) string {
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	return strconv.FormatFloat(f, format, -1, 64)
}
