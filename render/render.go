// Package render writes results as JSON lines: a summary record first, then one record per series.
package render

import (
	"encoding/json"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/junglestyle/prometheus-tsdb-reader/query"
)

// Summary is what qualifies a result: what ran, over what dump, with which engine.
type Summary struct {
	Command       string
	PromQLVersion string
	Dump          Dump
	WAL           WAL
	Expression    string
	Time          time.Time
}

// Dump is the dump's range and the TSDB root the run used.
type Dump struct {
	Start, End time.Time
	Root       string
}

// WAL is what the run did with the WAL.
type WAL struct {
	State string
	Bytes int64
}

// Instant writes an instant query's summary, then its records.
func Instant(w io.Writer, s Summary, r *query.Result) error {
	enc := newEncoder(w)
	if err := enc.Encode(summaryOf(s, r)); err != nil {
		return err
	}
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

func newEncoder(w io.Writer) *json.Encoder {
	enc := json.NewEncoder(w)
	// PromQL is full of < and >, and a reader should see them as they are.
	enc.SetEscapeHTML(false)
	return enc
}

type summaryRecord struct {
	Kind          string    `json:"kind"`
	Command       string    `json:"command"`
	PromQLVersion string    `json:"promql_version"`
	Dump          dumpRec   `json:"dump"`
	WAL           walRec    `json:"wal"`
	Expression    string    `json:"expression"`
	Time          timeOf    `json:"time"`
	Series        int       `json:"series"`
	Points        int       `json:"points"`
	Warnings      []warning `json:"warnings"`
}

type dumpRec struct {
	Start timeOf `json:"start"`
	End   timeOf `json:"end"`
	Root  string `json:"root"`
}

type walRec struct {
	State string `json:"state"`
	Bytes int64  `json:"bytes"`
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
}

func summaryOf(s Summary, r *query.Result) summaryRecord {
	warnings := make([]warning, 0, len(r.Annotations))
	for _, a := range r.Annotations {
		warnings = append(warnings, warning{Code: "promql", Message: a})
	}
	return summaryRecord{
		Kind:          "summary",
		Command:       s.Command,
		PromQLVersion: s.PromQLVersion,
		Dump:          dumpRec{Start: timeOf(s.Dump.Start), End: timeOf(s.Dump.End), Root: s.Dump.Root},
		WAL:           walRec(s.WAL),
		Expression:    s.Expression,
		Time:          timeOf(s.Time),
		Series:        len(r.Series),
		Points:        r.Points(),
		Warnings:      warnings,
	}
}

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
