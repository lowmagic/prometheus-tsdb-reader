package render

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/junglestyle/prometheus-tsdb-reader/query"
)

func TestSampleValues(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 500_000_000, time.UTC)
	h := &query.Histogram{Count: 3, Sum: 4.5, Buckets: []query.Bucket{
		{Lower: 1, Upper: 2, UpperInclusive: true, Count: 2},
		{Lower: -0.001, Upper: 0.001, LowerInclusive: true, UpperInclusive: true, Count: 1},
	}}
	r := &query.Result{Type: query.Vector}
	for _, p := range []query.Point{
		{T: at, F: math.NaN()}, {T: at, F: math.Inf(1)}, {T: at, F: math.Inf(-1)},
		{T: at, F: 1e-7}, {T: at, F: 0.25}, {T: at, F: 1e21}, {T: at, H: h},
	} {
		r.Series = append(r.Series, query.Series{Points: []query.Point{p}})
	}

	var out bytes.Buffer
	if err := Instant(&out, Summary{}, r); err != nil {
		t.Fatalf("Instant() = %v, want nil", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")[1:]
	want := []string{
		`{"kind":"series","metric":{},"value":["2026-01-01T00:00:00.5Z","NaN"]}`,
		`{"kind":"series","metric":{},"value":["2026-01-01T00:00:00.5Z","+Inf"]}`,
		`{"kind":"series","metric":{},"value":["2026-01-01T00:00:00.5Z","-Inf"]}`,
		`{"kind":"series","metric":{},"value":["2026-01-01T00:00:00.5Z","1e-07"]}`,
		`{"kind":"series","metric":{},"value":["2026-01-01T00:00:00.5Z","0.25"]}`,
		`{"kind":"series","metric":{},"value":["2026-01-01T00:00:00.5Z","1e+21"]}`,
		`{"kind":"series","metric":{},"value":["2026-01-01T00:00:00.5Z",` +
			`{"count":"3","sum":"4.5","buckets":[[0,"1","2","2"],[3,"-0.001","0.001","1"]]}]}`,
	}
	if diff := cmp.Diff(want, lines); diff != "" {
		t.Errorf("Instant() records (-want +got):\n%s", diff)
	}
}
