package named

import (
	"slices"
	"testing"
	"time"
)

func TestShippedLoads(t *testing.T) {
	f, err := Parse(Shipped())
	if err != nil {
		t.Fatalf("Parse(Shipped()) = %v", err)
	}
	want := []string{"pod-cpu", "pod-memory", "node-cpu", "node-iowait", "node-load", "node-memory", "node-network",
		"node-disk-write-latency"}
	if got := f.Names(); !slices.Equal(got, want) {
		t.Errorf("Parse(Shipped()).Names() = %v, want %v", got, want)
	}
}

func TestParseRefuses(t *testing.T) {
	const flags = `"flags": [{"name": "instance", "label": "instance", "match": "exact"}]`
	tests := []struct {
		name, file string
	}{
		{"not JSON", `{`},
		{"unknown field", `{"flags": [], "queries": [], "extra": 1}`},
		{"bad match", `{"flags": [{"name": "instance", "label": "instance", "match": "glob"}], "queries": []}`},
		{"bad label", `{"flags": [{"name": "instance", "label": "", "match": "exact"}], "queries": []}`},
		{"undeclared flag", `{` + flags + `, "queries": [{"name": "q", "flags": ["device"], "expressions": [{"name": "e", "promql": "up"}]}]}`},
		{"no expressions", `{` + flags + `, "queries": [{"name": "q", "flags": [], "expressions": []}]}`},
		{"bad PromQL", `{` + flags + `, "queries": [{"name": "q", "flags": [], "expressions": [{"name": "e", "promql": "up{"}]}]}`},
		{"expression flag not the query's", `{` + flags + `, "queries": [{"name": "q", "flags": [], ` +
			`"expressions": [{"name": "e", "promql": "up", "flags": ["instance"]}]}]}`},
		{"compare unknown expression", `{` + flags + `, "queries": [{"name": "q", "flags": [], ` +
			`"expressions": [{"name": "e", "promql": "up"}], "compare": {"left": "e", "right": "f", "on": ["job"]}}]}`},
		{"query twice", `{` + flags + `, "queries": [{"name": "q", "flags": [], "expressions": [{"name": "e", "promql": "up"}]}, ` +
			`{"name": "q", "flags": [], "expressions": [{"name": "e", "promql": "up"}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.file)); err == nil {
				t.Errorf("Parse(%s) = nil error, want one", tt.file)
			}
		})
	}
}

// An expression that lists its own flags takes only those, and an unset flag adds no matcher.
func TestBuildExpressionFlags(t *testing.T) {
	f, err := Parse([]byte(`{
		"flags": [
			{"name": "instance", "label": "instance", "match": "exact"},
			{"name": "device", "label": "device", "match": "regex"}
		],
		"queries": [{"name": "q", "flags": ["instance", "device"], "expressions": [
			{"name": "cpu", "promql": "rate(cpu[5m])", "flags": ["instance"]},
			{"name": "disk", "promql": "disk"}
		]}]
	}`))
	if err != nil {
		t.Fatalf("Parse = %v", err)
	}
	q, _ := f.Query("q")
	built, err := f.Build(q, map[string]string{"instance": "a", "device": "sd.*"}, time.Minute)
	if err != nil {
		t.Fatalf("Build = %v", err)
	}
	// The printer sorts matchers.
	want := []Built{{"cpu", `rate(cpu{instance="a"}[1m])`}, {"disk", `disk{device=~"sd.*",instance="a"}`}}
	if !slices.Equal(built, want) {
		t.Errorf("Build = %q, want %q", built, want)
	}

	built, err = f.Build(q, map[string]string{}, time.Minute)
	if err != nil {
		t.Fatalf("Build = %v", err)
	}
	if built[1].PromQL != "disk" {
		t.Errorf("Build with no values: disk = %q, want %q", built[1].PromQL, "disk")
	}
}
