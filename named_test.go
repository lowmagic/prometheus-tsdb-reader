package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/lowmagic/prometheus-tsdb-reader/internal/fixture"
	"github.com/lowmagic/prometheus-tsdb-reader/named"
)

// writeNamed writes the fixture carrying the named queries' series.
func writeNamed(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := fixture.Named(t.Context(), dir); err != nil {
		t.Fatalf("fixture.Named(%s) = %v", dir, err)
	}
	return dir
}

// normalized parses expr and prints it with each selector's matchers sorted, so two expressions that differ only
// in matcher order and spacing print the same.
func normalized(t *testing.T, expr string) string {
	t.Helper()
	e, err := parser.NewParser(parser.Options{}).ParseExpr(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	parser.Inspect(e, func(node parser.Node, _ []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok {
			slices.SortFunc(vs.LabelMatchers, func(a, b *labels.Matcher) int {
				return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Type, b.Type), cmp.Compare(a.Value, b.Value))
			})
		}
		return nil
	})
	return e.String()
}

// Each shipped query runs, and runs the spec's expressions with the flags' matchers added and --window as every
// range.
func TestNamedExpressions(t *testing.T) {
	dumpDir := writeNamed(t)
	const ns, pods, node = `namespace="shop", pod=~"checkout-.*"`, "--pod", "node-1.example.com"
	const inst = `instance="node-1.example.com"`
	const vm = `namespace="vms", mount_point=~"/data"`
	podFlags := []string{"--namespace", "shop", pods, "checkout-.*"}
	tests := []struct {
		name  string
		flags []string
		want  map[string]string
	}{
		{"pod-cpu", podFlags, map[string]string{
			"used":  `sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{container!="", container!="POD", ` + ns + `}[2m]))`,
			"limit": `sum by (namespace, pod) (kube_pod_container_resource_limits{resource="cpu", ` + ns + `})`,
		}},
		{"pod-memory", podFlags, map[string]string{
			"used":  `sum by (namespace, pod) (container_memory_working_set_bytes{container!="", container!="POD", ` + ns + `})`,
			"limit": `sum by (namespace, pod) (kube_pod_container_resource_limits{resource="memory", ` + ns + `})`,
		}},
		{"node-cpu", []string{"--instance", node}, map[string]string{
			"busy": `1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle", ` + inst + `}[2m]))`,
		}},
		{"node-iowait", []string{"--instance", node}, map[string]string{
			"iowait": `avg by (instance) (rate(node_cpu_seconds_total{mode="iowait", ` + inst + `}[2m]))`,
		}},
		{"node-load", []string{"--instance", node}, map[string]string{
			"load1":         `node_load1{` + inst + `}`,
			"load5":         `node_load5{` + inst + `}`,
			"load15":        `node_load15{` + inst + `}`,
			"load1_per_cpu": `instance:node_load1_per_cpu:ratio{` + inst + `}`,
		}},
		{"node-memory", []string{"--instance", node}, map[string]string{
			"used":      `1 - node_memory_MemAvailable_bytes{` + inst + `} / node_memory_MemTotal_bytes{` + inst + `}`,
			"available": `node_memory_MemAvailable_bytes{` + inst + `}`,
		}},
		{"node-network", []string{"--instance", node, "--device", "eth.*"}, map[string]string{
			"receive":  `sum by (instance, device) (rate(node_network_receive_bytes_total{` + inst + `, device=~"eth.*"}[2m]))`,
			"transmit": `sum by (instance, device) (rate(node_network_transmit_bytes_total{` + inst + `, device=~"eth.*"}[2m]))`,
		}},
		{"node-rules", []string{"--instance", node, "--device", "sd.*"}, map[string]string{
			"cpu_utilisation":        `instance:node_cpu_utilisation:rate1m{` + inst + `}`,
			"memory_utilisation":     `instance:node_memory_utilisation:ratio{` + inst + `}`,
			"network_receive_bytes":  `instance:node_network_receive_bytes_excluding_lo:rate1m{` + inst + `}`,
			"network_transmit_bytes": `instance:node_network_transmit_bytes_excluding_lo:rate1m{` + inst + `}`,
			"network_receive_drop":   `instance:node_network_receive_drop_excluding_lo:rate1m{` + inst + `}`,
			"network_transmit_drop":  `instance:node_network_transmit_drop_excluding_lo:rate1m{` + inst + `}`,
			"disk_io_time":           `instance_device:node_disk_io_time_seconds:rate1m{` + inst + `, device=~"sd.*"}`,
			"disk_io_time_weighted":  `instance_device:node_disk_io_time_weighted_seconds:rate1m{` + inst + `, device=~"sd.*"}`,
		}},
		{"vm-filesystem", []string{"--namespace", "vms", "--mount", "/data"}, map[string]string{
			"used": `sum by (namespace, name, mount_point) (kubevirt_vmi_filesystem_used_bytes{` + vm + `}) / ` +
				`sum by (namespace, name, mount_point) (kubevirt_vmi_filesystem_capacity_bytes{` + vm + `})`,
			"free": `1 - sum by (namespace, name, mount_point) (kubevirt_vmi_filesystem_used_bytes{` + vm + `}) / ` +
				`sum by (namespace, name, mount_point) (kubevirt_vmi_filesystem_capacity_bytes{` + vm + `})`,
		}},
		{"node-disk-write-latency", []string{"--instance", node, "--device", "sda"}, map[string]string{
			"write_latency": `rate(node_disk_write_time_seconds_total{` + inst + `, device=~"sda"}[2m]) / ` +
				`rate(node_disk_writes_completed_total{` + inst + `, device=~"sda"}[2m])`,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", dumpDir, "--scratch", t.TempDir(), "named", tt.name, "--window", "2m"}, tt.flags...)
			records := mustRun(t, args...)
			summary := records[0]
			got := map[string]string{}
			exprs, _ := summary["expressions"].([]any)
			for _, e := range exprs {
				name, _ := field(e, "name").(string)
				promql, _ := field(e, "promql").(string)
				got[name] = normalized(t, promql)
				if empty := field(e, "empty"); empty != nil {
					t.Errorf("run(%q): expression %s is empty: %v", args, name, empty)
				}
			}
			want := map[string]string{}
			for name, promql := range tt.want {
				want[name] = normalized(t, promql)
			}
			if diff := gocmp.Diff(want, got); diff != "" {
				t.Errorf("run(%q) expressions (-want +got):\n%s", args, diff)
			}
			if summary["command"] != "named "+tt.name {
				t.Errorf("run(%q): command = %v, want %q", args, summary["command"], "named "+tt.name)
			}
			for _, r := range records[1:] {
				expression, _ := r["expression"].(string)
				if _, ok := want[expression]; !ok || r["kind"] != "series" {
					t.Errorf("run(%q): record %v doesn't name one of the expressions", args, r)
				}
			}
		})
	}
}

// A value with a quote and a backslash becomes a matcher that matches it, on every expression.
func TestNamedEscaping(t *testing.T) {
	dumpDir := writeNamed(t)
	args := []string{"--dump", dumpDir, "--scratch", t.TempDir(), "named", "pod-memory", "--namespace", fixture.OddNamespace}
	records := mustRun(t, args...)
	var got [][2]any
	for _, r := range records[1:] {
		got = append(got, [2]any{r["expression"], field(r, "metric", "namespace")})
	}
	want := [][2]any{{"used", fixture.OddNamespace}, {"limit", fixture.OddNamespace}}
	if diff := gocmp.Diff(want, got); diff != "" {
		t.Errorf("run(%q) records' expression and namespace (-want +got):\n%s", args, diff)
	}
}

// A pod with usage and no limit is named in the summary's unpaired, and a query that compares nothing has none.
func TestNamedUnpaired(t *testing.T) {
	dumpDir := writeNamed(t)
	tests := []struct {
		name string
		args []string
		want any
	}{
		{"pod with no limit", []string{"named", "pod-cpu", "--namespace", "shop"},
			[]any{map[string]any{"namespace": "shop", "pod": fixture.NoLimitPod}}},
		{"every pod limited", []string{"named", "pod-cpu", "--namespace", "shop", "--pod", "checkout-1"}, []any{}},
		{"no compare", []string{"named", "node-cpu"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", dumpDir, "--scratch", t.TempDir()}, tt.args...)
			summary := mustRun(t, args...)[0]
			if diff := gocmp.Diff(tt.want, summary["unpaired"]); diff != "" {
				t.Errorf("run(%q): unpaired (-want +got):\n%s", args, diff)
			}
		})
	}
}

// The limits are shared among a named query's expressions, so a cut never drops one of them, and the pairing is
// made before the cut.
func TestNamedShare(t *testing.T) {
	dumpDir := writeNamed(t)
	args := []string{"--dump", dumpDir, "--scratch", t.TempDir(), "--max-series", "2", "named", "pod-cpu"}
	records := mustRun(t, args...)
	// 4 pods use CPU and 3 have a limit; each expression keeps 1.
	want := map[string]any{"limit": "series", "value": 2.0, "full": 7.0}
	if diff := gocmp.Diff(want, records[0]["truncated"]); diff != "" {
		t.Errorf("run(%q): truncated (-want +got):\n%s", args, diff)
	}
	var kept []any
	for _, r := range records[1:] {
		kept = append(kept, r["expression"])
	}
	if diff := gocmp.Diff([]any{"used", "limit"}, kept); diff != "" {
		t.Errorf("run(%q): records' expressions (-want +got):\n%s", args, diff)
	}
	if unpaired, _ := records[0]["unpaired"].([]any); len(unpaired) != 1 {
		t.Errorf("run(%q): unpaired = %v, want only %s", args, records[0]["unpaired"], fixture.NoLimitPod)
	}
}

// --queries reads an edited copy of the shipped file in its place.
func TestNamedQueriesFile(t *testing.T) {
	dumpDir := writeNamed(t)
	var file named.File
	if err := json.Unmarshal(named.Shipped(), &file); err != nil {
		t.Fatalf("json.Unmarshal(the shipped file) = %v", err)
	}
	file.Queries = append(file.Queries, named.Query{
		Name:        "node-load15",
		Flags:       []string{"instance"},
		Expressions: []named.Expression{{Name: "load15", PromQL: "node_load15"}},
	})
	content, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("json.Marshal(the edited file) = %v", err)
	}
	path := filepath.Join(t.TempDir(), "queries.json")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	args := []string{"--dump", dumpDir, "--scratch", t.TempDir(), "--queries", path,
		"named", "node-load15", "--instance", "node-2.example.com"}
	records := mustRun(t, args...)
	if len(records) != 2 || field(records[1], "metric", "instance") != "node-2.example.com" {
		t.Errorf("run(%q) = %v, want node-2.example.com's node_load15", args, records)
	}
}

func TestNamedBadArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no name", []string{"named"}},
		{"unknown query", []string{"named", "nonsense"}},
		{"promql", []string{"named", "node-cpu", "--promql", "up"}},
		{"flag the query doesn't take", []string{"named", "pod-cpu", "--instance", "node-1.example.com"}},
		{"bad regex", []string{"named", "pod-cpu", "--pod", "("}},
		{"zero window", []string{"named", "node-cpu", "--window", "0s"}},
		{"positional", []string{"named", "node-cpu", "node-1.example.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append([]string{"--dump", ".", "--scratch", t.TempDir()}, tt.args...)
			err := run(t.Context(), args, &stdout, &stderr)
			if code := exitCode(err); code != exitBadArgs {
				t.Errorf("run(%q) = %v, exit code %d; want %d", args, err, code, exitBadArgs)
			}
			if stdout.Len() != 0 {
				t.Errorf("run(%q) wrote %q to stdout, want nothing", args, &stdout)
			}
		})
	}
}

// query metadata lists what the WAL's metadata records carry, and tells a dump with none from a metric with none.
func TestQueryMetadata(t *testing.T) {
	withMetadata := t.TempDir()
	if err := fixture.Metadata(t.Context(), withMetadata); err != nil {
		t.Fatalf("fixture.Metadata(%s) = %v", withMetadata, err)
	}
	without := writeFixture(t, 15*time.Second)
	every := []map[string]any{}
	for _, m := range fixture.MetadataMetrics {
		every = append(every, map[string]any{"kind": "metadata", "metric": m.Name, "type": m.Type, "unit": m.Unit, "help": m.Help})
	}
	tests := []struct {
		name    string
		dump    string
		args    []string
		empty   any
		records []map[string]any
	}{
		{"every metric", withMetadata, nil, nil, every},
		{"one metric", withMetadata, []string{"--metric", "node_load1"}, nil, every[:1]},
		{"a metric with none", withMetadata, []string{"--metric", "up"},
			map[string]any{"reason": "no-metadata-for-metric", "selectors": []any{"up"}}, nil},
		{"a dump with none", without, nil, map[string]any{"reason": "no-metadata", "selectors": []any{}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", tt.dump, "--scratch", t.TempDir(), "query", "metadata"}, tt.args...)
			records := mustRun(t, args...)
			if diff := gocmp.Diff(tt.empty, records[0]["empty"]); diff != "" {
				t.Errorf("run(%q): empty (-want +got):\n%s", args, diff)
			}
			if diff := gocmp.Diff(tt.records, records[1:], cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("run(%q) records (-want +got):\n%s", args, diff)
			}
		})
	}
}
