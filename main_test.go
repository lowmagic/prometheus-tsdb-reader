package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/common/model"

	"github.com/junglestyle/prometheus-tsdb-reader/dump"
	"github.com/junglestyle/prometheus-tsdb-reader/internal/fixture"
)

func TestQueryInstant(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	before := snapshot(t, dumpDir)
	scratch := t.TempDir()

	var stdout, stderr bytes.Buffer
	args := []string{"--dump", dumpDir, "--scratch", scratch, "query", "instant", "--promql", "up"}
	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run(%q) = %v, want nil; stderr:\n%s", args, err, &stderr)
	}

	if diff := cmp.Diff(before, snapshot(t, dumpDir)); diff != "" {
		t.Errorf("run(%q) changed the dump (-before +after):\n%s", args, diff)
	}
	if left, _ := os.ReadDir(scratch); len(left) != 0 {
		t.Errorf("run(%q) left %v under scratch, want nothing", args, left)
	}

	records := decodeLines(t, stdout.Bytes())
	if len(records) == 0 {
		t.Fatalf("run(%q) wrote nothing to stdout", args)
	}
	summary := records[0]
	version, _ := summary["promql_version"].(string)
	if !regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(version) {
		t.Errorf("run(%q) promql_version = %q, want a release such as 3.15.1", args, version)
	}
	delete(summary, "promql_version")
	wal, _ := summary["wal"].(map[string]any)
	if size, _ := wal["bytes"].(float64); size <= 0 {
		t.Errorf("run(%q) wal.bytes = %v, want the WAL's size", args, wal["bytes"])
	}
	delete(wal, "bytes")

	end := fixture.End().Format(time.RFC3339)
	want := []map[string]any{
		{
			"kind":    "summary",
			"command": "query instant",
			"dump": map[string]any{
				"start": fixture.Start().Format(time.RFC3339),
				"end":   end,
				"root":  ".",
			},
			"wal":             map[string]any{"state": "replayed", "newest": end},
			"expression":      "up",
			"time":            end,
			"series":          2.0,
			"points":          2.0,
			"empty":           nil,
			"sample_interval": "15s",
			"warnings":        []any{},
		},
		{
			"kind":   "series",
			"metric": map[string]any{"__name__": "up", "instance": "node-a", "job": "node-exporter"},
			"value":  []any{end, "1"},
		},
		{
			"kind":   "series",
			"metric": map[string]any{"__name__": "up", "instance": "node-b", "job": "node-exporter"},
			"value":  []any{end, "0"},
		},
	}
	if diff := cmp.Diff(want, records); diff != "" {
		t.Errorf("run(%q) stdout (-want +got):\n%s", args, diff)
	}
}

func TestScratchInsideDumpIsRefused(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	before := snapshot(t, dumpDir)

	var stdout, stderr bytes.Buffer
	args := []string{"--dump", dumpDir, "--scratch", dumpDir, "query", "instant", "--promql", "up"}
	err := run(t.Context(), args, &stdout, &stderr)
	var usage *usageError
	if err == nil || errors.As(err, &usage) {
		t.Fatalf("run(%q) = %v, want a failed run", args, err)
	}
	if diff := cmp.Diff(before, snapshot(t, dumpDir)); diff != "" {
		t.Errorf("run(%q) changed the dump (-before +after):\n%s", args, diff)
	}
	records := decodeLines(t, stdout.Bytes())
	if len(records) != 1 || records[0]["kind"] != "error" || records[0]["exit"] != 1.0 {
		t.Errorf("run(%q) stdout = %v, want one error record with exit 1", args, records)
	}
}

func TestBadArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no dump", []string{"query", "instant", "--promql", "up"}},
		{"no expression", []string{"--dump", ".", "query", "instant"}},
		{"positional expression", []string{"--dump", ".", "query", "instant", "up"}},
		{"bad time", []string{"--dump", ".", "query", "instant", "--promql", "up", "--time", "yesterday"}},
		{"unknown command", []string{"--dump", ".", "query", "nonsense"}},
		// --promql has one spelling, so a caller that denies that argument denies free-form PromQL.
		{"single-dash promql", []string{"--dump", ".", "query", "instant", "-promql", "up"}},
		{"promql with =", []string{"--dump", ".", "query", "instant", "--promql=up"}},
		{"single-dash promql with =", []string{"--dump", ".", "query", "instant", "-promql=up"}},
		{"series with =", []string{"--dump", ".", "query", "series", "--promql", "up", "--promql=up"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(t.Context(), tt.args, &stdout, &stderr)
			var usage *usageError
			if !errors.As(err, &usage) {
				t.Errorf("run(%q) = %v, want a usage error", tt.args, err)
			}
			if stdout.Len() != 0 {
				t.Errorf("run(%q) wrote %q to stdout, want nothing", tt.args, &stdout)
			}
		})
	}
}

// writeFixture writes the synthetic dump and checks it has a chunks_head/ file, without which a run would
// pass on a dump whose copy it never made.
func writeFixture(t *testing.T, interval time.Duration) string {
	t.Helper()
	dir := t.TempDir()
	if err := fixture.Write(t.Context(), dir, interval); err != nil {
		t.Fatalf("fixture.Write(%s) = %v", dir, err)
	}
	files, err := os.ReadDir(filepath.Join(dir, "chunks_head"))
	if err != nil || len(files) == 0 {
		t.Fatalf("fixture has no chunks_head/ files: %v, %v", files, err)
	}
	return dir
}

// snapshot records every path under dir with its mode, modification time and, for a file, its content hash.
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	paths := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		state := info.Mode().String() + " " + info.ModTime().Format(time.RFC3339Nano)
		if e.Type().IsRegular() {
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			state += fmt.Sprintf(" %x", sha256.Sum256(content))
		}
		paths[strings.TrimPrefix(path, dir)] = state
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot %s: %v", dir, err)
	}
	return paths
}

func decodeLines(t *testing.T, out []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	for line := range bytes.Lines(out) {
		var r map[string]any
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("stdout line %q isn't JSON: %v", line, err)
		}
		records = append(records, r)
	}
	return records
}

func TestInfoLayouts(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	tests := []struct {
		name, prefix, root string
		compress           bool
	}{
		{"top level", "", ".", true},
		{"under data", "data/", "data", true},
		{"under prometheus", "prometheus/", "prometheus", true},
		{"deeper", "capture/prometheus/", "capture/prometheus", true},
		{"uncompressed", "prometheus/", "prometheus", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := t.TempDir()
			archive := writeArchive(t, parent, "dump.tar.gz", func(w io.Writer) error {
				return fixture.Archive(w, dumpDir, tt.prefix, tt.compress)
			})
			scratch := mkdir(t, parent, "scratch")

			records := mustRun(t, "--dump", archive, "--scratch", scratch, "info")
			wantDump := map[string]any{
				"start": fixture.Start().Format(time.RFC3339),
				"end":   fixture.End().Format(time.RFC3339),
				"root":  tt.root,
			}
			if diff := cmp.Diff(wantDump, records[0]["dump"]); diff != "" {
				t.Errorf("info on %s: dump (-want +got):\n%s", tt.name, diff)
			}
			wal, _ := records[0]["wal"].(map[string]any)
			if wal["state"] != "read" || wal["newest"] != fixture.End().Format(time.RFC3339) {
				t.Errorf("info on %s: wal = %v, want state read and newest %s", tt.name, wal, fixture.End())
			}
			var blocks int
			for _, r := range records[1:] {
				if r["kind"] == "block" && r["status"] == "ok" {
					blocks++
				}
			}
			if blocks != 2 {
				t.Errorf("info on %s: %d ok block records, want 2: %v", tt.name, blocks, records[1:])
			}
			if names := dirNames(t, scratch); !slices.Equal(names, []string{"dump"}) {
				t.Errorf("info on %s left %v under scratch, want only the extraction dump", tt.name, names)
			}
		})
	}
}

func TestExtractionIsReused(t *testing.T) {
	parent := t.TempDir()
	dumpDir := writeFixture(t, 15*time.Second)
	archive := writeArchive(t, parent, "dump.tar.gz", func(w io.Writer) error {
		return fixture.Archive(w, dumpDir, "prometheus/", true)
	})
	scratch := mkdir(t, parent, "scratch")
	mustRun(t, "--dump", archive, "--scratch", scratch, "info")

	extraction := filepath.Join(scratch, "dump")
	before := snapshot(t, extraction)
	records := mustRun(t, "--dump", extraction, "--scratch", scratch, "query", "instant", "--promql", "up")
	if records[0]["series"] != 2.0 {
		t.Errorf("query on the extraction: series = %v, want 2", records[0]["series"])
	}
	if diff := cmp.Diff(before, snapshot(t, extraction)); diff != "" {
		t.Errorf("query on the extraction changed it (-before +after):\n%s", diff)
	}

	// The archive again would extract over the first extraction, so the run is refused and says where it is.
	var stdout, stderr bytes.Buffer
	args := []string{"--dump", archive, "--scratch", scratch, "info"}
	if err := run(t.Context(), args, &stdout, &stderr); err == nil {
		t.Errorf("run(%q) with the extraction in place = nil, want an error", args)
	}
}

func TestRefusedArchives(t *testing.T) {
	type entry struct {
		hdr     tar.Header
		content string
	}
	file := func(name, content string) entry {
		return entry{tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))}, content}
	}
	link := func(typ byte, name, target string) entry {
		return entry{tar.Header{Name: name, Typeflag: typ, Linkname: target, Mode: 0o777}, ""}
	}
	tests := []struct {
		name    string
		entries []entry
		// entry is the entry the refusal names, or "" when the run fails as os.Root stops a write.
		entry string
		args  []string
	}{
		{name: "parent path", entries: []entry{file("../escaped", "x")}, entry: "../escaped"},
		{name: "nested parent path", entries: []entry{file("a/../../escaped", "x")}, entry: "a/../../escaped"},
		{name: "absolute path", entries: []entry{file("/escaped", "x")}, entry: "/escaped"},
		{name: "symlink out", entries: []entry{link(tar.TypeSymlink, "wal", "../escaped")}, entry: "wal"},
		{name: "absolute symlink", entries: []entry{link(tar.TypeSymlink, "wal", "/")}, entry: "wal"},
		{
			name: "symlink out through a symlink",
			entries: []entry{
				link(tar.TypeSymlink, "b", "."), link(tar.TypeSymlink, "a", "b/.."),
			},
			entry: "a",
		},
		{
			name: "write through a symlink out",
			entries: []entry{
				link(tar.TypeSymlink, "b", "."), link(tar.TypeSymlink, "a", "b/.."), file("a/escaped", "x"),
			},
		},
		{name: "hard link out", entries: []entry{link(tar.TypeLink, "wal", "../escaped")}, entry: "wal"},
		{
			// d/s leads to x inside the extraction, but the same target text from wal/ leads outside it.
			name: "hard link to a symlink",
			entries: []entry{
				link(tar.TypeSymlink, "d/s", "../x"), link(tar.TypeLink, "wal/00000002", "d/s"),
			},
			entry: "wal/00000002",
		},
		{
			name:    "device",
			entries: []entry{{tar.Header{Name: "dev", Typeflag: tar.TypeChar, Mode: 0o600, Devmajor: 1, Devminor: 3}, ""}},
			entry:   "dev",
		},
		{
			name:    "oversize",
			entries: []entry{file("small", "x"), file("big", strings.Repeat("x", 100))},
			entry:   "big",
			args:    []string{"--max-extract-bytes", "50"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := t.TempDir()
			archive := writeArchive(t, parent, "dump.tar.gz", func(w io.Writer) (err error) {
				gz := gzip.NewWriter(w)
				tw := tar.NewWriter(gz)
				for _, e := range tt.entries {
					if err := tw.WriteHeader(&e.hdr); err != nil {
						return err
					}
					if _, err := tw.Write([]byte(e.content)); err != nil {
						return err
					}
				}
				return errors.Join(tw.Close(), gz.Close())
			})
			scratch := mkdir(t, parent, "scratch")

			var stdout, stderr bytes.Buffer
			args := append([]string{"--dump", archive, "--scratch", scratch}, tt.args...)
			args = append(args, "info")
			err := run(t.Context(), args, &stdout, &stderr)
			var refused *dump.RefusedError
			switch {
			case err == nil:
				t.Fatalf("run(%q) = nil, want a refusal", args)
			case tt.entry != "" && (!errors.As(err, &refused) || refused.Entry != tt.entry):
				t.Errorf("run(%q) = %v, want a refusal naming entry %q", args, err, tt.entry)
			}
			records := decodeLines(t, stdout.Bytes())
			if len(records) != 1 || records[0]["kind"] != "error" || records[0]["exit"] != 1.0 {
				t.Errorf("run(%q) stdout = %v, want one error record with exit 1", args, records)
			}
			if names := dirNames(t, parent); !slices.Equal(names, []string{"dump.tar.gz", "scratch"}) {
				t.Errorf("run(%q) left %v beside the archive, want only the archive and scratch", args, names)
			}
			if names := dirNames(t, scratch); len(names) != 0 {
				t.Errorf("run(%q) left %v under scratch, want nothing", args, names)
			}
		})
	}
}

// A dangling link that leads out of the extraction through another link would reach a file created beside the
// extraction later, so it is removed.
func TestDanglingLinkIsRemoved(t *testing.T) {
	parent := t.TempDir()
	archive := writeArchive(t, parent, "dump.tar.gz", func(w io.Writer) (err error) {
		gz := gzip.NewWriter(w)
		tw := tar.NewWriter(gz)
		for _, hdr := range []tar.Header{
			{Name: "b", Typeflag: tar.TypeSymlink, Linkname: ".", Mode: 0o777},
			{Name: "a", Typeflag: tar.TypeSymlink, Linkname: "b/../later", Mode: 0o777},
			{Name: "wal/", Typeflag: tar.TypeDir, Mode: 0o755},
		} {
			if err := tw.WriteHeader(&hdr); err != nil {
				return err
			}
		}
		return errors.Join(tw.Close(), gz.Close())
	})
	scratch := mkdir(t, parent, "scratch")

	var stdout, stderr bytes.Buffer
	args := []string{"--dump", archive, "--scratch", scratch, "info"}
	err := run(t.Context(), args, &stdout, &stderr)
	if refused := (*dump.RefusedError)(nil); errors.As(err, &refused) {
		t.Fatalf("run(%q) = %v, want the archive extracted", args, err)
	}
	if _, err := os.Lstat(filepath.Join(scratch, "dump", "a")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("run(%q) kept the dangling link a: Lstat = %v, want it gone", args, err)
	}
	if _, err := os.Lstat(filepath.Join(scratch, "dump", "b")); err != nil {
		t.Errorf("run(%q) removed the link b, which resolves: %v", args, err)
	}
}

func TestRootChoice(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := fixture.Write(t.Context(), mkdir(t, dir, name), 15*time.Second); err != nil {
			t.Fatalf("fixture.Write(%s) = %v", name, err)
		}
	}

	var stdout, stderr bytes.Buffer
	args := []string{"--dump", dir, "--scratch", t.TempDir(), "info"}
	err := run(t.Context(), args, &stdout, &stderr)
	var usage *usageError
	if err == nil || errors.As(err, &usage) {
		t.Errorf("run(%q) with two roots = %v, want a failed run", args, err)
	}

	records := mustRun(t, "--dump", dir, "--scratch", t.TempDir(), "--root", "b", "info")
	dumpRec, _ := records[0]["dump"].(map[string]any)
	if root := dumpRec["root"]; root != "b" {
		t.Errorf("info --root b: root = %v, want b", root)
	}
}

// mustRun runs the tool and returns its stdout records, failing the test if the run fails.
func mustRun(t *testing.T, args ...string) []map[string]any {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run(%q) = %v, want nil; stderr:\n%s", args, err, &stderr)
	}
	records := decodeLines(t, stdout.Bytes())
	if len(records) == 0 || records[0]["kind"] != "summary" {
		t.Fatalf("run(%q) stdout = %v, want a summary first", args, records)
	}
	return records
}

func writeArchive(t *testing.T, dir, name string, write func(io.Writer) error) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if err := errors.Join(write(f), f.Close()); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func mkdir(t *testing.T, parent, name string) string {
	t.Helper()
	dir := filepath.Join(parent, name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	return dir
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestRangeSteps(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		args     []string
		step     string
		warning  string // the step warning's code, or "" for none
	}{
		{"1m data, default step", time.Minute, nil, "1m", ""},
		{"1m data, coarse step", time.Minute, []string{"--step", "15m"}, "15m", "step-coarse"},
		{"1m data, fine step", time.Minute, []string{"--step", "10s"}, "10s", "step-finer-than-data"},
		{"15s data, default step", 15 * time.Second, nil, "1m", "step-coarse"},
	}
	dumps := map[time.Duration]string{
		time.Minute:      writeFixture(t, time.Minute),
		15 * time.Second: writeFixture(t, 15*time.Second),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", dumps[tt.interval], "--scratch", t.TempDir(),
				"query", "range", "--promql", "up"}, tt.args...)
			summary := mustRun(t, args...)[0]

			wantEnd := fixture.End().Format(time.RFC3339)
			if summary["end"] != wantEnd || summary["step"] != tt.step {
				t.Errorf("run(%q): end %v, step %v; want end %s, step %s", args, summary["end"], summary["step"], wantEnd, tt.step)
			}
			if want := model.Duration(tt.interval).String(); summary["sample_interval"] != want {
				t.Errorf("run(%q): sample_interval = %v, want %s", args, summary["sample_interval"], want)
			}
			codes := []string{}
			warnings, _ := summary["warnings"].([]any)
			for _, w := range warnings {
				if code, _ := w.(map[string]any)["code"].(string); strings.HasPrefix(code, "step-") {
					codes = append(codes, code)
				}
			}
			want := []string{}
			if tt.warning != "" {
				want = []string{tt.warning}
			}
			if !slices.Equal(codes, want) {
				t.Errorf("run(%q): step warnings %v, want %v", args, codes, want)
			}
		})
	}
}

func TestEmptyReasons(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	dayAfter := fixture.End().Add(24 * time.Hour).Format(time.RFC3339)
	tests := []struct {
		name      string
		args      []string
		reason    string
		selectors []any
	}{
		{"after the dump", []string{"query", "range", "--promql", "up", "--end", dayAfter}, "outside-dump-range", []any{}},
		{"missing metric", []string{"query", "range", "--promql", "node_load1"}, "no-matching-series", []any{"node_load1"}},
		{
			"stopped series", []string{"query", "range", "--promql", `up{instance="node-c"}`},
			"no-samples-in-range", []any{`up{instance="node-c"}`},
		},
		{"comparison", []string{"query", "range", "--promql", "up > 5"}, "empty-evaluation", []any{}},
		{"instant after the dump", []string{"query", "instant", "--promql", "up", "--time", dayAfter}, "outside-dump-range", []any{}},
		{"series of a missing metric", []string{"query", "series", "--promql", "node_load1"}, "no-matching-series", []any{"node_load1"}},
		{
			"stopped series in a subquery", []string{"query", "instant", "--promql", `max_over_time(up{instance="node-c"}[10m:1m])`},
			"no-samples-in-range", []any{`up{instance="node-c"}`},
		},
		{
			"offset side", []string{"query", "range", "--promql", "up - up offset 1d"},
			"no-samples-in-range", []any{"up offset 1d"},
		},
		{"offset", []string{"query", "instant", "--promql", "up offset 1d"}, "no-samples-in-range", []any{"up offset 1d"}},
		{"departed target", []string{"query", "instant", "--promql", fixture.Departed}, "no-samples-in-range", []any{fixture.Departed}},
		{"missing label", []string{"query", "labels", "nonexistent"}, "no-matching-series", []any{`{nonexistent!=""}`}},
		{
			"label the selector lacks", []string{"query", "labels", "mode", "--promql", "up"},
			"no-matching-series", []any{`up{mode!=""}`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", dumpDir, "--scratch", t.TempDir()}, tt.args...)
			records := mustRun(t, args...)
			want := map[string]any{"reason": tt.reason, "selectors": tt.selectors}
			if diff := cmp.Diff(want, records[0]["empty"]); diff != "" {
				t.Errorf("run(%q): empty (-want +got):\n%s", args, diff)
			}
			for _, r := range records[1:] {
				if r["kind"] != "labels" { // A label listing writes its record, empty, either way.
					t.Errorf("run(%q): record %v after the summary, want none", args, r)
				}
			}
		})
	}
}

func TestListings(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	tests := []struct {
		name string
		args []string
		want []map[string]any
	}{
		{
			"series", []string{"query", "series", "--promql", "up", "--promql", `up{instance="node-b"}`},
			[]map[string]any{
				{"kind": "series", "metric": map[string]any{"__name__": "up", "instance": "node-a", "job": "node-exporter"}},
				{"kind": "series", "metric": map[string]any{"__name__": "up", "instance": "node-b", "job": "node-exporter"}},
				{"kind": "series", "metric": map[string]any{"__name__": "up", "instance": "node-c", "job": "node-exporter"}},
			},
		},
		{
			"label names", []string{"query", "labels"},
			[]map[string]any{{"kind": "labels", "names": []any{"__name__", "instance", "job"}}},
		},
		{
			"label values", []string{"query", "labels", "instance", "--promql", `{instance!="node-a"}`},
			[]map[string]any{{"kind": "labels", "name": "instance", "values": []any{"node-b", "node-c", "node-d"}}},
		},
		{
			"label values in the last hour", []string{"query", "labels", "instance", "--start", "1h"},
			[]map[string]any{{"kind": "labels", "name": "instance", "values": []any{"node-a", "node-b", "node-d"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", dumpDir, "--scratch", t.TempDir()}, tt.args...)
			records := mustRun(t, args...)
			if records[0]["empty"] != nil {
				t.Errorf("run(%q): empty = %v, want null", args, records[0]["empty"])
			}
			if diff := cmp.Diff(tt.want, records[1:]); diff != "" {
				t.Errorf("run(%q) records (-want +got):\n%s", args, diff)
			}
		})
	}
}

func TestSampleIntervalVaries(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{"steady", []string{"query", "range", "--promql", "up", "--start", "7h"}, false},
		{"changing", []string{"query", "range", "--promql", fixture.Irregular, "--start", "7h"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", dumpDir, "--scratch", t.TempDir()}, tt.args...)
			summary := mustRun(t, args...)[0]
			warned := false
			warnings, _ := summary["warnings"].([]any)
			for _, w := range warnings {
				warning, _ := w.(map[string]any)
				warned = warned || warning["code"] == "sample-interval-varies"
			}
			if warned != tt.want {
				t.Errorf("run(%q): sample-interval-varies warned %v, want %v; warnings %v", args, warned, tt.want, warnings)
			}
		})
	}
}

func TestSampleInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		args     []string
		want     string
	}{
		{"subquery", 15 * time.Second, []string{"query", "instant", "--promql", "max_over_time(up[10m:1m])"}, "15s"},
		// An instant query reads 5m of samples, so 2m samples are further apart than a quarter of the window.
		{"sparse instant", 2 * time.Minute, []string{"query", "instant", "--promql", "up"}, "2m"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--dump", writeFixture(t, tt.interval), "--scratch", t.TempDir()}, tt.args...)
			summary := mustRun(t, args...)[0]
			if summary["sample_interval"] != tt.want || summary["empty"] != nil {
				t.Errorf("run(%q): sample_interval %v, empty %v; want %s and null", args,
					summary["sample_interval"], summary["empty"], tt.want)
			}
		})
	}
}

// An out-of-order block doesn't hold the in-order samples in its range, so the replay keeps them from the WAL,
// as Prometheus's own open does.
func TestOutOfOrderBlock(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	blocks, err := fixture.Blocks(dumpDir)
	if err != nil {
		t.Fatal(err)
	}
	// Without chunks_head/ the WAL is the head's only source, so the samples after the in-order block come from
	// it alone; a replay skips WAL samples older than a chunk it loaded from chunks_head/.
	for _, dir := range []string{blocks[1], filepath.Join(dumpDir, "chunks_head")} {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.OutOfOrder(dumpDir, fixture.Start().Add(2*time.Hour), fixture.Start().Add(4*time.Hour)); err != nil {
		t.Fatal(err)
	}

	for _, at := range []time.Duration{3 * time.Hour, 4 * time.Hour, 5 * time.Hour} {
		args := []string{"--dump", dumpDir, "--scratch", t.TempDir(), "query", "instant",
			"--promql", `count_over_time(up{instance="node-a"}[1h])`, "--time", fixture.Start().Add(at).Format(time.RFC3339)}
		records := mustRun(t, args...)
		if len(records) != 2 {
			t.Fatalf("run(%q) = %v, want one series", args, records)
		}
		if value, _ := records[1]["value"].([]any); len(value) != 2 || value[1] != "240" {
			t.Errorf("run(%q): value %v, want 240 samples in the hour", args, records[1]["value"])
		}
	}
}

// With the first block gone, the WAL still holds its samples, but a replay drops them, so the dump starts at
// the remaining block.
func TestWALBeforeBlocks(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	blocks, err := fixture.Blocks(dumpDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(blocks[0]); err != nil {
		t.Fatal(err)
	}

	wantStart := fixture.Start().Add(2 * time.Hour).Format(time.RFC3339)
	for _, command := range [][]string{{"info"}, {"query", "instant", "--promql", "up"}} {
		args := append([]string{"--dump", dumpDir, "--scratch", t.TempDir()}, command...)
		dumpRec, _ := mustRun(t, args...)[0]["dump"].(map[string]any)
		if dumpRec["start"] != wantStart {
			t.Errorf("run(%q): dump.start = %v, want %s", args, dumpRec["start"], wantStart)
		}
	}
	args := []string{"--dump", dumpDir, "--scratch", t.TempDir(), "query", "instant", "--promql", "up",
		"--time", fixture.Start().Add(time.Hour).Format(time.RFC3339)}
	empty, _ := mustRun(t, args...)[0]["empty"].(map[string]any)
	if empty["reason"] != "outside-dump-range" {
		t.Errorf("run(%q): empty = %v, want outside-dump-range", args, empty)
	}
}

// failingWriter accepts limit bytes and fails every write after.
type failingWriter struct {
	limit int
	bytes.Buffer
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.Len()+len(p) > w.limit {
		return 0, errors.New("disk full")
	}
	return w.Buffer.Write(p)
}

// A run whose stdout fails partway has written part of its result, and an error record after it would join a
// cut line, so none is written.
func TestFailedWriteAddsNoRecord(t *testing.T) {
	dumpDir := writeFixture(t, 15*time.Second)
	stdout := &failingWriter{limit: 8 << 10}
	var stderr bytes.Buffer
	args := []string{"--dump", dumpDir, "--scratch", t.TempDir(), "query", "range", "--promql", "up", "--step", "15s"}
	if err := run(t.Context(), args, stdout, &stderr); err == nil {
		t.Fatalf("run(%q) with stdout failing = nil, want an error", args)
	}
	if stdout.Len() == 0 || bytes.Contains(stdout.Bytes(), []byte(`"kind":"error"`)) {
		t.Errorf("run(%q) wrote %d bytes, with an error record: %t; want part of the result and no error record",
			args, stdout.Len(), bytes.Contains(stdout.Bytes(), []byte(`"kind":"error"`)))
	}
}
