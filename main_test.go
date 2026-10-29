package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/junglestyle/prometheus-tsdb-reader/internal/fixture"
)

func TestQueryInstant(t *testing.T) {
	dumpDir := writeFixture(t)
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
			"wal":        map[string]any{"state": "replayed"},
			"expression": "up",
			"time":       end,
			"series":     2.0,
			"points":     2.0,
			"warnings":   []any{},
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
	dumpDir := writeFixture(t)
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
func writeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := fixture.Write(t.Context(), dir); err != nil {
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
