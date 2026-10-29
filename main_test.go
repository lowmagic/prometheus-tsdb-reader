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

	"github.com/junglestyle/prometheus-tsdb-reader/dump"
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
			"wal":        map[string]any{"state": "replayed", "newest": end},
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

func TestInfoLayouts(t *testing.T) {
	dumpDir := writeFixture(t)
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
	dumpDir := writeFixture(t)
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

func TestRootChoice(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := fixture.Write(t.Context(), mkdir(t, dir, name)); err != nil {
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
