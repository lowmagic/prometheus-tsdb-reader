// Package dump finds the TSDB inside a dump and lists its blocks and WAL.
package dump

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/oklog/ulid/v2"
)

// Dump is a TSDB root and what it holds. Paths are absolute.
type Dump struct {
	// Path is the directory the dump was given as. Nothing is written under it.
	Path string
	// Root is the directory holding the blocks and the WAL.
	Root string
	// RootName is Root relative to the path the dump was given as, "." when they're the same.
	RootName string
	// Blocks are the block directories, in name order.
	Blocks []string
	// WAL is the write-ahead log directory, or "" when the dump has none.
	WAL string
	// ChunksHead is the head's chunk directory, or "" when the dump has none.
	ChunksHead string
	// WBL is the out-of-order write-behind log directory, or "" when the dump has none.
	WBL string
}

// Open finds the TSDB in dir, whose blocks and WAL sit at its top level.
func Open(dir string) (Dump, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return Dump{}, fmt.Errorf("resolve dump %s: %w", dir, err)
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		return Dump{}, fmt.Errorf("read dump %s: %w", dir, err)
	}

	d := Dump{Path: abs, Root: abs, RootName: "."}
	var seen []string
	for _, e := range entries {
		seen = append(seen, e.Name())
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(abs, e.Name())
		switch {
		case e.Name() == "wal":
			d.WAL = path
		case e.Name() == "chunks_head":
			d.ChunksHead = path
		case e.Name() == "wbl":
			d.WBL = path
		case isBlock(path, e.Name()):
			d.Blocks = append(d.Blocks, path)
		}
	}
	if len(d.Blocks) == 0 && d.WAL == "" {
		return Dump{}, fmt.Errorf("no TSDB in %s: want block directories or wal/, saw %s",
			dir, strings.Join(seen, ", "))
	}
	sort.Strings(d.Blocks)
	return d, nil
}

// isBlock reports whether a directory is named by a ULID and has a meta.json, as a block does.
func isBlock(path, name string) bool {
	if _, err := ulid.ParseStrict(name); err != nil {
		return false
	}
	_, err := os.Stat(filepath.Join(path, "meta.json"))
	return err == nil
}
