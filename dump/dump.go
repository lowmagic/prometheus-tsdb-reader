// Package dump accepts a dump as a directory or an archive, extracts an archive safely under scratch, and finds
// the TSDB inside it.
package dump

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/oklog/ulid/v2"
)

// Dump is a TSDB root and what it holds. Paths are absolute.
type Dump struct {
	// Path is the dump's directory: the one given, or where the archive was extracted. Nothing is written under it.
	Path string
	// Root is the directory holding the blocks and the WAL.
	Root string
	// RootName is Root relative to Path, "." when they're the same.
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

// Options says where an archive goes and which root to use.
type Options struct {
	// Scratch is the directory an archive is extracted under.
	Scratch string
	// Root picks the TSDB root, relative to the dump's directory, when the dump holds more than one. Empty
	// means find the only one.
	Root string
	// MaxExtractBytes is the most an extraction writes.
	MaxExtractBytes int64
}

// maxListed bounds how many directories an error lists.
const maxListed = 20

// Open accepts path as a directory or an archive, extracting an archive under opts.Scratch, and finds the TSDB
// root in it.
func Open(ctx context.Context, path string, opts Options, logger *slog.Logger) (Dump, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Dump{}, fmt.Errorf("open dump: %w", err)
	}
	dir := path
	if !info.IsDir() {
		dir, err = extract(ctx, path, opts, logger)
		if err != nil {
			return Dump{}, err
		}
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return Dump{}, fmt.Errorf("resolve dump %s: %w", path, err)
	}

	root, err := findRoot(dir, opts.Root)
	if err != nil {
		return Dump{}, err
	}
	d, err := list(root)
	if err != nil {
		return Dump{}, err
	}
	d.Path = dir
	d.RootName, err = filepath.Rel(dir, root)
	if err != nil {
		return Dump{}, err
	}
	logger.Info("found TSDB", "root", d.RootName, "blocks", len(d.Blocks), "wal", d.WAL != "")
	return d, nil
}

// findRoot returns the one directory under dir that holds blocks or a WAL, or the one pick names.
func findRoot(dir, pick string) (string, error) {
	if pick != "" {
		root := filepath.Join(dir, pick)
		if !filepath.IsLocal(pick) {
			return "", fmt.Errorf("--root %s leaves the dump's directory", pick)
		}
		ok, err := isRoot(root)
		if err != nil {
			return "", fmt.Errorf("read --root %s: %w", pick, err)
		}
		if !ok {
			return "", fmt.Errorf("--root %s holds neither block directories nor wal/", pick)
		}
		return root, nil
	}

	var roots, seen []string
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || !e.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path) // path is under dir, as WalkDir found it there.
		ok, err := isRoot(path)
		if err != nil {
			return err
		}
		if ok {
			roots = append(roots, rel)
			return filepath.SkipDir
		}
		seen = append(seen, rel)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("look for the TSDB in %s: %w", dir, err)
	}
	switch len(roots) {
	case 0:
		return "", fmt.Errorf("no TSDB in %s: no directory holds block directories or wal/; saw %s",
			dir, listed(seen))
	case 1:
		return filepath.Join(dir, roots[0]), nil
	}
	return "", fmt.Errorf("%d TSDBs in %s: %s; pick one with --root", len(roots), dir, listed(roots))
}

// isRoot reports whether dir holds a block directory or a wal/ directory.
func isRoot(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if e.Name() == "wal" || isBlock(filepath.Join(dir, e.Name()), e.Name()) {
			return true, nil
		}
	}
	return false, nil
}

// list returns what the TSDB root holds.
func list(root string) (Dump, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return Dump{}, fmt.Errorf("read TSDB root: %w", err)
	}
	d := Dump{Root: root}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(root, e.Name())
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

func listed(names []string) string {
	if len(names) == 0 {
		return "nothing"
	}
	if len(names) > maxListed {
		return fmt.Sprintf("%s and %d more", strings.Join(names[:maxListed], ", "), len(names)-maxListed)
	}
	return strings.Join(names, ", ")
}
