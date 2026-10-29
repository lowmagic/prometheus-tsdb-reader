// Package fixture writes synthetic TSDB dumps for tests with Prometheus's own tsdb package. Every name in them
// is invented.
package fixture

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

// Span is how long the fixture's samples run.
const Span = 7 * time.Hour

// Start is the fixture's first sample time.
func Start() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// End is the fixture's last sample time.
func End() time.Time { return Start().Add(Span) }

// target is one node-exporter target's up value and how long it was scraped.
type target struct {
	instance string
	up       float64
	until    time.Time
}

// targets are the fixture's series: node-a up and node-b down for the whole span, and node-c up for the first
// hour only, so it has no samples in the default range at the end.
func targets() []target {
	return []target{
		{"node-a", 1, End()},
		{"node-b", 0, End()},
		{"node-c", 1, Start().Add(time.Hour)},
	}
}

// Irregular is a series scraped every 15s for the first half of the span and every minute after, whatever
// interval the rest of the fixture has.
const Irregular = `node_boot_time_seconds{instance="node-d"}`

// Write writes the fixture's TSDB into dir, with up scraped every interval, and Irregular. Compacting it leaves the first four
// hours in two 2h blocks and the last three in the WAL, where the head has filled enough chunks to write
// chunks_head/ files.
func Write(ctx context.Context, dir string, interval time.Duration) (err error) {
	opts := tsdb.DefaultOptions()
	opts.MinBlockDuration = (2 * time.Hour).Milliseconds()
	opts.MaxBlockDuration = opts.MinBlockDuration
	db, err := tsdb.Open(dir, nil, nil, opts, nil)
	if err != nil {
		return fmt.Errorf("open fixture TSDB: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	db.DisableCompactions()

	app := db.Appender(ctx)
	for t := Start(); !t.After(End()); t = t.Add(interval) {
		for _, tg := range targets() {
			if t.After(tg.until) {
				continue
			}
			ls := labels.FromStrings("__name__", "up", "instance", tg.instance, "job", "node-exporter")
			if _, err := app.Append(0, ls, t.UnixMilli(), tg.up); err != nil {
				return errors.Join(fmt.Errorf("append fixture sample: %w", err), app.Rollback())
			}
		}
	}
	irregular := labels.FromStrings("__name__", "node_boot_time_seconds", "instance", "node-d", "job", "node-exporter")
	for t := Start(); !t.After(End()); t = t.Add(15 * time.Second) {
		if t.Sub(Start()) > Span/2 && t.Sub(Start())%time.Minute != 0 {
			continue
		}
		if _, err := app.Append(0, irregular, t.UnixMilli(), float64(Start().Unix())); err != nil {
			return errors.Join(fmt.Errorf("append fixture sample: %w", err), app.Rollback())
		}
	}
	if err := app.Commit(); err != nil {
		return fmt.Errorf("commit fixture samples: %w", err)
	}
	if err := db.Compact(ctx); err != nil {
		return fmt.Errorf("compact fixture: %w", err)
	}
	return nil
}

// Archive writes the directory dir to w as a tar stream with every entry under prefix, gzip-compressed when
// compress is set, as a dump arrives.
func Archive(w io.Writer, dir, prefix string, compress bool) (err error) {
	if compress {
		gz := gzip.NewWriter(w)
		defer func() { err = errors.Join(err, gz.Close()) }()
		w = gz
	}
	tw := tar.NewWriter(w)
	defer func() { err = errors.Join(err, tw.Close()) }()
	return filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || (rel == "." && prefix == "") {
			return err
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(filepath.Join(prefix, rel))
		if e.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !e.Type().IsRegular() {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = tw.Write(content)
		return err
	})
}
