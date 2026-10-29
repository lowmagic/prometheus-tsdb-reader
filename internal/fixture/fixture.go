// Package fixture writes synthetic TSDB dumps for tests with Prometheus's own tsdb package. Every name in them
// is invented.
package fixture

import (
	"archive/tar"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
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

// Departed is a series whose target disappeared 3m before the end: scraped every 15s until then, then the stale
// marker Prometheus writes when a target disappears.
const Departed = `process_start_time_seconds{job="departed"}`

// Write writes the fixture's TSDB into dir, with up scraped every interval, Irregular and Departed. Compacting it leaves the first four
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
	departed := labels.FromStrings("__name__", "process_start_time_seconds", "job", "departed")
	gone := End().Add(-3 * time.Minute)
	for t := Start(); !t.After(gone); t = t.Add(15 * time.Second) {
		v := float64(Start().Unix())
		if t.Equal(gone) {
			v = math.Float64frombits(value.StaleNaN)
		}
		if _, err := app.Append(0, departed, t.UnixMilli(), v); err != nil {
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

// Blocks returns the block directories in the fixture at dir, in time order: the 2h blocks starting at Start
// and 2h after it.
func Blocks(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var blocks []string
	starts := map[string]int64{}
	for _, e := range entries {
		meta, err := readMeta(filepath.Join(dir, e.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, filepath.Join(dir, e.Name()))
		starts[blocks[len(blocks)-1]] = meta.MinTime
	}
	if len(blocks) != 2 {
		return nil, fmt.Errorf("fixture at %s has %d blocks, want 2", dir, len(blocks))
	}
	slices.SortFunc(blocks, func(a, b string) int { return cmp.Compare(starts[a], starts[b]) })
	return blocks, nil
}

func readMeta(block string) (tsdb.BlockMeta, error) {
	var meta tsdb.BlockMeta
	content, err := os.ReadFile(filepath.Join(block, "meta.json"))
	if err != nil {
		return meta, err
	}
	return meta, json.Unmarshal(content, &meta)
}

// OutOfOrder writes a block into dir covering [from, to) that Prometheus would have cut from out-of-order
// samples, holding one series with a sample at each end and none of the fixture's others.
func OutOfOrder(dir string, from, to time.Time) (err error) {
	ls := labels.FromStrings("__name__", "late_sample", "job", "late")
	series := storage.NewListSeries(ls, []chunks.Sample{
		floatSample{from.UnixMilli(), 1}, floatSample{to.Add(-time.Second).UnixMilli(), 1},
	})
	block, err := tsdb.CreateBlock([]storage.Series{series}, dir, to.Sub(from).Milliseconds(), slog.New(slog.DiscardHandler))
	if err != nil {
		return fmt.Errorf("write out-of-order block: %w", err)
	}
	meta, err := readMeta(block)
	if err != nil {
		return err
	}
	meta.Compaction.SetOutOfOrder()
	content, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(block, "meta.json"), content, 0o600)
}

// floatSample is one float sample, as storage.NewListSeries takes it.
type floatSample struct {
	t int64
	f float64
}

func (s floatSample) T() int64                    { return s.t }
func (floatSample) ST() int64                     { return 0 }
func (s floatSample) F() float64                  { return s.f }
func (floatSample) H() *histogram.Histogram       { return nil }
func (floatSample) FH() *histogram.FloatHistogram { return nil }
func (floatSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s floatSample) Copy() chunks.Sample         { return s }

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
