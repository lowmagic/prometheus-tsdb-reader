// Package store opens a dump's blocks and builds its head from the WAL under scratch, as one queryable.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/wlog"

	"github.com/junglestyle/prometheus-tsdb-reader/dump"
)

// WAL states, as the summary reports them.
const (
	WALReplayed = "replayed"
	WALAbsent   = "absent"
)

// WAL is what the store did with the dump's write-ahead log.
type WAL struct {
	State string
	Bytes int64
}

// Store is a dump's blocks and head, read without writing under the dump. It is a storage.Queryable.
type Store struct {
	blocks []*tsdb.Block
	head   *tsdb.Head
	runDir string
	wal    WAL
}

// Open opens the blocks in d and replays its WAL into a head whose chunk files live in a run directory it
// creates under scratch. Close removes that directory.
func Open(ctx context.Context, d dump.Dump, scratch string, logger *slog.Logger) (_ *Store, err error) {
	if err := checkOutside(scratch, d.Path); err != nil {
		return nil, err
	}
	runDir, err := os.MkdirTemp(scratch, "prometheus-tsdb-reader-run-")
	if err != nil {
		return nil, fmt.Errorf("create run directory under %s: %w", scratch, err)
	}
	s := &Store{runDir: runDir, wal: WAL{State: WALAbsent}}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()

	// The tsdb package logs at info level for routine work; its warnings are worth passing on.
	tsdbLogger := slog.New(levelHandler{slog.LevelWarn, logger.Handler()})

	for _, dir := range d.Blocks {
		b, err := tsdb.OpenBlock(tsdbLogger, dir, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
		if err != nil {
			return nil, fmt.Errorf("open block %s: %w", filepath.Base(dir), err)
		}
		s.blocks = append(s.blocks, b)
	}
	sort.Slice(s.blocks, func(i, j int) bool { return s.blocks[i].Meta().MinTime < s.blocks[j].Meta().MinTime })
	logger.Info("opened blocks", "count", len(s.blocks))

	if d.WBL != "" {
		logger.Warn("not reading wbl/: out-of-order samples in the dump are left out")
	}

	// The head cuts, truncates and removes chunk files as it replays, so it gets its own copy of them.
	if d.ChunksHead != "" {
		method, err := copyDir(ctx, d.ChunksHead, filepath.Join(runDir, "chunks_head"))
		if err != nil {
			return nil, fmt.Errorf("copy chunks_head: %w", err)
		}
		logger.Info("copied chunks_head under scratch", "by", method, "dir", runDir)
	}

	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = runDir
	var w *wlog.WL
	if d.WAL != "" {
		s.wal.Bytes, err = dirBytes(d.WAL)
		if err != nil {
			return nil, fmt.Errorf("size WAL: %w", err)
		}
		// wlog.Open only records the directory; the head reads segments from it and writes nothing there.
		w, err = wlog.Open(tsdbLogger, d.WAL)
		if err != nil {
			return nil, fmt.Errorf("open WAL: %w", err)
		}
		logger.Info("replaying WAL", "bytes", s.wal.Bytes)
	}
	s.head, err = tsdb.NewHead(nil, tsdbLogger, w, nil, opts, tsdb.NewHeadStats())
	if err != nil {
		return nil, fmt.Errorf("create head: %w", err)
	}
	minValid := int64(math.MinInt64)
	if len(s.blocks) > 0 {
		minValid = s.blocks[len(s.blocks)-1].Meta().MaxTime
	}
	if err := s.head.Init(minValid); err != nil {
		return nil, fmt.Errorf("replay WAL: %w", err)
	}
	if w != nil {
		s.wal.State = WALReplayed
		logger.Info("replayed WAL", "series", s.head.NumSeries())
	}
	return s, nil
}

// Querier returns a querier over every block overlapping [mint, maxt] and the head.
func (s *Store) Querier(mint, maxt int64) (_ storage.Querier, err error) {
	var queriers []storage.Querier
	defer func() {
		if err != nil {
			for _, q := range queriers {
				err = errors.Join(err, q.Close())
			}
		}
	}()
	for _, b := range s.blocks {
		if !b.OverlapsClosedInterval(mint, maxt) {
			continue
		}
		q, err := tsdb.NewBlockQuerier(b, mint, maxt)
		if err != nil {
			return nil, fmt.Errorf("query block %s: %w", b.Meta().ULID, err)
		}
		queriers = append(queriers, q)
	}
	if maxt >= s.head.MinTime() {
		q, err := tsdb.NewBlockQuerier(tsdb.NewRangeHead(s.head, mint, maxt), mint, maxt)
		if err != nil {
			return nil, fmt.Errorf("query head: %w", err)
		}
		queriers = append(queriers, q)
	}
	return storage.NewMergeQuerier(queriers, nil, storage.ChainedSeriesMerge), nil
}

// Range returns the dump's first and last sample times, and false when it holds no samples.
func (s *Store) Range() (start, end time.Time, ok bool) {
	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
	if len(s.blocks) > 0 {
		mint = s.blocks[0].Meta().MinTime
		// A block's MaxTime is exclusive.
		maxt = s.blocks[len(s.blocks)-1].Meta().MaxTime - 1
	}
	// An empty head's MinTime and MaxTime are MaxInt64 and MinInt64, so they change nothing here.
	mint = min(mint, s.head.MinTime())
	maxt = max(maxt, s.head.MaxTime())
	if mint > maxt {
		return time.Time{}, time.Time{}, false
	}
	return timestamp.Time(mint), timestamp.Time(maxt), true
}

// WAL says whether the WAL was replayed and how big it is.
func (s *Store) WAL() WAL { return s.wal }

// Close closes the blocks and the head and removes the run directory.
func (s *Store) Close() error {
	var errs []error
	for _, b := range s.blocks {
		errs = append(errs, b.Close())
	}
	if s.head != nil {
		errs = append(errs, s.head.Close())
	}
	errs = append(errs, os.RemoveAll(s.runDir))
	return errors.Join(errs...)
}

// checkOutside refuses a scratch directory inside the dump, which would put the run's writes under it.
func checkOutside(scratch, dumpPath string) error {
	s, err := resolve(scratch)
	if err != nil {
		return fmt.Errorf("resolve scratch %s: %w", scratch, err)
	}
	d, err := resolve(dumpPath)
	if err != nil {
		return fmt.Errorf("resolve dump %s: %w", dumpPath, err)
	}
	if s == d || strings.HasPrefix(s, d+string(filepath.Separator)) {
		return fmt.Errorf("scratch %s is inside the dump %s, and nothing is written under the dump", scratch, dumpPath)
	}
	return nil
}

func resolve(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

// copyDir gives dst the regular files in src, by hard link where it can and by copying where it can't, as
// across filesystems. It returns "copy" if any file had to be copied, else "link".
func copyDir(ctx context.Context, src, dst string) (string, error) {
	if err := os.Mkdir(dst, 0o750); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return "", err
	}
	method := "link"
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if !e.Type().IsRegular() {
			continue
		}
		from, to := filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())
		if os.Link(from, to) == nil {
			continue
		}
		method = "copy"
		if err := copyFile(from, to); err != nil {
			return "", err
		}
	}
	return method, nil
}

func copyFile(from, to string) (err error) {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, in.Close()) }()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, out.Close()) }()
	_, err = io.Copy(out, in)
	return err
}

func dirBytes(dir string) (int64, error) {
	var n int64
	err := filepath.WalkDir(dir, func(_ string, e fs.DirEntry, err error) error {
		if err != nil || !e.Type().IsRegular() {
			return err
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		n += info.Size()
		return nil
	})
	return n, err
}

// levelHandler drops records below its level before passing them on.
type levelHandler struct {
	level slog.Level
	slog.Handler
}

func (h levelHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level && h.Handler.Enabled(ctx, l)
}

func (h levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return levelHandler{h.level, h.Handler.WithAttrs(attrs)}
}

func (h levelHandler) WithGroup(name string) slog.Handler {
	return levelHandler{h.level, h.Handler.WithGroup(name)}
}
