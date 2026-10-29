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
	// WALReplayed is a WAL replayed into the head, so queries see its samples.
	WALReplayed = "replayed"
	// WALRead is a WAL decoded for its range without building a head.
	WALRead = "read"
	// WALAbsent is a dump with no WAL.
	WALAbsent = "absent"
)

// WAL is what the store did with the dump's write-ahead log.
type WAL struct {
	State string
	Bytes int64
	// Newest is the WAL's newest sample, when Sampled.
	Newest  time.Time
	Sampled bool
}

// BlockOK is the status of a block that opened.
const BlockOK = "ok"

// Block is one block's directory, range and size.
type Block struct {
	Dir string
	// Start and End are the block's range as its meta.json gives it; End is exclusive.
	Start, End time.Time
	Series     uint64
	Status     string
}

// Options says whether to build a head, and where its files go.
type Options struct {
	// Scratch is the directory the run directory goes under.
	Scratch string
	// Replay builds the head from the WAL, so queries see its samples. Without it the store only reports what
	// the dump covers, and writes nothing.
	Replay bool
}

// Store is a dump's blocks and head, read without writing under the dump. It is a storage.Queryable.
type Store struct {
	blocks []*tsdb.Block
	// head is nil unless the WAL was replayed.
	head   *tsdb.Head
	runDir string
	wal    WAL
	// mint and maxt are the first and last sample times in the blocks and the WAL.
	mint, maxt int64
}

// Open opens the blocks in d and decodes its WAL for its range. With opts.Replay it also replays the WAL into a
// head whose chunk files live in a run directory under opts.Scratch, which Close removes.
func Open(ctx context.Context, d dump.Dump, opts Options, logger *slog.Logger) (_ *Store, err error) {
	s := &Store{wal: WAL{State: WALAbsent}, mint: math.MaxInt64, maxt: math.MinInt64}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()

	// The tsdb package logs at info level for routine work; its warnings are worth passing on.
	tsdbLogger := slog.New(levelHandler{slog.LevelWarn, logger.Handler()})

	if err := s.openBlocks(ctx, d.Blocks, tsdbLogger); err != nil {
		return nil, err
	}
	logger.Info("opened blocks", "count", len(s.blocks))
	if d.WBL != "" {
		logger.Warn("not reading wbl/: out-of-order samples in the dump are left out")
	}
	if d.WAL == "" {
		return s, nil
	}

	s.wal.Bytes, err = dirBytes(d.WAL)
	if err != nil {
		return nil, fmt.Errorf("size WAL: %w", err)
	}
	logger.Info("reading WAL", "bytes", s.wal.Bytes)
	minValid := s.minValid()
	span, err := readWAL(ctx, d.WAL, minValid, tsdbLogger)
	if err != nil {
		return nil, fmt.Errorf("read WAL: %w", err)
	}
	s.wal.State = WALRead
	if span.sampled {
		s.wal.Newest, s.wal.Sampled = timestamp.Time(span.newest).UTC(), true
	}
	if span.kept {
		s.mint, s.maxt = min(s.mint, span.oldestKept), max(s.maxt, span.newest)
	}

	if !opts.Replay {
		return s, nil
	}
	if err := s.replay(ctx, d, opts.Scratch, minValid, tsdbLogger, logger); err != nil {
		return nil, err
	}
	s.wal.State = WALReplayed
	return s, nil
}

func (s *Store) openBlocks(ctx context.Context, dirs []string, tsdbLogger *slog.Logger) error {
	for _, dir := range dirs {
		b, err := tsdb.OpenBlock(tsdbLogger, dir, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
		if err != nil {
			return fmt.Errorf("open block %s: %w", filepath.Base(dir), err)
		}
		s.blocks = append(s.blocks, b)
	}
	if len(s.blocks) == 0 {
		return nil
	}
	sort.Slice(s.blocks, func(i, j int) bool { return s.blocks[i].Meta().MinTime < s.blocks[j].Meta().MinTime })

	newest := s.blocks[0]
	for _, b := range s.blocks {
		s.mint = min(s.mint, b.Meta().MinTime)
		if b.Meta().MaxTime > newest.Meta().MaxTime {
			newest = b
		}
	}
	last, ok, err := lastSample(ctx, newest)
	if err != nil {
		return fmt.Errorf("find the last sample in block %s: %w", newest.Meta().ULID, err)
	}
	if ok {
		s.maxt = max(s.maxt, last)
	}
	return nil
}

// minValid is the time before which a replay drops the WAL's samples, as Prometheus's own open sets it: the
// latest end of a block cut from in-order samples. Blocks cut from out-of-order samples, or from only some of
// the head's series, don't count, since the WAL still holds in-order samples in their range.
func (s *Store) minValid() int64 {
	t := int64(math.MinInt64)
	for _, b := range s.blocks {
		c := b.Meta().Compaction
		if !c.FromOutOfOrder() && !c.FromStaleSeries() && !c.FromSelectedSeries() {
			t = max(t, b.Meta().MaxTime)
		}
	}
	return t
}

// replay builds the head from the WAL in a run directory under scratch, keeping samples from minValid on.
func (s *Store) replay(ctx context.Context, d dump.Dump, scratch string, minValid int64, tsdbLogger, logger *slog.Logger) error {
	if err := checkOutside(scratch, d.Path); err != nil {
		return err
	}
	var err error
	s.runDir, err = os.MkdirTemp(scratch, "prometheus-tsdb-reader-run-")
	if err != nil {
		return fmt.Errorf("create run directory under %s: %w", scratch, err)
	}

	// The head cuts, truncates and removes chunk files as it replays, so it gets its own copy of them.
	if d.ChunksHead != "" {
		method, err := copyDir(ctx, d.ChunksHead, filepath.Join(s.runDir, "chunks_head"))
		if err != nil {
			return fmt.Errorf("copy chunks_head: %w", err)
		}
		logger.Info("copied chunks_head under scratch", "by", method, "dir", s.runDir)
	}

	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = s.runDir
	// wlog.Open only records the directory; the head reads segments from it and writes nothing there.
	w, err := wlog.Open(tsdbLogger, d.WAL)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	head, err := tsdb.NewHead(nil, tsdbLogger, w, nil, opts, tsdb.NewHeadStats())
	if err != nil {
		return fmt.Errorf("create head: %w", err)
	}
	logger.Info("replaying WAL", "bytes", s.wal.Bytes)
	// Head.Init takes no context, so it runs on its own and an interrupt stops waiting for it. The head is then
	// left unclosed, since closing it would race the replay, and Close removes the run directory under it, which
	// fails the replay's next chunk write. A caller that stops a replay this way is expected to exit.
	replayed := make(chan error, 1)
	go func() { replayed <- head.Init(minValid) }()
	select {
	case err := <-replayed:
		s.head = head
		if err != nil {
			return fmt.Errorf("replay WAL: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("replay WAL: %w", ctx.Err())
	}
	logger.Info("replayed WAL", "series", s.head.NumSeries())
	return nil
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
	if s.head != nil && maxt >= s.head.MinTime() {
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
	if s.mint > s.maxt {
		return time.Time{}, time.Time{}, false
	}
	return timestamp.Time(s.mint).UTC(), timestamp.Time(s.maxt).UTC(), true
}

// WAL says what was done with the WAL, how big it is and its newest sample.
func (s *Store) WAL() WAL { return s.wal }

// Blocks returns the blocks in start order.
func (s *Store) Blocks() []Block {
	out := make([]Block, 0, len(s.blocks))
	for _, b := range s.blocks {
		m := b.Meta()
		out = append(out, Block{
			Dir:    filepath.Base(b.Dir()),
			Start:  timestamp.Time(m.MinTime).UTC(),
			End:    timestamp.Time(m.MaxTime).UTC(),
			Series: m.Stats.NumSeries,
			Status: BlockOK,
		})
	}
	return out
}

// Close closes the blocks and the head and removes the run directory.
func (s *Store) Close() error {
	var errs []error
	for _, b := range s.blocks {
		errs = append(errs, b.Close())
	}
	if s.head != nil {
		errs = append(errs, s.head.Close())
	}
	if s.runDir != "" {
		errs = append(errs, os.RemoveAll(s.runDir))
	}
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
