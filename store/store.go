// Package store opens a dump's blocks and builds its head from the WAL under scratch, as one queryable.
package store

import (
	"cmp"
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
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
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
	// WALSkipped is a WAL left out of the dump at the caller's request. It is still decoded for its own range.
	WALSkipped = "skipped"
	// WALFailed is a WAL with a record that couldn't be read, past which nothing is kept.
	WALFailed = "failed"
	// WALAbsent is a dump with no WAL.
	WALAbsent = "absent"
)

// WAL is what the store did with the dump's write-ahead log.
type WAL struct {
	State string
	Bytes int64
	// Newest is the newest sample in any readable record, when Sampled.
	Newest  time.Time
	Sampled bool
	// Err is why a failed WAL failed.
	Err error
	// Missing is the span of samples a failed WAL leaves out.
	Missing Missing
}

// Missing is the span of samples a failed WAL leaves out.
type Missing struct {
	// Start is the dump's newest sample before the failure, when Started.
	Start   time.Time
	Started bool
	// End is the newest sample in a readable record after the failure, when Ended. Without one the span is
	// open-ended.
	End   time.Time
	Ended bool
}

// Block statuses, as info lists them.
const (
	// BlockOK is a block that opened, which queries read.
	BlockOK = "ok"
	// BlockCorrupt is a block that didn't open. Its range is a gap.
	BlockCorrupt = "corrupt"
	// BlockSuperseded is a block whose samples a readable compacted block holds, which queries read instead.
	BlockSuperseded = "superseded"
)

// Block is one block's directory, range, size and status.
type Block struct {
	Dir string
	// HasMeta is false when the block's meta.json couldn't be read, so its range and series count are unknown.
	HasMeta bool
	// Start and End are the block's range as its meta.json gives it; End is exclusive.
	Start, End time.Time
	Series     uint64
	Status     string
	// Err is why the block didn't open, or nil.
	Err error
}

// Overlaps reports whether the block's range overlaps [start, end]. A block with an unknown range may overlap
// any.
func (b Block) Overlaps(start, end time.Time) bool {
	return !b.HasMeta || !b.Start.After(end) && start.Before(b.End)
}

// Options says whether to build a head, and where its files go.
type Options struct {
	// Scratch is the directory the run directory goes under.
	Scratch string
	// Replay builds the head from the WAL, so queries see its samples. Without it the store only reports what
	// the dump covers, and writes nothing.
	Replay bool
	// SkipWAL leaves the WAL out of the dump: it isn't replayed, and the dump ends with its blocks. It is still
	// decoded for its own range.
	SkipWAL bool
}

// Store is a dump's blocks and head, read without writing under the dump. It is a storage.Queryable.
type Store struct {
	// blocks are the blocks queries read, in start order.
	blocks []*tsdb.Block
	// listed is every block with its status.
	listed []Block
	// head is nil unless the WAL was replayed.
	head   *tsdb.Head
	runDir string
	wal    WAL
	// mint and maxt are the first and last sample times in the blocks and the WAL.
	mint, maxt int64
}

// Open opens the blocks in d and decodes its WAL for its range. With opts.Replay it also replays the WAL into a
// head whose chunk files live in a run directory under opts.Scratch, which Close removes. A block that doesn't
// open is skipped, and a WAL record that can't be read ends what is kept of the WAL; Blocks and WAL report them.
func Open(ctx context.Context, d dump.Dump, opts Options, logger *slog.Logger) (_ *Store, err error) {
	s := &Store{wal: WAL{State: WALAbsent}, mint: math.MaxInt64, maxt: math.MinInt64}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()

	// The tsdb package logs at info level for routine work; its warnings are worth passing on.
	tsdbLogger := slog.New(levelHandler{slog.LevelWarn, logger.Handler()})

	if err := s.openBlocks(ctx, d.Blocks, tsdbLogger, logger); err != nil {
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
	pass, err := readWAL(ctx, d.WAL, minValid, tsdbLogger)
	if err != nil {
		return nil, fmt.Errorf("read WAL: %w", err)
	}
	if pass.sampled {
		s.wal.Newest, s.wal.Sampled = timestamp.Time(pass.newest).UTC(), true
	}
	if pass.failure != nil {
		logger.Warn("the WAL has a record that can't be read; a replay keeps nothing after it", "err", pass.failure)
	}
	if opts.SkipWAL {
		s.wal.State = WALSkipped
		logger.Info("skipping the WAL: the dump ends with its newest block")
		return s, nil
	}
	if pass.kept {
		s.mint, s.maxt = min(s.mint, pass.oldestKept), max(s.maxt, pass.newestKept)
	}

	s.wal.State = WALRead
	// Where the WAL's readable records leave the dump, before the head adds chunks it had written.
	kept := s.maxt
	var failure error
	if pass.failure != nil {
		failure = fmt.Errorf("read WAL: %w", pass.failure)
	}
	if opts.Replay {
		failure, err = s.replay(ctx, d, opts.Scratch, minValid, tsdbLogger, logger)
		if err != nil {
			return nil, err
		}
		s.wal.State = WALReplayed
		// After a failure the head also holds the chunks it had written to chunks_head/ before it, which can be
		// newer than the failure.
		s.maxt = max(s.maxt, s.head.MaxTime())
	}
	if failure != nil {
		s.fail(failure, kept, pass)
	}
	return s, nil
}

// fail records a WAL that failed with err. Its samples are missing from kept, the dump's newest sample before
// the failure, to the newest sample the pass found after it, or when the pass found no failure, its newest.
// Chunks a replay keeps from chunks_head/ can reach into that span, but only for the series whose chunks the
// head had written, so the span stands.
func (s *Store) fail(err error, kept int64, pass walPass) {
	s.wal.State, s.wal.Err = WALFailed, err
	m := &s.wal.Missing
	if kept != math.MinInt64 {
		m.Start, m.Started = timestamp.Time(kept).UTC(), true
	}
	switch {
	case pass.failure != nil && pass.after:
		m.End, m.Ended = timestamp.Time(pass.newestAfter).UTC(), true
	case pass.failure == nil && pass.sampled:
		m.End, m.Ended = timestamp.Time(pass.newest).UTC(), true
	}
}

// openBlocks opens the blocks in dirs. A block that doesn't open, or whose index fails as its newest sample is
// found, is corrupt; a block whose samples a readable block holds, as its compaction parent or source, is
// superseded. Queries read the rest.
func (s *Store) openBlocks(ctx context.Context, dirs []string, tsdbLogger, logger *slog.Logger) error {
	var opened []*tsdb.Block
	for _, dir := range dirs {
		b, err := tsdb.OpenBlock(tsdbLogger, dir, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
		if err != nil {
			logger.Warn("skipping a block that doesn't open", "block", filepath.Base(dir), "err", err)
			s.listed = append(s.listed, corrupt(dir, err))
			continue
		}
		opened = append(opened, b)
	}
	opened, err := s.findEnd(ctx, opened, logger)
	if err != nil {
		return errors.Join(err, closeBlocks(opened))
	}

	held := map[ulid.ULID]bool{}
	for _, b := range opened {
		c := b.Meta().Compaction
		for _, p := range c.Parents {
			held[p.ULID] = true
		}
		for _, src := range c.Sources {
			if src != b.Meta().ULID {
				held[src] = true
			}
		}
	}
	for i, b := range s.listed {
		if id, err := ulid.ParseStrict(b.Dir); err == nil && held[id] {
			logger.Info("a compacted block holds the samples of a corrupt one", "block", b.Dir)
			s.listed[i].Status = BlockSuperseded
		}
	}
	var errs []error
	for _, b := range opened {
		if held[b.Meta().ULID] {
			s.listed = append(s.listed, listed(b, BlockSuperseded, nil))
			errs = append(errs, b.Close())
			continue
		}
		s.listed = append(s.listed, listed(b, BlockOK, nil))
		s.blocks = append(s.blocks, b)
		s.mint = min(s.mint, b.Meta().MinTime)
	}
	slices.SortFunc(s.blocks, func(a, b *tsdb.Block) int { return cmp.Compare(a.Meta().MinTime, b.Meta().MinTime) })
	slices.SortFunc(s.listed, func(a, b Block) int {
		if a.HasMeta != b.HasMeta {
			if a.HasMeta {
				return -1
			}
			return 1
		}
		return cmp.Or(a.Start.Compare(b.Start), strings.Compare(a.Dir, b.Dir))
	})
	return errors.Join(errs...)
}

// findEnd sets the dump's end from the newest sample in the blocks, reading the index of each block whose range
// could hold one newer than any found, newest end first, since blocks can overlap. A block whose index fails is
// corrupt; findEnd closes it and returns the others.
func (s *Store) findEnd(ctx context.Context, opened []*tsdb.Block, logger *slog.Logger) ([]*tsdb.Block, error) {
	slices.SortFunc(opened, func(a, b *tsdb.Block) int { return cmp.Compare(b.Meta().MaxTime, a.Meta().MaxTime) })
	readable := opened[:0:0]
	for i, b := range opened {
		// A block's samples are all before its MaxTime.
		if s.maxt != math.MinInt64 && b.Meta().MaxTime-1 <= s.maxt {
			return append(readable, opened[i:]...), nil
		}
		last, ok, err := lastSample(ctx, b)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return append(readable, opened[i:]...), ctxErr
		}
		if err != nil {
			err = fmt.Errorf("read index: %w", err)
			logger.Warn("skipping a block whose index can't be read", "block", filepath.Base(b.Dir()), "err", err)
			s.listed = append(s.listed, listed(b, BlockCorrupt, err))
			if err := b.Close(); err != nil {
				return append(readable, opened[i+1:]...), fmt.Errorf("close block %s: %w", b.Meta().ULID, err)
			}
			continue
		}
		if ok {
			s.maxt = max(s.maxt, last)
		}
		readable = append(readable, b)
	}
	return readable, nil
}

// corrupt lists a block that didn't open, with the range its meta.json gives when that reads.
func corrupt(dir string, err error) Block {
	b := Block{Dir: filepath.Base(dir), Status: BlockCorrupt, Err: err}
	content, readErr := os.ReadFile(filepath.Join(dir, "meta.json"))
	var meta tsdb.BlockMeta
	if readErr != nil || json.Unmarshal(content, &meta) != nil || meta.Version != 1 {
		return b
	}
	b.HasMeta = true
	b.Start, b.End = timestamp.Time(meta.MinTime).UTC(), timestamp.Time(meta.MaxTime).UTC()
	b.Series = meta.Stats.NumSeries
	return b
}

func listed(b *tsdb.Block, status string, err error) Block {
	m := b.Meta()
	return Block{
		Dir:     filepath.Base(b.Dir()),
		HasMeta: true,
		Start:   timestamp.Time(m.MinTime).UTC(),
		End:     timestamp.Time(m.MaxTime).UTC(),
		Series:  m.Stats.NumSeries,
		Status:  status,
		Err:     err,
	}
}

func closeBlocks(blocks []*tsdb.Block) error {
	var errs []error
	for _, b := range blocks {
		errs = append(errs, b.Close())
	}
	return errors.Join(errs...)
}

// minValid is the time before which a replay drops the WAL's samples, as Prometheus's own open sets it: the
// latest end of a block cut from in-order samples. Blocks cut from out-of-order samples, or from only some of
// the head's series, don't count, since the WAL still holds in-order samples in their range. Nor do corrupt
// blocks: their compaction may be unknown, and leaving them out lets the WAL fill in what it holds of their
// range.
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

// replay builds the head from the WAL in a run directory under scratch, keeping samples from minValid on. A
// record the replay can't read ends it; the head keeps what it replayed before, and replay returns the failure.
func (s *Store) replay(ctx context.Context, d dump.Dump, scratch string, minValid int64, tsdbLogger, logger *slog.Logger) (failure, err error) {
	if err := checkOutside(scratch, d.Path); err != nil {
		return nil, err
	}
	s.runDir, err = os.MkdirTemp(scratch, "prometheus-tsdb-reader-run-")
	if err != nil {
		return nil, fmt.Errorf("create run directory under %s: %w", scratch, err)
	}

	// The head cuts, truncates and removes chunk files as it replays, so it gets its own copy of them.
	if d.ChunksHead != "" {
		method, err := copyDir(ctx, d.ChunksHead, filepath.Join(s.runDir, "chunks_head"))
		if err != nil {
			return nil, fmt.Errorf("copy chunks_head: %w", err)
		}
		logger.Info("copied chunks_head under scratch", "by", method, "dir", s.runDir)
	}

	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = s.runDir
	// wlog.Open only records the directory; the head reads segments from it and writes nothing there.
	w, err := wlog.Open(tsdbLogger, d.WAL)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	head, err := tsdb.NewHead(nil, tsdbLogger, w, nil, opts, tsdb.NewHeadStats())
	if err != nil {
		return nil, fmt.Errorf("create head: %w", err)
	}
	logger.Info("replaying WAL", "bytes", s.wal.Bytes)
	// Head.Init takes no context, so it runs on its own and an interrupt stops waiting for it. The head is then
	// left unclosed, since closing it would race the replay, and Close removes the run directory under it, which
	// fails the replay's next chunk write. A caller that stops a replay this way is expected to exit.
	replayed := make(chan error, 1)
	go func() { replayed <- head.Init(minValid) }()
	select {
	case err := <-replayed:
		// Head.Init keeps what it replayed when it fails, as Prometheus's own open relies on.
		s.head = head
		if _, ok := errors.AsType[*wlog.CorruptionErr](err); ok {
			logger.Warn("the WAL replay stopped at a record it can't read; answering from what it replayed",
				"series", s.head.NumSeries(), "err", err)
			return fmt.Errorf("replay WAL: %w", err), nil
		}
		if err != nil {
			return nil, fmt.Errorf("replay WAL: %w", err)
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("replay WAL: %w", ctx.Err())
	}
	logger.Info("replayed WAL", "series", s.head.NumSeries())
	return nil, nil
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

// Blocks returns every block with its status: those with a range in start order, then those without.
func (s *Store) Blocks() []Block { return s.listed }

// Gaps returns the corrupt blocks, whose ranges the dump is missing.
func (s *Store) Gaps() []Block {
	var gaps []Block
	for _, b := range s.listed {
		if b.Status == BlockCorrupt {
			gaps = append(gaps, b)
		}
	}
	return gaps
}

// Close closes the blocks and the head and removes the run directory.
func (s *Store) Close() error {
	errs := []error{closeBlocks(s.blocks)}
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
