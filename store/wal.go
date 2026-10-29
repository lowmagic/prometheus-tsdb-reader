package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// walPageSize is the WAL's page size, unexported in wlog. A record's fragments never cross a page boundary, so
// a reader started at one is at the start of a fragment, and the reader checks a page's zero padding against the
// page boundaries it counts from where it started.
const walPageSize = 32 << 10

// walPass is what one decoding pass over the WAL found: the span of its samples, the part of it a replay keeps,
// and the first record it couldn't read, past which a replay keeps nothing.
type walPass struct {
	minValid int64
	// newest is the newest sample in any readable record, when sampled.
	newest  int64
	sampled bool
	// oldestKept and newestKept bound the samples a replay keeps, those from minValid on before the failure, when
	// kept.
	oldestKept, newestKept int64
	kept                   bool
	// failure is the first record that couldn't be read, or nil.
	failure *wlog.CorruptionErr
	// newestAfter is the newest sample in a readable record after the failure, when after.
	newestAfter int64
	after       bool
}

func (p *walPass) add(t int64) {
	if !p.sampled {
		p.newest, p.sampled = t, true
	}
	p.newest = max(p.newest, t)
	switch {
	case p.failure != nil:
		if !p.after {
			p.newestAfter, p.after = t, true
		}
		p.newestAfter = max(p.newestAfter, t)
	case t < p.minValid:
	case !p.kept:
		p.oldestKept, p.newestKept, p.kept = t, t, true
	default:
		p.oldestKept, p.newestKept = min(p.oldestKept, t), max(p.newestKept, t)
	}
}

// fail records a record that couldn't be read, if it's the first.
func (p *walPass) fail(seg segment, offset int64, err error) {
	if p.failure == nil {
		p.failure = &wlog.CorruptionErr{Dir: seg.dir, Segment: seg.index, Offset: offset, Err: err}
	}
}

// segment is one WAL or checkpoint segment file.
type segment struct {
	dir   string
	index int
}

func (s segment) name() string { return wlog.SegmentName(s.dir, s.index) }

// readWAL decodes every record in the segments a replay reads, the WAL's last checkpoint and the segments after
// it, and returns what it found, counting as kept the samples a replay from minValid keeps. It decodes every
// record type a replay decodes, so it fails where a replay fails. Past a failure it reads on from the next record
// it can find, so the samples after it are known. It builds no head, so it holds one record at a time in memory.
// An error opening a segment, rather than reading one, stops Prometheus's own replay too, and is returned.
func readWAL(ctx context.Context, dir string, minValid int64, logger *slog.Logger) (walPass, error) {
	segs, err := replayed(dir)
	if err != nil {
		return walPass{}, err
	}
	p := walPass{minValid: minValid}
	dec := record.NewDecoder(nil, logger)
	for _, seg := range segs {
		if err := p.readSegment(ctx, &dec, seg); err != nil {
			return walPass{}, err
		}
	}
	return p, nil
}

// replayed lists the segments a replay reads, as Head.Init does: the last checkpoint's, then the WAL's from the
// one after it, or from the first index when there is no checkpoint.
func replayed(dir string) ([]segment, error) {
	var segs []segment
	from := 0
	checkpoint, idx, err := wlog.LastCheckpoint(dir)
	switch {
	case err == nil:
		first, last, err := wlog.Segments(checkpoint)
		if err != nil {
			return nil, fmt.Errorf("list checkpoint segments: %w", err)
		}
		for i := first; i <= last; i++ {
			segs = append(segs, segment{checkpoint, i})
		}
		from = idx + 1
	case !errors.Is(err, record.ErrNotFound):
		return nil, fmt.Errorf("find checkpoint: %w", err)
	}
	_, last, err := wlog.Segments(dir)
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}
	for i := from; i <= last; i++ {
		segs = append(segs, segment{dir, i})
	}
	return segs, nil
}

// readSegment decodes one segment. After a failure the pass looks for the next record it can read: a reader
// stops at the end of the fragment it couldn't use, which is where the next fragment starts unless the damage was
// to that fragment's own length, so the pass starts another reader there. A reader started between page
// boundaries would misjudge the page's padding, so it reads only to the page's end, and the pass goes on from the
// next page whatever it found. A record split across that boundary is lost; the records after it are read.
func (p *walPass) readSegment(ctx context.Context, dec *record.Decoder, seg segment) error {
	info, err := os.Stat(seg.name())
	if err != nil {
		return err
	}
	for from := int64(0); from < info.Size(); {
		aligned := from%walPageSize == 0
		to := info.Size()
		if !aligned {
			to = (from/walPageSize + 1) * walPageSize
		}
		stopped, err := p.readRange(ctx, dec, seg, from, to)
		if err != nil {
			return err
		}
		if stopped < 0 || !aligned {
			from = to
			continue
		}
		from = stopped
	}
	return nil
}

// readRange decodes the records in seg from offset from, which is page-aligned or after a failure, until to, and
// returns the offset after the fragment a reader couldn't read, or -1 when it read to the end. A record whose
// fragments read but that doesn't decode is a failure the reader reads past.
func (p *walPass) readRange(ctx context.Context, dec *record.Decoder, seg segment, from, to int64) (_ int64, err error) {
	s, err := wlog.OpenReadSegment(seg.name())
	if err != nil {
		return 0, err
	}
	// The segment's own reader is the one a replay reads with; it pads a segment cut short, as the live one is,
	// out to its page boundary.
	rc, err := wlog.NewSegmentBufReaderWithOffset(int(from), s)
	if err != nil {
		return 0, errors.Join(err, s.Close())
	}
	defer func() { err = errors.Join(err, rc.Close()) }()
	var in io.Reader = rc
	if from%walPageSize != 0 {
		in = io.LimitReader(rc, to-from)
	}
	r := wlog.NewReader(in)
	for r.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := p.decode(dec, r.Record()); err != nil {
			p.fail(seg, from+r.Offset(), err)
		}
	}
	if r.Err() == nil {
		return -1, nil
	}
	cause := r.Err()
	if cerr, ok := errors.AsType[*wlog.CorruptionErr](cause); ok {
		cause = cerr.Err
	}
	// Any damage takes reading at least the header byte to find, so failing before it is reading the file.
	if r.Offset() == 0 {
		return 0, fmt.Errorf("read %s: %w", seg.name(), cause)
	}
	p.fail(seg, from+r.Offset(), cause)
	return from + r.Offset(), nil
}

// decode decodes one record of a type a replay decodes, adding its samples' times to the pass.
func (p *walPass) decode(dec *record.Decoder, rec []byte) error {
	var err error
	switch dec.Type(rec) {
	case record.Series:
		_, err = dec.Series(rec, nil)
	case record.Samples, record.SamplesV2:
		var samples []record.RefSample
		samples, err = dec.Samples(rec, nil)
		for _, s := range samples {
			p.add(s.T)
		}
	case record.HistogramSamples, record.CustomBucketsHistogramSamples, record.HistogramSamplesV2:
		var histograms []record.RefHistogramSample
		histograms, err = dec.HistogramSamples(rec, nil)
		for _, h := range histograms {
			p.add(h.T)
		}
	case record.FloatHistogramSamples, record.CustomBucketsFloatHistogramSamples, record.FloatHistogramSamplesV2:
		var histograms []record.RefFloatHistogramSample
		histograms, err = dec.FloatHistogramSamples(rec, nil)
		for _, h := range histograms {
			p.add(h.T)
		}
	case record.Tombstones:
		_, err = dec.Tombstones(rec, []tombstones.Stone(nil))
	case record.Exemplars:
		_, err = dec.Exemplars(rec, nil)
	case record.Metadata:
		_, err = dec.Metadata(rec, nil)
	}
	if err != nil {
		return fmt.Errorf("decode %s: %w", dec.Type(rec), err)
	}
	return nil
}

// lastSample returns the newest sample time in a block, from its chunks' ranges in the index, and false when
// the block holds no chunks. A block's MaxTime is the end of the range it was cut for, not its last sample.
func lastSample(ctx context.Context, b *tsdb.Block) (_ int64, _ bool, err error) {
	ir, err := b.Index()
	if err != nil {
		return 0, false, err
	}
	defer func() { err = errors.Join(err, ir.Close()) }()
	name, value := index.AllPostingsKey()
	p, err := ir.Postings(ctx, name, value)
	if err != nil {
		return 0, false, err
	}
	var (
		builder labels.ScratchBuilder
		chks    []chunks.Meta
	)
	maxt := int64(math.MinInt64)
	for p.Next() {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		if err := ir.Series(p.At(), &builder, &chks); err != nil {
			return 0, false, err
		}
		for _, c := range chks {
			maxt = max(maxt, c.MaxTime)
		}
	}
	return maxt, maxt != math.MinInt64, p.Err()
}
