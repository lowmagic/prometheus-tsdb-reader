package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// walSpan is the newest sample time a WAL holds, when sampled, and the oldest a replay keeps, when kept.
type walSpan struct {
	minValid   int64
	newest     int64
	sampled    bool
	oldestKept int64
	kept       bool
}

func (s *walSpan) add(t int64) {
	if !s.sampled {
		s.newest, s.sampled = t, true
	}
	s.newest = max(s.newest, t)
	if t < s.minValid {
		return
	}
	if !s.kept {
		s.oldestKept, s.kept = t, true
	}
	s.oldestKept = min(s.oldestKept, t)
}

// readWAL decodes every record in the WAL's last checkpoint and the segments after it, the ones a replay reads,
// and returns the span of their samples, counting as kept those a replay from minValid keeps. It builds no
// head, so it holds one record at a time in memory.
func readWAL(ctx context.Context, dir string, minValid int64, logger *slog.Logger) (span walSpan, err error) {
	span.minValid = minValid
	ranges := []wlog.SegmentRange{{Dir: dir, First: -1, Last: -1}}
	checkpoint, idx, err := wlog.LastCheckpoint(dir)
	switch {
	case err == nil:
		ranges = []wlog.SegmentRange{{Dir: checkpoint, First: -1, Last: -1}, {Dir: dir, First: idx + 1, Last: -1}}
	case !errors.Is(err, record.ErrNotFound):
		return walSpan{}, fmt.Errorf("find checkpoint: %w", err)
	}
	rc, err := wlog.NewSegmentsRangeReader(ranges...)
	if err != nil {
		return walSpan{}, err
	}
	defer func() { err = errors.Join(err, rc.Close()) }()

	dec := record.NewDecoder(nil, logger)
	var (
		samples    []record.RefSample
		histograms []record.RefHistogramSample
		floatHists []record.RefFloatHistogramSample
	)
	r := wlog.NewReader(rc)
	for r.Next() {
		if err := ctx.Err(); err != nil {
			return walSpan{}, err
		}
		rec := r.Record()
		switch dec.Type(rec) {
		case record.Samples, record.SamplesV2:
			samples, err = dec.Samples(rec, samples[:0])
			for _, s := range samples {
				span.add(s.T)
			}
		case record.HistogramSamples, record.CustomBucketsHistogramSamples, record.HistogramSamplesV2:
			histograms, err = dec.HistogramSamples(rec, histograms[:0])
			for _, h := range histograms {
				span.add(h.T)
			}
		case record.FloatHistogramSamples, record.CustomBucketsFloatHistogramSamples, record.FloatHistogramSamplesV2:
			floatHists, err = dec.FloatHistogramSamples(rec, floatHists[:0])
			for _, h := range floatHists {
				span.add(h.T)
			}
		}
		if err != nil {
			return walSpan{}, fmt.Errorf("decode record: %w", err)
		}
	}
	return span, r.Err()
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
