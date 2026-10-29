// Package fixture writes synthetic TSDB dumps for tests with Prometheus's own tsdb package. Every name in them
// is invented.
package fixture

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

// The fixture's samples: two node-exporter targets scraped every Interval for Span, node-a up and node-b down.
const (
	Interval = 15 * time.Second
	Span     = 7 * time.Hour
)

// Start is the fixture's first sample time.
func Start() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// End is the fixture's last sample time.
func End() time.Time { return Start().Add(Span) }

// Up returns each target's value of up, by instance.
func Up() map[string]float64 { return map[string]float64{"node-a": 1, "node-b": 0} }

// Write writes the fixture's TSDB into dir. Compacting it leaves the first four hours in two 2h blocks and the
// last three in the WAL, where the head has filled enough chunks to write chunks_head/ files.
func Write(ctx context.Context, dir string) (err error) {
	opts := tsdb.DefaultOptions()
	opts.MinBlockDuration = (2 * time.Hour).Milliseconds()
	opts.MaxBlockDuration = opts.MinBlockDuration
	db, err := tsdb.Open(dir, nil, nil, opts, nil)
	if err != nil {
		return fmt.Errorf("open fixture TSDB: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	db.DisableCompactions()

	up := Up()
	app := db.Appender(ctx)
	for t := Start(); !t.After(End()); t = t.Add(Interval) {
		for instance, v := range up {
			ls := labels.FromStrings("__name__", "up", "instance", instance, "job", "node-exporter")
			if _, err := app.Append(0, ls, t.UnixMilli(), v); err != nil {
				return errors.Join(fmt.Errorf("append fixture sample: %w", err), app.Rollback())
			}
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
