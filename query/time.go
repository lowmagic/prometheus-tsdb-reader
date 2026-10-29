package query

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
)

// defaultRange is how far back from its end a range starts when --start is unset.
const defaultRange = 2 * time.Hour

// ErrStartAfterEnd is a range whose start is after its end.
var ErrStartAfterEnd = errors.New("start is after end")

// Time is a time flag's value: an RFC 3339 time, or a duration back from the dump's end. The zero Time is
// unset, which is the dump's end for --time and --end.
type Time struct {
	abs time.Time
	ago time.Duration
	set bool
}

// ParseTime reads an RFC 3339 time, or a Prometheus duration such as 6h or 1d that counts back from the dump's
// end.
func ParseTime(s string) (Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return Time{abs: t, set: true}, nil
	}
	d, err := model.ParseDuration(s)
	if err != nil {
		return Time{}, fmt.Errorf("time %q is neither an RFC 3339 time nor a duration", s)
	}
	return Time{ago: time.Duration(d), set: true}, nil
}

// At returns the time t names, given the dump's end.
func (t Time) At(end time.Time) time.Time {
	if !t.abs.IsZero() {
		return t.abs.UTC()
	}
	return end.Add(-t.ago).UTC()
}

// bounds resolves a range query's start and end against the dump: the end defaults to the dump's end and the
// start to defaultRange before the end.
func bounds(d Dump, start, end Time) (time.Time, time.Time, error) {
	e := end.At(d.End)
	return checked(e.Add(-defaultRange), start, e, d)
}

// listingBounds resolves a listing's start and end against the dump: they default to the whole dump, as the
// HTTP API's series and label endpoints do.
func listingBounds(d Dump, start, end Time) (time.Time, time.Time, error) {
	return checked(d.Start.UTC(), start, end.At(d.End), d)
}

// checked returns start, or def when start is unset, with end, and refuses a start after the end.
func checked(def time.Time, start Time, e time.Time, d Dump) (time.Time, time.Time, error) {
	s := def
	if start.set {
		s = start.At(d.End)
	}
	if s.After(e) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: %s after %s", ErrStartAfterEnd,
			s.Format(time.RFC3339), e.Format(time.RFC3339))
	}
	return s, e, nil
}
