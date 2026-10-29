package query

import (
	"fmt"
	"time"

	"github.com/prometheus/common/model"
)

// Time is a time flag's value: an RFC 3339 time, or a duration back from the dump's end. The zero Time is the
// dump's end.
type Time struct {
	abs time.Time
	ago time.Duration
}

// ParseTime reads an RFC 3339 time, or a Prometheus duration such as 6h or 1d that counts back from the dump's
// end.
func ParseTime(s string) (Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return Time{abs: t}, nil
	}
	d, err := model.ParseDuration(s)
	if err != nil {
		return Time{}, fmt.Errorf("time %q is neither an RFC 3339 time nor a duration", s)
	}
	return Time{ago: time.Duration(d)}, nil
}

// At returns the time t names, given the dump's end.
func (t Time) At(end time.Time) time.Time {
	if !t.abs.IsZero() {
		return t.abs.UTC()
	}
	return end.Add(-t.ago).UTC()
}
