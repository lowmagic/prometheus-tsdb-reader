package query

import (
	"fmt"
	"time"

	"github.com/prometheus/common/model"
)

// maxDefaultPoints is the most points per series the default step yields.
const maxDefaultPoints = 500

// Step warning codes.
const (
	StepCoarse = "step-coarse"
	StepFine   = "step-finer-than-data"
)

// defaultStep is the smallest step on the ladder that gives at most maxDefaultPoints points per series, or the
// largest when none does. Its floor is 1m, not the finest scrape interval, so the default range over many
// series stays within the point limit.
func defaultStep(start, end time.Time) time.Duration {
	ladder := []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 6 * time.Hour}
	for _, step := range ladder {
		if int64(end.Sub(start)/step)+1 <= maxDefaultPoints {
			return step
		}
	}
	return ladder[len(ladder)-1]
}

// stepWarnings says when a step is coarser or finer than the samples it's evaluated over.
func stepWarnings(step, interval time.Duration, known bool) []Warning {
	switch {
	case !known || step == interval:
		return nil
	case step > interval:
		return []Warning{{Code: StepCoarse, Message: fmt.Sprintf(
			"the %s step is coarser than the %s between samples: each point shows one sample of about every %d, "+
				"so a peak shorter than %s can be invisible", duration(step), duration(interval), step/interval,
			duration(step))}}
	}
	return []Warning{{Code: StepFine, Message: fmt.Sprintf(
		"the %s step is finer than the %s between samples: the extra points repeat the same samples",
		duration(step), duration(interval))}}
}

func duration(d time.Duration) string { return model.Duration(d).String() }
