package report

import (
	"math"
	"sort"
	"time"
)

// Percentile is the p-quantile (0..1) of an ascending-sorted sample by linear
// interpolation between closest ranks: the same definition as PostgreSQL's
// percentile_cont, so the in-memory and SQL stores agree. It is 0 for an
// empty sample.
func Percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := p * float64(n-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

// TimingOf summarises durations in seconds (any order) into the
// mean/median/95th-percentile triple the mean-time-to-resolve report serves.
// Every field is nil for an empty sample.
func TimingOf(seconds []float64) (mean, p50, p95 *float64) {
	if len(seconds) == 0 {
		return nil, nil, nil
	}
	s := append([]float64(nil), seconds...)
	sort.Float64s(s)
	sum := 0.0
	for _, v := range s {
		sum += v
	}
	m, f, p := sum/float64(len(s)), Percentile(s, 0.50), Percentile(s, 0.95)
	return &m, &f, &p
}

// Seconds is d in seconds as a float.
func Seconds(d time.Duration) float64 { return d.Seconds() }

// Share is part over total as a fraction (0 when total is 0): the pure
// definition behind every rate and share the reports serve, mirrored by the
// SQL stores computing counts only.
func Share(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total)
}
