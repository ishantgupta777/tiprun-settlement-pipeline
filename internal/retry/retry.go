// Package retry provides exponential backoff with full jitter for the chain
// submitter's retry strategy.
package retry

import (
	"math/rand"
	"time"
)

// Backoff returns the base (un-jittered) delay for a 1-indexed attempt:
// base * 2^(attempt-1), capped at max. Kept pure/deterministic for testing.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		d = max
	}
	return d
}

// Jitter applies full jitter: a uniform random duration in [0, d]. Spreads out
// concurrent retries to avoid thundering herds.
func Jitter(d time.Duration, rng *rand.Rand) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rng.Int63n(int64(d) + 1))
}
