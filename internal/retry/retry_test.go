package retry

import (
	"math/rand"
	"testing"
	"time"
)

func TestBackoffExponential(t *testing.T) {
	base := 100 * time.Millisecond
	max := 5 * time.Second
	want := []time.Duration{
		100 * time.Millisecond, // attempt 1
		200 * time.Millisecond, // attempt 2
		400 * time.Millisecond, // attempt 3
		800 * time.Millisecond, // attempt 4
	}
	for i, w := range want {
		if got := Backoff(i+1, base, max); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestBackoffCap(t *testing.T) {
	base := 1 * time.Second
	max := 4 * time.Second
	if got := Backoff(10, base, max); got != max {
		t.Errorf("Backoff(10) = %v, want cap %v", got, max)
	}
}

func TestJitterWithinBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	d := time.Second
	for i := 0; i < 1000; i++ {
		j := Jitter(d, rng)
		if j < 0 || j > d {
			t.Fatalf("jitter %v out of [0,%v]", j, d)
		}
	}
}
