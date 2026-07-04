// Package dedup provides a bounded, in-memory "seen recently" set used by the
// ingestor to drop duplicate trades produced by at-least-once replay (e.g. a
// feed reconnect resending, or reprocessing of uncommitted offsets).
//
// It is intentionally in-memory and bounded: it is a best-effort optimization,
// NOT the pipeline's correctness guarantee. The authoritative idempotency
// guard for settlement is the batch_id honored by the chain submitter. On
// restart this set starts empty, so duplicates spanning a restart may pass
// through (documented limitation).
package dedup

// Set is a fixed-capacity set with FIFO eviction. Not safe for concurrent use;
// intended to be owned by a single consumer goroutine.
type Set struct {
	capacity int
	seen     map[string]struct{}
	order    []string
	next     int
}

// New returns a Set remembering up to capacity most-recent keys.
func New(capacity int) *Set {
	if capacity <= 0 {
		capacity = 1
	}
	return &Set{
		capacity: capacity,
		seen:     make(map[string]struct{}, capacity),
		order:    make([]string, capacity),
	}
}

// Add records key. It returns true if the key is new, false if it was already
// present (i.e. a duplicate).
func (s *Set) Add(key string) bool {
	if _, ok := s.seen[key]; ok {
		return false
	}
	// Evict the slot we're about to overwrite once the ring is full.
	if old := s.order[s.next]; old != "" {
		delete(s.seen, old)
	}
	s.order[s.next] = key
	s.next = (s.next + 1) % s.capacity
	s.seen[key] = struct{}{}
	return true
}

// Len returns the current number of remembered keys.
func (s *Set) Len() int { return len(s.seen) }
