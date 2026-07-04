package dedup

import "testing"

func TestAddDetectsDuplicates(t *testing.T) {
	s := New(3)
	if !s.Add("a") {
		t.Fatal("first add of a should be new")
	}
	if s.Add("a") {
		t.Fatal("second add of a should be duplicate")
	}
	if !s.Add("b") || !s.Add("c") {
		t.Fatal("b,c should be new")
	}
}

func TestEvictionByCapacity(t *testing.T) {
	s := New(2)
	s.Add("a")
	s.Add("b")
	// Adding c evicts a (FIFO).
	s.Add("c")
	if s.Len() != 2 {
		t.Fatalf("len = %d, want 2", s.Len())
	}
	if !s.Add("a") {
		t.Fatal("a was evicted, should be treated as new again")
	}
}
