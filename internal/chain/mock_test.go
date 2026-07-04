package chain

import (
	"context"
	"testing"
	"time"
)

func TestSubmitSuccess(t *testing.T) {
	c := New(0, 0, 0.0) // never fails
	tx, err := c.Submit(context.Background(), "batch-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tx) != 66 { // "0x" + 64 hex chars
		t.Fatalf("unexpected tx hash %q (len %d)", tx, len(tx))
	}
}

func TestSubmitAlwaysFails(t *testing.T) {
	c := New(0, 0, 1.0) // always fails
	if _, err := c.Submit(context.Background(), "batch-1"); err != ErrSubmit {
		t.Fatalf("expected ErrSubmit, got %v", err)
	}
}

func TestIdempotency(t *testing.T) {
	c := New(0, 0, 0.0)
	tx1, err := c.Submit(context.Background(), "batch-42")
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := c.Submit(context.Background(), "batch-42")
	if err != nil {
		t.Fatal(err)
	}
	if tx1 != tx2 {
		t.Fatalf("idempotency broken: %q != %q", tx1, tx2)
	}
}

func TestSubmitRespectsContext(t *testing.T) {
	c := New(time.Hour, time.Hour, 0.0) // long delay
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Submit(ctx, "batch-1"); err == nil {
		t.Fatal("expected context error")
	}
}
