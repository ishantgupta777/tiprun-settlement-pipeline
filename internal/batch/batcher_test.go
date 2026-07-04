package batch

import (
	"testing"
	"time"

	"github.com/tiprun/settlement-pipeline/internal/schema"
)

func tr(id string) schema.Trade {
	return schema.Trade{TradeID: id, Coin: "BTC", Side: schema.SideBuy, Price: "10", Size: "2", Notional: "20"}
}

func TestFlushOnSize(t *testing.T) {
	b := New(Config{MaxSize: 3, MaxWait: time.Hour})
	now := time.Now()
	b.Add(tr("1"), now)
	b.Add(tr("2"), now)
	if _, ok := b.FlushReason(now); ok {
		t.Fatal("should not flush before size reached")
	}
	b.Add(tr("3"), now)
	reason, ok := b.FlushReason(now)
	if !ok || reason != schema.FlushReasonSize {
		t.Fatalf("expected size flush, got %q ok=%v", reason, ok)
	}
}

func TestFlushOnTime(t *testing.T) {
	b := New(Config{MaxSize: 100, MaxWait: 5 * time.Second})
	start := time.Now()
	b.Add(tr("1"), start)
	if _, ok := b.FlushReason(start.Add(4 * time.Second)); ok {
		t.Fatal("should not flush before window elapses")
	}
	reason, ok := b.FlushReason(start.Add(5 * time.Second))
	if !ok || reason != schema.FlushReasonTime {
		t.Fatalf("expected time flush, got %q ok=%v", reason, ok)
	}
}

func TestEmptyNeverFlushes(t *testing.T) {
	b := New(Config{MaxSize: 1, MaxWait: time.Second})
	if _, ok := b.FlushReason(time.Now().Add(time.Hour)); ok {
		t.Fatal("empty batch should never flush")
	}
}

func TestBuildComputesTotalAndCount(t *testing.T) {
	trades := []schema.Trade{tr("1"), tr("2")}
	b, err := Build(trades, schema.FlushReasonSize, "batch-0-0-1", time.Now())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if b.TradeCount != 2 || b.TotalNotional != "40" || b.BatchID != "batch-0-0-1" {
		t.Fatalf("unexpected batch: %+v", b)
	}
	// Build must copy trades so later mutation of the source doesn't leak in.
	trades[0].TradeID = "mutated"
	if b.Trades[0].TradeID == "mutated" {
		t.Fatal("Build did not copy trades")
	}
}
