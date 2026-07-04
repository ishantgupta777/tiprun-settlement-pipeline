// Package batch contains the pure accumulation/flush logic for the batch
// publisher, kept free of Kafka types so it is unit-testable in isolation.
//
// A batch flushes when EITHER the size threshold is reached OR MaxWait elapses
// since the first trade entered the (currently open) batch.
package batch

import (
	"time"

	"github.com/tiprun/settlement-pipeline/internal/money"
	"github.com/tiprun/settlement-pipeline/internal/schema"
)

// Config holds the externally-configurable flush thresholds.
type Config struct {
	MaxSize int           // flush when this many trades accumulate
	MaxWait time.Duration // flush this long after the first trade in the batch
}

// Batcher accumulates trades for the current (open) batch.
type Batcher struct {
	cfg     Config
	trades  []schema.Trade
	firstAt time.Time
}

func New(cfg Config) *Batcher {
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = 1
	}
	return &Batcher{cfg: cfg}
}

// Add appends a trade, recording the arrival time of the first trade so the
// time-based flush window is measured from it.
func (b *Batcher) Add(t schema.Trade, now time.Time) {
	if len(b.trades) == 0 {
		b.firstAt = now
	}
	b.trades = append(b.trades, t)
}

// Size returns the number of accumulated trades.
func (b *Batcher) Size() int { return len(b.trades) }

// Empty reports whether the current batch has no trades.
func (b *Batcher) Empty() bool { return len(b.trades) == 0 }

// Deadline returns the time-based flush deadline and whether a batch is open.
func (b *Batcher) Deadline() (time.Time, bool) {
	if b.Empty() {
		return time.Time{}, false
	}
	return b.firstAt.Add(b.cfg.MaxWait), true
}

// FlushReason returns the reason to flush now, or ("", false) if not yet due.
func (b *Batcher) FlushReason(now time.Time) (schema.FlushReason, bool) {
	if b.Empty() {
		return "", false
	}
	if len(b.trades) >= b.cfg.MaxSize {
		return schema.FlushReasonSize, true
	}
	if deadline, ok := b.Deadline(); ok && !now.Before(deadline) {
		return schema.FlushReasonTime, true
	}
	return "", false
}

// Trades returns the accumulated trades (not a copy; caller must not retain
// after Reset).
func (b *Batcher) Trades() []schema.Trade { return b.trades }

// Reset clears the batch for the next accumulation cycle.
func (b *Batcher) Reset() {
	b.trades = nil
	b.firstAt = time.Time{}
}

// Build assembles the immutable batch message. batchID is supplied by the
// caller (derived from the source offset span for best-effort idempotency).
func Build(trades []schema.Trade, reason schema.FlushReason, batchID string, now time.Time) (schema.Batch, error) {
	notionals := make([]string, len(trades))
	for i, t := range trades {
		notionals[i] = t.Notional
	}
	total, err := money.SumNotionals(notionals)
	if err != nil {
		return schema.Batch{}, err
	}
	cp := make([]schema.Trade, len(trades))
	copy(cp, trades)
	return schema.Batch{
		BatchID:       batchID,
		CreatedAtMS:   now.UnixMilli(),
		FlushReason:   reason,
		TradeCount:    len(cp),
		TotalNotional: total,
		Trades:        cp,
		Producer:      "batch-publisher",
	}, nil
}
