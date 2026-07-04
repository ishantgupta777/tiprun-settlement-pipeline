// Package validate holds the pipeline's validation rules. Trade-level rules run
// in the ingestor; batch-level rules run in the chain submitter before
// submission. Rules are documented in the README.
package validate

import (
	"fmt"

	"github.com/tiprun/settlement-pipeline/internal/money"
	"github.com/tiprun/settlement-pipeline/internal/schema"
)

// Trade validates a single trade. Malformed trades are dropped by the ingestor.
func Trade(t schema.Trade) error {
	if t.TradeID == "" {
		return fmt.Errorf("empty trade_id")
	}
	if t.Coin == "" {
		return fmt.Errorf("empty coin")
	}
	if t.Side != schema.SideBuy && t.Side != schema.SideSell {
		return fmt.Errorf("invalid side %q", t.Side)
	}
	if !money.IsPositive(t.Price) {
		return fmt.Errorf("non-positive price %q", t.Price)
	}
	if !money.IsPositive(t.Size) {
		return fmt.Errorf("non-positive size %q", t.Size)
	}
	return nil
}

// Batch validates a settlement batch before chain submission. A failure here is
// non-retryable (the batch is structurally invalid), so the caller dead-letters
// it directly rather than retrying.
func Batch(b schema.Batch) error {
	if b.BatchID == "" {
		return fmt.Errorf("empty batch_id")
	}
	if b.TradeCount <= 0 {
		return fmt.Errorf("non-positive trade_count %d", b.TradeCount)
	}
	if b.TradeCount != len(b.Trades) {
		return fmt.Errorf("trade_count %d != len(trades) %d", b.TradeCount, len(b.Trades))
	}
	seen := make(map[string]struct{}, len(b.Trades))
	notionals := make([]string, 0, len(b.Trades))
	for i, t := range b.Trades {
		if err := Trade(t); err != nil {
			return fmt.Errorf("trade[%d] invalid: %w", i, err)
		}
		if _, dup := seen[t.TradeID]; dup {
			return fmt.Errorf("duplicate trade_id %q in batch", t.TradeID)
		}
		seen[t.TradeID] = struct{}{}
		notionals = append(notionals, t.Notional)
	}
	sum, err := money.SumNotionals(notionals)
	if err != nil {
		return fmt.Errorf("sum notionals: %w", err)
	}
	if !money.Equal(sum, b.TotalNotional) {
		return fmt.Errorf("total_notional %q != recomputed %q", b.TotalNotional, sum)
	}
	return nil
}
