// Package schema defines the internal message contracts exchanged over Kafka.
//
// Prices and sizes are carried as decimal strings (not floats) to avoid
// binary floating-point rounding drift across the pipeline. Numeric checks
// are performed with math/big where exactness matters (see chain validation).
package schema

import (
	"encoding/json"
	"fmt"
)

// Source identifies the origin of a trade event.
const SourceHyperliquid = "hyperliquid"

// Side is the taker side of a trade, normalized from Hyperliquid's "B"/"A".
type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

// Trade is the internal representation of a single trade event. It flows on
// the `trades` topic (produced by the feed adapter) and, post-validation, on
// the `trades.normalized` topic (produced by the ingestor).
type Trade struct {
	// TradeID is globally unique: "{event_time_ms}-{coin}-{tid}".
	TradeID      string `json:"trade_id"`
	Coin         string `json:"coin"`
	Side         Side   `json:"side"`
	Price        string `json:"price"`          // decimal string
	Size         string `json:"size"`           // decimal string
	Notional     string `json:"notional"`       // decimal string, price*size
	EventTimeMS  int64  `json:"event_time_ms"`  // exchange event time (ms)
	Hash         string `json:"hash"`           // Hyperliquid tx hash (zero-hash => TWAP fill)
	IngestTimeMS int64  `json:"ingest_time_ms"` // adapter receive time (ms)
	Source       string `json:"source"`
}

// FlushReason explains why a batch was flushed.
type FlushReason string

const (
	FlushReasonSize FlushReason = "size"
	FlushReasonTime FlushReason = "time"
)

// Batch is a group of trades published atomically as a single message on the
// `settlement_batches` topic.
type Batch struct {
	BatchID       string      `json:"batch_id"`
	CreatedAtMS   int64       `json:"created_at_ms"`
	FlushReason   FlushReason `json:"flush_reason"`
	TradeCount    int         `json:"trade_count"`
	TotalNotional string      `json:"total_notional"` // decimal string
	Trades        []Trade     `json:"trades"`
	Producer      string      `json:"producer"`
}

// DeadLetter wraps a batch that could not be settled, for the `dead_letter`
// topic. The original batch is preserved verbatim so nothing is ever lost.
type DeadLetter struct {
	BatchID       string `json:"batch_id"`
	OriginalBatch Batch  `json:"original_batch"`
	FailureReason string `json:"failure_reason"`
	Attempts      int    `json:"attempts"`
	LastError     string `json:"last_error"`
	FailedAtMS    int64  `json:"failed_at_ms"`
}

// Marshal helpers keep JSON handling consistent and centralize error wrapping.

func (t *Trade) Marshal() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("marshal trade: %w", err)
	}
	return b, nil
}

func UnmarshalTrade(b []byte) (Trade, error) {
	var t Trade
	if err := json.Unmarshal(b, &t); err != nil {
		return Trade{}, fmt.Errorf("unmarshal trade: %w", err)
	}
	return t, nil
}

func (b *Batch) Marshal() ([]byte, error) {
	out, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("marshal batch: %w", err)
	}
	return out, nil
}

func UnmarshalBatch(b []byte) (Batch, error) {
	var out Batch
	if err := json.Unmarshal(b, &out); err != nil {
		return Batch{}, fmt.Errorf("unmarshal batch: %w", err)
	}
	return out, nil
}

func (d *DeadLetter) Marshal() ([]byte, error) {
	out, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("marshal dead letter: %w", err)
	}
	return out, nil
}
