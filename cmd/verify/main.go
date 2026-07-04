// Command verify reads settlement_batches and dead_letter from the beginning
// and reports batch/trade counts plus DISTINCT trade_ids. It is used to assert
// the no-data-loss property end-to-end: every trade produced upstream must
// appear in exactly one settled-or-dead-lettered batch (duplicates are allowed
// under at-least-once; losses are not).
//
// It drains each topic until no new records arrive for the idle window, then
// prints a summary and (if EXPECT is set) exits non-zero on a shortfall.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/tiprun/settlement-pipeline/internal/config"
	"github.com/tiprun/settlement-pipeline/internal/log"
	"github.com/tiprun/settlement-pipeline/internal/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := log.New("verify")
	brokers := config.Brokers()
	batchesTopic := config.GetString("BATCHES_TOPIC", "settlement_batches")
	dlqTopic := config.GetString("DEAD_LETTER_TOPIC", "dead_letter")
	idle := config.GetDuration("VERIFY_IDLE", 2*time.Second)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(batchesTopic, dlqTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		logger.Error("client init failed", "error", err)
		os.Exit(1)
	}
	defer cl.Close()

	ctx := context.Background()
	distinct := map[string]struct{}{}
	dlqTradeIDs := map[string]struct{}{}
	var batchCount, dlqCount, sumTradeCount int

	for {
		pollCtx, cancel := context.WithTimeout(ctx, idle)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if fetches.Empty() {
			break // idle window elapsed with no new records => drained
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			switch rec.Topic {
			case batchesTopic:
				b, err := schema.UnmarshalBatch(rec.Value)
				if err != nil {
					logger.Warn("bad batch record", "error", err)
					return
				}
				batchCount++
				sumTradeCount += b.TradeCount
				for _, t := range b.Trades {
					distinct[t.TradeID] = struct{}{}
				}
			case dlqTopic:
				dlqCount++
				var dl schema.DeadLetter
				if err := json.Unmarshal(rec.Value, &dl); err != nil {
					logger.Warn("bad dead-letter record", "error", err)
					return
				}
				for _, t := range dl.OriginalBatch.Trades {
					dlqTradeIDs[t.TradeID] = struct{}{}
					distinct[t.TradeID] = struct{}{} // dead-lettered trades are still accounted for
				}
			}
		})
	}

	logger.Info("verification summary",
		"batches", batchCount,
		"sum_trade_count", sumTradeCount,
		"distinct_trade_ids", len(distinct),
		"dead_letter_records", dlqCount,
		"dead_letter_distinct_trade_ids", len(dlqTradeIDs),
	)

	if exp := os.Getenv("EXPECT"); exp != "" {
		expect, err := strconv.Atoi(exp)
		if err == nil {
			if len(distinct) < expect {
				logger.Error("DATA LOSS DETECTED",
					"expected_min_distinct", expect, "got_distinct", len(distinct))
				fmt.Fprintf(os.Stderr, "FAIL: expected >= %d distinct trades, got %d\n", expect, len(distinct))
				os.Exit(1)
			}
			logger.Info("no data loss", "expected_min_distinct", expect, "got_distinct", len(distinct))
		}
	}
}
