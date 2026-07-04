// Command batch-publisher consumes normalized trades, accumulates them, and
// publishes each batch as a SINGLE atomic Kafka message to `settlement_batches`
// when a size or time threshold is met.
//
// Delivery: consumed offsets are committed only AFTER the batch message is
// acknowledged (commit-after-produce => at-least-once). If the process crashes
// with an open, uncommitted batch, those normalized records are re-delivered on
// restart and re-batched, so no trade is lost.
//
// Idempotency aid: the batch_id is derived from the source offset span
// ("batch-p{part}-{min}-{max}"), so a batch re-created from the same offsets
// gets the same id, letting the chain submitter dedupe. This is best-effort:
// time-based flush boundaries can shift after a restart (documented).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/tiprun/settlement-pipeline/internal/batch"
	"github.com/tiprun/settlement-pipeline/internal/config"
	"github.com/tiprun/settlement-pipeline/internal/kafka"
	"github.com/tiprun/settlement-pipeline/internal/log"
	"github.com/tiprun/settlement-pipeline/internal/run"
	"github.com/tiprun/settlement-pipeline/internal/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := log.New("batch-publisher")

	brokers := config.Brokers()
	group := config.GetString("PUBLISHER_GROUP", "batch-publisher")
	inTopic := config.GetString("NORMALIZED_TOPIC", "trades.normalized")
	outTopic := config.GetString("BATCHES_TOPIC", "settlement_batches")
	cfg := batch.Config{
		MaxSize: config.GetInt("BATCH_MAX_SIZE", 50),
		MaxWait: config.GetDuration("BATCH_MAX_WAIT", 10*time.Second),
	}
	logger.Info("starting", "brokers", brokers, "group", group,
		"in_topic", inTopic, "out_topic", outTopic,
		"batch_max_size", cfg.MaxSize, "batch_max_wait", cfg.MaxWait.String())

	consumer, err := kafka.NewConsumer(brokers, group, inTopic)
	if err != nil {
		logger.Error("consumer init failed", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		logger.Error("producer init failed", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := run.SignalContext()
	defer cancel()

	batcher := batch.New(cfg)
	var batchRecs []*kgo.Record
	var batchesOut, tradesOut uint64

	// flush produces the current batch (if any), commits its source offsets, and
	// resets. Returns false only if interrupted by shutdown before commit.
	flush := func(reason schema.FlushReason) bool {
		if len(batchRecs) == 0 {
			return true
		}
		if batcher.Empty() {
			// Only unparseable records accumulated; nothing to settle but we must
			// still commit so we don't reprocess them forever.
			if err := consumer.CommitRecords(ctx, batchRecs...); err != nil {
				logger.Error("commit (no-trade) failed", "error", err)
				return false
			}
			batchRecs = nil
			return true
		}
		batchID := deterministicID(batchRecs)
		b, err := batch.Build(batcher.Trades(), reason, batchID, time.Now())
		if err != nil {
			logger.Error("build batch failed", "error", err, "batch_id", batchID)
			return false
		}
		val, err := b.Marshal()
		if err != nil {
			logger.Error("marshal batch failed", "error", err, "batch_id", batchID)
			return false
		}
		if err := kafka.ProduceSyncRetry(ctx, producer, outTopic, []byte(batchID), val); err != nil {
			logger.Warn("flush interrupted by shutdown", "batch_id", batchID)
			return false
		}
		if err := consumer.CommitRecords(ctx, batchRecs...); err != nil {
			logger.Error("commit after produce failed", "error", err, "batch_id", batchID)
			return false
		}
		batchesOut++
		tradesOut += uint64(b.TradeCount)
		logger.Info("batch flushed",
			"batch_id", batchID, "reason", string(reason),
			"trade_count", b.TradeCount, "total_notional", b.TotalNotional,
			"batches_out", batchesOut, "trades_out", tradesOut)
		batcher.Reset()
		batchRecs = nil
		return true
	}

	const pollCap = time.Second
	for {
		if ctx.Err() != nil {
			break
		}

		// Bound the poll so we wake to honor the time-based flush deadline.
		wait := pollCap
		if deadline, open := batcher.Deadline(); open {
			if d := time.Until(deadline); d < wait {
				wait = d
			}
		}
		if wait <= 0 {
			flush(schema.FlushReasonTime)
			continue
		}

		pollCtx, pollCancel := context.WithTimeout(ctx, wait)
		fetches := consumer.PollFetches(pollCtx)
		pollCancel()

		if ctx.Err() != nil {
			break // parent cancelled => shutting down
		}
		fetches.EachError(func(t string, p int32, err error) {
			// A deadline error is our own bounded poll timeout (used to wake for
			// time-based flushes), not a real fault; ignore it.
			if ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
				logger.Error("fetch error", "topic", t, "partition", p, "error", err)
			}
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })

		// Add records one at a time so the size threshold flushes precisely at
		// MaxSize even when a single poll returns many records.
		for _, rec := range records {
			batchRecs = append(batchRecs, rec)
			tr, err := schema.UnmarshalTrade(rec.Value)
			if err != nil {
				logger.Warn("skip unparseable normalized record", "error", err, "offset", rec.Offset)
				continue
			}
			batcher.Add(tr, time.Now())
			if reason, ok := batcher.FlushReason(time.Now()); ok {
				if !flush(reason) {
					break
				}
			}
		}

		// Time-based flush for a partially-filled batch whose window elapsed.
		if reason, ok := batcher.FlushReason(time.Now()); ok {
			flush(reason)
		}
	}

	// Best-effort final flush of an open batch on graceful shutdown. Uses a
	// short fresh context because the parent is already cancelled.
	if !batcher.Empty() {
		fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if len(batchRecs) > 0 {
			batchID := deterministicID(batchRecs)
			if b, err := batch.Build(batcher.Trades(), schema.FlushReasonTime, batchID, time.Now()); err == nil {
				if val, err := b.Marshal(); err == nil {
					if err := kafka.ProduceSync(fctx, producer, outTopic, []byte(batchID), val); err == nil {
						_ = consumer.CommitRecords(fctx, batchRecs...)
						batchesOut++
						logger.Info("final batch flushed on shutdown", "batch_id", batchID, "trade_count", b.TradeCount)
					}
				}
			}
		}
		fcancel()
	}
	logger.Info("shutdown complete", "batches_out", batchesOut, "trades_out", tradesOut)
}

// deterministicID builds a stable id from the source offset span so a batch
// re-created from the same records after a restart gets the same id.
func deterministicID(recs []*kgo.Record) string {
	type span struct{ min, max int64 }
	byPart := map[int32]*span{}
	for _, r := range recs {
		s, ok := byPart[r.Partition]
		if !ok {
			byPart[r.Partition] = &span{min: r.Offset, max: r.Offset}
			continue
		}
		if r.Offset < s.min {
			s.min = r.Offset
		}
		if r.Offset > s.max {
			s.max = r.Offset
		}
	}
	parts := make([]int, 0, len(byPart))
	for p := range byPart {
		parts = append(parts, int(p))
	}
	sort.Ints(parts)
	var sb strings.Builder
	sb.WriteString("batch")
	for _, p := range parts {
		s := byPart[int32(p)]
		sb.WriteString(fmt.Sprintf("-p%d_%d_%d", p, s.min, s.max))
	}
	return sb.String()
}
