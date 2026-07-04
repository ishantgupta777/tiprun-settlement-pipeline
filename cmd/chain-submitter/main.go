// Command chain-submitter consumes settlement batches, validates each one,
// submits it to the mock chain with a bounded retry strategy, and commits the
// Kafka offset only after the batch is either successfully settled OR safely
// dead-lettered. Batches are NEVER silently dropped.
//
// Delivery guarantee: at-least-once. The offset is committed only after a
// terminal outcome (success or DLQ publish). Failure window: a crash between a
// successful chain submit and the offset commit re-delivers the batch; the mock
// chain's batch_id idempotency returns the same tx hash instead of double
// -submitting. See README for the full trade-off.
package main

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/tiprun/settlement-pipeline/internal/chain"
	"github.com/tiprun/settlement-pipeline/internal/config"
	"github.com/tiprun/settlement-pipeline/internal/kafka"
	"github.com/tiprun/settlement-pipeline/internal/log"
	"github.com/tiprun/settlement-pipeline/internal/retry"
	"github.com/tiprun/settlement-pipeline/internal/run"
	"github.com/tiprun/settlement-pipeline/internal/schema"
	"github.com/tiprun/settlement-pipeline/internal/validate"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := log.New("chain-submitter")

	brokers := config.Brokers()
	group := config.GetString("SUBMITTER_GROUP", "chain-submitter")
	inTopic := config.GetString("BATCHES_TOPIC", "settlement_batches")
	dlqTopic := config.GetString("DEAD_LETTER_TOPIC", "dead_letter")
	maxAttempts := config.GetInt("SUBMIT_MAX_ATTEMPTS", 4)
	baseBackoff := config.GetDuration("SUBMIT_BASE_BACKOFF", 200*time.Millisecond)
	maxBackoff := config.GetDuration("SUBMIT_MAX_BACKOFF", 5*time.Second)
	chainClient := chain.New(
		config.GetDuration("CHAIN_MIN_DELAY", 50*time.Millisecond),
		config.GetDuration("CHAIN_MAX_DELAY", 500*time.Millisecond),
		config.GetFloat("CHAIN_FAILURE_RATE", 0.2),
	)
	logger.Info("starting", "brokers", brokers, "group", group,
		"in_topic", inTopic, "dlq_topic", dlqTopic,
		"max_attempts", maxAttempts, "base_backoff", baseBackoff.String())

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

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var settled, deadLettered uint64

	deadLetter := func(b schema.Batch, reason string, attempts int, lastErr string) bool {
		dl := schema.DeadLetter{
			BatchID: b.BatchID, OriginalBatch: b, FailureReason: reason,
			Attempts: attempts, LastError: lastErr, FailedAtMS: time.Now().UnixMilli(),
		}
		val, err := dl.Marshal()
		if err != nil {
			logger.Error("marshal dead letter failed", "error", err, "batch_id", b.BatchID)
			return false
		}
		if err := kafka.ProduceSyncRetry(ctx, producer, dlqTopic, []byte(b.BatchID), val); err != nil {
			logger.Warn("dead-letter produce interrupted by shutdown", "batch_id", b.BatchID)
			return false
		}
		deadLettered++
		logger.Warn("batch dead-lettered", "batch_id", b.BatchID, "reason", reason,
			"attempts", attempts, "last_error", lastErr, "dead_lettered", deadLettered)
		return true
	}

	for {
		fetches := consumer.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			if ctx.Err() == nil {
				logger.Error("fetch error", "topic", t, "partition", p, "error", err)
			}
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })

		handled := 0
		for _, rec := range records {
			if ctx.Err() != nil {
				break
			}
			b, err := schema.UnmarshalBatch(rec.Value)
			if err != nil {
				// Unparseable batch is non-retryable; dead-letter a stub so nothing
				// is dropped, then commit.
				stub := schema.Batch{BatchID: "unparseable-" + rec.Topic}
				if !deadLetter(stub, "unparseable_batch", 0, err.Error()) {
					break
				}
				handled++
				continue
			}

			// Validation failures are non-retryable -> straight to DLQ.
			if verr := validate.Batch(b); verr != nil {
				if !deadLetter(b, "validation_failed", 0, verr.Error()) {
					break
				}
				handled++
				continue
			}

			// Submit with bounded exponential backoff + full jitter.
			ok, attempts, lastErr := submit(ctx, chainClient, b, maxAttempts, baseBackoff, maxBackoff, rng, logger)
			if ctx.Err() != nil {
				break
			}
			if ok {
				settled++
				handled++
				continue
			}
			if !deadLetter(b, "submit_exhausted", attempts, lastErr) {
				break
			}
			handled++
		}

		if handled > 0 && ctx.Err() == nil {
			if err := consumer.CommitRecords(ctx, records[:handled]...); err != nil {
				logger.Error("commit failed", "error", err)
				continue
			}
		}
	}
	logger.Info("shutdown complete", "settled", settled, "dead_lettered", deadLettered)
}

// submit attempts submission up to maxAttempts times. Returns (success,
// attemptsUsed, lastError).
func submit(
	ctx context.Context, client *chain.Client, b schema.Batch,
	maxAttempts int, base, max time.Duration, rng *rand.Rand, logger *slog.Logger,
) (bool, int, string) {
	var lastErr string
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		tx, err := client.Submit(ctx, b.BatchID)
		if err == nil {
			logger.Info("batch settled",
				"batch_id", b.BatchID, "tx_hash", tx, "attempt", attempt,
				"trade_count", b.TradeCount, "total_notional", b.TotalNotional)
			return true, attempt, ""
		}
		if ctx.Err() != nil {
			return false, attempt, ctx.Err().Error()
		}
		lastErr = err.Error()
		logger.Warn("submit attempt failed", "batch_id", b.BatchID,
			"attempt", attempt, "max_attempts", maxAttempts, "error", lastErr)
		if attempt < maxAttempts {
			delay := retry.Jitter(retry.Backoff(attempt, base, max), rng)
			select {
			case <-ctx.Done():
				return false, attempt, ctx.Err().Error()
			case <-time.After(delay):
			}
		}
	}
	return false, maxAttempts, lastErr
}
