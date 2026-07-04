// Command ingestor consumes raw trades from the `trades` topic, validates and
// deduplicates them, and republishes the clean stream to `trades.normalized`.
//
// Delivery: offsets are committed only AFTER the produce to `trades.normalized`
// is acknowledged (commit-after-produce => at-least-once). On restart, any
// records consumed-but-not-committed are reprocessed and re-produced; the
// in-memory dedup set suppresses most in-window duplicates, and the batch
// publisher/chain submitter tolerate the rest.
package main

import (
	"os"

	"github.com/tiprun/settlement-pipeline/internal/config"
	"github.com/tiprun/settlement-pipeline/internal/dedup"
	"github.com/tiprun/settlement-pipeline/internal/kafka"
	"github.com/tiprun/settlement-pipeline/internal/log"
	"github.com/tiprun/settlement-pipeline/internal/run"
	"github.com/tiprun/settlement-pipeline/internal/schema"
	"github.com/tiprun/settlement-pipeline/internal/validate"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := log.New("ingestor")

	brokers := config.Brokers()
	group := config.GetString("INGESTOR_GROUP", "ingestor")
	inTopic := config.GetString("TRADES_TOPIC", "trades")
	outTopic := config.GetString("NORMALIZED_TOPIC", "trades.normalized")
	dedupWindow := config.GetInt("DEDUP_WINDOW", 100000)
	logger.Info("starting", "brokers", brokers, "group", group,
		"in_topic", inTopic, "out_topic", outTopic, "dedup_window", dedupWindow)

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

	seen := dedup.New(dedupWindow)
	var consumed, forwarded, dropped, deduped uint64

	for {
		fetches := consumer.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			logger.Error("fetch error", "topic", t, "partition", p, "error", err)
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })

		// Process in order. A record is "handled" once it is either forwarded,
		// dropped, or deduped. We only commit the contiguous handled prefix so a
		// shutdown mid-batch never advances the offset past an unproduced record.
		handled := 0
		for _, rec := range records {
			consumed++
			tr, err := schema.UnmarshalTrade(rec.Value)
			if err != nil {
				dropped++
				logger.Warn("drop unparseable trade", "error", err, "offset", rec.Offset)
				handled++
				continue
			}
			if err := validate.Trade(tr); err != nil {
				dropped++
				logger.Warn("drop invalid trade", "error", err, "trade_id", tr.TradeID)
				handled++
				continue
			}
			if !seen.Add(tr.TradeID) {
				deduped++
				logger.Debug("drop duplicate trade", "trade_id", tr.TradeID)
				handled++
				continue
			}
			// Blocking retry: returns error only on shutdown, in which case we
			// stop without counting this record as handled.
			if err := kafka.ProduceSyncRetry(ctx, producer, outTopic, []byte(tr.Coin), rec.Value); err != nil {
				logger.Warn("produce interrupted by shutdown", "trade_id", tr.TradeID)
				break
			}
			forwarded++
			handled++
			logger.Debug("trade forwarded", "trade_id", tr.TradeID, "coin", tr.Coin)
		}

		if handled > 0 && ctx.Err() == nil {
			if err := consumer.CommitRecords(ctx, records[:handled]...); err != nil {
				logger.Error("commit failed", "error", err)
				continue
			}
			logger.Info("committed batch", "records", handled,
				"consumed", consumed, "forwarded", forwarded,
				"dropped", dropped, "deduped", deduped)
		}
	}
	logger.Info("shutdown complete", "consumed", consumed, "forwarded", forwarded,
		"dropped", dropped, "deduped", deduped)
}
