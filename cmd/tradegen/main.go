// Command tradegen is a test harness that produces synthetic, schema-valid
// trade messages to the `trades` topic at a configurable rate. It lets us run
// the pipeline end-to-end deterministically without depending on the live
// Hyperliquid feed.
package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/tiprun/settlement-pipeline/internal/config"
	"github.com/tiprun/settlement-pipeline/internal/kafka"
	"github.com/tiprun/settlement-pipeline/internal/log"
	"github.com/tiprun/settlement-pipeline/internal/money"
	"github.com/tiprun/settlement-pipeline/internal/run"
	"github.com/tiprun/settlement-pipeline/internal/schema"
)

func main() {
	logger := log.New("tradegen")

	brokers := config.Brokers()
	topic := config.GetString("TRADES_TOPIC", "trades")
	rate := config.GetInt("GEN_RATE", 20)
	count := config.GetInt("GEN_COUNT", 0) // 0 = forever
	coins := config.GetStringSlice("GEN_COINS", []string{"BTC", "ETH"})
	if rate <= 0 {
		rate = 1
	}
	logger.Info("starting", "brokers", brokers, "topic", topic,
		"rate_per_sec", rate, "count", count, "coins", coins)

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		logger.Error("producer init failed", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := run.SignalContext()
	defer cancel()

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var tid int64
	var produced int
	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown complete", "total_produced", produced)
			return
		case <-ticker.C:
			tid++
			tr := synthTrade(rng, coins, tid)
			val, err := tr.Marshal()
			if err != nil {
				logger.Error("marshal failed", "error", err)
				continue
			}
			if err := kafka.ProduceSync(ctx, producer, topic, []byte(tr.Coin), val); err != nil {
				logger.Error("produce failed", "error", err, "trade_id", tr.TradeID)
				continue
			}
			produced++
			logger.Debug("trade produced", "trade_id", tr.TradeID,
				"coin", tr.Coin, "notional", tr.Notional, "total", produced)
			if count > 0 && produced >= count {
				logger.Info("reached count, stopping", "total_produced", produced)
				return
			}
		}
	}
}

func synthTrade(rng *rand.Rand, coins []string, tid int64) schema.Trade {
	coin := coins[rng.Intn(len(coins))]
	now := time.Now().UnixMilli()
	price := fmt.Sprintf("%.2f", 100+rng.Float64()*70000)
	size := fmt.Sprintf("%.5f", 0.0001+rng.Float64()*2)
	notional, _ := money.Notional(price, size)
	side := schema.SideBuy
	if rng.Intn(2) == 0 {
		side = schema.SideSell
	}
	return schema.Trade{
		TradeID:      fmt.Sprintf("%d-%s-%d", now, coin, tid),
		Coin:         coin,
		Side:         side,
		Price:        price,
		Size:         size,
		Notional:     notional,
		EventTimeMS:  now,
		Hash:         fmt.Sprintf("0xsynthetic%012d", tid),
		IngestTimeMS: now,
		Source:       "tradegen",
	}
}
