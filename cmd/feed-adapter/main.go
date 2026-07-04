// Command feed-adapter connects to the Hyperliquid trades WebSocket, transforms
// each event into the internal schema, and produces it to the `trades` topic.
//
// This is the ingress boundary of the pipeline. The live feed is not
// replayable, so we favor liveness: on a produce failure we rely on the
// idempotent producer's internal retries and log any hard failure.
package main

import (
	"context"
	"os"

	"github.com/tiprun/settlement-pipeline/internal/config"
	"github.com/tiprun/settlement-pipeline/internal/hyperliquid"
	"github.com/tiprun/settlement-pipeline/internal/kafka"
	"github.com/tiprun/settlement-pipeline/internal/log"
	"github.com/tiprun/settlement-pipeline/internal/run"
	"github.com/tiprun/settlement-pipeline/internal/schema"
)

func main() {
	logger := log.New("feed-adapter")

	brokers := config.Brokers()
	topic := config.GetString("TRADES_TOPIC", "trades")
	cfg := hyperliquid.Config{
		URL:          config.GetString("HL_WS_URL", "wss://api.hyperliquid.xyz/ws"),
		Coins:        config.GetStringSlice("HL_COINS", []string{"BTC", "ETH"}),
		PingInterval: config.GetDuration("HL_PING_INTERVAL", 0),
	}
	logger.Info("starting", "brokers", brokers, "topic", topic,
		"ws_url", cfg.URL, "coins", cfg.Coins)

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		logger.Error("producer init failed", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := run.SignalContext()
	defer cancel()

	var produced uint64
	onTrade := func(ctx context.Context, tr schema.Trade) error {
		val, err := tr.Marshal()
		if err != nil {
			logger.Error("marshal trade failed", "error", err, "trade_id", tr.TradeID)
			return nil // drop malformed, keep the feed alive
		}
		if err := kafka.ProduceSync(ctx, producer, topic, []byte(tr.Coin), val); err != nil {
			logger.Error("produce failed", "error", err, "trade_id", tr.TradeID)
			return nil // producer retried internally; do not tear down the feed
		}
		produced++
		logger.Debug("trade produced", "trade_id", tr.TradeID, "coin", tr.Coin,
			"notional", tr.Notional, "total_produced", produced)
		return nil
	}

	client := hyperliquid.New(cfg, logger)
	if err := client.Run(ctx, onTrade); err != nil && ctx.Err() == nil {
		logger.Error("feed adapter stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete", "total_produced", produced)
}
