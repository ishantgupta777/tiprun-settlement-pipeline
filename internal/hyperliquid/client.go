// Package hyperliquid implements a resilient WebSocket client for the
// Hyperliquid public trades feed and transforms raw events into the internal
// schema.Trade contract.
//
// Resilience: the connection is supervised by a reconnect loop with capped
// exponential backoff. A ping is sent on a fixed interval because the server
// closes connections idle for 60s.
package hyperliquid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tiprun/settlement-pipeline/internal/money"
	"github.com/tiprun/settlement-pipeline/internal/schema"
)

// WsTrade mirrors Hyperliquid's trade message shape.
type WsTrade struct {
	Coin  string    `json:"coin"`
	Side  string    `json:"side"` // "B" (bid/buy) or "A" (ask/sell)
	Px    string    `json:"px"`
	Sz    string    `json:"sz"`
	Hash  string    `json:"hash"`
	Time  int64     `json:"time"` // ms
	Tid   int64     `json:"tid"`
	Users [2]string `json:"users"`
}

type wsEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type subscribeMsg struct {
	Method       string            `json:"method"`
	Subscription map[string]string `json:"subscription"`
}

// Config controls the client.
type Config struct {
	URL          string
	Coins        []string
	PingInterval time.Duration
}

// Client is a supervised Hyperliquid trades WebSocket consumer.
type Client struct {
	cfg Config
	log *slog.Logger
}

func New(cfg Config, log *slog.Logger) *Client {
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 20 * time.Second
	}
	return &Client{cfg: cfg, log: log}
}

// TradeHandler is invoked for each normalized trade. Returning an error is
// logged but does not tear down the connection (the feed is not replayable, so
// we favor liveness).
type TradeHandler func(context.Context, schema.Trade) error

// Run supervises the connection until ctx is cancelled. Each disconnect
// triggers a reconnect with capped exponential backoff.
func (c *Client) Run(ctx context.Context, onTrade TradeHandler) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := c.runOnce(ctx, onTrade)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.Warn("websocket disconnected, will reconnect",
			"error", err, "backoff", backoff.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runOnce handles a single connection lifecycle: dial, subscribe, ping, read.
func (c *Client) runOnce(ctx context.Context, onTrade TradeHandler) error {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, c.cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.cfg.URL, err)
	}
	defer conn.Close()
	c.log.Info("websocket connected", "url", c.cfg.URL, "coins", c.cfg.Coins)

	for _, coin := range c.cfg.Coins {
		sub := subscribeMsg{
			Method:       "subscribe",
			Subscription: map[string]string{"type": "trades", "coin": coin},
		}
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe %s: %w", coin, err)
		}
		c.log.Info("subscribed", "coin", coin)
	}

	// Ping loop keeps the connection alive; cancels when the read loop exits.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go c.pingLoop(connCtx, conn)

	for {
		if connCtx.Err() != nil {
			return connCtx.Err()
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := c.handleMessage(connCtx, raw, onTrade); err != nil {
			c.log.Error("handle message failed", "error", err)
		}
	}
}

func (c *Client) pingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteJSON(map[string]string{"method": "ping"}); err != nil {
				c.log.Warn("ping failed", "error", err)
				return
			}
		}
	}
}

func (c *Client) handleMessage(ctx context.Context, raw []byte, onTrade TradeHandler) error {
	var env wsEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("unmarshal envelope: %w", err)
	}
	switch env.Channel {
	case "trades":
		var trades []WsTrade
		if err := json.Unmarshal(env.Data, &trades); err != nil {
			return fmt.Errorf("unmarshal trades: %w", err)
		}
		for _, wt := range trades {
			tr, err := Transform(wt)
			if err != nil {
				c.log.Warn("skip malformed trade", "error", err, "coin", wt.Coin, "tid", wt.Tid)
				continue
			}
			if err := onTrade(ctx, tr); err != nil {
				return err
			}
		}
	case "subscriptionResponse", "pong":
		// expected control messages, ignore
	default:
		c.log.Debug("ignoring channel", "channel", env.Channel)
	}
	return nil
}

// Transform converts a raw WsTrade into the internal schema.Trade. It computes
// notional and derives a globally-unique trade_id ("{time}-{coin}-{tid}") per
// the Hyperliquid docs guidance.
func Transform(wt WsTrade) (schema.Trade, error) {
	if wt.Coin == "" || wt.Px == "" || wt.Sz == "" {
		return schema.Trade{}, errors.New("missing required field")
	}
	var side schema.Side
	switch wt.Side {
	case "B":
		side = schema.SideBuy
	case "A":
		side = schema.SideSell
	default:
		return schema.Trade{}, fmt.Errorf("unknown side %q", wt.Side)
	}
	notional, err := money.Notional(wt.Px, wt.Sz)
	if err != nil {
		return schema.Trade{}, err
	}
	return schema.Trade{
		TradeID:      fmt.Sprintf("%d-%s-%d", wt.Time, wt.Coin, wt.Tid),
		Coin:         wt.Coin,
		Side:         side,
		Price:        wt.Px,
		Size:         wt.Sz,
		Notional:     notional,
		EventTimeMS:  wt.Time,
		Hash:         wt.Hash,
		IngestTimeMS: time.Now().UnixMilli(),
		Source:       schema.SourceHyperliquid,
	}, nil
}
