// Package chain provides a mock on-chain client. It simulates realistic
// behavior: a random submission delay and a configurable random failure rate.
//
// It also enforces idempotency by batch_id: if the same batch is submitted
// again (e.g. after an at-least-once re-delivery), it returns the previously
// assigned transaction hash WITHOUT re-submitting. This is what makes the
// pipeline's duplicate-delivery risk tolerable.
package chain

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math/big"
	mrand "math/rand"
	"sync"
	"time"
)

// ErrSubmit is returned to simulate a transient chain submission failure.
var ErrSubmit = errors.New("chain: transient submission error")

// Client is the mock chain client.
type Client struct {
	minDelay    time.Duration
	maxDelay    time.Duration
	failureRate float64

	mu        sync.Mutex
	rng       *mrand.Rand
	submitted map[string]string // batch_id -> tx hash
}

// New builds a mock client. failureRate is clamped to [0,1].
func New(minDelay, maxDelay time.Duration, failureRate float64) *Client {
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	if failureRate < 0 {
		failureRate = 0
	}
	if failureRate > 1 {
		failureRate = 1
	}
	return &Client{
		minDelay:    minDelay,
		maxDelay:    maxDelay,
		failureRate: failureRate,
		rng:         mrand.New(mrand.NewSource(time.Now().UnixNano())),
		submitted:   make(map[string]string),
	}
}

// Submit simulates submitting a batch on-chain. It blocks for a random delay,
// then either returns a tx hash or ErrSubmit. Idempotent by batchID.
func (c *Client) Submit(ctx context.Context, batchID string) (string, error) {
	// Idempotency: never re-submit a batch we've already settled.
	c.mu.Lock()
	if h, ok := c.submitted[batchID]; ok {
		c.mu.Unlock()
		return h, nil
	}
	delay := c.minDelay
	if span := c.maxDelay - c.minDelay; span > 0 {
		delay += time.Duration(c.rng.Int63n(int64(span)))
	}
	fail := c.rng.Float64() < c.failureRate
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(delay):
	}

	if fail {
		return "", ErrSubmit
	}

	h := randomTxHash()
	c.mu.Lock()
	// Re-check in case a concurrent submit for the same id won the race.
	if existing, ok := c.submitted[batchID]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.submitted[batchID] = h
	c.mu.Unlock()
	return h, nil
}

func randomTxHash() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// Fallback: extremely unlikely; use a big.Int from time.
		return "0x" + big.NewInt(time.Now().UnixNano()).Text(16)
	}
	return "0x" + hex.EncodeToString(b)
}
