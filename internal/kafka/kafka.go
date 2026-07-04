// Package kafka wraps franz-go with the small set of behaviors every service
// needs: an idempotent producer (acks=all) and a manual-commit consumer group.
//
// Manual commit is the key design lever: consumers commit offsets only AFTER
// the downstream effect (produce/submit) succeeds, giving at-least-once.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducer returns a franz-go client configured as an idempotent producer.
// Idempotence + RequireAllISRAcks avoids duplicates from producer-side retries
// and guards against acknowledged-but-lost writes.
func NewProducer(brokers []string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent producer is on by default when acks=all; set explicitly.
		kgo.ProducerBatchMaxBytes(16<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("new producer: %w", err)
	}
	return cl, nil
}

// ProduceSync produces a single record synchronously and returns once the
// broker has acknowledged it (or errored). Callers rely on this to decide
// whether it is safe to commit the source offset.
func ProduceSync(ctx context.Context, cl *kgo.Client, topic string, key, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	res := cl.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

// ProduceSyncRetry blocks until the record is durably produced or ctx is
// cancelled. franz-go already retries transient broker errors internally; this
// adds an outer backoff loop so a longer broker outage cannot cause the caller
// to advance its consumer offset past an unproduced record (no data loss).
// It returns a non-nil error only when ctx is cancelled.
func ProduceSyncRetry(ctx context.Context, cl *kgo.Client, topic string, key, value []byte) error {
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second
	for {
		if err := ProduceSync(ctx, cl, topic, key, value); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// NewConsumer returns a consumer-group client with autocommit DISABLED so the
// caller controls exactly when offsets are committed.
//
// A short session timeout (6s, Redpanda's minimum) with a matching heartbeat
// makes the group evict a crashed member quickly, so a restarted component
// reclaims its partitions within seconds instead of the default ~45s. This is
// what makes "restart mid-run and keep making progress" actually fast.
func NewConsumer(brokers []string, group string, topics ...string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		// Start from the earliest offset for a brand-new group so no trades are
		// skipped on first run.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("new consumer: %w", err)
	}
	return cl, nil
}
