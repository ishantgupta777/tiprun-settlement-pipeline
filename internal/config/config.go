// Package config loads configuration from environment variables so that all
// thresholds/timeouts are externalizable without code changes (12-factor).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// GetString returns the env var or a default.
func GetString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// GetStringSlice parses a comma-separated env var into a slice.
func GetStringSlice(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// GetInt parses an int env var or returns the default.
func GetInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return def
	}
	return n
}

// GetFloat parses a float env var or returns the default.
func GetFloat(key string, def float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return def
	}
	return f
}

// GetDuration parses a Go duration string (e.g. "5s") or returns the default.
func GetDuration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return def
	}
	return d
}

// Brokers returns the Kafka/Redpanda broker list.
//
// Default is localhost:19092 — the host-facing ("OUTSIDE") listener published
// by docker-compose. Containers on the compose network override this with
// KAFKA_BROKERS=redpanda:9092.
func Brokers() []string {
	return GetStringSlice("KAFKA_BROKERS", []string{"localhost:19092"})
}

// Describe renders a key/value string for startup logging.
func Describe(kv map[string]any) string {
	var sb strings.Builder
	first := true
	for k, v := range kv {
		if !first {
			sb.WriteString(" ")
		}
		first = false
		sb.WriteString(fmt.Sprintf("%s=%v", k, v))
	}
	return sb.String()
}
