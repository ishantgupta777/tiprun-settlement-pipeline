#!/usr/bin/env bash
# Creates the four Kafka topics the pipeline needs. Idempotent: re-running is
# safe (rpk returns "TOPIC_ALREADY_EXISTS" which we tolerate).
#
# Usage:
#   ./scripts/create-topics.sh            # runs against the `redpanda` container
#   RPK="rpk" ./scripts/create-topics.sh  # run against a local rpk / cluster
set -euo pipefail

# By default exec rpk inside the redpanda container. Override RPK to point at a
# local rpk binary talking to KAFKA_BROKERS.
CONTAINER="${REDPANDA_CONTAINER:-redpanda}"
RPK="${RPK:-docker exec ${CONTAINER} rpk}"

# Single partition keeps per-coin ordering trivial for the demo. Bump
# TRADES_PARTITIONS if you want to demonstrate parallel consumers.
TRADES_PARTITIONS="${TRADES_PARTITIONS:-1}"

topics=(
  "trades:${TRADES_PARTITIONS}"
  "trades.normalized:${TRADES_PARTITIONS}"
  "settlement_batches:1"
  "dead_letter:1"
)

echo "Creating topics via: ${RPK}"
for entry in "${topics[@]}"; do
  name="${entry%%:*}"
  parts="${entry##*:}"
  echo "  -> ${name} (partitions=${parts})"
  ${RPK} topic create "${name}" -p "${parts}" 2>&1 | sed 's/^/     /' || true
done

echo "Current topics:"
${RPK} topic list
