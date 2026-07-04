#!/usr/bin/env bash
# Demonstrates the retry + dead-letter path. Runs the chain-submitter over the
# batches currently in settlement_batches with a 100% mock-chain failure rate,
# so every batch exhausts its retries and is dead-lettered (never dropped).
#
# Run scripts/e2e.sh first to populate settlement_batches.
# Usage: ./scripts/dlq-demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

LOGDIR="${LOGDIR:-/tmp/tiprun}"
mkdir -p "$LOGDIR"
go build -o bin/chain-submitter ./cmd/chain-submitter
go build -o bin/verify ./cmd/verify

GROUP="dlq-demo-$(date +%s)"
echo "== running chain-submitter (fresh group=$GROUP, failure_rate=1.0, max_attempts=3) =="
CHAIN_FAILURE_RATE=1.0 SUBMIT_MAX_ATTEMPTS=3 SUBMIT_BASE_BACKOFF=100ms \
  SUBMITTER_GROUP="$GROUP" ./bin/chain-submitter >"$LOGDIR/dlq.log" 2>&1 &
CS_PID=$!
sleep 8
kill "$CS_PID" >/dev/null 2>&1 || true
sleep 1

echo "== retry attempts (sample) =="
grep -E '"msg":"submit attempt failed"' "$LOGDIR/dlq.log" | head -6
echo "== dead-letter results =="
grep -E '"msg":"batch dead-lettered"' "$LOGDIR/dlq.log"
echo "== dead_letter topic offset =="
docker exec redpanda rpk topic describe dead_letter -p 2>/dev/null | awk 'NR==2{print "high-watermark="$6}'
echo "== verify (all dead-lettered trades still accounted for) =="
./bin/verify
