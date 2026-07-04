#!/usr/bin/env bash
# End-to-end pipeline test with a mid-flight restart drill.
#
# Flow: reset topics -> start ingestor + batch-publisher + chain-submitter ->
# produce N synthetic trades -> KILL and RESTART the batch-publisher mid-run ->
# drain -> verify no data loss (distinct settled/dead-lettered trades >= N).
#
# Usage: N=200 ./scripts/e2e.sh
set -euo pipefail
cd "$(dirname "$0")/.."

N="${N:-200}"
RATE="${RATE:-40}"
LOGDIR="${LOGDIR:-/tmp/tiprun}"
mkdir -p "$LOGDIR"

# Pre-flight: this script runs its own pipeline instances and counts trades
# end-to-end. If other pipeline processes are already running (e.g. the demo
# services), they would share consumer groups / pollute the `trades` topic and
# make the count meaningless. Refuse to run instead of reporting a false result.
if ps -A -o command 2>/dev/null | grep -E "(bin|cmd)/(feed-adapter|ingestor|batch-publisher|chain-submitter|tradegen)" | grep -qv grep; then
  echo "ERROR: pipeline processes are already running:" >&2
  ps -A -o command | grep -E "(bin|cmd)/(feed-adapter|ingestor|batch-publisher|chain-submitter|tradegen)" | grep -v grep >&2
  echo "" >&2
  echo "Stop them first (Ctrl-C those terminals). This script starts its own" >&2
  echo "isolated instances and needs the trades topic to itself." >&2
  exit 1
fi

# Isolated, per-run consumer groups so a rerun (or a leftover process) can never
# collide with this script's own services.
RUN_ID="$(date +%s)"
ING_GROUP="e2e-ingestor-$RUN_ID"
PUB_GROUP="e2e-batch-publisher-$RUN_ID"
SUB_GROUP="e2e-chain-submitter-$RUN_ID"

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

echo "== 1. reset topics (docker compose down -v && up) =="
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d redpanda topic-init >/dev/null
# Wait for topic-init one-shot to finish creating topics.
for i in $(seq 1 30); do
  if docker exec redpanda rpk topic list 2>/dev/null | grep -q dead_letter; then break; fi
  sleep 1
done
docker exec redpanda rpk topic list

echo "== 2. build binaries =="
go build -o bin/ingestor ./cmd/ingestor
go build -o bin/batch-publisher ./cmd/batch-publisher
go build -o bin/chain-submitter ./cmd/chain-submitter
go build -o bin/tradegen ./cmd/tradegen
go build -o bin/verify ./cmd/verify

echo "== 3. start ingestor, batch-publisher, chain-submitter (groups tagged $RUN_ID) =="
INGESTOR_GROUP="$ING_GROUP" ./bin/ingestor >"$LOGDIR/ingestor.log" 2>&1 &
pids+=($!)
PUBLISHER_GROUP="$PUB_GROUP" BATCH_MAX_SIZE="${BATCH_MAX_SIZE:-25}" BATCH_MAX_WAIT="${BATCH_MAX_WAIT:-2s}" \
  ./bin/batch-publisher >"$LOGDIR/bp.log" 2>&1 &
BP_PID=$!; pids+=("$BP_PID")
SUBMITTER_GROUP="$SUB_GROUP" CHAIN_FAILURE_RATE="${CHAIN_FAILURE_RATE:-0.2}" SUBMIT_MAX_ATTEMPTS="${SUBMIT_MAX_ATTEMPTS:-5}" \
  ./bin/chain-submitter >"$LOGDIR/cs.log" 2>&1 &
pids+=($!)
sleep 1

echo "== 4. produce N=$N trades (rate=$RATE/s) in background =="
GEN_COUNT="$N" GEN_RATE="$RATE" ./bin/tradegen >"$LOGDIR/gen.log" 2>&1 &
GEN_PID=$!; pids+=("$GEN_PID")

echo "== 5. RESTART DRILL: kill batch-publisher mid-run, restart it =="
sleep 2
echo "   killing batch-publisher (pid $BP_PID) ..."
kill -9 "$BP_PID" >/dev/null 2>&1 || true
sleep 1
echo "   restarting batch-publisher (same group, resumes from committed offset) ..."
PUBLISHER_GROUP="$PUB_GROUP" BATCH_MAX_SIZE="${BATCH_MAX_SIZE:-25}" BATCH_MAX_WAIT="${BATCH_MAX_WAIT:-2s}" \
  ./bin/batch-publisher >>"$LOGDIR/bp.log" 2>&1 &
BP_PID=$!; pids+=("$BP_PID")

echo "== 6. wait for producer to finish, then drain =="
wait "$GEN_PID" 2>/dev/null || true
# Drain must exceed the 6s consumer-group session timeout so the restarted
# batch-publisher has time to be assigned partitions and catch up.
sleep 15

echo "== 7. topic offsets (message counts) =="
for t in trades trades.normalized settlement_batches dead_letter; do
  hwm=$(docker exec redpanda rpk topic describe "$t" -p 2>/dev/null | awk 'NR==2{print $6}')
  printf "   %-20s high-watermark=%s\n" "$t" "$hwm"
done

echo "== 8. verify no data loss (EXPECT=$N distinct trades) =="
EXPECT="$N" ./bin/verify

echo "== 9. component summaries =="
echo "--- ingestor ---";        grep -E '"msg":"committed batch"' "$LOGDIR/ingestor.log" | tail -1 || true
echo "--- batch-publisher ---"; grep -E '"msg":"batch flushed"' "$LOGDIR/bp.log" | wc -l | xargs echo "batches flushed:"
echo "--- chain-submitter ---"; grep -cE '"msg":"batch settled"' "$LOGDIR/cs.log" | xargs echo "settled:"; \
                                grep -cE '"msg":"batch dead-lettered"' "$LOGDIR/cs.log" | xargs echo "dead-lettered:"

echo "== done. logs in $LOGDIR/ =="
