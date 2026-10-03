#!/usr/bin/env bash
# Experiment E2: the waiting room under a T0 stampede. Run with make load-e2,
# on a stack started with make up. It
#   1. recreates queue-svc and the edge with the per-IP limits off
#      (loadtest/e2/compose.yaml): one load generator is one IP;
#   2. creates an event that opens in a few seconds, admitting ADMIT_RATE
#      people per second with room for every user;
#   3. runs loadtest/e2/waiting-room.js in a k6 container on the edge network
#      and keeps the log, the k6 summary and queue-svc's metrics in
#      loadtest/results/;
#   4. restores queue-svc and the edge, whatever happened.
# USERS, JOIN_PEAK, ADMIT_RATE and POLL_EVERY scale the run (defaults: the
# design's 50,000 users polling every 3 s).
set -euo pipefail
cd "$(dirname "$0")/../.."

USERS=${USERS:-50000}
JOIN_PEAK=${JOIN_PEAK:-2000}
ADMIT_RATE=${ADMIT_RATE:-1000}
POLL_EVERY=${POLL_EVERY:-3}
DSN=${HOLDFAST_TEST_POSTGRES_DSN:-postgres://holdfast:holdfast@localhost:5432/holdfast?sslmode=disable}
STAMP="$(date -u +%Y-%m-%dT%H%M)-$(git rev-parse --short HEAD)"
OUT=loadtest/results
LOG="$OUT/e2-$STAMP.log"
SUMMARY="results/e2-$STAMP-summary.json" # relative to loadtest/, as the k6 container sees it
E2=(docker compose -f compose.yaml -f loadtest/e2/compose.yaml)

restore() {
  echo "== restoring queue-svc and the edge with their normal limits"
  docker compose up -d --wait queue nginx >/dev/null 2>&1 || echo "restore failed: run make up"
}
trap restore EXIT

echo "== switching the per-IP limits off for the run"
"${E2[@]}" up -d --wait queue nginx
docker compose exec -T nginx nginx -T 2>/dev/null | grep -q "x_holdfast_e2_unused" || { echo "the edge did not load the E2 limits"; exit 1; }
for _ in $(seq 1 50); do curl -sf localhost:9092/readyz >/dev/null && break; sleep 0.2; done

OPENS=$(date -u -d '+8 seconds' +%Y-%m-%dT%H:%M:%SZ)
# As many units as users: E2 measures the waiting room, so the admission
# leader's units cap (units left x 1.3, P17) must not be what limits it.
EVENT=$(go run ./cmd/holdfastctl event create --name "E2 $STAMP" --capacity "$USERS" --opens-at "$OPENS" \
  --admission-rate "$ADMIT_RATE" --max-sessions "$USERS" --dsn "$DSN" 2>&1 |
  grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
[ -n "$EVENT" ] || { echo "could not create the event"; exit 1; }
echo "== event $EVENT opens at $OPENS"
while [ "$(date -u +%s)" -lt "$(date -u -d "$OPENS" +%s)" ]; do sleep 0.2; done

{
  echo "E2 $STAMP: USERS=$USERS JOIN_PEAK=$JOIN_PEAK ADMIT_RATE=$ADMIT_RATE POLL_EVERY=$POLL_EVERY event=$EVENT"
  echo "host: $(nproc) CPUs and $(free -g | awk '/Mem:/{print $2}') GB in WSL2, shared by the stack and the load generator (k6 in a container on the edge network)"
} | tee "$LOG"
set +e
"${E2[@]}" --profile e2 run --rm -T k6 run \
  --summary-export "$SUMMARY" --summary-trend-stats "avg,med,p(95),p(99),max" \
  -e BASE=http://nginx -e ADMIN=http://queue:9090 -e EVENT="$EVENT" -e USERS="$USERS" \
  -e JOIN_PEAK="$JOIN_PEAK" -e ADMIT_RATE="$ADMIT_RATE" -e POLL_EVERY="$POLL_EVERY" \
  e2/waiting-room.js 2>&1 | tee -a "$LOG"
status=${PIPESTATUS[0]}
set -e
curl -s localhost:9092/metrics | grep "^holdfast_" > "$OUT/e2-$STAMP-queue-metrics.txt" || true
# Retire the event: off the work list, so queue-svc stops its controller, and
# its queue keys gone (the same as queue.Store.Purge).
docker compose exec -T valkey valkey-cli SREM q:events "$EVENT" >/dev/null
for k in config state members seq admitted status; do docker compose exec -T valkey valkey-cli UNLINK "q:{$EVENT}:$k" >/dev/null; done
for k in epoch sessions; do docker compose exec -T valkey valkey-cli UNLINK "adm:{$EVENT}:$k" >/dev/null; done

python3 - "loadtest/$SUMMARY" <<'PY' | tee -a "$LOG"
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
def count(name):
    v = m.get(name, {})
    return int(v.get("values", v).get("count", 0))
print("\nStatus polls per 10 s window: edge = answered to clients, origin = reached queue-svc")
te = to = 0
w = 0
while f"edge_status_requests{{win:{w}}}" in m:
    e, o = count(f"edge_status_requests{{win:{w}}}"), count(f"origin_status_requests{{win:{w}}}")
    te, to = te + e, to + o
    print(f"  {w*10:3d}-{w*10+10:3d} s  edge {e:9,d}  origin {o:4d}  " + (f"{e // o:,d}:1" if o else "-"))
    w += 1
print(f"  total     edge {te:9,d}  origin {to:4d}  " + (f"{te // to:,d}:1" if to else "-"))
PY
echo "log: $LOG"
echo "summary: loadtest/$SUMMARY"
exit "$status"
