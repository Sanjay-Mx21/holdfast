#!/usr/bin/env bash
# Experiment E3, payment chaos (design doc 13.3; task 5.5): BUYERS whole
# purchases through the running stack while mockpsp injects the design's
# fault mix: 20% duplicate webhooks, 10% delayed by 30 to 90 s, 5% lost, 10%
# payment failures and 5% API timeouts. It passes when
#   - I2 (no double charge) stays 0 throughout, sampled every 15 s;
#   - every payment the provider captured reaches HoldFast (lost webhooks
#     recovered by polling and the reconciler);
#   - no captured money is left unresolved (I3), within 15 minutes of the
#     buyers finishing;
#   - every other invariant is 0 at the end.
# Run with make chaos-e3 on a stack started with make up; it restores the
# normal stack afterwards. BUYERS, CONCURRENCY and RATE scale it.
set -euo pipefail
cd "$(dirname "$0")/.."
source chaos/lib.sh

BUYERS=${BUYERS:-5000}
CONCURRENCY=${CONCURRENCY:-300}
RATE=${RATE:-200}
FAULTS='{"duplicateRate":0.2,"delayRate":0.1,"delayMin":"30s","delayMax":"90s","lossRate":0.05,"failureRate":0.1,"timeoutRate":0.05,"timeoutDelay":"10s"}'
LOG="$OUT/e3-$STAMP.log"
REPORT="$OUT/e3-$STAMP-buyers.json"
exec > >(tee "$LOG") 2>&1

trap chaos_down EXIT
chaos_up
# The session budget by Little's Law: the rate times the 10-minute session
# TTL (a smaller one throttles the run; D58).
EVENT=$(create_event "E3 $STAMP" "$BUYERS" "$RATE" $((RATE * 600)))
[ -n "$EVENT" ] || { echo "could not create the event"; exit 1; }
say "E3: $BUYERS buyers, $CONCURRENCY at a time, event $EVENT admitting up to $RATE a second"
echo "faults: $FAULTS"
captured0=$(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}')
dropped0=$(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_webhooks_total{result="dropped",type="payment.captured"}')
faults "$FAULTS" >/dev/null

# The invariants and the admission rate, sampled while the buyers run.
(
	while sleep 15; do
		v=$(violations)
		echo "   auditor: $v admission rate $(curl -s -m 3 localhost:9092/metrics | awk -v e="$EVENT" 'index($0, "holdfast_queue_admission_rate{event=\"" e "\"}") == 1 { print $2 }')/s"
	done
) &
sampler=$!
start=$(now)
set +e
go run ./cmd/buyers -event "$EVENT" -buyers "$BUYERS" -concurrency "$CONCURRENCY" -json "$REPORT"
buyers_status=$?
set -e
kill "$sampler" 2>/dev/null || true
wait "$sampler" 2>/dev/null || true
say "buyers done after $(since "$start") s (exit $buyers_status); faults off, waiting for the stack to settle"
faults '{}' >/dev/null

# Settle: every capture known to HoldFast, nothing captured left unresolved.
captured=$(($(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}') - captured0))
settled=$(settle "$EVENT" "$captured") || settled=""

echo
say "results"
summary "$EVENT" "$captured"
echo "capture webhooks the provider never sent: $(($(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_webhooks_total{result="dropped",type="payment.captured"}') - dropped0))"
pass=1
[ -n "$settled" ] || { echo "FAIL: not settled 15 minutes after the buyers finished"; pass=0; }
[ "$(known "$EVENT")" -ge "$captured" ] || { echo "FAIL: captures the provider made are unknown to HoldFast"; pass=0; }
[ "$(unresolved "$EVENT")" = 0 ] || { echo "FAIL: captured money unresolved"; pass=0; }
if violations | grep -qE '=[1-9]'; then echo "FAIL: an invariant is violated"; pass=0; fi
if grep -qE 'I2=[1-9]' "$LOG"; then echo "FAIL: I2 was violated during the run"; pass=0; fi
if [ "$pass" = 1 ]; then
	echo "RESULT: PASS - settled ${settled} s after the buyers finished; I2 0 throughout; every capture known and resolved"
else
	echo "RESULT: FAIL"
	exit 1
fi
