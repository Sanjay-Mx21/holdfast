#!/usr/bin/env bash
# Drill RB-5, the payment provider down (task 5.7; docs/runbooks/payment.md):
# buyers purchase while mockpsp's outage is switched on for OUTAGE seconds,
# then off. Every second it samples payment-svc's circuit breaker and the
# event's admission rate, and records when the breaker opened, when
# admissions paused, when the breaker closed, when admissions resumed and
# when they were back to the event's rate. Then the usual checks: every
# capture known and resolved, every invariant 0. Run with make drill-rb5 on
# a stack started with make up; it restores the normal stack afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."
source chaos/lib.sh

BUYERS=${BUYERS:-4000}
CONCURRENCY=${CONCURRENCY:-200}
RATE=${RATE:-20}
OUTAGE=${OUTAGE:-60}
LOG="$OUT/rb5-$STAMP.log"
REPORT="$OUT/rb5-$STAMP-buyers.json"
TIMINGS="$OUT/rb5-$STAMP-timeline.tsv"
exec > >(tee "$LOG") 2>&1

buyers=""
cleanup() {
	if [ -n "$buyers" ]; then pkill -INT -P "$buyers" 2>/dev/null || true; kill -INT "$buyers" 2>/dev/null || true; fi
	chaos_down
}
trap cleanup EXIT

breaker() { curl -s -m 2 localhost:9094/metrics | awk '/^holdfast_psp_breaker_state/ { print $2 }'; }
rate() { curl -s -m 2 localhost:9092/metrics | awk -v e="$EVENT" 'index($0, "holdfast_queue_admission_rate{event=\"" e "\"}") == 1 { print $2 }'; }
record() { printf '%s\t%s\n' "$1" "$2" | tee -a "$TIMINGS"; }
# watch_until SECONDS DESCRIPTION TEST: samples every second until TEST
# succeeds, and records the time since t0.
watch_until() {
	local limit=$1 what=$2
	local end=$(($(date +%s) + limit))
	shift 2
	until "$@"; do
		[ "$(date +%s)" -lt "$end" ] || { record "$what" "not within $limit s"; return 1; }
		sleep 1
	done
	record "$what" "$(since "$t0")"
}
is_open() { [ "$(breaker)" = 1 ]; }
is_closed() { [ "$(breaker)" = 0 ]; }
paused() { [ "$(rate)" = 0 ]; }
admitting() { awk -v r="$(rate)" 'BEGIN { exit !(r > 0) }'; }
full_rate() { awk -v r="$(rate)" -v c="$RATE" 'BEGIN { exit !(r >= c) }'; }

chaos_up
EVENT=$(create_event "RB-5 drill $STAMP" "$BUYERS" "$RATE" $((RATE * 600)))
[ -n "$EVENT" ] || { echo "could not create the event"; exit 1; }
say "RB-5 drill: $BUYERS buyers, event $EVENT admitting up to $RATE a second; the provider down for $OUTAGE s"
printf 'step\tseconds_after_the_outage_began\n' >"$TIMINGS"
captured0=$(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}')

go run ./cmd/buyers -event "$EVENT" -buyers "$BUYERS" -concurrency "$CONCURRENCY" -json "$REPORT" &
buyers=$!
sleep 60
say "admission rate before: $(rate)/s; breaker: $(breaker)"

say "the provider goes down"
t0=$(now)
faults '{"outage":true}' >/dev/null
watch_until 60 breaker_open is_open || true
watch_until 30 admissions_paused paused || true
while [ "$(awk -v a="$t0" -v b="$(now)" 'BEGIN { print (b - a < '"$OUTAGE"') }')" = 1 ]; do sleep 1; done

say "the provider is back"
record provider_back "$(since "$t0")"
faults '{}' >/dev/null
watch_until 120 breaker_closed is_closed || true
watch_until 60 admissions_resumed admitting || true
watch_until 120 admissions_at_full_rate full_rate || true

say "waiting for the buyers"
set +e
wait "$buyers"
set -e
buyers=""
captured=$(($(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}') - captured0))
settled=$(settle "$EVENT" "$captured") || settled=""

echo
say "results"
summary "$EVENT" "$captured"
echo "timeline (seconds after the provider went down):"
column -t -s $'\t' "$TIMINGS" | sed 's/^/  /'
pass=1
[ -n "$settled" ] || { echo "FAIL: not settled 15 minutes after the buyers finished"; pass=0; }
if violations | grep -qE '=[1-9]'; then echo "FAIL: an invariant is violated"; pass=0; fi
grep -q 'admissions_paused	[0-9]' "$TIMINGS" || { echo "FAIL: admissions did not pause"; pass=0; }
grep -q 'admissions_resumed	[0-9]' "$TIMINGS" || { echo "FAIL: admissions did not resume"; pass=0; }
if [ "$pass" = 1 ]; then
	echo "RESULT: PASS - admissions paused and resumed by themselves; every capture resolved; every invariant 0"
else
	echo "RESULT: FAIL"
	exit 1
fi
