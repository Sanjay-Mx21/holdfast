#!/usr/bin/env bash
# Drill RB-4, a manual refund (task 5.7; docs/runbooks/saga.md). It makes,
# from faults alone, the case RB-4 exists for: a buyer charged for a unit
# that does not exist, whose automatic refund failed for good.
#   1. An event with two units; buyer A buys one (the queue stays open).
#   2. Valkey loses that sale (a lost write, as after a failover): inventory
#      offers two units again (InventoryDrift fires after 2 minutes).
#   3. mockpsp delays every webhook by 20 s; buyer B holds both units,
#      books and pays, and as soon as the provider captures B's money it
#      goes down.
#   4. B's capture arrives; PostgreSQL's guard refuses the sale (only one
#      unit is left), so B's booking needs a refund; the provider is down, so the
#      refund is retried for 10 minutes and dead-lettered
#      (MessagesDeadLettered fires).
#   5. The provider comes back, and the operator runs RB-4:
#      holdfastctl refund --booking <B>. Timed until B's booking is REFUNDED.
#   6. RB-2 repairs the drift; every invariant must end at 0.
# About 13 minutes. Run with make drill-rb4 on a stack started with make up.
set -euo pipefail
cd "$(dirname "$0")/.."
source chaos/lib.sh

LOG="$OUT/rb4-$STAMP.log"
TIMINGS="$OUT/rb4-$STAMP-timeline.tsv"
exec > >(tee "$LOG") 2>&1
trap chaos_down EXIT

ctl=$(mktemp -d)/holdfastctl
go build -o "$ctl" ./cmd/holdfastctl
record() { printf '%s\t%s\n' "$1" "$2" | tee -a "$TIMINGS"; }
booking_of() { sql "SELECT status FROM booking.bookings WHERE id = '$1'"; }
# within SECONDS WHAT TEST...: waits for TEST, or fails the drill (D60).
within() {
	local end=$(($(date +%s) + $1)) what=$2
	shift 2
	until "$@"; do
		[ "$(date +%s)" -lt "$end" ] || { echo "FAIL: $what did not happen in time"; exit 1; }
		sleep 0.2
	done
}

chaos_up
EVENT=$(create_event "RB-4 drill $STAMP" 2 50 600)
[ -n "$EVENT" ] || { echo "could not create the event"; exit 1; }
printf 'step\tseconds\n' >"$TIMINGS"

say "1. buyer A buys one of the two units"
go run ./cmd/buyers -event "$EVENT" -buyers 1 -users "rb4-a-$STAMP" | tail -3

say "2. Valkey loses that sale: two units are offered again"
docker compose exec -T valkey valkey-cli SET "inv:{$EVENT}:avail" 2 >/dev/null

say "3. buyer B pays for two; the provider goes down as soon as it has the money"
faults '{"delayRate":1,"delayMin":"20s","delayMax":"20s"}' >/dev/null
captured0=$(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}')
go run ./cmd/buyers -event "$EVENT" -buyers 1 -qty 2 -users "rb4-b-$STAMP" -settle 30s >/tmp/rb4-buyer-b.txt 2>&1 &
b=$!
captured() { [ "$(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}')" -gt "$captured0" ]; }
within 120 "buyer B's payment" captured || { cat /tmp/rb4-buyer-b.txt; exit 1; }
faults '{"outage":true}' >/dev/null
t0=$(now)
record provider_down_after_the_capture 0
wait "$b" || true
B=$(sql "SELECT id FROM booking.bookings WHERE event_id = '$EVENT' AND status <> 'CONFIRMED'")
[ -n "$B" ] || { echo "no second booking"; exit 1; }
echo "buyer B's booking: $B"

say "4. waiting for the guard's refusal, then for the refund to be given up"
refund_required() { [ "$(booking_of "$B")" = REFUND_REQUIRED ]; }
within 120 "the guard's refusal" refund_required
record booking_refund_required "$(since "$t0")"
dead() { [ "$(metric localhost:9094 'holdfast_kafka_consumed_total{result="dead_lettered",topic="holdfast.booking.v1"}')" -gt 0 ]; }
within 900 "the refund's dead-lettering" dead
record refund_dead_lettered "$(since "$t0")"
echo "booking $B: $(booking_of "$B"); intent: $(sql "SELECT status FROM payment.payment_intents WHERE booking_id = '$B'")"

say "5. the provider is back; RB-4"
faults '{}' >/dev/null
s=$(now)
KAFKA_BROKERS=localhost:29092 "$ctl" refund --dsn "$DSN" --booking "$B"
refunded() { [ "$(booking_of "$B")" = REFUNDED ]; }
within 300 "the refund" refunded
record rb4_refund_to_refunded "$(since "$s")"
record money_back_after_the_capture "$(since "$t0")"

say "6. RB-2: inventory back to PostgreSQL's records"
"$ctl" freeze --valkey localhost:6379 --event "$EVENT"
"$ctl" inventory rebuild --valkey localhost:6379 --dsn "$DSN" --event "$EVENT" | grep -E 'available|holds'
"$ctl" unfreeze --valkey localhost:6379 --event "$EVENT"
sleep 20

echo
say "results"
echo "booking $B: $(booking_of "$B"); intent: $(sql "SELECT status FROM payment.payment_intents WHERE booking_id = '$B'")"
echo "invariants: $(violations)"
column -t -s $'\t' "$TIMINGS" | sed 's/^/  /'
if violations | grep -qE '=[1-9]' || [ "$(booking_of "$B")" != REFUNDED ]; then
	echo "RESULT: FAIL"
	exit 1
fi
echo "RESULT: PASS - the refund the provider's outage defeated was made by RB-4; every invariant 0"
