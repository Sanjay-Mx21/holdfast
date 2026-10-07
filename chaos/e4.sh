#!/usr/bin/env bash
# Experiment E4, infrastructure chaos (design doc 13.3; task 5.5): buyers
# purchase continuously while, one after another,
#   1. the Valkey primary is killed (SIGKILL): Sentinel promotes the
#      replica, the killed node rejoins as a replica, and runbook RB-2
#      (freeze, rebuild inventory from PostgreSQL, unfreeze) is practised and
#      timed, since a failover can lose writes;
#   2. booking-svc is killed mid-saga and started again 10 s later;
#   3. Kafka is restarted;
#   4. PostgreSQL is cut off for 5 s from booking-svc, payment-svc and
#      queue-svc (Toxiproxy).
# It records when each struck and how long recovery took, then waits for the
# stack to settle. It passes when every invariant is 0 and every payment the
# provider captured is known to HoldFast and resolved. Run with
# make chaos-e4 on a stack started with make up; it restores the normal
# stack (and the Valkey primary) afterwards. BUYERS, CONCURRENCY and RATE
# scale it; the faults are GAP seconds apart.
set -euo pipefail
cd "$(dirname "$0")/.."
source chaos/lib.sh

BUYERS=${BUYERS:-12000} # at 15 a second, a sale long enough for all four faults
CONCURRENCY=${CONCURRENCY:-200}
RATE=${RATE:-15}
GAP=${GAP:-60}
LOG="$OUT/e4-$STAMP.log"
REPORT="$OUT/e4-$STAMP-buyers.json"
TIMINGS="$OUT/e4-$STAMP-recovery.tsv"
exec > >(tee "$LOG") 2>&1

ctl=$(mktemp -d)/holdfastctl
go build -o "$ctl" ./cmd/holdfastctl

# The Valkey nodes as the host reaches them, and which one is primary.
sentinel() { docker compose exec -T sentinel-1 valkey-cli -p 26379 "$@" 2>/dev/null | tr -d '\r'; }
primary() {
	case "$(sentinel sentinel get-master-addr-by-name holdfast | head -1)" in
	10.251.0.10) echo valkey ;; 10.251.0.11) echo valkey-replica ;; *) echo unknown ;;
	esac
}
host_port() { [ "$1" = valkey ] && echo 6379 || echo 6380; }
in_sync() { docker compose exec -T "$1" valkey-cli info replication 2>/dev/null | tr -d '\r' | grep -q '^master_link_status:up'; }
ready() { curl -sf -m 1 "localhost:$1/readyz" >/dev/null; }
untilted() { ! docker compose exec -T sentinel-1 valkey-cli -p 26379 info sentinel 2>/dev/null | grep -q '^sentinel_tilt:1'; }
untilted_all() {
	local n
	for n in sentinel-1 sentinel-2 sentinel-3; do
		docker compose exec -T "$n" valkey-cli -p 26379 info sentinel 2>/dev/null | grep -q '^sentinel_tilt:1' && return 1
	done
	return 0
}
wait_for() {
	local deadline=$(($(date +%s) + $1))
	shift
	until "$@"; do
		[ "$(date +%s)" -lt "$deadline" ] || return 1
		sleep 0.2
	done
}
record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$TIMINGS"; }
# running: whether the buyers are still at it, so every fault is seen to strike mid-sale.
running() { if [ -n "$buyers" ] && kill -0 "$buyers" 2>/dev/null; then echo yes; else echo no; fi; }

killed=""
buyers=""
cleanup() {
	if [ -n "$buyers" ]; then pkill -INT -P "$buyers" 2>/dev/null || true; kill -INT "$buyers" 2>/dev/null || true; fi
	if [ -n "$killed" ]; then docker compose start "$killed" >/dev/null 2>&1 || true; fi
	# Back to the starting layout, since host tools talk to the valkey node:
	# a coordinated failover in a moment when no Sentinel is in TILT mode,
	# tried for up to 5 minutes.
	local end=$(($(date +%s) + 300))
	while [ "$(primary)" = valkey-replica ] && [ "$(date +%s)" -lt "$end" ]; do
		if in_sync valkey && untilted_all; then
			sentinel sentinel failover holdfast COORDINATED >/dev/null || true
			sleep 10
		else
			sleep 1
		fi
	done
	if [ "$(primary)" != valkey ]; then
		echo "WARNING: Valkey's primary is still $(primary): fail back by hand (runbook RB-VK-2)"
	fi
	chaos_down
}
trap cleanup EXIT

chaos_up
# The session budget by Little's Law: the rate times the 10-minute session
# TTL (a smaller one throttles the run; D58).
EVENT=$(create_event "E4 $STAMP" "$BUYERS" "$RATE" $((RATE * 600)))
[ -n "$EVENT" ] || { echo "could not create the event"; exit 1; }
say "E4: $BUYERS buyers, $CONCURRENCY at a time, event $EVENT admitting up to $RATE a second; faults $GAP s apart"
printf 'fault\tmeasure\tseconds\n' >"$TIMINGS"
captured0=$(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}')

go run ./cmd/buyers -event "$EVENT" -buyers "$BUYERS" -concurrency "$CONCURRENCY" -json "$REPORT" &
buyers=$!
sleep "$GAP"

# 1. The Valkey primary.
from=$(primary)
to=$([ "$from" = valkey ] && echo valkey-replica || echo valkey)
wait_for 75 untilted || say "the Sentinels are still in TILT mode (E26); killing anyway"
say "1. killing the Valkey primary ($from); buyers running: $(running)"
t0=$(now)
killed=$from
docker compose kill "$from" >/dev/null 2>&1
if wait_for 120 sh -c "[ \"\$(docker compose exec -T sentinel-1 valkey-cli -p 26379 sentinel get-master-addr-by-name holdfast | head -1 | tr -d '\r')\" = $([ "$to" = valkey ] && echo 10.251.0.10 || echo 10.251.0.11) ]"; then
	record valkey_primary promoted "$(since "$t0")"
	wait_for 60 ready 9091 && record valkey_primary inventory_ready "$(since "$t0")"
	wait_for 60 ready 9092 && record valkey_primary queue_ready "$(since "$t0")"
	sleep 5
	t1=$(now)
	docker compose start "$from" >/dev/null 2>&1
	killed=""
	# It comes back as a second primary (its configuration still says so)
	# until a Sentinel demotes it, which TILT mode can delay (E26). The
	# services follow the Sentinels meanwhile; tools talking to it directly
	# would not.
	if wait_for 300 in_sync "$from"; then
		record valkey_primary old_primary_rejoined "$(since "$t1")"
	else
		record valkey_primary old_primary_not_rejoined_in 300
	fi
else
	# The Sentinels can stay in TILT mode on this laptop (E26) and then
	# fail over nothing: the outage lasts until the primary is back.
	say "no promotion within 120 s (Sentinels in TILT mode: $(sentinel info sentinel | grep -c '^sentinel_tilt:1') of 1 checked); starting $from again"
	record valkey_primary not_promoted_tilt 120
	docker compose start "$from" >/dev/null 2>&1
	killed=""
	to=$from
	wait_for 60 ready 9091 && record valkey_primary inventory_ready_after_restart "$(since "$t0")"
	wait_for 60 ready 9092 && record valkey_primary queue_ready_after_restart "$(since "$t0")"
fi

# RB-2 after the failover, on the new primary, timed step by step.
say "RB-2: freeze, rebuild inventory from PostgreSQL, unfreeze"
vk=localhost:$(host_port "$to")
rb=$(now)
s=$(now); "$ctl" freeze --valkey "$vk" --event "$EVENT"; record rb2 freeze "$(since "$s")"
s=$(now); "$ctl" inventory rebuild --valkey "$vk" --dsn "$DSN" --event "$EVENT" --dry-run; record rb2 dry_run "$(since "$s")"
s=$(now); "$ctl" inventory rebuild --valkey "$vk" --dsn "$DSN" --event "$EVENT"; record rb2 rebuild "$(since "$s")"
s=$(now); "$ctl" unfreeze --valkey "$vk" --event "$EVENT"; record rb2 unfreeze "$(since "$s")"
record rb2 total "$(since "$rb")"
sleep "$GAP"

# 2. booking-svc, mid-saga.
say "2. killing booking-svc; buyers running: $(running)"
t0=$(now)
docker compose kill booking >/dev/null 2>&1
killed=booking
sleep 10
docker compose start booking >/dev/null 2>&1
killed=""
wait_for 120 ready 9093 && record booking_svc ready_after_kill "$(since "$t0")"
sleep "$GAP"

# 3. Kafka.
say "3. restarting Kafka; buyers running: $(running)"
t0=$(now)
docker compose restart kafka >/dev/null 2>&1
# Serving again: a client's request answered. (Docker's health status lags
# far behind under load: its check starts a JVM with a 10 s timeout.)
kafka_serving() { docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; }
wait_for 300 kafka_serving && record kafka serving "$(since "$t0")"
outbox_empty() { [ "$(metric localhost:9093 'holdfast_outbox_pending')" = 0 ] && [ "$(metric localhost:9094 'holdfast_outbox_pending')" = 0 ]; }
wait_for 300 outbox_empty && record kafka outboxes_drained "$(since "$t0")"
sleep "$GAP"

# 4. PostgreSQL, cut off for 5 s.
say "4. cutting PostgreSQL off for 5 s; buyers running: $(running)"
t0=$(now)
curl -sf -X POST "$TOXIPROXY/proxies/postgres" -d '{"enabled":false}' >/dev/null
sleep 5
curl -sf -X POST "$TOXIPROXY/proxies/postgres" -d '{"enabled":true}' >/dev/null
wait_for 60 sh -c 'curl -sf -m 1 localhost:9093/readyz >/dev/null && curl -sf -m 1 localhost:9094/readyz >/dev/null' &&
	record postgres services_ready "$(since "$t0")"

say "faults done; waiting for the buyers"
set +e
wait "$buyers"
set -e
buyers=""
ended=$(now)
captured=$(($(metric "$MOCKPSP_ADMIN" 'holdfast_mockpsp_payments_total{result="captured"}') - captured0))
settled=$(settle "$EVENT" "$captured") || settled=""

echo
say "results"
summary "$EVENT" "$captured"
echo "recovery (seconds):"
column -t -s $'\t' "$TIMINGS" | sed 's/^/  /'
pass=1
[ -n "$settled" ] || { echo "FAIL: not settled 15 minutes after the buyers finished"; pass=0; }
if violations | grep -qE '=[1-9]'; then echo "FAIL: an invariant is violated"; pass=0; fi
if [ "$pass" = 1 ]; then
	echo "RESULT: PASS - every fault recovered, settled ${settled} s after the buyers finished, every invariant 0"
else
	echo "RESULT: FAIL"
	exit 1
fi
