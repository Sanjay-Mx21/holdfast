#!/usr/bin/env bash
# Shared by the chaos experiments (chaos/README.md). Sourced from the
# repository root by chaos/e3.sh and chaos/e4.sh.

CHAOS=(docker compose -f compose.yaml -f chaos/compose.yaml)
DSN=${HOLDFAST_TEST_POSTGRES_DSN:-postgres://holdfast:holdfast@localhost:5432/holdfast?sslmode=disable}
MOCKPSP_ADMIN=http://localhost:9095
# Compose's development default for the admin tokens (compose.yaml); set
# ADMIN_TOKEN if the stack runs with another.
MOCKPSP_TOKEN=${ADMIN_TOKEN:-dev-only-admin-token-change-me-0123456789}
TOXIPROXY=http://localhost:8474
AUDITOR=http://localhost:9097
STAMP="$(date -u +%Y-%m-%dT%H%M)-$(git rev-parse --short HEAD)"
OUT=${OUT:-loadtest/results} # OUT=/tmp for a trial run

now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
say() { echo "== $(date -u +%H:%M:%S) $*"; }

# chaos_up: the stack with the chaos overlay (dev identity, no proof of
# work, no per-IP limits, PostgreSQL through Toxiproxy).
chaos_up() {
	say "switching to the chaos overlay"
	"${CHAOS[@]}" up -d --wait toxiproxy queue booking payment nginx >/dev/null 2>&1 ||
		{ echo "the chaos overlay did not come up"; return 1; }
	curl -sf "$TOXIPROXY/proxies/postgres" >/dev/null || { echo "Toxiproxy has no postgres proxy"; return 1; }
}

# chaos_down: the normal stack again, faults cleared, Toxiproxy gone.
chaos_down() {
	say "restoring the normal stack"
	faults '{}' >/dev/null 2>&1 || true
	curl -sf -X POST "$TOXIPROXY/proxies/postgres" -d '{"enabled":true}' >/dev/null 2>&1 || true
	docker compose up -d --wait queue booking payment nginx >/dev/null 2>&1 || echo "restore failed: run make up"
	"${CHAOS[@]}" rm -sf toxiproxy >/dev/null 2>&1 || true
}

# faults JSON: replaces mockpsp's faults.
faults() {
	curl -sf -X PUT "$MOCKPSP_ADMIN/internal/v1/faults" -H "Authorization: Bearer $MOCKPSP_TOKEN" \
		-H 'Content-Type: application/json' -d "$1"
}

# create_event NAME CAPACITY RATE SESSIONS: an event on sale in a few
# seconds; prints its ID.
create_event() {
	local opens
	opens=$(date -u -d '+5 seconds' +%Y-%m-%dT%H:%M:%SZ)
	go run ./cmd/holdfastctl event create --name "$1" --capacity "$2" --per-user-limit 4 --opens-at "$opens" \
		--admission-rate "$3" --max-sessions "$4" --dsn "$DSN" 2>&1 |
		grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1
	sleep 6
}

# metric URL NAME[{labels}]: the sum of a metric's samples.
metric() {
	curl -s -m 5 "$1/metrics" | awk -v m="$2" 'index($0, m) == 1 && $0 !~ /^#/ { s += $NF } END { printf "%d", s }'
}

# violations: every invariant's count from the auditor, on one line.
violations() {
	curl -s -m 5 "$AUDITOR/metrics" | awk -F'"' '/^holdfast_invariant_violations/ { split($3, v, " "); printf "%s=%s ", $2, v[2] }'
}

# sql QUERY: one PostgreSQL query, unaligned, through the container.
sql() { docker compose exec -T postgres psql -U holdfast holdfast -At -F' ' -c "$1"; }

# known EVENT: the event's payments HoldFast knows were captured.
known() {
	sql "SELECT count(*) FROM payment.payment_intents WHERE event_id = '$1' AND status IN ('CAPTURED','REFUND_PENDING','REFUNDED')"
}

# unresolved EVENT: captured money neither confirmed nor refunded (I3's
# question, without its 15-minute grace).
unresolved() {
	sql "SELECT count(*) FROM payment.payment_intents i LEFT JOIN booking.bookings b ON b.id = i.booking_id
	     WHERE i.event_id = '$1' AND (i.status = 'REFUND_PENDING' OR (i.status = 'CAPTURED' AND b.status IS DISTINCT FROM 'CONFIRMED'))"
}

# settle EVENT CAPTURED: waits up to 15 minutes until HoldFast knows every
# capture the provider made (CAPTURED of them) and none is unresolved;
# prints how many seconds that took, or fails.
settle() {
	local from k u
	from=$(now)
	while [ "$(awk -v a="$from" -v b="$(now)" 'BEGIN { print (b - a < 900) }')" = 1 ]; do
		k=$(known "$1")
		u=$(unresolved "$1")
		if [ "$k" -ge "$2" ] && [ "$u" = 0 ]; then
			since "$from"
			return 0
		fi
		echo "   $(date -u +%H:%M:%S) captured at the provider $2, known to HoldFast $k, unresolved $u" >&2
		sleep 15
	done
	return 1
}

# summary EVENT CAPTURED: what happened to the event's bookings and payments.
summary() {
	echo "bookings:  $(sql "SELECT status, count(*) FROM booking.bookings WHERE event_id = '$1' GROUP BY 1 ORDER BY 1" | paste -sd' ')"
	echo "intents:   $(sql "SELECT status, count(*) FROM payment.payment_intents WHERE event_id = '$1' GROUP BY 1 ORDER BY 1" | paste -sd' ')"
	echo "provider:  $2 captured; HoldFast knows $(known "$1"), $(unresolved "$1") unresolved"
	echo "payment-svc captures by source: $(curl -s -m 5 localhost:9094/metrics | awk '/^holdfast_payment_captures_total/ { printf "%s ", $0 }')"
	echo "reconciler findings: $(curl -s -m 5 localhost:9094/metrics | awk '/^holdfast_recon_mismatch_total/ && $NF > 0 { printf "%s ", $0 }')"
	echo "invariants: $(violations)"
}
