#!/usr/bin/env bash
# Valkey failover drill (task 5.2, docs/runbooks/valkey.md). Fails the
# primary over to the replica while `holdfastctl valkey probe` writes through
# the Sentinels, then reports:
#   - how long Sentinel took to promote the replica;
#   - how long writes stopped, and whether any acknowledged write was lost;
#   - how long each service's readiness failed;
#   - (crash) how long the old primary took to rejoin as a replica;
#   - the auditor's invariants afterwards.
# Then it fails back, so the stack ends as it started.
#
#   scripts/valkey-failover-drill.sh crash     # SIGKILL the primary (the default)
#   scripts/valkey-failover-drill.sh planned   # SENTINEL FAILOVER ... COORDINATED:
#                                              # writes pause until the replica has
#                                              # caught up, so none are lost
#   scripts/valkey-failover-drill.sh forced    # SENTINEL FAILOVER without COORDINATED:
#                                              # the old primary takes writes for a
#                                              # moment more, and they are lost
#   FAILBACK=0 scripts/valkey-failover-drill.sh  # stay on the promoted replica
#
# Sentinel enters TILT mode, and does nothing for 30 s, whenever the wall
# clock steps backwards. Some VMs step it every 30 s (WSL2 on the
# development laptop, E26), so by default every action waits until no
# Sentinel is in TILT: the drill then measures the failover, not the clock.
# AVOID_TILT=0 acts at once, TILT or not.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=${1:-crash}
FAILBACK=${FAILBACK:-1}
PROBE_FOR=${PROBE_FOR:-120s}
AVOID_TILT=${AVOID_TILT:-1}
case $MODE in
crash | planned | forced) ;;
*)
	echo "usage: $0 [crash|planned|forced]" >&2
	exit 2
	;;
esac
WORK=$(mktemp -d)
killed="" # the node the crash drill killed, until it is started again
cleanup() {
	# A drill that stops early must not leave a node down: without a
	# promotion, the stack would have no primary at all.
	if [ -n "$killed" ]; then
		echo "== the drill stopped early: starting $killed again"
		docker compose start "$killed" >/dev/null 2>&1 || true
	fi
	rm -rf "$WORK"
}
trap cleanup EXIT

dc() { docker compose "$@"; }
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
sentinel() { dc exec -T sentinel-1 valkey-cli -p 26379 "$@" 2>/dev/null | tr -d '\r'; }
# The nodes' fixed addresses on the valkey network (compose.yaml).
addr_of() { case $1 in valkey) echo 10.251.0.10 ;; valkey-replica) echo 10.251.0.11 ;; esac; }
node_at() { case $1 in 10.251.0.10) echo valkey ;; 10.251.0.11) echo valkey-replica ;; *) echo "$1" ;; esac; }
primary() { node_at "$(sentinel sentinel get-master-addr-by-name holdfast | head -1)"; }
role() { dc exec -T "$1" valkey-cli info replication 2>/dev/null | tr -d '\r' | grep -E '^(role|master_link_status):' | paste -sd' ' || true; }
in_sync() { [ "$(role "$1")" = "role:slave master_link_status:up" ]; }
# Whether Sentinel lists node $1 as a healthy replica (flags exactly "slave").
known_replica() { sentinel sentinel replicas holdfast | paste -sd' ' | grep -qE "name $(addr_of "$1"):6379 .*flags slave( |$)"; }
# wait_for SECONDS COMMAND...: retries COMMAND every 200 ms.
wait_for() {
	local deadline=$(($(date +%s) + $1))
	shift
	until "$@"; do
		[ "$(date +%s)" -lt "$deadline" ] || return 1
		sleep 0.2
	done
}
is_primary() { [ "$(primary)" = "$1" ]; }

# watch_ready PORT NAME SECONDS: reports each time the service's readiness
# fails and how long until it recovers.
watch_ready() {
	local port=$1 name=$2 end=$(($(date +%s) + $3)) down=""
	while [ "$(date +%s)" -lt "$end" ]; do
		if curl -sf -m 1 "localhost:$port/readyz" >/dev/null; then
			if [ -n "$down" ]; then
				echo "$name: unready for $(since "$down") s"
				down=""
			fi
		elif [ -z "$down" ]; then
			down=$(now)
		fi
		sleep 0.2
	done
	if [ -n "$down" ]; then echo "$name: still unready at the end"; fi
}

# tilted: how many Sentinels are in TILT mode now.
tilted() {
	local n=0 s
	for s in sentinel-1 sentinel-2 sentinel-3; do
		if dc exec -T "$s" valkey-cli -p 26379 info sentinel 2>/dev/null | grep -q '^sentinel_tilt:1'; then n=$((n + 1)); fi
	done
	echo "$n"
}
untilted() { [ "$(tilted)" = 0 ]; }
# settle: waits for every Sentinel to leave TILT (unless AVOID_TILT=0), and
# says how many are in it when the drill acts.
settle() {
	if [ "$AVOID_TILT" = 1 ] && ! untilted; then
		echo "== waiting for the Sentinels to leave TILT mode"
		wait_for 75 untilted || echo "   still in TILT after 75 s; acting anyway"
	fi
	echo "== Sentinels in TILT mode now: $(tilted) of 3"
}

# fail_over TO [COORDINATED]: a manual switch, once Sentinel knows TO as a
# healthy replica.
fail_over() {
	wait_for 60 known_replica "$1" || {
		echo "Sentinel does not list $1 as a healthy replica"
		return 1
	}
	settle
	local reply
	t0=$(now) # the clock starts here, after any wait for TILT to end
	reply=$(sentinel sentinel failover holdfast ${2:-})
	[ "$reply" = OK ] || {
		echo "SENTINEL FAILOVER refused: $reply"
		return 1
	}
}

from=$(primary)
case $from in
valkey) to=valkey-replica ;;
valkey-replica) to=valkey ;;
*)
	echo "Sentinel reports no primary (got '$from')"
	exit 1
	;;
esac
echo "== primary $from; replica $to ($(role "$to"))"
in_sync "$to" || {
	echo "the replica is not in sync: not drilling"
	exit 1
}
sentinel sentinel ckquorum holdfast | grep -q '^OK' || {
	echo "the Sentinels have no quorum: not drilling"
	exit 1
}

echo "== probing writes through the Sentinels for $PROBE_FOR"
dc run --rm -T holdfastctl valkey probe --for "$PROBE_FOR" >"$WORK/probe" 2>&1 &
probe=$!
sleep 8 # a steady baseline first
settle
for svc in 9091:inventory 9092:queue 9097:auditor; do
	watch_ready "${svc%%:*}" "${svc#*:}" 60 >"$WORK/ready-${svc#*:}" &
done

case $MODE in
crash)
	t0=$(now)
	echo "== killing $from (SIGKILL)"
	killed=$from
	dc kill "$from" >/dev/null 2>&1
	;;
planned)
	echo "== SENTINEL FAILOVER COORDINATED: $from to $to"
	fail_over "$to" COORDINATED
	;;
forced)
	echo "== SENTINEL FAILOVER (not coordinated): $from to $to"
	fail_over "$to"
	;;
esac
wait_for 90 is_primary "$to" || {
	echo "no promotion within 90 s"
	exit 1
}
promoted=$(since "$t0")
echo "== Sentinel made $to the primary after $promoted s"

rejoined=""
if [ "$MODE" = crash ]; then
	sleep 5
	t1=$(now)
	echo "== restarting $from"
	dc start "$from" >/dev/null 2>&1
	killed=""
	wait_for 120 in_sync "$from" || {
		echo "$from did not rejoin as a replica within 120 s"
		exit 1
	}
	rejoined=$(since "$t1")
	echo "== $from rejoined as a replica of $to after $rejoined s"
else
	wait_for 60 in_sync "$from" || {
		echo "$from is not in sync as a replica after 60 s"
		exit 1
	}
	echo "== $from is a replica of $to, in sync"
fi

wait "$probe" || true
wait # the readiness watchers
echo "== the probe (times from its start):"
grep -E '^t\+|writes,' "$WORK/probe" | sed 's/^/   /'
echo "== readiness:"
cat "$WORK"/ready-* | sed 's/^/   /'
if ! cat "$WORK"/ready-* | grep -q .; then echo "   every service stayed ready"; fi
result=$(grep '^RESULT' "$WORK/probe" | sed 's/^RESULT //')

if [ "$FAILBACK" = 1 ]; then
	echo "== failing back to $from (coordinated)"
	fail_over "$from" COORDINATED
	wait_for 90 is_primary "$from" || {
		echo "failback did not complete within 90 s"
		exit 1
	}
	wait_for 60 in_sync "$to" || {
		echo "$to is not in sync after failback"
		exit 1
	}
	echo "== $from is the primary again; $to in sync"
fi

sleep 20 # an auditor check or two
echo "== invariants (auditor):"
curl -s -m 5 localhost:9097/metrics | grep '^holdfast_invariant_violations' | sed 's/^/   /'
echo "DRILL mode=$MODE promoted_s=$promoted ${rejoined:+rejoined_s=$rejoined }$result"
