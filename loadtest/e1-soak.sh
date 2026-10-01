#!/usr/bin/env bash
# E1 soak (task 1.5.7): run `make e1` RUNS times (default 100) against the
# local stack and record every run. Unlike a stop-at-first-failure loop, it
# counts every pass and failure. Run from the repository root:
#   ./loadtest/e1-soak.sh            (or RUNS=10 ./loadtest/e1-soak.sh)
# Writes loadtest/results/e1-soak-<date>-<sha>.log (full output) and
# ...-summary.tsv (one row per run with latency percentiles).
set -u
cd "$(dirname "$0")/.." || exit 1
RUNS=${RUNS:-100}
sha=$(git rev-parse --short HEAD)
mkdir -p loadtest/results
LOG="loadtest/results/e1-soak-$(date +%F)-${sha}.log"
SUM="loadtest/results/e1-soak-$(date +%F)-${sha}-summary.tsv"
: > "$LOG"
printf 'run\tstarted_ist\tresult\tseconds\thold_p50_ms\thold_p95_ms\thold_p99_ms\tguard_p50_ms\tguard_p95_ms\tguard_p99_ms\n' > "$SUM"
pass=0; fail=0
for i in $(seq 1 "$RUNS"); do
  start=$(TZ=Asia/Kolkata date '+%F %T'); t0=$(date +%s)
  out=$(make e1 2>&1); rc=$?
  secs=$(( $(date +%s) - t0 ))
  { echo "===== run $i of $RUNS, $start IST, exit $rc ====="; echo "$out"; } >> "$LOG"
  lat=$(echo "$out" | grep -E '^\s+latency' | sed -E 's/.*p50 ([0-9.]+) ms \| p95 ([0-9.]+) ms \| p99 ([0-9.]+) ms.*/\1\t\2\t\3/' | paste -sd'\t')
  if [ "$rc" -eq 0 ] && echo "$out" | grep -q 'RESULT: PASS'; then res=PASS; pass=$((pass+1)); else res=FAIL; fail=$((fail+1)); fi
  printf '%s\t%s\t%s\t%s\t%s\n' "$i" "$start" "$res" "$secs" "$lat" >> "$SUM"
  echo "run $i: $res (${secs}s)  passed $pass, failed $fail"
done
echo "SOAK DONE: $pass of $RUNS passed, $fail failed"
echo "log: $LOG"
echo "summary: $SUM"
[ "$fail" -eq 0 ]
