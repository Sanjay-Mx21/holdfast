# Experiment results

Raw evidence for every number HoldFast publishes. A number that is not backed
by a file in this folder must not appear in the README, a resume or a post.

## E1 soak, 2026-10-01 (task 1.5.7)

**Result: 100 of 100 runs passed. 900 of 900 invariant checks passed, 0 failed.**
Every run granted exactly 1,000 holds to 50,000 buyers and sold exactly 1,000
units through the PostgreSQL guard out of 10,000 confirmations.

| File | Content |
|---|---|
| `e1-soak-2026-10-01-bf64b21.log` | Full output of all 100 runs |
| `e1-soak-2026-10-01-bf64b21-summary.tsv` | One row per run: start time, result, duration, latency percentiles |
| `e1-soak-2026-10-01-bf64b21-on-battery-aborted.log` and `-summary.tsv` | An earlier attempt on battery power, stopped after 5 runs (all passed); kept for the record, not part of the result |

### Method

- Commit `bf64b21` (code identical to `1101cc3`; later commits changed docs only).
- `loadtest/e1-soak.sh` (same logic as the script that produced these files, made portable): 100 consecutive `make e1` runs (`cmd/contention -mode all`,
  defaults: 50,000 buyers, 1,000 units, quantity 1, 1,000 attempts in flight;
  10,000 guard confirmations, 64 in flight, per-user cap 4). Unlike the plan's
  loop in section 18, it does not stop at the first failure, so every run is counted.
- Started 22:24:41 IST, last run started 22:50:20 IST. Run time: median 15 s, maximum 36 s.
- Against the local `make up` stack (PostgreSQL 18, Valkey 9.1.2), which kept
  running throughout; inventory-svc was idle. Its sweeper cannot affect E1,
  whose holds live for 10 minutes.

### Hardware and environment

| | |
|---|---|
| CPU | Intel Core i7-1250U, 10 cores (2P + 8E), 12 threads |
| Memory | 15.7 GB; WSL2 and Docker get 12 CPUs and 7.6 GB (WSL defaults, no `.wslconfig`) |
| OS | Windows 11 Home build 26300; WSL2 kernel 6.6.87.2; Ubuntu 24.04.5 LTS |
| Docker | Docker Desktop, engine 29.8.0, WSL2 backend |
| Go | 1.27.1 |
| Power | On AC (charging from 9%), Windows "Balanced" power plan |
| Load | A laptop in normal use, not a dedicated machine |

### Latency

Each run reports its own p50, p95 and p99 latency per call, as measured by the
harness. The table shows how those per-run values are spread across the 100
runs; no run was dropped.

| Per-run value | Best run | Median run | 90th percentile run | Worst run |
|---|---|---|---|---|
| Hold p50 | 32.5 ms | 41.0 ms | 48.3 ms | 137.3 ms (run 7) |
| Hold p95 | 49.7 ms | 64.6 ms | 89.3 ms | 497.8 ms (run 1) |
| Hold p99 | 74.1 ms | 114.3 ms | 154.2 ms | 1,088.5 ms (run 1) |
| Guard p50 | 14.5 ms | 16.3 ms | 18.9 ms | 26.9 ms (run 1) |
| Guard p95 | 273.1 ms | 313.4 ms | 351.0 ms | 581.3 ms (run 1) |
| Guard p99 | 844.6 ms | 918.3 ms | 1,037.1 ms | 2,002.2 ms (run 1) |

Throughput: hold attempts median 21,126 per second (range 5,202 to 24,749);
guard confirmations median 1,111 per second (range 561 to 1,168).

### Caveats

- **Run 1 is an outlier caused by the operator, not the system.** Docker and WSL
  diagnostics were run at the moment it started, competing for CPU. Without
  run 1, the worst hold p99 is 398.5 ms (run 7).
- **These are not SLO measurements.** The hold SLO in the design doc (p99 ≤ 100 ms)
  applies to the HTTP API at design load, about 250 holds per second. E1 drives
  the service layer directly at about 21,000 attempts per second with 1,000 in
  flight, about 85 times the design rate, and its latency includes queueing in
  the harness. The median-run hold p99 of 114 ms must not be quoted as the API's p99.
- Guard latency comes from 64 concurrent confirmations on one hot row by
  design: the guard serialises them. It is a stress figure, not a checkout time.
- A shared laptop is noisy. Repeat on a quiet or dedicated machine before
  publishing any latency number.
