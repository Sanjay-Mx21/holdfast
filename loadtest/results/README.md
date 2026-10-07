# Experiment results

Raw evidence for every number HoldFast publishes. A number that is not backed
by a file in this folder must not appear in the README, a resume or a post.

## E4 infrastructure chaos, 2026-10-07 (task 5.5)

**Result: PASS. 12,000 buyers bought through the running stack while,
one after another, Valkey's primary was killed, booking-svc was killed,
Kafka was restarted and PostgreSQL was cut off for 5 s. Every capture was
resolved, every invariant was 0, nothing was dead-lettered, and runbook
RB-2 rebuilt the sale's inventory from PostgreSQL in 2.3 s mid-sale. The
run before it failed, and found P56.**

| File | Content |
|---|---|
| `e4-2026-10-07T1106-1998aae.log` | The passing run: the faults, the buyers' report, the settlement check |
| `e4-2026-10-07T1106-1998aae-recovery.tsv` | Its recovery times |
| `e4-2026-10-07T1106-1998aae-buyers.json` | Its buyers' report |
| `e4-2026-10-07T1019-1998aae.*` | The run that failed (P56), with a real Valkey failover |
| `e4-2026-10-07T0953-1998aae.*`, `e4-2026-10-07T1001-1998aae.*` | Earlier attempts: no failover (Sentinel in TILT, E26); a PostgreSQL cut that came after the sale |

### Method

```bash
make chaos-e4   # BUYERS=12000 CONCURRENCY=200 RATE=15 GAP=60
```

- **Load:** the same buyers as E3 (`cmd/buyers`), admitted at 15 a
  second, so the sale lasts through all four faults (the log says, at each
  fault, that the buyers were still running). No payment faults.
- **Faults, 60 s apart:**
  1. the Valkey primary is killed (SIGKILL); once Sentinel promotes the
     replica (or, if it cannot, the killed node is started again), runbook
     RB-2 is practised: freeze the sale, dry run, rebuild inventory from
     PostgreSQL, unfreeze;
  2. booking-svc is killed, mid-saga, and started 10 s later;
  3. Kafka is restarted;
  4. PostgreSQL is cut off from booking-svc, payment-svc and queue-svc
     for 5 s (Toxiproxy).
- **Pass:** every payment the provider captured is known to HoldFast and
  resolved within 15 minutes of the buyers finishing, and every invariant
  is 0.

### Recovery times

| Fault | Measure | Passing run (T1106) | Failover run (T1019) |
|---|---|---|---|
| Valkey primary killed | Replica promoted | none: the Sentinels were in TILT mode (E26); the killed node was restarted after 120 s | **7.3 s** |
| | inventory-svc and queue-svc ready | 132.6 s, 132.8 s (after the restart) | **8.7 s, 9.2 s** |
| | The killed node back as a replica | (it was the primary again) | 176 s (TILT delayed the Sentinels' demotion) |
| RB-2 | Freeze, dry run, rebuild, unfreeze | 0.1, 0.5, 1.3, 0.1 s: **2.3 s** | 0.1, 0.4, 0.9, 0.2 s: **2.0 s** |
| booking-svc killed | Ready again (10 s of it down by design) | 21.3 s | 20.6 s |
| Kafka restarted | Serving / both outboxes drained | 52.1 s / 56.7 s | 47.9 s / 52.4 s |
| PostgreSQL cut 5 s | booking-svc and payment-svc ready | 5.2 s | 5.3 s |

### Numbers (the passing run)

| Measure | Value |
|---|---|
| Buyers / confirmed | 12,000 / 11,913 |
| Gave up during the 132 s Valkey outage | 87 (71 joining, 15 claiming their turn, 1 booking): retries ran out before Valkey came back; none had paid |
| Captures at the provider / known / unresolved | 11,913 / 11,913 / 0 |
| Dead-lettered messages | 0 (the payment topic stayed at the 29 left by earlier runs, replayed with RB-3) |
| Inventory drift afterwards | 0 on every event |
| Every invariant | 0 |
| Requests repeated after a failure | joining 2,884, admission claims 435, bookings 348, booking reads 497, holds 1 |

### What the failed run found (P56)

The run with the real failover ended with **I3 at 3**: three buyers paid,
and their bookings were cancelled, unpaid, seven minutes later. Their
`payment.captured` events had reached booking-svc's saga during the 5 s
PostgreSQL cut, failed five quick attempts in two seconds, and gone to the
dead-letter topic. The Valkey failovers had dead-lettered 26 more, after
the saga confirmed the bookings but before it could mark their holds sold,
which left 22 units of inventory drift that the auditor's gauge reported.
Replaying the dead-letter topic (RB-3, 3.7 s) refunded the three and
repaired the drift. The fix: a consumer retries a failure that is not
permanent for 10 minutes before dead-lettering it, and inventory-svc
reports Valkey failover errors as UNAVAILABLE (P57). The passing run,
with a longer Valkey outage, dead-lettered nothing.

### Caveats

- **The laptop's clock defeats Sentinel (E26).** WSL2 steps the clock
  back every 30 s; Sentinel enters TILT mode on each step and, when steps
  come slightly faster than its 30 s period, stays in it. Two of the four
  attempts could not fail over at all. The failover numbers (7.3 s)
  come from the run where TILT allowed it, and match task 5.2's drill
  (7.1 s).
- **One run each**, on one machine shared by the stack and the load.
- No payment faults during E4: they are E3's.

## E3 payment chaos, 2026-10-07 (task 5.5)

**Result: PASS. 5,000 buyers made whole purchases through the running
stack under the design's full fault mix. I2 (no double charge) stayed 0
throughout, every capture reached HoldFast (220 lost capture webhooks,
every one recovered by status polling), and no captured money was left
unresolved once the buyers finished.**

| File | Content |
|---|---|
| `e3-2026-10-07T0908-1998aae.log` | The run: the auditor and admission rate every 15 s, the buyers' report, the settlement check |
| `e3-2026-10-07T0908-1998aae-buyers.json` | The buyers' report (outcomes, retries, payment to confirmation) |
| `e3-2026-10-07T0908-1998aae-mockpsp-metrics.txt` | mockpsp's counters afterwards (they include two 60-buyer trial runs made just before) |

### Method

```bash
BUYERS=5000 CONCURRENCY=300 RATE=200 make chaos-e3
```

- **Path:** every buyer goes through the edge like a browser: join the
  waiting room, claim the turn, hold one unit at inventory-svc, book at
  booking-svc, pay at mockpsp's checkout, then wait for the booking to
  settle (`cmd/buyers`). The stack runs under `chaos/compose.yaml`: buyers
  identify themselves with `X-Dev-User-Id`, join without proof of work,
  and the per-IP limits are lifted (one load generator).
- **Faults (mockpsp, the design's E3 mix):** 20% of webhooks duplicated,
  10% delayed 30 to 90 s, 5% lost; 10% of payments fail; 5% of API answers
  held back 10 s, past payment-svc's 2 s attempt timeout.
- **Event:** 5,000 units, admission up to 200 a second (adaptive), a
  session budget of 1,000 (see the caveats).
- **Machine:** the development laptop, with the whole stack and the load
  generator; WSL2 limited to 6 GB.

### Numbers

| Measure | Value |
|---|---|
| Buyers | 5,000 |
| Paid and confirmed | 4,481 |
| Payment declined (the failure fault) and the booking cancelled | 519 (10.4%) |
| Captures at the provider / known to HoldFast / unresolved | 4,481 / 4,481 / 0 |
| Capture webhooks lost / recovered by polling | 220 / 220 |
| I2 violations, sampled every 15 s | 0 throughout |
| Every invariant at the end | 0 |
| Booking requests repeated after a 503 (the timeout fault) | 523, all then succeeded |
| Payment to confirmed booking | p50 6.4 s, p95 90 s, p99 148 s (the 30 to 90 s delays, and polling's 2-minute wait for lost webhooks) |
| Admission rate applied (adaptive) | 200/s most of the time; down to 10/s while timeouts pushed the provider's p99 over 2 s |

### Caveats

- **Slow because of the session budget, by mistake (D58).** The run took
  44 minutes: it stalled at 1,000, 2,000 and 3,000 buyers for 6 to 8
  minutes each, because every admitted buyer keeps a session slot for its
  10-minute TTL and the event allowed 1,000. The design sizes the budget
  by Little's Law (rate × TTL: 120,000 here); the scripts do so now. It
  changes how long the run takes, not what it checks.
- **I3's 15 minutes were never tested at the edge:** everything settled
  within seconds of the last buyer, because polling recovered every lost
  capture within its 2-minute wait. The reconciler had nothing to repair.
- **The first trial run found P55:** admission stalled near the end of a
  sale because buyers who had already bought still counted against the
  units cap. Fixed before this run.

## Phase 4 demo: a ticket bought by hand during a stampede, 2026-10-04

**Result: the buyer's purchase went through mid-rush and the dashboard showed
it live (Phase 4's first exit criterion). Every join queue-svc received was
admitted and got a token; the origin stayed at about 0.5 status requests a
second. The scaled-down E2 thresholds failed on the load generator's side:
k6 could not send every planned join while two browsers and Grafana shared
the laptop.**

| File | Content |
|---|---|
| `phase4-stampede.gif` | The mission-control dashboard during run 2, one frame every 4 s: the queue fills, admissions flow, then the hand purchase appears (hold, confirmed booking, payment, capture-to-confirm) |
| `phase4-mission-control.png` | The whole dashboard after a purchase, all four rows |
| `e2-2026-10-03T2001-2255911.log`, `-summary.json`, `-queue-metrics.txt` | Run 2: k6 output and summary, queue-svc's metrics at the end |
| `e2-2026-10-03T1955-2255911.log`, `-summary.json`, `-queue-metrics.txt` | Run 1, at a larger load |

### Method

- `make load-e2` (`loadtest/e2/run.sh`) scaled down. Run 1: 5,000 users,
  500 joins a second at peak, 100 admissions a second. Run 2: 3,000 users,
  300 joins a second, 60 admissions a second. Both polled every 3 s, with
  proof of work off for the load test (`POW_DIFFICULTY=0`, E2's override).
- At the same time, on the same event: headless Chromium captured the
  dashboard every 4 s (the GIF), and a second Chromium ran the web app's
  Playwright test (`web/e2e/purchase.spec.ts`), 25 s into the rush: sign in,
  join, wait for the turn, hold 2, book, pay at mockpsp, see it confirmed,
  with axe on every page.
- Commit `2255911` (run.sh stamps the files with it); the E2 event now has
  as many units as users (P43), so the units cap does not limit E2.

### Numbers

| | Run 1 | Run 2 |
|---|---|---|
| Users planned | 5,000 | 3,000 |
| Joins queue-svc received (k6, plus the hand buyer) | 3,188 | 2,481 |
| Tokens issued | 3,187 to k6 users, every one that joined | 2,480 to k6 users, every one that joined |
| Claims refused | 1,801, all `NOT_IN_QUEUE`: users whose joins k6 never sent | 520, likewise |
| k6 iterations dropped | 66,393 | 15,995 |
| Status polls at the edge, requests at the origin | 88,967 and 49 | 76,641 and 50 |
| The hand purchase | Passed, joined in 262 ms | Passed, joined in 3,712 ms; capture to confirm 1.99 s (p99 over 5 minutes) |

### Caveats

- These are demo runs, not E2 results: the generator, two browsers, Grafana
  and the whole stack shared one laptop, and k6 dropped joins in both runs.
  E2 at design scale needs a separate load generator (Phase 6, task 6.1).
- The thresholds k6 reports as failed (tokens issued, join p99) fail for that
  reason; the server-side counts above are the meaningful ones.

## One purchase, one trace, 2026-10-03 (Phase 3 demo checkpoint)

`phase3-one-purchase-trace.png` is Jaeger's view of one purchase made
through the edge with the whole stack running: 20 spans across booking,
inventory and payment in one trace. From the top:

1. `POST /v1/bookings` and its gRPC calls: `GetHold` and `MarkPaying` in
   inventory (with their Valkey commands), and `CreateIntent` in payment,
   with its call to the provider.
2. `payment.apply_webhook`: the provider's capture, continuing the booking's
   trace. The link icon points to the webhook's own trace.
3. The capture's publish, then booking-svc's saga (`process`), inventory's
   `Confirm` and the `booking.confirmed.v1` publish.

The saga ran about 14 s after the request because booking-svc had just been
restarted and its consumer group was rejoining. Jaeger marks the trace
"Incomplete" for a harmless reason: the test request was sent with a
hand-made `traceparent`, so the root span's parent is not in Jaeger.

Captured with headless Chrome on the Compose network:

```bash
docker run --rm --network holdfast_default -v /tmp/shot:/out zenika/alpine-chrome:124 \
  --no-sandbox --hide-scrollbars --window-size=1600,1100 --virtual-time-budget=15000 \
  --screenshot=/out/trace.png http://jaeger:16686/trace/ce2f176f13a4ed1cec90696842652bd5
```

## E1 part B with injected faults, 2026-10-03 (Phase 3 exit)

**Result: all 12 checks passed. 1,000 purchases completed with I1, I2 and I4
at 0 violations under the design's E3 fault mix.**

| File | Content |
|---|---|
| `e1-purchase-faults-2026-10-03.json` | The report: outcomes, latencies, checks |

### Method

```bash
go run ./cmd/contention -mode purchase -purchases 2000 -capacity 1000 -buyers-per-user 6 \
  -psp-faults '{"duplicateRate":0.2,"delayRate":0.1,"delayMin":"1s","delayMax":"3s","lossRate":0.05,"timeoutRate":0.05,"timeoutDelay":"3s"}' \
  -json loadtest/results/e1-purchase-faults-2026-10-03.json
```

- **Load:** 2,000 buyers race for 1,000 units, sharing user IDs six at a
  time, so the per-user cap of 4 refuses some.
- **Path:** each winner books, and pays at an in-process mockpsp. The
  real inventory, booking and payment services run in one process
  against local Valkey and PostgreSQL (its own `<name>_e1` database), and
  the payment events are fed to the booking saga twice each.
- **Faults (seeded):** 10% of payments fail; 20% of webhooks are
  duplicated, 10% delayed 1 to 3 s and 5% lost; 5% of provider answers are
  held back 3 s, past payment-svc's 2 s attempt timeout. The delays are
  shorter than E3's 30 to 90 s to keep the run short. The poller runs
  while the run waits, so lost webhooks are recovered as in production.
- **Machine:** the development laptop, with the whole `make up` stack
  running.

### Numbers

| Measure | Value |
|---|---|
| Holds granted (1,000 units) | 1,000 (500 refused sold out, 500 by the per-user cap) |
| Paid and confirmed | 898 |
| Payment failed and cancelled | 102 (rate 0.10) |
| Webhooks duplicated / delayed / lost | 207 / 105 / 46 |
| Duplicates dropped by payment-svc | 207 |
| Captures learned by polling | 45 |
| Provider answers held back | 67 |
| PostgreSQL sold, Valkey left | 898, 102 (capacity 1,000) |
| Receivable in the ledger | 89,800 paise = 898 captures |
| Most units bought by one user | 4 (limit 4) |

### Limits

- One process, one machine, one run. This is a correctness experiment, not
  a load test, and its latencies are not SLO measurements.
- I3 (every capture confirmed or refunded within 15 minutes) holds here for
  every capture, but its guarantee for webhooks lost beyond the poller's
  reach needs the Phase 5 reconciler.
- The guard never refused a sale in this run, because Valkey and PostgreSQL
  agreed. Refusals and refunds are exercised by the saga tests and by the
  task 3.11 and 3.14 live checks.

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

## E6 fairness, 2026-10-02 (task 2.12)

**Result: PASS.** Before T0, join time did not predict queue position
(Spearman's ρ = −0.0008 over 100,000 joiners; the pass limit is |ρ| < 0.0127,
four standard errors). After T0, position was exactly the arrival order
(ρ = 1 over 10,000 joiners), and every one of them stood behind every
lottery joiner.

| File | Content |
|---|---|
| `e6-2026-10-02T0820-a5eaa55.log` | Output of `make fairness-e6` |
| `e6-2026-10-02T0820-a5eaa55.json` | The same report as JSON |

### Method

- Commit `a5eaa55`, `make fairness-e6` (`cmd/fairness`, defaults), at 13:50 IST
  against the local `make up` stack's Valkey. It calls the queue service and
  its Lua scripts directly, like E1, so the HTTP rate limits play no part:
  E6 tests the order, not the front door.
- 100,000 users join concurrently (256 in flight) before T0, each with its send
  time recorded; the tool waits until Valkey's clock passes T0; 10,000 users
  join one at a time; 10,000 more join concurrently. Positions are then read
  through the service, as a client would.

| Phase | Joins | Rate | Join p99 | Spearman ρ (send time, position) | Earliest tenth in the first tenth |
|---|---|---|---|---|---|
| Before T0, concurrent | 100,000 | 14,621/s | 57.5 ms | −0.000775 | 10.2% (a fair lottery: 10%) |
| After T0, one at a time | 10,000 | 4,195/s | 0.55 ms | exactly 1 | 100% |
| After T0, concurrent (reported only) | 10,000 | 28,539/s | 16.0 ms | 0.999637 | 99.3% |

### Caveats

- "Exactly 1" needs the client's order to be the arrival order, so those joins
  go one at a time. With many clients at once, a send time taken by the client
  can disagree with the order Valkey received the joins in by the time a request
  is in flight; the concurrent phase shows how much (ρ = 0.9996). The server's
  arrival order itself is FIFO by construction (`join.lua`) and pinned by the
  F1 model test.
- One run is evidence, not a distribution. ρ for a fair lottery varies from run
  to run with standard error 1/√(n−1) ≈ 0.0032; this run's −0.0008 is well
  inside that.

## E2 waiting-room stampede, 2026-10-02 (task 2.12)

**Result: the edge works as designed; the laptop cannot generate the design
load.** In both runs the origin answered the status document 4 to 6 times per
10 seconds, about 0.5 requests per second, whatever the edge served: from
1,478 to 81,434 polls per 10 seconds. At the sustainable load, all 50,000
simulated users flowed from join to admission token. Join latency missed the
150 ms p99 SLO in both runs (203 ms and 518 ms).

| File | Content |
|---|---|
| `e2-2026-10-02T0823-a5eaa55.log`, `-summary.json`, `-queue-metrics.txt` | Run A, the completion run: k6 output and summary, queue-svc's metrics at the end |
| `e2-2026-10-02T0821-a5eaa55.log`, `-summary.json`, `-queue-metrics.txt` | Run B, the design-scale attempt |

### Method

- Commit `a5eaa55`, `make load-e2` (`loadtest/e2/run.sh` and
  `waiting-room.js`), at 13:51 and 13:53 IST, against the local `make up` stack,
  with Windows kept awake for the duration (see Caveats).
- The event opens 8 seconds before k6 starts, so every join is after T0. Joins
  ramp from 0 to JOIN_PEAK per second in 10 s and hold there until USERS have
  joined. Every joined user polls `GET /v1/events/{id}/status` through the edge
  every POLL_EVERY seconds, so the polls grow with the queue. Once admission has
  reached them, users claim their turn (`POST /v1/queue/{id}/admit`), asking again
  once a second while it is not yet theirs.
- k6 runs in a container on the edge network and talks to NGINX directly, as a
  CDN would. The per-IP limits are switched off for the run, because one load
  generator is one IP; the per-user limits stay on.
- Edge counts are the status responses k6 received. Origin counts come from
  queue-svc's own request counter, read once a second by a sampler in the same
  k6 run. Both are kept per 10-second window.
- Users are arrival rates, not 50,000 virtual users: one iteration is one
  request by some user. 50,000 k6 virtual users would need tens of gigabytes.

| | Run A (completion) | Run B (design scale) |
|---|---|---|
| Users, join peak, admission rate, poll interval | 50,000, 1,000/s, 500/s, 30 s | 50,000, 2,000/s, 1,000/s, 3 s |
| Requests served, average | 1,956/s | 5,672/s |
| Iterations k6 could not start (`dropped_iterations`) | 128 | 340,140 |
| Joined, and tokens issued | **50,000 and 50,000** | 43,856 and 43,856 |
| Claims refused | 0 | 1,746, all `NOT_IN_QUEUE` (users whose joins were dropped) |
| Status polls at the edge | 253,343 | 483,366 |
| Status requests at the origin | 90 (5 in every 10 s window) | 51 (4 to 6 per window) |
| Polls per origin request | 2,814 overall, 3,334 at the plateau | 9,477 overall, up to 13,572 per window |
| Join p50, p95, p99 (k6) | 5.0, 83.9, **203.3** ms | 162.9, 377.5, **518.4** ms |
| Join requests over 100 ms inside queue-svc | 3.5% | 61% |

Run A's edge polls per 10 s window climb with the queue (1,478, 4,455,
7,432, 10,400, 13,383, 16,128, then about 16,670 for the rest of the run)
while the origin count stays at 5 in every window.

### Caveats

- **The design load did not fit on this laptop.** 50,000 users polling every
  3 s is about 16,700 requests per second on top of the joins. With the stack
  and k6 sharing 12 threads and 7.6 GB, the most served was about 5,700 per
  second (in profiling runs k6 alone used 2 to 4.6 cores); k6 dropped 340,140 of the
  planned iterations in run B. Run A keeps 50,000 users but polls every 30 s.
  The design-scale run needs a separate load generator (design doc 13.3).
- **The join SLO was missed in both runs.** queue-svc's own latency histogram
  shows the tail is at the origin: in run A, 87% of joins took under 10 ms
  inside queue-svc but 3.5% took over 100 ms. A CPU profile taken during the
  join phase (not committed) found no hot spot in HoldFast's code: about a third
  of queue-svc's CPU went to system calls and another third to the Go scheduler
  waiting for CPU, on a host where k6, NGINX, queue-svc and Valkey compete for
  the same cores. The SLO has to be checked on a host that does not also run
  the load generator.
- The origin sees about one status request every 2 seconds rather than every
  second: NGINX's one-second entries plus `proxy_cache_background_update` refresh
  in the background, so the origin is asked roughly once per expiry cycle.
- **An earlier attempt is not in this folder.** At 13:20 IST, during a run on
  uncommitted code, Windows entered Modern Standby and froze the stack and the
  load generator for 17 minutes; on resume, Valkey's clock had moved on and
  thousands of session slots had expired. That run was discarded (progress log
  E10), and the recorded runs held Windows awake with `SetThreadExecutionState`
  for their duration. Earlier trial runs on uncommitted code were discarded too.
