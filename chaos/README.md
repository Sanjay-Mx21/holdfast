# Chaos experiments

The design's experiments E3 (payment chaos) and E4 (infrastructure chaos),
run against the whole local stack (task 5.5). Each one drives real
purchases with simulated buyers while faults strike, then waits for the
stack to settle and checks the invariants with the auditor. Results go to
`loadtest/results/`.

| Command | What breaks | Passes when |
|---|---|---|
| `make chaos-e3` | mockpsp's fault mix for the whole run: 20% duplicate webhooks, 10% delayed by 30 to 90 s, 5% lost, 10% failed payments, 5% API timeouts | I2 is 0 throughout; every payment the provider captured is known to HoldFast and resolved (confirmed or refunded) within 15 minutes; every invariant is 0 |
| `make chaos-e4` | One after another: the Valkey primary killed (then runbook RB-2 practised), booking-svc killed mid-saga, Kafka restarted, PostgreSQL cut off for 5 s | Every fault recovers (times recorded); the same settlement and invariant checks |

Both start from a stack brought up with `make up`, and restore it
afterwards (and E4 fails Valkey back to its first primary).

Two runbook drills use the same pieces (task 5.7; `docs/runbooks/README.md`):

| Command | What breaks | Passes when |
|---|---|---|
| `make drill-rb5` | The payment provider is down for 60 s mid-sale | Admissions pause and resume by themselves (the timeline is recorded); the same settlement and invariant checks |
| `make drill-rb4` | Valkey loses a sale, and the provider goes down as a buyer pays for the phantom units: the refund is given up | The operator's `holdfastctl refund` refunds the buyer (timed), RB-2 repairs the drift, every invariant 0 |

## The pieces

- **`cmd/buyers`**: simulated buyers. Each joins the waiting room, waits for
  its turn and claims it, holds units, books them, pays at mockpsp's
  checkout, and waits for its booking to settle. Every request is repeated
  on failure with the same idempotency key, so a buyer never buys twice
  whatever fails underneath. It reports every buyer's outcome, the retries
  per stage and the time from payment to confirmation.
- **`chaos/compose.yaml`**: the overlay the experiments run under. Buyers
  name themselves in `X-Dev-User-Id` and join without proof of work (the
  design's load-test bypasses, refused in production). Per-IP limits are
  lifted, since every buyer comes from one address. booking-svc,
  payment-svc and queue-svc reach PostgreSQL through **Toxiproxy**
  (`chaos/toxiproxy.json`), which E4 uses to cut them off; the auditor
  keeps its direct connection, so the observer sees through the fault.
- **`chaos/lib.sh`**: the shared steps (the overlay, events, mockpsp's
  faults, metrics, the settlement check).

Scale with `BUYERS`, `CONCURRENCY` and `RATE` (the event's admission rate);
E4's faults are `GAP` seconds apart. `OUT=/tmp` keeps a trial run out of
`loadtest/results/`.

## What the buyers' outcomes mean

| Outcome | Meaning |
|---|---|
| `confirmed` | Paid, and the booking confirmed |
| `refunded`, `cancelled` | Paid, but the booking could not be confirmed: the money must come back (I3) |
| `declined` | The provider declined the payment (mockpsp's failure fault); the booking runs out its payment window |
| `sold_out` | No unit left |
| `not_admitted`, `turn_expired` | The waiting room never admitted the buyer in time, or the session ran out |
| `<stage>_failed` | A request kept failing after every retry, or was refused: in E4, `book_failed` is a buyer whose hold the RB-2 rebuild released before they booked ("hold expired, try again") |
| `unsettled_<status>` | Paid, but the booking was still in that status when the buyer stopped waiting; the settlement check then waits for the stack |
