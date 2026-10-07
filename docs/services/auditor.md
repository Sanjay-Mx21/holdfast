# The invariant auditor

Continuously checks that HoldFast's invariants hold, from the stores
themselves, and publishes the number of violations of each. Binary:
`cmd/auditor`. Code: `internal/auditor`. Built in Phase 5 (task 5.1).

## What it checks

Every `AUDIT_INTERVAL` (15 s, and once at start) it runs one check per
invariant and sets `holdfast_invariant_violations{invariant}` to the number
of violations found. **Every value must be 0.** The alert
`InvariantViolation` pages when one is not (runbook
`docs/runbooks/auditor.md`).

| Invariant | Counted as a violation | Source |
|---|---|---|
| **I1** no oversell | An event whose confirmed bookings hold more units than its capacity, or whose guard counter `sold` exceeds it | `booking.bookings`, `booking.event_inventory` |
| **I2** no double charge | A payment intent whose capture is booked twice in the ledger | `payment.ledger_entries` |
| **I3** money safety | Money captured more than `MONEY_DEADLINE` (15 minutes) ago that has not ended as a confirmed booking or a refunded intent | `payment.payment_intents`, the ledger, `booking.bookings` |
| **I4** per-user cap | An (event, user) pair whose units held for payment or bought exceed the event's limit, by the bookings or by the guard's per-user counter | `booking.bookings`, `booking.user_event_purchases`, `booking.events` |
| **I5** no lost units | A hold still in inventory's expiry index more than `HOLD_GRACE` (5 minutes) past its expiry, by Valkey's clock | Valkey `inv:events`, `inv:{E}:expiry` |

Beside the invariants it compares each provisioned event's Valkey pool with
PostgreSQL (task 5.3): `holdfast_inventory_drift_units{event}` is how many
units `inv:{E}:avail` offers beyond capacity − sold − units in pending
bookings. Healthy inventory never offers more (a hold not yet booked only
lowers it); more means Valkey lost writes or drifted, and the remedy is a
rebuild (`holdfastctl inventory rebuild`, RB-INV-4). Valkey is read before
PostgreSQL, so a booking cancelled between the reads cannot look like
drift; a purchase completing between them can, for one check, which is why
`InventoryDrift` waits 2 minutes.

The checks do not re-read what a database constraint already forces
(`sold <= capacity`, at most 10 units per booking): each recomputes the
invariant from independent records, so a bug in the code that maintains a
counter shows up here.

If one check fails (PostgreSQL or Valkey unreachable), that invariant keeps
its last published value, the others are still published, the run counts as
an error, and `holdfast_auditor_last_success_timestamp_seconds` does not
move: after 2 minutes `AuditorStale` pages. Before its first successful
check the gauges do not exist, so the dashboard's tiles show grey "no data",
never a green 0 nobody measured.

## Why it may read every schema

Services never read each other's tables (AGENTS.md, rule 4). The auditor is
the design's one exception (design doc 8.1): an observer, not a participant.
It never writes:

- its PostgreSQL connections start every transaction read-only
  (`postgres.NewReadOnlyPool`, `default_transaction_read_only`), so a write
  fails even with credentials that could write (tested);
- in production it would also use a read-only role across the schemas;
  locally it shares the stack's DSN;
- it only counts members of inventory's Valkey keys.

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `POSTGRES_*`, `VALKEY_*`)
are defined in `internal/platform/config`. It serves only the admin port
(`ADMIN_ADDR`): `/metrics`, `/livez`, `/readyz` (PostgreSQL and Valkey),
`/buildz`, pprof. Compose maps it to 9097.

| Variable | Default | Meaning |
|---|---|---|
| `AUDIT_INTERVAL` | `15s` | Time between checks (1 s to 10 m) |
| `MONEY_DEADLINE` | `15m` | I3: how long captured money may stay unresolved |
| `HOLD_GRACE` | `5m` | I5: how long past its expiry a hold may stay unreleased |

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_invariant_violations` | `invariant` | Violations found by the latest check; must be 0 |
| `holdfast_inventory_drift_units` | `event` | Units Valkey offers beyond PostgreSQL's free units, per provisioned event (task 5.3); must be 0, `InventoryDrift` fires after 2 minutes |
| `holdfast_auditor_runs_total` | `result` | Checks: ok, error (some invariant could not be checked) |
| `holdfast_auditor_last_success_timestamp_seconds` | | When every invariant was last checked; `AuditorStale` fires after 2 minutes |

## Tests

`internal/auditor/auditor_integration_test.go` runs against a database and a
Valkey logical database of its own (`testenv.NewIsolated`: `holdfast_audit`
and database 3, emptied before each test). It covers:

- a clean store with no violations;
- each invariant counted exactly, with near misses that must not count (a
  capture a minute old, a user exactly at the limit, a hold within the
  grace);
- that its connections cannot write;
- that a failed check keeps the others.
