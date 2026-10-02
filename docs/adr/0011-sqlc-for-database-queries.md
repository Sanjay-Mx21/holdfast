# 0011. sqlc for database queries

- Status: accepted
- Date: 2026-10-02

## Context

From Phase 3, booking-svc and payment-svc do most of their work in
PostgreSQL: bookings and their state machine, idempotency keys, the outbox,
processed messages, intents and the ledger. Their correctness rests on exact
SQL: conditional updates (compare-and-set on a status), `FOR UPDATE SKIP
LOCKED` batches, `ON CONFLICT DO NOTHING` deduplication. An ORM hides that SQL
and makes it hard to express; hand-written `pgx` calls keep the SQL visible
but scan rows by position, so a column change breaks them at run time.
Drop 1's final guard (`internal/booking/guard`) is hand-written `pgx`, which
was fine for three statements.

## Decision

Write SQL by hand and generate typed Go for it with sqlc
(`sqlc.yaml`, queries in `internal/booking/queries`, generated code in
`internal/booking/bookingdb`, committed).

- The schema sqlc checks against is the migrations themselves
  (`db/migrations/booking`), so a query that names a missing column or uses
  the wrong type fails at `make gen`, not in production.
- Generated code uses `pgx/v5` directly; UUIDs are `uuid.UUID`, times
  `time.Time`.
- Every state change stays one conditional statement, as AGENTS.md requires:
  the queries are where that rule is visible and reviewed.
- CI regenerates with the pinned sqlc (1.31.1) and fails if the committed
  code is stale.

## Consequences

- Queries are reviewed as SQL; the Go around them is boilerplate nobody
  writes by hand.
- sqlc's parser must understand everything in the migrations. It handles the
  `plpgsql` trigger in `00002`; anything it cannot parse must go in a
  migration sqlc is not pointed at, or be expressed differently.
- Dynamic queries (optional filters) do not fit sqlc well; write them with
  `pgx` directly when they come, as the guard does today.
- Contributors need sqlc for `make gen` (`make tools` installs it); building
  and testing do not.
