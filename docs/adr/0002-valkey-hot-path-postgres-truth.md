# 0002. Holds in Valkey, truth in PostgreSQL

- Status: accepted
- Date: 2026-09-25

## Context

At sale opening a single event receives tens of thousands of hold attempts
per second. Taking a PostgreSQL row lock per attempt serialises everything
on one hot row. Money and bookings, however, need durable transactional
truth.

## Decision

- Holds live in Valkey. Every state change is one atomic Lua script over keys
  that share the hash tag `{eventID}`, so a script touches one cluster slot.
- Confirmed sales pass the PostgreSQL final guard (`internal/booking/guard`):
  conditional writes in the same transaction as the booking.
- Valkey state is a projection that can be rebuilt from PostgreSQL
  (`holdfastctl inventory provision`).

## Consequences

- Two stores must agree, so we need late-confirmation handling (implemented),
  reconciliation (Phase 5) and a rebuild runbook (docs/runbooks/inventory.md).
- AOF with `appendfsync everysec` can lose about one second of holds on a
  crash; the rebuild restores the pool from PostgreSQL.
- One event's throughput is bounded by one Valkey core; many events scale
  horizontally across shards.
