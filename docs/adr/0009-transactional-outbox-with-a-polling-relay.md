# 0009. Transactional outbox with a polling relay

- Status: accepted
- Date: 2026-10-03

## Context

When a booking is confirmed, two things must both happen or neither:

- the row changes in PostgreSQL;
- `booking.confirmed.v1` reaches Kafka.

Writing to the database and then publishing loses the event if the process
dies in between. Publishing first announces a change that may then roll
back. There is no transaction spanning PostgreSQL and Kafka.

The usual alternatives:

- **Two-phase commit:** neither system supports it here.
- **Change data capture (Debezium) from the write-ahead log:** robust, but
  it adds Kafka Connect, replication slots and their operations to a
  laptop-scale project.
- **A polling relay over an outbox table:** simple, and plenty for the event
  rates of a ticket sale. Confirmations run at tens to low hundreds per
  second. The hot path (holds) never touches Kafka.

## Decision

- Each service schema has an `outbox` table. A state change and its event
  are written **in the same transaction**. Each event is a Protobuf payload
  with a UUIDv7 `event_id` (the `ce_id`), its topic, its key (the booking ID)
  and the request's trace context.
- `internal/platform/outbox` relays the rows to Kafka:
  - **One leader per schema**, by PostgreSQL advisory lock, so one
    booking's events are published in commit order.
  - **Each pass** claims up to `OUTBOX_BATCH` unpublished rows (`FOR UPDATE
    SKIP LOCKED`), publishes them with `acks=all` and idempotence, and marks
    them published in the same transaction.
  - **Failures and crashes:** a failed publish leaves the rows for the next
    pass. A crash after publishing but before marking publishes them again,
    under the same IDs.
  - **Cleanup:** published rows are deleted after 7 days.
  - **Metrics:** `holdfast_outbox_lag_seconds`, `holdfast_outbox_pending`
    and `holdfast_outbox_relay_leader`.
- Delivery is therefore **at least once**, and every consumer deduplicates
  on `ce_id` (ADR 0008).

## Consequences

- No event is lost and none is announced for a change that rolled back
  (invariants I2 and I3 depend on it). A crash costs only duplicates, which
  consumers drop. `TestACrashAfterPublishingRepublishesTheSameEvents` pins
  the duplicates' IDs.
- Latency is up to one relay interval (200 ms by default) plus Kafka's.
  Fine for the saga, and the buyer's request never waits for it.
- The outbox table is extra write load in the same transaction, and a
  growing backlog is the signal to watch if Kafka is down: rows accumulate
  safely and drain when it returns.
- One relay per schema is a throughput ceiling (one ordered publisher).
  Should it bind, we can partition the relay by key range or move to CDC,
  without changing producers or consumers.
