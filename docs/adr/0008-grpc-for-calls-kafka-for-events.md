# 0008. gRPC for synchronous calls, Kafka for asynchronous events

- Status: accepted
- Date: 2026-10-03

## Context

From Phase 3, services talk to each other in two different ways.

- **booking-svc needs answers now.** It must know that a hold exists and is
  now PAYING (inventory-svc), and it needs a checkout URL (payment-svc),
  before it can answer the buyer's `POST /v1/bookings`. The caller waits,
  and it needs a clear yes or no, or a retryable failure.
- **Other work is a consequence of something that already happened.** A
  capture must eventually confirm a booking; a refused sale must eventually
  be refunded. Nobody is waiting for these. They must survive a service
  being down, and happen exactly once in effect, however often they are
  delivered.

HTTP/JSON for everything would make the second kind fragile: retries, ordering
and durability would all live in the caller. A broker for everything would
make the first kind slow and awkward: request and reply over topics, with no
deadline the caller can rely on.

## Decision

- **Synchronous, internal calls use gRPC** (`proto/holdfast/*/v1`, generated
  with Buf). All of them go through `internal/platform/grpcx` on an
  internal-only port, with:
  - per-service Ed25519 tokens and a per-method allowlist (deny by default);
  - required deadlines (800 ms by default; 8 s for payment, whose calls
    include the provider's);
  - retries of `UNAVAILABLE` only;
  - error reasons in `ErrorInfo`, so clients map them to typed errors.
- **Asynchronous work uses Kafka events** (`proto/holdfast/events/v1`,
  CloudEvents headers). Each service writes its events to its own outbox
  (ADR 0009) and consumes other services' topics as a consumer group, with
  manual commits after the database transaction, retries with backoff, and
  a dead-letter topic.
  - Every consumer deduplicates on `ce_id` in the same transaction as the
    effect, and tolerates IDs it does not know.
  - Topics are keyed by booking ID, so one booking's events stay in order.
- The buyer-facing API stays HTTP/JSON behind the edge. Contracts change only
  by addition (`buf breaking` in CI); a breaking change means a `v2`.

## Consequences

- A slow or failed dependency on the synchronous path surfaces at once as
  503 with `Retry-After`. The booking API is idempotent and resumable
  (design doc 9.5), so the buyer's retry finishes the job.
- Asynchronous steps survive any one service being down: events wait in the
  outbox or in Kafka. The price is eventual consistency. A booking is
  PENDING_PAYMENT for a moment after the capture, and consumer lag must be
  watched (`holdfast_kafka_consumer_lag`).
- Two transports to operate and observe. Traces span both: `traceparent`
  travels in gRPC metadata and in Kafka headers, and is stored with outbox
  rows.
- Poison messages land in `<topic>.dlq.v1`. `holdfastctl dlq replay`
  (runbook RB-3) puts them back once the cause is fixed. This is safe
  because every consumer is idempotent.
