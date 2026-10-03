# payment-svc

Takes the money for bookings: one payment intent per booking, an order at the
payment provider (PSP) for each intent, the provider's webhooks, a
double-entry ledger, and status polling for intents whose webhook never came.
Binary: `cmd/payment`. Code: `internal/payment` (the provider client in
`internal/payment/psp`). Schema: `payment` (migrations in
`db/migrations/payment`, queries in `internal/payment/queries`).

**Status:** Phase 3 in progress. Built: the schema (task 3.8), the service
(task 3.9), and mockpsp, the local provider (task 3.10,
`docs/services/mockpsp.md`); booking-svc calls payment-svc in Compose.
Refunds of bookings the final guard refused: task 3.11.

## Responsibilities

- Create one intent per booking, however often booking-svc asks (invariant
  I2: at most one captured payment per booking).
- Open the provider's order with the intent ID as its idempotency key.
- Apply the provider's webhooks exactly once, forward only, and book every
  capture and refund in the ledger.
- Find out what happened to intents with no news (status polling).
- Refund the bookings booking-svc cannot confirm.
- Announce every outcome on `holdfast.payment.v1` (the outbox).

## Internal gRPC API

`holdfast.payment.v1.PaymentService` (`proto/holdfast/payment/v1`) on
`GRPC_ADDR` (`:7070`), through `internal/platform/grpcx`. Only booking-svc may
call it (service tokens; `GRPC_TRUSTED_CALLERS`).

### `CreateIntent`

Request: `booking_id`, `event_id`, `amount_paise`, `expires_at` (the booking's
payment deadline). Response: `intent_id`, `checkout_url`.

- The first call creates the intent (`CREATED`), then the provider's order,
  and records the order and checkout URL. A later call for the same booking
  returns the same intent and URL without calling the provider again.
- If the provider is unreachable after the intent was created, the call fails
  `UNAVAILABLE` and the next call finishes the job: the intent ID is the
  order's idempotency key, so the provider returns the same order.

| Code | Reason | When |
|---|---|---|
| `INVALID_ARGUMENT` | `INVALID_REQUEST` | Missing or malformed IDs, a non-positive amount, no expiry |
| `FAILED_PRECONDITION` | `INTENT_CONFLICT` | The booking already has an intent for another amount |
| `UNAVAILABLE` | `PSP_UNAVAILABLE` | The provider is unreachable or the breaker is open; retry |

booking-svc's typed client is `payment.Client`.

## The provider client (`internal/payment/psp`)

The provider's API: `POST /v1/orders` and `POST /v1/refunds` (with
`Idempotency-Key`: the intent ID), `GET /v1/orders/{id}`, and
`GET /v1/settlements` (the report the Phase 5 reconciler reads), with a
bearer API key. mockpsp implements it locally.

- Each attempt is bounded by `PSP_TIMEOUT`; the caller's context bounds the
  whole call.
- Network errors, timeouts, 429 and 5xx are retried, three attempts in all,
  with exponential backoff and jitter (100 ms, then 200 ms, each halved
  plus random). Every call is idempotent at the provider, so retrying is safe.
- Other 4xx are not retried (`psp.ErrRejected`).
- A **circuit breaker** opens after 5 consecutive calls that ended
  unavailable, and fails calls at once for 30 s. Then one trial call is let
  through: success closes it, failure opens it again. A caller giving up
  (its context ended) does not count against the provider.

## Webhooks: `POST /v1/webhooks/psp`

On the public port. The provider signs each callback:

```http
POST /v1/webhooks/psp
X-PSP-Timestamp: 1790000000
X-PSP-Signature: <hex HMAC-SHA256 of "<timestamp>.<body>" with PSP_WEBHOOK_SECRET>

{"id":"evt_...","type":"payment.captured","orderId":"order_...",
 "paymentId":"pay_...","amountPaise":500000,"createdAt":"..."}
```

1. Bodies over 64 KiB: 413.
2. The signature is checked in constant time, and the timestamp must be
   within `WEBHOOK_TOLERANCE` (5 minutes) of now, either way. Failure: 401,
   and nothing is recorded.
3. In one transaction:
   - the webhook is recorded by its event ID; a duplicate delivery inserts
     nothing and changes nothing;
   - the intent moves, forward only;
   - the move is booked in the ledger;
   - the event is written to the outbox.
4. 200 tells the provider to stop. 503 (a database failure) makes it retry,
   which deduplication makes safe. A body that cannot be read, a webhook for
   an order HoldFast never made, or a move the intent has already passed is
   acknowledged with 200.

| Webhook | Intent move | Ledger | Event |
|---|---|---|---|
| `payment.captured` | `CREATED`, `FAILED` or `EXPIRED` to `CAPTURED` (a capture always wins: money moved) | D `psp_receivable`, C `unearned_revenue:<event>` | `payment.captured.v1` |
| `payment.failed` | `CREATED` to `FAILED` | | `payment.failed.v1` |
| `order.expired` | `CREATED` to `EXPIRED` | | `payment.expired.v1` |
| `refund.completed` | `REFUND_PENDING` to `REFUNDED` | the capture's entries reversed | `refund.completed.v1` |

A capture whose amount differs from the intent's is **never** booked: it is
logged as an error and counted (`holdfast_payment_amount_mismatch_total`) for
a human (reconciliation, Phase 5).

## Status polling

Every `POLL_INTERVAL` (30 s), each replica claims up to `POLL_BATCH` intents
that are still `CREATED` with an order `POLL_AFTER` (2 minutes) after
creation (`FOR UPDATE SKIP LOCKED`, so replicas take disjoint batches). It
takes the least recently polled first, never polled before all, and stamps
`polled_at` on every poll, failed or not: intents whose polls keep failing
cannot starve the rest (P34). It asks the provider for each order's status,
with a 3-second deadline per call.
A captured, failed or expired order gets the same move, ledger entries and
event as its webhook would have. The webhook arriving later then changes
nothing.

## Refunds

payment-svc consumes `holdfast.booking.v1` as the group `payment-refunds`
(`internal/payment/refunds.go`).

1. For `booking.refund_required.v1`, one transaction records the message's
   `ce_id` (a redelivery changes nothing) and moves the booking's intent
   `CAPTURED` to `REFUND_PENDING`.
2. After the commit, on every delivery while the intent is
   `REFUND_PENDING`, it asks the provider for a full refund
   (`POST /v1/refunds`, keyed by the intent ID, so repeats are harmless).
3. The provider's `refund.completed` webhook finishes it (`REFUNDED`, the
   capture's ledger entries reversed, `refund.completed.v1`), and booking-svc
   closes the booking.

Errors:

- The provider unreachable: the message is retried.
- The provider refusing the refund: dead-lettered for a human (runbooks
  RB-3 and RB-4 in `docs/runbooks/saga.md`; `holdfastctl refund --booking
  ID` asks again once the cause is fixed).
- A booking with no intent: logged and skipped (P30).
- An intent that was never captured: left alone.

## The ledger

`payment.ledger_entries` holds double-entry transactions: each capture debits
`psp_receivable` and credits the event's `unearned_revenue:<event ID>`. A
completed refund reverses both. A deferred constraint trigger refuses, at
commit, any transaction whose debits and credits differ.

## Tracing

Intents store the trace context of the request that created them
(booking-svc's `POST /v1/bookings`; migration `payment/00004`). A webhook
arrives in a trace of its own, since providers do not propagate ours, so
the work it causes runs in a span (`payment.apply_webhook`, or
`payment.apply_poll` for the poller) that continues the stored trace and
links to the webhook's. The capture's event, booking-svc's saga and
inventory's confirmation follow it: one trace per purchase.

## Events (outbox)

Written to `payment.outbox` in the same transaction as the change, with the
request's trace context. The shared outbox relay (`internal/platform/outbox`,
schema `payment`, source `payment-svc`) publishes them to `holdfast.payment.v1`,
keyed by booking ID. Payloads are `holdfast.events.v1` messages.

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `POSTGRES_*`, `KAFKA_*`,
`OTEL_*`) are defined in `internal/platform/config` and
`internal/platform/otel`.

| Variable | Default | Meaning |
|---|---|---|
| `GRPC_ADDR` | `:7070` | Internal gRPC API |
| `GRPC_TRUSTED_CALLERS` | empty | `name=/path/to/key.pub` pairs; Compose trusts booking-svc |
| `TOKEN_LEEWAY` | `5s` | Clock skew allowed on service tokens |
| `PSP_BASE_URL` | `http://mockpsp:8080` | The provider's API |
| `PSP_API_KEY` | empty | Bearer key for the provider |
| `PSP_WEBHOOK_SECRET` | required | Webhook signing secret, at least 32 characters |
| `PSP_TIMEOUT` | `2s` | Deadline of one provider attempt |
| `WEBHOOK_TOLERANCE` | `5m` | How far a webhook's timestamp may be from now (30 s to 1 h) |
| `POLL_INTERVAL` | `30s` | Status polling period |
| `POLL_AFTER` | `2m` | Age at which a `CREATED` intent is polled |
| `POLL_BATCH` | `20` | Intents polled per pass |
| `OUTBOX_BATCH` | `500` | Events published per relay transaction |
| `OUTBOX_INTERVAL` | `200ms` | Relay pause after a pass that found less than a full batch |

Compose's `PSP_API_KEY` and `PSP_WEBHOOK_SECRET` are development placeholders
shared with mockpsp (`.env` can override them); real ones come from a secret
store. Locally, Compose maps the public port to
8084, the admin port to 9094 and gRPC to 7072.

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_payment_intents_created_total` | | Intents created |
| `holdfast_webhooks_total` | `type`, `duplicate` | Webhooks by type (the provider's four, plus `malformed`, `bad_signature`, `other`) |
| `holdfast_payment_captures_total` | `via` | Captures learned by `webhook` or `poll` |
| `holdfast_payment_amount_mismatch_total` | | Captures refused for a different amount: page a human |
| `holdfast_payment_refund_requests_total` | `result` | Refund requests: requested, retry, rejected |
| `holdfast_payment_polls_total` | `result` | Polled orders by provider status, `provider_error`, or `error` for a failed pass |
| `holdfast_psp_requests_total` | `op`, `result` | Provider calls: ok, retried, rejected, unavailable, breaker_open |
| `holdfast_psp_breaker_state` | | 0 closed, 0.5 half-open, 1 open |
| `holdfast_outbox_*` | `schema` | The relay, as in booking-svc |
| `holdfast_grpc_server_handled_total` | `method`, `code` | gRPC calls |
| `holdfast_http_*` | `route`, `code` | RED metrics per route pattern |
