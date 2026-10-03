# booking-svc

Turns a buyer's hold into a booking that waits for payment, and carries the
booking to its end: confirmed, cancelled or refunded. Binary: `cmd/booking`.
Code: `internal/booking`. Schema: `booking` (migrations in
`db/migrations/booking`, queries in `internal/booking/queries`).

**Status:** built in Phase 3: the schema and its state machine (task 3.5),
the booking API and the deadline job (task 3.6), the outbox relay (task
3.7), payment intents from payment-svc (task 3.9), the saga that confirms,
cancels or refunds bookings from payment events (task 3.11), and the
operator commands of runbooks RB-3 and RB-4 (task 3.14). Buyers are
identified by auth-svc's access tokens since task 4.2.

## Responsibilities

- Create a booking for a hold, once, however often the request is retried.
- Protect the hold for the payment window (inventory-svc, over gRPC).
- Get the booking's payment intent and checkout URL (payment-svc, over gRPC).
- Record every state change and its event in one transaction (the outbox).
- Confirm a paid booking if the final guard takes the sale, ask for a refund
  if it refuses, cancel one whose payment failed or expired (the saga).
- Settle the hold in inventory-svc after each decision.
- Cancel bookings whose payment deadline passed.
- Show buyers their own bookings, and nobody else's.
- Publish the event catalog: what is on sale and when (task 4.5).

## API

### `POST /v1/bookings`

```http
POST /v1/bookings
Authorization: Bearer <access token>   (auth-svc; X-Dev-User-Id only with DEV_IDENTITY)
Idempotency-Key: <8 to 255 printable characters>
Content-Type: application/json

{"eventId": "0196f0c1-...", "holdId": "1f0e..."}
```

```http
HTTP/1.1 201 Created

{"bookingId":"01a0...","eventId":"0196...","holdId":"1f0e...","quantity":2,
 "amountPaise":500000,"status":"PENDING_PAYMENT","paymentDeadline":"2026-10-05T12:07:00Z",
 "intentId":"01a1...","checkoutUrl":"https://psp.example/pay/order_..."}
```

- The hold must be the buyer's own, and still HELD or PAYING. booking-svc
  calls inventory-svc's `GetHold`, then `MarkPaying`, which protects the hold
  for the payment window (10 minutes). The booking's `paymentDeadline` is
  the end of that window minus `PAYMENT_GRACE` (3 minutes; design doc 6.1),
  so a capture reported a little after the deadline still finds its units.
  `amountPaise` is the quantity times the event's price.
- booking-svc then asks payment-svc for the booking's intent (`CreateIntent`,
  idempotent per booking; the intent expires at the payment deadline) and
  returns its `intentId` and `checkoutUrl`. Without `PAYMENT_GRPC_ADDR` the
  booking is created without them.

**Idempotency** (design doc 9.5). The `Idempotency-Key` is required and
belongs to the buyer.

- The first request with a key claims it. Its answer, success or definitive
  failure, is stored with the key, and every retry with the same key gets
  exactly that answer, byte for byte, with `Idempotent-Replayed: true`.
- The same key with a different request (another event or hold) is refused
  with 422 `IDEMPOTENCY_KEY_REUSED`.
- If an attempt fails part-way (inventory unreachable: 503 with
  `Retry-After`), or payment-svc or its provider is (also 503), the key
  stays in progress and a retry with the same key
  **resumes**: every step is idempotent (`GetHold`, `MarkPaying`, one booking
  per hold, the intent keyed by the booking). Concurrent identical requests
  produce one booking and one event, and all get the same answer.
- A second key for the same hold returns the existing booking.
- Keys are deleted after 24 hours.

Errors: 400 `INVALID_REQUEST`, `INVALID_IDEMPOTENCY_KEY` or
`IDEMPOTENCY_KEY_REQUIRED`; 404 `HOLD_NOT_FOUND` (also for someone else's
hold) or `EVENT_NOT_FOUND`; 409 `HOLD_NOT_AVAILABLE` (released, sold or
expired); 422 `IDEMPOTENCY_KEY_REUSED`; 503 when inventory-svc, payment-svc
or the payment provider is unreachable (retry with the same key).

### `GET /v1/bookings/{bookingID}`

The buyer's own booking, in the same shape. Anyone else's, or one that does
not exist, is 404 `BOOKING_NOT_FOUND`, so booking IDs cannot be probed.

### `GET /v1/events` and `GET /v1/events/{eventID}`

The event catalog (`internal/booking/catalog`), public, `Cache-Control:
public, max-age=60` (the edge caches it). An event:

```json
{"eventId":"0196f0c1-...","name":"Coldplay, Mumbai","saleOpensAt":"2026-10-05T12:00:00Z",
 "verifiedOnlyUntil":"2026-10-05T12:15:00Z","perUserLimit":4,"unitPricePaise":250000,"capacity":1000}
```

`verifiedOnlyUntil` and `agentLockoutUntil` are present only when the event
has those policy windows. The list (`{"events":[...]}`) holds sales still to
open, soonest first, then sales opened within the last week, most recent
first, at most 50. Live state is not here: the queue's state comes from
queue-svc's status document, units left from inventory-svc's availability.
An unknown event is 404 `EVENT_NOT_FOUND`; a malformed ID 400
`INVALID_REQUEST`.

## States

```text
PENDING_PAYMENT ──captured, guards pass──▶ CONFIRMED
       │ ──payment failed or expired──▶ CANCELLED ──late capture, guards pass──▶ CONFIRMED
       │                                    └──late capture, a guard refuses──▶ REFUND_REQUIRED
       └──captured, a guard refuses──▶ REFUND_REQUIRED ──refund completed──▶ REFUNDED
```

A trigger on `booking.bookings` refuses every other move, even from a raw
`UPDATE`. Moves are compare-and-set (`UPDATE ... WHERE status = <expected>`),
so two actors cannot both move a booking.

## The saga

booking-svc consumes `holdfast.payment.v1` as the consumer group
`booking-saga` (`internal/booking/saga.go`; design doc 6.4, 7.2, 9.7).
Each message is applied in one transaction:

1. The message's `ce_id` is recorded in `processed_messages`. If it is
   already there (a redelivery), nothing changes.
2. The booking is locked (`SELECT ... FOR UPDATE`). An unknown booking is
   logged and skipped (P30).
3. The decision, its status move and its outbox event are written.

| Payment event | Booking | Then |
|---|---|---|
| `payment.captured.v1` | `PENDING_PAYMENT` or `CANCELLED`: `guard.Reserve` (event capacity, per-user cap) in a savepoint. Accepted: `CONFIRMED` and `booking.confirmed.v1`. Refused: `REFUND_REQUIRED` and `booking.refund_required.v1` (reason `GUARD_REJECTED`, or `LATE_CAPTURE` for a cancelled booking) | Anything else: already decided, nothing to do |
| `payment.failed.v1`, `payment.expired.v1` | `PENDING_PAYMENT`: `CANCELLED` and `booking.cancelled.v1` (`PAYMENT_FAILED` or `PAYMENT_EXPIRED`) | |
| `refund.completed.v1` | `REFUND_REQUIRED`: `REFUNDED` and `booking.refunded.v1` | |

A capture for a cancelled booking is a **late capture** (design doc 7.2): it
is honoured when the guard allows and refunded when it does not. Both
outcomes are counted in `holdfast_late_confirm_total`.

**Inventory is settled after the commit**, from the booking's status, not
from the message:

- `CONFIRMED`: inventory-svc's `Confirm` makes the hold SOLD. For a released
  hold it re-takes the units (`late`).
- `CANCELLED` or `REFUND_REQUIRED`: `ReleaseForFailedPayment` releases the
  hold, so every hold ends SOLD or RELEASED (I5).

Both calls are idempotent and run on every delivery. A failed call (for
example, inventory-svc unreachable) fails the message, which is retried. The
transaction is skipped the second time, but the call is made again.
Unreadable messages, and inventory answers that cannot improve (no such hold
or event), go to the dead-letter topic for a human. PostgreSQL already holds
the decision.

payment-svc refunds a `REFUND_REQUIRED` booking (`booking.refund_required.v1`;
see `docs/services/payment.md`). The provider's completion arrives as
`refund.completed.v1`.

Operators recover stuck bookings through the same path, never by editing
rows (`docs/runbooks/saga.md`):

- `holdfastctl dlq replay` (RB-3) puts dead-lettered events back;
- `holdfastctl refund --booking ID` (RB-4) asks for a stuck refund again.
  It publishes a new `booking.refund_required.v1` with reason `OPERATOR`,
  for a `REFUND_REQUIRED` booking only. The decisions behind the saga are
  ADRs 0008 to 0010.

## Background work

The **deadline job** runs in every replica every `DEADLINE_SCAN_INTERVAL`
(5 s): it claims overdue `PENDING_PAYMENT` bookings with `FOR UPDATE SKIP
LOCKED` (concurrent replicas take disjoint batches), moves each to
`CANCELLED` and writes `booking.cancelled.v1` (reason `PAYMENT_EXPIRED`) in
the same transaction. inventory-svc releases the hold itself when its payment
window ends (or the saga releases it earlier, when `payment.expired.v1`
arrives). Once an hour the job deletes idempotency keys older than a day.

## Events (outbox)

Written to `booking.outbox` in the same transaction as the change they
describe, with the request's trace context in `headers`, and published to
`holdfast.booking.v1` (keyed by booking ID) by the **outbox relay**
(`internal/platform/outbox`). Payloads are `holdfast.events.v1` messages; the
`ce_id` header is the row's `event_id`, which consumers deduplicate on.

- One relay leads across replicas (a PostgreSQL advisory lock), so one
  booking's events are published in the order they were committed.
- Each pass publishes up to `OUTBOX_BATCH` rows and marks them published in
  the same transaction. A failed publish leaves them for the next pass; a
  crash after publishing republishes them (delivery is at least once).
- The relay continues the stored trace: its `publish` span joins the request
  that caused the event.
- Published rows are deleted after 7 days.
- `holdfast_outbox_lag_seconds` (age of the oldest unpublished event) and
  `holdfast_outbox_pending` show whether it keeps up.

| Event | When |
|---|---|
| `booking.created.v1` | A booking was created |
| `booking.confirmed.v1` | The payment was captured and the final guard took the sale |
| `booking.cancelled.v1` | Its deadline passed, or its payment failed or expired |
| `booking.refund_required.v1` | The payment was captured but the guard refused the sale; payment-svc refunds it |
| `booking.refunded.v1` | The refund completed; the booking is over |

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `POSTGRES_*`, `OTEL_*`) are
defined in `internal/platform/config` and `internal/platform/otel`.

| Variable | Default | Meaning |
|---|---|---|
| `ACCESS_JWKS_URL` | none | auth-svc's key set (`http://auth:8080/.well-known/jwks.json` in Compose): buyers are identified by its access tokens. Required unless `DEV_IDENTITY` is on; readiness waits until a key is known |
| `DEV_IDENTITY` | `false` | Also accept `X-Dev-User-Id` from requests without a token (development, load tests; refused in production) |
| `INVENTORY_GRPC_ADDR` | `inventory:7070` | inventory-svc's internal gRPC API |
| `SERVICE_PRIVATE_KEY_FILE` | required | booking-svc's Ed25519 key for service tokens; inventory-svc and payment-svc trust the public half |
| `INVENTORY_TIMEOUT` | `800ms` | Deadline of each inventory call, retries included |
| `PAYMENT_GRPC_ADDR` | empty | payment-svc's internal gRPC API; empty: no payment intents (Compose: `payment:7070`) |
| `PAYMENT_TIMEOUT` | `8s` | Deadline of each payment-svc call, which includes the provider's |
| `PAYMENT_GRACE` | `3m` | How long the hold's protection outlives the payment deadline; keep it below inventory-svc's `PAYMENT_WINDOW` |
| `KAFKA_BROKERS` | `localhost:29092` | Kafka, for the outbox relay (Compose: `kafka:9092`) |
| `OUTBOX_BATCH` | `500` | Events published per relay transaction |
| `OUTBOX_INTERVAL` | `200ms` | Relay pause after a pass that found less than a full batch |
| `DEADLINE_SCAN_INTERVAL` | `5s` | Deadline job period |
| `DEADLINE_BATCH` | `100` | Bookings cancelled per transaction |

Locally, Compose maps the public port to 8083 and the admin port to 9093; the
edge routes `/v1/bookings` here.

## Metrics

`holdfast_capture_to_confirm_seconds` (histogram, task 4.6) times each
confirmation from the payment's capture (the capture event's CloudEvents
time) to the saga's committed decision; redeliveries and duplicates are not
timed. Its p99 is the SLO of design doc 12.1 (at most 5 s), shown on the
mission-control dashboard.

| Metric | Labels | Use |
|---|---|---|
| `holdfast_booking_requests_total` | `result` | POST outcomes: created, replayed, key_reused, hold_not_found, hold_not_available, event_not_found, error |
| `holdfast_bookings_created_total` | | Bookings inserted |
| `holdfast_booking_requests_resumed_total` | | Requests that resumed an earlier, unfinished attempt |
| `holdfast_bookings_expired_total` | | Bookings cancelled at their deadline |
| `holdfast_booking_deadline_runs_total` | `result` | Deadline job passes: ok, error |
| `holdfast_booking_saga_outcomes_total` | `status` | Saga moves: CONFIRMED, REFUND_REQUIRED, CANCELLED, REFUNDED |
| `holdfast_late_confirm_total` | `result` | Captures after cancellation: confirmed, refund_required |
| `holdfast_booking_saga_skipped_total` | `reason` | Events with no change: duplicate, unknown_booking, already_decided |
| `holdfast_booking_inventory_settled_total` | `result` | Inventory calls: confirm_confirmed, confirm_replay, confirm_late, released, release_noop |
| `holdfast_kafka_consumed_total`, `holdfast_kafka_consumer_lag` | `topic`, `result` / `group`, `topic` | The saga consumer |
| `holdfast_outbox_published_total` | `schema` | Events published by the relay |
| `holdfast_outbox_pending` | `schema` | Events not yet published |
| `holdfast_outbox_lag_seconds` | `schema` | Age of the oldest unpublished event (0 when none) |
| `holdfast_outbox_relay_leader` | `schema` | 1 while this replica leads the relay |
| `holdfast_http_*` | `route`, `code` | RED metrics per route pattern |
