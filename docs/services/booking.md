# booking-svc

Turns a buyer's hold into a booking that waits for payment, and carries the
booking to its end: confirmed, cancelled or refunded. Binary: `cmd/booking`.
Code: `internal/booking`. Schema: `booking` (migrations in
`db/migrations/booking`, queries in `internal/booking/queries`).

**Status:** Phase 3 in progress. Built: the schema and its state machine
(task 3.5), the booking API and the deadline job (task 3.6). Payment intents
arrive with payment-svc (task 3.9), the outbox relay with task 3.7, and
confirmation and compensation with task 3.11.

## Responsibilities

- Create a booking for a hold, once, however often the request is retried.
- Protect the hold for the payment window (inventory-svc, over gRPC).
- Record every state change and its event in one transaction (the outbox).
- Cancel bookings whose payment deadline passed.
- Show buyers their own bookings, and nobody else's.

## API

### `POST /v1/bookings`

```http
POST /v1/bookings
X-Dev-User-Id: <buyer id>          (development identity until Phase 4)
Idempotency-Key: <8 to 255 printable characters>
Content-Type: application/json

{"eventId": "0196f0c1-...", "holdId": "1f0e..."}
```

```http
HTTP/1.1 201 Created

{"bookingId":"01a0...","eventId":"0196...","holdId":"1f0e...","quantity":2,
 "amountPaise":500000,"status":"PENDING_PAYMENT","paymentDeadline":"2026-10-05T12:10:00Z"}
```

- The hold must be the buyer's own, and still HELD or PAYING. booking-svc
  calls inventory-svc's `GetHold`, then `MarkPaying`, which protects the hold
  for the payment window; the booking's `paymentDeadline` is the end of that
  window, and `amountPaise` is the quantity times the event's price.
- `checkoutUrl` and `intentId` appear once payment-svc exists (task 3.9).

**Idempotency** (design doc 9.5). The `Idempotency-Key` is required and
belongs to the buyer.

- The first request with a key claims it. Its answer, success or definitive
  failure, is stored with the key, and every retry with the same key gets
  exactly that answer, byte for byte, with `Idempotent-Replayed: true`.
- The same key with a different request (another event or hold) is refused
  with 422 `IDEMPOTENCY_KEY_REUSED`.
- If an attempt fails part-way (inventory unreachable: 503 with
  `Retry-After`), the key stays in progress and a retry with the same key
  **resumes**: every step is idempotent (`GetHold`, `MarkPaying`, one booking
  per hold, the intent keyed by the booking). Concurrent identical requests
  produce one booking and one event, and all get the same answer.
- A second key for the same hold returns the existing booking.
- Keys are deleted after 24 hours.

Errors: 400 `INVALID_REQUEST`, `INVALID_IDEMPOTENCY_KEY` or
`IDEMPOTENCY_KEY_REQUIRED`; 404 `HOLD_NOT_FOUND` (also for someone else's
hold) or `EVENT_NOT_FOUND`; 409 `HOLD_NOT_AVAILABLE` (released, sold or
expired); 422 `IDEMPOTENCY_KEY_REUSED`; 503 when inventory-svc is
unreachable (retry with the same key).

### `GET /v1/bookings/{bookingID}`

The buyer's own booking, in the same shape. Anyone else's, or one that does
not exist, is 404 `BOOKING_NOT_FOUND`, so booking IDs cannot be probed.

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

## Background work

The **deadline job** runs in every replica every `DEADLINE_SCAN_INTERVAL`
(5 s): it claims overdue `PENDING_PAYMENT` bookings with `FOR UPDATE SKIP
LOCKED` (concurrent replicas take disjoint batches), moves each to
`CANCELLED` and writes `booking.cancelled.v1` (reason `PAYMENT_EXPIRED`) in
the same transaction. inventory-svc releases the hold itself when its payment
window ends. Once an hour the job deletes idempotency keys older than a day.

## Events (outbox)

Written to `booking.outbox` in the same transaction as the change they
describe, with the request's trace context in `headers`; published to
`holdfast.booking.v1` by the relay (task 3.7). Payloads are
`holdfast.events.v1` messages.

| Event | When |
|---|---|
| `booking.created.v1` | A booking was created |
| `booking.cancelled.v1` | Its deadline passed (later: its payment failed) |

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `POSTGRES_*`, `OTEL_*`) are
defined in `internal/platform/config` and `internal/platform/otel`.

| Variable | Default | Meaning |
|---|---|---|
| `DEV_IDENTITY` | `false` | Trust `X-Dev-User-Id` as the buyer (refused in production) |
| `INVENTORY_GRPC_ADDR` | `inventory:7070` | inventory-svc's internal gRPC API |
| `SERVICE_PRIVATE_KEY_FILE` | required | booking-svc's Ed25519 key for service tokens; inventory-svc trusts the public half |
| `INVENTORY_TIMEOUT` | `800ms` | Deadline of each inventory call, retries included |
| `DEADLINE_SCAN_INTERVAL` | `5s` | Deadline job period |
| `DEADLINE_BATCH` | `100` | Bookings cancelled per transaction |

Locally, Compose maps the public port to 8083 and the admin port to 9093; the
edge routes `/v1/bookings` here.

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_booking_requests_total` | `result` | POST outcomes: created, replayed, key_reused, hold_not_found, hold_not_available, event_not_found, error |
| `holdfast_bookings_created_total` | | Bookings inserted |
| `holdfast_booking_requests_resumed_total` | | Requests that resumed an earlier, unfinished attempt |
| `holdfast_bookings_expired_total` | | Bookings cancelled at their deadline |
| `holdfast_booking_deadline_runs_total` | `result` | Deadline job passes: ok, error |
| `holdfast_http_*` | `route`, `code` | RED metrics per route pattern |
