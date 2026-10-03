# mockpsp

A stand-in payment provider for development and tests. It plays the
provider payment-svc talks to, and it can be told to misbehave in the ways
a real provider's sandbox cannot (design doc section 4). Binary:
`cmd/mockpsp`. Code: `internal/mockpsp`. The API contract it implements is
`internal/payment/psp`.

**A test tool:**

- it never asks for card details;
- no real money moves;
- its state is in memory and resettable;
- it must never face the internet.

**Status:** built in task 3.10. Compose runs it, and payment-svc and
booking-svc use it end to end.

## Provider API (public port)

Every call needs `Authorization: Bearer <API_KEY>` when `API_KEY` is set.
The same key is payment-svc's `PSP_API_KEY`.

| Call | What it does |
|---|---|
| `POST /v1/orders` | Opens an order. `Idempotency-Key` is required (payment-svc sends the intent ID). The same key and body return the same order (200, `Idempotent-Replayed: true`); the same key with a different body is 422 `IDEMPOTENCY_KEY_REUSED`. Body: `amountPaise` > 0, `currency` `INR`, `expiresAt` in the future, `reference`. 201 with `orderId`, `checkoutUrl`, `status` `CREATED`. |
| `GET /v1/orders/{orderId}` | The order: `CREATED`, `CAPTURED` (with `paymentId`), `FAILED` (with `failureReason`) or `EXPIRED`. 404 `ORDER_NOT_FOUND`. |
| `POST /v1/refunds` | A full refund of a captured payment, once per payment. `Idempotency-Key` is required. Body: `paymentId`, `amountPaise`. 201 `PENDING`; it completes after `REFUND_DELAY` with a `refund.completed` webhook. Errors: 404 `PAYMENT_NOT_FOUND`, 422 `NOT_REFUNDABLE` (not captured, or not the full amount), 409 `ALREADY_REFUNDED`. |
| `GET /v1/settlements?from=&to=` | The settlement report: captures and completed refunds in `[from, to)` (RFC 3339, at most 31 days), oldest first. Each item has `type` (`capture` or `refund`), `orderId`, `paymentId`, `refundId`, `reference` (the intent ID), `amountPaise` and `at`. The reconciler (Phase 5) compares it with HoldFast's records. |

## Checkout (public port)

- `GET /checkout/{orderId}` is the hosted payment page: the amount, the
  deadline, and **Pay** and **Decline** buttons. Locally:
  `http://localhost:8085/checkout/...`, the `checkoutUrl` in a booking.
- `POST /checkout/{orderId}/pay` captures, unless the failure fault strikes
  (then it fails with `card_declined`).
- `POST /checkout/{orderId}/fail` declines (`declined_by_buyer`).
- A failed order can be paid again until it expires; a captured one stays
  captured; an expired one answers 409 `ORDER_CLOSED`.
- Browsers are redirected back to the page. Clients sending
  `Accept: application/json` (load tests) get the order as JSON.

Open orders, created or failed, expire at `expiresAt`. A sweep every second
marks them `EXPIRED` and sends `order.expired`.

## Webhooks

Each state change sends a webhook to `WEBHOOK_URL` (payment-svc's
`/v1/webhooks/psp`): `payment.captured`, `payment.failed`, `order.expired`
or `refund.completed`. The body is `psp.Webhook`.

- **Signature:** `X-PSP-Timestamp` (Unix seconds) and `X-PSP-Signature`,
  the hex HMAC-SHA256 of `<timestamp>.<body>` with `WEBHOOK_SECRET`. Each
  attempt is signed afresh.
- **Retries:** any answer other than 2xx, or a network error, is retried up
  to `WEBHOOK_MAX_ATTEMPTS` times. The wait starts at `WEBHOOK_RETRY_BASE`
  and doubles up to 30 s. Retries keep the event ID, so the receiver can
  deduplicate them.

## Fault injection (admin port)

All admin calls need `Authorization: Bearer <ADMIN_TOKEN>`.

| Call | What it does |
|---|---|
| `GET /internal/v1/faults` | The current faults |
| `PUT /internal/v1/faults` | Replaces them (unknown fields and out-of-range values are refused) |
| `POST /internal/v1/reset` | Forgets every order and refund, abandons pending webhooks, clears the faults |
| `GET /internal/v1/stats` | Orders by status, and refunds |

```json
{"duplicateRate": 0.2, "delayRate": 0.1, "delayMin": "30s", "delayMax": "90s",
 "lossRate": 0.05, "failureRate": 0.1, "timeoutRate": 0.05, "timeoutDelay": "10s",
 "outage": false}
```

| Fault | Effect | What it tests in payment-svc |
|---|---|---|
| `duplicateRate` | A webhook is delivered twice, with the same event ID | Deduplication (I2) |
| `delayRate`, `delayMin`, `delayMax` | A webhook's first delivery waits 30 to 90 s by default | Late and out-of-order webhooks |
| `lossRate` | A webhook is never sent; the order still changes | Status polling, then reconciliation |
| `failureRate` | A payment that would succeed fails (`card_declined`) | The failure path and compensation |
| `timeoutRate`, `timeoutDelay` | The call is processed, but its answer is held back (10 s by default) | The client's timeout, retry with the same idempotency key, and the breaker |
| `outage` | Every API and checkout call gets 503 | The circuit breaker and backpressure |

Rates are drawn independently per webhook, payment or call, from a seeded
source (`SEED`; 0 seeds from the clock and logs the seed), so a run can be
repeated. `FAULTS` sets faults at start-up, in the same JSON. The example
above is design doc experiment E3's mix.

```bash
curl -X PUT localhost:9095/internal/v1/faults \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"duplicateRate":0.2,"lossRate":0.05}'
```

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`) are defined in
`internal/platform/config`. The public server sets no request timeout, so
that the timeout fault can hold answers back.

| Variable | Default | Meaning |
|---|---|---|
| `PUBLIC_URL` | `http://localhost:8085` | Base of checkout URLs, as browsers reach it |
| `API_KEY` | empty | Bearer key required on the API (empty: none) |
| `WEBHOOK_URL` | required | Where webhooks go |
| `WEBHOOK_SECRET` | required | Signing secret, at least 32 characters (payment-svc's `PSP_WEBHOOK_SECRET`) |
| `ADMIN_TOKEN` | required | Operator token for the admin API, at least 32 characters |
| `REFUND_DELAY` | `2s` | How long a refund stays pending |
| `WEBHOOK_MAX_ATTEMPTS` | `8` | Delivery attempts per webhook |
| `WEBHOOK_RETRY_BASE` | `1s` | First retry wait |
| `SEED` | `0` | Fault seed (0: from the clock) |
| `FAULTS` | empty | Faults at start-up, as JSON |

Locally, Compose maps the public port to 8085 and the admin port to 9095.
Compose's key, secret and token are development placeholders that
`.env` can override (`PSP_API_KEY`, `PSP_WEBHOOK_SECRET`, `ADMIN_TOKEN`).

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_mockpsp_orders_total` | `result` | created, replayed, key_reused |
| `holdfast_mockpsp_payments_total` | `result` | captured, failed, expired, refunded |
| `holdfast_mockpsp_webhooks_total` | `type`, `result` | delivered, retried, gave_up, and the faults dropped, duplicated, delayed |
| `holdfast_mockpsp_faults_total` | `fault` | API faults injected: timeout, outage |
| `holdfast_http_*` | `route`, `code` | RED metrics per route pattern |

A chaos run can compare what was injected (`dropped`, `duplicated`) with what
payment-svc saw (`holdfast_webhooks_total{duplicate="true"}`,
`holdfast_payment_captures_total{via="poll"}`).
