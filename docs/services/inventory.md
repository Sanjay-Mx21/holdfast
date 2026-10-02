# inventory-svc

Holds seats on the hot path without ever overselling. Binary: `cmd/inventory`.
Code: `internal/inventory`.

## Responsibilities

- Create, read and cancel holds for admitted buyers.
- Protect holds during checkout (`MarkPaying`) and settle them (`Confirm`,
  `ReleaseForFailedPayment`). These are service methods today; booking-svc
  will call them over gRPC in Phase 3.
- Release expired holds (sweeper).
- Serve availability for the waiting room and clients.

## API

### `POST /v1/events/{eventID}/holds`

Headers: `Authorization: Bearer <admission token>` and
`Idempotency-Key: <8-255 chars of [A-Za-z0-9_.:-]>`.

```http
POST /v1/events/0196f0c1-.../holds
Content-Type: application/json

{"quantity": 2}
```

```http
HTTP/1.1 201 Created
Location: /v1/events/0196f0c1-.../holds/1e9299be-...

{"holdId":"1e9299be-...","eventId":"0196f0c1-...","quantity":2,
 "state":"HELD","expiresAt":"2026-09-25T10:05:00Z","remaining":998}
```

Repeating the request with the same key returns the same hold with
`Idempotent-Replayed: true`. Reusing a key with a different quantity is
rejected with `IDEMPOTENCY_KEY_REUSED`. A released hold keeps its key for one
hour (`HOLD_EXPIRED`); after that the key can create a new hold.

### `GET /v1/events/{eventID}/holds/{holdID}`

Returns the caller's hold. Another user's hold is reported as `HOLD_NOT_FOUND`,
so hold IDs cannot be probed.

### `DELETE /v1/events/{eventID}/holds/{holdID}`

Releases the caller's hold and returns 204. It is idempotent: an
already-released hold also returns 204. A hold in checkout or already sold
returns `HOLD_NOT_CANCELLABLE`.

### `GET /v1/events/{eventID}/availability`

Public and cacheable (`Cache-Control: public, max-age=1`):
`{"eventId":"...","available":998,"capacity":1000,"soldOut":false}`.
`available` is never negative.

### `PUT /internal/v1/events/{eventID}/inventory` (admin port)

Operator token required. Body: `{"capacity":1000,"perUserLimit":4}`. Returns
201 when created and 200 when already provisioned with identical settings;
different settings return 409 `PROVISION_CONFLICT`.

### Internal gRPC API (contract defined; served from task 3.4)

`holdfast.inventory.v1.InventoryService` (`proto/holdfast/inventory/v1/inventory.proto`)
is the API booking-svc will call: `GetHold`, `MarkPaying`, `Confirm` and
`ReleaseForFailedPayment`, each a thin adapter over the same service methods as
the HTTP API, and each safe to retry. Errors are gRPC status codes with a
`google.rpc.ErrorInfo` reason (`INVALID_REQUEST`, `EVENT_NOT_PROVISIONED`,
`HOLD_NOT_FOUND`, `HOLD_EXPIRED`).

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `UNAUTHENTICATED` | 401 | Missing, invalid or expired token (`WWW-Authenticate` is set) |
| `TOKEN_EVENT_MISMATCH` | 403 | Admission token issued for a different event |
| `IDEMPOTENCY_KEY_REQUIRED` | 400 | Missing `Idempotency-Key` |
| `INVALID_REQUEST` | 400 | Malformed ID or idempotency key |
| `INVALID_BODY`, `MALFORMED_JSON`, `EMPTY_BODY`, `TRAILING_DATA`, `INVALID_FIELD_TYPE` | 400 | Body problems |
| `BODY_TOO_LARGE` | 413 | Body above `HTTP_MAX_BODY_BYTES` |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | Body is not JSON |
| `INVALID_QUANTITY` | 422 | Quantity outside 1..per-user limit |
| `USER_LIMIT` | 422 | Per-user cap reached |
| `IDEMPOTENCY_KEY_REUSED` | 422 | Same key, different quantity |
| `SOLD_OUT` | 409 | Not enough units left |
| `HOLD_EXPIRED` | 409 | Hold expired or released |
| `HOLD_NOT_CANCELLABLE` | 409 | Hold in checkout or sold |
| `PROVISION_CONFLICT` | 409 | Event already provisioned differently |
| `EVENT_NOT_FOUND` | 404 | Event not provisioned |
| `HOLD_NOT_FOUND` | 404 | Unknown hold, or another user's hold |
| `ROUTE_NOT_FOUND`, `METHOD_NOT_ALLOWED` | 404, 405 | No such endpoint or method |
| `UNAVAILABLE` | 503 | Valkey unreachable or timed out; retry after `Retry-After` |
| `INTERNAL` | 500 | Unexpected error, logged with the request ID |

## Hold lifecycle

```
HELD --MarkPaying--> PAYING --Confirm--> SOLD
  |                    |
  +-- expiry, cancel   +-- payment failed, deadline passed
  v                    v
RELEASED <-------------+       (Confirm on RELEASED = late: units re-taken)
```

## Keyspace

| Key | Type | Content |
|---|---|---|
| `inv:{E}:avail` | string (int) | Units left; can dip below 0 after a late confirm |
| `inv:{E}:config` | hash | `capacity`, `per_user_limit` |
| `inv:{E}:user:{U}` | string (int) | Units the user currently holds or bought |
| `inv:{E}:hold:{H}` | hash | `user`, `qty`, `state`, `expires_at`, `created_at`, `released_by`; released holds expire after 1 h |
| `inv:{E}:expiry` | sorted set | Member `H|U`, score = expiry in ms |
| `inv:events` | set | Provisioned events (the sweeper's work list) |

## Scripts

| Script | Replies |
|---|---|
| `provision.lua` | 1 created, 0 identical settings, -1 conflict |
| `hold.lua` | {1 held, 2 replay, 0 sold out, -1 user limit, -2 bad quantity, -3 not provisioned} |
| `mark_paying.lua` | {1 marked, 2 already paying, 0 expired or missing, -1 not owner} |
| `release.lua` | 1 released, 0 nothing to do, -1 not expired yet, -2 paying (cannot cancel), -3 not owner |
| `confirm.lua` | 1 confirmed, 2 replay, 3 late, -1 not owner |

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `SHUTDOWN_*`, `VALKEY_*`)
are defined in `internal/platform/config`. Service settings:

| Variable | Default | Meaning |
|---|---|---|
| `HOLD_TTL` | `5m` | Lifetime of a HELD hold (1s to 1h) |
| `PAYMENT_WINDOW` | `10m` | Protection of a PAYING hold; must be at least `HOLD_TTL` |
| `SWEEP_INTERVAL` | `500ms` | Sweeper period (at least 50ms) |
| `SWEEP_BATCH` | `500` | Holds released per query |
| `ADMISSION_JWKS_URL` | none | queue-svc's key set, e.g. `http://queue:8080/.well-known/jwks.json`; followed without restarts (see `docs/services/queue.md`, decisions) |
| `ADMISSION_PUBLIC_KEY_FILES` | none | Comma-separated PEM public keys, trusted in addition to the JWKS. At least one of the two is required |
| `JWKS_REFRESH_INTERVAL` | `5m` | Periodic refresh of the key set |
| `JWKS_MIN_REFRESH_INTERVAL` | `30s` | Shortest gap between fetches, also for tokens with unknown key IDs |
| `TOKEN_LEEWAY` | `5s` | Clock skew tolerated on token times |
| `ADMIN_TOKEN` | required | Operator token, at least 32 characters; removed from the environment after loading |
| `HTTP_ACCESS_LOG_SUCCESS` | `true` | Log successful requests; set to `false` for load tests |

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_holds_total` | `result` | Outcomes: held, replay, sold_out, user_limit, invalid, not_provisioned, error |
| `holdfast_hold_script_duration_seconds` | | Valkey round-trip latency of `hold.lua` |
| `holdfast_hold_releases_total` | `mode` | EXPIRE, USER_CANCEL, PAYMENT_FAILED |
| `holdfast_hold_confirms_total` | `outcome` | confirmed, replay, late |
| `holdfast_inventory_available` | `event` | Last observed pool per event |
| `holdfast_sweeper_runs_total` | `result` | ok, error |
| `holdfast_sweeper_duration_seconds` | | One pass over all events |
| `holdfast_authn_jwks_fetches_total` | `result` | Key-set fetches: ok, error |
| `holdfast_http_requests_total` | `route`, `code` | RED metrics per route pattern |
| `holdfast_http_request_duration_seconds` | `route` | Latency per route pattern |
