# queue-svc

The waiting room: decides who may enter the purchase path, in what order and
how fast. Binary: `cmd/queue`. Code: `internal/queue`.

**Status:** Phase 2 in progress. Built: provisioning (task 2.1), joining
(task 2.2), the T0 transition (task 2.3) and positions (task 2.4). The
admission controller, the status document and admission tokens follow in
tasks 2.5 to 2.8.

## Responsibilities

- Store each event's queue settings and open its waiting room in state PRE (built).
- Accept joins: a random lottery position before T0, arrival order after (built).
- Switch from PRE to OPEN at T0 and tell buyers their rank (built).
- Admit buyers at a controlled rate and issue admission tokens (tasks 2.5 to 2.7).
- Publish the status document every client polls (task 2.6).

## API

### `POST /v1/queue/{eventID}/join`

Puts the caller in the event's waiting room. No body.

Until auth-svc exists (Phase 4) the caller's identity is the
`X-Dev-User-Id` header, a UUID, accepted only when `DEV_IDENTITY=true`
(never in production; see Configuration).

```http
POST /v1/queue/0196f0c1-.../join
X-Dev-User-Id: 0196f0c2-0000-7000-8000-000000000001
```

```http
HTTP/1.1 202 Accepted
Cache-Control: no-store

{"eventId":"0196f0c1-...","joined":true,"ordering":"LOTTERY"}
```

- `ordering` is `LOTTERY` for a join before T0: the position is a random draw,
  so joining early gives no advantage over joining just before T0. After T0 it
  is `FIFO`: arrival order, behind every lottery joiner.
- Joining again returns 202 with `"joined": false` and the original ordering.
  It never re-rolls the lottery and never creates a second place.
- Joins are accepted while the sale is `FROZEN` (arrival order); `SOLD_OUT` and
  `CLOSED` refuse them.
- Rank lookups arrive with task 2.4.

**Rate limits.** Each join takes a token from two buckets, and the first
empty one refuses the request with 429 `RATE_LIMITED` and `Retry-After`:

| Bucket | Key | Default | Purpose |
|---|---|---|---|
| Per client IP | `rl:join-ip:<ip>` | burst 30, 10 per second | One machine hammering with many identities |
| Per user | `rl:join-user:<user id>` | burst 5, 1 per second | One identity hammering from many machines |

IPv6 clients are limited per /64 network, since one subscriber usually owns a
whole /64. The client IP is the connection's address: behind a proxy every
request shares the proxy's address, so trusting `X-Forwarded-For` from NGINX
is part of task 2.9. A refused join never reaches the queue.

### `GET /v1/queue/{eventID}/me`

Where the caller stands. Same identity as joining (`X-Dev-User-Id` until
Phase 4).

Before T0, by Valkey's clock, there is no rank yet: lottery positions keep
arriving until T0. The response says when the draw closes:

```http
HTTP/1.1 200 OK
Cache-Control: private, no-store

{"eventId":"0196f0c1-...","state":"PRE","randomizingAt":"2026-10-05T12:00:00Z"}
```

From T0 on it gives the 1-based rank. No lottery position can be added after
T0, so a rank never gets worse; it improves as people ahead are admitted:

```http
HTTP/1.1 200 OK
Cache-Control: private, no-store

{"eventId":"0196f0c1-...","state":"OPEN","rank":18204}
```

- A queue still marked `PRE` after T0 (nobody has flipped it yet) is reported
  as `OPEN` with a rank: T0 is the clock's call. The lookup itself is read-only.
- Ranks are still reported in `FROZEN`, `SOLD_OUT` and `CLOSED`, with that state.
- Clients should ask once after T0 and then follow the shared status document
  (task 2.6), comparing their rank with `admittedUpTo`. A per-user bucket
  (`rl:position-user:<user id>`, default burst 10, 1 per second) refuses
  polling with 429 `RATE_LIMITED` and `Retry-After`.
- A user who never joined gets 404 `NOT_IN_QUEUE`.

### `PUT /internal/v1/events/{eventID}/queue` (admin port)

Operator token required. Stores the event's queue settings and puts the
queue in state `PRE`.

```http
PUT /internal/v1/events/0196f0c1-.../queue
Authorization: Bearer <operator token>
Content-Type: application/json

{"opensAt":"2026-10-05T12:00:00Z","admissionRatePerSecond":83,"maxSessions":10000,"sessionTtlSeconds":600}
```

```http
HTTP/1.1 201 Created

{"eventId":"0196f0c1-...","created":true}
```

| Field | Meaning | Bounds |
|---|---|---|
| `opensAt` | T0, RFC 3339. Joins before it get a lottery position, joins after it arrival order | After 2020-01-01; stored with millisecond precision |
| `admissionRatePerSecond` | Most buyers admitted per second | 1 to 100,000 |
| `maxSessions` | Most concurrent checkout sessions (L in Little's Law) | 1 to 10,000,000 |
| `sessionTtlSeconds` | Lifetime of an admitted buyer's session | 60 to 3,600 |

Returns 201 when created and 200 when already provisioned with identical
settings, so retries are safe. Different settings return 409
`PROVISION_CONFLICT` and change nothing. Provisioning never moves a queue that
is already past `PRE` back to `PRE`.

`holdfastctl event create` provisions the queue together with inventory:
`--admission-rate` (default 83), `--max-sessions` (default 10,000) and
`--session-ttl` (default 10m); the opening time is the event's `--opens-at`.
It validates these flags before writing anything to PostgreSQL.

## The T0 transition

T0 is the event's `opensAt`. It is decided by **Valkey's clock**, the same
clock every replica sees, never by when some process gets round to flipping
the state:

- **A join opens the queue.** `join.lua` compares the server time with
  `opens_at_ms`. If T0 has passed but the state still reads `PRE`, the join
  flips it to `OPEN` in the same atomic step and takes a place in arrival
  order. Nobody who joins at or after T0 can get a lottery position.
- **The opener opens it otherwise.** Every queue-svc replica runs an opener
  that, every `OPEN_CHECK_INTERVAL` (default 250 ms), runs `open.lua` for each
  event in `q:events`. The flip is conditional (`PRE` only, and only from T0
  on) and idempotent, so racing openers are harmless and no leader is needed.
  The opener only decides how soon readers of the state see `OPEN` when nobody
  joins; it never decides who gets a lottery position.

Each opening is logged at INFO as `queue opened at T0` with `late_ms` (how long
after T0 the opener got to it) and counted in `holdfast_queue_opened_total`.

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `UNAUTHENTICATED` | 401 | Join: missing or malformed `X-Dev-User-Id`, or buyer identity not enabled. Admin: missing or wrong operator token |
| `INVALID_REQUEST` | 400 | Malformed event ID, or a setting outside its bounds (`detail` says which) |
| `INVALID_BODY`, `MALFORMED_JSON`, `EMPTY_BODY`, `TRAILING_DATA`, `INVALID_FIELD_TYPE` | 400 | Provisioning body problems, including unknown fields |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | Provisioning body is not JSON |
| `EVENT_NOT_FOUND` | 404 | Join or position: the event has no waiting room (not provisioned) |
| `NOT_IN_QUEUE` | 404 | Position: the caller has not joined this event's queue |
| `QUEUE_CLOSED` | 409 | Join: the queue is `SOLD_OUT` or `CLOSED` |
| `PROVISION_CONFLICT` | 409 | Queue already provisioned with different settings |
| `RATE_LIMITED` | 429 | Join or position: a rate-limit bucket is empty; retry after `Retry-After` seconds |
| `ROUTE_NOT_FOUND`, `METHOD_NOT_ALLOWED` | 404, 405 | No such endpoint or method |
| `UNAVAILABLE` | 503 | Valkey unreachable or timed out; retry after `Retry-After` |
| `INTERNAL` | 500 | Unexpected error, logged with the request ID |

## Queue states

```
PRE --T0--> OPEN --> SOLD_OUT --> CLOSED
             |  ^
             v  |
            FROZEN
```

Provisioning creates `PRE`; T0 turns it into `OPEN` (see above). The freeze
switch is task 4.3; `SOLD_OUT` and `CLOSED` come with the admission controller.

## Keyspace

| Key | Type | Content |
|---|---|---|
| `q:{E}:config` | hash | `opens_at_ms`, `admission_rate`, `max_sessions`, `session_ttl_ms`; written once by `provision.lua` |
| `q:{E}:state` | string | `PRE`, `OPEN`, `FROZEN`, `SOLD_OUT` or `CLOSED` |
| `q:{E}:members` | sorted set | user ID → score: lottery score in [0, 1) before T0, 1 plus the arrival number after |
| `q:{E}:seq` | integer | Arrival counter for joins after T0 |
| `q:events` | set | Provisioned events: the opener's work list (one global key, never used inside multi-key scripts) |
| `rl:SCOPE:ID` | hash | Token bucket: `tokens`, `ts_ms`; expires once the bucket would be full again |

The remaining keys in the design doc (section 8.2): `q:{E}:admitted`,
`q:{E}:status`, `adm:*` and `jti:*` arrive with tasks 2.5 to 2.8.

## Scripts

| Script | Replies |
|---|---|
| `provision.lua` | 1 created, 0 identical settings, -1 conflict |
| `join.lua` | {1 joined, 0 already joined, -1 closed, -2 not provisioned, member's score, 1 if this join opened the queue at T0} |
| `open.lua` | {1 opened, 0 nothing to do, -1 not provisioned; ms late after T0, or ms left until T0} |
| `position.lua` | {1 ranked, 0 before T0, -1 not in queue, -2 not provisioned; rank or opens_at_ms; state} |
| `token_bucket.lua` (`internal/platform/ratelimit`) | {allowed 1 or 0, remaining tokens × 1000, retry after ms} |

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `SHUTDOWN_*`, `VALKEY_*`)
are defined in `internal/platform/config`. Service settings:

| Variable | Default | Meaning |
|---|---|---|
| `ADMIN_TOKEN` | required | Operator token, at least 32 characters; removed from the environment after loading |
| `DEV_IDENTITY` | `false` | Accept `X-Dev-User-Id` as the buyer's identity. Anyone can claim any user ID with it, so queue-svc refuses to start with it when `ENVIRONMENT=production`. When off, buyer requests get 401 until auth-svc exists |
| `JOIN_IP_BURST` | `30` | Per-IP bucket size |
| `JOIN_IP_PER_SECOND` | `10` | Per-IP refill rate |
| `JOIN_USER_BURST` | `5` | Per-user bucket size |
| `JOIN_USER_PER_SECOND` | `1` | Per-user refill rate |
| `OPEN_CHECK_INTERVAL` | `250ms` | How often the opener looks for queues due to open (10ms to 10s) |
| `POSITION_USER_BURST` | `10` | Per-user bucket size for position lookups |
| `POSITION_USER_PER_SECOND` | `1` | Per-user refill rate for position lookups |

Locally, Compose maps the public port to 8082 and the admin port to 9092,
and sets `DEV_IDENTITY=true`.

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_queue_joins_total` | `result` | Join outcomes: joined, already_joined, rate_limited_ip, rate_limited_user, closed, not_found, invalid, error |
| `holdfast_queue_position_lookups_total` | `result` | Position lookups: ranked, randomizing, not_in_queue, not_found, rate_limited, invalid, error |
| `holdfast_queue_opened_total` | `by` | T0 transitions: `join` (a join got there first) or `opener` |
| `holdfast_queue_opener_runs_total` | `result` | Opener passes: ok, error |
| `holdfast_queue_opener_duration_seconds` | | One opener pass over all events |
| `holdfast_http_requests_total` | `route`, `code` | RED metrics per route pattern |
| `holdfast_http_request_duration_seconds` | `route` | Latency per route pattern |

Queue size, `admittedUpTo`, sessions, admission rate and leader epoch arrive
with task 2.10.
