# queue-svc

The waiting room: decides who may enter the purchase path, in what order and
how fast. Binary: `cmd/queue`. Code: `internal/queue`.

**Status:** built in Phase 2 (tasks 2.1 to 2.14): provisioning, joining, the
T0 transition, positions, the admission controller, the status document,
admission tokens and their trust by inventory-svc, the edge, metrics and a
dashboard, tests, experiments E2 and E6, and operator commands. Not built:
oversubscription and `SOLD_OUT` (below). Decisions: ADR 0005 (lottery before
T0, FIFO after), ADR 0006 (cached status polling), ADR 0007 (leader election
with fencing).

## Responsibilities

- Store each event's queue settings and open its waiting room in state PRE (built).
- Accept joins: a random lottery position before T0, arrival order after (built).
- Switch from PRE to OPEN at T0 and tell buyers their rank (built).
- Admit buyers at a controlled rate and issue admission tokens (built).
- Publish the status document every client polls (built).

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
- Your rank: `GET /v1/queue/{eventID}/me` (below).

**Rate limits.** Each join takes a token from two buckets, and the first
empty one refuses the request with 429 `RATE_LIMITED` and `Retry-After`:

| Bucket | Key | Default | Purpose |
|---|---|---|---|
| Per client IP | `rl:join-ip:<ip>` | burst 30, 10 per second | One machine hammering with many identities |
| Per user | `rl:join-user:<user id>` | burst 5, 1 per second | One identity hammering from many machines |

IPv6 clients are limited per /64 network, since one subscriber usually owns a
whole /64. The client IP is the connection's address, unless the connection
comes from a trusted proxy (`TRUSTED_PROXIES`, the NGINX edge): then it is the
last `X-Forwarded-For` entry, which the edge sets to the address it saw and
never lets a client supply. A refused join never reaches the queue.

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
- Clients should ask once after T0 and then follow the shared status document,
  comparing their rank with `admittedUpTo`. A per-user bucket
  (`rl:position-user:<user id>`, default burst 10, 1 per second) refuses
  polling with 429 `RATE_LIMITED` and `Retry-After`.
- A user who never joined gets 404 `NOT_IN_QUEUE`.

### `GET /v1/events/{eventID}/status`

The status document: the one thing the whole waiting room polls (every 3 s
plus jitter, in the design). No identity needed.

```http
HTTP/1.1 200 OK
Cache-Control: public, max-age=1

{"eventId":"0196f0c1-...","state":"OPEN","opensAt":"2026-10-05T12:00:00Z",
 "admittedUpTo":4200,"queueSize":50000,"updatedAt":"2026-10-05T12:01:30.250Z"}
```

- A buyer whose rank (from `/me`) is at most `admittedUpTo` may claim their
  turn (`POST /v1/queue/{eventID}/admit`).
- The admission leader rewrites it on every tick, inside the fenced
  `advance.lua`, in every state: it is always consistent with `admittedUpTo`,
  and a stale leader cannot overwrite it. `updatedAt` says how fresh it is.
- `state` follows the T0 clock rule: `PRE` past T0 reads as `OPEN`.
- It is identical for every client, so a shared cache may keep it for one
  second (`public, max-age=1`): through the NGINX edge the origin sees about
  one request per second per cache node, however many clients poll (measured
  locally: 2,000 polls in 5.4 s reached queue-svc 3 times). It is not rate
  limited per client.
- Until a leader has written one (just provisioned, or no leader elected), a
  fallback is built from the raw keys with `"updatedAt": null`, so a missing
  leader is visible. An event without a queue returns 404 `EVENT_NOT_FOUND`;
  errors are never publicly cacheable (`no-store`).

### `POST /v1/queue/{eventID}/admit`

Exchange your turn for an admission token. Same identity as joining. No body.

```http
HTTP/1.1 200 OK
Cache-Control: no-store

{"eventId":"0196f0c1-...","token":"<signed admission token (JWT)>","expiresAt":"2026-10-05T12:10:00Z","rank":4200}
```

- Allowed while your rank is within `admittedUpTo` and the session slot the
  leader gave your rank is alive (it lasts the session TTL from the moment you
  were admitted). The slot's expiry is judged by Valkey's clock, so a slot
  that has run out is expired even before the leader's next tick removes it.
  Otherwise 409 `NOT_YOUR_TURN` (the detail gives your rank and
  how far admission has got), 409 `TURN_EXPIRED`, 404 `NOT_IN_QUEUE`, 409
  `QUEUE_CLOSED` (`SOLD_OUT` or `CLOSED`) or 404 `EVENT_NOT_FOUND`. A `FROZEN`
  sale still honours turns already given.
- The token is an EdDSA JWT: `iss` `holdfast-queue`, `aud` `holdfast-inventory`,
  `sub` your user ID, `evt` the event, `rank`, `sid` (the same for every claim
  by this user on this event), a unique `jti`, and `exp` at the earlier of
  `ADMISSION_TOKEN_TTL` and your session slot's expiry. inventory-svc accepts
  holds only with it.
- Claiming again is safe: it returns a fresh token for the same session. A
  per-user bucket (`rl:admit-user:<user id>`, default burst 5, 1 per second)
  refuses hammering with 429 `RATE_LIMITED`.

### `GET /.well-known/jwks.json`

The public keys admission tokens are signed with, as RFC 8037 JWKs (`kty`
`OKP`, `crv` `Ed25519`), each with the `kid` tokens carry. Public,
`Cache-Control: public, max-age=300`. During a key rotation it lists the new
signing key and the old one (`ADMISSION_EXTRA_PUBLIC_KEY_FILES`), so tokens
signed by either stay verifiable. inventory-svc follows this set (see the
decisions below).

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

`holdfastctl queue provision --event <id>` provisions the queue of an event
that already exists (created with `--no-provision`, or after Valkey lost its
keys: RB-Q-8). It takes the same three flags; the opening time comes from the
event in PostgreSQL unless `--opens-at` is given. It is idempotent, and a
conflict says how to read the stored settings.

`holdfastctl queue status --event <id>` shows what an operator needs in one
place: the state (and the stored state, if T0 has passed but nobody has
flipped it yet), the opening time, the queue size, `admittedUpTo`, active
session slots (unexpired by Valkey's clock) against the budget, the admission
rate and session TTL, the leader's epoch, the status document's age, and
whether the event is on the work list. `--json` prints the same as JSON.

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

## Admission

Admission moves `admittedUpTo` (`q:{E}:admitted`), the highest rank allowed
into the purchase path. Buyers compare their rank with it through the status
document and exchange their turn for an admission token. Why one leader, and
how a stale one is stopped: ADR 0007.

- **One leader per event.** Every queue-svc replica runs a controller for every
  event in `q:events`; they compete for a PostgreSQL advisory lock
  (`pg_try_advisory_lock`, keyed by FNV-1a of the event ID) held on a dedicated
  connection taken out of the pool. If the leader's process dies, the
  connection drops, PostgreSQL releases the lock, and a standby takes over on
  its next attempt (`LEADER_RETRY_INTERVAL`, 2 s).
- **Fencing.** Each new term increments `adm:{E}:epoch`. `advance.lua` refuses
  a caller whose epoch is not current, so a leader that paused and woke up
  after a successor was elected cannot admit anyone; its term ends.
- **Each tick** (`ADMISSION_TICK`, 250 ms) the leader asks `advance.lua` to admit
  up to what its allowance holds: a token bucket at the event's admission rate,
  capped at one second's worth, so a pause never becomes a burst. In one atomic
  step the script removes expired session slots, caps admissions so active
  slots never exceed `maxSessions` (Little's Law: L = λ × W), never passes the
  last member, moves `admittedUpTo`, and gives each newly admitted rank a slot
  in `adm:{E}:sessions` that lasts the session TTL.
- **Only `OPEN` admits.** `FROZEN` pauses admission; slots keep expiring.
- **Controllers follow the work list.** A replica starts a controller for each
  event in `q:events` (rescanned every `ADMISSION_RESCAN_INTERVAL`). A
  controller whose event no longer exists (its settings are gone) stops, and
  reads the settings before starting a term so it never recreates the event's
  keys; if the event is provisioned again, a new controller starts.
- PostgreSQL is used only for elections. Readiness does not depend on it: if it
  is down, admissions pause and joining and positions keep working.

Not built: limiting admissions by the units left in inventory
(oversubscription) and marking the queue `SOLD_OUT` (progress log P17). Both
need inventory's state, which queue-svc may only get through inventory-svc's
API, and the units in active holds are not exposed there yet. Recommended at
the end of Phase 2: build them in Phase 3, alongside booking-svc's
cross-service calls. Until then the queue keeps admitting (within the session
budget) after the last unit is held, and those buyers get 409 `SOLD_OUT` from
inventory: nothing oversells. Adaptive admission (AIMD) is Phase 5.

## Decisions: how admission tokens are trusted (task 2.8)

**One token per admission session, not single use.** A token may be used for
several requests during its short life. A single-use token (a `jti` store
consulted on every hold) would refuse a hold retried after a network failure,
breaking the rule that every request can be retried. Reuse is bounded instead:

- the token is bound to one user and one event;
- it expires at the earlier of `ADMISSION_TOKEN_TTL` (10 minutes) and the
  buyer's session slot;
- holds are idempotent: a retried request returns the original hold;
- the per-user cap (I4) bounds the units any token can hold, whatever it does.

A stolen token therefore lets its thief act as that one buyer, on that one
event, within that buyer's cap, for at most the rest of their session.

**Keys come from this service's JWKS.** inventory-svc fetches
`/.well-known/jwks.json` (`ADMISSION_JWKS_URL`) instead of being configured
with key files (still supported, and trusted in addition):

- it refreshes every 5 minutes, and at once when a token carries an unknown
  key ID, but at most every 30 seconds, so made-up key IDs cannot make it
  hammer queue-svc;
- known keys are answered without waiting on any fetch; a failed fetch keeps
  the last good keys; keys no longer published are dropped at the next
  refresh;
- only Ed25519 signing keys whose `kid` is the key ID of the key itself are
  accepted;
- inventory-svc is not ready (`/readyz`) while it knows no key.

So a key rotation needs no restart of inventory-svc, but publish the new key
before signing with it (RB-Q-7): an abrupt switch can see new tokens refused
for up to 30 seconds.

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `UNAUTHENTICATED` | 401 | Join: missing or malformed `X-Dev-User-Id`, or buyer identity not enabled. Admin: missing or wrong operator token |
| `INVALID_REQUEST` | 400 | Malformed event ID, or a setting outside its bounds (`detail` says which) |
| `INVALID_BODY`, `MALFORMED_JSON`, `EMPTY_BODY`, `TRAILING_DATA`, `INVALID_FIELD_TYPE` | 400 | Provisioning body problems, including unknown fields |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | Provisioning body is not JSON |
| `EVENT_NOT_FOUND` | 404 | Join, position or status: the event has no waiting room (not provisioned) |
| `NOT_IN_QUEUE` | 404 | Position or admit: the caller has not joined this event's queue |
| `QUEUE_CLOSED` | 409 | Join or admit: the queue is `SOLD_OUT` or `CLOSED` |
| `NOT_YOUR_TURN` | 409 | Admit: your rank is above `admittedUpTo` |
| `TURN_EXPIRED` | 409 | Admit: you were admitted, but your session slot has run out |
| `PROVISION_CONFLICT` | 409 | Queue already provisioned with different settings |
| `RATE_LIMITED` | 429 | Join, position or admit: a rate-limit bucket is empty; retry after `Retry-After` seconds |
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
| `q:{E}:admitted` | integer | `admittedUpTo`, the highest admitted rank |
| `q:{E}:status` | string (JSON) | The status document: `state`, `opensAtMs`, `admittedUpTo`, `queueSize`, `updatedAtMs`; rewritten every tick by the leader |
| `adm:{E}:epoch` | integer | Fencing token: incremented by every new admission leader |
| `adm:{E}:sessions` | sorted set | Admitted rank → session expiry in ms; the concurrency budget |
| `q:events` | set | Provisioned events: the opener's and the admission controllers' work list (one global key, never used inside multi-key scripts) |
| `rl:SCOPE:ID` | hash | Token bucket: `tokens`, `ts_ms`; expires once the bucket would be full again |

The design doc's `jti:*` key (single-use admission tokens, section 8.2) is not
built: task 2.8 decided that tokens stay reusable within their session (below).

## Scripts

| Script | Replies |
|---|---|
| `provision.lua` | 1 created, 0 identical settings, -1 conflict |
| `join.lua` | {1 joined, 0 already joined, -1 closed, -2 not provisioned, member's score, 1 if this join opened the queue at T0} |
| `open.lua` | {1 opened, 0 nothing to do, -1 not provisioned; ms late after T0, or ms left until T0} |
| `position.lua` | {1 ranked, 0 before T0, -1 not in queue, -2 not provisioned; rank or opens_at_ms; state} |
| `advance.lua` | {1 admitted some, 0 nothing to admit, -1 fenced, -2 not provisioned; admittedUpTo, admitted now, active sessions, queue size}; also rewrites `q:{E}:status` unless fenced |
| `status.lua` | {1 the leader's document, 0 fallback without `updatedAtMs`, -2 not provisioned; the document as JSON} |
| `admit.lua` | {1 admitted, 0 not your turn, -1 not in queue, -2 not provisioned, -3 turn expired (slot gone, or past its expiry by Valkey's clock), -4 closed; rank; slot expiry ms or admittedUpTo} |
| `token_bucket.lua` (`internal/platform/ratelimit`) | {allowed 1 or 0, remaining tokens × 1000, retry after ms} |

## Tests

| Layer | What it covers | Where |
|---|---|---|
| Unit | The leader's allowance (rate, one-second cap, fractions); handlers, validation and error mapping; lottery scores; the Spearman helper | `admission_test.go`, `handler_test.go`, `lottery_test.go`, `model_test.go`, `internal/stats` |
| Fake clock | The leader's tick loop under `testing/synctest`: exact tick times, no starting burst, carried allowance capped at one second, stepping down when fenced, when the lock's connection dies or when the event is gone, surviving a failed tick | `admission_synctest_test.go` |
| Integration | Every script against real Valkey: provisioning, joins (idempotent, no re-roll, concurrent), the T0 switch by Valkey's clock, positions, a stale epoch refused, the session budget, slot expiry, the status document, claims | `store_integration_test.go`, `admission_integration_test.go` |
| Model (F1) | Random interleavings of joins, rejoins, ticks, claims, expiring slots, T0 and freezes, checked step by step against a reference model: the queue's order, which ranks each tick admits, every claim's answer, and that no rank changes once admission has begun | `fairness_model_integration_test.go` (12 seeds × 400 steps; a failure prints its seed and step) |
| Failover | Two real controllers on PostgreSQL and Valkey: one leader, the standby takes over with a higher epoch when the leader dies, the old epoch is fenced, only the new leader exports gauges; a controller stops when its event is removed | `admission_integration_test.go` |

`make test` runs the unit and fake-clock tests; `make itest` runs the rest.
Timing-dependent integration tests judge time by Valkey's clock and skip only
the check a WSL2 wall-clock step invalidates, with a log line (progress log
E7).

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `SHUTDOWN_*`, `VALKEY_*`,
`POSTGRES_*`) are defined in `internal/platform/config`; `POSTGRES_DSN` is
required (admission leader election). Service settings:

| Variable | Default | Meaning |
|---|---|---|
| `ADMIN_TOKEN` | required | Operator token, at least 32 characters; removed from the environment after loading |
| `DEV_IDENTITY` | `false` | Accept `X-Dev-User-Id` as the buyer's identity. Anyone can claim any user ID with it, so queue-svc refuses to start with it when `ENVIRONMENT=production`. When off, buyer requests get 401 until auth-svc exists |
| `JOIN_IP_BURST` | `30` | Per-IP bucket size |
| `JOIN_IP_PER_SECOND` | `10` | Per-IP refill rate |
| `JOIN_USER_BURST` | `5` | Per-user bucket size |
| `JOIN_USER_PER_SECOND` | `1` | Per-user refill rate |
| `TRUSTED_PROXIES` | none | CIDRs of the edge; only their `X-Forwarded-For` is believed (Compose: `10.250.0.10/32`) |
| `OPEN_CHECK_INTERVAL` | `250ms` | How often the opener looks for queues due to open (10ms to 10s) |
| `POSITION_USER_BURST` | `10` | Per-user bucket size for position lookups |
| `POSITION_USER_PER_SECOND` | `1` | Per-user refill rate for position lookups |
| `ADMISSION_PRIVATE_KEY_FILE` | required | Ed25519 private key (PEM) that signs admission tokens; Compose mounts the dev key from `make keys` |
| `ADMISSION_EXTRA_PUBLIC_KEY_FILES` | none | Comma-separated PEM public keys also published in the JWKS (the old key during a rotation) |
| `ADMISSION_TOKEN_TTL` | `10m` | Longest token lifetime (1m to 1h); never beyond the session slot |
| `ADMIT_USER_BURST` | `5` | Per-user bucket size for claims |
| `ADMIT_USER_PER_SECOND` | `1` | Per-user refill rate for claims |
| `ADMISSION_TICK` | `250ms` | How often a leader admits (10ms to 10s) |
| `LEADER_RETRY_INTERVAL` | `2s` | How often a standby tries to become leader (100ms to 1m) |
| `ADMISSION_RESCAN_INTERVAL` | `2s` | How often new events get a controller (100ms to 1m) |

Locally, Compose maps the public port to 8082 and the admin port to 9092,
and sets `DEV_IDENTITY=true`.

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_queue_joins_total` | `result` | Join outcomes: joined, already_joined, rate_limited_ip, rate_limited_user, closed, not_found, invalid, error |
| `holdfast_queue_position_lookups_total` | `result` | Position lookups: ranked, randomizing, not_in_queue, not_found, rate_limited, invalid, error |
| `holdfast_queue_admitted_total` | `event` | People admitted into the purchase path |
| `holdfast_queue_admits_total` | `result` | Turn claims: issued, not_your_turn, expired, not_in_queue, closed, not_found, rate_limited, invalid, error |
| `holdfast_queue_admission_ticks_total` | `result` | Leader ticks: advanced, idle, fenced, error |
| `holdfast_queue_leader_terms_total` | | Admission leadership terms won by this process |
| `holdfast_queue_admission_leader` | `event` | 1 while this process leads the event (bounded by the number of events) |
| `holdfast_queue_leader_epoch` | `event` | Fencing epoch of this process's term; exported by the leader only |
| `holdfast_queue_size` | `event` | People in the queue, admitted or not; set by the leader on every tick |
| `holdfast_queue_admitted_up_to` | `event` | `admittedUpTo`, the highest admitted rank; set by the leader on every tick |
| `holdfast_queue_active_sessions` | `event` | Unexpired session slots; set by the leader on every tick |
| `holdfast_queue_max_sessions` | `event` | The session budget (Little's Law L); set when a term starts |
| `holdfast_queue_status_age_seconds` | `event` | Age of the status document by Valkey's clock (the clock that stamped it), measured by the opener in every replica; absent until a leader has written one |
| `holdfast_queue_opened_total` | `by` | T0 transitions: `join` (a join got there first) or `opener` |
| `holdfast_queue_opener_runs_total` | `result` | Opener passes: ok, error |
| `holdfast_queue_opener_duration_seconds` | | One opener pass over all events |
| `holdfast_http_requests_total` | `route`, `code` | RED metrics per route pattern |
| `holdfast_http_request_duration_seconds` | `route` | Latency per route pattern |

Every `event` label is a server-side event ID, so the label set is bounded by
the number of provisioned events. The leader-only gauges belong to the
current term: a replica deletes its series when its term ends, so after a
failover only the new leader exports them. The admission rate is
`rate(holdfast_queue_admitted_total[1m])`.

The Grafana dashboard **HoldFast / Queue**
(`deploy/grafana/dashboards/queue.json`) charts these per event: queue
size, `admittedUpTo`, sessions against the budget, admission rate, status
age, leaders and epochs, and the join, claim, tick and HTTP counters. A
status age above a few seconds while the queue is `OPEN` means no leader is
writing (RB-Q-5).
