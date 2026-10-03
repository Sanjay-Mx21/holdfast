# queue-svc

The waiting room: decides who may enter the purchase path, in what order and
how fast. Binary: `cmd/queue`. Code: `internal/queue`.

**Status:** built in Phase 2 (tasks 2.1 to 2.14): provisioning, joining, the
T0 transition, positions, the admission controller, the status document,
admission tokens and their trust by inventory-svc, the edge, metrics and a
dashboard, tests, experiments E2 and E6, and operator commands. Phase 4
(task 4.3) added the policy windows at join, the freeze switch, and admission
by the units left with `SOLD_OUT` (P17); task 4.4 added proof of work at
join. Decisions: ADR 0005 (lottery before
T0, FIFO after), ADR 0006 (cached status polling), ADR 0007 (leader election
with fencing).

## Responsibilities

- Store each event's queue settings and open its waiting room in state PRE (built).
- Accept joins: a random lottery position before T0, arrival order after (built).
- Switch from PRE to OPEN at T0 and tell buyers their rank (built).
- Admit buyers at a controlled rate and issue admission tokens (built).
- Publish the status document every client polls (built).
- Apply the sale's policy windows at join: verified buyers only, no agents (built, task 4.3).
- Make every join pay a small proof of work (built, task 4.4).
- Pause admissions on the freeze switch, and mark a sold-out queue `SOLD_OUT` (built, task 4.3).

## API

### `GET /v1/queue/{eventID}/challenge`

A proof-of-work challenge for the caller and this event (`internal/pow`).
Same identity as joining.

```http
HTTP/1.1 200 OK
Cache-Control: no-store

{"required":true,"challenge":"1759501720000.18.r4nd0m...","difficulty":18,"expiresAt":"2026-10-05T12:02:00Z"}
```

- **The work:** find a nonce, a decimal string, such that
  SHA-256(`challenge` + `:` + nonce) starts with at least `difficulty` zero
  bits: 2^`difficulty` hashes on average. The web app solves it in a Web
  Worker (`web/src/lib/pow/`); `holdfastctl pow solve --challenge <c>` solves
  it for curl and scripts.
- **Stateless:** a challenge is an HMAC (`POW_SECRET`) over the event, the
  user, its expiry, its difficulty and a random value. Any replica verifies
  it with one HMAC and one SHA-256, and nothing is stored. A solved challenge
  is bound to one user and one event, and joining is idempotent, so reusing it
  before it expires (`POW_CHALLENGE_TTL`, 2 minutes) gains nothing.
- **Adaptive:** the difficulty is `POW_DIFFICULTY` (18 bits) normally, plus one
  bit for each doubling of this replica's challenge rate above
  `POW_SURGE_RATE` (200 per second), up to `POW_MAX_DIFFICULTY` (22). Each bit
  doubles every client's expected work, so a surge, when bots matter most,
  costs more per join.
- **Sizing:** the solver does about 0.9 million hashes a second on the
  development laptop (Node 24, the same engine as Chrome), so 18 bits takes
  about 0.3 s there. A mid-range phone is typically several times slower,
  about 1 to 2 s: the design's target. This is an estimate; real phones have
  not been measured yet. The time varies a lot from one challenge to the
  next, since the work is a lottery.
- With `POW_DIFFICULTY=0` (development and load tests; refused in production)
  the answer is `{"required":false}` and joins need no proof.
- An honest limit: proof of work raises the cost of automation, but it does
  not stop a determined, well-funded attacker. It is one layer, with rate
  limits, verified identities and the policy windows.

### `POST /v1/queue/{eventID}/join`

Puts the caller in the event's waiting room. The body carries the solved
challenge (with `POW_DIFFICULTY=0`, no body is needed):

```json
{"pow": {"challenge": "1759501720000.18.r4nd0m...", "nonce": "90165"}}
```

The proof of work is checked before anything else touches Valkey, even the
rate limits: it costs one HMAC and one SHA-256. No body, or no `pow`, is 400
`POW_REQUIRED`; a wrong nonce, or a challenge issued to someone else or for
another event, is 403 `POW_INVALID`; an expired one is 403 `POW_EXPIRED` (get
a new one).

The caller is the signed-in buyer: an auth-svc access token in
`Authorization: Bearer` (`docs/services/auth.md`), verified against
auth-svc's published keys. With `DEV_IDENTITY=true` (never in production),
a request without a token may name its buyer in `X-Dev-User-Id` instead; the
E2 load test does, to simulate 50,000 buyers.

```http
POST /v1/queue/0196f0c1-.../join
Authorization: Bearer <access token>
Content-Type: application/json

{"pow":{"challenge":"...","nonce":"90165"}}
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
- The event's policy windows (below) may refuse the caller with 403 and
  `Retry-After`: the seconds until they may join.
- Your rank: `GET /v1/queue/{eventID}/me` (below).

**Policy windows** (`internal/policy`, task 4.3). An event may have two,
set when its queue is provisioned (`verifiedOnlyUntil`, `agentLockoutUntil`):

| Window | Until it ends | Refusal |
|---|---|---|
| Verified only | Only buyers whose identity is verified may join: the access token's `vrf` claim, true after a phone-code sign-in. The development header is never verified | 403 `VERIFIED_ONLY` |
| Agent lockout | Buyers with the `AGENT` role may not join (the IRCTC Tatkal rule) | 403 `AGENT_LOCKOUT` |

When both refuse a caller, the window that ends later is reported, so a
buyer is never told to come back too early. The check runs before the join,
by queue-svc's clock (windows are minutes long; skew between hosts is
negligible). A refused caller gets no place. The windows only govern
joining: someone who joined is never removed. The per-user cap (I4) is
enforced where units are held and sold (inventory-svc and booking-svc).

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

Where the caller stands. Same identity as joining (an access token).

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

| `verifiedOnlyUntil` | Optional, RFC 3339: verified buyers only until then | After 2020-01-01; absent means no window |
| `agentLockoutUntil` | Optional, RFC 3339: no agents until then | After 2020-01-01; absent means no window |

Returns 201 when created and 200 when already provisioned with identical
settings, so retries are safe. Different settings return 409
`PROVISION_CONFLICT` and change nothing. Provisioning never moves a queue that
is already past `PRE` back to `PRE`. The policy windows are not part of the
fixed settings: every accepted call (201 or 200) sets them to its own, so an
operator can move or lift a window by provisioning again with the same
settings.

`holdfastctl event create` provisions the queue together with inventory:
`--admission-rate` (default 83), `--max-sessions` (default 10,000) and
`--session-ttl` (default 10m); the opening time is the event's `--opens-at`.
`--verified-only-for` and `--agent-lockout-for` (durations after opening;
default 0, no window) set the policy windows, stored in the catalog
(`booking.events`) and in the queue. It validates these flags before writing
anything to PostgreSQL.

`holdfastctl queue provision --event <id>` provisions the queue of an event
that already exists (created with `--no-provision`, or after Valkey lost its
keys: RB-Q-8). It takes the same three flags; the opening time and the policy
windows come from the event in PostgreSQL unless `--opens-at` is given (then
there are no windows). It is idempotent, and a conflict says how to read the
stored settings.

### `POST /internal/v1/events/{eventID}/freeze` and `/unfreeze` (admin port)

Operator token required. No body. The queue's half of the freeze switch
(runbook RB-1): `freeze` turns an `OPEN` queue `FROZEN`, so the leader admits
nobody; `unfreeze` turns it back. Joins, positions and claims of turns
already given carry on.

```http
HTTP/1.1 200 OK

{"eventId":"0196f0c1-...","state":"FROZEN","changed":true}
```

Switching again returns 200 with `"changed": false`. A queue in another state
(before T0, sold out, closed) returns 409 `STATE_CONFLICT` and is left alone.
inventory-svc's endpoint of the same path stops new holds; `holdfastctl
freeze --event <id>` and `unfreeze` switch both halves, holds first.

`holdfastctl queue status --event <id>` shows what an operator needs in one
place: the state (and the stored state, if T0 has passed but nobody has
flipped it yet), the opening time, the queue size, `admittedUpTo`, active
session slots (unexpired by Valkey's clock) against the budget, the admission
rate and session TTL, the leader's epoch, the status document's age, and
whether the event is on the work list, and the policy windows. `--json`
prints the same as JSON.

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
  (`pg_try_advisory_lock`, keyed by FNV-1a of the event ID). A replica holds
  all of its events' locks on **one** connection taken out of the pool
  (`LockSession`; ADR 0007's amendment, P39), so queue-svc uses one
  connection for leadership however many events it runs. A term that ends
  releases its lock. If the replica's process dies, or its session drops,
  every lock it held is released at once, and the standbys take over on
  their next attempt (`LEADER_RETRY_INTERVAL`, 2 s).
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
- **By the units left (P17).** With `INVENTORY_GRPC_ADDR` set, the leader
  reads the event's availability from inventory-svc (`GetAvailability` over
  gRPC, with a deadline of one tick) before each advance. Active sessions are
  then also capped at the units left × `OVERSUBSCRIPTION_FACTOR` (1.3: not
  everyone admitted buys), inside the same `advance.lua` step, so thousands
  are not admitted to fight over the last few units; units returned by
  expired holds reopen admission. When no units are left and no hold is open
  (none can return any), the leader marks the queue `SOLD_OUT` with the
  fenced `soldout.lua`: joins and claims are refused from then on, and the
  status document says so on the same tick. A `FROZEN` queue is never marked
  (freezing is the operator's call). If inventory cannot be read, the tick
  admits without the units cap rather than stopping: inventory still refuses
  every hold beyond capacity, so failing open costs buyers a `SOLD_OUT` answer
  from inventory, never a unit. A queue with no sale in inventory (a load
  test's) is never capped.
- **Controllers follow the work list.** A replica starts a controller for each
  event in `q:events` (rescanned every `ADMISSION_RESCAN_INTERVAL`). A
  controller whose event no longer exists (its settings are gone) stops, and
  reads the settings before starting a term so it never recreates the event's
  keys; if the event is provisioned again, a new controller starts.
- PostgreSQL is used only for elections. Readiness does not depend on it: if it
  is down, admissions pause and joining and positions keep working.

The oversubscription factor is fixed for now; tuning it from the observed
conversion rate, and adaptive admission (AIMD), are Phase 5.

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
| `UNAUTHENTICATED` | 401 | Buyer endpoints: no access token, or one that is invalid or expired (refresh it at auth-svc). Admin: missing or wrong operator token |
| `POW_REQUIRED` | 400 | Join: no solved challenge in the body |
| `POW_INVALID` | 403 | Join: the nonce does not solve the challenge, or the challenge was not issued to this user for this event |
| `POW_EXPIRED` | 403 | Join: the challenge has expired; get a new one |
| `VERIFIED_ONLY` | 403 | Join: only verified buyers may join until the window ends; `Retry-After` gives the seconds left |
| `AGENT_LOCKOUT` | 403 | Join: agents may not join until the lockout ends; `Retry-After` gives the seconds left |
| `INVALID_REQUEST` | 400 | Malformed event ID, or a setting outside its bounds (`detail` says which) |
| `INVALID_BODY`, `MALFORMED_JSON`, `EMPTY_BODY`, `TRAILING_DATA`, `INVALID_FIELD_TYPE` | 400 | Provisioning body problems, including unknown fields |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | Provisioning body is not JSON |
| `EVENT_NOT_FOUND` | 404 | Join, position or status: the event has no waiting room (not provisioned) |
| `NOT_IN_QUEUE` | 404 | Position or admit: the caller has not joined this event's queue |
| `QUEUE_CLOSED` | 409 | Join or admit: the queue is `SOLD_OUT` or `CLOSED` |
| `NOT_YOUR_TURN` | 409 | Admit: your rank is above `admittedUpTo` |
| `TURN_EXPIRED` | 409 | Admit: you were admitted, but your session slot has run out |
| `PROVISION_CONFLICT` | 409 | Queue already provisioned with different settings |
| `STATE_CONFLICT` | 409 | Freeze or unfreeze: the queue is in a state the switch does not apply to (`detail` says which) |
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
switch moves `OPEN` to `FROZEN` and back (`state.lua`, a compare-and-set).
The admission leader moves `OPEN` to `SOLD_OUT` when inventory is gone
(`soldout.lua`). Nothing sets `CLOSED` yet.

## Keyspace

| Key | Type | Content |
|---|---|---|
| `q:{E}:config` | hash | `opens_at_ms`, `admission_rate`, `max_sessions`, `session_ttl_ms`; written once by `provision.lua` |
| `q:{E}:state` | string | `PRE`, `OPEN`, `FROZEN`, `SOLD_OUT` or `CLOSED` |
| `q:{E}:members` | sorted set | user ID → score: lottery score in [0, 1) before T0, 1 plus the arrival number after |
| `q:{E}:seq` | integer | Arrival counter for joins after T0 |
| `q:{E}:admitted` | integer | `admittedUpTo`, the highest admitted rank |
| `q:{E}:status` | string (JSON) | The status document: `state`, `opensAtMs`, `admittedUpTo`, `queueSize`, `updatedAtMs`; rewritten every tick by the leader |
| `q:{E}:policy` | hash | `verified_only_until_ms`, `agent_lockout_until_ms` (0: no window); set by every accepted `provision.lua` call |
| `adm:{E}:epoch` | integer | Fencing token: incremented by every new admission leader |
| `adm:{E}:sessions` | sorted set | Admitted rank → session expiry in ms; the concurrency budget |
| `q:events` | set | Provisioned events: the opener's and the admission controllers' work list (one global key, never used inside multi-key scripts) |
| `rl:SCOPE:ID` | hash | Token bucket: `tokens`, `ts_ms`; expires once the bucket would be full again |

The design doc's `jti:*` key (single-use admission tokens, section 8.2) is not
built: task 2.8 decided that tokens stay reusable within their session (below).

## Scripts

| Script | Replies |
|---|---|
| `provision.lua` | 1 created, 0 identical settings, -1 conflict; sets the policy windows unless -1 |
| `join.lua` | {1 joined, 0 already joined, -1 closed, -2 not provisioned, member's score, 1 if this join opened the queue at T0} |
| `open.lua` | {1 opened, 0 nothing to do, -1 not provisioned; ms late after T0, or ms left until T0} |
| `position.lua` | {1 ranked, 0 before T0, -1 not in queue, -2 not provisioned; rank or opens_at_ms; state} |
| `advance.lua` | {1 admitted some, 0 nothing to admit, -1 fenced, -2 not provisioned; admittedUpTo, admitted now, active sessions, queue size, state shown}; takes the units cap (-1: none); also rewrites `q:{E}:status` unless fenced |
| `soldout.lua` | 1 marked `SOLD_OUT`, 0 not `OPEN`, -1 fenced |
| `state.lua` | {1 moved, 0 already in the target state, -1 in another state, -2 not provisioned; the state after} |
| `status.lua` | {1 the leader's document, 0 fallback without `updatedAtMs`, -2 not provisioned; the document as JSON} |
| `admit.lua` | {1 admitted, 0 not your turn, -1 not in queue, -2 not provisioned, -3 turn expired (slot gone, or past its expiry by Valkey's clock), -4 closed; rank; slot expiry ms or admittedUpTo} |
| `token_bucket.lua` (`internal/platform/ratelimit`) | {allowed 1 or 0, remaining tokens × 1000, retry after ms} |

## Tests

| Layer | What it covers | Where |
|---|---|---|
| Unit | The leader's allowance (rate, one-second cap, fractions); the state gauge; handlers, validation and error mapping, policy refusals with `Retry-After`, the freeze routes; lottery scores; the policy rules; the Spearman helper | `admission_test.go`, `handler_test.go`, `lottery_test.go`, `model_test.go`, `internal/policy`, `internal/stats` |
| Fake clock | The leader's tick loop under `testing/synctest`: exact tick times, no starting burst, carried allowance capped at one second, stepping down when fenced, when the lock's session is lost or when the event is gone, surviving a failed tick; the units cap from inventory (and none when it fails or has no sale), the sold-out mark, fencing while marking | `admission_synctest_test.go` |
| Proof of work | Issue, solve and verify; another user, another event, a forged difficulty, a foreign MAC, bad nonces and expiry refused; the difficulty rising with the rate and falling after a quiet second; a hash vector computed independently (Python's hashlib) that the TypeScript solver must match too; the challenge route and every join refusal, none of which reaches the limiter or the service | `internal/pow`, `handler_test.go`, `web/src/lib/pow/solve.test.ts` (`node --test`) |
| Policy and freeze | Windows stored and checked at join, replaced by re-provisioning, kept on a refused one; the freeze switch's states; the units cap in `advance.lua`; the fenced sold-out mark, which leaves a frozen queue alone | `policy_integration_test.go` |
| Integration | Every script against real Valkey: provisioning, joins (idempotent, no re-roll, concurrent), the T0 switch by Valkey's clock, positions, a stale epoch refused, the session budget, slot expiry, the status document, claims; leadership: one session holding 12 events' locks on one connection, and a killed session handing every event to the other replica with newer epochs | `store_integration_test.go`, `admission_integration_test.go` |
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
| `ACCESS_JWKS_URL` | none | auth-svc's key set (`http://auth:8080/.well-known/jwks.json` in Compose): buyers are identified by its access tokens. Required unless `DEV_IDENTITY` is on; readiness waits until a key is known |
| `DEV_IDENTITY` | `false` | Also accept `X-Dev-User-Id` from requests without a token (development, load tests). Anyone can claim any user ID with it, so queue-svc refuses to start with it when `ENVIRONMENT=production` |
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
| `INVENTORY_GRPC_ADDR` | none | inventory-svc's gRPC API (`inventory:7070` in Compose). Set: leaders cap sessions by the units left and mark sold-out queues `SOLD_OUT`. Empty: neither (logged at start) |
| `SERVICE_PRIVATE_KEY_FILE` | none | Ed25519 private key (PEM) signing queue-svc's service tokens for inventory; required with `INVENTORY_GRPC_ADDR`. Compose mounts `queue.key` from `make keys`; inventory trusts `queue.pub` |
| `OVERSUBSCRIPTION_FACTOR` | `1.3` | Sessions per unit left (1 to 10) |
| `POW_DIFFICULTY` | `18` | Proof-of-work bits at normal load; `0` turns proof of work off (refused in production) |
| `POW_MAX_DIFFICULTY` | `22` | Most bits during a surge (up to 30) |
| `POW_SURGE_RATE` | `200` | Challenges per second, per replica, above which the difficulty rises |
| `POW_CHALLENGE_TTL` | `2m` | Lifetime of a challenge (10s to 10m) |
| `POW_SECRET` | required with proof of work | HMAC key of the challenges, at least 32 characters, the same on every replica; removed from the environment after loading |

Locally, Compose maps the public port to 8082 and the admin port to 9092,
and points `ACCESS_JWKS_URL` at auth-svc; `DEV_IDENTITY=true make up` (or
the E2 load test's override) turns the development header on.

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_queue_joins_total` | `result` | Join outcomes: joined, already_joined, rate_limited_ip, rate_limited_user, closed, not_found, invalid, agent_lockout, verified_only, pow_required, pow_invalid, pow_expired, error |
| `holdfast_queue_pow_challenges_total` | | Proof-of-work challenges issued |
| `holdfast_queue_pow_difficulty` | | Bits of the last challenge this process issued: above `POW_DIFFICULTY` during a surge |
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
| `holdfast_queue_state` | `event`, `state` | 1 for the queue's state as the status document shows it, 0 for the four others; set by the leader on every tick, so it follows switches made by `holdfastctl` too |
| `holdfast_queue_inventory_reads_total` | `result` | Leaders' availability reads: ok, not_provisioned (no sale in inventory: no cap), error (no cap this tick) |
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
