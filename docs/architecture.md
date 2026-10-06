# Architecture context pack

A compact map of the system as built. The design doc
(`docs/design/holdfast-design-and-build-plan.mdx`) is the long-form rationale;
where Drop 1 deviates from it, ADRs 0003 and 0004 record why.

## 1. Summary

HoldFast sells scarce inventory during extreme demand spikes. Built so far:
the hot path (inventory-svc: atomic holds in Valkey), the durable backstop (the
PostgreSQL final guard), the waiting room in front of them (queue-svc and the
NGINX edge, Phase 2), the operator CLI, and experiments that prove no oversell
on every CI run (E1), a fair queue order (E6) and a flat origin under a
stampede (E2).

## 2. Invariants

IDs match section 2.3 of the design doc.

| ID | Invariant | Enforced by (Drop 1) |
|---|---|---|
| I1 | No oversell: confirmed units never exceed capacity | `hold.lua` (atomic check and decrement); the guard's conditional `UPDATE` plus the `no_oversell` CHECK |
| I2 | No double charge: at most one captured payment per booking | Payment intents and webhook dedup, Phase 3 |
| I3 | Money safety: every captured payment ends CONFIRMED or REFUNDED | Booking and payment saga, Phase 3; status polling; the reconciler against the provider's settlement report (task 5.1) |
| I4 | Per-user cap: held plus sold units per user per event never exceed the limit | Per-user counter in `hold.lua`; conditional upsert in the guard |
| I5 | No lost units: every hold ends SOLD or RELEASED | Expiry index plus sweeper; `release.lua` and `confirm.lua` |
| F1 | Fairness: random order before T0, FIFO after, one slot per identity | `join.lua`: `ZADD NX` with a `crypto/rand` score before T0 and an arrival number after, T0 judged by Valkey's clock (ADR 0005); checked by the F1 model test and experiment E6 |

## 3. Services and ownership

| Service | Status | Owns | Talks to |
|---|---|---|---|
| inventory-svc (`cmd/inventory`) | Built; internal gRPC API from task 3.4; the freeze flag from task 4.3 | Valkey keys `inv:*` | Valkey; called by booking-svc and queue-svc over gRPC |
| queue-svc (`cmd/queue`) | Built (Phase 2): provisioning, joining, the T0 transition, positions, admission, the status document, admission tokens and their JWKS; task 4.3: policy windows, the freeze switch, admission by units left and `SOLD_OUT` | Valkey keys `q:*`, `adm:*`, `rl:*` | Valkey; PostgreSQL for leader election only; inventory-svc over gRPC (units left) |
| NGINX edge (`deploy/nginx`) | Built (Phase 2) | Nothing | queue-svc, inventory-svc, booking-svc, auth-svc, the web app |
| web app (`web/`) | Built (Phase 4, task 4.5): Next.js static export served by nginx; `docs/web.md` | Nothing (static files) | The public API, through the edge |
| booking-svc (`cmd/booking`) | Built (Phase 3): the idempotent booking API, the deadline job, the saga (ADR 0010) | `booking` schema (with the final guard, `internal/booking/guard`) | PostgreSQL; inventory-svc and payment-svc over gRPC |
| payment-svc (`cmd/payment`) | Built (Phase 3): intents, provider orders, webhooks, the ledger, status polling, refunds | `payment` schema | PostgreSQL; the payment provider over HTTPS; called by booking-svc over gRPC; receives the provider's webhooks |
| mockpsp (`cmd/mockpsp`) | Built (task 3.10): test tool, never deployed | In-memory orders and refunds | Sends signed webhooks to payment-svc; called by payment-svc; fault injection on its admin port |
| holdfastctl (`cmd/holdfastctl`) | Built | Nothing (operator tool) | PostgreSQL, Valkey, Kafka (topic creation) |
| auth-svc (`cmd/auth`) | Built (Phase 4, task 4.1): sign-in with a phone code (mock SMS), access tokens, rotating refresh tokens with reuse detection, JWKS | `auth` schema | PostgreSQL; its JWKS is fetched by the services that verify access tokens |
| auditor (`cmd/auditor`) | Built (Phase 5, task 5.1): checks I1 to I5 every 15 s; `docs/services/auditor.md` | Nothing (read-only observer) | Reads every schema over read-only connections, and inventory's Valkey keys: the one exception to rule 4 (design doc 8.1) |

## 4. Runtime topology

Each service listens on two ports:

- **public** (`HTTP_ADDR`, default `:8080`): the buyer-facing API behind the edge;
- **admin** (`ADMIN_ADDR`, default `:9090`): `/metrics`, `/livez`, `/readyz`,
  `/buildz`, `/debug/pprof/*` and operator APIs. Internal network only.

A service that other services call also serves **gRPC** (`GRPC_ADDR`;
inventory-svc and payment-svc on `:7070`), internal network only, through
`internal/platform/grpcx`: service-token authentication with a per-method
allowlist, required deadlines, tracing and metrics on the server; tokens, an
800 ms default deadline and bounded retries of `UNAVAILABLE` on the client.

Locally, `compose.yaml` runs PostgreSQL 18, Valkey 9.1, a one-shot migration
job, Kafka 4.3 (KRaft, with a one-shot topic job and Redpanda Console),
inventory-svc, queue-svc, booking-svc, payment-svc, mockpsp, the NGINX edge, Prometheus, Grafana, the
OpenTelemetry Collector and Jaeger.

**The edge** (`deploy/nginx/nginx.conf`, host port 8088) is the buyers' front
door, the role a CDN plays in production:

- routes `/v1/queue/*` and `/.well-known/jwks.json` to queue-svc, the hold
  paths to inventory-svc, `/v1/bookings` and the event catalog
  (`/v1/events`, `/v1/events/{id}`) to booking-svc, and `/v1/auth/*` to
  auth-svc; any other `/v1/` path, admin paths included, is a 404;
- serves the web app (`/`, everything outside `/v1/`) from the `web`
  container, caching pages for a minute and hashed assets for a year, keyed
  by path alone, so one cached page serves every event; the catalog is
  cached for its minute too;
- micro-caches `/v1/events/{id}/status` and `/v1/events/{id}/availability`
  for one second with `proxy_cache_lock` (concurrent misses wait for one
  origin fetch), keyed by path only, serving stale copies while updating or
  if the origin fails;
- rate-limits `/v1/queue/` per client address (10/s, burst 20; the zone is in
  `deploy/nginx/edge-limits.conf`, which `make load-e2` swaps out);
- overwrites `X-Forwarded-For`, `X-Real-IP` and `X-Request-Id`. It sits on its
  own network at a fixed address that queue-svc trusts (`TRUSTED_PROXIES`).

## 5. Request lifecycles

### The buyer's journey (through the edge)

1. Before or after T0: `POST /v1/queue/{id}/join`. The event's policy windows
   come first (`internal/policy`): until they end, only verified buyers may
   join and agents may not (403 with `Retry-After`). Before that the join
   must carry a solved proof-of-work challenge (`GET /v1/queue/{id}/challenge`,
   solved in a Web Worker; `internal/pow`), checked with one hash before
   anything touches Valkey. Then `join.lua` gives a
   lottery score before T0 (by Valkey's clock), an arrival number after, and
   never a second place (ADR 0005).
2. After T0: `GET /v1/queue/{id}/me` once, for the buyer's rank.
3. Every few seconds: `GET /v1/events/{id}/status`, answered by the edge's
   one-second cache; the origin sees about one request every second or two
   per event, whatever the crowd (ADR 0006, experiment E2).
4. Meanwhile one admission leader per event (ADR 0007) reads the units left
   from inventory-svc over gRPC and runs `advance.lua` every 250 ms: it moves
   `admittedUpTo` at the event's rate, gives each newly admitted rank a
   session slot for the session TTL, and never exceeds the session budget nor
   the units left × 1.3. With no units left and no open hold, the leader
   marks the queue `SOLD_OUT`.
5. Once `admittedUpTo` reaches the buyer's rank: `POST /v1/queue/{id}/admit`.
   `admit.lua` checks the rank and that the slot is alive by Valkey's clock;
   queue-svc signs an Ed25519 admission token that expires with the slot.
6. With the token: `POST /v1/events/{id}/holds` at inventory-svc, which
   verifies it against queue-svc's JWKS (below).
7. With the hold: `POST /v1/bookings` at booking-svc (idempotent; a retry
   resumes). booking-svc checks the hold and marks it PAYING over gRPC, then
   writes the booking and its `booking.created` event in one transaction.
   It then gets the booking's payment intent from payment-svc over gRPC,
   which opens an order at the provider keyed by the intent ID, and answers
   with the `checkoutUrl`. The deadline is the hold's protection minus a
   3-minute grace (design doc 6.1).
8. The buyer pays at the provider (locally mockpsp's checkout page). Its webhook (`POST /v1/webhooks/psp` at
   payment-svc, HMAC-verified, deduplicated by event ID) captures the intent,
   books it in the ledger and writes `payment.captured.v1`, all in one
   transaction. Intents with no news after 2 minutes are polled.
9. booking-svc's saga consumes `payment.captured.v1`. In one transaction it
   runs the final guard and confirms the booking (`booking.confirmed.v1`),
   or marks it `REFUND_REQUIRED` if the guard refuses. After the commit it
   makes the hold SOLD over gRPC (or releases it). A refused sale is refunded
   by payment-svc through the provider, and the booking ends `REFUNDED`. A
   failed or expired payment cancels the booking and releases its hold.

### `POST /v1/events/{eventID}/holds`

1. Global middleware, outermost first: request ID, access log, metrics, panic
   recovery, security headers, body limit, request timeout.
2. `RequireAdmission` verifies the Ed25519 admission token: issuer, audience,
   expiry, trusted key ID.
3. The handler checks the token's event matches the path, requires
   `Idempotency-Key` and strictly decodes `{"quantity": n}`.
4. The service canonicalises the IDs, validates the key and quantity, and
   derives the hold ID as UUIDv5(user, idempotency key).
5. The store runs `hold.lua`: provisioned? quantity valid? replay? sale frozen?
   per-user cap? units left? Then it decrements, records the hold and indexes
   its expiry, all in one atomic step.
6. The reply code maps to 201 (held or replayed) or to a problem document
   (409 SOLD_OUT, 422 USER_LIMIT, 503 SALE_PAUSED, and so on).

## 6. Data

**Valkey** (hot, rebuildable). Every key of an event shares the hash tag
`{eventID}`; see `docs/services/inventory.md` (`inv:*`) and
`docs/services/queue.md` (`q:*`, `adm:*`, `rl:*`) for the keyspace. Inventory
can be rebuilt from PostgreSQL; a queue's settings can be put back
(`holdfastctl queue provision`, RB-Q-8), but who had joined lives only in
Valkey.

**PostgreSQL** (truth), schema `booking`:

- `events`: catalog, with application-generated UUIDv7 IDs;
- `event_inventory`: capacity and sold, with `CHECK (sold <= capacity)`;
- `user_event_purchases`: per-user units, the I4 guard;
- `bookings` (from Phase 3): one per hold, with a trigger that refuses any
  status move outside the state machine (`PENDING_PAYMENT` to `CONFIRMED`,
  `CANCELLED` or `REFUND_REQUIRED`; a late capture from `CANCELLED`;
  `REFUND_REQUIRED` to `REFUNDED`);
- `idempotency_keys`, the transactional `outbox` and `processed_messages`,
  the tables every service schema gets for retried requests, events and
  redelivered messages.

Schema `payment` (from Phase 3, owned by payment-svc, task 3.9):

- `payment_intents`: one per booking (invariant I2), with a trigger for the
  state machine in design doc 7.3; a capture wins from `FAILED` or `EXPIRED`,
  because money moved;
- `psp_orders`, and `webhook_events` keyed by the PSP's event ID, so a
  duplicate delivery inserts nothing;
- `ledger_entries`: double-entry bookkeeping; a deferred constraint trigger
  refuses, at commit, any transaction whose debits and credits differ;
- `outbox` and `processed_messages`, as in `booking`.

Queries on both schemas are hand-written SQL with typed Go generated by sqlc
(`internal/<service>/queries` to `internal/<service>/<service>db`; ADR 0011);
every state change is one conditional statement.

Migrations are embedded SQL applied by `holdfastctl migrate` (ADR 0003).

**Contracts** (from Phase 3). Service-to-service APIs and event payloads are
Protobuf, in one Buf workspace (`buf.yaml`, `proto/`): `holdfast.inventory.v1`
(inventory-svc's internal gRPC API) and `holdfast.events.v1` (the Kafka event
payloads). Go code is generated into `internal/gen` by `make gen` and
committed. CI lints them, checks formatting, refuses wire- or JSON-breaking
changes against `main` (fields may be added, never renumbered or removed),
and fails if the generated code is stale.

**Kafka** (events between services, from Phase 3). Topics are created
explicitly (`holdfastctl kafka topics`; auto-creation is off), each with a
dead-letter topic:

| Topic | Key | Producer |
|---|---|---|
| `holdfast.booking.v1` | booking ID | booking-svc, via its outbox; consumed by payment-svc (`payment-refunds`) |
| `holdfast.payment.v1` | booking ID | payment-svc, via its outbox; consumed by booking-svc (`booking-saga`) |
| `holdfast.inventory.v1` | event ID | inventory-svc, informational snapshots |
| `<topic>.dlq.v1` (for example `holdfast.booking.dlq.v1`) | original key | any consumer, after repeated failures |

Payloads are Protobuf messages from `proto/holdfast/events/v1` (booking and
payment events, each documented with its `ce_type`). Every message carries CloudEvents attributes as headers (`ce_id`, the
deduplication key; `ce_type`; `ce_source`; `ce_time`). `internal/platform/kafka`
produces with `acks=all` and idempotence, and consumes with manual commits:
an offset is committed only after the handler finishes, so handlers must be
idempotent, and a message that keeps failing is dead-lettered instead of
stalling its partition.

## 7. Background work

The expiry sweeper runs in every replica, every `SWEEP_INTERVAL`, in batches
of `SWEEP_BATCH`. `release.lua` re-checks state and expiry against the Valkey
server clock, so racing sweepers are harmless and application clock skew can
only delay a release, never cause an early one.

The queue opener runs in every queue-svc replica, every `OPEN_CHECK_INTERVAL`.
`open.lua` flips a queue from PRE to OPEN only once the Valkey server clock
reaches its opening time, so racing openers are harmless. Joins apply the same
rule themselves, so the opener's timing never affects who gets a lottery
position.

Admission controllers run for every event in every queue-svc replica. Exactly
one per event leads, holding a PostgreSQL advisory lock; a replica holds all
its events' locks on one shared connection (ADR 0007, amended). Every 250 ms
the leader reads the units left from inventory-svc (`GetAvailability`, a
deadline of one tick) and runs `advance.lua`, which refuses a stale epoch
(fencing) and caps concurrent sessions by Little's Law and by the units left
× `OVERSUBSCRIPTION_FACTOR`. If inventory cannot be read, the tick admits
without the units cap (inventory still refuses every hold beyond capacity).
The fenced `soldout.lua` marks a queue `SOLD_OUT` once no units are left and
no hold is open.

**The freeze switch** (runbook RB-1, `docs/runbooks/sale.md`) is two flags:
inventory's `frozen` field stops new holds, and the queue's `FROZEN` state
stops admissions. `holdfastctl freeze` sets both, holds first.

The **outbox relay** runs in every replica of a service that writes events
(booking-svc and payment-svc); exactly one per schema leads, holding a
PostgreSQL advisory lock on a dedicated connection, and publishes the outbox
to Kafka in commit order, marking each batch published in the same
transaction (`internal/platform/outbox`; ADR 0009).

**Consumers** run in every replica as Kafka consumer groups:

- booking-svc's saga (`booking-saga` on `holdfast.payment.v1`; ADR 0010);
- payment-svc's refunds (`payment-refunds` on `holdfast.booking.v1`).

Each message is applied in one transaction with its dedup record. Offsets
are committed after it. A message that keeps failing goes to its
dead-letter topic, which `holdfastctl dlq replay` drains (runbook RB-3,
`docs/runbooks/saga.md`).

booking-svc's **deadline job** cancels overdue `PENDING_PAYMENT` bookings
(`FOR UPDATE SKIP LOCKED`). payment-svc's **status poller** asks the
provider about quiet intents, least recently polled first.

payment-svc's **reconciler** (task 5.1) runs in one replica at a time, under
an advisory lock, every 5 minutes: it compares the provider's settlement
report for the last 2 hours with the intents and the ledger, applies
captures and refunds whose webhooks were lost through the same idempotent
paths, asks again for refunds stuck in `REFUND_PENDING`, and reports what
only a human can decide (`docs/services/payment.md`).

The **auditor** (`cmd/auditor`, task 5.1) recomputes I1 to I5 from the
stores every 15 seconds and publishes the number of violations of each
(`docs/services/auditor.md`).

## 8. Security

- queue-svc issues admission tokens when a buyer's turn comes
  (`POST /v1/queue/{id}/admit`), capped at their session slot, and publishes
  its public keys at `/.well-known/jwks.json`.
- Buyers present admission tokens: EdDSA JWTs bound to one user and one
  event, with audience `holdfast-inventory` and issuer `holdfast-queue`.
  inventory-svc follows queue-svc's JWKS (refreshed every 5 minutes, and on an
  unknown key ID at most every 30 seconds), so key rotation needs no restart.
  Tokens are usable for their short life, not single use, so retries work;
  reuse is bounded by the session-capped expiry, the user and event binding,
  idempotent holds and the per-user cap (decision record in
  `docs/services/queue.md`). Only
  the EdDSA algorithm is accepted, and `kid` selects among trusted public keys
  so rotation needs no downtime.
- auth-svc signs people in with a one-time code sent to their phone
  (`docs/services/auth.md`). It issues 15-minute EdDSA access tokens, and
  refresh tokens in an httpOnly, SameSite=Strict cookie that rotate on every
  use; presenting a rotated one revokes the whole login. Phones, codes and
  refresh tokens are stored only as HMACs or hashes. Why short-lived access
  tokens and rotating refresh tokens: ADR 0012.
- queue-svc and booking-svc identify buyers by those access tokens
  (`authn.RequireUser`), verified against auth-svc's JWKS
  (`ACCESS_JWKS_URL`); a token that fails is refused, with no fallback. With
  `DEV_IDENTITY=true` (development and load tests only; refused in
  production) a request without a token may name its buyer in
  `X-Dev-User-Id`.
- The policy windows (`internal/policy`) use the access token's `role` and
  `vrf` (verified) claims: verified buyers only, and no agents, until each
  window ends. A development-header caller is never verified. Every sale
  rule, and where it is enforced: `docs/services/policy.md`.
- Joining passes per-IP (IPv6 per /64) and per-user token buckets in Valkey
  (`internal/platform/ratelimit`), shared by every replica.
- Joining also costs a proof of work (`internal/pow`): a stateless HMAC
  challenge bound to the user, the event and an expiry, whose difficulty
  rises with the challenge rate. It raises the cost of bots, not a wall.
- Services call each other over gRPC with service tokens: short-lived
  Ed25519 JWTs signed by the caller's own key and checked against that
  caller's public key, with a per-method allowlist (decision record in
  `docs/services/inventory.md`). Traffic between services is plaintext on the
  internal network until Phase 6 adds TLS or a mesh.
- Operator endpoints live on the admin port behind a static bearer token
  compared in constant time; auth-svc replaces it in Phase 4.
- IDs are canonicalised to lower-case hyphenated UUIDs before they become
  Valkey keys, so user input cannot inject key syntax (`{`, `}`, `|`).
- Secrets are read from the environment and removed from it after loading.
  Images are distroless and run as a non-root user.

## 9. Observability

- **Logs:** JSON via `log/slog`, tagged with service, env, version and
  `request_id`. Client errors log at WARN, server errors at ERROR. Setting
  `HTTP_ACCESS_LOG_SUCCESS=false` turns off per-request success logs, which
  cost a lot during a surge.
- **Metrics:** `holdfast_holds_total{result}`,
  `holdfast_hold_script_duration_seconds`, `holdfast_hold_releases_total{mode}`,
  `holdfast_hold_confirms_total{outcome}`, `holdfast_inventory_available{event}`,
  `holdfast_sweeper_*`, `holdfast_http_*` (labelled by route pattern, never
  raw paths) and `holdfast_build_info`. queue-svc adds `holdfast_queue_*`:
  join, position, claim, tick and opener outcomes, and per-event gauges for
  queue size, `admittedUpTo`, sessions, the session budget, the leader's epoch,
  the queue's state and the status document's age, and the leaders' inventory
  reads (`docs/services/queue.md`). Kafka consumers
  count `holdfast_kafka_consumed_total{topic,result}` (ok, retried,
  dead_lettered).
- **Correctness:** the auditor publishes
  `holdfast_invariant_violations{invariant}` (I1 to I5, every value must be
  0), and the reconciler `holdfast_recon_mismatch_total{type}`.
- **Alerts** (task 5.1, `deploy/alerting/holdfast.rules.yml`, loaded by
  Prometheus): `InvariantViolation`, `MoneySafetyBreach`,
  `ReconciliationNeedsAHuman`, `AuditorStale` and `ReconcilerStale`, each
  with its runbook in `docs/runbooks/auditor.md`. Nothing routes them yet
  (Alertmanager is task 5.6); Prometheus's Alerts page shows them.
- **Dashboards:** Grafana. **HoldFast / Mission control** (design doc 12.4,
  task 4.6) puts a live sale on one screen, filtered by event:
  - the queue: state, size, `admittedUpTo`, sessions against the budget, the admission rate, the proof-of-work difficulty, joins by result;
  - the sale: units available, holds per second, bookings confirmed, capture-to-confirm p99 against its 5 s SLO, booking outcomes, payments and the provider's breaker;
  - correctness: tiles for invariants I1 to I5 (from the auditor) and the reconciliation findings that need a human, which show grey "no data" if their publisher is down (never a false green), plus outbox lag, consumer lag, amount mismatches and late captures;
  - health: p50, p95 and p99 latency and the 5xx share per service, Valkey's CPU and command rate, PostgreSQL's transactions and connections.

  It is generated by `deploy/grafana/mission-control.py`. HoldFast / Inventory and HoldFast / Queue have the per-service detail.
- **Store exporters:** `redis_exporter` (works with Valkey) and
  `postgres-exporter` in Compose, scraped as jobs `valkey` and `postgres`.
  postgres-exporter cannot read the server's CPU, so the dashboard shows
  PostgreSQL's transactions per second and connections instead.
- **Health:** `/livez` never checks dependencies. `/readyz` checks Valkey (and,
  on inventory-svc with a JWKS URL, that some admission-token key is known) and
  returns 503 from the moment SIGTERM arrives.
- **Outbox and consumers:** `holdfast_outbox_lag_seconds{schema}` and
  `holdfast_outbox_pending{schema}` show whether a service's relay keeps up;
  `holdfast_kafka_consumer_lag{group,topic}` how far each consumer group is
  behind.
- **Traces** (from Phase 3, `internal/platform/otel`): every public request
  gets a server span named after its route pattern, continuing the caller's
  W3C `traceparent`; Kafka events carry `traceparent`, and consumers continue
  the producer's trace; Valkey commands get spans only inside a trace.
  The provider's webhook cannot carry our trace, so payment-svc stores the
  booking request's trace context with the intent and continues it when a
  webhook or a poll settles the intent (linked to the webhook's own trace).
  A purchase is therefore one trace: the booking request (booking,
  inventory, payment, the provider call), then the capture, the saga and the
  inventory confirmation. Spans go over OTLP to the
  OpenTelemetry Collector, which forwards them to Jaeger
  (http://localhost:16686). Log lines carry `trace_id` and `span_id`.

## 10. Failure modes

| Failure | Behaviour | Runbook |
|---|---|---|
| Valkey unreachable | 503 `UNAVAILABLE` with `Retry-After`; readiness fails | RB-INV-1 |
| Valkey data lost | Rebuild the pool from PostgreSQL (capacity minus sold) | RB-INV-4 |
| Something looks wrong during a sale | Freeze: no new holds, no admissions; holds and payments in flight carry on | RB-1 |
| inventory-svc unreachable from queue-svc | Admission ignores the units left until it is back (fails open; inventory still guards capacity) | RB-Q-5 |
| Sweeper failing | Expired holds keep units; any healthy replica recovers them | RB-INV-2 |
| Payment confirmed after hold expiry | `confirm.lua` re-takes the units ("late") instead of dropping the sale | RB-INV-6 |
| Pod killed | Readiness drains first; in-flight requests finish within `SHUTDOWN_TIMEOUT` | n/a |
| queue-svc or inventory-svc down behind the edge | The edge keeps serving the last cached status and availability (stale, up to 30 s); other paths return 502 | RB-Q-2, RB-INV-1 |
| A provider webhook lost, and polling gave up | The reconciler applies the capture or refund from the settlement report within one pass (5 minutes) | RB-AUD-3 |
| An invariant violated | The auditor's gauge rises within 15 s; `InvariantViolation` fires | RB-AUD-1, RB-AUD-2 |
| The auditor or the reconciler stops | Its last-success timestamp stops; `AuditorStale` or `ReconcilerStale` fires | RB-AUD-4 |

## 11. Testing strategy

- Unit tests (`make test`) cover validation, error mapping, middleware, auth
  and config.
- Integration tests (`make itest`) run real Lua scripts and SQL: lifecycle
  scenarios, 2,000-goroutine contention, a randomized model check (1,500
  steps against an executable specification), guard concurrency and
  migration-runner guarantees. For the queue: every script against real
  Valkey, an F1 model test (random joins, ticks, claims, expiries, T0 and
  freezes against a reference model) and a failover test with two real
  admission controllers.
- Fake-clock tests (`testing/synctest`) drive the admission leader's tick loop
  through seconds of ticking with exact timings, in microseconds.
- The saga and payments (Phase 3), against real PostgreSQL, Kafka and an
  in-process mockpsp:
  - the saga's decision table: every payment event against a booking in
    every status, which moves only along design doc 7.2;
  - redelivery: every consumer applied twice; duplicates on the wire
    through the real Kafka consumer;
  - a relay that crashed after publishing republishes under the same IDs;
  - duplicate, delayed, late and out-of-order webhooks (a capture always
    wins; nothing moves backwards);
  - lost webhooks recovered by polling, and by the reconciler from the
    settlement report (task 5.1), which also reports what it cannot repair
    and runs in one replica at a time;
  - gRPC contracts with the production interceptors (inventory and
    payment, including the per-caller allowlists).
  - Tests that claim from a shared queue take a cross-process lock
    (`testenv.Exclusive`), and integration tests use their own database
    (`<name>_test`) and Valkey database (1).
- The auditor (task 5.1): each invariant violated on purpose and counted
  exactly, near misses not counted, its connections unable to write, and a
  failed check leaving the others published. It runs on a database and a
  Valkey database of its own (`testenv.NewIsolated`), which it empties.
- E1 (`make e1`, and every CI run): 50,000 buyers for 1,000 units, plus
  10,000 concurrent confirmations through the guard. Part B (`-mode
  purchase`) runs 2,000 whole purchases for 1,000 units through the real
  services in one process. Holds, bookings and payments at an in-process
  mockpsp that fails 10% of them; every payment event goes to the saga
  twice. It checks that every booking ends confirmed or cancelled, every
  hold SOLD or RELEASED, and that PostgreSQL, Valkey and the ledger agree on
  the sold count. It uses its own database, `<name>_e1`, so a running stack
  cannot act on its purchases.
- E6 (`make fairness-e6`): 100,000 joins before T0 and 20,000 after,
  checking with Spearman's rank correlation that join time does not predict
  a lottery position and that positions after T0 are the arrival order.
- E2 (`make load-e2`): a k6 stampede through the edge, comparing the status
  polls the edge answers with those that reach queue-svc. Results and their
  limits are in `loadtest/results/README.md`.

## 12. Adding a service

1. `cmd/<svc>/main.go`: compose `config` blocks, `logging.New`,
   `metrics.NewRegistry`, clients, `health.New`, `httpx.NewRouter` with the
   standard middleware, `httpx.NewAdminRouter`, then `app.Run`.
2. Domain code in `internal/<domain>`; its schema in `db/migrations/<schema>/`,
   registered in `db/migrations/embed.go`.
3. Add the service to `compose.yaml`, the CI image matrix and
   `deploy/prometheus/prometheus.yml`.
4. Document it in `docs/services/<svc>.md`, with a runbook.

## 13. Glossary

- **Hold:** a temporary claim on units, created before payment.
- **Admission token:** proof that the waiting room admitted a user to one event.
- **Final guard:** PostgreSQL conditional writes that confirm a sale.
- **Late confirm:** a payment that succeeds after its hold was released.
