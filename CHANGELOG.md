# Changelog

All notable changes to HoldFast. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
the milestone tags in the design plan (`docs/design/`, section 17.1).

## [Unreleased]

Hardened (milestone M5, build plan Phase 5): HoldFast survives its
failures, notices when something is wrong, and says how fast it recovers.
The reconciler, the invariant auditor, Valkey failover, inventory rebuilt
from PostgreSQL, adaptive admission, chaos experiments E3 and E4, alerting
with Alertmanager, and runbooks RB-1 to RB-5 practised and timed, with
ADR 0013.

### Added

- **The reconciler** (task 5.1, payment-svc): every 5 minutes, one replica
  compares the provider's settlement report for the last 2 hours with the
  records, applies captures and refunds whose webhooks were lost, asks
  again for refunds stuck 15 minutes, and reports what needs a person
  (`holdfast_recon_mismatch_total{type}`).
- **The invariant auditor** (task 5.1, `cmd/auditor`, port 9097):
  recomputes I1 to I5 every 15 s over read-only connections
  (`holdfast_invariant_violations{invariant}`), and inventory drift per
  event (`holdfast_inventory_drift_units{event}`, task 5.3).
- **Valkey high availability** (task 5.2): a replica and three Sentinels at
  fixed addresses; the services and holdfastctl follow the primary through
  the Sentinels; `holdfastctl valkey probe` and `make drill-valkey`.
- **`holdfastctl inventory rebuild`** (task 5.3, runbook RB-2): sets an
  event's pool, per-user counters and pending bookings' holds to
  PostgreSQL's records in one atomic script, holds frozen; `--dry-run`.
- **Adaptive admission** (task 5.4, ADR 0013): each admission leader runs
  AIMD on its rate from the payment provider's pressure (payment-svc's new
  `GetPressure` RPC), pausing while the provider's breaker is open;
  payment-svc probes the provider so the breaker can close again.
- **Chaos** (task 5.5): `cmd/buyers` (simulated buyers making whole
  purchases), `chaos/` with Toxiproxy, `make chaos-e3` and
  `make chaos-e4`; results in `loadtest/results/README.md`.
- **Alerting** (tasks 5.1 and 5.6): 15 Prometheus rules with promtool unit
  tests in `make lint` and CI, and Alertmanager (port 9098); runbook index
  `docs/runbooks/alerts.md`.
- **Runbooks** (task 5.7): RB-5 (the provider down), `make drill-rb5` and
  `make drill-rb4`, and `docs/runbooks/README.md` with every runbook's
  measured time.
- The README's "How fast it recovers" table; ADR 0013.

### Changed

- Kafka consumers retry a failure that is not permanent for 10 minutes
  before dead-lettering it (it was 5 attempts in about 2 seconds).
- The provider's settlement report is paged (`limit`, `after`, `next`).
- Prometheus loads only `*.rules.yml` from `deploy/alerting/`.

### Fixed

- P56: a 5 s database outage dead-lettered payments, leaving paid bookings
  cancelled (I3), and Valkey failovers left inventory drift.
- P55: admission stalled near the end of a sale while finished buyers'
  sessions counted against the units cap.
- P58: the reconciler failed on every pass once the settlement report
  outgrew 1 MiB.
- P57: inventory-svc's gRPC API reported Valkey failover errors as
  INTERNAL, not UNAVAILABLE.
- P47 (Sentinel stalled on hostnames), P48 (holdfastctl ignored
  `VALKEY_SENTINEL_MASTER`), P49 (planned switches now coordinated), P52
  and P54 (counters pre-registered), P53 (the breaker could not close
  under backpressure), P59 (reconciler log floods).

### Known issues

- On WSL2 the clock steps back every 30 s; Sentinel then sits in TILT mode
  and may not fail over (E26). Production hosts slew their clocks.
- Hold and booking p99 do not yet feed adaptive admission; only the
  payment provider's pressure does.
- mockpsp keeps its state in memory: restarting it makes the reconciler
  report the captures made before (silence the alert for its window).

## [0.4.0] - 2026-10-06

The resume-ready product (milestone M4, build plan Phase 4): a real person
signs in with their phone, joins a fair waiting room through a web app,
buys a ticket and watches the sale on one dashboard. auth-svc, the sale
rules (verified-only and agent-lockout windows, a freeze switch, admission
by units left), proof of work at join, the Next.js web app, the
mission-control dashboard and README v1 join the system, with ADR 0012.

### Added

- **auth-svc** (task 4.1, `cmd/auth`):
  - sign-in with a six-digit code sent to a phone through a mock SMS gateway
    (`POST /v1/auth/otp/request` and `/otp/verify`), with per-phone and
    per-address rate limits, 5 attempts per code, single use and a 5-minute
    life;
  - 15-minute EdDSA access tokens (`authn.AccessIssuer` and
    `AccessVerifier`) and the JWKS that verifies them;
  - refresh tokens in an httpOnly, SameSite=Strict cookie, rotated on every
    `POST /v1/auth/refresh`; presenting a rotated one revokes the whole
    login (reuse detection); `POST /v1/auth/logout`;
  - the `auth` schema (`auth/00001`), which stores phones, codes and refresh
    tokens only as HMACs or hashes;
  - in development, the code can be read at `GET /v1/auth/dev/inbox`.
  - The edge routes `/v1/auth/`; Compose runs it on 8086 and 9096; service
    doc `docs/services/auth.md`.
- **Real identity** (task 4.2): queue-svc and booking-svc identify buyers by
  auth-svc's access tokens (`Authorization: Bearer`), verified against its
  JWKS (`ACCESS_JWKS_URL`), with the user and role in the request context
  (`authn.RequireUser`). The development header `X-Dev-User-Id` now works
  only with `DEV_IDENTITY=true`, which Compose leaves off (the E2 load test
  turns it on for queue-svc). The README quickstart signs in with a phone
  code.
- **Sale policies** (task 4.3, `internal/policy`, `docs/services/policy.md`):
  - verified-only and agent-lockout windows per event, checked at join
    (403 `VERIFIED_ONLY` or `AGENT_LOCKOUT` with `Retry-After`), using the
    access token's role and a new `vrf` (verified) claim; set with
    `holdfastctl event create --verified-only-for` and `--agent-lockout-for`;
  - the **freeze switch**: inventory's `frozen` flag (new holds get 503
    `SALE_PAUSED`) and the queue's `FROZEN` state, with admin endpoints on
    both and `holdfastctl freeze` and `unfreeze`; runbook RB-1
    (`docs/runbooks/sale.md`);
  - **admission by units left** (P17): the leader reads inventory's new gRPC
    `GetAvailability` and keeps open sessions within units left × 1.3
    (`OVERSUBSCRIPTION_FACTOR`), and marks the queue `SOLD_OUT` once no units
    and no open holds remain.
- **Proof of work at join** (task 4.4, `internal/pow`):
  `GET /v1/queue/{id}/challenge` issues stateless HMAC challenges whose
  difficulty rises with the challenge rate (18 to 22 bits); joins carry the
  solution and are checked with one hash before any Valkey work (400
  `POW_REQUIRED`, 403 `POW_INVALID` or `POW_EXPIRED`). A TypeScript solver
  runs in a Web Worker; `holdfastctl pow solve` serves curl.
  `POW_DIFFICULTY=0` turns it off for load tests and is refused in production.
- **The web app** (task 4.5, `web/`, `docs/web.md`): a Next.js 16.3 static
  export behind the edge, with the events list, sign-in, the event page with
  the proof of work, the waiting room, holds and booking, and the booking's
  status; accessibility basics; a Playwright purchase test with axe; CI job
  `web` (now required). booking-svc serves the catalog: `GET /v1/events` and
  `GET /v1/events/{id}`, cached for a minute at the edge.
- **Mission control** (task 4.6): the Grafana dashboard
  HoldFast / Mission control (the queue, the sale, correctness, health),
  generated by `deploy/grafana/mission-control.py`; booking-svc's
  `holdfast_capture_to_confirm_seconds`; Valkey and PostgreSQL exporters.
- **README v1** (task 4.7), with a GIF of the dashboard during a k6
  stampede while a ticket is bought by hand.
- **ADR 0012** (task 4.8): short-lived access tokens and refresh tokens that
  rotate with reuse detection.

### Fixed

- queue-svc holds all of a replica's admission-leader locks on one
  PostgreSQL connection (`LockSession`) instead of one connection per event
  (P39). A term that ends releases its lock; losing the session hands every
  event to the standbys, as a crash does, and fencing is unchanged. ADR 0007
  is amended.
- booking-svc's counters are created at zero, so `rate()` and `increase()`
  see their first event (P42).
- Experiment E2 gives its event as many units as users, so the admission
  units cap cannot stall it (P43).
- A race in the outbox relay's leadership test (P41).

### Known issues

- No reconciler or invariant auditor yet (Phase 5, task 5.1): the
  dashboard's I1 to I5 tiles show "auditor: Phase 5", and I3 is fully
  guaranteed only once the reconciler exists.
- Signing out ends the refresh family; an access token already issued lives
  out its 15 minutes.
- The proof of work's 1 to 2 s on a phone is an estimate; only desktop
  browsers were measured.
- The oversubscription factor (1.3) and the admission rate are fixed; AIMD
  is Phase 5 (task 5.4).
- The demo video v1 is not recorded yet.
- Carried over: Compose-built images report `version: dev` on `/buildz`
  (P8); admin routers have no body-size limit (P12); E2 at the design load
  waits for task 6.1.

## [0.3.0] - 2026-10-03

The MVP backend (milestone M3, build plan Phase 3): a buyer can book a held
seat, pay, and end with a confirmed ticket or a refund. booking-svc,
payment-svc and mockpsp (a fault-injecting payment provider) join the
system, with Kafka, the transactional outbox, gRPC between services,
OpenTelemetry tracing (one trace per purchase) and the booking saga, and
the decision records behind them (ADRs 0008 to 0011).

### Added

- **Kafka** (task 3.1): Kafka 4.3.1 in KRaft mode and Redpanda Console
  (http://localhost:8089) in Compose; `holdfastctl kafka topics` and
  `make topics` create `holdfast.booking.v1`, `holdfast.payment.v1`,
  `holdfast.inventory.v1` and their dead-letter topics (auto-creation is off),
  and `make up` runs it. `internal/platform/kafka`: an idempotent producer
  whose events carry CloudEvents headers, and a consumer-group helper with
  manual commits, retries with backoff and dead-lettering. Metric
  `holdfast_kafka_consumed_total{topic,result}`.
- **Tracing** (task 3.2): `internal/platform/otel` with W3C trace-context
  propagation and OTLP export; server spans per request, named after the
  route; `traceparent` through Kafka, with consumers continuing the
  producer's trace; Valkey spans inside traces; `trace_id` and `span_id` on
  log lines. inventory-svc and queue-svc are traced. Compose adds the
  OpenTelemetry Collector and Jaeger (http://localhost:16686).
- **Contracts** (task 3.3): a Buf workspace with
  `holdfast.inventory.v1.InventoryService` (`GetHold`, `MarkPaying`,
  `Confirm`, `ReleaseForFailedPayment`) and the booking and payment event
  payloads in `holdfast.events.v1`; generated Go in `internal/gen`
  (`make gen`); CI runs `buf lint`, `buf format`, `buf breaking` against
  `main` and a generated-code check.
- **inventory over gRPC** (task 3.4): inventory-svc serves
  `holdfast.inventory.v1.InventoryService` on `GRPC_ADDR` (`:7070`), for
  booking-svc only. Service-to-service authentication with per-service
  Ed25519 tokens (`authn.ServiceTokenSource`, `authn.ServiceVerifier`);
  `internal/platform/grpcx` with tracing, metrics, required deadlines,
  per-method allowlists, and clients with default deadlines and bounded
  retries; a typed `inventory.Client`. `make keys` creates booking-svc's dev
  key. Runbook RB-INV-7.
- **Booking schema** (task 3.5): migration `booking/00002` with `bookings`
  (its state machine enforced by a trigger), `idempotency_keys`, the
  transactional `outbox` and `processed_messages`; typed queries generated by
  sqlc (ADR 0011) in `internal/booking/bookingdb`; `make gen` runs sqlc and CI
  checks the generated code.
- **booking-svc** (task 3.6, `cmd/booking`): `POST /v1/bookings` turns a hold
  into a booking waiting for payment, idempotently (stored answers replayed
  byte for byte, retries resume a partial attempt, one booking per hold);
  `GET /v1/bookings/{id}` is owner-only; a deadline job cancels overdue
  bookings. Each change and its event are written in one transaction. The edge
  routes `/v1/bookings`; service doc `docs/services/booking.md`.
- **Payment schema** (task 3.8): migration `payment/00001` with
  `payment_intents` (one per booking; its state machine enforced by a
  trigger), `psp_orders`, `webhook_events` (deduplicated by the PSP's event
  ID), a double-entry `ledger_entries` table whose transactions must balance
  at commit, and the outbox and processed messages; sqlc queries in
  `internal/payment/paymentdb`.
- **Outbox relay** (task 3.7, `internal/platform/outbox`): publishes a
  service's outbox to Kafka in commit order, one leader per schema, at least
  once, continuing each event's stored trace; booking-svc runs it. Metrics
  `holdfast_outbox_lag_seconds` and `holdfast_outbox_pending`; the Kafka
  consumer helper reports `holdfast_kafka_consumer_lag`.
- **payment-svc** (task 3.9, `cmd/payment`):
  - `holdfast.payment.v1.PaymentService.CreateIntent` over gRPC, for
    booking-svc only: one intent per booking, the provider's order keyed by
    the intent ID.
  - A provider client (`internal/payment/psp`) with retries, backoff and
    jitter, behind a circuit breaker (`internal/platform/breaker`).
  - `POST /v1/webhooks/psp`: HMAC-SHA256 checked in constant time,
    deduplicated by event ID, forward-only moves, each with its ledger
    entries and event in one transaction.
  - Status polling of quiet intents, and the payment outbox relay.
  - Migration `payment/00002` adds the intent's event ID, for the ledger's
    revenue account.
  - booking-svc returns `intentId` and `checkoutUrl` when
    `PAYMENT_GRPC_ADDR` is set. Compose runs payment-svc (ports 8084, 9094,
    7072); service doc `docs/services/payment.md`.
- **mockpsp** (task 3.10, `cmd/mockpsp`): the stand-in payment provider.
  - The provider API on the `internal/payment/psp` contract: idempotent
    orders, full refunds, and `GET /v1/settlements` (with a client method
    for the Phase 5 reconciler).
  - A hosted checkout page (http://localhost:8085/checkout/...), which also
    answers JSON for load tests.
  - HMAC-signed webhooks, retried with backoff under the same event ID.
  - On the admin port, behind `ADMIN_TOKEN`, a fault-injection API:
    duplicated, delayed and lost webhooks, failed payments, held-back
    answers and outages, drawn from a seeded source.
  - Compose runs it, and booking-svc now calls payment-svc, so a booking
    returns a working `checkoutUrl`. Service doc `docs/services/mockpsp.md`.
- **The booking saga** (task 3.11):
  - booking-svc consumes `holdfast.payment.v1` (group `booking-saga`). In one
    transaction per message: a capture runs the final guard and confirms
    the booking, or marks it `REFUND_REQUIRED`; a failed or expired payment
    cancels it; a completed refund closes it as `REFUNDED`.
  - Late captures are honoured when the guard allows and counted in
    `holdfast_late_confirm_total`.
  - After each commit the hold is made SOLD or released over gRPC,
    idempotently, on every delivery.
  - payment-svc consumes `booking.refund_required.v1` (group
    `payment-refunds`) and refunds through the provider.
  - New event `booking.refunded.v1`.
- **E1 part B** (task 3.12): `cmd/contention -mode purchase`, part of
  `make e1` and CI.
  - Whole purchases for 1,000 units through the real services: holds,
    bookings, payments at an in-process mockpsp failing 10% of them, and the
    saga, with every payment event delivered twice.
  - It checks that every booking ends confirmed or cancelled and every hold
    SOLD or RELEASED, and that PostgreSQL, Valkey and the ledger agree on
    the sold count.
  - It runs in its own database (`postgres.SiblingDatabase`, `<name>_e1`),
    which testenv now shares.
- **Phase 3 tests** (task 3.13):
  - the saga's decision table (every payment event against every booking
    status);
  - the saga behind the real Kafka consumer, with duplicates on the wire
    and an inventory failure retried;
  - an outbox relay that crashed after publishing republishes the same
    event IDs;
  - late and out-of-order webhooks, and delayed duplicates through mockpsp.
- **Operating the saga** (task 3.14):
  - `holdfastctl dlq replay --topic T` republishes a dead-letter topic's
    messages to their original topic, with their `ce_id` (`--dry-run`,
    `--max`; progress kept in a consumer group).
  - `holdfastctl refund --booking ID` asks payment-svc again to refund a
    booking stuck in `REFUND_REQUIRED` (new refund reason `OPERATOR`).
  - Runbooks RB-3 (drain a dead-letter topic) and RB-4 (manual refund) in
    `docs/runbooks/saga.md`.
  - ADRs 0008 (gRPC for calls, Kafka for events), 0009 (transactional
    outbox with a polling relay) and 0010 (an orchestrated saga).
- **Phase 3 exit:**
  - A purchase is one trace. payment-svc stores the booking request's
    trace context with the intent (migration `payment/00004`) and continues
    it when a webhook or a poll settles the intent, linked to the
    webhook's own trace.
  - E1's purchase mode takes `-psp-faults` (mockpsp's fault mix) and
    `-buyers-per-user`, polls for lost webhooks while it waits, and checks
    the per-user cap (I4). 1,000 purchases under the E3 mix:
    `loadtest/results/`.

### Fixed

- booking-svc's payment deadline is now the hold's protection minus
  `PAYMENT_GRACE` (3 minutes; design doc 6.1). It used to equal the end of
  the protection, leaving no grace for a late capture (P32).
- `make itest` no longer fails while `make up` is running (P7): integration
  tests use Valkey logical database 1 (`testenv.ValkeyDB`), so the running
  services, on database 0, never sweep or admit the tests' keys. holdfastctl
  reads `VALKEY_DB` like the services. Stopping inventory-svc first is no
  longer needed.
- The Kafka integration tests wait for a new topic's partition leaders
  before publishing, instead of failing now and then with
  `UNKNOWN_TOPIC_OR_PARTITION` (P33).
- Integration tests use their own PostgreSQL database, `<database>_test`
  (`holdfast_test` locally), created on first use (P35). The `make up`
  stack's pollers and outbox relays no longer see the tests' rows, and its
  topics no longer carry the tests' events (P30).
- payment-svc's status poller takes the least recently polled intents first
  (migration `payment/00003` adds `polled_at`), so intents whose polls keep
  failing no longer starve the rest (P34).
- Integration tests that claim from a shared queue (overdue bookings, open
  intents) take a cross-process lock (`testenv.Exclusive`), so parallel test
  packages no longer claim each other's rows (P36).
- The rate limiter's refill test allows 500 ms per token instead of 50 ms,
  so a loaded machine no longer fails it (P37).
- A purchase is one trace: the provider's webhook used to start a separate
  trace for the capture, the saga and inventory's confirmation (P38; see
  "Phase 3 exit" above).

### Known issues

- queue-svc holds one PostgreSQL connection per provisioned event for its
  admission leader's lock (P39). Connections grow with the number of events;
  locally, `make e1` fails its "no infrastructure errors" check when run
  beside the full stack (every invariant passes). Being fixed next.
- The queue still admits buyers after the last unit is held, and never marks
  itself `SOLD_OUT` (P17); they get 409 `SOLD_OUT` from inventory-svc and
  nothing oversells. Planned for Phase 4 (task 4.3).
- No reconciler yet (Phase 5): a webhook lost and missed by polling stays
  unresolved until then, so I3 is fully guaranteed only from Phase 5.
- Refunds of confirmed sales are not supported; `holdfastctl refund` handles
  bookings stuck in `REFUND_REQUIRED` only.
- Buyers are still identified by the `X-Dev-User-Id` development header
  (real identity in Phase 4).
- Carried over: Compose-built images report `version: dev` on `/buildz`
  (P8); admin routers have no body-size limit (P12); E2 at the design load
  waits for task 6.1.

## [0.2.0] - 2026-10-02

The fair waiting room (milestone M2, build plan Phase 2): queue-svc, the NGINX
edge, admission tokens trusted by inventory-svc through a JWKS, experiments E2
and E6, and the decision records behind them (ADRs 0005 to 0007).

### Added

- **queue-svc** (`cmd/queue`), the waiting-room service (Phase 2). Task 2.1:
  `PUT /internal/v1/events/{id}/queue` on the admin port stores an event's
  opening time, admission rate, maximum sessions and session TTL, and opens
  its queue in state PRE. Idempotent; different settings return 409
  `PROVISION_CONFLICT`. Service doc `docs/services/queue.md`, runbook
  `docs/runbooks/queue.md`.
- `holdfastctl event create` provisions the queue as well as inventory
  (`--admission-rate`, `--max-sessions`, `--session-ttl`).
- **Joining the queue** (task 2.2): `POST /v1/queue/{id}/join`. Before T0 a
  joiner gets a random lottery position from `crypto/rand`; from T0 on, a
  place in arrival order behind every lottery joiner. Idempotent: a rejoin
  keeps the original position. Per-IP (IPv6 per /64) and per-user token
  buckets return 429 `RATE_LIMITED` with `Retry-After`. Buyers are
  identified by `X-Dev-User-Id` when `DEV_IDENTITY=true`, which production
  refuses. Metric `holdfast_queue_joins_total{result}`.
- `internal/platform/ratelimit`: a token bucket in Valkey, shared by every
  replica.
- **The T0 transition** (task 2.3): the queue switches from lottery to arrival
  order at the opening time, judged by Valkey's clock. The first join after T0
  flips the state itself; an idempotent opener in every replica flips it when
  nobody joins (`OPEN_CHECK_INTERVAL`, default 250ms). Metrics
  `holdfast_queue_opened_total{by}`, `holdfast_queue_opener_runs_total{result}`
  and `holdfast_queue_opener_duration_seconds`.
- **Positions** (task 2.4): `GET /v1/queue/{id}/me` returns `randomizingAt`
  before T0 and the 1-based rank from T0 on, judged by Valkey's clock. Per-user
  rate limit (`POSITION_USER_*`), 404 `NOT_IN_QUEUE`, `private, no-store`.
  Metric `holdfast_queue_position_lookups_total{result}`.
- **Admission controller** (task 2.5): one leader per event, elected with a
  PostgreSQL advisory lock and fenced by `adm:{E}:epoch`, admits people every
  250 ms at the event's rate while capping concurrent sessions at
  `maxSessions` (Little's Law). Only `OPEN` admits; `FROZEN` pauses. queue-svc
  now requires `POSTGRES_DSN` (elections only). Metrics for admissions, ticks,
  terms and the current leader.
- **Status document** (task 2.6): `GET /v1/events/{id}/status` returns the
  state, opening time, `admittedUpTo`, queue size and update time, rewritten by
  the admission leader every tick and served with `public, max-age=1`; a
  fallback with `updatedAt: null` until a leader has written it.
- **Admission tokens** (task 2.7): `POST /v1/queue/{id}/admit` issues an
  Ed25519 token once your rank is within `admittedUpTo`, expiring at the earlier
  of 10 minutes and your session slot; `GET /.well-known/jwks.json` publishes
  the public keys. `authn.IssueUntil` and `authn.NewJWKSet` added. queue-svc
  requires `ADMISSION_PRIVATE_KEY_FILE`. Inventory accepts these tokens.
- **inventory-svc follows the queue's JWKS** (task 2.8): `ADMISSION_JWKS_URL`,
  refreshed every 5 minutes and on an unknown key ID at most every 30 seconds,
  with readiness failing while no key is known; key files still work and are
  trusted too. `authn.JWKSClient`, `authn.ParseJWK` and `authn.NewVerifierWith`
  added. Decided that admission tokens stay reusable within their
  session-capped life rather than single use, so retries keep working.
- **The NGINX edge** (task 2.9, `deploy/nginx/nginx.conf`, host port 8088): routes
  queue and inventory paths, micro-caches the status and availability
  documents for one second with request collapsing, rate-limits the queue per
  client, and passes the client's address. queue-svc believes
  `X-Forwarded-For` only from `TRUSTED_PROXIES`. The README quickstart now
  walks the buyer journey through the edge.
- **Queue metrics and dashboard** (task 2.10): per-event gauges
  `holdfast_queue_size`, `_admitted_up_to`, `_active_sessions`,
  `_max_sessions` and `_leader_epoch` from the admission leader, and
  `holdfast_queue_status_age_seconds` from every replica;
  `holdfast_queue_admitted_total` is labelled by event. Grafana dashboard
  HoldFast / Queue.
- **Queue tests** (task 2.11): a model test for fairness (F1) that checks
  random interleavings of joins, ticks, claims, expiring sessions, T0 and
  freezes against a reference model; the leader's tick loop tested under
  `testing/synctest`; `internal/stats` with Spearman's rank correlation for
  experiment E6.
- **`holdfastctl queue provision` and `queue status`** (task 2.13): provision
  an existing event's waiting room (opening time from PostgreSQL), and see its
  state, size, admission, sessions, leader and status-document age at a
  glance (`--json` too). Runbook RB-Q-8: putting a queue back after Valkey
  lost it.
- ADRs 0005 (lottery before T0, FIFO after), 0006 (cached status polling
  instead of WebSockets or SSE) and 0007 (leader election with PostgreSQL
  advisory locks plus fencing epochs); the buyer's journey in
  `docs/architecture.md` (task 2.14).
- **Experiments E6 and E2** (task 2.12): `make fairness-e6` (`cmd/fairness`)
  checks that join time does not predict a lottery position and that
  positions after T0 are the arrival order; `make load-e2` (`loadtest/e2`)
  runs a k6 stampede through the edge and compares edge and origin status
  traffic. Results in `loadtest/results/`. `queue.Store.Purge` removes an
  event's queue keys.

### Fixed

- `scripts/check-migrations.sh` treated any changed file under
  `db/migrations` as an edited migration, so registering a new schema in
  `embed.go` failed the check; it now compares the `.sql` files only.
- `make up` restarts the NGINX edge too: like Prometheus and Grafana it reads
  its mounted config only at start-up, so a new route stayed invisible.
- `holdfast_queue_status_age_seconds` is measured by Valkey's clock, so clock
  skew between queue-svc and Valkey no longer shows up as staleness; its test
  no longer fails when the wall clock steps.
- Claiming a turn whose session slot had expired, before the leader's next
  tick removed it, returned 500 instead of 409 `TURN_EXPIRED`; `admit.lua`
  now judges the slot by Valkey's clock.
- An admission controller whose event was removed never stopped: it retried
  every 2 seconds, opening a PostgreSQL connection and recreating the event's
  epoch key each time. It now stops, and never recreates keys.
- The inventory dashboard's HTTP panels counted queue-svc's requests too;
  they now filter on `job="inventory"`.
- `make up` restarts Prometheus and Grafana, which read their config and
  dashboards only at start-up; a new dashboard or scrape target stayed
  invisible on a running stack.

- The design had the admission leader flip the queue to OPEN at T0. On a
  250 ms tick, or during a leader failover, joins arriving after T0 but before
  the flip would still have received lottery positions. T0 is now judged by
  Valkey's clock inside every join.
- The design doc's `join.lua` gave a lottery position to joins while the sale
  was `FROZEN`, which would have let them jump ahead of everyone who joined
  after T0. The built script, and the doc, give the lottery to `PRE` only.

### Changed

- `ADMISSION_PUBLIC_KEY_FILES` is no longer required by inventory-svc when
  `ADMISSION_JWKS_URL` is set; Compose uses the JWKS.
- The edge's per-IP zone lives in `deploy/nginx/edge-limits.conf`, included by
  `nginx.conf`, so a load test can replace it without forking the config.
- `prometheus/client_golang` upgraded from 1.23.2 to 1.24.1, proposed by
  Dependabot (#1).

### Known issues

- Experiment E2 was accepted at 30-second polling (50,000 users from join to
  admission token, origin flat). The design's 3-second polling load did not fit
  on the development laptop with k6 alongside, and join p99 was 203 ms against
  the 150 ms SLO there; the design-scale run moves to Phase 6 (task 6.1), on
  separate machines. See `loadtest/results/README.md`.
- The queue does not yet limit admissions to the units left (oversubscription)
  or mark itself `SOLD_OUT`; buyers admitted after the last unit get 409
  `SOLD_OUT` from inventory-svc, and nothing oversells. Recommended for
  Phase 3.
- `make itest` is flaky while the `make up` stack is running, because
  inventory-svc's sweeper also releases the tests' holds. Run
  `docker compose stop inventory` first. CI is not affected.
- Compose-built images report `version: dev` and `commit: unknown` on `/buildz`.
- `holdfast-explained-simply.mdx` still uses the old invariant numbering.

## [0.1.0] - 2026-10-01

Drop 1: the foundation and the inventory correctness core (milestones M0 and
M1), brought up and proven on real infrastructure (milestone M1.5).

### Added

- **inventory-svc** (`cmd/inventory`): holds seats on the hot path. Every state
  change is one atomic Lua script in Valkey (`provision`, `hold`, `mark_paying`,
  `release`, `confirm`), with idempotent retries through `Idempotency-Key`,
  per-user caps, an expiry sweeper that is safe in every replica, checkout
  protection, and late payment confirmations that re-take units.
- **PostgreSQL final guard** (`internal/booking/guard`): conditional writes and
  a `no_oversell` CHECK constraint make overselling and per-user cap violations
  database errors, whatever the fast path believes. Event catalog and migration
  `booking/00001`.
- **holdfastctl**: migrations, dev keys and admission tokens, event creation,
  inventory provisioning, rebuild and status.
- **Experiment E1** (`cmd/contention`): 50,000 concurrent buyers for 1,000
  units, plus 10,000 concurrent confirmations through the guard. Runs in CI on
  every push.
- **Platform foundation** (`internal/platform`): configuration, structured
  logging, HTTP toolkit with RFC 9457 errors, health checks with draining,
  Prometheus metrics, Ed25519 admission tokens, PostgreSQL pool with an
  embedded migration runner, Valkey client, component lifecycle and build info.
- Local stack (`compose.yaml`): PostgreSQL 18, Valkey 9.1, inventory-svc,
  Prometheus and Grafana with an inventory dashboard.
- CI: lint, unit tests and integration tests with the race detector, E1,
  govulncheck, gitleaks, and image builds.
- Docs: architecture, inventory service reference, runbooks RB-INV-1 to
  RB-INV-6, ADRs 0001 to 0004, design and build plan v2.0, a plain-language
  explainer, and a progress log with every issue met during bring-up.
- **E1 soak results** (`loadtest/results/`): 100 of 100 runs passed, with the
  hardware note, method, latency spread and caveats, plus the soak script
  `loadtest/e1-soak.sh`.
- MIT license, Dependabot, and branch protection on `main`.

### Changed

- Invariant IDs now match the design plan: I1 no oversell, I2 no double charge,
  I3 money safety, I4 per-user cap, I5 no lost units, F1 fairness.

### Fixed

- `go.sum` was missing, which failed every CI job.
- `scripts/check-migrations.sh` was not executable in git.
- `.gitattributes` keeps LF line endings on Windows checkouts.

### Security

- pgx upgraded to v5.11.0 (GO-2026-5004: SQL injection via placeholder
  confusion with dollar-quoted strings; also GO-2026-4771 and GO-2026-4772),
  x/text to v0.42.0 (GO-2026-5970) and x/sys to v0.48.0 (GO-2026-5024).

### Known issues

- `make itest` is flaky while the `make up` stack is running, because
  inventory-svc's sweeper also releases the tests' holds. Run
  `docker compose stop inventory` first. CI is not affected.
- Compose-built images report `version: dev` and `commit: unknown` on `/buildz`.
- `holdfast-explained-simply.mdx` still uses the old invariant numbering.

[Unreleased]: https://github.com/Sanjay-Mx21/holdfast/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/Sanjay-Mx21/holdfast/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Sanjay-Mx21/holdfast/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Sanjay-Mx21/holdfast/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.1.0
