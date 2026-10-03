# HoldFast

A surge-proof booking engine. When 500,000 people chase 1,000 seats, HoldFast
sells exactly 1,000 (never 1,001) and stays up while doing it.

> **Status: [v0.3.0](https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.3.0) released: the MVP backend (build plan Phase 3), bookings, payments and the saga, behind the fair waiting room of [v0.2.0](https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.2.0) and the inventory core of [v0.1.0](https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.1.0). Next: Phase 4, identity, sale policies and the web app.**
> The full design lives in [`docs/design/holdfast-design-and-build-plan.mdx`](docs/design/holdfast-design-and-build-plan.mdx).

## What works today

- **inventory-svc** holds seats on the hot path. Every state change is one
  atomic Lua script in Valkey: no locks, no check-then-act races. It supports
  idempotent retries, per-user caps, hold expiry with a sweeper, checkout
  protection and late payment confirmations.
- **The PostgreSQL final guard** makes overselling a constraint violation,
  whatever the fast path believes.
- **queue-svc** (Phase 2) stores each event's waiting-room
  settings and lets buyers join: a random lottery position before the sale
  opens, arrival order after, behind per-IP and per-user rate limits. The
  switch at the opening time follows Valkey's clock, buyers can look up their
  rank, a leader-elected controller admits them at the event's rate without
  exceeding its concurrent-session budget, one cacheable status document
  tells everyone how far admission has got, and an admitted buyer exchanges
  their turn for the signed admission token inventory-svc requires;
  inventory-svc follows the queue's published keys, so rotating them needs no
  restart.
- **booking-svc** (Phase 3) turns a hold into a booking that waits for
  payment. Its API is idempotent Stripe-style: a retried request gets the
  same answer, and a request that failed part-way resumes. It protects the
  hold over gRPC, writes each state change and its event in one transaction
  (an outbox), and cancels bookings whose payment deadline passes. Its
  **saga** turns payment events into sales: the PostgreSQL final guard
  confirms the booking, or a refund is requested; failed and late payments
  are compensated.
- **payment-svc** (Phase 3) creates one payment intent per booking, talks to
  the payment provider through retries and a circuit breaker, applies its
  HMAC-signed webhooks exactly once, keeps a double-entry ledger, polls for
  lost webhooks and refunds what the saga cannot confirm.
- **mockpsp** (Phase 3) stands in for the payment provider: a checkout page,
  signed webhooks, and fault injection (duplicated, delayed and lost
  webhooks, failed payments, slow answers, outages) for tests.
- **auth-svc** (Phase 4) signs people in with a code sent to their phone
  (through a mock SMS gateway). It issues short-lived EdDSA access tokens
  and rotating refresh tokens whose reuse revokes the whole login, and it
  never stores a phone number, code or token in the clear.
- **Sale policies** (Phase 4): an event may open to verified buyers only for
  its first minutes and keep agents out for longer, as IRCTC does for
  Tatkal. Admission follows the units left (about 1.3 sessions per unit), so
  thousands are not let in to fight over the last few, and the queue says
  `SOLD_OUT` once the last unit is sold. A **freeze switch** stops new holds
  and admissions at once while purchases in flight finish (runbook RB-1).
- **holdfastctl** runs migrations, generates dev keys and tokens, creates
  events, provisions their inventory and queue, rebuilds inventory, shows an
  event's availability and waiting room (`queue status`), and freezes or
  unfreezes a sale.
- **Experiment E1** (`cmd/contention`) fires 50,000 concurrent buyers at 1,000
  units and fails unless exactly 1,000 holds are granted. Its part B runs
  whole purchases (holds, bookings, payments with 10% failing, the saga) and
  fails unless every unit is sold once or returned. CI runs it on every push.

## Quickstart

Requirements: Go 1.27+, Docker with Compose v2, make.

```bash
make up                                          # dev keys, PostgreSQL, Valkey, Kafka and its topics, Redpanda Console, migrations, inventory-svc, queue-svc, booking-svc, payment-svc, mockpsp, auth-svc, the NGINX edge, Prometheus, Grafana, OTel Collector, Jaeger
make event NAME="Coldplay Mumbai" CAPACITY=1000  # the event, its inventory and its waiting room (opens now); prints the event ID
export EVENT=<event id>
EDGE=localhost:8088                              # the buyers' front door

# Sign in: a code goes to your phone through the mock SMS gateway, whose
# inbox is readable in development.
curl -s -X POST $EDGE/v1/auth/otp/request -H 'Content-Type: application/json' -d '{"phone":"+919876543210"}'
CODE=$(curl -s "$EDGE/v1/auth/dev/inbox?phone=%2B919876543210" | jq -r .text | grep -o '[0-9]\{6\}')
export ME=$(curl -s -c cookies.txt -X POST $EDGE/v1/auth/otp/verify -H 'Content-Type: application/json' \
  -d "{\"phone\":\"+919876543210\",\"code\":\"$CODE\"}" | jq -r .accessToken)   # 15 minutes; the refresh token is in cookies.txt
# Later: curl -s -b cookies.txt -c cookies.txt -X POST $EDGE/v1/auth/refresh

curl -s -X POST $EDGE/v1/queue/$EVENT/join -H "Authorization: Bearer $ME"   # join the waiting room
curl -s $EDGE/v1/events/$EVENT/status                                       # the shared status document (cached 1 s)
curl -s $EDGE/v1/queue/$EVENT/me -H "Authorization: Bearer $ME"             # your rank
export TOKEN=$(curl -s -X POST $EDGE/v1/queue/$EVENT/admit -H "Authorization: Bearer $ME" | jq -r .token)   # admission token

curl -s $EDGE/v1/events/$EVENT/availability
curl -s -X POST $EDGE/v1/events/$EVENT/holds \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: order-$(date +%s)" \
  -H 'Content-Type: application/json' -d '{"quantity":2}'
export HOLD=<hold id>

curl -s -X POST $EDGE/v1/bookings -H "Authorization: Bearer $ME" \
  -H "Idempotency-Key: booking-$(date +%s)" \
  -H 'Content-Type: application/json' -d "{\"eventId\":\"$EVENT\",\"holdId\":\"$HOLD\"}"   # book it

go run ./cmd/holdfastctl freeze --event $EVENT     # runbook RB-1: no new holds, admissions paused
go run ./cmd/holdfastctl unfreeze --event $EVENT

make e1                                          # contention experiment against the local stack
make fairness-e6                                 # fairness of the queue order (E6)
make load-e2                                     # k6 stampede on the waiting room through the edge (E2)
```

`holdfastctl event create` takes `--verified-only-for 15m` and
`--agent-lockout-for 30m` to open a sale to verified buyers first and keep
agents out (both off by default; `docs/services/queue.md`).

`make -s token EVENT=$EVENT` still mints an admission token directly,
bypassing the waiting room, for development and load tests. Starting the
stack with `DEV_IDENTITY=true make up` lets requests without an access token
name their buyer in `X-Dev-User-Id` instead (the E2 load test does this for
queue-svc); never in production.

Dashboards: Grafana at http://localhost:3000 (HoldFast / Inventory and
HoldFast / Queue) and Prometheus at http://localhost:9090. The service's own
metrics and health checks are on its admin port: http://localhost:9091/metrics
and `/readyz` (queue-svc: http://localhost:9092). Prometheus and Grafana read
their config and dashboards only at start-up; `make up` restarts both, so a
changed scrape config or a new dashboard loads.

Kafka (from Phase 3): the broker is on `localhost:29092`, and Redpanda Console
at http://localhost:8089 shows topics, messages and consumer groups.
`make topics` creates the topics (`make up` does it too).

Traces: Jaeger at http://localhost:16686. Every request through the services
is traced (the span is named after the route), and log lines carry its
`trace_id`, so a log line leads to its trace.

## Repository layout

```
cmd/
  inventory/           inventory-svc: public API on :8080, admin on :9090
  queue/               queue-svc (waiting room): public API on :8080, admin on :9090
  holdfastctl/         operator CLI
  contention/          experiment E1
internal/
  platform/            shared foundation: app lifecycle, authn, config, health,
                       httpx, logging, metrics, postgres, valkey, buildinfo
  inventory/           holds: Lua scripts, store, service, sweeper, HTTP handler
  queue/               waiting room: Lua scripts, store, service, HTTP handler
  booking/catalog/     event catalog in PostgreSQL
  booking/guard/       final guard against oversell and per-user cap violations
  testenv/             wiring for integration tests
db/migrations/         SQL migrations per schema, embedded in the binaries
deploy/                Prometheus and Grafana provisioning
docs/                  architecture, service reference, runbooks, ADRs, design
scripts/               repository checks
```

Next drops, following the design doc: the rest of Phase 4 (proof of work,
the web client, the mission-control dashboard); then chaos drills, the
reconciler, load tests and benchmarks (Phases 5-7).

## Testing

| Command | Runs | Needs |
|---|---|---|
| `make test` | Unit tests with the race detector | Nothing |
| `make itest` | Integration tests against real PostgreSQL, Valkey and Kafka: script behaviour, concurrency, randomized model checks, the guard, migrations, consumers | Docker |
| `make e1` | Contention experiment; exits non-zero on any invariant violation | The local stack |
| `make fairness-e6` | Fairness experiment: lottery before T0, arrival order after it; exits non-zero if the order is not fair | The local stack |
| `make load-e2` | Waiting-room stampede through the edge with k6; results in `loadtest/results/` | The local stack (`make up`) |
| `make lint` | golangci-lint and migration checks | golangci-lint (`make tools`) |
| `make gen` | Generate Go code: the protobuf contracts in `proto/` (buf) and the SQL queries in `internal/booking/queries` (sqlc) | buf and sqlc (`make tools`) |

Integration tests read `HOLDFAST_TEST_VALKEY_ADDR` and
`HOLDFAST_TEST_POSTGRES_DSN` and skip when they are unset, so they can also
target your own instances. The randomized test prints its seed; rerun a
failure with `HOLDFAST_TEST_SEED=<seed>`.

## API at a glance

| Method and path | Auth | Purpose |
|---|---|---|
| `POST /v1/events/{eventID}/holds` | Admission token + `Idempotency-Key` | Hold units |
| `GET /v1/events/{eventID}/holds/{holdID}` | Admission token | Read your hold |
| `DELETE /v1/events/{eventID}/holds/{holdID}` | Admission token | Release your hold |
| `GET /v1/events/{eventID}/availability` | Public | Remaining units (cacheable for 1 s) |
| `PUT /internal/v1/events/{eventID}/inventory` | Operator token, admin port only | Provision inventory |
| `POST /v1/auth/otp/request` | Public (auth-svc) | Send a sign-in code to a phone |
| `POST /v1/auth/otp/verify` | Public (auth-svc) | Sign in with the code: an access token, and a refresh cookie |
| `POST /v1/auth/refresh` | Refresh cookie (auth-svc) | A new access token; the refresh token rotates |
| `POST /v1/auth/logout` | Refresh cookie (auth-svc) | Sign out |
| `POST /v1/queue/{eventID}/join` | Access token (queue-svc) | Join the waiting room |
| `GET /v1/queue/{eventID}/me` | Access token (queue-svc) | Your rank after T0, or when the lottery closes |
| `GET /v1/events/{eventID}/status` | Public (queue-svc) | The shared status document (cacheable for 1 s) |
| `POST /v1/queue/{eventID}/admit` | Access token (queue-svc) | Exchange your turn for an admission token |
| `GET /.well-known/jwks.json` | Public (queue-svc) | Public keys of admission tokens |
| `PUT /internal/v1/events/{eventID}/queue` | Operator token, queue-svc admin port only | Provision the queue, with its policy windows |
| `POST /internal/v1/events/{eventID}/freeze`, `/unfreeze` | Operator token, admin port only (queue-svc and inventory-svc) | The freeze switch: pause admissions, stop new holds |
| `POST /v1/bookings` | Access token + `Idempotency-Key` (booking-svc) | Book a hold: a booking waiting for payment |
| `GET /v1/bookings/{bookingID}` | Access token (booking-svc) | Your booking |

Errors are RFC 9457 problem documents with stable `code` values; see
[`docs/services/inventory.md`](docs/services/inventory.md).

## Conventions

- [`CONTRIBUTING.md`](CONTRIBUTING.md): commit format, review checklist, migration policy.
- [`AGENTS.md`](AGENTS.md): rules for AI coding agents working in this repository.
- [`docs/architecture.md`](docs/architecture.md): system map, invariants, failure modes.
- [`docs/adr/`](docs/adr): architecture decisions.

Dependencies are pinned in `go.mod`. `make deps-upgrade` moves to the latest
releases; run `make test itest e1` afterwards.

## License

[MIT](LICENSE) © 2026 Sanjay M
