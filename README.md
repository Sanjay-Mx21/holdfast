# HoldFast

A surge-proof booking engine. When 500,000 people chase 1,000 seats, HoldFast
sells exactly 1,000 (never 1,001) and stays up while doing it.

> **Status: [v0.1.0](https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.1.0) released: Drop 1, foundation and inventory (build plan phases 0-1, brought up and proven in M1.5). Next: Phase 2, the waiting room.**
> The full design lives in [`docs/design/holdfast-design-and-build-plan.mdx`](docs/design/holdfast-design-and-build-plan.mdx).

## What works today

- **inventory-svc** holds seats on the hot path. Every state change is one
  atomic Lua script in Valkey: no locks, no check-then-act races. It supports
  idempotent retries, per-user caps, hold expiry with a sweeper, checkout
  protection and late payment confirmations.
- **The PostgreSQL final guard** makes overselling a constraint violation,
  whatever the fast path believes.
- **queue-svc** (Phase 2, in progress) stores each event's waiting-room
  settings and lets buyers join: a random lottery position before the sale
  opens, arrival order after, behind per-IP and per-user rate limits. The
  switch at the opening time follows Valkey's clock, and buyers can look up
  their rank. Admission comes next.
- **holdfastctl** runs migrations, generates dev keys and tokens, creates
  events, provisions their inventory and queue, and rebuilds inventory.
- **Experiment E1** (`cmd/contention`) fires 50,000 concurrent buyers at 1,000
  units and fails unless exactly 1,000 holds are granted. CI runs it on every push.

## Quickstart

Requirements: Go 1.27+, Docker with Compose v2, make.

```bash
go mod tidy                                      # resolve modules and write go.sum; commit it
make up                                          # dev keys, then PostgreSQL, Valkey, migrations, inventory, Prometheus, Grafana
make event NAME="Coldplay Mumbai" CAPACITY=1000  # prints the event ID
export EVENT=<event id>
export TOKEN=$(make -s token EVENT=$EVENT)       # dev-only admission token (stands in for the Phase 2 waiting room)

curl -s localhost:8081/v1/events/$EVENT/availability
curl -s -X POST localhost:8081/v1/events/$EVENT/holds \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: order-$(date +%s)" \
  -H 'Content-Type: application/json' -d '{"quantity":2}'

make e1                                          # contention experiment against the local stack
```

Dashboards: Grafana at http://localhost:3000 (HoldFast / Inventory) and
Prometheus at http://localhost:9090. The service's own metrics and health
checks are on its admin port: http://localhost:9091/metrics and `/readyz`
(queue-svc: http://localhost:9092). Prometheus does not reload its config on
its own: after `deploy/prometheus/prometheus.yml` changes, run
`docker compose restart prometheus`.

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

Next drops, following the design doc: the rest of `queue` (Phase 2); `booking`, `payment`
and `mockpsp` with gRPC, the outbox and tracing (Phase 3); `auth` and the web
client (Phase 4); then chaos drills, load tests and benchmarks (Phases 5-7).

## Testing

| Command | Runs | Needs |
|---|---|---|
| `make test` | Unit tests with the race detector | Nothing |
| `make itest` | Integration tests against real PostgreSQL and Valkey: script behaviour, concurrency, a randomized model check, the guard, migrations | Docker |
| `make e1` | Contention experiment; exits non-zero on any invariant violation | The local stack |
| `make lint` | golangci-lint and migration checks | golangci-lint (`make tools`) |

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
| `POST /v1/queue/{eventID}/join` | `X-Dev-User-Id` until Phase 4 (queue-svc) | Join the waiting room |
| `GET /v1/queue/{eventID}/me` | `X-Dev-User-Id` until Phase 4 (queue-svc) | Your rank after T0, or when the lottery closes |
| `PUT /internal/v1/events/{eventID}/queue` | Operator token, queue-svc admin port only | Provision the queue |

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
