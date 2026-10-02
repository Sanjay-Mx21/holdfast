# HoldFast

A surge-proof booking engine. When 500,000 people chase 1,000 seats, HoldFast
sells exactly 1,000 (never 1,001) and stays up while doing it.

> **Status: [v0.2.0](https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.2.0) released: the fair waiting room (build plan Phase 2) in front of the inventory core of [v0.1.0](https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.1.0). Next: Phase 3, the booking saga and payments.**
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
- **holdfastctl** runs migrations, generates dev keys and tokens, creates
  events, provisions their inventory and queue, rebuilds inventory, and shows
  an event's availability and waiting room (`queue status`).
- **Experiment E1** (`cmd/contention`) fires 50,000 concurrent buyers at 1,000
  units and fails unless exactly 1,000 holds are granted. CI runs it on every push.

## Quickstart

Requirements: Go 1.27+, Docker with Compose v2, make.

```bash
make up                                          # dev keys, PostgreSQL, Valkey, migrations, inventory-svc, queue-svc, the NGINX edge, Prometheus, Grafana
make event NAME="Coldplay Mumbai" CAPACITY=1000  # the event, its inventory and its waiting room (opens now); prints the event ID
export EVENT=<event id>
export ME=$(cat /proc/sys/kernel/random/uuid)    # your buyer ID (development identity until Phase 4)
EDGE=localhost:8088                              # the buyers' front door

curl -s -X POST $EDGE/v1/queue/$EVENT/join -H "X-Dev-User-Id: $ME"   # join the waiting room
curl -s $EDGE/v1/events/$EVENT/status                                # the shared status document (cached 1 s)
curl -s $EDGE/v1/queue/$EVENT/me -H "X-Dev-User-Id: $ME"             # your rank
export TOKEN=$(curl -s -X POST $EDGE/v1/queue/$EVENT/admit -H "X-Dev-User-Id: $ME" | jq -r .token)

curl -s $EDGE/v1/events/$EVENT/availability
curl -s -X POST $EDGE/v1/events/$EVENT/holds \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: order-$(date +%s)" \
  -H 'Content-Type: application/json' -d '{"quantity":2}'

make e1                                          # contention experiment against the local stack
make fairness-e6                                 # fairness of the queue order (E6)
make load-e2                                     # k6 stampede on the waiting room through the edge (E2)
```

`make -s token EVENT=$EVENT` still mints a token directly, bypassing the waiting
room, for development and load tests.

Dashboards: Grafana at http://localhost:3000 (HoldFast / Inventory and
HoldFast / Queue) and Prometheus at http://localhost:9090. The service's own
metrics and health checks are on its admin port: http://localhost:9091/metrics
and `/readyz` (queue-svc: http://localhost:9092). Prometheus and Grafana read
their config and dashboards only at start-up; `make up` restarts both, so a
changed scrape config or a new dashboard loads.

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

Next drops, following the design doc: `booking`, `payment`
and `mockpsp` with gRPC, the outbox and tracing (Phase 3); `auth` and the web
client (Phase 4); then chaos drills, load tests and benchmarks (Phases 5-7).

## Testing

| Command | Runs | Needs |
|---|---|---|
| `make test` | Unit tests with the race detector | Nothing |
| `make itest` | Integration tests against real PostgreSQL and Valkey: script behaviour, concurrency, a randomized model check, the guard, migrations | Docker |
| `make e1` | Contention experiment; exits non-zero on any invariant violation | The local stack |
| `make fairness-e6` | Fairness experiment: lottery before T0, arrival order after it; exits non-zero if the order is not fair | The local stack |
| `make load-e2` | Waiting-room stampede through the edge with k6; results in `loadtest/results/` | The local stack (`make up`) |
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
| `GET /v1/events/{eventID}/status` | Public (queue-svc) | The shared status document (cacheable for 1 s) |
| `POST /v1/queue/{eventID}/admit` | `X-Dev-User-Id` until Phase 4 (queue-svc) | Exchange your turn for an admission token |
| `GET /.well-known/jwks.json` | Public (queue-svc) | Public keys of admission tokens |
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
