# Architecture context pack

A compact map of the system as built. The design doc
(`docs/design/holdfast-design-and-build-plan.mdx`) is the long-form rationale;
where Drop 1 deviates from it, ADRs 0003 and 0004 record why.

## 1. Summary

HoldFast sells scarce inventory during extreme demand spikes. Drop 1 contains
the hot path (inventory-svc: atomic holds in Valkey), the durable backstop (the
PostgreSQL final guard), the operator CLI and a contention experiment that
proves the no-oversell property on every CI run.

## 2. Invariants

IDs match section 2.3 of the design doc.

| ID | Invariant | Enforced by (Drop 1) |
|---|---|---|
| I1 | No oversell: confirmed units never exceed capacity | `hold.lua` (atomic check and decrement); the guard's conditional `UPDATE` plus the `no_oversell` CHECK |
| I2 | No double charge: at most one captured payment per booking | Payment intents and webhook dedup, Phase 3 |
| I3 | Money safety: every captured payment ends CONFIRMED or REFUNDED | Booking and payment saga, Phase 3; reconciler, Phase 5 |
| I4 | Per-user cap: held plus sold units per user per event never exceed the limit | Per-user counter in `hold.lua`; conditional upsert in the guard |
| I5 | No lost units: every hold ends SOLD or RELEASED | Expiry index plus sweeper; `release.lua` and `confirm.lua` |
| F1 | Fairness: random order before T0, FIFO after, one slot per identity | Waiting room, Phase 2 |

## 3. Services and ownership

| Service | Status | Owns | Talks to |
|---|---|---|---|
| inventory-svc (`cmd/inventory`) | Built | Valkey keys `inv:*` | Valkey |
| queue-svc (`cmd/queue`) | Phase 2 in progress: provisioning, joining, the T0 transition, positions, admission, the status document and admission tokens built | Valkey keys `q:*`, `adm:*`, `rl:*` | Valkey; PostgreSQL for leader election only |
| booking final guard (`internal/booking`) | Built as a library | `booking` schema | PostgreSQL |
| holdfastctl (`cmd/holdfastctl`) | Built | Nothing (operator tool) | PostgreSQL, Valkey |
| booking, payment, auth | Planned | See design doc | |

## 4. Runtime topology

Each service listens on two ports:

- **public** (`HTTP_ADDR`, default `:8080`): the buyer-facing API behind the edge;
- **admin** (`ADMIN_ADDR`, default `:9090`): `/metrics`, `/livez`, `/readyz`,
  `/buildz`, `/debug/pprof/*` and operator APIs. Internal network only.

Locally, `compose.yaml` runs PostgreSQL 18, Valkey 9.1, a one-shot migration
job, inventory-svc, queue-svc, Prometheus and Grafana.

## 5. Request lifecycle: `POST /v1/events/{eventID}/holds`

1. Global middleware, outermost first: request ID, access log, metrics, panic
   recovery, security headers, body limit, request timeout.
2. `RequireAdmission` verifies the Ed25519 admission token: issuer, audience,
   expiry, trusted key ID.
3. The handler checks the token's event matches the path, requires
   `Idempotency-Key` and strictly decodes `{"quantity": n}`.
4. The service canonicalises the IDs, validates the key and quantity, and
   derives the hold ID as UUIDv5(user, idempotency key).
5. The store runs `hold.lua`: provisioned? quantity valid? replay? per-user cap?
   units left? Then it decrements, records the hold and indexes its expiry,
   all in one atomic step.
6. The reply code maps to 201 (held or replayed) or to a problem document
   (409 SOLD_OUT, 422 USER_LIMIT, and so on).

## 6. Data

**Valkey** (hot, rebuildable). Every key of an event shares the hash tag
`{eventID}`; see `docs/services/inventory.md` (`inv:*`) and
`docs/services/queue.md` (`q:*`) for the keyspace.

**PostgreSQL** (truth), schema `booking`:

- `events`: catalog, with application-generated UUIDv7 IDs;
- `event_inventory`: capacity and sold, with `CHECK (sold <= capacity)`;
- `user_event_purchases`: per-user units, the I4 guard.

Migrations are embedded SQL applied by `holdfastctl migrate` (ADR 0003).

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
one per event leads, holding a PostgreSQL advisory lock on a dedicated
connection; every 250 ms it runs `advance.lua`, which refuses a stale epoch
(fencing) and caps concurrent sessions (Little's Law).

## 8. Security

- queue-svc issues admission tokens when a buyer's turn comes
  (`POST /v1/queue/{id}/admit`), capped at their session slot, and publishes
  its public keys at `/.well-known/jwks.json`.
- Buyers present admission tokens: EdDSA JWTs bound to one user and one
  event, with audience `holdfast-inventory` and issuer `holdfast-queue`. Only
  the EdDSA algorithm is accepted, and `kid` selects among trusted public keys
  so rotation needs no downtime.
- Until auth-svc exists, queue-svc identifies buyers by the `X-Dev-User-Id`
  header when `DEV_IDENTITY=true`. Anyone can claim any ID with it, so the
  service refuses to start with it in production.
- Joining passes per-IP (IPv6 per /64) and per-user token buckets in Valkey
  (`internal/platform/ratelimit`), shared by every replica.
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
  raw paths) and `holdfast_build_info`.
- **Health:** `/livez` never checks dependencies. `/readyz` checks Valkey and
  returns 503 from the moment SIGTERM arrives.
- Tracing arrives with the first cross-service call (ADR 0004).

## 10. Failure modes

| Failure | Behaviour | Runbook |
|---|---|---|
| Valkey unreachable | 503 `UNAVAILABLE` with `Retry-After`; readiness fails | RB-INV-1 |
| Valkey data lost | Rebuild the pool from PostgreSQL (capacity minus sold) | RB-INV-4 |
| Sweeper failing | Expired holds keep units; any healthy replica recovers them | RB-INV-2 |
| Payment confirmed after hold expiry | `confirm.lua` re-takes the units ("late") instead of dropping the sale | RB-INV-6 |
| Pod killed | Readiness drains first; in-flight requests finish within `SHUTDOWN_TIMEOUT` | n/a |

## 11. Testing strategy

- Unit tests (`make test`) cover validation, error mapping, middleware, auth
  and config.
- Integration tests (`make itest`) run real Lua scripts and SQL: lifecycle
  scenarios, 2,000-goroutine contention, a randomized model check (1,500
  steps against an executable specification), guard concurrency and
  migration-runner guarantees.
- E1 (`make e1`, and every CI run): 50,000 buyers for 1,000 units, plus
  10,000 concurrent confirmations through the guard.

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
