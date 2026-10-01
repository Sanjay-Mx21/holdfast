# AGENTS.md

Context for AI coding agents and new contributors. Read it before changing code.

## Load first

| File | Why |
|---|---|
| `docs/architecture.md` | System map, invariants, where everything lives |
| `docs/services/<service>.md` | Read before touching that service |
| `CONTRIBUTING.md` | Commit format, review checklist, testing expectations |
| `docs/design/holdfast-design-and-build-plan.mdx` | Full design and build plan (read only the relevant section) |

## Non-negotiables

1. **Never oversell (I1).** Every inventory mutation is one atomic Lua script or one conditional SQL statement. No read-modify-write across round trips, no application-level locks.
2. **Everything is retried.** Mutating APIs take an `Idempotency-Key` or derive deterministic IDs. Handlers and consumers assume at-least-once delivery.
3. **PostgreSQL is the source of truth.** Valkey is a fast projection that can be rebuilt (`holdfastctl inventory provision`).
4. **One schema per service.** A service never reads another service's tables; it calls that service's API.
5. **Applied migrations are immutable.** Add a new migration; `scripts/check-migrations.sh` enforces it.
6. **Internal endpoints live on the admin port** and require operator credentials.
7. **Never read, print or commit secrets:** `.env`, `.local/`, key files, tokens.
8. **Contracts change together with their docs.** API, error codes, keys, scripts, metrics or invariants change: update `docs/` in the same PR.

## Commands

```bash
make test     # unit tests with the race detector; no dependencies
make itest    # integration tests; starts PostgreSQL + Valkey with Docker
make e1       # contention experiment; must end with "RESULT: PASS"
make lint     # golangci-lint + migration checks
```

## Definition of done

- Unit tests for logic, integration tests for anything touching Valkey or PostgreSQL, failure paths included.
- `make e1` still passes after changes near inventory or the final guard.
- Metric labels stay bounded: route patterns and enums, never client-supplied values.
- Docs updated, and the commit follows `CONTRIBUTING.md`.
