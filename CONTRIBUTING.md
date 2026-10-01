# Contributing

## Workflow

1. Branch from `main` as `<tag>/<short-description>`, for example `feat/queue-admission`.
2. Keep pull requests small and single-purpose.
3. Before pushing run `make fmt vet lint test itest`, plus `make e1` if you touched inventory or the guard.

## Commit messages (the 50/72 rule)

```
<tag>: <Subject in imperative mood, capitalised, no period>

<Body wrapped at 72 characters: why the change was needed, what it
affects, trade-offs. The diff already shows how.>

Refs: #12
```

- Subject of at most 50 characters. Check it with "If applied, this commit will ...".
- Tags: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `ci`, `chore`, `style`.
- One logical change per commit. Link issues and PRs in the footer (`Refs:`, `Fixes:`, `PR:`).

```
fix: Re-take units on late payment confirmation

A payment confirmed after its hold expired was dropped, leaving a paid
booking with no inventory behind it. confirm.lua now takes the units
from the pool again and reports a late outcome, which reconciliation
flags if the pool goes negative.

Fixes: #27
```

## Review checklist

- [ ] **Invariants:** can this oversell, lose units or break a per-user cap under concurrency?
- [ ] **Atomicity:** every check-then-act is one Lua script or one conditional SQL statement.
- [ ] **Idempotency:** safe to retry at every layer; replays return the original result.
- [ ] **Errors:** mapped to stable problem codes; nothing internal leaks to clients; unexpected errors are logged with the request ID.
- [ ] **Context:** the request context reaches every Valkey and PostgreSQL call.
- [ ] **Observability:** bounded metric labels, structured logs, no secrets or tokens in logs.
- [ ] **Tests:** success and failure paths; integration tests for storage behaviour.
- [ ] **Docs:** service docs, runbooks and ADRs updated when contracts or decisions change.

## Migrations

- One new file per change: `db/migrations/<schema>/NNNNN_description.sql`, goose format.
- Never edit or delete a merged migration.
- Breaking changes use expand and contract: add, backfill, switch readers, remove in a later release.

## Dependencies

- Prefer the standard library; add a module only when it removes real complexity.
- Upgrades: `make deps-upgrade`, then `make test itest e1` before merging.

## Architecture decisions

Record significant decisions in `docs/adr/`, starting from `0000-template.md`.
