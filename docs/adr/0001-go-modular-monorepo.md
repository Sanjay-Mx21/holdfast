# 0001. One Go module, one binary per service

- Status: accepted
- Date: 2026-09-25

## Context

HoldFast has several services (queue, inventory, booking, payment, auth)
that share cross-cutting needs: configuration, logging, HTTP hardening,
health checks, metrics, authentication, database access. The team is one
developer who needs independent deployability without paying for
duplicated plumbing or drifting conventions.

## Decision

- One repository and one Go module. Each service is a binary in `cmd/<service>`.
- Domain code lives in `internal/<domain>`; shared infrastructure in `internal/platform`.
- Standard library first: `net/http` routing (method and wildcard patterns), `log/slog`, `flag`.
  Third-party modules only for PostgreSQL (pgx), Valkey (go-redis), metrics
  (Prometheus client), JWT, environment config and UUIDs.
- One Dockerfile builds any binary, selected with `--build-arg SERVICE=<name>`.

## Consequences

- One CI pipeline, atomic cross-service refactors, one set of conventions.
- `internal/` keeps packages private to this module.
- Service boundaries remain a discipline, not a compiler guarantee: no shared
  tables, no calls into another service's internals (AGENTS.md rules 3-4).
