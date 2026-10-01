# 0003. Embedded migration runner

- Status: accepted
- Date: 2026-09-25

## Context

Migrations must run exactly once even when several replicas start together,
ship with the code that depends on them, and not require extra tools in the
runtime images. The design doc proposed goose.

## Decision

A small runner (`internal/platform/postgres/migrate.go`) applies SQL files
embedded from `db/migrations/<schema>/`:

- a PostgreSQL advisory lock serialises concurrent runners;
- each migration runs in its own transaction with its bookkeeping row;
- per-schema `schema_migrations` tables store a SHA-256 checksum, so editing
  an applied migration fails loudly;
- files use goose naming and markers, so the goose CLI stays a drop-in alternative.

Migrations run as a one-shot job (`holdfastctl migrate`) before services start.

## Consequences

- No down-migrations at runtime; breaking changes use expand and contract.
- Statements that cannot run in a transaction (for example
  `CREATE INDEX CONCURRENTLY`) need a no-transaction mode, to be added when
  the first such migration appears.
