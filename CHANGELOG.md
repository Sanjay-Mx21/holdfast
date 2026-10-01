# Changelog

All notable changes to HoldFast. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
the milestone tags in the design plan (`docs/design/`, section 17.1).

## [Unreleased]

### Changed

- `prometheus/client_golang` upgraded from 1.23.2 to 1.24.1, proposed by
  Dependabot (#1).

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

[Unreleased]: https://github.com/Sanjay-Mx21/holdfast/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Sanjay-Mx21/holdfast/releases/tag/v0.1.0
