# 0004. Add gRPC and tracing with the first cross-service call

- Status: accepted
- Date: 2026-09-25

## Context

The design doc schedules OpenTelemetry tracing in Phase 0 and inventory's
internal gRPC API in Phase 1. In Drop 1, inventory-svc has no caller yet and
every request stays inside one process.

## Decision

Introduce both in Phase 3, together with booking-svc, the first service that
calls inventory. Drop 1 ships structured logs with request IDs, Prometheus
metrics, health checks, pprof and build info.

## Consequences

- No API surface without a consumer and no traces with a single span.
- `X-Request-Id` already propagates, so traces can be linked to logs later.
- The service layer is transport-agnostic: gRPC arrives as a new adapter
  next to the HTTP handler, not as a rewrite.
