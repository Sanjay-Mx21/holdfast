# queue-svc runbook

Operator procedures for the waiting room. Service reference:
`docs/services/queue.md`. Grows with each Phase 2 task.

## RB-Q-1 `PROVISION_CONFLICT`

**Symptom:** `PUT /internal/v1/events/{id}/queue` or `holdfastctl event create`
fails with 409 `PROVISION_CONFLICT`.

**Meaning:** the event's queue was already provisioned with different
settings. Nothing was changed: the stored settings still apply.

1. Read the stored settings:
   `docker compose exec valkey valkey-cli HGETALL "q:{<event id>}:config"`
   (`opens_at_ms` is milliseconds since the Unix epoch; `session_ttl_ms` is milliseconds).
2. If the request was a mistaken retry with different values, resend it with
   the stored values: the call returns 200 and changes nothing.
3. Changing settings of a provisioned queue is not supported yet. Before the
   sale opens (state `PRE`, no buyers joined), a development environment can
   delete `q:{<event id>}:config` and provision again. Never do this once the
   state is past `PRE` or buyers have joined.

## RB-Q-2 Valkey unreachable

**Symptom:** queue requests return 503 `UNAVAILABLE` with `Retry-After`;
`/readyz` on the admin port fails its `valkey` check.

1. Check Valkey: `docker compose ps valkey` and
   `docker compose exec valkey valkey-cli ping`.
2. queue-svc reconnects on its own once Valkey answers; readiness recovers
   without a restart.
3. If Valkey lost its data, re-provision each event's queue with its original
   settings (provisioning is idempotent). Joined members cannot be recovered
   yet; the design's recovery path arrives with Phase 5.
