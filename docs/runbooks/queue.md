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

## RB-Q-3 Legitimate buyers get `RATE_LIMITED`

**Symptom:** joins return 429 `RATE_LIMITED`;
`holdfast_queue_joins_total{result="rate_limited_ip"}` or
`{result="rate_limited_user"}` climbs.

1. Tell the two apart with the metric's `result` label.
2. **Per IP, many users:** usually a shared address: a campus, an office or a
   mobile carrier's NAT. If every bucket shows the edge's own address,
   `TRUSTED_PROXIES` does not match the edge, so queue-svc ignores its
   `X-Forwarded-For`. Check the busiest buckets:
   `docker compose exec valkey valkey-cli --scan --pattern 'rl:join-ip:*'`.
   Raise `JOIN_IP_BURST` and `JOIN_IP_PER_SECOND` and restart queue-svc.
3. **Per user:** a client retrying in a tight loop. Joining is idempotent, so
   one successful join is enough; fix the client before raising
   `JOIN_USER_BURST`.
4. Buckets expire on their own once full. To clear one at once:
   `valkey-cli DEL "rl:join-ip:<ip>"` (IPv6: the /64 with `:` written as `_`).

## RB-Q-4 The queue did not open at T0

**Symptom:** after the opening time, `q:{<event id>}:state` still reads `PRE`,
and `queue opened at T0` is missing from the logs.

Fairness is not at risk: every join after T0 opens the queue itself and gets a
place in arrival order. What is stale is the state other readers see.

1. Check the opener: `holdfast_queue_opener_runs_total{result="error"}`
   climbing, or `opener pass failed` in the logs, usually means Valkey trouble
   (RB-Q-2).
2. Check the event is on the work list:
   `valkey-cli SISMEMBER q:events <event id>`. Provisioning adds it; if it is
   missing, provision again with the same settings (idempotent) to re-register it.
3. Check the opening time: `valkey-cli HGET "q:{<event id>}:config" opens_at_ms`
   against `valkey-cli TIME` (seconds and microseconds). T0 is judged by
   Valkey's clock, not the operator's.

## RB-Q-5 Admissions are not advancing

**Symptom:** `q:{<event id>}:admitted` stays put while people wait;
`holdfast_queue_admitted_total` is flat.

1. Is anyone leading? `holdfast_queue_admission_leader{event="<event id>"}` is 1
   on exactly one replica. If none: PostgreSQL may be unreachable (elections
   need it); check `docker compose ps postgres` and the queue-svc logs for
   `admission: leadership term ended`.
2. Is the queue `OPEN`? `FROZEN`, `SOLD_OUT` and `CLOSED` admit nobody.
3. Is the session budget full? Compare
   `valkey-cli ZCARD "adm:{<event id>}:sessions"` with `max_sessions` in
   `q:{<event id>}:config`. Slots free themselves after the session TTL.
4. Repeated `fenced off by a newer leader` warnings mean two processes keep
   taking over from each other; check that every replica reaches the same
   PostgreSQL.

## RB-Q-6 Moving leadership to another replica

Restart the leading replica (`docker compose restart queue` locally). Its
connection closes, PostgreSQL releases the advisory lock, and a standby takes
over within `LEADER_RETRY_INTERVAL`. The new leader's epoch is higher, so
anything the old one still tries is refused.

## RB-Q-7 Rotating the admission-token signing key

Tokens live at most `ADMISSION_TOKEN_TTL` (10 minutes by default), so a
rotation only has to overlap for that long. inventory-svc follows
`/.well-known/jwks.json` and needs no restart; it refreshes every
`JWKS_REFRESH_INTERVAL` (5 minutes) and on an unknown key ID at most every
`JWKS_MIN_REFRESH_INTERVAL` (30 seconds).

1. Generate a new key pair (`holdfastctl keys generate --out-dir <dir>`).
2. **Publish before signing.** Add the new public key to
   `ADMISSION_EXTRA_PUBLIC_KEY_FILES` while queue-svc still signs with the old
   private key, and restart queue-svc. Check that `/.well-known/jwks.json`
   lists both key IDs.
3. Wait one `JWKS_REFRESH_INTERVAL`, so every inventory-svc has fetched the new
   key (`holdfast_authn_jwks_fetches_total{result="ok"}` rises on each).
4. Switch `ADMISSION_PRIVATE_KEY_FILE` to the new key, put the **old** public
   key in `ADMISSION_EXTRA_PUBLIC_KEY_FILES`, and restart queue-svc.
5. After `ADMISSION_TOKEN_TTL` has passed, remove the old public key from
   `ADMISSION_EXTRA_PUBLIC_KEY_FILES` and restart queue-svc.

Skipping steps 2 and 3 still works without restarts, but tokens signed with
the new key can be refused (401) for up to 30 seconds after inventory-svc's
last fetch.
