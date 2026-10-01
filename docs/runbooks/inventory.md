# Runbook: inventory-svc

Each entry lists symptoms, impact, how to diagnose, how to mitigate, and follow-up.
`E` is an event ID. Keys are described in `docs/services/inventory.md`.

## RB-INV-1 Valkey unreachable

- **Symptoms:** `/readyz` returns 503 with a `valkey` check error; the API
  returns 503 `UNAVAILABLE`; `holdfast_http_requests_total{code="503"}` rises.
- **Impact:** no new holds. Existing holds and PostgreSQL data are unaffected;
  expiries resume once Valkey is back.
- **Diagnose:** `valkey-cli -h <host> ping`, `valkey-cli info replication`,
  Sentinel or cluster status, network policy.
- **Mitigate:** restore or fail over Valkey. Clients reconnect automatically;
  no restart is needed. If data was lost, continue with RB-INV-4.

## RB-INV-2 Expired holds are not released

- **Symptoms:** `holdfast_sweeper_runs_total{result="error"}` rising;
  `holdfast_hold_releases_total{mode="EXPIRE"}` flat; availability lower than
  expected; `valkey-cli zrangebyscore 'inv:{E}:expiry' -inf <now-ms> limit 0 10`
  returns members.
- **Impact:** units stay locked in dead holds and the sale looks sold out early.
- **Diagnose:** `sweep failed` log lines; Valkey latency; application clock
  (candidates use the application clock, release decisions the Valkey clock).
- **Mitigate:** fix the Valkey issue, restart one replica if its sweeper is
  wedged (any replica can sweep), and correct clock drift (NTP).

## RB-INV-3 `PROVISION_CONFLICT`

- **Meaning:** someone tried to provision an event with a different capacity
  or per-user limit. Live capacity changes are deliberately refused.
- **Procedure (sale stopped):** update `booking.event_inventory.capacity` (and
  `booking.events.per_user_limit`) in PostgreSQL, delete the event's keys
  (`valkey-cli --scan --pattern 'inv:{E}:*' | xargs valkey-cli unlink`), then
  run `holdfastctl inventory provision --event E`. Deleting the keys drops live
  holds, so do this only when no sale is running.

## RB-INV-4 Rebuild Valkey inventory from PostgreSQL

Use this after Valkey data loss (crash without AOF, failover to an empty
replica, accidental flush).

1. Stop hold traffic: scale inventory-svc to zero or block `/v1/events/E/holds` at the edge.
2. For each event, run `holdfastctl inventory provision --event E`. The pool starts
   at capacity minus units sold in PostgreSQL. Per-user counters start empty;
   the final guard still enforces caps at confirmation.
3. Check with `holdfastctl inventory status --event E`, then restore traffic.

Once booking-svc exists (Phase 3), units in pending checkouts are subtracted too.

## RB-INV-5 Rotate the admission-token signing key

1. `holdfastctl keys generate --out-dir <dir> --name admission-<date>`.
2. Deploy inventory-svc with both public keys:
   `ADMISSION_PUBLIC_KEY_FILES=old.pub,new.pub`.
3. Switch the issuer (queue-svc, or tooling) to the new private key.
4. Wait at least the maximum token lifetime (15 minutes by default).
5. Remove the old public key and redeploy.

## RB-INV-6 Negative availability or suspected oversell

- **Expected case:** `holdfast_hold_confirms_total{outcome="late"}` increased.
  A payment succeeded after its hold expired, and the units were re-taken, so
  `avail` can dip below zero by at most the late units. PostgreSQL still
  guarantees `sold <= capacity` for confirmed sales.
- **Unexpected case (no late confirms):** stop the sale, capture
  `valkey-cli get 'inv:{E}:avail'` and
  `SELECT capacity, sold FROM booking.event_inventory WHERE event_id = 'E'`,
  and open an incident. PostgreSQL is authoritative; rebuild with RB-INV-4 after
  the investigation. E1 runs on every CI build to keep this case impossible.
