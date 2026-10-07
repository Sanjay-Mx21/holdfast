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
  no restart is needed. If data was lost, continue with RB-INV-4. With
  Sentinel (task 5.2) a dead primary is replaced in about 7 s by itself:
  see `docs/runbooks/valkey.md` (RB-VK-1), and RB-VK-3 if it is not.

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

## RB-INV-4 Rebuild Valkey inventory from PostgreSQL (RB-2)

Use this after Valkey lost writes or everything: a failover (RB-VK-1), a
crash, an accidental flush, or counters that disagree with PostgreSQL.
PostgreSQL is the source of truth; `holdfastctl inventory rebuild` (task
5.3) makes Valkey agree with it, for one event, in one atomic script:

| Valkey | Set to, from PostgreSQL |
|---|---|
| `inv:{E}:avail` | capacity − sold − units in `PENDING_PAYMENT` bookings |
| `inv:{E}:user:U` | units the user bought (`user_event_purchases`) plus their pending bookings'; every other counter deleted |
| the pending bookings' holds | kept, or recreated, as PAYING until the payment deadline plus `PAYMENT_GRACE` (`--grace`, 3 minutes): their confirmation then finds them and does not take their units twice |
| every other open hold | released (`released_by` `REBUILD`): it has no booking, so its units are back |
| `inv:{E}:config` | capacity and per-user limit from the catalog; `frozen` set |

1. **Freeze the sale** (RB-1): `holdfastctl freeze --event E`. The rebuild
   freezes inventory's holds itself before reading PostgreSQL, but the
   queue's admissions are only paused by `freeze`. (If Valkey lost the
   event entirely, `freeze` reports inventory "not provisioned": go on.)
2. **Look first:** `holdfastctl inventory rebuild --event E --dry-run`
   prints what PostgreSQL says, the pool before and after, and how many
   counters and holds would change. It writes nothing.
3. **Rebuild:** `holdfastctl inventory rebuild --event E`. Holds stay
   frozen afterwards. It is idempotent: a second run changes nothing.
4. **Check:** `holdfastctl inventory status --event E`, and the auditor's
   invariants (`localhost:9097/metrics`: I1, I4 and I5 at 0).
5. **Unfreeze:** `holdfastctl unfreeze --event E`.

Buyers whose holds had no booking yet see "hold expired, try again"; buyers
paying keep their holds. Repeat for each event on sale.

- **Races are on the safe side.** Payments carry on during a freeze. A
  booking confirmed between the read and the rebuild keeps its SOLD hold
  ("confirmed meanwhile"), and its units are counted once either way. A
  booking cancelled in between leaves its units out of the pool until the
  next rebuild: fewer units for sale, never more. PostgreSQL's final guard
  decides every sale regardless.
- **Cost:** the script blocks Valkey while it runs, for every event on
  that Valkey: 662 ms for a sale with 50,000 buyers, 10,000 pending
  bookings and 5,000 abandoned holds (1.25 s end to end; dry run 158 ms).
  Smaller sales take milliseconds.
- `holdfastctl inventory provision` only creates a missing pool and never
  replaces one: it is for new events.

## RB-INV-5 Rotate the admission-token signing key

With `ADMISSION_JWKS_URL` set (the default in Compose), inventory-svc follows
queue-svc's key set and needs no change: follow RB-Q-7.

With key files only (`ADMISSION_PUBLIC_KEY_FILES`):

1. `holdfastctl keys generate --out-dir <dir> --name admission-<date>`.
2. Deploy inventory-svc with both public keys:
   `ADMISSION_PUBLIC_KEY_FILES=old.pub,new.pub`.
3. Switch the issuer (queue-svc, or tooling) to the new private key.
4. Wait at least the maximum token lifetime (`ADMISSION_TOKEN_TTL`, 10 minutes
   by default).
5. Remove the old public key and redeploy.

## RB-INV-6 Negative availability or suspected oversell

- **Expected case:** `holdfast_hold_confirms_total{outcome="late"}` increased.
  A payment succeeded after its hold expired, and the units were re-taken, so
  `avail` can dip below zero by at most the late units. PostgreSQL still
  guarantees `sold <= capacity` for confirmed sales.
- **Unexpected case (no late confirms):** stop the sale, capture
  `valkey-cli get 'inv:{E}:avail'` and
  `SELECT capacity, sold FROM booking.event_inventory WHERE event_id = 'E'`,
  and open an incident. PostgreSQL is authoritative; rebuild with RB-INV-4
  (`holdfastctl inventory rebuild`) after the investigation. E1 runs on every CI build to keep this case impossible.

## RB-INV-7 booking-svc's gRPC calls are refused

**Symptom:** booking-svc logs `Unauthenticated` or `PermissionDenied` from
inventory, and `holdfast_grpc_server_handled_total{code="Unauthenticated"}`
(or `PermissionDenied`) rises on inventory-svc.

1. `Unauthenticated`: the token is missing, expired or signed by a key
   inventory-svc does not trust. Check that `GRPC_TRUSTED_CALLERS` lists
   `booking=` with the public half of the key booking-svc signs with, and that
   the two hosts' clocks agree within `TOKEN_LEEWAY`. inventory-svc logs each
   refusal (`grpc call failed`, with the method and reason).
2. `PermissionDenied`: the caller is trusted but may not call that method
   (`inventory.GRPCAllow`). A new caller or method is a code change, not a
   configuration change.
3. **Rotating booking-svc's key:** inventory-svc trusts one key per caller,
   so a rotation is not seamless. Generate a new pair, then restart
   inventory-svc trusting the new public key and booking-svc signing with the
   new private key as close together as possible: calls signed with the old
   key are refused until booking-svc runs with the new one. (Trusting two keys
   per caller during a rotation would remove the gap; not built yet.) If a key
   is compromised, remove it at once: the refusals are the point.

