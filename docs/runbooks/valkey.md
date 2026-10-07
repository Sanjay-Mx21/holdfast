# Runbook: Valkey high availability

Valkey runs as a primary, one replica and three Sentinels (task 5.2;
`compose.yaml`, `deploy/valkey/`). The services and holdfastctl find the
primary through the Sentinels (`VALKEY_ADDRS` lists the Sentinels,
`VALKEY_SENTINEL_MASTER=holdfast`), so a failover needs no restart and no
configuration change.

| Member | Compose service | Address on the `valkey` network | Host port |
|---|---|---|---|
| Node, first the primary | `valkey` | 10.251.0.10:6379 | 6379 |
| Node, first the replica | `valkey-replica` | 10.251.0.11:6379 | 6380 (inspection only) |
| Sentinels | `sentinel-1` to `sentinel-3` | 10.251.0.21 to .23, port 26379 | none |

Members announce fixed addresses, never hostnames: Sentinel resolves names
inside its main loop, and a dead node's name stalled it into TILT mode,
where it would not fail over (P47).

Useful commands (from the repository root):

```bash
docker compose exec sentinel-1 valkey-cli -p 26379 sentinel get-master-addr-by-name holdfast   # who is primary
docker compose exec sentinel-1 valkey-cli -p 26379 sentinel ckquorum holdfast                 # can they fail over?
docker compose exec sentinel-1 valkey-cli -p 26379 info sentinel                              # sentinel_tilt
docker compose exec valkey valkey-cli info replication                                       # role, link, offsets
```

## What a failover costs

Measured with `make drill-valkey` on the development laptop, 2026-10-07
(`holdfastctl valkey probe` writing 10 times a second through the
Sentinels, the services' own client):

| Drill | Primary promoted after | Writes stopped for | Acknowledged writes lost | Services unready |
|---|---|---|---|---|
| `crash`: the primary killed (SIGKILL) | 7.1 s (5 s to declare it down, then the election) | 6.4 s | 0 | 5.2 to 6.5 s |
| `planned`: `SENTINEL FAILOVER holdfast COORDINATED` | 4.6 s | 74 ms (one write timed out) | 0 | none |
| `forced`: `SENTINEL FAILOVER holdfast`, not coordinated | 3.8 s | 0 | **10** (about one second) | none |

The killed node restarted and rejoined as a replica 17.3 s after it came
back. Every invariant stayed at 0 throughout.

- **Replication is asynchronous.** A primary that dies takes with it what
  the replica had not yet received. At the drill's load the replica was
  never behind, so nothing was lost; under a sale's load, up to about a
  second of holds, joins and admissions can be. PostgreSQL's final guard
  keeps I1 either way; `holdfastctl inventory rebuild` (RB-INV-4) puts
  inventory back.
- **Never fail over without `COORDINATED`** (P49). A plain `SENTINEL
  FAILOVER` promotes the replica while the old primary still takes writes,
  and those are discarded when it becomes a replica: in the drill, a
  second of acknowledged writes. `COORDINATED` pauses writes on the primary
  until the replica has caught up, then swaps the roles.
- **During the outage** inventory-svc and queue-svc answer 503
  `UNAVAILABLE` with `Retry-After` and fail readiness; the admission leader
  skips ticks. Clients reconnect to the new primary by themselves when the
  Sentinels announce it.

## RB-VK-1 The primary failed over

- **Symptoms:** a burst of 503s and failed readiness on inventory-svc and
  queue-svc for a few seconds; Sentinel logs `+switch-master holdfast`; the
  primary's address (`get-master-addr-by-name`) has changed.
- **Impact:** no holds or joins for about 7 s. Writes the old primary had
  not replicated are lost.
- **Do:**
  1. If a sale is live, freeze it (`holdfastctl freeze`, RB-1).
  2. Rebuild its inventory from PostgreSQL: `holdfastctl inventory
     rebuild --event E` (RB-INV-4, the design's RB-2), which repairs
     whatever the failover lost: the pool, the per-user counters, and the
     holds of bookings waiting for payment.
  3. Check the auditor's invariants (`localhost:9097/metrics`), then
     unfreeze.
  4. Bring the failed node back: it rejoins as a replica by itself (the
     Sentinels reconfigure it). Check `info replication` on it:
     `role:slave`, `master_link_status:up`.
- **Afterwards:** the old replica stays the primary. To return to the
  starting layout, fail back with RB-VK-2.

## RB-VK-2 Planned switch, and failing back

To move the primary off a node (maintenance, or back after RB-VK-1):

```bash
docker compose exec sentinel-1 valkey-cli -p 26379 sentinel failover holdfast COORDINATED
```

The replica must be in sync first (`master_link_status:up` on it), and the
Sentinels must not be in TILT mode (`sentinel_tilt:0`). Writes pause for
well under a second; nothing is lost.

**Locally,** tools on the host (`make itest`, `make e1`) talk to
localhost:6379, the `valkey` node, directly. While `valkey-replica` is the
primary they fail with `READONLY`: fail back first.

## RB-VK-3 The Sentinels cannot fail over

- **Symptoms:** `sentinel ckquorum holdfast` does not answer `OK`;
  `info sentinel` shows `sentinel_tilt:1`; Sentinel logs `+tilt`.
- **Quorum:** two Sentinels must see the primary down, and a majority (two
  of three) must vote. Bring back the missing Sentinels.
- **TILT mode:** a Sentinel stops acting for 30 s whenever its clock jumps
  backwards or its loop stalls for over 2 s. On the development laptop
  WSL2 steps the clock back about 60 ms every 30 s, so the Sentinels are in
  TILT about half the time (E26): a crash then takes up to 30 s longer to
  fail over (25.7 s instead of 7.1 s in the first drill). Production hosts
  slew their clocks with NTP instead.

## RB-VK-4 The failover drill

```bash
make drill-valkey MODE=crash     # or planned, or forced
```

`scripts/valkey-failover-drill.sh` checks that the replica is in sync and
the Sentinels have quorum, waits until no Sentinel is in TILT mode
(`AVOID_TILT=0` to act anyway), starts the probe, fails over, times the
promotion and each service's readiness, restarts a killed node and waits
for it to rejoin, fails back (`FAILBACK=0` to stay), and prints the
auditor's invariants. Its last line sums it up:

```text
DRILL mode=crash promoted_s=7.1 rejoined_s=17.3 writes=1145 failed=13 outages=1 longest_outage_ms=6397 lost=0 recovered=true
```

If the drill stops early, it starts a node it killed again, so the stack is
never left without a primary.
