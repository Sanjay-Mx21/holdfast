# 0007. Leader election with PostgreSQL advisory locks plus fencing epochs

- Status: accepted
- Date: 2026-10-02

## Context

Admission must be paced by exactly one writer per event: two controllers
ticking at once would admit at twice the rate and overrun the session budget.
Every queue-svc replica must be able to take over when the leader dies, within
seconds. And a leader can stop without dying: a long garbage-collection pause,
a frozen VM or a network partition can leave it believing it still leads after
a successor has been elected. (On the development laptop a whole VM froze for
17 minutes during an experiment, progress log E10.)

Options considered: a lease in Valkey (`SET NX PX`), which depends on clocks
and on Valkey's own failover; etcd, Consul or ZooKeeper, which add a system to
run; a Kubernetes lease, which does not exist outside Kubernetes. PostgreSQL is
already running and already the source of truth.

## Decision

- **Election:** each replica runs a controller per event (from the `q:events`
  work list) that calls `pg_try_advisory_lock(key)` on a connection taken out
  of the pool for good, where `key` is FNV-1a of a HoldFast prefix and the
  event ID. The lock belongs to the session: if the process dies and its
  connection closes, PostgreSQL releases it, and a standby wins it on its next
  attempt (`LEADER_RETRY_INTERVAL`, 2 s).
- **Fencing:** every new term increments `adm:{E}:epoch` and remembers its
  value. `advance.lua` refuses, atomically, any tick whose epoch is not the
  current one, so a leader that wakes up after being replaced cannot admit
  anyone or overwrite the status document; it sees `ErrFenced` and steps down.
- **Liveness:** the leader pings its lock connection before every tick and
  steps down if it has gone. A controller reads the event's settings before
  starting a term and stops when the event no longer exists, so it never
  recreates a removed event's keys (progress log P24).

## Consequences

- Correctness does not depend on the election being perfect: even with two
  processes believing they lead, only the newest epoch's ticks take effect.
  The failover integration test runs two real controllers, kills the leader,
  and checks the takeover, the higher epoch and that the old epoch is fenced.
- Admission pauses while PostgreSQL is unreachable; joining, positions and the
  status document do not depend on it (readiness does not either).
- Each leading event holds one PostgreSQL connection per replica that leads
  it, and every standby attempt opens one briefly. Thousands of simultaneous
  events would need connection pooling designed for it.
- Failover is fast only if PostgreSQL notices the dead connection. A clean
  shutdown or a process crash closes it at once; a host that vanishes without
  closing TCP holds the lock until PostgreSQL's TCP keepalive gives up, which
  with default settings can take far longer than 2 seconds. Tuning
  `tcp_keepalives_*` (or an application-level lease timeout) is a Phase 5
  chaos-testing item.
