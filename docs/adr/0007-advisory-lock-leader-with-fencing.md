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

## Amendment, 2026-10-03: one lock session per replica

The decision above held one PostgreSQL connection per leading event, so
queue-svc's connections grew with the number of events. On the development
machine it reached 31 for 35 queues, and experiment E1 could no longer fit
beside it in PostgreSQL's 100 connections (progress log P39).

Each replica now holds all of its events' locks on **one** connection,
`LockSession` in `internal/queue/locksession.go`. Advisory locks belong to a
session, and a session can hold many.

- **The rest of the decision stands:** the election, the lock keys, the
  fencing epochs and the retry interval are unchanged.
- **A term that ends releases its lock explicitly** (`pg_advisory_unlock`)
  instead of closing a connection. Locks stack within a session, so an
  unlock that fails drops the whole session rather than risk a lock left
  held.
- **Losing the session is losing every lock at once**, exactly as a process
  crash always was. A generation number makes every leader of that session
  step down on its next tick. A standby replica takes the events over with
  newer epochs, and fencing refuses anything the old leaders still try.
  `TestLosingTheSessionHandsEveryEventOver` kills the session's backend and
  checks all three.
- **Queries on the shared session run detached** from the caller's
  cancellation, with a 5 s timeout. A query cancelled part-way can break a
  pgx connection, and one controller shutting down must not take the other
  events' locks with it.
- **Liveness pings are shared:** at most one per tick interval for the whole
  session, not one per leader per tick.
- **Standby attempts no longer open a connection each.**

Consequence, replacing the third point above: a replica uses one connection
for leadership however many events it runs (`TestOneSessionLeadsManyEvents`
checks one backend holding 12 events' locks). The trade-off: a broken
session moves all of a replica's events to the standbys at once, rather than
one at a time.

