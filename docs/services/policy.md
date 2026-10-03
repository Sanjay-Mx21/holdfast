# Sale policies

The rules of a sale: who may enter, how much they may buy, how fast the crowd
is let in, and how an operator stops it. There is no policy service: each
rule is enforced where its data lives, so the hot path never waits on a
policy call. The pure decision code is `internal/policy`. Built in Phase 4
(task 4.3; proof of work in 4.4).

| Policy | What it does | Enforced by | Reference |
|---|---|---|---|
| **Verified-only window** | Until it ends, only buyers with a verified identity (a phone-code sign-in, the access token's `vrf` claim) may join | queue-svc at join, `policy.Check`; 403 `VERIFIED_ONLY` with `Retry-After` | [queue](queue.md#post-v1queueeventidjoin) |
| **Agent lockout** | Until it ends, buyers with the `AGENT` role may not join (the IRCTC Tatkal rule) | queue-svc at join, `policy.Check`; 403 `AGENT_LOCKOUT` with `Retry-After` | [queue](queue.md#post-v1queueeventidjoin) |
| **Per-user cap** (I4) | Held plus bought units per user per event never exceed the limit (1 to 10, default 4) | inventory-svc's `hold.lua` (atomic counter), then booking-svc's final guard in PostgreSQL | [inventory](inventory.md), [booking](booking.md) |
| **Proof of work** | Every join pays a few hundred thousand hashes, more during a surge | queue-svc: a stateless HMAC challenge, verified before any Valkey work | [queue](queue.md#get-v1queueeventidchallenge) |
| **Rate limits** | Per IP (IPv6 per /64) and per user, on joins, rank lookups and claims; per IP at the edge; per phone on sign-in codes | queue-svc (token buckets in Valkey), the edge, auth-svc | [queue](queue.md), [auth](auth.md) |
| **Admission rate and budget** | People are admitted at the event's rate, within a concurrent-session budget (Little's Law) and the units left × 1.3 | queue-svc's admission leader (`advance.lua`) | [queue](queue.md#admission) |
| **Freeze switch** | No new holds and no admissions; holds and payments in flight finish | inventory's `frozen` flag (`hold.lua`) and the queue's `FROZEN` state; `holdfastctl freeze` sets both | [RB-1](../runbooks/sale.md) |

## The windows

`internal/policy` is pure: `Check(rules, buyer, now)` returns nil or a
`*policy.Refused` with the reason and the time the buyer may join.

- An event's windows are stored with its catalog entry
  (`booking.events.verified_window_ends_at`, `agent_lockout_ends_at`) and in
  its queue (`q:{E}:policy`), set when the queue is provisioned
  (`holdfastctl event create --verified-only-for 15m --agent-lockout-for 30m`).
- When both windows refuse a buyer, the one that ends later is reported, so
  nobody is told to come back too early.
- A buyer identified by the development header has no role and is never
  verified.
- The windows govern joining only: someone already in the queue is never
  removed.

Their unit tests are a table in `internal/policy/policy_test.go`; the
integration tests (`internal/queue/policy_integration_test.go`) cover the
stored windows, re-provisioning and the join refusals.

## Why each rule lives where it does

- The windows need the buyer's identity and the event's settings, both at
  hand in queue-svc at join: checking there costs one small Valkey read.
- The per-user cap must be atomic with the units it limits, so it is part of
  the hold script, and the PostgreSQL guard re-checks it for every sale.
- The freeze must stop holds even from buyers admitted before it, so
  inventory checks its own flag; the queue's state stops new admissions and
  tells the waiting room.
