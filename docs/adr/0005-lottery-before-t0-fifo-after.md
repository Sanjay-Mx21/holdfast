# 0005. Lottery before T0, FIFO after

- Status: accepted
- Date: 2026-10-02

## Context

A flash sale opens at a published time, T0. Long before it, people (and bots)
sit on the page. If the queue were first come, first served from the moment
it opens for joining, places would go to whoever joined earliest, which
rewards refreshing, scripts and the lowest network latency, and gives nothing
to someone who arrives a few minutes before T0. If joining only opened at T0,
everyone would arrive in the same second, and the order would again be decided
by milliseconds of latency.

After T0 the situation is different: somebody arriving later than somebody
else has no claim to stand ahead of them.

Whatever rule is chosen must be checkable: "fair" has to be something a test
and an experiment can measure (invariant F1).

## Decision

- **Before T0, a lottery.** A joiner gets a random score in [0, 1), drawn
  server-side from `crypto/rand` (`internal/queue/lottery.go`), so every
  pre-T0 order is equally likely, however early or how often someone joined.
- **After T0, arrival order.** A joiner gets 1 plus an arrival counter, so
  every post-T0 joiner stands behind every lottery joiner, in the order the
  server received them. This also applies while a sale is `FROZEN`, which is
  only reachable after T0.
- **One place per identity, never re-rolled.** `join.lua` adds a member with
  `ZADD NX` after checking it is not there; a rejoin returns the original
  score.
- **T0 is decided by Valkey's clock, inside `join.lua`.** The first join at or
  after T0 flips the queue from `PRE` to `OPEN` itself, in the same atomic
  step; an idempotent opener in every replica flips it when nobody joins. No
  process's tick or failover can let a late joiner into the lottery.

## Consequences

- Fairness is measurable and measured: the F1 model test checks the order
  step by step against a reference model, and experiment E6 found Spearman's
  ρ between join time and position −0.0008 over 100,000 lottery joiners and
  exactly 1 after T0 (`loadtest/results/`).
- Nobody knows their position before T0; `GET /v1/queue/{id}/me` returns
  `randomizingAt` instead. Clients must explain this to buyers.
- The unit of fairness is the identity, so the lottery is only as fair as
  identities are hard to multiply. Until auth-svc (Phase 4), buyers are
  identified by a development header; per-IP and per-user rate limits slow
  abuse but do not stop someone with many accounts.
- The design doc's first `join.lua` gave the lottery to every state except
  `OPEN`, which would have let `FROZEN` joiners jump the queue, and flipped T0
  from the admission leader's tick, which would have let late joiners into
  the lottery. Both were corrected while building (progress log P13, P15).
