# 0006. Cached status polling instead of WebSockets or SSE

- Status: accepted
- Date: 2026-10-02

## Context

Tens of thousands of people wait in one event's queue at the same time, and
each needs to learn when their turn has come. A push channel (WebSockets or
Server-Sent Events) holds one open connection per waiting client: memory and
file descriptors on every replica, sticky load balancing, a reconnect storm on
every deploy or failover, and a fan-out of per-user messages exactly when the
system is busiest. Polling an origin that computes a per-user answer would
put the whole crowd's request rate on queue-svc.

What every waiting client needs to know is the same for all of them: how far
admission has got. Only their own rank differs, and it does not change once
T0 has passed.

## Decision

- **One status document per event**, identical for every client: state,
  opening time, `admittedUpTo`, queue size and `updatedAt`. The admission
  leader rewrites it in Valkey on every tick, inside the fenced `advance.lua`,
  so a stale leader cannot overwrite it.
- `GET /v1/events/{id}/status` serves it with `Cache-Control: public,
  max-age=1`. The edge (NGINX here, a CDN in production) caches it for one
  second per event, with `proxy_cache_lock` so concurrent misses wait for one
  origin fetch, and serves a stale copy while refreshing or if the origin
  fails. The cache key is the path alone, so a query string cannot bypass it.
- A client looks up its own rank once after T0 (`GET /v1/queue/{id}/me`,
  per-user rate limited), then polls the status document every few seconds
  with jitter, and claims its turn (`POST /v1/queue/{id}/admit`) once
  `admittedUpTo` reaches its rank.

## Consequences

- Origin load per event is constant, whatever the crowd. In experiment E2 the
  origin answered about 0.5 status requests per second while the edge served
  up to 1,667 polls per second at 30-second polling and 8,143 per second at
  3-second polling (`loadtest/results/`).
- queue-svc stays stateless towards waiting clients: no connections to drain
  on deploy, nothing to rebalance on failover.
- A buyer learns that their turn has come up to one poll interval plus one
  second late. Turns last the session TTL (10 minutes by default), so this is
  acceptable; the session slot, not the poll, bounds how long they have.
- Freshness must be watched: `holdfast_queue_status_age_seconds` grows when no
  leader writes the document (runbook RB-Q-5), and the document says when it
  was written.
- Clients must jitter their polls, or a CDN refresh can line them up.
