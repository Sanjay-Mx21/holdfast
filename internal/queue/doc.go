// Package queue is the waiting room: it decides who may enter the purchase
// path, in what order and how fast.
//
// All state for an event lives in Valkey under keys that share the hash tag
// {eventID}, and every state change is one atomic Lua script (scripts/*.lua),
// as in package inventory.
//
// Built so far (Phase 2):
//   - provisioning (task 2.1): an operator stores the event's queue settings
//     (sale opening time, admission rate, maximum concurrent sessions,
//     session lifetime) and the queue starts in state PRE;
//   - joining (task 2.2): before T0 a joiner gets a random lottery position,
//     after T0 a place in arrival order; joining is idempotent and passes
//     per-IP and per-user token buckets first;
//   - the T0 transition (task 2.3): PRE becomes OPEN when Valkey's clock
//     reaches the opening time, done by the first join after T0 or by the
//     Opener, whichever comes first;
//   - positions (task 2.4): before T0 a member learns when the lottery
//     closes, from T0 on their 1-based rank;
//   - admission (task 2.5): one leader per event, elected with a PostgreSQL
//     advisory lock and fenced by an epoch, admits people at the event's
//     rate while capping concurrent sessions (Little's Law);
//   - the status document (task 2.6): the leader rewrites it every tick; it
//     is the same for every client, so the edge can cache it for a second;
//   - admission tokens (task 2.7): an admitted buyer exchanges their turn for
//     an Ed25519-signed token, bounded by their session slot, that
//     inventory-svc requires for holds; the public keys are published as a
//     JWKS.
//
// Queue states:
//
//	PRE --T0--> OPEN --> SOLD_OUT --> CLOSED
//	             |  ^
//	             v  |
//	            FROZEN
package queue
