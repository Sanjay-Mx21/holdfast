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
//     per-IP and per-user token buckets first.
//
// The T0 transition, ranks, the admission controller and the status
// document follow in tasks 2.3 to 2.7.
//
// Queue states:
//
//	PRE --T0--> OPEN --> SOLD_OUT --> CLOSED
//	             |  ^
//	             v  |
//	            FROZEN
package queue
