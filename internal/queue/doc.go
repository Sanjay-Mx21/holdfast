// Package queue is the waiting room: it decides who may enter the purchase
// path, in what order and how fast.
//
// All state for an event lives in Valkey under keys that share the hash tag
// {eventID}, and every state change is one atomic Lua script (scripts/*.lua),
// as in package inventory.
//
// Built so far (Phase 2, task 2.1): provisioning. An operator stores the
// event's queue settings (sale opening time, admission rate, maximum
// concurrent sessions, session lifetime) and the queue starts in state PRE.
// Joining, the T0 transition, ranks, the admission controller and the status
// document follow in tasks 2.2 to 2.7.
//
// Queue states:
//
//	PRE --T0--> OPEN --> SOLD_OUT --> CLOSED
//	             |  ^
//	             v  |
//	            FROZEN
package queue
