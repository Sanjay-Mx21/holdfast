// Package inventory is the hot path of HoldFast: it holds seats during a
// surge without ever overselling.
//
// All mutable state for an event lives in Valkey under keys that share the
// hash tag {eventID}, and every state change is one atomic Lua script
// (scripts/*.lua). That gives us:
//
//   - I1 no oversell on the fast path: avail never drops below zero because
//     the check and the decrement happen in one atomic step;
//   - I5 no lost units: every hold is either confirmed or released, driven by
//     an expiry index that the sweeper drains;
//   - I4 per-user caps: a per-user counter is checked in the same step.
//
// PostgreSQL remains the source of truth (see internal/booking/guard); Valkey
// is a fast, rebuildable projection of it.
//
// Hold lifecycle:
//
//	HELD --checkout--> PAYING --payment ok--> SOLD
//	  |                  |
//	  +-- expiry/cancel  +-- payment failed / deadline passed
//	  v                  v
//	RELEASED <-----------+
package inventory
