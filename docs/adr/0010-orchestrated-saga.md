# 0010. An orchestrated saga instead of choreography or a workflow engine

- Status: accepted
- Date: 2026-10-03

## Context

A purchase crosses three services and two stores:

1. a hold in inventory-svc (Valkey);
2. a booking in booking-svc (PostgreSQL);
3. a payment in payment-svc (PostgreSQL) and at the payment provider.

It can end several ways: sold; cancelled with the units back; refunded
because the final guard refused a capture; or a late capture honoured or
refunded. Every captured payment must end CONFIRMED or REFUNDED
(invariant I3), and nothing may be sold twice (I1, I2).

The options:

- **Choreography:** each service reacts to the others' events with no owner
  of the flow. No coordinator, but the purchase's state is spread across
  three services. "Where is this booking stuck?" has no single answer, and
  adding a step means changing several services' reactions.
- **A workflow engine (Temporal):** durable execution, timers and retries for
  free. But it means another stateful cluster to run and learn, its own
  persistence, and it puts the flow's state outside the services'
  databases, where the final guard and the outbox live.
- **Orchestration by the service that owns the outcome:** booking-svc owns
  the booking's state machine (design doc 7.2), so it decides. The others
  do what they are asked and report what happened.

## Decision

booking-svc orchestrates (`internal/booking/saga.go`).

- **The booking row is the saga's state.** Its trigger refuses every move
  outside design doc 7.2.
- **Synchronous steps** happen in `POST /v1/bookings` over gRPC (ADR 0008):
  the hold is marked PAYING, and the payment intent and checkout URL are
  created. The request is idempotent and resumable.
- **Asynchronous steps** are booking-svc's reactions to payment-svc's events,
  each one transaction (dedup, the booking locked, the decision, its outbox
  event):
  - a capture runs the final guard and confirms, or marks the booking
    REFUND_REQUIRED;
  - a failed or expired payment cancels it;
  - a completed refund closes it as REFUNDED.
- **Compensation is explicit:**
  - a cancelled or refund-required booking's hold is released;
  - a refund-required booking is refunded by payment-svc, which consumes
    `booking.refund_required.v1`;
  - a late capture is honoured if the guard allows and refunded if not.
- **Inventory is settled after the commit, from the booking's state**
  (`Confirm` or `ReleaseForFailedPayment`, both idempotent), so a failed
  call is retried by redelivery.
- **Timeouts are the deadline job** (overdue PENDING_PAYMENT bookings) and
  the provider's order expiry. Lost webhooks are found by payment-svc's
  status poller; the Phase 5 reconciler compares settlement reports.

## Consequences

- One place answers "what happened to this booking": its row and its outbox
  events. The decision table (`TestSagaDecisionTable`) and E1 part B test
  the whole flow, with failures and redelivery.
- booking-svc is coupled to the order of steps. A new step (a fraud check,
  say) is a change to the saga and its state machine, deliberately in one
  place.
- We own durability and timers: the outbox, idempotent consumers, the
  deadline job and the poller. This is more code than with Temporal, but it
  sits in the same PostgreSQL transactions as the guard and the ledger,
  and it needs no other cluster. If flows multiply (seat maps, transfers,
  partial refunds), a workflow engine is worth revisiting.
- Operators recover stuck flows through the normal path, never by editing
  rows: `holdfastctl dlq replay` (RB-3) and `holdfastctl refund` (RB-4).
