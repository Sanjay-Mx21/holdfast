# Sale runbook

Operator procedures for a sale as a whole, across queue-svc and
inventory-svc. Service references: `docs/services/queue.md` and
`docs/services/inventory.md`.

## RB-1 Freeze the sale

**When:** anything looks wrong during a live sale: units or bookings that do
not add up, a payment provider in trouble, a bot wave, a bad deploy. Freezing
buys time without losing anyone's place or money.

```bash
holdfastctl freeze --event <event id>
```

```
inventory: holds frozen: no new holds
queue: FROZEN: admissions paused
```

What a freeze does, in this order:

1. **inventory-svc takes no new holds.** A buyer who tries gets 503
   `SALE_PAUSED` with `Retry-After: 10`. A retry of a hold made before the
   freeze still returns it, and existing holds carry on: checkout, payment,
   confirmation, cancellation and expiry all work as usual, so no buyer
   halfway through paying is hurt.
2. **queue-svc admits nobody.** The queue goes `FROZEN`; the status document
   says so within one admission tick (250 ms), and the waiting room shows the
   sale as paused. Joins still take a place (arrival order), ranks are still
   reported, and people already admitted keep their turn.

Running it again is harmless (`(already so)`). Check the state with
`holdfastctl queue status --event <event id>` (state `FROZEN`) and
`holdfastctl inventory status --event <event id>` (`frozen: true`); the
dashboard's `holdfast_queue_state{state="FROZEN"}` is 1.

**Unfreeze** when the cause is resolved:

```bash
holdfastctl unfreeze --event <event id>
```

```
inventory: holds resumed
queue: OPEN: admissions resumed
```

Holds resume first, so the people admitted already can buy, then admissions.

**Notes:**

- If the queue is not `OPEN` (before T0, sold out or closed), only the holds
  are switched and the command says `queue: left as it is (...)`. Before T0,
  admissions will start at T0 while holds stay frozen: unfreeze before then,
  or expect buyers to see `SALE_PAUSED`.
- A frozen queue is never marked `SOLD_OUT` by the admission leader; if the
  last units sell during a freeze, the queue becomes `SOLD_OUT` on the first
  tick after unfreezing.
- Each half has an admin endpoint (`POST /internal/v1/events/{id}/freeze` and
  `/unfreeze`, operator token, on each service's admin port) for automation;
  freezing only one half is rarely what you want.
- A freeze needs the event's inventory in Valkey. After Valkey lost it
  (RB-INV-4), provision the inventory first, then freeze.
