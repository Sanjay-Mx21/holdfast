# Correctness runbook: the auditor and the reconciler

What to do when a correctness alert fires (`deploy/alerting/holdfast.rules.yml`).
Background: the invariants are in `docs/architecture.md` (section 2), the
auditor in `docs/services/auditor.md`, the reconciler in
`docs/services/payment.md`.

The first move is always the same: **if a sale is live, freeze it**
(`holdfastctl freeze --event <id>`, RB-1). A frozen sale takes no new holds
and admits nobody, while payments already under way finish. Then
investigate.

## RB-AUD-1 `InvariantViolation`

**Symptom:** `holdfast_invariant_violations{invariant="I<n>"}` is above 0.
The mission-control dashboard's tile for that invariant is red.

Each check is one SQL query (or, for I5, one Valkey count) in
`internal/auditor/auditor.go`. Run the same query without `count(*)` to list
the rows: connect with `docker compose exec postgres psql -U holdfast holdfast`.

| Invariant | What it found | Look at |
|---|---|---|
| I1 no oversell | An event whose confirmed bookings hold more units than its capacity (or whose `sold` counter passed capacity) | `booking.bookings` with `status = 'CONFIRMED'` for the event, against `booking.event_inventory`. Every confirmation went through the final guard; find the one that did not (a bug, or a row written by hand) |
| I2 no double charge | An intent whose capture is booked twice in the ledger | `payment.ledger_entries` for the intent (`account = 'psp_receivable' AND direction = 'D'`). The second capture must be reversed by a refund (RB-4) |
| I3 money safety | Money captured over 15 minutes ago whose booking is not confirmed and that is not refunded | See `MoneySafetyBreach` below |
| I4 per-user cap | A user with more units held for payment or bought than the event allows | `booking.bookings` (`PENDING_PAYMENT`, `CONFIRMED`) and `booking.user_event_purchases` for the event and user |
| I5 no lost units | Holds still in inventory's expiry index 5 minutes past their expiry: the sweeper is not releasing them | `valkey-cli ZRANGEBYSCORE "inv:{<event>}:expiry" -inf <now-5min in ms>`; then the inventory runbook's sweeper section (RB-INV-2) |

A violation that disappears by itself was transient (I5 during a sweeper
restart, for example). One that stays is a defect: record it as an issue.

## RB-AUD-2 `MoneySafetyBreach`

**Symptom:** I3 is above 0. A buyer has paid, and HoldFast has neither
confirmed their booking nor refunded them, for over 15 minutes.

1. List them: the I3 query in `internal/auditor/auditor.go` without
   `count(*)` gives the intents. For each, note the intent's `status` and
   its booking's.
2. **Intent `CAPTURED`, booking `PENDING_PAYMENT` or `CANCELLED`:** the saga
   has not processed `payment.captured.v1`. Check booking-svc's saga is
   consuming (`holdfast_kafka_consumer_lag{group="booking-saga"}`), and the
   dead-letter topic (RB-3: `holdfastctl dlq replay`).
3. **Intent `CAPTURED`, booking `REFUND_REQUIRED`:** payment-svc has not
   started the refund. Check its refund consumer
   (`holdfast_kafka_consumer_lag{group="payment-refunds"}`) and the dead-letter
   topic; `holdfastctl refund` asks again (RB-4).
4. **Intent `REFUND_PENDING`:** the provider has not completed the refund.
   The reconciler asks again after 15 minutes (`refund_stuck` in
   `holdfast_recon_mismatch_total`); if the provider refuses, it is a human
   decision with the provider.

## RB-AUD-3 `ReconciliationNeedsAHuman`

**Symptom:** `holdfast_recon_mismatch_total` rose for a kind the reconciler
does not repair. payment-svc's error logs (`payment reconciler: a difference
with the provider needs a human`) name each intent.

| Kind | Meaning | Action |
|---|---|---|
| `amount_mismatch` | The provider moved a different amount than the intent's | Never booked. Compare the provider's dashboard with `payment.payment_intents`; refund the difference with the provider |
| `capture_unknown_to_psp` | HoldFast booked a capture the provider's report does not contain | A data-integrity incident: the webhook or poll that captured it must be traced (Jaeger, `payment.apply_webhook`) and the provider asked |
| `refund_unknown_to_holdfast` | The provider refunded a payment HoldFast never asked to refund | Ask the provider why; the booking may still be confirmed |
| `unknown_order` | The provider reports money for an intent HoldFast does not have | Another environment sharing the provider account, or lost data; check the reference |

The repaired kinds (`missed_capture`, `missed_refund`, `refund_stuck`) need
no action; a steady stream of them means webhooks are being lost, which the
provider should know.

## RB-AUD-4 `AuditorStale` or `ReconcilerStale`

**Symptom:** no completed check for 2 minutes (auditor) or pass for 15
minutes (reconciler).

1. Is it running? `docker compose ps auditor payment`; its readiness:
   `curl localhost:9097/readyz` (auditor), `curl localhost:9094/readyz`
   (payment-svc).
2. Its logs say which check or call fails: `docker compose logs auditor`
   (`auditor: check failed`) or `docker compose logs payment`
   (`payment reconciler: pass failed`).
3. The auditor needs PostgreSQL and Valkey; the reconciler needs PostgreSQL
   and the provider's settlement report (`GET /v1/settlements`).

While the auditor is stale, nothing watches the invariants: treat a live
sale with care until it is back.
