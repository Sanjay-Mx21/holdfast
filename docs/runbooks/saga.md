# Booking saga runbook

Operator procedures for the purchase flow after the hold: bookings,
payments, refunds and the events between them. Service references:
`docs/services/booking.md`, `docs/services/payment.md`,
`docs/services/mockpsp.md`. Decisions behind it: ADRs 0008 to 0010.

Two rules for every procedure here:

- **Never edit rows by hand.** Every fix goes through the normal path (an
  event, a command) so the state machines, the ledger and the outbox stay
  consistent.
- **Replaying is safe.** Every consumer deduplicates on `ce_id` in the same
  transaction as its effect, and the provider's calls are keyed by the
  intent, so delivering something twice changes nothing.

## RB-3 Drain a dead-letter topic

**Symptom:**

- `holdfast_kafka_consumed_total{result="dead_lettered"}` rose;
- or a consumer logged `kafka: message dead-lettered`;
- or bookings stopped moving: `holdfast_kafka_consumer_lag` is flat but
  bookings stay `PENDING_PAYMENT` or `REFUND_REQUIRED`.

**Meaning:** a consumer gave up on a message. It was unreadable, or failed
`MAX_ATTEMPTS` times, or its handler said it can never succeed (for
example, inventory-svc does not know the hold). The message waits in
`<topic>.dlq.v1` with its reason. The consumer moved on, so later messages
were not blocked.

| Consumer group | Topic | Its dead-letter topic |
|---|---|---|
| `booking-saga` (booking-svc) | `holdfast.payment.v1` | `holdfast.payment.dlq.v1` |
| `payment-refunds` (payment-svc) | `holdfast.booking.v1` | `holdfast.booking.dlq.v1` |

1. **Look:**
   - in Redpanda Console (http://localhost:8089), open the dead-letter
     topic. Each message keeps its original headers (`ce_id`, `ce_type`,
     `traceparent`) plus `dlq_reason`, `dlq_topic`, `dlq_partition`,
     `dlq_offset` and `dlq_group`;
   - or list them without changing anything:

   ```bash
   holdfastctl dlq replay --topic holdfast.payment.dlq.v1 --dry-run
   ```

2. **Find the cause** from `dlq_reason` and the consumer's logs. The trace ID
   in `traceparent` finds the request in Jaeger. Typical causes, and what
   to do:
   - **`inventory: hold not found`:** inventory lost the hold (Valkey data
     loss). Rebuild inventory first (`holdfastctl inventory rebuild`;
     runbook RB-INV-4).
   - **`the provider refused the refund`:** see RB-4.
   - **`decode` errors:** a producer sent something the consumer cannot
     read. Fix the producer or the consumer; never the message.
   - **A transient cause** (a database or dependency outage) that outlasted
     the retries, which last 10 minutes since P56 (they were 2 seconds, and
     experiment E4's 5 s database outage dead-lettered payments): make sure
     it is over. Replaying puts each payment through the saga again: a
     capture whose booking was cancelled meanwhile is refunded, and one
     whose booking was confirmed marks its hold sold in inventory.
3. **Replay** once the cause is fixed:

   ```bash
   holdfastctl dlq replay --topic holdfast.payment.dlq.v1
   ```

   - Each message goes back to its original topic, with its key, headers
     and `ce_id` intact.
   - The consumer group `holdfastctl-dlq-replay` records how far the topic
     was replayed, so a second run replays only messages that arrived
     since.
   - `--max N` replays a few first, to check the fix.
   - Replay reads up to the topic's end when it starts. If messages land in
     the dead-letter topic again, the cause is not fixed: stop and look
     again.
4. **Check:**
   - the affected bookings moved (`GET /v1/bookings/{id}`, or
     `holdfast_booking_saga_outcomes_total`);
   - `holdfast_kafka_consumed_total{result="dead_lettered"}` stays flat.

## RB-4 Manual refund

**Symptom:** a buyer was charged but has no ticket, and their booking has
been `REFUND_REQUIRED` for more than a few minutes.

- `holdfast_payment_refund_requests_total{result="rejected"}` rose, or the
  refund request was dead-lettered (RB-3).
- Or the intent is `REFUND_PENDING` but the provider's `refund.completed`
  webhook never came.

**Meaning:** the saga decided to refund (the final guard refused the sale,
or the capture came too late), but the refund did not complete.

1. **Find the booking** and confirm its status is `REFUND_REQUIRED` (the
   buyer's booking page, or `GET /v1/bookings/{id}` as the buyer).
2. **If the refund request was dead-lettered**, fix the cause and replay it
   (RB-3). If the provider refused it (`NOT_REFUNDABLE`,
   `ALREADY_REFUNDED`), check the payment in the provider's dashboard
   first. A refund that already happened only needs its webhook (step 4).
3. **Ask again:**

   ```bash
   holdfastctl refund --booking <booking id>
   ```

   - It publishes a new `booking.refund_required.v1` (reason `OPERATOR`).
     payment-svc handles it like the saga's own: the intent goes to
     `REFUND_PENDING`, and the provider is asked for the refund, keyed by
     the intent, so it can never refund twice.
   - It refuses any booking that is not `REFUND_REQUIRED`:
     - `CONFIRMED`: a sale; refunding sales is not supported yet;
     - `REFUNDED`: done;
     - `PENDING_PAYMENT` or `CANCELLED`: no captured payment (a capture
       that arrives later is the saga's to decide).
4. **Wait for the provider's `refund.completed` webhook.** The intent
   becomes `REFUNDED` (with the capture's ledger entries reversed), then
   the booking becomes `REFUNDED` (`booking.refunded.v1`). If the webhook
   is lost, the Phase 5 reconciler finds the refund in the settlement
   report.
5. **Check:** the booking is `REFUNDED`, and
   `holdfast_payment_refund_requests_total{result="requested"}` rose by one.
