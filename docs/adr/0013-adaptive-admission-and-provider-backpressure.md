# 0013. Adaptive admission (AIMD), with backpressure from the payment provider

- Status: accepted
- Date: 2026-10-07

## Context

The waiting room admits buyers at a rate an operator sets per event
(Phase 2). That rate is a guess made before the sale, and the purchase path
behind it is not constant:

- **The payment provider is the slowest step and the least controllable.**
  Every purchase creates an order there and waits for a capture. When it
  slows down or fails, checkouts stall, sessions run out, and buyers who
  waited an hour are let in to fail.
- **When it is down, admitting anyone is pointless.** payment-svc's circuit
  breaker (task 3.9) fails calls fast, but the waiting room kept sending
  people into a checkout that could not be paid.
- **The signal must be cheap and local.** The admission leader ticks every
  250 ms per event; it cannot run queries against Prometheus, and a
  monitoring outage must not stop a sale.

The options:

- **A fixed rate** (as built): simple, but wrong whenever the guess is.
- **Closed-loop control from metrics** (Prometheus queries): rich signals,
  but the admission path would depend on monitoring, with scrape delays of
  seconds.
- **Backpressure only:** pause while the breaker is open, admit at the
  fixed rate otherwise. It handles the outage, not a provider that is slow
  but up.
- **AIMD on the rate, fed by the provider's own health:** TCP's congestion
  control: add a little while healthy, halve on trouble.

## Decision

Each admission leader runs AIMD on its admission rate, with the event's
configured rate as the ceiling, fed by the payment provider's pressure as
payment-svc measures it.

- **The signal:** payment-svc's provider client keeps a 10-second window of
  its calls (count, failures after retries, a latency histogram) and the
  breaker's state; the gRPC method `GetPressure` returns them (queue-svc
  only). One watcher per queue-svc replica reads it every second for all of
  its leaders.
- **The control, each tick:** breaker open, rate 0 (pause); half-open, the
  floor (5% of the event's rate); more than 1% of calls failed, or p99
  above 2 s (judged from 20 calls), halve, at most every 5 s, never below
  the floor; no reading for 5 s, halve ("unknown": slowing down beats
  guessing, and stopping would turn a monitoring fault into an outage);
  otherwise add 5% of the event's rate a second. After a pause the rate
  starts again at the floor, like TCP's slow start.
- **Recovery is active.** An open breaker closes only after a trial call
  succeeds, and while admissions are paused no checkout makes one. So
  payment-svc runs a prober: a cheap call every 5 s while the breaker is not
  closed. Without it, admissions could stay paused long after the provider
  came back (progress log P53).

## Consequences

- **A provider outage costs a pause, not a pile-up.** Drill RB-5 (task
  5.7) measures how quickly admissions stop and restart.
- **The configured rate becomes a ceiling, not a promise.** Operators read
  `holdfast_queue_admission_rate{event}` (and the dashboard's "allowed"
  line) to see what is applied, and `holdfast_queue_admission_backoffs_total`
  to see why it is lower.
- **The provider's p99 drives admission even when timeouts are the
  provider's injected fault**: in experiment E3, 5% of answers held back
  10 s pushed p99 past 2 s, and the rate dropped to its floor at times.
  That is the intended behaviour; the SLO (`AIMD_LATENCY_SLO`) is the knob.
- **Not wired yet:** the design also names hold and booking p99 as
  signals. inventory-svc and booking-svc expose no pressure, so only the
  provider counts; adding them means a `GetPressure` of their own and a
  combined judgement.
- **A new leader starts at the configured rate** and adapts from its first
  tick; a failover during an outage pauses at once.
- **queue-svc now calls payment-svc** (one call per replica per second), so
  payment-svc trusts queue-svc's service key for that one method.
