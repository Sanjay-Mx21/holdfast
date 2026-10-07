# Alerts

Every alert HoldFast raises, what it means, and where to go
(`deploy/alerting/holdfast.rules.yml`, task 5.1 and 5.6). Each rule has a
unit test in `deploy/alerting/tests.yml`; `scripts/check-alerts.sh` (part of
`make lint` and CI) checks the rules and runs the tests.

Prometheus evaluates the rules and sends firing alerts to **Alertmanager**
(http://localhost:9098, `deploy/alertmanager/alertmanager.yml`), which
groups them, routes `severity: page` and `severity: ticket` to their
receivers, and holds silences. Locally the receivers send nothing anywhere:
look at Alertmanager's page or Prometheus's Alerts page. While the
provider's breaker is open, `AdmissionStalled` and `CheckoutLatencyHigh`
are inhibited: they are the breaker's consequences.

| Alert | Severity | Fires when | Go to |
|---|---|---|---|
| `InvariantViolation` | page | An invariant (I1 to I5) is above 0 | RB-AUD-1 (`auditor.md`) |
| `MoneySafetyBreach` | page | Money captured over 15 minutes ago is neither confirmed nor refunded (I3) | RB-AUD-2 |
| `InventoryDrift` | page | Valkey offers units PostgreSQL does not have free, for 2 minutes | RB-AUD-5, then RB-INV-4 |
| `ReconciliationNeedsAHuman` | page | The reconciler found a difference it does not repair | RB-AUD-3 |
| `AuditorStale` | page | No auditor check for 2 minutes | RB-AUD-4 |
| `ReconcilerStale` | ticket | No reconciliation pass for 15 minutes | RB-AUD-4 |
| `ServiceDown` | page | A scrape target unreachable for a minute | below |
| `OutboxLagHigh` | page | An outbox's oldest event waited over 30 s, for a minute | below |
| `ConsumerLagHigh` | page | A consumer group over 500 messages behind for 2 minutes | below |
| `MessagesDeadLettered` | page | A message went to a dead-letter topic | RB-3 (`saga.md`) |
| `CheckoutLatencyHigh` | ticket | Hold p99 over 100 ms, or booking p99 over 300 ms, for 2 minutes | below |
| `CaptureToConfirmSlow` | ticket | Payment to confirmed booking p99 over 5 s, for 5 minutes | below |
| `PSPBreakerOpen` | page | The payment provider's circuit breaker is open | RB-5 (`payment.md`) |
| `AdmissionStalled` | page | Open, people waiting, units left, nobody admitted, and not on purpose (a full session budget, or paused for the provider) | RB-Q-5 (`queue.md`) |
| `ValkeyReplicaMissing` | ticket | Valkey's primary has had no replica for 2 minutes | RB-VK-1 (`valkey.md`) |

## `ServiceDown`

`docker compose ps <job>`; its logs; its `/readyz` on the admin port. A
service that keeps restarting usually says why in its last log lines
(configuration, a dependency at start). Valkey's nodes are scraped through
redis_exporter, so a dead Valkey node shows as `redis_up 0`, not here:
`ValkeyReplicaMissing` and the services' readiness cover it.

## `OutboxLagHigh`

Events written by booking-svc or payment-svc are not reaching Kafka, so the
saga and refunds wait. Check, in order:

1. Kafka: `docker compose ps kafka`, and that a client can talk to it
   (`kafka-broker-api-versions.sh`; experiment E4 measured a restart at
   about 50 s).
2. The relay's leader: `holdfast_outbox_relay_leader{schema}` must be 1 on
   exactly one replica; its logs (`outbox:`).
3. Once Kafka answers, the backlog drains by itself (E4: 5 s after Kafka
   came back).

## `ConsumerLagHigh`

A consumer group is behind. Either it cannot keep up (CPU, a slow
dependency), or it is waiting out a dependency on purpose: a message whose
handler fails without being permanent holds its partition for up to 10
minutes before it is dead-lettered (P56). The group's logs say which
(`kafka: handler failed; retrying`, with how long). Fix the dependency;
the lag drains by itself.

## `CheckoutLatencyHigh`

Holds or bookings are slower than the design's SLOs (design doc 2.4).
Valkey's CPU (dashboard, health row) for holds; PostgreSQL and the payment
provider for bookings (booking-svc creates the provider's order in the
request). Adaptive admission already slows the queue down when the provider
is slow (task 5.4); if it is something else, consider freezing (RB-1).

## `CaptureToConfirmSlow`

Paid bookings take more than 5 s (p99) to confirm. The saga reads
`payment.captured` from Kafka: check `ConsumerLagHigh`, `OutboxLagHigh`,
and inventory-svc (each confirmation marks the hold sold there).
