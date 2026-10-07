# Runbooks

What to do when something goes wrong, and how long it takes. The design's
five operational runbooks (design doc 13.5) live in the service runbooks
next to them; every one has been practised against the running stack, most
during the chaos experiments (task 5.7; `loadtest/results/README.md`).

| Runbook | When | Where | Practised | Measured |
|---|---|---|---|---|
| **RB-1** Freeze the sale | Anything looks wrong during a live sale | `sale.md` | E4, after a Valkey failover mid-sale | `freeze` 0.1 s; `unfreeze` 0.1 to 0.2 s |
| **RB-2** Rebuild inventory from PostgreSQL | After a Valkey failover or data loss, or `InventoryDrift` | `inventory.md` (RB-INV-4) | E4, mid-sale, twice; three drifted dev events (task 5.3) | Freeze, dry run, rebuild, unfreeze: 2.0 and 2.3 s for a 12,000-buyer sale |
| **RB-3** Drain a dead-letter topic | `MessagesDeadLettered` | `saga.md` | A real incident: E4's 29 dead-lettered payments (P56) | 29 messages replayed in 3.7 s; I3 and drift back to 0 within 15 s |
| **RB-4** Manual refund | A customer charged and not refunded; a refund given up | `saga.md` | `make drill-rb4`: a refund the provider's outage defeated | `holdfastctl refund` to `REFUNDED` in 30 s (the breaker's cooldown after the outage); the buyer's money back 11 minutes after the capture, inside I3's 15 |
| **RB-5** Payment provider outage | `PSPBreakerOpen` | `payment.md` | `make drill-rb5`: the provider down for 60 s mid-sale | Automatic: admissions paused 1.9 s after the outage began; resumed 7.1 s after the provider was back, and at full rate 26 s after |

Every alert says which runbook it needs: `alerts.md`. The service
runbooks, by service: `inventory.md` (RB-INV), `queue.md` (RB-Q),
`saga.md` (booking-svc's saga and refunds), `payment.md`, `auditor.md`
(RB-AUD, the correctness alerts), `valkey.md` (RB-VK, failover) and
`sale.md`.

## The drills

```bash
make drill-valkey MODE=crash   # Valkey failover (RB-VK-4)
make chaos-e4                  # RB-1 and RB-2 mid-sale, with infrastructure faults
make drill-rb5                 # the provider down for 60 s mid-sale
make drill-rb4                 # a refund the provider's outage defeated, made by hand
```

Each starts from a stack brought up with `make up`, records its timeline in
`loadtest/results/`, ends by checking every invariant, and restores the
normal stack.
