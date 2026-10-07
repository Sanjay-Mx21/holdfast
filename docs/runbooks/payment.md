# Runbook: payment-svc and the payment provider

Background: `docs/services/payment.md`. The provider is mockpsp locally
(`docs/services/mockpsp.md`), whose admin port (9095) injects the faults
these entries describe.

## RB-5 The payment provider is down

**Alert:** `PSPBreakerOpen` (page). **Symptoms:** create-booking returns
503 (`PSP_UNAVAILABLE`); `holdfast_psp_breaker_state` is 1;
`holdfast_queue_admission_rate` is 0 for every event on sale; the
mission-control dashboard's admission tile shows "allowed 0".

**What happens by itself** (tasks 3.9 and 5.4):

1. After 5 failed provider calls in a row, payment-svc's circuit breaker
   opens: calls fail at once instead of piling up behind timeouts.
2. Within a second, every admission leader reads the open breaker from
   payment-svc (`GetPressure`) and stops admitting: nobody new is sent
   into a checkout that cannot be paid. The waiting room keeps everyone's
   place and the status document stays fresh.
3. Every 5 s payment-svc's prober tries the provider. After the breaker's
   30 s cooldown, a successful trial closes the breaker.
4. Admissions resume at 5% of the event's rate and climb by 5% a second
   (adaptive admission), so the provider is not hit by the whole backlog
   at once.

Buyers already in checkout keep their holds and sessions; a booking whose
payment cannot be taken before its deadline is cancelled and its units go
back, and a payment captured late is refunded (I3).

**Do:**

- Watch it recover: the breaker state, then the admission rate climbing.
  Nothing else is needed if the provider comes back.
- If the outage lasts, and the sale's deadline matters more than its
  fairness, freeze it (RB-1) and tell buyers; unfreeze when the provider is
  back.
- If the breaker stays open while the provider answers (its status page,
  a manual call), check payment-svc's prober in the logs
  (`holdfast_psp_requests_total{op="probe"}`): it should be counting.
- Afterwards, the reconciler (every 5 minutes) applies any capture whose
  webhook the outage lost; `ReconciliationNeedsAHuman` reports what it
  cannot.

**Practised** with `make drill-rb5` (task 5.7): the provider down for 60 s
with 4,000 buyers at 20 admissions a second. The breaker opened 1.5 s into
the outage and admissions paused at 1.9 s; the provider was back at 60.3 s,
the breaker closed at 66.1 s, admissions resumed at 67.4 s and were back at
the full rate at 86.2 s. Every capture was resolved and every invariant
stayed 0 (`loadtest/results/README.md`).

**By hand:** with the stack up and a sale running, switch mockpsp's outage
on and off:

```bash
curl -X PUT localhost:9095/internal/v1/faults -H "Authorization: Bearer $ADMIN_TOKEN" -d '{"outage":true}'
curl -X PUT localhost:9095/internal/v1/faults -H "Authorization: Bearer $ADMIN_TOKEN" -d '{}'
```
