package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/breaker"
)

var testAIMD = AIMDConfig{
	LatencySLO: time.Second, MaxErrorRatio: 0.01, MinCalls: 20,
	Cooldown: time.Second, Step: 0.1, Floor: 0.05, StaleAfter: 5 * time.Second,
}

func TestClassify(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	fresh := func(p psp.Pressure) reading { return reading{p: p, at: now.Add(-time.Second), ok: true} }
	tests := []struct {
		name   string
		r      reading
		want   health
		reason string
	}{
		{"never read", reading{}, strained, backoffUnknown},
		{"stale", reading{p: psp.Pressure{Breaker: breaker.Closed}, at: now.Add(-6 * time.Second), ok: true}, strained, backoffUnknown},
		{"breaker open", fresh(psp.Pressure{Breaker: breaker.Open}), halted, backoffBreaker},
		{"breaker half-open", fresh(psp.Pressure{Breaker: breaker.HalfOpen}), probing, backoffHalfOpen},
		{"too few calls to judge", fresh(psp.Pressure{Calls: 19, Failures: 19, P99: time.Minute}), healthy, ""},
		{"errors above 1%", fresh(psp.Pressure{Calls: 100, Failures: 2, P99: 100 * time.Millisecond}), strained, backoffErrors},
		{"errors at 1%", fresh(psp.Pressure{Calls: 100, Failures: 1, P99: 100 * time.Millisecond}), healthy, ""},
		{"slow", fresh(psp.Pressure{Calls: 100, P99: 1500 * time.Millisecond}), strained, backoffLatency},
		{"at the SLO", fresh(psp.Pressure{Calls: 100, P99: time.Second}), healthy, ""},
	}
	for _, tt := range tests {
		h, reason := classify(tt.r, now, testAIMD)
		if h != tt.want || reason != tt.reason {
			t.Errorf("%s: got %v %q, want %v %q", tt.name, h, reason, tt.want, tt.reason)
		}
	}
}

func TestAIMD(t *testing.T) {
	t0 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	a := newAIMD(100, testAIMD, t0) // step 10/s, floor 5/s
	check := func(when int, h health, wantRate float64, wantBackoff bool) {
		t.Helper()
		r, b := a.next(at(when), h)
		if r != wantRate || b != wantBackoff {
			t.Fatalf("t=%dms %v: rate %v backoff %v, want %v %v", when, h, r, b, wantRate, wantBackoff)
		}
	}
	check(250, healthy, 100, false)   // starts at the ceiling, never above it
	check(500, strained, 50, true)    // multiplicative decrease
	check(750, strained, 50, false)   // within the cooldown: no second cut
	check(1500, strained, 25, true)   // the next cooldown
	check(2500, healthy, 35, false)   // additive increase, 10 a second
	check(3000, healthy, 40, false)   // pro rata
	check(3250, halted, 0, true)      // breaker open: pause at once
	check(3500, halted, 0, false)     // still paused, counted once
	check(4000, probing, 5, false)    // half-open: from the floor
	check(4500, healthy, 10, false)   // closed: climbs again
	check(14500, healthy, 100, false) // back at the ceiling, capped
	for when := 15000; when <= 30000; when += 1000 {
		r, _ := a.next(at(when), strained)
		if r < a.floor {
			t.Fatalf("rate %v under the floor %v", r, a.floor)
		}
	}
	if a.rate != 5 {
		t.Fatalf("a long strain ends at the floor, got %v", a.rate)
	}

	// A tiny configured rate: the floor and the step are at least one a second.
	small := newAIMD(2, testAIMD, t0)
	if small.floor != 1 || small.step != 1 {
		t.Fatalf("floor %v step %v, want 1 and 1", small.floor, small.step)
	}
}

type fakePressure struct {
	fail atomic.Bool
	p    psp.Pressure
}

func (f *fakePressure) GetPressure(context.Context) (psp.Pressure, error) {
	if f.fail.Load() {
		return psp.Pressure{}, context.DeadlineExceeded
	}
	return f.p, nil
}

// A failed read keeps the last good reading, which then grows stale and
// reads as unknown.
func TestPressureWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, m := syncController(time.Second)
		src := &fakePressure{p: psp.Pressure{Breaker: breaker.Closed, Calls: 30}}
		w := NewPressureWatch(src, time.Second, m, c.log)
		if h, _ := classify(w.current(), time.Now(), testAIMD); h != strained {
			t.Fatalf("before any read: %v, want strained (unknown)", h)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		synctest.Wait()
		if r := w.current(); !r.ok || r.p.Calls != 30 {
			t.Fatalf("after the first read: %+v", r)
		}
		src.fail.Store(true)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if h, reason := classify(w.current(), time.Now(), testAIMD); h != strained || reason != backoffUnknown {
			t.Fatalf("10 s of failed reads: %v %q, want strained, unknown", h, reason)
		}
		cancel()
		<-done
		if ok, failed := testutil.ToFloat64(m.pressure.WithLabelValues(pressureReadOK)), testutil.ToFloat64(m.pressure.WithLabelValues(pressureReadError)); ok != 1 || failed < 9 {
			t.Fatalf("reads: %v ok, %v failed", ok, failed)
		}
	})
}

// The leader under a fake clock: the provider's breaker opens for two
// seconds, then closes. Admissions stop at once, resume at the floor, and
// climb back.
func TestLeadPausesWhileTheBreakerIsOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, m := syncController(250 * time.Millisecond)
		w := NewPressureWatch(nil, time.Second, m, c.log)
		c.cfg.Pressure, c.cfg.AIMD = w, testAIMD
		set := func(s breaker.State) {
			w.latest.Store(&reading{p: psp.Pressure{Breaker: s}, at: time.Now(), ok: true})
		}
		set(breaker.Closed)
		f := &fakeAdvance{start: time.Now()}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.lead(ctx, 7, 100, alive, f) }()

		// Mid-tick moments, so a change never races a tick.
		time.Sleep(2*time.Second + 100*time.Millisecond)
		set(breaker.Open)
		time.Sleep(2 * time.Second)
		set(breaker.Closed)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}

		admitted := func(from, to time.Duration) int {
			n := 0
			for _, call := range f.calls {
				if call.at > from && call.at <= to {
					n += call.n
				}
			}
			return n
		}
		if n := admitted(0, 2*time.Second); n < 170 || n > 200 {
			t.Errorf("before the outage: %d admitted in 2 s at 100/s", n)
		}
		if n := admitted(2100*time.Millisecond, 4100*time.Millisecond); n != 0 {
			t.Errorf("%d admitted while the breaker was open", n)
		}
		// After: from the floor (5/s), +10/s each second: about 5+15+25.
		if n := admitted(4100*time.Millisecond, 7100*time.Millisecond); n < 25 || n > 60 {
			t.Errorf("after the outage: %d admitted in 3 s, want a slow climb", n)
		}
		if got := testutil.ToFloat64(m.backoffs.WithLabelValues(backoffBreaker)); got != 1 {
			t.Errorf("breaker back-offs counted %v times, want 1", got)
		}
		if got := testutil.ToFloat64(m.rate.WithLabelValues(syncEvent)); got <= 5 || got >= 100 {
			t.Errorf("rate gauge %v, want climbing between the floor and the ceiling", got)
		}
	})
}
