package psp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/breaker"
)

func TestWindowPercentileAndExpiry(t *testing.T) {
	var w window
	t0 := time.Unix(1_791_000_000, 0)
	for range 98 {
		w.observe(t0, 40*time.Millisecond, false)
	}
	w.observe(t0.Add(500*time.Millisecond), 900*time.Millisecond, true)
	w.observe(t0.Add(3*time.Second), 900*time.Millisecond, false)

	calls, failures, p99 := w.snapshot(t0.Add(5 * time.Second))
	if calls != 100 || failures != 1 || p99 != time.Second {
		t.Fatalf("snapshot = %d calls, %d failures, p99 %v; want 100, 1, 1s", calls, failures, p99)
	}
	// The first second leaves the window after 10 s; the call at t0+3s stays.
	calls, _, p99 = w.snapshot(t0.Add(10 * time.Second))
	if calls != 1 || p99 != time.Second {
		t.Fatalf("after 10 s: %d calls, p99 %v; want 1, 1s", calls, p99)
	}
	// A slot is reused when its second comes round: t0+10s shares t0's slot.
	w.observe(t0.Add(10*time.Second), 20*time.Millisecond, false)
	calls, _, p99 = w.snapshot(t0.Add(10 * time.Second))
	if calls != 2 || p99 != time.Second {
		t.Fatalf("after reuse: %d calls, p99 %v; want 2, 1s", calls, p99)
	}
	// Slower than every bound: reported as twice the last one.
	var slow window
	slow.observe(t0, time.Minute, false)
	if _, _, p99 := slow.snapshot(t0); p99 != 20*time.Second {
		t.Fatalf("overflow p99 %v, want 20s", p99)
	}
	var empty window
	if c, f, p := empty.snapshot(t0); c != 0 || f != 0 || p != 0 {
		t.Fatalf("empty window: %d %d %v", c, f, p)
	}
}

func TestPressureReportsFailuresAndTheBreaker(t *testing.T) {
	c, _, _ := newClient(t, http.StatusInternalServerError) // down; the breaker opens after 2 calls
	ctx := context.Background()
	for range 3 {
		if _, err := c.CreateOrder(ctx, "intent-1", CreateOrder{AmountPaise: 100}); err == nil {
			t.Fatal("a call to a provider that is down succeeded")
		}
	}
	p := c.Pressure()
	// The third call failed fast on the open breaker: it never reached the
	// provider, so it is not one of the window's calls.
	if p.Breaker != breaker.Open || p.Calls != 2 || p.Failures != 2 || p.Window != PressureWindow {
		t.Fatalf("pressure %+v, want open with 2 calls and 2 failures", p)
	}

	ok, _, _ := newClient(t, http.StatusOK)
	if _, err := ok.CreateOrder(ctx, "intent-2", CreateOrder{AmountPaise: 100}); err != nil {
		t.Fatal(err)
	}
	if p := ok.Pressure(); p.Breaker != breaker.Closed || p.Calls != 1 || p.Failures != 0 || p.P99 == 0 {
		t.Fatalf("pressure %+v, want closed with 1 call and a p99", p)
	}
}

// With admissions paused nothing calls the provider; the prober's trial call
// is what closes the breaker once the provider is back.
func TestTheProberClosesTheBreakerWhenTheProviderRecovers(t *testing.T) {
	p := &provider{codes: []int{500, 500, 500, 500, 500, 500, http.StatusOK}} // 2 calls of 3 attempts fail, then up
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	m := NewMetrics(prometheus.NewRegistry())
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k", BreakerThreshold: 2, BreakerCooldown: 20 * time.Millisecond}, m)
	if err != nil {
		t.Fatal(err)
	}
	c.sleep = func(context.Context, time.Duration) error { return nil }
	ctx := context.Background()
	for range 2 {
		_, _ = c.GetOrder(ctx, "order_1")
	}
	if s := c.breaker.State(); s != breaker.Open {
		t.Fatalf("breaker %v, want open", s)
	}

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	go func() { _ = NewProber(c, 10*time.Millisecond).Run(runCtx) }()
	for c.breaker.State() != breaker.Closed {
		if runCtx.Err() != nil {
			t.Fatalf("the breaker is still %v after 2 s of probing", c.breaker.State())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("probe", "ok")); got < 1 {
		t.Fatalf("probe ok counted %v times", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("create_order", "ok")); got != 0 {
		t.Fatalf("a pre-registered series is %v, want 0", got)
	}
}
