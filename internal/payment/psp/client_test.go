package psp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// provider serves the PSP API from a list of status codes, one per request
// (the last repeats), counting requests.
type provider struct {
	mu    sync.Mutex
	codes []int
	calls atomic.Int32
	last  atomic.Pointer[http.Request]
}

func (p *provider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := int(p.calls.Add(1))
	p.last.Store(r)
	p.mu.Lock()
	code := p.codes[min(n, len(p.codes))-1]
	p.mu.Unlock()
	if code != http.StatusOK {
		http.Error(w, `{"error":"nope"}`, code)
		return
	}
	_ = json.NewEncoder(w).Encode(Order{OrderID: "order_1", CheckoutURL: "https://psp.test/pay/order_1", Status: OrderCreated, AmountPaise: 5000})
}

func newClient(t *testing.T, codes ...int) (*Client, *provider, *Metrics) {
	t.Helper()
	p := &provider{codes: codes}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	m := NewMetrics(prometheus.NewRegistry())
	c, err := New(Config{BaseURL: srv.URL, APIKey: "test-key", BreakerThreshold: 2, BreakerCooldown: time.Hour}, m)
	if err != nil {
		t.Fatal(err)
	}
	c.sleep = func(context.Context, time.Duration) error { return nil }
	return c, p, m
}

func TestCreateOrderSendsTheIdempotencyKey(t *testing.T) {
	c, p, m := newClient(t, http.StatusOK)
	o, err := c.CreateOrder(context.Background(), "intent-1", CreateOrder{AmountPaise: 5000, Currency: "INR", Reference: "intent-1"})
	if err != nil || o.OrderID != "order_1" || o.CheckoutURL == "" {
		t.Fatalf("CreateOrder = %+v, %v", o, err)
	}
	r := p.last.Load()
	if r.Header.Get("Idempotency-Key") != "intent-1" || r.Header.Get("Authorization") != "Bearer test-key" || r.URL.Path != "/v1/orders" {
		t.Fatalf("request %s %s %v", r.Method, r.URL.Path, r.Header)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("create_order", "ok")); got != 1 {
		t.Fatalf("ok requests %v", got)
	}
}

func TestRetriesTransientFailures(t *testing.T) {
	c, p, m := newClient(t, http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusOK)
	if _, err := c.GetOrder(context.Background(), "order_1"); err != nil {
		t.Fatalf("GetOrder after two transient failures: %v", err)
	}
	if n := p.calls.Load(); n != 3 {
		t.Fatalf("%d attempts, want 3", n)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("get_order", "retried")); got != 2 {
		t.Fatalf("retried %v, want 2", got)
	}
}

func TestDoesNotRetryRejections(t *testing.T) {
	c, p, _ := newClient(t, http.StatusUnprocessableEntity)
	_, err := c.CreateOrder(context.Background(), "intent-1", CreateOrder{AmountPaise: 5000})
	if !errors.Is(err, ErrRejected) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err %v, want ErrRejected", err)
	}
	if n := p.calls.Load(); n != 1 {
		t.Fatalf("%d attempts for a 4xx, want 1", n)
	}
	// Rejections are the provider working: they never open the breaker.
	for range 3 {
		_, _ = c.CreateOrder(context.Background(), "intent-1", CreateOrder{AmountPaise: 5000})
	}
	if n := p.calls.Load(); n != 4 {
		t.Fatalf("%d calls reached the provider, want 4", n)
	}
}

func TestBreakerOpensWhileTheProviderIsDown(t *testing.T) {
	c, p, m := newClient(t, http.StatusBadGateway)
	for range 2 { // threshold 2, three attempts each
		if _, err := c.GetOrder(context.Background(), "order_1"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("err %v, want ErrUnavailable", err)
		}
	}
	if n := p.calls.Load(); n != 6 {
		t.Fatalf("%d attempts, want 6", n)
	}
	_, err := c.GetOrder(context.Background(), "order_1")
	if !errors.Is(err, ErrUnavailable) || p.calls.Load() != 6 {
		t.Fatalf("with the breaker open: %v after %d calls; want a fast failure", err, p.calls.Load())
	}
	if got := testutil.ToFloat64(m.breakerOpen); got != 1 {
		t.Fatalf("breaker gauge %v, want 1", got)
	}
}

func TestCallerCancellationDoesNotOpenTheBreaker(t *testing.T) {
	c, p, _ := newClient(t, http.StatusServiceUnavailable)
	for range 5 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _ = c.GetOrder(ctx, "order_1")
	}
	p.mu.Lock()
	p.codes = []int{http.StatusOK}
	p.mu.Unlock()
	if _, err := c.GetOrder(context.Background(), "order_1"); err != nil {
		t.Fatalf("after cancelled calls: %v; the breaker should still be closed", err)
	}
}

func TestNewRejectsABadBaseURL(t *testing.T) {
	for _, u := range []string{"", "not a url"} {
		if _, err := New(Config{BaseURL: u}, nil); err == nil {
			t.Fatalf("base URL %q accepted", u)
		}
	}
}
