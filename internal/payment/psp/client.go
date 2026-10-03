// Package psp is payment-svc's client for the payment service provider
// (mockpsp locally) and the shape of the provider's webhooks.
//
// The provider's API (implemented by cmd/mockpsp):
//
//	POST /v1/orders          create an order; Idempotency-Key = the intent ID
//	GET  /v1/orders/{id}     an order's status
//	POST /v1/refunds         refund a payment; Idempotency-Key = the intent ID
//
// Every call is idempotent on the provider's side, so the client retries
// network errors, timeouts, 429 and 5xx with exponential backoff and jitter,
// behind a circuit breaker that fails fast while the provider is down.
package psp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/breaker"
)

// Order statuses at the provider.
const (
	OrderCreated  = "CREATED"
	OrderCaptured = "CAPTURED"
	OrderFailed   = "FAILED"
	OrderExpired  = "EXPIRED"
)

// ErrUnavailable means the provider could not be reached (or answered 5xx)
// after the retries, or the breaker is open. Retry later.
var ErrUnavailable = errors.New("psp: unavailable")

// ErrRejected means the provider refused the request (4xx): retrying the same
// request will not help.
var ErrRejected = errors.New("psp: request rejected")

// Order is the provider's view of an order.
type Order struct {
	OrderID       string    `json:"orderId"`
	CheckoutURL   string    `json:"checkoutUrl"`
	Status        string    `json:"status"`
	AmountPaise   int64     `json:"amountPaise"`
	ExpiresAt     time.Time `json:"expiresAt"`
	PaymentID     string    `json:"paymentId,omitempty"`
	FailureReason string    `json:"failureReason,omitempty"`
}

// CreateOrder is the request to open an order.
type CreateOrder struct {
	AmountPaise int64     `json:"amountPaise"`
	Currency    string    `json:"currency"`
	ExpiresAt   time.Time `json:"expiresAt"`
	Reference   string    `json:"reference"` // the intent ID
}

// Refund is the provider's view of a refund.
type Refund struct {
	RefundID    string `json:"refundId"`
	PaymentID   string `json:"paymentId"`
	AmountPaise int64  `json:"amountPaise"`
	Status      string `json:"status"` // PENDING or COMPLETED; completion also arrives as a webhook
}

// Config configures the client.
type Config struct {
	BaseURL string
	APIKey  string
	// Timeout bounds one attempt (default 2 s); the caller's context bounds
	// the whole call, retries included.
	Timeout     time.Duration
	MaxAttempts int // default 3
	// Breaker: open after BreakerThreshold consecutive failed calls, for
	// BreakerCooldown (defaults 5 and 30 s).
	BreakerThreshold int
	BreakerCooldown  time.Duration
}

// Client calls the provider.
type Client struct {
	cfg     Config
	http    *http.Client
	breaker *breaker.Breaker
	m       *Metrics
	sleep   func(context.Context, time.Duration) error
}

// New returns a client.
func New(cfg Config, m *Metrics) (*Client, error) {
	if _, err := url.ParseRequestURI(cfg.BaseURL); err != nil || cfg.BaseURL == "" {
		return nil, fmt.Errorf("psp: invalid base URL %q", cfg.BaseURL)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 3
	}
	if cfg.BreakerThreshold < 1 {
		cfg.BreakerThreshold = 5
	}
	if cfg.BreakerCooldown <= 0 {
		cfg.BreakerCooldown = 30 * time.Second
	}
	c := &Client{
		cfg:  cfg,
		http: &http.Client{Timeout: cfg.Timeout, Transport: otelhttp.NewTransport(http.DefaultTransport)},
		m:    m,
		sleep: func(ctx context.Context, d time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
				return nil
			}
		},
	}
	c.breaker = breaker.New(cfg.BreakerThreshold, cfg.BreakerCooldown, func(s breaker.State) {
		if m != nil {
			m.breakerOpen.Set(map[breaker.State]float64{breaker.Closed: 0, breaker.HalfOpen: 0.5, breaker.Open: 1}[s])
		}
	})
	return c, nil
}

// CreateOrder opens an order for an intent. The intent ID is the idempotency
// key: calling it again returns the same order.
func (c *Client) CreateOrder(ctx context.Context, intentID string, req CreateOrder) (Order, error) {
	var o Order
	err := c.do(ctx, "create_order", http.MethodPost, "/v1/orders", intentID, req, &o)
	return o, err
}

// GetOrder reads an order's status.
func (c *Client) GetOrder(ctx context.Context, orderID string) (Order, error) {
	var o Order
	err := c.do(ctx, "get_order", http.MethodGet, "/v1/orders/"+url.PathEscape(orderID), "", nil, &o)
	return o, err
}

// CreateRefund refunds a captured payment in full. The intent ID is the
// idempotency key: one refund per intent, however often it is requested.
func (c *Client) CreateRefund(ctx context.Context, intentID, paymentID string, amountPaise int64) (Refund, error) {
	var r Refund
	err := c.do(ctx, "create_refund", http.MethodPost, "/v1/refunds", intentID,
		map[string]any{"paymentId": paymentID, "amountPaise": amountPaise}, &r)
	return r, err
}

// do runs one call through the breaker, retrying what may succeed later.
func (c *Client) do(ctx context.Context, op, method, path, idemKey string, body, out any) error {
	if err := c.breaker.Allow(); err != nil {
		c.m.request(op, "breaker_open")
		return fmt.Errorf("%w: circuit breaker open", ErrUnavailable)
	}
	err := c.retry(ctx, op, method, path, idemKey, body, out)
	c.breaker.Done(errors.Is(err, ErrUnavailable) && ctx.Err() == nil) // the caller giving up is not the provider failing
	return err
}

func (c *Client) retry(ctx context.Context, op, method, path, idemKey string, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	var last error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		retryable, err := c.once(ctx, method, path, idemKey, payload, out)
		if err == nil {
			c.m.request(op, "ok")
			return nil
		}
		last = err
		if !retryable {
			c.m.request(op, "rejected")
			return err
		}
		if attempt == c.cfg.MaxAttempts || ctx.Err() != nil {
			break
		}
		c.m.request(op, "retried")
		backoff := 100 * time.Millisecond << (attempt - 1)
		if err := c.sleep(ctx, backoff/2+rand.N(backoff/2+1)); err != nil { //nolint:gosec // jitter
			break
		}
	}
	c.m.request(op, "unavailable")
	return fmt.Errorf("%w: %w", ErrUnavailable, last)
}

// once makes one attempt. retryable is true for failures a later attempt may
// not hit: network errors, timeouts, 429 and 5xx.
func (c *Client) once(ctx context.Context, method, path, idemKey string, payload []byte, out any) (retryable bool, err error) {
	var rd io.Reader
	if payload != nil {
		rd = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.BaseURL+path, rd)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return true, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return true, fmt.Errorf("psp: %s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(data))
	case resp.StatusCode >= 400:
		return false, fmt.Errorf("%w: %s %s: %d %s", ErrRejected, method, path, resp.StatusCode, bytes.TrimSpace(data))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return true, fmt.Errorf("psp: decode %s %s: %w", method, path, err)
	}
	return false, nil
}

// Metrics count calls to the provider.
type Metrics struct {
	requests    *prometheus.CounterVec
	breakerOpen prometheus.Gauge
}

// NewMetrics registers the provider metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_psp_requests_total",
			Help: "Calls to the payment provider by operation and result: ok, retried (one failed attempt), rejected (4xx), unavailable, breaker_open.",
		}, []string{"op", "result"}),
		breakerOpen: f.NewGauge(prometheus.GaugeOpts{
			Name: "holdfast_psp_breaker_state",
			Help: "The provider's circuit breaker: 0 closed, 0.5 half-open, 1 open.",
		}),
	}
}

func (m *Metrics) request(op, result string) {
	if m != nil {
		m.requests.WithLabelValues(op, result).Inc()
	}
}
