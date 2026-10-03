package mockpsp

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

// DispatcherConfig configures webhook delivery.
type DispatcherConfig struct {
	URL    string // where webhooks go (payment-svc's /v1/webhooks/psp)
	Secret []byte // signing secret, shared with the receiver
	// MaxAttempts per delivery (default 8); a non-2xx answer or a network
	// error is retried after RetryBase, doubling up to RetryMax (defaults
	// 1 s and 30 s), as real providers do.
	MaxAttempts int
	RetryBase   time.Duration
	RetryMax    time.Duration
	Timeout     time.Duration // per attempt (default 5 s)
	Concurrency int           // deliveries in flight (default 32)
}

// Dispatcher delivers signed webhooks, applying the injected faults.
type Dispatcher struct {
	cfg    DispatcherConfig
	faults *Injector
	m      *Metrics
	log    *slog.Logger
	http   *http.Client
	slots  chan struct{}
	gen    atomic.Uint64 // bumped by Reset: older deliveries are abandoned
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	now    func() time.Time
}

// NewDispatcher returns a dispatcher. It sends nothing until Run starts.
func NewDispatcher(cfg DispatcherConfig, faults *Injector, m *Metrics, log *slog.Logger) *Dispatcher {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 8
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = time.Second
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 30 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 32
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		cfg: cfg, faults: faults, m: m, log: log, http: &http.Client{Timeout: cfg.Timeout},
		slots: make(chan struct{}, cfg.Concurrency), ctx: ctx, cancel: cancel, now: time.Now,
	}
}

// Name implements app.Component.
func (d *Dispatcher) Name() string { return "webhook-dispatcher" }

// Run implements app.Component: deliveries run until ctx ends.
func (d *Dispatcher) Run(ctx context.Context) error {
	<-ctx.Done()
	d.cancel()
	d.wg.Wait()
	return nil
}

// Reset abandons every delivery in flight or waiting.
func (d *Dispatcher) Reset() { d.gen.Add(1) }

// Send queues one webhook. The event ID is fixed now, so retries and
// injected duplicates carry the same ID, as a real provider's would.
func (d *Dispatcher) Send(wh psp.Webhook) {
	wh.ID = "evt_" + uuid.NewString()
	if wh.CreatedAt.IsZero() {
		wh.CreatedAt = d.now().UTC()
	}
	body, err := json.Marshal(wh)
	if err != nil {
		d.log.Error("mockpsp: encode webhook", "err", err)
		return
	}
	if d.faults.Lose() {
		d.m.webhook(wh.Type, "dropped")
		d.log.Info("mockpsp: webhook dropped (loss fault)", "event_id", wh.ID, "type", wh.Type, "order_id", wh.OrderID)
		return
	}
	copies := 1
	if d.faults.Duplicate() {
		copies = 2
		d.m.webhook(wh.Type, "duplicated")
	}
	delay := d.faults.Delay()
	if delay > 0 {
		d.m.webhook(wh.Type, "delayed")
	}
	gen := d.gen.Load()
	for i := range copies {
		d.wg.Add(1)
		go d.deliver(gen, wh, body, delay+time.Duration(i)*50*time.Millisecond)
	}
}

func (d *Dispatcher) deliver(gen uint64, wh psp.Webhook, body []byte, delay time.Duration) {
	defer d.wg.Done()
	if !d.wait(delay) {
		return
	}
	backoff := d.cfg.RetryBase
	for attempt := 1; attempt <= d.cfg.MaxAttempts; attempt++ {
		if d.gen.Load() != gen {
			return
		}
		if d.post(body) {
			d.m.webhook(wh.Type, "delivered")
			return
		}
		if attempt == d.cfg.MaxAttempts {
			break
		}
		d.m.webhook(wh.Type, "retried")
		if !d.wait(backoff) {
			return
		}
		backoff = min(2*backoff, d.cfg.RetryMax)
	}
	d.m.webhook(wh.Type, "gave_up")
	d.log.Warn("mockpsp: webhook not delivered; giving up", "event_id", wh.ID, "type", wh.Type, "attempts", d.cfg.MaxAttempts)
}

// wait sleeps for t, or returns false if the dispatcher is stopping.
func (d *Dispatcher) wait(t time.Duration) bool {
	if t <= 0 {
		return d.ctx.Err() == nil
	}
	timer := time.NewTimer(t)
	defer timer.Stop()
	select {
	case <-d.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// post makes one signed attempt, with a fresh timestamp, and reports
// whether the receiver answered 2xx.
func (d *Dispatcher) post(body []byte) bool {
	select {
	case d.slots <- struct{}{}:
	case <-d.ctx.Done():
		return false
	}
	defer func() { <-d.slots }()
	ts := strconv.FormatInt(d.now().Unix(), 10)
	req, err := http.NewRequestWithContext(d.ctx, http.MethodPost, d.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(psp.HeaderTimestamp, ts)
	req.Header.Set(psp.HeaderSignature, psp.Sign(d.cfg.Secret, ts, body))
	resp, err := d.http.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
