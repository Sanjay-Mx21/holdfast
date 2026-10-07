package psp

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/breaker"
)

// Pressure is how the provider is coping (task 5.4): the breaker's state and
// the calls of the last PressureWindow. queue-svc's admission leaders read it
// through payment-svc's GetPressure and slow down, or pause, accordingly.
type Pressure struct {
	Breaker breaker.State
	// Calls completed in the window, and those that failed (unreachable,
	// timed out or 5xx after the retries; a 4xx is an answer).
	Calls, Failures int64
	// P99 of the calls' durations, retries included: the upper bound of the
	// histogram bucket it falls in. Zero without calls.
	P99    time.Duration
	Window time.Duration
}

// PressureWindow is how far back Pressure looks: long enough to hold a
// meaningful number of calls, short enough to react within seconds.
const PressureWindow = 10 * time.Second

// latencyBounds are the histogram's bucket upper bounds; a call slower than
// the last falls in an overflow bucket reported as twice the last bound.
var latencyBounds = []time.Duration{
	25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond,
	300 * time.Millisecond, 500 * time.Millisecond, 750 * time.Millisecond, time.Second,
	1500 * time.Millisecond, 2 * time.Second, 3 * time.Second, 5 * time.Second, 10 * time.Second,
}

// window keeps one slot per second of PressureWindow, each a count, a
// failure count and a latency histogram. A slot is reused when its second
// comes round again.
type window struct {
	mu    sync.Mutex
	slots [int(PressureWindow / time.Second)]slot
}

type slot struct {
	second          int64 // the Unix second this slot counts
	calls, failures int64
	hist            [14]int64 // len(latencyBounds) + overflow
}

func (w *window) observe(now time.Time, d time.Duration, failed bool) {
	sec := now.Unix()
	w.mu.Lock()
	defer w.mu.Unlock()
	s := &w.slots[sec%int64(len(w.slots))]
	if s.second != sec {
		*s = slot{second: sec}
	}
	s.calls++
	if failed {
		s.failures++
	}
	b := len(latencyBounds)
	for i, bound := range latencyBounds {
		if d <= bound {
			b = i
			break
		}
	}
	s.hist[b]++
}

// snapshot sums the slots within the window ending at now.
func (w *window) snapshot(now time.Time) (calls, failures int64, p99 time.Duration) {
	oldest := now.Unix() - int64(len(w.slots)) + 1
	var hist [14]int64
	w.mu.Lock()
	for _, s := range w.slots {
		if s.second < oldest || s.second > now.Unix() {
			continue
		}
		calls += s.calls
		failures += s.failures
		for i, n := range s.hist {
			hist[i] += n
		}
	}
	w.mu.Unlock()
	if calls == 0 {
		return 0, 0, 0
	}
	rank := (calls*99 + 99) / 100 // the ceiling of 99% of the calls
	var seen int64
	for i, n := range hist {
		seen += n
		if seen >= rank {
			if i == len(latencyBounds) {
				return calls, failures, 2 * latencyBounds[len(latencyBounds)-1]
			}
			return calls, failures, latencyBounds[i]
		}
	}
	return calls, failures, 2 * latencyBounds[len(latencyBounds)-1]
}

// Pressure reports the breaker's state and the window's calls.
func (c *Client) Pressure() Pressure {
	calls, failures, p99 := c.window.snapshot(c.now())
	return Pressure{Breaker: c.breaker.State(), Calls: calls, Failures: failures, P99: p99, Window: PressureWindow}
}

// Probe makes one cheap call, an empty settlement report, through the
// breaker. While admissions are paused for an open breaker no checkout
// calls the provider, so without a probe nothing would ever try the
// half-open call that closes it again (task 5.4). Under the breaker's
// cooldown it returns at once without calling.
func (c *Client) Probe(ctx context.Context) error {
	now := c.now().UTC()
	q := url.Values{"from": {now.Add(-time.Second).Format(time.RFC3339Nano)}, "to": {now.Format(time.RFC3339Nano)}, "limit": {"1"}}
	var s Settlement
	return c.do(ctx, "probe", http.MethodGet, "/v1/settlements?"+q.Encode(), "", nil, &s)
}

// Prober probes the provider while the breaker is not closed (an app
// component in payment-svc).
type Prober struct {
	c        *Client
	interval time.Duration
}

// NewProber probes through c every interval.
func NewProber(c *Client, interval time.Duration) *Prober {
	return &Prober{c: c, interval: interval}
}

// Name implements app.Component.
func (p *Prober) Name() string { return "psp-prober" }

// Run implements app.Component.
func (p *Prober) Run(ctx context.Context) error {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if p.c.breaker.State() != breaker.Closed {
				callCtx, cancel := context.WithTimeout(ctx, p.interval)
				_ = p.c.Probe(callCtx) // the outcome moves the breaker; nothing else to do
				cancel()
			}
		}
	}
}
