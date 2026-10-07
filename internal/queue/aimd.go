package queue

import (
	"context"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/breaker"
)

// Adaptive admission (task 5.4, design doc 7.3): like TCP congestion
// control, the leader raises its admission rate a little at a time while the
// purchase path is healthy and halves it when it is not, so it settles on
// what the downstream can take instead of trusting a guessed number. The
// event's configured admission rate is the ceiling. The signal is the
// payment provider's pressure, read from payment-svc: the provider is the
// slowest and least controllable step of a purchase, and its circuit breaker
// opening means no purchase can complete, so admissions pause (backpressure).

// AIMDConfig tunes adaptive admission.
type AIMDConfig struct {
	// LatencySLO is the provider's p99 above which admissions back off.
	LatencySLO time.Duration
	// MaxErrorRatio is the share of failed provider calls above which
	// admissions back off (design: 1%).
	MaxErrorRatio float64
	// MinCalls is how many calls the window must hold before its latency or
	// error ratio means anything.
	MinCalls int64
	// Cooldown is the least time between two halvings: the pressure window
	// needs that long to show the effect of the last one.
	Cooldown time.Duration
	// Step is the additive increase per second while healthy, and Floor the
	// lowest rate while strained, each as a fraction of the configured rate
	// (and never below one admission a second).
	Step, Floor float64
	// StaleAfter is the age at which a pressure reading counts as unknown.
	StaleAfter time.Duration
}

// health is what the pressure says about the purchase path.
type health int

const (
	healthy  health = iota // raise the rate
	strained               // halve it (at most once per cooldown)
	probing                // the breaker is half-open: hold at the floor
	halted                 // the breaker is open: admit nobody
)

// Back-off reasons, the values of holdfast_queue_admission_backoffs_total's
// reason label.
const (
	backoffLatency  = "latency"
	backoffErrors   = "errors"
	backoffUnknown  = "unknown" // no recent pressure reading
	backoffBreaker  = "breaker_open"
	backoffHalfOpen = "breaker_half_open"
)

// reading is the latest pressure, or none (ok false) if it could not be read.
type reading struct {
	p  psp.Pressure
	at time.Time
	ok bool
}

// classify turns a reading into health, with the reason when not healthy.
func classify(r reading, now time.Time, cfg AIMDConfig) (health, string) {
	if !r.ok || now.Sub(r.at) > cfg.StaleAfter {
		return strained, backoffUnknown
	}
	switch r.p.Breaker {
	case breaker.Open:
		return halted, backoffBreaker
	case breaker.HalfOpen:
		return probing, backoffHalfOpen
	}
	if r.p.Calls < cfg.MinCalls {
		return healthy, "" // too few calls to judge: no news is good news
	}
	if float64(r.p.Failures)/float64(r.p.Calls) > cfg.MaxErrorRatio {
		return strained, backoffErrors
	}
	if r.p.P99 > cfg.LatencySLO {
		return strained, backoffLatency
	}
	return healthy, ""
}

// aimd is one leader's rate controller. It starts at the configured rate.
type aimd struct {
	cfg           AIMDConfig
	ceiling       float64
	step, floor   float64
	rate          float64
	last, lastCut time.Time
	paused        bool
}

func newAIMD(ceiling float64, cfg AIMDConfig, now time.Time) *aimd {
	return &aimd{
		cfg: cfg, ceiling: ceiling, rate: ceiling, last: now,
		step:  math.Max(1, ceiling*cfg.Step),
		floor: math.Min(ceiling, math.Max(1, ceiling*cfg.Floor)),
	}
}

// next applies one tick's health and returns the rate to admit at, and
// whether this tick backed off (halved, or paused).
func (a *aimd) next(now time.Time, h health) (rate float64, backedOff bool) {
	dt := now.Sub(a.last).Seconds()
	a.last = now
	if h != halted && a.paused {
		// The breaker has closed or half-opened: start again from the floor
		// and climb, like TCP's slow start after a timeout.
		a.paused, a.rate = false, a.floor
	}
	switch h {
	case halted:
		backedOff = !a.paused
		a.paused, a.rate = true, 0
	case probing:
		a.rate = a.floor
	case strained:
		if now.Sub(a.lastCut) >= a.cfg.Cooldown {
			a.rate = math.Max(a.floor, a.rate/2)
			a.lastCut, backedOff = now, true
		}
	case healthy:
		a.rate = math.Min(a.ceiling, a.rate+a.step*dt)
	}
	return a.rate, backedOff
}

// PressureReader reads the provider's pressure (*payment.Client).
type PressureReader interface {
	GetPressure(ctx context.Context) (psp.Pressure, error)
}

// PressureWatch reads the provider's pressure from payment-svc every
// Interval, once per queue-svc replica, for all of its admission leaders (an
// app component).
type PressureWatch struct {
	r        PressureReader
	interval time.Duration
	latest   atomic.Pointer[reading]
	m        *Metrics
	log      *slog.Logger
}

// NewPressureWatch reads through r every interval.
func NewPressureWatch(r PressureReader, interval time.Duration, m *Metrics, log *slog.Logger) *PressureWatch {
	w := &PressureWatch{r: r, interval: interval, m: m, log: log}
	w.latest.Store(&reading{})
	return w
}

// Name implements app.Component.
func (w *PressureWatch) Name() string { return "payment-pressure" }

// Run implements app.Component.
func (w *PressureWatch) Run(ctx context.Context) error {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	failing := false
	for {
		readCtx, cancel := context.WithTimeout(ctx, w.interval)
		p, err := w.r.GetPressure(readCtx)
		cancel()
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			w.m.pressureRead(pressureReadError)
			if !failing {
				w.log.Warn("admission: cannot read the payment provider's pressure; admissions back off until it can", "err", err)
			}
			failing = true // the last good reading stays, and grows stale
		default:
			w.m.pressureRead(pressureReadOK)
			if failing {
				w.log.Info("admission: the payment provider's pressure is readable again")
			}
			failing = false
			w.latest.Store(&reading{p: p, at: time.Now(), ok: true})
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (w *PressureWatch) current() reading { return *w.latest.Load() }
