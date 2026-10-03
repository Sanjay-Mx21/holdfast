package mockpsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Faults are the failures mockpsp injects. Rates are probabilities from 0
// to 1, drawn independently for each webhook, payment or API call. The zero
// value injects nothing. Design doc E3 runs with duplicate 0.2, delay 0.1
// (30 to 90 s), loss 0.05, failure 0.1 and timeout 0.05.
type Faults struct {
	// DuplicateRate: a webhook is delivered twice (the same event ID).
	DuplicateRate float64 `json:"duplicateRate"`
	// DelayRate: a webhook's first delivery waits between DelayMin and
	// DelayMax.
	DelayRate float64  `json:"delayRate"`
	DelayMin  Duration `json:"delayMin"`
	DelayMax  Duration `json:"delayMax"`
	// LossRate: a webhook is never sent (the order still changes, so only
	// polling or reconciliation can find out).
	LossRate float64 `json:"lossRate"`
	// FailureRate: a payment attempt that would succeed fails instead
	// ("card_declined").
	FailureRate float64 `json:"failureRate"`
	// TimeoutRate: an API call is processed, but its answer is held back
	// for TimeoutDelay, longer than any sane client waits. A retry with the
	// same idempotency key gets the stored result.
	TimeoutRate  float64  `json:"timeoutRate"`
	TimeoutDelay Duration `json:"timeoutDelay"`
	// Outage: every API and checkout call fails with 503. Webhooks already
	// queued are still delivered.
	Outage bool `json:"outage"`
}

// Validate checks the rates and durations.
func (f Faults) Validate() error {
	var errs []error
	for name, r := range map[string]float64{
		"duplicateRate": f.DuplicateRate, "delayRate": f.DelayRate, "lossRate": f.LossRate,
		"failureRate": f.FailureRate, "timeoutRate": f.TimeoutRate,
	} {
		if r < 0 || r > 1 {
			errs = append(errs, fmt.Errorf("%s must be between 0 and 1", name))
		}
	}
	if f.DelayMin < 0 || f.DelayMax < f.DelayMin || f.DelayMax.D() > 10*time.Minute {
		errs = append(errs, errors.New("delayMin and delayMax must satisfy 0 <= delayMin <= delayMax <= 10m"))
	}
	if f.TimeoutDelay < 0 || f.TimeoutDelay.D() > 5*time.Minute {
		errs = append(errs, errors.New("timeoutDelay must be between 0 and 5m"))
	}
	return errors.Join(errs...)
}

// withDefaults fills unset durations: delays of 30 to 90 s, a 10 s timeout.
func (f Faults) withDefaults() Faults {
	if f.DelayMax == 0 {
		f.DelayMin, f.DelayMax = Duration(30*time.Second), Duration(90*time.Second)
	}
	if f.TimeoutDelay == 0 {
		f.TimeoutDelay = Duration(10 * time.Second)
	}
	return f
}

// Duration is a time.Duration written as a Go duration string ("30s").
type Duration time.Duration

// D returns d as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New(`durations are strings such as "30s"`)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Injector decides, with a seeded random source, which faults happen.
type Injector struct {
	mu     sync.Mutex
	faults Faults
	rnd    *rand.Rand
}

// NewInjector returns an injector with no faults. The same seed gives the
// same sequence of decisions for the same sequence of calls.
func NewInjector(seed uint64) *Injector {
	return &Injector{rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), faults: Faults{}.withDefaults()} //nolint:gosec // fault injection, not security
}

// Set replaces the faults.
func (in *Injector) Set(f Faults) error {
	if err := f.Validate(); err != nil {
		return err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	in.faults = f.withDefaults()
	return nil
}

// Faults returns the current faults.
func (in *Injector) Faults() Faults {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.faults
}

func (in *Injector) roll(rate func(Faults) float64) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	r := rate(in.faults)
	return r > 0 && in.rnd.Float64() < r
}

// Outage reports whether the provider is down.
func (in *Injector) Outage() bool { return in.Faults().Outage }

// Duplicate reports whether a webhook is delivered twice.
func (in *Injector) Duplicate() bool {
	return in.roll(func(f Faults) float64 { return f.DuplicateRate })
}

// Lose reports whether a webhook is dropped.
func (in *Injector) Lose() bool { return in.roll(func(f Faults) float64 { return f.LossRate }) }

// Fail reports whether a payment attempt fails.
func (in *Injector) Fail() bool { return in.roll(func(f Faults) float64 { return f.FailureRate }) }

// Timeout returns how long to hold back an API answer (0: answer now).
func (in *Injector) Timeout() time.Duration {
	if !in.roll(func(f Faults) float64 { return f.TimeoutRate }) {
		return 0
	}
	return in.Faults().TimeoutDelay.D()
}

// Delay returns how long to hold a webhook's first delivery (0: send now).
func (in *Injector) Delay() time.Duration {
	in.mu.Lock()
	defer in.mu.Unlock()
	f := in.faults
	if f.DelayRate <= 0 || in.rnd.Float64() >= f.DelayRate {
		return 0
	}
	span := f.DelayMax.D() - f.DelayMin.D()
	if span <= 0 {
		return f.DelayMin.D()
	}
	return f.DelayMin.D() + time.Duration(in.rnd.Int64N(int64(span)+1))
}
