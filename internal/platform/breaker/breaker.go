// Package breaker is a circuit breaker for calls to a dependency that can go
// down (the payment provider). After Threshold consecutive failures it opens
// and refuses calls at once for Cooldown, so callers fail fast instead of
// piling up behind timeouts; then it lets one trial call through (half-open):
// success closes it, failure opens it again.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned while the breaker refuses calls.
var ErrOpen = errors.New("breaker: open")

// State is the breaker's state.
type State int

// States.
const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	return [...]string{"closed", "open", "half_open"}[s]
}

// Breaker guards one dependency. The zero value is not usable; use New.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time
	onChange  func(State)

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	trial    bool // a half-open trial call is in flight
}

// New returns a closed breaker. onChange (may be nil) is called with every
// new state, for metrics and logs.
func New(threshold int, cooldown time.Duration, onChange func(State)) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now, onChange: onChange}
}

// Allow reports whether a call may go ahead now. Every allowed call must be
// followed by exactly one Done.
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return ErrOpen
		}
		b.set(HalfOpen)
		b.trial = true
		return nil
	case HalfOpen:
		if b.trial {
			return ErrOpen // one trial at a time
		}
		b.trial = true
	}
	return nil
}

// Done records the outcome of an allowed call: failure means the dependency
// misbehaved (unreachable, timed out, 5xx), not that it gave a valid "no".
func (b *Breaker) Done(failure bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == HalfOpen {
		b.trial = false
		if failure {
			b.openedAt = b.now()
			b.set(Open)
			return
		}
		b.failures = 0
		b.set(Closed)
		return
	}
	if !failure {
		b.failures = 0
		return
	}
	b.failures++
	if b.state == Closed && b.failures >= b.threshold {
		b.openedAt = b.now()
		b.set(Open)
	}
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) set(s State) {
	if s == b.state {
		return
	}
	b.state = s
	if b.onChange != nil {
		b.onChange(s)
	}
}
