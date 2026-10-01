package queue

import (
	"fmt"
	"time"
)

// State is the lifecycle state of an event's queue (q:{E}:state).
type State string

// Queue states.
const (
	StatePre     State = "PRE"
	StateOpen    State = "OPEN"
	StateFrozen  State = "FROZEN"
	StateSoldOut State = "SOLD_OUT"
	StateClosed  State = "CLOSED"
)

// Bounds for EventConfig. The admission rate and session limits size the
// purchase path with Little's Law (design doc 3.3): 10,000 sessions of about
// 120 s each sustain about 83 admissions per second.
const (
	MinSessionTTL    = time.Minute
	MaxSessionTTL    = time.Hour
	MaxAdmissionRate = 100_000
	MaxSessions      = 10_000_000
)

// earliestOpening rejects zero or nonsensical opening times.
var earliestOpening = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// EventConfig is the queue configuration of an event, stored in q:{E}:config.
type EventConfig struct {
	// OpensAt is T0: joins before it get a random lottery position, joins
	// after it are first come, first served. Stored with millisecond precision.
	OpensAt time.Time
	// AdmissionRate is the most buyers admitted per second.
	AdmissionRate int
	// MaxSessions caps concurrent checkout sessions (L in Little's Law).
	MaxSessions int
	// SessionTTL is how long an admitted buyer's session lasts.
	SessionTTL time.Duration
}

// Validate checks the configuration's bounds.
func (c EventConfig) Validate() error {
	if c.OpensAt.Before(earliestOpening) {
		return fmt.Errorf("%w: opensAt must be a time after 2020-01-01", ErrInvalidRequest)
	}
	if c.AdmissionRate < 1 || c.AdmissionRate > MaxAdmissionRate {
		return fmt.Errorf("%w: admission rate must be between 1 and %d per second", ErrInvalidRequest, MaxAdmissionRate)
	}
	if c.MaxSessions < 1 || c.MaxSessions > MaxSessions {
		return fmt.Errorf("%w: maximum sessions must be between 1 and %d", ErrInvalidRequest, MaxSessions)
	}
	if c.SessionTTL < MinSessionTTL || c.SessionTTL > MaxSessionTTL {
		return fmt.Errorf("%w: session TTL must be between %s and %s", ErrInvalidRequest, MinSessionTTL, MaxSessionTTL)
	}
	if c.SessionTTL%time.Second != 0 {
		return fmt.Errorf("%w: session TTL must be a whole number of seconds", ErrInvalidRequest)
	}
	return nil
}

// Ordering says how a member's queue position is decided.
type Ordering string

// Orderings.
const (
	// OrderingLottery: joined before T0; the position is a random draw.
	OrderingLottery Ordering = "LOTTERY"
	// OrderingFIFO: joined after T0; the position is the arrival order.
	OrderingFIFO Ordering = "FIFO"
)

// JoinResult is the outcome of a successful Join.
type JoinResult struct {
	EventID string
	// Joined is false when the user was already in the queue; the original
	// position stands (no re-rolling the lottery).
	Joined   bool
	Ordering Ordering
	// openedQueue is true when this join performed the T0 transition
	// (counted in metrics; not part of the API).
	openedQueue bool
}

// orderingOf derives the ordering from a member's score: lottery scores are
// in [0, 1), post-T0 scores are 1 plus the arrival counter.
func orderingOf(score float64) Ordering {
	if score < 1 {
		return OrderingLottery
	}
	return OrderingFIFO
}
