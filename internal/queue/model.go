package queue

import (
	"fmt"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/policy"
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
	// Policy holds the event's policy windows (zero times: none), checked
	// at join. Unlike the settings above, they are not fixed: provisioning
	// again with the same settings replaces them.
	Policy policy.Rules
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
	for name, t := range map[string]time.Time{"verifiedOnlyUntil": c.Policy.VerifiedOnlyUntil, "agentLockoutUntil": c.Policy.AgentLockoutUntil} {
		if !t.IsZero() && t.Before(earliestOpening) {
			return fmt.Errorf("%w: %s must be a time after 2020-01-01, or absent for no window", ErrInvalidRequest, name)
		}
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

// Position is where a user stands in an event's queue.
type Position struct {
	EventID string
	// State is the queue's state. A queue still marked PRE after T0 (nobody
	// has flipped it yet) is reported as OPEN: T0 is decided by the clock.
	State State
	// Rank is the 1-based place in line from T0 on; zero before T0.
	Rank int64
	// RandomizingAt is T0 while the lottery is still open; zero from T0 on.
	RandomizingAt time.Time
}

// Advance is the outcome of one admission tick.
type Advance struct {
	// AdmittedUpTo is the highest admitted rank (q:{E}:admitted).
	AdmittedUpTo int64
	// Admitted is how many people this tick admitted.
	Admitted int64
	// ActiveSessions counts unexpired session slots after this tick.
	ActiveSessions int64
	// QueueSize counts everyone in the queue (q:{E}:members).
	QueueSize int64
	// State is the queue's state as the status document shows it.
	State State
}

// Status is an event's status document: the same for every client, so the
// edge can cache it for a second and absorb the waiting room's polling.
type Status struct {
	EventID string
	// State follows the T0 clock rule: PRE past T0 reads as OPEN.
	State        State
	OpensAt      time.Time
	AdmittedUpTo int64
	QueueSize    int64
	// UpdatedAt is when the admission leader last wrote the document; zero
	// when no leader has written it yet (a fallback built from the raw keys).
	UpdatedAt time.Time
}

// Overview is an operator's view of one event's queue: the status clients
// see, plus the settings and admission state behind it (holdfastctl queue
// status).
type Overview struct {
	Status
	Config EventConfig
	// StoredState is the state as stored; Status.State applies the T0 rule.
	StoredState State
	// LeaderEpoch is the fencing epoch of the latest admission term; 0 if no
	// controller has led yet.
	LeaderEpoch int64
	// ActiveSessions counts session slots that are unexpired by Valkey's clock.
	ActiveSessions int64
	// OnWorkList is true while the event is in q:events, so every replica's
	// opener and admission controller serve it.
	OnWorkList bool
	// Now is Valkey's clock when the overview was read.
	Now time.Time
}

// statusDoc is the JSON stored in q:{E}:status by advance.lua.
type statusDoc struct {
	State        State `json:"state"`
	OpensAtMs    int64 `json:"opensAtMs"`
	AdmittedUpTo int64 `json:"admittedUpTo"`
	QueueSize    int64 `json:"queueSize"`
	UpdatedAtMs  int64 `json:"updatedAtMs"`
}

// Turn is a user's claim on their turn: they are admitted, and their
// session slot lasts until SessionExpires.
type Turn struct {
	EventID        string
	UserID         string
	Rank           int64
	SessionExpires time.Time
}

// NotYourTurnError says how far the user still has to wait.
type NotYourTurnError struct {
	Rank         int64
	AdmittedUpTo int64
}

func (e *NotYourTurnError) Error() string {
	return fmt.Sprintf("queue: not your turn yet: rank %d, admitted up to %d", e.Rank, e.AdmittedUpTo)
}

// Unwrap makes errors.Is(err, ErrNotYourTurn) true.
func (e *NotYourTurnError) Unwrap() error { return ErrNotYourTurn }
