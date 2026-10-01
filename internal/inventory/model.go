package inventory

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// HoldState is the lifecycle state of a hold.
type HoldState string

// Hold states.
const (
	StateHeld     HoldState = "HELD"
	StatePaying   HoldState = "PAYING"
	StateSold     HoldState = "SOLD"
	StateReleased HoldState = "RELEASED"
)

// ReleaseMode records why a hold's units went back to the pool.
type ReleaseMode string

// Release modes.
const (
	ReleaseExpire        ReleaseMode = "EXPIRE"
	ReleaseUserCancel    ReleaseMode = "USER_CANCEL"
	ReleasePaymentFailed ReleaseMode = "PAYMENT_FAILED"
)

// ConfirmOutcome describes how a confirmation was applied.
type ConfirmOutcome string

// Confirm outcomes.
const (
	ConfirmApplied ConfirmOutcome = "confirmed"
	ConfirmReplay  ConfirmOutcome = "replay"
	ConfirmLate    ConfirmOutcome = "late"
)

// Hold is a temporary claim on units of one event by one user.
type Hold struct {
	ID        string
	EventID   string
	UserID    string
	Quantity  int
	State     HoldState
	ExpiresAt time.Time
}

// EventConfig is the inventory configuration of an event.
type EventConfig struct {
	Capacity     int
	PerUserLimit int
	// InitialAvailable is the starting pool; zero means Capacity. When
	// rebuilding Valkey from PostgreSQL it is capacity minus units sold.
	InitialAvailable int
}

func (c EventConfig) initialAvailable() int {
	if c.InitialAvailable == 0 {
		return c.Capacity
	}
	return c.InitialAvailable
}

// Validate checks the configuration's bounds.
func (c EventConfig) Validate() error {
	if c.Capacity < 1 || c.Capacity > 10_000_000 {
		return fmt.Errorf("%w: capacity must be between 1 and 10000000", ErrInvalidRequest)
	}
	if c.PerUserLimit < 1 || c.PerUserLimit > 10 {
		return fmt.Errorf("%w: per-user limit must be between 1 and 10", ErrInvalidRequest)
	}
	if c.InitialAvailable < 0 || c.InitialAvailable > c.Capacity {
		return fmt.Errorf("%w: initial available units must be between 0 and capacity", ErrInvalidRequest)
	}
	return nil
}

// Availability is a point-in-time view of an event's remaining units.
type Availability struct {
	EventID   string
	Available int
	Capacity  int
}

// SoldOut reports whether no units are left. Late confirmations can push
// the counter below zero, so anything <= 0 counts as sold out.
func (a Availability) SoldOut() bool { return a.Available <= 0 }

// CreateHoldRequest is the input to Service.CreateHold.
type CreateHoldRequest struct {
	EventID        string
	UserID         string
	IdempotencyKey string
	Quantity       int
}

// HoldResult is the outcome of a successful CreateHold.
type HoldResult struct {
	Hold      Hold
	Remaining int
	// Replayed is true when the idempotency key matched an existing hold and
	// the original result was returned instead of holding again.
	Replayed bool
}

// holdNamespace scopes deterministic hold IDs to HoldFast (UUIDv5 namespace).
var holdNamespace = uuid.MustParse("5f3c2d8e-7b1a-4c6e-9d2f-8a4b6c1e3f70")

// HoldIDFor derives the hold ID from (user, idempotency key). Retrying a
// request with the same key therefore targets the same hold, which is what
// makes CreateHold safe to retry at every layer.
func HoldIDFor(userID, idempotencyKey string) string {
	return uuid.NewSHA1(holdNamespace, []byte(userID+"\x00"+idempotencyKey)).String()
}
