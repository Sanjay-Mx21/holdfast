package inventory

import "errors"

// Domain errors. The HTTP layer maps each one to a stable problem code.
var (
	ErrInvalidRequest       = errors.New("inventory: invalid request")
	ErrInvalidQuantity      = errors.New("inventory: invalid quantity")
	ErrEventNotProvisioned  = errors.New("inventory: event not provisioned")
	ErrSoldOut              = errors.New("inventory: sold out")
	ErrUserLimit            = errors.New("inventory: per-user limit reached")
	ErrHoldNotFound         = errors.New("inventory: hold not found")
	ErrHoldExpired          = errors.New("inventory: hold expired or released")
	ErrHoldNotCancellable   = errors.New("inventory: hold is in checkout and cannot be cancelled")
	ErrIdempotencyKeyReused = errors.New("inventory: idempotency key reused with different parameters")
	ErrProvisionConflict    = errors.New("inventory: event already provisioned with different settings")
	ErrSalePaused           = errors.New("inventory: the sale is frozen; no new holds until it resumes")

	// errNotExpiredYet is internal: the sweeper saw a hold its clock thought
	// had expired but the server's clock did not. It retries on the next tick.
	errNotExpiredYet = errors.New("inventory: hold not expired yet")
)
