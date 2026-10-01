package queue

import "errors"

// Domain errors. The HTTP layer maps each one to a stable problem code.
var (
	ErrInvalidRequest    = errors.New("queue: invalid request")
	ErrProvisionConflict = errors.New("queue: event already provisioned with different settings")
)
