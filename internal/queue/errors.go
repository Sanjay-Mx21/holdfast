package queue

import "errors"

// Domain errors. The HTTP layer maps each one to a stable problem code.
var (
	ErrInvalidRequest    = errors.New("queue: invalid request")
	ErrProvisionConflict = errors.New("queue: event already provisioned with different settings")
	ErrEventNotFound     = errors.New("queue: event not provisioned")
	ErrQueueClosed       = errors.New("queue: queue is closed")
	ErrNotInQueue        = errors.New("queue: user has not joined this queue")
	ErrNotYourTurn       = errors.New("queue: not your turn yet")
	ErrTurnExpired       = errors.New("queue: your turn has expired")
	// ErrStateConflict: the queue is not in a state the operation applies
	// to, such as freezing a queue before T0 or after it sold out.
	ErrStateConflict = errors.New("queue: the queue is not in a state this applies to")

	// ErrFenced means an admission write carried a stale epoch: another
	// controller has become leader since, and this one must step down.
	ErrFenced = errors.New("queue: fenced: not the current admission leader")
)
