-- name: CreateBooking :one
-- A booking starts PENDING_PAYMENT. hold_id is unique: a second booking for
-- the same hold fails with a unique violation instead of being created.
INSERT INTO booking.bookings (id, event_id, user_id, hold_id, qty, amount_paise, payment_deadline)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetBooking :one
SELECT * FROM booking.bookings WHERE id = $1;

-- name: GetBookingForUser :one
-- Owner-only read: another user's booking is "not found", never "forbidden",
-- so booking IDs cannot be probed.
SELECT * FROM booking.bookings WHERE id = $1 AND user_id = $2;

-- name: GetBookingByHold :one
SELECT * FROM booking.bookings WHERE hold_id = $1;

-- name: TransitionBooking :one
-- Compare-and-set: moves the booking only if it is still in the expected
-- status. No row means another actor moved it first (or it does not exist);
-- the caller re-reads and decides. The trigger refuses invalid moves.
UPDATE booking.bookings
SET status = sqlc.arg(to_status), version = version + 1, updated_at = now()
WHERE id = sqlc.arg(id) AND status = sqlc.arg(from_status)
RETURNING *;

-- name: SetBookingIntent :one
-- Records payment-svc's intent once; a retry with the same intent is a no-op,
-- a different one is refused (no row).
UPDATE booking.bookings
SET intent_id = sqlc.arg(intent_id), updated_at = now()
WHERE id = sqlc.arg(id) AND (intent_id IS NULL OR intent_id = sqlc.arg(intent_id))
RETURNING *;

-- name: ClaimExpiredBookings :many
-- The deadline job: pending bookings past their deadline, oldest first,
-- locked so concurrent jobs (one per replica) take disjoint batches.
-- Call inside a transaction that also moves them.
SELECT * FROM booking.bookings
WHERE status = 'PENDING_PAYMENT' AND payment_deadline < now()
ORDER BY payment_deadline
LIMIT sqlc.arg(batch)
FOR UPDATE SKIP LOCKED;
