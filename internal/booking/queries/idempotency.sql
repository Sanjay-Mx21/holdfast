-- name: BeginIdempotentRequest :one
-- Claims a key for a request. No row means the key exists already: read it
-- with GetIdempotencyKey and replay, wait, or refuse a different request.
INSERT INTO booking.idempotency_keys (user_id, idem_key, request_hash)
VALUES ($1, $2, $3)
ON CONFLICT (user_id, idem_key) DO NOTHING
RETURNING *;

-- name: GetIdempotencyKey :one
SELECT * FROM booking.idempotency_keys WHERE user_id = $1 AND idem_key = $2;

-- name: CompleteIdempotentRequest :one
-- Stores the response once; only an IN_PROGRESS key can complete.
UPDATE booking.idempotency_keys
SET status = 'COMPLETED', booking_id = sqlc.arg(booking_id), response_code = sqlc.arg(response_code),
    response_body = sqlc.arg(response_body), updated_at = now()
WHERE user_id = sqlc.arg(user_id) AND idem_key = sqlc.arg(idem_key) AND status = 'IN_PROGRESS'
RETURNING *;

-- name: DeleteIdempotencyKeysBefore :execrows
-- Keys are kept for 24 hours (design doc 9.5); retries come within minutes.
DELETE FROM booking.idempotency_keys WHERE created_at < sqlc.arg(before);
