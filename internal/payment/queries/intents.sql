-- name: CreateIntent :one
-- One intent per booking: a second create for the same booking inserts
-- nothing (no row), and the caller reads the existing one.
INSERT INTO payment.payment_intents (id, booking_id, amount_paise, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (booking_id) DO NOTHING
RETURNING *;

-- name: GetIntent :one
SELECT * FROM payment.payment_intents WHERE id = $1;

-- name: GetIntentByBooking :one
SELECT * FROM payment.payment_intents WHERE booking_id = $1;

-- name: GetIntentByOrder :one
SELECT * FROM payment.payment_intents WHERE psp_order_id = $1;

-- name: AttachOrder :one
-- Records the PSP's order once; repeating it with the same order is a no-op,
-- a different one is refused (no row).
UPDATE payment.payment_intents
SET psp_order_id = sqlc.arg(psp_order_id), checkout_url = sqlc.arg(checkout_url), updated_at = now()
WHERE id = sqlc.arg(id) AND (psp_order_id IS NULL OR psp_order_id = sqlc.arg(psp_order_id))
RETURNING *;

-- name: CaptureIntent :one
-- Compare-and-set to CAPTURED from any status that a capture may follow
-- (a capture always wins: money moved). No row: already captured or later.
UPDATE payment.payment_intents
SET status = 'CAPTURED', psp_payment_id = sqlc.arg(psp_payment_id), version = version + 1, updated_at = now()
WHERE id = sqlc.arg(id) AND status IN ('CREATED', 'FAILED', 'EXPIRED')
RETURNING *;

-- name: TransitionIntent :one
-- Compare-and-set for the other moves; the trigger refuses invalid ones.
UPDATE payment.payment_intents
SET status = sqlc.arg(to_status), version = version + 1, updated_at = now()
WHERE id = sqlc.arg(id) AND status = sqlc.arg(from_status)
RETURNING *;

-- name: ClaimOpenIntents :many
-- Status polling: intents still CREATED after a while, possibly because a
-- webhook was lost. Locked so concurrent pollers take disjoint batches.
SELECT * FROM payment.payment_intents
WHERE status = 'CREATED' AND created_at < sqlc.arg(created_before) AND psp_order_id IS NOT NULL
ORDER BY created_at
LIMIT sqlc.arg(batch)
FOR UPDATE SKIP LOCKED;

-- name: InsertPSPOrder :exec
INSERT INTO payment.psp_orders (psp_order_id, intent_id, amount_paise, checkout_url, expires_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (psp_order_id) DO NOTHING;
