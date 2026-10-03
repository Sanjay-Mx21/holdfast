-- name: UpsertVerifiedUser :one
-- A verified phone signs in: the first time it creates the user, later
-- times it only records the new verification. One user per phone.
INSERT INTO auth.users (id, phone_hmac, phone_last4, phone_verified_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (phone_hmac) DO UPDATE SET phone_verified_at = now()
RETURNING *;

-- name: GetUser :one
SELECT * FROM auth.users WHERE id = $1;
