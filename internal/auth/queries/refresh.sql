-- name: InsertRefreshToken :exec
INSERT INTO auth.refresh_tokens (token_hash, user_id, family_id, expires_at)
VALUES ($1, $2, $3, $4);

-- name: RotateRefreshToken :one
-- Compare-and-set: only a live token that was never replaced can be
-- rotated, once. No row: replaced, revoked, expired or unknown.
UPDATE auth.refresh_tokens SET replaced_by = sqlc.arg(replaced_by)
WHERE token_hash = sqlc.arg(token_hash)
  AND replaced_by IS NULL AND revoked_at IS NULL AND expires_at > now()
RETURNING user_id, family_id;

-- name: GetRefreshToken :one
SELECT * FROM auth.refresh_tokens WHERE token_hash = $1;

-- name: RevokeFamily :execrows
-- Ends a login: every token of the family stops working.
UPDATE auth.refresh_tokens SET revoked_at = now()
WHERE family_id = $1 AND revoked_at IS NULL;
