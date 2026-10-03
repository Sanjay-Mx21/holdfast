-- name: LockPhone :exec
-- Serialises OTP requests for one phone within the caller's transaction, so
-- the rate limit cannot be raced.
SELECT pg_advisory_xact_lock(sqlc.arg(key)::bigint);

-- name: RecentChallenges :one
-- How many codes the phone was sent since a time, and when the first and
-- the last of them were (the epoch when none).
SELECT count(*)::int AS sent,
       coalesce(min(created_at), 'epoch'::timestamptz)::timestamptz AS oldest,
       coalesce(max(created_at), 'epoch'::timestamptz)::timestamptz AS latest
FROM auth.otp_challenges
WHERE phone_hmac = sqlc.arg(phone_hmac) AND created_at > sqlc.arg(since);

-- name: InsertChallenge :exec
INSERT INTO auth.otp_challenges (id, phone_hmac, code_hash, expires_at)
VALUES ($1, $2, $3, $4);

-- name: TakeAttempt :one
-- Counts one guess against the phone's latest open challenge, in one
-- statement: concurrent guesses each take an attempt, and none is allowed
-- past the limit. No row: no open challenge, or no attempts left.
UPDATE auth.otp_challenges AS o SET attempts = o.attempts + 1
WHERE o.id = (
    SELECT c.id FROM auth.otp_challenges AS c
    WHERE c.phone_hmac = sqlc.arg(phone_hmac) AND c.consumed_at IS NULL AND c.expires_at > now()
    ORDER BY c.created_at DESC
    LIMIT 1
  )
  AND o.attempts < sqlc.arg(max_attempts)::smallint AND o.consumed_at IS NULL AND o.expires_at > now()
RETURNING o.id, o.code_hash, o.attempts;

-- name: ConsumeChallenge :execrows
-- A correct code is used once.
UPDATE auth.otp_challenges SET consumed_at = now()
WHERE id = $1 AND consumed_at IS NULL;
