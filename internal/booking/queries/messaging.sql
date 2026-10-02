-- name: InsertOutboxEvent :exec
-- Write in the same transaction as the state change the event describes.
INSERT INTO booking.outbox (event_id, topic, aggregate_id, event_type, payload, headers)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: ClaimUnpublishedEvents :many
-- The relay's batch, in commit order, locked so a second relay (during a
-- leadership handover) skips rather than duplicates them.
SELECT * FROM booking.outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT sqlc.arg(batch)
FOR UPDATE SKIP LOCKED;

-- name: MarkEventsPublished :exec
UPDATE booking.outbox SET published_at = now() WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND published_at IS NULL;

-- name: RecordProcessedMessage :execrows
-- Returns 1 the first time a consumer sees a message and 0 for a redelivery.
-- Run in the same transaction as the message's effect.
INSERT INTO booking.processed_messages (consumer, message_id)
VALUES ($1, $2)
ON CONFLICT (consumer, message_id) DO NOTHING;
