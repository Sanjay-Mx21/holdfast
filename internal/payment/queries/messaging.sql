-- name: InsertOutboxEvent :exec
-- Write in the same transaction as the state change the event describes.
INSERT INTO payment.outbox (event_id, topic, aggregate_id, event_type, payload, headers)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: RecordProcessedMessage :execrows
-- 1 the first time a consumer sees a message, 0 for a redelivery.
INSERT INTO payment.processed_messages (consumer, message_id)
VALUES ($1, $2)
ON CONFLICT (consumer, message_id) DO NOTHING;
