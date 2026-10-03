-- +goose Up
-- Status polling rotates through the open intents: the least recently
-- polled first, never polled before all. Each poll stamps polled_at, even a
-- failed one, so intents whose polls keep failing cannot hold the head of
-- the queue and starve the rest (P34).
ALTER TABLE payment.payment_intents ADD COLUMN polled_at timestamptz;
DROP INDEX payment.payment_intents_open_idx;
CREATE INDEX payment_intents_open_idx ON payment.payment_intents (polled_at NULLS FIRST, created_at)
  WHERE status = 'CREATED';

-- +goose Down
DROP INDEX payment.payment_intents_open_idx;
CREATE INDEX payment_intents_open_idx ON payment.payment_intents (created_at)
  WHERE status = 'CREATED';
ALTER TABLE payment.payment_intents DROP COLUMN IF EXISTS polled_at;
