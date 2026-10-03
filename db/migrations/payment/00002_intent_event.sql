-- +goose Up
-- The event a payment is for: the ledger credits unearned_revenue:<event>.
-- Nullable for intents created before this migration; payment-svc always
-- sets it from now on.
ALTER TABLE payment.payment_intents ADD COLUMN event_id uuid;

-- +goose Down
ALTER TABLE payment.payment_intents DROP COLUMN IF EXISTS event_id;
