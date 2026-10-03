-- +goose Up
-- The trace context of the request that created the intent (booking-svc's
-- POST /v1/bookings). Webhooks and polls that settle the intent continue it,
-- so a purchase is one trace from the booking to the sale.
ALTER TABLE payment.payment_intents ADD COLUMN trace_context jsonb; -- NULL: none stored

-- +goose Down
ALTER TABLE payment.payment_intents DROP COLUMN IF EXISTS trace_context;
