-- name: RecordWebhook :execrows
-- 1 for a webhook seen for the first time, 0 for a duplicate delivery.
-- Run in the same transaction as the webhook's effect.
INSERT INTO payment.webhook_events (psp_event_id, event_type, psp_order_id, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (psp_event_id) DO NOTHING;

-- name: InsertLedgerEntry :exec
-- Insert every entry of one txn_id in the same database transaction: the
-- deferred trigger refuses, at commit, a transaction whose debits and
-- credits differ.
INSERT INTO payment.ledger_entries (txn_id, account, direction, amount_paise, intent_id)
VALUES ($1, $2, $3, $4, $5);

-- name: AccountBalance :one
-- Debits minus credits for one account.
SELECT coalesce(sum(CASE direction WHEN 'D' THEN amount_paise ELSE -amount_paise END), 0)::bigint AS balance
FROM payment.ledger_entries WHERE account = $1;
