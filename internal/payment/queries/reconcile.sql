-- name: CapturesBookedBetween :many
-- Intents whose capture was booked in the ledger in [from, to): what the
-- provider's settlement report must also contain (reconciliation, design
-- doc 9.9). A capture is the debit of psp_receivable.
SELECT DISTINCT i.id, i.psp_payment_id, i.amount_paise
FROM payment.ledger_entries l
JOIN payment.payment_intents i ON i.id = l.intent_id
WHERE l.account = 'psp_receivable' AND l.direction = 'D'
  AND l.created_at >= sqlc.arg(from_time) AND l.created_at < sqlc.arg(to_time);

-- name: StuckRefunds :many
-- Refunds requested (REFUND_PENDING) and still not completed after a while:
-- the provider call may never have succeeded.
SELECT * FROM payment.payment_intents
WHERE status = 'REFUND_PENDING' AND updated_at < sqlc.arg(before)
ORDER BY updated_at
LIMIT sqlc.arg(batch);
