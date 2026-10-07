-- What an event's inventory must be, by the records (runbook RB-2, task
-- 5.3). Read together in one repeatable-read transaction (InventorySnapshot).

-- name: SnapshotEventInventory :one
SELECT i.capacity, i.sold, e.per_user_limit
FROM booking.event_inventory i
JOIN booking.events e ON e.id = i.event_id
WHERE i.event_id = $1;

-- name: SnapshotPendingBookings :many
-- Bookings waiting for payment still hold their units (a PAYING hold).
SELECT hold_id, user_id, qty, payment_deadline
FROM booking.bookings
WHERE event_id = $1 AND status = 'PENDING_PAYMENT'
ORDER BY created_at, id;

-- name: SnapshotPurchases :many
-- Units each user bought, as the final guard counted them.
SELECT user_id, qty
FROM booking.user_event_purchases
WHERE event_id = $1 AND qty > 0;
