package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/paymentdb"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

// ApplyWebhook applies one verified provider callback, exactly once: the
// webhook's record (deduplicated by the provider's event ID), the intent's
// forward-only move, its ledger entries and its event all commit in one
// transaction. A duplicate, an event for an unknown order, or a move the
// intent has already passed changes nothing and is still acknowledged.
func (s *Service) ApplyWebhook(ctx context.Context, raw []byte) error {
	var wh psp.Webhook
	if err := json.Unmarshal(raw, &wh); err != nil || wh.ID == "" || wh.Type == "" {
		s.m.webhook("malformed", false)
		return nil // acknowledged: retrying a body we cannot read will not help
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		n, err := q.RecordWebhook(ctx, paymentdb.RecordWebhookParams{
			PspEventID: wh.ID, EventType: wh.Type, PspOrderID: text(wh.OrderID), Payload: raw,
		})
		if err != nil {
			return err
		}
		if n == 0 {
			s.m.webhook(wh.Type, true)
			return nil
		}
		s.m.webhook(wh.Type, false)
		in, err := q.GetIntentByOrder(ctx, text(wh.OrderID))
		if errors.Is(err, pgx.ErrNoRows) {
			// An order HoldFast never made (or a test's): record it, skip it.
			s.log.WarnContext(ctx, "payment: webhook for an unknown order", "order_id", wh.OrderID, "type", wh.Type)
			return nil
		}
		if err != nil {
			return err
		}
		switch wh.Type {
		case psp.EventPaymentCaptured:
			return s.capture(ctx, q, in, wh.PaymentID, wh.AmountPaise, "webhook")
		case psp.EventPaymentFailed:
			return s.fail(ctx, q, in, wh.Reason)
		case psp.EventOrderExpired:
			return s.expire(ctx, q, in)
		case psp.EventRefundCompleted:
			return s.refunded(ctx, q, in, wh.RefundID)
		}
		s.log.WarnContext(ctx, "payment: unknown webhook type", "type", wh.Type)
		return nil
	})
}

// capture moves the intent to CAPTURED (from CREATED, FAILED or EXPIRED: a
// capture always wins), books it in the ledger and announces it. Already
// captured or refunding: nothing to do.
func (s *Service) capture(ctx context.Context, q *paymentdb.Queries, in paymentdb.Intent, paymentID string, amount int64, via string) error {
	if amount != in.AmountPaise {
		// Never book a capture for a different amount; reconciliation (Phase
		// 5) pages a human.
		s.m.mismatch.Inc()
		s.log.ErrorContext(ctx, "payment: captured amount differs from the intent", "intent_id", in.ID,
			"intent_paise", in.AmountPaise, "captured_paise", amount)
		return nil
	}
	if paymentID == "" {
		return fmt.Errorf("payment: capture of intent %s without a payment ID", in.ID)
	}
	captured, err := q.CaptureIntent(ctx, paymentdb.CaptureIntentParams{ID: in.ID, PspPaymentID: text(paymentID)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !captured.EventID.Valid {
		return errMissingEventRef
	}
	if err := book(ctx, q, captured, "psp_receivable", revenue(captured)); err != nil {
		return err
	}
	s.m.captured(via)
	return writeEvent(ctx, q, captured.BookingID, eventCaptured, &eventsv1.PaymentCaptured{
		BookingId: captured.BookingID.String(), IntentId: captured.ID.String(), PspPaymentId: paymentID,
		AmountPaise: captured.AmountPaise, CapturedAt: timestamppb.New(captured.UpdatedAt),
	})
}

func (s *Service) fail(ctx context.Context, q *paymentdb.Queries, in paymentdb.Intent, reason string) error {
	failed, err := q.TransitionIntent(ctx, paymentdb.TransitionIntentParams{ID: in.ID, FromStatus: "CREATED", ToStatus: "FAILED"})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // already captured, expired or failed: forward only
	}
	if err != nil {
		return err
	}
	return writeEvent(ctx, q, failed.BookingID, eventFailed, &eventsv1.PaymentFailed{
		BookingId: failed.BookingID.String(), IntentId: failed.ID.String(), Reason: reason,
		FailedAt: timestamppb.New(failed.UpdatedAt),
	})
}

func (s *Service) expire(ctx context.Context, q *paymentdb.Queries, in paymentdb.Intent) error {
	expired, err := q.TransitionIntent(ctx, paymentdb.TransitionIntentParams{ID: in.ID, FromStatus: "CREATED", ToStatus: "EXPIRED"})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return writeEvent(ctx, q, expired.BookingID, eventExpired, &eventsv1.PaymentExpired{
		BookingId: expired.BookingID.String(), IntentId: expired.ID.String(), ExpiredAt: timestamppb.New(expired.UpdatedAt),
	})
}

// refunded completes a refund requested earlier (task 3.11): REFUND_PENDING
// to REFUNDED, with the reverse ledger entries.
func (s *Service) refunded(ctx context.Context, q *paymentdb.Queries, in paymentdb.Intent, refundID string) error {
	done, err := q.TransitionIntent(ctx, paymentdb.TransitionIntentParams{ID: in.ID, FromStatus: "REFUND_PENDING", ToStatus: "REFUNDED"})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !done.EventID.Valid {
		return errMissingEventRef
	}
	if err := book(ctx, q, done, revenue(done), "psp_receivable"); err != nil {
		return err
	}
	return writeEvent(ctx, q, done.BookingID, eventRefunded, &eventsv1.RefundCompleted{
		BookingId: done.BookingID.String(), IntentId: done.ID.String(), PspRefundId: refundID,
		AmountPaise: done.AmountPaise, RefundedAt: timestamppb.New(done.UpdatedAt),
	})
}

func revenue(in paymentdb.Intent) string { return "unearned_revenue:" + in.EventID.UUID.String() }

// book writes one balanced ledger transaction: debit one account, credit the
// other, for the intent's amount.
func book(ctx context.Context, q *paymentdb.Queries, in paymentdb.Intent, debit, credit string) error {
	txn := uuid.Must(uuid.NewV7())
	for _, e := range []struct{ account, dir string }{{debit, "D"}, {credit, "C"}} {
		if err := q.InsertLedgerEntry(ctx, paymentdb.InsertLedgerEntryParams{
			TxnID: txn, Account: e.account, Direction: e.dir, AmountPaise: in.AmountPaise, IntentID: in.ID,
		}); err != nil {
			return err
		}
	}
	return nil
}

// applyOrder applies the provider's view of an order found by polling: the
// same moves as the webhooks, with no webhook to record.
func (s *Service) applyOrder(ctx context.Context, q *paymentdb.Queries, in paymentdb.Intent, o psp.Order) error {
	switch o.Status {
	case psp.OrderCaptured:
		return s.capture(ctx, q, in, o.PaymentID, o.AmountPaise, "poll")
	case psp.OrderFailed:
		return s.fail(ctx, q, in, o.FailureReason)
	case psp.OrderExpired:
		return s.expire(ctx, q, in)
	}
	return nil // still CREATED: the buyer may be paying right now
}
