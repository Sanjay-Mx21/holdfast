package payment

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/paymentdb"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

// RefundConsumer is the consumer group that starts refunds: its name in
// processed_messages and in Kafka.
const RefundConsumer = "payment-refunds"

const bookingRefundRequired = "booking.refund_required.v1"

// HandleBookingEvent implements kafka.Handler for holdfast.booking.v1. A
// booking.refund_required.v1 moves the booking's intent from CAPTURED to
// REFUND_PENDING (with the message's dedup record, in one transaction), then
// asks the provider for the refund, keyed by the intent ID. The provider
// call runs after the commit on every delivery while the intent is
// REFUND_PENDING, so a failed call is retried by the redelivery; the
// provider's idempotency makes the repeats harmless. The refund completes
// with the provider's refund.completed webhook (ApplyWebhook).
func (s *Service) HandleBookingEvent(ctx context.Context, msg kafka.Message) error {
	if msg.Type() != bookingRefundRequired {
		return nil
	}
	var ev eventsv1.BookingRefundRequired
	if msg.ID() == "" || proto.Unmarshal(msg.Value, &ev) != nil {
		return kafka.Permanent(fmt.Errorf("payment: unreadable %s %q", msg.Type(), msg.ID()))
	}
	bookingID, err := uuid.Parse(ev.GetBookingId())
	if err != nil {
		return kafka.Permanent(fmt.Errorf("payment: %s with booking ID %q", msg.Type(), ev.GetBookingId()))
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		n, err := q.RecordProcessedMessage(ctx, paymentdb.RecordProcessedMessageParams{Consumer: RefundConsumer, MessageID: msg.ID()})
		if err != nil || n == 0 {
			return err // a redelivery: already moved
		}
		in, err := q.GetIntentByBooking(ctx, bookingID)
		if errors.Is(err, pgx.ErrNoRows) {
			s.log.WarnContext(ctx, "payment: refund for a booking without an intent; skipped", "booking_id", bookingID)
			return nil
		}
		if err != nil {
			return err
		}
		_, err = q.TransitionIntent(ctx, paymentdb.TransitionIntentParams{ID: in.ID, FromStatus: "CAPTURED", ToStatus: "REFUND_PENDING"})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // already refunding or refunded
		}
		return err
	})
	if err != nil {
		return err
	}
	in, err := s.q.GetIntentByBooking(ctx, bookingID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && in.Status != "REFUND_PENDING") {
		return nil
	}
	if err != nil {
		return err
	}
	r, err := s.psp.CreateRefund(ctx, in.ID.String(), in.PspPaymentID.String, in.AmountPaise)
	switch {
	case errors.Is(err, psp.ErrRejected):
		s.m.refunds.WithLabelValues("rejected").Inc()
		return kafka.Permanent(fmt.Errorf("payment: intent %s: the provider refused the refund: %w", in.ID, err))
	case err != nil:
		s.m.refunds.WithLabelValues("retry").Inc()
		return fmt.Errorf("payment: intent %s: refund: %w", in.ID, err)
	}
	s.m.refunds.WithLabelValues("requested").Inc()
	s.log.InfoContext(ctx, "payment: refund requested", "intent_id", in.ID, "booking_id", bookingID, "refund_id", r.RefundID)
	return nil
}
