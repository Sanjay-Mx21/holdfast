package booking

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/bookingdb"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/guard"
	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

// Payment events the saga consumes (holdfast.payment.v1).
const (
	paymentCaptured = "payment.captured.v1"
	paymentFailed   = "payment.failed.v1"
	paymentExpired  = "payment.expired.v1"
	refundCompleted = "refund.completed.v1"
)

// SagaConsumer is the saga's consumer group: its name in processed_messages
// and in Kafka.
const SagaConsumer = "booking-saga"

// Settler settles holds in inventory-svc (*inventory.Client). Both calls are
// idempotent.
type Settler interface {
	Confirm(ctx context.Context, eventID, userID, holdID string, qty int) (inventory.ConfirmOutcome, error)
	ReleaseForFailedPayment(ctx context.Context, eventID, userID, holdID string) (bool, error)
}

// Saga carries bookings to their end from payment-svc's events (design doc
// 6.4, 7.2 and 9.7): a capture confirms the booking if the final guard
// accepts the sale and asks for a refund if it does not; a failed or
// expired payment cancels it; a completed refund closes it.
//
// Each message is applied in one transaction with its dedup record, so a
// redelivery changes nothing. Inventory is settled after the commit, from
// the booking's state rather than from the message: a confirmed booking's
// hold is confirmed; a cancelled or refunded one's released. Both calls are idempotent,
// so they run on every delivery, and a failed call is retried by the
// redelivery even though the transaction is not.
type Saga struct {
	pool *pgxpool.Pool
	q    *bookingdb.Queries
	inv  Settler
	m    *Metrics
	log  *slog.Logger
}

// NewSaga returns the saga.
func NewSaga(pool *pgxpool.Pool, inv Settler, m *Metrics, log *slog.Logger) *Saga {
	return &Saga{pool: pool, q: bookingdb.New(pool), inv: inv, m: m, log: log}
}

// Handle implements kafka.Handler.
func (s *Saga) Handle(ctx context.Context, msg kafka.Message) error {
	switch msg.Type() {
	case paymentCaptured:
		var ev eventsv1.PaymentCaptured
		if err := decodeEvent(msg, &ev); err != nil {
			return err
		}
		id, err := bookingID(ev.GetBookingId())
		if err != nil {
			return err
		}
		capturedAt, _ := msg.Time()
		return s.captured(ctx, msg.ID(), id, &ev, capturedAt)
	case paymentFailed, paymentExpired:
		reason := eventsv1.CancellationReason_CANCELLATION_REASON_PAYMENT_FAILED
		var raw string
		if msg.Type() == paymentExpired {
			var ev eventsv1.PaymentExpired
			if err := decodeEvent(msg, &ev); err != nil {
				return err
			}
			raw, reason = ev.GetBookingId(), eventsv1.CancellationReason_CANCELLATION_REASON_PAYMENT_EXPIRED
		} else {
			var ev eventsv1.PaymentFailed
			if err := decodeEvent(msg, &ev); err != nil {
				return err
			}
			raw = ev.GetBookingId()
		}
		id, err := bookingID(raw)
		if err != nil {
			return err
		}
		return s.ended(ctx, msg.ID(), id, reason)
	case refundCompleted:
		var ev eventsv1.RefundCompleted
		if err := decodeEvent(msg, &ev); err != nil {
			return err
		}
		id, err := bookingID(ev.GetBookingId())
		if err != nil {
			return err
		}
		return s.refunded(ctx, msg.ID(), id, &ev)
	}
	return nil // not for the saga
}

func decodeEvent(msg kafka.Message, into proto.Message) error {
	if msg.ID() == "" {
		return kafka.Permanent(fmt.Errorf("booking: %s without an event ID", msg.Type()))
	}
	if err := proto.Unmarshal(msg.Value, into); err != nil {
		return kafka.Permanent(fmt.Errorf("booking: decode %s: %w", msg.Type(), err))
	}
	return nil
}

func bookingID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, kafka.Permanent(fmt.Errorf("booking: event with booking ID %q", raw))
	}
	return id, nil
}

// apply runs fn in a transaction with the message's dedup record and the
// booking locked. A duplicate or an unknown booking (a test's, or another
// environment's: P30) does nothing.
func (s *Saga) apply(ctx context.Context, msgID string, id uuid.UUID, fn func(q *bookingdb.Queries, tx pgx.Tx, b bookingdb.Booking) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		n, err := q.RecordProcessedMessage(ctx, bookingdb.RecordProcessedMessageParams{Consumer: SagaConsumer, MessageID: msgID})
		if err != nil {
			return err
		}
		if n == 0 {
			s.m.saga.WithLabelValues("duplicate").Inc()
			return nil
		}
		b, err := q.GetBookingForUpdate(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			s.m.saga.WithLabelValues("unknown_booking").Inc()
			s.log.WarnContext(ctx, "booking: payment event for an unknown booking; skipped", "booking_id", id)
			return nil
		}
		if err != nil {
			return err
		}
		return fn(q, tx, b)
	})
}

// captured decides a captured booking: CONFIRMED if the final guard takes
// the sale, REFUND_REQUIRED if it refuses. A capture after the booking was
// cancelled is a late capture: honoured if the guard allows, refunded if
// not (design doc 7.2). capturedAt, the capture event's time (zero if
// unknown), times the confirmation for the capture-to-confirm SLO.
func (s *Saga) captured(ctx context.Context, msgID string, id uuid.UUID, ev *eventsv1.PaymentCaptured, capturedAt time.Time) error {
	confirmed := false
	err := s.apply(ctx, msgID, id, func(q *bookingdb.Queries, tx pgx.Tx, b bookingdb.Booking) error {
		confirmed = false
		if b.Status != StatusPendingPayment && b.Status != StatusCancelled {
			s.m.saga.WithLabelValues("already_decided").Inc()
			return nil // confirmed or refunding already
		}
		late := b.Status == StatusCancelled
		limit, err := q.GetPerUserLimit(ctx, b.EventID)
		if err != nil {
			return fmt.Errorf("booking: per-user limit: %w", err)
		}
		gerr := guard.Reserve(ctx, tx, guard.Reservation{EventID: b.EventID, UserID: b.UserID, Qty: int(b.Qty), PerUserLimit: int(limit)})
		switch {
		case gerr == nil:
			done, err := q.TransitionBooking(ctx, bookingdb.TransitionBookingParams{ID: b.ID, FromStatus: b.Status, ToStatus: StatusConfirmed})
			if err != nil {
				return fmt.Errorf("booking: confirm: %w", err)
			}
			s.m.decided(StatusConfirmed, late)
			confirmed = true
			return writeEvent(ctx, q, b.ID, eventConfirmed, &eventsv1.BookingConfirmed{
				BookingId: b.ID.String(), EventId: b.EventID.String(), UserId: b.UserID.String(), HoldId: b.HoldID.String(),
				Quantity: int32(b.Qty), ConfirmedAt: timestamppb.New(done.UpdatedAt),
			})
		case errors.Is(gerr, guard.ErrSoldOut), errors.Is(gerr, guard.ErrUserCapExceeded):
			done, err := q.TransitionBooking(ctx, bookingdb.TransitionBookingParams{ID: b.ID, FromStatus: b.Status, ToStatus: StatusRefundRequired})
			if err != nil {
				return fmt.Errorf("booking: refund required: %w", err)
			}
			reason := eventsv1.RefundReason_REFUND_REASON_GUARD_REJECTED
			if late {
				reason = eventsv1.RefundReason_REFUND_REASON_LATE_CAPTURE
			}
			s.m.decided(StatusRefundRequired, late)
			s.log.InfoContext(ctx, "booking: captured but the final guard refused; refund required",
				"booking_id", b.ID, "late", late, "guard", gerr.Error())
			return writeEvent(ctx, q, b.ID, eventRefundRequired, &eventsv1.BookingRefundRequired{
				BookingId: b.ID.String(), IntentId: ev.GetIntentId(), AmountPaise: ev.GetAmountPaise(),
				Reason: reason, RequiredAt: timestamppb.New(done.UpdatedAt),
			})
		case errors.Is(gerr, guard.ErrUnknownEvent), errors.Is(gerr, guard.ErrInvalid):
			return kafka.Permanent(fmt.Errorf("booking %s: %w", b.ID, gerr)) // data needing a human
		default:
			return gerr
		}
	})
	if err != nil {
		return err
	}
	if confirmed && !capturedAt.IsZero() {
		s.m.confirmedAfter(capturedAt) // committed: the buyer's booking is confirmed
	}
	return s.settle(ctx, id)
}

// ended cancels a booking whose payment failed or expired.
func (s *Saga) ended(ctx context.Context, msgID string, id uuid.UUID, reason eventsv1.CancellationReason) error {
	err := s.apply(ctx, msgID, id, func(q *bookingdb.Queries, _ pgx.Tx, b bookingdb.Booking) error {
		if b.Status != StatusPendingPayment {
			s.m.saga.WithLabelValues("already_decided").Inc()
			return nil
		}
		done, err := q.TransitionBooking(ctx, bookingdb.TransitionBookingParams{ID: b.ID, FromStatus: b.Status, ToStatus: StatusCancelled})
		if err != nil {
			return fmt.Errorf("booking: cancel: %w", err)
		}
		s.m.decided(StatusCancelled, false)
		return writeEvent(ctx, q, b.ID, eventCancelled, cancelledEvent(done, reason))
	})
	if err != nil {
		return err
	}
	return s.settle(ctx, id)
}

// refunded closes a booking whose refund completed.
func (s *Saga) refunded(ctx context.Context, msgID string, id uuid.UUID, ev *eventsv1.RefundCompleted) error {
	return s.apply(ctx, msgID, id, func(q *bookingdb.Queries, _ pgx.Tx, b bookingdb.Booking) error {
		if b.Status != StatusRefundRequired {
			s.m.saga.WithLabelValues("already_decided").Inc()
			return nil
		}
		done, err := q.TransitionBooking(ctx, bookingdb.TransitionBookingParams{ID: b.ID, FromStatus: b.Status, ToStatus: StatusRefunded})
		if err != nil {
			return fmt.Errorf("booking: refunded: %w", err)
		}
		s.m.decided(StatusRefunded, false)
		return writeEvent(ctx, q, b.ID, eventRefunded, &eventsv1.BookingRefunded{
			BookingId: b.ID.String(), IntentId: ev.GetIntentId(), AmountPaise: ev.GetAmountPaise(),
			PspRefundId: ev.GetPspRefundId(), RefundedAt: timestamppb.New(done.UpdatedAt),
		})
	})
}

// settle brings inventory in line with the booking: a confirmed booking's
// hold becomes SOLD (a late one re-takes its units); the hold of a booking
// that was cancelled or will be refunded is released. Errors are returned
// so the message is retried.
func (s *Saga) settle(ctx context.Context, id uuid.UUID) error {
	if s.inv == nil {
		return nil
	}
	b, err := s.q.GetBooking(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	event, user, hold := b.EventID.String(), b.UserID.String(), b.HoldID.String()
	switch b.Status {
	case StatusConfirmed:
		out, err := s.inv.Confirm(ctx, event, user, hold, int(b.Qty))
		if err != nil {
			return settleError(b.ID, "confirm", err)
		}
		s.m.settled.WithLabelValues("confirm_" + string(out)).Inc()
	case StatusCancelled, StatusRefundRequired: // no sale: every hold ends SOLD or RELEASED (I5)
		released, err := s.inv.ReleaseForFailedPayment(ctx, event, user, hold)
		if err != nil {
			return settleError(b.ID, "release", err)
		}
		if released {
			s.m.settled.WithLabelValues("released").Inc()
		} else {
			s.m.settled.WithLabelValues("release_noop").Inc()
		}
	}
	return nil
}

// settleError retries what may pass later (inventory-svc unreachable) and
// dead-letters what will not (no such hold or event in inventory: a human
// reconciles it; PostgreSQL already holds the decision).
func settleError(id uuid.UUID, op string, err error) error {
	err = fmt.Errorf("booking %s: %s in inventory: %w", id, op, err)
	for _, definitive := range []error{inventory.ErrHoldNotFound, inventory.ErrEventNotProvisioned, inventory.ErrInvalidRequest, inventory.ErrInvalidQuantity} {
		if errors.Is(err, definitive) {
			return kafka.Permanent(err)
		}
	}
	return err
}
