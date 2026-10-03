package booking

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/bookingdb"
	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

// ErrNotRefundable means the booking is not waiting for a refund.
var ErrNotRefundable = errors.New("booking: not waiting for a refund")

// Publisher publishes events (*kafka.Producer).
type Publisher interface {
	Publish(ctx context.Context, events ...kafka.Event) error
}

// RequestRefundAgain asks payment-svc again to refund a booking stuck in
// REFUND_REQUIRED (runbook RB-4): it publishes a new
// booking.refund_required.v1 (reason OPERATOR), which payment-svc's refund
// consumer handles like the saga's own. The provider refund is keyed by the
// intent, so asking again never refunds twice. No row is edited by hand.
//
// Only a booking in REFUND_REQUIRED qualifies: a confirmed booking is a sale
// (the state machine has no way back from it), a refunded one is done, and
// a pending or cancelled one has no captured payment to return.
func RequestRefundAgain(ctx context.Context, pool *pgxpool.Pool, pub Publisher, id uuid.UUID) (bookingdb.Booking, error) {
	b, err := bookingdb.New(pool).GetBooking(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return bookingdb.Booking{}, ErrNotFound
	}
	if err != nil {
		return bookingdb.Booking{}, err
	}
	if b.Status != StatusRefundRequired {
		return b, fmt.Errorf("%w: it is %s", ErrNotRefundable, b.Status)
	}
	payload, err := proto.Marshal(&eventsv1.BookingRefundRequired{
		BookingId: b.ID.String(), IntentId: uuidString(b.IntentID), AmountPaise: b.AmountPaise,
		Reason: eventsv1.RefundReason_REFUND_REASON_OPERATOR, RequiredAt: timestamppb.New(time.Now()),
	})
	if err != nil {
		return b, err
	}
	return b, pub.Publish(ctx, kafka.Event{
		Topic: kafka.TopicBooking, Key: b.ID.String(), Value: payload,
		Type: eventRefundRequired, Source: "holdfastctl",
	})
}

func uuidString(u uuid.NullUUID) string {
	if !u.Valid {
		return ""
	}
	return u.UUID.String()
}
