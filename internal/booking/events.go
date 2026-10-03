package booking

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/bookingdb"
	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

// Event types (the ce_type header), from proto/holdfast/events/v1/booking.proto.
const (
	eventCreated        = "booking.created.v1"
	eventConfirmed      = "booking.confirmed.v1"
	eventCancelled      = "booking.cancelled.v1"
	eventRefundRequired = "booking.refund_required.v1"
	eventRefunded       = "booking.refunded.v1"
)

// writeEvent adds an event to the outbox, inside the caller's transaction. It
// stores the current trace context, so the relay's publish (task 3.7) and
// every consumer join the trace of the request that caused the event.
func writeEvent(ctx context.Context, q *bookingdb.Queries, aggregate uuid.UUID, eventType string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("booking: encode %s: %w", eventType, err)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers, err := json.Marshal(carrier)
	if err != nil {
		return err
	}
	return q.InsertOutboxEvent(ctx, bookingdb.InsertOutboxEventParams{
		EventID: uuid.Must(uuid.NewV7()), Topic: kafka.TopicBooking, AggregateID: aggregate,
		EventType: eventType, Payload: payload, Headers: headers,
	})
}

func createdEvent(b bookingdb.Booking) *eventsv1.BookingCreated {
	return &eventsv1.BookingCreated{
		BookingId: b.ID.String(), EventId: b.EventID.String(), UserId: b.UserID.String(), HoldId: b.HoldID.String(),
		Quantity: int32(b.Qty), AmountPaise: b.AmountPaise,
		PaymentDeadline: timestamppb.New(b.PaymentDeadline), CreatedAt: timestamppb.New(b.CreatedAt),
	}
}

func cancelledEvent(b bookingdb.Booking, reason eventsv1.CancellationReason) *eventsv1.BookingCancelled {
	return &eventsv1.BookingCancelled{
		BookingId: b.ID.String(), EventId: b.EventID.String(), UserId: b.UserID.String(), HoldId: b.HoldID.String(),
		Reason: reason, CancelledAt: timestamppb.New(b.UpdatedAt),
	}
}
