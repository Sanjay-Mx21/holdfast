package payment

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/paymentdb"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

// Event types (the ce_type header), from proto/holdfast/events/v1/payment.proto.
const (
	eventCaptured = "payment.captured.v1"
	eventFailed   = "payment.failed.v1"
	eventExpired  = "payment.expired.v1"
	eventRefunded = "refund.completed.v1"
)

// writeEvent adds an event to the outbox inside the caller's transaction,
// keyed by the booking ID so it stays in order with the booking's own events,
// and with the current trace context.
func writeEvent(ctx context.Context, q *paymentdb.Queries, bookingID uuid.UUID, eventType string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("payment: encode %s: %w", eventType, err)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers, err := json.Marshal(carrier)
	if err != nil {
		return err
	}
	return q.InsertOutboxEvent(ctx, paymentdb.InsertOutboxEventParams{
		EventID: uuid.Must(uuid.NewV7()), Topic: kafka.TopicPayment, AggregateID: bookingID,
		EventType: eventType, Payload: payload, Headers: headers,
	})
}
