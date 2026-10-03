package payment

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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

// traceContext captures ctx's trace, to be stored with an intent.
func traceContext(ctx context.Context) []byte {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	b, err := json.Marshal(carrier)
	if err != nil {
		return nil
	}
	return b
}

// continueTrace starts a span in the trace stored with the intent (the
// booking request that created it), linked to the caller's own trace (the
// provider's webhook, or a poll). What follows (the ledger, the event, and
// through the outbox booking-svc's saga and inventory) then belongs to the
// purchase's trace: one trace from the booking to the sale. Without a
// stored trace, the span is a child of ctx.
func continueTrace(ctx context.Context, in paymentdb.Intent, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("github.com/Sanjay-Mx21/holdfast/internal/payment")
	var carrier propagation.MapCarrier
	if len(in.TraceContext) == 0 || json.Unmarshal(in.TraceContext, &carrier) != nil {
		return tracer.Start(ctx, name)
	}
	stored := otel.GetTextMapPropagator().Extract(ctx, carrier)
	if !trace.SpanContextFromContext(stored).IsValid() {
		return tracer.Start(ctx, name)
	}
	return tracer.Start(stored, name, trace.WithLinks(trace.LinkFromContext(ctx)))
}
