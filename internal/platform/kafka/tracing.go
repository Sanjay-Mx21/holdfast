package kafka

import (
	"context"
	"maps"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HeaderTraceParent carries W3C trace context across Kafka, so a purchase is
// one trace from the HTTP request through every consumer.
const HeaderTraceParent = "traceparent"

// tracer is looked up on every use: a tracer taken once at start-up would stay
// bound to whichever provider was installed first.
func tracer() trace.Tracer {
	return otel.Tracer("github.com/Sanjay-Mx21/holdfast/internal/platform/kafka")
}

// startPublish starts the producer span of one event and returns the headers
// to send, with traceparent pointing at that span. An event that already
// carries a traceparent (one stored by an outbox when the business
// transaction ran) continues that trace rather than the publisher's, so the
// relay's hop joins the request that caused the event.
func startPublish(ctx context.Context, e Event) (trace.Span, map[string]string) {
	prop := otel.GetTextMapPropagator()
	parent := ctx
	if e.Headers[HeaderTraceParent] != "" {
		parent = prop.Extract(ctx, propagation.MapCarrier(e.Headers))
	}
	ctx, span := tracer().Start(parent, "publish "+e.Topic, trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", e.Topic),
			attribute.String("messaging.message.id", e.ID),
			attribute.String("messaging.kafka.message.key", e.Key),
		))
	headers := maps.Clone(e.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	prop.Inject(ctx, propagation.MapCarrier(headers))
	return span, headers
}

// startProcess starts the consumer span of one record, as a child of the
// producer span in its headers: the consumer hands its handler one message
// at a time, so the hop stays inside the producer's trace.
func startProcess(ctx context.Context, r *kgo.Record, group string) (context.Context, trace.Span) {
	carrier := propagation.MapCarrier{}
	for _, h := range r.Headers {
		carrier[h.Key] = string(h.Value)
	}
	parent := otel.GetTextMapPropagator().Extract(ctx, carrier)
	return tracer().Start(parent, "process "+r.Topic, trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", r.Topic),
			attribute.String("messaging.consumer.group.name", group),
			attribute.String("messaging.destination.partition.id", strconv.Itoa(int(r.Partition))),
			attribute.Int64("messaging.kafka.offset", r.Offset),
			attribute.String("messaging.message.id", carrier[HeaderID]),
		))
}

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
