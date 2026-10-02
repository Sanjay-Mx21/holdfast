package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// recordSpans installs a tracer provider that records spans, and the W3C
// propagator (otel.Setup does both in a service).
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })
	return rec
}

func TestPublishContinuesTheCallersTrace(t *testing.T) {
	rec := recordSpans(t)
	ctx, parent := otel.Tracer("test").Start(context.Background(), "request")
	span, headers := startPublish(ctx, Event{Topic: TopicBooking, Key: "b1", ID: "id-1"})
	span.End()
	parent.End()

	got := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(headers))
	sc := trace.SpanContextFromContext(got)
	if sc.TraceID() != parent.SpanContext().TraceID() || sc.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("traceparent %q does not point at the publish span in the caller's trace", headers[HeaderTraceParent])
	}
	ended := rec.Ended()
	if len(ended) != 2 || ended[0].Name() != "publish holdfast.booking.v1" || ended[0].Parent().SpanID() != parent.SpanContext().SpanID() ||
		ended[0].SpanKind() != trace.SpanKindProducer {
		t.Fatalf("publish span: %+v", ended[0])
	}
}

func TestPublishKeepsATraceStoredWithTheEvent(t *testing.T) {
	recordSpans(t)
	// An outbox stored the request's trace context with the event.
	ctx, original := otel.Tracer("test").Start(context.Background(), "original request")
	stored := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, stored)
	original.End()

	_, relay := otel.Tracer("test").Start(context.Background(), "relay tick")
	span, headers := startPublish(trace.ContextWithSpan(context.Background(), relay), Event{Topic: TopicBooking, Key: "b1", Headers: stored})
	span.End()
	relay.End()
	if span.SpanContext().TraceID() != original.SpanContext().TraceID() {
		t.Fatal("the publish joined the relay's trace instead of the stored one")
	}
	if headers[HeaderTraceParent] == stored[HeaderTraceParent] {
		t.Fatal("traceparent still points at the original span, not the publish span")
	}
	if stored[HeaderTraceParent] == headers[HeaderTraceParent] || len(stored) != 1 {
		t.Fatal("the event's own headers were modified")
	}
}

func TestProcessContinuesTheProducersTrace(t *testing.T) {
	rec := recordSpans(t)
	ctx, producer := otel.Tracer("test").Start(context.Background(), "publish")
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	producer.End()

	r := &kgo.Record{Topic: TopicPayment, Partition: 2, Offset: 41, Headers: []kgo.RecordHeader{
		{Key: HeaderTraceParent, Value: []byte(carrier[HeaderTraceParent])},
		{Key: HeaderID, Value: []byte("msg-1")},
	}}
	hctx, span := startProcess(context.Background(), r, "booking-svc")
	endSpan(span, nil)
	if trace.SpanContextFromContext(hctx).TraceID() != producer.SpanContext().TraceID() {
		t.Fatal("the handler's context is not in the producer's trace")
	}
	ended := rec.Ended()
	last := ended[len(ended)-1]
	if last.Name() != "process holdfast.payment.v1" || last.Parent().SpanID() != producer.SpanContext().SpanID() || last.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("process span: name %q parent %s kind %s", last.Name(), last.Parent().SpanID(), last.SpanKind())
	}
}

func TestProcessWithoutTraceContextStartsANewTrace(t *testing.T) {
	recordSpans(t)
	hctx, span := startProcess(context.Background(), &kgo.Record{Topic: TopicPayment}, "g")
	defer span.End()
	if !trace.SpanContextFromContext(hctx).IsValid() {
		t.Fatal("no span for a message without trace context")
	}
}
