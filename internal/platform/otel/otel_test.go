package otel

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestSetupWithoutAnExporterStillTracesAndPropagates(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	shutdown, exporting, err := Setup(context.Background(), "test-svc", "v0", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = shutdown(context.Background()) }()
	if exporting {
		t.Fatal("exporting without an endpoint")
	}
	ctx, span := otel.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	if !span.SpanContext().IsValid() {
		t.Fatal("spans have no IDs: log lines could not carry a trace_id")
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if carrier["traceparent"] == "" {
		t.Fatal("no traceparent injected")
	}
}

func TestSetupWithAnEndpointExports(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:1")
	shutdown, exporting, err := Setup(context.Background(), "test-svc", "v0", "test")
	if err != nil {
		t.Fatal(err)
	}
	if !exporting {
		t.Fatal("not exporting with an endpoint set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	_ = shutdown(ctx) // nothing listens on port 1; shutdown must still return
}
