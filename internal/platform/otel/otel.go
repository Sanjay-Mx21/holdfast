// Package otel sets up OpenTelemetry tracing for a HoldFast process: W3C
// trace context propagation always, and export over OTLP/HTTP to a collector
// when OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is
// set. Sampling follows the standard OTEL_TRACES_SAMPLER and
// OTEL_TRACES_SAMPLER_ARG variables (default: parent-based, always on).
//
// Spans are created by the transports (httpx, the Kafka client, Valkey calls
// inside a request); services rarely need the API directly.
package otel

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup installs the global tracer provider and propagator. The returned
// function flushes buffered spans and must be called on shutdown. exporting
// reports whether spans leave the process.
func Setup(ctx context.Context, service, version, environment string) (shutdown func(context.Context) error, exporting bool, err error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", service),
			attribute.String("service.version", version),
			attribute.String("deployment.environment.name", environment),
		),
		resource.WithFromEnv(), // OTEL_RESOURCE_ATTRIBUTES
	)
	if err != nil {
		return nil, false, fmt.Errorf("otel: resource: %w", err)
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx) // endpoint, headers and timeouts from the standard variables
		if err != nil {
			return nil, false, fmt.Errorf("otel: exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
		exporting = true
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, exporting, nil
}
