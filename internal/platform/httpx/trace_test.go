package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })
	return rec
}

func TestTraceNamesSpansAfterRoutesAndContinuesTheCallersTrace(t *testing.T) {
	rec := recordSpans(t)
	var logs bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&logs, nil))
	rt := NewRouter(Trace(), RequestID(), AccessLog(base, true))
	var handlerTrace trace.TraceID
	rt.HandleFunc("GET /v1/events/{eventID}/status", func(w http.ResponseWriter, r *http.Request) {
		handlerTrace = trace.SpanContextFromContext(r.Context()).TraceID()
		w.WriteHeader(http.StatusOK)
	})

	// The caller (the edge, another service) sends its trace context.
	callerCtx, caller := otel.Tracer("test").Start(context.Background(), "caller")
	req := httptest.NewRequest(http.MethodGet, "/v1/events/0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77/status", nil)
	otel.GetTextMapPropagator().Inject(callerCtx, propagation.HeaderCarrier(req.Header))
	rt.ServeHTTP(httptest.NewRecorder(), req)
	caller.End()

	if handlerTrace != caller.SpanContext().TraceID() {
		t.Fatal("the handler did not run in the caller's trace")
	}
	var server sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			server = s
		}
	}
	if server == nil || server.Name() != "GET /v1/events/{eventID}/status" || server.Parent().SpanID() != caller.SpanContext().SpanID() {
		t.Fatalf("server span %v: want the route pattern as its name, child of the caller", server)
	}
	route := false
	for _, a := range server.Attributes() {
		route = route || a == attribute.String("http.route", "GET /v1/events/{eventID}/status")
	}
	if !route {
		t.Fatalf("server span lacks http.route: %v", server.Attributes())
	}

	var line map[string]any
	if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
		t.Fatalf("access log: %v: %s", err, logs.Bytes())
	}
	if line["trace_id"] != caller.SpanContext().TraceID().String() {
		t.Fatalf("access log line has trace_id %v, want %s", line["trace_id"], caller.SpanContext().TraceID())
	}
}

func TestTraceKeepsUnmatchedRequestsBounded(t *testing.T) {
	rec := recordSpans(t)
	rt := NewRouter(Trace())
	rt.HandleFunc("GET /known", func(w http.ResponseWriter, _ *http.Request) {})
	rt.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/random/path/12345", nil))
	ended := rec.Ended()
	if len(ended) != 1 || ended[0].Name() != "GET unmatched" {
		t.Fatalf("spans %v, want one named %q", ended, "GET unmatched")
	}
}
