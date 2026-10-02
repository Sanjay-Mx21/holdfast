package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestLogLinesCarryTheTraceID(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "info", "json", "svc", "test")
	if err != nil {
		t.Fatal(err)
	}
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	log.With("k", "v").InfoContext(ctx, "inside a span")
	log.InfoContext(context.Background(), "outside")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("%d lines", len(lines))
	}
	var in, out map[string]any
	_ = json.Unmarshal(lines[0], &in)
	_ = json.Unmarshal(lines[1], &out)
	if in["trace_id"] != span.SpanContext().TraceID().String() || in["span_id"] != span.SpanContext().SpanID().String() || in["k"] != "v" {
		t.Fatalf("line inside a span: %s", lines[0])
	}
	if _, ok := out["trace_id"]; ok {
		t.Fatalf("line outside a span has a trace_id: %s", lines[1])
	}
	if out["service"] != "svc" {
		t.Fatalf("service attribute lost: %s", lines[1])
	}
}

func TestARequestLoggerWithATraceIDIsNotStampedTwice(t *testing.T) {
	var buf bytes.Buffer
	log, _ := New(&buf, "info", "json", "svc", "test")
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	tid := span.SpanContext().TraceID().String()

	log.With("trace_id", tid).InfoContext(ctx, "request line")
	if n := bytes.Count(buf.Bytes(), []byte(`"trace_id"`)); n != 1 {
		t.Fatalf("trace_id appears %d times: %s", n, buf.Bytes())
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"span_id"`)) {
		t.Fatalf("span_id missing: %s", buf.Bytes())
	}
}
