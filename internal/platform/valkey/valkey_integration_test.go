//go:build integration

package valkey

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

func TestCommandsAreTracedOnlyInsideATrace(t *testing.T) {
	addr := os.Getenv("HOLDFAST_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("set HOLDFAST_TEST_VALKEY_ADDR to run Valkey integration tests")
	}
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	ctx := context.Background()
	c, err := New(ctx, config.Valkey{Addrs: []string{addr}, PoolSize: 2,
		DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	key := "test:trace:" + uuid.NewString()
	defer c.Del(ctx, key)

	// Outside a trace (a background tick): nothing recorded.
	if err := c.Set(ctx, key, "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Ended()); n != 0 {
		t.Fatalf("%d spans for commands outside any trace", n)
	}

	// Inside a request's trace: one client span per command, a missing key
	// is not an error, and pipelines get one span.
	rctx, req := otel.Tracer("test").Start(ctx, "request")
	_ = c.Get(rctx, key).Err()
	_ = c.Get(rctx, key+":missing").Err()
	pipe := c.Pipeline()
	pipe.Get(rctx, key)
	pipe.Incr(rctx, key+":n")
	_, _ = pipe.Exec(rctx)
	c.Del(rctx, key+":n")
	req.End()

	var names []string
	for _, s := range rec.Ended() {
		if s.Name() == "request" {
			continue
		}
		names = append(names, s.Name())
		if s.Parent().TraceID() != req.SpanContext().TraceID() || s.SpanKind() != trace.SpanKindClient {
			t.Fatalf("span %s is not a client span in the request's trace", s.Name())
		}
		if s.Name() == "valkey GET" && s.Status().Code != 0 {
			t.Fatalf("a missing key was recorded as an error: %+v", s.Status())
		}
	}
	want := []string{"valkey GET", "valkey GET", "valkey pipeline", "valkey DEL"}
	if len(names) != len(want) {
		t.Fatalf("spans %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("spans %v, want %v", names, want)
		}
	}
}
