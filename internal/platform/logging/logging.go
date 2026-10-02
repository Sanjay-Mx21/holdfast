// Package logging builds the structured (slog) logger every service uses and
// carries request-scoped loggers through context.Context.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
)

type ctxKey struct{}

// New builds the process logger. Every record carries the service,
// environment and version so logs from all services can be queried together.
func New(w io.Writer, level, format, service, environment string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("logging: invalid level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("logging: unknown format %q", format)
	}
	return slog.New(traceHandler{Handler: h}).With(
		slog.String("service", service),
		slog.String("env", environment),
		slog.String("version", buildinfo.Get().Version),
	), nil
}

// WithContext returns a copy of ctx that carries l.
func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext returns the request-scoped logger in ctx, or slog.Default().
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

// traceHandler adds trace_id and span_id to every record logged with a
// context that carries a span, so a log line leads straight to its trace. A
// logger that already carries a trace_id (a request-scoped logger, see
// httpx.AccessLog) only gets the span_id.
type traceHandler struct {
	slog.Handler
	hasTraceID bool
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		if !h.hasTraceID {
			r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
		}
		r.AddAttrs(slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	has := h.hasTraceID
	for _, a := range attrs {
		has = has || a.Key == "trace_id"
	}
	return traceHandler{Handler: h.Handler.WithAttrs(attrs), hasTraceID: has}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{Handler: h.Handler.WithGroup(name), hasTraceID: h.hasTraceID}
}
