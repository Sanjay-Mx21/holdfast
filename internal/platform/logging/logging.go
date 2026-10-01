// Package logging builds the structured (slog) logger every service uses and
// carries request-scoped loggers through context.Context.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"

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
	return slog.New(h).With(
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
