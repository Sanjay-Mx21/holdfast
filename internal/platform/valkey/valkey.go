// Package valkey builds the Valkey/Redis client (standalone, Sentinel or
// Cluster, chosen by configuration) and its readiness check.
package valkey

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
)

// New connects and verifies the server answers PING.
func New(ctx context.Context, cfg config.Valkey, clientName string) (redis.UniversalClient, error) {
	c := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        cfg.Addrs,
		MasterName:   cfg.MasterName,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		ClientName:   clientName,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
	c.AddHook(tracingHook{})
	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("valkey: ping %v: %w", cfg.Addrs, err)
	}
	return c, nil
}

// Check returns a readiness check for c.
func Check(c redis.UniversalClient) health.Check {
	return health.Check{Name: "valkey", Fn: func(ctx context.Context) error { return c.Ping(ctx).Err() }}
}

// tracingHook records a client span for every command or pipeline issued
// inside a trace (a request, a consumed message). Commands outside one, such
// as the sweeper's and the admission leader's ticks, record nothing: they run
// several times a second per event and would bury every request's trace.
type tracingHook struct{}

// tracer is looked up on every use: a tracer taken once at start-up would stay
// bound to whichever provider was installed first.
func tracer() trace.Tracer {
	return otel.Tracer("github.com/Sanjay-Mx21/holdfast/internal/platform/valkey")
}

func (tracingHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (tracingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !trace.SpanContextFromContext(ctx).IsValid() {
			return next(ctx, cmd)
		}
		name := strings.ToUpper(cmd.Name())
		ctx, span := tracer().Start(ctx, "valkey "+name, trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("db.system.name", "valkey"), attribute.String("db.operation.name", name)))
		defer span.End()
		err := next(ctx, cmd)
		record(span, err)
		return err
	}
}

func (tracingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !trace.SpanContextFromContext(ctx).IsValid() {
			return next(ctx, cmds)
		}
		ctx, span := tracer().Start(ctx, "valkey pipeline", trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("db.system.name", "valkey"), attribute.Int("db.operation.batch.size", len(cmds))))
		defer span.End()
		err := next(ctx, cmds)
		record(span, err)
		return err
	}
}

// record marks a failed command; a missing key (redis.Nil) is an answer,
// not a failure.
func record(span trace.Span, err error) {
	if err != nil && !errors.Is(err, redis.Nil) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
