package grpcx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
)

// ServerConfig configures a gRPC server.
type ServerConfig struct {
	// Verifier checks the callers' service tokens.
	Verifier *authn.ServiceVerifier
	// Allow lists, per full method name ("/holdfast.inventory.v1.InventoryService/Confirm"),
	// the services that may call it. A method that is not listed is refused
	// to everyone: access is denied by default.
	Allow map[string][]string
}

type callerKey struct{}

// CallerFrom returns the authenticated calling service in a handler's context.
func CallerFrom(ctx context.Context) string {
	if c, ok := ctx.Value(callerKey{}).(*string); ok {
		return *c
	}
	return ""
}

// withCaller records the caller where the outer interceptors (logging) can
// read it too: they put an empty holder in the context first.
func withCaller(ctx context.Context, caller string) context.Context {
	if c, ok := ctx.Value(callerKey{}).(*string); ok {
		*c = caller
		return ctx
	}
	return context.WithValue(ctx, callerKey{}, &caller)
}

// NewServer returns a server with HoldFast's interceptors and the standard
// health service (always reported SERVING; readiness lives on the admin
// port). Register the API, then run it with Serve.
func NewServer(cfg ServerConfig, reg prometheus.Registerer, log *slog.Logger) *grpc.Server {
	m := newServerMetrics(reg)
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(recovery(log), observe(m, log), authenticate(cfg), requireDeadline()),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	return srv
}

// isHealth reports whether method belongs to the health service, which needs
// neither a token nor a deadline (probes call it).
func isHealth(method string) bool { return strings.HasPrefix(method, "/grpc.health.v1.Health/") }

func recovery(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if v := recover(); v != nil {
				log.ErrorContext(ctx, "grpc: panic recovered", "method", info.FullMethod, "panic", v, "stack", string(debug.Stack()))
				err = Error(codes.Internal, "INTERNAL", "internal error")
			}
		}()
		return h(ctx, req)
	}
}

func authenticate(cfg ServerConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if isHealth(info.FullMethod) {
			return h(ctx, req)
		}
		var token string
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get("authorization"); len(v) > 0 {
				token, _ = strings.CutPrefix(v[0], "Bearer ")
			}
		}
		if token == "" || cfg.Verifier == nil {
			return nil, Error(codes.Unauthenticated, "UNAUTHENTICATED", "a service token is required")
		}
		caller, err := cfg.Verifier.Verify(token)
		if err != nil {
			return nil, Error(codes.Unauthenticated, "UNAUTHENTICATED", "invalid service token")
		}
		if !slices.Contains(cfg.Allow[info.FullMethod], caller) {
			return nil, Error(codes.PermissionDenied, "PERMISSION_DENIED", fmt.Sprintf("%s may not call %s", caller, info.FullMethod))
		}
		return h(withCaller(ctx, caller), req)
	}
}

// requireDeadline refuses calls without a deadline: a call that may wait
// forever ties up the server when the caller has long given up.
func requireDeadline() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); !ok && !isHealth(info.FullMethod) {
			return nil, Error(codes.InvalidArgument, "DEADLINE_REQUIRED", "every call must carry a deadline")
		}
		if err := ctx.Err(); err != nil {
			return nil, Error(codes.DeadlineExceeded, "DEADLINE_EXCEEDED", "the caller's deadline has passed")
		}
		return h(ctx, req)
	}
}

type serverMetrics struct {
	handled  *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newServerMetrics(reg prometheus.Registerer) *serverMetrics {
	f := promauto.With(reg)
	return &serverMetrics{
		handled: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_grpc_server_handled_total",
			Help: "gRPC calls handled, by full method name and status code.",
		}, []string{"method", "code"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "holdfast_grpc_server_handling_seconds",
			Help:    "gRPC call latency by full method name.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"method"}),
	}
}

// observe records metrics and logs failed calls. Method labels are the
// registered method names, so they stay bounded.
func observe(m *serverMetrics, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		ctx = context.WithValue(ctx, callerKey{}, new(string)) // filled in by authenticate
		start := time.Now()
		resp, err := h(ctx, req)
		code := Code(err)
		m.handled.WithLabelValues(info.FullMethod, code.String()).Inc()
		m.duration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		if err != nil {
			level := slog.LevelWarn
			if code == codes.Internal || code == codes.Unknown {
				level = slog.LevelError
			}
			log.Log(ctx, level, "grpc call failed", "method", info.FullMethod, "code", code.String(),
				"reason", Reason(err), "caller", CallerFrom(ctx), "duration", time.Since(start), "err", err)
		}
		return resp, err
	}
}

// Server runs a gRPC server as an app.Component.
type Server struct {
	name string
	addr string
	srv  *grpc.Server
	log  *slog.Logger
}

// NewComponent wraps srv to listen on addr.
func NewComponent(name, addr string, srv *grpc.Server, log *slog.Logger) *Server {
	return &Server{name: name, addr: addr, srv: srv, log: log}
}

// Name implements app.Component.
func (s *Server) Name() string { return s.name }

// Run serves until ctx ends, then stops gracefully (in-flight calls finish,
// for at most 10 seconds).
func (s *Server) Run(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpcx: listen %s: %w", s.addr, err)
	}
	s.log.Info("grpc server listening", "server", s.name, "addr", ln.Addr().String())
	errc := make(chan error, 1)
	go func() { errc <- s.srv.Serve(ln) }()
	select {
	case err := <-errc:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("grpcx: serve: %w", err)
	case <-ctx.Done():
	}
	stopped := make(chan struct{})
	go func() { s.srv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		s.srv.Stop()
	}
	return nil
}
