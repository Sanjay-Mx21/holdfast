package grpcx

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
)

// ClientConfig configures a connection to another HoldFast service.
type ClientConfig struct {
	// Target is the server address, for example "inventory:7070".
	Target string
	// Tokens signs this service's tokens for the callee.
	Tokens *authn.ServiceTokenSource
	// Timeout is the deadline given to calls whose context has none
	// (default 800 ms, design doc 10.2). It covers every retry.
	Timeout time.Duration
	// MaxAttempts bounds tries per call, retries of UNAVAILABLE included
	// (default 3).
	MaxAttempts int
	// Options are extra dial options (tests use an in-memory dialer).
	Options []grpc.DialOption
}

// Dial returns a connection (lazily established) with tracing, service
// tokens, a default deadline and bounded retries.
func Dial(cfg ClientConfig) (*grpc.ClientConn, error) {
	if cfg.Target == "" || cfg.Tokens == nil {
		return nil, fmt.Errorf("grpcx: a client needs a target and a token source")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 800 * time.Millisecond
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 3
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithPerRPCCredentials(tokenCredentials{cfg.Tokens}),
		grpc.WithChainUnaryInterceptor(defaultDeadline(cfg.Timeout)),
	}, cfg.Options...)
	if cfg.MaxAttempts > 1 {
		opts = append(opts, grpc.WithDefaultServiceConfig(retryPolicy(cfg.MaxAttempts)))
	} else {
		opts = append(opts, grpc.WithDisableRetry()) // gRPC's retry policy needs at least 2 attempts
	}
	conn, err := grpc.NewClient(cfg.Target, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpcx: dial %s: %w", cfg.Target, err)
	}
	return conn, nil
}

// retryPolicy retries UNAVAILABLE (the server could not be reached or was
// shutting down) with exponential backoff. Other codes are answers, and are
// returned as they are.
func retryPolicy(attempts int) string {
	return fmt.Sprintf(`{"methodConfig": [{"name": [{}], "retryPolicy": {
		"maxAttempts": %d, "initialBackoff": "0.05s", "maxBackoff": "0.5s",
		"backoffMultiplier": 2, "retryableStatusCodes": ["UNAVAILABLE"]}}]}`, attempts)
}

func defaultDeadline(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// tokenCredentials attaches this service's token to every call.
type tokenCredentials struct{ src *authn.ServiceTokenSource }

func (t tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	tok, err := t.src.Token()
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

// RequireTransportSecurity is false: internal traffic is plaintext until
// Phase 6 adds TLS or a mesh (see the package comment).
func (tokenCredentials) RequireTransportSecurity() bool { return false }
