package grpcx

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	inventoryv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/inventory/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
)

// fakeInventory answers GetHold however the test says.
type fakeInventory struct {
	inventoryv1.UnimplementedInventoryServiceServer
	calls   atomic.Int64
	getHold func(ctx context.Context) (*inventoryv1.GetHoldResponse, error)
	lastCtx atomic.Value
}

func (f *fakeInventory) GetHold(ctx context.Context, _ *inventoryv1.GetHoldRequest) (*inventoryv1.GetHoldResponse, error) {
	f.calls.Add(1)
	f.lastCtx.Store(ctx)
	return f.getHold(ctx)
}

type rig struct {
	fake    *fakeInventory
	reg     *prometheus.Registry
	dialer  func(context.Context, string) (net.Conn, error)
	booking *authn.ServiceTokenSource
	other   *authn.ServiceTokenSource
}

func newRig(t *testing.T) *rig {
	t.Helper()
	key := func() ed25519.PrivateKey { _, k, _ := ed25519.GenerateKey(rand.Reader); return k }
	bookingKey, auditorKey := key(), key()
	v := authn.NewServiceVerifier("inventory", map[string]ed25519.PublicKey{
		"booking": bookingKey.Public().(ed25519.PublicKey),
		"auditor": auditorKey.Public().(ed25519.PublicKey),
	}, time.Second)
	r := &rig{
		fake: &fakeInventory{getHold: func(context.Context) (*inventoryv1.GetHoldResponse, error) {
			return &inventoryv1.GetHoldResponse{}, nil
		}},
		reg:     prometheus.NewRegistry(),
		booking: authn.NewServiceTokenSource(bookingKey, "booking", "inventory"),
		other:   authn.NewServiceTokenSource(auditorKey, "auditor", "inventory"),
	}
	srv := NewServer(ServerConfig{Verifier: v, Allow: map[string][]string{
		inventoryv1.InventoryService_GetHold_FullMethodName: {"booking"},
	}}, r.reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	inventoryv1.RegisterInventoryServiceServer(srv, r.fake)
	ln := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	r.dialer = func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }
	return r
}

// client dials with grpcx's client settings.
func (r *rig) client(t *testing.T, tokens *authn.ServiceTokenSource, attempts int) inventoryv1.InventoryServiceClient {
	t.Helper()
	conn, err := Dial(ClientConfig{Target: "passthrough:///bufnet", Tokens: tokens, Timeout: 800 * time.Millisecond, MaxAttempts: attempts,
		Options: []grpc.DialOption{grpc.WithContextDialer(r.dialer)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return inventoryv1.NewInventoryServiceClient(conn)
}

func (r *rig) handled(t *testing.T, code codes.Code) float64 {
	t.Helper()
	m, err := r.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, mf := range m {
		if mf.GetName() != "holdfast_grpc_server_handled_total" {
			continue
		}
		for _, s := range mf.GetMetric() {
			for _, l := range s.GetLabel() {
				if l.GetName() == "code" && l.GetValue() == code.String() {
					total += s.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

func TestAuthenticatedCallerReachesTheHandler(t *testing.T) {
	r := newRig(t)
	if _, err := r.client(t, r.booking, 3).GetHold(context.Background(), &inventoryv1.GetHoldRequest{}); err != nil {
		t.Fatal(err)
	}
	ctx := r.fake.lastCtx.Load().(context.Context)
	if CallerFrom(ctx) != "booking" {
		t.Fatalf("caller %q, want booking", CallerFrom(ctx))
	}
	if d, ok := ctx.Deadline(); !ok || time.Until(d) > time.Second {
		t.Fatalf("handler deadline %v %v: want the client's default of 800ms", d, ok)
	}
	if r.handled(t, codes.OK) != 1 {
		t.Fatal("OK call not counted")
	}
}

func TestCallsWithoutAValidTokenOrPermissionAreRefused(t *testing.T) {
	r := newRig(t)
	// Not allowed: auditor is trusted but may not call GetHold.
	_, err := r.client(t, r.other, 1).GetHold(context.Background(), &inventoryv1.GetHoldRequest{})
	if Code(err) != codes.PermissionDenied || Reason(err) != "PERMISSION_DENIED" {
		t.Fatalf("auditor: %v", err)
	}
	// No token at all.
	conn, _ := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(r.dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	// Only a guard against a hang: it covers the health check below too, and
	// one second ran out under a loaded -race run (P50).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = inventoryv1.NewInventoryServiceClient(conn).GetHold(ctx, &inventoryv1.GetHoldRequest{})
	if Code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
	// Not registered in the allowlist: refused to everyone.
	_, err = r.client(t, r.booking, 1).Confirm(context.Background(), &inventoryv1.ConfirmRequest{})
	if Code(err) != codes.PermissionDenied {
		t.Fatalf("unlisted method: %v", err)
	}
	if r.fake.calls.Load() != 0 {
		t.Fatal("a refused call reached the handler")
	}
	// The health service needs no token, so probes work.
	hc, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || hc.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health: %v %v", hc, err)
	}
}

func TestCallsWithoutADeadlineAreRefused(t *testing.T) {
	r := newRig(t)
	conn, _ := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(r.dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(tokenCredentials{r.booking}))
	defer conn.Close()
	_, err := inventoryv1.NewInventoryServiceClient(conn).GetHold(context.Background(), &inventoryv1.GetHoldRequest{})
	if Code(err) != codes.InvalidArgument || Reason(err) != "DEADLINE_REQUIRED" {
		t.Fatalf("no deadline: %v", err)
	}
}

func TestUnavailableIsRetriedWithinBounds(t *testing.T) {
	r := newRig(t)
	r.fake.getHold = func(context.Context) (*inventoryv1.GetHoldResponse, error) {
		if r.fake.calls.Load() < 3 {
			return nil, Error(codes.Unavailable, "SHUTTING_DOWN", "try another replica")
		}
		return &inventoryv1.GetHoldResponse{}, nil
	}
	if _, err := r.client(t, r.booking, 3).GetHold(context.Background(), &inventoryv1.GetHoldRequest{}); err != nil {
		t.Fatalf("with 3 attempts: %v", err)
	}
	if n := r.fake.calls.Load(); n != 3 {
		t.Fatalf("%d attempts, want 3", n)
	}

	r.fake.calls.Store(0)
	_, err := r.client(t, r.booking, 2).GetHold(context.Background(), &inventoryv1.GetHoldRequest{})
	if Code(err) != codes.Unavailable || r.fake.calls.Load() != 2 {
		t.Fatalf("with 2 attempts: %v after %d calls, want UNAVAILABLE after 2", err, r.fake.calls.Load())
	}

	// Answers are not retried.
	r.fake.calls.Store(0)
	r.fake.getHold = func(context.Context) (*inventoryv1.GetHoldResponse, error) {
		return nil, Error(codes.NotFound, "HOLD_NOT_FOUND", "no")
	}
	_, err = r.client(t, r.booking, 3).GetHold(context.Background(), &inventoryv1.GetHoldRequest{})
	if Reason(err) != "HOLD_NOT_FOUND" || r.fake.calls.Load() != 1 {
		t.Fatalf("NOT_FOUND: %v after %d calls, want one call", err, r.fake.calls.Load())
	}
}

func TestPanicsBecomeInternalErrors(t *testing.T) {
	r := newRig(t)
	r.fake.getHold = func(context.Context) (*inventoryv1.GetHoldResponse, error) { panic("boom") }
	c := r.client(t, r.booking, 1)
	_, err := c.GetHold(context.Background(), &inventoryv1.GetHoldRequest{})
	if Code(err) != codes.Internal {
		t.Fatalf("panic: %v", err)
	}
	r.fake.getHold = func(context.Context) (*inventoryv1.GetHoldResponse, error) {
		return &inventoryv1.GetHoldResponse{}, nil
	}
	if _, err := c.GetHold(context.Background(), &inventoryv1.GetHoldRequest{}); err != nil {
		t.Fatalf("server did not survive the panic: %v", err)
	}
}

func TestErrorReasons(t *testing.T) {
	err := Error(codes.FailedPrecondition, "HOLD_EXPIRED", "expired")
	if Code(err) != codes.FailedPrecondition || Reason(err) != "HOLD_EXPIRED" {
		t.Fatalf("round trip: %v %q", Code(err), Reason(err))
	}
	if Reason(errors.New("plain")) != "" || Code(errors.New("plain")) != codes.Unknown || Code(nil) != codes.OK {
		t.Fatal("non-status errors")
	}
}

func TestDialNeedsATargetAndTokens(t *testing.T) {
	if _, err := Dial(ClientConfig{Target: "x"}); err == nil {
		t.Fatal("dialled without a token source")
	}
}
