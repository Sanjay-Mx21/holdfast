//go:build integration

package inventory

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"

	inventoryv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/inventory/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/grpcx"
)

// grpcClient serves f's service over gRPC on a real TCP port with the
// production interceptors and returns booking-svc's typed client.
func (f *fixture) grpcClient(t *testing.T) *Client { return f.grpcClientAs(t, "booking") }

// grpcClientAs is grpcClient for the service named caller.
func (f *fixture) grpcClientAs(t *testing.T, caller string) *Client {
	t.Helper()
	_, callerKey, _ := ed25519.GenerateKey(rand.Reader)
	v := authn.NewServiceVerifier("inventory", map[string]ed25519.PublicKey{caller: callerKey.Public().(ed25519.PublicKey)}, time.Second)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := grpcx.NewServer(grpcx.ServerConfig{Verifier: v, Allow: GRPCAllow()}, prometheus.NewRegistry(), quiet)
	inventoryv1.RegisterInventoryServiceServer(srv, NewGRPCServer(f.svc))
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	conn, err := grpcx.Dial(grpcx.ClientConfig{Target: ln.Addr().String(), Tokens: authn.NewServiceTokenSource(callerKey, caller, "inventory")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return NewClient(conn)
}

// TestGRPCContract drives a hold through checkout over gRPC and checks every
// answer matches the service layer's, errors included.
func TestGRPCContract(t *testing.T) {
	f := newFixture(t, 10, 4)
	c := f.grpcClient(t)
	user := uuid.NewString()
	res, err := f.create(user, "grpc-key-0001", 2)
	if err != nil {
		t.Fatal(err)
	}
	h := res.Hold

	got, err := c.GetHold(ctx, f.eventID, user, h.ID)
	if err != nil || got.ID != h.ID || got.Quantity != 2 || got.State != StateHeld || !got.ExpiresAt.Equal(h.ExpiresAt) {
		t.Fatalf("GetHold = %+v, %v; want %+v", got, err, h)
	}
	if _, err := c.GetHold(ctx, f.eventID, uuid.NewString(), h.ID); !errors.Is(err, ErrHoldNotFound) {
		t.Fatalf("another user's hold: %v, want ErrHoldNotFound", err)
	}
	if _, err := c.GetHold(ctx, f.eventID, user, "not-a-uuid"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a bad hold ID: %v, want ErrInvalidRequest", err)
	}

	until, err := c.MarkPaying(ctx, f.eventID, user, h.ID)
	if err != nil || time.Until(until) < time.Minute {
		t.Fatalf("MarkPaying = %s, %v; want the payment window", until, err)
	}
	again, err := c.MarkPaying(ctx, f.eventID, user, h.ID)
	if err != nil || !again.Equal(until) {
		t.Fatalf("MarkPaying retry = %s, %v; want the same deadline %s", again, err, until)
	}

	if out, err := c.Confirm(ctx, f.eventID, user, h.ID, 2); err != nil || out != ConfirmApplied {
		t.Fatalf("Confirm = %s, %v", out, err)
	}
	if out, err := c.Confirm(ctx, f.eventID, user, h.ID, 2); err != nil || out != ConfirmReplay {
		t.Fatalf("Confirm retry = %s, %v; want replay", out, err)
	}
	if _, err := c.Confirm(ctx, f.eventID, user, h.ID, 11); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Confirm with 11 units: %v, want ErrInvalidRequest", err)
	}
	if released, err := c.ReleaseForFailedPayment(ctx, f.eventID, user, h.ID); err != nil || released {
		t.Fatalf("release of a sold hold = %v, %v; want false", released, err)
	}
	if got, _ := c.GetHold(ctx, f.eventID, user, h.ID); got.State != StateSold {
		t.Fatalf("state %s, want SOLD", got.State)
	}
}

func TestGRPCExpiredAndFailedPayments(t *testing.T) {
	f := newFixture(t, 10, 4)
	c := f.grpcClient(t)
	user := uuid.NewString()

	// A hold that expired before checkout started cannot be protected.
	res, _ := f.create(user, "grpc-key-0002", 1)
	if err := f.rdb.HSet(ctx, keysFor(f.eventID).hold(res.Hold.ID), "expires_at", strconv.Itoa(1)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MarkPaying(ctx, f.eventID, user, res.Hold.ID); !errors.Is(err, ErrHoldExpired) {
		t.Fatalf("MarkPaying an expired hold: %v, want ErrHoldExpired", err)
	}

	// A failed payment returns the units, once.
	res, _ = f.create(user, "grpc-key-0003", 1)
	before := f.avail(t)
	if _, err := c.MarkPaying(ctx, f.eventID, user, res.Hold.ID); err != nil {
		t.Fatal(err)
	}
	if released, err := c.ReleaseForFailedPayment(ctx, f.eventID, user, res.Hold.ID); err != nil || !released {
		t.Fatalf("release = %v, %v", released, err)
	}
	if released, err := c.ReleaseForFailedPayment(ctx, f.eventID, user, res.Hold.ID); err != nil || released {
		t.Fatalf("second release = %v, %v; want false", released, err)
	}
	if after := f.avail(t); after != before+1 {
		t.Fatalf("available %d, want %d", after, before+1)
	}
	if _, err := c.GetHold(ctx, uuid.NewString(), user, res.Hold.ID); !errors.Is(err, ErrHoldNotFound) && !errors.Is(err, ErrEventNotProvisioned) {
		t.Fatalf("unknown event: %v", err)
	}
}

// TestGRPCAvailabilityForQueue: queue-svc reads units left, open holds and
// the freeze flag; booking-svc may not.
func TestGRPCAvailabilityForQueue(t *testing.T) {
	f := newFixture(t, 10, 4)
	if _, err := f.create(uuid.NewString(), "grpc-key-0001", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.SetFrozen(ctx, f.eventID, true); err != nil {
		t.Fatal(err)
	}
	a, err := f.grpcClientAs(t, "queue").GetAvailability(ctx, f.eventID)
	if err != nil || a.Available != 7 || a.Capacity != 10 || a.ActiveHolds != 1 || !a.Frozen {
		t.Fatalf("GetAvailability = %+v, %v; want 7 of 10, 1 open hold, frozen", a, err)
	}
	if _, err := f.grpcClientAs(t, "queue").GetAvailability(ctx, uuid.NewString()); !errors.Is(err, ErrEventNotProvisioned) {
		t.Fatalf("unknown event: %v, want ErrEventNotProvisioned", err)
	}
	if _, err := f.grpcClient(t).GetAvailability(ctx, f.eventID); grpcx.Code(err) != codes.PermissionDenied {
		t.Fatal("booking-svc must not read availability over gRPC")
	}
}
