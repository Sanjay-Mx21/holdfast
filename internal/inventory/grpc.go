package inventory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/inventory/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/grpcx"
)

// Error reasons on the internal gRPC API (proto/holdfast/inventory/v1).
const (
	reasonInvalidRequest      = "INVALID_REQUEST"
	reasonEventNotProvisioned = "EVENT_NOT_PROVISIONED"
	reasonHoldNotFound        = "HOLD_NOT_FOUND"
	reasonHoldExpired         = "HOLD_EXPIRED"
)

// GRPCServer is the gRPC adapter of the service layer: booking-svc's view of
// inventory. It adds no rules of its own.
type GRPCServer struct {
	inventoryv1.UnimplementedInventoryServiceServer
	svc *Service
}

// NewGRPCServer returns the adapter for svc.
func NewGRPCServer(svc *Service) *GRPCServer { return &GRPCServer{svc: svc} }

// GRPCAllow lets each caller use only what it needs: booking-svc drives a
// hold through checkout; queue-svc reads availability to pace admissions.
func GRPCAllow() map[string][]string {
	booking := []string{"booking"}
	return map[string][]string{
		inventoryv1.InventoryService_GetHold_FullMethodName:                 booking,
		inventoryv1.InventoryService_MarkPaying_FullMethodName:              booking,
		inventoryv1.InventoryService_Confirm_FullMethodName:                 booking,
		inventoryv1.InventoryService_ReleaseForFailedPayment_FullMethodName: booking,
		inventoryv1.InventoryService_GetAvailability_FullMethodName:         {"queue"},
	}
}

func ref(r *inventoryv1.HoldRef) (event, user, hold string) {
	return r.GetEventId(), r.GetUserId(), r.GetHoldId()
}

// GetHold implements inventoryv1.InventoryServiceServer.
func (g *GRPCServer) GetHold(ctx context.Context, req *inventoryv1.GetHoldRequest) (*inventoryv1.GetHoldResponse, error) {
	e, u, id := ref(req.GetHold())
	h, err := g.svc.GetHold(ctx, e, u, id)
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.GetHoldResponse{Hold: &inventoryv1.Hold{
		HoldId: h.ID, EventId: h.EventID, UserId: h.UserID, Quantity: int32(h.Quantity), //nolint:gosec // 1 to 10
		State: holdStateToProto[h.State], ExpiresAt: timestamppb.New(h.ExpiresAt),
	}}, nil
}

// MarkPaying implements inventoryv1.InventoryServiceServer.
func (g *GRPCServer) MarkPaying(ctx context.Context, req *inventoryv1.MarkPayingRequest) (*inventoryv1.MarkPayingResponse, error) {
	e, u, h := ref(req.GetHold())
	until, err := g.svc.MarkPaying(ctx, e, u, h)
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.MarkPayingResponse{ProtectedUntil: timestamppb.New(until)}, nil
}

// Confirm implements inventoryv1.InventoryServiceServer.
func (g *GRPCServer) Confirm(ctx context.Context, req *inventoryv1.ConfirmRequest) (*inventoryv1.ConfirmResponse, error) {
	e, u, h := ref(req.GetHold())
	out, err := g.svc.Confirm(ctx, e, u, h, int(req.GetQuantity()))
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.ConfirmResponse{Outcome: confirmToProto[out]}, nil
}

// ReleaseForFailedPayment implements inventoryv1.InventoryServiceServer.
func (g *GRPCServer) ReleaseForFailedPayment(ctx context.Context, req *inventoryv1.ReleaseForFailedPaymentRequest) (*inventoryv1.ReleaseForFailedPaymentResponse, error) {
	e, u, h := ref(req.GetHold())
	released, err := g.svc.ReleaseForFailedPayment(ctx, e, u, h)
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.ReleaseForFailedPaymentResponse{Released: released}, nil
}

// GetAvailability implements inventoryv1.InventoryServiceServer.
func (g *GRPCServer) GetAvailability(ctx context.Context, req *inventoryv1.GetAvailabilityRequest) (*inventoryv1.GetAvailabilityResponse, error) {
	a, err := g.svc.Availability(ctx, req.GetEventId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &inventoryv1.GetAvailabilityResponse{
		Available: int64(a.Available), Capacity: int64(a.Capacity),
		ActiveHolds: int64(a.ActiveHolds), Frozen: a.Frozen,
	}, nil
}

// toStatus maps domain errors to gRPC statuses with stable reasons.
func toStatus(err error) error {
	switch {
	case errors.Is(err, ErrInvalidRequest), errors.Is(err, ErrInvalidQuantity):
		return grpcx.Error(codes.InvalidArgument, reasonInvalidRequest, err.Error())
	case errors.Is(err, ErrEventNotProvisioned):
		return grpcx.Error(codes.NotFound, reasonEventNotProvisioned, "event not provisioned")
	case errors.Is(err, ErrHoldNotFound):
		return grpcx.Error(codes.NotFound, reasonHoldNotFound, "hold not found")
	case errors.Is(err, ErrHoldExpired):
		return grpcx.Error(codes.FailedPrecondition, reasonHoldExpired, "hold expired or released")
	case errors.Is(err, context.DeadlineExceeded):
		return grpcx.Error(codes.DeadlineExceeded, "DEADLINE_EXCEEDED", "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return grpcx.Error(codes.Canceled, "CANCELED", "call cancelled")
	case isUnavailable(err):
		// Valkey unreachable or failing over: UNAVAILABLE, which callers
		// (and grpcx's retry policy) treat as worth retrying (P57).
		return grpcx.Error(codes.Unavailable, "UNAVAILABLE", "inventory is temporarily unavailable")
	}
	return grpcx.Error(codes.Internal, "INTERNAL", "internal error")
}

var holdStateToProto = map[HoldState]inventoryv1.HoldState{
	StateHeld: inventoryv1.HoldState_HOLD_STATE_HELD, StatePaying: inventoryv1.HoldState_HOLD_STATE_PAYING,
	StateSold: inventoryv1.HoldState_HOLD_STATE_SOLD, StateReleased: inventoryv1.HoldState_HOLD_STATE_RELEASED,
}

var confirmToProto = map[ConfirmOutcome]inventoryv1.ConfirmOutcome{
	ConfirmApplied: inventoryv1.ConfirmOutcome_CONFIRM_OUTCOME_CONFIRMED,
	ConfirmReplay:  inventoryv1.ConfirmOutcome_CONFIRM_OUTCOME_REPLAY,
	ConfirmLate:    inventoryv1.ConfirmOutcome_CONFIRM_OUTCOME_LATE,
}

// Client is booking-svc's typed view of inventory over gRPC: the same
// methods and errors as the service layer, so callers never see protobuf.
type Client struct {
	c inventoryv1.InventoryServiceClient
}

// NewClient wraps a connection from grpcx.Dial.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{c: inventoryv1.NewInventoryServiceClient(conn)}
}

func holdRef(eventID, userID, holdID string) *inventoryv1.HoldRef {
	return &inventoryv1.HoldRef{EventId: eventID, UserId: userID, HoldId: holdID}
}

// GetHold returns a hold owned by userID.
func (c *Client) GetHold(ctx context.Context, eventID, userID, holdID string) (Hold, error) {
	resp, err := c.c.GetHold(ctx, &inventoryv1.GetHoldRequest{Hold: holdRef(eventID, userID, holdID)})
	if err != nil {
		return Hold{}, fromStatus(err)
	}
	h := resp.GetHold()
	var state HoldState
	for s, p := range holdStateToProto {
		if p == h.GetState() {
			state = s
		}
	}
	return Hold{ID: h.GetHoldId(), EventID: h.GetEventId(), UserID: h.GetUserId(), Quantity: int(h.GetQuantity()),
		State: state, ExpiresAt: h.GetExpiresAt().AsTime()}, nil
}

// MarkPaying protects a hold for the payment window and returns its end.
func (c *Client) MarkPaying(ctx context.Context, eventID, userID, holdID string) (time.Time, error) {
	resp, err := c.c.MarkPaying(ctx, &inventoryv1.MarkPayingRequest{Hold: holdRef(eventID, userID, holdID)})
	if err != nil {
		return time.Time{}, fromStatus(err)
	}
	return resp.GetProtectedUntil().AsTime(), nil
}

// Confirm marks a hold SOLD.
func (c *Client) Confirm(ctx context.Context, eventID, userID, holdID string, qty int) (ConfirmOutcome, error) {
	resp, err := c.c.Confirm(ctx, &inventoryv1.ConfirmRequest{Hold: holdRef(eventID, userID, holdID), Quantity: int32(qty)}) //nolint:gosec // validated by the server
	if err != nil {
		return "", fromStatus(err)
	}
	for o, p := range confirmToProto {
		if p == resp.GetOutcome() {
			return o, nil
		}
	}
	return "", fmt.Errorf("inventory: unknown confirm outcome %v", resp.GetOutcome())
}

// ReleaseForFailedPayment returns a hold's units after a payment failure.
func (c *Client) ReleaseForFailedPayment(ctx context.Context, eventID, userID, holdID string) (bool, error) {
	resp, err := c.c.ReleaseForFailedPayment(ctx, &inventoryv1.ReleaseForFailedPaymentRequest{Hold: holdRef(eventID, userID, holdID)})
	if err != nil {
		return false, fromStatus(err)
	}
	return resp.GetReleased(), nil
}

// GetAvailability reads an event's units left, open holds and freeze flag.
func (c *Client) GetAvailability(ctx context.Context, eventID string) (Availability, error) {
	resp, err := c.c.GetAvailability(ctx, &inventoryv1.GetAvailabilityRequest{EventId: eventID})
	if err != nil {
		return Availability{}, fromStatus(err)
	}
	return Availability{
		EventID: eventID, Available: int(resp.GetAvailable()), Capacity: int(resp.GetCapacity()),
		ActiveHolds: int(resp.GetActiveHolds()), Frozen: resp.GetFrozen(),
	}, nil
}

// fromStatus turns a status back into the domain error it came from; other
// failures (unreachable, deadline, unauthenticated) keep their status.
func fromStatus(err error) error {
	switch grpcx.Reason(err) {
	case reasonInvalidRequest:
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	case reasonEventNotProvisioned:
		return fmt.Errorf("%w: %w", ErrEventNotProvisioned, err)
	case reasonHoldNotFound:
		return fmt.Errorf("%w: %w", ErrHoldNotFound, err)
	case reasonHoldExpired:
		return fmt.Errorf("%w: %w", ErrHoldExpired, err)
	}
	return fmt.Errorf("inventory: %w", err)
}
