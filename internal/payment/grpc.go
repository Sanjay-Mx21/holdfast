package payment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	paymentv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/payment/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/grpcx"
)

// Error reasons on the internal gRPC API (proto/holdfast/payment/v1).
const (
	reasonInvalidRequest = "INVALID_REQUEST"
	reasonIntentConflict = "INTENT_CONFLICT"
	reasonPSPUnavailable = "PSP_UNAVAILABLE"
)

// GRPCServer adapts the service to holdfast.payment.v1.PaymentService.
type GRPCServer struct {
	paymentv1.UnimplementedPaymentServiceServer
	svc *Service
}

// NewGRPCServer returns the adapter for svc.
func NewGRPCServer(svc *Service) *GRPCServer { return &GRPCServer{svc: svc} }

// GRPCAllow lets booking-svc, and only it, create intents.
func GRPCAllow() map[string][]string {
	return map[string][]string{paymentv1.PaymentService_CreateIntent_FullMethodName: {"booking"}}
}

// CreateIntent implements paymentv1.PaymentServiceServer.
func (g *GRPCServer) CreateIntent(ctx context.Context, req *paymentv1.CreateIntentRequest) (*paymentv1.CreateIntentResponse, error) {
	booking, err1 := uuid.Parse(req.GetBookingId())
	event, err2 := uuid.Parse(req.GetEventId())
	if err1 != nil || err2 != nil || req.GetExpiresAt() == nil {
		return nil, grpcx.Error(codes.InvalidArgument, reasonInvalidRequest, "booking_id and event_id must be UUIDs, and expires_at is required")
	}
	in, err := g.svc.CreateIntent(ctx, booking, event, req.GetAmountPaise(), req.GetExpiresAt().AsTime())
	switch {
	case errors.Is(err, ErrInvalidRequest):
		return nil, grpcx.Error(codes.InvalidArgument, reasonInvalidRequest, err.Error())
	case errors.Is(err, ErrIntentConflict):
		return nil, grpcx.Error(codes.FailedPrecondition, reasonIntentConflict, err.Error())
	case errors.Is(err, ErrPSPUnavailable):
		return nil, grpcx.Error(codes.Unavailable, reasonPSPUnavailable, "the payment provider is unavailable")
	case err != nil:
		return nil, grpcx.Error(codes.Internal, "INTERNAL", "internal error")
	}
	return &paymentv1.CreateIntentResponse{IntentId: in.ID.String(), CheckoutUrl: in.CheckoutURL}, nil
}

// Client is booking-svc's view of payment-svc: it satisfies booking.Intents.
type Client struct {
	c paymentv1.PaymentServiceClient
}

// NewClient wraps a connection from grpcx.Dial.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{c: paymentv1.NewPaymentServiceClient(conn)}
}

// CreateIntent returns the booking's intent and checkout URL (idempotent).
func (c *Client) CreateIntent(ctx context.Context, bookingID, eventID uuid.UUID, amountPaise int64, expiresAt time.Time) (uuid.UUID, string, error) {
	resp, err := c.c.CreateIntent(ctx, &paymentv1.CreateIntentRequest{
		BookingId: bookingID.String(), EventId: eventID.String(), AmountPaise: amountPaise,
		ExpiresAt: timestamppb.New(expiresAt),
	})
	if err != nil {
		switch grpcx.Reason(err) {
		case reasonIntentConflict:
			return uuid.Nil, "", fmt.Errorf("%w: %w", ErrIntentConflict, err)
		case reasonPSPUnavailable:
			return uuid.Nil, "", fmt.Errorf("%w: %w", ErrPSPUnavailable, err)
		}
		return uuid.Nil, "", fmt.Errorf("payment: %w", err)
	}
	id, err := uuid.Parse(resp.GetIntentId())
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("payment: bad intent ID %q", resp.GetIntentId())
	}
	return id, resp.GetCheckoutUrl(), nil
}
