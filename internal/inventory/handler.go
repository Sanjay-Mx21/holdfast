package inventory

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
)

// service is what the HTTP layer needs from *Service. Depending on this
// interface keeps handlers unit-testable without Valkey.
type service interface {
	CreateHold(ctx context.Context, req CreateHoldRequest) (HoldResult, error)
	GetHold(ctx context.Context, eventID, userID, holdID string) (Hold, error)
	CancelHold(ctx context.Context, eventID, userID, holdID string) error
	Availability(ctx context.Context, eventID string) (Availability, error)
	Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error)
	SetFrozen(ctx context.Context, eventID string, frozen bool) (bool, error)
}

// Handler exposes inventory over HTTP.
type Handler struct {
	svc service
}

// NewHandler returns a Handler.
func NewHandler(svc service) *Handler { return &Handler{svc: svc} }

// Register mounts buyer-facing routes on public and operator routes on
// internal (the admin port). admission authenticates buyers with their
// waiting-room token; operator authenticates staff.
func (h *Handler) Register(public, internal *httpx.Router, admission, operator httpx.Middleware) {
	public.Handle("POST /v1/events/{eventID}/holds", admission(http.HandlerFunc(h.createHold)))
	public.Handle("GET /v1/events/{eventID}/holds/{holdID}", admission(http.HandlerFunc(h.getHold)))
	public.Handle("DELETE /v1/events/{eventID}/holds/{holdID}", admission(http.HandlerFunc(h.cancelHold)))
	public.Handle("GET /v1/events/{eventID}/availability", http.HandlerFunc(h.availability))
	internal.Handle("PUT /internal/v1/events/{eventID}/inventory", operator(http.HandlerFunc(h.provision)))
	internal.Handle("POST /internal/v1/events/{eventID}/freeze", operator(h.setFrozen(true)))
	internal.Handle("POST /internal/v1/events/{eventID}/unfreeze", operator(h.setFrozen(false)))
}

// setFrozen freezes or unfreezes the event's holds (runbook RB-1). It is
// half of the freeze switch: queue-svc's admin endpoint pauses admissions,
// and holdfastctl freeze does both.
func (h *Handler) setFrozen(frozen bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		eventID := r.PathValue("eventID")
		changed, err := h.svc.SetFrozen(r.Context(), eventID, frozen)
		if err != nil {
			writeError(w, r, err)
			return
		}
		logging.FromContext(r.Context()).Info("sale freeze switched", "event_id", eventID, "frozen", frozen, "changed", changed)
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"eventId": strings.ToLower(eventID), "frozen": frozen, "changed": changed})
	})
}

type holdResponse struct {
	HoldID    string    `json:"holdId"`
	EventID   string    `json:"eventId"`
	Quantity  int       `json:"quantity"`
	State     HoldState `json:"state"`
	ExpiresAt time.Time `json:"expiresAt"`
	Remaining *int      `json:"remaining,omitempty"`
}

func toHoldResponse(h Hold, remaining *int) holdResponse {
	return holdResponse{
		HoldID: h.ID, EventID: h.EventID, Quantity: h.Quantity,
		State: h.State, ExpiresAt: h.ExpiresAt, Remaining: remaining,
	}
}

func (h *Handler) createHold(w http.ResponseWriter, r *http.Request) {
	claims, ok := admitted(w, r)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		httpx.WriteProblem(w, r, httpx.BadRequest("IDEMPOTENCY_KEY_REQUIRED", "the Idempotency-Key header is required"))
		return
	}
	var body struct {
		Quantity int `json:"quantity"`
	}
	if p := httpx.DecodeJSON(r, &body); p != nil {
		httpx.WriteProblem(w, r, p)
		return
	}
	res, err := h.svc.CreateHold(r.Context(), CreateHoldRequest{
		EventID:        r.PathValue("eventID"),
		UserID:         claims.Subject,
		IdempotencyKey: key,
		Quantity:       body.Quantity,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", "/v1/events/"+res.Hold.EventID+"/holds/"+res.Hold.ID)
	remaining := res.Remaining
	httpx.WriteJSON(w, http.StatusCreated, toHoldResponse(res.Hold, &remaining))
}

func (h *Handler) getHold(w http.ResponseWriter, r *http.Request) {
	claims, ok := admitted(w, r)
	if !ok {
		return
	}
	hold, err := h.svc.GetHold(r.Context(), r.PathValue("eventID"), claims.Subject, r.PathValue("holdID"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, toHoldResponse(hold, nil))
}

func (h *Handler) cancelHold(w http.ResponseWriter, r *http.Request) {
	claims, ok := admitted(w, r)
	if !ok {
		return
	}
	if err := h.svc.CancelHold(r.Context(), r.PathValue("eventID"), claims.Subject, r.PathValue("holdID")); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type availabilityResponse struct {
	EventID   string `json:"eventId"`
	Available int    `json:"available"`
	Capacity  int    `json:"capacity"`
	SoldOut   bool   `json:"soldOut"`
}

func (h *Handler) availability(w http.ResponseWriter, r *http.Request) {
	a, err := h.svc.Availability(r.Context(), r.PathValue("eventID"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	available := max(a.Available, 0)
	// Cacheable for one second: at surge time the CDN absorbs the polling
	// instead of Valkey, at the cost of numbers up to a second stale.
	w.Header().Set("Cache-Control", "public, max-age=1")
	httpx.WriteJSON(w, http.StatusOK, availabilityResponse{
		EventID: a.EventID, Available: available, Capacity: a.Capacity, SoldOut: a.SoldOut(),
	})
}

type provisionRequest struct {
	Capacity     int `json:"capacity"`
	PerUserLimit int `json:"perUserLimit"`
}

func (h *Handler) provision(w http.ResponseWriter, r *http.Request) {
	var body provisionRequest
	if p := httpx.DecodeJSON(r, &body); p != nil {
		httpx.WriteProblem(w, r, p)
		return
	}
	eventID := r.PathValue("eventID")
	created, err := h.svc.Provision(r.Context(), eventID, EventConfig{Capacity: body.Capacity, PerUserLimit: body.PerUserLimit})
	if err != nil {
		writeError(w, r, err)
		return
	}
	logging.FromContext(r.Context()).Info("inventory provisioned",
		"event_id", eventID, "capacity", body.Capacity, "per_user_limit", body.PerUserLimit, "created", created)
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	httpx.WriteJSON(w, status, map[string]any{"eventId": strings.ToLower(eventID), "created": created})
}

// admitted returns the caller's admission claims and checks the token was
// issued for the event in the path: a token for one event must never unlock
// another event's inventory.
func admitted(w http.ResponseWriter, r *http.Request) (*authn.AdmissionClaims, bool) {
	claims, ok := authn.AdmissionFrom(r.Context())
	if !ok {
		httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "admission token required"))
		return nil, false
	}
	if !strings.EqualFold(claims.EventID, r.PathValue("eventID")) {
		httpx.WriteProblem(w, r, httpx.Forbidden("TOKEN_EVENT_MISMATCH", "admission token was issued for a different event"))
		return nil, false
	}
	return claims, true
}

// writeError maps domain errors to problem responses. Anything unexpected is
// logged with the request ID and reported as a generic 500.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var p *httpx.Problem
	switch {
	case errors.As(err, &p):
	case errors.Is(err, ErrInvalidRequest):
		p = httpx.BadRequest("INVALID_REQUEST", strings.TrimPrefix(err.Error(), ErrInvalidRequest.Error()+": "))
	case errors.Is(err, ErrInvalidQuantity):
		p = httpx.Unprocessable("INVALID_QUANTITY", strings.TrimPrefix(err.Error(), ErrInvalidQuantity.Error()+": "))
	case errors.Is(err, ErrSoldOut):
		p = httpx.Conflict("SOLD_OUT", "no units left for this event")
	case errors.Is(err, ErrUserLimit):
		p = httpx.Unprocessable("USER_LIMIT", "per-user limit for this event reached")
	case errors.Is(err, ErrEventNotProvisioned):
		p = httpx.NotFound("EVENT_NOT_FOUND", "event is not on sale")
	case errors.Is(err, ErrHoldNotFound):
		p = httpx.NotFound("HOLD_NOT_FOUND", "hold not found")
	case errors.Is(err, ErrHoldExpired):
		p = httpx.Conflict("HOLD_EXPIRED", "hold expired or was released; retry with a new Idempotency-Key")
	case errors.Is(err, ErrHoldNotCancellable):
		p = httpx.Conflict("HOLD_NOT_CANCELLABLE", "hold is in checkout or already sold")
	case errors.Is(err, ErrIdempotencyKeyReused):
		p = httpx.Unprocessable("IDEMPOTENCY_KEY_REUSED", "Idempotency-Key was already used with a different quantity")
	case errors.Is(err, ErrProvisionConflict):
		p = httpx.Conflict("PROVISION_CONFLICT", "event already provisioned with different settings; see the inventory runbook")
	case errors.Is(err, ErrSalePaused):
		p = httpx.NewProblem(http.StatusServiceUnavailable, "SALE_PAUSED", "the sale is paused; your place is kept, retry after the time in Retry-After")
		p.RetryAfter = salePausedRetryAfter
	case isUnavailable(err):
		logging.FromContext(r.Context()).Warn("dependency unavailable", "err", err)
		p = httpx.Unavailable("inventory is temporarily unavailable; retry shortly", 1)
	default:
		logging.FromContext(r.Context()).Error("unhandled error", "err", err)
		p = httpx.Internal()
	}
	httpx.WriteProblem(w, r, p)
}

// salePausedRetryAfter is the Retry-After, in seconds, of a hold refused
// because the sale is frozen. How long a freeze lasts is an operator's call,
// so clients simply ask again at a gentle pace.
const salePausedRetryAfter = 10

func isUnavailable(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, redis.ErrPoolTimeout) ||
		errors.As(err, &netErr)
}
