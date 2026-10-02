package booking

import (
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/grpcx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
)

// Handler is booking-svc's public HTTP API.
type Handler struct {
	svc *Service
}

// NewHandler returns the API for svc.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Register adds the routes; identity authenticates buyers.
func (h *Handler) Register(public *httpx.Router, identity httpx.Middleware) {
	public.Handle("POST /v1/bookings", identity(http.HandlerFunc(h.create)))
	public.Handle("GET /v1/bookings/{bookingID}", identity(http.HandlerFunc(h.get)))
}

type createBody struct {
	EventID string `json:"eventId"`
	HoldID  string `json:"holdId"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	user, ok := authn.UserFrom(r.Context())
	if !ok {
		httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "authentication required"))
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		httpx.WriteProblem(w, r, httpx.BadRequest("IDEMPOTENCY_KEY_REQUIRED", "send an Idempotency-Key header, and reuse it to retry"))
		return
	}
	var body createBody
	if p := httpx.DecodeJSON(r, &body); p != nil {
		httpx.WriteProblem(w, r, p)
		return
	}
	res, err := h.svc.Create(r.Context(), CreateRequest{UserID: user, EventID: body.EventID, HoldID: body.HoldID, IdempotencyKey: key})
	if p, ok := Problem(err); ok {
		httpx.WriteProblem(w, r, p)
		return
	}
	if err != nil {
		writeTransient(w, r, err)
		return
	}
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	ct := "application/json"
	if res.Code >= 400 {
		ct = "application/problem+json"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(res.Code)
	_, _ = w.Write(res.Body)
}

// writeTransient answers a failure that a retry with the same key can fix.
func writeTransient(w http.ResponseWriter, r *http.Request, err error) {
	switch grpcx.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		logging.FromContext(r.Context()).WarnContext(r.Context(), "booking: inventory unavailable", "err", err)
		httpx.WriteProblem(w, r, httpx.Unavailable("inventory is unavailable; retry with the same Idempotency-Key", 1))
		return
	}
	logging.FromContext(r.Context()).ErrorContext(r.Context(), "booking: create failed", "err", err)
	httpx.WriteProblem(w, r, httpx.Internal())
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	user, ok := authn.UserFrom(r.Context())
	if !ok {
		httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "authentication required"))
		return
	}
	v, err := h.svc.Get(r.Context(), user, r.PathValue("bookingID"))
	switch {
	case errors.Is(err, ErrNotFound):
		httpx.WriteProblem(w, r, httpx.NotFound("BOOKING_NOT_FOUND", "no such booking"))
	case err != nil:
		logging.FromContext(r.Context()).ErrorContext(r.Context(), "booking: get failed", "err", err)
		httpx.WriteProblem(w, r, httpx.Internal())
	default:
		httpx.WriteJSON(w, http.StatusOK, v)
	}
}
