package queue

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
)

// service is what the HTTP layer needs from *Service. Depending on this
// interface keeps handlers unit-testable without Valkey.
type service interface {
	Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error)
}

// Handler exposes the queue over HTTP.
type Handler struct {
	svc service
}

// NewHandler returns a Handler.
func NewHandler(svc service) *Handler { return &Handler{svc: svc} }

// Register mounts operator routes on internal (the admin port), behind
// operator authentication. Buyer-facing routes arrive with joining (task 2.2).
func (h *Handler) Register(internal *httpx.Router, operator httpx.Middleware) {
	internal.Handle("PUT /internal/v1/events/{eventID}/queue", operator(http.HandlerFunc(h.provision)))
}

type provisionRequest struct {
	OpensAt                time.Time `json:"opensAt"`
	AdmissionRatePerSecond int       `json:"admissionRatePerSecond"`
	MaxSessions            int       `json:"maxSessions"`
	SessionTTLSeconds      int       `json:"sessionTtlSeconds"`
}

func (h *Handler) provision(w http.ResponseWriter, r *http.Request) {
	var body provisionRequest
	if p := httpx.DecodeJSON(r, &body); p != nil {
		httpx.WriteProblem(w, r, p)
		return
	}
	eventID := r.PathValue("eventID")
	cfg := EventConfig{
		OpensAt:       body.OpensAt,
		AdmissionRate: body.AdmissionRatePerSecond,
		MaxSessions:   body.MaxSessions,
		SessionTTL:    time.Duration(body.SessionTTLSeconds) * time.Second,
	}
	created, err := h.svc.Provision(r.Context(), eventID, cfg)
	if err != nil {
		writeError(w, r, err)
		return
	}
	logging.FromContext(r.Context()).Info("queue provisioned",
		"event_id", eventID, "opens_at", cfg.OpensAt, "admission_rate", cfg.AdmissionRate,
		"max_sessions", cfg.MaxSessions, "session_ttl", cfg.SessionTTL, "created", created)
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	httpx.WriteJSON(w, status, map[string]any{"eventId": strings.ToLower(eventID), "created": created})
}

// writeError maps domain errors to problem responses. Anything unexpected is
// logged with the request ID and reported as a generic 500.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var p *httpx.Problem
	switch {
	case errors.As(err, &p):
	case errors.Is(err, ErrInvalidRequest):
		p = httpx.BadRequest("INVALID_REQUEST", strings.TrimPrefix(err.Error(), ErrInvalidRequest.Error()+": "))
	case errors.Is(err, ErrProvisionConflict):
		p = httpx.Conflict("PROVISION_CONFLICT", "queue already provisioned with different settings; see the queue runbook")
	case isUnavailable(err):
		logging.FromContext(r.Context()).Warn("dependency unavailable", "err", err)
		p = httpx.Unavailable("queue is temporarily unavailable; retry shortly", 1)
	default:
		logging.FromContext(r.Context()).Error("unhandled error", "err", err)
		p = httpx.Internal()
	}
	httpx.WriteProblem(w, r, p)
}

func isUnavailable(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, redis.ErrPoolTimeout) ||
		errors.As(err, &netErr)
}
