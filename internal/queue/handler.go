package queue

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/ratelimit"
)

// service is what the HTTP layer needs from *Service. Depending on this
// interface keeps handlers unit-testable without Valkey.
type service interface {
	Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error)
	Join(ctx context.Context, eventID, userID string) (JoinResult, error)
}

// limiter is what the HTTP layer needs from *ratelimit.Limiter.
type limiter interface {
	Allow(ctx context.Context, scope, id string, rule ratelimit.Rule) (ratelimit.Decision, error)
}

// JoinLimits are the token buckets applied to joining: one per client IP
// (an IPv6 client counts per /64 network) and one per user.
type JoinLimits struct {
	PerIP   ratelimit.Rule
	PerUser ratelimit.Rule
}

// Rate-limit scopes (the SCOPE part of rl:SCOPE:ID keys).
const (
	scopeJoinIP   = "join-ip"
	scopeJoinUser = "join-user"
)

// Handler exposes the queue over HTTP.
type Handler struct {
	svc    service
	lim    limiter
	limits JoinLimits
	m      *Metrics
}

// NewHandler returns a Handler.
func NewHandler(svc service, lim limiter, limits JoinLimits, m *Metrics) *Handler {
	return &Handler{svc: svc, lim: lim, limits: limits, m: m}
}

// Register mounts buyer-facing routes on public, behind identity (who the
// caller is), and operator routes on internal (the admin port), behind
// operator authentication.
func (h *Handler) Register(public, internal *httpx.Router, identity, operator httpx.Middleware) {
	public.Handle("POST /v1/queue/{eventID}/join", identity(http.HandlerFunc(h.join)))
	internal.Handle("PUT /internal/v1/events/{eventID}/queue", operator(http.HandlerFunc(h.provision)))
}

type joinResponse struct {
	EventID  string   `json:"eventId"`
	Joined   bool     `json:"joined"`
	Ordering Ordering `json:"ordering"`
}

func (h *Handler) join(w http.ResponseWriter, r *http.Request) {
	user, ok := authn.UserFrom(r.Context())
	if !ok {
		httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "authentication required"))
		return
	}
	// Per-IP first: it is the cheaper signal against one machine hammering
	// with many identities. Then per user, against one identity on many IPs.
	if !h.allow(w, r, scopeJoinIP, clientID(r), h.limits.PerIP, joinRateLimitedIP) ||
		!h.allow(w, r, scopeJoinUser, user, h.limits.PerUser, joinRateLimitUser) {
		return
	}
	res, err := h.svc.Join(r.Context(), r.PathValue("eventID"), user)
	if err != nil {
		h.m.join(joinResultOf(err))
		writeError(w, r, err)
		return
	}
	if res.Joined {
		h.m.join(joinJoined)
	} else {
		h.m.join(joinAlready)
	}
	if res.openedQueue {
		h.m.transition(openedByJoin)
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusAccepted, joinResponse{EventID: res.EventID, Joined: res.Joined, Ordering: res.Ordering})
}

// allow takes a token for (scope, id). When the bucket is empty it writes
// 429 with Retry-After and returns false.
func (h *Handler) allow(w http.ResponseWriter, r *http.Request, scope, id string, rule ratelimit.Rule, result string) bool {
	d, err := h.lim.Allow(r.Context(), scope, id, rule)
	if err != nil {
		h.m.join(joinError)
		writeError(w, r, err)
		return false
	}
	if d.Allowed {
		return true
	}
	h.m.join(result)
	p := httpx.NewProblem(http.StatusTooManyRequests, "RATE_LIMITED", "too many join attempts; retry after the time in Retry-After")
	p.RetryAfter = int(math.Ceil(d.RetryAfter.Seconds()))
	if p.RetryAfter < 1 {
		p.RetryAfter = 1
	}
	httpx.WriteProblem(w, r, p)
	return false
}

// clientID turns the connection's remote address into a rate-limit ID. An
// IPv6 subscriber usually owns a whole /64 and can rotate addresses inside
// it, so IPv6 clients are limited per /64. Colons become underscores to keep
// the key unambiguous. Behind a proxy every request shares the proxy's
// address; trusting a forwarded-for header is left to the NGINX task (2.9).
func clientID(r *http.Request) string {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return "unknown"
	}
	a := ap.Addr().Unmap().WithZone("")
	if a.Is6() {
		p, _ := a.Prefix(64)
		a = p.Addr()
	}
	return strings.ReplaceAll(a.String(), ":", "_")
}

func joinResultOf(err error) string {
	switch {
	case errors.Is(err, ErrQueueClosed):
		return joinClosed
	case errors.Is(err, ErrEventNotFound):
		return joinNotFound
	case errors.Is(err, ErrInvalidRequest):
		return joinInvalid
	default:
		return joinError
	}
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
	case errors.Is(err, ErrEventNotFound):
		p = httpx.NotFound("EVENT_NOT_FOUND", "this event has no waiting room")
	case errors.Is(err, ErrQueueClosed):
		p = httpx.Conflict("QUEUE_CLOSED", "the waiting room for this event is closed")
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
