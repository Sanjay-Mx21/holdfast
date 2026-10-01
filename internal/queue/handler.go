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
	Position(ctx context.Context, eventID, userID string) (Position, error)
	Status(ctx context.Context, eventID string) (Status, error)
}

// limiter is what the HTTP layer needs from *ratelimit.Limiter.
type limiter interface {
	Allow(ctx context.Context, scope, id string, rule ratelimit.Rule) (ratelimit.Decision, error)
}

// Limits are the token buckets applied to buyer requests. Joining passes one
// per client IP (an IPv6 client counts per /64 network) and one per user;
// position lookups pass one per user, since clients should ask once after T0
// and then follow the shared status document.
type Limits struct {
	JoinPerIP       ratelimit.Rule
	JoinPerUser     ratelimit.Rule
	PositionPerUser ratelimit.Rule
}

// Rate-limit scopes (the SCOPE part of rl:SCOPE:ID keys).
const (
	scopeJoinIP       = "join-ip"
	scopeJoinUser     = "join-user"
	scopePositionUser = "position-user"
)

// Handler exposes the queue over HTTP.
type Handler struct {
	svc    service
	lim    limiter
	limits Limits
	m      *Metrics
}

// NewHandler returns a Handler.
func NewHandler(svc service, lim limiter, limits Limits, m *Metrics) *Handler {
	return &Handler{svc: svc, lim: lim, limits: limits, m: m}
}

// Register mounts buyer-facing routes on public, behind identity (who the
// caller is), and operator routes on internal (the admin port), behind
// operator authentication.
func (h *Handler) Register(public, internal *httpx.Router, identity, operator httpx.Middleware) {
	public.Handle("POST /v1/queue/{eventID}/join", identity(http.HandlerFunc(h.join)))
	public.Handle("GET /v1/queue/{eventID}/me", identity(http.HandlerFunc(h.position)))
	public.Handle("GET /v1/events/{eventID}/status", http.HandlerFunc(h.status))
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
	if !h.allow(w, r, scopeJoinIP, clientID(r), h.limits.JoinPerIP, h.m.join, joinRateLimitedIP) ||
		!h.allow(w, r, scopeJoinUser, user, h.limits.JoinPerUser, h.m.join, joinRateLimitUser) {
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

type positionResponse struct {
	EventID       string     `json:"eventId"`
	State         State      `json:"state"`
	Rank          int64      `json:"rank,omitempty"`
	RandomizingAt *time.Time `json:"randomizingAt,omitempty"`
}

func (h *Handler) position(w http.ResponseWriter, r *http.Request) {
	user, ok := authn.UserFrom(r.Context())
	if !ok {
		httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "authentication required"))
		return
	}
	if !h.allow(w, r, scopePositionUser, user, h.limits.PositionPerUser, h.m.position, positionRateLimited) {
		return
	}
	pos, err := h.svc.Position(r.Context(), r.PathValue("eventID"), user)
	if err != nil {
		h.m.position(positionResultOf(err))
		writeError(w, r, err)
		return
	}
	resp := positionResponse{EventID: pos.EventID, State: pos.State}
	if pos.Rank > 0 {
		h.m.position(positionRanked)
		resp.Rank = pos.Rank
	} else {
		h.m.position(positionRandomizing)
		at := pos.RandomizingAt
		resp.RandomizingAt = &at
	}
	// One user's place in line: never stored by a shared cache.
	w.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(w, http.StatusOK, resp)
}

type statusResponse struct {
	EventID      string     `json:"eventId"`
	State        State      `json:"state"`
	OpensAt      time.Time  `json:"opensAt"`
	AdmittedUpTo int64      `json:"admittedUpTo"`
	QueueSize    int64      `json:"queueSize"`
	UpdatedAt    *time.Time `json:"updatedAt"`
}

// status serves the status document. It is identical for every client and
// needs no identity, so a shared cache may keep it for one second: the edge
// absorbs the waiting room's polling and the origin sees about one request
// per second per cache node.
func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	st, err := h.svc.Status(r.Context(), r.PathValue("eventID"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	resp := statusResponse{
		EventID: st.EventID, State: st.State, OpensAt: st.OpensAt,
		AdmittedUpTo: st.AdmittedUpTo, QueueSize: st.QueueSize,
	}
	if !st.UpdatedAt.IsZero() {
		at := st.UpdatedAt
		resp.UpdatedAt = &at
	}
	w.Header().Set("Cache-Control", "public, max-age=1")
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// allow takes a token for (scope, id), recording refusals and limiter
// failures with record. When the bucket is empty it writes 429 with
// Retry-After and returns false.
func (h *Handler) allow(w http.ResponseWriter, r *http.Request, scope, id string, rule ratelimit.Rule, record func(string), result string) bool {
	d, err := h.lim.Allow(r.Context(), scope, id, rule)
	if err != nil {
		record("error")
		writeError(w, r, err)
		return false
	}
	if d.Allowed {
		return true
	}
	record(result)
	p := httpx.NewProblem(http.StatusTooManyRequests, "RATE_LIMITED", "too many requests; retry after the time in Retry-After")
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

func positionResultOf(err error) string {
	switch {
	case errors.Is(err, ErrNotInQueue):
		return positionNotInQueue
	case errors.Is(err, ErrEventNotFound):
		return positionNotFound
	case errors.Is(err, ErrInvalidRequest):
		return positionInvalid
	default:
		return positionError
	}
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
	case errors.Is(err, ErrNotInQueue):
		p = httpx.NotFound("NOT_IN_QUEUE", "you have not joined this event's waiting room")
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
