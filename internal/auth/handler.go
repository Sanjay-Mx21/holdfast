package auth

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

// RefreshCookie is the cookie that carries the refresh token. It is httpOnly
// (scripts cannot read it), SameSite=Strict (other sites cannot make the
// browser send it), Secure (never sent over plain HTTP; browsers make an
// exception for http://localhost, so development needs no setting) and
// scoped to /v1/auth.
const RefreshCookie = "hf_refresh"

// HandlerConfig tunes the HTTP layer.
type HandlerConfig struct {
	// DevInbox serves the mock SMS inbox at GET /v1/auth/dev/inbox
	// (development only; refused in production by the config).
	DevInbox *Inbox
}

// Handler serves auth-svc's API.
type Handler struct {
	svc  *Service
	cfg  HandlerConfig
	jwks authn.JWKSet
}

// NewHandler returns the handler; jwks is the set of public keys access
// tokens are signed with.
func NewHandler(svc *Service, jwks authn.JWKSet, cfg HandlerConfig) *Handler {
	return &Handler{svc: svc, cfg: cfg, jwks: jwks}
}

// Register mounts the routes.
func (h *Handler) Register(public *httpx.Router) {
	public.HandleFunc("POST /v1/auth/otp/request", h.requestOTP)
	public.HandleFunc("POST /v1/auth/otp/verify", h.verifyOTP)
	public.HandleFunc("POST /v1/auth/refresh", h.refresh)
	public.HandleFunc("POST /v1/auth/logout", h.logout)
	public.HandleFunc("GET /.well-known/jwks.json", h.keys)
	if h.cfg.DevInbox != nil {
		public.HandleFunc("GET /v1/auth/dev/inbox", h.inbox)
	}
}

type phoneRequest struct {
	Phone string `json:"phone"`
}

type verifyRequest struct {
	Phone string `json:"phone"`
	Code  string `json:"code"`
}

// sessionView is the body of a sign-in or a refresh. The refresh token is
// never in it: it travels only in the cookie.
type sessionView struct {
	AccessToken string `json:"accessToken"`
	TokenType   string `json:"tokenType"`
	ExpiresIn   int64  `json:"expiresIn"`
	UserID      string `json:"userId"`
	Role        string `json:"role"`
}

func (h *Handler) requestOTP(w http.ResponseWriter, r *http.Request) {
	var req phoneRequest
	if p := httpx.DecodeJSON(r, &req); p != nil {
		httpx.WriteProblem(w, r, p)
		return
	}
	err := h.svc.RequestOTP(r.Context(), req.Phone)
	var limited *RateLimited
	switch {
	case errors.Is(err, ErrInvalidPhone):
		httpx.WriteProblem(w, r, httpx.BadRequest("INVALID_PHONE", "phone must be E.164 (+919876543210) or a 10-digit Indian mobile number"))
	case errors.As(err, &limited):
		secs := int(math.Ceil(limited.RetryAfter.Seconds()))
		p := httpx.NewProblem(http.StatusTooManyRequests, "RATE_LIMITED", "too many codes requested for this number; try again later")
		w.Header().Set("Retry-After", strconv.Itoa(max(secs, 1)))
		httpx.WriteProblem(w, r, p)
	case err != nil:
		httpx.WriteProblem(w, r, httpx.Unavailable("could not send a code; try again", 5))
	default:
		httpx.WriteJSON(w, http.StatusAccepted, map[string]int{"expiresInSeconds": int(h.svc.cfg.CodeTTL.Seconds())})
	}
}

func (h *Handler) verifyOTP(w http.ResponseWriter, r *http.Request) {
	var req verifyRequest
	if p := httpx.DecodeJSON(r, &req); p != nil {
		httpx.WriteProblem(w, r, p)
		return
	}
	sess, err := h.svc.VerifyOTP(r.Context(), req.Phone, req.Code)
	switch {
	case errors.Is(err, ErrInvalidPhone):
		httpx.WriteProblem(w, r, httpx.BadRequest("INVALID_PHONE", "phone must be E.164 (+919876543210) or a 10-digit Indian mobile number"))
	case errors.Is(err, ErrInvalidCode):
		httpx.WriteProblem(w, r, httpx.Unauthorized("INVALID_CODE", "the code is wrong, expired or used up; request a new one"))
	case err != nil:
		httpx.WriteProblem(w, r, httpx.Unavailable("could not sign in; try again", 2))
	default:
		h.writeSession(w, sess)
	}
}

func (h *Handler) refresh(w http.ResponseWriter, r *http.Request) {
	sess, err := h.svc.Refresh(r.Context(), cookieValue(r))
	switch {
	case errors.Is(err, ErrRefreshReused):
		h.clearCookie(w)
		httpx.WriteProblem(w, r, httpx.Unauthorized("REFRESH_REUSED", "this session was signed out because an old refresh token was reused; sign in again"))
	case errors.Is(err, ErrInvalidRefresh):
		h.clearCookie(w)
		httpx.WriteProblem(w, r, httpx.Unauthorized("INVALID_REFRESH", "sign in again"))
	case err != nil:
		httpx.WriteProblem(w, r, httpx.Unavailable("could not refresh; try again", 2))
	default:
		h.writeSession(w, sess)
	}
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Logout(r.Context(), cookieValue(r)); err != nil {
		httpx.WriteProblem(w, r, httpx.Unavailable("could not sign out; try again", 2))
		return
	}
	h.clearCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) keys(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=300")
	httpx.WriteJSON(w, http.StatusOK, h.jwks)
}

// inbox shows the last code-carrying message the mock gateway sent to a
// number (development only).
func (h *Handler) inbox(w http.ResponseWriter, r *http.Request) {
	phone, err := NormalizePhone(r.URL.Query().Get("phone"))
	if err != nil {
		httpx.WriteProblem(w, r, httpx.BadRequest("INVALID_PHONE", "phone must be E.164 or a 10-digit Indian mobile number"))
		return
	}
	m, ok := h.cfg.DevInbox.Last(phone)
	if !ok {
		httpx.WriteProblem(w, r, httpx.NotFound("NO_MESSAGE", "no message for this number"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, m)
}

func (h *Handler) writeSession(w http.ResponseWriter, s Session) {
	http.SetCookie(w, &http.Cookie{
		Name: RefreshCookie, Value: s.RefreshToken, Path: "/v1/auth",
		Expires: s.RefreshExpires, MaxAge: int(time.Until(s.RefreshExpires).Seconds()),
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
	})
	httpx.WriteJSON(w, http.StatusOK, sessionView{
		AccessToken: s.AccessToken, TokenType: "Bearer",
		ExpiresIn: int64(time.Until(s.AccessExpires).Round(time.Second).Seconds()),
		UserID:    s.UserID.String(), Role: s.Role,
	})
}

func (h *Handler) clearCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: RefreshCookie, Value: "", Path: "/v1/auth", MaxAge: -1,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
	})
}

func cookieValue(r *http.Request) string {
	c, err := r.Cookie(RefreshCookie)
	if err != nil {
		return ""
	}
	return c.Value
}
