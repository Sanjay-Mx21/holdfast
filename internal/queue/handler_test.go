package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/ratelimit"
)

type fakeService struct {
	provision func(ctx context.Context, eventID string, cfg EventConfig) (bool, error)
	join      func(ctx context.Context, eventID, userID string) (JoinResult, error)
}

func (f *fakeService) Provision(ctx context.Context, e string, c EventConfig) (bool, error) {
	return f.provision(ctx, e, c)
}
func (f *fakeService) Join(ctx context.Context, e, u string) (JoinResult, error) {
	return f.join(ctx, e, u)
}

// fakeLimiter allows everything unless a scope is listed in deny; it records
// every (scope, id) it was asked about.
type fakeLimiter struct {
	deny  map[string]time.Duration
	err   error
	calls []string
}

func (f *fakeLimiter) Allow(_ context.Context, scope, id string, _ ratelimit.Rule) (ratelimit.Decision, error) {
	f.calls = append(f.calls, scope+"="+id)
	if f.err != nil {
		return ratelimit.Decision{}, f.err
	}
	if wait, ok := f.deny[scope]; ok {
		return ratelimit.Decision{Allowed: false, RetryAfter: wait}, nil
	}
	return ratelimit.Decision{Allowed: true}, nil
}

var testLimits = JoinLimits{
	PerIP:   ratelimit.Rule{Capacity: 30, Rate: 10},
	PerUser: ratelimit.Rule{Capacity: 5, Rate: 1},
}

type harness struct {
	public, internal *httpx.Router
	lim              *fakeLimiter
	m                *Metrics
}

// newHarness wires the real routes. The identity stub turns the X-Test-User
// header into an authenticated user; operator auth is a pass-through.
func newHarness(svc service) harness {
	public := httpx.NewRouter(httpx.RequestID())
	internal := httpx.NewRouter(httpx.RequestID())
	identity := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			if u := r.Header.Get("X-Test-User"); u != "" {
				ctx = authn.WithUser(ctx, u)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	lim := &fakeLimiter{}
	m := NewMetrics(prometheus.NewRegistry())
	NewHandler(svc, lim, testLimits, m).Register(public, internal, identity, func(next http.Handler) http.Handler { return next })
	return harness{public: public, internal: internal, lim: lim, m: m}
}

func send(h http.Handler, method, path, body, contentType string, headers map[string]string, remote string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if remote != "" {
		req.RemoteAddr = remote
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func put(h http.Handler, path, body, contentType string) *httptest.ResponseRecorder {
	return send(h, http.MethodPut, path, body, contentType, nil, "")
}

func problemCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v (body %q)", err, rec.Body.String())
	}
	return p.Code
}

const (
	eventID   = "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"
	userID    = "0196f0c2-0000-7000-8000-000000000001"
	eventPath = "/internal/v1/events/0196F0C1-7A3E-7C51-9B0E-5D2F8A1C4E77/queue"
	joinPath  = "/v1/queue/" + eventID + "/join"
	validBody = `{"opensAt":"2026-10-05T12:00:00Z","admissionRatePerSecond":83,"maxSessions":10000,"sessionTtlSeconds":600}`
)

// joins reads the holdfast_queue_joins_total counter for one result.
func joins(t *testing.T, m *Metrics, result string) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.joins.WithLabelValues(result).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

func noProvision(context.Context, string, EventConfig) (bool, error) {
	return false, errors.New("provision must not be called")
}

// --- provisioning ---

func TestProvisionMapsBodyAndReportsCreation(t *testing.T) {
	for _, created := range []bool{true, false} {
		t.Run(fmt.Sprint("created=", created), func(t *testing.T) {
			var gotEvent string
			var gotCfg EventConfig
			h := newHarness(&fakeService{provision: func(_ context.Context, e string, c EventConfig) (bool, error) {
				gotEvent, gotCfg = e, c
				return created, nil
			}})
			rec := put(h.internal, eventPath, validBody, "application/json")

			wantStatus := http.StatusOK
			if created {
				wantStatus = http.StatusCreated
			}
			if rec.Code != wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, wantStatus, rec.Body)
			}
			want := EventConfig{
				OpensAt:       time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
				AdmissionRate: 83, MaxSessions: 10_000, SessionTTL: 10 * time.Minute,
			}
			if !gotCfg.OpensAt.Equal(want.OpensAt) || gotCfg.AdmissionRate != want.AdmissionRate ||
				gotCfg.MaxSessions != want.MaxSessions || gotCfg.SessionTTL != want.SessionTTL {
				t.Fatalf("config = %+v, want %+v", gotCfg, want)
			}
			if gotEvent != "0196F0C1-7A3E-7C51-9B0E-5D2F8A1C4E77" {
				t.Fatalf("event passed to service = %q", gotEvent)
			}
			var resp map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if resp["eventId"] != eventID || resp["created"] != created {
				t.Fatalf("response = %v", resp)
			}
		})
	}
}

func TestProvisionErrors(t *testing.T) {
	failing := func(err error) func(context.Context, string, EventConfig) (bool, error) {
		return func(context.Context, string, EventConfig) (bool, error) { return false, err }
	}
	tests := []struct {
		name        string
		body        string
		contentType string
		svc         func(context.Context, string, EventConfig) (bool, error)
		status      int
		code        string
	}{
		{"empty body", "", "application/json", noProvision, http.StatusBadRequest, "EMPTY_BODY"},
		{"malformed JSON", `{"opensAt":`, "application/json", noProvision, http.StatusBadRequest, "MALFORMED_JSON"},
		{"unknown field", `{"opensAt":"2026-10-05T12:00:00Z","capacity":5}`, "application/json", noProvision, http.StatusBadRequest, "INVALID_BODY"},
		{"wrong type", `{"maxSessions":"many"}`, "application/json", noProvision, http.StatusBadRequest, "INVALID_FIELD_TYPE"},
		{"not JSON", validBody, "text/plain", noProvision, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE"},
		{"validation", validBody, "application/json", failing(fmt.Errorf("%w: admission rate must be between 1 and 100000 per second", ErrInvalidRequest)), http.StatusBadRequest, "INVALID_REQUEST"},
		{"conflict", validBody, "application/json", failing(ErrProvisionConflict), http.StatusConflict, "PROVISION_CONFLICT"},
		{"Valkey down", validBody, "application/json", failing(&net.OpError{Op: "dial", Err: errors.New("connection refused")}), http.StatusServiceUnavailable, "UNAVAILABLE"},
		{"timeout", validBody, "application/json", failing(context.DeadlineExceeded), http.StatusServiceUnavailable, "UNAVAILABLE"},
		{"unexpected", validBody, "application/json", failing(errors.New("boom")), http.StatusInternalServerError, "INTERNAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := put(newHarness(&fakeService{provision: tt.svc}).internal, eventPath, tt.body, tt.contentType)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.status, rec.Body)
			}
			if got := problemCode(t, rec); got != tt.code {
				t.Fatalf("code = %q, want %q", got, tt.code)
			}
		})
	}
}

func TestValidationMessageReachesClient(t *testing.T) {
	h := newHarness(&fakeService{provision: func(context.Context, string, EventConfig) (bool, error) {
		return false, fmt.Errorf("%w: maximum sessions must be between 1 and 10000000", ErrInvalidRequest)
	}})
	rec := put(h.internal, eventPath, validBody, "application/json")
	if !strings.Contains(rec.Body.String(), "maximum sessions must be between 1 and 10000000") {
		t.Fatalf("detail missing from body %s", rec.Body)
	}
	if strings.Contains(rec.Body.String(), "queue: invalid request") {
		t.Fatalf("internal error prefix leaked: %s", rec.Body)
	}
}

func TestProvisionRouteIsAdminAndPUTOnly(t *testing.T) {
	h := newHarness(&fakeService{provision: func(context.Context, string, EventConfig) (bool, error) { return true, nil }})
	if rec := send(h.internal, http.MethodPost, eventPath, validBody, "application/json", nil, ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST on admin: status %d, want 405", rec.Code)
	}
	if rec := put(h.public, eventPath, validBody, "application/json"); rec.Code != http.StatusNotFound {
		t.Fatalf("PUT on public port: status %d, want 404", rec.Code)
	}
}

// --- joining ---

func TestJoinAccepted(t *testing.T) {
	for _, tt := range []struct {
		joined   bool
		ordering Ordering
		metric   string
	}{
		{true, OrderingLottery, joinJoined},
		{false, OrderingFIFO, joinAlready},
	} {
		t.Run(tt.metric, func(t *testing.T) {
			var gotEvent, gotUser string
			h := newHarness(&fakeService{join: func(_ context.Context, e, u string) (JoinResult, error) {
				gotEvent, gotUser = e, u
				return JoinResult{EventID: eventID, Joined: tt.joined, Ordering: tt.ordering}, nil
			}})
			rec := send(h.public, http.MethodPost, joinPath, "", "", map[string]string{"X-Test-User": userID}, "203.0.113.7:51234")
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status %d, want 202 (body %s)", rec.Code, rec.Body)
			}
			var resp joinResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if resp != (joinResponse{EventID: eventID, Joined: tt.joined, Ordering: tt.ordering}) {
				t.Fatalf("response %+v", resp)
			}
			if gotEvent != eventID || gotUser != userID {
				t.Fatalf("service got event %q user %q", gotEvent, gotUser)
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("join response must not be cacheable")
			}
			want := []string{"join-ip=203.0.113.7", "join-user=" + userID}
			if strings.Join(h.lim.calls, ",") != strings.Join(want, ",") {
				t.Fatalf("limiter calls %v, want %v", h.lim.calls, want)
			}
			if got := joins(t, h.m, tt.metric); got != 1 {
				t.Fatalf("metric %s = %v, want 1", tt.metric, got)
			}
		})
	}
}

func TestJoinRequiresIdentity(t *testing.T) {
	h := newHarness(&fakeService{join: func(context.Context, string, string) (JoinResult, error) {
		t.Fatal("service called without a user")
		return JoinResult{}, nil
	}})
	rec := send(h.public, http.MethodPost, joinPath, "", "", nil, "")
	if rec.Code != http.StatusUnauthorized || problemCode(t, rec) != "UNAUTHENTICATED" {
		t.Fatalf("status %d body %s, want 401 UNAUTHENTICATED", rec.Code, rec.Body)
	}
	if len(h.lim.calls) != 0 {
		t.Fatalf("rate limiter consulted before authentication: %v", h.lim.calls)
	}
}

func TestJoinRateLimited(t *testing.T) {
	for _, tt := range []struct {
		scope      string
		wait       time.Duration
		retryAfter string
		metric     string
	}{
		{scopeJoinIP, 1500 * time.Millisecond, "2", joinRateLimitedIP},
		{scopeJoinUser, 200 * time.Millisecond, "1", joinRateLimitUser},
	} {
		t.Run(tt.scope, func(t *testing.T) {
			h := newHarness(&fakeService{join: func(context.Context, string, string) (JoinResult, error) {
				t.Fatal("service called despite the rate limit")
				return JoinResult{}, nil
			}})
			h.lim.deny = map[string]time.Duration{tt.scope: tt.wait}
			rec := send(h.public, http.MethodPost, joinPath, "", "", map[string]string{"X-Test-User": userID}, "203.0.113.7:51234")
			if rec.Code != http.StatusTooManyRequests || problemCode(t, rec) != "RATE_LIMITED" {
				t.Fatalf("status %d body %s, want 429 RATE_LIMITED", rec.Code, rec.Body)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.retryAfter {
				t.Fatalf("Retry-After %q, want %q (rounded up, at least 1)", got, tt.retryAfter)
			}
			if got := joins(t, h.m, tt.metric); got != 1 {
				t.Fatalf("metric %s = %v, want 1", tt.metric, got)
			}
		})
	}
}

func TestJoinLimiterFailureIsUnavailable(t *testing.T) {
	h := newHarness(&fakeService{})
	h.lim.err = &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	rec := send(h.public, http.MethodPost, joinPath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status %d, Retry-After %q; want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestJoinErrors(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
		metric string
	}{
		{ErrEventNotFound, http.StatusNotFound, "EVENT_NOT_FOUND", joinNotFound},
		{ErrQueueClosed, http.StatusConflict, "QUEUE_CLOSED", joinClosed},
		{fmt.Errorf("%w: eventId must be a UUID", ErrInvalidRequest), http.StatusBadRequest, "INVALID_REQUEST", joinInvalid},
		{context.DeadlineExceeded, http.StatusServiceUnavailable, "UNAVAILABLE", joinError},
		{errors.New("boom"), http.StatusInternalServerError, "INTERNAL", joinError},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			h := newHarness(&fakeService{join: func(context.Context, string, string) (JoinResult, error) {
				return JoinResult{}, tt.err
			}})
			rec := send(h.public, http.MethodPost, joinPath, "", "", map[string]string{"X-Test-User": userID}, "")
			if rec.Code != tt.status || problemCode(t, rec) != tt.code {
				t.Fatalf("status %d body %s, want %d %s", rec.Code, rec.Body, tt.status, tt.code)
			}
			if got := joins(t, h.m, tt.metric); got != 1 {
				t.Fatalf("metric %s = %v, want 1", tt.metric, got)
			}
		})
	}
}

func TestClientID(t *testing.T) {
	tests := []struct{ remote, want string }{
		{"203.0.113.7:51234", "203.0.113.7"},
		{"[::ffff:203.0.113.7]:443", "203.0.113.7"},
		{"[2001:db8:1:2:aaaa:bbbb:cccc:dddd]:443", "2001_db8_1_2__"},
		{"[2001:db8:1:2::1]:443", "2001_db8_1_2__"},
		{"[fe80::1%eth0]:443", "fe80__"},
		{"not an address", "unknown"},
	}
	for _, tt := range tests {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = tt.remote
		if got := clientID(r); got != tt.want {
			t.Errorf("clientID(%q) = %q, want %q", tt.remote, got, tt.want)
		}
	}
	// Two addresses in the same /64 share a bucket; another /64 does not.
	a := httptest.NewRequest(http.MethodPost, "/", nil)
	a.RemoteAddr = "[2001:db8:1:2::1]:1"
	b := httptest.NewRequest(http.MethodPost, "/", nil)
	b.RemoteAddr = "[2001:db8:1:3::1]:1"
	if clientID(a) == clientID(b) {
		t.Fatal("different /64 networks share a rate-limit bucket")
	}
}
