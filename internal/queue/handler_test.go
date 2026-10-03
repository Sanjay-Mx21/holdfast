package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/ratelimit"
	"github.com/Sanjay-Mx21/holdfast/internal/policy"
)

type fakeService struct {
	provision func(ctx context.Context, eventID string, cfg EventConfig) (bool, error)
	join      func(ctx context.Context, eventID, userID string) (JoinResult, error)
	position  func(ctx context.Context, eventID, userID string) (Position, error)
	status    func(ctx context.Context, eventID string) (Status, error)
	admit     func(ctx context.Context, eventID, userID string) (Turn, error)
	freeze    func(ctx context.Context, eventID string, frozen bool) (bool, error)
	buyer     policy.Buyer // the last joiner's
}

func (f *fakeService) Provision(ctx context.Context, e string, c EventConfig) (bool, error) {
	return f.provision(ctx, e, c)
}
func (f *fakeService) Join(ctx context.Context, e, u string, b policy.Buyer) (JoinResult, error) {
	f.buyer = b
	return f.join(ctx, e, u)
}
func (f *fakeService) Freeze(ctx context.Context, e string) (bool, error) {
	return f.freeze(ctx, e, true)
}
func (f *fakeService) Unfreeze(ctx context.Context, e string) (bool, error) {
	return f.freeze(ctx, e, false)
}
func (f *fakeService) Position(ctx context.Context, e, u string) (Position, error) {
	return f.position(ctx, e, u)
}
func (f *fakeService) Status(ctx context.Context, e string) (Status, error) {
	return f.status(ctx, e)
}
func (f *fakeService) Admit(ctx context.Context, e, u string) (Turn, error) {
	return f.admit(ctx, e, u)
}

// fakeIssuer records what it was asked to sign.
type fakeIssuer struct {
	user, event, session string
	rank                 int64
	notAfter             time.Time
	err                  error
}

func (f *fakeIssuer) IssueUntil(user, event, session string, rank int64, notAfter time.Time) (string, time.Time, error) {
	f.user, f.event, f.session, f.rank, f.notAfter = user, event, session, rank, notAfter
	if f.err != nil {
		return "", time.Time{}, f.err
	}
	return "signed-token", notAfter, nil
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

var testLimits = Limits{
	JoinPerIP:       ratelimit.Rule{Capacity: 30, Rate: 10},
	JoinPerUser:     ratelimit.Rule{Capacity: 5, Rate: 1},
	PositionPerUser: ratelimit.Rule{Capacity: 10, Rate: 1},
	AdmitPerUser:    ratelimit.Rule{Capacity: 5, Rate: 1},
}

type harness struct {
	public, internal *httpx.Router
	lim              *fakeLimiter
	m                *Metrics
	iss              *fakeIssuer
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
			// X-Test-Role: the caller signed in with an access token.
			if role := r.Header.Get("X-Test-Role"); role != "" {
				c := &authn.AccessClaims{Role: role, Verified: r.Header.Get("X-Test-Verified") == "true"}
				c.Subject = r.Header.Get("X-Test-User")
				ctx = authn.WithAccess(ctx, c)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	lim := &fakeLimiter{}
	m := NewMetrics(prometheus.NewRegistry())
	iss := &fakeIssuer{}
	NewHandler(svc, lim, testLimits, m, iss, authn.JWKSet{Keys: []authn.JWK{{Kty: "OKP", Crv: "Ed25519", X: "x", Kid: "kid-1", Alg: "EdDSA", Use: "sig"}}}).
		Register(public, internal, identity, func(next http.Handler) http.Handler { return next })
	return harness{public: public, internal: internal, lim: lim, m: m, iss: iss}
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
		if got := clientID(r, nil); got != tt.want {
			t.Errorf("clientID(%q) = %q, want %q", tt.remote, got, tt.want)
		}
	}
	// Two addresses in the same /64 share a bucket; another /64 does not.
	a := httptest.NewRequest(http.MethodPost, "/", nil)
	a.RemoteAddr = "[2001:db8:1:2::1]:1"
	b := httptest.NewRequest(http.MethodPost, "/", nil)
	b.RemoteAddr = "[2001:db8:1:3::1]:1"
	if clientID(a, nil) == clientID(b, nil) {
		t.Fatal("different /64 networks share a rate-limit bucket")
	}
}

func TestJoinThatOpensTheQueueIsCounted(t *testing.T) {
	h := newHarness(&fakeService{join: func(context.Context, string, string) (JoinResult, error) {
		return JoinResult{EventID: eventID, Joined: true, Ordering: OrderingFIFO, openedQueue: true}, nil
	}})
	rec := send(h.public, http.MethodPost, joinPath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "opened") {
		t.Fatalf("internal T0 flag leaked into the response: %s", rec.Body)
	}
	var out dto.Metric
	if err := h.m.opened.WithLabelValues(openedByJoin).Write(&out); err != nil {
		t.Fatal(err)
	}
	if got := out.GetCounter().GetValue(); got != 1 {
		t.Fatalf("holdfast_queue_opened_total{by=join} = %v, want 1", got)
	}
}

// --- position ---

const mePath = "/v1/queue/" + eventID + "/me"

func positions(t *testing.T, m *Metrics, result string) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.positions.WithLabelValues(result).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

func TestPositionRanked(t *testing.T) {
	var gotEvent, gotUser string
	h := newHarness(&fakeService{position: func(_ context.Context, e, u string) (Position, error) {
		gotEvent, gotUser = e, u
		return Position{EventID: eventID, State: StateOpen, Rank: 18204}, nil
	}})
	rec := send(h.public, http.MethodGet, mePath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 (body %s)", rec.Code, rec.Body)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"eventId":"`+eventID+`","state":"OPEN","rank":18204}` {
		t.Fatalf("body %s", got)
	}
	if gotEvent != eventID || gotUser != userID {
		t.Fatalf("service got event %q user %q", gotEvent, gotUser)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "private, no-store" {
		t.Fatalf("Cache-Control %q: one user's place must never be shared-cached", cc)
	}
	if h.lim.calls[0] != "position-user="+userID {
		t.Fatalf("limiter calls %v", h.lim.calls)
	}
	if positions(t, h.m, positionRanked) != 1 {
		t.Fatal("ranked lookup not counted")
	}
}

func TestPositionBeforeT0(t *testing.T) {
	t0 := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	h := newHarness(&fakeService{position: func(context.Context, string, string) (Position, error) {
		return Position{EventID: eventID, State: StatePre, RandomizingAt: t0}, nil
	}})
	rec := send(h.public, http.MethodGet, mePath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 (body %s)", rec.Code, rec.Body)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"eventId":"`+eventID+`","state":"PRE","randomizingAt":"2026-10-05T12:00:00Z"}` {
		t.Fatalf("body %s: before T0 there must be no rank, only randomizingAt", got)
	}
	if positions(t, h.m, positionRandomizing) != 1 {
		t.Fatal("randomizing lookup not counted")
	}
}

func TestPositionErrors(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
		metric string
	}{
		{ErrNotInQueue, http.StatusNotFound, "NOT_IN_QUEUE", positionNotInQueue},
		{ErrEventNotFound, http.StatusNotFound, "EVENT_NOT_FOUND", positionNotFound},
		{fmt.Errorf("%w: eventId must be a UUID", ErrInvalidRequest), http.StatusBadRequest, "INVALID_REQUEST", positionInvalid},
		{context.DeadlineExceeded, http.StatusServiceUnavailable, "UNAVAILABLE", positionError},
		{errors.New("boom"), http.StatusInternalServerError, "INTERNAL", positionError},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			h := newHarness(&fakeService{position: func(context.Context, string, string) (Position, error) {
				return Position{}, tt.err
			}})
			rec := send(h.public, http.MethodGet, mePath, "", "", map[string]string{"X-Test-User": userID}, "")
			if rec.Code != tt.status || problemCode(t, rec) != tt.code {
				t.Fatalf("status %d body %s, want %d %s", rec.Code, rec.Body, tt.status, tt.code)
			}
			if positions(t, h.m, tt.metric) != 1 {
				t.Fatalf("metric %s not counted", tt.metric)
			}
		})
	}
}

func TestPositionRequiresIdentityAndIsRateLimited(t *testing.T) {
	h := newHarness(&fakeService{position: func(context.Context, string, string) (Position, error) {
		t.Fatal("service called")
		return Position{}, nil
	}})
	if rec := send(h.public, http.MethodGet, mePath, "", "", nil, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no identity: status %d, want 401", rec.Code)
	}
	h.lim.deny = map[string]time.Duration{scopePositionUser: 700 * time.Millisecond}
	rec := send(h.public, http.MethodGet, mePath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("status %d Retry-After %q, want 429 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	if positions(t, h.m, positionRateLimited) != 1 {
		t.Fatal("rate-limited lookup not counted")
	}
	// The per-user position bucket must not touch the join buckets.
	if len(h.lim.calls) != 1 || h.lim.calls[0] != "position-user="+userID {
		t.Fatalf("limiter calls %v", h.lim.calls)
	}
}

// --- status document ---

const statusPath = "/v1/events/" + eventID + "/status"

func TestStatusIsPublicAndCacheable(t *testing.T) {
	opens := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	updated := opens.Add(90 * time.Second)
	h := newHarness(&fakeService{status: func(_ context.Context, e string) (Status, error) {
		if e != eventID {
			t.Errorf("service got event %q", e)
		}
		return Status{EventID: eventID, State: StateOpen, OpensAt: opens, AdmittedUpTo: 4200, QueueSize: 50000, UpdatedAt: updated}, nil
	}})
	rec := send(h.public, http.MethodGet, statusPath, "", "", nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d (body %s)", rec.Code, rec.Body)
	}
	want := `{"eventId":"` + eventID + `","state":"OPEN","opensAt":"2026-10-05T12:00:00Z","admittedUpTo":4200,"queueSize":50000,"updatedAt":"2026-10-05T12:01:30Z"}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Fatalf("body %s\nwant %s", got, want)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "public, max-age=1" {
		t.Fatalf("Cache-Control %q, want public, max-age=1", cc)
	}
	if len(h.lim.calls) != 0 {
		t.Fatalf("the status document must not be rate limited per client: %v", h.lim.calls)
	}
}

func TestStatusFallbackHasNullUpdatedAt(t *testing.T) {
	h := newHarness(&fakeService{status: func(context.Context, string) (Status, error) {
		return Status{EventID: eventID, State: StatePre, OpensAt: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}, nil
	}})
	rec := send(h.public, http.MethodGet, statusPath, "", "", nil, "")
	if !strings.Contains(rec.Body.String(), `"updatedAt":null`) {
		t.Fatalf("fallback document should say updatedAt null, got %s", rec.Body)
	}
}

func TestStatusErrors(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{ErrEventNotFound, http.StatusNotFound, "EVENT_NOT_FOUND"},
		{fmt.Errorf("%w: eventId must be a UUID", ErrInvalidRequest), http.StatusBadRequest, "INVALID_REQUEST"},
		{context.DeadlineExceeded, http.StatusServiceUnavailable, "UNAVAILABLE"},
	} {
		h := newHarness(&fakeService{status: func(context.Context, string) (Status, error) { return Status{}, tt.err }})
		rec := send(h.public, http.MethodGet, statusPath, "", "", nil, "")
		if rec.Code != tt.status || problemCode(t, rec) != tt.code {
			t.Fatalf("status %d body %s, want %d %s", rec.Code, rec.Body, tt.status, tt.code)
		}
		if strings.Contains(rec.Header().Get("Cache-Control"), "public") {
			t.Fatalf("an error response must not be publicly cacheable: %q", rec.Header().Get("Cache-Control"))
		}
	}
}

// --- admission tokens ---

const admitPath = "/v1/queue/" + eventID + "/admit"

func admits(t *testing.T, m *Metrics, result string) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.admits.WithLabelValues(result).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

func TestAdmitIssuesAToken(t *testing.T) {
	expires := time.Date(2026, 10, 5, 12, 10, 0, 0, time.UTC)
	h := newHarness(&fakeService{admit: func(_ context.Context, e, u string) (Turn, error) {
		if e != eventID || u != userID {
			t.Errorf("service got event %q user %q", e, u)
		}
		return Turn{EventID: eventID, UserID: userID, Rank: 42, SessionExpires: expires}, nil
	}})
	rec := send(h.public, http.MethodPost, admitPath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d (body %s)", rec.Code, rec.Body)
	}
	want := `{"eventId":"` + eventID + `","token":"signed-token","expiresAt":"2026-10-05T12:10:00Z","rank":42}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Fatalf("body %s\nwant %s", got, want)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("a token response must never be cached")
	}
	if h.iss.user != userID || h.iss.event != eventID || h.iss.rank != 42 || !h.iss.notAfter.Equal(expires) ||
		h.iss.session != SessionID(eventID, userID) {
		t.Fatalf("issuer asked to sign %+v", h.iss)
	}
	if h.lim.calls[0] != "admit-user="+userID {
		t.Fatalf("limiter calls %v", h.lim.calls)
	}
	if admits(t, h.m, admitIssued) != 1 {
		t.Fatal("issued token not counted")
	}
}

func TestAdmitRefusals(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
		detail string
		metric string
	}{
		{&NotYourTurnError{Rank: 900, AdmittedUpTo: 512}, http.StatusConflict, "NOT_YOUR_TURN", "your rank is 900, admission has reached 512", admitNotYourTurn},
		{ErrTurnExpired, http.StatusConflict, "TURN_EXPIRED", "expired", admitExpired},
		{ErrNotInQueue, http.StatusNotFound, "NOT_IN_QUEUE", "", admitNotInQueue},
		{ErrQueueClosed, http.StatusConflict, "QUEUE_CLOSED", "", admitClosed},
		{ErrEventNotFound, http.StatusNotFound, "EVENT_NOT_FOUND", "", admitNotFound},
		{fmt.Errorf("%w: eventId must be a UUID", ErrInvalidRequest), http.StatusBadRequest, "INVALID_REQUEST", "", admitInvalid},
		{errors.New("boom"), http.StatusInternalServerError, "INTERNAL", "", admitError},
	} {
		t.Run(tt.code, func(t *testing.T) {
			h := newHarness(&fakeService{admit: func(context.Context, string, string) (Turn, error) { return Turn{}, tt.err }})
			rec := send(h.public, http.MethodPost, admitPath, "", "", map[string]string{"X-Test-User": userID}, "")
			if rec.Code != tt.status || problemCode(t, rec) != tt.code {
				t.Fatalf("status %d body %s, want %d %s", rec.Code, rec.Body, tt.status, tt.code)
			}
			if tt.detail != "" && !strings.Contains(rec.Body.String(), tt.detail) {
				t.Fatalf("detail missing %q: %s", tt.detail, rec.Body)
			}
			if h.iss.user != "" {
				t.Fatal("a token was signed for a refused claim")
			}
			if admits(t, h.m, tt.metric) != 1 {
				t.Fatalf("metric %s not counted", tt.metric)
			}
		})
	}
}

func TestAdmitSigningFailureIsInternal(t *testing.T) {
	h := newHarness(&fakeService{admit: func(context.Context, string, string) (Turn, error) {
		return Turn{EventID: eventID, UserID: userID, Rank: 1, SessionExpires: time.Now().Add(time.Minute)}, nil
	}})
	h.iss.err = errors.New("authn: token would already be expired")
	rec := send(h.public, http.MethodPost, admitPath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "signed-token") {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	if admits(t, h.m, admitError) != 1 {
		t.Fatal("signing failure not counted")
	}
}

func TestAdmitRequiresIdentityAndIsRateLimited(t *testing.T) {
	h := newHarness(&fakeService{admit: func(context.Context, string, string) (Turn, error) {
		t.Fatal("service called")
		return Turn{}, nil
	}})
	if rec := send(h.public, http.MethodPost, admitPath, "", "", nil, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no identity: %d, want 401", rec.Code)
	}
	h.lim.deny = map[string]time.Duration{scopeAdmitUser: time.Second}
	rec := send(h.public, http.MethodPost, admitPath, "", "", map[string]string{"X-Test-User": userID}, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", rec.Code)
	}
	if admits(t, h.m, admitRateLimited) != 1 {
		t.Fatal("rate limit not counted")
	}
}

func TestJWKSIsPublished(t *testing.T) {
	h := newHarness(&fakeService{})
	rec := send(h.public, http.MethodGet, "/.well-known/jwks.json", "", "", nil, "")
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "public, max-age=300" {
		t.Fatalf("status %d Cache-Control %q", rec.Code, rec.Header().Get("Cache-Control"))
	}
	want := `{"keys":[{"kty":"OKP","crv":"Ed25519","x":"x","kid":"kid-1","alg":"EdDSA","use":"sig"}]}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Fatalf("body %s", got)
	}
}

func TestSessionIDIsStablePerEventAndUser(t *testing.T) {
	a := SessionID(eventID, userID)
	if a != SessionID(eventID, userID) {
		t.Fatal("session ID changes between claims")
	}
	if a == SessionID(eventID, "0196f0c2-0000-7000-8000-000000000002") || a == SessionID("0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e78", userID) {
		t.Fatal("different users or events share a session ID")
	}
}

func TestClientIDBehindTheEdge(t *testing.T) {
	edge := []netip.Prefix{netip.MustParsePrefix("10.250.0.10/32")}
	req := func(remote string, xff ...string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = remote
		for _, v := range xff {
			r.Header.Add("X-Forwarded-For", v)
		}
		return r
	}
	tests := []struct {
		name string
		r    *http.Request
		want string
	}{
		{"edge sets the client", req("10.250.0.10:5000", "203.0.113.7"), "203.0.113.7"},
		{"only the last entry is believed", req("10.250.0.10:5000", "198.51.100.1, 203.0.113.7"), "203.0.113.7"},
		{"last of several headers", req("10.250.0.10:5000", "198.51.100.1", "203.0.113.9"), "203.0.113.9"},
		{"IPv6 client behind the edge, per /64", req("10.250.0.10:5000", "2001:db8:1:2::99"), "2001_db8_1_2__"},
		{"untrusted sender's header ignored", req("172.20.0.1:5000", "203.0.113.7"), "172.20.0.1"},
		{"edge without the header", req("10.250.0.10:5000"), "10.250.0.10"},
		{"garbage header falls back to the edge", req("10.250.0.10:5000", "not-an-ip"), "10.250.0.10"},
	}
	for _, tt := range tests {
		if got := clientID(tt.r, edge); got != tt.want {
			t.Errorf("%s: clientID = %q, want %q", tt.name, got, tt.want)
		}
	}
	// Without trusted proxies, a forwarded-for header is never believed.
	if got := clientID(req("10.250.0.10:5000", "203.0.113.7"), nil); got != "10.250.0.10" {
		t.Errorf("no trusted proxies: clientID = %q, want the connection address", got)
	}
}

func TestJoinPassesTheBuyerAndMapsPolicyRefusals(t *testing.T) {
	ev, user := "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77", "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e78"
	until := time.Now().Add(90 * time.Second)
	svc := &fakeService{join: func(context.Context, string, string) (JoinResult, error) {
		return JoinResult{}, &policy.Refused{Reason: policy.ReasonAgentLockout, Until: until}
	}}
	h := newHarness(svc)
	rec := send(h.public, http.MethodPost, "/v1/queue/"+ev+"/join", "", "", map[string]string{
		"X-Test-User": user, "X-Test-Role": "AGENT", "X-Test-Verified": "true",
	}, "192.0.2.1:1234")
	if svc.buyer != (policy.Buyer{Role: "AGENT", Verified: true}) {
		t.Fatalf("buyer %+v, want the token's role and verification", svc.buyer)
	}
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), `"code":"AGENT_LOCKOUT"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
	if ra := rec.Header().Get("Retry-After"); ra != "90" && ra != "89" {
		t.Fatalf("Retry-After %q, want the seconds until the window ends (90)", ra)
	}
	var out dto.Metric
	_ = h.m.joins.WithLabelValues(joinAgentLockout).Write(&out)
	if out.GetCounter().GetValue() != 1 {
		t.Fatalf("agent_lockout joins = %v, want 1", out.GetCounter().GetValue())
	}

	// The development header alone is an unverified buyer with no role.
	svc.join = func(context.Context, string, string) (JoinResult, error) {
		return JoinResult{}, &policy.Refused{Reason: policy.ReasonVerifiedOnly, Until: time.Now().Add(-time.Second)}
	}
	rec = send(h.public, http.MethodPost, "/v1/queue/"+ev+"/join", "", "", map[string]string{"X-Test-User": user}, "192.0.2.1:1234")
	if svc.buyer != (policy.Buyer{}) || rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), `"code":"VERIFIED_ONLY"`) {
		t.Fatalf("buyer %+v; got %d %s", svc.buyer, rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After %q, want at least 1", rec.Header().Get("Retry-After"))
	}
}

func TestFreezeRoutesAreAdminOnly(t *testing.T) {
	ev := "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"
	var calls []bool
	svc := &fakeService{freeze: func(_ context.Context, e string, frozen bool) (bool, error) {
		calls = append(calls, frozen)
		return true, nil
	}}
	h := newHarness(svc)
	for _, tc := range []struct{ path, state string }{{"freeze", "FROZEN"}, {"unfreeze", "OPEN"}} {
		rec := send(h.internal, http.MethodPost, "/internal/v1/events/"+ev+"/"+tc.path, "", "", nil, "10.0.0.1:1")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"state":"`+tc.state+`"`) {
			t.Fatalf("%s: %d %s", tc.path, rec.Code, rec.Body.String())
		}
		if rec := send(h.public, http.MethodPost, "/internal/v1/events/"+ev+"/"+tc.path, "", "", nil, "10.0.0.1:1"); rec.Code != http.StatusNotFound {
			t.Fatalf("%s must not exist on the public router, got %d", tc.path, rec.Code)
		}
	}
	if len(calls) != 2 || !calls[0] || calls[1] {
		t.Fatalf("calls %v, want [true false]", calls)
	}
	svc.freeze = func(context.Context, string, bool) (bool, error) {
		return false, fmt.Errorf("%w: it is PRE, and only a queue in state OPEN can become FROZEN", ErrStateConflict)
	}
	rec := send(h.internal, http.MethodPost, "/internal/v1/events/"+ev+"/freeze", "", "", nil, "10.0.0.1:1")
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), `"code":"STATE_CONFLICT"`) || !strings.Contains(rec.Body.String(), "it is PRE") {
		t.Fatalf("conflict: %d %s", rec.Code, rec.Body.String())
	}
}
