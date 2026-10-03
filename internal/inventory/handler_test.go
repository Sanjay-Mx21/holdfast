package inventory

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

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

type fakeService struct {
	create    func(context.Context, CreateHoldRequest) (HoldResult, error)
	get       func(ctx context.Context, eventID, userID, holdID string) (Hold, error)
	cancel    func(ctx context.Context, eventID, userID, holdID string) error
	avail     func(ctx context.Context, eventID string) (Availability, error)
	provision func(ctx context.Context, eventID string, cfg EventConfig) (bool, error)
	freeze    func(ctx context.Context, eventID string, frozen bool) (bool, error)
}

func (f *fakeService) CreateHold(ctx context.Context, r CreateHoldRequest) (HoldResult, error) {
	return f.create(ctx, r)
}
func (f *fakeService) GetHold(ctx context.Context, e, u, h string) (Hold, error) {
	return f.get(ctx, e, u, h)
}
func (f *fakeService) CancelHold(ctx context.Context, e, u, h string) error {
	return f.cancel(ctx, e, u, h)
}
func (f *fakeService) Availability(ctx context.Context, e string) (Availability, error) {
	return f.avail(ctx, e)
}
func (f *fakeService) Provision(ctx context.Context, e string, c EventConfig) (bool, error) {
	return f.provision(ctx, e, c)
}
func (f *fakeService) SetFrozen(ctx context.Context, e string, frozen bool) (bool, error) {
	return f.freeze(ctx, e, frozen)
}

type harness struct{ public, internal *httpx.Router }

// newHarness wires the real routes with stub auth: the admission stub turns
// X-Test-User / X-Test-Event headers into verified claims.
func newHarness(svc service) harness {
	public := httpx.NewRouter(httpx.RequestID())
	internal := httpx.NewRouter(httpx.RequestID())
	admission := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c := &authn.AdmissionClaims{
				EventID:          r.Header.Get("X-Test-Event"),
				RegisteredClaims: jwt.RegisteredClaims{Subject: r.Header.Get("X-Test-User")},
			}
			next.ServeHTTP(w, r.WithContext(authn.WithAdmission(r.Context(), c)))
		})
	}
	operator := func(next http.Handler) http.Handler { return next }
	NewHandler(svc).Register(public, internal, admission, operator)
	return harness{public: public, internal: internal}
}

func do(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func problemCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p httpx.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("not a problem document: %s", rec.Body.String())
	}
	return p.Code
}

func TestCreateHoldPassesIdentityFromTheTokenAndReturns201(t *testing.T) {
	ev, user, holdID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	expires := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	var got CreateHoldRequest
	h := newHarness(&fakeService{create: func(_ context.Context, r CreateHoldRequest) (HoldResult, error) {
		got = r
		return HoldResult{Hold: Hold{ID: holdID, EventID: ev, UserID: user, Quantity: 2, State: StateHeld, ExpiresAt: expires}, Remaining: 998}, nil
	}})
	rec := do(h.public, http.MethodPost, "/v1/events/"+ev+"/holds", `{"quantity":2}`, map[string]string{
		"X-Test-User": user, "X-Test-Event": ev, "Idempotency-Key": "order-00000001",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got != (CreateHoldRequest{EventID: ev, UserID: user, IdempotencyKey: "order-00000001", Quantity: 2}) {
		t.Fatalf("service got %+v", got)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["holdId"] != holdID || body["state"] != "HELD" || body["remaining"] != float64(998) || body["expiresAt"] != "2026-09-25T10:00:00Z" {
		t.Fatalf("body %v", body)
	}
	if rec.Header().Get("Location") != "/v1/events/"+ev+"/holds/"+holdID || rec.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("headers %v", rec.Header())
	}
}

func TestReplayIsMarked(t *testing.T) {
	ev, user := uuid.NewString(), uuid.NewString()
	h := newHarness(&fakeService{create: func(context.Context, CreateHoldRequest) (HoldResult, error) {
		return HoldResult{Hold: Hold{ID: uuid.NewString(), EventID: ev, State: StateHeld}, Replayed: true}, nil
	}})
	rec := do(h.public, http.MethodPost, "/v1/events/"+ev+"/holds", `{"quantity":1}`, map[string]string{
		"X-Test-User": user, "X-Test-Event": ev, "Idempotency-Key": "order-00000001",
	})
	if rec.Code != http.StatusCreated || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("status %d, headers %v", rec.Code, rec.Header())
	}
}

func TestCreateHoldRejectsRequestsBeforeCallingTheService(t *testing.T) {
	ev, other := uuid.NewString(), uuid.NewString()
	h := newHarness(&fakeService{create: func(context.Context, CreateHoldRequest) (HoldResult, error) {
		t.Fatal("service must not be called")
		return HoldResult{}, nil
	}})
	cases := []struct {
		name, tokenEvent, key, body string
		status                      int
		code                        string
	}{
		{"token for another event", other, "order-00000001", `{"quantity":1}`, 403, "TOKEN_EVENT_MISMATCH"},
		{"missing idempotency key", ev, "", `{"quantity":1}`, 400, "IDEMPOTENCY_KEY_REQUIRED"},
		{"unknown field", ev, "order-00000001", `{"quantity":1,"price":0}`, 400, "INVALID_BODY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{"X-Test-User": uuid.NewString(), "X-Test-Event": tc.tokenEvent}
			if tc.key != "" {
				headers["Idempotency-Key"] = tc.key
			}
			rec := do(h.public, http.MethodPost, "/v1/events/"+ev+"/holds", tc.body, headers)
			if rec.Code != tc.status || problemCode(t, rec) != tc.code {
				t.Fatalf("got %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestDomainErrorsMapToStableProblemCodes(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"sold out", ErrSoldOut, 409, "SOLD_OUT"},
		{"user limit", ErrUserLimit, 422, "USER_LIMIT"},
		{"invalid quantity", fmt.Errorf("%w: quantity must be between 1 and 4 for this event", ErrInvalidQuantity), 422, "INVALID_QUANTITY"},
		{"invalid request", fmt.Errorf("%w: eventId must be a UUID", ErrInvalidRequest), 400, "INVALID_REQUEST"},
		{"not provisioned", ErrEventNotProvisioned, 404, "EVENT_NOT_FOUND"},
		{"key reused", ErrIdempotencyKeyReused, 422, "IDEMPOTENCY_KEY_REUSED"},
		{"hold expired", ErrHoldExpired, 409, "HOLD_EXPIRED"},
		{"sale paused", ErrSalePaused, 503, "SALE_PAUSED"},
		{"valkey closed", fmt.Errorf("inventory: hold: %w", redis.ErrClosed), 503, "UNAVAILABLE"},
		{"deadline", fmt.Errorf("inventory: hold: %w", context.DeadlineExceeded), 503, "UNAVAILABLE"},
		{"network", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, 503, "UNAVAILABLE"},
		{"unexpected", errors.New("boom"), 500, "INTERNAL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := uuid.NewString()
			h := newHarness(&fakeService{create: func(context.Context, CreateHoldRequest) (HoldResult, error) {
				return HoldResult{}, tc.err
			}})
			rec := do(h.public, http.MethodPost, "/v1/events/"+ev+"/holds", `{"quantity":1}`, map[string]string{
				"X-Test-User": uuid.NewString(), "X-Test-Event": ev, "Idempotency-Key": "order-00000001",
			})
			if rec.Code != tc.status || problemCode(t, rec) != tc.code {
				t.Fatalf("got %d %s", rec.Code, rec.Body.String())
			}
			wantRetry := map[string]string{"UNAVAILABLE": "1", "SALE_PAUSED": "10"}[tc.code]
			if tc.status == 503 && rec.Header().Get("Retry-After") != wantRetry {
				t.Fatalf("503 must tell clients when to retry: Retry-After %q, want %q", rec.Header().Get("Retry-After"), wantRetry)
			}
			if tc.code == "INVALID_QUANTITY" && !strings.Contains(rec.Body.String(), `"detail":"quantity must be between 1 and 4 for this event"`) {
				t.Fatalf("detail should be client-friendly: %s", rec.Body.String())
			}
		})
	}
}

func TestGetAndCancelHold(t *testing.T) {
	ev, user, holdID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	var cancelled string
	h := newHarness(&fakeService{
		get: func(_ context.Context, e, u, id string) (Hold, error) {
			return Hold{ID: id, EventID: e, UserID: u, Quantity: 1, State: StatePaying}, nil
		},
		cancel: func(_ context.Context, _, _, id string) error { cancelled = id; return nil },
	})
	headers := map[string]string{"X-Test-User": user, "X-Test-Event": ev}
	rec := do(h.public, http.MethodGet, "/v1/events/"+ev+"/holds/"+holdID, "", headers)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"state":"PAYING"`) || strings.Contains(rec.Body.String(), "remaining") {
		t.Fatalf("get: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(h.public, http.MethodDelete, "/v1/events/"+ev+"/holds/"+holdID, "", headers)
	if rec.Code != http.StatusNoContent || cancelled != holdID {
		t.Fatalf("cancel: %d, cancelled %q", rec.Code, cancelled)
	}
}

func TestAvailabilityIsCacheableAndNeverNegative(t *testing.T) {
	ev := uuid.NewString()
	h := newHarness(&fakeService{avail: func(_ context.Context, e string) (Availability, error) {
		return Availability{EventID: e, Available: -2, Capacity: 100}, nil
	}})
	rec := do(h.public, http.MethodGet, "/v1/events/"+ev+"/availability", "", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "public, max-age=1" {
		t.Fatalf("status %d, Cache-Control %q", rec.Code, rec.Header().Get("Cache-Control"))
	}
	var body availabilityResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Available != 0 || !body.SoldOut || body.Capacity != 100 {
		t.Fatalf("body %+v", body)
	}
}

func TestProvisionLivesOnlyOnTheInternalRouter(t *testing.T) {
	ev := uuid.NewString()
	results := []struct {
		created bool
		err     error
		status  int
	}{{true, nil, 201}, {false, nil, 200}, {false, ErrProvisionConflict, 409}}
	for _, r := range results {
		h := newHarness(&fakeService{provision: func(_ context.Context, _ string, c EventConfig) (bool, error) {
			if c.Capacity != 1000 || c.PerUserLimit != 4 {
				t.Fatalf("config %+v", c)
			}
			return r.created, r.err
		}})
		rec := do(h.internal, http.MethodPut, "/internal/v1/events/"+ev+"/inventory", `{"capacity":1000,"perUserLimit":4}`, nil)
		if rec.Code != r.status {
			t.Fatalf("internal: got %d, want %d (%s)", rec.Code, r.status, rec.Body.String())
		}
		if rec := do(h.public, http.MethodPut, "/internal/v1/events/"+ev+"/inventory", `{"capacity":1000,"perUserLimit":4}`, nil); rec.Code != http.StatusNotFound {
			t.Fatalf("operator route must not exist on the public router, got %d", rec.Code)
		}
	}
}

func TestFreezeLivesOnlyOnTheInternalRouter(t *testing.T) {
	ev := uuid.NewString()
	var calls []bool
	h := newHarness(&fakeService{freeze: func(_ context.Context, e string, frozen bool) (bool, error) {
		if e != ev {
			t.Fatalf("event %q", e)
		}
		calls = append(calls, frozen)
		return true, nil
	}})
	for _, tc := range []struct {
		path   string
		frozen bool
	}{{"freeze", true}, {"unfreeze", false}} {
		rec := do(h.internal, http.MethodPost, "/internal/v1/events/"+ev+"/"+tc.path, "", nil)
		want := fmt.Sprintf(`"frozen":%t`, tc.frozen)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) || !strings.Contains(rec.Body.String(), `"changed":true`) {
			t.Fatalf("%s: %d %s", tc.path, rec.Code, rec.Body.String())
		}
		if rec := do(h.public, http.MethodPost, "/internal/v1/events/"+ev+"/"+tc.path, "", nil); rec.Code != http.StatusNotFound {
			t.Fatalf("%s must not exist on the public router, got %d", tc.path, rec.Code)
		}
	}
	if len(calls) != 2 || !calls[0] || calls[1] {
		t.Fatalf("calls %v, want [true false]", calls)
	}
	h = newHarness(&fakeService{freeze: func(context.Context, string, bool) (bool, error) { return false, ErrEventNotProvisioned }})
	if rec := do(h.internal, http.MethodPost, "/internal/v1/events/"+ev+"/freeze", "", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown event: got %d", rec.Code)
	}
}
