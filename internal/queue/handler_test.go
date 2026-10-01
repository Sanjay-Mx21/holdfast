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

	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

type fakeService struct {
	provision func(ctx context.Context, eventID string, cfg EventConfig) (bool, error)
}

func (f *fakeService) Provision(ctx context.Context, e string, c EventConfig) (bool, error) {
	return f.provision(ctx, e, c)
}

// newAdmin wires the real routes with a pass-through operator middleware.
func newAdmin(svc service) *httpx.Router {
	internal := httpx.NewRouter(httpx.RequestID())
	NewHandler(svc).Register(internal, func(next http.Handler) http.Handler { return next })
	return internal
}

func put(h http.Handler, path, body string, contentType string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
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
	eventPath = "/internal/v1/events/0196F0C1-7A3E-7C51-9B0E-5D2F8A1C4E77/queue"
	validBody = `{"opensAt":"2026-10-05T12:00:00Z","admissionRatePerSecond":83,"maxSessions":10000,"sessionTtlSeconds":600}`
)

func TestProvisionMapsBodyAndReportsCreation(t *testing.T) {
	for _, created := range []bool{true, false} {
		t.Run(fmt.Sprint("created=", created), func(t *testing.T) {
			var gotEvent string
			var gotCfg EventConfig
			h := newAdmin(&fakeService{provision: func(_ context.Context, e string, c EventConfig) (bool, error) {
				gotEvent, gotCfg = e, c
				return created, nil
			}})
			rec := put(h, eventPath, validBody, "application/json")

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
			if resp["eventId"] != "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77" || resp["created"] != created {
				t.Fatalf("response = %v", resp)
			}
		})
	}
}

func TestProvisionErrors(t *testing.T) {
	notCalled := func(context.Context, string, EventConfig) (bool, error) {
		return false, errors.New("service must not be called")
	}
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
		{"empty body", "", "application/json", notCalled, http.StatusBadRequest, "EMPTY_BODY"},
		{"malformed JSON", `{"opensAt":`, "application/json", notCalled, http.StatusBadRequest, "MALFORMED_JSON"},
		{"unknown field", `{"opensAt":"2026-10-05T12:00:00Z","capacity":5}`, "application/json", notCalled, http.StatusBadRequest, "INVALID_BODY"},
		{"wrong type", `{"maxSessions":"many"}`, "application/json", notCalled, http.StatusBadRequest, "INVALID_FIELD_TYPE"},
		{"not JSON", validBody, "text/plain", notCalled, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE"},
		{"validation", validBody, "application/json", failing(fmt.Errorf("%w: admission rate must be between 1 and 100000 per second", ErrInvalidRequest)), http.StatusBadRequest, "INVALID_REQUEST"},
		{"conflict", validBody, "application/json", failing(ErrProvisionConflict), http.StatusConflict, "PROVISION_CONFLICT"},
		{"Valkey down", validBody, "application/json", failing(&net.OpError{Op: "dial", Err: errors.New("connection refused")}), http.StatusServiceUnavailable, "UNAVAILABLE"},
		{"timeout", validBody, "application/json", failing(context.DeadlineExceeded), http.StatusServiceUnavailable, "UNAVAILABLE"},
		{"unexpected", validBody, "application/json", failing(errors.New("boom")), http.StatusInternalServerError, "INTERNAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := put(newAdmin(&fakeService{provision: tt.svc}), eventPath, tt.body, tt.contentType)
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
	h := newAdmin(&fakeService{provision: func(context.Context, string, EventConfig) (bool, error) {
		return false, fmt.Errorf("%w: maximum sessions must be between 1 and 10000000", ErrInvalidRequest)
	}})
	rec := put(h, eventPath, validBody, "application/json")
	if !strings.Contains(rec.Body.String(), "maximum sessions must be between 1 and 10000000") {
		t.Fatalf("detail missing from body %s", rec.Body)
	}
	if strings.Contains(rec.Body.String(), "queue: invalid request") {
		t.Fatalf("internal error prefix leaked: %s", rec.Body)
	}
}

func TestProvisionRouteIsPUTOnly(t *testing.T) {
	h := newAdmin(&fakeService{provision: func(context.Context, string, EventConfig) (bool, error) { return true, nil }})
	req := httptest.NewRequest(http.MethodPost, eventPath, strings.NewReader(validBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}
}
