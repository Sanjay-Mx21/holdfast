package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func readyz(h *Health) (int, map[string]any) {
	rec := httptest.NewRecorder()
	h.Readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestReadiness(t *testing.T) {
	ok := Check{Name: "ok", Fn: func(context.Context) error { return nil }}
	down := Check{Name: "down", Fn: func(context.Context) error { return errors.New("connection refused") }}
	hung := Check{Name: "hung", Fn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}

	cases := []struct {
		name   string
		checks []Check
		want   int
	}{
		{"all dependencies up", []Check{ok}, http.StatusOK},
		{"one dependency down", []Check{ok, down}, http.StatusServiceUnavailable},
		{"hung dependency times out", []Check{hung}, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			code, _ := readyz(New(100*time.Millisecond, tc.checks...))
			if code != tc.want {
				t.Fatalf("status = %d, want %d", code, tc.want)
			}
			if time.Since(start) > time.Second {
				t.Fatal("readiness must respect its timeout")
			}
		})
	}
}

func TestDrainingFailsReadinessButNotLiveness(t *testing.T) {
	h := New(time.Second, Check{Name: "ok", Fn: func(context.Context) error { return nil }})
	h.SetDraining()
	if code, body := readyz(h); code != http.StatusServiceUnavailable || body["status"] != "draining" {
		t.Fatalf("readyz while draining = %d %v", code, body)
	}
	rec := httptest.NewRecorder()
	h.Livez(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("livez while draining = %d, want 200", rec.Code)
	}
}
