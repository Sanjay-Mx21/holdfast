package httpx

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	return p
}

func TestRouterLabelsRoutesAndAnswersUnknownRoutesWithProblems(t *testing.T) {
	rt := NewRouter(RequestID())
	rt.HandleFunc("GET /v1/things/{id}", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id"), "route": RouteFrom(r.Context())})
	})

	t.Run("matched route carries its pattern", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/things/42", nil))
		var body map[string]string
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if rec.Code != http.StatusOK || body["id"] != "42" || body["route"] != "GET /v1/things/{id}" {
			t.Fatalf("got %d %v", rec.Code, body)
		}
	})
	t.Run("unknown path is a 404 problem", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
		p := decodeProblem(t, rec)
		if rec.Code != http.StatusNotFound || p.Code != "ROUTE_NOT_FOUND" || p.RequestID == "" || p.Instance != "/nope" {
			t.Fatalf("got %d %+v", rec.Code, p)
		}
	})
	t.Run("wrong method is a 405 problem with Allow", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/things/42", nil))
		p := decodeProblem(t, rec)
		if rec.Code != http.StatusMethodNotAllowed || p.Code != "METHOD_NOT_ALLOWED" {
			t.Fatalf("got %d %+v", rec.Code, p)
		}
		if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
			t.Fatalf("Allow = %q, want it to list GET", allow)
		}
	})
}

func TestRequestIDKeepsWellFormedIDsAndReplacesOthers(t *testing.T) {
	var seen string
	h := RequestID()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = RequestIDFrom(r.Context()) }))
	for _, tc := range []struct {
		in   string
		keep bool
	}{
		{"edge-7f3a_01.b:c", true},
		{"", false},
		{"has space", false},
		{"inject\r\nX-Evil: 1", false},
		{strings.Repeat("a", 129), false},
	} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if tc.in != "" {
			req.Header[HeaderRequestID] = []string{tc.in}
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		got := rec.Header().Get(HeaderRequestID)
		if got != seen {
			t.Fatalf("response ID %q != context ID %q", got, seen)
		}
		if tc.keep && got != tc.in {
			t.Fatalf("well-formed ID %q replaced by %q", tc.in, got)
		}
		if !tc.keep {
			if u, err := uuid.Parse(got); err != nil || u.Version() != 7 {
				t.Fatalf("input %q: want a generated UUIDv7, got %q", tc.in, got)
			}
		}
	}
}

func TestRecoverTurnsPanicsIntoOpaque500Problems(t *testing.T) {
	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("secret internals") }), RequestID(), Recover())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	p := decodeProblem(t, rec)
	if rec.Code != http.StatusInternalServerError || p.Code != "INTERNAL" || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}

func TestDecodeJSON(t *testing.T) {
	type body struct {
		Quantity int `json:"quantity"`
	}
	cases := []struct {
		name, contentType, body string
		limit                   int64
		status                  int
		code                    string
	}{
		{"valid", "application/json", `{"quantity":2}`, 0, 0, ""},
		{"valid with charset", "application/json; charset=utf-8", `{"quantity":2}`, 0, 0, ""},
		{"no content type", "", `{"quantity":2}`, 0, 0, ""},
		{"unknown field", "application/json", `{"quantity":2,"price":1}`, 0, 400, "INVALID_BODY"},
		{"trailing data", "application/json", `{"quantity":2}{}`, 0, 400, "TRAILING_DATA"},
		{"empty", "application/json", ``, 0, 400, "EMPTY_BODY"},
		{"malformed", "application/json", `{"quantity":`, 0, 400, "MALFORMED_JSON"},
		{"wrong type", "application/json", `{"quantity":"two"}`, 0, 400, "INVALID_FIELD_TYPE"},
		{"too large", "application/json", `{"quantity":2}`, 5, 413, "BODY_TOO_LARGE"},
		{"wrong media type", "text/plain", `{"quantity":2}`, 0, 415, "UNSUPPORTED_MEDIA_TYPE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.limit > 0 {
				req.Body = http.MaxBytesReader(httptest.NewRecorder(), req.Body, tc.limit)
			}
			var b body
			p := DecodeJSON(req, &b)
			if tc.status == 0 {
				if p != nil || b.Quantity != 2 {
					t.Fatalf("got problem %+v, quantity %d", p, b.Quantity)
				}
				return
			}
			if p == nil || p.Status != tc.status || p.Code != tc.code {
				t.Fatalf("got %+v, want %d %s", p, tc.status, tc.code)
			}
		})
	}
}

func TestHTTPMetricsUseRoutePatternsNotRawPaths(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewHTTPMetrics(reg)
	rt := NewRouter(m.Middleware())
	rt.HandleFunc("GET /v1/things/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	for _, p := range []string{"/v1/things/1", "/v1/things/2", "/random/path"} {
		rt.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
	}
	if got := counter(t, reg, "holdfast_http_requests_total", map[string]string{"route": "GET /v1/things/{id}", "code": "204"}); got != 2 {
		t.Fatalf("matched route count = %v, want 2", got)
	}
	if got := counter(t, reg, "holdfast_http_requests_total", map[string]string{"route": "unmatched", "code": "404"}); got != 1 {
		t.Fatalf("unmatched count = %v, want 1", got)
	}
}

func counter(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			matched := 0
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok {
					if want != lp.GetValue() {
						continue next
					}
					matched++
				}
			}
			if matched == len(labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func testHTTPConfig() config.HTTP {
	return config.HTTP{
		ReadHeaderTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
		IdleTimeout: time.Second, ShutdownTimeout: 2 * time.Second,
	}
}

func TestServerStopsCleanlyWhenContextIsCancelled(t *testing.T) {
	srv := NewServer("test", "127.0.0.1:0", http.NotFoundHandler(), testHTTPConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestServerFailsFastWhenThePortIsTaken(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	srv := NewServer("test", ln.Addr().String(), http.NotFoundHandler(), testHTTPConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("want an error for a busy port")
	}
}
