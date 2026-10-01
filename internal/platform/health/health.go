// Package health implements liveness and readiness endpoints.
//
// Liveness answers "is the process alive?" and never checks dependencies, so a
// flaky database can't make an orchestrator restart healthy pods in a loop.
// Readiness answers "should this instance receive traffic?": it checks
// dependencies and turns false as soon as shutdown begins, so load balancers
// stop routing new requests before the server stops accepting them.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Check is one dependency probe. Fn must respect ctx cancellation.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Health aggregates dependency checks.
type Health struct {
	checks   []Check
	timeout  time.Duration
	draining atomic.Bool
}

// New returns a Health that gives each readiness evaluation at most timeout.
func New(timeout time.Duration, checks ...Check) *Health {
	return &Health{checks: checks, timeout: timeout}
}

// SetDraining makes readiness fail from now on. Called when shutdown starts.
func (h *Health) SetDraining() { h.draining.Store(true) }

// Livez always reports OK while the process can serve HTTP.
func (h *Health) Livez(w http.ResponseWriter, _ *http.Request) {
	write(w, http.StatusOK, map[string]string{"status": "ok"})
}

type checkResult struct {
	Name  string `json:"name"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// Readyz runs every check concurrently and reports 503 if any fails.
func (h *Health) Readyz(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		write(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results := make([]checkResult, len(h.checks))
	var wg sync.WaitGroup
	for i, c := range h.checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := checkResult{Name: c.Name, OK: true}
			if err := c.Fn(ctx); err != nil {
				res.OK, res.Error = false, err.Error()
			}
			results[i] = res
		}()
	}
	wg.Wait()

	status, overall := http.StatusOK, "ok"
	for _, res := range results {
		if !res.OK {
			status, overall = http.StatusServiceUnavailable, "unavailable"
			break
		}
	}
	write(w, status, map[string]any{"status": overall, "checks": results})
}

func write(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
