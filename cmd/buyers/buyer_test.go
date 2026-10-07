package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const testEvent = "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"

// fakeStack answers like the edge and mockpsp: each buyer waits one turn,
// its first hold meets a 503 (a failover), its booking settles on the second
// read, and one buyer's payment is declined.
type fakeStack struct {
	mu       sync.Mutex
	srv      *httptest.Server
	decline  string
	calls    map[string]int    // by user and step
	holdKeys map[string]string // the Idempotency-Key of each user's first hold
	mismatch []string
}

func (f *fakeStack) count(user, step string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[user+" "+step]++
	return f.calls[user+" "+step]
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeStack) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user := r.Header.Get("X-Dev-User-Id")
	p := r.URL.Path
	switch {
	case p == "/v1/queue/"+testEvent+"/join":
		reply(w, 200, map[string]any{"rank": 1})
	case p == "/v1/queue/"+testEvent+"/admit":
		if f.count(user, "admit") == 1 {
			reply(w, 409, map[string]string{"code": "NOT_YOUR_TURN"})
			return
		}
		reply(w, 200, map[string]string{"token": "t-" + user})
	case p == "/v1/events/"+testEvent+"/holds":
		user = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer t-")
		key := r.Header.Get("Idempotency-Key")
		if f.count(user, "hold") == 1 {
			f.mu.Lock()
			f.holdKeys[user] = key
			f.mu.Unlock()
			w.Header().Set("Retry-After", "0")
			reply(w, 503, map[string]string{"code": "UNAVAILABLE"})
			return
		}
		f.mu.Lock()
		if f.holdKeys[user] != key {
			f.mismatch = append(f.mismatch, user)
		}
		f.mu.Unlock()
		reply(w, 201, map[string]string{"holdId": "h-" + user})
	case p == "/v1/bookings" && r.Method == http.MethodPost:
		reply(w, 201, map[string]string{"bookingId": "b-" + user, "status": "PENDING_PAYMENT",
			"checkoutUrl": f.srv.URL + "/checkout/o-" + user})
	case strings.HasPrefix(p, "/checkout/o-") && strings.HasSuffix(p, "/pay"):
		user = strings.TrimSuffix(strings.TrimPrefix(p, "/checkout/o-"), "/pay")
		status := "CAPTURED"
		if user == f.decline {
			status = "FAILED"
		}
		reply(w, 200, map[string]string{"status": status})
	case strings.HasPrefix(p, "/v1/bookings/b-"):
		status := "PENDING_PAYMENT"
		if f.count(user, "read") > 1 {
			status = "CONFIRMED"
		}
		reply(w, 200, map[string]string{"bookingId": "b-" + user, "status": status})
	default:
		reply(w, 404, map[string]string{"code": "NOT_FOUND"})
	}
}

func TestBuyersRunWholePurchases(t *testing.T) {
	f := &fakeStack{calls: map[string]int{}, holdKeys: map[string]string{}, decline: userID("t", 2)}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	report := filepath.Join(t.TempDir(), "report.json")

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"-base", f.srv.URL, "-event", testEvent, "-buyers", "3", "-concurrency", "2",
		"-users", "t", "-poll", "1ms", "-json", report,
	}, &out)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}
	var r Report
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	if r.Outcomes[outConfirmed] != 2 || r.Outcomes[outDeclined] != 1 || len(r.Outcomes) != 2 {
		t.Fatalf("outcomes %v, want 2 confirmed and 1 declined\n%s", r.Outcomes, out.String())
	}
	if r.Retries["hold"] != 3 || r.Retries["admit"] != 0 {
		t.Fatalf("retries %v: each hold once after its 503; waiting for a turn is not a retry", r.Retries)
	}
	if len(f.mismatch) > 0 {
		t.Fatalf("holds retried with another Idempotency-Key: %v", f.mismatch)
	}
	if len(r.PayToConfirm) != 4 {
		t.Fatalf("pay-to-confirm summary %v", r.PayToConfirm)
	}
	for _, want := range []string{"3 buyers for event " + testEvent, "confirmed", "declined", "hold"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("report lacks %q:\n%s", want, out.String())
		}
	}
}

func TestBuyersStopAtSoldOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/join"):
			reply(w, 200, map[string]any{})
		case strings.HasSuffix(r.URL.Path, "/admit"):
			reply(w, 409, map[string]string{"code": "QUEUE_CLOSED"})
		default:
			t.Errorf("a sold-out buyer went on to %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-base", srv.URL, "-event", testEvent, "-buyers", "2", "-users", "s"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), outSoldOut) {
		t.Fatalf("report:\n%s", out.String())
	}
}

func TestRunRejectsBadFlags(t *testing.T) {
	for _, args := range [][]string{{}, {"-event", "x"}, {"-event", testEvent, "-buyers", "0"}} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}
