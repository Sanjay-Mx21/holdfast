package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Outcomes: every buyer ends with exactly one.
const (
	outConfirmed   = "confirmed"    // paid, and the booking confirmed
	outRefunded    = "refunded"     // paid, but refused at confirmation and refunded
	outCancelled   = "cancelled"    // paid, but the booking was cancelled (I3 says the money must come back)
	outDeclined    = "declined"     // the provider declined the payment (the failure fault)
	outSoldOut     = "sold_out"     // no unit left: the queue or inventory said so
	outNotAdmitted = "not_admitted" // never admitted before the deadline
	outTurnExpired = "turn_expired" // admitted, but the session slot ran out first
	outUnsettled   = "unsettled_"   // + the booking's status when the settle deadline passed
	outFailed      = "_failed"      // stage + "_failed": a request that kept failing
)

// options are what every buyer needs.
type options struct {
	base, event      string
	qty              int
	attempts         int           // per request, retries included
	pollEvery        time.Duration // admit and booking polls
	admitDeadline    time.Duration // from joining
	settle           time.Duration // from paying to a final booking status
	userNamespace    string
	sleep            func(context.Context, time.Duration) error
	httpClient       *http.Client
	retryAfterCapped time.Duration
}

// stats are shared by every buyer.
type stats struct {
	mu       sync.Mutex
	outcomes map[string]int
	retries  map[string]int // by stage: requests repeated after a failure
	settle   []time.Duration
}

func newStats() *stats { return &stats{outcomes: map[string]int{}, retries: map[string]int{}} }

func (s *stats) outcome(o string) {
	s.mu.Lock()
	s.outcomes[o]++
	s.mu.Unlock()
}

func (s *stats) retry(stage string) {
	s.mu.Lock()
	s.retries[stage]++
	s.mu.Unlock()
}

func (s *stats) settled(d time.Duration) {
	s.mu.Lock()
	s.settle = append(s.settle, d)
	s.mu.Unlock()
}

// problem is the body of an error answer (httpx.Problem): its code.
type problem struct {
	Code string `json:"code"`
}

// answer is one HTTP exchange: the status, the problem code of an error, and
// the Retry-After header.
type answer struct {
	status     int
	code       string
	retryAfter time.Duration
}

func (a answer) ok() bool { return a.status >= 200 && a.status < 300 }

// retryable: no answer at all, a 5xx, or 429. Every request a buyer makes is
// idempotent (an Idempotency-Key, or naturally), so repeating it is safe.
func (a answer) retryable() bool { return a.status == 0 || a.status >= 500 || a.status == 429 }

// once makes one request; out receives a 2xx body.
func (o *options) once(ctx context.Context, method, url string, hdr map[string]string, body, out any) (answer, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return answer{}, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return answer{}, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := o.httpClient.Do(req)
	if err != nil {
		return answer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	a := answer{status: resp.StatusCode}
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
		a.retryAfter = time.Duration(s) * time.Second
	}
	if !a.ok() {
		var p problem
		_ = json.Unmarshal(data, &p)
		a.code = p.Code
		return a, nil
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return a, fmt.Errorf("decode %s %s: %w", method, url, err)
		}
	}
	return a, nil
}

// call repeats a request while its failure may pass (retryable), up to
// attempts, with jittered exponential backoff or the server's Retry-After.
func (o *options) call(ctx context.Context, st *stats, stage, method, url string, hdr map[string]string, body, out any) (answer, error) {
	var a answer
	var err error
	for attempt := 1; ; attempt++ {
		a, err = o.once(ctx, method, url, hdr, body, out)
		if err == nil && !a.retryable() {
			return a, nil
		}
		if attempt >= o.attempts || ctx.Err() != nil {
			if err == nil {
				err = fmt.Errorf("%s %s: %d %s after %d attempts", method, url, a.status, a.code, attempt)
			}
			return a, err
		}
		st.retry(stage)
		wait := min(3*time.Second, 200*time.Millisecond<<min(attempt-1, 4))
		wait = wait/2 + rand.N(wait/2+1) //nolint:gosec // jitter
		if a.retryAfter > 0 {
			wait = min(a.retryAfter, o.retryAfterCapped)
		}
		if err := o.sleep(ctx, wait); err != nil {
			return a, err
		}
	}
}

type admission struct {
	Token string `json:"token"`
}

type hold struct {
	HoldID string `json:"holdId"`
}

type booking struct {
	BookingID   string `json:"bookingId"`
	Status      string `json:"status"`
	CheckoutURL string `json:"checkoutUrl"`
}

type order struct {
	Status string `json:"status"`
}

// buy runs one buyer's whole purchase and returns its outcome.
func (o *options) buy(ctx context.Context, st *stats, user string) string {
	who := map[string]string{"X-Dev-User-Id": user}
	q := o.base + "/v1/queue/" + o.event

	if a, err := o.call(ctx, st, "join", http.MethodPost, q+"/join", who, nil, nil); err != nil || !a.ok() {
		return "join" + outFailed
	}

	// Wait for the turn: claim it until admitted.
	var adm admission
	deadline := time.Now().Add(o.admitDeadline)
	for {
		a, err := o.call(ctx, st, "admit", http.MethodPost, q+"/admit", who, nil, &adm)
		switch {
		case err != nil:
			return "admit" + outFailed
		case a.ok():
		case a.code == "NOT_YOUR_TURN":
			if time.Now().After(deadline) {
				return outNotAdmitted
			}
			if err := o.sleep(ctx, o.pollEvery); err != nil {
				return outNotAdmitted
			}
			continue
		case a.code == "QUEUE_CLOSED":
			return outSoldOut
		case a.code == "TURN_EXPIRED":
			return outTurnExpired
		default:
			return "admit" + outFailed
		}
		break
	}

	var h hold
	a, err := o.call(ctx, st, "hold", http.MethodPost, o.base+"/v1/events/"+o.event+"/holds",
		map[string]string{"Authorization": "Bearer " + adm.Token, "Idempotency-Key": "hold-" + user},
		map[string]int{"quantity": o.qty}, &h)
	switch {
	case err != nil:
		return "hold" + outFailed
	case a.code == "SOLD_OUT":
		return outSoldOut
	case !a.ok():
		return "hold" + outFailed
	}

	var b booking
	bookHdr := map[string]string{"X-Dev-User-Id": user, "Idempotency-Key": "book-" + user}
	if a, err := o.call(ctx, st, "book", http.MethodPost, o.base+"/v1/bookings", bookHdr,
		map[string]string{"eventId": o.event, "holdId": h.HoldID}, &b); err != nil || !a.ok() {
		return "book" + outFailed
	}

	var ord order
	if a, err := o.call(ctx, st, "pay", http.MethodPost, b.CheckoutURL+"/pay", nil, nil, &ord); err != nil || !a.ok() {
		return "pay" + outFailed
	}
	if ord.Status != "CAPTURED" {
		return outDeclined // the booking runs out its payment window and is cancelled
	}

	// Paid: wait for the saga to settle the booking.
	paid := time.Now()
	settleBy := paid.Add(o.settle)
	for {
		a, err := o.call(ctx, st, "settle", http.MethodGet, o.base+"/v1/bookings/"+b.BookingID, who, nil, &b)
		if err == nil && a.ok() {
			switch b.Status {
			case "CONFIRMED":
				st.settled(time.Since(paid))
				return outConfirmed
			case "REFUNDED":
				return outRefunded
			case "CANCELLED":
				return outCancelled
			}
		}
		if time.Now().After(settleBy) || ctx.Err() != nil {
			if b.Status == "" {
				return outUnsettled + "unknown"
			}
			return outUnsettled + b.Status
		}
		if err := o.sleep(ctx, o.pollEvery); err != nil {
			return outUnsettled + b.Status
		}
	}
}

// sleepCtx waits d, or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
