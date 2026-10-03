package breaker

import (
	"errors"
	"testing"
	"time"
)

func newTest(threshold int, cooldown time.Duration) (*Breaker, *time.Time, *[]State) {
	now := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	var changes []State
	b := New(threshold, cooldown, func(s State) { changes = append(changes, s) })
	b.now = func() time.Time { return now }
	return b, &now, &changes
}

func call(b *Breaker, fail bool) error {
	if err := b.Allow(); err != nil {
		return err
	}
	b.Done(fail)
	return nil
}

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	b, _, changes := newTest(3, time.Minute)
	_ = call(b, true)
	_ = call(b, true)
	_ = call(b, false) // a success resets the count
	_ = call(b, true)
	_ = call(b, true)
	if b.State() != Closed {
		t.Fatal("opened before 3 consecutive failures")
	}
	_ = call(b, true)
	if b.State() != Open || len(*changes) != 1 {
		t.Fatalf("state %s after 3 consecutive failures, changes %v", b.State(), *changes)
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("an open breaker allowed a call: %v", err)
	}
}

func TestHalfOpenAllowsOneTrial(t *testing.T) {
	b, now, changes := newTest(1, time.Minute)
	_ = call(b, true)
	*now = now.Add(time.Minute)
	if err := b.Allow(); err != nil {
		t.Fatalf("no trial after the cooldown: %v", err)
	}
	if b.State() != HalfOpen {
		t.Fatalf("state %s, want half_open", b.State())
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatal("a second call went through while the trial was in flight")
	}
	b.Done(false)
	if b.State() != Closed {
		t.Fatalf("a successful trial left it %s", b.State())
	}
	if got := *changes; len(got) != 3 || got[0] != Open || got[1] != HalfOpen || got[2] != Closed {
		t.Fatalf("changes %v", got)
	}
}

func TestAFailedTrialOpensAgain(t *testing.T) {
	b, now, _ := newTest(1, time.Minute)
	_ = call(b, true)
	*now = now.Add(time.Minute)
	_ = call(b, true)
	if b.State() != Open {
		t.Fatalf("state %s after a failed trial, want open", b.State())
	}
	*now = now.Add(30 * time.Second)
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatal("the cooldown did not restart after the failed trial")
	}
	if Closed.String() != "closed" || HalfOpen.String() != "half_open" {
		t.Fatal("state names")
	}
}
