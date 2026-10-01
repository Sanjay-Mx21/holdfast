package queue

import (
	"testing"
	"time"
)

func TestAllowanceRefillsAtRateAndCapsAtOneSecond(t *testing.T) {
	start := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	a := newAllowance(80, start)
	if got := a.available(start); got != 0 {
		t.Fatalf("available at start = %d, want 0 (no burst on becoming leader)", got)
	}
	if got := a.available(start.Add(250 * time.Millisecond)); got != 20 {
		t.Fatalf("after 250ms at 80/s = %d, want 20", got)
	}
	a.take(20)
	if got := a.available(start.Add(500 * time.Millisecond)); got != 20 {
		t.Fatalf("after another 250ms = %d, want 20", got)
	}
	// A long pause refills at most one second's worth.
	if got := a.available(start.Add(time.Minute)); got != 80 {
		t.Fatalf("after a long pause = %d, want 80 (one second's worth)", got)
	}
}

func TestAllowanceKeepsUnusedTokensAndCarriesFractions(t *testing.T) {
	start := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	a := newAllowance(10, start)
	// 10/s on a 30ms tick: 0.3 tokens per tick; fractions must accumulate.
	total := 0
	for i := 1; i <= 100; i++ {
		n := a.available(start.Add(time.Duration(i) * 30 * time.Millisecond))
		a.take(int64(n))
		total += n
	}
	if total < 29 || total > 30 {
		t.Fatalf("admitted %d over 3s at 10/s, want 29 to 30", total)
	}
	// Admitting fewer than available (session budget full) keeps the rest.
	b := newAllowance(10, start)
	if got := b.available(start.Add(500 * time.Millisecond)); got != 5 {
		t.Fatalf("available = %d, want 5", got)
	}
	b.take(2)
	if got := b.available(start.Add(500 * time.Millisecond)); got != 3 {
		t.Fatalf("after taking 2 of 5, available = %d, want 3", got)
	}
	b.take(10)
	if got := b.available(start.Add(500 * time.Millisecond)); got != 0 {
		t.Fatalf("over-take must floor at 0, got %d", got)
	}
}

func TestLeaderLockKey(t *testing.T) {
	a := leaderLockKey("0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77")
	if a != leaderLockKey("0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77") {
		t.Fatal("lock key is not stable")
	}
	if a == leaderLockKey("0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e78") {
		t.Fatal("different events share a lock key")
	}
}
