package queue

import (
	"strconv"
	"testing"
)

func TestLotteryScoreRangeAndRoundTrip(t *testing.T) {
	seen := make(map[string]bool, 10_000)
	for range 10_000 {
		s, err := lotteryScore()
		if err != nil {
			t.Fatal(err)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatalf("score %q does not parse: %v", s, err)
		}
		if f < 0 || f >= 1 {
			t.Fatalf("score %v outside [0, 1)", f)
		}
		if strconv.FormatFloat(f, 'g', 17, 64) != s {
			t.Fatalf("score %q does not round-trip", s)
		}
		if seen[s] {
			t.Fatalf("duplicate score %q in 10,000 draws (53 bits of entropy should not collide)", s)
		}
		seen[s] = true
	}
}

func TestOrderingOf(t *testing.T) {
	for _, tt := range []struct {
		score float64
		want  Ordering
	}{
		{0, OrderingLottery}, {0.999999, OrderingLottery}, {1, OrderingFIFO}, {2, OrderingFIFO}, {1_000_001, OrderingFIFO},
	} {
		if got := orderingOf(tt.score); got != tt.want {
			t.Errorf("orderingOf(%v) = %s, want %s", tt.score, got, tt.want)
		}
	}
}
