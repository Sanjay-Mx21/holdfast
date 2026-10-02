package stats

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-12 }

func TestRanksAverageTies(t *testing.T) {
	tests := []struct {
		in, want []float64
	}{
		{[]float64{10, 20, 20, 30}, []float64{1, 2.5, 2.5, 4}},
		{[]float64{3, 1, 2}, []float64{3, 1, 2}},
		{[]float64{5, 5, 5}, []float64{2, 2, 2}},
		{[]float64{2, 1, 2, 1}, []float64{3.5, 1.5, 3.5, 1.5}},
		{nil, []float64{}},
	}
	for _, tt := range tests {
		if got := Ranks(tt.in); !slices.Equal(got, tt.want) {
			t.Errorf("Ranks(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestSpearmanMonotonic(t *testing.T) {
	x := []float64{1, 2, 3, 4, 5, 6}
	// Any increasing transformation correlates perfectly: only order matters.
	up := []float64{-10, 0, 0.5, 7, 100, 1e9}
	down := slices.Clone(up)
	slices.Reverse(down)
	if r, err := Spearman(x, up); err != nil || r != 1 {
		t.Fatalf("increasing: %v %v, want exactly 1", r, err)
	}
	if r, err := Spearman(x, down); err != nil || r != -1 {
		t.Fatalf("decreasing: %v %v, want exactly -1", r, err)
	}
}

func TestSpearmanKnownValues(t *testing.T) {
	// Textbook example: ranks differ by d = (0,0,1,-1,0); 1 - 6*2/(5*24) = 0.9.
	r, err := Spearman([]float64{1, 2, 3, 4, 5}, []float64{1, 2, 4, 3, 5})
	if err != nil || !near(r, 0.9) {
		t.Fatalf("got %v %v, want 0.9", r, err)
	}
	// With ties: x ranks [1 2.5 2.5 4], y ranks [1 2 3 4]; Pearson of those.
	r, err = Spearman([]float64{1, 2, 2, 3}, []float64{1, 2, 3, 4})
	want := 4.5 / math.Sqrt(4.5*5)
	if err != nil || !near(r, want) {
		t.Fatalf("with ties: got %v %v, want %v", r, err, want)
	}
}

func TestSpearmanIndependentIsNearZero(t *testing.T) {
	// 100,000 lottery positions against arrival order, as in E6. For
	// independent series the standard error is 1/sqrt(n-1), about 0.0032, so
	// |r| < 0.02 is more than six standard errors: a fixed seed keeps it exact.
	rng := rand.New(rand.NewPCG(1, 2))
	n := 100_000
	arrival := make([]float64, n)
	position := make([]float64, n)
	for i := range n {
		arrival[i] = float64(i)
		position[i] = rng.Float64()
	}
	r, err := Spearman(arrival, position)
	if err != nil || math.Abs(r) > 0.02 {
		t.Fatalf("independent series: r = %v %v, want close to 0", r, err)
	}
}

func TestSpearmanErrors(t *testing.T) {
	if _, err := Spearman([]float64{1, 2}, []float64{1}); err == nil {
		t.Fatal("different lengths accepted")
	}
	if _, err := Spearman([]float64{1}, []float64{1}); !errors.Is(err, ErrTooFew) {
		t.Fatalf("one pair: %v, want ErrTooFew", err)
	}
	if _, err := Spearman([]float64{1, 2, 3}, []float64{7, 7, 7}); !errors.Is(err, ErrConstant) {
		t.Fatalf("constant series: %v, want ErrConstant", err)
	}
}
