// Package stats holds the small statistics HoldFast's experiments need.
package stats

import (
	"errors"
	"math"
	"slices"
	"sort"
)

// ErrTooFew means a correlation was asked of fewer than two pairs.
var ErrTooFew = errors.New("stats: need at least two pairs")

// ErrConstant means one series has no spread, so no correlation exists.
var ErrConstant = errors.New("stats: a series is constant")

// Spearman returns Spearman's rank correlation between x and y: the Pearson
// correlation of their ranks, with tied values given the average of the ranks
// they span. It is 1 when y always rises with x, -1 when it always falls, and
// close to 0 when the order of one says nothing about the other.
//
// Experiment E6 uses it on (join time, queue position): close to 0 for
// lottery joiners, exactly 1 for joiners after T0.
func Spearman(x, y []float64) (float64, error) {
	if len(x) != len(y) {
		return 0, errors.New("stats: series differ in length")
	}
	if len(x) < 2 {
		return 0, ErrTooFew
	}
	rx, ry := Ranks(x), Ranks(y)
	// Identical or exactly reversed ranks are a perfect correlation; say so
	// exactly, rather than as 1 minus a rounding error for large n.
	same, reversed := true, true
	for i := range rx {
		same = same && rx[i] == ry[i]
		reversed = reversed && rx[i]+ry[i] == float64(len(rx)+1)
	}
	switch {
	case same && !isConstant(rx):
		return 1, nil
	case reversed && !isConstant(rx):
		return -1, nil
	}
	return pearson(rx, ry)
}

func isConstant(r []float64) bool { return slices.Min(r) == slices.Max(r) }

// Ranks returns the 1-based rank of every value in v, in v's order. Tied
// values share the average of the ranks they span: Ranks([10 20 20 30]) is
// [1 2.5 2.5 4].
func Ranks(v []float64) []float64 {
	idx := make([]int, len(v))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return v[idx[a]] < v[idx[b]] })
	r := make([]float64, len(v))
	for i := 0; i < len(idx); {
		j := i + 1
		for j < len(idx) && v[idx[j]] == v[idx[i]] {
			j++
		}
		avg := float64(i+j+1) / 2 // average of the 1-based ranks i+1 .. j
		for k := i; k < j; k++ {
			r[idx[k]] = avg
		}
		i = j
	}
	return r
}

func pearson(x, y []float64) (float64, error) {
	n := float64(len(x))
	var mx, my float64
	for i := range x {
		mx += x[i]
		my += y[i]
	}
	mx /= n
	my /= n
	var sxy, sxx, syy float64
	for i := range x {
		dx, dy := x[i]-mx, y[i]-my
		sxy += dx * dy
		sxx += dx * dx
		syy += dy * dy
	}
	if sxx == 0 || syy == 0 {
		return 0, ErrConstant
	}
	return sxy / math.Sqrt(sxx*syy), nil
}
