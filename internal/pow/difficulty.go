package pow

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Difficulty decides how hard new challenges are: Base normally, and one bit
// more for each doubling of the challenge rate above Surge (challenges per
// second in this replica), up to Max. One more bit doubles every client's
// expected work, so a spike in demand, which is when bots matter most, raises
// the cost of each join. Each replica judges its own share of the traffic,
// so no coordination is needed.
type Difficulty struct {
	base, max int
	surge     float64
	now       func() time.Time

	mu     sync.Mutex
	window time.Time // when the current one-second window started
	count  int       // challenges issued in the current window
	last   int       // challenges issued in the previous window
}

// NewDifficulty returns a Difficulty. base and max are bits (1 to
// MaxDifficulty, base <= max); surge is a rate per second above zero.
func NewDifficulty(base, max int, surge float64) (*Difficulty, error) {
	if base < 1 || max > MaxDifficulty || base > max {
		return nil, fmt.Errorf("pow: difficulties must satisfy 1 <= base <= max <= %d", MaxDifficulty)
	}
	if surge <= 0 {
		return nil, fmt.Errorf("pow: the surge rate must be above zero")
	}
	return &Difficulty{base: base, max: max, surge: surge, now: time.Now}, nil
}

// Next records one challenge about to be issued and returns its difficulty.
func (d *Difficulty) Next() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.roll()
	d.count++
	return d.level()
}

// Current returns the difficulty a challenge issued now would get, without
// recording one.
func (d *Difficulty) Current() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.roll()
	return d.level()
}

// roll starts a new one-second window when the current one is over.
func (d *Difficulty) roll() {
	now := d.now()
	switch elapsed := now.Sub(d.window); {
	case elapsed < time.Second:
		return
	case elapsed < 2*time.Second:
		d.last = d.count
	default:
		d.last = 0 // a quiet second in between
	}
	d.window, d.count = now, 0
}

// level is the difficulty for the higher of the last and the current
// window's rate, so a surge counts as soon as it starts.
func (d *Difficulty) level() int {
	rate := float64(max(d.last, d.count))
	if rate <= d.surge {
		return d.base
	}
	extra := int(math.Floor(math.Log2(rate/d.surge))) + 1
	return min(d.base+extra, d.max)
}
