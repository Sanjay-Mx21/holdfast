package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
)

// The leader's tick loop under testing/synctest: time is fake and advances
// only when every goroutine in the bubble is blocked, so the tests below
// cover seconds of ticking in microseconds, with exact timings.

const syncEvent = "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"

// tickCall is one call the loop made to advance.
type tickCall struct {
	at       time.Duration // since the term started
	n        int
	unitsCap int
}

// fakeAdvance admits min(n, budget()) and records every call.
type fakeAdvance struct {
	start   time.Time
	calls   []tickCall
	budget  func() int
	err     func(call int) error
	soldOut int   // MarkSoldOut calls
	markErr error // what MarkSoldOut returns
}

func (f *fakeAdvance) MarkSoldOut(context.Context, string, int64) (bool, error) {
	f.soldOut++
	return f.markErr == nil && f.soldOut == 1, f.markErr
}

func (f *fakeAdvance) AdvanceWithin(_ context.Context, eventID string, _ int64, n, unitsCap int) (Advance, error) {
	f.calls = append(f.calls, tickCall{at: time.Since(f.start), n: n, unitsCap: unitsCap})
	if eventID != syncEvent {
		return Advance{}, errors.New("wrong event")
	}
	if f.err != nil {
		if err := f.err(len(f.calls)); err != nil {
			return Advance{}, err
		}
	}
	admitted := n
	if f.budget != nil {
		admitted = min(n, f.budget())
	}
	return Advance{Admitted: int64(admitted)}, nil
}

func syncController(tick time.Duration) (*Controller, *Metrics) {
	m := NewMetrics(prometheus.NewRegistry())
	c := &Controller{eventID: syncEvent, cfg: AdmissionConfig{Tick: tick}, m: m, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return c, m
}

func alive(context.Context) error { return nil }

// runFor runs lead for d of fake time, then stops it and returns its result.
func runFor(t *testing.T, c *Controller, rate int, d time.Duration, ping func(context.Context) error, f *fakeAdvance) error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	f.start = time.Now()
	done := make(chan error, 1)
	go func() { done <- c.lead(ctx, 7, rate, ping, f) }()
	time.Sleep(d)
	synctest.Wait()
	cancel()
	return <-done
}

func TestLeadTicksAtTheRateWithNoStartingBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, m := syncController(250 * time.Millisecond)
		f := &fakeAdvance{}
		if err := runFor(t, c, 80, 10*time.Second, alive, f); err != nil {
			t.Fatalf("lead returned %v after cancel, want nil", err)
		}
		if len(f.calls) != 40 {
			t.Fatalf("%d ticks in 10s at 250ms, want 40", len(f.calls))
		}
		total := 0
		for i, call := range f.calls {
			if want := time.Duration(i+1) * 250 * time.Millisecond; call.at != want {
				t.Fatalf("tick %d at %s, want %s", i+1, call.at, want)
			}
			if call.n != 20 { // 80 per second, a quarter of a second at a time
				t.Fatalf("tick %d asked for %d, want 20", i+1, call.n)
			}
			total += call.n
		}
		if total != 800 {
			t.Fatalf("admitted %d in 10s at 80/s, want 800", total)
		}
		if got := ticks(t, m, tickAdvanced); got != 40 {
			t.Fatalf("advanced ticks = %v, want 40", got)
		}
	})
}

func TestLeadCarriesUnusedAllowanceButNeverMoreThanOneSecond(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := syncController(250 * time.Millisecond)
		// The session budget is full for 3 s, then frees 1,000 slots.
		start := time.Now()
		f := &fakeAdvance{budget: func() int {
			if time.Since(start) < 3*time.Second {
				return 0
			}
			return 1000
		}}
		if err := runFor(t, c, 80, 5*time.Second, alive, f); err != nil {
			t.Fatal(err)
		}
		for _, call := range f.calls {
			if call.n > 80 {
				t.Fatalf("asked for %d at %s: more than one second's worth (80)", call.n, call.at)
			}
		}
		// While blocked the allowance fills to 80 and stays there; the first
		// tick with room spends it all, then ticks return to 20.
		first := -1
		for i, call := range f.calls {
			if call.at >= 3*time.Second {
				first = i
				break
			}
		}
		if first < 0 || f.calls[first].n != 80 || f.calls[first+1].n != 20 {
			t.Fatalf("after the budget freed: %v, want 80 then 20", f.calls[first:first+2])
		}
	})
}

func TestLeadStepsDownWhenFenced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, m := syncController(250 * time.Millisecond)
		f := &fakeAdvance{err: func(call int) error {
			if call == 3 {
				return ErrFenced
			}
			return nil
		}}
		f.start = time.Now()
		err := c.lead(t.Context(), 7, 80, alive, f)
		if !errors.Is(err, ErrFenced) {
			t.Fatalf("lead returned %v, want ErrFenced", err)
		}
		if len(f.calls) != 3 || time.Since(f.start) != 750*time.Millisecond {
			t.Fatalf("stepped down after %d ticks and %s, want 3 and 750ms", len(f.calls), time.Since(f.start))
		}
		if got := ticks(t, m, tickFenced); got != 1 {
			t.Fatalf("fenced ticks = %v, want 1", got)
		}
	})
}

func TestLeadSurvivesAFailedTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, m := syncController(250 * time.Millisecond)
		f := &fakeAdvance{err: func(call int) error {
			if call == 2 {
				return errors.New("valkey hiccup")
			}
			return nil
		}}
		if err := runFor(t, c, 80, 2*time.Second, alive, f); err != nil {
			t.Fatal(err)
		}
		if len(f.calls) != 8 {
			t.Fatalf("%d ticks in 2s, want 8 (a failed tick must not stop the loop)", len(f.calls))
		}
		// Nobody was admitted on the failed tick, so its allowance carries over.
		if f.calls[2].n != 40 {
			t.Fatalf("tick after the failure asked for %d, want 40", f.calls[2].n)
		}
		if got := ticks(t, m, tickError); got != 1 {
			t.Fatalf("error ticks = %v, want 1", got)
		}
	})
}

func TestLeadStepsDownWhenTheLockConnectionDies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := syncController(250 * time.Millisecond)
		dead := errors.New("connection reset")
		pings := 0
		ping := func(context.Context) error {
			pings++
			if pings == 4 {
				return dead
			}
			return nil
		}
		f := &fakeAdvance{start: time.Now()}
		err := c.lead(t.Context(), 7, 80, ping, f)
		if !errors.Is(err, dead) {
			t.Fatalf("lead returned %v, want the ping error", err)
		}
		if len(f.calls) != 3 {
			t.Fatalf("%d ticks admitted, want 3: no tick may admit after the lock is lost", len(f.calls))
		}
	})
}

// ticks reads holdfast_queue_admission_ticks_total{result}.
func ticks(t *testing.T, m *Metrics, result string) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.ticks.WithLabelValues(result).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

func TestLeadStopsWhenTheEventIsGone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := syncController(250 * time.Millisecond)
		f := &fakeAdvance{start: time.Now(), err: func(call int) error {
			if call == 2 {
				return ErrEventNotFound
			}
			return nil
		}}
		err := c.lead(t.Context(), 7, 80, alive, f)
		if !errors.Is(err, ErrEventNotFound) || len(f.calls) != 2 {
			t.Fatalf("lead returned %v after %d ticks, want ErrEventNotFound after 2", err, len(f.calls))
		}
	})
}

// fakeInventory answers GetAvailability from avail(call), counting calls.
type fakeInventory struct {
	calls int
	avail func(call int) (inventory.Availability, error)
}

func (f *fakeInventory) GetAvailability(_ context.Context, eventID string) (inventory.Availability, error) {
	f.calls++
	if eventID != syncEvent {
		return inventory.Availability{}, errors.New("wrong event")
	}
	return f.avail(f.calls)
}

func TestLeadCapsSessionsByUnitsLeft(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, m := syncController(250 * time.Millisecond)
		inv := &fakeInventory{avail: func(call int) (inventory.Availability, error) {
			switch {
			case call == 2:
				return inventory.Availability{}, errors.New("inventory unreachable")
			case call == 3:
				return inventory.Availability{}, fmt.Errorf("inventory: %w", inventory.ErrEventNotProvisioned)
			case call <= 4:
				return inventory.Availability{Available: 100, ActiveHolds: 7}, nil
			default: // the last unit went into a hold
				return inventory.Availability{Available: 0, ActiveHolds: 3}, nil
			}
		}}
		c.cfg.Inventory, c.cfg.Oversubscription = inv, 1.3
		f := &fakeAdvance{}
		if err := runFor(t, c, 80, 2*time.Second, alive, f); err != nil {
			t.Fatal(err)
		}
		want := []int{130, -1, -1, 130, 0, 0, 0, 0}
		if len(f.calls) != len(want) {
			t.Fatalf("%d ticks, want %d", len(f.calls), len(want))
		}
		for i, call := range f.calls {
			if call.unitsCap != want[i] {
				t.Fatalf("tick %d: units cap %d, want %d (100 units x 1.3; no cap when inventory fails or has no such sale; 0 with no units)", i+1, call.unitsCap, want[i])
			}
		}
		if f.soldOut != 0 {
			t.Fatal("open holds may still return units: not sold out yet")
		}
		for result, want := range map[string]float64{inventoryReadOK: 6, inventoryReadError: 1, inventoryReadNotProvisioned: 1} {
			var out dto.Metric
			_ = m.invReads.WithLabelValues(result).Write(&out)
			if out.GetCounter().GetValue() != want {
				t.Fatalf("inventory reads %s = %v, want %v", result, out.GetCounter().GetValue(), want)
			}
		}
	})
}

func TestLeadMarksSoldOutWhenNoUnitsAndNoHolds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := syncController(250 * time.Millisecond)
		c.cfg.Inventory = &fakeInventory{avail: func(int) (inventory.Availability, error) {
			return inventory.Availability{Available: 0, ActiveHolds: 0}, nil
		}}
		c.cfg.Oversubscription = 1.3
		f := &fakeAdvance{}
		if err := runFor(t, c, 80, time.Second, alive, f); err != nil {
			t.Fatal(err)
		}
		if f.soldOut != 4 || len(f.calls) != 4 {
			t.Fatalf("%d sold-out marks and %d ticks, want 4 and 4: the status document is still written", f.soldOut, len(f.calls))
		}
	})
}

func TestLeadStepsDownWhenFencedMarkingSoldOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := syncController(250 * time.Millisecond)
		c.cfg.Inventory = &fakeInventory{avail: func(int) (inventory.Availability, error) {
			return inventory.Availability{}, nil
		}}
		f := &fakeAdvance{markErr: ErrFenced}
		if err := c.lead(t.Context(), 7, 80, alive, f); !errors.Is(err, ErrFenced) || len(f.calls) != 0 {
			t.Fatalf("lead returned %v after %d advances, want ErrFenced before any", err, len(f.calls))
		}
	})
}
