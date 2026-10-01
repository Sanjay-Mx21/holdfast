package inventory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Sweeper releases expired holds. Every replica may run one: release.lua
// re-checks state and expiry atomically against the server clock, so racing
// sweepers are harmless and there is no leader election to get wrong.
type Sweeper struct {
	store    *Store
	interval time.Duration
	batch    int64
	m        *Metrics
	log      *slog.Logger
	now      func() time.Time
}

// NewSweeper returns a sweeper that runs every interval, releasing up to
// batch holds per event per query.
func NewSweeper(store *Store, interval time.Duration, batch int, m *Metrics, log *slog.Logger) *Sweeper {
	return &Sweeper{store: store, interval: interval, batch: int64(batch), m: m, log: log, now: time.Now}
}

// Name implements app.Component.
func (s *Sweeper) Name() string { return "inventory-sweeper" }

// Run implements app.Component.
func (s *Sweeper) Run(ctx context.Context) error {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		start := time.Now()
		n, err := s.SweepOnce(ctx)
		s.m.sweepDuration.Observe(time.Since(start).Seconds())
		switch {
		case err != nil && ctx.Err() == nil:
			s.m.sweeps.WithLabelValues("error").Inc()
			s.log.Warn("sweep failed", "err", err)
		case err == nil:
			s.m.sweeps.WithLabelValues("ok").Inc()
			if n > 0 {
				s.log.Debug("expired holds released", "count", n)
			}
		}
	}
}

// SweepOnce releases every expired hold across all events and returns how
// many holds it released.
func (s *Sweeper) SweepOnce(ctx context.Context) (int, error) {
	events, err := s.store.Events(ctx)
	if err != nil {
		return 0, fmt.Errorf("list events: %w", err)
	}
	total := 0
	var errs []error
	for _, ev := range events {
		n, err := s.sweepEvent(ctx, ev)
		total += n
		if err != nil {
			errs = append(errs, fmt.Errorf("event %s: %w", ev, err))
			if ctx.Err() != nil {
				break
			}
		}
	}
	return total, errors.Join(errs...)
}

func (s *Sweeper) sweepEvent(ctx context.Context, eventID string) (int, error) {
	released := 0
	for {
		members, err := s.store.ExpiredMembers(ctx, eventID, s.now(), s.batch)
		if err != nil {
			return released, err
		}
		progress := 0
		for _, member := range members {
			holdID, userID, ok := parseExpiryMember(member)
			if !ok {
				if err := s.store.RemoveExpiryMember(ctx, eventID, member); err != nil {
					return released, err
				}
				progress++
				continue
			}
			freed, err := s.store.Release(ctx, eventID, userID, holdID, ReleaseExpire)
			switch {
			case errors.Is(err, errNotExpiredYet):
				continue // our clock runs ahead of the server's; retry next tick
			case errors.Is(err, ErrHoldNotFound):
				if err := s.store.RemoveExpiryMember(ctx, eventID, member); err != nil {
					return released, err
				}
			case err != nil:
				return released, err
			}
			progress++
			if freed {
				released++
				s.m.releases.WithLabelValues(string(ReleaseExpire)).Inc()
			}
		}
		// Stop when the index is drained or nothing moved (avoids spinning on
		// members that are "expired" by our clock but not by the server's).
		if int64(len(members)) < s.batch || progress == 0 {
			return released, nil
		}
	}
}
