package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Opener performs the T0 transition: every interval it flips each
// provisioned queue from PRE to OPEN once Valkey's clock reaches the event's
// opening time. Every replica may run one: open.lua is conditional and
// idempotent, so racing openers are harmless and there is no leader to get
// wrong. A join that arrives first opens the queue itself (join.lua), so the
// opener only matters when nobody joins at T0; its interval bounds how stale
// the state can look to readers, never who gets a lottery position.
type Opener struct {
	store    *Store
	interval time.Duration
	m        *Metrics
	log      *slog.Logger
}

// NewOpener returns an opener that checks every interval.
func NewOpener(store *Store, interval time.Duration, m *Metrics, log *slog.Logger) *Opener {
	return &Opener{store: store, interval: interval, m: m, log: log}
}

// Name implements app.Component.
func (o *Opener) Name() string { return "queue-opener" }

// Run implements app.Component.
func (o *Opener) Run(ctx context.Context) error {
	t := time.NewTicker(o.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		start := time.Now()
		_, err := o.OpenDue(ctx)
		o.m.openerTime.Observe(time.Since(start).Seconds())
		switch {
		case err != nil && ctx.Err() == nil:
			o.m.openerRuns.WithLabelValues("error").Inc()
			o.log.Warn("opener pass failed", "err", err)
		case err == nil:
			o.m.openerRuns.WithLabelValues("ok").Inc()
		}
	}
}

// OpenDue runs the T0 transition for every provisioned event whose opening
// time has come and returns how many queues this call opened.
func (o *Opener) OpenDue(ctx context.Context) (int, error) {
	events, err := o.store.Events(ctx)
	if err != nil {
		return 0, err
	}
	opened := 0
	var errs []error
	for _, ev := range events {
		did, late, err := o.store.Open(ctx, ev)
		switch {
		case errors.Is(err, ErrEventNotFound):
			continue // listed but without settings: nothing to open
		case err != nil:
			errs = append(errs, fmt.Errorf("event %s: %w", ev, err))
			if ctx.Err() != nil {
				return opened, errors.Join(errs...)
			}
			continue
		}
		if did {
			opened++
			o.m.transition(openedByOpener)
			o.log.Info("queue opened at T0", "event_id", ev, "late_ms", late.Milliseconds())
		}
		o.observeStatusAge(ctx, ev)
	}
	return opened, errors.Join(errs...)
}

// observeStatusAge exports how old the status document clients are served
// is. It grows while no leader writes it, which is when it matters.
func (o *Opener) observeStatusAge(ctx context.Context, eventID string) {
	age, ok, err := o.store.StatusAge(ctx, eventID)
	switch {
	case err != nil:
		o.log.Warn("status age: read failed", "event_id", eventID, "err", err)
	case !ok:
		o.m.statusAge.DeleteLabelValues(eventID) // no leader has written one yet
	default:
		o.m.statusAge.WithLabelValues(eventID).Set(age.Seconds())
	}
}
