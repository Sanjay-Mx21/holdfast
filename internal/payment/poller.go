package payment

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/paymentdb"
)

// Poller asks the provider about intents still CREATED a while after their
// order was opened, in case a webhook was lost, and applies what it learns
// through the same moves as the webhooks. It takes the least recently
// polled intents first, so ones whose polls keep failing cannot starve the
// rest. It runs in every replica; FOR
// UPDATE SKIP LOCKED gives concurrent pollers disjoint batches.
type Poller struct {
	svc      *Service
	interval time.Duration
	after    time.Duration
	batch    int32
	log      *slog.Logger
}

// NewPoller polls every interval, intents older than after, batch at a time.
func NewPoller(svc *Service, interval, after time.Duration, batch int, log *slog.Logger) *Poller {
	return &Poller{svc: svc, interval: interval, after: after, batch: int32(batch), log: log} //nolint:gosec // validated in config
}

// Name implements app.Component.
func (p *Poller) Name() string { return "payment-status-poller" }

// Run implements app.Component.
func (p *Poller) Run(ctx context.Context) error {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := p.Pass(ctx); err != nil && ctx.Err() == nil {
			p.svc.m.polls.WithLabelValues("error").Inc()
			p.log.Warn("payment poller: pass failed", "err", err)
		}
	}
}

// Pass polls one batch. Each intent's provider call has its own short
// deadline, so one slow answer cannot hold the batch's locks for long.
func (p *Poller) Pass(ctx context.Context) error {
	ctx, span := otel.Tracer("github.com/Sanjay-Mx21/holdfast/internal/payment").Start(ctx, "payment.poll_open_intents")
	defer span.End()
	return pgx.BeginFunc(ctx, p.svc.pool, func(tx pgx.Tx) error {
		q := p.svc.q.WithTx(tx)
		open, err := q.ClaimOpenIntents(ctx, paymentdb.ClaimOpenIntentsParams{CreatedBefore: time.Now().Add(-p.after), Batch: p.batch})
		if err != nil {
			return fmt.Errorf("payment: claim open intents: %w", err)
		}
		for _, in := range open {
			cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			order, err := p.svc.psp.GetOrder(cctx, in.PspOrderID.String)
			cancel()
			if err := q.MarkPolled(ctx, in.ID); err != nil {
				return fmt.Errorf("payment: mark polled: %w", err)
			}
			if err != nil {
				p.svc.m.polls.WithLabelValues("provider_error").Inc()
				continue // try again next pass
			}
			if err := p.svc.applyOrder(ctx, q, in, order); err != nil {
				return err
			}
			p.svc.m.polls.WithLabelValues(order.Status).Inc()
		}
		return nil
	})
}
