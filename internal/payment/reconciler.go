package payment

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/paymentdb"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

// Settlements is what the reconciler needs from the provider (*psp.Client).
type Settlements interface {
	Settlements(ctx context.Context, from, to time.Time) (psp.Settlement, error)
}

// Kinds of reconciliation finding, the values of the type label of
// holdfast_recon_mismatch_total (design doc 9.9).
const (
	// Repaired by the reconciler:
	FindingMissedCapture = "missed_capture" // the provider captured; HoldFast still says CREATED, FAILED or EXPIRED
	FindingMissedRefund  = "missed_refund"  // the provider completed a refund; HoldFast still says REFUND_PENDING
	FindingRefundStuck   = "refund_stuck"   // REFUND_PENDING for too long: the refund is asked for again
	// For a human (an alert pages):
	FindingAmountMismatch = "amount_mismatch"            // the provider moved a different amount
	FindingCaptureUnknown = "capture_unknown_to_psp"     // HoldFast booked a capture the provider does not report
	FindingRefundUnknown  = "refund_unknown_to_holdfast" // the provider refunded what HoldFast never asked to refund
	FindingUnknownOrder   = "unknown_order"              // the provider reports money for an intent HoldFast does not have
)

var findingKinds = []string{
	FindingMissedCapture, FindingMissedRefund, FindingRefundStuck,
	FindingAmountMismatch, FindingCaptureUnknown, FindingRefundUnknown, FindingUnknownOrder,
}

// Finding is one difference between the provider's report and HoldFast's
// records.
type Finding struct {
	Kind     string
	IntentID uuid.UUID // uuid.Nil when the provider's reference is not one
	Detail   string
}

// ReconcilerConfig times the reconciler.
type ReconcilerConfig struct {
	// Interval between passes (design: 5 minutes).
	Interval time.Duration
	// Window is how far back each pass looks (design: 2 hours).
	Window time.Duration
	// Margin keeps HoldFast's own captures away from the window's edges
	// when looking for captures the provider does not report: a webhook can
	// arrive minutes after the capture, so a capture booked just inside the
	// window may have happened at the provider just before it, and one
	// booked a moment ago may not be in the report yet.
	Margin time.Duration
	// RefundStuckAfter is how long a refund may stay REFUND_PENDING before
	// it is asked for again.
	RefundStuckAfter time.Duration
}

// Reconciler compares the provider's settlement report with payment-svc's
// records and repairs what lost webhooks left behind, through the same
// idempotent paths the webhooks use: a capture the provider made is
// applied (the saga then confirms or refunds), a refund it completed is
// completed. What it cannot repair (a capture the provider does not know,
// a different amount) it counts and logs for a human. One replica
// reconciles at a time, under a PostgreSQL advisory lock.
type Reconciler struct {
	svc *Service
	psp Settlements
	cfg ReconcilerConfig
	log *slog.Logger
	now func() time.Time
}

// NewReconciler returns the reconciler.
func NewReconciler(svc *Service, settlements Settlements, cfg ReconcilerConfig, log *slog.Logger) *Reconciler {
	return &Reconciler{svc: svc, psp: settlements, cfg: cfg, log: log, now: time.Now}
}

// Name implements app.Component.
func (r *Reconciler) Name() string { return "payment-reconciler" }

// Run implements app.Component: a pass at start, then every Interval.
func (r *Reconciler) Run(ctx context.Context) error {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		if _, err := r.Pass(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("payment reconciler: pass failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// reconLockKey is the advisory-lock key that makes one replica reconcile at
// a time.
var reconLockKey = func() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("holdfast/payment/reconciler"))
	return int64(h.Sum64()) //nolint:gosec // the bit pattern is the key
}()

// errLocked means another replica is reconciling.
var errLocked = errors.New("payment: another replica is reconciling")

// Pass reconciles once and returns what it found. Another replica holding
// the lock makes it a no-op (nil findings, no error).
func (r *Reconciler) Pass(ctx context.Context) ([]Finding, error) {
	ctx, span := otel.Tracer("github.com/Sanjay-Mx21/holdfast/internal/payment").Start(ctx, "payment.reconcile")
	defer span.End()
	findings, err := r.pass(ctx)
	switch {
	case errors.Is(err, errLocked):
		r.svc.m.reconRuns.WithLabelValues("skipped").Inc()
		return nil, nil
	case err != nil:
		r.svc.m.reconRuns.WithLabelValues("error").Inc()
		return findings, err
	}
	r.svc.m.reconRuns.WithLabelValues("ok").Inc()
	r.svc.m.reconLastSuccess.SetToCurrentTime()
	for _, f := range findings {
		r.svc.m.reconMismatch.WithLabelValues(f.Kind).Inc()
	}
	return findings, nil
}

func (r *Reconciler) pass(ctx context.Context) ([]Finding, error) {
	conn, err := r.svc.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("payment: reconcile: %w", err)
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", reconLockKey).Scan(&locked); err != nil {
		return nil, fmt.Errorf("payment: reconcile lock: %w", err)
	}
	if !locked {
		return nil, errLocked
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", reconLockKey)
	}()

	now := r.now()
	from := now.Add(-r.cfg.Window)
	report, err := r.psp.Settlements(ctx, from, now)
	if err != nil {
		return nil, fmt.Errorf("payment: settlement report: %w", err)
	}
	var findings []Finding
	add := func(f Finding) {
		findings = append(findings, f)
		attrs := []any{"kind", f.Kind, "intent_id", f.IntentID, "detail", f.Detail}
		switch f.Kind {
		case FindingMissedCapture, FindingMissedRefund, FindingRefundStuck:
			r.log.WarnContext(ctx, "payment reconciler: repaired a difference with the provider", attrs...)
		default:
			r.log.ErrorContext(ctx, "payment reconciler: a difference with the provider needs a human", attrs...)
		}
	}

	reported := make(map[uuid.UUID]bool)
	for _, item := range report.Items {
		id, err := uuid.Parse(item.Reference)
		if err != nil {
			add(Finding{Kind: FindingUnknownOrder, Detail: fmt.Sprintf("%s for order %s with reference %q", item.Type, item.OrderID, item.Reference)})
			continue
		}
		if item.Type == psp.SettledCapture {
			reported[id] = true
		}
		f, err := r.applyItem(ctx, id, item)
		if err != nil {
			return findings, err
		}
		if f != nil {
			add(*f)
		}
	}

	// Captures HoldFast booked that the provider does not report.
	booked, err := r.svc.q.CapturesBookedBetween(ctx, paymentdb.CapturesBookedBetweenParams{
		FromTime: from.Add(r.cfg.Margin), ToTime: now.Add(-r.cfg.Margin),
	})
	if err != nil {
		return findings, fmt.Errorf("payment: captures booked: %w", err)
	}
	for _, c := range booked {
		if !reported[c.ID] {
			add(Finding{Kind: FindingCaptureUnknown, IntentID: c.ID,
				Detail: fmt.Sprintf("payment %s for %d paise is not in the provider's report", c.PspPaymentID.String, c.AmountPaise)})
		}
	}

	// Refunds that never completed: ask again (the provider is idempotent).
	stuck, err := r.svc.q.StuckRefunds(ctx, paymentdb.StuckRefundsParams{Before: now.Add(-r.cfg.RefundStuckAfter), Batch: 50})
	if err != nil {
		return findings, fmt.Errorf("payment: stuck refunds: %w", err)
	}
	for _, in := range stuck {
		add(Finding{Kind: FindingRefundStuck, IntentID: in.ID, Detail: "REFUND_PENDING since " + in.UpdatedAt.UTC().Format(time.RFC3339)})
		if _, err := r.svc.psp.CreateRefund(ctx, in.ID.String(), in.PspPaymentID.String, in.AmountPaise); err != nil {
			r.svc.m.refunds.WithLabelValues("retry").Inc()
			r.log.WarnContext(ctx, "payment reconciler: refund request failed again", "intent_id", in.ID, "err", err)
			continue
		}
		r.svc.m.refunds.WithLabelValues("requested").Inc()
	}
	return findings, nil
}

// applyItem compares one settled item with its intent and repairs what can
// be repaired, in one transaction with the intent locked.
func (r *Reconciler) applyItem(ctx context.Context, id uuid.UUID, item psp.SettlementItem) (*Finding, error) {
	var found *Finding
	err := pgx.BeginFunc(ctx, r.svc.pool, func(tx pgx.Tx) error {
		q := r.svc.q.WithTx(tx)
		in, err := q.GetIntent(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			found = &Finding{Kind: FindingUnknownOrder, IntentID: id, Detail: fmt.Sprintf("%s for order %s", item.Type, item.OrderID)}
			return nil
		}
		if err != nil {
			return err
		}
		if item.AmountPaise != in.AmountPaise {
			found = &Finding{Kind: FindingAmountMismatch, IntentID: id,
				Detail: fmt.Sprintf("%s of %d paise, intent of %d paise", item.Type, item.AmountPaise, in.AmountPaise)}
			return nil
		}
		switch item.Type {
		case psp.SettledCapture:
			if in.Status == "CREATED" || in.Status == "FAILED" || in.Status == "EXPIRED" {
				found = &Finding{Kind: FindingMissedCapture, IntentID: id, Detail: "intent was " + in.Status}
				return r.svc.capture(ctx, q, in, item.PaymentID, item.AmountPaise, "reconciler")
			}
		case psp.SettledRefund:
			switch in.Status {
			case "REFUND_PENDING":
				found = &Finding{Kind: FindingMissedRefund, IntentID: id, Detail: "refund " + item.RefundID}
				return r.svc.refunded(ctx, q, in, item.RefundID)
			case "REFUNDED":
			default:
				found = &Finding{Kind: FindingRefundUnknown, IntentID: id, Detail: "refund " + item.RefundID + " while the intent is " + in.Status}
			}
		}
		return nil
	})
	return found, err
}
