//go:build integration

package paymentdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func newIntent(t *testing.T, q *Queries, amount int64) Intent {
	t.Helper()
	in, err := q.CreateIntent(ctx, CreateIntentParams{ID: uuid.Must(uuid.NewV7()), BookingID: uuid.New(), EventID: uuid.NullUUID{UUID: uuid.New(), Valid: true}, AmountPaise: amount, ExpiresAt: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func TestOneIntentPerBooking(t *testing.T) {
	q := New(testenv.Postgres(t))
	first := newIntent(t, q, 5000)
	if first.Status != "CREATED" || first.Currency != "INR" {
		t.Fatalf("new intent %+v", first)
	}
	_, err := q.CreateIntent(ctx, CreateIntentParams{ID: uuid.Must(uuid.NewV7()), BookingID: first.BookingID, AmountPaise: 5000, ExpiresAt: time.Now()})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("a second intent for the booking: %v, want no rows", err)
	}
	if got, err := q.GetIntentByBooking(ctx, first.BookingID); err != nil || got.ID != first.ID {
		t.Fatalf("by booking: %v", err)
	}
	// The PSP order is recorded once.
	o1, o2 := "order_"+uuid.NewString(), "order_"+uuid.NewString()
	if _, err := q.AttachOrder(ctx, AttachOrderParams{ID: first.ID, PspOrderID: pgText(o1), CheckoutUrl: pgText("https://psp/pay/1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.AttachOrder(ctx, AttachOrderParams{ID: first.ID, PspOrderID: pgText(o1), CheckoutUrl: pgText("https://psp/pay/1")}); err != nil {
		t.Fatalf("the same order again: %v", err)
	}
	if _, err := q.AttachOrder(ctx, AttachOrderParams{ID: first.ID, PspOrderID: pgText(o2), CheckoutUrl: pgText("https://psp/pay/2")}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("a different order: %v, want no rows", err)
	}
}

func TestIntentStateMachine(t *testing.T) {
	q := New(testenv.Postgres(t))
	move := func(id uuid.UUID, from, to string) error {
		_, err := q.TransitionIntent(ctx, TransitionIntentParams{ID: id, FromStatus: from, ToStatus: to})
		return err
	}
	capture := func(id uuid.UUID, payment string) (Intent, error) {
		return q.CaptureIntent(ctx, CaptureIntentParams{ID: id, PspPaymentID: pgText(payment)})
	}

	// A capture always wins: after a failure, and after an expiry.
	for _, from := range []string{"FAILED", "EXPIRED"} {
		in := newIntent(t, q, 1000)
		if err := move(in.ID, "CREATED", from); err != nil {
			t.Fatal(err)
		}
		got, err := capture(in.ID, "pay_"+uuid.NewString()[:8])
		if err != nil || got.Status != "CAPTURED" || got.Version != 2 {
			t.Fatalf("%s -> CAPTURED: %+v %v", from, got, err)
		}
		if _, err := capture(in.ID, "pay_again"); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("capturing twice: %v, want no rows", err)
		}
	}

	// Refunds, then nothing more.
	in := newIntent(t, q, 1000)
	if _, err := capture(in.ID, "pay_"+uuid.NewString()[:8]); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"FAILED", "EXPIRED", "CREATED", "REFUNDED"} {
		if err := move(in.ID, "CAPTURED", bad); pgCode(err) != "23514" {
			t.Fatalf("CAPTURED -> %s: %v, want a check violation", bad, err)
		}
	}
	if err := move(in.ID, "CAPTURED", "REFUND_PENDING"); err != nil {
		t.Fatal(err)
	}
	if err := move(in.ID, "REFUND_PENDING", "REFUNDED"); err != nil {
		t.Fatal(err)
	}
	if _, err := capture(in.ID, "pay_x"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("capturing a refunded intent: %v, want no rows", err)
	}

	// A captured intent must name the PSP payment.
	raw := newIntent(t, q, 1000)
	pool := testenv.Postgres(t)
	if _, err := pool.Exec(ctx, `UPDATE payment.payment_intents SET status = 'CAPTURED' WHERE id = $1`, raw.ID); pgCode(err) != "23514" {
		t.Fatalf("CAPTURED without a payment ID: %v, want a check violation", err)
	}
}

func TestWebhooksAreRecordedOnce(t *testing.T) {
	q := New(testenv.Postgres(t))
	id := "evt_" + uuid.NewString()
	p := RecordWebhookParams{PspEventID: id, EventType: "payment.captured", PspOrderID: pgText("order_x"), Payload: []byte(`{"a":1}`)}
	if n, err := q.RecordWebhook(ctx, p); err != nil || n != 1 {
		t.Fatalf("first delivery: %d %v", n, err)
	}
	if n, err := q.RecordWebhook(ctx, p); err != nil || n != 0 {
		t.Fatalf("duplicate delivery: %d %v, want 0", n, err)
	}
}

func TestLedgerTransactionsMustBalance(t *testing.T) {
	pool := testenv.Postgres(t)
	q := New(pool)
	in := newIntent(t, q, 2500)
	revenue := "unearned_revenue:" + uuid.NewString()
	entry := func(q *Queries, txn uuid.UUID, account, dir string, amount int64) error {
		return q.InsertLedgerEntry(ctx, InsertLedgerEntryParams{TxnID: txn, Account: account, Direction: dir, AmountPaise: amount, IntentID: in.ID})
	}
	inTx := func(fn func(q *Queries) error) error {
		return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return fn(q.WithTx(tx)) })
	}

	// A capture: debit the receivable, credit unearned revenue.
	if err := inTx(func(q *Queries) error {
		txn := uuid.New()
		if err := entry(q, txn, revenue, "C", 2500); err != nil { // credit first: checked only at commit
			return err
		}
		return entry(q, txn, "psp_receivable", "D", 2500)
	}); err != nil {
		t.Fatalf("a balanced transaction: %v", err)
	}
	// Unbalanced: refused at commit, nothing kept.
	err := inTx(func(q *Queries) error { return entry(q, uuid.New(), revenue, "C", 100) })
	if pgCode(err) != "23514" {
		t.Fatalf("an unbalanced transaction: %v, want a check violation", err)
	}
	err = inTx(func(q *Queries) error {
		txn := uuid.New()
		if err := entry(q, txn, revenue, "C", 100); err != nil {
			return err
		}
		return entry(q, txn, "psp_receivable", "D", 99)
	})
	if pgCode(err) != "23514" {
		t.Fatalf("debits 99, credits 100: %v, want a check violation", err)
	}
	if bal, err := q.AccountBalance(ctx, revenue); err != nil || bal != -2500 {
		t.Fatalf("revenue balance %d %v, want -2500 (credit)", bal, err)
	}
	// Unknown accounts are refused outright.
	if err := entry(q, uuid.New(), "my_pocket", "D", 1); pgCode(err) != "23514" {
		t.Fatalf("an unknown account: %v, want a check violation", err)
	}
}

func TestStatusPollingTakesDisjointBatches(t *testing.T) {
	pool := testenv.Postgres(t)
	testenv.Exclusive(t, pool, "payment-poll")
	q := New(pool)
	var ours []uuid.UUID
	for range 3 {
		in := newIntent(t, q, 1000)
		if _, err := q.AttachOrder(ctx, AttachOrderParams{ID: in.ID, PspOrderID: pgText("order_" + uuid.NewString()), CheckoutUrl: pgText("u")}); err != nil {
			t.Fatal(err)
		}
		ours = append(ours, in.ID)
	}
	before := time.Now().Add(time.Second)
	t.Cleanup(func() {
		for _, id := range ours {
			_, _ = q.TransitionIntent(ctx, TransitionIntentParams{ID: id, FromStatus: "CREATED", ToStatus: "EXPIRED"})
		}
	})
	txA, _ := pool.Begin(ctx)
	defer txA.Rollback(ctx)
	txB, _ := pool.Begin(ctx)
	defer txB.Rollback(ctx)
	a, err := q.WithTx(txA).ClaimOpenIntents(ctx, ClaimOpenIntentsParams{CreatedBefore: before, Batch: 2})
	if err != nil || len(a) != 2 {
		t.Fatalf("first poller: %d %v", len(a), err)
	}
	b, err := q.WithTx(txB).ClaimOpenIntents(ctx, ClaimOpenIntentsParams{CreatedBefore: before, Batch: 100_000})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[uuid.UUID]int{}
	for _, in := range append(a, b...) {
		seen[in.ID]++
	}
	for _, id := range ours {
		if seen[id] != 1 {
			t.Fatalf("intent %s claimed %d times, want once", id, seen[id])
		}
	}
}

func pgText(s string) pgtype.Text { return pgtype.Text{String: s, Valid: true} }
