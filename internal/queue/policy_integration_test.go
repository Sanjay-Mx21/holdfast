//go:build integration

package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/policy"
)

// TestPolicyWindowsAtJoin: the windows stored at provisioning decide who may
// join; provisioning again with the same settings replaces them.
func TestPolicyWindowsAtJoin(t *testing.T) {
	f := newFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	cfg := validConfig()
	cfg.Policy = policy.Rules{VerifiedOnlyUntil: now.Add(time.Hour), AgentLockoutUntil: now.Add(30 * time.Minute)}
	_, err := f.svc.Provision(ctx, f.eventID, cfg)
	mustErr(t, "provision", err, nil)

	rules, err := f.store.Policy(ctx, f.eventID)
	mustErr(t, "read policy", err, nil)
	if !rules.VerifiedOnlyUntil.Equal(cfg.Policy.VerifiedOnlyUntil) || !rules.AgentLockoutUntil.Equal(cfg.Policy.AgentLockoutUntil) {
		t.Fatalf("stored %+v, want %+v", rules, cfg.Policy)
	}
	o, err := f.svc.Overview(ctx, f.eventID)
	mustErr(t, "overview", err, nil)
	mustEqual(t, "overview's verified window", o.Config.Policy.VerifiedOnlyUntil.Equal(cfg.Policy.VerifiedOnlyUntil), true)

	refused := func(what string, buyer policy.Buyer, reason string) {
		t.Helper()
		_, err := f.svc.Join(ctx, f.eventID, uuid.NewString(), buyer)
		var r *policy.Refused
		if !errors.As(err, &r) || r.Reason != reason {
			t.Fatalf("%s: %v, want refused with %s", what, err, reason)
		}
	}
	refused("an unverified buyer", policy.Buyer{Role: "BUYER"}, policy.ReasonVerifiedOnly)
	refused("the development header", anyone, policy.ReasonVerifiedOnly)
	refused("a verified agent", policy.Buyer{Role: "AGENT", Verified: true}, policy.ReasonAgentLockout)
	res, err := f.svc.Join(ctx, f.eventID, uuid.NewString(), policy.Buyer{Role: "BUYER", Verified: true})
	mustErr(t, "a verified buyer", err, nil)
	mustEqual(t, "joined", res.Joined, true)

	// Past the lockout, a verified agent may join.
	f.svc.now = func() time.Time { return now.Add(31 * time.Minute) }
	_, err = f.svc.Join(ctx, f.eventID, uuid.NewString(), policy.Buyer{Role: "AGENT", Verified: true})
	mustErr(t, "an agent after the lockout", err, nil)
	f.svc.now = time.Now

	// Re-provisioning with the same settings and no windows lifts them.
	cfg.Policy = policy.Rules{}
	created, err := f.svc.Provision(ctx, f.eventID, cfg)
	mustErr(t, "provision again", err, nil)
	mustEqual(t, "created again", created, false)
	_, err = f.svc.Join(ctx, f.eventID, uuid.NewString(), anyone)
	mustErr(t, "anyone, without windows", err, nil)

	// Different settings are refused, and leave the windows alone.
	cfg.Policy = policy.Rules{VerifiedOnlyUntil: now.Add(time.Hour)}
	cfg.MaxSessions++
	_, err = f.svc.Provision(ctx, f.eventID, cfg)
	mustErr(t, "provision with other settings", err, ErrProvisionConflict)
	rules, _ = f.store.Policy(ctx, f.eventID)
	mustEqual(t, "windows after a refused provisioning", rules == policy.Rules{}, true)
}

func TestFreezeSwitch(t *testing.T) {
	pre := newFixture(t)
	cfg := validConfig()
	cfg.OpensAt = time.Now().Add(time.Hour)
	_, err := pre.svc.Provision(ctx, pre.eventID, cfg)
	mustErr(t, "provision", err, nil)
	_, err = pre.svc.Freeze(ctx, pre.eventID)
	mustErr(t, "freeze before T0", err, ErrStateConflict)

	f := newFixture(t)
	f.openQueueWith(t, 10, 100)
	epoch, err := f.store.NewTerm(ctx, f.eventID)
	mustErr(t, "new term", err, nil)
	changed, err := f.svc.Freeze(ctx, f.eventID)
	mustErr(t, "freeze", err, nil)
	mustEqual(t, "froze", changed, true)
	changed, err = f.svc.Freeze(ctx, f.eventID)
	mustErr(t, "freeze again", err, nil)
	mustEqual(t, "froze twice", changed, false)
	mustEqual(t, "state", f.state(t), string(StateFrozen))

	adv, err := f.store.Advance(ctx, f.eventID, epoch, 5)
	mustErr(t, "advance while frozen", err, nil)
	mustEqual(t, "admitted while frozen", adv.Admitted, int64(0))
	res, err := f.svc.Join(ctx, f.eventID, uuid.NewString(), anyone)
	mustErr(t, "join while frozen", err, nil)
	mustEqual(t, "ordering while frozen", res.Ordering, OrderingFIFO)
	st, _ := f.svc.Status(ctx, f.eventID)
	mustEqual(t, "status while frozen", st.State, StateFrozen)

	changed, err = f.svc.Unfreeze(ctx, f.eventID)
	mustErr(t, "unfreeze", err, nil)
	mustEqual(t, "unfroze", changed, true)
	adv, err = f.store.Advance(ctx, f.eventID, epoch, 5)
	mustErr(t, "advance after unfreezing", err, nil)
	mustEqual(t, "admitted after unfreezing", adv.Admitted, int64(5))

	_, err = f.svc.Freeze(ctx, uuid.NewString())
	mustErr(t, "freeze an unknown event", err, ErrEventNotFound)
	_, err = f.svc.Unfreeze(ctx, "not-a-uuid")
	mustErr(t, "unfreeze a bad ID", err, ErrInvalidRequest)
}

// TestAdvanceWithinUnitsCap: open sessions never exceed the units cap, on
// top of the session budget (P17).
func TestAdvanceWithinUnitsCap(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 20, 100)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	steps := []struct {
		n, unitsCap int
		admitted    int64
	}{
		{10, 3, 3},   // 3 sessions for the units left
		{10, 3, 0},   // all 3 in use
		{10, 5, 2},   // more units (holds expired): 2 more
		{10, 0, 0},   // none left
		{10, -1, 10}, // no cap: the rate decides
	}
	for i, s := range steps {
		adv, err := f.store.AdvanceWithin(ctx, f.eventID, epoch, s.n, s.unitsCap)
		mustErr(t, "advance", err, nil)
		if adv.Admitted != s.admitted {
			t.Fatalf("step %d (cap %d): admitted %d, want %d", i+1, s.unitsCap, adv.Admitted, s.admitted)
		}
	}
}

func TestMarkSoldOut(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 10, 100)
	old, _ := f.store.NewTerm(ctx, f.eventID)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	_, err := f.store.MarkSoldOut(ctx, f.eventID, old)
	mustErr(t, "a stale leader", err, ErrFenced)
	mustEqual(t, "state after a stale mark", f.state(t), string(StateOpen))

	marked, err := f.store.MarkSoldOut(ctx, f.eventID, epoch)
	mustErr(t, "mark", err, nil)
	mustEqual(t, "marked", marked, true)
	marked, err = f.store.MarkSoldOut(ctx, f.eventID, epoch)
	mustErr(t, "mark again", err, nil)
	mustEqual(t, "marked twice", marked, false)

	_, err = f.svc.Join(ctx, f.eventID, uuid.NewString(), anyone)
	mustErr(t, "join a sold-out queue", err, ErrQueueClosed)
	_, err = f.store.Advance(ctx, f.eventID, epoch, 5)
	mustErr(t, "advance", err, nil)
	st, _ := f.svc.Status(ctx, f.eventID)
	mustEqual(t, "status", st.State, StateSoldOut)
	_, err = f.svc.Unfreeze(ctx, f.eventID)
	mustErr(t, "unfreeze a sold-out queue", err, ErrStateConflict)

	// A frozen queue stays frozen: freezing is the operator's call.
	g := newFixture(t)
	g.openQueueWith(t, 1, 100)
	e, _ := g.store.NewTerm(ctx, g.eventID)
	_, _ = g.svc.Freeze(ctx, g.eventID)
	marked, err = g.store.MarkSoldOut(ctx, g.eventID, e)
	mustErr(t, "mark a frozen queue", err, nil)
	mustEqual(t, "marked a frozen queue", marked, false)
	mustEqual(t, "frozen queue's state", g.state(t), string(StateFrozen))
}
