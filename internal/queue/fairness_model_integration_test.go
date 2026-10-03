//go:build integration

package queue

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// F1 model test: random interleavings of joins, rejoins, admission ticks,
// turn claims, expiring sessions, the T0 transition and freezes run against
// the real scripts in Valkey, while a reference model predicts every reply.
// After every step it checks fairness (invariant F1):
//
//   - the queue's order is the model's: lottery scores before T0, arrival
//     order after it, one place per user, never re-rolled;
//   - admissions take ranks strictly in order, each tick exactly the next
//     ranks in line, within the rate, the session budget and the queue;
//   - a claim succeeds only within admittedUpTo, with a live slot;
//   - once anyone has been admitted, nobody's rank changes: nobody can ever
//     be admitted ahead of someone with a lower rank.
//
// A failure prints the seed and the step, so it can be replayed.

const f1Seeds, f1Steps = 12, 400

type f1Model struct {
	score        map[string]float64 // every member's score
	users        []string           // members in join order (to pick from)
	stored       State              // q:{E}:state as stored
	pastT0       bool               // opens_at_ms is in the past
	seq          int64              // post-T0 arrivals so far
	admittedUpTo int64
	live         map[int64]bool // ranks whose session slot is alive
	maxSessions  int64
	claimed      map[string]int64 // user -> rank when their claim first succeeded
	claims       map[string]int   // claim outcomes seen, to show coverage
}

// order is the queue as the model sees it: by score, ties by member, as a
// Valkey sorted set orders them.
func (m *f1Model) order() []string {
	o := slices.Clone(m.users)
	slices.SortFunc(o, func(a, b string) int {
		if c := cmp.Compare(m.score[a], m.score[b]); c != 0 {
			return c
		}
		return cmp.Compare(a, b)
	})
	return o
}

func (m *f1Model) rank(user string) int64 {
	return int64(slices.Index(m.order(), user)) + 1
}

func TestFairnessModel(t *testing.T) {
	for seed := range uint64(f1Seeds) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runF1Model(t, seed) })
	}
}

func runF1Model(t *testing.T, seed uint64) {
	f := newFixture(t)
	rng := rand.New(rand.NewPCG(seed, 0x484f4c44))
	k := keysFor(f.eventID)

	m := &f1Model{
		score: map[string]float64{}, stored: StatePre, live: map[int64]bool{},
		maxSessions: int64(2 + rng.IntN(25)), claimed: map[string]int64{}, claims: map[string]int{},
	}
	// Provision directly (not on the work list, so no running queue-svc
	// interferes), with T0 an hour away; the test moves T0 itself.
	cfg := validConfig()
	code, err := provisionScript.Run(ctx, f.rdb, []string{k.config(), k.state(), k.policy()},
		time.Now().Add(time.Hour).UnixMilli(), cfg.AdmissionRate, m.maxSessions, cfg.SessionTTL.Milliseconds(), 0, 0).Int64()
	if err != nil || code != 1 {
		t.Fatalf("provision: %d %v", code, err)
	}
	t.Cleanup(func() { _ = f.rdb.Del(ctx, k.admitted(), k.epoch(), k.sessions(), k.status()).Err() })
	epoch, err := f.store.NewTerm(ctx, f.eventID)
	if err != nil {
		t.Fatal(err)
	}
	t0Step := f1Steps/5 + rng.IntN(f1Steps*2/5)

	step := 0
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("seed %d, step %d: %s", seed, step, fmt.Sprintf(format, args...))
	}

	join := func(user string) {
		t.Helper()
		lottery, err := lotteryScore()
		if err != nil {
			t.Fatal(err)
		}
		out, err := f.store.Join(ctx, f.eventID, user, lottery)
		if err != nil {
			fail("join: %v", err)
		}
		wantOpened := m.stored == StatePre && m.pastT0
		if out.opened != wantOpened {
			fail("join opened the queue = %v, want %v", out.opened, wantOpened)
		}
		if wantOpened {
			m.stored = StateOpen
		}
		if old, ok := m.score[user]; ok {
			if out.joined || out.score != old {
				fail("rejoin: joined=%v score %v, want the original %v (no re-roll)", out.joined, out.score, old)
			}
			return
		}
		if !out.joined {
			fail("a new user's join was not counted")
		}
		if m.stored == StatePre {
			want, _ := strconv.ParseFloat(lottery, 64)
			if out.score != want || out.score >= 1 {
				fail("lottery join got %v, want its lottery score %v", out.score, want)
			}
		} else {
			m.seq++
			if out.score != float64(1+m.seq) {
				fail("join after T0 got %v, want arrival number %d", out.score, 1+m.seq)
			}
		}
		m.score[user] = out.score
		m.users = append(m.users, user)
	}

	for step = 1; step <= f1Steps; step++ {
		if step == t0Step {
			// T0 arrives: from now on Valkey's clock says the queue is open.
			past := strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10)
			if err := f.rdb.HSet(ctx, k.config(), "opens_at_ms", past).Err(); err != nil {
				t.Fatal(err)
			}
			m.pastT0 = true
			if rng.IntN(2) == 0 { // the opener gets there first; otherwise the next join does
				opened, _, err := f.store.Open(ctx, f.eventID)
				if err != nil || !opened {
					fail("open at T0: %v %v", opened, err)
				}
				m.stored = StateOpen
			}
			continue
		}

		switch p := rng.IntN(100); {
		case p < 35 || len(m.users) == 0: // a new user joins
			join(uuid.NewString())

		case p < 45: // someone joins again
			join(m.users[rng.IntN(len(m.users))])

		case p < 72: // an admission tick
			n := rng.IntN(7)
			adv, err := f.store.Advance(ctx, f.eventID, epoch, n)
			if err != nil {
				fail("advance: %v", err)
			}
			var want int64
			if m.stored == StateOpen {
				want = min(int64(n), m.maxSessions-int64(len(m.live)), int64(len(m.users))-m.admittedUpTo)
			}
			if adv.Admitted != want {
				fail("tick asked for %d in state %s admitted %d, want %d (live %d of %d, %d in line, admittedUpTo %d)",
					n, m.stored, adv.Admitted, want, len(m.live), m.maxSessions, len(m.users), m.admittedUpTo)
			}
			// Exactly the next ranks in line got slots, nobody else.
			for r := m.admittedUpTo + 1; r <= m.admittedUpTo+want; r++ {
				if err := f.rdb.ZScore(ctx, k.sessions(), strconv.FormatInt(r, 10)).Err(); err != nil {
					fail("rank %d admitted without a session slot: %v", r, err)
				}
				m.live[r] = true
			}
			if err := f.rdb.ZScore(ctx, k.sessions(), strconv.FormatInt(m.admittedUpTo+want+1, 10)).Err(); !errors.Is(err, redis.Nil) {
				fail("rank %d got a slot ahead of its turn", m.admittedUpTo+want+1)
			}
			m.admittedUpTo += want
			if adv.AdmittedUpTo != m.admittedUpTo || adv.ActiveSessions != int64(len(m.live)) || adv.QueueSize != int64(len(m.users)) {
				fail("tick reported %+v, want admittedUpTo %d, %d sessions, queue %d", adv, m.admittedUpTo, len(m.live), len(m.users))
			}

		case p < 90: // a member (or, sometimes, a stranger) claims their turn
			user := uuid.NewString()
			switch c := rng.IntN(10); {
			case c < 5 && m.admittedUpTo > 0: // someone already admitted (most claims in a real sale)
				user = m.order()[rng.Int64N(m.admittedUpTo)]
			case c < 9:
				user = m.users[rng.IntN(len(m.users))]
			}
			turn, err := f.svc.Admit(ctx, f.eventID, user)
			r := m.rank(user)
			var nt *NotYourTurnError
			m.claims[claimOutcome(r, m)]++
			switch {
			case r == 0:
				if !errors.Is(err, ErrNotInQueue) {
					fail("a stranger's claim: %v, want ErrNotInQueue", err)
				}
			case r > m.admittedUpTo:
				if !errors.As(err, &nt) || nt.Rank != r || nt.AdmittedUpTo != m.admittedUpTo {
					fail("claim at rank %d with admittedUpTo %d: %v, want not your turn", r, m.admittedUpTo, err)
				}
			case !m.live[r]:
				if !errors.Is(err, ErrTurnExpired) {
					fail("claim at rank %d whose slot expired: %v, want ErrTurnExpired", r, err)
				}
			default:
				if err != nil || turn.Rank != r {
					fail("claim at rank %d within admittedUpTo %d: rank %d, %v", r, m.admittedUpTo, turn.Rank, err)
				}
				if _, ok := m.claimed[user]; !ok {
					m.claimed[user] = r
				}
			}

		case p < 96: // a session ends: its slot's TTL passes (the leader sweeps it on its next tick)
			if len(m.live) == 0 {
				continue
			}
			ranks := make([]int64, 0, len(m.live))
			for r := range m.live {
				ranks = append(ranks, r)
			}
			slices.Sort(ranks)
			r := ranks[rng.IntN(len(ranks))]
			past := float64(time.Now().Add(-time.Second).UnixMilli())
			if err := f.rdb.ZAddXX(ctx, k.sessions(), redis.Z{Score: past, Member: strconv.FormatInt(r, 10)}).Err(); err != nil {
				t.Fatal(err)
			}
			delete(m.live, r)

		default: // an operator freezes or resumes the sale (only after T0)
			if m.stored == StatePre {
				continue
			}
			next := StateFrozen
			if m.stored == StateFrozen {
				next = StateOpen
			}
			f.setState(t, next)
			m.stored = next
		}

		// The real queue is the model's queue.
		got, err := f.rdb.ZRange(ctx, k.members(), 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		if want := m.order(); !slices.Equal(got, want) {
			fail("queue order differs from the model:\n got %v\nwant %v", got, want)
		}
		// F1: once admissions have begun, ranks are fixed.
		if m.admittedUpTo > 0 && m.pastT0 {
			for user, r := range m.claimed {
				if now := m.rank(user); now != r {
					fail("user admitted at rank %d is now at rank %d", r, now)
				}
			}
		}
		if m.admittedUpTo > 0 && !m.pastT0 {
			fail("someone was admitted before T0")
		}
	}
	t.Logf("seed %d: %d members, %d admitted, budget %d, T0 at step %d, claims %v",
		seed, len(m.users), m.admittedUpTo, m.maxSessions, t0Step, m.claims)
}

// claimOutcome names what the model expects a claim at rank r to get.
func claimOutcome(r int64, m *f1Model) string {
	switch {
	case r == 0:
		return "not_in_queue"
	case r > m.admittedUpTo:
		return "not_your_turn"
	case !m.live[r]:
		return "expired"
	}
	return "issued"
}
