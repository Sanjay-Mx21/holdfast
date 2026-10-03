package queue

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/policy"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

func loadScript(name string) *redis.Script {
	src, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(fmt.Sprintf("queue: embedded script %s: %v", name, err))
	}
	return redis.NewScript(string(src))
}

var (
	provisionScript = loadScript("provision.lua")
	joinScript      = loadScript("join.lua")
	openScript      = loadScript("open.lua")
	positionScript  = loadScript("position.lua")
	advanceScript   = loadScript("advance.lua")
	statusScript    = loadScript("status.lua")
	admitScript     = loadScript("admit.lua")
	stateScript     = loadScript("state.lua")
	soldOutScript   = loadScript("soldout.lua")

	allScripts = []*redis.Script{
		provisionScript, joinScript, openScript, positionScript, advanceScript, statusScript, admitScript,
		stateScript, soldOutScript,
	}
)

// Store is the Valkey-backed state of the waiting room. It runs the atomic
// scripts and translates their reply codes into domain values and errors; it
// holds no business policy (that lives in Service).
type Store struct {
	rdb redis.UniversalClient
}

// NewStore returns a Store backed by rdb.
func NewStore(rdb redis.UniversalClient) *Store { return &Store{rdb: rdb} }

// LoadScripts preloads every script (SCRIPT LOAD) so requests right after a
// deploy or failover go straight to EVALSHA. Script.Run already falls back to
// EVAL on NOSCRIPT, so this is an optimisation, not a correctness requirement.
func (s *Store) LoadScripts(ctx context.Context) error {
	for _, sc := range allScripts {
		if err := sc.Load(ctx, s.rdb).Err(); err != nil {
			return fmt.Errorf("queue: load script: %w", err)
		}
	}
	return nil
}

// Provision stores an event's queue settings exactly once and puts the queue
// in state PRE; it sets the policy windows on every accepted call. It
// reports whether this call created the settings.
func (s *Store) Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error) {
	k := keysFor(eventID)
	code, err := provisionScript.Run(ctx, s.rdb, []string{k.config(), k.state(), k.policy()},
		cfg.OpensAt.UnixMilli(), cfg.AdmissionRate, cfg.MaxSessions, cfg.SessionTTL.Milliseconds(),
		unixMilliOrZero(cfg.Policy.VerifiedOnlyUntil), unixMilliOrZero(cfg.Policy.AgentLockoutUntil)).Int64()
	if err != nil {
		return false, fmt.Errorf("queue: provision: %w", err)
	}
	if code < 0 {
		return false, ErrProvisionConflict
	}
	// Register with the opener even on a retry: SADD is idempotent, and it
	// repairs the rare case where an earlier attempt died between the calls.
	if err := s.rdb.SAdd(ctx, eventsKey, eventID).Err(); err != nil {
		return false, fmt.Errorf("queue: register event: %w", err)
	}
	return code == 1, nil
}

// Policy reads the event's policy windows. An event provisioned without any,
// or not provisioned at all, has none.
func (s *Store) Policy(ctx context.Context, eventID string) (policy.Rules, error) {
	vals, err := s.rdb.HMGet(ctx, keysFor(eventID).policy(), "verified_only_until_ms", "agent_lockout_until_ms").Result()
	if err != nil {
		return policy.Rules{}, fmt.Errorf("queue: read policy: %w", err)
	}
	return policy.Rules{VerifiedOnlyUntil: msTime(vals[0]), AgentLockoutUntil: msTime(vals[1])}, nil
}

func unixMilliOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// msTime parses a stored millisecond time; "0" or nothing is the zero time.
func msTime(v any) time.Time {
	raw, _ := v.(string)
	ms, _ := strconv.ParseInt(raw, 10, 64)
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// joinOutcome is what join.lua reports for one join.
type joinOutcome struct {
	joined bool    // this call added the user
	score  float64 // the member's score, new or original
	opened bool    // this call flipped the queue from PRE to OPEN (T0)
}

// Join adds userID to the event's queue with lotteryScore if Valkey's clock
// is still before T0, or with the next arrival number from T0 on.
func (s *Store) Join(ctx context.Context, eventID, userID, lotteryScore string) (joinOutcome, error) {
	k := keysFor(eventID)
	res, err := joinScript.Run(ctx, s.rdb, []string{k.state(), k.members(), k.seq(), k.config()},
		userID, lotteryScore).Slice()
	if err != nil {
		return joinOutcome{}, fmt.Errorf("queue: join: %w", err)
	}
	if len(res) != 3 {
		return joinOutcome{}, fmt.Errorf("queue: join: unexpected reply %v", res)
	}
	code, _ := res[0].(int64)
	switch code {
	case -2:
		return joinOutcome{}, ErrEventNotFound
	case -1:
		return joinOutcome{}, ErrQueueClosed
	}
	raw, _ := res[1].(string)
	score, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return joinOutcome{}, fmt.Errorf("queue: join: bad score %q", raw)
	}
	opened, _ := res[2].(int64)
	return joinOutcome{joined: code == 1, score: score, opened: opened == 1}, nil
}

// Open performs the T0 transition if it is due: PRE becomes OPEN once
// Valkey's clock reaches the opening time. It reports whether this call
// opened the queue and, if so, how late after T0 it did.
func (s *Store) Open(ctx context.Context, eventID string) (bool, time.Duration, error) {
	k := keysFor(eventID)
	res, err := openScript.Run(ctx, s.rdb, []string{k.state(), k.config()}).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("queue: open: %w", err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("queue: open: unexpected reply %v", res)
	}
	switch res[0] {
	case -1:
		return false, 0, ErrEventNotFound
	case 1:
		return true, time.Duration(res[1]) * time.Millisecond, nil
	}
	return false, 0, nil
}

// Position reads where userID stands: before T0 (by Valkey's clock) the
// opening time, from T0 on the 1-based rank.
func (s *Store) Position(ctx context.Context, eventID, userID string) (Position, error) {
	k := keysFor(eventID)
	res, err := positionScript.Run(ctx, s.rdb, []string{k.state(), k.config(), k.members()}, userID).Slice()
	if err != nil {
		return Position{}, fmt.Errorf("queue: position: %w", err)
	}
	if len(res) != 3 {
		return Position{}, fmt.Errorf("queue: position: unexpected reply %v", res)
	}
	code, _ := res[0].(int64)
	value, _ := res[1].(int64)
	state, _ := res[2].(string)
	switch code {
	case -2:
		return Position{}, ErrEventNotFound
	case -1:
		return Position{}, ErrNotInQueue
	case 0:
		return Position{EventID: eventID, State: State(state), RandomizingAt: time.UnixMilli(value).UTC()}, nil
	}
	return Position{EventID: eventID, State: State(state), Rank: value}, nil
}

// NewTerm starts a leadership term for the event's admission controller and
// returns its fencing epoch. Every call returns a larger epoch, so writes
// from any earlier leader are refused from now on.
func (s *Store) NewTerm(ctx context.Context, eventID string) (int64, error) {
	epoch, err := s.rdb.Incr(ctx, keysFor(eventID).epoch()).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: new term: %w", err)
	}
	return epoch, nil
}

// Advance is one admission tick for the leader holding epoch: it admits up to
// n more people, within the session budget. A caller whose epoch is no
// longer current gets ErrFenced and must stop leading.
func (s *Store) Advance(ctx context.Context, eventID string, epoch int64, n int) (Advance, error) {
	return s.AdvanceWithin(ctx, eventID, epoch, n, -1)
}

// AdvanceWithin is Advance with a second cap on open sessions, from the
// units left (P17): active sessions never exceed unitsCap. -1 means no such
// cap.
func (s *Store) AdvanceWithin(ctx context.Context, eventID string, epoch int64, n, unitsCap int) (Advance, error) {
	k := keysFor(eventID)
	res, err := advanceScript.Run(ctx, s.rdb,
		[]string{k.epoch(), k.state(), k.config(), k.members(), k.admitted(), k.sessions(), k.status()},
		epoch, n, unitsCap).Slice()
	if err != nil {
		return Advance{}, fmt.Errorf("queue: advance: %w", err)
	}
	if len(res) != 6 {
		return Advance{}, fmt.Errorf("queue: advance: unexpected reply %v", res)
	}
	var nums [5]int64
	for i := range nums {
		nums[i], _ = res[i].(int64)
	}
	switch nums[0] {
	case -1:
		return Advance{}, ErrFenced
	case -2:
		return Advance{}, ErrEventNotFound
	}
	state, _ := res[5].(string)
	return Advance{AdmittedUpTo: nums[1], Admitted: nums[2], ActiveSessions: nums[3], QueueSize: nums[4], State: State(state)}, nil
}

// MarkSoldOut is the leader's sold-out transition: an OPEN queue becomes
// SOLD_OUT. It reports whether this call marked it; a queue in any other
// state (sold out already, frozen, closed) is left alone. A caller whose
// epoch is no longer current gets ErrFenced.
func (s *Store) MarkSoldOut(ctx context.Context, eventID string, epoch int64) (bool, error) {
	k := keysFor(eventID)
	code, err := soldOutScript.Run(ctx, s.rdb, []string{k.epoch(), k.state()}, epoch).Int64()
	if err != nil {
		return false, fmt.Errorf("queue: mark sold out: %w", err)
	}
	if code == -1 {
		return false, ErrFenced
	}
	return code == 1, nil
}

// Transition moves the queue from one state to another (the freeze switch).
// It reports whether this call moved it: a queue already in the target state
// is not an error, a queue in any other state is ErrStateConflict.
func (s *Store) Transition(ctx context.Context, eventID string, from, to State) (bool, error) {
	res, err := stateScript.Run(ctx, s.rdb, []string{keysFor(eventID).state()}, string(from), string(to)).Slice()
	if err != nil {
		return false, fmt.Errorf("queue: change state: %w", err)
	}
	if len(res) != 2 {
		return false, fmt.Errorf("queue: change state: unexpected reply %v", res)
	}
	code, _ := res[0].(int64)
	state, _ := res[1].(string)
	switch code {
	case -2:
		return false, ErrEventNotFound
	case -1:
		return false, fmt.Errorf("%w: it is %s, and only a queue in state %s can become %s", ErrStateConflict, state, from, to)
	}
	return code == 1, nil
}

// Status reads the event's status document, or a fallback built from the
// raw keys (with a zero UpdatedAt) if no admission leader has written one.
func (s *Store) Status(ctx context.Context, eventID string) (Status, error) {
	k := keysFor(eventID)
	res, err := statusScript.Run(ctx, s.rdb,
		[]string{k.status(), k.state(), k.config(), k.admitted(), k.members()}).Slice()
	if err != nil {
		return Status{}, fmt.Errorf("queue: status: %w", err)
	}
	if len(res) != 2 {
		return Status{}, fmt.Errorf("queue: status: unexpected reply %v", res)
	}
	if code, _ := res[0].(int64); code == -2 {
		return Status{}, ErrEventNotFound
	}
	raw, _ := res[1].(string)
	var d statusDoc
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return Status{}, fmt.Errorf("queue: status: bad document: %w", err)
	}
	st := Status{
		EventID: eventID, State: d.State, OpensAt: time.UnixMilli(d.OpensAtMs).UTC(),
		AdmittedUpTo: d.AdmittedUpTo, QueueSize: d.QueueSize,
	}
	if d.UpdatedAtMs > 0 {
		st.UpdatedAt = time.UnixMilli(d.UpdatedAtMs).UTC()
	}
	return st, nil
}

// Admit checks whether userID may claim their turn: their rank is within
// admittedUpTo and their session slot is alive.
func (s *Store) Admit(ctx context.Context, eventID, userID string) (Turn, error) {
	k := keysFor(eventID)
	res, err := admitScript.Run(ctx, s.rdb, []string{k.state(), k.members(), k.admitted(), k.sessions()}, userID).Int64Slice()
	if err != nil {
		return Turn{}, fmt.Errorf("queue: admit: %w", err)
	}
	if len(res) != 3 {
		return Turn{}, fmt.Errorf("queue: admit: unexpected reply %v", res)
	}
	switch res[0] {
	case -1:
		return Turn{}, ErrNotInQueue
	case -2:
		return Turn{}, ErrEventNotFound
	case -3:
		return Turn{}, ErrTurnExpired
	case -4:
		return Turn{}, ErrQueueClosed
	case 0:
		return Turn{}, &NotYourTurnError{Rank: res[1], AdmittedUpTo: res[2]}
	}
	return Turn{EventID: eventID, UserID: userID, Rank: res[1], SessionExpires: time.UnixMilli(res[2]).UTC()}, nil
}

// StatusAge returns how long ago the admission leader last wrote the event's
// status document, by Valkey's clock (the clock that stamped it, so skew
// between hosts cannot distort it); ok is false if no leader has written one.
func (s *Store) StatusAge(ctx context.Context, eventID string) (time.Duration, bool, error) {
	pipe := s.rdb.Pipeline()
	docCmd := pipe.Get(ctx, keysFor(eventID).status())
	timeCmd := pipe.Time(ctx)
	_, err := pipe.Exec(ctx)
	if errors.Is(docCmd.Err(), redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("queue: read status: %w", err)
	}
	var d statusDoc
	if err := json.Unmarshal([]byte(docCmd.Val()), &d); err != nil {
		return 0, false, fmt.Errorf("queue: bad status document: %w", err)
	}
	if d.UpdatedAtMs == 0 {
		return 0, false, errors.New("queue: status document has no update time")
	}
	return timeCmd.Val().Sub(time.UnixMilli(d.UpdatedAtMs)), true, nil
}

// Overview reads an event's settings, stored state, leader epoch, live
// sessions and work-list membership for operators. It is not one atomic
// snapshot (the leader may tick in between), which is fine for a status view;
// the Status part is left for the caller.
func (s *Store) Overview(ctx context.Context, eventID string) (Overview, error) {
	k := keysFor(eventID)
	pipe := s.rdb.Pipeline()
	cfgCmd := pipe.HMGet(ctx, k.config(), "opens_at_ms", "admission_rate", "max_sessions", "session_ttl_ms")
	policyCmd := pipe.HMGet(ctx, k.policy(), "verified_only_until_ms", "agent_lockout_until_ms")
	stateCmd := pipe.Get(ctx, k.state())
	epochCmd := pipe.Get(ctx, k.epoch())
	listedCmd := pipe.SIsMember(ctx, eventsKey, eventID)
	timeCmd := pipe.Time(ctx)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return Overview{}, fmt.Errorf("queue: overview: %w", err)
	}
	vals := cfgCmd.Val()
	num := func(i int) int64 {
		v, _ := vals[i].(string)
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	if vals[0] == nil || stateCmd.Val() == "" {
		return Overview{}, ErrEventNotFound
	}
	now := timeCmd.Val()
	live, err := s.rdb.ZCount(ctx, k.sessions(), "("+strconv.FormatInt(now.UnixMilli(), 10), "+inf").Result()
	if err != nil {
		return Overview{}, fmt.Errorf("queue: overview: %w", err)
	}
	epoch, _ := strconv.ParseInt(epochCmd.Val(), 10, 64)
	pol := policyCmd.Val()
	return Overview{
		Config: EventConfig{
			OpensAt: time.UnixMilli(num(0)).UTC(), AdmissionRate: int(num(1)),
			MaxSessions: int(num(2)), SessionTTL: time.Duration(num(3)) * time.Millisecond,
			Policy: policy.Rules{VerifiedOnlyUntil: msTime(pol[0]), AgentLockoutUntil: msTime(pol[1])},
		},
		StoredState: State(stateCmd.Val()), LeaderEpoch: epoch, ActiveSessions: live,
		OnWorkList: listedCmd.Val(), Now: now.UTC(),
	}, nil
}

// Purge deletes every key of an event and takes it off the work list, so
// every replica's opener and admission controller let it go. For tests and
// tooling only: never run it against an event that is on sale.
func (s *Store) Purge(ctx context.Context, eventID string) error {
	if err := s.rdb.SRem(ctx, eventsKey, eventID).Err(); err != nil {
		return fmt.Errorf("queue: purge: %w", err)
	}
	k := keysFor(eventID)
	if err := s.rdb.Unlink(ctx, k.config(), k.state(), k.members(), k.seq(), k.admitted(), k.status(), k.policy(), k.epoch(), k.sessions()).Err(); err != nil {
		return fmt.Errorf("queue: purge: %w", err)
	}
	return nil
}

// Events lists provisioned events (the opener's work list).
func (s *Store) Events(ctx context.Context) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, eventsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("queue: list events: %w", err)
	}
	return ids, nil
}
