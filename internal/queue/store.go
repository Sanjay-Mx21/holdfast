package queue

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
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

	allScripts = []*redis.Script{provisionScript, joinScript, openScript, positionScript, advanceScript, statusScript, admitScript}
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
// in state PRE. It reports whether this call created them.
func (s *Store) Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error) {
	k := keysFor(eventID)
	code, err := provisionScript.Run(ctx, s.rdb, []string{k.config(), k.state()},
		cfg.OpensAt.UnixMilli(), cfg.AdmissionRate, cfg.MaxSessions, cfg.SessionTTL.Milliseconds()).Int64()
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
	k := keysFor(eventID)
	res, err := advanceScript.Run(ctx, s.rdb,
		[]string{k.epoch(), k.state(), k.config(), k.members(), k.admitted(), k.sessions(), k.status()},
		epoch, n).Int64Slice()
	if err != nil {
		return Advance{}, fmt.Errorf("queue: advance: %w", err)
	}
	if len(res) != 4 {
		return Advance{}, fmt.Errorf("queue: advance: unexpected reply %v", res)
	}
	switch res[0] {
	case -1:
		return Advance{}, ErrFenced
	case -2:
		return Advance{}, ErrEventNotFound
	}
	return Advance{AdmittedUpTo: res[1], Admitted: res[2], ActiveSessions: res[3]}, nil
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

// Events lists provisioned events (the opener's work list).
func (s *Store) Events(ctx context.Context) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, eventsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("queue: list events: %w", err)
	}
	return ids, nil
}
