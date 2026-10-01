package queue

import (
	"context"
	"embed"
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

	allScripts = []*redis.Script{provisionScript, joinScript, openScript, positionScript}
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

// Events lists provisioned events (the opener's work list).
func (s *Store) Events(ctx context.Context) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, eventsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("queue: list events: %w", err)
	}
	return ids, nil
}
