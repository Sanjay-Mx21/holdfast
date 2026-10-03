package inventory

import (
	"context"
	"embed"
	"errors"
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
		panic(fmt.Sprintf("inventory: embedded script %s: %v", name, err))
	}
	return redis.NewScript(string(src))
}

var (
	provisionScript  = loadScript("provision.lua")
	holdScript       = loadScript("hold.lua")
	markPayingScript = loadScript("mark_paying.lua")
	releaseScript    = loadScript("release.lua")
	confirmScript    = loadScript("confirm.lua")
	freezeScript     = loadScript("freeze.lua")

	allScripts = []*redis.Script{provisionScript, holdScript, markPayingScript, releaseScript, confirmScript, freezeScript}
)

// Store is the Valkey-backed hot state of inventory. It runs the atomic
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
			return fmt.Errorf("inventory: load script: %w", err)
		}
	}
	return nil
}

// Provision sets up an event's counters exactly once.
func (s *Store) Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error) {
	k := keysFor(eventID)
	code, err := provisionScript.Run(ctx, s.rdb, []string{k.avail(), k.config()},
		cfg.Capacity, cfg.PerUserLimit, cfg.initialAvailable()).Int64()
	if err != nil {
		return false, fmt.Errorf("inventory: provision: %w", err)
	}
	if code < 0 {
		return false, ErrProvisionConflict
	}
	// Register with the sweeper even on a retry: SADD is idempotent, and it
	// repairs the rare case where an earlier attempt died between the calls.
	if err := s.rdb.SAdd(ctx, eventsKey, eventID).Err(); err != nil {
		return false, fmt.Errorf("inventory: register event: %w", err)
	}
	return code == 1, nil
}

type holdReply struct {
	code, n, expiresAtMs int64
}

func (s *Store) hold(ctx context.Context, eventID, userID, holdID string, qty int, ttl time.Duration) (holdReply, error) {
	k := keysFor(eventID)
	vals, err := holdScript.Run(ctx, s.rdb,
		[]string{k.avail(), k.config(), k.user(userID), k.hold(holdID), k.expiry()},
		qty, userID, expiryMember(holdID, userID), ttl.Milliseconds(),
	).Int64Slice()
	if err != nil {
		return holdReply{}, fmt.Errorf("inventory: hold: %w", err)
	}
	if len(vals) != 3 {
		return holdReply{}, fmt.Errorf("inventory: hold: unexpected reply %v", vals)
	}
	return holdReply{code: vals[0], n: vals[1], expiresAtMs: vals[2]}, nil
}

// GetHold reads a hold (including a released tombstone, kept for one hour).
func (s *Store) GetHold(ctx context.Context, eventID, holdID string) (Hold, error) {
	m, err := s.rdb.HGetAll(ctx, keysFor(eventID).hold(holdID)).Result()
	if err != nil {
		return Hold{}, fmt.Errorf("inventory: get hold: %w", err)
	}
	if len(m) == 0 {
		return Hold{}, ErrHoldNotFound
	}
	qty, _ := strconv.Atoi(m["qty"])
	h := Hold{ID: holdID, EventID: eventID, UserID: m["user"], Quantity: qty, State: HoldState(m["state"])}
	if ms, err := strconv.ParseInt(m["expires_at"], 10, 64); err == nil && ms > 0 {
		h.ExpiresAt = time.UnixMilli(ms).UTC()
	}
	return h, nil
}

// MarkPaying moves a HELD hold to PAYING and extends its protection window.
func (s *Store) MarkPaying(ctx context.Context, eventID, userID, holdID string, window time.Duration) (time.Time, error) {
	k := keysFor(eventID)
	vals, err := markPayingScript.Run(ctx, s.rdb, []string{k.hold(holdID), k.expiry()},
		userID, expiryMember(holdID, userID), window.Milliseconds()).Int64Slice()
	if err != nil {
		return time.Time{}, fmt.Errorf("inventory: mark paying: %w", err)
	}
	if len(vals) != 2 {
		return time.Time{}, fmt.Errorf("inventory: mark paying: unexpected reply %v", vals)
	}
	switch vals[0] {
	case 1, 2:
		return time.UnixMilli(vals[1]).UTC(), nil
	case -1:
		return time.Time{}, ErrHoldNotFound // another user's hold: don't reveal it exists
	default:
		return time.Time{}, ErrHoldExpired
	}
}

// Release returns a hold's units to the pool. It reports false when there was
// nothing to release (already released, sold, or unknown).
func (s *Store) Release(ctx context.Context, eventID, userID, holdID string, mode ReleaseMode) (bool, error) {
	k := keysFor(eventID)
	code, err := releaseScript.Run(ctx, s.rdb,
		[]string{k.avail(), k.user(userID), k.hold(holdID), k.expiry()},
		expiryMember(holdID, userID), string(mode), userID,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("inventory: release: %w", err)
	}
	switch code {
	case 1:
		return true, nil
	case 0:
		return false, nil
	case -1:
		return false, errNotExpiredYet
	case -2:
		return false, ErrHoldNotCancellable
	case -3:
		return false, ErrHoldNotFound
	default:
		return false, fmt.Errorf("inventory: release: unexpected reply code %d", code)
	}
}

// Confirm marks a hold SOLD after PostgreSQL committed the booking.
func (s *Store) Confirm(ctx context.Context, eventID, userID, holdID string, qty int) (ConfirmOutcome, error) {
	k := keysFor(eventID)
	code, err := confirmScript.Run(ctx, s.rdb,
		[]string{k.hold(holdID), k.expiry(), k.avail(), k.user(userID)},
		expiryMember(holdID, userID), qty, userID,
	).Int64()
	if err != nil {
		return "", fmt.Errorf("inventory: confirm: %w", err)
	}
	switch code {
	case 1:
		return ConfirmApplied, nil
	case 2:
		return ConfirmReplay, nil
	case 3:
		return ConfirmLate, nil
	case -1:
		return "", ErrHoldNotFound
	default:
		return "", fmt.Errorf("inventory: confirm: unexpected reply code %d", code)
	}
}

// Availability reads an event's remaining and total units, its open holds
// and whether it is frozen.
func (s *Store) Availability(ctx context.Context, eventID string) (Availability, error) {
	k := keysFor(eventID)
	pipe := s.rdb.Pipeline() // the keys share a hash slot, so this is one round trip
	availCmd := pipe.Get(ctx, k.avail())
	capCmd := pipe.HGet(ctx, k.config(), "capacity")
	frozenCmd := pipe.HGet(ctx, k.config(), "frozen")
	holdsCmd := pipe.ZCard(ctx, k.expiry())
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return Availability{}, fmt.Errorf("inventory: availability: %w", err)
	}
	capacity, err := capCmd.Int()
	if errors.Is(err, redis.Nil) {
		return Availability{}, ErrEventNotProvisioned
	}
	if err != nil {
		return Availability{}, fmt.Errorf("inventory: availability: %w", err)
	}
	avail, err := availCmd.Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Availability{}, fmt.Errorf("inventory: availability: %w", err)
	}
	return Availability{
		EventID: eventID, Available: avail, Capacity: capacity,
		ActiveHolds: int(holdsCmd.Val()), Frozen: frozenCmd.Val() == "1",
	}, nil
}

// SetFrozen sets or clears the event's freeze flag. It reports whether this
// call changed it.
func (s *Store) SetFrozen(ctx context.Context, eventID string, frozen bool) (bool, error) {
	flag := "0"
	if frozen {
		flag = "1"
	}
	code, err := freezeScript.Run(ctx, s.rdb, []string{keysFor(eventID).config()}, flag).Int64()
	if err != nil {
		return false, fmt.Errorf("inventory: freeze: %w", err)
	}
	if code < 0 {
		return false, ErrEventNotProvisioned
	}
	return code == 1, nil
}

// Events lists provisioned events (the sweeper's work list).
func (s *Store) Events(ctx context.Context) ([]string, error) {
	return s.rdb.SMembers(ctx, eventsKey).Result()
}

// ExpiredMembers returns up to limit expiry-index members due at or before upTo.
func (s *Store) ExpiredMembers(ctx context.Context, eventID string, upTo time.Time, limit int64) ([]string, error) {
	return s.rdb.ZRangeByScore(ctx, keysFor(eventID).expiry(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(upTo.UnixMilli(), 10),
		Count: limit,
	}).Result()
}

// RemoveExpiryMember drops a malformed member from the expiry index.
func (s *Store) RemoveExpiryMember(ctx context.Context, eventID, member string) error {
	return s.rdb.ZRem(ctx, keysFor(eventID).expiry(), member).Err()
}

// Purge deletes every key of an event. For tests and tooling only: never run
// it against an event that is on sale.
func (s *Store) Purge(ctx context.Context, eventID string) error {
	iter := s.rdb.Scan(ctx, 0, keysFor(eventID).prefix()+"*", 1000).Iterator()
	var batch []string
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())
		if len(batch) == 500 {
			if err := s.rdb.Unlink(ctx, batch...).Err(); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := s.rdb.Unlink(ctx, batch...).Err(); err != nil {
			return err
		}
	}
	return s.rdb.SRem(ctx, eventsKey, eventID).Err()
}
