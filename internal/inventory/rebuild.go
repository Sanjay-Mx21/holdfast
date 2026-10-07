package inventory

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"
)

var rebuildScript = loadScript("rebuild.lua")

// RebuildPlan is what an event's inventory must be, by PostgreSQL's records
// (runbook RB-2, task 5.3). NewRebuildPlan derives it.
type RebuildPlan struct {
	Capacity, PerUserLimit int
	// Available is capacity minus units sold minus units in pending
	// bookings. It can be negative only if PostgreSQL disagrees with itself.
	Available int
	// Users maps each user who bought units or has a pending booking to
	// those units together.
	Users map[string]int
	// Holds are the pending bookings' holds, kept or recreated as PAYING.
	Holds []RebuildHold
}

// RebuildHold is the hold behind one PENDING_PAYMENT booking.
type RebuildHold struct {
	HoldID, UserID string
	Qty            int
	// ExpiresAt is when the sweeper may release it: the booking's payment
	// deadline plus booking-svc's grace, as mark_paying set it.
	ExpiresAt time.Time
}

// NewRebuildPlan computes the plan from an event's capacity, per-user limit
// and units sold, the units each user bought, and its pending bookings'
// holds.
func NewRebuildPlan(capacity, perUserLimit, sold int, purchases map[string]int, pending []RebuildHold) RebuildPlan {
	p := RebuildPlan{
		Capacity: capacity, PerUserLimit: perUserLimit,
		Available: capacity - sold,
		Users:     make(map[string]int, len(purchases)+len(pending)),
		Holds:     pending,
	}
	for user, qty := range purchases {
		if qty > 0 {
			p.Users[user] = qty
		}
	}
	for _, h := range pending {
		p.Available -= h.Qty
		p.Users[h.UserID] += h.Qty
	}
	return p
}

func (p RebuildPlan) validate() error {
	if p.Capacity < 1 || p.PerUserLimit < 1 {
		return fmt.Errorf("inventory: rebuild: capacity %d and per-user limit %d must be positive", p.Capacity, p.PerUserLimit)
	}
	for _, h := range p.Holds {
		if h.HoldID == "" || h.UserID == "" || h.Qty < 1 {
			return fmt.Errorf("inventory: rebuild: malformed hold %+v", h)
		}
	}
	return nil
}

// RebuildResult says what a rebuild changed (or, in a dry run, would).
type RebuildResult struct {
	// AvailableBefore is the pool before; Provisioned is false if the event
	// had no pool at all (Valkey lost everything).
	AvailableBefore int64
	Provisioned     bool
	WasFrozen       bool
	// Per-user counters set to a different value, and deleted.
	UsersChanged, UsersDeleted int
	// Pending bookings' holds: recreated (missing or released), kept
	// (already held or paying), or already sold (confirmed meanwhile).
	HoldsRecreated, HoldsKept, HoldsSold int
	// Open holds with no pending booking, released.
	HoldsDropped int
}

// Rebuild makes an event's Valkey inventory match plan in one atomic script
// and leaves the sale frozen. With dryRun it changes nothing and reports
// what it would change. Freeze the sale before reading the plan from
// PostgreSQL: holds taken in between would be dropped.
func (s *Store) Rebuild(ctx context.Context, eventID string, plan RebuildPlan, dryRun bool) (RebuildResult, error) {
	if err := plan.validate(); err != nil {
		return RebuildResult{}, err
	}
	k := keysFor(eventID)

	// The per-user counters that exist now, and the open holds.
	existing := map[string]bool{}
	iter := s.rdb.Scan(ctx, 0, k.user("*"), 1000).Iterator()
	for iter.Next(ctx) {
		existing[iter.Val()] = true
	}
	if err := iter.Err(); err != nil {
		return RebuildResult{}, fmt.Errorf("inventory: rebuild: scan users: %w", err)
	}
	members, err := s.rdb.ZRange(ctx, k.expiry(), 0, -1).Result()
	if err != nil {
		return RebuildResult{}, fmt.Errorf("inventory: rebuild: read expiry index: %w", err)
	}

	keep := make(map[string]bool, len(plan.Holds))
	for _, h := range plan.Holds {
		keep[h.HoldID] = true
	}
	users := slices.Sorted(maps.Keys(plan.Users)) // a deterministic order, for tests and replays
	for _, u := range users {
		delete(existing, k.user(u))
	}
	stale := slices.Sorted(maps.Keys(existing)) // counters of users with nothing left
	var drop []string                           // expiry members of holds with no pending booking
	for _, m := range members {
		holdID, _, ok := parseExpiryMember(m)
		if !ok || !keep[holdID] {
			drop = append(drop, m)
		}
	}

	keyList := []string{k.avail(), k.config(), k.expiry()}
	dry := "0"
	if dryRun {
		dry = "1"
	}
	args := []any{dry, plan.Capacity, plan.PerUserLimit, plan.Available, len(users), len(stale), len(plan.Holds), len(drop)}
	for _, u := range users {
		keyList = append(keyList, k.user(u))
		args = append(args, plan.Users[u])
	}
	keyList = append(keyList, stale...)
	for _, h := range plan.Holds {
		keyList = append(keyList, k.hold(h.HoldID))
		args = append(args, h.UserID, h.Qty, h.ExpiresAt.UnixMilli(), expiryMember(h.HoldID, h.UserID))
	}
	for _, m := range drop {
		holdID, _, ok := parseExpiryMember(m)
		if !ok {
			holdID = "malformed" // the script finds no state there and only removes the member
		}
		keyList = append(keyList, k.hold(holdID))
		args = append(args, m)
	}

	vals, err := rebuildScript.Run(ctx, s.rdb, keyList, args...).Slice()
	if err != nil {
		return RebuildResult{}, fmt.Errorf("inventory: rebuild: %w", err)
	}
	r, err := parseRebuildReply(vals)
	if err != nil {
		return RebuildResult{}, err
	}
	if !dryRun {
		// Register with the sweeper, as Provision does (SADD is idempotent).
		if err := s.rdb.SAdd(ctx, eventsKey, eventID).Err(); err != nil {
			return r, fmt.Errorf("inventory: rebuild: register event: %w", err)
		}
	}
	return r, nil
}

func parseRebuildReply(vals []any) (RebuildResult, error) {
	if len(vals) != 8 {
		return RebuildResult{}, fmt.Errorf("inventory: rebuild: unexpected reply %v", vals)
	}
	n := make([]int, 8)
	for i := 1; i < 8; i++ {
		v, ok := vals[i].(int64)
		if !ok {
			return RebuildResult{}, fmt.Errorf("inventory: rebuild: unexpected reply %v", vals)
		}
		n[i] = int(v)
	}
	r := RebuildResult{
		WasFrozen: n[1] == 1, UsersChanged: n[2], UsersDeleted: n[3],
		HoldsRecreated: n[4], HoldsKept: n[5], HoldsSold: n[6], HoldsDropped: n[7],
	}
	switch before := vals[0].(type) {
	case string:
		if before != "none" {
			v, err := strconv.ParseInt(before, 10, 64)
			if err != nil {
				return RebuildResult{}, errors.New("inventory: rebuild: unreadable available count " + before)
			}
			r.AvailableBefore, r.Provisioned = v, true
		}
	default:
		return RebuildResult{}, fmt.Errorf("inventory: rebuild: unexpected reply %v", vals)
	}
	return r, nil
}
