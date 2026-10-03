// Package policy holds the sale's admission rules (design doc sections 1 and
// 11): who may enter an event's waiting room, and when. It is pure: callers
// load an event's Rules and the buyer's Buyer, and ask Check.
//
//   - Agent lockout: buyers with the AGENT role are kept out until the
//     window ends, mirroring the IRCTC rule that bars agents from the
//     opening minutes of Tatkal booking.
//   - Verified-only window: until it ends, only buyers with a verified
//     identity may join. Signing in with a phone code (auth-svc) verifies a
//     buyer; the development header does not.
//
// The per-user cap (invariant I4) is enforced where units are held and sold
// (inventory-svc's hold.lua and booking-svc's final guard), and the freeze
// switch where admissions and holds happen (queue-svc and inventory-svc).
package policy

import (
	"fmt"
	"time"
)

// Rules are one event's policy windows. A zero time means no window.
type Rules struct {
	VerifiedOnlyUntil time.Time
	AgentLockoutUntil time.Time
}

// Buyer is what the rules need to know about the buyer.
type Buyer struct {
	Role     string // "BUYER", "AGENT" or "ADMIN"; empty for the development header
	Verified bool
}

// Reasons a buyer is refused.
const (
	ReasonAgentLockout = "AGENT_LOCKOUT"
	ReasonVerifiedOnly = "VERIFIED_ONLY"
)

// Refused says why the buyer may not join yet, and from when they may.
type Refused struct {
	Reason string
	Until  time.Time
}

func (r *Refused) Error() string {
	switch r.Reason {
	case ReasonAgentLockout:
		return fmt.Sprintf("policy: agents may join from %s", r.Until.UTC().Format(time.RFC3339))
	default:
		return fmt.Sprintf("policy: only verified buyers may join until %s", r.Until.UTC().Format(time.RFC3339))
	}
}

// Check returns nil if the buyer may join at now, or *Refused. When both
// windows apply, the one that ends later is reported, so the buyer is never
// told to come back too early.
func Check(r Rules, b Buyer, now time.Time) error {
	var refused *Refused
	if b.Role == "AGENT" && now.Before(r.AgentLockoutUntil) {
		refused = &Refused{Reason: ReasonAgentLockout, Until: r.AgentLockoutUntil}
	}
	if !b.Verified && now.Before(r.VerifiedOnlyUntil) && (refused == nil || r.VerifiedOnlyUntil.After(refused.Until)) {
		refused = &Refused{Reason: ReasonVerifiedOnly, Until: r.VerifiedOnlyUntil}
	}
	if refused != nil {
		return refused
	}
	return nil
}
