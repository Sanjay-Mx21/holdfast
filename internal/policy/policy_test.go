package policy

import (
	"errors"
	"testing"
	"time"
)

func TestCheck(t *testing.T) {
	now := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	before, after := now.Add(-time.Minute), now.Add(time.Minute)
	later := now.Add(10 * time.Minute)
	buyer := Buyer{Role: "BUYER", Verified: true}
	agent := Buyer{Role: "AGENT", Verified: true}
	unverified := Buyer{Role: "BUYER"}
	devHeader := Buyer{}
	cases := []struct {
		name   string
		rules  Rules
		buyer  Buyer
		reason string // "" allowed
		until  time.Time
	}{
		{"no windows, anyone", Rules{}, devHeader, "", time.Time{}},
		{"a verified buyer in the verified window", Rules{VerifiedOnlyUntil: after}, buyer, "", time.Time{}},
		{"an unverified buyer in the verified window", Rules{VerifiedOnlyUntil: after}, unverified, ReasonVerifiedOnly, after},
		{"the development header in the verified window", Rules{VerifiedOnlyUntil: after}, devHeader, ReasonVerifiedOnly, after},
		{"an unverified buyer after the window", Rules{VerifiedOnlyUntil: before}, unverified, "", time.Time{}},
		{"the window ends exactly now", Rules{VerifiedOnlyUntil: now}, unverified, "", time.Time{}},
		{"an agent in the lockout", Rules{AgentLockoutUntil: after}, agent, ReasonAgentLockout, after},
		{"an agent after the lockout", Rules{AgentLockoutUntil: before}, agent, "", time.Time{}},
		{"a buyer in the lockout", Rules{AgentLockoutUntil: after}, buyer, "", time.Time{}},
		{"an admin in the lockout", Rules{AgentLockoutUntil: after}, Buyer{Role: "ADMIN", Verified: true}, "", time.Time{}},
		{"both windows: the later one is reported", Rules{AgentLockoutUntil: after, VerifiedOnlyUntil: later}, Buyer{Role: "AGENT"}, ReasonVerifiedOnly, later},
		{"both windows, the lockout later", Rules{AgentLockoutUntil: later, VerifiedOnlyUntil: after}, Buyer{Role: "AGENT"}, ReasonAgentLockout, later},
	}
	for _, c := range cases {
		err := Check(c.rules, c.buyer, now)
		var r *Refused
		switch {
		case c.reason == "" && err != nil:
			t.Errorf("%s: refused (%v), want allowed", c.name, err)
		case c.reason != "" && (!errors.As(err, &r) || r.Reason != c.reason || !r.Until.Equal(c.until)):
			t.Errorf("%s: %v, want %s until %s", c.name, err, c.reason, c.until)
		}
	}
}
