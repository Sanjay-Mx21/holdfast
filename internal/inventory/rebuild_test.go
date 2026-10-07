package inventory

import (
	"maps"
	"testing"
	"time"
)

func TestNewRebuildPlan(t *testing.T) {
	exp := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	p := NewRebuildPlan(100, 4, 30,
		map[string]int{"u1": 2, "u2": 4, "u0": 0},
		[]RebuildHold{
			{HoldID: "h1", UserID: "u1", Qty: 1, ExpiresAt: exp}, // bought 2 and pending 1
			{HoldID: "h3", UserID: "u3", Qty: 3, ExpiresAt: exp},
		})
	if p.Available != 100-30-4 {
		t.Fatalf("available %d", p.Available)
	}
	if want := map[string]int{"u1": 3, "u2": 4, "u3": 3}; !maps.Equal(p.Users, want) {
		t.Fatalf("users %v, want %v", p.Users, want)
	}
	if len(p.Holds) != 2 || p.validate() != nil {
		t.Fatalf("plan %+v", p)
	}

	for _, bad := range []RebuildPlan{
		{Capacity: 0, PerUserLimit: 4},
		{Capacity: 10, PerUserLimit: 0},
		{Capacity: 10, PerUserLimit: 4, Holds: []RebuildHold{{HoldID: "h", UserID: "u", Qty: 0}}},
		{Capacity: 10, PerUserLimit: 4, Holds: []RebuildHold{{UserID: "u", Qty: 1}}},
	} {
		if bad.validate() == nil {
			t.Errorf("plan %+v accepted", bad)
		}
	}
}

func TestParseRebuildReply(t *testing.T) {
	r, err := parseRebuildReply([]any{"57", int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7)})
	want := RebuildResult{AvailableBefore: 57, Provisioned: true, WasFrozen: true, UsersChanged: 2, UsersDeleted: 3,
		HoldsRecreated: 4, HoldsKept: 5, HoldsSold: 6, HoldsDropped: 7}
	if err != nil || r != want {
		t.Fatalf("got %+v, %v", r, err)
	}
	r, err = parseRebuildReply([]any{"none", int64(0), int64(0), int64(0), int64(1), int64(0), int64(0), int64(0)})
	if err != nil || r.Provisioned || r.HoldsRecreated != 1 {
		t.Fatalf("got %+v, %v", r, err)
	}
	for _, bad := range [][]any{{"1"}, {"x", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)}, {"1", "a", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)}} {
		if _, err := parseRebuildReply(bad); err == nil {
			t.Errorf("reply %v accepted", bad)
		}
	}
}
