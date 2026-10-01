package inventory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// A Service with a nil Store: any attempt to reach Valkey panics, which
// proves validation happens before any I/O.
func validatingOnlyService(t *testing.T) *Service {
	t.Helper()
	svc, err := NewService(nil, Config{HoldTTL: time.Minute, PaymentWindow: 2 * time.Minute}, NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestHoldIDForIsDeterministicAndScopedToTheUser(t *testing.T) {
	u1, u2 := uuid.NewString(), uuid.NewString()
	a := HoldIDFor(u1, "order-00000001")
	if a != HoldIDFor(u1, "order-00000001") {
		t.Fatal("same user and key must give the same hold ID")
	}
	if a == HoldIDFor(u1, "order-00000002") || a == HoldIDFor(u2, "order-00000001") {
		t.Fatal("different key or user must give a different hold ID")
	}
	if id, err := uuid.Parse(a); err != nil || id.Version() != 5 {
		t.Fatalf("want a UUIDv5, got %q", a)
	}
}

func TestCanonicalUUID(t *testing.T) {
	id := uuid.NewString()
	got, err := canonicalUUID("x", strings.ToUpper(id))
	if err != nil || got != id {
		t.Fatalf("upper-case form: got %q, %v", got, err)
	}
	for _, bad := range []string{"", "not-a-uuid", "{" + id + "}", "urn:uuid:" + id, strings.ReplaceAll(id, "-", ""), id + "|x"} {
		if _, err := canonicalUUID("x", bad); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}

func TestValidIdempotencyKey(t *testing.T) {
	for k, want := range map[string]bool{
		"order-2026-09-25:abc_1.2": true,
		"12345678":                 true,
		strings.Repeat("k", 255):   true,
		"short":                    false,
		strings.Repeat("k", 256):   false,
		"has space-123":            false,
		"brace{tag}-123":           false,
		"pipe|separated":           false,
	} {
		if got := validIdempotencyKey(k); got != want {
			t.Fatalf("validIdempotencyKey(%q) = %v, want %v", k, got, want)
		}
	}
}

func TestEveryKeyOfAnEventSharesOneHashTag(t *testing.T) {
	k := keysFor("evt")
	for _, key := range []string{k.avail(), k.config(), k.expiry(), k.user("u"), k.hold("h")} {
		if !strings.HasPrefix(key, "inv:{evt}:") {
			t.Fatalf("%q does not start with the event hash tag", key)
		}
	}
	h, u, ok := parseExpiryMember(expiryMember("hold-1", "user-1"))
	if !ok || h != "hold-1" || u != "user-1" {
		t.Fatalf("round trip failed: %q %q %v", h, u, ok)
	}
	for _, bad := range []string{"", "no-separator", "|user", "hold|"} {
		if _, _, ok := parseExpiryMember(bad); ok {
			t.Fatalf("%q must not parse", bad)
		}
	}
}

func TestEventConfigValidate(t *testing.T) {
	good := EventConfig{Capacity: 100, PerUserLimit: 4}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []EventConfig{
		{Capacity: 0, PerUserLimit: 4},
		{Capacity: 10_000_001, PerUserLimit: 4},
		{Capacity: 100, PerUserLimit: 0},
		{Capacity: 100, PerUserLimit: 11},
		{Capacity: 100, PerUserLimit: 4, InitialAvailable: 101},
		{Capacity: 100, PerUserLimit: 4, InitialAvailable: -1},
	} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%+v must be invalid", bad)
		}
	}
}

func TestServiceConfigIsValidated(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	if _, err := NewService(nil, Config{HoldTTL: 0, PaymentWindow: time.Minute}, m); err == nil {
		t.Fatal("zero hold TTL must be rejected")
	}
	if _, err := NewService(nil, Config{HoldTTL: 5 * time.Minute, PaymentWindow: time.Minute}, m); err == nil {
		t.Fatal("payment window shorter than the hold TTL must be rejected")
	}
}

func TestInputIsValidatedBeforeAnyIO(t *testing.T) {
	svc := validatingOnlyService(t)
	ctx := context.Background()
	ev, user, hold := uuid.NewString(), uuid.NewString(), uuid.NewString()
	create := func(r CreateHoldRequest) error { _, err := svc.CreateHold(ctx, r); return err }

	checks := []struct {
		name string
		err  error
		want error
	}{
		{"bad event", create(CreateHoldRequest{EventID: "x", UserID: user, IdempotencyKey: "order-0001", Quantity: 1}), ErrInvalidRequest},
		{"bad user", create(CreateHoldRequest{EventID: ev, UserID: "alice", IdempotencyKey: "order-0001", Quantity: 1}), ErrInvalidRequest},
		{"short key", create(CreateHoldRequest{EventID: ev, UserID: user, IdempotencyKey: "k1", Quantity: 1}), ErrInvalidRequest},
		{"zero quantity", create(CreateHoldRequest{EventID: ev, UserID: user, IdempotencyKey: "order-0001", Quantity: 0}), ErrInvalidQuantity},
		{"huge quantity", create(CreateHoldRequest{EventID: ev, UserID: user, IdempotencyKey: "order-0001", Quantity: 11}), ErrInvalidQuantity},
		{"get with bad hold id", func() error { _, err := svc.GetHold(ctx, ev, user, "h"); return err }(), ErrInvalidRequest},
		{"confirm zero quantity", func() error { _, err := svc.Confirm(ctx, ev, user, hold, 0); return err }(), ErrInvalidQuantity},
		{"provision bad config", func() error { _, err := svc.Provision(ctx, ev, EventConfig{}); return err }(), ErrInvalidRequest},
		{"availability bad id", func() error { _, err := svc.Availability(ctx, "{x}"); return err }(), ErrInvalidRequest},
	}
	for _, c := range checks {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, c.err, c.want)
		}
	}
}
