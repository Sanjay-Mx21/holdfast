package queue

import (
	"errors"
	"testing"
	"time"
)

func validConfig() EventConfig {
	return EventConfig{
		OpensAt:       time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
		AdmissionRate: 83,
		MaxSessions:   10_000,
		SessionTTL:    10 * time.Minute,
	}
}

func TestEventConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*EventConfig)
		ok     bool
	}{
		{"valid", func(*EventConfig) {}, true},
		{"zero opening time", func(c *EventConfig) { c.OpensAt = time.Time{} }, false},
		{"opening before 2020", func(c *EventConfig) { c.OpensAt = time.Date(2019, 12, 31, 0, 0, 0, 0, time.UTC) }, false},
		{"opening in the past is allowed", func(c *EventConfig) { c.OpensAt = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC) }, true},
		{"rate zero", func(c *EventConfig) { c.AdmissionRate = 0 }, false},
		{"rate at maximum", func(c *EventConfig) { c.AdmissionRate = MaxAdmissionRate }, true},
		{"rate above maximum", func(c *EventConfig) { c.AdmissionRate = MaxAdmissionRate + 1 }, false},
		{"sessions zero", func(c *EventConfig) { c.MaxSessions = 0 }, false},
		{"sessions negative", func(c *EventConfig) { c.MaxSessions = -1 }, false},
		{"sessions above maximum", func(c *EventConfig) { c.MaxSessions = MaxSessions + 1 }, false},
		{"session TTL at minimum", func(c *EventConfig) { c.SessionTTL = MinSessionTTL }, true},
		{"session TTL below minimum", func(c *EventConfig) { c.SessionTTL = 59 * time.Second }, false},
		{"session TTL at maximum", func(c *EventConfig) { c.SessionTTL = MaxSessionTTL }, true},
		{"session TTL above maximum", func(c *EventConfig) { c.SessionTTL = MaxSessionTTL + time.Second }, false},
		{"session TTL with fractional seconds", func(c *EventConfig) { c.SessionTTL = 90*time.Second + 500*time.Millisecond }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			err := c.Validate()
			if tt.ok && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Validate() = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestCanonicalUUID(t *testing.T) {
	got, err := canonicalUUID("eventId", "0196F0C1-7A3E-7C51-9B0E-5D2F8A1C4E77")
	if err != nil || got != "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77" {
		t.Fatalf("canonicalUUID upper-case = %q, %v", got, err)
	}
	for _, bad := range []string{"", "not-a-uuid", "{0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77}", "0196f0c17a3e7c519b0e5d2f8a1c4e77", "urn:uuid:0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"} {
		if _, err := canonicalUUID("eventId", bad); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("canonicalUUID(%q) = %v, want ErrInvalidRequest", bad, err)
		}
	}
}
