package ratelimit

import "testing"

func TestRuleValidate(t *testing.T) {
	tests := []struct {
		rule Rule
		ok   bool
	}{
		{Rule{Capacity: 1, Rate: 1}, true},
		{Rule{Capacity: 30, Rate: 0.5}, true},
		{Rule{Capacity: 0, Rate: 1}, false},
		{Rule{Capacity: 1_000_001, Rate: 1}, false},
		{Rule{Capacity: 5, Rate: 0}, false},
		{Rule{Capacity: 5, Rate: -1}, false},
		{Rule{Capacity: 5, Rate: 1_000_001}, false},
	}
	for _, tt := range tests {
		if err := tt.rule.Validate(); (err == nil) != tt.ok {
			t.Errorf("%+v.Validate() = %v, want ok=%v", tt.rule, err, tt.ok)
		}
	}
}

func TestKeyParts(t *testing.T) {
	if got := Key("join-ip", "203.0.113.7"); got != "rl:join-ip:203.0.113.7" {
		t.Fatalf("Key = %q", got)
	}
	for _, ok := range []string{"join-ip", "203.0.113.7", "2001_db8_1__", "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"} {
		if !validPart(ok) {
			t.Errorf("validPart(%q) = false, want true", ok)
		}
	}
	long := make([]byte, 129)
	for i := range long {
		long[i] = 'a'
	}
	for _, bad := range []string{"", "a:b", "x{y}", "has space", "new\nline", string(long)} {
		if validPart(bad) {
			t.Errorf("validPart(%q) = true, want false", bad)
		}
	}
}
