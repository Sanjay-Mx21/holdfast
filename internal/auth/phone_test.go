package auth

import (
	"errors"
	"testing"
)

func TestNormalizePhone(t *testing.T) {
	for in, want := range map[string]string{
		"+919876543210":     "+919876543210",
		"+91 98765-43210":   "+919876543210",
		"9876543210":        "+919876543210",
		" 98765 43210 ":     "+919876543210",
		"+1 (415) 555.0100": "+14155550100",
		"+447911123456":     "+447911123456",
	} {
		got, err := NormalizePhone(in)
		if err != nil || got != want {
			t.Errorf("NormalizePhone(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{
		"", "12345", "5876543210", // a 10-digit number that is not an Indian mobile
		"+0123456789", "+12", "+1234567890123456", "98765abc10", "91+9876543210", "++919876543210",
	} {
		if got, err := NormalizePhone(bad); !errors.Is(err, ErrInvalidPhone) {
			t.Errorf("NormalizePhone(%q) = %q, %v; want ErrInvalidPhone", bad, got, err)
		}
	}
	if l := last4("+919876543210"); l != "3210" {
		t.Fatalf("last4 = %q", l)
	}
}

func TestValidCode(t *testing.T) {
	for code, want := range map[string]bool{"123456": true, "000000": true, "12345": false, "1234567": false, "12a456": false, "": false} {
		if validCode(code) != want {
			t.Errorf("validCode(%q) = %v", code, !want)
		}
	}
	for range 100 {
		c, err := sixDigits()
		if err != nil || !validCode(c) {
			t.Fatalf("sixDigits() = %q, %v", c, err)
		}
	}
}
