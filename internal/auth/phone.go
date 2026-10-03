package auth

import (
	"errors"
	"strings"
)

// ErrInvalidPhone means the number is not a phone number HoldFast accepts.
var ErrInvalidPhone = errors.New("auth: invalid phone number")

// NormalizePhone returns the number in E.164 form ("+919876543210"). It
// accepts E.164 with spaces, dashes or dots, and a bare 10-digit Indian
// mobile number (first digit 6 to 9), which gets +91.
func NormalizePhone(raw string) (string, error) {
	var b strings.Builder
	for i, r := range strings.TrimSpace(raw) {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '+' && i == 0:
			b.WriteRune(r)
		case r == ' ' || r == '-' || r == '.' || r == '(' || r == ')':
		default:
			return "", ErrInvalidPhone
		}
	}
	s := b.String()
	if !strings.HasPrefix(s, "+") {
		if len(s) == 10 && s[0] >= '6' && s[0] <= '9' {
			return "+91" + s, nil
		}
		return "", ErrInvalidPhone
	}
	digits := s[1:]
	// E.164: at most 15 digits, no leading zero in the country code.
	if len(digits) < 8 || len(digits) > 15 || digits[0] == '0' {
		return "", ErrInvalidPhone
	}
	return s, nil
}

// last4 is what HoldFast keeps of the number in the clear, for display.
func last4(e164 string) string { return e164[len(e164)-4:] }
