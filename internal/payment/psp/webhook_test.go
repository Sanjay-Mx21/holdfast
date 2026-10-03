package psp

import (
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestVerify(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Unix(1_790_000_000, 0)
	ts := strconv.FormatInt(now.Unix(), 10)
	body := []byte(`{"id":"evt_1","type":"payment.captured"}`)
	sig := Sign(secret, ts, body)
	if err := Verify(secret, ts, sig, body, now, 5*time.Minute); err != nil {
		t.Fatalf("a good signature: %v", err)
	}
	old := strconv.FormatInt(now.Add(-6*time.Minute).Unix(), 10)
	future := strconv.FormatInt(now.Add(6*time.Minute).Unix(), 10)
	cases := map[string]struct {
		secret        []byte
		ts, sig, body string
	}{
		"tampered body":                 {secret, ts, sig, `{"id":"evt_1","type":"payment.failed"}`},
		"wrong secret":                  {[]byte("another-secret-another-secret-xx"), ts, sig, string(body)},
		"replayed with a new timestamp": {secret, strconv.FormatInt(now.Unix()+1, 10), sig, string(body)},
		"too old":                       {secret, old, Sign(secret, old, body), string(body)},
		"from the future":               {secret, future, Sign(secret, future, body), string(body)},
		"no timestamp":                  {secret, "", sig, string(body)},
		"not hex":                       {secret, ts, "zz" + sig[2:], string(body)},
		"no signature":                  {secret, ts, "", string(body)},
	}
	for name, c := range cases {
		if err := Verify(c.secret, c.ts, c.sig, []byte(c.body), now, 5*time.Minute); !errors.Is(err, ErrBadSignature) {
			t.Errorf("%s: %v, want ErrBadSignature", name, err)
		}
	}
}
