//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

// The probe against a real Valkey: every write succeeds, the counter only
// grows, and it leaves no key behind.
func TestValkeyProbe(t *testing.T) {
	rdb := testenv.Valkey(t)
	out, err := run(t, "valkey", "probe", "--valkey", os.Getenv(testenv.EnvValkeyAddr),
		"--for", "400ms", "--every", "20ms")
	if err != nil {
		t.Fatalf("probe: %v\n%s", err, out)
	}
	if !strings.Contains(out, "failed=0 outages=0 longest_outage_ms=0 lost=0 recovered=true") ||
		strings.Contains(out, "RESULT writes=0 ") {
		t.Fatalf("unexpected result:\n%s", out)
	}
	if strings.Contains(out, "writes failing") || strings.Contains(out, "now written to server") {
		t.Fatalf("a steady server reported a change:\n%s", out)
	}
	keys, err := rdb.Keys(context.Background(), "drill:probe:*").Result()
	if err != nil || len(keys) != 0 {
		t.Fatalf("probe keys left behind: %v (err %v)", keys, err)
	}
}
