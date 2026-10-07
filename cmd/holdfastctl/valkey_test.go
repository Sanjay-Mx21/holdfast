package main

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestValkeyConfig(t *testing.T) {
	cfg, err := valkeyConfig("sentinel-1:26379,sentinel-2:26379", env(map[string]string{
		"VALKEY_SENTINEL_MASTER": "holdfast", "VALKEY_DB": "2", "VALKEY_PASSWORD": "pw",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.Addrs, []string{"sentinel-1:26379", "sentinel-2:26379"}) ||
		cfg.MasterName != "holdfast" || cfg.DB != 2 || cfg.Password != "pw" {
		t.Fatalf("config %+v", cfg)
	}

	// No master name: a standalone node, as before.
	cfg, err = valkeyConfig("localhost:6379", env(nil))
	if err != nil || cfg.MasterName != "" || cfg.DB != 0 {
		t.Fatalf("config %+v, err %v", cfg, err)
	}

	if _, err := valkeyConfig("localhost:6379", env(map[string]string{"VALKEY_DB": "-1"})); err == nil {
		t.Fatal("VALKEY_DB=-1 accepted")
	}
}

func TestRunID(t *testing.T) {
	info := "# Server\r\nvalkey_version:9.1.2\r\nrun_id:5d1c0a9f3b2e4d6c8a7f\r\ntcp_port:6379\r\n"
	if got := runID(info); got != "5d1c0a9f3b2e4d6c8a7f" {
		t.Fatalf("run ID %q", got)
	}
	if got := runID("# Server\r\n"); got != "unknown" {
		t.Fatalf("run ID %q", got)
	}
}

func TestProbe(t *testing.T) {
	var out bytes.Buffer
	start := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC)
	p := &probe{out: &out, start: start}
	at := func(ms int) time.Time { return start.Add(time.Duration(ms) * time.Millisecond) }
	down := errors.New("connection refused")

	p.observe(at(100), 1, "aaaaaaaaaaaa", nil)
	p.observe(at(200), 2, "aaaaaaaaaaaa", nil)
	p.observe(at(300), 0, "", down) // the primary dies
	p.observe(at(400), 0, "", down)
	p.observe(at(6300), 3, "bbbbbbbbbbbb", nil) // promoted replica: nothing lost
	p.observe(at(6400), 2, "bbbbbbbbbbbb", nil) // 2 and 3 vanished (counter back to 1)
	p.observe(at(6500), 5, "bbbbbbbbbbbb", nil) // a timed-out write applied: no loss
	p.observe(at(6600), 0, "", down)            // still failing at the end
	p.summary()

	if p.writes != 8 || p.failed != 3 || p.lost != 2 || len(p.outages) != 1 || p.outages[0] != 6*time.Second {
		t.Fatalf("probe %+v", p)
	}
	for _, want := range []string{
		"t+  0.30s writes failing: connection refused",
		"t+  6.30s writes resumed after 6s",
		"t+  6.30s now written to server bbbbbbbb (was aaaaaaaa)",
		"t+  6.40s 2 acknowledged writes lost (counter back from 3 to 2)",
		"8 writes, 3 failed, 1 outages, longest 6s, 2 acknowledged writes lost (writes still failing at the end)",
		"RESULT writes=8 failed=3 outages=1 longest_outage_ms=6000 lost=2 recovered=false",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if strings.Count(out.String(), "writes failing") != 2 {
		t.Errorf("each outage should be reported once:\n%s", out.String())
	}
}
