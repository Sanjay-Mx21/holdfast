//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/queue"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

// run runs one holdfastctl command and returns what it printed.
func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	t.Setenv("VALKEY_DB", strconv.Itoa(testenv.ValkeyDB)) // where testenv.Valkey looks
	var out bytes.Buffer
	stdout = &out
	t.Cleanup(func() { stdout = os.Stdout })
	err := dispatch(context.Background(), args)
	return out.String(), err
}

func TestQueueProvisionAndStatus(t *testing.T) {
	rdb := testenv.Valkey(t)
	pool := testenv.Postgres(t)
	ctx := context.Background()
	valkeyAddr := os.Getenv(testenv.EnvValkeyAddr)
	dsn := os.Getenv(testenv.EnvPostgresDSN)
	store := queue.NewStore(rdb)

	// An event in the catalog, its queue not provisioned (as with --no-provision).
	opensAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	id, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "holdfastctl queue test", SaleOpensAt: opensAt, PerUserLimit: 4, UnitPricePaise: 100, Capacity: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	ev := id.String()
	t.Cleanup(func() { _ = store.Purge(context.Background(), ev) })

	if _, err := run(t, "queue", "status", "--valkey", valkeyAddr, "--event", ev); !errors.Is(err, queue.ErrEventNotFound) {
		t.Fatalf("status before provisioning: %v, want ErrEventNotFound", err)
	}

	// The opening time comes from PostgreSQL.
	out, err := run(t, "queue", "provision", "--valkey", valkeyAddr, "--dsn", dsn, "--event", ev,
		"--admission-rate", "50", "--max-sessions", "200", "--session-ttl", "5m")
	if err != nil || !strings.Contains(out, "queue provisioned: opens "+opensAt.Format(time.RFC3339)) {
		t.Fatalf("provision: %q %v", out, err)
	}
	// Again with the same settings: nothing to do.
	out, err = run(t, "queue", "provision", "--valkey", valkeyAddr, "--dsn", dsn, "--event", ev,
		"--admission-rate", "50", "--max-sessions", "200", "--session-ttl", "5m")
	if err != nil || !strings.Contains(out, "nothing to do") {
		t.Fatalf("second provision: %q %v", out, err)
	}
	// Different settings: refused, with a pointer to queue status.
	_, err = run(t, "queue", "provision", "--valkey", valkeyAddr, "--dsn", dsn, "--event", ev, "--admission-rate", "60")
	if !errors.Is(err, queue.ErrProvisionConflict) || !strings.Contains(err.Error(), "queue status") {
		t.Fatalf("conflicting provision: %v, want ErrProvisionConflict", err)
	}

	// The stack's queue-svc, if running, may start leading the event at any
	// moment, so only fields a leader cannot change are checked here; the
	// leader line is covered by TestPrintQueueStatus.
	out, err = run(t, "queue", "status", "--valkey", valkeyAddr, "--event", ev)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"PRE, opens " + opensAt.Format(time.RFC3339), "0 in line", "0 active of 200; 50 admissions/s; sessions last 5m0s",
		"work list   yes"} {
		if !strings.Contains(out, want) {
			t.Errorf("status output lacks %q:\n%s", want, out)
		}
	}

	out, err = run(t, "queue", "status", "--valkey", valkeyAddr, "--event", ev, "--json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		EventID       string `json:"eventId"`
		State         string `json:"state"`
		AdmissionRate int    `json:"admissionRate"`
		MaxSessions   int    `json:"maxSessions"`
		SessionTTL    string `json:"sessionTtl"`
		OnWorkList    bool   `json:"onWorkList"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("status --json: %v\n%s", err, out)
	}
	if doc.EventID != ev || doc.State != "PRE" || doc.AdmissionRate != 50 || doc.MaxSessions != 200 ||
		doc.SessionTTL != "5m0s" || !doc.OnWorkList {
		t.Fatalf("status --json = %+v", doc)
	}
}

func TestQueueProvisionNeedsAnOpeningTime(t *testing.T) {
	rdb := testenv.Valkey(t)
	valkeyAddr := os.Getenv(testenv.EnvValkeyAddr)
	ev := uuid.Must(uuid.NewV7()).String()
	t.Cleanup(func() { _ = queue.NewStore(rdb).Purge(context.Background(), ev) })

	t.Setenv("POSTGRES_DSN", "")
	if _, err := run(t, "queue", "provision", "--valkey", valkeyAddr, "--event", ev); err == nil || !strings.Contains(err.Error(), "--opens-at") {
		t.Fatalf("without a DSN or --opens-at: %v", err)
	}
	if _, err := run(t, "queue", "provision", "--valkey", valkeyAddr, "--event", "nope", "--opens-at", "2026-10-05T12:00:00Z"); err == nil {
		t.Fatal("a bad event ID was accepted")
	}
	if _, err := run(t, "queue", "provision", "--valkey", valkeyAddr, "--event", ev, "--opens-at", "2026-10-05T12:00:00Z", "--admission-rate", "0"); !errors.Is(err, queue.ErrInvalidRequest) {
		t.Fatalf("a zero admission rate: %v, want ErrInvalidRequest", err)
	}
	// With an explicit opening time no database is needed.
	out, err := run(t, "queue", "provision", "--valkey", valkeyAddr, "--event", ev, "--opens-at", "2026-10-05T12:00:00Z")
	if err != nil || !strings.Contains(out, "queue provisioned: opens 2026-10-05T12:00:00Z, 83 admissions/s") {
		t.Fatalf("provision with --opens-at: %q %v", out, err)
	}
}
