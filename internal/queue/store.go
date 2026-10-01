package queue

import (
	"context"
	"embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

func loadScript(name string) *redis.Script {
	src, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(fmt.Sprintf("queue: embedded script %s: %v", name, err))
	}
	return redis.NewScript(string(src))
}

var (
	provisionScript = loadScript("provision.lua")

	allScripts = []*redis.Script{provisionScript}
)

// Store is the Valkey-backed state of the waiting room. It runs the atomic
// scripts and translates their reply codes into domain values and errors; it
// holds no business policy (that lives in Service).
type Store struct {
	rdb redis.UniversalClient
}

// NewStore returns a Store backed by rdb.
func NewStore(rdb redis.UniversalClient) *Store { return &Store{rdb: rdb} }

// LoadScripts preloads every script (SCRIPT LOAD) so requests right after a
// deploy or failover go straight to EVALSHA. Script.Run already falls back to
// EVAL on NOSCRIPT, so this is an optimisation, not a correctness requirement.
func (s *Store) LoadScripts(ctx context.Context) error {
	for _, sc := range allScripts {
		if err := sc.Load(ctx, s.rdb).Err(); err != nil {
			return fmt.Errorf("queue: load script: %w", err)
		}
	}
	return nil
}

// Provision stores an event's queue settings exactly once and puts the queue
// in state PRE. It reports whether this call created them.
func (s *Store) Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error) {
	k := keysFor(eventID)
	code, err := provisionScript.Run(ctx, s.rdb, []string{k.config(), k.state()},
		cfg.OpensAt.UnixMilli(), cfg.AdmissionRate, cfg.MaxSessions, cfg.SessionTTL.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("queue: provision: %w", err)
	}
	if code < 0 {
		return false, ErrProvisionConflict
	}
	return code == 1, nil
}
