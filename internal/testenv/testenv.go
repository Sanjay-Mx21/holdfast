// Package testenv connects integration tests to real Valkey and PostgreSQL.
//
// Valkey and Postgres skip the calling test when their environment variable
// is unset, so a plain `go test ./...` stays hermetic, while `make itest` and
// CI (which start the dependencies) run the full suite.
package testenv

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/db/migrations"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
)

// Environment variables that enable integration tests.
const (
	EnvValkeyAddr  = "HOLDFAST_TEST_VALKEY_ADDR"
	EnvPostgresDSN = "HOLDFAST_TEST_POSTGRES_DSN"
)

// Valkey returns a client for the test Valkey, closed when the test ends.
func Valkey(t testing.TB) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv(EnvValkeyAddr)
	if addr == "" {
		t.Skipf("set %s to run Valkey integration tests", EnvValkeyAddr)
	}
	c, err := valkey.New(context.Background(), config.Valkey{
		Addrs: []string{addr}, PoolSize: 256,
		DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
	}, "holdfast-test")
	if err != nil {
		t.Fatalf("connect to Valkey at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

var (
	migrateOnce sync.Once
	migrateErr  error
)

// Postgres returns a pool for the test database with every schema migrated
// (once per test process). The pool is closed when the test ends.
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(EnvPostgresDSN)
	if dsn == "" {
		t.Skipf("set %s to run PostgreSQL integration tests", EnvPostgresDSN)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, config.Postgres{
		DSN: dsn, MaxConns: 32, MinConns: 0, MaxConnLifetime: time.Hour,
		MaxConnIdleTime: time.Minute, StatementTimeout: 30 * time.Second,
	}, "holdfast-test")
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	migrateOnce.Do(func() {
		quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
		for _, s := range migrations.All() {
			if _, err := postgres.Migrate(ctx, pool, s.Name, s.Files, quiet); err != nil {
				migrateErr = err
				return
			}
		}
	})
	if migrateErr != nil {
		t.Fatalf("migrate test database: %v", migrateErr)
	}
	return pool
}
