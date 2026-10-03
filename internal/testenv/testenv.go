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
	"strings"
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
	EnvValkeyAddr   = "HOLDFAST_TEST_VALKEY_ADDR"
	EnvPostgresDSN  = "HOLDFAST_TEST_POSTGRES_DSN"
	EnvKafkaBrokers = "HOLDFAST_TEST_KAFKA_BROKERS"
)

// ValkeyDB is the logical database integration tests use. Services started
// by `make up` use database 0, so they never see the tests' keys: inventory-
// svc's sweeper, for one, would otherwise release the tests' holds (P7).
const ValkeyDB = 1

// Valkey returns a client for the test Valkey's database ValkeyDB, closed
// when the test ends.
func Valkey(t testing.TB) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv(EnvValkeyAddr)
	if addr == "" {
		t.Skipf("set %s to run Valkey integration tests", EnvValkeyAddr)
	}
	c, err := valkey.New(context.Background(), config.Valkey{
		Addrs: []string{addr}, DB: ValkeyDB, PoolSize: 256,
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
	createOnce  sync.Once
	testDSN     string
	createErr   error
)

// PostgresDSN returns the DSN of the test database: the database named in
// HOLDFAST_TEST_POSTGRES_DSN with "_test" appended (holdfast becomes
// holdfast_test), created if it does not exist. Services started by
// `make up` use the database itself, so their outbox relays and pollers
// never see the tests' rows, and the tests never see theirs (P30, P35).
func PostgresDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv(EnvPostgresDSN)
	if dsn == "" {
		t.Skipf("set %s to run PostgreSQL integration tests", EnvPostgresDSN)
	}
	createOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		testDSN, createErr = postgres.SiblingDatabase(ctx, dsn, "_test")
	})
	if createErr != nil {
		t.Fatalf("test database: %v", createErr)
	}
	return testDSN
}

// Postgres returns a pool for the test database (PostgresDSN) with every
// schema migrated (once per test process). The pool is closed when the test
// ends.
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := PostgresDSN(t)
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

// Exclusive holds a lock named name, across test processes, until the test
// ends. Tests that claim from a shared queue (overdue bookings, open
// intents) take it: the claims take every eligible row, so two packages
// claiming at once would lock or move each other's rows (P36).
func Exclusive(t testing.TB, pool *pgxpool.Pool, name string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("exclusive %s: %v", name, err)
	}
	key := "holdfast.testenv." + name
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext($1))", key); err != nil {
		conn.Release()
		t.Fatalf("exclusive %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", key)
		conn.Release()
	})
}

// Kafka returns the client settings for the test Kafka cluster.
func Kafka(t testing.TB) config.Kafka {
	t.Helper()
	brokers := os.Getenv(EnvKafkaBrokers)
	if brokers == "" {
		t.Skipf("set %s to run Kafka integration tests", EnvKafkaBrokers)
	}
	return config.Kafka{
		Brokers: strings.Split(brokers, ","), DialTimeout: 5 * time.Second, DeliveryTimeout: 30 * time.Second,
	}
}
