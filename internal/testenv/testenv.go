// Package testenv connects integration tests to real Valkey and PostgreSQL.
//
// Valkey and Postgres skip the calling test when their environment variable
// is unset, so a plain `go test ./...` stays hermetic, while `make itest` and
// CI (which start the dependencies) run the full suite.
package testenv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	createOnce.Do(func() { testDSN, createErr = ensureTestDatabase(dsn) })
	if createErr != nil {
		t.Fatalf("test database: %v", createErr)
	}
	return testDSN
}

// ensureTestDatabase derives the test database's DSN from dsn and creates
// the database if it is missing.
func ensureTestDatabase(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return "", fmt.Errorf("%s must be a postgres:// URL", EnvPostgresDSN)
	}
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return "", fmt.Errorf("%s must name a database", EnvPostgresDSN)
	}
	if !strings.HasSuffix(name, "_test") {
		name += "_test"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// Test packages run as parallel processes: one creates, the rest wait.
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext('holdfast.testenv.createdb'))"); err != nil {
		return "", fmt.Errorf("lock: %w", err)
	}
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			return "", fmt.Errorf("create database %s: %w", name, err)
		}
	}
	u.Path = "/" + name
	return u.String(), nil
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
