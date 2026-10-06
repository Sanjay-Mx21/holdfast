package testenv

import (
	"context"
	"io"
	"log/slog"
	"os"
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

// Isolated is a PostgreSQL database and a Valkey logical database that one
// test package owns alone, emptied at the start of each test. Tests that
// measure the whole store (the invariant auditor counts every violation in
// it) cannot share the common test database, where other packages' rows
// from this and earlier runs live.
type Isolated struct {
	// DSN of the isolated database, migrated with every schema.
	DSN string
	// Pool is a read-write pool on it, for the test to arrange data.
	Pool *pgxpool.Pool
	// Valkey is a client on the isolated logical database.
	Valkey redis.UniversalClient
}

// NewIsolated returns the isolated stores named by suffix (the database is
// the configured one plus suffix, holdfast_audit for "_audit") and valkeyDB.
// It holds a cross-process lock named suffix for the whole test, empties
// every table of every schema and flushes valkeyDB. Choose a valkeyDB no
// other package uses (the common tests use ValkeyDB, services 0).
func NewIsolated(t testing.TB, suffix string, valkeyDB int) Isolated {
	t.Helper()
	dsn := os.Getenv(EnvPostgresDSN)
	addr := os.Getenv(EnvValkeyAddr)
	if dsn == "" || addr == "" {
		t.Skipf("set %s and %s to run isolated integration tests", EnvPostgresDSN, EnvValkeyAddr)
	}
	ctx := context.Background()
	isoDSN, err := postgres.SiblingDatabase(ctx, dsn, suffix)
	if err != nil {
		t.Fatalf("isolated database: %v", err)
	}
	pool, err := postgres.NewPool(ctx, config.Postgres{
		DSN: isoDSN, MaxConns: 8, MinConns: 0, MaxConnLifetime: time.Hour,
		MaxConnIdleTime: time.Minute, StatementTimeout: 30 * time.Second,
	}, "holdfast-test-isolated")
	if err != nil {
		t.Fatalf("connect to the isolated database: %v", err)
	}
	t.Cleanup(pool.Close)
	Exclusive(t, pool, "isolated"+suffix)

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, s := range migrations.All() {
		if _, err := postgres.Migrate(ctx, pool, s.Name, s.Files, quiet); err != nil {
			t.Fatalf("migrate the isolated database: %v", err)
		}
	}
	// Empty every table except the migration bookkeeping.
	rows, err := pool.Query(ctx, `
		SELECT schemaname, tablename FROM pg_tables
		WHERE schemaname IN (SELECT unnest($1::text[])) AND tablename <> 'schema_migrations'`, schemaNames())
	if err != nil {
		t.Fatal(err)
	}
	tables, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (string, error) {
		var schema, table string
		err := r.Scan(&schema, &table)
		return pgx.Identifier{schema, table}.Sanitize(), err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		if _, err := pool.Exec(ctx, "TRUNCATE "+tbl+" CASCADE"); err != nil {
			t.Fatalf("empty %s: %v", tbl, err)
		}
	}

	rdb, err := valkey.New(ctx, config.Valkey{
		Addrs: []string{addr}, DB: valkeyDB, PoolSize: 16,
		DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
	}, "holdfast-test-isolated")
	if err != nil {
		t.Fatalf("connect to Valkey: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush Valkey database %d: %v", valkeyDB, err)
	}
	return Isolated{DSN: isoDSN, Pool: pool, Valkey: rdb}
}

func schemaNames() []string {
	var names []string
	for _, s := range migrations.All() {
		names = append(names, s.Name)
	}
	return names
}
