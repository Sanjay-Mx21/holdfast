//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func freshSchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	schema := "migtest_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	return schema
}

func file(sql string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(sql)} }

func TestMigrateAppliesOnceAndDetectsEdits(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx := context.Background()
	s := freshSchema(t, pool)
	files := fstest.MapFS{
		"00001_create_widgets.sql": file("-- +goose Up\nCREATE TABLE " + s + ".widgets (id int PRIMARY KEY);\n-- +goose Down\nDROP TABLE " + s + ".widgets;\n"),
		"00002_add_name.sql":       file("-- +goose Up\nALTER TABLE " + s + ".widgets ADD COLUMN name text;\n"),
	}
	if n, err := postgres.Migrate(ctx, pool, s, files, quiet); err != nil || n != 2 {
		t.Fatalf("first run: n=%d err=%v", n, err)
	}
	if n, err := postgres.Migrate(ctx, pool, s, files, quiet); err != nil || n != 0 {
		t.Fatalf("second run must be a no-op: n=%d err=%v", n, err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+s+".widgets (id, name) VALUES (1, 'a')"); err != nil {
		t.Fatalf("schema not as expected (Down section must never run): %v", err)
	}
	files["00001_create_widgets.sql"] = file("-- +goose Up\nCREATE TABLE " + s + ".widgets (id bigint PRIMARY KEY);\n")
	if _, err := postgres.Migrate(ctx, pool, s, files, quiet); !errors.Is(err, postgres.ErrChecksumMismatch) {
		t.Fatalf("edited migration: got %v, want ErrChecksumMismatch", err)
	}
}

func TestFailedMigrationLeavesNoTrace(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx := context.Background()
	s := freshSchema(t, pool)
	files := fstest.MapFS{
		"00001_broken.sql": file("-- +goose Up\nCREATE TABLE " + s + ".half (id int);\nSELEC 1;\n"),
	}
	if _, err := postgres.Migrate(ctx, pool, s, files, quiet); err == nil {
		t.Fatal("want an error from invalid SQL")
	}
	var table *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::text", s+".half").Scan(&table); err != nil {
		t.Fatal(err)
	}
	var applied int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+s+".schema_migrations").Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if table != nil || applied != 0 {
		t.Fatalf("partial state left behind: table=%v applied=%d", table, applied)
	}
}

func TestMigrateRejectsMalformedInput(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx := context.Background()
	up := "-- +goose Up\nSELECT 1;\n"
	cases := map[string]struct {
		schema string
		files  fstest.MapFS
	}{
		"bad file name":     {"migtest_ok", fstest.MapFS{"1_init.sql": file(up)}},
		"duplicate version": {"migtest_ok", fstest.MapFS{"00001_a.sql": file(up), "00001_b.sql": file(up)}},
		"missing up marker": {"migtest_ok", fstest.MapFS{"00001_a.sql": file("SELECT 1;")}},
		"bad schema name":   {"Bad-Schema", fstest.MapFS{"00001_a.sql": file(up)}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := postgres.Migrate(ctx, pool, tc.schema, tc.files, quiet); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

func TestConcurrentRunnersApplyEachMigrationExactlyOnce(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx := context.Background()
	s := freshSchema(t, pool)
	files := fstest.MapFS{
		"00001_a.sql": file("-- +goose Up\nCREATE TABLE " + s + ".a (id int);\n"),
		"00002_b.sql": file("-- +goose Up\nCREATE TABLE " + s + ".b (id int);\n"),
		"00003_c.sql": file("-- +goose Up\nCREATE TABLE " + s + ".c (id int);\n"),
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := postgres.Migrate(ctx, pool, s, files, quiet)
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent runner failed: %v", err)
	}
	if total != 3 {
		t.Fatalf("migrations applied %d times in total, want 3", total)
	}
}
