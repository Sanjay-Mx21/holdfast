package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Migration files follow goose conventions ("00001_name.sql" with
// "-- +goose Up" and "-- +goose Down" markers), so the goose CLI remains a
// drop-in alternative to this runner.
var (
	fileRe   = regexp.MustCompile(`^(\d{5})_([a-z0-9_]+)\.sql$`)
	schemaRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

const (
	upMarker   = "-- +goose Up"
	downMarker = "-- +goose Down"
)

type migration struct {
	version  int64
	name     string
	up       string
	checksum string
}

// ErrChecksumMismatch means an already-applied migration file was edited.
// Applied migrations are immutable: add a new migration instead.
var ErrChecksumMismatch = errors.New("postgres: applied migration was modified")

// Migrate applies the pending migrations in files to schema, in version order.
//
//   - Concurrent runners (several replicas starting at once) are serialised
//     by a PostgreSQL advisory lock.
//   - Each migration runs in its own transaction together with its
//     bookkeeping row, so a failure leaves no half-applied state.
//   - Editing an applied migration fails loudly (ErrChecksumMismatch).
func Migrate(ctx context.Context, pool *pgxpool.Pool, schema string, files fs.FS, log *slog.Logger) (int, error) {
	if !schemaRe.MatchString(schema) {
		return 0, fmt.Errorf("postgres: invalid schema name %q", schema)
	}
	migrations, err := load(files)
	if err != nil {
		return 0, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: acquire: %w", err)
	}
	defer conn.Release()

	lockKey := "holdfast.migrate." + schema
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext($1))", lockKey); err != nil {
		return 0, fmt.Errorf("postgres: migration lock: %w", err)
	}
	defer func() {
		// Unlock before the connection returns to the pool; session-level
		// advisory locks otherwise outlive this function.
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(hashtext($1))", lockKey)
	}()

	table := pgx.Identifier{schema, "schema_migrations"}.Sanitize()
	bootstrap := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;
CREATE TABLE IF NOT EXISTS %s (
  version    bigint      PRIMARY KEY,
  name       text        NOT NULL,
  checksum   text        NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);`, pgx.Identifier{schema}.Sanitize(), table)
	if _, err := conn.Exec(ctx, bootstrap); err != nil {
		return 0, fmt.Errorf("postgres: bootstrap migrations table: %w", err)
	}

	applied := map[int64]string{}
	rows, err := conn.Query(ctx, "SELECT version, checksum FROM "+table)
	if err != nil {
		return 0, fmt.Errorf("postgres: read applied migrations: %w", err)
	}
	for rows.Next() {
		var v int64
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			rows.Close()
			return 0, err
		}
		applied[v] = sum
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	count := 0
	for _, m := range migrations {
		if sum, ok := applied[m.version]; ok {
			if sum != m.checksum {
				return count, fmt.Errorf("%w: %s version %d (%s)", ErrChecksumMismatch, schema, m.version, m.name)
			}
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.up); err != nil {
				return fmt.Errorf("apply: %w", err)
			}
			_, err := tx.Exec(ctx, "INSERT INTO "+table+" (version, name, checksum) VALUES ($1, $2, $3)",
				m.version, m.name, m.checksum)
			return err
		})
		if err != nil {
			return count, fmt.Errorf("postgres: migration %s/%05d_%s: %w", schema, m.version, m.name, err)
		}
		count++
		log.Info("migration applied", "schema", schema, "version", m.version, "name", m.name)
	}
	return count, nil
}

func load(files fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("postgres: read migrations: %w", err)
	}
	var out []migration
	seen := map[int64]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		match := fileRe.FindStringSubmatch(e.Name())
		if match == nil {
			return nil, fmt.Errorf("postgres: migration %q must be named NNNNN_snake_case.sql", e.Name())
		}
		version, _ := strconv.ParseInt(match[1], 10, 64)
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("postgres: duplicate migration version %d (%s, %s)", version, prev, e.Name())
		}
		seen[version] = e.Name()

		raw, err := fs.ReadFile(files, e.Name())
		if err != nil {
			return nil, err
		}
		text := string(raw)
		start := strings.Index(text, upMarker)
		if start < 0 {
			return nil, fmt.Errorf("postgres: migration %q has no %q marker", e.Name(), upMarker)
		}
		up := text[start+len(upMarker):]
		if end := strings.Index(up, downMarker); end >= 0 {
			up = up[:end]
		}
		sum := sha256.Sum256(raw)
		out = append(out, migration{version: version, name: match[2], up: up, checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}
