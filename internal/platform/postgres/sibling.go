package postgres

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
)

// SiblingDatabase returns the DSN of the database named like dsn's plus
// suffix (holdfast becomes holdfast_test), on the same server, creating it
// if it does not exist. A name that already ends in suffix is kept.
// Integration tests and experiments use one, so a running stack's pollers,
// relays and consumers never see their rows, and they never see the stack's.
// Concurrent callers are serialised with an advisory lock.
func SiblingDatabase(ctx context.Context, dsn, suffix string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return "", fmt.Errorf("postgres: %q is not a postgres:// URL", redact(dsn))
	}
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return "", fmt.Errorf("postgres: %q names no database", redact(dsn))
	}
	if !strings.HasSuffix(name, suffix) {
		name += suffix
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", fmt.Errorf("postgres: connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext('holdfast.postgres.createdb'))"); err != nil {
		return "", fmt.Errorf("postgres: lock: %w", err)
	}
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			return "", fmt.Errorf("postgres: create database %s: %w", name, err)
		}
	}
	u.Path = "/" + name
	return u.String(), nil
}

// redact hides a DSN's password.
func redact(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if _, ok := u.User.Password(); ok {
			u.User = url.UserPassword(u.User.Username(), "xxxxx")
		}
		return u.String()
	}
	return "<dsn>"
}
