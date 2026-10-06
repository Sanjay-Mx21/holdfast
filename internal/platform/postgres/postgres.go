// Package postgres builds pgx connection pools with production defaults and
// runs embedded schema migrations.
package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
)

// NewPool opens a pool and verifies connectivity. Every connection gets a
// statement timeout (runaway queries can't pin connections during a surge) and
// an idle-in-transaction timeout (a crashed client can't hold row locks).
func NewPool(ctx context.Context, cfg config.Postgres, appName string) (*pgxpool.Pool, error) {
	return newPool(ctx, cfg, appName, false)
}

// NewReadOnlyPool is NewPool for a reader that must never write (the
// invariant auditor): every transaction is read-only, so a write fails even
// when the role itself could write.
func NewReadOnlyPool(ctx context.Context, cfg config.Postgres, appName string) (*pgxpool.Pool, error) {
	return newPool(ctx, cfg, appName, true)
}

func newPool(ctx context.Context, cfg config.Postgres, appName string, readOnly bool) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	pc.MinConns = cfg.MinConns
	pc.MaxConnLifetime = cfg.MaxConnLifetime
	pc.MaxConnIdleTime = cfg.MaxConnIdleTime
	rp := pc.ConnConfig.RuntimeParams
	rp["application_name"] = appName
	rp["statement_timeout"] = strconv.FormatInt(cfg.StatementTimeout.Milliseconds(), 10)
	rp["idle_in_transaction_session_timeout"] = "30000"
	if readOnly {
		rp["default_transaction_read_only"] = "on"
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}

// Check returns a readiness check for pool.
func Check(pool *pgxpool.Pool) health.Check {
	return health.Check{Name: "postgres", Fn: pool.Ping}
}
