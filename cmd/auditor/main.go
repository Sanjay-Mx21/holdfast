// Command auditor continuously checks HoldFast's invariants I1 to I5 from
// the stores and publishes holdfast_invariant_violations{invariant}
// (internal/auditor). It has no public API: its admin port (ADMIN_ADDR)
// serves /metrics, /livez, /readyz, /buildz and pprof. Its PostgreSQL
// connections are read-only, and it only reads inventory's Valkey keys.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/auditor"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
)

const serviceName = "auditor"

type config struct {
	Service  cfgpkg.Service
	HTTP     cfgpkg.HTTP
	Postgres cfgpkg.Postgres
	Valkey   cfgpkg.Valkey

	// AUDIT_INTERVAL between checks; MONEY_DEADLINE for I3 (design: 15
	// minutes); HOLD_GRACE before an expired, unreleased hold counts as
	// lost (I5).
	Interval      time.Duration `env:"AUDIT_INTERVAL" envDefault:"15s"`
	MoneyDeadline time.Duration `env:"MONEY_DEADLINE" envDefault:"15m"`
	HoldGrace     time.Duration `env:"HOLD_GRACE" envDefault:"5m"`
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP, c.Postgres, c.Valkey)}
	if c.Interval < time.Second || c.Interval > 10*time.Minute {
		errs = append(errs, errors.New("AUDIT_INTERVAL must be between 1s and 10m"))
	}
	if c.MoneyDeadline < time.Minute || c.HoldGrace < 10*time.Second {
		errs = append(errs, errors.New("MONEY_DEADLINE must be at least 1m and HOLD_GRACE at least 10s"))
	}
	return errors.Join(errs...)
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, serviceName+":", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := cfgpkg.Load[config]()
	if err != nil {
		return err
	}
	log, err := logging.New(os.Stdout, cfg.Service.LogLevel, cfg.Service.LogFormat, serviceName, cfg.Service.Environment)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	bi := buildinfo.Get()
	log.Info("starting", "commit", bi.Commit, "go_version", bi.GoVersion)

	reg := metrics.NewRegistry(serviceName)
	db, err := postgres.NewReadOnlyPool(ctx, cfg.Postgres, serviceName)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb, err := valkey.New(ctx, cfg.Valkey, serviceName)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	hc := health.New(2*time.Second, postgres.Check(db), valkey.Check(rdb))
	admin := httpx.NewAdminRouter(metrics.Handler(reg), hc)
	a := auditor.New(db, rdb, auditor.Config{
		Interval: cfg.Interval, MoneyDeadline: cfg.MoneyDeadline, HoldGrace: cfg.HoldGrace,
	}, auditor.NewMetrics(reg), log)

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay,
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log),
		a,
	)
}
