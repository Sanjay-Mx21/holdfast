// Command inventory runs inventory-svc: seat holds on the hot path.
//
// Ports: HTTP_ADDR serves the public API; ADMIN_ADDR (internal only) serves
// /metrics, /livez, /readyz, /buildz, /debug/pprof and operator endpoints.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
)

const serviceName = "inventory"

type config struct {
	Service cfgpkg.Service
	HTTP    cfgpkg.HTTP
	Valkey  cfgpkg.Valkey

	HoldTTL       time.Duration `env:"HOLD_TTL" envDefault:"5m"`
	PaymentWindow time.Duration `env:"PAYMENT_WINDOW" envDefault:"10m"`
	SweepInterval time.Duration `env:"SWEEP_INTERVAL" envDefault:"500ms"`
	SweepBatch    int           `env:"SWEEP_BATCH" envDefault:"500"`

	AdmissionPublicKeyFiles []string      `env:"ADMISSION_PUBLIC_KEY_FILES,required" envSeparator:","`
	TokenLeeway             time.Duration `env:"TOKEN_LEEWAY" envDefault:"5s"`
	AdminToken              string        `env:"ADMIN_TOKEN,required,unset"`
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP, c.Valkey)}
	if c.SweepInterval < 50*time.Millisecond {
		errs = append(errs, errors.New("SWEEP_INTERVAL must be at least 50ms"))
	}
	if c.SweepBatch < 1 || c.SweepBatch > 10_000 {
		errs = append(errs, errors.New("SWEEP_BATCH must be between 1 and 10000"))
	}
	if len(c.AdminToken) < 32 {
		errs = append(errs, errors.New("ADMIN_TOKEN must be at least 32 characters"))
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

	rdb, err := valkey.New(ctx, cfg.Valkey, serviceName)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	keys, err := authn.LoadPublicKeys(cfg.AdmissionPublicKeyFiles)
	if err != nil {
		return err
	}
	verifier := authn.NewVerifier(keys, cfg.TokenLeeway)

	store := inventory.NewStore(rdb)
	if err := store.LoadScripts(ctx); err != nil {
		return err
	}
	invMetrics := inventory.NewMetrics(reg)
	svc, err := inventory.NewService(store, inventory.Config{HoldTTL: cfg.HoldTTL, PaymentWindow: cfg.PaymentWindow}, invMetrics)
	if err != nil {
		return err
	}

	hc := health.New(2*time.Second, valkey.Check(rdb))
	httpMetrics := httpx.NewHTTPMetrics(reg)
	public := httpx.NewRouter(
		httpx.RequestID(),
		httpx.AccessLog(log, cfg.HTTP.AccessLogSuccess),
		httpMetrics.Middleware(),
		httpx.Recover(),
		httpx.SecurityHeaders(),
		httpx.BodyLimit(cfg.HTTP.MaxBodyBytes),
		httpx.Timeout(cfg.HTTP.RequestTimeout),
	)
	admin := httpx.NewAdminRouter(metrics.Handler(reg), hc)
	inventory.NewHandler(svc).Register(public, admin,
		authn.RequireAdmission(verifier),
		authn.RequireStaticToken(cfg.AdminToken),
	)

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay,
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log, httpx.WithWriteTimeout(90*time.Second)),
		inventory.NewSweeper(store, cfg.SweepInterval, cfg.SweepBatch, invMetrics, log),
	)
}
