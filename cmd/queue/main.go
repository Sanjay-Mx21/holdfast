// Command queue runs queue-svc: the waiting room and admission control.
//
// Ports: HTTP_ADDR serves the public API (joining the queue); ADMIN_ADDR
// (internal only) serves /metrics, /livez, /readyz, /buildz, /debug/pprof
// and operator endpoints.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/ratelimit"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
	"github.com/Sanjay-Mx21/holdfast/internal/queue"
)

const serviceName = "queue"

type config struct {
	Service cfgpkg.Service
	HTTP    cfgpkg.HTTP
	Valkey  cfgpkg.Valkey

	AdminToken string `env:"ADMIN_TOKEN,required,unset"`

	// DevIdentity trusts the X-Dev-User-Id header as the buyer's identity
	// until auth-svc exists (Phase 4). Never allowed in production.
	DevIdentity bool `env:"DEV_IDENTITY" envDefault:"false"`

	JoinIPBurst       int     `env:"JOIN_IP_BURST" envDefault:"30"`
	JoinIPPerSecond   float64 `env:"JOIN_IP_PER_SECOND" envDefault:"10"`
	JoinUserBurst     int     `env:"JOIN_USER_BURST" envDefault:"5"`
	JoinUserPerSecond float64 `env:"JOIN_USER_PER_SECOND" envDefault:"1"`

	// OpenCheckInterval is how often the opener looks for queues due to open
	// at T0. Joins open a due queue themselves, so this only bounds how long
	// the state can read PRE after T0 when nobody joins.
	OpenCheckInterval time.Duration `env:"OPEN_CHECK_INTERVAL" envDefault:"250ms"`
}

func (c *config) joinLimits() queue.JoinLimits {
	return queue.JoinLimits{
		PerIP:   ratelimit.Rule{Capacity: c.JoinIPBurst, Rate: c.JoinIPPerSecond},
		PerUser: ratelimit.Rule{Capacity: c.JoinUserBurst, Rate: c.JoinUserPerSecond},
	}
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP, c.Valkey)}
	if len(c.AdminToken) < 32 {
		errs = append(errs, errors.New("ADMIN_TOKEN must be at least 32 characters"))
	}
	if c.OpenCheckInterval < 10*time.Millisecond || c.OpenCheckInterval > 10*time.Second {
		errs = append(errs, errors.New("OPEN_CHECK_INTERVAL must be between 10ms and 10s"))
	}
	if c.DevIdentity && c.Service.Environment == "production" {
		errs = append(errs, errors.New("DEV_IDENTITY must not be enabled in production: anyone could claim any user ID"))
	}
	l := c.joinLimits()
	if err := l.PerIP.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("JOIN_IP_BURST / JOIN_IP_PER_SECOND: %w", err))
	}
	if err := l.PerUser.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("JOIN_USER_BURST / JOIN_USER_PER_SECOND: %w", err))
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

	store := queue.NewStore(rdb)
	if err := store.LoadScripts(ctx); err != nil {
		return err
	}
	svc := queue.NewService(store)
	lim := ratelimit.New(rdb)
	if err := lim.Load(ctx); err != nil {
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
	identity := noIdentity
	if cfg.DevIdentity {
		log.Warn("DEV_IDENTITY is on: buyers are identified by the X-Dev-User-Id header, which anyone can set")
		identity = authn.RequireDevIdentity()
	}
	qm := queue.NewMetrics(reg)
	queue.NewHandler(svc, lim, cfg.joinLimits(), qm).
		Register(public, admin, identity, authn.RequireStaticToken(cfg.AdminToken))

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay,
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log, httpx.WithWriteTimeout(90*time.Second)),
		queue.NewOpener(store, cfg.OpenCheckInterval, qm, log),
	)
}

// noIdentity rejects every buyer request: without DEV_IDENTITY there is no
// way to authenticate buyers until auth-svc exists (Phase 4).
func noIdentity(http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "buyer authentication is not available yet"))
	})
}
