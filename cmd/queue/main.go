// Command queue runs queue-svc: the waiting room and admission control.
//
// Ports: HTTP_ADDR serves the public API (joining the queue, positions);
// ADMIN_ADDR (internal only) serves /metrics, /livez, /readyz, /buildz,
// /debug/pprof and operator endpoints. Background work: the T0 opener and
// the admission controllers (one per event, leader-elected in PostgreSQL).
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/ratelimit"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
	"github.com/Sanjay-Mx21/holdfast/internal/queue"
)

const serviceName = "queue"

type config struct {
	Service  cfgpkg.Service
	HTTP     cfgpkg.HTTP
	Valkey   cfgpkg.Valkey
	Postgres cfgpkg.Postgres

	AdminToken string `env:"ADMIN_TOKEN,required,unset"`

	// DevIdentity trusts the X-Dev-User-Id header as the buyer's identity
	// until auth-svc exists (Phase 4). Never allowed in production.
	DevIdentity bool `env:"DEV_IDENTITY" envDefault:"false"`

	JoinIPBurst       int     `env:"JOIN_IP_BURST" envDefault:"30"`
	JoinIPPerSecond   float64 `env:"JOIN_IP_PER_SECOND" envDefault:"10"`
	JoinUserBurst     int     `env:"JOIN_USER_BURST" envDefault:"5"`
	JoinUserPerSecond float64 `env:"JOIN_USER_PER_SECOND" envDefault:"1"`

	PositionUserBurst     int     `env:"POSITION_USER_BURST" envDefault:"10"`
	PositionUserPerSecond float64 `env:"POSITION_USER_PER_SECOND" envDefault:"1"`
	AdmitUserBurst        int     `env:"ADMIT_USER_BURST" envDefault:"5"`
	AdmitUserPerSecond    float64 `env:"ADMIT_USER_PER_SECOND" envDefault:"1"`

	// Admission tokens: signed with the private key; the JWKS publishes its
	// public key plus any extra public keys (the old key during a rotation).
	AdmissionPrivateKeyFile      string        `env:"ADMISSION_PRIVATE_KEY_FILE,required"`
	AdmissionExtraPublicKeyFiles []string      `env:"ADMISSION_EXTRA_PUBLIC_KEY_FILES" envSeparator:","`
	AdmissionTokenTTL            time.Duration `env:"ADMISSION_TOKEN_TTL" envDefault:"10m"`

	// TrustedProxies are the addresses (CIDRs) of the edge: only requests
	// from them have their X-Forwarded-For believed for per-IP limits.
	TrustedProxies []string `env:"TRUSTED_PROXIES" envSeparator:","`

	// OpenCheckInterval is how often the opener looks for queues due to open
	// at T0. Joins open a due queue themselves, so this only bounds how long
	// the state can read PRE after T0 when nobody joins.
	OpenCheckInterval time.Duration `env:"OPEN_CHECK_INTERVAL" envDefault:"250ms"`

	// Admission control: the leader of each event admits every
	// ADMISSION_TICK; standbys retry leadership every LEADER_RETRY_INTERVAL;
	// new events are picked up every ADMISSION_RESCAN_INTERVAL.
	AdmissionTick           time.Duration `env:"ADMISSION_TICK" envDefault:"250ms"`
	LeaderRetryInterval     time.Duration `env:"LEADER_RETRY_INTERVAL" envDefault:"2s"`
	AdmissionRescanInterval time.Duration `env:"ADMISSION_RESCAN_INTERVAL" envDefault:"2s"`
}

func (c *config) trustedProxies() ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, s := range c.TrustedProxies {
		p, err := netip.ParsePrefix(strings.TrimSpace(s))
		if err != nil {
			return nil, fmt.Errorf("TRUSTED_PROXIES: %q is not a CIDR such as 10.250.0.10/32", s)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

func (c *config) limits() queue.Limits {
	return queue.Limits{
		JoinPerIP:       ratelimit.Rule{Capacity: c.JoinIPBurst, Rate: c.JoinIPPerSecond},
		JoinPerUser:     ratelimit.Rule{Capacity: c.JoinUserBurst, Rate: c.JoinUserPerSecond},
		PositionPerUser: ratelimit.Rule{Capacity: c.PositionUserBurst, Rate: c.PositionUserPerSecond},
		AdmitPerUser:    ratelimit.Rule{Capacity: c.AdmitUserBurst, Rate: c.AdmitUserPerSecond},
	}
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP, c.Valkey, c.Postgres)}
	if len(c.AdminToken) < 32 {
		errs = append(errs, errors.New("ADMIN_TOKEN must be at least 32 characters"))
	}
	if _, err := c.trustedProxies(); err != nil {
		errs = append(errs, err)
	}
	if c.OpenCheckInterval < 10*time.Millisecond || c.OpenCheckInterval > 10*time.Second {
		errs = append(errs, errors.New("OPEN_CHECK_INTERVAL must be between 10ms and 10s"))
	}
	if c.AdmissionTick < 10*time.Millisecond || c.AdmissionTick > 10*time.Second {
		errs = append(errs, errors.New("ADMISSION_TICK must be between 10ms and 10s"))
	}
	if c.LeaderRetryInterval < 100*time.Millisecond || c.LeaderRetryInterval > time.Minute {
		errs = append(errs, errors.New("LEADER_RETRY_INTERVAL must be between 100ms and 1m"))
	}
	if c.AdmissionRescanInterval < 100*time.Millisecond || c.AdmissionRescanInterval > time.Minute {
		errs = append(errs, errors.New("ADMISSION_RESCAN_INTERVAL must be between 100ms and 1m"))
	}
	if c.DevIdentity && c.Service.Environment == "production" {
		errs = append(errs, errors.New("DEV_IDENTITY must not be enabled in production: anyone could claim any user ID"))
	}
	l := c.limits()
	if err := l.JoinPerIP.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("JOIN_IP_BURST / JOIN_IP_PER_SECOND: %w", err))
	}
	if err := l.JoinPerUser.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("JOIN_USER_BURST / JOIN_USER_PER_SECOND: %w", err))
	}
	if err := l.PositionPerUser.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("POSITION_USER_BURST / POSITION_USER_PER_SECOND: %w", err))
	}
	if err := l.AdmitPerUser.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("ADMIT_USER_BURST / ADMIT_USER_PER_SECOND: %w", err))
	}
	if c.AdmissionTokenTTL < time.Minute || c.AdmissionTokenTTL > time.Hour {
		errs = append(errs, errors.New("ADMISSION_TOKEN_TTL must be between 1m and 1h"))
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

	// PostgreSQL elects the admission leaders (advisory locks). Readiness does
	// not depend on it: if it is down, admissions pause but joining and
	// positions keep working.
	pool, err := postgres.NewPool(ctx, cfg.Postgres, serviceName)
	if err != nil {
		return err
	}
	defer pool.Close()

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
	raw, err := os.ReadFile(filepath.Clean(cfg.AdmissionPrivateKeyFile))
	if err != nil {
		return fmt.Errorf("read admission private key: %w", err)
	}
	signingKey, err := authn.ParsePrivateKeyPEM(raw)
	if err != nil {
		return err
	}
	published := []ed25519.PublicKey{signingKey.Public().(ed25519.PublicKey)}
	if len(cfg.AdmissionExtraPublicKeyFiles) > 0 {
		extra, err := authn.LoadPublicKeys(cfg.AdmissionExtraPublicKeyFiles)
		if err != nil {
			return err
		}
		for _, k := range extra {
			published = append(published, k)
		}
	}
	log.Info("signing admission tokens", "kid", authn.KeyID(published[0]), "published_keys", len(published))

	identity := noIdentity
	if cfg.DevIdentity {
		log.Warn("DEV_IDENTITY is on: buyers are identified by the X-Dev-User-Id header, which anyone can set")
		identity = authn.RequireDevIdentity()
	}
	qm := queue.NewMetrics(reg)
	trusted, _ := cfg.trustedProxies() // validated at load
	if len(trusted) > 0 {
		log.Info("trusting X-Forwarded-For from the edge", "proxies", cfg.TrustedProxies)
	}
	queue.NewHandler(svc, lim, cfg.limits(), qm, authn.NewIssuer(signingKey, cfg.AdmissionTokenTTL), authn.NewJWKSet(published...)).
		TrustProxies(trusted...).
		Register(public, admin, identity, authn.RequireStaticToken(cfg.AdminToken))

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay,
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log, httpx.WithWriteTimeout(90*time.Second)),
		queue.NewOpener(store, cfg.OpenCheckInterval, qm, log),
		queue.NewAdmission(store, pool, queue.AdmissionConfig{
			Tick: cfg.AdmissionTick, RetryLeadership: cfg.LeaderRetryInterval, Rescan: cfg.AdmissionRescanInterval,
		}, qm, log),
	)
}

// noIdentity rejects every buyer request: without DEV_IDENTITY there is no
// way to authenticate buyers until auth-svc exists (Phase 4).
func noIdentity(http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "buyer authentication is not available yet"))
	})
}
