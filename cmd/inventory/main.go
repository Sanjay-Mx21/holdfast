// Command inventory runs inventory-svc: seat holds on the hot path.
//
// Ports: HTTP_ADDR serves the public API; ADMIN_ADDR (internal only) serves
// /metrics, /livez, /readyz, /buildz, /debug/pprof and operator endpoints.
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	inventoryv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/inventory/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/grpcx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
	hfotel "github.com/Sanjay-Mx21/holdfast/internal/platform/otel"
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

	// Trusted admission-token keys: queue-svc's published key set
	// (ADMISSION_JWKS_URL), key files, or both. At least one is required.
	AdmissionPublicKeyFiles []string      `env:"ADMISSION_PUBLIC_KEY_FILES" envSeparator:","`
	AdmissionJWKSURL        string        `env:"ADMISSION_JWKS_URL"`
	JWKSRefreshInterval     time.Duration `env:"JWKS_REFRESH_INTERVAL" envDefault:"5m"`
	JWKSMinRefreshInterval  time.Duration `env:"JWKS_MIN_REFRESH_INTERVAL" envDefault:"30s"`
	TokenLeeway             time.Duration `env:"TOKEN_LEEWAY" envDefault:"5s"`

	// The internal gRPC API for booking-svc (proto/holdfast/inventory/v1),
	// on the internal network only. Callers authenticate with service tokens
	// signed by their own keys: GRPC_TRUSTED_CALLERS lists them as
	// "booking=/keys/booking.pub". With none, every call is refused.
	GRPCAddr           string   `env:"GRPC_ADDR" envDefault:":7070"`
	GRPCTrustedCallers []string `env:"GRPC_TRUSTED_CALLERS" envSeparator:","`
	AdminToken         string   `env:"ADMIN_TOKEN,required,unset"`
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
	if len(c.AdmissionPublicKeyFiles) == 0 && c.AdmissionJWKSURL == "" {
		errs = append(errs, errors.New("set ADMISSION_JWKS_URL, ADMISSION_PUBLIC_KEY_FILES, or both"))
	}
	if c.AdmissionJWKSURL != "" {
		if u, err := url.Parse(c.AdmissionJWKSURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, errors.New("ADMISSION_JWKS_URL must be an http(s) URL"))
		}
	}
	if c.JWKSMinRefreshInterval < time.Second || c.JWKSRefreshInterval < c.JWKSMinRefreshInterval {
		errs = append(errs, errors.New("JWKS_MIN_REFRESH_INTERVAL must be at least 1s and no more than JWKS_REFRESH_INTERVAL"))
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

	shutdownTracing, exporting, err := hfotel.Setup(ctx, serviceName, bi.Version, cfg.Service.Environment)
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx) // flush buffered spans
	}()
	log.Info("tracing", "exporting", exporting)

	reg := metrics.NewRegistry(serviceName)

	rdb, err := valkey.New(ctx, cfg.Valkey, serviceName)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	var static map[string]ed25519.PublicKey
	if len(cfg.AdmissionPublicKeyFiles) > 0 {
		if static, err = authn.LoadPublicKeys(cfg.AdmissionPublicKeyFiles); err != nil {
			return err
		}
	}
	checks := []health.Check{valkey.Check(rdb)}
	var background []app.Component
	verifier := authn.NewVerifier(static, cfg.TokenLeeway)
	if cfg.AdmissionJWKSURL != "" {
		jwks := authn.NewJWKSClient(authn.JWKSOptions{
			URL: cfg.AdmissionJWKSURL, Static: static,
			RefreshEvery: cfg.JWKSRefreshInterval, MinRefresh: cfg.JWKSMinRefreshInterval,
			// Fetches carry traceparent, so queue-svc's span joins the trace.
			Client: &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)},
		}, reg, log)
		// Not fatal: queue-svc may start later. Readiness stays false until
		// some key is known, and the first token with a new kid fetches again.
		if err := jwks.Refresh(ctx); err != nil {
			log.Warn("jwks: initial fetch failed; will retry", "url", cfg.AdmissionJWKSURL, "err", err)
		}
		verifier = authn.NewVerifierWith(jwks.Lookup, cfg.TokenLeeway)
		checks = append(checks, health.Check{Name: "admission-keys", Fn: jwks.Check})
		background = append(background, jwks)
	}

	store := inventory.NewStore(rdb)
	if err := store.LoadScripts(ctx); err != nil {
		return err
	}
	invMetrics := inventory.NewMetrics(reg)
	svc, err := inventory.NewService(store, inventory.Config{HoldTTL: cfg.HoldTTL, PaymentWindow: cfg.PaymentWindow}, invMetrics)
	if err != nil {
		return err
	}

	hc := health.New(2*time.Second, checks...)
	httpMetrics := httpx.NewHTTPMetrics(reg)
	public := httpx.NewRouter(
		httpx.Trace(), // outermost: the span covers the whole request
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

	var callers map[string]ed25519.PublicKey
	if len(cfg.GRPCTrustedCallers) > 0 {
		if callers, err = authn.LoadServiceKeys(cfg.GRPCTrustedCallers); err != nil {
			return err
		}
	} else {
		log.Warn("grpc: no trusted callers (GRPC_TRUSTED_CALLERS); every call will be refused")
	}
	grpcSrv := grpcx.NewServer(grpcx.ServerConfig{
		Verifier: authn.NewServiceVerifier(serviceName, callers, cfg.TokenLeeway),
		Allow:    inventory.GRPCAllow(),
	}, reg, log)
	inventoryv1.RegisterInventoryServiceServer(grpcSrv, inventory.NewGRPCServer(svc))

	components := append([]app.Component{
		grpcx.NewComponent("grpc", cfg.GRPCAddr, grpcSrv, log),
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log, httpx.WithWriteTimeout(90*time.Second)),
		inventory.NewSweeper(store, cfg.SweepInterval, cfg.SweepBatch, invMetrics, log),
	}, background...)
	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay, components...)
}
