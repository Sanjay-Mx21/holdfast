// Command auth runs auth-svc: sign-in with a one-time code sent to a phone
// (a mock SMS gateway), 15-minute EdDSA access tokens, rotating refresh
// tokens with reuse detection, and the JWKS other services verify access
// tokens with. Its admin port serves /metrics, health and pprof.
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/auth"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
	hfotel "github.com/Sanjay-Mx21/holdfast/internal/platform/otel"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
)

const serviceName = "auth"

type config struct {
	Service  cfgpkg.Service
	HTTP     cfgpkg.HTTP
	Postgres cfgpkg.Postgres

	// SigningKeyFile is the Ed25519 key access tokens are signed with; its
	// public half is published at /.well-known/jwks.json.
	SigningKeyFile string `env:"SIGNING_KEY_FILE,required"`
	// PhonePepper keys the HMACs of phone numbers and codes. Changing it
	// orphans every user, so it is set once per environment.
	PhonePepper string        `env:"PHONE_PEPPER,required,unset"`
	AccessTTL   time.Duration `env:"ACCESS_TTL" envDefault:"15m"`
	RefreshTTL  time.Duration `env:"REFRESH_TTL" envDefault:"720h"`
	OTPTTL      time.Duration `env:"OTP_TTL" envDefault:"5m"`
	OTPAttempts int           `env:"OTP_MAX_ATTEMPTS" envDefault:"5"`
	OTPResend   time.Duration `env:"OTP_RESEND_AFTER" envDefault:"30s"`
	OTPPerHour  int           `env:"OTP_MAX_PER_HOUR" envDefault:"5"`
	// DevSMSInbox serves the mock SMS inbox at GET /v1/auth/dev/inbox, so the
	// code can be read in development. Never in production.
	DevSMSInbox bool `env:"DEV_SMS_INBOX" envDefault:"false"`
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP, c.Postgres)}
	if len(c.PhonePepper) < 32 {
		errs = append(errs, errors.New("PHONE_PEPPER must be at least 32 characters"))
	}
	if c.AccessTTL < time.Minute || c.AccessTTL > time.Hour {
		errs = append(errs, errors.New("ACCESS_TTL must be between 1m and 1h"))
	}
	if c.RefreshTTL < time.Hour || c.RefreshTTL > 90*24*time.Hour {
		errs = append(errs, errors.New("REFRESH_TTL must be between 1h and 90 days"))
	}
	if c.OTPTTL < time.Minute || c.OTPTTL > 15*time.Minute || c.OTPAttempts < 1 || c.OTPAttempts > 10 {
		errs = append(errs, errors.New("OTP_TTL must be between 1m and 15m and OTP_MAX_ATTEMPTS 1 to 10"))
	}
	if c.OTPResend < time.Second || c.OTPPerHour < 1 || c.OTPPerHour > 60 {
		errs = append(errs, errors.New("OTP_RESEND_AFTER must be at least 1s and OTP_MAX_PER_HOUR 1 to 60"))
	}
	if c.Service.Environment == "production" && c.DevSMSInbox {
		errs = append(errs, errors.New("DEV_SMS_INBOX must not be enabled in production: anyone could read sign-in codes"))
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
		_ = shutdownTracing(sctx)
	}()
	log.Info("tracing", "exporting", exporting)

	raw, err := os.ReadFile(filepath.Clean(cfg.SigningKeyFile))
	if err != nil {
		return fmt.Errorf("read SIGNING_KEY_FILE: %w", err)
	}
	key, err := authn.ParsePrivateKeyPEM(raw)
	if err != nil {
		return fmt.Errorf("SIGNING_KEY_FILE: %w", err)
	}
	log.Info("signing access tokens", "kid", authn.KeyID(key.Public().(ed25519.PublicKey)))

	reg := metrics.NewRegistry(serviceName)
	pool, err := postgres.NewPool(ctx, cfg.Postgres, serviceName)
	if err != nil {
		return err
	}
	defer pool.Close()

	inbox := auth.NewInbox(10_000)
	svc := auth.NewService(pool, auth.Config{
		Pepper: []byte(cfg.PhonePepper), CodeTTL: cfg.OTPTTL, MaxAttempts: cfg.OTPAttempts,
		ResendAfter: cfg.OTPResend, MaxPerHour: cfg.OTPPerHour, RefreshTTL: cfg.RefreshTTL,
	}, inbox, authn.NewAccessIssuer(key, cfg.AccessTTL), auth.NewMetrics(reg), log)
	var hcfg auth.HandlerConfig
	if cfg.DevSMSInbox {
		log.Warn("DEV_SMS_INBOX is on: anyone can read sign-in codes at GET /v1/auth/dev/inbox")
		hcfg.DevInbox = inbox
	}

	hc := health.New(2*time.Second, postgres.Check(pool))
	httpMetrics := httpx.NewHTTPMetrics(reg)
	public := httpx.NewRouter(
		httpx.Trace(),
		httpx.RequestID(),
		httpx.AccessLog(log, cfg.HTTP.AccessLogSuccess),
		httpMetrics.Middleware(),
		httpx.Recover(),
		httpx.SecurityHeaders(),
		httpx.BodyLimit(cfg.HTTP.MaxBodyBytes),
		httpx.Timeout(cfg.HTTP.RequestTimeout),
	)
	auth.NewHandler(svc, authn.NewJWKSet(key.Public().(ed25519.PublicKey)), hcfg).Register(public)
	admin := httpx.NewAdminRouter(metrics.Handler(reg), hc)

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay,
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log, httpx.WithWriteTimeout(90*time.Second)),
	)
}
