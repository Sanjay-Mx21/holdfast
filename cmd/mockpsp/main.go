// Command mockpsp runs the stand-in payment provider (internal/mockpsp): the
// provider API payment-svc calls, a hosted checkout page, signed webhooks to
// WEBHOOK_URL, and on the admin port a fault-injection API for chaos runs.
// It is a test tool: never expose it to the internet.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/mockpsp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
)

const serviceName = "mockpsp"

type config struct {
	Service cfgpkg.Service
	HTTP    cfgpkg.HTTP

	// PublicURL is where buyers' browsers reach the checkout page.
	PublicURL string `env:"PUBLIC_URL" envDefault:"http://localhost:8085"`
	// APIKey, if set, is required on every API call (payment-svc's
	// PSP_API_KEY).
	APIKey        string        `env:"API_KEY,unset"`
	WebhookURL    string        `env:"WEBHOOK_URL,required"`
	WebhookSecret string        `env:"WEBHOOK_SECRET,required,unset"`
	AdminToken    string        `env:"ADMIN_TOKEN,required,unset"`
	RefundDelay   time.Duration `env:"REFUND_DELAY" envDefault:"2s"`
	// Webhook retries: MAX_ATTEMPTS attempts, waiting RETRY_BASE, doubling
	// up to 30 s.
	WebhookMaxAttempts int           `env:"WEBHOOK_MAX_ATTEMPTS" envDefault:"8"`
	WebhookRetryBase   time.Duration `env:"WEBHOOK_RETRY_BASE" envDefault:"1s"`
	// Seed for fault decisions; 0 picks one from the clock (logged).
	Seed uint64 `env:"SEED" envDefault:"0"`
	// Faults to start with, as JSON (the admin API's format).
	Faults string `env:"FAULTS"`
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP)}
	for name, v := range map[string]string{"PUBLIC_URL": c.PublicURL, "WEBHOOK_URL": c.WebhookURL} {
		if u, err := url.ParseRequestURI(v); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			errs = append(errs, fmt.Errorf("%s must be an http(s) URL", name))
		}
	}
	if len(c.WebhookSecret) < 32 {
		errs = append(errs, errors.New("WEBHOOK_SECRET must be at least 32 characters"))
	}
	if len(c.AdminToken) < 32 {
		errs = append(errs, errors.New("ADMIN_TOKEN must be at least 32 characters"))
	}
	if c.RefundDelay < 0 || c.RefundDelay > 10*time.Minute {
		errs = append(errs, errors.New("REFUND_DELAY must be between 0 and 10m"))
	}
	if c.WebhookMaxAttempts < 1 || c.WebhookMaxAttempts > 50 || c.WebhookRetryBase < 10*time.Millisecond {
		errs = append(errs, errors.New("WEBHOOK_MAX_ATTEMPTS must be 1 to 50 and WEBHOOK_RETRY_BASE at least 10ms"))
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

	seed := cfg.Seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano()) //nolint:gosec // a seed, not a secret
	}
	log.Info("starting", "commit", bi.Commit, "go_version", bi.GoVersion, "seed", seed, "webhook_url", cfg.WebhookURL)

	reg := metrics.NewRegistry(serviceName)
	m := mockpsp.NewMetrics(reg)
	faults := mockpsp.NewInjector(seed)
	if cfg.Faults != "" {
		var f mockpsp.Faults
		dec := json.NewDecoder(strings.NewReader(cfg.Faults))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&f); err != nil {
			return fmt.Errorf("FAULTS: %w", err)
		}
		if err := faults.Set(f); err != nil {
			return fmt.Errorf("FAULTS: %w", err)
		}
		log.Warn("starting with faults", "faults", faults.Faults())
	}
	hooks := mockpsp.NewDispatcher(mockpsp.DispatcherConfig{
		URL: cfg.WebhookURL, Secret: []byte(cfg.WebhookSecret),
		MaxAttempts: cfg.WebhookMaxAttempts, RetryBase: cfg.WebhookRetryBase,
	}, faults, m, log)
	provider := mockpsp.New(mockpsp.Config{APIKey: cfg.APIKey, RefundDelay: cfg.RefundDelay},
		mockpsp.NewStore(strings.TrimRight(cfg.PublicURL, "/")), faults, hooks, m, log)

	hc := health.New(2 * time.Second)
	httpMetrics := httpx.NewHTTPMetrics(reg)
	// No request timeout: the timeout fault holds answers back on purpose.
	public := httpx.NewRouter(
		httpx.RequestID(),
		httpx.AccessLog(log, cfg.HTTP.AccessLogSuccess),
		httpMetrics.Middleware(),
		httpx.Recover(),
		httpx.SecurityHeaders(),
	)
	admin := httpx.NewAdminRouter(metrics.Handler(reg), hc)
	provider.Register(public, admin, authn.RequireStaticToken(cfg.AdminToken))

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay,
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log, httpx.WithWriteTimeout(6*time.Minute)),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log),
		hooks,
		provider,
	)
}
