// Command payment runs payment-svc: payment intents for bookings
// (holdfast.payment.v1 over gRPC, for booking-svc), the payment provider's
// orders and webhooks (POST /v1/webhooks/psp), the double-entry ledger,
// status polling for intents whose webhook never came, refunds started by
// booking-svc's events, and the payment outbox relay. Its admin port serves /metrics, health and pprof.
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	paymentv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/payment/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/payment"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/app"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	cfgpkg "github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/grpcx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/metrics"
	hfotel "github.com/Sanjay-Mx21/holdfast/internal/platform/otel"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/outbox"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
)

const serviceName = "payment"

type config struct {
	Service  cfgpkg.Service
	HTTP     cfgpkg.HTTP
	Postgres cfgpkg.Postgres
	Kafka    cfgpkg.Kafka

	// The internal gRPC API, for booking-svc's service tokens.
	GRPCAddr           string        `env:"GRPC_ADDR" envDefault:":7070"`
	GRPCTrustedCallers []string      `env:"GRPC_TRUSTED_CALLERS" envSeparator:","`
	TokenLeeway        time.Duration `env:"TOKEN_LEEWAY" envDefault:"5s"`

	// The payment provider (mockpsp locally).
	PSPBaseURL       string        `env:"PSP_BASE_URL" envDefault:"http://mockpsp:8080"`
	PSPAPIKey        string        `env:"PSP_API_KEY,unset"`
	PSPWebhookSecret string        `env:"PSP_WEBHOOK_SECRET,required,unset"`
	PSPTimeout       time.Duration `env:"PSP_TIMEOUT" envDefault:"2s"`
	WebhookTolerance time.Duration `env:"WEBHOOK_TOLERANCE" envDefault:"5m"`

	// Status polling for intents still CREATED POLL_AFTER after creation.
	PollInterval time.Duration `env:"POLL_INTERVAL" envDefault:"30s"`
	PollAfter    time.Duration `env:"POLL_AFTER" envDefault:"2m"`
	PollBatch    int           `env:"POLL_BATCH" envDefault:"20"`

	OutboxBatch    int           `env:"OUTBOX_BATCH" envDefault:"500"`
	OutboxInterval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"200ms"`
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP, c.Postgres, c.Kafka)}
	if u, err := url.ParseRequestURI(c.PSPBaseURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		errs = append(errs, errors.New("PSP_BASE_URL must be an http(s) URL"))
	}
	if len(c.PSPWebhookSecret) < 32 {
		errs = append(errs, errors.New("PSP_WEBHOOK_SECRET must be at least 32 characters"))
	}
	if c.WebhookTolerance < 30*time.Second || c.WebhookTolerance > time.Hour {
		errs = append(errs, errors.New("WEBHOOK_TOLERANCE must be between 30s and 1h"))
	}
	if c.PollInterval < time.Second || c.PollAfter < 10*time.Second || c.PollBatch < 1 || c.PollBatch > 1000 {
		errs = append(errs, errors.New("POLL_INTERVAL must be at least 1s, POLL_AFTER at least 10s, POLL_BATCH 1 to 1000"))
	}
	if c.OutboxBatch < 1 || c.OutboxBatch > 10_000 || c.OutboxInterval < 10*time.Millisecond {
		errs = append(errs, errors.New("OUTBOX_BATCH must be 1 to 10000 and OUTBOX_INTERVAL at least 10ms"))
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

	reg := metrics.NewRegistry(serviceName)
	pool, err := postgres.NewPool(ctx, cfg.Postgres, serviceName)
	if err != nil {
		return err
	}
	defer pool.Close()

	provider, err := psp.New(psp.Config{BaseURL: cfg.PSPBaseURL, APIKey: cfg.PSPAPIKey, Timeout: cfg.PSPTimeout}, psp.NewMetrics(reg))
	if err != nil {
		return err
	}
	pm := payment.NewMetrics(reg)
	svc := payment.NewService(pool, provider, pm, log)

	producer, err := kafka.NewProducer(ctx, cfg.Kafka, serviceName)
	if err != nil {
		return err
	}
	defer producer.Close()
	relay, err := outbox.NewRelay(pool, producer, outbox.Config{
		Schema: "payment", Source: "payment-svc", Batch: cfg.OutboxBatch, Interval: cfg.OutboxInterval,
	}, outbox.NewMetrics(reg), log)
	if err != nil {
		return err
	}

	// Refunds: booking-svc's refund_required events start them.
	refunds, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.ConsumerConfig{
		Group: payment.RefundConsumer, Topics: []string{kafka.TopicBooking},
	}, svc.HandleBookingEvent, kafka.NewMetrics(reg), log)
	if err != nil {
		return err
	}

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
		Allow:    payment.GRPCAllow(),
	}, reg, log)
	paymentv1.RegisterPaymentServiceServer(grpcSrv, payment.NewGRPCServer(svc))

	hc := health.New(2*time.Second, postgres.Check(pool))
	httpMetrics := httpx.NewHTTPMetrics(reg)
	public := httpx.NewRouter(
		httpx.Trace(),
		httpx.RequestID(),
		httpx.AccessLog(log, cfg.HTTP.AccessLogSuccess),
		httpMetrics.Middleware(),
		httpx.Recover(),
		httpx.SecurityHeaders(),
		httpx.Timeout(cfg.HTTP.RequestTimeout),
	)
	payment.NewWebhookHandler(svc, []byte(cfg.PSPWebhookSecret), cfg.WebhookTolerance).Register(public)
	admin := httpx.NewAdminRouter(metrics.Handler(reg), hc)

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay,
		grpcx.NewComponent("grpc", cfg.GRPCAddr, grpcSrv, log),
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log, httpx.WithWriteTimeout(90*time.Second)),
		payment.NewPoller(svc, cfg.PollInterval, cfg.PollAfter, cfg.PollBatch, log),
		relay,
		refunds,
	)
}
