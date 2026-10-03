// Command booking runs booking-svc: buyers turn holds into bookings that wait
// for payment (POST /v1/bookings, idempotent; GET /v1/bookings/{id},
// owner-only), and overdue bookings are cancelled. It calls inventory-svc
// over gRPC with its own service token; its admin port serves /metrics,
// health and pprof.
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
	"path/filepath"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/Sanjay-Mx21/holdfast/internal/booking"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/payment"
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

const serviceName = "booking"

type config struct {
	Service  cfgpkg.Service
	HTTP     cfgpkg.HTTP
	Postgres cfgpkg.Postgres
	Kafka    cfgpkg.Kafka

	// AccessJWKSURL is auth-svc's key set: buyers are identified by its access
	// tokens (Authorization: Bearer). Required unless DEV_IDENTITY is on.
	AccessJWKSURL string `env:"ACCESS_JWKS_URL"`
	// DevIdentity also accepts the X-Dev-User-Id header on requests that carry
	// no token, for development and load tests (E2 simulates 50,000 buyers
	// with it). Never allowed in production.
	DevIdentity bool `env:"DEV_IDENTITY" envDefault:"false"`

	// inventory-svc's internal gRPC API, and the key booking-svc signs its
	// service tokens with (inventory trusts the matching public key).
	InventoryGRPCAddr     string        `env:"INVENTORY_GRPC_ADDR" envDefault:"inventory:7070"`
	ServicePrivateKeyFile string        `env:"SERVICE_PRIVATE_KEY_FILE,required"`
	InventoryTimeout      time.Duration `env:"INVENTORY_TIMEOUT" envDefault:"800ms"`

	// payment-svc's internal gRPC API. Empty: bookings are created without
	// a payment intent or checkout URL. Its calls include the payment
	// provider's, so they get a longer deadline.
	PaymentGRPCAddr string        `env:"PAYMENT_GRPC_ADDR"`
	PaymentTimeout  time.Duration `env:"PAYMENT_TIMEOUT" envDefault:"8s"`
	// PaymentGrace: a PAYING hold outlives the booking's deadline by this
	// much, to absorb late webhooks (design doc 6.1). Must stay below
	// inventory-svc's PAYMENT_WINDOW.
	PaymentGrace time.Duration `env:"PAYMENT_GRACE" envDefault:"3m"`

	// The outbox relay publishes booking events to Kafka (one leader across
	// replicas).
	OutboxBatch    int           `env:"OUTBOX_BATCH" envDefault:"500"`
	OutboxInterval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"200ms"`

	// The deadline job cancels bookings whose payment deadline passed.
	DeadlineScanInterval time.Duration `env:"DEADLINE_SCAN_INTERVAL" envDefault:"5s"`
	DeadlineBatch        int           `env:"DEADLINE_BATCH" envDefault:"100"`
}

func (c *config) Validate() error {
	errs := []error{cfgpkg.ValidateAll(c.Service, c.HTTP, c.Postgres, c.Kafka)}
	if c.OutboxBatch < 1 || c.OutboxBatch > 10_000 {
		errs = append(errs, errors.New("OUTBOX_BATCH must be between 1 and 10000"))
	}
	if c.OutboxInterval < 10*time.Millisecond || c.OutboxInterval > time.Minute {
		errs = append(errs, errors.New("OUTBOX_INTERVAL must be between 10ms and 1m"))
	}
	if c.DevIdentity && c.Service.Environment == "production" {
		errs = append(errs, errors.New("DEV_IDENTITY must not be enabled in production: anyone could claim any user ID"))
	}
	if c.AccessJWKSURL == "" && !c.DevIdentity {
		errs = append(errs, errors.New("set ACCESS_JWKS_URL (auth-svc's key set) so buyers can sign in"))
	}
	if c.AccessJWKSURL != "" {
		if u, err := url.Parse(c.AccessJWKSURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, errors.New("ACCESS_JWKS_URL must be an http(s) URL"))
		}
	}
	if c.PaymentTimeout < 100*time.Millisecond || c.PaymentTimeout > time.Minute {
		errs = append(errs, errors.New("PAYMENT_TIMEOUT must be between 100ms and 1m"))
	}
	if c.PaymentGrace < 0 || c.PaymentGrace > 30*time.Minute {
		errs = append(errs, errors.New("PAYMENT_GRACE must be between 0 and 30m"))
	}
	if c.InventoryTimeout < 50*time.Millisecond || c.InventoryTimeout > 30*time.Second {
		errs = append(errs, errors.New("INVENTORY_TIMEOUT must be between 50ms and 30s"))
	}
	if c.DeadlineScanInterval < 100*time.Millisecond || c.DeadlineScanInterval > 5*time.Minute {
		errs = append(errs, errors.New("DEADLINE_SCAN_INTERVAL must be between 100ms and 5m"))
	}
	if c.DeadlineBatch < 1 || c.DeadlineBatch > 10_000 {
		errs = append(errs, errors.New("DEADLINE_BATCH must be between 1 and 10000"))
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

	raw, err := os.ReadFile(filepath.Clean(cfg.ServicePrivateKeyFile))
	if err != nil {
		return fmt.Errorf("read service key: %w", err)
	}
	key, err := authn.ParsePrivateKeyPEM(raw)
	if err != nil {
		return err
	}
	conn, err := grpcx.Dial(grpcx.ClientConfig{
		Target: cfg.InventoryGRPCAddr, Tokens: authn.NewServiceTokenSource(key, serviceName, "inventory"),
		Timeout: cfg.InventoryTimeout,
	})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	log.Info("calling inventory over gRPC", "addr", cfg.InventoryGRPCAddr, "kid", authn.KeyID(key.Public().(ed25519.PublicKey)))

	producer, err := kafka.NewProducer(ctx, cfg.Kafka, serviceName)
	if err != nil {
		return err
	}
	defer producer.Close()
	relay, err := outbox.NewRelay(pool, producer, outbox.Config{
		Schema: "booking", Source: "booking-svc", Batch: cfg.OutboxBatch, Interval: cfg.OutboxInterval,
	}, outbox.NewMetrics(reg), log)
	if err != nil {
		return err
	}

	bm := booking.NewMetrics(reg)
	// Payment intents arrive with payment-svc (task 3.9); until then bookings
	// are created without one.
	var intents booking.Intents
	if cfg.PaymentGRPCAddr != "" {
		pconn, err := grpcx.Dial(grpcx.ClientConfig{
			Target: cfg.PaymentGRPCAddr, Tokens: authn.NewServiceTokenSource(key, serviceName, "payment"),
			Timeout: cfg.PaymentTimeout,
		})
		if err != nil {
			return err
		}
		defer func() { _ = pconn.Close() }()
		intents = payment.NewClient(pconn)
		log.Info("creating payment intents over gRPC", "addr", cfg.PaymentGRPCAddr)
	} else {
		log.Warn("PAYMENT_GRPC_ADDR is not set: bookings are created without a payment intent")
	}
	inv := inventory.NewClient(conn)
	svc := booking.NewService(pool, inv, intents, booking.Config{Grace: cfg.PaymentGrace}, bm, log)

	// The saga: payment-svc's events confirm, cancel or refund bookings.
	saga, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.ConsumerConfig{
		Group: booking.SagaConsumer, Topics: []string{kafka.TopicPayment},
	}, booking.NewSaga(pool, inv, bm, log).Handle, kafka.NewMetrics(reg), log)
	if err != nil {
		return err
	}

	// Buyers: auth-svc's access tokens, and the development header if allowed.
	if cfg.DevIdentity {
		log.Warn("DEV_IDENTITY is on: requests without a token may name their buyer in X-Dev-User-Id, which anyone can set")
	}
	identity, accessKeys := authn.NewAccessIdentity(ctx, cfg.AccessJWKSURL, cfg.DevIdentity, 5*time.Second,
		&http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}, reg, log)
	var background []app.Component
	checks := []health.Check{postgres.Check(pool)}
	if accessKeys != nil {
		checks = append(checks, health.Check{Name: "access-keys", Fn: accessKeys.Check})
		background = append(background, accessKeys)
	}
	hc := health.New(2*time.Second, checks...)
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
	admin := httpx.NewAdminRouter(metrics.Handler(reg), hc)

	booking.NewHandler(svc).Register(public, identity)

	return app.Run(ctx, log, hc, cfg.HTTP.DrainDelay, append([]app.Component{
		httpx.NewServer("public", cfg.HTTP.Addr, public, cfg.HTTP, log),
		httpx.NewServer("admin", cfg.HTTP.AdminAddr, admin, cfg.HTTP, log, httpx.WithWriteTimeout(90*time.Second)),
		booking.NewDeadlineJob(pool, cfg.DeadlineScanInterval, cfg.DeadlineBatch, bm, log),
		relay,
		saga,
	}, background...)...)
}
