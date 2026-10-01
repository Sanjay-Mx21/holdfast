// Command holdfastctl is the operator CLI: database migrations, development
// keys and tokens, event creation, and inventory and queue provisioning. It reuses the
// services' own packages, so every rule has exactly one implementation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/db/migrations"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
	"github.com/Sanjay-Mx21/holdfast/internal/queue"
)

type command struct {
	path  string
	short string
	run   func(ctx context.Context, args []string) error
}

func commands() []command {
	return []command{
		{"migrate", "apply pending database migrations for every schema", cmdMigrate},
		{"keys generate", "generate an Ed25519 key pair for admission tokens", cmdKeysGenerate},
		{"token mint", "mint admission tokens (development and load tests only)", cmdTokenMint},
		{"event create", "create an event in PostgreSQL and provision its inventory and queue", cmdEventCreate},
		{"inventory provision", "provision or rebuild an event's Valkey inventory from PostgreSQL", cmdInventoryProvision},
		{"inventory status", "show an event's live availability", cmdInventoryStatus},
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := dispatch(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "holdfastctl:", err)
		os.Exit(1)
	}
}

func dispatch(ctx context.Context, args []string) error {
	for _, c := range commands() {
		parts := strings.Fields(c.path)
		if len(args) >= len(parts) && slices.Equal(args[:len(parts)], parts) {
			return c.run(ctx, args[len(parts):])
		}
	}
	usage(os.Stderr)
	if len(args) == 0 || slices.Contains([]string{"help", "-h", "--help"}, args[0]) {
		return nil
	}
	return fmt.Errorf("unknown command %q", strings.Join(args, " "))
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "Usage: holdfastctl <command> [flags]\n\nCommands:")
	for _, c := range commands() {
		fmt.Fprintf(w, "  %-22s %s\n", c.path, c.short)
	}
	fmt.Fprintln(w, "\nRun 'holdfastctl <command> -h' for the command's flags.")
}

// Connection flags fall back to the same environment variables the services use.
func dsnFlag(fs *flag.FlagSet) *string {
	return fs.String("dsn", os.Getenv("POSTGRES_DSN"), "PostgreSQL DSN (env POSTGRES_DSN)")
}

func valkeyFlag(fs *flag.FlagSet) *string {
	def := os.Getenv("VALKEY_ADDRS")
	if def == "" {
		def = "localhost:6379"
	}
	return fs.String("valkey", def, "comma-separated Valkey addresses (env VALKEY_ADDRS)")
}

func openPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, errors.New("a PostgreSQL DSN is required (--dsn or POSTGRES_DSN)")
	}
	return postgres.NewPool(ctx, config.Postgres{
		DSN: dsn, MaxConns: 4, MinConns: 0, MaxConnLifetime: time.Hour,
		MaxConnIdleTime: time.Minute, StatementTimeout: time.Minute,
	}, "holdfastctl")
}

func openValkey(ctx context.Context, addrs string) (redis.UniversalClient, error) {
	return valkey.New(ctx, config.Valkey{
		Addrs: strings.Split(addrs, ","), Password: os.Getenv("VALKEY_PASSWORD"), PoolSize: 4,
		DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
	}, "holdfastctl")
}

func inventoryService(rdb redis.UniversalClient) (*inventory.Service, error) {
	return inventory.NewService(inventory.NewStore(rdb),
		inventory.Config{HoldTTL: 5 * time.Minute, PaymentWindow: 10 * time.Minute},
		inventory.NewMetrics(prometheus.NewRegistry()))
}

func cmdMigrate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	dsn := dsnFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	pool, err := openPool(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	for _, s := range migrations.All() {
		n, err := postgres.Migrate(ctx, pool, s.Name, s.Files, slog.Default())
		if err != nil {
			return err
		}
		fmt.Printf("schema %-10s %d migration(s) applied\n", s.Name, n)
	}
	return nil
}

func cmdKeysGenerate(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("keys generate", flag.ContinueOnError)
	outDir := fs.String("out-dir", ".local/keys", "directory for the key files")
	name := fs.String("name", "admission", "base file name")
	ifMissing := fs.Bool("if-missing", false, "do nothing if the key already exists")
	if err := fs.Parse(args); err != nil {
		return err
	}
	privPath := filepath.Join(*outDir, *name+".key")
	pubPath := filepath.Join(*outDir, *name+".pub")
	if _, err := os.Stat(privPath); err == nil {
		if *ifMissing {
			fmt.Println("key exists, skipping:", privPath)
			return nil
		}
		return fmt.Errorf("%s already exists (use --if-missing to skip)", privPath)
	}
	privPEM, pubPEM, err := authn.GenerateKeyPair()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*outDir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(privPath, privPEM, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(pubPath, pubPEM, 0o644); err != nil {
		return err
	}
	pub, err := authn.ParsePublicKeyPEM(pubPEM)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s and %s (kid %s)\n", privPath, pubPath, authn.KeyID(pub))
	return nil
}

func cmdTokenMint(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("token mint", flag.ContinueOnError)
	keyPath := fs.String("key", ".local/keys/admission.key", "Ed25519 private key (PEM)")
	event := fs.String("event", "", "event ID (required)")
	user := fs.String("user", "", "user ID (default: random); ignored when --users > 1")
	users := fs.Int("users", 1, "mint tokens for N random users, printed as CSV user_id,token")
	ttl := fs.Duration("ttl", 15*time.Minute, "token lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := uuid.Parse(*event); err != nil {
		return errors.New("--event must be a UUID")
	}
	raw, err := os.ReadFile(*keyPath)
	if err != nil {
		return err
	}
	key, err := authn.ParsePrivateKeyPEM(raw)
	if err != nil {
		return err
	}
	issuer := authn.NewIssuer(key, *ttl)
	fmt.Fprintln(os.Stderr, "warning: tokens minted here bypass the waiting room; use for development and load tests only")
	if *users > 1 {
		fmt.Println("user_id,token")
		for i := 0; i < *users; i++ {
			uid := uuid.NewString()
			tok, _, err := issuer.Issue(uid, strings.ToLower(*event), uuid.NewString(), int64(i+1))
			if err != nil {
				return err
			}
			fmt.Printf("%s,%s\n", uid, tok)
		}
		return nil
	}
	uid := *user
	if uid == "" {
		uid = uuid.NewString()
	}
	if _, err := uuid.Parse(uid); err != nil {
		return errors.New("--user must be a UUID")
	}
	tok, exp, err := issuer.Issue(strings.ToLower(uid), strings.ToLower(*event), uuid.NewString(), 1)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "user %s, expires %s\n", uid, exp.Format(time.RFC3339))
	fmt.Println(tok)
	return nil
}

func cmdEventCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("event create", flag.ContinueOnError)
	dsn := dsnFlag(fs)
	vk := valkeyFlag(fs)
	name := fs.String("name", "", "event name (required)")
	capacity := fs.Int("capacity", 1000, "total units")
	perUser := fs.Int("per-user-limit", 4, "maximum units per user")
	price := fs.Int64("price-paise", 250000, "unit price in paise")
	opens := fs.String("opens-at", "", "sale opening time, RFC 3339 (default: now)")
	rate := fs.Int("admission-rate", 83, "queue: buyers admitted per second")
	sessions := fs.Int("max-sessions", 10_000, "queue: maximum concurrent checkout sessions")
	sessionTTL := fs.Duration("session-ttl", 10*time.Minute, "queue: lifetime of an admitted buyer's session")
	noProvision := fs.Bool("no-provision", false, "only write PostgreSQL; provision Valkey later")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opensAt := time.Now().UTC()
	if *opens != "" {
		t, err := time.Parse(time.RFC3339, *opens)
		if err != nil {
			return fmt.Errorf("--opens-at: %w", err)
		}
		opensAt = t
	}
	qcfg := queue.EventConfig{OpensAt: opensAt, AdmissionRate: *rate, MaxSessions: *sessions, SessionTTL: *sessionTTL}
	// Check the queue settings before writing anything, so a bad flag never
	// leaves an event in PostgreSQL that cannot be put on sale.
	if err := qcfg.Validate(); err != nil {
		return err
	}
	pool, err := openPool(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	id, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: *name, SaleOpensAt: opensAt, PerUserLimit: *perUser, UnitPricePaise: *price, Capacity: *capacity,
	})
	if err != nil {
		return err
	}
	fmt.Println("event created:", id)
	if *noProvision {
		return nil
	}
	if err := provision(ctx, *vk, id.String(), inventory.EventConfig{Capacity: *capacity, PerUserLimit: *perUser}); err != nil {
		return err
	}
	return provisionQueue(ctx, *vk, id.String(), qcfg)
}

func cmdInventoryProvision(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inventory provision", flag.ContinueOnError)
	dsn := dsnFlag(fs)
	vk := valkeyFlag(fs)
	event := fs.String("event", "", "event ID (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, err := uuid.Parse(*event)
	if err != nil {
		return errors.New("--event must be a UUID")
	}
	pool, err := openPool(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	e, err := catalog.Get(ctx, pool, id)
	if err != nil {
		return err
	}
	// PostgreSQL is the source of truth: the pool starts at capacity minus
	// units already sold. (Once booking-svc exists, units in pending
	// bookings are subtracted too; see the inventory runbook.)
	return provision(ctx, *vk, id.String(), inventory.EventConfig{
		Capacity: e.Capacity, PerUserLimit: e.PerUserLimit, InitialAvailable: e.Capacity - e.Sold,
	})
}

func provision(ctx context.Context, addrs, eventID string, cfg inventory.EventConfig) error {
	rdb, err := openValkey(ctx, addrs)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	svc, err := inventoryService(rdb)
	if err != nil {
		return err
	}
	if cfg.InitialAvailable == 0 && cfg.Capacity > 0 {
		cfg.InitialAvailable = cfg.Capacity
	}
	if cfg.InitialAvailable == 0 {
		return errors.New("nothing to provision: event is fully sold in PostgreSQL")
	}
	created, err := svc.Provision(ctx, eventID, cfg)
	if err != nil {
		return err
	}
	if created {
		fmt.Printf("inventory provisioned: %d/%d units available, per-user limit %d\n",
			cfg.InitialAvailable, cfg.Capacity, cfg.PerUserLimit)
	} else {
		fmt.Println("inventory already provisioned with the same settings; nothing to do")
	}
	return nil
}

func provisionQueue(ctx context.Context, addrs, eventID string, cfg queue.EventConfig) error {
	rdb, err := openValkey(ctx, addrs)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	created, err := queue.NewService(queue.NewStore(rdb)).Provision(ctx, eventID, cfg)
	if err != nil {
		return err
	}
	if created {
		fmt.Printf("queue provisioned: opens %s, %d admissions/s, %d sessions of %s, state PRE\n",
			cfg.OpensAt.UTC().Format(time.RFC3339), cfg.AdmissionRate, cfg.MaxSessions, cfg.SessionTTL)
	} else {
		fmt.Println("queue already provisioned with the same settings; nothing to do")
	}
	return nil
}

func cmdInventoryStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inventory status", flag.ContinueOnError)
	vk := valkeyFlag(fs)
	event := fs.String("event", "", "event ID (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rdb, err := openValkey(ctx, *vk)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	svc, err := inventoryService(rdb)
	if err != nil {
		return err
	}
	a, err := svc.Availability(ctx, *event)
	if err != nil {
		return err
	}
	fmt.Printf("event %s: %d of %d units available (sold out: %t)\n", a.EventID, a.Available, a.Capacity, a.SoldOut())
	return nil
}
