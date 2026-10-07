// Command holdfastctl is the operator CLI: database migrations, development
// keys and tokens, event creation, inventory and queue provisioning, and the
// sale's freeze switch. It reuses the services' own packages, so every rule
// has exactly one implementation.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
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
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
	"github.com/Sanjay-Mx21/holdfast/internal/policy"
	"github.com/Sanjay-Mx21/holdfast/internal/pow"
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
		{"queue provision", "provision an event's waiting room (opening time from PostgreSQL by default)", cmdQueueProvision},
		{"queue status", "show an event's waiting room: state, size, admission, sessions, leader", cmdQueueStatus},
		{"freeze", "freeze a sale (runbook RB-1): no new holds, admissions paused", cmdFreeze},
		{"unfreeze", "resume a frozen sale (runbook RB-1)", cmdUnfreeze},
		{"pow solve", "solve a waiting-room proof-of-work challenge (for curl and scripts)", cmdPoWSolve},
		{"kafka topics", "create every Kafka topic HoldFast uses (idempotent; auto-creation is off)", cmdKafkaTopics},
		{"dlq replay", "publish a dead-letter topic's messages back to their topic (runbook RB-3)", cmdDLQReplay},
		{"refund", "ask payment-svc again to refund a booking stuck in REFUND_REQUIRED (runbook RB-4)", cmdRefund},
		{"valkey probe", "write to Valkey steadily and report outages and lost writes (failover drill)", cmdValkeyProbe},
	}
}

// stdout is where commands print their results (tests replace it).
var stdout io.Writer = os.Stdout

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
	return fs.String("valkey", def, "comma-separated Valkey addresses, or Sentinels' with VALKEY_SENTINEL_MASTER (env VALKEY_ADDRS)")
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

// openValkey connects to addrs, configured from the environment like the
// services (valkeyConfig).
func openValkey(ctx context.Context, addrs string) (redis.UniversalClient, error) {
	cfg, err := valkeyConfig(addrs, os.Getenv)
	if err != nil {
		return nil, err
	}
	return valkey.New(ctx, cfg, "holdfastctl")
}

// valkeyConfig is the client configuration for addrs, with VALKEY_PASSWORD,
// VALKEY_DB and VALKEY_SENTINEL_MASTER from getenv as the services read
// them: with a master name, addrs are Sentinels (task 5.2).
func valkeyConfig(addrs string, getenv func(string) string) (config.Valkey, error) {
	db := 0
	if v := getenv("VALKEY_DB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return config.Valkey{}, fmt.Errorf("VALKEY_DB must be a database number, got %q", v)
		}
		db = n
	}
	return config.Valkey{
		Addrs: strings.Split(addrs, ","), MasterName: getenv("VALKEY_SENTINEL_MASTER"),
		Password: getenv("VALKEY_PASSWORD"), DB: db, PoolSize: 4,
		DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
	}, nil
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
	verifiedFor := fs.Duration("verified-only-for", 0, "policy: only verified buyers may join for this long after opening (0: no window)")
	lockoutFor := fs.Duration("agent-lockout-for", 0, "policy: agents may not join for this long after opening (0: no window)")
	qf := addQueueFlags(fs)
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
	if *verifiedFor < 0 || *lockoutFor < 0 {
		return errors.New("--verified-only-for and --agent-lockout-for must not be negative")
	}
	verifiedUntil, lockoutUntil := windowEnd(opensAt, *verifiedFor), windowEnd(opensAt, *lockoutFor)
	qcfg := qf.config(opensAt)
	qcfg.Policy = policyRules(verifiedUntil, lockoutUntil)
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
		VerifiedWindowEndsAt: verifiedUntil, AgentLockoutEndsAt: lockoutUntil,
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

// windowEnd is opensAt plus d, or nil (no window) when d is zero.
func windowEnd(opensAt time.Time, d time.Duration) *time.Time {
	if d == 0 {
		return nil
	}
	t := opensAt.Add(d)
	return &t
}

// policyRules turns the catalog's optional window ends into queue rules.
func policyRules(verifiedUntil, lockoutUntil *time.Time) policy.Rules {
	var r policy.Rules
	if verifiedUntil != nil {
		r.VerifiedOnlyUntil = *verifiedUntil
	}
	if lockoutUntil != nil {
		r.AgentLockoutUntil = *lockoutUntil
	}
	return r
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
	if errors.Is(err, queue.ErrProvisionConflict) {
		return fmt.Errorf("%w: a queue's settings are fixed once provisioned; see 'holdfastctl queue status --event %s'", err, eventID)
	}
	if err != nil {
		return err
	}
	if created {
		fmt.Fprintf(stdout, "queue provisioned: opens %s, %d admissions/s, %d sessions of %s, state PRE\n",
			cfg.OpensAt.UTC().Format(time.RFC3339), cfg.AdmissionRate, cfg.MaxSessions, cfg.SessionTTL)
	} else {
		fmt.Fprintln(stdout, "queue already provisioned with the same settings; nothing to do but set the policy windows")
	}
	fmt.Fprintf(stdout, "policy: %s\n", describePolicy(cfg.Policy))
	return nil
}

func describePolicy(r policy.Rules) string {
	var parts []string
	if !r.VerifiedOnlyUntil.IsZero() {
		parts = append(parts, "verified buyers only until "+r.VerifiedOnlyUntil.UTC().Format(time.RFC3339))
	}
	if !r.AgentLockoutUntil.IsZero() {
		parts = append(parts, "no agents until "+r.AgentLockoutUntil.UTC().Format(time.RFC3339))
	}
	if len(parts) == 0 {
		return "no windows: anyone signed in may join"
	}
	return strings.Join(parts, "; ")
}

// queueFlags are the waiting-room settings, shared by event create and queue
// provision.
type queueFlags struct {
	rate, sessions *int
	sessionTTL     *time.Duration
}

func addQueueFlags(fs *flag.FlagSet) queueFlags {
	return queueFlags{
		rate:       fs.Int("admission-rate", 83, "queue: buyers admitted per second"),
		sessions:   fs.Int("max-sessions", 10_000, "queue: maximum concurrent checkout sessions"),
		sessionTTL: fs.Duration("session-ttl", 10*time.Minute, "queue: lifetime of an admitted buyer's session"),
	}
}

func (q queueFlags) config(opensAt time.Time) queue.EventConfig {
	return queue.EventConfig{OpensAt: opensAt, AdmissionRate: *q.rate, MaxSessions: *q.sessions, SessionTTL: *q.sessionTTL}
}

// cmdQueueProvision provisions an event's waiting room: for an event created
// with --no-provision, or to put its settings back after Valkey lost them.
// The opening time and the policy windows come from PostgreSQL (the catalog)
// unless --opens-at is given, which sets no windows; the admission settings
// are not stored there, so they come from flags.
// Rebuilding cannot bring back who had joined: the queue lives in Valkey only.
func cmdQueueProvision(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("queue provision", flag.ContinueOnError)
	dsn := dsnFlag(fs)
	vk := valkeyFlag(fs)
	event := fs.String("event", "", "event ID (required)")
	opens := fs.String("opens-at", "", "sale opening time, RFC 3339 (default: the event's opening time in PostgreSQL)")
	qf := addQueueFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, err := uuid.Parse(*event)
	if err != nil {
		return errors.New("--event must be a UUID")
	}
	var opensAt time.Time
	var rules policy.Rules
	if *opens != "" {
		if opensAt, err = time.Parse(time.RFC3339, *opens); err != nil {
			return fmt.Errorf("--opens-at: %w", err)
		}
	} else {
		pool, err := openPool(ctx, *dsn)
		if err != nil {
			return fmt.Errorf("%w (or pass --opens-at)", err)
		}
		defer pool.Close()
		e, err := catalog.Get(ctx, pool, id)
		if err != nil {
			return err
		}
		opensAt = e.SaleOpensAt
		rules = policyRules(e.VerifiedWindowEndsAt, e.AgentLockoutEndsAt)
	}
	cfg := qf.config(opensAt)
	cfg.Policy = rules
	if err := cfg.Validate(); err != nil {
		return err
	}
	return provisionQueue(ctx, *vk, id.String(), cfg)
}

func cmdQueueStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("queue status", flag.ContinueOnError)
	vk := valkeyFlag(fs)
	event := fs.String("event", "", "event ID (required)")
	asJSON := fs.Bool("json", false, "print the overview as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rdb, err := openValkey(ctx, *vk)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	o, err := queue.NewService(queue.NewStore(rdb)).Overview(ctx, *event)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(queueStatusJSON(o))
	}
	printQueueStatus(stdout, o)
	return nil
}

func printQueueStatus(w io.Writer, o queue.Overview) {
	state := string(o.State)
	if o.State != o.StoredState {
		state += fmt.Sprintf(" (stored %s: T0 has passed and no join or opener has flipped it yet)", o.StoredState)
	}
	when := "opens"
	if !o.Now.Before(o.Config.OpensAt) {
		when = "opened"
	}
	fmt.Fprintf(w, "event %s\n", o.EventID)
	fmt.Fprintf(w, "  %-11s %s, %s %s\n", "state", state, when, o.Config.OpensAt.Format(time.RFC3339))
	fmt.Fprintf(w, "  %-11s %d in line, %d admitted (admittedUpTo)\n", "queue", o.QueueSize, o.AdmittedUpTo)
	fmt.Fprintf(w, "  %-11s %d active of %d; %d admissions/s; sessions last %s\n", "sessions",
		o.ActiveSessions, o.Config.MaxSessions, o.Config.AdmissionRate, o.Config.SessionTTL)
	leader := "no controller has led yet"
	if o.LeaderEpoch > 0 {
		leader = fmt.Sprintf("epoch %d", o.LeaderEpoch)
	}
	doc := "not written yet (no leader)"
	if !o.UpdatedAt.IsZero() {
		doc = fmt.Sprintf("written %s ago", o.Now.Sub(o.UpdatedAt).Round(time.Millisecond))
	}
	fmt.Fprintf(w, "  %-11s %s; status document %s\n", "leader", leader, doc)
	fmt.Fprintf(w, "  %-11s %s\n", "policy", describePolicy(o.Config.Policy))
	list := "yes"
	if !o.OnWorkList {
		list = "NO: no opener or admission controller serves this event"
	}
	fmt.Fprintf(w, "  %-11s %s\n", "work list", list)
}

func queueStatusJSON(o queue.Overview) any {
	var updated *time.Time
	if !o.UpdatedAt.IsZero() {
		updated = &o.UpdatedAt
	}
	return struct {
		EventID        string     `json:"eventId"`
		State          string     `json:"state"`
		StoredState    string     `json:"storedState"`
		OpensAt        time.Time  `json:"opensAt"`
		QueueSize      int64      `json:"queueSize"`
		AdmittedUpTo   int64      `json:"admittedUpTo"`
		ActiveSessions int64      `json:"activeSessions"`
		MaxSessions    int        `json:"maxSessions"`
		AdmissionRate  int        `json:"admissionRate"`
		SessionTTL     string     `json:"sessionTtl"`
		LeaderEpoch    int64      `json:"leaderEpoch"`
		UpdatedAt      *time.Time `json:"updatedAt"`
		OnWorkList     bool       `json:"onWorkList"`
		Now            time.Time  `json:"now"`
		// The policy windows; null means none.
		VerifiedOnlyUntil *time.Time `json:"verifiedOnlyUntil"`
		AgentLockoutUntil *time.Time `json:"agentLockoutUntil"`
	}{o.EventID, string(o.State), string(o.StoredState), o.Config.OpensAt, o.QueueSize, o.AdmittedUpTo,
		o.ActiveSessions, o.Config.MaxSessions, o.Config.AdmissionRate, o.Config.SessionTTL.String(),
		o.LeaderEpoch, updated, o.OnWorkList, o.Now,
		nonZero(o.Config.Policy.VerifiedOnlyUntil), nonZero(o.Config.Policy.AgentLockoutUntil)}
}

func nonZero(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// cmdFreeze is runbook RB-1's switch: inventory stops taking new holds
// first, then the queue stops admitting. Holds already made, checkouts and
// payments carry on. Running it again is harmless.
func cmdFreeze(ctx context.Context, args []string) error {
	return freezeSwitch(ctx, "freeze", args, true)
}

// cmdUnfreeze resumes the sale: holds first, so the people admitted already
// can buy, then admissions.
func cmdUnfreeze(ctx context.Context, args []string) error {
	return freezeSwitch(ctx, "unfreeze", args, false)
}

func freezeSwitch(ctx context.Context, name string, args []string, frozen bool) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	vk := valkeyFlag(fs)
	event := fs.String("event", "", "event ID (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := uuid.Parse(*event); err != nil {
		return errors.New("--event must be a UUID")
	}
	rdb, err := openValkey(ctx, *vk)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	inv, err := inventoryService(rdb)
	if err != nil {
		return err
	}
	changed, err := inv.SetFrozen(ctx, *event, frozen)
	if err != nil {
		return fmt.Errorf("inventory: %w", err)
	}
	holds := map[bool]string{true: "holds frozen: no new holds", false: "holds resumed"}[frozen]
	fmt.Fprintf(stdout, "inventory: %s%s\n", holds, unchangedNote(changed))

	qsvc := queue.NewService(queue.NewStore(rdb))
	if frozen {
		changed, err = qsvc.Freeze(ctx, *event)
	} else {
		changed, err = qsvc.Unfreeze(ctx, *event)
	}
	switch {
	case errors.Is(err, queue.ErrStateConflict):
		// The sale's holds are switched; the queue has nothing to pause or
		// resume (before T0, sold out or closed).
		fmt.Fprintf(stdout, "queue: left as it is (%s)\n", strings.TrimPrefix(err.Error(), queue.ErrStateConflict.Error()+": "))
	case err != nil:
		return fmt.Errorf("queue: %w", err)
	default:
		admissions := map[bool]string{true: "FROZEN: admissions paused", false: "OPEN: admissions resumed"}[frozen]
		fmt.Fprintf(stdout, "queue: %s%s\n", admissions, unchangedNote(changed))
	}
	return nil
}

func unchangedNote(changed bool) string {
	if changed {
		return ""
	}
	return " (already so)"
}

// cmdPoWSolve prints the nonce that solves a challenge from
// GET /v1/queue/{id}/challenge, so the quickstart and scripts can join with
// curl. Browsers solve challenges in a Web Worker instead.
func cmdPoWSolve(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("pow solve", flag.ContinueOnError)
	challenge := fs.String("challenge", "", "the challenge (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	d, err := pow.DifficultyOf(*challenge)
	if err != nil {
		return errors.New("--challenge is not a waiting-room challenge")
	}
	fmt.Fprintln(stdout, pow.Solve(*challenge, d))
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
	fmt.Printf("event %s: %d of %d units available (sold out: %t), %d open holds, frozen: %t\n",
		a.EventID, a.Available, a.Capacity, a.SoldOut(), a.ActiveHolds, a.Frozen)
	return nil
}

// cmdKafkaTopics creates HoldFast's topics and their dead-letter topics.
// Brokers run with auto-creation off, so a topic exists only if this created
// it; existing topics are left exactly as they are.
func cmdKafkaTopics(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("kafka topics", flag.ContinueOnError)
	def := os.Getenv("KAFKA_BROKERS")
	if def == "" {
		def = "localhost:29092"
	}
	brokers := fs.String("brokers", def, "comma-separated Kafka brokers (env KAFKA_BROKERS)")
	partitions := fs.Int("partitions", 6, "partitions for new topics (design: 6 locally, 24 or more in production)")
	replication := fs.Int("replication-factor", 1, "replication factor for new topics (design: 1 locally, 3 in production)")
	retention := fs.Duration("retention", 7*24*time.Hour, "retention for new topics")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *partitions < 1 || *partitions > 1000 || *replication < 1 || *replication > 5 {
		return errors.New("--partitions must be 1 to 1000 and --replication-factor 1 to 5")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	res, err := kafka.EnsureTopics(ctx, config.Kafka{
		Brokers: strings.Split(*brokers, ","), DialTimeout: 5 * time.Second, DeliveryTimeout: 30 * time.Second,
	}, kafka.TopicSpec{
		Partitions: int32(*partitions), ReplicationFactor: int16(*replication), //nolint:gosec // bounded above
		Retention: strconv.FormatInt(retention.Milliseconds(), 10),
	}, kafka.Topics()...)
	for _, r := range res {
		state := "exists"
		if r.Created {
			state = "created"
		}
		fmt.Fprintf(stdout, "%-26s %-8s %d partitions\n", r.Topic, state, r.Partitions)
	}
	return err
}
