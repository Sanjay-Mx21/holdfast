// Command contention is experiment E1. It hammers the two layers that
// enforce "never oversell" and then checks the invariants:
//
//	-mode holds   buyers race for units through the real inventory service
//	              and its atomic Valkey Lua scripts (the fast path);
//	-mode guard     confirmations race through the PostgreSQL final guard
//	                (conditional UPDATE + per-user cap) for the same units;
//	-mode purchase  part B: whole purchases (hold, booking, payment with
//	                injected failures, the saga) through the real services,
//	                checking the final sold count in both stores;
//	-mode all       all three.
//
// It exits with status 1 if any invariant is violated, so CI runs it on every
// push: a regression that could oversell fails the build.
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
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sanjay-Mx21/holdfast/db/migrations"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/guard"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
)

type options struct {
	mode          string
	buyers        int
	guardAttempts int
	capacity      int
	qty           int
	perUser       int
	concurrency   int
	valkeyAddrs   string
	dsn           string
	keep          bool
	jsonPath      string
	purchases     int
	payFailure    float64
	pspFaults     string
	buyersPerUser int
}

// Check is one invariant and whether it held.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// Report is the outcome of one mode.
type Report struct {
	Mode        string             `json:"mode"`
	Attempts    int                `json:"attempts"`
	Capacity    int                `json:"capacity"`
	Qty         int                `json:"qty"`
	Concurrency int                `json:"concurrency"`
	Outcomes    map[string]int     `json:"outcomes"`
	ElapsedMS   float64            `json:"elapsedMs"`
	PerSecond   float64            `json:"attemptsPerSecond"`
	LatencyMS   map[string]float64 `json:"latencyMs"`
	Checks      []Check            `json:"checks"`
}

func (r *Report) passed() bool {
	for _, c := range r.Checks {
		if !c.OK {
			return false
		}
	}
	return true
}

func main() {
	var o options
	flag.StringVar(&o.mode, "mode", "holds", "holds | guard | purchase | all")
	flag.IntVar(&o.buyers, "buyers", 50_000, "concurrent hold attempts, one buyer each")
	flag.IntVar(&o.guardAttempts, "guard-attempts", 10_000, "concurrent confirmations for the guard mode")
	flag.IntVar(&o.capacity, "capacity", 1_000, "units on sale")
	flag.IntVar(&o.qty, "qty", 1, "units per attempt")
	flag.IntVar(&o.perUser, "per-user-limit", 4, "per-user cap")
	flag.IntVar(&o.concurrency, "concurrency", 1_000, "attempts in flight at once")
	flag.StringVar(&o.valkeyAddrs, "valkey", envOr("VALKEY_ADDRS", "localhost:6379"), "Valkey address(es)")
	flag.StringVar(&o.dsn, "dsn", os.Getenv("POSTGRES_DSN"), "PostgreSQL DSN for -mode guard and purchase")
	flag.IntVar(&o.purchases, "purchases", 2_000, "buyers attempting a whole purchase in -mode purchase")
	flag.Float64Var(&o.payFailure, "pay-failure", 0.1, "share of payments the provider fails in -mode purchase")
	flag.StringVar(&o.pspFaults, "psp-faults", "", `more mockpsp faults for -mode purchase, as its admin API's JSON, e.g. {"duplicateRate":0.2,"lossRate":0.05}`)
	flag.IntVar(&o.buyersPerUser, "buyers-per-user", 1, "buyers sharing one user ID in -mode purchase (above the per-user limit, the cap refuses some)")
	flag.BoolVar(&o.keep, "keep", false, "keep the test event's data afterwards")
	flag.StringVar(&o.jsonPath, "json", "", "also write the reports as JSON to this file")
	flag.Parse()

	if o.qty < 1 || o.qty > o.perUser || o.capacity < 1 || o.concurrency < 1 || o.purchases < 1 || o.payFailure < 0 || o.payFailure > 1 || o.buyersPerUser < 1 {
		fatal(errors.New("need 1 <= qty <= per-user-limit, capacity >= 1, concurrency >= 1, purchases >= 1, 0 <= pay-failure <= 1, buyers-per-user >= 1"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var reports []*Report
	if o.mode == "holds" || o.mode == "all" {
		r, err := runHolds(ctx, o)
		if err != nil {
			fatal(err)
		}
		reports = append(reports, r)
	}
	if o.mode == "guard" || o.mode == "all" {
		r, err := runGuard(ctx, o)
		if err != nil {
			fatal(err)
		}
		reports = append(reports, r)
	}
	if o.mode == "purchase" || o.mode == "all" {
		r, err := runPurchase(ctx, o)
		if err != nil {
			fatal(err)
		}
		reports = append(reports, r)
	}
	if len(reports) == 0 {
		fatal(fmt.Errorf("unknown -mode %q", o.mode))
	}

	ok := true
	for _, r := range reports {
		printReport(os.Stdout, r)
		ok = ok && r.passed()
	}
	if o.jsonPath != "" {
		data, _ := json.MarshalIndent(reports, "", "  ")
		if err := os.WriteFile(o.jsonPath, append(data, '\n'), 0o644); err != nil {
			fatal(err)
		}
	}
	if !ok {
		fmt.Println("RESULT: FAIL - an invariant was violated")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS - no oversell, no lost units, caps enforced")
}

// runHolds races o.buyers distinct buyers for o.capacity units through the
// inventory service (the same code path the HTTP handler uses).
func runHolds(ctx context.Context, o options) (*Report, error) {
	rdb, err := valkey.New(ctx, config.Valkey{
		Addrs: strings.Split(o.valkeyAddrs, ","), PoolSize: min(o.concurrency, 512),
		DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
	}, "holdfast-contention")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()

	store := inventory.NewStore(rdb)
	svc, err := inventory.NewService(store,
		inventory.Config{HoldTTL: 10 * time.Minute, PaymentWindow: 15 * time.Minute},
		inventory.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		return nil, err
	}
	eventID := uuid.Must(uuid.NewV7()).String()
	if _, err := svc.Provision(ctx, eventID, inventory.EventConfig{Capacity: o.capacity, PerUserLimit: o.perUser}); err != nil {
		return nil, err
	}
	if !o.keep {
		defer func() { _ = store.Purge(context.WithoutCancel(ctx), eventID) }()
	}

	type result struct {
		hold inventory.Hold
		err  error
	}
	results := make([]result, o.buyers)
	latencies := make([]time.Duration, o.buyers)
	start := time.Now()
	parallel(o.buyers, o.concurrency, func(i int) {
		t0 := time.Now()
		res, err := svc.CreateHold(ctx, inventory.CreateHoldRequest{
			EventID: eventID, UserID: uuid.NewString(),
			IdempotencyKey: fmt.Sprintf("e1-holds-%08d", i), Quantity: o.qty,
		})
		latencies[i] = time.Since(t0)
		results[i] = result{hold: res.Hold, err: err}
	})
	elapsed := time.Since(start)

	r := newReport("holds", o.buyers, o, elapsed, latencies)
	var held []inventory.Hold
	var firstErr error
	for _, res := range results {
		switch {
		case res.err == nil:
			r.Outcomes["held"]++
			held = append(held, res.hold)
		case errors.Is(res.err, inventory.ErrSoldOut):
			r.Outcomes["sold_out"]++
		default:
			r.Outcomes["error"]++
			if firstErr == nil {
				firstErr = res.err
			}
		}
	}

	expected := min(o.buyers, o.capacity/o.qty)
	avail, err := svc.Availability(ctx, eventID)
	if err != nil {
		return nil, err
	}
	readable := 0
	for _, h := range held {
		got, err := svc.GetHold(ctx, eventID, h.UserID, h.ID)
		if err == nil && got.State == inventory.StateHeld && got.Quantity == o.qty {
			readable++
		}
	}
	r.check("I1 holds granted == units available for sale", r.Outcomes["held"] == expected,
		fmt.Sprintf("held %d, expected %d", r.Outcomes["held"], expected))
	r.check("I1 pool never negative and fully accounted", avail.Available == o.capacity-r.Outcomes["held"]*o.qty && avail.Available >= 0,
		fmt.Sprintf("available %d = capacity %d - held units %d", avail.Available, o.capacity, r.Outcomes["held"]*o.qty))
	r.check("I5 every granted hold is stored as HELD", readable == len(held),
		fmt.Sprintf("%d of %d readable", readable, len(held)))
	r.check("no infrastructure errors", r.Outcomes["error"] == 0, errDetail(firstErr))
	return r, nil
}

// runGuard races confirmations through the PostgreSQL final guard, then
// checks the per-user cap under concurrency on a second event.
func runGuard(ctx context.Context, o options) (*Report, error) {
	if o.dsn == "" {
		return nil, errors.New("-mode guard needs -dsn or POSTGRES_DSN")
	}
	maxConns := int32(min(o.concurrency, 64))
	pool, err := postgres.NewPool(ctx, config.Postgres{
		DSN: o.dsn, MaxConns: maxConns, MinConns: 0, MaxConnLifetime: time.Hour,
		MaxConnIdleTime: time.Minute, StatementTimeout: 30 * time.Second,
	}, "holdfast-contention")
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, s := range migrations.All() {
		if _, err := postgres.Migrate(ctx, pool, s.Name, s.Files, quiet); err != nil {
			return nil, err
		}
	}

	newEvent := func(name string, capacity int) (uuid.UUID, error) {
		return catalog.Create(ctx, pool, catalog.NewEvent{
			Name: name, SaleOpensAt: time.Now(), PerUserLimit: o.perUser, UnitPricePaise: 100, Capacity: capacity,
		})
	}
	eventID, err := newEvent("E1 guard contention", o.capacity)
	if err != nil {
		return nil, err
	}
	capEventID, err := newEvent("E1 per-user cap", o.perUser*100)
	if err != nil {
		return nil, err
	}
	if !o.keep {
		defer func() {
			bg := context.WithoutCancel(ctx)
			_ = catalog.Delete(bg, pool, eventID)
			_ = catalog.Delete(bg, pool, capEventID)
		}()
	}

	errs := make([]error, o.guardAttempts)
	latencies := make([]time.Duration, o.guardAttempts)
	start := time.Now()
	parallel(o.guardAttempts, int(maxConns), func(i int) {
		t0 := time.Now()
		errs[i] = guard.ReserveTx(ctx, pool, guard.Reservation{
			EventID: eventID, UserID: uuid.New(), Qty: o.qty, PerUserLimit: o.perUser,
		})
		latencies[i] = time.Since(t0)
	})
	elapsed := time.Since(start)

	r := newReport("guard", o.guardAttempts, o, elapsed, latencies)
	r.Concurrency = int(maxConns)
	var firstErr error
	for _, err := range errs {
		switch {
		case err == nil:
			r.Outcomes["reserved"]++
		case errors.Is(err, guard.ErrSoldOut):
			r.Outcomes["sold_out"]++
		default:
			r.Outcomes["error"]++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	ev, err := catalog.Get(ctx, pool, eventID)
	if err != nil {
		return nil, err
	}
	var purchased int
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(sum(qty), 0) FROM booking.user_event_purchases WHERE event_id = $1`, eventID,
	).Scan(&purchased); err != nil {
		return nil, err
	}

	// Per-user cap: one user fires many concurrent single-unit confirmations.
	sameUser := uuid.New()
	capAttempts := o.perUser * 10
	capErrs := make([]error, capAttempts)
	parallel(capAttempts, int(maxConns), func(i int) {
		capErrs[i] = guard.ReserveTx(ctx, pool, guard.Reservation{
			EventID: capEventID, UserID: sameUser, Qty: 1, PerUserLimit: o.perUser,
		})
	})
	capOK, capRejected := 0, 0
	for _, err := range capErrs {
		switch {
		case err == nil:
			capOK++
		case errors.Is(err, guard.ErrUserCapExceeded):
			capRejected++
		}
	}

	expected := min(o.guardAttempts, o.capacity/o.qty)
	r.check("I1 reservations == units available for sale", r.Outcomes["reserved"] == expected,
		fmt.Sprintf("reserved %d, expected %d", r.Outcomes["reserved"], expected))
	r.check("I1 sold never exceeds capacity", ev.Sold <= ev.Capacity && ev.Sold == r.Outcomes["reserved"]*o.qty,
		fmt.Sprintf("sold %d of %d", ev.Sold, ev.Capacity))
	r.check("ledger consistent (sum of purchases == sold)", purchased == ev.Sold,
		fmt.Sprintf("purchases %d, sold %d", purchased, ev.Sold))
	r.check("I4 per-user cap holds under concurrency", capOK == o.perUser && capOK+capRejected == capAttempts,
		fmt.Sprintf("%d of %d concurrent attempts by one user accepted (cap %d)", capOK, capAttempts, o.perUser))
	r.check("no infrastructure errors", r.Outcomes["error"] == 0, errDetail(firstErr))
	return r, nil
}

// parallel runs fn(0..n-1) with at most workers invocations in flight.
func parallel(n, workers int, fn func(i int)) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < min(workers, n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				fn(i)
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

func newReport(mode string, attempts int, o options, elapsed time.Duration, lat []time.Duration) *Report {
	sorted := slices.Clone(lat)
	slices.Sort(sorted)
	pct := func(p float64) float64 {
		if len(sorted) == 0 {
			return 0
		}
		idx := int(p * float64(len(sorted)-1))
		return float64(sorted[idx].Microseconds()) / 1000
	}
	return &Report{
		Mode: mode, Attempts: attempts, Capacity: o.capacity, Qty: o.qty, Concurrency: o.concurrency,
		Outcomes:  map[string]int{},
		ElapsedMS: float64(elapsed.Microseconds()) / 1000,
		PerSecond: float64(attempts) / elapsed.Seconds(),
		LatencyMS: map[string]float64{"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99), "max": pct(1)},
	}
}

func (r *Report) check(name string, ok bool, detail string) {
	r.Checks = append(r.Checks, Check{Name: name, OK: ok, Detail: detail})
}

func printReport(w io.Writer, r *Report) {
	fmt.Fprintf(w, "\nE1 [%s] %d attempts -> %d units (qty %d, concurrency %d)\n",
		r.Mode, r.Attempts, r.Capacity, r.Qty, r.Concurrency)
	keys := make([]string, 0, len(r.Outcomes))
	for k := range r.Outcomes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "  %-12s %d\n", k, r.Outcomes[k])
	}
	fmt.Fprintf(w, "  %-12s %.0f ms (%.0f attempts/s)\n", "elapsed", r.ElapsedMS, r.PerSecond)
	fmt.Fprintf(w, "  %-12s p50 %.2f ms | p95 %.2f ms | p99 %.2f ms | max %.2f ms\n", "latency",
		r.LatencyMS["p50"], r.LatencyMS["p95"], r.LatencyMS["p99"], r.LatencyMS["max"])
	for _, c := range r.Checks {
		mark := "PASS"
		if !c.OK {
			mark = "FAIL"
		}
		fmt.Fprintf(w, "  [%s] %s (%s)\n", mark, c.Name, c.Detail)
	}
}

func errDetail(err error) string {
	if err == nil {
		return "none"
	}
	return "first error: " + err.Error()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "contention:", err)
	os.Exit(2)
}
