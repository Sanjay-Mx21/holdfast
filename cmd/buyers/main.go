// Command buyers simulates buyers making whole purchases through the running
// stack, for the chaos experiments E3 and E4 (task 5.5). Each buyer joins
// the waiting room, waits for its turn and claims it, holds units at
// inventory-svc, books them at booking-svc, pays at the payment provider's
// checkout (mockpsp), and waits for the booking to settle. Every request is
// repeated on failure with the same idempotency key, so a buyer whose
// request met a killed service or a Valkey failover never buys twice.
//
// It needs a stack whose queue-svc and booking-svc accept X-Dev-User-Id
// (DEV_IDENTITY) and ask no proof of work: chaos/compose.yaml.
//
//	go run ./cmd/buyers -event <id> -buyers 5000 -concurrency 200 -json out.json
//
// It prints how every buyer ended, the retries per stage, and how long paid
// bookings took to confirm. Whether the invariants held is the auditor's
// job (localhost:9097), after the stack has settled.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "buyers:", err)
		os.Exit(1)
	}
}

// Report is the run's result, also written as JSON with -json.
type Report struct {
	Event       string         `json:"event"`
	Buyers      int            `json:"buyers"`
	Concurrency int            `json:"concurrency"`
	Started     time.Time      `json:"started"`
	Seconds     float64        `json:"seconds"`
	Outcomes    map[string]int `json:"outcomes"`
	Retries     map[string]int `json:"retries"`
	// PayToConfirm summarises how long paid bookings took to be confirmed
	// (milliseconds): p50, p95, p99 and max.
	PayToConfirm map[string]int64 `json:"payToConfirmMs"`
}

func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("buyers", flag.ContinueOnError)
	o := &options{sleep: sleepCtx, retryAfterCapped: 5 * time.Second}
	fs.StringVar(&o.base, "base", "http://localhost:8088", "the edge (queue, inventory and booking APIs)")
	fs.StringVar(&o.event, "event", "", "event ID (required)")
	fs.IntVar(&o.qty, "qty", 1, "units each buyer holds")
	fs.IntVar(&o.attempts, "attempts", 30, "attempts per request, retries included")
	fs.DurationVar(&o.pollEvery, "poll", 2*time.Second, "time between claims while waiting for a turn, and between booking reads")
	fs.DurationVar(&o.admitDeadline, "admit-within", 20*time.Minute, "how long a buyer waits for its turn")
	fs.DurationVar(&o.settle, "settle", 6*time.Minute, "how long a paid booking may take to settle (delayed and lost webhooks take minutes)")
	fs.StringVar(&o.userNamespace, "users", "", "names this run's users (default: the start time), so a rerun brings new buyers")
	buyers := fs.Int("buyers", 1000, "buyers")
	concurrency := fs.Int("concurrency", 100, "buyers in flight at once")
	jsonOut := fs.String("json", "", "also write the report to this file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := uuid.Parse(o.event); err != nil {
		return fmt.Errorf("-event must be an event ID")
	}
	if *buyers < 1 || *concurrency < 1 || o.qty < 1 || o.attempts < 1 {
		return fmt.Errorf("-buyers, -concurrency, -qty and -attempts must be positive")
	}
	if o.userNamespace == "" {
		o.userNamespace = time.Now().UTC().Format(time.RFC3339Nano)
	}
	o.base = strings.TrimRight(o.base, "/")
	o.httpClient = &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{
		MaxIdleConns: *concurrency * 2, MaxIdleConnsPerHost: *concurrency * 2, IdleConnTimeout: time.Minute,
	}}

	st := newStats()
	start := time.Now()
	work := make(chan int)
	var wg sync.WaitGroup
	for range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				st.outcome(o.buy(ctx, st, userID(o.userNamespace, i)))
			}
		}()
	}
	progress := time.NewTicker(15 * time.Second)
	defer progress.Stop()
	go func() {
		for range progress.C {
			st.mu.Lock()
			done := 0
			for _, n := range st.outcomes {
				done += n
			}
			st.mu.Unlock()
			fmt.Fprintf(out, "%5.0fs  %d of %d buyers done\n", time.Since(start).Seconds(), done, *buyers)
		}
	}()
feed:
	for i := range *buyers {
		select {
		case work <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()

	r := Report{
		Event: o.event, Buyers: *buyers, Concurrency: *concurrency, Started: start.UTC(),
		Seconds: time.Since(start).Seconds(), Outcomes: st.outcomes, Retries: st.retries,
		PayToConfirm: percentiles(st.settle),
	}
	printReport(out, r)
	if *jsonOut != "" {
		b, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*jsonOut, append(b, '\n'), 0o600); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// userID is buyer i's user ID: stable within a run, new in the next.
func userID(namespace string, i int) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, fmt.Appendf(nil, "holdfast-buyers/%s/%d", namespace, i)).String()
}

func percentiles(ds []time.Duration) map[string]int64 {
	if len(ds) == 0 {
		return map[string]int64{}
	}
	slices.Sort(ds)
	at := func(q float64) int64 { return ds[min(len(ds)-1, int(q*float64(len(ds))))].Milliseconds() }
	return map[string]int64{"p50": at(0.5), "p95": at(0.95), "p99": at(0.99), "max": ds[len(ds)-1].Milliseconds()}
}

func printReport(w io.Writer, r Report) {
	fmt.Fprintf(w, "\n%d buyers for event %s in %.0f s (%d at a time)\n", r.Buyers, r.Event, r.Seconds, r.Concurrency)
	fmt.Fprintln(w, "outcomes:")
	for _, k := range slices.Sorted(maps.Keys(r.Outcomes)) {
		fmt.Fprintf(w, "  %-28s %6d\n", k, r.Outcomes[k])
	}
	fmt.Fprintln(w, "requests repeated after a failure, by stage:")
	if len(r.Retries) == 0 {
		fmt.Fprintln(w, "  none")
	}
	for _, k := range slices.Sorted(maps.Keys(r.Retries)) {
		fmt.Fprintf(w, "  %-28s %6d\n", k, r.Retries[k])
	}
	if p := r.PayToConfirm; len(p) > 0 {
		fmt.Fprintf(w, "payment to confirmed booking: p50 %d ms, p95 %d ms, p99 %d ms, max %d ms\n", p["p50"], p["p95"], p["p99"], p["max"])
	}
}
