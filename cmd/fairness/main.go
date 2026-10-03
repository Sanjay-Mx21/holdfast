// Command fairness is experiment E6. It checks the waiting room's fairness
// promise (invariant F1) statistically, through the real queue service and
// its Lua scripts in Valkey:
//
//  1. -pre users join concurrently before T0, each with its send time
//     recorded. Their positions are a lottery, so Spearman's rank correlation
//     between join time and position must be close to 0: joining early buys
//     nothing.
//  2. Once Valkey's clock passes T0, -post users join one at a time. Their
//     positions are their arrival order, so the correlation must be exactly
//     1, and every one of them must stand behind every lottery joiner.
//  3. -post-concurrent more users join concurrently after T0. Their send
//     times are taken by many clients at once, so they can disagree with the
//     order Valkey received them in; the correlation is reported, not judged.
//
// It exits with status 1 if a check fails.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
	"github.com/Sanjay-Mx21/holdfast/internal/policy"
	"github.com/Sanjay-Mx21/holdfast/internal/queue"
	"github.com/Sanjay-Mx21/holdfast/internal/stats"
)

type options struct {
	pre            int
	post           int
	postConcurrent int
	concurrency    int
	t0In           time.Duration
	valkeyAddrs    string
	keep           bool
	jsonPath       string
}

// Check is one pass criterion and whether it held.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// Phase is what one group of joiners got.
type Phase struct {
	Name        string             `json:"name"`
	Joins       int                `json:"joins"`
	Concurrency int                `json:"concurrency"`
	ElapsedMS   float64            `json:"elapsedMs"`
	PerSecond   float64            `json:"joinsPerSecond"`
	LatencyMS   map[string]float64 `json:"latencyMs"`
	Spearman    float64            `json:"spearman"`
	// FirstDecileStay is the share of the earliest 10% of joiners who stand
	// in the first 10% of positions: 0.1 for a fair lottery, 1 for FIFO.
	FirstDecileStay float64 `json:"firstDecileStay"`
	MinRank         int64   `json:"minRank"`
	MaxRank         int64   `json:"maxRank"`
}

// Report is the outcome of the experiment.
type Report struct {
	EventID string  `json:"eventId"`
	Started string  `json:"started"`
	Phases  []Phase `json:"phases"`
	Checks  []Check `json:"checks"`
}

func (r *Report) check(name string, ok bool, detail string) {
	r.Checks = append(r.Checks, Check{Name: name, OK: ok, Detail: detail})
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
	flag.IntVar(&o.pre, "pre", 100_000, "users who join before T0, concurrently")
	flag.IntVar(&o.post, "post", 10_000, "users who join after T0, one at a time")
	flag.IntVar(&o.postConcurrent, "post-concurrent", 10_000, "users who join after T0 concurrently (reported only)")
	flag.IntVar(&o.concurrency, "concurrency", 256, "joins in flight at once for the concurrent phases")
	flag.DurationVar(&o.t0In, "t0-in", 30*time.Second, "how far ahead T0 is set; the pre-T0 joins must finish before it")
	flag.StringVar(&o.valkeyAddrs, "valkey", envOr("VALKEY_ADDRS", "localhost:6379"), "Valkey address(es)")
	flag.BoolVar(&o.keep, "keep", false, "keep the test event's data afterwards")
	flag.StringVar(&o.jsonPath, "json", "", "also write the report as JSON to this file")
	flag.Parse()
	if o.pre < 10 || o.post < 10 || o.postConcurrent < 0 || o.concurrency < 1 {
		fatal(errors.New("need -pre >= 10, -post >= 10, -post-concurrent >= 0, -concurrency >= 1"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r, err := run(ctx, o)
	if err != nil {
		fatal(err)
	}
	printReport(os.Stdout, r)
	if o.jsonPath != "" {
		data, _ := json.MarshalIndent(r, "", "  ")
		if err := os.WriteFile(o.jsonPath, append(data, '\n'), 0o644); err != nil {
			fatal(err)
		}
	}
	if !r.passed() {
		fmt.Println("RESULT: FAIL - the queue's order is not fair")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS - lottery before T0, arrival order after it")
}

// joiner is one simulated user.
type joiner struct {
	user     string
	sentAt   time.Time
	latency  time.Duration
	ordering queue.Ordering
	err      error
	rank     int64
}

func run(ctx context.Context, o options) (*Report, error) {
	rdb, err := valkey.New(ctx, config.Valkey{
		Addrs: strings.Split(o.valkeyAddrs, ","), PoolSize: min(o.concurrency, 512),
		DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
	}, "holdfast-fairness")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()
	store := queue.NewStore(rdb)
	if err := store.LoadScripts(ctx); err != nil {
		return nil, err
	}
	svc := queue.NewService(store)

	eventID := uuid.Must(uuid.NewV7()).String()
	opensAt := time.Now().Add(o.t0In).Truncate(time.Millisecond)
	if _, err := svc.Provision(ctx, eventID, queue.EventConfig{
		OpensAt: opensAt, AdmissionRate: 1, MaxSessions: 1, SessionTTL: time.Minute,
	}); err != nil {
		return nil, err
	}
	if !o.keep {
		defer func() { _ = store.Purge(context.WithoutCancel(ctx), eventID) }()
	}
	r := &Report{EventID: eventID, Started: time.Now().UTC().Format(time.RFC3339)}

	join := func(j *joiner) {
		j.sentAt = time.Now()
		res, err := svc.Join(ctx, eventID, j.user, policy.Buyer{})
		j.latency = time.Since(j.sentAt)
		j.ordering, j.err = res.Ordering, err
	}
	newJoiners := func(n int) []*joiner {
		js := make([]*joiner, n)
		for i := range js {
			js[i] = &joiner{user: uuid.NewString()}
		}
		return js
	}

	// 1. Before T0, concurrently.
	pre := newJoiners(o.pre)
	start := time.Now()
	parallel(len(pre), o.concurrency, func(i int) { join(pre[i]) })
	preElapsed := time.Since(start)
	if time.Now().After(opensAt) {
		return nil, fmt.Errorf("the pre-T0 joins took %s, past T0 (-t0-in %s); raise -t0-in", preElapsed.Round(time.Millisecond), o.t0In)
	}

	// 2. Wait for T0 by Valkey's clock, the clock the scripts use.
	for {
		now, err := rdb.Time(ctx).Result()
		if err != nil {
			return nil, err
		}
		if !now.Before(opensAt) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(opensAt.Sub(now), time.Second)):
		}
	}

	// 3. After T0: one at a time, then concurrently.
	post := newJoiners(o.post)
	start = time.Now()
	for _, j := range post {
		join(j)
	}
	postElapsed := time.Since(start)
	postC := newJoiners(o.postConcurrent)
	start = time.Now()
	parallel(len(postC), o.concurrency, func(i int) { join(postC[i]) })
	postCElapsed := time.Since(start)

	// Everyone's position, read through the service as a client would.
	all := slices.Concat(pre, post, postC)
	var mu sync.Mutex
	var posErr error
	parallel(len(all), o.concurrency, func(i int) {
		p, err := svc.Position(ctx, eventID, all[i].user)
		if err != nil {
			mu.Lock()
			posErr = errors.Join(posErr, err)
			mu.Unlock()
			return
		}
		all[i].rank = p.Rank
	})
	if posErr != nil {
		return nil, fmt.Errorf("read positions: %w", posErr)
	}

	prePhase := phase("before T0 (concurrent)", pre, o.concurrency, preElapsed)
	postPhase := phase("after T0 (one at a time)", post, 1, postElapsed)
	r.Phases = append(r.Phases, prePhase, postPhase)
	if len(postC) >= 2 {
		r.Phases = append(r.Phases, phase("after T0 (concurrent, reported only)", postC, o.concurrency, postCElapsed))
	}

	errs := 0
	for _, j := range all {
		if j.err != nil {
			errs++
		}
	}
	r.check("every join succeeded", errs == 0, fmt.Sprintf("%d errors", errs))
	r.check("every pre-T0 joiner got a lottery position", allOrdering(pre, queue.OrderingLottery), "")
	r.check("every post-T0 joiner got an arrival-order position", allOrdering(post, queue.OrderingFIFO) && allOrdering(postC, queue.OrderingFIFO), "")
	limit := 4 / math.Sqrt(float64(len(pre)-1)) // four standard errors of rho under independence
	r.check("pre-T0: join time does not predict position",
		math.Abs(prePhase.Spearman) < limit,
		fmt.Sprintf("Spearman %.5f, want |rho| < %.5f (4 standard errors for n = %d)", prePhase.Spearman, limit, len(pre)))
	r.check("post-T0: position is exactly the arrival order",
		postPhase.Spearman == 1,
		fmt.Sprintf("Spearman %v, want exactly 1", postPhase.Spearman))
	r.check("every post-T0 joiner stands behind every pre-T0 joiner",
		prePhase.MaxRank == int64(len(pre)) && postPhase.MinRank == int64(len(pre))+1,
		fmt.Sprintf("pre-T0 ranks %d to %d, post-T0 ranks from %d", prePhase.MinRank, prePhase.MaxRank, postPhase.MinRank))
	return r, nil
}

// phase summarises one group: speed, latency, and how join time relates to
// position.
func phase(name string, js []*joiner, concurrency int, elapsed time.Duration) Phase {
	lat := make([]time.Duration, len(js))
	sent := make([]float64, len(js))
	ranks := make([]float64, len(js))
	p := Phase{Name: name, Joins: len(js), Concurrency: concurrency,
		ElapsedMS: float64(elapsed.Microseconds()) / 1000, PerSecond: float64(len(js)) / elapsed.Seconds(),
		MinRank: math.MaxInt64}
	for i, j := range js {
		lat[i] = j.latency
		sent[i] = float64(j.sentAt.UnixNano())
		ranks[i] = float64(j.rank)
		p.MinRank, p.MaxRank = min(p.MinRank, j.rank), max(p.MaxRank, j.rank)
	}
	slices.Sort(lat)
	pct := func(q float64) float64 { return float64(lat[int(q*float64(len(lat)-1))].Microseconds()) / 1000 }
	p.LatencyMS = map[string]float64{"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99), "max": pct(1)}
	p.Spearman, _ = stats.Spearman(sent, ranks)

	// Of the earliest tenth of joiners (by send time), how many stand in the
	// first tenth of this group's positions?
	tenth := len(js) / 10
	bySent := slices.Clone(js)
	slices.SortFunc(bySent, func(a, b *joiner) int { return a.sentAt.Compare(b.sentAt) })
	stay := 0
	for _, j := range bySent[:tenth] {
		if j.rank-p.MinRank < int64(tenth) {
			stay++
		}
	}
	if tenth > 0 {
		p.FirstDecileStay = float64(stay) / float64(tenth)
	}
	return p
}

func allOrdering(js []*joiner, want queue.Ordering) bool {
	for _, j := range js {
		if j.err == nil && j.ordering != want {
			return false
		}
	}
	return true
}

func parallel(n, workers int, fn func(i int)) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range min(workers, n) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				fn(i)
			}
		}()
	}
	for i := range n {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

func printReport(w io.Writer, r *Report) {
	fmt.Fprintf(w, "\nE6 fairness, event %s\n", r.EventID)
	for _, p := range r.Phases {
		fmt.Fprintf(w, "  %s\n", p.Name)
		fmt.Fprintf(w, "    %-16s %d in %.0f ms (%.0f joins/s, concurrency %d)\n", "joins", p.Joins, p.ElapsedMS, p.PerSecond, p.Concurrency)
		fmt.Fprintf(w, "    %-16s p50 %.2f ms | p95 %.2f ms | p99 %.2f ms | max %.2f ms\n", "join latency",
			p.LatencyMS["p50"], p.LatencyMS["p95"], p.LatencyMS["p99"], p.LatencyMS["max"])
		fmt.Fprintf(w, "    %-16s %d to %d\n", "positions", p.MinRank, p.MaxRank)
		fmt.Fprintf(w, "    %-16s %.6f\n", "Spearman rho", p.Spearman)
		fmt.Fprintf(w, "    %-16s %.1f%% of the earliest tenth stand in the first tenth (lottery: 10%%, FIFO: 100%%)\n",
			"first decile", 100*p.FirstDecileStay)
	}
	fmt.Fprintln(w, "  checks")
	for _, c := range r.Checks {
		mark := "ok  "
		if !c.OK {
			mark = "FAIL"
		}
		fmt.Fprintf(w, "    %s %s", mark, c.Name)
		if c.Detail != "" {
			fmt.Fprintf(w, ": %s", c.Detail)
		}
		fmt.Fprintln(w)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fairness:", err)
	os.Exit(2)
}
