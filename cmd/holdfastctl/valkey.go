package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// cmdValkeyProbe writes to Valkey through the services' own client every
// --every for --for, and reports when writes stop and resume, which server
// takes them, and whether any acknowledged write was lost: the measurement
// behind the failover drill (task 5.2, docs/runbooks/valkey.md). Run it
// where the services run, so Sentinel's answers resolve:
//
//	docker compose run --rm -T holdfastctl valkey probe --for 90s
func cmdValkeyProbe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("valkey probe", flag.ContinueOnError)
	vk := valkeyFlag(fs)
	dur := fs.Duration("for", time.Minute, "how long to probe")
	every := fs.Duration("every", 100*time.Millisecond, "time between writes")
	timeout := fs.Duration("timeout", 500*time.Millisecond, "deadline of each write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dur <= 0 || *every <= 0 || *timeout <= 0 {
		return fmt.Errorf("--for, --every and --timeout must be positive")
	}
	rdb, err := openValkey(ctx, *vk)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	// One counter per run: INCR's answer says whether earlier increments
	// survived. It expires by itself if the probe is killed.
	key := "drill:probe:" + uuid.NewString()
	defer rdb.Del(context.WithoutCancel(ctx), key)
	p := &probe{out: stdout, start: time.Now()}
	fmt.Fprintf(stdout, "probing %s every %s for %s\n", *vk, *every, *dur)

	tick := time.NewTicker(*every)
	defer tick.Stop()
	end := time.After(*dur)
	for {
		select {
		case <-ctx.Done():
			p.summary()
			return ctx.Err()
		case <-end:
			p.summary()
			return nil
		case <-tick.C:
			v, server, err := probeWrite(ctx, rdb, key, *timeout)
			p.observe(time.Now(), v, server, err)
		}
	}
}

// probeWrite increments key and reads the answering server's run ID, in one
// round trip to whichever server the client takes for the primary.
func probeWrite(ctx context.Context, rdb redis.UniversalClient, key string, timeout time.Duration) (int64, string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var incr *redis.IntCmd
	var info *redis.StringCmd
	_, err := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		incr = p.Incr(ctx, key)
		p.Expire(ctx, key, time.Hour)
		info = p.Info(ctx, "server")
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	return incr.Val(), runID(info.Val()), nil
}

// runID picks run_id out of INFO's text; it changes when another server
// answers (or the same one restarted).
func runID(info string) string {
	for line := range strings.SplitSeq(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "run_id:"); ok {
			return v
		}
	}
	return "unknown"
}

// probe follows the writes' outcomes and reports changes as they happen.
type probe struct {
	out   io.Writer
	start time.Time

	writes, failed int
	lastAck        int64     // the counter's value at the last acknowledged write
	lost           int64     // acknowledged increments that later disappeared
	server         string    // the run ID that answered last
	down           time.Time // when writes began failing; zero while they succeed
	outages        []time.Duration
}

func (p *probe) at(now time.Time) string {
	return fmt.Sprintf("t+%6.2fs", now.Sub(p.start).Seconds())
}

// observe records one write: v is the counter after it, server who took it.
func (p *probe) observe(now time.Time, v int64, server string, err error) {
	p.writes++
	if err != nil {
		p.failed++
		if p.down.IsZero() {
			p.down = now
			fmt.Fprintf(p.out, "%s writes failing: %v\n", p.at(now), err)
		}
		return
	}
	if !p.down.IsZero() {
		gap := now.Sub(p.down)
		p.outages = append(p.outages, gap)
		p.down = time.Time{}
		fmt.Fprintf(p.out, "%s writes resumed after %s\n", p.at(now), gap.Round(time.Millisecond))
	}
	if server != p.server {
		if p.server != "" {
			fmt.Fprintf(p.out, "%s now written to server %s (was %s)\n", p.at(now), short(server), short(p.server))
		}
		p.server = server
	}
	// A write that timed out may still have applied, so the counter can jump
	// ahead; it can only go back if acknowledged increments were lost.
	if p.lastAck > 0 && v <= p.lastAck {
		n := p.lastAck - v + 1
		p.lost += n
		fmt.Fprintf(p.out, "%s %d acknowledged writes lost (counter back from %d to %d)\n", p.at(now), n, p.lastAck, v)
	}
	p.lastAck = v
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// summary prints the totals and a RESULT line for scripts.
func (p *probe) summary() {
	longest := time.Duration(0)
	for _, o := range p.outages {
		longest = max(longest, o)
	}
	still := ""
	if !p.down.IsZero() {
		still = " (writes still failing at the end)"
	}
	fmt.Fprintf(p.out, "%d writes, %d failed, %d outages, longest %s, %d acknowledged writes lost%s\n",
		p.writes, p.failed, len(p.outages), longest.Round(time.Millisecond), p.lost, still)
	fmt.Fprintf(p.out, "RESULT writes=%d failed=%d outages=%d longest_outage_ms=%d lost=%d recovered=%t\n",
		p.writes, p.failed, len(p.outages), longest.Milliseconds(), p.lost, p.down.IsZero())
}
