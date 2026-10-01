// Package ratelimit is a token-bucket rate limiter whose state lives in
// Valkey, so every replica of a service shares the same buckets.
//
// A bucket holds up to Capacity tokens and refills at Rate tokens per second.
// Each request takes one token; a request that finds the bucket empty is
// refused and told when a token will be available. Capacity is the burst a
// client may send at once; Rate is its sustained request rate.
package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed token_bucket.lua
var tokenBucketSrc string

var tokenBucket = redis.NewScript(tokenBucketSrc)

// Rule configures one family of buckets.
type Rule struct {
	// Capacity is the burst size: the most requests allowed at once.
	Capacity int
	// Rate is the sustained rate, in tokens per second.
	Rate float64
}

// Validate checks the rule's bounds.
func (r Rule) Validate() error {
	if r.Capacity < 1 || r.Capacity > 1_000_000 {
		return errors.New("ratelimit: capacity must be between 1 and 1000000")
	}
	if r.Rate <= 0 || r.Rate > 1_000_000 {
		return errors.New("ratelimit: rate must be above 0 and at most 1000000 per second")
	}
	return nil
}

// Decision is the outcome of one Allow call.
type Decision struct {
	Allowed bool
	// Remaining is the number of whole tokens left after this request.
	Remaining int
	// RetryAfter is how long until a token is available; zero when allowed.
	RetryAfter time.Duration
}

// Limiter takes tokens from Valkey-backed buckets.
type Limiter struct {
	rdb redis.UniversalClient
}

// New returns a Limiter backed by rdb.
func New(rdb redis.UniversalClient) *Limiter { return &Limiter{rdb: rdb} }

// Load preloads the script (SCRIPT LOAD); Script.Run falls back to EVAL
// anyway, so this is an optimisation.
func (l *Limiter) Load(ctx context.Context) error {
	if err := tokenBucket.Load(ctx, l.rdb).Err(); err != nil {
		return fmt.Errorf("ratelimit: load script: %w", err)
	}
	return nil
}

// Allow takes one token from the bucket for (scope, id) under rule. scope
// names the limit (for example "join-ip") and must be a fixed string; id is
// the client being limited. Neither may contain ':' or braces, so a client
// cannot reach another client's key.
func (l *Limiter) Allow(ctx context.Context, scope, id string, rule Rule) (Decision, error) {
	if err := rule.Validate(); err != nil {
		return Decision{}, err
	}
	if !validPart(scope) || !validPart(id) {
		return Decision{}, fmt.Errorf("ratelimit: invalid scope %q or id", scope)
	}
	res, err := tokenBucket.Run(ctx, l.rdb, []string{Key(scope, id)}, rule.Capacity, rule.Rate).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimit: %w", err)
	}
	if len(res) != 3 {
		return Decision{}, fmt.Errorf("ratelimit: unexpected reply %v", res)
	}
	return Decision{
		Allowed:    res[0] == 1,
		Remaining:  int(res[1] / 1000),
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
	}, nil
}

// Key returns the Valkey key of the bucket for (scope, id).
func Key(scope, id string) string { return "rl:" + scope + ":" + id }

func validPart(s string) bool {
	return s != "" && len(s) <= 128 && !strings.ContainsAny(s, ":{} \t\r\n")
}
