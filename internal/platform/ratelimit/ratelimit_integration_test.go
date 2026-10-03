//go:build integration

package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

func newLimiter(t *testing.T) (*Limiter, string) {
	t.Helper()
	rdb := testenv.Valkey(t)
	l := New(rdb)
	if err := l.Load(ctx); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), Key("test", id)).Err() })
	return l, id
}

func TestBurstThenRefuseWithRetryAfter(t *testing.T) {
	l, id := newLimiter(t)
	rule := Rule{Capacity: 3, Rate: 1}
	for i := range 3 {
		d, err := l.Allow(ctx, "test", id, rule)
		if err != nil || !d.Allowed {
			t.Fatalf("request %d: %+v, %v; want allowed", i+1, d, err)
		}
		if d.Remaining != 2-i {
			t.Fatalf("request %d: remaining %d, want %d", i+1, d.Remaining, 2-i)
		}
	}
	d, err := l.Allow(ctx, "test", id, rule)
	if err != nil || d.Allowed {
		t.Fatalf("4th request: %+v, %v; want refused", d, err)
	}
	if d.RetryAfter <= 0 || d.RetryAfter > time.Second {
		t.Fatalf("retry after %s, want within (0, 1s] at 1 token/s", d.RetryAfter)
	}
}

func TestRefillsOverTime(t *testing.T) {
	l, id := newLimiter(t)
	// One token every 500 ms: wide enough that a loaded machine still makes
	// the second call well inside it (at 50 ms it did not: P37).
	rule := Rule{Capacity: 1, Rate: 2}
	if d, _ := l.Allow(ctx, "test", id, rule); !d.Allowed {
		t.Fatal("first request refused")
	}
	if d, _ := l.Allow(ctx, "test", id, rule); d.Allowed {
		t.Fatal("second immediate request allowed")
	}
	time.Sleep(600 * time.Millisecond)
	if d, err := l.Allow(ctx, "test", id, rule); err != nil || !d.Allowed {
		t.Fatalf("after refill: %+v, %v; want allowed", d, err)
	}
}

func TestBucketsAreIndependent(t *testing.T) {
	l, id := newLimiter(t)
	other := uuid.NewString()
	t.Cleanup(func() { _ = l.rdb.Del(context.Background(), Key("test", other)).Err() })
	rule := Rule{Capacity: 1, Rate: 0.1}
	if d, _ := l.Allow(ctx, "test", id, rule); !d.Allowed {
		t.Fatal("first client refused")
	}
	if d, _ := l.Allow(ctx, "test", other, rule); !d.Allowed {
		t.Fatal("second client refused because of the first client's bucket")
	}
}

func TestConcurrentRequestsNeverOverspend(t *testing.T) {
	l, id := newLimiter(t)
	rule := Rule{Capacity: 25, Rate: 0.01} // effectively no refill during the test
	var allowed, refused atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := l.Allow(ctx, "test", id, rule)
			switch {
			case err != nil:
				t.Error(err)
			case d.Allowed:
				allowed.Add(1)
			default:
				refused.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 25 || refused.Load() != 175 {
		t.Fatalf("allowed %d, refused %d; want exactly 25 and 175", allowed.Load(), refused.Load())
	}
}

func TestBucketExpiresOnceFull(t *testing.T) {
	l, id := newLimiter(t)
	rule := Rule{Capacity: 10, Rate: 5} // full again after 2 s
	if _, err := l.Allow(ctx, "test", id, rule); err != nil {
		t.Fatal(err)
	}
	ttl, err := l.rdb.PTTL(ctx, Key("test", id)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 2*time.Second || ttl > 3*time.Second {
		t.Fatalf("TTL %s, want just over the 2 s full-refill time", ttl)
	}
}

func TestRejectsUnsafeKeys(t *testing.T) {
	l, _ := newLimiter(t)
	for _, id := range []string{"", "a:b", "{x}"} {
		if _, err := l.Allow(ctx, "test", id, Rule{Capacity: 1, Rate: 1}); err == nil {
			t.Errorf("Allow with id %q succeeded, want an error", id)
		}
	}
}
