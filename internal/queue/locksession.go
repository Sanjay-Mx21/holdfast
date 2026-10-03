package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errLockLost means the session that held a leader's lock is gone: the
// connection dropped or was replaced, so the lock was released.
var errLockLost = errors.New("queue: the leadership lock's session was lost")

// lockQueryTimeout bounds every query on the shared session. The queries run
// detached from the caller's context: a pgx query cancelled part-way can
// break its connection, and one controller shutting down must not take the
// other events' locks with it.
const lockQueryTimeout = 5 * time.Second

// LockSession holds a replica's admission-leader locks on one PostgreSQL
// connection. Advisory locks belong to a session and a session can hold many,
// so every event's leader in the replica shares it: queue-svc uses one
// connection for leadership however many events it runs, not one per event
// (P39). Losing the connection releases all of its locks at once, as a crash
// would; each leader notices through Alive and steps down, and the fencing
// epoch still refuses a stale leader's writes (ADR 0007).
type LockSession struct {
	pool      *pgxpool.Pool
	pingEvery time.Duration

	mu       sync.Mutex
	conn     *pgx.Conn
	gen      uint64 // changes whenever the connection is opened or dropped
	lastPing time.Time
}

// NewLockSession returns a session that opens its connection on first use.
// Leaders' liveness checks share at most one ping per pingEvery.
func NewLockSession(pool *pgxpool.Pool, pingEvery time.Duration) *LockSession {
	return &LockSession{pool: pool, pingEvery: pingEvery}
}

// TryLock tries to take key without waiting. It returns the session
// generation the lock belongs to, for Alive and Unlock.
func (s *LockSession) TryLock(ctx context.Context, key int64) (gen uint64, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.connect(ctx); err != nil {
		return 0, false, err
	}
	qctx, cancel := detached(ctx)
	defer cancel()
	if err := s.conn.QueryRow(qctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok); err != nil {
		s.drop()
		return 0, false, fmt.Errorf("try lock: %w", err)
	}
	return s.gen, ok, nil
}

// Alive reports whether a lock taken in generation gen is still held: the
// session is the same one and answers a ping.
func (s *LockSession) Alive(ctx context.Context, gen uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || s.gen != gen {
		return errLockLost
	}
	if time.Since(s.lastPing) < s.pingEvery {
		return nil
	}
	qctx, cancel := detached(ctx)
	defer cancel()
	if err := s.conn.Ping(qctx); err != nil {
		s.drop()
		return fmt.Errorf("%w: %w", errLockLost, err)
	}
	s.lastPing = time.Now()
	return nil
}

// Unlock releases key, if the session is still generation gen. Advisory locks
// stack within a session, so an unlock that fails drops the whole session
// rather than risk leaving the lock held.
func (s *LockSession) Unlock(ctx context.Context, key int64, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || s.gen != gen {
		return // already released with its session
	}
	qctx, cancel := detached(ctx)
	defer cancel()
	var released bool
	if err := s.conn.QueryRow(qctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&released); err != nil || !released {
		s.drop()
	}
}

// Close releases every lock the session holds.
func (s *LockSession) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drop()
}

// connect opens the connection if there is none. The caller holds mu.
func (s *LockSession) connect(ctx context.Context) error {
	if s.conn != nil {
		return nil
	}
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	// Hijacked: the locks belong to this session, so it must never go back to
	// the pool while they are held.
	s.conn = pc.Hijack()
	s.gen++
	s.lastPing = time.Now()
	return nil
}

// drop closes the connection, which releases all of its locks. The caller
// holds mu.
func (s *LockSession) drop() {
	if s.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), lockQueryTimeout)
	defer cancel()
	_ = s.conn.Close(ctx)
	s.conn = nil
	s.gen++
}

func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), lockQueryTimeout)
}
