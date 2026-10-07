package inventory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// replyError is a Valkey error reply, as go-redis returns one.
type replyError string

func (e replyError) Error() string { return string(e) }
func (replyError) RedisError()     {}

// P57: what a failover throws at inventory is "unavailable", over HTTP (503)
// and gRPC (UNAVAILABLE), so callers retry it; anything else stays internal.
func TestFailoverErrorsAreUnavailable(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{context.DeadlineExceeded, true},
		{redis.ErrPoolTimeout, true},
		{fmt.Errorf("inventory: hold: %w", io.EOF), true}, // a connection dropped at the switch
		{&net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{replyError("READONLY You can't write against a read only replica."), true},
		{replyError("LOADING Valkey is loading the dataset in memory"), true},
		{replyError("MASTERDOWN Link with MASTER is down"), true},
		{replyError("ERR unknown command"), false},
		{errors.New("something else"), false},
	} {
		if got := isUnavailable(tt.err); got != tt.want {
			t.Errorf("isUnavailable(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}

	if c := status.Code(toStatus(fmt.Errorf("confirm: %w", io.EOF))); c != codes.Unavailable {
		t.Errorf("a dropped connection over gRPC: %v, want Unavailable", c)
	}
	if c := status.Code(toStatus(errors.New("a bug"))); c != codes.Internal {
		t.Errorf("an unexpected error over gRPC: %v, want Internal", c)
	}
}
