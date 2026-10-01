// Package valkey builds the Valkey/Redis client (standalone, Sentinel or
// Cluster, chosen by configuration) and its readiness check.
package valkey

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
)

// New connects and verifies the server answers PING.
func New(ctx context.Context, cfg config.Valkey, clientName string) (redis.UniversalClient, error) {
	c := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        cfg.Addrs,
		MasterName:   cfg.MasterName,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		ClientName:   clientName,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("valkey: ping %v: %w", cfg.Addrs, err)
	}
	return c, nil
}

// Check returns a readiness check for c.
func Check(c redis.UniversalClient) health.Check {
	return health.Check{Name: "valkey", Fn: func(ctx context.Context) error { return c.Ping(ctx).Err() }}
}
