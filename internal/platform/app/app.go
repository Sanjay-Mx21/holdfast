// Package app runs a service's long-lived components (HTTP servers, background
// workers) and coordinates graceful shutdown.
//
// Shutdown sequence on SIGTERM:
//  1. readiness turns false (load balancers stop sending new traffic);
//  2. wait the drain delay so in-flight routing updates propagate;
//  3. cancel every component and wait for all of them to return.
//
// If any component fails, the others are stopped and Run returns the error.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// Component is a unit of work that runs until its context is cancelled.
// Run must return nil on a clean stop.
type Component interface {
	Name() string
	Run(ctx context.Context) error
}

// Drainer is told when shutdown begins (typically *health.Health).
type Drainer interface {
	SetDraining()
}

// Run blocks until a termination signal arrives or a component fails.
func Run(ctx context.Context, log *slog.Logger, drainer Drainer, drainDelay time.Duration, components ...Component) error {
	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	g, gctx := errgroup.WithContext(runCtx)
	for _, c := range components {
		g.Go(func() error {
			log.Info("component starting", "component", c.Name())
			if err := c.Run(gctx); err != nil {
				return fmt.Errorf("%s: %w", c.Name(), err)
			}
			log.Info("component stopped", "component", c.Name())
			return nil
		})
	}

	select {
	case <-sigCtx.Done():
		log.Info("shutdown signal received; draining", "drain_delay", drainDelay.String())
		if drainer != nil {
			drainer.SetDraining()
		}
		select {
		case <-time.After(drainDelay):
		case <-gctx.Done():
		}
	case <-gctx.Done():
		log.Error("a component stopped unexpectedly; shutting down")
	}

	cancel()
	return g.Wait()
}

// Func adapts a function to Component.
type Func struct {
	ComponentName string
	RunFunc       func(ctx context.Context) error
}

// Name implements Component.
func (f Func) Name() string { return f.ComponentName }

// Run implements Component.
func (f Func) Run(ctx context.Context) error { return f.RunFunc(ctx) }
