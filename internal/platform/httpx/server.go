package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

// Server is an http.Server with production timeouts that implements
// app.Component: it serves until its context is cancelled, then shuts down
// gracefully, letting in-flight requests finish within the shutdown timeout.
type Server struct {
	name            string
	srv             *http.Server
	log             *slog.Logger
	shutdownTimeout time.Duration
}

// ServerOption customises the underlying http.Server.
type ServerOption func(*http.Server)

// WithWriteTimeout overrides the write timeout (the admin server needs a long
// one so 30-second CPU profiles can complete).
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(s *http.Server) { s.WriteTimeout = d }
}

// NewServer builds a server named name (used in logs) listening on addr.
func NewServer(name, addr string, h http.Handler, cfg config.HTTP, log *slog.Logger, opts ...ServerOption) *Server {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    1 << 16,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	for _, o := range opts {
		o(srv)
	}
	return &Server{name: name, srv: srv, log: log, shutdownTimeout: cfg.ShutdownTimeout}
}

// Name implements app.Component.
func (s *Server) Name() string { return s.name + "-http" }

// Run implements app.Component.
func (s *Server) Run(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.srv.Addr) // listen first so a busy port fails fast
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.srv.Addr, err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(ln) }()
	s.log.Info("http server listening", "server", s.name, "addr", ln.Addr().String())

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}
