// Package app assembles the supervisor and the HTTP server into a complete daemon runtime.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gosup/internal/config"
	"gosup/internal/logx"
	"gosup/internal/rpc"
	"gosup/internal/supervisor"
)

// Daemon is the complete daemon runtime: supervisor + HTTP server + signal handling.
type Daemon struct {
	cfg *config.Config
	log *logx.Logger
	mgr *supervisor.Manager
	api *rpc.Server
	srv *http.Server

	shutdownOnce sync.Once
}

// New creates the runtime and starts listening on the HTTP port.
func New(cfg *config.Config, log *logx.Logger) (*Daemon, error) {
	mgr, err := supervisor.New(cfg, log)
	if err != nil {
		return nil, err
	}
	d := &Daemon{cfg: cfg, log: log, mgr: mgr}

	api := rpc.New(mgr, log)
	d.api = api
	ln, err := net.Listen("tcp", cfg.Daemon.Listen)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", cfg.Daemon.Listen, err)
	}
	scheme := "http"
	if cfg.Daemon.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Daemon.TLSCert, cfg.Daemon.TLSKey)
		if err != nil {
			ln.Close()
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}
		ln = tls.NewListener(ln, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
		scheme = "https"
	}
	d.srv = &http.Server{
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return mgr.Context() },
	}
	go func() {
		log.Infof("daemon", "HTTP server started: %s://%s (POST /rpc, GET /events)", scheme, ln.Addr())
		if err := d.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("daemon", "HTTP server exited unexpectedly: %v", err)
		}
	}()
	return d, nil
}

// Manager returns the supervisor.
func (d *Daemon) Manager() *supervisor.Manager { return d.mgr }

// Reload re-reads the config file and applies it; when restart is true, running programs whose config changed are restarted.
func (d *Daemon) Reload(restart bool) (supervisor.ReloadResult, error) {
	cfg, err := d.mgr.ReloadFromDisk()
	if err != nil {
		return supervisor.ReloadResult{}, err
	}
	return d.mgr.Reload(cfg, restart)
}

// Run starts all programs and blocks until SIGTERM/SIGINT/SIGQUIT is received.
func (d *Daemon) Run() error {
	d.log.Infof("daemon", "gosup started, pid=%d, config file %s, managing %d programs",
		os.Getpid(), d.cfg.Path, len(d.cfg.Programs))
	d.mgr.Start()

	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGHUP)
	for {
		sig := <-sigCh
		if sig == syscall.SIGHUP {
			// SIGHUP: reload the config. If the config is malformed, keep running with the old one.
			d.log.Infof("daemon", "received SIGHUP, re-reading config %s", d.cfg.Path)
			res, err := d.Reload(false)
			if err != nil {
				d.log.Errorf("daemon", "config reload failed, keeping the original config: %v", err)
				continue
			}
			d.log.Infof("daemon", "config reloaded: added %v, removed %v, updated %v, pending %v, fields requiring a daemon restart %v",
				res.Added, res.Removed, res.Updated, res.PendingRestart, res.RequiresRestart)
			continue
		}
		// signal.Notify only ever delivers syscall.Signal on Unix, but keep the
		// fallback so a future non-Unix build cannot panic here.
		name := fmt.Sprint(sig)
		if s, ok := sig.(syscall.Signal); ok {
			name = supervisor.SignalName(s)
		}
		d.log.Infof("daemon", "received %s, shutting down", name)
		d.shutdown()
		return nil
	}
}

func (d *Daemon) shutdown() {
	d.shutdownOnce.Do(func() {
		// The HTTP part only waits 5 seconds: SSE streams have already been canceled, so the remaining requests are short.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Cancel SSE streams first: http.Server.Shutdown does not interrupt active handlers,
		// otherwise every long-lived connection would drag shutdown out to the timeout.
		d.api.CloseStreams()
		_ = d.srv.Shutdown(ctx)
		// Program shutdown is managed uniformly by the supervisor according to shutdown_timeout.
		_ = d.mgr.Shutdown()
		d.log.Infof("daemon", "gosup exited")
	})
}
