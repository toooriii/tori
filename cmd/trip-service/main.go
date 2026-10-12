package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/toooriii/tori/internal/config"
	api "github.com/toooriii/tori/internal/generated"
	"github.com/toooriii/tori/internal/handler"
	"github.com/toooriii/tori/internal/postgres"
	"github.com/toooriii/tori/internal/repository"
	"github.com/toooriii/tori/internal/txmanager"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return err
	}

	tx := txmanager.New(pool, pgx.ReadCommitted)
	trips := repository.NewTripRepository(tx)
	idem := repository.NewIdempotencyRepository(tx)
	h := handler.New(log, tx, trips, idem, pool.Ping, cfg.Database.QueryTimeout)

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	router := chi.NewRouter()
	router.Use(h.Recoverer)

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: api.HandlerWithOptions(h, api.ChiServerOptions{
			BaseRouter:       router,
			ErrorHandlerFunc: handler.ParamError,
		}),
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server started", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			pool.Close()
			return fmt.Errorf("http server: %w", err)
		}
		pool.Close()
		return nil
	case <-ctx.Done():
	}

	stop()
	log.Info("shutting down", "timeout", cfg.ShutdownTimeout.String())

	time.AfterFunc(cfg.ShutdownTimeout, func() {
		log.Warn("shutdown timeout exceeded, forcing exit")
		os.Exit(1)
	})

	cancelBase()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}

	pool.Close()
	log.Info("stopped")
	return nil
}
