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

	"github.com/toooriii/tori/internal/config"
	"github.com/toooriii/tori/internal/container"
	"github.com/toooriii/tori/internal/infra/telemetry"
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
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	tel, err := telemetry.Init(ctx, cfg.Telemetry, "dev")
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		if err := tel.Shutdown(shutCtx); err != nil {
			fmt.Fprintln(os.Stderr, "telemetry shutdown:", err)
		}
	}()

	base := slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{Level: cfg.LogLevel},
	)
	log := slog.New(telemetry.NewTraceHandler(base))

	c, err := container.New(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("container: %w", err)
	}
	defer c.Close()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           c.Handler,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		BaseContext: func(net.Listener) context.Context {
			return context.Background()
		},
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server started", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	stop()

	log.Info(
		"shutting down",
		"timeout", cfg.ShutdownTimeout.String(),
	)

	timer := time.AfterFunc(cfg.ShutdownTimeout, func() {
		log.Warn("shutdown timeout exceeded, forcing exit")
		os.Exit(1)
	})
	defer timer.Stop()
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		cfg.ShutdownTimeout,
	)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}
	log.Info("stopped")
	return nil
}
