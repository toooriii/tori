package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"time"
)

type Database struct {
	URL             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
	QueryTimeout    time.Duration
}

type Config struct {
	HTTPAddr          string
	LogLevel          slog.Level
	ShutdownTimeout   time.Duration
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	Database          Database
	Telemetry         Telemetry
	Positions         Positions
}

type Telemetry struct {
	ServiceName   string
	OTLPEndpoint  string
	OTLPProtocol  string
	Sampler       string
	ResourceAttrs string
}

type Positions struct {
	CheckInterval time.Duration
	StaleAfter    time.Duration
	Workers       int
	QueueSize     int
	PushURL       string
	PushTimeout   time.Duration
	BatchSize     int
	PriorityQueue bool
}

func Load() (Config, error) {
	l := &loader{}

	cfg := Config{
		HTTPAddr:          l.str("HTTP_ADDR", ":8080"),
		ShutdownTimeout:   l.dur("SHUTDOWN_TIMEOUT", 10*time.Second),
		ReadTimeout:       l.dur("HTTP_READ_TIMEOUT", 10*time.Second),
		ReadHeaderTimeout: l.dur("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		WriteTimeout:      l.dur("HTTP_WRITE_TIMEOUT", 15*time.Second),
		IdleTimeout:       l.dur("HTTP_IDLE_TIMEOUT", 60*time.Second),
		Database: Database{
			URL:             l.required("DATABASE_URL"),
			MaxConns:        l.int32Val("DATABASE_MAX_CONNS", 10, 1, math.MaxInt32),
			MinConns:        l.int32Val("DATABASE_MIN_CONNS", 2, 0, math.MaxInt32),
			MaxConnLifetime: l.dur("DATABASE_MAX_CONN_LIFETIME", 30*time.Minute),
			ConnectTimeout:  l.dur("DATABASE_CONNECT_TIMEOUT", 5*time.Second),
			QueryTimeout:    l.dur("DATABASE_QUERY_TIMEOUT", 3*time.Second),
		},
		Telemetry: Telemetry{
			ServiceName:   l.str("OTEL_SERVICE_NAME", "trip-service"),
			OTLPEndpoint:  l.str("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
			OTLPProtocol:  l.str("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
			Sampler:       l.str("OTEL_TRACES_SAMPLER", "parentbased_always_on"),
			ResourceAttrs: l.str("OTEL_RESOURCE_ATTRIBUTES", "service.namespace=tripgo,deployment.environment=local"),
		},
		Positions: Positions{
			CheckInterval: l.dur("POSITION_CHECK_INTERVAL", 30*time.Second),
			StaleAfter:    l.dur("POSITION_STALE_AFTER", 60*time.Second),
			Workers:       l.int("POSITION_WORKERS", 4),
			QueueSize:     l.int("POSITION_QUEUE_SIZE", 100),
			PushURL:       l.required("PUSH_HTTP_URL"),
			PushTimeout:   l.dur("PUSH_REQUEST_TIMEOUT", time.Second),
			BatchSize:     l.int("POSITION_BATCH_SIZE", 0),
			PriorityQueue: l.bool("POSITION_PRIORITY_QUEUE", false),
		},
	}

	if s := os.Getenv("LOG_LEVEL"); s != "" {
		if err := cfg.LogLevel.UnmarshalText([]byte(s)); err != nil {
			l.errs = append(l.errs, fmt.Errorf("LOG_LEVEL: %w", err))
		}
	}

	if cfg.Positions.Workers < 1 {
		l.errs = append(l.errs, errors.New("POSITION_WORKERS must be >= 1"))
	}
	if cfg.Positions.QueueSize < 1 {
		l.errs = append(l.errs, errors.New("POSITION_QUEUE_SIZE must be >= 1"))
	}
	if cfg.Positions.BatchSize < 0 {
		l.errs = append(l.errs, errors.New("POSITION_BATCH_SIZE must be >= 0"))
	}

	if cfg.Database.MinConns < 0 || cfg.Database.MinConns > cfg.Database.MaxConns {
		l.errs = append(l.errs, errors.New("DATABASE_MIN_CONNS must be in [0, DATABASE_MAX_CONNS]"))
	}

	if err := errors.Join(l.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

type loader struct{ errs []error }

func (l *loader) str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (l *loader) required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		l.errs = append(l.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

func (l *loader) dur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		l.errs = append(l.errs, fmt.Errorf("%s: expected positive duration like 5s, got %q", key, v))
		return def
	}
	return d
}

func (l *loader) int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: expected integer, got %q", key, v))
		return def
	}
	return n
}

func (l *loader) int32Val(key string, def, min, max int) int32 {
	n := l.int(key, def)
	if n < min || n > max {
		l.errs = append(l.errs, fmt.Errorf("%s must be in [%d, %d], got %d", key, min, max, n))
		return int32(def) //nolint:gosec
	}
	return int32(n) //nolint:gosec
}

func (l *loader) bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: expected bool, got %q", key, v))
		return def
	}
	return b
}
