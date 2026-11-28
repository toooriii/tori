package container

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/toooriii/tori/internal/config"
	httptransport "github.com/toooriii/tori/internal/http"
	createHandler "github.com/toooriii/tori/internal/http/trip/create"
	finishHandler "github.com/toooriii/tori/internal/http/trip/finish"
	getHandler "github.com/toooriii/tori/internal/http/trip/get"
	"github.com/toooriii/tori/internal/infra/postgres"
	"github.com/toooriii/tori/internal/infra/postgres/transaction"
	"github.com/toooriii/tori/internal/infra/telemetry"
	idemRepo "github.com/toooriii/tori/internal/repository/idempotency"
	tripRepo "github.com/toooriii/tori/internal/repository/trip"
	createUC "github.com/toooriii/tori/internal/usecase/trip/create"
	finishUC "github.com/toooriii/tori/internal/usecase/trip/finish"
	getUC "github.com/toooriii/tori/internal/usecase/trip/get"
)

type Container struct {
	Log     *slog.Logger
	Pool    *pgxpool.Pool
	Tx      *transaction.Manager
	Handler http.Handler
}

func New(
	ctx context.Context,
	cfg config.Config,
	log *slog.Logger,
) (*Container, error) {
	metrics, err := telemetry.NewTripMetrics(
		otel.Meter("trip-service/usecase"),
	)
	if err != nil {
		return nil, fmt.Errorf("init trip metrics: %w", err)
	}

	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return nil, err
	}

	tx := transaction.New(pool, pgx.ReadCommitted)
	trips := tripRepo.New(tx)
	idem := idemRepo.New(tx)
	create := createUC.New(trips, idem, tx, metrics.Created)
	get := getUC.New(trips)
	finish := finishUC.New(trips, tx, metrics.Completed)

	handler := httptransport.New(
		createHandler.New(create),
		getHandler.New(get),
		finishHandler.New(finish),
		pool.Ping,
		log,
	)
	return &Container{
		Log:     log,
		Pool:    pool,
		Tx:      tx,
		Handler: handler,
	}, nil
}

func (c *Container) Close() {
	if c.Pool != nil {
		c.Pool.Close()
	}
}
