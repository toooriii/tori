package container

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/toooriii/tori/internal/config"
	"github.com/toooriii/tori/internal/gateway/push"
	createHandler "github.com/toooriii/tori/internal/http/trip/create"
	finishHandler "github.com/toooriii/tori/internal/http/trip/finish"
	getHandler "github.com/toooriii/tori/internal/http/trip/get"
	positionAddHandler "github.com/toooriii/tori/internal/http/trip/position/add"
	positionListHandler "github.com/toooriii/tori/internal/http/trip/position/list"
	"github.com/toooriii/tori/internal/infra/postgres"
	"github.com/toooriii/tori/internal/infra/postgres/transaction"
	idemRepo "github.com/toooriii/tori/internal/repository/idempotency"
	posRepo "github.com/toooriii/tori/internal/repository/position"
	tripRepo "github.com/toooriii/tori/internal/repository/trip"
	createUC "github.com/toooriii/tori/internal/usecase/trip/create"
	finishUC "github.com/toooriii/tori/internal/usecase/trip/finish"
	getUC "github.com/toooriii/tori/internal/usecase/trip/get"
	positionAddUC "github.com/toooriii/tori/internal/usecase/trip/position/add"
	positionListUC "github.com/toooriii/tori/internal/usecase/trip/position/list"
	"github.com/toooriii/tori/internal/worker/positions"
)

type Container struct {
	Log  *slog.Logger
	Pool *pgxpool.Pool
	Tx   *transaction.Manager

	CreateTrip         *createHandler.Handler
	GetTrip            *getHandler.Handler
	FinishTrip         *finishHandler.Handler
	CreateTripPosition *positionAddHandler.Handler
	ListTripPositions  *positionListHandler.Handler

	Ping      func(ctx context.Context) error
	Positions *positions.Component

	PushClient *push.Client
}

type tickerAdapter struct {
	t *time.Ticker
}

func (a tickerAdapter) C() <-chan time.Time { return a.t.C }
func (a tickerAdapter) Stop()               { a.t.Stop() }

func newTicker(d time.Duration) positions.Ticker {
	return tickerAdapter{t: time.NewTicker(d)}
}

func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*Container, error) {
	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return nil, err
	}

	tx := transaction.New(pool, pgx.ReadCommitted)

	trips := tripRepo.New(tx)
	idem := idemRepo.New(tx)
	positionsRepo := posRepo.New(tx)

	pushClient := push.New(cfg.Positions.PushURL, cfg.Positions.PushTimeout)

	positionsComponent, err := positions.New(
		cfg.Positions,
		trips,
		pushClient,
		log,
		newTicker,
	)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("positions component: %w", err)
	}

	create := createUC.New(trips, idem, tx)
	get := getUC.New(trips)
	finish := finishUC.New(trips, tx)
	posAdd := positionAddUC.New(positionsRepo, tx, positionsComponent)
	posList := positionListUC.New(trips, positionsRepo)

	return &Container{
		Log:                log,
		Pool:               pool,
		Tx:                 tx,
		CreateTrip:         createHandler.New(create),
		GetTrip:            getHandler.New(get),
		FinishTrip:         finishHandler.New(finish),
		CreateTripPosition: positionAddHandler.New(posAdd),
		ListTripPositions:  positionListHandler.New(posList),
		Ping:               pool.Ping,
		Positions:          positionsComponent,
		PushClient:         pushClient,
	}, nil
}

func (c *Container) Close() {
	if c.PushClient != nil {
		c.PushClient.CloseIdleConnections()
	}
	if c.Pool != nil {
		c.Pool.Close()
	}
}
