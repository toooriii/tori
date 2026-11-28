//go:generate mockgen -source=contract.go -destination=mocks_test.go -package=positions

package positions

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/repository/trip"
)

type Job struct {
	TripID     uuid.UUID
	DriverID   uuid.UUID
	StaleAt    time.Time
	generation uint64
}

type Sender interface {
	SendRequestPosition(ctx context.Context, tripID, driverID uuid.UUID) error
}

type StaleSource interface {
	Stale(ctx context.Context, staleAfter time.Duration, limit int, after *trip.StaleCursor) ([]trip.StaleTrip, error)
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type TickerFactory func(d time.Duration) Ticker

type Clock func() time.Time

type Queue interface {
	TryEnqueue(j Job) EnqueueResult
	Cancel(tripID uuid.UUID) bool
	Pop(ctx context.Context) (Job, bool)
	Done(j Job)
	Len() int
	Close()
}

type EnqueueResult int

const (
	EnqueueQueued EnqueueResult = iota
	EnqueueDuplicate
	EnqueueDropped
	EnqueueEvicted
)
