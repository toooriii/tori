package positions

import (
	"context"
	"log/slog"
	"time"

	"github.com/toooriii/tori/internal/repository/trip"
)

type scanner struct {
	source     StaleSource
	queue      Queue
	log        *slog.Logger
	metrics    *metrics
	staleAfter time.Duration
	batchSize  int
	queueSize  int
	newTicker  TickerFactory
}

func newScanner(source StaleSource, q Queue, log *slog.Logger, m *metrics, staleAfter time.Duration, batchSize, queueSize int, ticker TickerFactory) *scanner {
	return &scanner{
		source:     source,
		queue:      q,
		log:        log,
		metrics:    m,
		staleAfter: staleAfter,
		batchSize:  batchSize,
		queueSize:  queueSize,
		newTicker:  ticker,
	}
}

func (s *scanner) run(ctx context.Context, interval time.Duration) {
	t := s.newTicker(interval)
	defer t.Stop()

	s.log.Info("positions scanner started", "interval", interval.String(), "batch_size", s.batchSize)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("positions scanner stopped")
			return
		case <-t.C():
			s.tick(ctx)
		}
	}
}

func (s *scanner) tick(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "positions.scan")
	defer span.End()

	if s.batchSize > 0 {
		s.tickBatches(ctx)
		return
	}
	s.tickAll(ctx)
}

func (s *scanner) tickAll(ctx context.Context) {
	trips, err := s.source.Stale(ctx, s.staleAfter, 0, nil)
	if err != nil {
		s.log.ErrorContext(ctx, "stale trips query failed", "error", err)
		return
	}
	for _, tr := range trips {
		s.enqueue(ctx, tr)
	}
	s.metrics.setQueueSize(ctx, s.queue.Len())
}

func (s *scanner) tickBatches(ctx context.Context) {
	var after *trip.StaleCursor
	for {
		trips, err := s.source.Stale(ctx, s.staleAfter, s.batchSize, after)
		if err != nil {
			s.log.ErrorContext(ctx, "stale trips query failed", "error", err)
			return
		}
		if len(trips) == 0 {
			return
		}
		for _, tr := range trips {
			s.enqueue(ctx, tr)
		}
		s.metrics.setQueueSize(ctx, s.queue.Len())

		if s.queue.Len() >= s.queueSize {
			return
		}
		last := trips[len(trips)-1]
		after = &trip.StaleCursor{StaleAt: last.StaleAt, TripID: last.TripID}
		if len(trips) < s.batchSize {
			return
		}
	}
}

func (s *scanner) enqueue(ctx context.Context, tr trip.StaleTrip) {
	res := s.queue.TryEnqueue(Job{
		TripID:   tr.TripID,
		DriverID: tr.DriverID,
		StaleAt:  tr.StaleAt,
	})
	switch res {
	case EnqueueQueued:
		s.metrics.staleCheck(ctx, "queued")
	case EnqueueDuplicate:
		s.metrics.staleCheck(ctx, "duplicate")
	case EnqueueDropped:
		s.metrics.staleCheck(ctx, "dropped")
		s.log.WarnContext(ctx, "positions queue full, trip dropped",
			"trip_id", tr.TripID.String())
	}
}
