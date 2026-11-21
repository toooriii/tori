package positions

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/toooriii/tori/internal/gateway/push"
)

type pool struct {
	queue       Queue
	sender      Sender
	log         *slog.Logger
	metrics     *metrics
	workers     int
	requestTout time.Duration
	wg          sync.WaitGroup
	appCtx      context.Context
}

func newPool(q Queue, sender Sender, log *slog.Logger, m *metrics, workers int, requestTimeout time.Duration) *pool {
	return &pool{
		queue:       q,
		sender:      sender,
		log:         log,
		metrics:     m,
		workers:     workers,
		requestTout: requestTimeout,
	}
}

func (p *pool) start(ctx context.Context) {
	p.appCtx = ctx
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

func (p *pool) worker(id int) {
	defer p.wg.Done()

	for {
		job, ok := p.queue.Pop(p.appCtx)
		if !ok {
			return
		}
		p.handle(id, job)
	}
}

func (p *pool) handle(workerID int, j Job) {
	defer p.queue.Done(j)

	ctx, span := tracer.Start(p.appCtx, "positions.request",
		trace.WithLinks(trace.LinkFromContext(p.appCtx)),
	)
	defer span.End()

	p.metrics.workerStarted(ctx)
	start := time.Now()

	reqCtx, cancel := context.WithTimeout(ctx, p.requestTout)
	err := p.sender.SendRequestPosition(reqCtx, j.TripID, j.DriverID)
	cancel()

	dur := time.Since(start)
	p.metrics.workerFinished(ctx)

	switch {
	case err == nil:
		p.metrics.request(ctx, "ok", dur)
	case push.IsTransport(err):
		p.metrics.request(ctx, "transport_error", dur)
		p.log.WarnContext(ctx, "push request failed", "worker", workerID,
			"trip_id", j.TripID.String(), "error", err)
	case isServerError(err):
		p.metrics.request(ctx, "server_error", dur)
		p.log.WarnContext(ctx, "push service returned error", "worker", workerID,
			"trip_id", j.TripID.String(), "error", err)
	case isClientError(err):
		p.metrics.request(ctx, "client_error", dur)
		p.log.WarnContext(ctx, "push service rejected request", "worker", workerID,
			"trip_id", j.TripID.String(), "error", err)
	default:
		p.metrics.request(ctx, "error", dur)
		p.log.ErrorContext(ctx, "push request failed", "worker", workerID,
			"trip_id", j.TripID.String(), "error", err)
	}
}

func isServerError(err error) bool {
	var se *push.ServerError
	return errors.As(err, &se)
}

func isClientError(err error) bool {
	var ce *push.ClientError
	return errors.As(err, &ce)
}
