package positions

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/toooriii/tori/internal/config"
)

var tracer = otel.Tracer("trip-service/worker/positions")

type Component struct {
	queue    Queue
	scanner  *scanner
	pool     *pool
	interval time.Duration

	scanCancel context.CancelFunc
	scanDone   chan struct{}
	workCancel context.CancelFunc
	workDone   chan struct{}
}

func New(
	cfg config.Positions,
	source StaleSource,
	sender Sender,
	log *slog.Logger,
	ticker TickerFactory,
) (*Component, error) {
	m, err := newMetrics()
	if err != nil {
		return nil, err
	}

	var q Queue
	if cfg.PriorityQueue {
		q = newPriorityQueue(cfg.QueueSize)
	} else {
		q = newFIFOQueue(cfg.QueueSize)
	}

	return &Component{
		queue:    q,
		scanner:  newScanner(source, q, log, m, cfg.StaleAfter, cfg.BatchSize, cfg.QueueSize, ticker),
		pool:     newPool(q, sender, log, m, cfg.Workers, cfg.PushTimeout),
		interval: cfg.CheckInterval,
	}, nil
}

func (c *Component) Start(ctx context.Context) {
	scanCtx, scanCancel := context.WithCancel(ctx)
	workCtx, workCancel := context.WithCancel(ctx)
	c.scanCancel = scanCancel
	c.workCancel = workCancel
	c.scanDone = make(chan struct{})
	c.workDone = make(chan struct{})

	c.pool.start(workCtx)
	go func() {
		defer close(c.scanDone)
		c.scanner.run(scanCtx, c.interval)
	}()
	go func() {
		defer close(c.workDone)
		c.pool.wg.Wait()
	}()
}

func (c *Component) Cancel(tripID uuid.UUID) bool {
	return c.queue.Cancel(tripID)
}

func (c *Component) Stop(ctx context.Context) {
	c.scanCancel()
	select {
	case <-c.scanDone:
	case <-ctx.Done():
	}

	c.queue.Close()
	select {
	case <-c.workDone:
	case <-ctx.Done():
		c.workCancel()
		<-c.workDone
	}
}
