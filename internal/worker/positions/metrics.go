package positions

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	staleChecks     metric.Int64Counter
	queueSize       metric.Int64Gauge
	workersActive   metric.Int64UpDownCounter
	requests        metric.Int64Counter
	requestDuration metric.Float64Histogram
}

func newMetrics() (*metrics, error) {
	m := otel.Meter("trip-service/worker/positions")

	staleChecks, err := m.Int64Counter("tripgo.stale_checks",
		metric.WithUnit("{check}"),
		metric.WithDescription("Results of stale trip checks"))
	if err != nil {
		return nil, err
	}

	queueSize, err := m.Int64Gauge("tripgo.position_requests.queue.size",
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs currently in the positions queue"))
	if err != nil {
		return nil, err
	}

	workersActive, err := m.Int64UpDownCounter("tripgo.position_requests.workers.active",
		metric.WithUnit("{worker}"),
		metric.WithDescription("Workers currently processing a job"))
	if err != nil {
		return nil, err
	}

	requests, err := m.Int64Counter("tripgo.position_requests",
		metric.WithUnit("{request}"),
		metric.WithDescription("Position requests to Push Service"))
	if err != nil {
		return nil, err
	}

	requestDuration, err := m.Float64Histogram("tripgo.position_requests.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of a position request to Push Service"))
	if err != nil {
		return nil, err
	}

	return &metrics{
		staleChecks:     staleChecks,
		queueSize:       queueSize,
		workersActive:   workersActive,
		requests:        requests,
		requestDuration: requestDuration,
	}, nil
}

func (m *metrics) staleCheck(ctx context.Context, result string) {
	m.staleChecks.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func (m *metrics) setQueueSize(ctx context.Context, n int) {
	m.queueSize.Record(ctx, int64(n))
}

func (m *metrics) workerStarted(ctx context.Context) {
	m.workersActive.Add(ctx, 1)
}

func (m *metrics) workerFinished(ctx context.Context) {
	m.workersActive.Add(ctx, -1)
}

func (m *metrics) request(ctx context.Context, result string, d time.Duration) {
	m.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	m.requestDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("result", result)))
}
