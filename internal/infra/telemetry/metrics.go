package telemetry

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

type TripMetrics struct {
	Created   metric.Int64Counter
	Completed metric.Int64Counter
}

func NewTripMetrics(meter metric.Meter) (*TripMetrics, error) {
	created, err := meter.Int64Counter(
		"tripgo.trips.created",
		metric.WithUnit("{trip}"),
		metric.WithDescription("Created trips"),
	)
	if err != nil {
		return nil, fmt.Errorf("create trips created counter: %w", err)
	}

	completed, err := meter.Int64Counter(
		"tripgo.trips.completed",
		metric.WithUnit("{trip}"),
		metric.WithDescription("Completed trips"),
	)
	if err != nil {
		return nil, fmt.Errorf("create trips completed counter: %w", err)
	}

	return &TripMetrics{
		Created:   created,
		Completed: completed,
	}, nil
}
