package model

import (
	"time"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/domain/position"
)

type Position struct {
	ID          int64
	TripID      uuid.UUID
	Coordinates position.Coordinates
	RecordedAt  time.Time
}

func FromDomainPosition(p position.Position) Position {
	return Position{
		ID:          p.ID,
		TripID:      p.TripID,
		Coordinates: p.Coordinates,
		RecordedAt:  p.RecordedAt,
	}
}

func (m Position) ToDomain() position.Position {
	return position.Position{
		ID:          m.ID,
		TripID:      m.TripID,
		Coordinates: m.Coordinates,
		RecordedAt:  m.RecordedAt,
	}
}
