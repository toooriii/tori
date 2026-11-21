package list

import (
	"context"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/model"
)

type UseCase struct {
	trips     tripRepository
	positions positionRepository
}

func New(trips tripRepository, positions positionRepository) *UseCase {
	return &UseCase{trips: trips, positions: positions}
}

func (uc *UseCase) Do(ctx context.Context, tripID uuid.UUID) ([]model.Position, error) {
	if _, err := uc.trips.Get(ctx, tripID); err != nil {
		return nil, err
	}
	return uc.positions.List(ctx, tripID)
}
