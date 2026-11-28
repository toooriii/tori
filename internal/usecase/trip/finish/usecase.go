package finish

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/domain/trip"
	"github.com/toooriii/tori/internal/model"
)

type UseCase struct {
	trips          tripRepository
	tx             txManager
	tripsCompleted tripCounter
}

func New(
	trips tripRepository,
	tx txManager,
	tripsCompleted tripCounter,
) *UseCase {
	return &UseCase{
		trips:          trips,
		tx:             tx,
		tripsCompleted: tripsCompleted,
	}
}

func (uc *UseCase) Do(ctx context.Context, id uuid.UUID, now time.Time) (model.Trip, error) {
	var out model.Trip
	err := uc.tx.Do(ctx, func(ctx context.Context) error {
		got, err := uc.trips.Get(ctx, id)
		if err != nil {
			return err
		}
		domainTrip := got.ToDomain()
		if err := domainTrip.Finish(now); err != nil {
			return err
		}
		out, err = uc.trips.Finish(ctx, id, now)
		if err != nil {
			return err
		}
		from := string(trip.StatusActive)
		return uc.trips.AddHistory(ctx, out.ID, &from, string(trip.StatusCompleted), "trip finished")
	})

	if err == nil {
		uc.tripsCompleted.Add(ctx, 1)
	}

	return out, err
}
