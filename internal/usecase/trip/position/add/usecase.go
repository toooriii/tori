package add

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/toooriii/tori/internal/domain/position"
	"github.com/toooriii/tori/internal/model"
)

type Input struct {
	TripID      uuid.UUID
	Coordinates position.Coordinates
	RecordedAt  time.Time
	Now         time.Time
}

type Output struct {
	Position model.Position
}

type UseCase struct {
	positions positionRepository
	tx        txManager
	canceller queueCanceller
}

func New(positions positionRepository, tx txManager, canceller queueCanceller) *UseCase {
	return &UseCase{positions: positions, tx: tx, canceller: canceller}
}

func (uc *UseCase) Do(ctx context.Context, in Input) (Output, error) {
	domainPos := position.Position{
		TripID:      in.TripID,
		Coordinates: in.Coordinates,
		RecordedAt:  in.RecordedAt.UTC().Truncate(time.Microsecond),
	}
	if err := domainPos.Validate(in.Now.UTC()); err != nil {
		return Output{}, err
	}

	mt := model.FromDomainPosition(domainPos)

	var id int64
	err := uc.tx.Do(ctx, func(ctx context.Context) error {
		var saveErr error
		id, saveErr = uc.positions.Save(ctx, mt)
		return saveErr
	})
	if err != nil {
		return Output{}, err
	}

	uc.canceller.Cancel(in.TripID)

	mt.ID = id
	positionsSaved.Add(ctx, 1)
	return Output{Position: mt}, nil
}

var positionsSaved, _ = otel.Meter("trip-service/usecase").Int64Counter(
	"tripgo.positions.saved",
	metric.WithUnit("{position}"),
	metric.WithDescription("Saved trip positions"),
)
