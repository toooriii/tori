//go:generate mockgen -source ${GOFILE} -destination mocks_test.go -package ${GOPACKAGE}_test

package add

import (
	"context"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/model"
)

type (
	positionRepository interface {
		Save(ctx context.Context, p model.Position) (int64, error)
	}

	txManager interface {
		Do(ctx context.Context, fn func(ctx context.Context) error) error
	}

	queueCanceller interface {
		Cancel(tripID uuid.UUID) bool
	}
)
