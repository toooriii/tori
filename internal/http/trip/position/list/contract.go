//go:generate mockgen -source ${GOFILE} -destination mocks_test.go -package ${GOPACKAGE}_test

package list

import (
	"context"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/model"
)

type UseCase interface {
	Do(ctx context.Context, tripID uuid.UUID) ([]model.Position, error)
}
