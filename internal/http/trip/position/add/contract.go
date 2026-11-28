//go:generate mockgen -source ${GOFILE} -destination mocks_test.go -package ${GOPACKAGE}_test

package add

import (
	"context"

	usecase "github.com/toooriii/tori/internal/usecase/trip/position/add"
)

type UseCase interface {
	Do(ctx context.Context, in usecase.Input) (usecase.Output, error)
}
