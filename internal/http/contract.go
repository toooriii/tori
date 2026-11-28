package http

import (
	"context"
	"net/http"

	api "github.com/toooriii/tori/internal/generated"
)

type (
	createHandler interface {
		CreateTrip(http.ResponseWriter, *http.Request, api.CreateTripParams)
	}

	getHandler interface {
		GetTrip(http.ResponseWriter, *http.Request, api.TripId)
	}

	finishHandler interface {
		FinishTrip(http.ResponseWriter, *http.Request, api.TripId)
	}

	pingFunc func(ctx context.Context) error
)
