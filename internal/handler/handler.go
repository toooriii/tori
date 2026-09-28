package handler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	api "github.com/toooriii/tori/internal/generated"
	"github.com/toooriii/tori/internal/model"
	"github.com/toooriii/tori/internal/repository"
)

type TxManager interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

type Handler struct {
	log          *slog.Logger
	tx           TxManager
	trips        *repository.TripRepository
	ping         func(ctx context.Context) error
	queryTimeout time.Duration
}

var _ api.ServerInterface = (*Handler)(nil)

func New(
	log *slog.Logger,
	tx TxManager,
	trips *repository.TripRepository,
	ping func(ctx context.Context) error,
	queryTimeout time.Duration,
) *Handler {
	return &Handler{log: log, tx: tx, trips: trips, ping: ping, queryTimeout: queryTimeout}
}

func now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func (h *Handler) CreateTrip(w http.ResponseWriter, r *http.Request, _ api.CreateTripParams) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", "cannot read request body")
		return
	}
	req, err := decodeTripData(body)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	trip := model.Trip{
		ID:        uuid.New(),
		UserID:    req.UserId,
		DriverID:  req.DriverId,
		Start:     model.Point{Latitude: req.StartPoint.Latitude, Longitude: req.StartPoint.Longitude},
		End:       model.Point{Latitude: req.EndPoint.Latitude, Longitude: req.EndPoint.Longitude},
		Price:     req.Price,
		Status:    model.StatusActive,
		StartedAt: now(),
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.queryTimeout)
	defer cancel()

	err = h.tx.Do(ctx, func(ctx context.Context) error {
		if err := h.trips.Create(ctx, trip); err != nil {
			return err
		}
		return h.trips.AddHistory(ctx, trip.ID, nil, model.StatusActive, "trip created")
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	w.Header().Set("Location", "/api/v1/trips/"+trip.ID.String())
	writeJSON(w, http.StatusCreated, toAPI(trip))
}

func (h *Handler) GetTrip(w http.ResponseWriter, r *http.Request, tripId api.TripId) {
	ctx, cancel := context.WithTimeout(r.Context(), h.queryTimeout)
	defer cancel()

	trip, err := h.trips.Get(ctx, tripId)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toAPI(trip))
}

func (h *Handler) FinishTrip(w http.ResponseWriter, r *http.Request, tripId api.TripId) {
	ctx, cancel := context.WithTimeout(r.Context(), h.queryTimeout)
	defer cancel()

	var trip model.Trip
	err := h.tx.Do(ctx, func(ctx context.Context) error {
		var err error
		trip, err = h.trips.Finish(ctx, tripId, now())
		if err != nil {
			return err
		}
		from := model.StatusActive
		return h.trips.AddHistory(ctx, trip.ID, &from, model.StatusCompleted, "trip finished")
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toAPI(trip))
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, api.HealthResponse{Status: api.Ok})
}

func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.ping(ctx); err != nil {
		h.log.Warn("readiness check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, api.HealthResponse{Status: api.Unavailable})
		return
	}
	writeJSON(w, http.StatusOK, api.HealthResponse{Status: api.Ok})
}

func (h *Handler) Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				h.log.Error("panic in handler", "panic", rec, "stack", string(debug.Stack()))
				writeProblem(w, r, http.StatusInternalServerError, "internal_error", "Internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, model.ErrTripNotFound):
		writeProblem(w, r, http.StatusNotFound, "trip_not_found", "Trip was not found")
	case errors.Is(err, model.ErrTripCompleted):
		writeProblem(w, r, http.StatusConflict, "trip_completed", "Operation is not allowed for a completed trip")
	case errors.Is(err, model.ErrDriverBusy):
		writeProblem(w, r, http.StatusConflict, "driver_busy", "Driver already has an active trip")
	default:
		h.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "internal_error", "Internal server error")
	}
}

func toAPI(t model.Trip) api.Trip {
	return api.Trip{
		Id:         t.ID,
		UserId:     t.UserID,
		DriverId:   t.DriverID,
		StartPoint: api.Coordinates{Latitude: t.Start.Latitude, Longitude: t.Start.Longitude},
		EndPoint:   api.Coordinates{Latitude: t.End.Latitude, Longitude: t.End.Longitude},
		Price:      t.Price,
		Status:     api.TripStatus(t.Status),
		StartedAt:  t.StartedAt,
		FinishedAt: t.FinishedAt,
	}
}
