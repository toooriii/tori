package http

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	api "github.com/toooriii/tori/internal/generated"
	mw "github.com/toooriii/tori/internal/http/middleware"
)

type server struct {
	create createHandler
	get    getHandler
	finish finishHandler
	ping   pingFunc
}

func New(
	create createHandler,
	get getHandler,
	finish finishHandler,
	ping pingFunc,
	log *slog.Logger,
) http.Handler {
	s := &server{
		create: create,
		get:    get,
		finish: finish,
		ping:   ping,
	}

	router := chi.NewRouter()
	router.Use(mw.Recover(log))
	router.Use(mw.Tracing())
	router.Use(mw.Logging(log))

	return api.HandlerWithOptions(
		s,
		api.ChiServerOptions{
			BaseRouter:       router,
			ErrorHandlerFunc: paramError,
		},
	)
}

func (s *server) CreateTrip(
	w http.ResponseWriter,
	r *http.Request,
	params api.CreateTripParams,
) {
	s.create.CreateTrip(w, r, params)
}

func (s *server) GetTrip(
	w http.ResponseWriter,
	r *http.Request,
	id api.TripId,
) {
	s.get.GetTrip(w, r, id)
}

func (s *server) FinishTrip(
	w http.ResponseWriter,
	r *http.Request,
	id api.TripId,
) {
	s.finish.FinishTrip(w, r, id)
}

func (s *server) Health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *server) Ready(w http.ResponseWriter, r *http.Request) {
	if err := s.ping(r.Context()); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"unavailable"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func paramError(w http.ResponseWriter, _ *http.Request, err error) {
	d := err.Error()

	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusBadRequest)

	_ = json.NewEncoder(w).Encode(api.Problem{
		Type:   "https://tripgo.example/problems/invalid-request",
		Title:  "Invalid request",
		Status: http.StatusBadRequest,
		Code:   "invalid_request",
		Detail: &d,
	})
}
