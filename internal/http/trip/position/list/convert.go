package list

import (
	"encoding/json"
	"net/http"

	api "github.com/toooriii/tori/internal/generated"
	"github.com/toooriii/tori/internal/model"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblemBody(w http.ResponseWriter, status int, p api.Problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

func toAPI(positions []model.Position) api.TripPositionsResponse {
	out := make([]api.TripPosition, 0, len(positions))
	for _, p := range positions {
		out = append(out, api.TripPosition{
			Id:     p.ID,
			TripId: p.TripID,
			Coordinates: api.Coordinates{
				Latitude:  p.Coordinates.Latitude,
				Longitude: p.Coordinates.Longitude,
			},
			RecordedAt: p.RecordedAt,
		})
	}
	return api.TripPositionsResponse{Positions: out}
}
