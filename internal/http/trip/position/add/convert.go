package add

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

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	instance := r.URL.Path
	d := detail
	p := api.Problem{
		Type:     "https://tripgo.example/problems/" + code,
		Title:    titleOf(code),
		Status:   int32(status), //nolint:gosec
		Detail:   &d,
		Instance: &instance,
		Code:     code,
	}
	writeProblemBody(w, status, p)
}

func writeProblemBody(w http.ResponseWriter, status int, p api.Problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

func titleOf(code string) string {
	switch code {
	case "invalid_request":
		return "Invalid request"
	case "trip_not_found":
		return "Trip not found"
	case "trip_completed":
		return "Trip completed"
	default:
		return "Internal Server Error"
	}
}

func toAPI(p model.Position) api.TripPosition {
	return api.TripPosition{
		Id:     p.ID,
		TripId: p.TripID,
		Coordinates: api.Coordinates{
			Latitude:  p.Coordinates.Latitude,
			Longitude: p.Coordinates.Longitude,
		},
		RecordedAt: p.RecordedAt,
	}
}
