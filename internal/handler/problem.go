package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	api "github.com/toooriii/tori/internal/generated"
)

var problemTitles = map[string]string{
	"invalid_request": "Invalid request",
	"trip_not_found":  "Trip not found",
	"trip_completed":  "Trip completed",
	"driver_busy":     "Driver busy",
	"internal_error":  "Internal Server Error",
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	instance := r.URL.Path
	p := api.Problem{
		Type:     "https://tripgo.example/problems/" + strings.ReplaceAll(code, "_", "-"),
		Title:    problemTitles[code],
		Status:   int32(status), //nolint:gosec
		Detail:   &detail,
		Instance: &instance,
		Code:     code,
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

func ParamError(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, r, http.StatusBadRequest, "invalid_request", err.Error())
}
