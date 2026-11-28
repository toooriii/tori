package add

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/toooriii/tori/internal/domain/position"
	api "github.com/toooriii/tori/internal/generated"
	"github.com/toooriii/tori/internal/http/domainerror"
	usecase "github.com/toooriii/tori/internal/usecase/trip/position/add"
)

type Handler struct {
	uc UseCase
}

func New(uc UseCase) *Handler {
	return &Handler{uc: uc}
}

func (h *Handler) CreateTripPosition(w http.ResponseWriter, r *http.Request, tripID api.TripId) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", "cannot read request body")
		return
	}
	req, err := decodePositionData(body)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	out, err := h.uc.Do(r.Context(), usecase.Input{
		TripID:      tripID,
		Coordinates: position.Coordinates{Latitude: req.Coordinates.Latitude, Longitude: req.Coordinates.Longitude},
		RecordedAt:  req.RecordedAt,
		Now:         time.Now().UTC(),
	})
	if err != nil {
		status, p := domainerror.Problem(err)
		writeProblemBody(w, status, p)
		return
	}

	writeJSON(w, http.StatusCreated, toAPI(out.Position))
}

func decodePositionData(body []byte) (api.PositionData, error) {
	var req api.PositionData

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON body: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return req, errors.New("invalid JSON body: unexpected data after object")
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return req, fmt.Errorf("invalid JSON body: %w", err)
	}
	for _, k := range []string{"coordinates", "recorded_at"} {
		if !present(raw[k]) {
			return req, fmt.Errorf("field %q is required", k)
		}
	}
	var pt map[string]json.RawMessage
	if err := json.Unmarshal(raw["coordinates"], &pt); err != nil {
		return req, errors.New("field \"coordinates\" must be an object")
	}
	for _, c := range []string{"latitude", "longitude"} {
		if !present(pt[c]) {
			return req, fmt.Errorf("field \"coordinates.%s\" is required", c)
		}
	}
	return req, nil
}

func present(v json.RawMessage) bool {
	t := bytes.TrimSpace(v)
	return len(t) > 0 && string(t) != "null"
}
