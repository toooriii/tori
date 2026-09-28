package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"

	api "github.com/toooriii/tori/internal/generated"
)

func decodeTripData(body []byte) (api.TripData, error) {
	var req api.TripData

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
	for _, k := range []string{"user_id", "driver_id", "start_point", "end_point", "price"} {
		if !present(raw[k]) {
			return req, fmt.Errorf("field %q is required", k)
		}
	}
	for _, k := range []string{"start_point", "end_point"} {
		var pt map[string]json.RawMessage
		if err := json.Unmarshal(raw[k], &pt); err != nil {
			return req, fmt.Errorf("field %q must be an object", k)
		}
		for _, c := range []string{"latitude", "longitude"} {
			if !present(pt[c]) {
				return req, fmt.Errorf("field %q.%s is required", k, c)
			}
		}
	}

	if err := validateTripData(req); err != nil {
		return req, err
	}
	return req, nil
}

func present(v json.RawMessage) bool {
	t := bytes.TrimSpace(v)
	return len(t) > 0 && string(t) != "null"
}

func validateTripData(r api.TripData) error {
	if r.UserId == uuid.Nil {
		return errors.New("user_id must be a non-empty UUID")
	}
	if r.DriverId == uuid.Nil {
		return errors.New("driver_id must be a non-empty UUID")
	}
	if err := validatePoint("start_point", r.StartPoint); err != nil {
		return err
	}
	if err := validatePoint("end_point", r.EndPoint); err != nil {
		return err
	}
	if r.Price < 0 {
		return errors.New("price must be >= 0")
	}
	return nil
}

func validatePoint(name string, c api.Coordinates) error {
	if c.Latitude < -90 || c.Latitude > 90 {
		return fmt.Errorf("%s.latitude must be in [-90, 90]", name)
	}
	if c.Longitude < -180 || c.Longitude > 180 {
		return fmt.Errorf("%s.longitude must be in [-180, 180]", name)
	}
	return nil
}
