package position

import (
	"time"

	"github.com/google/uuid"
)

type Coordinates struct {
	Latitude  float64
	Longitude float64
}

type Position struct {
	ID          int64
	TripID      uuid.UUID
	Coordinates Coordinates
	RecordedAt  time.Time
}

func (p Position) Validate(now time.Time) error {
	if p.TripID == uuid.Nil {
		return &ValidationError{Field: "trip_id", Message: "must be non-empty UUID"}
	}
	if err := validateCoordinates("coordinates", p.Coordinates); err != nil {
		return err
	}
	if p.RecordedAt.IsZero() {
		return &ValidationError{Field: "recorded_at", Message: "must be set"}
	}
	if p.RecordedAt.After(now.Add(time.Minute)) {
		return &ValidationError{Field: "recorded_at", Message: "must not be in the future"}
	}
	return nil
}

func validateCoordinates(name string, c Coordinates) error {
	if c.Latitude < -90 || c.Latitude > 90 {
		return &ValidationError{Field: name + ".latitude", Message: "must be in [-90, 90]"}
	}
	if c.Longitude < -180 || c.Longitude > 180 {
		return &ValidationError{Field: name + ".longitude", Message: "must be in [-180, 180]"}
	}
	return nil
}
