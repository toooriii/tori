package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	StatusActive    = "active"
	StatusCompleted = "completed"
)

var (
	ErrTripNotFound        = errors.New("trip not found")
	ErrTripCompleted       = errors.New("trip already completed")
	ErrDriverBusy          = errors.New("driver already has an active trip")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different body")
)

type Point struct {
	Latitude  float64
	Longitude float64
}

type Trip struct {
	ID         uuid.UUID
	UserID     uuid.UUID
	DriverID   uuid.UUID
	Start      Point
	End        Point
	Price      int64
	Status     string
	StartedAt  time.Time
	FinishedAt *time.Time
}
