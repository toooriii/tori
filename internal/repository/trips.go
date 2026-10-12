package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/toooriii/tori/internal/model"
	"github.com/toooriii/tori/internal/txmanager"
)

const (
	pgUniqueViolation = "23505"
	activeDriverIndex = "trips_driver_active_uidx"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

var tripColumns = []string{
	"id", "user_id", "driver_id",
	"start_latitude", "start_longitude", "end_latitude", "end_longitude",
	"price", "status", "started_at", "finished_at",
}

type TripRepository struct {
	tx *txmanager.Manager
}

func NewTripRepository(tx *txmanager.Manager) *TripRepository {
	return &TripRepository{tx: tx}
}

func (r *TripRepository) Create(ctx context.Context, t model.Trip) error {
	q, args, err := psql.Insert("trips").
		Columns("id", "user_id", "driver_id",
			"start_latitude", "start_longitude", "end_latitude", "end_longitude",
			"price", "status", "started_at").
		Values(t.ID, t.UserID, t.DriverID,
			t.Start.Latitude, t.Start.Longitude, t.End.Latitude, t.End.Longitude,
			t.Price, t.Status, t.StartedAt).
		ToSql()
	if err != nil {
		return fmt.Errorf("build insert trip: %w", err)
	}

	if _, err := r.tx.Querier(ctx).Exec(ctx, q, args...); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == activeDriverIndex {
			return model.ErrDriverBusy
		}
		return fmt.Errorf("insert trip: %w", err)
	}
	return nil
}

func (r *TripRepository) AddHistory(ctx context.Context, tripID uuid.UUID, from *string, to, reason string) error {
	q, args, err := psql.Insert("trip_status_history").
		Columns("trip_id", "from_status", "to_status", "reason").
		Values(tripID, from, to, reason).
		ToSql()
	if err != nil {
		return fmt.Errorf("build insert history: %w", err)
	}
	if _, err := r.tx.Querier(ctx).Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("insert history: %w", err)
	}
	return nil
}

func (r *TripRepository) Get(ctx context.Context, id uuid.UUID) (model.Trip, error) {
	q, args, err := psql.Select(tripColumns...).From("trips").Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return model.Trip{}, fmt.Errorf("build select trip: %w", err)
	}
	t, err := scanTrip(r.tx.Querier(ctx).QueryRow(ctx, q, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Trip{}, model.ErrTripNotFound
	}
	if err != nil {
		return model.Trip{}, fmt.Errorf("select trip: %w", err)
	}
	return t, nil
}

func (r *TripRepository) Finish(ctx context.Context, id uuid.UUID, at time.Time) (model.Trip, error) {
	q, args, err := psql.Update("trips").
		Set("status", model.StatusCompleted).
		Set("finished_at", at).
		Set("updated_at", at).
		Where(sq.Eq{"id": id, "status": model.StatusActive}).
		Suffix("RETURNING " + strings.Join(tripColumns, ", ")).
		ToSql()
	if err != nil {
		return model.Trip{}, fmt.Errorf("build update trip: %w", err)
	}

	t, err := scanTrip(r.tx.Querier(ctx).QueryRow(ctx, q, args...))
	if err == nil {
		return t, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return model.Trip{}, fmt.Errorf("update trip: %w", err)
	}

	if _, err := r.Get(ctx, id); err != nil {
		return model.Trip{}, err // ErrTripNotFound или ошибка БД
	}
	return model.Trip{}, model.ErrTripCompleted
}

func scanTrip(row pgx.Row) (model.Trip, error) {
	var t model.Trip
	err := row.Scan(
		&t.ID, &t.UserID, &t.DriverID,
		&t.Start.Latitude, &t.Start.Longitude, &t.End.Latitude, &t.End.Longitude,
		&t.Price, &t.Status, &t.StartedAt, &t.FinishedAt,
	)
	if err != nil {
		return model.Trip{}, err
	}
	t.StartedAt = t.StartedAt.UTC()
	if t.FinishedAt != nil {
		u := t.FinishedAt.UTC()
		t.FinishedAt = &u
	}
	return t, nil
}
