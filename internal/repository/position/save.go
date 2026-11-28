package position

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/toooriii/tori/internal/domain/trip"
	"github.com/toooriii/tori/internal/model"
)

const pgForeignKeyViolation = "23503"

func (r *Repo) Save(ctx context.Context, p model.Position) (int64, error) {
	insertQ, insertArgs, err := psql.Insert("trip_positions").
		Columns("trip_id", "latitude", "longitude", "recorded_at").
		Values(p.TripID, p.Coordinates.Latitude, p.Coordinates.Longitude, p.RecordedAt).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("build insert position: %w", err)
	}

	var id int64
	if err := r.q.Querier(ctx).QueryRow(ctx, insertQ, insertArgs...).Scan(&id); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation {
			return 0, trip.ErrTripNotFound
		}
		return 0, fmt.Errorf("insert position: %w", err)
	}

	updateQ, updateArgs, err := psql.Update("trips").
		Set("last_position_at", sq.Expr(
			"GREATEST(COALESCE(last_position_at, ?), ?)", p.RecordedAt, p.RecordedAt,
		)).
		Set("updated_at", sq.Expr("now()")).
		Where(sq.Eq{"id": p.TripID, "status": string(trip.StatusActive)}).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("build update last_position_at: %w", err)
	}

	tag, err := r.q.Querier(ctx).Exec(ctx, updateQ, updateArgs...)
	if err != nil {
		return 0, fmt.Errorf("update last_position_at: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return 0, r.classifyNoRows(ctx, p.TripID)
	}
	return id, nil
}

func (r *Repo) classifyNoRows(ctx context.Context, tripID uuid.UUID) error {
	q, args, err := psql.Select("status").From("trips").Where(sq.Eq{"id": tripID}).ToSql()
	if err != nil {
		return fmt.Errorf("build select status: %w", err)
	}
	var status string
	if err := r.q.Querier(ctx).QueryRow(ctx, q, args...).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return trip.ErrTripNotFound
		}
		return fmt.Errorf("select status: %w", err)
	}
	return trip.ErrTripCompleted
}
