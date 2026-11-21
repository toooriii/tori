//go:build integration

package position_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/toooriii/tori/internal/config"
	"github.com/toooriii/tori/internal/domain/position"
	"github.com/toooriii/tori/internal/domain/trip"
	"github.com/toooriii/tori/internal/infra/postgres"
	"github.com/toooriii/tori/internal/infra/postgres/transaction"
	"github.com/toooriii/tori/internal/model"
	posrepo "github.com/toooriii/tori/internal/repository/position"
)

func setupPos(t *testing.T) (*posrepo.Repo, *pgxpool.Pool, *transaction.Manager, context.Context) {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, "TRUNCATE trips, trip_positions RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	tx := transaction.New(pool, pgx.ReadCommitted)
	return posrepo.New(tx), pool, tx, ctx
}

func insertActiveTrip(t *testing.T, ctx context.Context, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO trips (id, user_id, driver_id, start_latitude, start_longitude, end_latitude, end_longitude, price, status, started_at) VALUES ($1,$2,$3,1,1,2,2,100,'active',now())`,
		id, uuid.New(), uuid.New())
	if err != nil {
		t.Fatalf("insert trip: %v", err)
	}
	return id
}

func TestSave_UpdatesLastPositionAtMonotonic(t *testing.T) {
	r, pool, tx, ctx := setupPos(t)
	tripID := insertActiveTrip(t, ctx, pool)

	newer := time.Now().UTC().Truncate(time.Microsecond)
	older := newer.Add(-5 * time.Minute)

	err := tx.Do(ctx, func(ctx context.Context) error {
		_, err := r.Save(ctx, model.Position{
			TripID: tripID, Coordinates: position.Coordinates{Latitude: 1, Longitude: 1}, RecordedAt: newer,
		})
		return err
	})
	if err != nil {
		t.Fatalf("save newer: %v", err)
	}

	err = tx.Do(ctx, func(ctx context.Context) error {
		_, err := r.Save(ctx, model.Position{
			TripID: tripID, Coordinates: position.Coordinates{Latitude: 2, Longitude: 2}, RecordedAt: older,
		})
		return err
	})
	if err != nil {
		t.Fatalf("save older: %v", err)
	}

	var got time.Time
	if err := pool.QueryRow(ctx, "SELECT last_position_at FROM trips WHERE id=$1", tripID).Scan(&got); err != nil {
		t.Fatalf("select last_position_at: %v", err)
	}
	if !got.UTC().Equal(newer) {
		t.Fatalf("last_position_at = %s, want %s", got.UTC(), newer)
	}

	var cnt int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM trip_positions WHERE trip_id=$1", tripID).Scan(&cnt); err != nil {
		t.Fatalf("count positions: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("positions = %d, want 2", cnt)
	}
}

func TestSave_CompletedTrip_Rollback(t *testing.T) {
	r, pool, tx, ctx := setupPos(t)
	tripID := insertActiveTrip(t, ctx, pool)
	if _, err := pool.Exec(ctx, "UPDATE trips SET status='completed', finished_at=now() WHERE id=$1", tripID); err != nil {
		t.Fatalf("complete trip: %v", err)
	}

	err := tx.Do(ctx, func(ctx context.Context) error {
		_, err := r.Save(ctx, model.Position{
			TripID: tripID, Coordinates: position.Coordinates{Latitude: 1, Longitude: 1}, RecordedAt: time.Now().UTC(),
		})
		return err
	})
	if !errors.Is(err, trip.ErrTripCompleted) {
		t.Fatalf("err = %v, want ErrTripCompleted", err)
	}

	var cnt int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM trip_positions WHERE trip_id=$1", tripID).Scan(&cnt); err != nil {
		t.Fatalf("count positions: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("positions = %d after rollback, want 0", cnt)
	}
}

func TestSave_UnknownTrip_NotFound(t *testing.T) {
	r, _, tx, ctx := setupPos(t)
	err := tx.Do(ctx, func(ctx context.Context) error {
		_, err := r.Save(ctx, model.Position{
			TripID: uuid.New(), Coordinates: position.Coordinates{Latitude: 1, Longitude: 1}, RecordedAt: time.Now().UTC(),
		})
		return err
	})
	if !errors.Is(err, trip.ErrTripNotFound) {
		t.Fatalf("err = %v, want ErrTripNotFound", err)
	}
}
