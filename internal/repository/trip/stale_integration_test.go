//go:build integration

package trip_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/toooriii/tori/internal/config"
	"github.com/toooriii/tori/internal/domain/trip"
	"github.com/toooriii/tori/internal/infra/postgres"
	"github.com/toooriii/tori/internal/infra/postgres/transaction"
	repo "github.com/toooriii/tori/internal/repository/trip"
)

func setupStale(t *testing.T) (*repo.TripRepository, *pgxpool.Pool, context.Context) {
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

	if _, err := pool.Exec(ctx, "TRUNCATE trips, trip_status_history, idempotency_keys, trip_positions RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	tx := transaction.New(pool, pgx.ReadCommitted)
	return repo.New(tx), pool, ctx
}

func insertTrip(t *testing.T, ctx context.Context, pool *pgxpool.Pool, startedAt time.Time, lastPos *time.Time, status trip.Status) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(ctx, `
INSERT INTO trips (id, user_id, driver_id, start_latitude, start_longitude, end_latitude, end_longitude, price, status, started_at, last_position_at, finished_at)
VALUES ($1, $2, $3, 1, 1, 2, 2, 100, $4, $5, $6, $7)`,
		id, uuid.New(), uuid.New(), string(status), startedAt, lastPos, finishedAt(status, startedAt))
	if err != nil {
		t.Fatalf("insert trip: %v", err)
	}
	return id
}

func finishedAt(status trip.Status, startedAt time.Time) *time.Time {
	if status == trip.StatusCompleted {
		t := startedAt.Add(time.Hour)
		return &t
	}
	return nil
}

func TestStale_SelectsOnlyStaleActive(t *testing.T) {
	r, pool, ctx := setupStale(t)
	now := time.Now().UTC()

	fresh := insertTrip(t, ctx, pool, now.Add(-time.Hour), ptr(now.Add(-10*time.Second)), trip.StatusActive)
	completed := insertTrip(t, ctx, pool, now.Add(-time.Hour), ptr(now.Add(-10*time.Minute)), trip.StatusCompleted)
	noPos := insertTrip(t, ctx, pool, now.Add(-time.Hour), nil, trip.StatusActive)
	oldPos := insertTrip(t, ctx, pool, now.Add(-time.Hour), ptr(now.Add(-10*time.Minute)), trip.StatusActive)

	got, err := r.Stale(ctx, 60*time.Second, 0, nil)
	if err != nil {
		t.Fatalf("stale: %v", err)
	}
	ids := map[uuid.UUID]bool{}
	for _, s := range got {
		ids[s.TripID] = true
	}
	if ids[fresh] {
		t.Fatal("fresh trip must not be selected")
	}
	if ids[completed] {
		t.Fatal("completed trip must not be selected")
	}
	if !ids[noPos] {
		t.Fatal("trip without positions must be selected")
	}
	if !ids[oldPos] {
		t.Fatal("trip with old position must be selected")
	}
	if got[0].TripID != noPos {
		t.Fatalf("first = %s, want trip without positions (oldest stale_at)", got[0].TripID)
	}
}

func TestStale_BatchBoundaryNoDuplicatesNoLoss(t *testing.T) {
	r, pool, ctx := setupStale(t)
	now := time.Now().UTC()
	same := now.Add(-10 * time.Minute)
	var all []uuid.UUID
	for i := 0; i < 5; i++ {
		all = append(all, insertTrip(t, ctx, pool, now.Add(-time.Hour), &same, trip.StatusActive))
	}
	for i := 0; i < 5; i++ {
		ts := now.Add(-time.Duration(20+i) * time.Minute)
		all = append(all, insertTrip(t, ctx, pool, now.Add(-time.Hour), &ts, trip.StatusActive))
	}

	seen := map[uuid.UUID]int{}
	var after *repo.StaleCursor
	for {
		page, err := r.Stale(ctx, 60*time.Second, 3, after)
		if err != nil {
			t.Fatalf("stale page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, s := range page {
			seen[s.TripID]++
		}
		last := page[len(page)-1]
		after = &repo.StaleCursor{StaleAt: last.StaleAt, TripID: last.TripID}
		if len(page) < 3 {
			break
		}
	}
	if len(seen) != len(all) {
		t.Fatalf("seen %d trips, want %d", len(seen), len(all))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("trip %s seen %d times", id, n)
		}
	}
}

func ptr(t time.Time) *time.Time { return &t }
