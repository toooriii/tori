package trip

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type StaleTrip struct {
	TripID   uuid.UUID
	DriverID uuid.UUID
	StaleAt  time.Time
}

type StaleCursor struct {
	StaleAt time.Time
	TripID  uuid.UUID
}

func (r *TripRepository) Stale(ctx context.Context, staleAfter time.Duration, limit int, after *StaleCursor) ([]StaleTrip, error) {
	const base = `
SELECT id, driver_id, COALESCE(last_position_at, started_at) AS stale_at
FROM trips
WHERE status = 'active'
  AND COALESCE(last_position_at, started_at) < now() - make_interval(secs => $1)`

	q := base
	args := []any{int(staleAfter.Seconds())}

	if after != nil {
		q += `
  AND (COALESCE(last_position_at, started_at), id) > ($2, $3)`
		args = append(args, after.StaleAt, after.TripID)
	}

	q += `
ORDER BY COALESCE(last_position_at, started_at) ASC, id ASC`

	if limit > 0 {
		q += fmt.Sprintf("\nLIMIT $%d", len(args)+1)
		args = append(args, limit)
	}

	rows, err := r.q.Querier(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("select stale trips: %w", err)
	}
	defer rows.Close()

	var out []StaleTrip
	for rows.Next() {
		var s StaleTrip
		if err := rows.Scan(&s.TripID, &s.DriverID, &s.StaleAt); err != nil {
			return nil, fmt.Errorf("scan stale trip: %w", err)
		}
		s.StaleAt = s.StaleAt.UTC()
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale trips: %w", err)
	}
	return out, nil
}
