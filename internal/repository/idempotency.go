package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/toooriii/tori/internal/txmanager"
)

type IdempotencyRecord struct {
	Key         uuid.UUID
	RequestHash string
	TripID      uuid.UUID
}

type IdempotencyRepository struct {
	tx *txmanager.Manager
}

func NewIdempotencyRepository(tx *txmanager.Manager) *IdempotencyRepository {
	return &IdempotencyRepository{tx: tx}
}

var ErrKeyAlreadyExists = errors.New("idempotency key already exists")

func (r *IdempotencyRepository) Save(ctx context.Context, key uuid.UUID, hash string, tripID uuid.UUID) error {
	q, args, err := psql.Insert("idempotency_keys").
		Columns("key", "request_hash", "trip_id").
		Values(key, hash, tripID).
		ToSql()
	if err != nil {
		return fmt.Errorf("build insert idempotency key: %w", err)
	}

	if _, err := r.tx.Querier(ctx).Exec(ctx, q, args...); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return ErrKeyAlreadyExists
		}
		return fmt.Errorf("insert idempotency key: %w", err)
	}
	return nil
}

func (r *IdempotencyRepository) Get(ctx context.Context, key uuid.UUID) (IdempotencyRecord, error) {
	q, args, err := psql.Select("key", "request_hash", "trip_id").
		From("idempotency_keys").
		Where("key = ?", key).
		ToSql()
	if err != nil {
		return IdempotencyRecord{}, fmt.Errorf("build select idempotency key: %w", err)
	}

	var rec IdempotencyRecord
	err = r.tx.Querier(ctx).QueryRow(ctx, q, args...).Scan(&rec.Key, &rec.RequestHash, &rec.TripID)
	if err != nil {
		return IdempotencyRecord{}, fmt.Errorf("select idempotency key: %w", err)
	}
	return rec, nil
}
