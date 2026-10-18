package storage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository provides a centralized data access interface for financial ledger
// and outbox operations on top of the PostgreSQL connection pool.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository constructs a Repository backed by the provided pgxpool.Pool.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		pool: pool,
	}
}

// Transfer initiates an atomic, double-entry financial transfer with deterministic locking
// and outbox event staging within a single transaction.
func (r *Repository) Transfer(ctx context.Context, params TransferParams) (*TransferResult, error) {
	return TransferTx(ctx, r.pool, params)
}

// Pool returns the underlying database connection pool.
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

