package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool initializes and configures a high-performance PostgreSQL connection pool.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	// Parse the raw connection string into a pgxpool configuration object.
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	// Configure pool parameters for production scalability.
	config.MaxConns = 25                      // Maximum number of active connections in the pool.
	config.MinConns = 5                       // Minimum warm (pre-authenticated, to handle sudden bursts of incoming requests) idle connections maintained at all times.
	config.MaxConnLifetime = 1 * time.Hour    // Maximum age of a connection before being closed and refreshed.
	config.MaxConnIdleTime = 30 * time.Minute // Duration an idle connection can remain open before pruning down to MinConns.

	// Construct the pool using the configured settings.
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connectivity and fail fast if the database is unreachable or credentials are bad.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
