package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"distributed-event-hub/internal/storage"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StreamPublisher abstracts the destination message broker (e.g., Redis Streams / PubSub).
// This interface decouples database polling from transport delivery, enabling seamless testing with mocks.
type StreamPublisher interface {
	PublishEvent(ctx context.Context, baseStream string, aggregateID string, eventID string, payload []byte, createdAt time.Time) error
}

// We wrote two different implementations that fulfill this exact above contract:
// internal/relay/publisher.go (RedisStreamPublisher)
// internal/streaming/producer.go (AdaptiveBatchProducer)

// OutboxPoller orchestrates the asynchronous extraction of staged outbox events
// from PostgreSQL and relays them to the distributed streaming backplane.
type OutboxPoller struct {
	pool      *pgxpool.Pool
	publisher StreamPublisher
	batchSize int
	interval  time.Duration
	stream    string
}

// NewOutboxPoller constructs a new OutboxPoller with production defaults.
func NewOutboxPoller(
	pool *pgxpool.Pool,
	publisher StreamPublisher,
	batchSize int,
	interval time.Duration,
	stream string,
) *OutboxPoller {
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	if stream == "" {
		stream = "events:transfers"
	}

	return &OutboxPoller{
		pool:      pool,
		publisher: publisher,
		batchSize: batchSize,
		interval:  interval,
		stream:    stream,
	}
}

// Start launches the poller's background run loop.
// It continuously polls for unhandled outbox events on each tick until the provided context is canceled.
func (p *OutboxPoller) Start(ctx context.Context) error {
	slog.Info("Starting transactional outbox relay poller",
		"batch_size", p.batchSize,
		"interval", p.interval,
		"stream", p.stream,
	)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context canceled: cleanly stopping outbox poller run loop", "reason", ctx.Err())
			return ctx.Err()

		case <-ticker.C:
			// Process a batch of pending events on each tick.
			if err := p.pollAndDispatchBatch(ctx); err != nil {
				// We log the error but avoid crashing the loop so the poller retries on the next tick.
				slog.Error("Error during outbox poll and dispatch cycle", "error", err)
			}
		}
	}
}

// ClaimBatch acquires a batch of unprocessed or expired-lease outbox events
// using non-blocking row-level locks (SELECT ... FOR UPDATE SKIP LOCKED).
//
// Crucial Architecture Decision:
// This function commits its transaction IMMEDIATELY after marking the rows as PROCESSING
// and setting locked_until = NOW() + 30s.
// By committing immediately, database row locks and connections are NOT held
// while communicating over the network to Redis.
func (p *OutboxPoller) ClaimBatch(ctx context.Context) ([]storage.OutboxEvent, error) {
	// 1. Open a short, dedicated transaction for claiming
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin claim transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 2. Select and lock available records using FOR UPDATE SKIP LOCKED
	// We claim events that are either:
	// a) status = 'PENDING'
	// b) status = 'PROCESSING' AND locked_until < NOW() (Worker crashed, lease expired)
	claimQuery := `
		WITH claimable AS (
			SELECT sequence_id
			FROM outbox_events
			WHERE status = 'PENDING'
			   OR (status = 'PROCESSING' AND locked_until < NOW())  -- <-- ZOMBIE RECOVERY!
			ORDER BY sequence_id ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events o
		SET status = 'PROCESSING',
		    locked_until = NOW() + INTERVAL '30 seconds',  -- <-- LEASE RENEWAL!
		    updated_at = NOW()
		FROM claimable c
		WHERE o.sequence_id = c.sequence_id
		RETURNING
			o.sequence_id,
			o.id,
			o.aggregate_type,
			o.aggregate_id,
			o.event_type,
			o.payload,
			o.status,
			o.retry_count,
			o.max_retries,
			o.locked_until,
			o.last_error,
			o.created_at,
			o.updated_at;
	`

	rows, err := tx.Query(ctx, claimQuery, p.batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query and claim outbox batch: %w", err)
	}
	defer rows.Close()

	var events []storage.OutboxEvent
	for rows.Next() {
		var ev storage.OutboxEvent
		err := rows.Scan(
			&ev.SequenceID,
			&ev.ID,
			&ev.AggregateType,
			&ev.AggregateID,
			&ev.EventType,
			&ev.Payload,
			&ev.Status,
			&ev.RetryCount,
			&ev.MaxRetries,
			&ev.LockedUntil,
			&ev.LastError,
			&ev.CreatedAt,
			&ev.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan claimed outbox event: %w", err)
		}
		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading claimed outbox rows: %w", err)
	}

	// 3. Commit immediately!
	// Locks are released and the DB connection is returned to the pool immediately.
	// The rows remain safely leased to us in the database via locked_until = NOW() + 30s.
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit claim transaction: %w", err)
	}

	return events, nil
}

// pollAndDispatchBatch executes a single poll-and-dispatch iteration.
func (p *OutboxPoller) pollAndDispatchBatch(ctx context.Context) error {
	events, err := p.ClaimBatch(ctx)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	slog.Debug("Claimed batch of outbox events", "count", len(events))

	for _, event := range events {
		// 1. Attempt to dispatch the event to the streaming publisher with partitioning by AggregateID
		// Because p.publisher holds the AdaptiveBatchProducer we gave it in main.go,
		// Go's runtime dynamic dispatch jumps straight to: Line 93 of
		// internal/streaming/producer.go → func (p *AdaptiveBatchProducer) PublishEvent(...)
		pubErr := p.publisher.PublishEvent(
			ctx,
			p.stream,
			event.AggregateID, // <--- RETRIEVED FROM DB AND PASSED TO PUBLISHER!
			event.ID.String(),
			event.Payload,
			event.CreatedAt)

		if pubErr == nil {
			// 2. On Success: Transition status to PUBLISHED
			if err := p.markPublished(ctx, event.SequenceID); err != nil {
				slog.Error("Failed to mark event as published",
					"sequence_id", event.SequenceID,
					"event_id", event.ID,
					"error", err,
				)
			}
		} else {
			// 3. On Failure: Handle retry logic and poison-pill isolation (DEAD status)
			slog.Warn("Failed to publish outbox event to stream, evaluating retry policy",
				"sequence_id", event.SequenceID,
				"event_id", event.ID,
				"current_retries", event.RetryCount,
				"max_retries", event.MaxRetries,
				"error", pubErr,
			)

			if err := p.handlePublishFailure(ctx, event, pubErr); err != nil {
				slog.Error("Failed to update outbox event failure state",
					"sequence_id", event.SequenceID,
					"event_id", event.ID,
					"error", err,
				)
			}
		}
	}

	return nil
}

// markPublished marks an event as PUBLISHED and clears its lease timestamp.
func (p *OutboxPoller) markPublished(ctx context.Context, sequenceID int64) error {
	query := `
		UPDATE outbox_events
		SET status = $1,
		    locked_until = NULL, -- <-- LEASE RELEASED
		    updated_at = NOW()
		WHERE sequence_id = $2;
	`
	_, err := p.pool.Exec(ctx, query, storage.OutboxStatusPublished, sequenceID)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event %d as published: %w", sequenceID, err)
	}
	return nil
}

// handlePublishFailure increments retry_count and logs last_error.
// If retry_count + 1 >= max_retries, it isolates the event as DEAD (Dead-Letter Queue).
// Otherwise, it resets the status back to PENDING and clears locked_until for an immediate or subsequent retry.
func (p *OutboxPoller) handlePublishFailure(ctx context.Context, event storage.OutboxEvent, pubErr error) error {
	newRetryCount := event.RetryCount + 1
	errorMessage := pubErr.Error()

	if newRetryCount >= event.MaxRetries {
		// 💀 Poison-pill threshold reached: Move to DEAD queue
		slog.Error("Outbox event exceeded max retries. Moving to DEAD letter queue",
			"sequence_id", event.SequenceID,
			"event_id", event.ID,
			"retries", newRetryCount,
			"max_retries", event.MaxRetries,
		)

		query := `
			UPDATE outbox_events
			SET status = $1,
			    retry_count = $2,
			    last_error = $3,
			    locked_until = NULL,  -- <-- LEASE IMMEDIATELY CLEARED
			    updated_at = NOW()
			WHERE sequence_id = $4;
		`
		_, err := p.pool.Exec(ctx, query, storage.OutboxStatusDead, newRetryCount, errorMessage, event.SequenceID)
		if err != nil {
			return fmt.Errorf("failed to mark outbox event %d as dead: %w", event.SequenceID, err)
		}
		return nil
	}

	// Below threshold: reset to PENDING and release lock so it can be retried
	query := `
		UPDATE outbox_events
		SET status = $1,
		    retry_count = $2,
		    last_error = $3,
		    locked_until = NULL,  -- <-- LEASE IMMEDIATELY CLEARED
		    updated_at = NOW()
		WHERE sequence_id = $4;
	`
	_, err := p.pool.Exec(ctx, query, storage.OutboxStatusPending, newRetryCount, errorMessage, event.SequenceID)
	if err != nil {
		return fmt.Errorf("failed to reset outbox event %d to pending for retry: %w", event.SequenceID, err)
	}

	return nil
}
