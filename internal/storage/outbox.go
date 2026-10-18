package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// OutboxStatus defines the lifecycle state machine of a transactional outbox event.
type OutboxStatus string

const (
	OutboxStatusPending    OutboxStatus = "PENDING"
	OutboxStatusProcessing OutboxStatus = "PROCESSING"
	OutboxStatusPublished  OutboxStatus = "PUBLISHED"
	OutboxStatusDead       OutboxStatus = "DEAD"
)

// OutboxEvent represents a persisted domain event waiting to be relayed to Redis Pub/Sub.
type OutboxEvent struct {
	SequenceID    int64        `json:"sequence_id"`
	ID            uuid.UUID    `json:"id"`
	AggregateType string       `json:"aggregate_type"`
	AggregateID   string       `json:"aggregate_id"`
	EventType     string       `json:"event_type"`
	Payload       []byte       `json:"payload"`
	Status        OutboxStatus `json:"status"`
	RetryCount    int          `json:"retry_count"`
	MaxRetries    int          `json:"max_retries"`
	LockedUntil   *time.Time   `json:"locked_until,omitempty"`
	LastError     *string      `json:"last_error,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
}

// StageOutboxEvent serializes the event payload and inserts a record into outbox_events
// within the caller's active database transaction (tx).
//
// By inserting into outbox_events inside the SAME transaction as the balance updates,
// we eliminate the Dual-Write Problem with zero window for data drift.
func StageOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	aggregateType string,
	aggregateID string,
	eventType string,
	payloadData any,
) (*OutboxEvent, error) {
	// 1. Serialize domain event to JSON
	payloadBytes, err := json.Marshal(payloadData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal outbox event payload: %w", err)
	}

	// 2. Insert outbox record with default PENDING status
	query := `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload,
			status
		)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING
			sequence_id,
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			payload,
			status,
			retry_count,
			max_retries,
			locked_until,
			last_error,
			created_at,
			updated_at;
	`

	var event OutboxEvent
	err = tx.QueryRow(
		ctx,
		query,
		aggregateType,
		aggregateID,
		eventType,
		payloadBytes,
		OutboxStatusPending,
	).Scan(
		&event.SequenceID,
		&event.ID,
		&event.AggregateType,
		&event.AggregateID,
		&event.EventType,
		&event.Payload,
		&event.Status,
		&event.RetryCount,
		&event.MaxRetries,
		&event.LockedUntil,
		&event.LastError,
		&event.CreatedAt,
		&event.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to stage outbox event: %w", err)
	}

	return &event, nil
}
