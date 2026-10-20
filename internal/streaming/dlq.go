package streaming

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"distributed-event-hub/internal/telemetry"

	"github.com/redis/go-redis/v9"
)

// DLQManager isolates poison-pill messages by inspecting the Pending Entries List (PEL).
// When an event repeatedly crashes consumers (exceeding maxDeliveryAttempts),
// DLQManager routes it to a Dead-Letter Queue stream and removes it from the active PEL with XACK.
type DLQManager struct {
	rdb                 *redis.Client
	groupName           string
	maxDeliveryAttempts int64
}

// NewDLQManager constructs a new DLQ manager for poison-pill isolation.
func NewDLQManager(rdb *redis.Client, groupName string, maxDeliveryAttempts int64) *DLQManager {
	if maxDeliveryAttempts <= 0 {
		maxDeliveryAttempts = 3 // Poison-pill threshold: isolate after 3 failed deliveries (delivery_count > 3)
	}
	return &DLQManager{
		rdb:                 rdb,
		groupName:           groupName,
		maxDeliveryAttempts: maxDeliveryAttempts,
	}
}

// InspectAndIsolatePEL scans the Pending Entries List (PEL) using XPENDING with range '-' to '+'.
// It detects messages whose delivery count exceeds maxDeliveryAttempts, moves them to a DLQ stream,
// and clears them from the source stream via XACK so consumers never choke on them again.
func (d *DLQManager) InspectAndIsolatePEL(ctx context.Context, streamKey string) (int, error) {
	// 1. Query detailed pending entries via XPENDING stream group - + 50
	// '-' denotes the minimum possible message ID (the very beginning of the PEL)
	// '+' denotes the maximum possible message ID (the very end of the PEL)
	// XPendingExt gives us metadata, not return the actual body/payload of the message.
	pendingEntries, err := d.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  d.groupName,
		Start:  "-",
		End:    "+",
		Count:  50,
	}).Result()

	if err != nil {
		return 0, fmt.Errorf("failed to query XPendingExt for stream %s: %w", streamKey, err)
	}

	isolatedCount := 0

	for _, entry := range pendingEntries {
		// 2. Check if delivery count exceeded poison-pill threshold
		if entry.RetryCount >= d.maxDeliveryAttempts {
			slog.Error("Poison-pill message detected in PEL! Exceeded max delivery attempts. Isolating to DLQ",
				"stream", streamKey,
				"group", d.groupName,
				"message_id", entry.ID,
				"consumer", entry.Consumer,
				"delivery_count", entry.RetryCount,
				"max_threshold", d.maxDeliveryAttempts,
				"idle_time", entry.Idle,
			)

			// 3. Fetch original message payload from the stream before isolating
			msgSlice, err := d.rdb.XRange(ctx, streamKey, entry.ID, entry.ID).Result()
			var originalValues map[string]interface{}
			if err == nil && len(msgSlice) > 0 {
				// msgSlice[0].Values is the map containing the original data fields
				originalValues = msgSlice[0].Values
			} else {
				// Fall back
				originalValues = map[string]interface{}{
					"unreadable": true,
					"error":      "failed to fetch original message payload",
				}
			}

			// 4. Construct Dead-Letter Queue stream key: events:dlq:{stream}
			// (e.g. events:transfers:0 -> events:dlq:events:transfers:0)
			dlqStreamKey := fmt.Sprintf("events:dlq:%s", streamKey)

			// Extract original payload bytes if present
			payload := ""
			if p, ok := originalValues["payload"]; ok {
				payload = fmt.Sprintf("%v", p)
			}

			// Enrich DLQ record with diagnostic fields:
			// original_id, payload, attempts, failure_reason, and timestamp.
			dlqValues := map[string]interface{}{
				"original_id":    entry.ID,
				"payload":        payload,
				"attempts":       entry.RetryCount,
				"failure_reason": fmt.Sprintf("exceeded max delivery attempts (%d); last consumer: %s; idle time: %v", entry.RetryCount, entry.Consumer, entry.Idle),
				"timestamp":      time.Now().UTC().Format(time.RFC3339),
			}

			// 5. Execute Atomic Redis Pipeline:
			// 1. XADD the failed event into events:dlq:{stream}
			// 2. XACK the message on the original stream to clear it from the active group's PEL
			// 3. XDEL the message from the source stream to prevent memory bloat
			pipe := d.rdb.Pipeline()

			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: dlqStreamKey,
				Values: dlqValues,
			})
			pipe.XAck(ctx, streamKey, d.groupName, entry.ID) // Remove it from the active group's PEL
			pipe.XDel(ctx, streamKey, entry.ID)              // Clean up the original source stream

			_, pipeErr := pipe.Exec(ctx)
			if pipeErr != nil {
				slog.Error("Failed to execute atomic DLQ isolation pipeline",
					"stream", streamKey,
					"dlq_stream", dlqStreamKey,
					"message_id", entry.ID,
					"error", pipeErr,
				)
				continue
			}

			slog.Info("Successfully executed atomic DLQ isolation (XADD + XACK + XDEL)",
				"source_stream", streamKey,
				"dlq_stream", dlqStreamKey,
				"message_id", entry.ID,
				"attempts", entry.RetryCount,
			)

			// 6. Track DLQ Counter: Increment total poison-pill count for this source and DLQ stream
			// streamKey is the exact name of the source stream being patrolled int the Janitor loop of consumer.go !
			telemetry.DLQCounter.WithLabelValues(
				streamKey,    // Label 1: "source_stream" -> "events:transfers:0"
				dlqStreamKey, // Label 2: "dlq_stream"    -> "events:dlq:events:transfers:0"
				d.groupName,  // Label 3: "group"         -> "hub-consumer-group"
			).Inc()
			isolatedCount++
		}
	}

	return isolatedCount, nil
}
