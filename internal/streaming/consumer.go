package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"distributed-event-hub/internal/socket"
	"distributed-event-hub/internal/telemetry"

	"github.com/redis/go-redis/v9"
)

// StreamMessageTask encapsulates a single stream message destined for worker pool processing.
type StreamMessageTask struct {
	StreamKey string         // We need which partition to send XACK later!
	Message   redis.XMessage // The actual Redis message (holds the message ID and the payload data).
}

// StreamConsumerGroup orchestrates the consumption of partitioned Redis Streams
// across multiple instances using Redis Consumer Groups.
type StreamConsumerGroup struct {
	rdb            *redis.Client
	hub            *socket.Hub
	baseStream     string
	groupName      string
	consumerName   string
	partitionCount int
	workerCount    int // Background workers we run concurrently.

	taskQueue chan StreamMessageTask // Our conveyor belt, reader puts on it, workers take them off
	wg        sync.WaitGroup
}

// NewStreamConsumerGroup constructs a new consumer group orchestrator.
func NewStreamConsumerGroup(
	rdb *redis.Client,
	hub *socket.Hub,
	baseStream string,
	groupName string,
	consumerName string,
	partitionCount int,
) *StreamConsumerGroup {
	if partitionCount <= 0 {
		partitionCount = 4
	}
	if groupName == "" {
		groupName = "hub-consumer-group"
	}
	if consumerName == "" {
		consumerName = "hub-consumer-node"
	}

	workerCount := 10 // Concurrent worker pool size

	return &StreamConsumerGroup{
		rdb:            rdb,
		hub:            hub,
		baseStream:     baseStream,
		groupName:      groupName,
		consumerName:   consumerName,
		partitionCount: partitionCount,
		workerCount:    workerCount,
		taskQueue:      make(chan StreamMessageTask, 1000), // Bounded worker pool channel
	}
}

// BootstrapConsumerGroups ensures consumer groups exist on all partition streams.
// It executes: XGROUP CREATE stream_key group_name $ MKSTREAM
// and traps/ignores BUSYGROUP errors so concurrent multi-instance launches do not crash.
func (c *StreamConsumerGroup) BootstrapConsumerGroups(ctx context.Context) error {
	for i := 0; i < c.partitionCount; i++ {
		streamKey := fmt.Sprintf("%s:%d", c.baseStream, i)

		// Execute XGROUP CREATE stream_key group_name $ MKSTREAM
		// "$" means only read brand-new messages arriving after group creation.
		// MKSTREAM automatically creates the stream key in Redis if it does not exist yet.
		err := c.rdb.XGroupCreateMkStream(ctx, streamKey, c.groupName, "$").Err()
		if err != nil {
			if strings.Contains(err.Error(), "BUSYGROUP") {
				slog.Debug("Consumer group already exists on partition, skipping bootstrap",
					"stream", streamKey,
					"group", c.groupName,
				)
				continue
			}
			return fmt.Errorf("failed to bootstrap consumer group for stream %s: %w", streamKey, err)
		}

		slog.Info("Successfully created consumer group on stream partition",
			"stream", streamKey,
			"group", c.groupName,
		)
	}

	return nil
}

// Start launches the worker pool and begins Loop A (Active Intake).
// It blocks until ctx is canceled, then cleanly shuts down workers.
func (c *StreamConsumerGroup) Start(ctx context.Context) error {
	// This is the initial setup step
	if err := c.BootstrapConsumerGroups(ctx); err != nil {
		return err
	}

	// 1. Spawn Worker Pool Goroutines
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.workerLoop(ctx, i)
	}

	// 2. Spawn Loop B: Orphan Adoption & Crash Recovery Janitor
	c.wg.Add(1)
	go c.janitorLoop(ctx)

	// 2. Build the list of all partitioned stream keys and their offset arguments
	// Redis's XREADGROUP command expects an array
	// where the first half is the stream names,
	// and the second half is the IDs.
	// In XREADGROUP, streams is passed as: [streamKey0, streamKey1, ..., ">", ">", ...]
	streams := make([]string, c.partitionCount*2)
	for i := 0; i < c.partitionCount; i++ {
		streams[i] = fmt.Sprintf("%s:%d", c.baseStream, i)
		streams[c.partitionCount+i] = ">" // Special ID '>' means: unassigned/new messages only!
	}

	slog.Info("Starting Loop A: Active Intake stream consumer",
		"group", c.groupName,
		"consumer", c.consumerName,
		"partitions", c.partitionCount,
		"workers", c.workerCount,
	)

	// 3. Loop A: Active Intake
	for {
		select {
		case <-ctx.Done():
			slog.Info("Context canceled: shutting down Active Intake consumer loop", "reason", ctx.Err())
			close(c.taskQueue)
			c.wg.Wait() // Wait for the 10 workers to finish their current tasks.
			return ctx.Err()
		default:
		}

		// Read new messages with bounded blocking (BLOCK 2s) to observe context cancellations cleanly
		entries, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.groupName,
			Consumer: c.consumerName,
			Streams:  streams,
			Count:    50,              // Fetch up to 50 messages per read across partitions
			Block:    2 * time.Second, // Bounded blocking: Wait upto 2s for a message to arrive, (if there are no new messages in the stream right now).
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				// redis.Nil indicates the 2-second block duration timed out with no new messages.
				// This is expected normal behavior; continue to next iteration.
				continue
			}
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				continue
			}
			slog.Error("Error reading from stream consumer group", "error", err)
			// Let Redis stabilize, don't spin for loop for a bit.
			time.Sleep(100 * time.Millisecond) // Back off slightly on network error
			continue
		}

		// Dispatch received messages into the bounded worker pool channel
		// streamEntry represents one specific stream partition.
		for _, streamEntry := range entries {
			for _, msg := range streamEntry.Messages {
				task := StreamMessageTask{
					StreamKey: streamEntry.Stream,
					Message:   msg,
				}

				select {
				case <-ctx.Done():
					continue
				case c.taskQueue <- task: // Blocks if worker pool is saturated (Backpressure)
				}
			}
		}
	}
}

// workerLoop is a dedicated worker goroutine that processes tasks from taskQueue
// and acknowledges them to Redis via XACK upon successful completion.
func (c *StreamConsumerGroup) workerLoop(ctx context.Context, workerID int) {
	defer c.wg.Done()

	for task := range c.taskQueue {
		// 1. Process Message: Extract payload and broadcast to WebSocket Hub
		var payloadBytes []byte
		if rawPayload, ok := task.Message.Values["payload"]; ok {
			switch p := rawPayload.(type) {
			case string:
				payloadBytes = []byte(p)
			case []byte:
				payloadBytes = p
			}
		}

		if len(payloadBytes) > 0 && c.hub != nil {
			c.hub.Broadcast <- payloadBytes
		}

		// 2. On Success: Explicitly acknowledge message via XACK targeting stream and group
		// task.Message.ID => The Redis-assigned timestamp ID of the message
		ackErr := c.rdb.XAck(ctx, task.StreamKey, c.groupName, task.Message.ID).Err()
		if ackErr != nil {
			slog.Error("Failed to XACK processed stream message",
				"stream", task.StreamKey,
				"group", c.groupName,
				"message_id", task.Message.ID,
				"worker_id", workerID,
				"error", ackErr,
			)
		} else {
			// 3. Track Dispatch Latency Histogram: from outbox_events.created_at to consumer worker's XACK
			if rawCreatedAt, ok := task.Message.Values["created_at"]; ok {
				var createdAtStr string
				switch v := rawCreatedAt.(type) {
				case string:
					createdAtStr = v
				case []byte:
					createdAtStr = string(v)
				}

				if createdAtStr != "" {
					if outboxTime, err := time.Parse(time.RFC3339Nano, createdAtStr); err == nil {
						latencySeconds := time.Since(outboxTime).Seconds()
						// Partition label extracted from stream key suffix (e.g. "events:transfers:2" -> "2")
						parts := strings.Split(task.StreamKey, ":")
						partitionLabel := parts[len(parts)-1]

						telemetry.DispatchLatencyHistogram.WithLabelValues(
							task.StreamKey,
							partitionLabel,
							c.groupName,
						).Observe(latencySeconds) // Increments the appropriate histogram bucket and updates the cumulative _sum and _count metrics.
					}
				}
			}
		}
	}
}

// janitorLoop implements Loop B: Orphan Adoption & Crash Recovery Janitor.
// It wakes up periodically (every 5 seconds) and scans each partition stream using XAUTOCLAIM.
// Any message that has been pending for longer than minIdle (30s) due to a dead, crashed,
// or partitioned worker is automatically claimed, reassigned to this consumer, and pushed into the worker pool.
func (c *StreamConsumerGroup) janitorLoop(ctx context.Context) {
	defer c.wg.Done()

	// Wake up every 5 seconds to patrol for abandoned messages.
	janitorTicker := time.NewTicker(5 * time.Second)
	defer janitorTicker.Stop()

	// The threshold. If a message was claimed less than 30 seconds ago, we leave it alone.
	minIdle := 30 * time.Second
	dlqManager := NewDLQManager(c.rdb, c.groupName, 3) // Isolate poison pills after 3 failed deliveries (delivery_count > 3)

	slog.Info("Starting Loop B: Orphan Adoption & Crash Recovery Janitor",
		"group", c.groupName,
		"consumer", c.consumerName,
		"min_idle", minIdle,
		"interval", 5*time.Second,
	)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context canceled: cleanly stopping janitor loop", "reason", ctx.Err())
			return

		case <-janitorTicker.C:
			// Inspect each stream partition for abandoned messages in the PEL (Pending Entries List)
			for i := 0; i < c.partitionCount; i++ {
				streamKey := fmt.Sprintf("%s:%d", c.baseStream, i)

				// Step 1: Inspect PEL with XPENDING - + to detect and isolate poison pills to DLQ
				if _, err := dlqManager.InspectAndIsolatePEL(ctx, streamKey); err != nil {
					slog.Error("Error during DLQ PEL inspection", "stream", streamKey, "error", err)
				}

				// Step 2: Auto-claim healthy abandoned messages whose idle time >= 30s
				startID := "0-0"
				for {
					// XAUTOCLAIM stream group consumer min-idle-time start [COUNT count]
					// Reassigns ownership of idle unacknowledged messages to this consumer!
					// nextStartID => Like a bookmark telling from where to continue in next batch.
					claimResult, nextStartID, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
						Stream:   streamKey,
						Group:    c.groupName,
						Consumer: c.consumerName,
						MinIdle:  minIdle, // 30s
						Start:    startID,
						Count:    50,
					}).Result()

					if err != nil {
						if !errors.Is(err, context.Canceled) && ctx.Err() == nil {
							slog.Error("Error executing XAUTOCLAIM on partition",
								"stream", streamKey,
								"error", err,
							)
						}
						break
					}

					// If orphaned messages were adopted, push them into our worker pool!
					if len(claimResult) > 0 {
						slog.Warn("Loop B Janitor recovered and adopted orphaned stream messages",
							"stream", streamKey,
							"recovered_count", len(claimResult),
							"reassigned_to", c.consumerName,
						)

						for _, msg := range claimResult {
							task := StreamMessageTask{
								StreamKey: streamKey,
								Message:   msg,
							}

							select {
							case <-ctx.Done():
								return
							case c.taskQueue <- task: // Feed the worker pool!
							}
						}
					}

					// If nextStartID is "0-0", we have completed a full scan of this partition's PEL
					if nextStartID == "0-0" || nextStartID == "" {
						break
					}
					startID = nextStartID // Move bookmark forward for the next batch
				}
			}
		}
	}
}
