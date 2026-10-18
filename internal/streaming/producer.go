package streaming

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
)

var (
	ErrProducerClosed = errors.New("streaming producer is closed")
)

// publishRequest encapsulates an individual event publish operation waiting to be batched.
type publishRequest struct {
	baseStream  string
	aggregateID string
	eventID     string
	payload     []byte
	errChan     chan error
	ctx         context.Context
}

// AdaptiveBatchProducer implements an adaptive micro-batching Redis Streams producer.
// It aggregates incoming publish requests in memory and flushes them in bulk using Redis Pipelines (XADD)
// with approximate stream capping (MAXLEN ~ 100000) for O(1) memory bound eviction.
type AdaptiveBatchProducer struct {
	rdb            *redis.Client
	partitionCount int
	batchThreshold int
	flushInterval  time.Duration
	// Maximum number of messages to keep in Redis before deleting the oldest ones.
	maxLenApprox int64

	// Channel where incoming requests wait.
	queue chan publishRequest
	// A signal channel used to tell the background worker to shut down cleanly.
	stop chan struct{}
	wg   sync.WaitGroup
}

// NewAdaptiveBatchProducer constructs and starts the background micro-batching producer.
func NewAdaptiveBatchProducer(
	rdb *redis.Client,
	partitionCount int,
	batchThreshold int,
	flushInterval time.Duration,
	maxLenApprox int64,
) *AdaptiveBatchProducer {
	if partitionCount <= 0 {
		partitionCount = 4
	}
	if batchThreshold <= 0 {
		batchThreshold = 500 // Micro-batch item threshold
	}
	if flushInterval <= 0 {
		flushInterval = 5 * time.Millisecond // 5ms elapsed flush window
	}
	if maxLenApprox <= 0 {
		maxLenApprox = 100000 // Keep up to ~100k events per stream partition
	}

	p := &AdaptiveBatchProducer{
		rdb:            rdb,
		partitionCount: partitionCount,
		batchThreshold: batchThreshold,
		flushInterval:  flushInterval,
		maxLenApprox:   maxLenApprox,
		queue:          make(chan publishRequest, batchThreshold*2),
		stop:           make(chan struct{}),
	}

	p.wg.Add(1)
	go p.runFlusher()

	return p
}

// GetPartitionKey computes the deterministic stream key for a given aggregate ID.
// Format: {baseStream}:{partition_id} (e.g., events:transfers:2)
func (p *AdaptiveBatchProducer) GetPartitionKey(baseStream string, aggregateID string) string {
	hash := xxhash.Sum64String(aggregateID)
	partitionID := hash % uint64(p.partitionCount)
	return fmt.Sprintf("%s:%d", baseStream, partitionID) // Returns "events:transfers:1"
}

// PublishEvent submits a publish request to the batching queue and blocks waiting
// for the pipeline flusher to execute and acknowledge the result.
func (p *AdaptiveBatchProducer) PublishEvent(
	ctx context.Context,
	baseStream string,
	aggregateID string,
	eventID string,
	payload []byte,
) error {
	// A private errChan (our walkie-talkie).
	errChan := make(chan error, 1)

	req := publishRequest{
		baseStream:  baseStream,
		aggregateID: aggregateID,
		eventID:     eventID,
		payload:     payload,
		errChan:     errChan,
		ctx:         ctx,
	}

	select {
	case <-p.stop:
		return ErrProducerClosed
	case <-ctx.Done():
		return ctx.Err()
	case p.queue <- req:
	}

	// Wait for pipeline execution acknowledgment
	// Blocks and waits on <-errChan.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// runFlusher is the dedicated background goroutine that accumulates publish requests
// until reaching either batchThreshold items or flushInterval time, then flushes them via pipeline.
func (p *AdaptiveBatchProducer) runFlusher() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	// An in-memory slice batch with initial capacity of 500.
	batch := make([]publishRequest, 0, p.batchThreshold)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.executePipeline(batch)
		// Reset batch to an empty slice.
		batch = make([]publishRequest, 0, p.batchThreshold)
	}

	for {
		select {
		case <-p.stop:
			// Drain remaining queued items before shutting down
			for {
				select {
				case req := <-p.queue:
					batch = append(batch, req)
					if len(batch) >= p.batchThreshold {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case req := <-p.queue:
			batch = append(batch, req)
			if len(batch) >= p.batchThreshold {
				flush()
			}

		case <-ticker.C:
			// Flush on elapsed tick even if batchThreshold is not met (low-latency delivery)
			flush()
		}
	}
}

// executePipeline buffers all accumulated XADD commands into a single Redis Pipeline,
// flushes them over the network in one round trip, and notifies each caller's errChan.
func (p *AdaptiveBatchProducer) executePipeline(batch []publishRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Creates a Redis Pipeline: pipe.
	// A pipeline queues commands in Go memory instead of sending them immediately.
	pipe := p.rdb.Pipeline()

	// 1. Buffer all XADD commands into the pipeline (Go's memory buffer).
	cmds := make([]*redis.StringCmd, len(batch))
	for i, req := range batch {
		streamKey := p.GetPartitionKey(req.baseStream, req.aggregateID)

		cmds[i] = pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: p.maxLenApprox, // Approximate capping (MAXLEN ~ 100000)
			Approx: true,           // Enables Redis ~ flag for O(1) macro-node trimming
			Values: map[string]interface{}{
				"event_id":     req.eventID,
				"aggregate_id": req.aggregateID,
				"payload":      req.payload,
			},
		})
	}

	// 2. Transmit the entire pipeline to Redis in ONE single network round-trip!
	_, pipeErr := pipe.Exec(ctx)

	// 3. Unpack command results and notify respective caller error channels
	for i, req := range batch {
		var err error
		if pipeErr != nil {
			// If Exec failed globally, or this specific command failed
			err = cmds[i].Err()
			if err == nil {
				err = pipeErr
			}
		} else {
			err = cmds[i].Err()
		}

		req.errChan <- err
	}
}

// Close gracefully stops the background flusher and flushes remaining events.
func (p *AdaptiveBatchProducer) Close() {
	close(p.stop)
	p.wg.Wait()
}

// PartitionCount returns the number of stream partitions.
func (p *AdaptiveBatchProducer) PartitionCount() int {
	return p.partitionCount
}
