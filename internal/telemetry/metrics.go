package telemetry

// metrics.go computes the lag numbers and holds them in memory,
// main.go serves those numbers over HTTP on /metrics,
// and Prometheus visits /metrics to record them and plot graphs.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	// The official Prometheus Go client.
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// ConsumerLagGauge tracks the total lag per stream partition and consumer group.
// Lag represents unprocessed messages: unread stream entries + pending unacknowledged entries in PEL.
// Gauge => A number that can go up and down.
// A GaugeVec lets us partition that number by labels (tags).
var ConsumerLagGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "eventhub",
		Subsystem: "streaming",
		Name:      "consumer_lag",
		// Together, Prometheus exports this as: eventhub_streaming_consumer_lag.
		// Help is displayed as comments in /metrics
		Help: "Total consumer lag per partition and group (unread stream entries + unacknowledged PEL entries).",
	},
	[]string{"stream", "partition", "group"}, // The three dimensions (labels) we attach to each number.
)

// DispatchLatencyHistogram tracks the end-to-end latency from PostgreSQL outbox_events.created_at
// to the consumer worker's explicit XACK in seconds.
// Histogram => Counts observations into discrete statistical "buckets".
var DispatchLatencyHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "eventhub",
		Subsystem: "streaming",
		Name:      "dispatch_latency_seconds",
		// Together, Prometheus exports this as: eventhub_streaming_dispatch_latency_seconds.
		// Help is displayed as comments in /metrics
		Help: "End-to-end latency from PostgreSQL outbox event creation to consumer worker XACK in seconds.",
		// Exponential-like buckets ranging from 5ms (fast local queue) to 10s (slow retries/backpressure)
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	},
	[]string{"stream", "partition", "group"},
)

// PipelineBatchSizeHistogram measures adaptive flusher efficiency, observing how close
// micro-batches get to the configured threshold (e.g., 500-item ceiling).
var PipelineBatchSizeHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "eventhub",
		Subsystem: "streaming",
		Name:      "pipeline_batch_size",
		// Together, Prometheus exports this as: eventhub_streaming_pipeline_batch_size.
		// Help is displayed as comments in /metrics
		Help: "Distribution of micro-batch sizes flushed to Redis Stream pipelines.",
		// Buckets spanning from single-item flush (low throughput / timeout tick) up to the 500-item ceiling
		Buckets: []float64{1, 5, 10, 25, 50, 100, 200, 300, 400, 500},
	},
	[]string{"stream"}, // The label tagging which stream was flushed (e.g., stream="events:transfers").
)

// DLQCounter tracks the total count and rate of poison-pill messages routed to Dead-Letter Queues (events:dlq:*).
// Counter => A monotonically increasing metric that only goes up (ideal for rate() queries in PromQL).
var DLQCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "eventhub",
		Subsystem: "streaming",
		Name:      "dlq_messages_total",
		// Together, Prometheus exports this as: eventhub_streaming_dlq_messages_total.
		// Help is displayed as comments in /metrics
		Help: "Total count of poison-pill messages routed to Dead-Letter Queues (events:dlq:*).",
	},
	[]string{"source_stream", "dlq_stream", "group"},
	// which stream produced the poison pill,
	// where it landed,
	// which consumer group detected it.
)

// init() runs automatically before main().
func init() {
	// Register metrics with Prometheus default registry
	prometheus.MustRegister(ConsumerLagGauge)
	prometheus.MustRegister(DispatchLatencyHistogram)
	prometheus.MustRegister(PipelineBatchSizeHistogram)
	prometheus.MustRegister(DLQCounter)
}

// LagExporter periodically computes consumer lag across all stream partitions.
// It asks Redis for the numbers and updates the Prometheus gauge.
type LagExporter struct {
	rdb            *redis.Client
	baseStream     string        // (e.g., "events:transfers").
	groupName      string        // (e.g., "hub-consumer-group").
	partitionCount int           // (e.g., 4).
	interval       time.Duration // How frequently we recompute lag.
	stopCh         chan struct{} // To signal the background goroutine during shutdown.
	wg             sync.WaitGroup
}

// NewLagExporter creates a new instance of LagExporter.
func NewLagExporter(
	rdb *redis.Client,
	baseStream string,
	groupName string,
	partitionCount int,
	interval time.Duration,
) *LagExporter {
	return &LagExporter{
		rdb:            rdb,
		baseStream:     baseStream,
		groupName:      groupName,
		partitionCount: partitionCount,
		interval:       interval,
		stopCh:         make(chan struct{}),
	}
}

// Start launches the background polling loop to update the lag gauge.
func (e *LagExporter) Start(ctx context.Context) {
	e.wg.Add(1)
	go e.run(ctx)
}

// Stop cleanly terminates the lag monitoring loop.
func (e *LagExporter) Stop() {
	close(e.stopCh)
	e.wg.Wait()
}

func (e *LagExporter) run(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	// Perform an initial calculation at startup
	e.collect(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh: // Exits if Stop() was called.
			return
		case <-ticker.C:
			e.collect(ctx)
		}
	}
}

// collect calculates the total lag per partition:
// total_lag = unread_stream_entries (from XINFO GROUPS) + pending_pel_entries (from XPENDING)
func (e *LagExporter) collect(ctx context.Context) {
	for i := 0; i < e.partitionCount; i++ {
		streamKey := fmt.Sprintf("%s:%d", e.baseStream, i)
		partitionLabel := fmt.Sprintf("%d", i)

		// 1. Fetch unread lag via XINFO GROUPS
		groups, err := e.rdb.XInfoGroups(ctx, streamKey).Result()
		if err != nil {
			// Stream or group might not exist yet before first publish
			continue
		}

		var groupLag int64
		var groupFound bool

		for _, grp := range groups {
			if grp.Name == e.groupName {
				groupFound = true
				// grp.Lag indicates unread entries beyond last-delivered-id
				groupLag = grp.Lag
				break
			}
		}

		if !groupFound {
			continue
		}

		// 2. Fetch pending (unacknowledged) entries from PEL via XPENDING
		pendingInfo, err := e.rdb.XPending(ctx, streamKey, e.groupName).Result()
		var pendingCount int64
		if err == nil && pendingInfo != nil {
			pendingCount = pendingInfo.Count
		}

		// 3. Total Consumer Lag = Unread entries + In-flight unacknowledged entries
		totalLag := float64(groupLag + pendingCount)

		// 4. Update Prometheus Gauge
		ConsumerLagGauge.WithLabelValues(streamKey, partitionLabel, e.groupName).Set(totalLag)

		slog.Debug("Updated consumer lag metric",
			"stream", streamKey,
			"partition", partitionLabel,
			"group", e.groupName,
			"unread_lag", groupLag,
			"pending_pel", pendingCount,
			"total_lag", totalLag,
		)
	}
}
