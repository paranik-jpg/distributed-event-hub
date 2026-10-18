package relay

import (
	"context"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
)

// RedisStreamPublisher implements StreamPublisher using Redis Streams (XADD) with deterministic partitioning.
type RedisStreamPublisher struct {
	rdb            *redis.Client
	partitionCount int
}

// NewRedisStreamPublisher constructs a publisher targeting Redis Streams with a default of 4 partitions.
func NewRedisStreamPublisher(rdb *redis.Client) *RedisStreamPublisher {
	return &RedisStreamPublisher{
		rdb:            rdb,
		partitionCount: 4,
	}
}

// PublishEvent appends the serialized event to the partitioned Redis Stream via XADD.
func (p *RedisStreamPublisher) PublishEvent(ctx context.Context, baseStream string, aggregateID string, eventID string, payload []byte) error {
	partitionID := xxhash.Sum64String(aggregateID) % uint64(p.partitionCount)
	streamKey := fmt.Sprintf("%s:%d", baseStream, partitionID)

	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"event_id":     eventID,
			"aggregate_id": aggregateID,
			"payload":      payload,
		},
	}).Err()
}

