package pubsub

import (
	"context"
	"distributed-event-hub/internal/socket"

	"github.com/redis/go-redis/v9"
)

type PubSub struct {
	rdb *redis.Client

	// To forward recieved messages
	hub *socket.Hub

	// e.g. "events:broadcast"
	channelName string
}

func NewPubSub(rdb *redis.Client, hub *socket.Hub, channelName string) *PubSub {
	return &PubSub{
		rdb:         rdb,
		hub:         hub,
		channelName: channelName,
	}
}

func (p *PubSub) Start(ctx context.Context) {
	// Subscribe Redis channel
	pubsub := p.rdb.Subscribe(ctx, p.channelName)

	_, err := pubsub.Receive(ctx)

	if err != nil {
		return
	}

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			pubsub.Close()
			return
		// Receive message from Redis channel
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// Push payload to respective local hub.Broadcast channels
			p.hub.Broadcast <- []byte(msg.Payload)
		}
	}
}

func (p *PubSub) Publish(ctx context.Context, payload []byte) error {
	// Issue to Redis channel events:broadcast.
	return p.rdb.Publish(ctx, p.channelName, payload).Err()
}
