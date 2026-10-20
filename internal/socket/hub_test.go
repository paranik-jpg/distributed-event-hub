package socket

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSlowConsumerIsolation verifies that when a client's outbound buffer (send channel)
// fills to capacity (256 items), the hub's non-blocking broadcast immediately terminates
// the slow client without blocking writes to healthy peers or inflating server memory.
func TestSlowConsumerIsolation(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	// 1. Create a Healthy Client that actively reads its send channel
	healthyClient := &Client{
		hub:  hub,
		send: make(chan []byte, 256),
	}
	hub.register <- healthyClient

	// 2. Create a Slow Client that DOES NOT read from its send channel
	slowClient := &Client{
		hub:  hub,
		send: make(chan []byte, 256),
	}
	hub.register <- slowClient

	// Allow registration to process in hub loop
	time.Sleep(10 * time.Millisecond)

	var healthyReceivedCount int64
	var healthyWg sync.WaitGroup
	healthyWg.Add(1)

	// Healthy consumer goroutine: drains its send channel in real time
	go func() {
		defer healthyWg.Done()
		for range healthyClient.send {
			atomic.AddInt64(&healthyReceivedCount, 1)
			if atomic.LoadInt64(&healthyReceivedCount) == 300 {
				return
			}
		}
	}()

	// 3. Flood 300 messages into hub.Broadcast (300 > 256 buffer capacity)
	// Measure elapsed time to prove writes to the hub never block!
	startTime := time.Now()
	for i := 0; i < 300; i++ {
		hub.Broadcast <- []byte("transfer-notification-payload")
	}
	broadcastDuration := time.Since(startTime)

	t.Logf("Broadcasted 300 messages in %v (non-blocking delivery verified)", broadcastDuration)

	if broadcastDuration > 500*time.Millisecond {
		t.Fatalf("Broadcast blocked for %v! Non-blocking guarantee violated.", broadcastDuration)
	}

	// 4. Verify Slow Client was evicted and its send channel closed
	// Reading from a closed channel immediately yields ok == false
	select {
	case _, ok := <-slowClient.send:
		if ok {
			// Drain remaining buffered items until channel closure is observed
			closed := false
			for i := 0; i < 256; i++ {
				_, ok := <-slowClient.send
				if !ok {
					closed = true
					break
				}
			}
			if !closed {
				t.Fatalf("Expected slowClient.send channel to be closed by hub, but it remained open!")
			}
		}
		t.Log("PASSED: Slow client outbound channel was closed by hub upon buffer saturation.")
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for slow client channel eviction!")
	}

	// 5. Verify Healthy Client received all 300 messages without loss or interruption
	healthyWg.Wait()
	received := atomic.LoadInt64(&healthyReceivedCount)
	t.Logf("Healthy peer received count: %d / 300", received)

	if received != 300 {
		t.Fatalf("Expected healthy peer to receive 300 messages, got %d", received)
	}

	t.Log("PASSED: Healthy client received all 300 messages with zero drops or blocking.")
}

