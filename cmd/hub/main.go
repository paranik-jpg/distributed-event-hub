package main

import (
	"context"
	"distributed-event-hub/internal/cache"
	"distributed-event-hub/internal/limiter"
	"distributed-event-hub/internal/pubsub"
	"distributed-event-hub/internal/socket"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	port       string = os.Getenv("PORT")
	redis_addr string = os.Getenv("REDIS_ADDR")
)

var ctx = context.Background()

func main() {

	if redis_addr == "" {
		redis_addr = "redis:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redis_addr, // use default Addr
		Password: "",         // no password set
		DB:       0,          // use default DB
	})

	pong, err := rdb.Ping(ctx).Result()
	fmt.Println(pong, err)

	hub := socket.NewHub()
	go hub.Run()

	ps := pubsub.NewPubSub(rdb, hub, "events:broadcast")
	go ps.Start(ctx)

	ratelimiter := limiter.NewLimiter(rdb, 10, time.Second)

	metricsHandler := cache.NewMetricsHandler(rdb)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		socket.ServeWs(hub, w, r)
	})

	mux.HandleFunc("POST /api/v1/broadcast", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if err := ps.Publish(r.Context(), body); err != nil {
			http.Error(w, "Failed to publish event", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status":"published"}`))
	})

	mux.Handle("GET /api/v1/metrics/{id}", metricsHandler)

	http.ListenAndServe(":"+port, ratelimiter.Middleware(mux))
}
