package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"distributed-event-hub/internal/cache"
	"distributed-event-hub/internal/limiter"
	"distributed-event-hub/internal/pubsub"
	"distributed-event-hub/internal/relay"
	"distributed-event-hub/internal/socket"
	"distributed-event-hub/internal/storage"
	"distributed-event-hub/internal/streaming"

	"github.com/redis/go-redis/v9"
)

var (
	port        string = os.Getenv("PORT")
	redisAddr   string = os.Getenv("REDIS_ADDR")
	databaseURL string = os.Getenv("DATABASE_URL")
)

var ctx = context.Background()

func main() {
	if port == "" {
		port = "8080"
	}
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	if databaseURL == "" {
		databaseURL = "postgres://postgres:password@localhost:5432/event_hub?sslmode=disable"
	}

	// 1. Initialize Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		slog.Error("Failed to connect to Redis", "error", err)
	} else {
		slog.Info("Connected to Redis", "response", pong)
	}

	// 2. Initialize PostgreSQL Connection Pool (pgxpool)
	pool, err := storage.NewPool(ctx, databaseURL)
	if err != nil {
		slog.Error("Fatal: failed to initialize PostgreSQL connection pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("Connected to PostgreSQL connection pool successfully")

	// 3. Initialize Storage Repository
	repo := storage.NewRepository(pool)

	// 4. Initialize WebSocket Hub
	hub := socket.NewHub()
	go hub.Run()

	// 5. Initialize Pub/Sub listener for real-time WebSocket distribution
	ps := pubsub.NewPubSub(rdb, hub, "events:broadcast")
	go ps.Start(ctx)

	// 6. Initialize Adaptive Micro-Batching Stream Producer (500 items or 5ms flush window, MAXLEN ~ 100k)
	streamPublisher := streaming.NewAdaptiveBatchProducer(
		rdb,
		4,                    // 4 parallel partition streams
		500,                  // 500-item micro-batch threshold
		5*time.Millisecond,   // 5ms elapsed window
		100000,               // MAXLEN ~ 100000 approximate capping
	)
	defer streamPublisher.Close()

	outboxPoller := relay.NewOutboxPoller(
		pool,
		streamPublisher,
		100,                 // batch size
		50*time.Millisecond, // polling interval
		"events:transfers",  // Base Redis stream key
	)
	go func() {
		if err := outboxPoller.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Outbox poller terminated with error", "error", err)
		}
	}()

	// 7. Rate limiter middleware (10 requests/sec per IP)
	ratelimiter := limiter.NewLimiter(rdb, 10, time.Second)

	// 8. HTTP Routing
	metricsHandler := cache.NewMetricsHandler(rdb)
	mux := http.NewServeMux()

	// WebSocket Upgrade endpoint
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		socket.ServeWs(hub, w, r)
	})

	// POST /api/v1/transfers: Atomic financial transfer with deterministic locking & outbox staging
	mux.HandleFunc("POST /api/v1/transfers", func(w http.ResponseWriter, r *http.Request) {
		var params storage.TransferParams
		if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
			http.Error(w, `{"error":"Invalid request payload"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		result, err := repo.Transfer(r.Context(), params)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case errors.Is(err, storage.ErrInsufficientBalance):
				w.WriteHeader(http.StatusUnprocessableEntity)
				w.Write([]byte(`{"error":"Insufficient balance"}`))
			case errors.Is(err, storage.ErrAccountNotFound):
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error":"One or more accounts not found"}`))
			case errors.Is(err, storage.ErrCurrencyMismatch):
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"error":"Currency mismatch between accounts"}`))
			case errors.Is(err, storage.ErrSelfTransfer):
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"error":"Cannot transfer to the same account"}`))
			case errors.Is(err, storage.ErrInvalidAmount):
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"error":"Transfer amount must be strictly positive"}`))
			default:
				slog.Error("Unexpected error during transfer execution", "error", err)
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error":"Internal transaction failure"}`))
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(result)
	})

	// Deprecated: Unauthenticated raw broadcast route (returns 410 Gone with migration guidance)
	mux.HandleFunc("POST /api/v1/broadcast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone) // HTTP 410 Gone
		w.Write([]byte(`{"error":"The /api/v1/broadcast endpoint has been deprecated. Use POST /api/v1/transfers to stage transactional events."}`))
	})

	// Cached Metrics endpoint
	mux.Handle("GET /api/v1/metrics/{id}", metricsHandler)

	// Wrap entire mux with distributed rate limiter
	serverAddr := ":" + port
	slog.Info(fmt.Sprintf("HTTP Server listening on %s", serverAddr))
	if err := http.ListenAndServe(serverAddr, ratelimiter.Middleware(mux)); err != nil {
		slog.Error("Server terminated", "error", err)
	}
}
