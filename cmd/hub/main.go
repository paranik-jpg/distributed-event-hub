package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-event-hub/internal/cache"
	"distributed-event-hub/internal/limiter"
	"distributed-event-hub/internal/relay"
	"distributed-event-hub/internal/socket"
	"distributed-event-hub/internal/storage"
	"distributed-event-hub/internal/streaming"
	"distributed-event-hub/internal/telemetry"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	// promhttp => The HTTP handler that translates Prometheus metrics into the scrape text format.
	"github.com/redis/go-redis/v9"
)

var (
	port        string = os.Getenv("PORT")
	redisAddr   string = os.Getenv("REDIS_ADDR")
	databaseURL string = os.Getenv("DATABASE_URL")
	instanceID  string = os.Getenv("INSTANCE_ID")
)

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
	if instanceID == "" {
		instanceID = "hub-node-default"
	}

	// 1. Root Context with Signal Trap for Coordinated Graceful Teardown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Initialize Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		slog.Error("Failed to connect to Redis", "error", err)
	} else {
		slog.Info("Connected to Redis successfully", "response", pong)
	}

	// 3. Initialize PostgreSQL Connection Pool (pgxpool)
	pool, err := storage.NewPool(ctx, databaseURL)
	if err != nil {
		slog.Error("Fatal: failed to initialize PostgreSQL connection pool", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to PostgreSQL connection pool successfully")

	// 4. Initialize Storage Repository
	repo := storage.NewRepository(pool)

	// 5. Initialize In-Memory WebSocket Hub
	hub := socket.NewHub()
	go hub.Run()

	// 6. Initialize Adaptive Micro-Batching Stream Producer
	// Connects the outbox relay directly to the partitioned stream producer
	streamPublisher := streaming.NewAdaptiveBatchProducer(
		rdb,
		4,                  // 4 parallel partition streams
		500,                // 500-item micro-batch threshold
		5*time.Millisecond, // 5ms elapsed window
		100000,             // MAXLEN ~ 100000 approximate capping
	)

	// 7. Initialize and start OutboxPoller (Relay Worker) as background goroutine
	outboxPoller := relay.NewOutboxPoller(
		pool,
		streamPublisher,     // <--- We inject AdaptiveBatchProducer!
		100,                 // batch size
		50*time.Millisecond, // polling interval
		"events:transfers",  // Base Redis stream key
	)
	go func() {
		if err := outboxPoller.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Outbox poller terminated with error", "error", err)
		}
	}()

	// 8. Initialize Dual-Loop Consumer Group Engine (replaces legacy listener.go)
	// Consumes events from Redis Streams and forwards them to hub.Broadcast for live WebSocket emission
	consumerGroup := streaming.NewStreamConsumerGroup(
		rdb,
		hub,
		"events:transfers",   // Base stream
		"hub-consumer-group", // Consumer group name
		instanceID,           // Unique consumer name (e.g., app-1 or app-2)
		4,                    // 4 partitions
	)
	go func() {
		if err := consumerGroup.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Stream consumer group terminated with error", "error", err)
		}
	}()

	// 8b. Initialize Prometheus Lag Exporter (monitors consumer lag per stream partition)
	lagExporter := telemetry.NewLagExporter(
		rdb,
		"events:transfers",
		"hub-consumer-group",
		4,
		2*time.Second,
	)
	lagExporter.Start(ctx)

	// 9. Rate limiter middleware (10 requests/sec per IP)
	ratelimiter := limiter.NewLimiter(rdb, 10, time.Second)

	// 10. HTTP Routing
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

	// Deprecated route guidance
	mux.HandleFunc("POST /api/v1/broadcast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone) // HTTP 410 Gone
		w.Write([]byte(`{"error":"The /api/v1/broadcast endpoint has been deprecated. Use POST /api/v1/transfers to stage transactional events."}`))
	})

	// Cached Metrics endpoint
	mux.Handle("GET /api/v1/metrics/{id}", metricsHandler)

	// Prometheus Metrics Scrape Endpoint
	mux.Handle("GET /metrics", promhttp.Handler())

	// 11. Start HTTP Server in a separate goroutine so main can wait for OS signals
	serverAddr := ":" + port
	server := &http.Server{
		Addr:    serverAddr,
		Handler: ratelimiter.Middleware(mux),
	}

	go func() {
		slog.Info(fmt.Sprintf("HTTP Server listening on %s", serverAddr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", "error", err)
		}
	}()

	// =========================================================================
	// 12. COORDINATED GRACEFUL TEARDOWN
	// =========================================================================
	<-ctx.Done() // Block until SIGINT or SIGTERM arrives
	slog.Info("Shutdown signal received: initiating coordinated graceful teardown...")

	// Create a hard 10-second timeout for the graceful shutdown phase
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Step A: Stop accepting new HTTP requests and finish in-flight requests
	slog.Info("Teardown Step A: Shutting down HTTP server...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Error during HTTP server shutdown", "error", err)
	}

	// Step B: Flush all active producer micro-batch pipelines to Redis
	slog.Info("Teardown Step B: Flushing active stream producer pipelines...")
	streamPublisher.Close()
	lagExporter.Stop()

	// Step C: Allow background outbox poller and consumer groups to exit (triggered by ctx cancellation)
	slog.Info("Teardown Step C: Draining consumer group worker pools and outbox poller...")
	time.Sleep(500 * time.Millisecond) // Allow active in-flight worker tasks to drain and send XACK

	// Step D: Close Redis and PostgreSQL connection pools
	slog.Info("Teardown Step D: Closing database and Redis connection pools...")
	pool.Close()
	if err := rdb.Close(); err != nil {
		slog.Error("Error closing Redis client", "error", err)
	}

	slog.Info("Graceful teardown completed successfully. Goodbye!")
}
