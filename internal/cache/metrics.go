package cache

import (
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

type Metric struct {
	ID          string    `json:"id"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryUsage float64   `json:"memory_usage"`
	Timestamp   time.Time `json:"timestamp"`
}

type MetricsHandler struct {
	rdb *redis.Client
}

func NewMetricsHandler(rdb *redis.Client) *MetricsHandler {
	return &MetricsHandler{
		rdb: rdb,
	}
}

func fetchFromDB(id string) *Metric {
	time.Sleep(50 * time.Millisecond)

	return &Metric{
		ID:        id,
		Timestamp: time.Now(),
	}
}

func (h *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	key := "metrics:" + id
	val, err := h.rdb.Get(r.Context(), key).Result()

	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write([]byte(val))
		return
	}

	if errors.Is(err, redis.Nil) {
		metric := fetchFromDB(id)
		payload, _ := json.Marshal(metric)

		jitter := time.Duration(rand.Intn(10)) * time.Second
		ttl := 60*time.Second + jitter

		h.rdb.Set(r.Context(), key, payload, ttl)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(payload))
	}
}
