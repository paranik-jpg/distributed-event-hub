package limiter

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Why Lua instead of regular Go Redis commands?

// If we did this in Go using standard commands:

// current, _ := rdb.Incr(ctx, key).Result()
// if current == 1 {
//    rdb.Expire(ctx, key, period) // ⚠️ DANGER ZONE
// }

// The Race Condition / Failure Problem: If our Go server crashes, reboots,
// or suffers network interruption right after INCR but before EXPIRE, that key in Redis will never expire.
// The counter stays at 1 permanently, and
// subsequent requests will increment it forever, permanently blocking that IP.

// Why Lua Solves This:

// Atomicity: Redis is single-threaded when executing commands.
// When Redis runs a Lua script, it executes from start to finish without any other command or script running in between.
// Zero Partial Failures: INCR and EXPIRE happen together as a single atomic transaction on the Redis server.
// Fewer Network Round Trips: Instead of multiple round trips (INCR → wait → EXPIRE → wait), we send 1 command to Redis.
var rateLimitScript = redis.NewScript(`
-- KEYS[1] is passed as "rate_limit:" + ip.
local current = redis.call('INCR', KEYS[1])
if current == 1 then
-- ARGV[2] is the window length in seconds int(l.period.Seconds()) (e.g. 1).
	redis.call('EXPIRE', KEYS[1], ARGV[2])
end
-- ARGV[1] is the threshold l.rate (e.g. 10).
if current > tonumber(ARGV[1]) then
	return 0
else
	return 1
end
`)

type Limiter struct {
	rdb    *redis.Client
	rate   int
	period time.Duration
}

func NewLimiter(rdb *redis.Client, rate int, period time.Duration) *Limiter {
	return &Limiter{
		rdb:    rdb,
		rate:   rate,
		period: period,
	}
}

func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	// Run the atomic Lua script in Redis
	res, err := rateLimitScript.Run(ctx, l.rdb, []string{"rate_limit:" + key}, l.rate, int(l.period.Seconds())).Int64()

	if err != nil {
		return false, err
	} else if res == 1 {
		return true, nil
	}
	return false, nil
}

func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ip string
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if comma := strings.Index(xff, ","); comma != -1 {
				ip = strings.TrimSpace(xff[:comma])
			} else {
				ip = strings.TrimSpace(xff)
			}
		} else {
			var err error
			ip, _, err = net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}
		}

		// Check Redis
		allowed, _ := l.Allow(r.Context(), ip)
		if !allowed {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1") // Tell the client to back off for 1 second
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"Too Many Requests"}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}
