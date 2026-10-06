package limiter

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var rateLimitScript = redis.NewScript(`
local current = redis.call('INCR', KEYS[1])
if current == 1 then
	redis.call('EXPIRE', KEYS[1], ARGV[2])
end
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

		allowed, _ := l.Allow(r.Context(), ip)
		if !allowed {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"Too Many Requests"}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}
