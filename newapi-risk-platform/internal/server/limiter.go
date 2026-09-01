package server

import (
	"sync"
	"time"
)

type localBucket struct {
	tokens  float64
	updated time.Time
}
type localLimiter struct {
	mu          sync.Mutex
	buckets     map[string]localBucket
	lastCleanup time.Time
}

func newLocalLimiter() *localLimiter { return &localLimiter{buckets: map[string]localBucket{}} }
func (l *localLimiter) Allow(key string, rate, burst int) bool {
	if rate < 1 || burst < 1 {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	bucket, ok := l.buckets[key]
	if !ok {
		bucket = localBucket{tokens: float64(burst), updated: now}
	}
	elapsed := now.Sub(bucket.updated).Seconds()
	bucket.tokens += elapsed * float64(rate)
	if bucket.tokens > float64(burst) {
		bucket.tokens = float64(burst)
	}
	bucket.updated = now
	allowed := bucket.tokens >= 1
	if allowed {
		bucket.tokens--
	}
	l.buckets[key] = bucket

	// Redis is the production limiter. This bounded fallback avoids an O(n)
	// sweep on every request when Redis is temporarily unavailable.
	if len(l.buckets) > 100000 && now.Sub(l.lastCleanup) > time.Minute {
		l.lastCleanup = now
		for k, b := range l.buckets {
			if now.Sub(b.updated) > 10*time.Minute {
				delete(l.buckets, k)
			}
		}
	}
	return allowed
}
