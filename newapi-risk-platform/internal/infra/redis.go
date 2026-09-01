package infra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ckbkdj/newapi-risk-gateway/internal/core"
	"github.com/redis/go-redis/v9"
)

var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now_raw = redis.call('TIME')
local now = tonumber(now_raw[1]) * 1000 + math.floor(tonumber(now_raw[2]) / 1000)
local values = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(values[1])
local ts = tonumber(values[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
local delta = math.max(0, now - ts)
tokens = math.min(burst, tokens + (delta * rate / 1000.0))
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
local ttl = math.ceil((burst / rate) * 2000)
redis.call('PEXPIRE', key, ttl)
return {allowed, math.floor(tokens), ttl}
`)

const SettingsInvalidationKey = "__settings__"

type Redis struct {
	client *redis.Client
}

func NewRedis(ctx context.Context, rawURL string, poolSize, minIdleConns int) (*Redis, error) {
	if rawURL == "" {
		return &Redis{}, nil
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	if poolSize < 1 {
		poolSize = 100
	}
	if minIdleConns < 0 || minIdleConns > poolSize {
		minIdleConns = 0
	}
	opts.PoolSize = poolSize
	opts.MinIdleConns = minIdleConns
	opts.ConnMaxIdleTime = 5 * time.Minute
	opts.ConnMaxLifetime = 30 * time.Minute
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Redis{client: client}, nil
}

func (r *Redis) Enabled() bool { return r != nil && r.client != nil }
func (r *Redis) Close() error {
	if !r.Enabled() {
		return nil
	}
	return r.client.Close()
}
func (r *Redis) Ping(ctx context.Context) error {
	if !r.Enabled() {
		return errors.New("redis disabled")
	}
	return r.client.Ping(ctx).Err()
}

func (r *Redis) Allow(ctx context.Context, key string, rate, burst int) (bool, int64, error) {
	if !r.Enabled() {
		return true, int64(burst), nil
	}
	if rate < 1 || burst < 1 {
		return true, int64(burst), nil
	}
	result, err := tokenBucketScript.Run(ctx, r.client, []string{"risk:ratelimit:" + key}, rate, burst, 1).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(result) < 2 {
		return false, 0, fmt.Errorf("unexpected rate-limit response")
	}
	allowed, _ := result[0].(int64)
	remaining, _ := result[1].(int64)
	return allowed == 1, remaining, nil
}

func (r *Redis) GetAudit(ctx context.Context, key string) (core.AuditResult, bool, error) {
	if !r.Enabled() {
		return core.AuditResult{}, false, nil
	}
	raw, err := r.client.Get(ctx, "risk:audit:"+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return core.AuditResult{}, false, nil
	}
	if err != nil {
		return core.AuditResult{}, false, err
	}
	var result core.AuditResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return core.AuditResult{}, false, err
	}
	result.Cached = true
	return result, true, nil
}

func (r *Redis) SetAudit(ctx context.Context, key string, result core.AuditResult, ttl time.Duration) error {
	if !r.Enabled() {
		return nil
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, "risk:audit:"+key, raw, ttl).Err()
}

func (r *Redis) PublishInvalidation(ctx context.Context, key string) error {
	if !r.Enabled() {
		return nil
	}
	return r.client.Publish(ctx, "risk:route:invalidate", key).Err()
}

func (r *Redis) SubscribeInvalidations(ctx context.Context, handler func(string)) error {
	if !r.Enabled() {
		return nil
	}
	pubsub := r.client.Subscribe(ctx, "risk:route:invalidate")
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}
	channel := pubsub.Channel(redis.WithChannelSize(256))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-channel:
			if !ok {
				return fmt.Errorf("route invalidation subscription closed")
			}
			if handler != nil {
				handler(message.Payload)
			}
		}
	}
}
