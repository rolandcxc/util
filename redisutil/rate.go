package redisutil

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/go-redis/redis/v8"
)

const script = `
local ratelimit_info = redis.call('HMGET', KEYS[1], 'last_time', 'last_tokens')
local last_time = tonumber(ratelimit_info[1])
local last_tokens = tonumber(ratelimit_info[2])

local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local fill_time = capacity/rate
local ttl = math.floor(fill_time*2)


if last_tokens == nil then
    last_tokens = capacity
end

if last_time == nil then
    last_time = 0
end

local delta = math.max(0, now-last_time)
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
if allowed then
    new_tokens = filled_tokens - requested
end

redis.call('HMSET', KEYS[1], 'last_time', now, 'last_tokens', new_tokens)
redis.call('expire', KEYS[1], ttl)

return { allowed, new_tokens }
`

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
const Inf = Limit(math.MaxFloat64)

// A Limiter controls how frequently events are allowed to happen.
type Limiter struct {
	cli   *redis.Client
	limit Limit
	burst int

	// mu sync.Mutex

	key string
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiter(cli *redis.Client, r Limit, b int, key string) *Limiter {
	return &Limiter{
		cli:   cli,
		limit: r,
		burst: b,
		key:   key,
	}
}

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

// Allow is shorthand for AllowN(time.Now(), 1).
func (lim *Limiter) Allow(ctx context.Context) bool {
	return lim.AllowN(ctx, time.Now(), 1)
}

// AllowN reports whether n events may happen at time now.
// Use this method if you intend to drop / skip events that exceed the rate limit.
// Otherwise use Reserve or Wait.
func (lim *Limiter) AllowN(ctx context.Context, now time.Time, n int) bool {
	return lim.reserveN(ctx, now, n).ok
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
type Reservation struct {
	ok     bool
	tokens int
}

func (lim *Limiter) reserveN(ctx context.Context, now time.Time, n int) Reservation {
	if lim.cli == nil {
		log.Printf("[RateLimit] redis client is nil, return allowed")
		return Reservation{
			ok:     true,
			tokens: n,
		}
	}

	results, err := lim.cli.Eval(ctx,
		script,
		[]string{lim.key},
		float64(lim.limit),
		lim.burst,
		now.Unix(),
		n,
	).Result()
	if err != nil {
		log.Printf("[RateLimit]fail to call rate limit: %s", err)
		return Reservation{
			ok:     true,
			tokens: n,
		}
	}

	rs, ok := results.([]interface{})
	if ok {
		newTokens, _ := rs[1].(int64)
		return Reservation{
			ok:     rs[0] == int64(1),
			tokens: int(newTokens),
		}
	}

	log.Printf("[RateLimit]fail to transform results")
	return Reservation{
		ok:     true,
		tokens: n,
	}
}
