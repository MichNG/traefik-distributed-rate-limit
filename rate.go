package traefik_distributed_rate_limit

// Token bucket limiter on top of a Redis store.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MichNG/traefik-distributed-rate-limit/internal/redis"
)

// ErrNoDecision is returned when the store cannot provide a decision, either
// because it is unreachable or because the circuit breaker is open. The
// middleware maps it according to the denyOnError policy.
var ErrNoDecision = errors.New("rate limit: no decision from the store")

// Limit describes the token bucket: Rate tokens per Period, up to Burst
// tokens available at once.
type Limit struct {
	Rate   int64
	Burst  int64
	Period time.Duration
}

// Result is the outcome of a rate limit check.
type Result struct {
	Limit      Limit
	Allowed    int64
	Remaining  int64
	RetryAfter time.Duration
	ResetAfter time.Duration
}

// Store is the part of the Redis client used by the middleware.
type Store interface {
	Close()
	Ping(ctx context.Context) error
	Del(ctx context.Context, key string) error
	NewScript(src string) redis.ScriptRunner
	Node() string
}

// Limiter evaluates token buckets stored in Redis.
type Limiter struct {
	store       Store
	allowN      redis.ScriptRunner
	breaker     *redis.Breaker
	redisPrefix string
}

// NewLimiter creates a limiter. The prefix scopes the counters to one
// middleware instance, exactly like Traefik does (`rate:<name>:<source>`).
func NewLimiter(store Store, name string, breaker redis.BreakerConfig) *Limiter {
	return &Limiter{
		store:       store,
		allowN:      store.NewScript(allowNLua),
		breaker:     redis.NewBreaker(breaker),
		redisPrefix: "rate:" + name + ":",
	}
}

// Allow reports whether one more request is allowed for the given source.
func (l *Limiter) Allow(ctx context.Context, source string, limit Limit) (*Result, error) {
	if limit.Rate <= 0 {
		return &Result{Limit: limit, Allowed: 1}, nil
	}

	if !l.breaker.Allow() {
		return nil, ErrNoDecision
	}

	out, err := l.allowN.Run(
		ctx,
		[]string{l.redisPrefix + source},
		fmt.Sprintf("%d", limit.Burst),
		fmt.Sprintf("%d", limit.Rate),
		fmt.Sprintf("%v", limit.Period.Seconds()),
		"1",
	)
	if err != nil {
		l.breaker.Failure()
		return nil, fmt.Errorf("evaluating the token bucket: %w", err)
	}

	l.breaker.Success()

	return &Result{
		Limit:      limit,
		Allowed:    out.Allowed,
		Remaining:  out.Remaining,
		RetryAfter: out.RetryAfter,
		ResetAfter: out.ResetAfter,
	}, nil
}

// Node returns the store node currently in use, for logging.
func (l *Limiter) Node() string { return l.store.Node() }
