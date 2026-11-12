package redis

// Circuit breaker protecting the store from repeated failures.
//
// It is intentionally simple and self-contained: the middleware only needs a
// yes/no answer to "is a decision available right now?", and that answer feeds
// the denyOnError policy.

import (
	"sync"
	"time"
)

// BreakerConfig configures the circuit breaker.
type BreakerConfig struct {
	// Threshold is the number of consecutive failures that opens the breaker.
	Threshold int64
	// Reattempt is how long the breaker stays open before a probe is allowed.
	Reattempt time.Duration
}

func (c BreakerConfig) withDefaults() BreakerConfig {
	if c.Threshold < 1 {
		c.Threshold = 3
	}
	if c.Reattempt <= 0 {
		c.Reattempt = 15 * time.Second
	}
	return c
}

// Breaker is a half-open circuit breaker.
type Breaker struct {
	cfg BreakerConfig

	mu        sync.Mutex
	failures  int64
	openUntil time.Time
	probing   bool
}

// NewBreaker creates a breaker.
func NewBreaker(cfg BreakerConfig) *Breaker {
	return &Breaker{cfg: cfg.withDefaults()}
}

// Allow reports whether the store may be used.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.openUntil.IsZero() {
		return true
	}
	if time.Now().Before(b.openUntil) {
		return false
	}

	// Half-open: let exactly one probe through.
	if b.probing {
		return false
	}
	b.probing = true
	return true
}

// Success closes the breaker.
func (b *Breaker) Success() {
	b.mu.Lock()
	b.failures = 0
	b.openUntil = time.Time{}
	b.probing = false
	b.mu.Unlock()
}

// Failure records a failure and opens the breaker when the threshold is
// reached.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.probing = false
	b.failures++
	if b.failures >= b.cfg.Threshold {
		b.openUntil = time.Now().Add(b.cfg.Reattempt)
	}
}

// Open reports whether the breaker is currently open (for tests/metrics).
func (b *Breaker) Open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.openUntil.IsZero() && time.Now().Before(b.openUntil)
}
