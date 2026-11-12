package redis

// Node discovery: where the client should connect, and what to do when that
// node fails.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// NodeProvider tells the client which node to use. The generation is bumped
// whenever the topology changes (master switch, endpoint rotation), which
// invalidates pooled connections and cached script SHAs.
type NodeProvider interface {
	// Node returns the address of the node to use and the current generation.
	Node(ctx context.Context) (addr string, generation uint64, err error)
	// Failure reports a node failure and returns true when a retry on another
	// (or re-resolved) node is worth it.
	Failure(err error) bool
	// NodeName returns the current node, for logging.
	NodeName() string
	// Start starts the optional background discovery of the node. It is called
	// lazily, on the first request, so that a middleware instance that never
	// sees traffic does not hold any goroutine. It is idempotent.
	Start(ctx context.Context)
	// StopDiscovery stops the background discovery without disabling it: a
	// later Start starts it again (idle stop).
	StopDiscovery()
	// Stop releases the provider resources for good.
	Stop()
}

// ---------------------------------------------------------------------------
// Sentinel
// ---------------------------------------------------------------------------

// SentinelProvider resolves the master through Redis Sentinel.
type SentinelProvider struct {
	resolver *SentinelResolver

	// watch enables the +switch-master subscription. When disabled, the master
	// is still tracked, but only on demand (READONLY / connection failures).
	watch bool
}

// NewSentinelProvider builds a provider over an already configured resolver.
// When watch is false, the +switch-master subscription is never started and
// failovers are handled by the pull mechanism only.
func NewSentinelProvider(resolver *SentinelResolver, watch bool) (*SentinelProvider, error) {
	if resolver == nil {
		return nil, errors.New("nil Sentinel resolver")
	}
	return &SentinelProvider{resolver: resolver, watch: watch}, nil
}

// Start implements NodeProvider.
func (p *SentinelProvider) Start(ctx context.Context) {
	if !p.watch {
		return
	}
	p.resolver.StartWatch(ctx)
}

// StopDiscovery implements NodeProvider.
func (p *SentinelProvider) StopDiscovery() {
	if !p.watch {
		return
	}
	p.resolver.StopWatch()
}

// Watching reports whether the +switch-master subscription is running.
func (p *SentinelProvider) Watching() bool { return p.resolver.Watching() }

// Node implements NodeProvider.
func (p *SentinelProvider) Node(ctx context.Context) (string, uint64, error) {
	return p.resolver.Master(ctx)
}

// Failure implements NodeProvider: only topology-change errors trigger a new
// resolution; the Sentinel watcher keeps the rest up to date.
func (p *SentinelProvider) Failure(err error) bool {
	return p.resolver.MarkStaleIfFailover(err)
}

// NodeName implements NodeProvider.
func (p *SentinelProvider) NodeName() string {
	addr, _ := p.resolver.MasterGeneration()
	return addr
}

// Generation returns the current master generation (no I/O).
func (p *SentinelProvider) Generation() uint64 {
	_, gen := p.resolver.MasterGeneration()
	return gen
}

// Stop implements NodeProvider.
func (p *SentinelProvider) Stop() {
	p.resolver.Stop()
}

// Start implements NodeProvider: nothing to discover for a static list.
func (p *StaticProvider) Start(context.Context) {}

// StopDiscovery implements NodeProvider.
func (p *StaticProvider) StopDiscovery() {}

// ---------------------------------------------------------------------------
// Static endpoint list (single instance mode)
// ---------------------------------------------------------------------------

// StaticProvider rotates over a static list of endpoints. It is used in single
// instance mode, where it also provides basic failover when several endpoints
// are configured.
type StaticProvider struct {
	endpoints []string

	// minSwitchInterval prevents flapping between endpoints when they all
	// answer with a connection error.
	minSwitchInterval time.Duration

	mu         sync.Mutex
	index      int
	generation uint64
	lastSwitch time.Time
}

// NewStaticProvider builds a provider over a list of host:port endpoints.
func NewStaticProvider(endpoints []string, minSwitchInterval time.Duration) (*StaticProvider, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("no Redis endpoint configured (store.redis.endpoints)")
	}

	uniq := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		if _, _, err := net.SplitHostPort(ep); err != nil {
			return nil, fmt.Errorf("invalid Redis endpoint %q: %w", ep, err)
		}
		dup := false
		for _, seen := range uniq {
			if seen == ep {
				dup = true
				break
			}
		}
		if !dup {
			uniq = append(uniq, ep)
		}
	}

	if minSwitchInterval <= 0 {
		minSwitchInterval = time.Second
	}

	return &StaticProvider{endpoints: uniq, minSwitchInterval: minSwitchInterval}, nil
}

// Node implements NodeProvider.
func (p *StaticProvider) Node(_ context.Context) (string, uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.endpoints[p.index], p.generation, nil
}

// Failure implements NodeProvider: rotate to the next endpoint, at most once
// per minSwitchInterval. With a single endpoint there is nothing to retry.
//
// Only node-level failures (connection loss, READONLY, MASTERDOWN, ...) are
// taken into account: a business-level RESP error such as NOSCRIPT or
// WRONGTYPE must not move traffic to another endpoint.
func (p *StaticProvider) Failure(err error) bool {
	if err == nil || !IsMasterFailoverError(err) {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.endpoints) < 2 {
		return false
	}
	if time.Since(p.lastSwitch) < p.minSwitchInterval {
		return false
	}

	p.index = (p.index + 1) % len(p.endpoints)
	p.generation++
	p.lastSwitch = time.Now()

	return true
}

// NodeName implements NodeProvider.
func (p *StaticProvider) NodeName() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.endpoints[p.index]
}

// Generation returns the current generation (no I/O).
func (p *StaticProvider) Generation() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation
}

// Stop implements NodeProvider.
func (p *StaticProvider) Stop() {}
