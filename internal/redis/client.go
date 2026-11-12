package redis

// The Redis store client: connection pooling bound to a node generation,
// transparent retry on topology change, and Lua script handling that survives
// a master switch.

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned when the client has been closed.
var ErrClosed = errors.New("redis: client is closed")

// ClientConfig is the data-plane part of `store.redis`.
type ClientConfig struct {
	Username string
	Password string
	Database int

	ConnectTimeout time.Duration
	CommandTimeout time.Duration

	// PoolSize is the maximum number of idle connections kept per node
	// (store.redis.poolSize).
	PoolSize int
	// MaxActiveConns is the maximum number of connections opened at the same
	// time, in use or idle (store.redis.maxActiveConns). It defaults to
	// 10 * GOMAXPROCS, like the Redis client used by Traefik OSS.
	MaxActiveConns int

	// IdleTimeout stops the background discovery (and the idle connections)
	// after that much inactivity (store.redis.idleTimeout). 0 disables it.
	IdleTimeout time.Duration

	// ShutdownGrace is how long the client keeps working after its lifecycle
	// context is cancelled, so that in-flight requests can finish before the
	// resources are released.
	ShutdownGrace time.Duration
}

func (c ClientConfig) withDefaults() ClientConfig {
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	if c.CommandTimeout <= 0 {
		c.CommandTimeout = 3 * time.Second
	}
	if c.PoolSize <= 0 {
		c.PoolSize = 5
	}
	if c.MaxActiveConns <= 0 {
		c.MaxActiveConns = 10 * runtime.GOMAXPROCS(0)
	}
	if c.MaxActiveConns < c.PoolSize {
		// The idle pool cannot exceed the total number of connections.
		c.MaxActiveConns = c.PoolSize
	}
	if c.ShutdownGrace <= 0 {
		c.ShutdownGrace = defaultShutdownGrace
	}
	return c
}

// defaultShutdownGrace leaves Traefik time to drain the requests that were in
// flight when the middleware instance was retired.
const defaultShutdownGrace = 3 * time.Second

// pooledConn is an idle connection plus its buffered reader and the node
// generation it was opened against.
type pooledConn struct {
	rc         *respConn
	generation uint64
}

// Client talks to the node returned by its NodeProvider.
//
// The number of connections is bounded twice: PoolSize limits the idle ones,
// MaxActiveConns limits all of them (in use or idle) through the `active`
// semaphore. A slot is held as long as the connection exists and released when
// the connection is closed, so that a traffic spike cannot exhaust the file
// descriptors of the process nor the Redis maxclients budget.
type Client struct {
	cfg      ClientConfig
	dialer   *Dialer
	provider NodeProvider

	// active is the semaphore of live connections (buffered channel).
	active chan struct{}

	// lifecycleCtx is the context Traefik passes to the middleware
	// constructor. It is cancelled when the middleware instance is replaced by
	// a configuration reload: that is the signal to release every resource.
	lifecycleCtx context.Context

	idleMu    sync.Mutex
	idleTimer *time.Timer

	// inFlight counts the commands being executed (drain accounting).
	inFlight int64

	mu         sync.Mutex
	idle       []*pooledConn
	generation uint64
	closed     bool
}

// NewClient builds the client and tries to resolve the first node. A failure
// to reach the store is logged but is not fatal: a temporary Redis or Sentinel
// outage must not prevent Traefik from loading the middleware (the requests
// are then handled according to the denyOnError policy). Configuration
// mistakes are already rejected by the middleware constructor.
func NewClient(ctx context.Context, cfg ClientConfig, tlsCfg *TLSConfig, provider NodeProvider, logger Logger) (*Client, error) {
	if provider == nil {
		return nil, errors.New("redis: nil node provider")
	}

	tlsConfig, err := tlsCfg.CreateTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("creating TLS config: %w", err)
	}

	cfg = cfg.withDefaults()

	client := &Client{
		cfg:          cfg,
		provider:     provider,
		active:       make(chan struct{}, cfg.MaxActiveConns),
		lifecycleCtx: ctx,
		dialer: &Dialer{
			TLS:         tlsConfig,
			DialTimeout: cfg.ConnectTimeout,
		},
	}

	if tlsCfg != nil && tlsCfg.InsecureSkipVerify {
		OrNop(logger).Logf("store.redis.tls.insecureSkipVerify is enabled: the Redis server certificate will not be verified")
	}

	if _, _, err := provider.Node(ctx); err != nil {
		OrNop(logger).Logf("no Redis node available yet: %v", err)
	}

	// The middleware instance is retired when Traefik cancels this context
	// (it happens on every configuration reload that rebuilds the chain). The
	// background discovery and the connections must not outlive it: this is
	// the only lifecycle signal available to a plugin.
	go client.releaseOnContextDone(ctx, OrNop(logger))

	return client, nil
}

// releaseOnContextDone waits for the middleware instance to be retired, then
// releases everything. In-flight commands are given the grace period to finish
// first, so that a request started just before a configuration reload is not
// broken by the shutdown (the grace bounds the wait).
func (c *Client) releaseOnContextDone(ctx context.Context, logger Logger) {
	<-ctx.Done()

	deadline := time.Now().Add(c.cfg.ShutdownGrace)
	for time.Now().Before(deadline) && atomic.LoadInt64(&c.inFlight) > 0 {
		time.Sleep(drainPollInterval)
	}

	logger.Logf("middleware instance retired (context cancelled): releasing the Redis resources (in-flight commands: %d)",
		atomic.LoadInt64(&c.inFlight))
	c.Close()
}

// drainPollInterval is how often the shutdown checks the in-flight commands.
const drainPollInterval = 25 * time.Millisecond

// Close releases the provider and closes idle connections. In-flight
// connections are closed when they are returned to the pool. It is idempotent.
func (c *Client) Close() {
	c.stopIdleTimer()
	c.provider.Stop()

	c.mu.Lock()
	alreadyClosed := c.closed
	c.closed = true
	idle := c.idle
	c.idle = nil
	c.mu.Unlock()

	if alreadyClosed {
		return
	}

	for _, pc := range idle {
		pc.rc.close()
		c.releaseSlot()
	}
}

// Node returns the node currently in use (for logging).
func (c *Client) Node() string { return c.provider.NodeName() }

func (c *Client) dropIdle() {
	c.mu.Lock()
	idle := c.idle
	c.idle = nil
	c.mu.Unlock()

	for _, pc := range idle {
		pc.rc.close()
		c.releaseSlot()
	}
}

// armIdleTimer (re)arms the idle stop: after IdleTimeout without any command,
// the background discovery is stopped and the idle connections are dropped. A
// later request starts the discovery again.
func (c *Client) armIdleTimer() {
	timeout := c.cfg.IdleTimeout
	if timeout <= 0 {
		return
	}

	c.idleMu.Lock()
	defer c.idleMu.Unlock()

	if c.idleTimer == nil {
		c.idleTimer = time.AfterFunc(timeout, func() { c.idleStop() })
		return
	}

	c.idleTimer.Reset(timeout)
}

func (c *Client) idleStop() {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return
	}

	c.provider.StopDiscovery()
	c.dropIdle()
}

func (c *Client) stopIdleTimer() {
	c.idleMu.Lock()
	defer c.idleMu.Unlock()

	if c.idleTimer != nil {
		c.idleTimer.Stop()
		c.idleTimer = nil
	}
}

// discard closes a broken connection and frees its slot.
func (c *Client) discard(pc *pooledConn) {
	pc.rc.close()
	c.releaseSlot()
}

// releaseSlot frees one connection slot.
func (c *Client) releaseSlot() {
	select {
	case <-c.active:
	default: // defensive: never block on an unbalanced release
	}
}

// ActiveConns returns the number of live connections (for logging/tests).
func (c *Client) ActiveConns() int { return len(c.active) }

// InFlight returns the number of commands being executed (for logging/tests).
func (c *Client) InFlight() int64 { return atomic.LoadInt64(&c.inFlight) }

func (c *Client) get(ctx context.Context) (*pooledConn, error) {
	// Lazy start, and restart after an idle stop: the background discovery
	// (the Sentinel subscription) only runs while the middleware serves
	// traffic. Start is idempotent, and it is a no-op once the client is
	// closed.
	c.provider.Start(c.lifecycleCtx)

	addr, gen, err := c.provider.Node(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if gen != c.generation {
		stale := c.idle
		c.idle = nil
		c.generation = gen
		c.mu.Unlock()
		for _, pc := range stale {
			pc.rc.close()
			c.releaseSlot()
		}
		c.mu.Lock()
	}
	if n := len(c.idle); n > 0 {
		pc := c.idle[n-1]
		c.idle = c.idle[:n-1]
		c.mu.Unlock()
		return pc, nil
	}
	c.mu.Unlock()

	// No idle connection available: take a slot before opening a new one, so
	// that the total number of connections stays bounded.
	select {
	case c.active <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for a free Redis connection: %w", ctx.Err())
	}

	conn, err := c.dialer.Dial(ctx, addr)
	if err != nil {
		<-c.active // no connection was created, release the slot
		return nil, fmt.Errorf("dialing %s: %w", addr, err)
	}

	rc := newRespConn(conn)
	if err := c.handshake(rc); err != nil {
		rc.close()
		<-c.active
		return nil, err
	}

	return &pooledConn{rc: rc, generation: gen}, nil
}

func (c *Client) handshake(rc *respConn) error {
	if err := setDeadline(rc.conn, c.cfg.ConnectTimeout, c.cfg.CommandTimeout); err != nil {
		return err
	}

	if c.cfg.Password != "" || c.cfg.Username != "" {
		args := []string{"AUTH", c.cfg.Password}
		if c.cfg.Username != "" {
			args = []string{"AUTH", c.cfg.Username, c.cfg.Password}
		}

		reply, err := rc.do(args...)
		if err != nil {
			return err
		}
		if reply.isError() {
			return fmt.Errorf("authenticating against Redis: %w", reply)
		}
	}

	reply, err := rc.do("SELECT", strconv.Itoa(c.cfg.Database))
	if err != nil {
		return err
	}
	if reply.isError() {
		return fmt.Errorf("selecting database %d: %w", c.cfg.Database, reply)
	}

	return nil
}

// put returns a connection to the pool, unless it belongs to a previous node
// generation or the pool is full.
func (c *Client) put(pc *pooledConn) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || pc.generation != c.generation || len(c.idle) >= c.cfg.PoolSize {
		pc.rc.close()
		c.releaseSlot()
		return
	}

	c.idle = append(c.idle, pc)
}

// DoRaw sends a command and returns the raw reply. RESP errors are returned in
// the reply (err == nil) so that callers such as Script can react to NOSCRIPT.
// Transport errors are retried once when they reveal a topology change
// (READONLY, MASTERDOWN, connection loss, ...).
func (c *Client) DoRaw(ctx context.Context, args ...string) (respValue, error) {
	atomic.AddInt64(&c.inFlight, 1)
	defer atomic.AddInt64(&c.inFlight, -1)

	c.armIdleTimer()

	for attempt := 0; attempt < 2; attempt++ {
		pc, err := c.get(ctx)
		if err != nil {
			return respValue{}, err
		}

		if err := setDeadline(pc.rc.conn, c.cfg.CommandTimeout, c.cfg.CommandTimeout); err != nil {
			pc.rc.close()
			return respValue{}, err
		}

		reply, err := pc.rc.do(args...)
		if err != nil {
			// Connection-level failure: the socket is not reusable.
			c.discard(pc)

			if c.provider.Failure(err) && attempt == 0 {
				continue
			}
			return respValue{}, err
		}

		// The socket is sane whatever the RESP status is, so it goes back to
		// the pool; only a topology-change error justifies another attempt.
		c.put(pc)

		if c.provider.Failure(reply) && attempt == 0 {
			continue
		}

		return reply, nil
	}

	return respValue{}, errors.New("redis: no reply")
}

// Do sends a command and converts a RESP error into a Go error.
func (c *Client) Do(ctx context.Context, args ...string) (respValue, error) {
	reply, err := c.DoRaw(ctx, args...)
	if err != nil {
		return respValue{}, err
	}
	if reply.isError() {
		return respValue{}, reply
	}
	return reply, nil
}

// Ping checks the connection.
func (c *Client) Ping(ctx context.Context) error {
	reply, err := c.Do(ctx, "PING")
	if err != nil {
		return err
	}
	if reply.String() != "PONG" {
		return fmt.Errorf("unexpected PING reply: %q", reply.String())
	}
	return nil
}

// Del deletes a key.
func (c *Client) Del(ctx context.Context, key string) error {
	reply, err := c.Do(ctx, "DEL", key)
	if err != nil {
		return err
	}
	if _, err := reply.Int(); err != nil {
		return fmt.Errorf("unexpected DEL reply: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Scripts
// ---------------------------------------------------------------------------

// ScriptRunner is the API of a Lua script bound to a client.
type ScriptRunner interface {
	Run(ctx context.Context, keys []string, args ...string) (ScriptOutput, error)
}

// ScriptOutput is the decoded reply of the token bucket Lua scripts.
type ScriptOutput struct {
	Allowed    int64
	Remaining  int64
	RetryAfter time.Duration
	ResetAfter time.Duration
}

// Script is a Lua script bound to a client.
//
// Two things matter for correctness of the rate limiter:
//   - the SHA is re-loaded whenever the generation changes, because a new node
//     (failover, restart) does not share the previous script cache;
//   - a NOSCRIPT reply triggers a transparent SCRIPT LOAD + retry instead of
//     being reported as a failure, which would trip the circuit breaker and
//     silently disable rate limiting.
type Script struct {
	client *Client
	src    string

	mu         sync.Mutex
	sha        string
	generation uint64
}

// NewScript creates a script bound to the client.
func (c *Client) NewScript(src string) ScriptRunner {
	return &Script{client: c, src: src, generation: ^uint64(0)}
}

// Run evaluates the script and decodes its token bucket reply.
func (s *Script) Run(ctx context.Context, keys []string, args ...string) (ScriptOutput, error) {
	for attempt := 0; attempt < 2; attempt++ {
		sha, err := s.ensureLoaded(ctx)
		if err != nil {
			return ScriptOutput{}, err
		}

		cmd := make([]string, 0, 3+len(keys)+len(args))
		cmd = append(cmd, "EVALSHA", sha, strconv.Itoa(len(keys)))
		cmd = append(cmd, keys...)
		cmd = append(cmd, args...)

		reply, err := s.client.DoRaw(ctx, cmd...)
		if err != nil {
			return ScriptOutput{}, err
		}
		if reply.isError() {
			if isNoScript(reply) || IsMasterFailoverError(reply) {
				s.invalidate()
				continue
			}
			return ScriptOutput{}, reply
		}

		return decodeScriptOutput(reply)
	}

	return ScriptOutput{}, errors.New("redis: script reload loop")
}

func (s *Script) ensureLoaded(ctx context.Context) (string, error) {
	s.mu.Lock()
	sha, generation := s.sha, s.generation
	s.mu.Unlock()

	// A node switch invalidates the cached SHA.
	_, current := currentGeneration(s.client.provider)
	if sha != "" && generation == current {
		return sha, nil
	}

	reply, err := s.client.Do(ctx, "SCRIPT", "LOAD", s.src)
	if err != nil {
		return "", fmt.Errorf("loading script: %w", err)
	}
	if reply.isError() {
		return "", fmt.Errorf("loading script: %w", reply)
	}

	sha = reply.String()
	if len(sha) != 40 {
		return "", fmt.Errorf("unexpected script SHA: %q", sha)
	}

	s.mu.Lock()
	s.sha = sha
	s.generation = current
	s.mu.Unlock()

	return sha, nil
}

func (s *Script) invalidate() {
	s.mu.Lock()
	s.sha = ""
	s.mu.Unlock()
}

// generationReader is implemented by the providers that expose a generation
// without doing any I/O.
type generationReader interface {
	Generation() uint64
}

func currentGeneration(provider NodeProvider) (string, uint64) {
	if g, ok := provider.(generationReader); ok {
		return provider.NodeName(), g.Generation()
	}
	return provider.NodeName(), 0
}

func isNoScript(reply respValue) bool {
	return reply.isError() && len(reply.Error()) >= 8 && reply.Error()[:8] == "NOSCRIPT"
}

func decodeScriptOutput(reply respValue) (ScriptOutput, error) {
	items := reply.Array()
	if len(items) != 4 {
		return ScriptOutput{}, fmt.Errorf("unexpected script reply: got %d items, want 4", len(items))
	}

	allowed, err := items[0].Int()
	if err != nil {
		return ScriptOutput{}, fmt.Errorf("parsing allowed value: %w", err)
	}
	remaining, err := items[1].Int()
	if err != nil {
		return ScriptOutput{}, fmt.Errorf("parsing remaining value: %w", err)
	}
	retryAfter, err := items[2].Float()
	if err != nil {
		return ScriptOutput{}, fmt.Errorf("parsing retry_after value: %w", err)
	}
	resetAfter, err := items[3].Float()
	if err != nil {
		return ScriptOutput{}, fmt.Errorf("parsing reset_after value: %w", err)
	}

	return ScriptOutput{
		Allowed:    allowed,
		Remaining:  remaining,
		RetryAfter: durationFromSeconds(retryAfter),
		ResetAfter: durationFromSeconds(resetAfter),
	}, nil
}

// durationFromSeconds converts the Lua float seconds into a duration; -1 keeps
// the "not limited" convention of the scripts.
func durationFromSeconds(f float64) time.Duration {
	if f == -1 {
		return -1
	}
	return time.Duration(f * float64(time.Second))
}
