package redis

// Redis Sentinel support: master auto-discovery and failover handling.
//
// Two independent mechanisms are combined, which is what makes the switch of
// master correct and fast:
//
//  1. Push (primary): a dedicated connection subscribes to the Sentinel
//     `+switch-master` event. On the event the new master address is applied
//     immediately, without waiting for the next request to fail.
//
//  2. Pull (safety net): any IO-level error or `READONLY` reply on a data
//     connection marks the cached master as stale; the next request
//     re-resolves it with `SENTINEL get-master-addr-by-name`. Re-resolution is
//     single-flight and rate limited, so a burst of failing requests cannot
//     stampede the Sentinels.
//
// When the Sentinels are unreachable the last known master is kept, so a
// Sentinel outage alone never takes the rate limiter down.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SentinelConfig is the `store.redis.sentinel` section plus the shared
// connection parameters of `store.redis`.
type SentinelConfig struct {
	// Endpoints are the Sentinel addresses (host:port).
	Endpoints []string
	// MasterSet is `store.redis.sentinel.masterSet`.
	MasterSet string
	// Username/Password authenticate against the Sentinels themselves
	// (`store.redis.sentinel.username` / `.password`); they may differ from
	// the data-plane credentials.
	Username string
	Password string

	DialTimeout time.Duration
	ReadTimeout time.Duration

	// MinResolveInterval is the minimum delay between two forced
	// re-resolutions triggered by failing requests. Defaults to 1s.
	MinResolveInterval time.Duration
}

func (c SentinelConfig) validate() error {
	if len(c.Endpoints) == 0 {
		return errors.New("no Sentinel endpoint configured")
	}
	if c.MasterSet == "" {
		return errors.New("store.redis.sentinel.masterSet is required when using Sentinel")
	}
	for _, ep := range c.Endpoints {
		if _, _, err := net.SplitHostPort(ep); err != nil {
			return fmt.Errorf("invalid Sentinel endpoint %q: %w", ep, err)
		}
	}
	return nil
}

// SentinelResolver resolves and watches the master of a Sentinel set.
type SentinelResolver struct {
	cfg    SentinelConfig
	dialer *Dialer
	logger Logger

	// onChange is called (outside of any lock) whenever the master address
	// changes; the connection pool uses it to drop connections to the old
	// master.
	onChange func(master string, generation uint64)

	resolveMu sync.Mutex

	mu                 sync.Mutex
	master             string
	generation         uint64
	stale              bool
	lastResolveAttempt time.Time

	nextEndpoint uint64

	watchMu     sync.Mutex
	watchConn   net.Conn
	watchCancel context.CancelFunc
	watchDone   chan struct{}
	watchActive bool
	stopped     bool
}

// NewSentinelResolver creates a resolver. A nil dialer defaults to plain TCP,
// a nil logger is replaced by a no-op one.
func NewSentinelResolver(cfg SentinelConfig, dialer *Dialer, logger Logger) (*SentinelResolver, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if dialer == nil {
		dialer = &Dialer{}
	}
	if cfg.MinResolveInterval <= 0 {
		cfg.MinResolveInterval = time.Second
	}
	return &SentinelResolver{
		cfg:    cfg,
		dialer: dialer,
		logger: OrNop(logger),
	}, nil
}

// SetOnChange registers the master-change callback.
func (r *SentinelResolver) SetOnChange(fn func(master string, generation uint64)) {
	r.onChange = fn
}

// Master returns the current master address, re-resolving it when the cached
// value is missing or marked stale. If the Sentinels cannot be reached and a
// master was already known, that master is returned (and a warning is logged):
// a Sentinel outage must not break the data path.
func (r *SentinelResolver) Master(ctx context.Context) (string, uint64, error) {
	r.mu.Lock()
	addr, gen, stale, lastAttempt := r.master, r.generation, r.stale, r.lastResolveAttempt
	r.mu.Unlock()

	if addr != "" && !stale {
		return addr, gen, nil
	}
	if addr != "" && time.Since(lastAttempt) < r.cfg.MinResolveInterval {
		// A very recent attempt (failed, or not yet applied): keep the last
		// known master instead of hammering the Sentinels.
		return addr, gen, nil
	}

	return r.Resolve(ctx)
}

// Resolve forces a master lookup on the Sentinels, rotating over the
// configured endpoints until one answers. It is single-flight: concurrent
// callers share the result of one resolution.
func (r *SentinelResolver) Resolve(ctx context.Context) (string, uint64, error) {
	r.resolveMu.Lock()
	defer r.resolveMu.Unlock()

	// Another goroutine may have refreshed the master while we were waiting.
	r.mu.Lock()
	if r.master != "" && !r.stale && time.Since(r.lastResolveAttempt) < r.cfg.MinResolveInterval {
		addr, gen := r.master, r.generation
		r.mu.Unlock()
		return addr, gen, nil
	}
	// Record the attempt now: concurrent callers that stack up behind the
	// single-flight lock will not trigger another round of Sentinel queries.
	r.lastResolveAttempt = time.Now()
	r.mu.Unlock()

	var lastErr error
	n := len(r.cfg.Endpoints)
	start := int(atomic.AddUint64(&r.nextEndpoint, 1))

	for i := 0; i < n; i++ {
		endpoint := r.cfg.Endpoints[(start+i)%n]

		addr, err := r.queryMaster(ctx, endpoint)
		if err != nil {
			lastErr = fmt.Errorf("sentinel %s: %w", endpoint, err)
			r.logger.Logf("sentinel %s: %v", endpoint, err)
			continue
		}

		master, gen := r.setMaster(addr)
		return master, gen, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no Sentinel endpoint available")
	}

	r.mu.Lock()
	r.lastResolveAttempt = time.Now()
	cached, gen := r.master, r.generation
	r.mu.Unlock()

	if cached != "" {
		r.logger.Logf("keeping last known master %s: %v", cached, lastErr)
		return cached, gen, nil
	}

	return "", 0, lastErr
}

// MarkStale flags the cached master as possibly outdated; the next call to
// Master will re-resolve it (subject to MinResolveInterval).
func (r *SentinelResolver) MarkStale() {
	r.mu.Lock()
	r.stale = true
	r.mu.Unlock()
}

// MarkStaleIfFailover marks the master stale only for errors known to reveal a
// topology change (READONLY, MASTERDOWN, connection loss, ...).
func (r *SentinelResolver) MarkStaleIfFailover(err error) bool {
	if err == nil || !IsMasterFailoverError(err) {
		return false
	}

	r.mu.Lock()
	// Only the first failure of a burst forces an immediate re-resolution:
	// after that, the anti-stampede interval applies again, so a sustained
	// READONLY storm (or unreachable Sentinels) cannot turn every request into
	// a round of Sentinel queries.
	if !r.stale {
		r.stale = true
		r.lastResolveAttempt = time.Time{}
	}
	r.mu.Unlock()

	return true
}

// MasterGeneration returns the current master and generation without any
// network I/O.
func (r *SentinelResolver) MasterGeneration() (string, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.master, r.generation
}

func (r *SentinelResolver) setMaster(addr string) (string, uint64) {
	var old string
	var gen uint64
	changed := false

	r.mu.Lock()
	// The generation only changes on a switch: the first resolution is
	// generation 0.
	if r.master != "" && r.master != addr {
		r.generation++
		changed = true
	}
	old = r.master
	r.master = addr
	gen = r.generation
	r.stale = false
	r.lastResolveAttempt = time.Now()
	r.mu.Unlock()

	if changed {
		r.logger.Logf("master switched from %s to %s (generation %d)", old, addr, gen)
		if r.onChange != nil {
			r.onChange(addr, gen)
		}
	}

	return addr, gen
}

// queryMaster asks one Sentinel for the master address of the master set.
func (r *SentinelResolver) queryMaster(ctx context.Context, endpoint string) (string, error) {
	conn, err := r.dialer.Dial(ctx, endpoint)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	rc := newRespConn(conn)

	if err := r.authenticate(rc); err != nil {
		return "", err
	}

	return r.sendAndReadMaster(rc)
}

func (r *SentinelResolver) authenticate(rc *respConn) error {
	if err := setDeadline(rc.conn, r.cfg.DialTimeout, r.cfg.ReadTimeout); err != nil {
		return err
	}
	if r.cfg.Password == "" && r.cfg.Username == "" {
		return nil
	}

	args := []string{"AUTH", r.cfg.Password}
	if r.cfg.Username != "" {
		args = []string{"AUTH", r.cfg.Username, r.cfg.Password}
	}

	reply, err := rc.do(args...)
	if err != nil {
		return err
	}
	if reply.isError() {
		return redisError("Sentinel authentication failed", reply)
	}

	return nil
}

func (r *SentinelResolver) sendAndReadMaster(rc *respConn) (string, error) {
	if err := setDeadline(rc.conn, r.cfg.DialTimeout, r.cfg.ReadTimeout); err != nil {
		return "", err
	}

	reply, err := rc.do("SENTINEL", "get-master-addr-by-name", r.cfg.MasterSet)
	if err != nil {
		return "", err
	}
	if reply.isError() {
		return "", redisError("SENTINEL get-master-addr-by-name", reply)
	}

	return parseMasterAddr(reply, r.cfg.MasterSet)
}

func parseMasterAddr(reply respValue, masterSet string) (string, error) {
	items := reply.Array()
	if len(items) != 2 {
		return "", fmt.Errorf("unexpected reply for master set %q: got %d items, the Sentinel may not know this master set", masterSet, len(items))
	}

	host, port := items[0].String(), items[1].String()
	if host == "" || port == "" {
		return "", fmt.Errorf("incomplete master address for master set %q", masterSet)
	}

	return net.JoinHostPort(host, port), nil
}

// StartWatch starts the +switch-master subscription loop. It is idempotent,
// and it can be called again after StopWatch (lazy start / idle stop).
func (r *SentinelResolver) StartWatch(ctx context.Context) {
	r.watchMu.Lock()
	if r.stopped || r.watchActive {
		r.watchMu.Unlock()
		return
	}

	watchCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	r.watchCancel = cancel
	r.watchDone = done
	r.watchActive = true
	r.watchMu.Unlock()

	go r.watchLoop(watchCtx, done)
}

// StopWatch stops the subscription loop and unblocks its reader, without
// preventing a later StartWatch. It waits for the loop to return.
func (r *SentinelResolver) StopWatch() {
	r.watchMu.Lock()
	if !r.watchActive {
		r.watchMu.Unlock()
		return
	}

	// Note: no `r.watchCancel, r.watchDone = nil, nil` here. A multi-assignment
	// of untyped nil to two struct fields makes yaegi 0.16.1 (Traefik 3.5)
	// panic with "reflect: New(nil)" when the interpreter compiles the
	// assignment. The two fields are simply overwritten by the next StartWatch.
	cancel := r.watchCancel
	done := r.watchDone

	r.watchActive = false

	conn := r.watchConn
	r.watchMu.Unlock()

	cancel()
	if conn != nil {
		_ = conn.Close() // unblock the pub/sub reader
	}

	<-done
}

// Stop terminates the subscription loop for good: StartWatch becomes a no-op.
func (r *SentinelResolver) Stop() {
	r.watchMu.Lock()
	r.stopped = true
	r.watchMu.Unlock()

	r.StopWatch()
}

// Watching reports whether the subscription loop is running (tests, logging).
func (r *SentinelResolver) Watching() bool {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()
	return r.watchActive
}

func (r *SentinelResolver) watchLoop(ctx context.Context, done chan struct{}) {
	defer close(done)

	backoff := 200 * time.Millisecond
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		// A panic in the watcher must never reach the Traefik process: the
		// loop is not on the request path and net/http cannot recover it.
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					r.logger.Logf("Sentinel watcher panicked: %v", rec)
				}
			}()

			if err := r.watchOnce(ctx); err != nil {
				r.logger.Logf("Sentinel switch-master watch: %v", err)
			}
		}()

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// watchOnce subscribes to the switch-master channel on the first healthy
// Sentinel and reads the notification stream until the connection breaks.
func (r *SentinelResolver) watchOnce(ctx context.Context) error {
	var lastErr error
	n := len(r.cfg.Endpoints)
	start := int(atomic.AddUint64(&r.nextEndpoint, 1))

	for i := 0; i < n; i++ {
		endpoint := r.cfg.Endpoints[(start+i)%n]

		conn, err := r.dialer.Dial(ctx, endpoint)
		if err != nil {
			lastErr = err
			continue
		}

		rc := newRespConn(conn)

		if err := r.authenticate(rc); err != nil {
			rc.close()
			lastErr = err
			continue
		}

		// A pub/sub connection is idle by nature: only the write side gets a
		// deadline, the read side must not time out.
		if err := setDeadline(conn, r.cfg.DialTimeout, 0); err != nil {
			rc.close()
			lastErr = err
			continue
		}

		reply, err := rc.do("SUBSCRIBE", "+switch-master")
		if err != nil {
			rc.close()
			lastErr = err
			continue
		}
		if reply.isError() {
			rc.close()
			lastErr = redisError("SUBSCRIBE", reply)
			continue
		}

		r.watchMu.Lock()
		r.watchConn = conn
		r.watchMu.Unlock()

		err = r.readEvents(ctx, rc)

		r.watchMu.Lock()
		r.watchConn = nil
		r.watchMu.Unlock()
		rc.close()

		return err
	}

	if lastErr == nil {
		lastErr = errors.New("no Sentinel endpoint available for subscription")
	}

	return lastErr
}

// readEvents consumes the pub/sub stream and applies master switches.
func (r *SentinelResolver) readEvents(ctx context.Context, rc *respConn) error {
	// Unblock the reader when the watcher is stopped or when this connection is
	// replaced (the watcher reconnects).
	connDone := make(chan struct{})
	defer close(connDone)

	go func() {
		select {
		case <-ctx.Done():
			rc.close()
		case <-connDone:
		}
	}()

	for {
		value, err := readValue(rc.rd)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("reading Sentinel events: %w", err)
		}

		items := value.Array()
		// ["message", "+switch-master", "<masterSet> <oldIp> <oldPort> <newIp> <newPort>"]
		if len(items) != 3 || items[0].String() != "message" {
			continue
		}
		if items[1].String() != "+switch-master" {
			continue
		}

		masterSet, addr, ok := parseSwitchMaster(items[2].String())
		if !ok {
			r.logger.Logf("ignoring malformed +switch-master event: %q", items[2].String())
			continue
		}
		if masterSet != r.cfg.MasterSet {
			continue // event about another master set
		}

		r.setMaster(addr)
	}
}

// parseSwitchMaster parses "<masterSet> <oldIp> <oldPort> <newIp> <newPort>".
func parseSwitchMaster(payload string) (masterSet string, addr string, ok bool) {
	fields := strings.Fields(payload)
	if len(fields) < 5 {
		return "", "", false
	}

	return fields[0], net.JoinHostPort(fields[3], fields[4]), true
}

// IsMasterFailoverError reports whether err reveals that the node we talk to is
// not (or is no longer) the master: a demoted master answers READONLY, a
// failing or restarting one answers MASTERDOWN or LOADING.
func IsMasterFailoverError(err error) bool {
	if err == nil {
		return false
	}

	if netErr, ok := err.(net.Error); ok && netErr != nil {
		return true
	}
	if _, ok := err.(*net.OpError); ok {
		return true
	}

	msg := strings.ToUpper(err.Error())
	for _, marker := range []string{
		"READONLY", "MASTERDOWN", "LOADING",
		"CONNECTION RESET", "CONNECTION REFUSED", "CONNECTION CLOSED",
		"I/O TIMEOUT", "BROKEN PIPE", "EOF",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}

	return false
}
