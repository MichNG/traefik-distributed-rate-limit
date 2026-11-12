package redis

// Tests for the bounds on the number of Redis connections (review finding R2)
// and for the Sentinel re-resolution budget (R3) / watcher robustness (R5).

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// A configurable fake Redis server (delays replies, can close connections).
// ---------------------------------------------------------------------------

type fakeServer struct {
	ln net.Listener

	conns         int64
	live          int64
	maxLive       int64
	totalCommands int64

	delay time.Duration
	// closeAfter closes the connection after this many commands (0 = never).
	closeAfter int64
	// authError, when set, is returned for AUTH commands (simulates a bad
	// credential or a missing ACL username).
	authError string

	mu            sync.Mutex
	perConn       map[net.Conn]int64
	commandCounts map[string]int64
}

func newFakeServer(t *testing.T, delay time.Duration) *fakeServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	fs := &fakeServer{
		ln:            ln,
		delay:         delay,
		perConn:       map[net.Conn]int64{},
		commandCounts: map[string]int64{},
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			atomic.AddInt64(&fs.conns, 1)

			cur := atomic.AddInt64(&fs.live, 1)
			for {
				max := atomic.LoadInt64(&fs.maxLive)
				if cur <= max || atomic.CompareAndSwapInt64(&fs.maxLive, max, cur) {
					break
				}
			}

			go fs.handle(conn)
		}
	}()

	return fs
}

func (f *fakeServer) addr() string { return f.ln.Addr().String() }

func (f *fakeServer) connCount() int64 { return atomic.LoadInt64(&f.conns) }

// maxLiveConns returns the highest number of *simultaneously* open connections.
func (f *fakeServer) maxLiveConns() int64 { return atomic.LoadInt64(&f.maxLive) }

func (f *fakeServer) commandCount(name string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commandCounts[strings.ToUpper(name)]
}

func (f *fakeServer) handle(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		atomic.AddInt64(&f.live, -1)
	}()

	rd := bufio.NewReader(conn)

	for {
		value, err := readValue(rd)
		if err != nil {
			return
		}

		items := value.Array()
		args := make([]string, 0, len(items))
		for _, item := range items {
			args = append(args, item.String())
		}
		if len(args) == 0 {
			continue
		}

		atomic.AddInt64(&f.totalCommands, 1)

		f.mu.Lock()
		f.commandCounts[strings.ToUpper(args[0])]++
		f.perConn[conn]++
		perConn := f.perConn[conn]
		f.mu.Unlock()

		if f.delay > 0 {
			time.Sleep(f.delay)
		}

		if f.closeAfter > 0 && perConn >= f.closeAfter {
			return // simulate a node that drops the connection
		}

		name := strings.ToUpper(args[0])
		switch {
		case name == "AUTH" && f.authError != "":
			_ = writeRaw(conn, "-"+f.authError+"\r\n")
		case name == "PING":
			_ = writeRaw(conn, "+PONG\r\n")
		default:
			_ = writeRaw(conn, "+OK\r\n")
		}
	}
}

// ---------------------------------------------------------------------------
// R2: the number of live connections is bounded
// ---------------------------------------------------------------------------

func TestPoolBoundsConcurrentConnections(t *testing.T) {
	server := newFakeServer(t, 30*time.Millisecond)

	provider, err := NewStaticProvider([]string{server.addr()}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	const maxActive = 8

	cfg := ClientConfig{
		ConnectTimeout: time.Second,
		CommandTimeout: 10 * time.Second,
		PoolSize:       4,
		MaxActiveConns: maxActive,
	}

	client, err := NewClient(context.Background(), cfg, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	const inflight = 50

	// Sample the client-side high-water mark: this is the quantity the client
	// controls, and the one the option bounds.
	var clientMax int64
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-sampled:
				return
			default:
			}

			if n := int64(client.ActiveConns()); n > atomic.LoadInt64(&clientMax) {
				atomic.StoreInt64(&clientMax, n)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	errs := make(chan error, inflight)

	for i := 0; i < inflight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := client.Ping(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("Ping failed while the pool is bounded: %v", err)
	}

	if got := atomic.LoadInt64(&clientMax); got > maxActive {
		t.Fatalf("concurrent connections held by the client = %d, want <= MaxActiveConns (%d)", got, maxActive)
	}

	// Sanity check on the server side: an unbounded client would show ~inflight
	// simultaneous connections here. The exact number can be a little higher
	// than MaxActiveConns, because a TCP close is asynchronous and the server
	// notices it a moment after the client has recycled the socket.
	if got := server.maxLiveConns(); got > maxActive+5 {
		t.Fatalf("simultaneous connections observed by the server = %d, want <= MaxActiveConns+5 (%d)", got, maxActive)
	}

	t.Logf("%d concurrent requests: %d connections opened in total, client high-water %d, server high-water %d (MaxActiveConns=%d)",
		inflight, server.connCount(), atomic.LoadInt64(&clientMax), server.maxLiveConns(), maxActive)
}

func TestPoolSaturationFailsFastInsteadOfOpeningConnections(t *testing.T) {
	server := newFakeServer(t, 250*time.Millisecond)

	provider, err := NewStaticProvider([]string{server.addr()}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	cfg := ClientConfig{
		ConnectTimeout: time.Second,
		CommandTimeout: 5 * time.Second,
		PoolSize:       1,
		MaxActiveConns: 1,
	}

	client, err := NewClient(context.Background(), cfg, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	// Occupy the only slot with a slow command.
	busy := make(chan struct{})
	go func() {
		defer close(busy)
		_ = client.Ping(context.Background())
	}()

	// Give the goroutine the time to take the slot.
	deadline := time.Now().Add(2 * time.Second)
	for client.ActiveConns() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = client.Ping(ctx)
	if err == nil {
		t.Fatal("expected the saturated pool to refuse the request")
	}
	if !strings.Contains(err.Error(), "free Redis connection") {
		t.Fatalf("error = %v, want it to mention the missing free connection", err)
	}
	if got := server.connCount(); got != 1 {
		t.Fatalf("connections opened = %d, want 1 (no new connection while saturated)", got)
	}

	<-busy
}

func TestPoolReleasesSlotsOfBrokenConnections(t *testing.T) {
	server := newFakeServer(t, 0)
	server.closeAfter = 2 // the node drops the connection after 2 commands

	provider, err := NewStaticProvider([]string{server.addr()}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	cfg := ClientConfig{ConnectTimeout: time.Second, CommandTimeout: time.Second, PoolSize: 2, MaxActiveConns: 2}

	client, err := NewClient(context.Background(), cfg, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Every command either succeeds or fails on a dropped connection, but the
	// slots must always be released: the client must stay usable.
	for i := 0; i < 10; i++ {
		_ = client.Ping(context.Background())
	}

	if got := client.ActiveConns(); got > 2 {
		t.Fatalf("active connections = %d, want <= 2 (leaked slots)", got)
	}

	client.Close()

	if got := client.ActiveConns(); got != 0 {
		t.Fatalf("active connections after Close = %d, want 0", got)
	}
}

func TestPoolSizeIsNotExceededWhenIdle(t *testing.T) {
	server := newFakeServer(t, 0)

	provider, err := NewStaticProvider([]string{server.addr()}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	cfg := ClientConfig{ConnectTimeout: time.Second, CommandTimeout: time.Second, PoolSize: 1, MaxActiveConns: 4}
	client, err := NewClient(context.Background(), cfg, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.Ping(context.Background())
		}()
	}
	wg.Wait()

	// All connections but one must have been closed instead of pooled.
	if got := client.ActiveConns(); got > cfg.PoolSize {
		t.Fatalf("active (idle) connections = %d, want <= PoolSize (%d)", got, cfg.PoolSize)
	}
}

// ---------------------------------------------------------------------------
// R3: the Sentinel re-resolution budget per failure burst
// ---------------------------------------------------------------------------

func TestSentinelResolutionBudgetUnderPersistentFailover(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	provider, err := NewSentinelProvider(mustResolver(t, fs), true)
	if err != nil {
		t.Fatalf("NewSentinelProvider: %v", err)
	}

	client, err := NewClient(context.Background(), ClientConfig{CommandTimeout: time.Second}, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	script := client.NewScript("return {1,4,'-1','0'}")
	if _, err := script.Run(context.Background(), []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("first run: %v", err)
	}

	bootstrap := fs.acceptedConns()

	// The master is demoted and the Sentinel becomes unreachable.
	master.setReplica(true)
	fs.setBroken(true)

	const requests = 10
	for i := 0; i < requests; i++ {
		_, err := script.Run(context.Background(), []string{"k"}, "1", "100", "1", "1")
		if err == nil {
			t.Fatalf("request %d: expected an error while the store is degraded", i)
		}
	}

	attempts := fs.acceptedConns() - bootstrap
	t.Logf("%d failing requests caused %d Sentinel connection attempts", requests, attempts)

	// Before the fix this was ~4 attempts per request (39 for 10 requests).
	if attempts > 3 {
		t.Fatalf("Sentinel connection attempts = %d, want <= 3 (anti-stampede must hold)", attempts)
	}
}

// panickingLogger panics on its first message: it is used to check that a panic
// in the watcher does not kill the process.
type panickingLogger struct {
	panicked *atomic.Int32
}

func (l panickingLogger) Logf(string, ...interface{}) {
	if l.panicked.Add(1) == 1 {
		panic("boom in the plugin logger")
	}
}

func mustResolver(t *testing.T, fs *fakeSentinel) *SentinelResolver {
	t.Helper()

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	}, &Dialer{DialTimeout: 200 * time.Millisecond}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	return resolver
}

// ---------------------------------------------------------------------------
// R5: a panic inside the watcher must not kill the process
// ---------------------------------------------------------------------------

func TestSentinelWatcherSurvivesPanic(t *testing.T) {
	fs := newFakeSentinel(t)
	masterA := newFakeRedis(t, nil)
	masterB := newFakeRedis(t, nil)
	hostA, portA := masterA.hostPort()
	hostB, portB := masterB.hostPort()
	fs.setMaster(hostA, portA)

	// The logger panics on its first use, i.e. right when the watcher reports
	// its first connection failure.
	var panicked atomic.Int32
	logger := panickingLogger{panicked: &panicked}

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	}, &Dialer{DialTimeout: 200 * time.Millisecond}, logger)
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}
	defer resolver.Stop()

	if addr, _, err := resolver.Master(context.Background()); err != nil || addr != hostA+":"+portA {
		t.Fatalf("Master = (%q,%v), want %s:%s", addr, err, hostA, portA)
	}

	// The Sentinel is down: the watcher fails and panics in the logger.
	fs.setBroken(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver.StartWatch(ctx)

	waitFor(t, 3*time.Second, func() bool { return panicked.Load() >= 1 }, "the watcher never logged (and panicked)")

	// The Sentinel comes back: the watcher must still be alive and follow the
	// next master switch.
	fs.setBroken(false)

	waitFor(t, 5*time.Second, func() bool { return fs.subscribers() > 0 }, "the watcher did not resubscribe after the panic")

	changes := make(chan string, 4)
	resolver.SetOnChange(func(master string, _ uint64) { changes <- master })

	fs.trigger("mymaster", hostB, portB)

	select {
	case got := <-changes:
		if got != net.JoinHostPort(hostB, portB) {
			t.Fatalf("change = %q, want %q", got, net.JoinHostPort(hostB, portB))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the watcher did not survive the panic")
	}

	if _, gen := resolver.MasterGeneration(); gen != 1 {
		t.Fatalf("generation = %d, want 1", gen)
	}
}

// The error of an AUTH failure must carry the server message: a RESP error is a
// concrete interpreted type, and passing it to %w through yaegi's fmt wrapper
// renders "%!w(stdlib._fmt_Stringer=...)" instead of the reason.
func TestAuthenticationErrorIsReadable(t *testing.T) {
	server := newFakeServer(t, 0)
	server.authError = "WRONGPASS invalid username-password pair or user is disabled."

	provider, err := NewStaticProvider([]string{server.addr()}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	client, err := NewClient(context.Background(), ClientConfig{
		Username:       "app",
		Password:       "wrong",
		ConnectTimeout: time.Second,
		CommandTimeout: time.Second,
	}, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	err = client.Ping(context.Background())
	if err == nil {
		t.Fatal("expected the authentication to fail")
	}

	message := err.Error()
	if !strings.Contains(message, "WRONGPASS invalid username-password pair") {
		t.Fatalf("error = %q, want it to carry the server message", message)
	}
	if strings.Contains(message, "%!") {
		t.Fatalf("error = %q, want a readable message (no fmt markers)", message)
	}
	if !strings.Contains(message, "authenticating against Redis") {
		t.Fatalf("error = %q, want the authentication context", message)
	}
}
