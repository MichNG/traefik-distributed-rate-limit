package redis

// Tests for the lifecycle of a middleware instance (review finding R1): the
// background discovery starts lazily, is released when Traefik cancels the
// middleware context, and can be suspended while the instance is idle.

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"
)

func newLifecycleClient(t *testing.T, fs *fakeSentinel, cfg ClientConfig) (*Client, *SentinelProvider, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	}, &Dialer{DialTimeout: 200 * time.Millisecond}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	provider, err := NewSentinelProvider(resolver, true)
	if err != nil {
		t.Fatalf("NewSentinelProvider: %v", err)
	}

	client, err := NewClient(ctx, cfg, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	return client, provider, cancel
}

func TestWatcherStartsLazilyAndIsReleasedWithTheContext(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	goroutinesBefore := runtime.NumGoroutine()

	client, provider, cancel := newLifecycleClient(t, fs, ClientConfig{
		CommandTimeout: time.Second,
		ShutdownGrace:  10 * time.Millisecond,
	})

	// Nothing is subscribed until the middleware serves traffic.
	if provider.Watching() {
		t.Fatal("the watcher must not start at construction time")
	}
	if got := fs.subscribers(); got != 0 {
		t.Fatalf("Sentinel subscribers = %d, want 0 before the first request", got)
	}

	script := client.NewScript("return {1,4,'-1','0'}")
	if _, err := script.Run(context.Background(), []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("first run: %v", err)
	}

	waitFor(t, 3*time.Second, provider.Watching, "the watcher did not start on the first request")
	waitFor(t, 3*time.Second, func() bool { return fs.subscribers() > 0 }, "the watcher did not subscribe")

	// Traefik replaces the middleware instance: the context is cancelled.
	cancel()

	waitFor(t, 3*time.Second, func() bool { return !provider.Watching() }, "the watcher was not stopped after the context cancellation")

	err := client.Ping(context.Background())
	if err == nil {
		t.Fatal("expected the retired client to refuse requests")
	}

	waitFor(t, 5*time.Second, func() bool {
		return runtime.NumGoroutine() <= goroutinesBefore+2
	}, "goroutines were not released after the middleware was retired")

	if got := fs.subscribers(); got != 0 {
		t.Fatalf("Sentinel subscribers = %d, want 0 after the shutdown", got)
	}
}

func TestIdleStopSuspendsAndRestartsTheWatcher(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	client, provider, cancel := newLifecycleClient(t, fs, ClientConfig{
		CommandTimeout: time.Second,
		ShutdownGrace:  10 * time.Millisecond,
		IdleTimeout:    100 * time.Millisecond,
	})
	_ = cancel

	script := client.NewScript("return {1,4,'-1','0'}")
	if _, err := script.Run(context.Background(), []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("first run: %v", err)
	}
	waitFor(t, 3*time.Second, provider.Watching, "the watcher did not start on the first request")

	// No traffic for a while: the discovery is suspended and the idle
	// connections are dropped.
	waitFor(t, 3*time.Second, func() bool { return !provider.Watching() }, "the idle stop did not suspend the watcher")
	waitFor(t, 3*time.Second, func() bool { return client.ActiveConns() == 0 }, "the idle stop did not close the connections")
	if got := fs.subscribers(); got != 0 {
		t.Fatalf("Sentinel subscribers = %d, want 0 while idle", got)
	}

	// Traffic resumes: the discovery starts again and requests keep working.
	if _, err := script.Run(context.Background(), []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("run after the idle stop: %v", err)
	}
	waitFor(t, 3*time.Second, provider.Watching, "the watcher did not restart after the idle stop")
}

func TestWatchCanBeDisabled(t *testing.T) {
	fs := newFakeSentinel(t)
	oldMaster := newFakeRedis(t, nil)
	newMaster := newFakeRedis(t, nil)

	hostOld, portOld := oldMaster.hostPort()
	hostNew, portNew := newMaster.hostPort()
	fs.setMaster(hostOld, portOld)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	}, &Dialer{DialTimeout: 200 * time.Millisecond}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	provider, err := NewSentinelProvider(resolver, false) // watch disabled
	if err != nil {
		t.Fatalf("NewSentinelProvider: %v", err)
	}

	client, err := NewClient(ctx, ClientConfig{CommandTimeout: time.Second, ShutdownGrace: time.Second}, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	script := client.NewScript("return {1,4,'-1','0'}")
	if _, err := script.Run(ctx, []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("first run: %v", err)
	}

	if provider.Watching() {
		t.Fatal("the watcher must not start when it is disabled")
	}
	if got := fs.subscribers(); got != 0 {
		t.Fatalf("Sentinel subscribers = %d, want 0 (watch disabled)", got)
	}

	// A failover happens without any event: the pull mechanism must still
	// recover, on the demoted master's READONLY reply.
	oldMaster.setReplica(true)
	fs.setMaster(hostNew, portNew)

	if _, err := script.Run(ctx, []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("run after a failover: %v", err)
	}

	if client.Node() != net.JoinHostPort(hostNew, portNew) {
		t.Fatalf("node = %q, want %q", client.Node(), net.JoinHostPort(hostNew, portNew))
	}
}

func TestStopWatchIsIdempotentAndStartWatchIsNoOpAfterStop(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	}, &Dialer{DialTimeout: 200 * time.Millisecond}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	resolver.StopWatch() // no-op
	resolver.StartWatch(context.Background())
	waitFor(t, 3*time.Second, resolver.Watching, "the watcher did not start")

	resolver.StopWatch()
	resolver.StopWatch() // idempotent
	if resolver.Watching() {
		t.Fatal("the watcher must be stopped")
	}

	resolver.StartWatch(context.Background()) // restartable
	waitFor(t, 3*time.Second, resolver.Watching, "the watcher did not restart")

	resolver.Stop() // final
	if resolver.Watching() {
		t.Fatal("the watcher must be stopped for good")
	}

	resolver.StartWatch(context.Background())
	if resolver.Watching() {
		t.Fatal("StartWatch must be a no-op after Stop")
	}
}

// The shutdown must not break a command that is already in flight: the grace
// period is used to drain them.
func TestShutdownDrainsInFlightCommands(t *testing.T) {
	server := newFakeServer(t, 150*time.Millisecond)

	provider, err := NewStaticProvider([]string{server.addr()}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	client, err := NewClient(ctx, ClientConfig{
		ConnectTimeout: time.Second,
		CommandTimeout: 5 * time.Second,
		ShutdownGrace:  2 * time.Second,
	}, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results := make(chan error, 1)

	go func() {
		results <- client.Ping(context.Background())
	}()

	// Let the command reach the server, then retire the instance.
	waitFor(t, 2*time.Second, func() bool { return client.InFlight() > 0 }, "the command did not start")
	cancel()

	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("the in-flight command failed because of the shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight command never returned")
	}

	waitFor(t, 3*time.Second, func() bool { return client.Ping(context.Background()) != nil },
		"the client was not closed after draining")
}
