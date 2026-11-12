package redis

import (
	"context"
	"net"
	"testing"
	"time"
)

// newTestSentinelClient wires a resolver + provider + client pair the same way
// the middleware does, so the tests exercise the real composition.
func newTestSentinelClient(t *testing.T, fs *fakeSentinel, clientCfg ClientConfig, tlsCfg *TLSConfig) (*Client, context.Context) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tlsConfig, err := tlsCfg.CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig: %v", err)
	}

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: time.Second,
		ReadTimeout: time.Second,
	}, &Dialer{TLS: tlsConfig, DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	provider, err := NewSentinelProvider(resolver, true)
	if err != nil {
		t.Fatalf("NewSentinelProvider: %v", err)
	}

	resolver.StartWatch(ctx)

	client, err := NewClient(ctx, clientCfg, tlsCfg, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(client.Close)

	return client, ctx
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestClientFollowsSwitchMasterEvent(t *testing.T) {
	fs := newFakeSentinel(t)
	masterA := newFakeRedis(t, nil)
	masterB := newFakeRedis(t, nil)

	hostA, portA := masterA.hostPort()
	hostB, portB := masterB.hostPort()
	fs.setMaster(hostA, portA)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second}, nil)
	if client.Node() != net.JoinHostPort(hostA, portA) {
		t.Fatalf("node = %q, want %q", client.Node(), net.JoinHostPort(hostA, portA))
	}

	script := client.NewScript("return {1,4,'-1','0'}")

	if _, err := script.Run(ctx, []string{"rate_key"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("first run: %v", err)
	}

	if _, _, evalshas := masterA.counts(); evalshas != 1 {
		t.Fatalf("master A evalsha = %d, want 1", evalshas)
	}
	if _, _, evalshas := masterB.counts(); evalshas != 0 {
		t.Fatalf("master B evalsha = %d, want 0 before the failover", evalshas)
	}

	// Failover: the Sentinel promotes B and pushes the event.
	fs.trigger("mymaster", hostB, portB)

	waitFor(t, 5*time.Second, func() bool {
		return client.Node() == net.JoinHostPort(hostB, portB)
	}, "client did not follow the +switch-master event")

	// The next request goes to B without any error on the data path, and the
	// script is re-loaded on the new master.
	if _, err := script.Run(ctx, []string{"rate_key"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("run after failover: %v", err)
	}

	if _, _, evalshas := masterB.counts(); evalshas != 1 {
		t.Fatalf("master B evalsha = %d, want 1 after the failover", evalshas)
	}
	if _, loadsB, _ := masterB.counts(); loadsB != 1 {
		t.Fatalf("master B script loads = %d, want 1 (the script cache is per node)", loadsB)
	}
	if _, _, evalshas := masterA.counts(); evalshas != 1 {
		t.Fatalf("master A evalsha = %d, want 1 (no request must reach the demoted master)", evalshas)
	}
}

func TestClientRecoversWithoutSwitchMasterEvent(t *testing.T) {
	fs := newFakeSentinel(t)
	oldMaster := newFakeRedis(t, nil)
	newMaster := newFakeRedis(t, nil)

	hostOld, portOld := oldMaster.hostPort()
	hostNew, portNew := newMaster.hostPort()
	fs.setMaster(hostOld, portOld)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second}, nil)

	script := client.NewScript("return {1,4,'-1','0'}")
	if _, err := script.Run(ctx, []string{"rate_key"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// The topology changes but the client is not notified (watcher down, event
	// lost, Sentinel restarted). The old master is now a replica.
	oldMaster.setReplica(true)
	fs.setMaster(hostNew, portNew)

	// The READONLY reply must be interpreted as a failover and recovered from
	// transparently, within the very same request.
	if _, err := script.Run(ctx, []string{"rate_key"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("run after a silent failover: %v", err)
	}

	// 2 EVALSHA on the new master: the first discovers that it does not have
	// the script cached (NOSCRIPT), the second is the successful run.
	if _, _, evalshas := newMaster.counts(); evalshas != 2 {
		t.Fatalf("new master evalsha = %d, want 2", evalshas)
	}
	if client.Node() != net.JoinHostPort(hostNew, portNew) {
		t.Fatalf("node = %q, want %q", client.Node(), net.JoinHostPort(hostNew, portNew))
	}
}

func TestClientReloadsScriptOnNoscript(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second}, nil)
	script := client.NewScript("return {1,4,'-1','0'}")

	if _, err := script.Run(ctx, []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// The script cache is flushed (SCRIPT FLUSH, restart without a topology
	// change): the cached SHA is stale but the node is unchanged.
	master.forgetScripts()

	if _, err := script.Run(ctx, []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("run after the script cache flush: %v", err)
	}

	if _, loads, evalshas := master.counts(); loads != 2 || evalshas != 3 {
		t.Fatalf("loads = %d (want 2), evalshas = %d (want 3)", loads, evalshas)
	}
}

func TestClientDecodesScriptOutput(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second}, nil)
	script := client.NewScript("return {1,4,'-1','0'}")

	out, err := script.Run(ctx, []string{"k"}, "1", "100", "1", "1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if out.Allowed != 1 || out.Remaining != 4 {
		t.Fatalf("output = %+v, want allowed=1 remaining=4", out)
	}
	if out.RetryAfter != -1 {
		t.Fatalf("retry_after = %v, want -1 (not limited)", out.RetryAfter)
	}
	if out.ResetAfter != 0 {
		t.Fatalf("reset_after = %v, want 0", out.ResetAfter)
	}
}

func TestClientReusesTheConnectionPool(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second, PoolSize: 4}, nil)
	script := client.NewScript("return {1,4,'-1','0'}")

	for i := 0; i < 5; i++ {
		if _, err := script.Run(ctx, []string{"k"}, "1", "100", "1", "1"); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	if conns, _, _ := master.counts(); conns != 1 {
		t.Fatalf("connections opened = %d, want 1 (the pool must be reused)", conns)
	}
}

func TestClientTLS(t *testing.T) {
	pki := newTestPKI(t)

	// Both the Sentinel and the data connections are protected by the same
	// store.redis.tls configuration.
	fs := newFakeSentinelWithTLS(t, pki.TLSConfig)
	master := newFakeRedis(t, pki.TLSConfig)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second}, &TLSConfig{CA: pki.CACert})
	script := client.NewScript("return {1,4,'-1','0'}")

	if _, err := script.Run(ctx, []string{"k"}, "1", "100", "1", "1"); err != nil {
		t.Fatalf("run over TLS: %v", err)
	}
}

func TestClientTLSVerificationFailure(t *testing.T) {
	pki := newTestPKI(t)

	// The Sentinel is plain (so that the master still resolves), the master
	// requires TLS but the client has no CA and does not skip verification.
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, pki.TLSConfig)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: time.Second,
		ReadTimeout: time.Second,
	}, &Dialer{DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	provider, err := NewSentinelProvider(resolver, true)
	if err != nil {
		t.Fatalf("NewSentinelProvider: %v", err)
	}

	badClient, err := NewClient(ctx, ClientConfig{CommandTimeout: 500 * time.Millisecond}, &TLSConfig{}, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer badClient.Close()

	if err := badClient.Ping(ctx); err == nil {
		t.Fatal("expected the TLS verification to fail without a CA bundle")
	}
}

func TestClientPingAndDel(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second}, nil)

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := client.Del(ctx, "rate_key"); err != nil {
		t.Fatalf("Del: %v", err)
	}

	var cmds []string
	for _, cmd := range master.commands() {
		if cmd == "AUTH" || cmd == "SELECT" {
			continue // handshake
		}
		cmds = append(cmds, cmd)
	}
	if len(cmds) != 2 || cmds[0] != "PING" || cmds[1] != "DEL" {
		t.Fatalf("commands = %v, want [PING DEL]", cmds)
	}
}

func TestClientClosed(t *testing.T) {
	fs := newFakeSentinel(t)
	master := newFakeRedis(t, nil)
	host, port := master.hostPort()
	fs.setMaster(host, port)

	client, ctx := newTestSentinelClient(t, fs, ClientConfig{CommandTimeout: time.Second}, nil)
	client.Close()

	if err := client.Ping(ctx); err == nil {
		t.Fatal("expected an error after Close")
	}
}
