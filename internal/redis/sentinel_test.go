package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto/tls"
)

// fakeSentinel is a minimal Redis Sentinel stub supporting AUTH, PING,
// SENTINEL get-master-addr-by-name and SUBSCRIBE +switch-master.
type fakeSentinel struct {
	ln net.Listener

	mu       sync.Mutex
	host     string
	port     string
	authUser string
	authPass string
	subs     map[net.Conn]struct{}
	accepted int64
	broken   bool
}

func newFakeSentinel(t *testing.T) *fakeSentinel {
	t.Helper()
	return newFakeSentinelWithTLS(t, nil)
}

func newFakeSentinelWithTLS(t *testing.T, tlsConfig *tls.Config) *fakeSentinel {
	t.Helper()

	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var ln net.Listener = tcpLn
	if tlsConfig != nil {
		ln = tls.NewListener(tcpLn, tlsConfig)
	}

	fs := &fakeSentinel{ln: ln, host: "127.0.0.1", port: "6379", subs: map[net.Conn]struct{}{}}
	go fs.serve()

	t.Cleanup(func() { _ = ln.Close() })

	return fs
}

func (f *fakeSentinel) addr() string { return f.ln.Addr().String() }

func (f *fakeSentinel) setMaster(host, port string) {
	f.mu.Lock()
	f.host, f.port = host, port
	f.mu.Unlock()
}

// trigger pushes a +switch-master event to every subscriber.
func (f *fakeSentinel) trigger(masterSet, host, port string) {
	f.mu.Lock()
	subs := make([]net.Conn, 0, len(f.subs))
	for c := range f.subs {
		subs = append(subs, c)
	}
	f.host, f.port = host, port
	f.mu.Unlock()

	payload := fmt.Sprintf("%s 127.0.0.1 6379 %s %s", masterSet, host, port)
	for _, c := range subs {
		_ = writeCommand(c, "message", "+switch-master", payload)
	}
}

func (f *fakeSentinel) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}

		f.mu.Lock()
		f.accepted++
		broken := f.broken
		f.mu.Unlock()

		if broken {
			_ = conn.Close() // simulate an unreachable Sentinel
			continue
		}

		go f.handle(conn)
	}
}

// setBroken makes the Sentinel drop every new connection, like a down node.
func (f *fakeSentinel) setBroken(v bool) {
	f.mu.Lock()
	f.broken = v
	f.mu.Unlock()
}

// acceptedConns returns how many TCP connections the fake Sentinel accepted.
func (f *fakeSentinel) acceptedConns() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.accepted
}

// subscribers returns how many pub/sub subscriptions are established.
func (f *fakeSentinel) subscribers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs)
}

func (f *fakeSentinel) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	rd := bufio.NewReader(conn)
	for {
		value, err := readValue(rd)
		if err != nil {
			f.mu.Lock()
			delete(f.subs, conn)
			f.mu.Unlock()
			return
		}

		items := value.Array()
		if len(items) == 0 {
			continue
		}

		args := make([]string, 0, len(items))
		for _, item := range items {
			args = append(args, item.String())
		}

		switch strings.ToUpper(args[0]) {
		case "AUTH":
			f.mu.Lock()
			user, pass := f.authUser, f.authPass
			f.mu.Unlock()

			ok := false
			switch len(args) {
			case 2:
				ok = pass != "" && args[1] == pass
			case 3:
				ok = args[1] == user && args[2] == pass
			}

			if ok {
				_ = writeRaw(conn, "+OK\r\n")
			} else {
				_ = writeRaw(conn, "-ERR invalid password\r\n")
			}

		case "PING":
			_ = writeRaw(conn, "+PONG\r\n")

		case "SENTINEL":
			if len(args) >= 3 && strings.EqualFold(args[1], "get-master-addr-by-name") {
				f.mu.Lock()
				host, port := f.host, f.port
				f.mu.Unlock()

				if args[2] != "mymaster" {
					_ = writeRaw(conn, "*0\r\n")
					continue
				}

				_ = writeRaw(conn, fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(host), host, len(port), port))
				continue
			}
			_ = writeRaw(conn, "-ERR unknown SENTINEL subcommand\r\n")

		case "SUBSCRIBE":
			channel := "+switch-master"
			if len(args) > 1 {
				channel = args[1]
			}
			_ = writeRaw(conn, fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel))

			f.mu.Lock()
			f.subs[conn] = struct{}{}
			f.mu.Unlock()

			// Keep the connection open until the client closes it; events are
			// pushed by trigger().
			_, _ = rd.ReadByte()

			f.mu.Lock()
			delete(f.subs, conn)
			f.mu.Unlock()
			return

		default:
			_ = writeRaw(conn, "-ERR unknown command\r\n")
		}
	}
}

func writeRaw(conn net.Conn, s string) error {
	_, err := conn.Write([]byte(s))
	return err
}

func TestSentinelResolverResolve(t *testing.T) {
	fs := newFakeSentinel(t)
	fs.setMaster("10.0.0.1", "6380")

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints: []string{fs.addr()},
		MasterSet: "mymaster",
	}, &Dialer{DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addr, gen, err := resolver.Master(ctx)
	if err != nil {
		t.Fatalf("Master: %v", err)
	}
	if addr != "10.0.0.1:6380" {
		t.Fatalf("addr = %q, want 10.0.0.1:6380", addr)
	}
	if gen != 0 {
		t.Fatalf("generation = %d, want 0 for the first resolution", gen)
	}

	// Cached: no second network exchange needed.
	addr2, gen2, err := resolver.Master(ctx)
	if err != nil || addr2 != addr || gen2 != gen {
		t.Fatalf("cached Master = (%q,%d,%v)", addr2, gen2, err)
	}
}

func TestSentinelResolverUnknownMasterSet(t *testing.T) {
	fs := newFakeSentinel(t)

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints: []string{fs.addr()},
		MasterSet: "other",
	}, &Dialer{DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := resolver.Master(ctx); err == nil {
		t.Fatal("expected an error for an unknown master set")
	}
}

func TestSentinelResolverKeepsLastMasterOnSentinelOutage(t *testing.T) {
	fs := newFakeSentinel(t)

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints: []string{fs.addr()},
		MasterSet: "mymaster",
	}, &Dialer{DialTimeout: 200 * time.Millisecond}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}
	resolver.cfg.MinResolveInterval = time.Nanosecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := resolver.Master(ctx); err != nil {
		t.Fatalf("Master: %v", err)
	}

	// The Sentinel goes away: the last known master must be kept.
	fs.ln.Close()
	resolver.MarkStale()

	addr, _, err := resolver.Master(ctx)
	if err != nil {
		t.Fatalf("Master after sentinel outage: %v", err)
	}
	if addr != "127.0.0.1:6379" {
		t.Fatalf("addr = %q, want the last known 127.0.0.1:6379", addr)
	}
}

func TestSentinelResolverSentinelAuth(t *testing.T) {
	fs := newFakeSentinel(t)
	fs.mu.Lock()
	fs.authUser, fs.authPass = "sentinel-user", "s3ntinel"
	fs.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Wrong credentials: resolution fails.
	bad, err := NewSentinelResolver(SentinelConfig{
		Endpoints: []string{fs.addr()}, MasterSet: "mymaster", Password: "nope",
	}, &Dialer{DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}
	if _, _, err := bad.Master(ctx); err == nil {
		t.Fatal("expected an authentication error")
	}

	// Correct credentials, with an ACL username.
	good, err := NewSentinelResolver(SentinelConfig{
		Endpoints: []string{fs.addr()}, MasterSet: "mymaster",
		Username: "sentinel-user", Password: "s3ntinel",
	}, &Dialer{DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}
	if _, _, err := good.Master(ctx); err != nil {
		t.Fatalf("Master with ACL auth: %v", err)
	}
}

func TestSentinelResolverWatchSwitchMaster(t *testing.T) {
	fs := newFakeSentinel(t)

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints: []string{fs.addr()},
		MasterSet: "mymaster",
	}, &Dialer{DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	ctx0 := context.Background()
	if addr, gen, err := resolver.Master(ctx0); err != nil || addr != "127.0.0.1:6379" || gen != 0 {
		t.Fatalf("initial Master = (%q,%d,%v)", addr, gen, err)
	}

	changes := make(chan string, 4)
	resolver.SetOnChange(func(master string, generation uint64) {
		changes <- fmt.Sprintf("%s#%d", master, generation)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver.StartWatch(ctx)
	defer resolver.Stop()

	// Wait for the subscription to be established.
	deadline := time.Now().Add(3 * time.Second)
	for {
		fs.mu.Lock()
		n := len(fs.subs)
		fs.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	fs.mu.Lock()
	n := len(fs.subs)
	fs.mu.Unlock()
	if n == 0 {
		t.Fatal("sentinel watcher did not subscribe in time")
	}

	// A failover happens: the event must be applied without any request failing.
	fs.trigger("mymaster", "10.0.0.2", "6380")

	select {
	case got := <-changes:
		if got != "10.0.0.2:6380#1" {
			t.Fatalf("change = %q, want 10.0.0.2:6380#1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no +switch-master event applied")
	}

	addr, gen, err := resolver.Master(context.Background())
	if err != nil {
		t.Fatalf("Master: %v", err)
	}
	if addr != "10.0.0.2:6380" || gen != 1 {
		t.Fatalf("Master = (%q,%d), want (10.0.0.2:6380,1)", addr, gen)
	}
}

func TestSentinelResolverMarkStaleTriggersReresolve(t *testing.T) {
	fs := newFakeSentinel(t)

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints: []string{fs.addr()},
		MasterSet: "mymaster",
	}, &Dialer{DialTimeout: time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}
	resolver.cfg.MinResolveInterval = time.Nanosecond

	ctx := context.Background()
	if _, _, err := resolver.Master(ctx); err != nil {
		t.Fatalf("Master: %v", err)
	}

	// Topology changed but no event was received (e.g. watcher not connected).
	fs.setMaster("10.0.0.9", "6390")
	resolver.MarkStaleIfFailover(fmt.Errorf("READONLY You can't write against a read only replica"))

	addr, gen, err := resolver.Master(ctx)
	if err != nil {
		t.Fatalf("Master: %v", err)
	}
	if addr != "10.0.0.9:6390" {
		t.Fatalf("addr = %q, want 10.0.0.9:6390", addr)
	}
	if gen != 1 {
		t.Fatalf("generation = %d, want 1", gen)
	}
}

func TestIsMasterFailoverError(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("WRONGTYPE Operation against a key"), false},
		{fmt.Errorf("READONLY You can't write against a read only replica"), true},
		{fmt.Errorf("LOADING Redis is loading the dataset in memory"), true},
		{&net.OpError{Op: "dial", Err: fmt.Errorf("connection refused")}, true},
	}

	for _, c := range cases {
		if got := IsMasterFailoverError(c.err); got != c.want {
			t.Errorf("IsMasterFailoverError(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestParseSwitchMaster(t *testing.T) {
	masterSet, addr, ok := parseSwitchMaster("mymaster 127.0.0.1 6379 10.0.0.5 6380")
	if !ok || masterSet != "mymaster" || addr != "10.0.0.5:6380" {
		t.Fatalf("parseSwitchMaster = (%q,%q,%v)", masterSet, addr, ok)
	}

	if _, _, ok := parseSwitchMaster("garbage"); ok {
		t.Fatal("expected malformed payload to be rejected")
	}
}
