package redis

// Wire-level tests: what the client actually puts on the socket.
//
// The README promises a database index, an ACL username/password pair and the
// timeouts, and it promises that the TLS material protects the Sentinel
// connections too. These tests observe the wire instead of the error text: a
// recorder server keeps the full arguments of every command, in order, and a
// TLS listener counts the handshakes it completes.

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// A recorder server: full arguments, in order, and an optional silence.
// ---------------------------------------------------------------------------

type wireCommand struct {
	name string
	args []string
}

type wireServer struct {
	ln net.Listener

	mu     sync.Mutex
	cmds   []wireCommand
	silent bool
	// reply, when set, is sent instead of the normal answer, so that a server
	// error can be injected.
	reply string
	// delay postpones every answer (a slow, but available, server).
	delay time.Duration
}

func newWireServer(t *testing.T) *wireServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	server := &wireServer{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go server.handle(conn)
		}
	}()

	return server
}

func (s *wireServer) addr() string { return s.ln.Addr().String() }

func (s *wireServer) setSilent(v bool) {
	s.mu.Lock()
	s.silent = v
	s.mu.Unlock()
}

// recorded returns a copy of the commands received so far, in order.
func (s *wireServer) recorded() []wireCommand {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]wireCommand(nil), s.cmds...)
}

func (s *wireServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()

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

		s.mu.Lock()
		s.cmds = append(s.cmds, wireCommand{name: strings.ToUpper(args[0]), args: args})
		silent, reply, delay := s.silent, s.reply, s.delay
		s.mu.Unlock()

		if delay > 0 {
			time.Sleep(delay)
		}

		if silent {
			// Stay quiet: the client must fail on its own deadline.
			continue
		}

		if reply != "" {
			_ = writeRaw(conn, reply)
			continue
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			_ = writeRaw(conn, "+PONG\r\n")
		default:
			// AUTH, SELECT, and everything else the tests issue.
			_ = writeRaw(conn, "+OK\r\n")
		}
	}
}

// newWireClient wires a client to a single recorder server, the way the
// middleware wires the static provider.
func newWireClient(t *testing.T, cfg ClientConfig, addr string) *Client {
	t.Helper()

	provider, err := NewStaticProvider([]string{addr}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	client, err := NewClient(context.Background(), cfg, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(client.Close)

	return client
}

// ---------------------------------------------------------------------------
// store.redis.database
// ---------------------------------------------------------------------------

func TestDatabaseIsSelectedOnTheWire(t *testing.T) {
	cases := []struct {
		name     string
		database int
		want     string
	}{
		{name: "default database 0", database: 0, want: "0"},
		{name: "database 7", database: 7, want: "7"},
		{name: "database 15", database: 15, want: "15"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newWireServer(t)
			client := newWireClient(t, ClientConfig{
				Database:       tc.database,
				ConnectTimeout: 2 * time.Second,
				CommandTimeout: 2 * time.Second,
			}, server.addr())

			if err := client.Ping(context.Background()); err != nil {
				t.Fatalf("Ping: %v", err)
			}

			cmds := server.recorded()
			if len(cmds) < 2 {
				t.Fatalf("commands = %v, want at least SELECT and PING", cmds)
			}

			if cmds[0].name != "SELECT" || len(cmds[0].args) != 2 || cmds[0].args[1] != tc.want {
				t.Fatalf("first command = %v, want SELECT %s", cmds[0], tc.want)
			}

			if cmds[1].name != "PING" {
				t.Fatalf("second command = %v, want PING (on the selected database)", cmds[1])
			}
		})
	}
}

func TestDatabaseIsSelectedOncePerConnection(t *testing.T) {
	server := newWireServer(t)
	client := newWireClient(t, ClientConfig{
		Database:       3,
		ConnectTimeout: 2 * time.Second,
		CommandTimeout: 2 * time.Second,
	}, server.addr())

	for i := 0; i < 3; i++ {
		if err := client.Ping(context.Background()); err != nil {
			t.Fatalf("Ping %d: %v", i, err)
		}
	}

	var selects int
	for _, cmd := range server.recorded() {
		if cmd.name == "SELECT" {
			selects++
			if cmd.args[1] != "3" {
				t.Fatalf("SELECT %v, want SELECT 3", cmd.args)
			}
		}
	}

	// The connection is pooled, so a second SELECT would mean the handshake ran
	// again on a fresh socket.
	if selects != 1 {
		t.Fatalf("SELECT was sent %d times, want once for the pooled connection", selects)
	}
}

// ---------------------------------------------------------------------------
// store.redis.username / store.redis.password
// ---------------------------------------------------------------------------

func TestAuthOnTheWire(t *testing.T) {
	cases := []struct {
		name     string
		username string
		password string
		want     []string
	}{
		{name: "username and password", username: "app", password: "s3cret", want: []string{"AUTH", "app", "s3cret"}},
		{name: "password only", password: "s3cret", want: []string{"AUTH", "s3cret"}},
		{name: "username only", username: "app", want: []string{"AUTH", "app", ""}},
		{name: "no credentials", want: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newWireServer(t)
			client := newWireClient(t, ClientConfig{
				Username:       tc.username,
				Password:       tc.password,
				ConnectTimeout: 2 * time.Second,
				CommandTimeout: 2 * time.Second,
			}, server.addr())

			if err := client.Ping(context.Background()); err != nil {
				t.Fatalf("Ping: %v", err)
			}

			cmds := server.recorded()

			if tc.want == nil {
				for _, cmd := range cmds {
					if cmd.name == "AUTH" {
						t.Fatalf("no AUTH expected without credentials, got %v", cmd.args)
					}
				}
				if cmds[0].name != "SELECT" {
					t.Fatalf("first command = %v, want SELECT", cmds[0])
				}
				return
			}

			if cmds[0].name != "AUTH" {
				t.Fatalf("first command = %v, want AUTH", cmds[0])
			}
			if strings.Join(cmds[0].args, " ") != strings.Join(tc.want, " ") {
				t.Fatalf("AUTH = %v, want %v", cmds[0].args, tc.want)
			}
			// AUTH must precede the database selection.
			if cmds[1].name != "SELECT" {
				t.Fatalf("second command = %v, want SELECT after AUTH", cmds[1])
			}
		})
	}
}

func TestAuthErrorMentionsTheCommand(t *testing.T) {
	server := newWireServer(t)
	server.setSilent(false)

	// A server that refuses the credentials: the client must report the reply
	// instead of an opaque failure.
	server.mu.Lock()
	server.reply = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
	server.mu.Unlock()

	client := newWireClient(t, ClientConfig{
		Username:       "app",
		Password:       "wrong",
		ConnectTimeout: 2 * time.Second,
		CommandTimeout: 2 * time.Second,
	}, server.addr())

	err := client.Ping(context.Background())
	if err == nil {
		t.Fatal("Ping: expected an authentication error")
	}
	if !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("error = %q, want the server text (WRONGPASS)", err)
	}
}

// ---------------------------------------------------------------------------
// store.redis.timeout
// ---------------------------------------------------------------------------

func TestCommandTimeoutIsApplied(t *testing.T) {
	const timeout = 300 * time.Millisecond

	server := newWireServer(t)
	server.setSilent(true)

	client := newWireClient(t, ClientConfig{
		ConnectTimeout: timeout,
		CommandTimeout: timeout,
	}, server.addr())

	start := time.Now()
	err := client.Ping(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Ping: want a timeout error from a silent server")
	}
	if elapsed < timeout/2 {
		t.Fatalf("Ping failed after %v, want about the %v command timeout", elapsed, timeout)
	}
	if elapsed > 6*timeout {
		t.Fatalf("Ping failed after %v, want about the %v command timeout", elapsed, timeout)
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Logf("note: the error is %v (%T), not a net.Error with Timeout()", err, err)
	}
	t.Logf("silent server: error after %v: %v", elapsed.Round(time.Millisecond), err)
}

func TestServerReplyWithinTheTimeoutIsAccepted(t *testing.T) {
	server := newWireServer(t)
	server.mu.Lock()
	server.delay = 50 * time.Millisecond
	server.mu.Unlock()

	client := newWireClient(t, ClientConfig{
		ConnectTimeout: time.Second,
		CommandTimeout: time.Second,
	}, server.addr())

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v (a reply inside the timeout must be accepted)", err)
	}
}

func TestDialTimeoutIsApplied(t *testing.T) {
	const timeout = 300 * time.Millisecond

	// RFC 5737 TEST-NET-1: no host answers there, so the dial has to give up on
	// its own deadline (nothing is contacted, and no external service is used).
	client := newWireClient(t, ClientConfig{
		ConnectTimeout: timeout,
		CommandTimeout: timeout,
	}, "203.0.113.1:6379")

	start := time.Now()
	err := client.Ping(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Ping: want a dial failure against an unroutable address")
	}
	if elapsed > 6*timeout {
		t.Fatalf("dial failed after %v, want about the %v connect timeout", elapsed, timeout)
	}
	t.Logf("unroutable address: error after %v: %v", elapsed.Round(time.Millisecond), err)
}

// ---------------------------------------------------------------------------
// The TLS dialer is the one used for the Sentinels
// ---------------------------------------------------------------------------

// TestSentinelResolverResolvesOverTLS proves that the dialer handed to the
// resolver carries the TLS configuration: the Sentinel here only speaks TLS,
// and the resolution must still succeed.
func TestSentinelResolverResolvesOverTLS(t *testing.T) {
	pki := newTestPKI(t)

	fs := newFakeSentinelWithTLS(t, &tls.Config{Certificates: pki.TLSConfig.Certificates})
	fs.setMaster("127.0.0.1", "6390")

	tlsConfig, err := (&TLSConfig{CA: pki.CACert}).CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig: %v", err)
	}

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
	}, &Dialer{TLS: tlsConfig, DialTimeout: 2 * time.Second}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	addr, gen, err := resolver.Master(context.Background())
	if err != nil {
		t.Fatalf("Master over TLS: %v", err)
	}
	if addr != "127.0.0.1:6390" || gen != 0 {
		t.Fatalf("Master = (%q,%d), want (127.0.0.1:6390,0)", addr, gen)
	}
	if got := fs.acceptedConns(); got == 0 {
		t.Fatal("the Sentinel accepted no connection: the resolution did not go through the TLS dialer")
	}
}

// TestSentinelResolverWithoutTLSFailsAgainstTLSSentinel is the control: the
// same Sentinel, reached with a plain dialer, cannot be used - which is what
// makes the test above meaningful.
func TestSentinelResolverWithoutTLSFailsAgainstTLSSentinel(t *testing.T) {
	pki := newTestPKI(t)

	fs := newFakeSentinelWithTLS(t, &tls.Config{Certificates: pki.TLSConfig.Certificates})
	fs.setMaster("127.0.0.1", "6390")

	resolver, err := NewSentinelResolver(SentinelConfig{
		Endpoints:   []string{fs.addr()},
		MasterSet:   "mymaster",
		DialTimeout: 500 * time.Millisecond,
		ReadTimeout: 500 * time.Millisecond,
	}, &Dialer{DialTimeout: 500 * time.Millisecond}, testLoggerRef())
	if err != nil {
		t.Fatalf("NewSentinelResolver: %v", err)
	}

	if _, _, err := resolver.Master(context.Background()); err == nil {
		t.Fatal("Master without TLS: want a failure against a TLS-only Sentinel")
	}
}
