package traefik_distributed_rate_limit

// Benchmarks for the middleware. They run offline: a minimal in-process RESP
// server stands in for Redis, so the measured path covers everything a real
// request traverses except the Lua execution on the server and the network to
// it (the technique — a fake server that speaks just enough RESP for the
// token bucket script — is the one used in internal/redis/pool_test.go and
// internal/redis/lifecycle_test.go).
//
//	go test -bench . -benchmem                                    # fake store
//	REDIS_ADDR=127.0.0.1:6379 go test -bench . -benchmem         # + real Redis
//
// The scenarios are the two interesting middleware outcomes (the request is
// allowed, the request is denied) at 1 and 64 concurrent goroutines, with and
// without the X-RateLimit-* response headers.
//
// The fake server is written allocation-free on purpose: Go reports the
// allocations of the whole process, so a chatty test double would land in the
// B/op and allocs/op of the benchmarked middleware.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/MichNG/traefik-distributed-rate-limit/internal/utils"
)

// benchSourceHeader groups every benchmark request into a single bucket, so
// that the runs do not depend on the client address (Traefik sees a docker
// gateway address in the e2e harness).
const benchSourceHeader = "X-Bench-Source"

// benchPoolSize is deliberately larger than the highest concurrency used here:
// the pool and the connection limit are exercised by the unit tests, and here
// they would only add connection churn noise to the numbers.
const benchPoolSize = 128

// Canned token bucket replies: {allowed, remaining, retry_after, reset_after}.
var (
	benchAllowReply = []byte("*4\r\n:1\r\n:999\r\n$2\r\n-1\r\n$1\r\n0\r\n")
	benchDenyReply  = []byte("*4\r\n:0\r\n:0\r\n$2\r\n10\r\n$2\r\n10\r\n")
)

// ---------------------------------------------------------------------------
// Minimal RESP server.
// ---------------------------------------------------------------------------

// benchRedisServer answers PING, SELECT/AUTH, DEL, SCRIPT LOAD and EVALSHA. It
// returns a canned token bucket reply: allowed or denied, switched with
// setDeny. Nothing else is parsed, and the Lua script is never executed.
type benchRedisServer struct {
	ln net.Listener

	deny atomic.Bool

	mu        sync.Mutex
	conns     int
	scriptLds int
	evalshas  int
}

func benchStartRedis(tb testing.TB) *benchRedisServer {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listening: %v", err)
	}

	s := &benchRedisServer{ln: ln}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			s.mu.Lock()
			s.conns++
			s.mu.Unlock()

			go s.handle(conn)
		}
	}()

	tb.Cleanup(func() { _ = ln.Close() })

	return s
}

func (s *benchRedisServer) addr() string { return s.ln.Addr().String() }

func (s *benchRedisServer) setDeny(v bool) { s.deny.Store(v) }

func (s *benchRedisServer) counts() (conns, loads, evalshas int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns, s.scriptLds, s.evalshas
}

// benchRedisConn is the per-connection state: the reusable buffers that keep
// the test double allocation-free.
type benchRedisConn struct {
	conn    net.Conn
	rd      *bufio.Reader
	scratch []byte
	argv    [][]byte
}

func (s *benchRedisServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	c := &benchRedisConn{
		conn:    conn,
		rd:      bufio.NewReader(conn),
		scratch: make([]byte, 0, 8<<10),
		argv:    make([][]byte, 0, 8),
	}

	for {
		args, err := c.readCommand()
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		switch {
		case bytes.EqualFold(args[0], []byte("PING")):
			_, _ = c.conn.Write([]byte("+PONG\r\n"))

		case bytes.EqualFold(args[0], []byte("AUTH")), bytes.EqualFold(args[0], []byte("SELECT")):
			_, _ = c.conn.Write([]byte("+OK\r\n"))

		case bytes.EqualFold(args[0], []byte("DEL")):
			_, _ = c.conn.Write([]byte(":1\r\n"))

		case bytes.EqualFold(args[0], []byte("SCRIPT")):
			if len(args) < 3 {
				_, _ = c.conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				continue
			}

			sum := sha1.Sum(args[2])
			sha := hex.EncodeToString(sum[:])

			s.mu.Lock()
			s.scriptLds++
			s.mu.Unlock()

			_, _ = c.conn.Write([]byte("$40\r\n" + sha + "\r\n"))

		case bytes.EqualFold(args[0], []byte("EVALSHA")):
			s.mu.Lock()
			s.evalshas++
			s.mu.Unlock()

			if s.deny.Load() {
				_, _ = c.conn.Write(benchDenyReply)
				continue
			}

			_, _ = c.conn.Write(benchAllowReply)

		default:
			_, _ = c.conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

// readCommand reads one RESP array of bulk strings — the only shape the
// plugin's client sends — into the reusable buffers of the connection.
func (c *benchRedisConn) readCommand() ([][]byte, error) {
	c.scratch = c.scratch[:0]
	c.argv = c.argv[:0]

	header, err := c.rd.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	header = bytes.TrimRight(header, "\r\n")
	if len(header) == 0 || header[0] != '*' {
		return nil, fmt.Errorf("unexpected RESP array header %q", header)
	}

	n, err := benchParseInt(header[1:])
	if err != nil {
		return nil, fmt.Errorf("parsing the array length %q: %w", header, err)
	}

	for i := 0; i < n; i++ {
		bulkHeader, err := c.rd.ReadSlice('\n')
		if err != nil {
			return nil, err
		}

		bulkHeader = bytes.TrimRight(bulkHeader, "\r\n")
		if len(bulkHeader) == 0 || bulkHeader[0] != '$' {
			return nil, fmt.Errorf("unexpected bulk header %q", bulkHeader)
		}

		size, err := benchParseInt(bulkHeader[1:])
		if err != nil {
			return nil, fmt.Errorf("parsing the bulk length %q: %w", bulkHeader, err)
		}

		// The payload, and its trailing CRLF, land at the end of the scratch.
		start := len(c.scratch)
		end := start + size + 2

		if end > cap(c.scratch) {
			grown := make([]byte, end, 2*end)
			copy(grown, c.scratch)
			c.scratch = grown
		}

		c.scratch = c.scratch[:end]
		if _, err := io.ReadFull(c.rd, c.scratch[start:end]); err != nil {
			return nil, err
		}

		c.argv = append(c.argv, c.scratch[start:start+size])
	}

	return c.argv, nil
}

// benchParseInt parses a decimal number without allocating a string.
func benchParseInt(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty number")
	}

	n := 0
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("not a number: %q", b)
		}
		n = n*10 + int(ch-'0')
	}

	return n, nil
}

// ---------------------------------------------------------------------------
// Middleware under benchmark.
// ---------------------------------------------------------------------------

// benchCase is one benchmark scenario.
type benchCase struct {
	name    string
	limit   int64
	burst   int64
	headers bool
	deny    bool
}

var benchCases = []benchCase{
	{name: "allow", limit: 1_000_000, burst: 1_000_000},
	{name: "allow_headers", limit: 1_000_000, burst: 1_000_000, headers: true},
	{name: "deny", limit: 100, burst: 100, deny: true},
	{name: "deny_headers", limit: 100, burst: 100, headers: true, deny: true},
}

// benchMiddleware builds the real middleware (real Redis client, real RESP
// codec) pointed at the given endpoint.
func benchMiddleware(tb testing.TB, endpoint string, c benchCase) http.Handler {
	tb.Helper()

	cfg := CreateConfig()
	cfg.Limit = c.limit
	cfg.Period = "1s"
	cfg.Burst = c.burst
	cfg.ResponseHeaders = c.headers
	cfg.SourceCriterion = &utils.SourceCriterion{RequestHeaderName: benchSourceHeader}
	cfg.Store.Redis.Endpoints = []string{endpoint}
	cfg.Store.Redis.Timeout = "1s"
	cfg.Store.Redis.PoolSize = benchPoolSize
	cfg.Store.Redis.MaxActiveConns = benchPoolSize

	// The lifecycle context is cancelled at the end of the benchmark, exactly
	// like Traefik does when it retires the middleware instance.
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)

	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "text/plain")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})

	handler, err := New(ctx, next, cfg, "bench")
	if err != nil {
		tb.Fatalf("creating the middleware: %v", err)
	}

	return handler
}

// benchWriter is a ResponseWriter that keeps no state, except the header map
// reused by every iteration of a goroutine (the middleware sets the same keys
// again and again, so the map does not grow).
type benchWriter struct {
	header http.Header
}

func newBenchWriter() *benchWriter { return &benchWriter{header: make(http.Header)} }

func (w *benchWriter) Header() http.Header         { return w.header }
func (w *benchWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *benchWriter) WriteHeader(int)             {}

// benchRequest builds the single request reused by all iterations.
func benchRequest() *http.Request {
	req := httptest.NewRequest(http.MethodGet, "http://bench.local/", nil)
	req.RemoteAddr = "10.0.0.1:34567"
	req.Header.Set(benchSourceHeader, "bench-source")

	return req
}

// benchDrive performs total requests on h from concurrency goroutines.
func benchDrive(h http.Handler, req *http.Request, concurrency, total int) {
	if concurrency < 1 {
		concurrency = 1
	}
	if total <= 0 {
		return
	}

	per := total / concurrency
	rest := total % concurrency

	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		n := per
		if i < rest {
			n++
		}
		if n == 0 {
			continue
		}

		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			rw := newBenchWriter()
			for j := 0; j < n; j++ {
				h.ServeHTTP(rw, req)
			}
		}(n)
	}

	wg.Wait()
}

// benchRun warms the pool and the script cache, then measures.
func benchRun(b *testing.B, h http.Handler, req *http.Request, concurrency int) {
	b.Helper()

	benchDrive(h, req, concurrency, 4*concurrency)

	b.ReportAllocs()
	b.ResetTimer()
	benchDrive(h, req, concurrency, b.N)
}

// BenchmarkServeHTTP measures the middleware against the in-process RESP
// server: no docker, no external Redis.
func BenchmarkServeHTTP(b *testing.B) {
	srv := benchStartRedis(b)

	for _, c := range benchCases {
		handler := benchMiddleware(b, srv.addr(), c)
		srv.setDeny(c.deny)

		req := benchRequest()

		for _, concurrency := range []int{1, 64} {
			b.Run(fmt.Sprintf("%s/c%d", c.name, concurrency), func(b *testing.B) {
				benchRun(b, handler, req, concurrency)
			})
		}
	}
}

// BenchmarkServeHTTPRealRedis runs the same scenarios against a real Redis,
// when REDIS_ADDR is set. The deny scenarios use a one-token bucket, so that
// the Lua script itself returns the denials.
func BenchmarkServeHTTPRealRedis(b *testing.B) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		b.Skip("REDIS_ADDR is not set: skipping the benchmark against a real Redis")
	}

	cases := []benchCase{
		{name: "allow", limit: 1_000_000, burst: 1_000_000},
		{name: "allow_headers", limit: 1_000_000, burst: 1_000_000, headers: true},
		// One token per second: every request after the first one is denied.
		{name: "deny", limit: 1, burst: 1},
		{name: "deny_headers", limit: 1, burst: 1, headers: true},
	}

	for _, c := range cases {
		handler := benchMiddleware(b, addr, c)
		req := benchRequest()

		for _, concurrency := range []int{1, 64} {
			b.Run(fmt.Sprintf("%s/c%d", c.name, concurrency), func(b *testing.B) {
				benchRun(b, handler, req, concurrency)
			})
		}
	}
}

// TestBenchScenariosMatchTheirNames guards the benchmark: it fails if the
// "allow" scenario stops being allowed, or the "deny" one stops returning 429,
// so the numbers cannot silently measure something else.
func TestBenchScenariosMatchTheirNames(t *testing.T) {
	srv := benchStartRedis(t)

	for _, c := range benchCases {
		handler := benchMiddleware(t, srv.addr(), c)
		srv.setDeny(c.deny)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, benchRequest())

		want := http.StatusOK
		if c.deny {
			want = http.StatusTooManyRequests
		}

		if rec.Code != want {
			t.Fatalf("scenario %q: got status %d, want %d (%s)", c.name, rec.Code, want, rec.Body.String())
		}

		gotHeader := rec.Header().Get("X-RateLimit-Limit") != ""
		if gotHeader != c.headers {
			t.Fatalf("scenario %q: X-RateLimit-Limit present=%v, want %v", c.name, gotHeader, c.headers)
		}
	}

	if _, loads, evalshas := srv.counts(); loads == 0 || evalshas == 0 {
		t.Fatalf("the store was not exercised: script loads=%d, evalshas=%d", loads, evalshas)
	}
}

// BenchmarkNextHandler is the floor: the same driver with the plugin removed,
// so the plugin's share of the cost is visible.
func BenchmarkNextHandler(b *testing.B) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "text/plain")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})

	req := benchRequest()

	for _, concurrency := range []int{1, 64} {
		b.Run(fmt.Sprintf("next/c%d", concurrency), func(b *testing.B) {
			benchRun(b, next, req, concurrency)
		})
	}
}
