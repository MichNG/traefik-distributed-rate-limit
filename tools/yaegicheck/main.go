// Command yaegicheck loads the plugin with the Yaegi interpreter and the symbol
// table used by Traefik, then serves a few requests through the middleware.
//
// It is a regression guard for interpreter incompatibilities: Yaegi versions
// pin down what the plugin sources may contain (see the README), and a
// construct accepted by the Go compiler can crash the interpreter while
// Traefik loads the plugin. Run it before every release:
//
//	make yaegi-check
//
// The check is self-contained: the store is an in-process RESP stub, which also
// plays the Sentinel role for the lifecycle check.
package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

const pluginImport = "github.com/MichNG/traefik-distributed-rate-limit"

func main() {
	root, err := repoRoot()
	if err != nil {
		fail("locating the repository root: %v", err)
	}

	addr, stop := startMockRedis()
	defer stop()
	fmt.Printf("mock Redis listening on %s\n", addr)

	gopath, err := os.MkdirTemp("", "yaegicheck-gopath")
	if err != nil {
		fail("creating the temporary GOPATH: %v", err)
	}
	defer func() { _ = os.RemoveAll(gopath) }()

	if err := os.Setenv("GOPATH", gopath); err != nil {
		fail("setting GOPATH: %v", err)
	}

	src := filepath.Join(gopath, "src", pluginImport)
	if err := copyTree(root, src); err != nil {
		fail("copying the plugin sources: %v", err)
	}

	i := interp.New(interp.Options{GoPath: gopath, Env: os.Environ(), Stdout: os.Stdout, Stderr: os.Stderr})
	if err := i.Use(stdlib.Symbols); err != nil {
		fail("loading the stdlib symbols: %v", err)
	}

	fmt.Println("=== compiling the plugin with yaegi ===")
	if _, err := i.Eval(fmt.Sprintf("import %q", pluginImport)); err != nil {
		fail("the plugin does not compile under yaegi: %v", err)
	}

	cfg, err := i.Eval("traefik_distributed_rate_limit.CreateConfig()")
	if err != nil {
		fail("CreateConfig: %v", err)
	}
	configure(cfg, addr)

	newFn, err := i.Eval("traefik_distributed_rate_limit.New")
	if err != nil {
		fail("looking up New: %v", err)
	}

	backend := 0
	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		backend++
		rw.WriteHeader(http.StatusOK)
	})

	results := newFn.Call([]reflect.Value{
		reflect.ValueOf(context.Background()),
		reflect.ValueOf(next),
		cfg,
		reflect.ValueOf("yaegicheck"),
	})
	if len(results) > 1 && !results[1].IsNil() {
		fail("New returned an error: %v", results[1].Interface())
	}

	fmt.Println("=== serving requests (limit=3, burst=5, period=1s) ===")
	statuses := make([]int, 0, 8)
	for n := 0; n < 8; n++ {
		rec := httptest.NewRecorder()
		callHandler(results[0], rec)
		statuses = append(statuses, rec.Code)
	}

	allowed, denied := 0, 0
	for _, status := range statuses {
		switch status {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			denied++
		}
	}

	fmt.Printf("allowed=%d denied=%d backend_hits=%d\n", allowed, denied, backend)

	if allowed != allowFirst || denied != 3 || backend != allowFirst {
		fail("unexpected behaviour: statuses=%v", statuses)
	}

	fmt.Println("OK: the plugin compiles and behaves correctly under yaegi")

	checkLifecycle(i, newFn)
	checkCookieSource(i, newFn)
	checkConfigurationErrors(i, newFn)
}

// checkConfigurationErrors makes sure that an invalid configuration is rejected
// with an error that Traefik can report. This is not a formality: yaegi 0.16.1
// loses the error of a multi-value call forwarded from the exported New, so the
// plugin must build its results explicitly (otherwise Traefik only sees a nil
// handler and reports "invalid handler type: <nil>").
func checkConfigurationErrors(i *interp.Interpreter, newFn reflect.Value) {
	fmt.Println("=== configuration errors must reach the caller with their message ===")

	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(http.StatusOK) })

	cases := []struct {
		name   string
		mutate func(value reflect.Value)
		want   string
	}{
		{
			name: "invalid cookieName",
			mutate: func(value reflect.Value) {
				criterion := reflect.New(value.FieldByName("SourceCriterion").Type().Elem())
				criterion.Elem().FieldByName("CookieName").SetString("bad name")
				value.FieldByName("SourceCriterion").Set(criterion)
			},
			want: "invalid cookieName",
		},
		{
			name: "negative limit",
			mutate: func(value reflect.Value) {
				value.FieldByName("Limit").SetInt(-1)
			},
			want: "limit must be >= 0",
		},
	}

	for _, tc := range cases {
		cfg, err := i.Eval("traefik_distributed_rate_limit.CreateConfig()")
		if err != nil {
			fail("CreateConfig: %v", err)
		}

		value := cfg.Elem()
		value.FieldByName("Limit").SetInt(1)
		redisCfg := value.FieldByName("Store").FieldByName("Redis")
		redisCfg.FieldByName("Endpoints").Set(reflect.ValueOf([]string{"127.0.0.1:6379"}))
		redisCfg.FieldByName("Timeout").SetString("1s")

		tc.mutate(value)

		results := newFn.Call([]reflect.Value{
			reflect.ValueOf(context.Background()),
			reflect.ValueOf(next),
			cfg,
			reflect.ValueOf("yaegicheck-error"),
		})

		if len(results) < 2 || results[1].IsNil() {
			fail("%s: New returned no error (handler nil: %v): Traefik would report a nil handler", tc.name, results[0].IsNil())
		}

		message := fmt.Sprintf("%v", results[1].Interface())
		if !strings.Contains(message, tc.want) {
			fail("%s: error = %q, want it to contain %q", tc.name, message, tc.want)
		}

		fmt.Printf("%-20s -> error surfaced: %s\n", tc.name, message)
	}

	fmt.Println("OK: configuration errors reach the caller with their message")
}

// checkCookieSource exercises the cookie-based source (including a JSON field
// inside a percent-encoded cookie) under yaegi: encoding/json and net/url are
// called from interpreted code here.
func checkCookieSource(i *interp.Interpreter, newFn reflect.Value) {
	fmt.Println("=== cookie source: nested JSON field inside a cookie, missing cookie ===")

	serverAddr, stopServer, _, reset := newMockServer()
	defer stopServer()

	reset()

	cfg, err := i.Eval("traefik_distributed_rate_limit.CreateConfig()")
	if err != nil {
		fail("CreateConfig: %v", err)
	}

	value := cfg.Elem()
	value.FieldByName("Limit").SetInt(3)
	value.FieldByName("Burst").SetInt(5)
	value.FieldByName("Period").SetString("1s")

	criterion := reflect.New(value.FieldByName("SourceCriterion").Type().Elem())
	criterion.Elem().FieldByName("CookieName").SetString("session")
	criterion.Elem().FieldByName("CookieField").SetString("user.id")
	criterion.Elem().FieldByName("OnEmptySource").SetString("deny")
	value.FieldByName("SourceCriterion").Set(criterion)

	deny := reflect.New(value.FieldByName("DenyOnError").Type().Elem())
	deny.Elem().SetBool(true)
	value.FieldByName("DenyOnError").Set(deny)

	redisCfg := value.FieldByName("Store").FieldByName("Redis")
	redisCfg.FieldByName("Endpoints").Set(reflect.ValueOf([]string{serverAddr}))
	redisCfg.FieldByName("Timeout").SetString("2s")

	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(http.StatusOK) })

	var handler reflect.Value

	func() {
		defer func() {
			if r := recover(); r != nil {
				fail("New panicked: %v", r)
			}
		}()

		results := newFn.Call([]reflect.Value{
			reflect.ValueOf(context.Background()),
			reflect.ValueOf(next),
			cfg,
			reflect.ValueOf("yaegicheck-cookie"),
		})
		if len(results) > 1 && !results[1].IsNil() {
			fail("New returned an error: %v", results[1].Interface())
		}

		handler = results[0]
	}()

	cases := []struct {
		name   string
		cookie string
		want   int
	}{
		{name: "user acme", cookie: "session=%7B%22user%22%3A%7B%22id%22%3A%22acme%22%7D%7D", want: http.StatusOK},
		{name: "user globex", cookie: "session=%7B%22user%22%3A%7B%22id%22%3A%22globex%22%7D%7D", want: http.StatusOK},
		{name: "nested field missing", cookie: "session=%7B%22user%22%3A%7B%22plan%22%3A%22gold%22%7D%7D", want: http.StatusBadRequest},
		{name: "cookie missing", cookie: "", want: http.StatusBadRequest},
	}

	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		if tc.cookie != "" {
			req.Header.Set("Cookie", tc.cookie)
		}

		rec := httptest.NewRecorder()
		handler.MethodByName("ServeHTTP").Call([]reflect.Value{reflect.ValueOf(rec), reflect.ValueOf(req)})

		if rec.Code != tc.want {
			fail("%s: status = %d, want %d", tc.name, rec.Code, tc.want)
		}
		fmt.Printf("%-16s -> %d\n", tc.name, rec.Code)
	}

	fmt.Println("OK: cookie source (with a JSON field) behaves correctly under yaegi")
}

// checkLifecycle reproduces what Traefik does on a configuration reload: the
// middleware instance is created, serves traffic, then its context is
// cancelled and a new instance replaces it. The retired instance must release
// its Sentinel subscription and connections without panicking (a missing
// release panics yaegi 0.16.1, or leaks a goroutine/connection).
func checkLifecycle(i *interp.Interpreter, newFn reflect.Value) {
	fmt.Println("=== lifecycle: create -> serve -> retire -> create again ===")

	serverAddr, stopServer, counters, _ := newMockServer()
	defer stopServer()

	subscribers := &counters.subscribers
	_ = subscribers

	for round := 1; round <= 2; round++ {
		cfg, err := i.Eval("traefik_distributed_rate_limit.CreateConfig()")
		if err != nil {
			fail("CreateConfig: %v", err)
		}
		configureSentinel(cfg, serverAddr)

		next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(http.StatusOK) })
		ctx, cancel := context.WithCancel(context.Background())

		var handler reflect.Value

		func() {
			defer func() {
				if r := recover(); r != nil {
					fail("instance %d panicked: %v", round, r)
				}
			}()

			results := newFn.Call([]reflect.Value{
				reflect.ValueOf(ctx),
				reflect.ValueOf(next),
				cfg,
				reflect.ValueOf(fmt.Sprintf("yaegicheck-%d", round)),
			})
			if len(results) > 1 && !results[1].IsNil() {
				fail("instance %d returned an error: %v", round, results[1].Interface())
			}

			handler = results[0]
		}()

		rec := httptest.NewRecorder()
		callHandler(handler, rec)
		if rec.Code != http.StatusOK {
			fail("instance %d: status = %d, want 200", round, rec.Code)
		}

		waitFor(func() bool { return subscribers.Load() > 0 }, "the Sentinel subscription did not start", round)

		// Traefik replaces the middleware: the context is cancelled.
		cancel()

		// The client releases its resources after the grace period.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && subscribers.Load() > 0 {
			time.Sleep(50 * time.Millisecond)
		}

		if got := subscribers.Load(); got != 0 {
			fail("instance %d retired but %d Sentinel subscriptions are still open", round, got)
		}

		fmt.Printf("instance %d: served, retired and released (no subscription left)\n", round)
	}

	fmt.Println("OK: lifecycle (retire/release) behaves correctly under yaegi")
}

func waitFor(cond func() bool, what string, round int) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	fail("instance %d: %s", round, what)
}

// configureSentinel configures the middleware against the mock server used both
// as Sentinel and as master.
func configureSentinel(cfg reflect.Value, addr string) {
	value := cfg.Elem()
	value.FieldByName("Limit").SetInt(3)
	value.FieldByName("Burst").SetInt(5)
	value.FieldByName("Period").SetString("1s")

	deny := reflect.New(value.FieldByName("DenyOnError").Type().Elem())
	deny.Elem().SetBool(true)
	value.FieldByName("DenyOnError").Set(deny)

	redis := value.FieldByName("Store").FieldByName("Redis")
	redis.FieldByName("Endpoints").Set(reflect.ValueOf([]string{addr}))
	redis.FieldByName("Timeout").SetString("2s")

	sentinel := reflect.New(redis.FieldByName("Sentinel").Type().Elem())
	sentinel.Elem().FieldByName("MasterSet").SetString("mymaster")
	redis.FieldByName("Sentinel").Set(sentinel)
}

func configure(cfg reflect.Value, addr string) {
	value := cfg.Elem()
	value.FieldByName("Limit").SetInt(3)
	value.FieldByName("Burst").SetInt(5)
	value.FieldByName("Period").SetString("1s")
	value.FieldByName("ResponseHeaders").SetBool(true)

	deny := reflect.New(value.FieldByName("DenyOnError").Type().Elem())
	deny.Elem().SetBool(true)
	value.FieldByName("DenyOnError").Set(deny)

	redis := value.FieldByName("Store").FieldByName("Redis")
	redis.FieldByName("Endpoints").Set(reflect.ValueOf([]string{addr}))
	redis.FieldByName("Timeout").SetString("2s")
	redis.FieldByName("PoolSize").SetInt(2)
	redis.FieldByName("MaxActiveConns").SetInt(8)
}

func callHandler(handler reflect.Value, rec *httptest.ResponseRecorder) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	handler.MethodByName("ServeHTTP").Call([]reflect.Value{reflect.ValueOf(rec), reflect.ValueOf(req)})
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, ".traefik.yml")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no .traefik.yml found above the working directory")
		}
		dir = parent
	}
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "example", "tools", "docs":
				return filepath.SkipDir
			}
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), data, 0o644)
	})
}

func fail(format string, args ...interface{}) {
	fmt.Printf("FAILED: "+format+"\n", args...)
	os.Exit(1)
}

// ---------------------------------------------------------------------------
// In-process RESP stub: just enough for the token bucket script.
// ---------------------------------------------------------------------------

func startMockRedis() (string, func()) {
	addr, stop, _, _ := newMockServer()
	return addr, stop
}

// newMockServer starts a RESP stub that answers both the Redis commands used by
// the limiter and the Sentinel commands used by the master discovery, and
// reports how many +switch-master subscriptions are open.
// mockCounters are the counters shared by the mock server connections.
type mockCounters struct {
	evaluated   atomic.Int64
	subscribers atomic.Int64
}

func (c *mockCounters) reset() { c.evaluated.Store(0) }

func newMockServer() (string, func(), *mockCounters, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail("listening: %v", err)
	}

	counters := &mockCounters{}
	addr := ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveMock(conn, addr, counters)
		}
	}()

	return addr, func() { _ = ln.Close() }, counters, counters.reset
}

// allowFirst is the number of evaluations the mock accepts before denying: it
// matches the burst configured by the checker.
const allowFirst = 5

func serveMock(conn net.Conn, addr string, counters *mockCounters) {
	defer func() { _ = conn.Close() }()

	rd := bufio.NewReader(conn)
	loaded := map[string]bool{}

	for {
		args, err := readCommand(rd)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			_, _ = io.WriteString(conn, "+PONG\r\n")
		case "AUTH", "SELECT":
			_, _ = io.WriteString(conn, "+OK\r\n")
		case "SENTINEL":
			// The stub plays both roles: it reports itself as the master.
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				_, _ = io.WriteString(conn, "-ERR bad address\r\n")
				continue
			}
			_, _ = io.WriteString(conn, fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(host), host, len(port), port))
		case "SUBSCRIBE":
			channel := "+switch-master"
			_, _ = io.WriteString(conn, fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel))
			counters.subscribers.Add(1)
			_, _ = rd.ReadByte() // hold the subscription until the client closes it
			counters.subscribers.Add(-1)
			return
		case "DEL":
			_, _ = io.WriteString(conn, ":1\r\n")
		case "SCRIPT":
			if len(args) < 3 {
				_, _ = io.WriteString(conn, "-ERR wrong number of arguments\r\n")
				continue
			}
			sum := sha1.Sum([]byte(args[2]))
			sha := hex.EncodeToString(sum[:])
			loaded[sha] = true
			_, _ = io.WriteString(conn, fmt.Sprintf("$40\r\n%s\r\n", sha))
		case "EVALSHA":
			if len(args) < 2 || !loaded[args[1]] {
				_, _ = io.WriteString(conn, "-NOSCRIPT No matching script. Please use EVAL.\r\n")
				continue
			}

			// Stand-in for the token bucket: the first `allowFirst` evaluations
			// pass, the following ones are denied. The reply shape is the one
			// of the plugin scripts: {allowed, remaining, retry_after, reset_after}.
			instance := counters.evaluated.Add(1)
			if instance > allowFirst {
				_, _ = io.WriteString(conn, "*4\r\n:0\r\n:0\r\n$1\r\n1\r\n$1\r\n1\r\n")
				continue
			}
			_, _ = io.WriteString(conn, "*4\r\n:1\r\n:4\r\n$2\r\n-1\r\n$1\r\n0\r\n")
		default:
			_, _ = io.WriteString(conn, "-ERR unknown command\r\n")
		}
	}
}

func readCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("unexpected RESP line %q", line)
	}

	count := 0
	if _, err := fmt.Sscanf(line, "*%d", &count); err != nil {
		return nil, err
	}

	args := make([]string, 0, count)
	for n := 0; n < count; n++ {
		header, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimRight(header, "\r\n")

		size := 0
		if _, err := fmt.Sscanf(header, "$%d", &size); err != nil {
			return nil, err
		}

		payload := make([]byte, size+2)
		if _, err := io.ReadFull(rd, payload); err != nil {
			return nil, err
		}
		args = append(args, string(payload[:size]))
	}

	return args, nil
}
