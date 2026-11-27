package traefik_distributed_rate_limit

import (
	"bytes"
	"io"
	"log"
	"os"
	"strings"
	"testing"
)

// The plugin logs through the two channels Traefik redirects into its own
// logger: the interpreted os.Stdout (debug level in Traefik) is used for
// informational messages, the interpreted os.Stderr (error level) for errors.
// Writing to an explicit *os.File would bypass the Traefik log entirely.
func TestLoggerLevels(t *testing.T) {
	stderr := captureStderr(t)
	stdout := captureStdout(t)

	logger := newPluginLogger("my-middleware")

	logger.errorf("no rate limit decision from the store on node %q: %v", "redis:6379", "connection refused")
	logger.warnf("using Redis Sentinel, master set %q", "mymaster")

	if got := stderr(); !strings.Contains(got, `[distributed-ratelimit my-middleware] ERROR no rate limit decision from the store on node "redis:6379"`) {
		t.Errorf("error message not found on stderr (Traefik error level): %q", got)
	}
	if got := stderr(); strings.Contains(got, "using Redis Sentinel") {
		t.Errorf("informational message must not go to the error level: %q", got)
	}
	if got := stdout(); !strings.Contains(got, `[distributed-ratelimit my-middleware] WARN using Redis Sentinel, master set "mymaster"`) {
		t.Errorf("informational message not found on stdout (Traefik debug level): %q", got)
	}
}

// Identical messages are throttled, so that a store outage under load does not
// flood the Traefik log.
func TestLoggerThrottlesIdenticalMessages(t *testing.T) {
	stderr := captureStderr(t)

	logger := newPluginLogger("my-middleware")

	for i := 0; i < 5; i++ {
		logger.errorf("no rate limit decision from the store on node %q: %v", "redis:6379", "connection refused")
	}
	logger.errorf("another failure mode: %v", "timeout")

	if got := strings.Count(stderr(), "\n"); got != 2 {
		t.Fatalf("expected 2 lines (one per template), got %d", got)
	}
}

func captureStderr(t *testing.T) func() string {
	t.Helper()

	var buf bytes.Buffer

	previous := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(previous) })

	return buf.String
}

func captureStdout(t *testing.T) func() string {
	t.Helper()

	previous := os.Stdout

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating the pipe: %v", err)
	}
	os.Stdout = w

	t.Cleanup(func() {
		os.Stdout = previous
		_ = r.Close()
	})

	// Reading is done after the writes, so the pipe buffer (64 KiB) is enough
	// for the few short messages of these tests.
	return func() string {
		_ = w.Close()
		out, _ := io.ReadAll(r)
		return string(out)
	}
}

// Distinct messages that share one template (the deprecation warnings are all
// logged through "%s") must not be collapsed by the throttle, while a repeat of
// the very same message stays throttled. warnf keeps throttling per template:
// that is what a store outage needs.
func TestLoggerThrottlesPerMessage(t *testing.T) {
	stdout := captureStdout(t)

	logger := newPluginLogger("my-middleware")

	logger.warnMessage(`"average" is deprecated, use "limit" instead`)
	logger.warnMessage(`"redisDb" is deprecated, use "store.redis.database" instead`)
	logger.warnMessage(`"average" is deprecated, use "limit" instead`) // identical: throttled
	logger.warnf("%s", "first distinct template message")
	logger.warnf("%s", "second one, same template") // throttled by template

	out := stdout()
	if got := strings.Count(out, "is deprecated"); got != 2 {
		t.Errorf("distinct messages sharing a template: got %d lines, want 2:\n%s", got, out)
	}
	if !strings.Contains(out, "first distinct template message") {
		t.Errorf("warnf message not logged: %s", out)
	}
	if strings.Contains(out, "second one, same template") {
		t.Errorf("warnf must still throttle by template: %s", out)
	}
}
