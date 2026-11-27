package traefik_distributed_rate_limit

// Plugin logging.
//
// A Yaegi plugin cannot use the Traefik logger directly. Traefik configures the
// interpreter with interp.Options{Stdout: logs.NoLevel(..., DebugLevel),
// Stderr: logs.NoLevel(..., ErrorLevel)}: writes to the interpreted os.Stdout
// land in the Traefik log at debug level, writes to os.Stderr at error level.
// Verified against Traefik 3.5.6:
//
//	fmt.Println(...)  -> plugin logger, DBG
//	log.Println(...)  -> plugin logger, ERR   (the std logger writes to stderr)
//
// Both are used below, so that informational messages do not pollute the error
// level. Writing to an explicit *os.File instead (log.New(os.Stderr, ...))
// bypasses the redirection entirely: the output would go to the process
// stdout/stderr, ignoring the Traefik log level, format and log.filePath.
//
// Messages are also throttled per template, so that a store outage under load
// does not flood the Traefik log.

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const logThrottleInterval = 5 * time.Second

type pluginLogger struct {
	prefix string

	mu     sync.Mutex
	lastAt map[string]time.Time
}

func newPluginLogger(name string) *pluginLogger {
	return &pluginLogger{
		prefix: fmt.Sprintf("[distributed-ratelimit %s] ", name),
		lastAt: make(map[string]time.Time),
	}
}

// Logf implements redis.Logger: store diagnostics are reported as warnings.
func (l *pluginLogger) Logf(format string, args ...interface{}) {
	l.warnf(format, args...)
}

// warnf logs an informational message (throttled). It lands in the Traefik log
// at debug level, which is the right place for configuration and topology
// messages: `log.level: DEBUG` is required to see them.
//
// The throttle key is the format, which is what an outage needs (the node of a
// store error varies, the template does not). Messages that are all rendered
// through the same template - the deprecation warnings, logged with "%s" - must
// use warnMessage instead, or every warning but the first would be dropped.
func (l *pluginLogger) warnf(format string, args ...interface{}) {
	l.write(infoLevel, format, args...)
}

// warnMessage logs an already rendered message, throttled by the message
// itself, so that distinct messages sharing one template are all reported while
// a repetition of the same one stays throttled.
func (l *pluginLogger) warnMessage(msg string) {
	l.writeKeyed(infoLevel, msg, msg)
}

// errorf logs an error (throttled). It lands in the Traefik log at error level.
func (l *pluginLogger) errorf(format string, args ...interface{}) {
	l.write(errorLevel, format, args...)
}

const (
	infoLevel  = "WARN"
	errorLevel = "ERROR"
)

func (l *pluginLogger) write(level, format string, args ...interface{}) {
	l.writeKeyed(level, format, fmt.Sprintf(format, args...))
}

// writeKeyed throttles on key and writes msg at the given level.
func (l *pluginLogger) writeKeyed(level, key, msg string) {
	l.mu.Lock()
	if last, ok := l.lastAt[key]; ok && time.Since(last) < logThrottleInterval {
		l.mu.Unlock()
		return
	}
	l.lastAt[key] = time.Now()
	l.mu.Unlock()

	line := l.prefix + level + " " + msg

	if level == errorLevel {
		log.Println(line) // interpreted os.Stderr -> Traefik error level
		return
	}

	fmt.Println(line) // interpreted os.Stdout -> Traefik debug level
}
