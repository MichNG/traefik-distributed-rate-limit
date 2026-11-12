package redis

// testLogger is a goroutine-safe logger for the tests.
//
// The lifecycle goroutine of a client (releaseOnContextDone) may live longer
// than the test that created it, so a *testing.T must not be used as the logger:
// the go race detector rightly reports a race when a retired client logs after
// the test has finished.

import (
	"fmt"
	"strings"
	"sync"
)

type testLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *testLogger) Logf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, fmt.Sprintf(format, args...))
}

// messages returns a copy of the logged messages.
func (l *testLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.msgs...)
}

// testLoggerRef returns a fresh logger for one test object.
func testLoggerRef() *testLogger { return &testLogger{} }

// contains reports whether any message contains the given substring.
func (l *testLogger) contains(substr string) bool {
	for _, msg := range l.messages() {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}
