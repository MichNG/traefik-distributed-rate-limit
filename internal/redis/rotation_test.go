package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

// A dead endpoint (nothing listens, or the name does not resolve) must rotate
// the static list: otherwise every request keeps hitting it and, with the
// default denyOnError, the client answers 429 for good while a healthy endpoint
// sits next in store.redis.endpoints.
func TestDialFailureRotatesToTheNextEndpoint(t *testing.T) {
	live := newFakeRedis(t, nil)
	dead := closedPort(t)

	provider, err := NewStaticProvider([]string{dead, live.addr()}, time.Second)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	client, err := NewClient(context.Background(), ClientConfig{
		ConnectTimeout: 500 * time.Millisecond,
		CommandTimeout: time.Second,
		PoolSize:       2,
		MaxActiveConns: 2,
	}, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	// The first request dials the dead endpoint, the provider rotates, the
	// command is retried on the live one and succeeds.
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping after the rotation: %v", err)
	}

	if got, want := client.Node(), live.addr(); got != want {
		t.Fatalf("node = %q, want the live endpoint %q", got, want)
	}
	received := live.commands()
	found := false
	for _, command := range received {
		if command == "PING" {
			found = true
		}
	}
	if !found {
		t.Fatalf("commands received by the live endpoint = %v, want a PING among them", received)
	}
}

// A single endpoint cannot rotate: the failure is reported, and nothing else is
// tried (there is nowhere else to go).
func TestDialFailureWithASingleEndpointIsReported(t *testing.T) {
	dead := closedPort(t)

	provider, err := NewStaticProvider([]string{dead}, time.Second)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	client, err := NewClient(context.Background(), ClientConfig{
		ConnectTimeout: 500 * time.Millisecond,
		CommandTimeout: time.Second,
	}, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	if err := client.Ping(context.Background()); err == nil {
		t.Fatal("expected an error for an endpoint that refuses connections")
	}
}

// closedPort returns a host:port that nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("opening a listener: %v", err)
	}

	addr := listener.Addr().String()
	_ = listener.Close()

	return addr
}

// A name that does not resolve is the failure mode a container sees when a
// service is renamed or was never deployed; the endpoint list must rotate on it
// too, not only on the errors that arrive after the connection is established.
func TestDialFailureRotatesOnDNSFailure(t *testing.T) {
	live := newFakeRedis(t, nil)

	provider, err := NewStaticProvider([]string{"redis-nowhere.invalid:6379", live.addr()}, time.Second)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	client, err := NewClient(context.Background(), ClientConfig{
		ConnectTimeout: time.Second,
		CommandTimeout: time.Second,
	}, nil, provider, testLoggerRef())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping after the rotation: %v", err)
	}
	if got, want := client.Node(), live.addr(); got != want {
		t.Fatalf("node = %q, want %q", got, want)
	}
}

func TestIsMasterFailoverErrorIsWrapperAware(t *testing.T) {
	dns := &net.DNSError{Err: "no such host", Name: "redis-nowhere", IsNotFound: true}
	wrappedDNS := fmt.Errorf("dialing redis-nowhere:6379: %w", dns)

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "wrapped DNS failure", err: wrappedDNS, want: true},
		{name: "wrapped connection refused", err: fmt.Errorf("dialing redis:6379: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")}), want: true},
		{name: "READONLY reply", err: errors.New("READONLY You can't write against a read only replica"), want: true},
		{name: "no such host text", err: errors.New("lookup redis-nowhere: no such host"), want: true},
		{name: "business error", err: errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), want: false},
		{name: "saturated pool (wrapped context deadline)", err: fmt.Errorf("waiting for a free Redis connection: %w", context.DeadlineExceeded), want: false},
		{name: "cancelled request", err: fmt.Errorf("waiting for a free Redis connection: %w", context.Canceled), want: false},
		{name: "closed client", err: ErrClosed, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsMasterFailoverError(tc.err); got != tc.want {
				t.Fatalf("IsMasterFailoverError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
