package redis

import (
	"net"
	"testing"
	"time"
)

func TestBreakerOpensAndCloses(t *testing.T) {
	b := NewBreaker(BreakerConfig{Threshold: 3, Reattempt: 50 * time.Millisecond})

	if !b.Allow() {
		t.Fatal("a fresh breaker must be closed")
	}

	b.Failure()
	b.Failure()
	if !b.Allow() {
		t.Fatal("the breaker must stay closed below the threshold")
	}

	b.Failure()
	if b.Allow() || !b.Open() {
		t.Fatal("the breaker must be open at the threshold")
	}

	// Half-open: one probe is allowed after the reattempt delay (poll, so that
	// a slow CI runner cannot make this test flaky).
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		if !b.Open() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !b.Allow() {
		t.Fatal("the half-open breaker must allow a probe")
	}
	if b.Allow() {
		t.Fatal("only one probe must be allowed at a time")
	}

	// A successful probe closes the breaker and resets the failure count.
	b.Success()
	if !b.Allow() || b.Open() {
		t.Fatal("the breaker must be closed after a successful probe")
	}

	b.Failure()
	b.Failure()
	if !b.Allow() {
		t.Fatal("the failure count must have been reset by Success")
	}
}

func TestBreakerDefaults(t *testing.T) {
	b := NewBreaker(BreakerConfig{})

	if b.cfg.Threshold != 3 || b.cfg.Reattempt != 15*time.Second {
		t.Fatalf("defaults = %+v, want 3 failures / 15s", b.cfg)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestStaticProviderRotation(t *testing.T) {
	p, err := NewStaticProvider([]string{"a:6379", "b:6379", "a:6379"}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	if len(p.endpoints) != 2 {
		t.Fatalf("endpoints = %v, want the duplicates removed", p.endpoints)
	}

	addr, gen, err := p.Node(nil)
	if err != nil || addr != "a:6379" || gen != 0 {
		t.Fatalf("Node = (%q,%d,%v), want (a:6379,0,nil)", addr, gen, err)
	}

	if p.Failure(nil) {
		t.Fatal("Failure(nil) must not switch")
	}

	// A failure rotates to the next endpoint and bumps the generation.
	if !p.Failure(errString("connection refused")) {
		t.Fatal("Failure must rotate to the next endpoint")
	}
	addr, gen, _ = p.Node(nil)
	if addr != "b:6379" || gen != 1 {
		t.Fatalf("Node = (%q,%d), want (b:6379,1)", addr, gen)
	}

	// The anti-flapping interval avoids switching again immediately.
	if p.Failure(errString("connection refused")) {
		t.Fatal("the rotation must be rate limited")
	}
}

func TestStaticProviderIgnoresBusinessErrors(t *testing.T) {
	p, err := NewStaticProvider([]string{"a:6379", "b:6379"}, time.Millisecond)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	before, _, _ := p.Node(nil)

	// A RESP error such as NOSCRIPT is a business-level error: it must not
	// move traffic to another endpoint.
	business := respValue{kind: respError, str: "NOSCRIPT No matching script. Please use EVAL."}
	if p.Failure(business) {
		t.Fatal("a NOSCRIPT reply must not rotate the endpoint list")
	}
	if wrongType := (respValue{kind: respError, str: "WRONGTYPE Operation against a key"}); p.Failure(wrongType) {
		t.Fatal("a WRONGTYPE reply must not rotate the endpoint list")
	}

	after, gen, _ := p.Node(nil)
	if before != after || gen != 0 {
		t.Fatalf("endpoint rotated on a business error: %q -> %q (generation %d)", before, after, gen)
	}

	// A connection-level failure still rotates.
	connErr := &net.OpError{Op: "dial", Err: errString("connection refused")}
	if !p.Failure(connErr) {
		t.Fatal("a connection failure must rotate the endpoint list")
	}
	if _, gen, _ := p.Node(nil); gen != 1 {
		t.Fatalf("generation = %d, want 1 after a connection failure", gen)
	}
}

func TestStaticProviderSingleEndpointNeverRotates(t *testing.T) {
	p, err := NewStaticProvider([]string{"a:6379"}, 0)
	if err != nil {
		t.Fatalf("NewStaticProvider: %v", err)
	}

	if p.Failure(errString("connection refused")) {
		t.Fatal("a single endpoint cannot be rotated: the error must surface to denyOnError")
	}
	if p.Generation() != 0 {
		t.Fatalf("generation = %d, want 0", p.Generation())
	}
}

func TestStaticProviderRejectsInvalidEndpoint(t *testing.T) {
	if _, err := NewStaticProvider([]string{"without-port"}, 0); err == nil {
		t.Fatal("expected an error for an endpoint without a port")
	}
	if _, err := NewStaticProvider(nil, 0); err == nil {
		t.Fatal("expected an error for an empty endpoint list")
	}
}
