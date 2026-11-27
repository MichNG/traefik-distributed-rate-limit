package traefik_distributed_rate_limit

// Coverage of the documented options that had no behavioural test yet:
//
//   * store.redis.sentinel.password (environment indirection and warning);
//   * sourceCriterion.ipStrategy.excludedIPs, as the criterion and as the
//     clientIP fallback;
//   * responseHeaders on the denial path (all four headers);
//   * period variants and errors, burst normalization;
//   * whitelistIPs variants (single address, CIDR, list, no match);
//   * onEmptySource=shared as the default, and denyOnError with
//     denyOnError=false;
//   * the TLS material reaching the Sentinel connections.
//
// Every test is offline: the store is a fake, and the TLS Sentinel here is a
// local listener.

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MichNG/traefik-distributed-rate-limit/internal/redis"
)

// ---------------------------------------------------------------------------
// store.redis.sentinel.password
// ---------------------------------------------------------------------------

func TestSentinelPasswordFromEnvironment(t *testing.T) {
	t.Setenv("TEST_SENTINEL_PASSWORD", "sentinel-from-env")
	t.Setenv("TEST_DATA_PASSWORD", "data-from-env")

	cfg := decode(t, `{"store": {"redis": {
		"endpoints": ["sentinel:26379"],
		"password": "$TEST_DATA_PASSWORD",
		"sentinel": {"masterSet": "mymaster", "password": "$TEST_SENTINEL_PASSWORD"}
	}}}`)

	warnings, err := cfg.normalize()
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	if cfg.Store.Redis.Sentinel.Password != "sentinel-from-env" {
		t.Fatalf("sentinel password = %q, want sentinel-from-env", cfg.Store.Redis.Sentinel.Password)
	}
	// The two pairs are independent.
	if cfg.Store.Redis.Password != "data-from-env" {
		t.Fatalf("data password = %q, want data-from-env", cfg.Store.Redis.Password)
	}

	for _, warning := range warnings {
		if strings.Contains(warning, "TEST_SENTINEL_PASSWORD") {
			t.Fatalf("unexpected warning for a variable that is set: %q", warning)
		}
	}
}

func TestMissingSentinelPasswordEnvironmentVariableIsReported(t *testing.T) {
	cfg := decode(t, `{"store": {"redis": {
		"endpoints": ["sentinel:26379"],
		"sentinel": {"masterSet": "mymaster", "password": "$DISTRIBUTED_RATELIMIT_SENTINEL_NOT_SET"}
	}}}`)

	warnings, err := cfg.normalize()
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	found := false
	for _, warning := range warnings {
		if strings.Contains(warning, "store.redis.sentinel.password") &&
			strings.Contains(warning, "DISTRIBUTED_RATELIMIT_SENTINEL_NOT_SET") {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings = %v, want one mentioning store.redis.sentinel.password and the missing variable", warnings)
	}

	// The unresolved reference is kept, so that a wrong configuration fails
	// loudly instead of authenticating without a password.
	if cfg.Store.Redis.Sentinel.Password != "$DISTRIBUTED_RATELIMIT_SENTINEL_NOT_SET" {
		t.Fatalf("sentinel password = %q, want the unresolved reference", cfg.Store.Redis.Sentinel.Password)
	}
}

// ---------------------------------------------------------------------------
// sourceCriterion.ipStrategy.excludedIPs
// ---------------------------------------------------------------------------

func TestExcludedIPsPickTheFirstUnlistedIP(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 5, "period": "10s",
		"sourceCriterion": {"ipStrategy": {"excludedIPs": ["10.0.0.0/8", "192.168.0.0/16"]}},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	// The proxy chain is scanned from the right: the first address that is not
	// in the excluded list is the client.
	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("10.0.0.1:1234", "", "X-Forwarded-For", "10.0.0.1, 192.168.1.1, 203.0.113.7"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(store.keys) != 1 || store.keys[0] != "rate:test:203.0.113.7" {
		t.Fatalf("keys = %v, want [rate:test:203.0.113.7]", store.keys)
	}

	// Another client ends up in its own bucket.
	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("10.0.0.1:1234", "", "X-Forwarded-For", "10.0.0.1, 203.0.113.8"))
	if len(store.keys) != 2 || store.keys[1] != "rate:test:203.0.113.8" {
		t.Fatalf("keys = %v, want the second client in its own bucket", store.keys)
	}
}

func TestExcludedIPsWithEveryAddressExcludedShareOneBucket(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 5, "period": "10s",
		"sourceCriterion": {"ipStrategy": {"excludedIPs": ["10.0.0.0/8"]}},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("10.0.0.1:1234", "", "X-Forwarded-For", "10.0.0.1, 10.0.0.2"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// No usable address: the default (shared) empty-source bucket applies.
	if len(store.keys) != 1 || store.keys[0] != "rate:test:" {
		t.Fatalf("keys = %v, want the shared bucket [rate:test:]", store.keys)
	}
}

func TestExcludedIPsAsClientIPFallback(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 5, "period": "10s",
		"sourceCriterion": {
			"cookieName": "session",
			"onEmptySource": "clientIP",
			"ipStrategy": {"excludedIPs": ["10.0.0.0/8"]}
		},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	// No cookie: the fallback is the first address that is not excluded.
	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("10.0.0.1:1234", "", "X-Forwarded-For", "10.0.0.1, 203.0.113.7"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(store.keys) != 1 || store.keys[0] != "rate:test:203.0.113.7" {
		t.Fatalf("keys = %v, want [rate:test:203.0.113.7] from the fallback", store.keys)
	}

	// With the cookie, the cookie is the source: the strategy is only a fallback.
	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("10.0.0.1:1234", "session=tenant-a", "X-Forwarded-For", "10.0.0.1, 203.0.113.7"))
	if len(store.keys) != 2 || store.keys[1] != "rate:test:tenant-a" {
		t.Fatalf("keys = %v, want the cookie to win over the fallback", store.keys)
	}
}

// ---------------------------------------------------------------------------
// responseHeaders
// ---------------------------------------------------------------------------

func TestResponseHeadersOnDenialIncludeAllFour(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 0, Remaining: 0, RetryAfter: time.Second, ResetAfter: 3 * time.Second})

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "10s", "responseHeaders": true, "store": {"redis": {"endpoints": ["redis:6379"]}}}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}

	want := map[string]string{
		"X-RateLimit-Limit":     "10",
		"X-RateLimit-Period":    "10s",
		"X-RateLimit-Remaining": "0",
		"X-RateLimit-Reset":     "3",
	}
	for header, value := range want {
		if got := rec.Header().Get(header); got != value {
			t.Errorf("on a denial: %s = %q, want %q", header, got, value)
		}
	}

	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Error("on a denial: Retry-After must be set")
	}
}

func TestResponseHeadersAreAbsentByDefault(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "10s", "store": {"redis": {"endpoints": ["redis:6379"]}}}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	for _, header := range []string{"X-RateLimit-Limit", "X-RateLimit-Period", "X-RateLimit-Remaining", "X-RateLimit-Reset"} {
		if got := rec.Header().Get(header); got != "" {
			t.Errorf("responseHeaders is disabled, but %s = %q", header, got)
		}
	}
}

// ---------------------------------------------------------------------------
// period and burst
// ---------------------------------------------------------------------------

func TestPeriodVariants(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "seconds", in: "1s", want: "1s"},
		{name: "minutes", in: "1m", want: "1m0s"},
		{name: "milliseconds", in: "500ms", want: "500ms"},
		{name: "legacy plain number of seconds", in: "10", want: "10s"},
		{name: "fractional seconds", in: "1500ms", want: "1.5s"},
		{name: "absent uses the default", in: "", want: "1s"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := decode(t, fmt.Sprintf(`{"limit": 5, "period": %q, "store": {"redis": {"endpoints": ["redis:6379"]}}}`, tc.in))

			if _, err := cfg.normalize(); err != nil {
				t.Fatalf("normalize(%q): %v", tc.in, err)
			}
			if cfg.Period != tc.want {
				t.Fatalf("period %q normalized to %q, want %q", tc.in, cfg.Period, tc.want)
			}
		})
	}
}

func TestPeriodErrors(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantErr string
	}{
		{name: "zero", in: "0s", wantErr: "period must be greater than 0"},
		{name: "zero as a plain number", in: "0", wantErr: "period must be greater than 0"},
		{name: "negative", in: "-1s", wantErr: "period must be greater than 0"},
		{name: "not a duration", in: "abc", wantErr: "invalid period"},
		{name: "unit only", in: "s", wantErr: "invalid period"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := decode(t, fmt.Sprintf(`{"limit": 5, "period": %q, "store": {"redis": {"endpoints": ["redis:6379"]}}}`, tc.in))

			_, err := cfg.normalize()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("normalize(%q) = %v, want an error containing %q", tc.in, err, tc.wantErr)
			}
		})
	}
}

func TestBurstIsNormalized(t *testing.T) {
	cases := []struct {
		in   int64
		want int64
	}{
		{in: 0, want: 1},
		{in: -3, want: 1},
		{in: 1, want: 1},
		{in: 5, want: 5},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("burst %d", tc.in), func(t *testing.T) {
			cfg := decode(t, fmt.Sprintf(`{"limit": 5, "burst": %d, "period": "1s", "store": {"redis": {"endpoints": ["redis:6379"]}}}`, tc.in))

			if _, err := cfg.normalize(); err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if cfg.Burst != tc.want {
				t.Fatalf("burst %d normalized to %d, want %d", tc.in, cfg.Burst, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// whitelistIPs
// ---------------------------------------------------------------------------

func TestWhitelistIPVariants(t *testing.T) {
	cases := []struct {
		name      string
		whitelist string
		remote    string
		wantKey   bool
	}{
		{name: "single address", whitelist: `["192.0.2.10"]`, remote: "192.0.2.10:1234"},
		{name: "CIDR range", whitelist: `["192.0.2.0/24"]`, remote: "192.0.2.10:1234"},
		{name: "several entries", whitelist: `["198.51.100.0/24", "192.0.2.10"]`, remote: "192.0.2.10:1234"},
		{name: "no match", whitelist: `["192.0.2.0/24"]`, remote: "203.0.113.5:1234", wantKey: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

			cfg := decode(t, fmt.Sprintf(`{
				"limit": 5, "burst": 5, "period": "10s",
				"whitelistIPs": %s,
				"store": {"redis": {"endpoints": ["redis:6379"]}}
			}`, tc.whitelist))

			rl, err := newTestMiddleware(t, cfg, store)
			if err != nil {
				t.Fatalf("newTestMiddleware: %v", err)
			}

			rec := httptest.NewRecorder()
			rl.ServeHTTP(rec, newRequest(tc.remote))

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}

			if tc.wantKey {
				if store.callCount() == 0 {
					t.Fatal("an address outside the whitelist must be rate limited (the store must be called)")
				}
				return
			}

			if store.callCount() != 0 {
				t.Fatalf("a whitelisted address must bypass the store entirely, calls = %d", store.callCount())
			}
		})
	}
}

func TestWhitelistIPInvalidEntryFailsAtCreation(t *testing.T) {
	cfg := decode(t, `{
		"limit": 5, "period": "1s",
		"whitelistIPs": ["not-an-address"],
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	if _, err := newTestMiddleware(t, cfg, &fakeStore{}); err == nil {
		t.Fatal("want an error for an entry that is neither an address nor a CIDR range")
	}
}

// ---------------------------------------------------------------------------
// onEmptySource
// ---------------------------------------------------------------------------

func TestOnEmptySourceSharedIsTheDefault(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	// onEmptySource is not set at all: "shared" must be assumed.
	cfg := decode(t, `{
		"limit": 5, "burst": 5, "period": "10s",
		"sourceCriterion": {"requestHeaderName": "X-Api-Key"},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	for i, remote := range []string{"192.0.2.10:1234", "198.51.100.7:9999"} {
		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest(remote, "", "X-Api-Key", ""))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, rec.Code)
		}
	}

	// Two unidentified clients, one shared bucket.
	const shared = "rate:test:"
	if len(store.keys) != 2 || store.keys[0] != shared || store.keys[1] != shared {
		t.Fatalf("keys = %v, want two hits on the shared bucket %q", store.keys, shared)
	}
}

func TestOnEmptySourceDenyOnErrorFailsOpenForCookies(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 3, "burst": 5, "period": "10s",
		"denyOnError": false,
		"sourceCriterion": {"cookieName": "session", "onEmptySource": "denyOnError"},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "other=1", "", ""))

	if rec.Code != http.StatusOK || rec.Body.String() != "backend" {
		t.Fatalf("status=%d body=%q, want the request to reach the backend", rec.Code, rec.Body.String())
	}
	if store.callCount() != 0 {
		t.Fatalf("denyOnError=false must not query the store, calls = %d", store.callCount())
	}
}

// ---------------------------------------------------------------------------
// The TLS configuration protects the Sentinel connections too
// ---------------------------------------------------------------------------

// tlsSentinelStub accepts connections on a TLS listener, counts the completed
// handshakes, and answers SENTINEL get-master-addr-by-name. It never talks
// plaintext, so a client without TLS cannot use it.
type tlsSentinelStub struct {
	addr           string
	masterHostPort string

	mu              sync.Mutex
	handshakes      int
	handshakeErrors int
}

func newTLSSentinelStub(t *testing.T, masterHostPort string) *tlsSentinelStub {
	t.Helper()

	cert, err := selfSignedCertificate()
	if err != nil {
		t.Fatalf("generating a certificate: %v", err)
	}

	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = tcpLn.Close() })

	stub := &tlsSentinelStub{addr: tcpLn.Addr().String(), masterHostPort: masterHostPort}

	go func() {
		for {
			conn, err := tcpLn.Accept()
			if err != nil {
				return
			}

			go func() {
				tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}})
				if err := tlsConn.Handshake(); err != nil {
					stub.mu.Lock()
					stub.handshakeErrors++
					stub.mu.Unlock()
					_ = conn.Close()
					return
				}

				stub.mu.Lock()
				stub.handshakes++
				stub.mu.Unlock()

				stub.serve(tlsConn)
			}()
		}
	}()

	return stub
}

func (s *tlsSentinelStub) counts() (handshakes, handshakeErrors int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handshakes, s.handshakeErrors
}

func (s *tlsSentinelStub) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	host, port, err := net.SplitHostPort(s.masterHostPort)
	if err != nil {
		return
	}

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		switch {
		case strings.Contains(strings.ToUpper(line), "AUTH"):
			_, _ = conn.Write([]byte("+OK\r\n"))
		case strings.Contains(strings.ToUpper(line), "PING"):
			_, _ = conn.Write([]byte("+PONG\r\n"))
		case strings.Contains(line, "get-master-addr-by-name"):
			_, _ = conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(host), host, len(port), port)))
		}
	}
}

// selfSignedPEM generates a self-signed certificate and its key, as PEM.
func selfSignedPEM() (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "sentinel-stub"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

func selfSignedCertificate() (tls.Certificate, error) {
	certPEM, keyPEM, err := selfSignedPEM()
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.X509KeyPair(certPEM, keyPEM)
}

// writeTestCertPair writes a self-signed certificate and its key to files, for
// the mTLS options of the README.
func writeTestCertPair(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	certPEM, keyPEM, err := selfSignedPEM()
	if err != nil {
		t.Fatalf("generating a certificate: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "client.crt")
	keyPath = filepath.Join(dir, "client.key")

	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("writing the certificate: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("writing the key: %v", err)
	}

	return certPath, keyPath
}

func TestSentinelConnectionsCarryTheConfiguredTLS(t *testing.T) {
	stub := newTLSSentinelStub(t, "127.0.0.1:6399")

	cfg := decode(t, fmt.Sprintf(`{
		"limit": 5, "burst": 5, "period": "10s",
		"store": {"redis": {
			"endpoints": [%q],
			"timeout": "500ms",
			"sentinel": {"masterSet": "mymaster"},
			"tls": {"insecureSkipVerify": true}
		}}
	}`, stub.addr))

	store, err := newStore(context.Background(), cfg, newPluginLogger("coverage"))
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	t.Cleanup(store.Close)

	if handshakes, _ := stub.counts(); handshakes == 0 {
		t.Fatal("the Sentinel connection was not established over TLS")
	}

	// The master resolved through the Sentinel is the one the plugin uses.
	if got := store.Node(); !strings.Contains(got, "6399") {
		t.Fatalf("store node = %q, want the master resolved over TLS (…:6399)", got)
	}
}

func TestSentinelConnectionsAreNotTLSWhenNotConfigured(t *testing.T) {
	stub := newTLSSentinelStub(t, "127.0.0.1:6399")

	cfg := decode(t, fmt.Sprintf(`{
		"limit": 5, "burst": 5, "period": "10s",
		"store": {"redis": {
			"endpoints": [%q],
			"timeout": "500ms",
			"sentinel": {"masterSet": "mymaster"}
		}}
	}`, stub.addr))

	store, err := newStore(context.Background(), cfg, newPluginLogger("coverage"))
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	t.Cleanup(store.Close)

	// Without store.redis.tls the client speaks plaintext: the TLS-only stub
	// cannot complete a handshake, which is what gives the test above its
	// meaning.
	handshakes, handshakeErrors := stub.counts()
	if handshakes != 0 {
		t.Fatalf("handshakes = %d, want none without store.redis.tls", handshakes)
	}
	if handshakeErrors == 0 {
		t.Fatal("the stub saw no failed handshake: is it really TLS-only?")
	}
}
