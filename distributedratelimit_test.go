package traefik_distributed_rate_limit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/MichNG/traefik-distributed-rate-limit/internal/redis"
)

// testCAPEM is a self-signed CA certificate used to exercise the TLS
// configuration paths without touching a real Redis server.
const testCAPEM = `-----BEGIN CERTIFICATE-----
MIIDEDCCAfigAwIBAgIUIMc7Pw4gWq7T1cDtlUL3Fj9cOg0wDQYJKoZIhvcNAQEL
BQAwKDEmMCQGA1UEAwwdZGlzdHJpYnV0ZWQtcmF0ZWxpbWl0LXRlc3QtY2EwHhcN
MjYxMTExMTkxNDM0WhcNMzYxMTA4MTkxNDM0WjAoMSYwJAYDVQQDDB1kaXN0cmli
dXRlZC1yYXRlbGltaXQtdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBALYpg/A+WA24YDcbVIAHj1y2yIitR3nTvgpsL0qaaoDlyQ8589tvEpsS
OUdRyFPZBNhCx+ulSFtd4rnrI6I6GSGIb+QKZzR+kOrtTIMsjFCXMglxSCijXMzz
oIRjK2Y2e4Zp/AAvp3rO5OHQjCnwGNfuK4oxFkA90dGroY/n/cv/955r0ezc1Tq/
QTBZtW49k5uf+btgXX4I+vo2jAPXXv4ECfzyG/WtVWslDRrqNiY+GFrM7MmRNKOb
ukCOzayRG1DPfAk3vablOcOx1sxRnk7rw8PPBrMWTNfqoVR1/69vntVl5UMujl//
yfSBSA2cA7xhfoPxyhAzhM4b/oNQgmUCAwEAAaMyMDAwHQYDVR0OBBYEFIEjn8Qd
+krWU7iLKaoZgNISdHVXMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQAD
ggEBAAy2ZaKA5ajk5sOEeokO66dm9a+Rk63JfTjBXShT/UDtb5O37i1BKwoHlvsO
hZcXGAELOJIZ/DE+RAimH4M/R8tsm930K1W4rPYFR+tqqwqVbXdWAaziZxvtZ4e5
aEw9xRfN2jvPpq7o4p3V3V0fYp6Rj9fuuoMC4z0DNMOlJM4OatdnFZVS9HkQw/mS
4o/UrFHkQml5vSGQ5LSIh7JrJl0Ci5fijmFVezmJ0Ap6kNulJrPdmCOmmBBqWJCG
XjLmJFNGKnPu4ZiZ1y5FFzqBY07pRcgSc6NCC7UJR7NPISliNJajOslppmdbJLZq
yKzAJad4Z1iIgGvqLve0FT7OnNc=
-----END CERTIFICATE-----`

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// fakeStore is a Store whose script behaviour is driven by the test.
type fakeStore struct {
	mu sync.Mutex

	output redis.ScriptOutput
	err    error

	// denied lists the sources whose bucket is exhausted.
	denied map[string]bool

	calls int
	keys  []string
	args  [][]string
}

func (s *fakeStore) Close()                                {}
func (s *fakeStore) Ping(_ context.Context) error          { return nil }
func (s *fakeStore) Del(_ context.Context, _ string) error { return nil }
func (s *fakeStore) Node() string                          { return "fake:6379" }
func (s *fakeStore) NewScript(_ string) redis.ScriptRunner { return &fakeScript{store: s} }

type fakeScript struct {
	store *fakeStore
}

func (f *fakeScript) Run(_ context.Context, keys []string, args ...string) (redis.ScriptOutput, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()

	f.store.calls++
	f.store.keys = append(f.store.keys, keys...)
	f.store.args = append(f.store.args, append([]string(nil), args...))

	if f.store.err != nil {
		return redis.ScriptOutput{}, f.store.err
	}

	if len(keys) > 0 && f.store.denied[keys[0]] {
		return redis.ScriptOutput{Allowed: 0, Remaining: 0, RetryAfter: time.Second, ResetAfter: time.Second}, nil
	}

	return f.store.output, nil
}

func (s *fakeStore) setOutput(out redis.ScriptOutput) {
	s.mu.Lock()
	s.output = out
	s.mu.Unlock()
}

// setDenied exhausts the bucket of the given sources.
func (s *fakeStore) setDenied(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.denied == nil {
		s.denied = map[string]bool{}
	}
	for _, key := range keys {
		s.denied[key] = true
	}
}

func (s *fakeStore) setError(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *fakeStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// decode mimics the decoding performed by Traefik's plugin loader
// (mapstructure with weakly typed input).
func decode(t *testing.T, raw string) *Config {
	t.Helper()

	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		t.Fatalf("unmarshaling the test configuration: %v", err)
	}

	cfg := CreateConfig()
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToSliceHookFunc(","),
		WeaklyTypedInput: true,
		Result:           cfg,
	})
	if err != nil {
		t.Fatalf("creating the decoder: %v", err)
	}
	if err := decoder.Decode(data); err != nil {
		t.Fatalf("decoding the test configuration: %v", err)
	}

	return cfg
}

func newTestMiddleware(t *testing.T, cfg *Config, store Store) (*ClusterRateLimit, error) {
	t.Helper()

	warnings, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	_ = warnings

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	next := http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("backend"))
	})

	if store == nil {
		store = &fakeStore{}
	}

	return newRateLimit(context.Background(), next, cfg, "test", store)
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

func TestDecodeHubSchema(t *testing.T) {
	caPath := writeTestCA(t)

	cfg := decode(t, fmt.Sprintf(`{
		"limit": 100,
		"period": "1s",
		"burst": 200,
		"denyOnError": false,
		"responseHeaders": true,
		"sourceCriterion": {"ipStrategy": {"excludedIPs": ["172.20.176.201"]}},
		"store": {
			"redis": {
				"endpoints": ["redis-0:6379", "redis-1:6379"],
				"username": "ratelimit-user",
				"password": "secret",
				"database": 2,
				"timeout": "500ms",
				"poolSize": 12,
				"maxActiveConns": 40,
				"idleTimeout": "30s",
				"tls": {"caBundle": %q, "insecureSkipVerify": true},
				"sentinel": {"masterSet": "mymaster", "username": "sent-user", "password": "sent-secret", "watch": false}
			}
		}
	}`, caPath))

	warnings, err := cfg.normalize()
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings for a Hub-schema config: %v", warnings)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if cfg.Limit != 100 || cfg.Burst != 200 || cfg.Period != "1s" {
		t.Fatalf("rate config = %+v", cfg)
	}
	if cfg.denyOnError() {
		t.Fatal("denyOnError = true, want false")
	}
	if !cfg.ResponseHeaders {
		t.Fatal("responseHeaders = false, want true")
	}
	if cfg.SourceCriterion == nil || cfg.SourceCriterion.IPStrategy == nil ||
		len(cfg.SourceCriterion.IPStrategy.ExcludedIPs) != 1 {
		t.Fatalf("sourceCriterion = %+v", cfg.SourceCriterion)
	}

	redisCfg := cfg.Store.Redis
	if len(redisCfg.Endpoints) != 2 || redisCfg.Username != "ratelimit-user" ||
		redisCfg.Password != "secret" || redisCfg.Database != 2 {
		t.Fatalf("store.redis = %+v", redisCfg)
	}
	if redisCfg.Sentinel == nil || redisCfg.Sentinel.MasterSet != "mymaster" {
		t.Fatalf("store.redis.sentinel = %+v", redisCfg.Sentinel)
	}
	if redisCfg.TLS == nil || redisCfg.TLS.CABundle != caPath || !redisCfg.TLS.InsecureSkipVerify {
		t.Fatalf("store.redis.tls = %+v", redisCfg.TLS)
	}

	timeout, err := cfg.timeout()
	if err != nil || timeout.String() != "500ms" {
		t.Fatalf("timeout = %v (%v), want 500ms", timeout, err)
	}
	if redisCfg.PoolSize != 12 || redisCfg.MaxActiveConns != 40 {
		t.Fatalf("pool configuration = poolSize:%d maxActiveConns:%d, want 12/40", redisCfg.PoolSize, redisCfg.MaxActiveConns)
	}

	if idle, err := cfg.idleTimeout(); err != nil || idle != 30*time.Second {
		t.Fatalf("idleTimeout = %v (%v), want 30s", idle, err)
	}
	if cfg.sentinelWatch() {
		t.Fatal("sentinel.watch = true, want false")
	}
}

func TestDefaults(t *testing.T) {
	cfg := decode(t, `{"store": {"redis": {"endpoints": ["redis:6379"]}}}`)

	if _, err := cfg.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if cfg.Limit != 0 {
		t.Fatalf("limit = %d, want 0 (no rate limiting)", cfg.Limit)
	}
	if cfg.Burst != 1 {
		t.Fatalf("burst = %d, want 1", cfg.Burst)
	}
	if cfg.Period != "1s" {
		t.Fatalf("period = %q, want 1s", cfg.Period)
	}
	if !cfg.denyOnError() {
		t.Fatal("denyOnError must default to true, like Traefik Hub")
	}
	if cfg.ResponseHeaders {
		t.Fatal("responseHeaders must default to false")
	}
	if idle, err := cfg.idleTimeout(); err != nil || idle != 0 {
		t.Fatalf("idleTimeout = %v (%v), want 0 (disabled)", idle, err)
	}
	if !cfg.sentinelWatch() {
		t.Fatal("sentinel.watch must default to true")
	}
}

func TestLegacyConfigurationIsStillAccepted(t *testing.T) {
	cfg := decode(t, `{
		"average": 5,
		"burst": 10,
		"period": 10,
		"redisAddress": "redis:6379",
		"redisDb": 1,
		"redisPassword": "legacy-secret",
		"redisConnectionTimeout": 2,
		"breakerThreshold": 7,
		"breakerReattempt": 30,
		"whitelistIPs": ["10.0.0.0/8"]
	}`)

	warnings, err := cfg.normalize()
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if len(warnings) < 5 {
		t.Fatalf("expected a deprecation warning per legacy key, got %v", warnings)
	}

	if cfg.Limit != 5 {
		t.Fatalf("limit = %d, want 5 (from average)", cfg.Limit)
	}
	if cfg.Period != "10s" {
		t.Fatalf("period = %q, want 10s (a bare number is legacy seconds)", cfg.Period)
	}
	redisCfg := cfg.Store.Redis
	if len(redisCfg.Endpoints) != 1 || redisCfg.Endpoints[0] != "redis:6379" {
		t.Fatalf("endpoints = %v", redisCfg.Endpoints)
	}
	if redisCfg.Database != 1 || redisCfg.Password != "legacy-secret" {
		t.Fatalf("store.redis = %+v", redisCfg)
	}
	if redisCfg.Timeout != "2s" {
		t.Fatalf("timeout = %q, want 2s", redisCfg.Timeout)
	}
	if redisCfg.Breaker == nil || redisCfg.Breaker.Threshold != 7 || redisCfg.Breaker.Reattempt != 30 {
		t.Fatalf("breaker = %+v", redisCfg.Breaker)
	}
	if len(cfg.WhitelistIPs) != 1 {
		t.Fatalf("whitelistIPs = %v", cfg.WhitelistIPs)
	}
}

func TestNewKeysWinOverLegacyOnes(t *testing.T) {
	cfg := decode(t, `{
		"limit": 42,
		"average": 5,
		"store": {"redis": {"endpoints": ["new:6379"], "username": "new-user"}},
		"redisAddress": "old:6379",
		"redisDb": 3
	}`)

	warnings, err := cfg.normalize()
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	if cfg.Limit != 42 {
		t.Fatalf("limit = %d, want 42", cfg.Limit)
	}
	if cfg.Store.Redis.Endpoints[0] != "new:6379" {
		t.Fatalf("endpoints = %v, want [new:6379]", cfg.Store.Redis.Endpoints)
	}
	if cfg.Store.Redis.Username != "new-user" {
		t.Fatalf("username = %q, want new-user", cfg.Store.Redis.Username)
	}
	if cfg.Store.Redis.Database != 0 {
		t.Fatalf("database = %d, want 0 (the legacy redisDb must not be applied when store.redis is configured)", cfg.Store.Redis.Database)
	}
	if len(warnings) == 0 {
		t.Fatal("expected deprecation warnings")
	}
}

func TestPasswordFromEnvironment(t *testing.T) {
	t.Setenv("TEST_REDIS_PASSWORD", "from-env")

	cfg := decode(t, `{"store": {"redis": {"endpoints": ["redis:6379"], "password": "$TEST_REDIS_PASSWORD"}}}`)

	if _, err := cfg.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if cfg.Store.Redis.Password != "from-env" {
		t.Fatalf("password = %q, want from-env", cfg.Store.Redis.Password)
	}
}

func TestMissingEnvironmentVariableIsReported(t *testing.T) {
	cfg := decode(t, `{"store": {"redis": {"endpoints": ["redis:6379"], "password": "$DISTRIBUTED_RATELIMIT_NOT_SET"}}}`)

	warnings, err := cfg.normalize()
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	found := false
	for _, warning := range warnings {
		if strings.Contains(warning, "DISTRIBUTED_RATELIMIT_NOT_SET") {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings = %v, want one mentioning the missing environment variable", warnings)
	}

	// The unresolved reference is kept: an AUTH attempt with it fails loudly,
	// instead of silently connecting without a password.
	if cfg.Store.Redis.Password != "$DISTRIBUTED_RATELIMIT_NOT_SET" {
		t.Fatalf("password = %q, want the unresolved reference", cfg.Store.Redis.Password)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "negative limit",
			raw:     `{"limit": -1, "store": {"redis": {"endpoints": ["redis:6379"]}}}`,
			wantErr: "limit must be >= 0",
		},
		{
			name:    "missing endpoints",
			raw:     `{"limit": 1}`,
			wantErr: "store.redis.endpoints must not be empty",
		},
		{
			name:    "cluster is not supported",
			raw:     `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "cluster": {}}}}`,
			wantErr: "store.redis.cluster is not supported yet",
		},
		{
			name:    "sentinel without masterSet",
			raw:     `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {}}}}`,
			wantErr: "store.redis.sentinel.masterSet is required",
		},
		{
			name:    "negative poolSize",
			raw:     `{"store": {"redis": {"endpoints": ["redis:6379"], "poolSize": -1}}}`,
			wantErr: "store.redis.poolSize must be >= 0",
		},
		{
			name:    "negative maxActiveConns",
			raw:     `{"store": {"redis": {"endpoints": ["redis:6379"], "maxActiveConns": -2}}}`,
			wantErr: "store.redis.maxActiveConns must be >= 0",
		},
		{
			name:    "invalid idleTimeout",
			raw:     `{"store": {"redis": {"endpoints": ["redis:6379"], "idleTimeout": "later"}}}`,
			wantErr: "invalid store.redis.idleTimeout",
		},
		{
			name:    "invalid period",
			raw:     `{"period": "quickly", "store": {"redis": {"endpoints": ["redis:6379"]}}}`,
			wantErr: "invalid period",
		},
		{
			name:    "invalid timeout",
			raw:     `{"store": {"redis": {"endpoints": ["redis:6379"], "timeout": "soon"}}}`,
			wantErr: "invalid store.redis.timeout",
		},
		{
			name:    "cert without key",
			raw:     `{"store": {"redis": {"endpoints": ["redis:6379"], "tls": {"cert": "CERT"}}}}`,
			wantErr: "both TLS cert and key must be defined",
		},
		{
			name:    "invalid CA",
			raw:     `{"store": {"redis": {"endpoints": ["redis:6379"], "tls": {"caBundle": "not-a-pem"}}}}`,
			wantErr: "invalid store.redis.tls configuration",
		},
		{
			name:    "invalid endpoint",
			raw:     `{"store": {"redis": {"endpoints": ["redis-without-port"]}}}`,
			wantErr: "invalid Redis endpoint",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := decode(t, tc.raw)
			warnings, err := cfg.normalize()
			_ = warnings
			if err == nil {
				err = cfg.validate()
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func writeTestCA(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, []byte(testCAPEM), 0o600); err != nil {
		t.Fatalf("writing the test CA: %v", err)
	}

	return path
}

func TestTLSConfigurationPassesValidation(t *testing.T) {
	cfg := decode(t, fmt.Sprintf(`{
		"limit": 1,
		"store": {"redis": {"endpoints": ["redis:6379"], "tls": {"caBundle": %q}}}
	}`, writeTestCA(t)))

	if _, err := cfg.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	tlsCfg := cfg.tlsConfig()
	if tlsCfg == nil {
		t.Fatal("tlsConfig() = nil, want a TLS configuration")
	}
	built, err := tlsCfg.CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig: %v", err)
	}
	if built.RootCAs == nil {
		t.Fatal("RootCAs must be set from the CA bundle")
	}
}

func TestCAIsAcceptedUnderBothNames(t *testing.T) {
	for _, raw := range []string{
		`{"store": {"redis": {"endpoints": ["redis:6379"], "tls": {"caBundle": "X"}}}}`,
		`{"store": {"redis": {"endpoints": ["redis:6379"], "tls": {"ca": "X"}}}}`,
	} {
		cfg := decode(t, raw)
		if _, err := cfg.normalize(); err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if got := cfg.tlsConfig(); got == nil || got.CA != "X" {
			t.Fatalf("tlsConfig() = %+v, want CA=X", got)
		}
	}
}

// ---------------------------------------------------------------------------
// ServeHTTP
// ---------------------------------------------------------------------------

func newRequest(remoteAddr string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.RemoteAddr = remoteAddr
	return req
}

func TestAllowsRequestUnderTheLimit(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "10s", "store": {"redis": {"endpoints": ["redis:6379"]}}}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "backend" {
		t.Fatalf("body = %q", rec.Body.String())
	}
	if store.callCount() != 1 {
		t.Fatalf("store calls = %d, want 1", store.callCount())
	}
	if rec.Header().Get("X-RateLimit-Remaining") != "" {
		t.Fatal("responseHeaders is disabled: no X-RateLimit-* header expected")
	}
}

func TestDeniesRequestOverTheLimit(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 0, Remaining: 0, RetryAfter: 2500 * time.Millisecond, ResetAfter: 9 * time.Second})

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "10s", "store": {"redis": {"endpoints": ["redis:6379"]}}}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("Retry-After = %q, want 3", got)
	}
	if rec.Body.String() == "backend" {
		t.Fatal("the request must not reach the backend")
	}
}

func TestDenyOnErrorDefaultsToFailClosed(t *testing.T) {
	store := &fakeStore{}
	store.setError(errors.New("connection refused"))

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "1s", "store": {"redis": {"endpoints": ["redis:6379"]}}}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 (denyOnError defaults to true)", rec.Code)
	}
	if rec.Body.String() == "backend" {
		t.Fatal("the request must not reach the backend")
	}
}

func TestDenyOnErrorFalseFailsOpen(t *testing.T) {
	store := &fakeStore{}
	store.setError(errors.New("connection refused"))

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "1s", "denyOnError": false, "store": {"redis": {"endpoints": ["redis:6379"]}}}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (fail-open)", rec.Code)
	}
	if rec.Body.String() != "backend" {
		t.Fatalf("body = %q, want backend", rec.Body.String())
	}
}

func TestLimitZeroDisablesRateLimiting(t *testing.T) {
	store := &fakeStore{}
	store.setError(errors.New("must not be called"))

	cfg := decode(t, `{"limit": 0, "store": {"redis": {"endpoints": ["redis:6379"]}}}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if store.callCount() != 0 {
		t.Fatalf("store calls = %d, want 0", store.callCount())
	}
}

func TestResponseHeaders(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 7, ResetAfter: 1500 * time.Millisecond})

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "10s", "responseHeaders": true, "store": {"redis": {"endpoints": ["redis:6379"]}}}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	want := map[string]string{
		"X-RateLimit-Limit":     "10",
		"X-RateLimit-Period":    "10s",
		"X-RateLimit-Remaining": "7",
		"X-RateLimit-Reset":     "2",
	}
	for header, value := range want {
		if got := rec.Header().Get(header); got != value {
			t.Errorf("%s = %q, want %q", header, got, value)
		}
	}
}

func TestResponseHeadersOnDenial(t *testing.T) {
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
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("X-RateLimit-Remaining = %q, want 0", got)
	}
}

func TestWhitelistBypassesRateLimiting(t *testing.T) {
	store := &fakeStore{}
	store.setError(errors.New("must not be called"))

	cfg := decode(t, `{
		"limit": 5, "burst": 10, "period": "1s",
		"whitelistIPs": ["192.0.2.0/24"],
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if store.callCount() != 0 {
		t.Fatalf("store calls = %d, want 0 (whitelisted)", store.callCount())
	}
}

func TestRateLimitByRequestHeader(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 10, "period": "10s",
		"sourceCriterion": {"requestHeaderName": "X-Api-Key"},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	request := func(apiKey string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		if apiKey != "" {
			req.Header.Set("X-Api-Key", apiKey)
		}

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, req)
		return rec
	}

	// Two different keys have their own bucket.
	if rec := request("tenant-a"); rec.Code != http.StatusOK {
		t.Fatalf("tenant-a: status = %d, want 200", rec.Code)
	}
	if rec := request("tenant-b"); rec.Code != http.StatusOK {
		t.Fatalf("tenant-b: status = %d, want 200", rec.Code)
	}
	// A request without the header shares the (empty) default bucket.
	if rec := request(""); rec.Code != http.StatusOK {
		t.Fatalf("no header: status = %d, want 200", rec.Code)
	}

	want := []string{"rate:test:tenant-a", "rate:test:tenant-b", "rate:test:"}
	if len(store.keys) != len(want) {
		t.Fatalf("keys = %v, want %v", store.keys, want)
	}
	for i, key := range want {
		if store.keys[i] != key {
			t.Errorf("keys[%d] = %q, want %q", i, store.keys[i], key)
		}
	}

	// The denial is per source as well: exhausting tenant-a does not affect
	// the other buckets.
	store.setDenied("rate:test:tenant-a")

	if rec := request("tenant-a"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("tenant-a after exhaustion: status = %d, want 429", rec.Code)
	}
	if rec := request("tenant-c"); rec.Code != http.StatusOK {
		t.Fatalf("tenant-c: status = %d, want 200 (its own bucket)", rec.Code)
	}
	if rec := request(""); rec.Code != http.StatusOK {
		t.Fatalf("no header: status = %d, want 200 (its own bucket)", rec.Code)
	}
}

func TestRateLimitByRequestHost(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 10, "period": "10s",
		"sourceCriterion": {"requestHost": true},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://tenant-a.example.com/", nil)
	req.RemoteAddr = "192.0.2.10:1234"

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(store.keys) != 1 || store.keys[0] != "rate:test:tenant-a.example.com" {
		t.Fatalf("keys = %v, want [rate:test:tenant-a.example.com]", store.keys)
	}
}

func TestSourceIsScopedToTheMiddleware(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 1})

	cfg := decode(t, `{"limit": 5, "burst": 10, "period": "1s", "store": {"redis": {"endpoints": ["redis:6379"]}}}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))

	if len(store.keys) != 1 || store.keys[0] != "rate:test:192.0.2.10" {
		t.Fatalf("keys = %v, want [rate:test:192.0.2.10]", store.keys)
	}

	// args = burst, rate, period seconds, cost
	if len(store.args) != 1 || fmt.Sprint(store.args[0]) != "[10 5 1 1]" {
		t.Fatalf("args = %v, want [10 5 1 1]", store.args)
	}
}

func TestCircuitBreakerDeniesAfterThreshold(t *testing.T) {
	store := &fakeStore{}
	store.setError(errors.New("connection refused"))

	cfg := decode(t, `{
		"limit": 5, "burst": 10, "period": "1s",
		"store": {"redis": {"endpoints": ["redis:6379"], "breaker": {"threshold": 2, "reattempt": 60}}}
	}`)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	// Two failures open the breaker.
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("request %d: status = %d, want 429", i, rec.Code)
		}
	}

	// From now on the store is not even called.
	before := store.callCount()
	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))
	if store.callCount() != before {
		t.Fatalf("store calls = %d, want %d (the breaker must be open)", store.callCount(), before)
	}
}

// ---------------------------------------------------------------------------
// Source criterion: cookie and empty-source policy
// ---------------------------------------------------------------------------

func cookieRequest(remoteAddr, cookie, header string, headerValue string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.RemoteAddr = remoteAddr
	if cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	if headerValue != "" {
		req.Header.Set(header, headerValue)
	}
	return req
}

func TestRateLimitByCookie(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 10, "period": "10s",
		"sourceCriterion": {"cookieName": "rl-tenant"},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	for _, cookie := range []string{"rl-tenant=tenant-a", "rl-tenant=tenant-b"} {
		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", cookie, "", ""))
		if rec.Code != http.StatusOK {
			t.Fatalf("cookie %q: status = %d, want 200", cookie, rec.Code)
		}
	}

	want := []string{"rate:test:tenant-a", "rate:test:tenant-b"}
	if len(store.keys) != 2 || store.keys[0] != want[0] || store.keys[1] != want[1] {
		t.Fatalf("keys = %v, want %v", store.keys, want)
	}
}

func TestRateLimitByCookieField(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 10, "period": "10s",
		"sourceCriterion": {"cookieName": "session", "cookieField": "tenant", "onEmptySource": "deny"},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	cases := []struct {
		name   string
		cookie string
		want   int
		key    string
	}{
		// A JSON cookie is percent-encoded in practice: the raw quotes are not
		// valid inside a cookie value, and the Go cookie parser drops them.
		{name: "json field present", cookie: `session=%7B%22tenant%22%3A%22acme%22%2C%22plan%22%3A%22gold%22%7D`, want: http.StatusOK, key: "rate:test:acme"},
		{name: "other tenant", cookie: `session=%7B%22tenant%22%3A%22globex%22%7D`, want: http.StatusOK, key: "rate:test:globex"},
		{name: "field missing", cookie: `session=%7B%22plan%22%3A%22gold%22%7D`, want: http.StatusBadRequest},
		{name: "not json", cookie: `session=not-a-json`, want: http.StatusBadRequest},
		{name: "field not a string", cookie: `session=%7B%22tenant%22%3A42%7D`, want: http.StatusBadRequest},
		{name: "empty cookie value", cookie: `session=`, want: http.StatusBadRequest},
		{name: "cookie absent", cookie: `other=1`, want: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(store.keys)

			rec := httptest.NewRecorder()
			rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", tc.cookie, "", ""))

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}

			if tc.key != "" {
				if len(store.keys) != before+1 || store.keys[before] != tc.key {
					t.Fatalf("keys = %v, want the last one to be %q", store.keys, tc.key)
				}
			} else if len(store.keys) != before {
				t.Fatalf("the store must not be called when the source is rejected: %v", store.keys)
			}
		})
	}
}

func TestRateLimitByNestedCookieField(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 5, "burst": 10, "period": "10s",
		"sourceCriterion": {"cookieName": "_sm_session", "cookieField": "user.id", "onEmptySource": "deny"},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	cases := []struct {
		name   string
		cookie string
		want   int
		key    string
	}{
		{
			name:   "nested field",
			cookie: `_sm_session=%7B%22user%22%3A%7B%22id%22%3A%22u1%22%7D%2C%22plan%22%3A%22gold%22%7D`,
			want:   http.StatusOK,
			key:    "rate:test:u1",
		},
		{name: "another user", cookie: `_sm_session=%7B%22user%22%3A%7B%22id%22%3A%22u2%22%7D%7D`, want: http.StatusOK, key: "rate:test:u2"},
		{name: "nested field missing", cookie: `_sm_session=%7B%22user%22%3A%7B%22name%22%3A%22bob%22%7D%7D`, want: http.StatusBadRequest},
		{name: "no cookie", cookie: "", want: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(store.keys)

			rec := httptest.NewRecorder()
			rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", tc.cookie, "", ""))

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.key != "" {
				if len(store.keys) != before+1 || store.keys[before] != tc.key {
					t.Fatalf("keys = %v, want the last one to be %q", store.keys, tc.key)
				}
			} else if len(store.keys) != before {
				t.Fatalf("the store must not be called: %v", store.keys)
			}
		})
	}
}

func TestOnEmptySourcePoliciesForHeaders(t *testing.T) {
	newMiddlewareWithPolicy := func(t *testing.T, policy string) (*ClusterRateLimit, *fakeStore) {
		t.Helper()

		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

		cfg := decode(t, fmt.Sprintf(`{
			"limit": 5, "burst": 10, "period": "10s",
			"sourceCriterion": {"requestHeaderName": "X-Api-Key", "onEmptySource": %q},
			"store": {"redis": {"endpoints": ["redis:6379"]}}
		}`, policy))

		rl, err := newTestMiddleware(t, cfg, store)
		if err != nil {
			t.Fatalf("newTestMiddleware: %v", err)
		}

		return rl, store
	}

	// shared (default): an unidentified client shares one bucket with all the
	// others.
	rl, store := newMiddlewareWithPolicy(t, "shared")
	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Api-Key", ""))
	if rec.Code != http.StatusOK || len(store.keys) != 1 || store.keys[0] != "rate:test:" {
		t.Fatalf("shared: status=%d keys=%v", rec.Code, store.keys)
	}

	// clientIP: the unidentified client is limited on its own IP.
	rl, store = newMiddlewareWithPolicy(t, "clientIP")
	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Api-Key", ""))
	if rec.Code != http.StatusOK || len(store.keys) != 1 || store.keys[0] != "rate:test:192.0.2.10" {
		t.Fatalf("clientIP: status=%d keys=%v", rec.Code, store.keys)
	}

	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("198.51.100.7:9999", "", "X-Api-Key", ""))
	if store.keys[1] != "rate:test:198.51.100.7" {
		t.Fatalf("clientIP: keys = %v, want the second client in its own bucket", store.keys)
	}

	// deny: the request is rejected before the store, so nobody can exhaust a
	// shared bucket.
	rl, store = newMiddlewareWithPolicy(t, "deny")
	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Api-Key", ""))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("deny: status = %d, want 400", rec.Code)
	}
	if len(store.keys) != 0 {
		t.Fatalf("deny: the store must not be called, keys=%v", store.keys)
	}

	// but a proper source still works with deny.
	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Api-Key", "tenant-a"))
	if rec.Code != http.StatusOK || store.keys[0] != "rate:test:tenant-a" {
		t.Fatalf("deny + header: status=%d keys=%v", rec.Code, store.keys)
	}
}

func TestSourceCriterionValidation(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "cookie and header are mutually exclusive",
			raw:     `{"limit": 1, "sourceCriterion": {"cookieName": "c", "requestHeaderName": "X-K"}, "store": {"redis": {"endpoints": ["redis:6379"]}}}`,
			wantErr: "mutually exclusive",
		},
		{
			name:    "invalid onEmptySource",
			raw:     `{"limit": 1, "sourceCriterion": {"requestHeaderName": "X-K", "onEmptySource": "explode"}, "store": {"redis": {"endpoints": ["redis:6379"]}}}`,
			wantErr: "invalid onEmptySource",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := decode(t, tc.raw)
			if _, err := newTestMiddleware(t, cfg, &fakeStore{}); err == nil ||
				!strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// onEmptySource=denyOnError: an unknown source is handled exactly like a store
// error, i.e. it follows the denyOnError flag.
func TestOnEmptySourceDenyOnError(t *testing.T) {
	newMiddleware := func(t *testing.T, denyOnError bool, cookie string) (*ClusterRateLimit, *fakeStore) {
		t.Helper()

		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

		cfg := decode(t, fmt.Sprintf(`{
			"limit": 3, "burst": 5, "period": "10s",
			"denyOnError": %t,
			"sourceCriterion": {"requestHeaderName": "X-Sm-User-Id", "onEmptySource": "denyOnError"},
			"store": {"redis": {"endpoints": ["redis:6379"]}}
		}`, denyOnError))

		rl, err := newTestMiddleware(t, cfg, store)
		if err != nil {
			t.Fatalf("newTestMiddleware: %v", err)
		}

		return rl, store
	}

	// denyOnError=true: 429, and the store is not queried.
	rl, store := newMiddleware(t, true, "")
	rec := httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Sm-User-Id", ""))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("denyOnError=true: status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Fatal("denyOnError=true: Retry-After must be set")
	}
	if len(store.keys) != 0 {
		t.Fatalf("denyOnError=true: the store must not be called, keys=%v", store.keys)
	}

	// denyOnError=false: the request reaches the backend, without rate limiting.
	rl, store = newMiddleware(t, false, "")
	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Sm-User-Id", ""))
	if rec.Code != http.StatusOK || rec.Body.String() != "backend" {
		t.Fatalf("denyOnError=false: status=%d body=%q, want 200 from the backend", rec.Code, rec.Body.String())
	}
	if len(store.keys) != 0 {
		t.Fatalf("denyOnError=false: the store must not be called, keys=%v", store.keys)
	}

	// both modes still rate limit a request that does carry the source.
	rl, store = newMiddleware(t, true, "")
	rec = httptest.NewRecorder()
	rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Sm-User-Id", "u123"))
	if rec.Code != http.StatusOK || store.keys[0] != "rate:test:u123" {
		t.Fatalf("with header: status=%d keys=%v", rec.Code, store.keys)
	}
}

// A missing cookie value and a missing header must be equivalent.
func TestOnEmptySourceDenyOnErrorWithCookie(t *testing.T) {
	store := &fakeStore{}
	store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})

	cfg := decode(t, `{
		"limit": 3, "burst": 5, "period": "10s",
		"denyOnError": true,
		"sourceCriterion": {"cookieName": "session", "cookieField": "userID", "onEmptySource": "denyOnError"},
		"store": {"redis": {"endpoints": ["redis:6379"]}}
	}`)

	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	cases := []struct {
		name   string
		cookie string
		want   int
	}{
		{name: "cookie without the field", cookie: `session=%7B%22tenant%22%3A%22a%22%7D`, want: http.StatusTooManyRequests},
		{name: "no cookie", cookie: "", want: http.StatusTooManyRequests},
		{name: "empty cookie", cookie: "session=", want: http.StatusTooManyRequests},
		{name: "cookie with the field", cookie: `session=%7B%22userID%22%3A%22u123%22%7D`, want: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", tc.cookie, "", ""))
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}
