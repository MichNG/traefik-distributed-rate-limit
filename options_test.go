package traefik_distributed_rate_limit

// TestOptionsCoverage walks every row of the README "Options" table (plus the
// legacy flat keys of the documented compatibility surface) and runs at least
// one check for each: the documented default, the accepted values, and/or the
// error a bad value produces. The row list is parsed from README.md, so a new
// documented option cannot go unnoticed - the test fails until a check is added.
//
//	go test -run TestOptionsCoverage -v ./...
//
// prints the matrix, one subtest per documented option. Behaviour is covered in
// depth by the dedicated tests; the cases below point at them when relevant.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MichNG/traefik-distributed-rate-limit/internal/redis"
)

// ---------------------------------------------------------------------------
// The README table
// ---------------------------------------------------------------------------

// readmeOptions returns the option names of the "### Options" table, in the
// order of the document, and the whole file.
func readmeOptions(t *testing.T) ([]string, string) {
	t.Helper()

	raw, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}

	text := string(raw)
	var rows []string
	inTable := false

	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)

		switch {
		case strings.HasPrefix(trimmed, "### Options"):
			inTable = true
		case inTable && strings.HasPrefix(trimmed, "###"):
			inTable = false
		case inTable && strings.HasPrefix(trimmed, "| `"):
			// The first cell may document two spellings ("`a` / `.b`"): the
			// first backticked token is the option, and both spellings are
			// checked by the same case.
			cell := strings.SplitN(trimmed, "|", 3)[1]
			if name := firstBackticked(cell); name != "" {
				rows = append(rows, name)
			}
		}
	}

	if len(rows) < 30 {
		t.Fatalf("found %d option rows in README.md, want the whole table (%v)", len(rows), rows)
	}

	return rows, text
}

func firstBackticked(s string) string {
	start := strings.Index(s, "`")
	if start < 0 {
		return ""
	}

	rest := s[start+1:]
	end := strings.Index(rest, "`")
	if end < 0 {
		return ""
	}

	return rest[:end]
}

// ---------------------------------------------------------------------------
// Helpers shared by the cases
// ---------------------------------------------------------------------------

// middlewareFor decodes a configuration, normalizes/validates it and builds the
// middleware, so that a case can exercise the documented behaviour.
func middlewareFor(t *testing.T, raw string, store *fakeStore) *ClusterRateLimit {
	t.Helper()

	cfg := decode(t, raw)
	rl, err := newTestMiddleware(t, cfg, store)
	if err != nil {
		t.Fatalf("newTestMiddleware: %v", err)
	}

	return rl
}

// normalized decodes and normalizes a configuration, failing the test on error.
func normalized(t *testing.T, raw string) *Config {
	t.Helper()

	cfg := decode(t, raw)
	if _, err := cfg.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}

	return cfg
}

func wantError(t *testing.T, raw, fragment string) {
	t.Helper()

	cfg := decode(t, raw)
	if _, err := cfg.normalize(); err != nil {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("normalize error = %v, want it to contain %q", err, fragment)
		}
		return
	}

	if err := cfg.validate(); err != nil {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("validate error = %v, want it to contain %q", err, fragment)
		}
		return
	}

	// A few options are checked while the middleware is built (the source
	// extractor, the whitelist checker, the TLS material of the store).
	if _, err := newTestMiddleware(t, cfg, &fakeStore{}); err != nil {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("creation error = %v, want it to contain %q", err, fragment)
		}
		return
	}

	t.Fatalf("no error at all for a configuration that must be refused (want %q)", fragment)
}

const storeStub = `"store": {"redis": {"endpoints": ["redis:6379"]}}`

// ---------------------------------------------------------------------------
// One case per documented option
// ---------------------------------------------------------------------------

var optionChecks = map[string]func(t *testing.T){
	"limit": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Limit != 0 {
			t.Fatalf("limit = %d, want the documented default 0 (no rate limiting)", cfg.Limit)
		}
		if cfg := normalized(t, `{"limit": 10, `+storeStub+`}`); cfg.Limit != 10 {
			t.Fatalf("limit = %d, want 10", cfg.Limit)
		}
		wantError(t, `{"limit": -1, `+storeStub+`}`, "limit must be >= 0")

		// limit: 0 disables rate limiting entirely (see
		// TestLimitZeroDisablesRateLimiting).
		store := &fakeStore{}
		rl := middlewareFor(t, `{"limit": 0, "burst": 1, "period": "1s", `+storeStub+`}`, store)
		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))
		if rec.Code != http.StatusOK || store.callCount() != 0 {
			t.Fatalf("limit=0: status=%d store calls=%d, want the request to pass untouched", rec.Code, store.callCount())
		}
	},

	"period": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Period != "1s" {
			t.Fatalf("period = %q, want the documented default 1s", cfg.Period)
		}
		// See TestPeriodVariants and TestPeriodErrors for the whole table.
		if cfg := normalized(t, `{"period": "1m", `+storeStub+`}`); cfg.Period != "1m0s" {
			t.Fatalf("period 1m normalized to %q, want 1m0s", cfg.Period)
		}
		wantError(t, `{"period": "0s", `+storeStub+`}`, "period must be greater than 0")
		wantError(t, `{"period": "abc", `+storeStub+`}`, "invalid period")
	},

	"burst": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Burst != 1 {
			t.Fatalf("burst = %d, want the documented default 1", cfg.Burst)
		}
		if cfg := normalized(t, `{"burst": 0, `+storeStub+`}`); cfg.Burst != 1 {
			t.Fatalf("burst 0 normalized to %d, want 1", cfg.Burst)
		}
		if cfg := normalized(t, `{"burst": 10, `+storeStub+`}`); cfg.Burst != 10 {
			t.Fatalf("burst = %d, want 10", cfg.Burst)
		}
	},

	"denyOnError": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); !cfg.denyOnError() {
			t.Fatal("denyOnError must default to true")
		}
		if cfg := normalized(t, `{"denyOnError": false, `+storeStub+`}`); cfg.denyOnError() {
			t.Fatal("denyOnError=false must be honoured")
		}
		if cfg := normalized(t, `{"denyOnError": true, `+storeStub+`}`); !cfg.denyOnError() {
			t.Fatal("denyOnError=true must be honoured")
		}
	},

	"responseHeaders": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.ResponseHeaders {
			t.Fatal("responseHeaders must default to false")
		}

		// Enabling it injects the four documented headers on an allowed request
		// (TestResponseHeaders and TestResponseHeadersOnDenialIncludeAllFour
		// cover the rest).
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4, ResetAfter: time.Second})
		rl := middlewareFor(t, `{"limit": 5, "burst": 10, "period": "10s", "responseHeaders": true, `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))
		for _, header := range []string{"X-RateLimit-Limit", "X-RateLimit-Period", "X-RateLimit-Remaining", "X-RateLimit-Reset"} {
			if rec.Header().Get(header) == "" {
				t.Errorf("responseHeaders=true: %s is missing", header)
			}
		}
	},

	"store.redis.endpoints": func(t *testing.T) {
		wantError(t, `{"limit": 1, "store": {"redis": {}}}`, "store.redis.endpoints must not be empty")

		cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379", "redis-2:6379"]}}}`)
		if len(cfg.Store.Redis.Endpoints) != 2 {
			t.Fatalf("endpoints = %v, want both", cfg.Store.Redis.Endpoints)
		}
		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis"]}}}`, "invalid Redis endpoint")
	},

	"store.redis.username": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Store.Redis.Username != "" {
			t.Fatalf("username = %q, want the documented default \"\"", cfg.Store.Redis.Username)
		}
		if cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "username": "app"}}}`); cfg.Store.Redis.Username != "app" {
			t.Fatalf("username = %q, want app", cfg.Store.Redis.Username)
		}
		// On the wire: TestAuthOnTheWire (AUTH <user> <pass> with a username,
		// AUTH <pass> without).
	},

	"store.redis.password": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Store.Redis.Password != "" {
			t.Fatalf("password = %q, want the documented default \"\"", cfg.Store.Redis.Password)
		}

		t.Setenv("OPTIONS_COVERAGE_PASSWORD", "from-env")
		if cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "password": "$OPTIONS_COVERAGE_PASSWORD"}}}`); cfg.Store.Redis.Password != "from-env" {
			t.Fatalf("password = %q, want the value of the environment variable", cfg.Store.Redis.Password)
		}

		cfg := decode(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "password": "$OPTIONS_COVERAGE_NOT_SET"}}}`)
		warnings, err := cfg.normalize()
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if !hasWarning(warnings, "OPTIONS_COVERAGE_NOT_SET") {
			t.Fatalf("warnings = %v, want one about the missing variable", warnings)
		}
	},

	"store.redis.database": func(t *testing.T) {
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Store.Redis.Database != 0 {
			t.Fatalf("database = %d, want the documented default 0", cfg.Store.Redis.Database)
		}
		if cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "database": 7}}}`); cfg.Store.Redis.Database != 7 {
			t.Fatalf("database = %d, want 7", cfg.Store.Redis.Database)
		}
		// On the wire: TestDatabaseIsSelectedOnTheWire (SELECT <index>).
	},

	"store.redis.timeout": func(t *testing.T) {
		cfg := normalized(t, "{"+storeStub+"}")
		if d, err := cfg.timeout(); err != nil || d != 2*time.Second {
			t.Fatalf("timeout = %v (%v), want the documented default 2s", d, err)
		}

		cfg = normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "timeout": "500ms"}}}`)
		if d, err := cfg.timeout(); err != nil || d != 500*time.Millisecond {
			t.Fatalf("timeout = %v (%v), want 500ms", d, err)
		}

		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "timeout": "0s"}}}`, "must be greater than 0")
		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "timeout": "abc"}}}`, "invalid store.redis.timeout")
		// Applied on the wire: TestCommandTimeoutIsApplied, TestDialTimeoutIsApplied.
	},

	"store.redis.poolSize": func(t *testing.T) {
		// 0 means "unset": the client then keeps maxActiveConns idle slots
		// (TestClientConfigDefaultsKeepIdleCapacity), which is what the README
		// documents as the default.
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Store.Redis.PoolSize != 0 {
			t.Fatalf("poolSize = %d, want 0 (unset: maxActiveConns)", cfg.Store.Redis.PoolSize)
		}
		if cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "poolSize": 12}}}`); cfg.Store.Redis.PoolSize != 12 {
			t.Fatalf("poolSize = %d, want 12", cfg.Store.Redis.PoolSize)
		}
		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "poolSize": -1}}}`, "store.redis.poolSize must be >= 0")
	},

	"store.redis.maxActiveConns": func(t *testing.T) {
		// 0 means "unset": the client defaults it to 10 * GOMAXPROCS
		// (TestClientConfigDefaultsKeepIdleCapacity).
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.Store.Redis.MaxActiveConns != 0 {
			t.Fatalf("maxActiveConns = %d, want 0 (unset: 10 * GOMAXPROCS)", cfg.Store.Redis.MaxActiveConns)
		}
		if cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "maxActiveConns": 40}}}`); cfg.Store.Redis.MaxActiveConns != 40 {
			t.Fatalf("maxActiveConns = %d, want 40", cfg.Store.Redis.MaxActiveConns)
		}
		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "maxActiveConns": -1}}}`, "store.redis.maxActiveConns must be >= 0")
	},

	"store.redis.sentinel.masterSet": func(t *testing.T) {
		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {}}}}`,
			"store.redis.sentinel.masterSet is required")

		cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster"}}}}`)
		if cfg.Store.Redis.Sentinel.MasterSet != "mymaster" {
			t.Fatalf("masterSet = %q, want mymaster", cfg.Store.Redis.Sentinel.MasterSet)
		}
	},

	"store.redis.sentinel.watch": func(t *testing.T) {
		cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster"}}}}`)
		if !cfg.sentinelWatch() {
			t.Fatal("sentinel.watch must default to true")
		}

		cfg = normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster", "watch": false}}}}`)
		if cfg.sentinelWatch() {
			t.Fatal("sentinel.watch=false must be honoured")
		}
	},

	"store.redis.sentinel.username": func(t *testing.T) {
		cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster"}}}}`)
		if cfg.Store.Redis.Sentinel.Username != "" {
			t.Fatalf("sentinel username = %q, want the documented default \"\"", cfg.Store.Redis.Sentinel.Username)
		}

		cfg = normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster", "username": "sentinel-user"}}}}`)
		if cfg.Store.Redis.Sentinel.Username != "sentinel-user" {
			t.Fatalf("sentinel username = %q, want sentinel-user", cfg.Store.Redis.Sentinel.Username)
		}
		// Authenticated on the wire: TestSentinelResolverSentinelAuth.
	},

	"store.redis.sentinel.password": func(t *testing.T) {
		cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster"}}}}`)
		if cfg.Store.Redis.Sentinel.Password != "" {
			t.Fatalf("sentinel password = %q, want the documented default \"\"", cfg.Store.Redis.Sentinel.Password)
		}

		// $ENV_VAR indirection and the warning for a variable that is not set
		// (TestSentinelPasswordFromEnvironment, and the missing-variable test).
		t.Setenv("OPTIONS_COVERAGE_SENTINEL_PASSWORD", "sentinel-from-env")
		cfg = normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster", "password": "$OPTIONS_COVERAGE_SENTINEL_PASSWORD"}}}}`)
		if cfg.Store.Redis.Sentinel.Password != "sentinel-from-env" {
			t.Fatalf("sentinel password = %q, want the value of the environment variable", cfg.Store.Redis.Sentinel.Password)
		}

		raw := decode(t, `{"limit": 1, "store": {"redis": {"endpoints": ["sentinel:26379"], "sentinel": {"masterSet": "mymaster", "password": "$OPTIONS_COVERAGE_SENTINEL_NOT_SET"}}}}`)
		warnings, err := raw.normalize()
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if !hasWarning(warnings, "store.redis.sentinel.password") {
			t.Fatalf("warnings = %v, want one about store.redis.sentinel.password", warnings)
		}
	},

	"store.redis.idleTimeout": func(t *testing.T) {
		cfg := normalized(t, "{"+storeStub+"}")
		if d, err := cfg.idleTimeout(); err != nil || d != 0 {
			t.Fatalf("idleTimeout = %v (%v), want the documented default 0 (disabled)", d, err)
		}

		cfg = normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "idleTimeout": "30s"}}}`)
		if d, err := cfg.idleTimeout(); err != nil || d != 30*time.Second {
			t.Fatalf("idleTimeout = %v (%v), want 30s", d, err)
		}

		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "idleTimeout": "-1s"}}}`, "must be >= 0")
		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "idleTimeout": "abc"}}}`, "invalid store.redis.idleTimeout")
	},

	"store.redis.tls.caBundle": func(t *testing.T) {
		// Inline PEM...
		cfg := normalized(t, fmt.Sprintf(`{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"caBundle": %q}}}}`, testCAPEM))
		if cfg.tlsConfig() == nil {
			t.Fatal("an inline caBundle must produce a TLS configuration")
		}
		if err := cfg.validate(); err != nil {
			t.Fatalf("validate with an inline CA: %v", err)
		}

		// ...or a file path.
		caPath := writeTestCA(t)
		cfg = normalized(t, fmt.Sprintf(`{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"caBundle": %q}}}}`, caPath))
		if err := cfg.validate(); err != nil {
			t.Fatalf("validate with a CA file: %v", err)
		}

		wantError(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"caBundle": "not-a-pem"}}}}`, "invalid store.redis.tls configuration")
	},

	"store.redis.tls.cert": func(t *testing.T) {
		certPath, keyPath := writeTestCertPair(t)

		cfg := normalized(t, fmt.Sprintf(`{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"cert": %q, "key": %q}}}}`, certPath, keyPath))
		if err := cfg.validate(); err != nil {
			t.Fatalf("validate with a client pair: %v", err)
		}

		// cert and key must be set together.
		wantError(t, fmt.Sprintf(`{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"cert": %q}}}}`, certPath),
			"both TLS cert and key must be defined")
	},

	"store.redis.tls.key": func(t *testing.T) {
		_, keyPath := writeTestCertPair(t)

		wantError(t, fmt.Sprintf(`{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"key": %q}}}}`, keyPath),
			"both TLS cert and key must be defined")

		cfg := normalized(t, fmt.Sprintf(`{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"cert": "x", "key": "y"}}}}`))
		if cfg.Store.Redis.TLS.Key != "y" {
			t.Fatalf("key = %q, want the configured value", cfg.Store.Redis.TLS.Key)
		}
	},

	"store.redis.tls.insecureSkipVerify": func(t *testing.T) {
		cfg := normalized(t, "{"+storeStub+"}")
		if cfg.tlsConfig() != nil {
			t.Fatal("no TLS material must mean no TLS configuration")
		}

		cfg = normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "tls": {"insecureSkipVerify": true}}}}`)
		if cfg.tlsConfig() == nil || !cfg.tlsConfig().InsecureSkipVerify {
			t.Fatal("insecureSkipVerify=true must produce a TLS configuration that skips the verification")
		}
		// Used end to end by TestSentinelConnectionsCarryTheConfiguredTLS.
	},

	"sourceCriterion.requestHost": func(t *testing.T) {
		cfg := normalized(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"requestHost": true}, `+storeStub+`}`)
		if !cfg.SourceCriterion.RequestHost {
			t.Fatal("requestHost=true must be honoured")
		}

		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"requestHost": true}, `+storeStub+`}`, store)

		req := httptest.NewRequest(http.MethodGet, "http://shop.example.com/", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, req)

		if len(store.keys) != 1 || store.keys[0] != "rate:test:shop.example.com" {
			t.Fatalf("keys = %v, want the request host as the source", store.keys)
		}
	},

	"sourceCriterion.requestHeaderName": func(t *testing.T) {
		cfg := normalized(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"requestHeaderName": "X-Api-Key"}, `+storeStub+`}`)
		if cfg.SourceCriterion.RequestHeaderName != "X-Api-Key" {
			t.Fatalf("requestHeaderName = %q, want X-Api-Key", cfg.SourceCriterion.RequestHeaderName)
		}

		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"requestHeaderName": "X-Api-Key"}, `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Api-Key", "key-a"))

		if len(store.keys) != 1 || store.keys[0] != "rate:test:key-a" {
			t.Fatalf("keys = %v, want the header value as the source", store.keys)
		}
	},

	"sourceCriterion.cookieName": func(t *testing.T) {
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"cookieName": "session"}, `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "session=tenant-a", "", ""))

		if len(store.keys) != 1 || store.keys[0] != "rate:test:tenant-a" {
			t.Fatalf("keys = %v, want the cookie value as the source", store.keys)
		}
		// A name that can never match is refused at creation.
		wantError(t, `{"limit": 1, "sourceCriterion": {"cookieName": "a b"}, `+storeStub+`}`, "invalid cookieName")
	},

	"sourceCriterion.cookieField": func(t *testing.T) {
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"cookieName": "session", "cookieField": "users[0].id"}, `+storeStub+`}`, store)

		// {"users":[{"id":"u1"}]} percent-encoded, as a client sends it.
		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", `session=%7B%22users%22%3A%5B%7B%22id%22%3A%22u1%22%7D%5D%7D`, "", ""))

		if len(store.keys) != 1 || store.keys[0] != "rate:test:u1" {
			t.Fatalf("keys = %v, want the array element as the source", store.keys)
		}
		// Scalars and dot paths too (TestRateLimitByCookieField and friends).
		wantError(t, `{"limit": 1, "sourceCriterion": {"cookieField": "tenant"}, `+storeStub+`}`, "cookieField requires cookieName")
	},

	"sourceCriterion.jwtClaim": func(t *testing.T) {
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"cookieName": "session", "jwtClaim": "sub"}, `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "session="+testJWT(`{"sub":"u1"}`), "", ""))

		if len(store.keys) != 1 || store.keys[0] != "rate:test:u1" {
			t.Fatalf("keys = %v, want the claim as the source", store.keys)
		}
		// The signature is not verified: a forged token still yields its claim
		// (the README documents the security consequence).
		rec = httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "session="+testJWT(`{"sub":"forged"}`), "", ""))
		if len(store.keys) != 2 || store.keys[1] != "rate:test:forged" {
			t.Fatalf("keys = %v, want the claim of an unsigned token", store.keys)
		}

		wantError(t, `{"limit": 1, "sourceCriterion": {"jwtClaim": "sub"}, `+storeStub+`}`, "jwtClaim requires cookieName or requestHeaderName")
	},

	"sourceCriterion.onEmptySource": func(t *testing.T) {
		for _, policy := range []string{"shared", "clientIP", "deny", "denyOnError"} {
			cfg := normalized(t, fmt.Sprintf(`{"limit": 1, "sourceCriterion": {"requestHeaderName": "X-K", "onEmptySource": %q}, %s}`, policy, storeStub))
			if cfg.SourceCriterion.OnEmptySource != policy {
				t.Fatalf("onEmptySource = %q, want %q", cfg.SourceCriterion.OnEmptySource, policy)
			}
		}

		wantError(t, `{"limit": 1, "sourceCriterion": {"requestHeaderName": "X-K", "onEmptySource": "explode"}, `+storeStub+`}`, "invalid onEmptySource")

		// The default is shared (TestOnEmptySourceSharedIsTheDefault covers the
		// behaviour of every value).
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"requestHeaderName": "X-K"}, `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-K", ""))
		if len(store.keys) != 1 || store.keys[0] != "rate:test:" {
			t.Fatalf("keys = %v, want the shared bucket by default", store.keys)
		}
	},

	"sourceCriterion.ipStrategy.depth": func(t *testing.T) {
		// Default 0: the remote address.
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"ipStrategy": {}}, `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Forwarded-For", "203.0.113.7"))
		if len(store.keys) != 1 || store.keys[0] != "rate:test:192.0.2.10" {
			t.Fatalf("keys = %v, want the remote address with depth 0", store.keys)
		}

		// Depth 1: the last X-Forwarded-For entry.
		store = &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl = middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"ipStrategy": {"depth": 1}}, `+storeStub+`}`, store)

		rec = httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("192.0.2.10:1234", "", "X-Forwarded-For", "203.0.113.7, 10.0.0.1"))
		if len(store.keys) != 1 || store.keys[0] != "rate:test:10.0.0.1" {
			t.Fatalf("keys = %v, want the depth-1 address", store.keys)
		}
	},

	"sourceCriterion.ipStrategy.excludedIPs": func(t *testing.T) {
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "sourceCriterion": {"ipStrategy": {"excludedIPs": ["10.0.0.0/8"]}}, `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, cookieRequest("10.0.0.1:1234", "", "X-Forwarded-For", "10.0.0.1, 203.0.113.7"))
		if len(store.keys) != 1 || store.keys[0] != "rate:test:203.0.113.7" {
			t.Fatalf("keys = %v, want the first address that is not excluded", store.keys)
		}
		// TestExcludedIPsPickTheFirstUnlistedIP and
		// TestExcludedIPsAsClientIPFallback cover the rest.
	},

	"whitelistIPs": func(t *testing.T) {
		store := &fakeStore{}
		store.setOutput(redis.ScriptOutput{Allowed: 1, Remaining: 4})
		rl := middlewareFor(t, `{"limit": 5, "burst": 5, "period": "10s", "whitelistIPs": ["192.0.2.0/24", "198.51.100.10"], `+storeStub+`}`, store)

		rec := httptest.NewRecorder()
		rl.ServeHTTP(rec, newRequest("192.0.2.10:1234"))
		if rec.Code != http.StatusOK || store.callCount() != 0 {
			t.Fatalf("a whitelisted address must bypass the limit, status=%d calls=%d", rec.Code, store.callCount())
		}

		rec = httptest.NewRecorder()
		rl.ServeHTTP(rec, newRequest("203.0.113.5:1234"))
		if store.callCount() != 1 {
			t.Fatalf("an address outside the whitelist must be rate limited, calls=%d", store.callCount())
		}

		wantError(t, `{"limit": 1, "whitelistIPs": ["nonsense"], `+storeStub+`}`, "invalid")
	},

	"store.redis.breaker.threshold": func(t *testing.T) {
		// The row documents two options: threshold and reattempt.
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.breakerConfig().Threshold != 3 {
			t.Fatalf("breaker threshold = %d, want the documented default 3", cfg.breakerConfig().Threshold)
		}
		if cfg := normalized(t, "{"+storeStub+"}"); cfg.breakerConfig().Reattempt != 15*time.Second {
			t.Fatalf("breaker reattempt = %v, want the documented default 15s", cfg.breakerConfig().Reattempt)
		}

		cfg := normalized(t, `{"limit": 1, "store": {"redis": {"endpoints": ["redis:6379"], "breaker": {"threshold": 7, "reattempt": 30}}}}`)
		if got := cfg.breakerConfig(); got.Threshold != 7 || got.Reattempt != 30*time.Second {
			t.Fatalf("breaker = %d/%v, want 7/30s", got.Threshold, got.Reattempt)
		}
		// The behaviour of the breaker is covered by
		// TestCircuitBreakerDeniesAfterThreshold and TestBreakerOpensAndCloses.
	},
}

// legacyOptions are the flat v1 keys the README documents as still accepted.
var legacyOptions = []struct {
	key     string
	raw     string
	check   func(t *testing.T, cfg *Config)
	warning string
}{
	{
		key:     "average",
		raw:     `{"average": 5, "period": "1s", ` + storeStub + `}`,
		warning: "average",
		check: func(t *testing.T, cfg *Config) {
			if cfg.Limit != 5 {
				t.Fatalf("limit = %d, want 5 from average", cfg.Limit)
			}
		},
	},
	{
		key:     "redisAddress",
		raw:     `{"limit": 1, "redisAddress": "redis:6379"}`,
		warning: "redisAddress",
		check: func(t *testing.T, cfg *Config) {
			if len(cfg.Store.Redis.Endpoints) != 1 || cfg.Store.Redis.Endpoints[0] != "redis:6379" {
				t.Fatalf("endpoints = %v, want [redis:6379]", cfg.Store.Redis.Endpoints)
			}
		},
	},
	{
		key:     "redisDb",
		raw:     `{"limit": 1, "redisAddress": "redis:6379", "redisDb": 3}`,
		warning: "redisDb",
		check: func(t *testing.T, cfg *Config) {
			if cfg.Store.Redis.Database != 3 {
				t.Fatalf("database = %d, want 3", cfg.Store.Redis.Database)
			}
		},
	},
	{
		key:     "redisPassword",
		raw:     `{"limit": 1, "redisAddress": "redis:6379", "redisPassword": "legacy"}`,
		warning: "redisPassword",
		check: func(t *testing.T, cfg *Config) {
			if cfg.Store.Redis.Password != "legacy" {
				t.Fatalf("password = %q, want legacy", cfg.Store.Redis.Password)
			}
		},
	},
	{
		key:     "redisConnectionTimeout",
		raw:     `{"limit": 1, "redisAddress": "redis:6379", "redisConnectionTimeout": 4}`,
		warning: "redisConnectionTimeout",
		check: func(t *testing.T, cfg *Config) {
			if d, err := cfg.timeout(); err != nil || d != 4*time.Second {
				t.Fatalf("timeout = %v (%v), want 4s", d, err)
			}
		},
	},
	{
		key:     "breakerThreshold",
		raw:     `{"limit": 1, "redisAddress": "redis:6379", "breakerThreshold": 9}`,
		warning: "breakerThreshold",
		check: func(t *testing.T, cfg *Config) {
			if cfg.breakerConfig().Threshold != 9 {
				t.Fatalf("threshold = %d, want 9", cfg.breakerConfig().Threshold)
			}
		},
	},
	{
		key:     "breakerReattempt",
		raw:     `{"limit": 1, "redisAddress": "redis:6379", "breakerReattempt": 45}`,
		warning: "breakerReattempt",
		check: func(t *testing.T, cfg *Config) {
			if cfg.breakerConfig().Reattempt != 45*time.Second {
				t.Fatalf("reattempt = %v, want 45s", cfg.breakerConfig().Reattempt)
			}
		},
	},
}

func hasWarning(warnings []string, fragment string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, fragment) {
			return true
		}
	}

	return false
}

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

func TestOptionsCoverage(t *testing.T) {
	rows, readme := readmeOptions(t)

	covered := make(map[string]bool, len(rows))

	for _, row := range rows {
		check, ok := optionChecks[row]
		if !ok {
			t.Errorf("README option %q has no check: add a case to optionChecks", row)
			continue
		}

		covered[row] = true

		t.Run(row, func(t *testing.T) {
			check(t)
			t.Logf("OK: %s (documented default, accepted values and errors)", row)
		})
	}

	// The legacy flat keys belong to the documented compatibility surface, even
	// though they live outside the Options table.
	for _, legacy := range legacyOptions {
		if !strings.Contains(readme, "`"+legacy.key+"`") {
			t.Errorf("README does not document the legacy key %q any more: drop its check", legacy.key)
		}

		t.Run("legacy "+legacy.key, func(t *testing.T) {
			cfg := decode(t, legacy.raw)

			warnings, err := cfg.normalize()
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if !hasWarning(warnings, legacy.warning) {
				t.Fatalf("warnings = %v, want a deprecation warning about %q", warnings, legacy.warning)
			}
			if err := cfg.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}

			legacy.check(t, cfg)
			t.Logf("OK: %s (accepted, mapped onto the schema, with a deprecation warning)", legacy.key)
		})
	}

	// A check without a documented row would silently test nothing: an option
	// removed from the README must be removed here too.
	for name := range optionChecks {
		if !covered[name] {
			t.Errorf("optionChecks covers %q, which is not a row of the README Options table", name)
		}
	}

	t.Logf("checked %d documented options (+%d legacy keys)", len(rows), len(legacyOptions))
}
