package utils

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJSONStringField(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		field   string
		want    string
		ok      bool
	}{
		{name: "top level string", payload: `{"tenant":"acme"}`, field: "tenant", want: "acme", ok: true},
		{name: "extra fields", payload: `{"plan":"gold","tenant":"acme"}`, field: "tenant", want: "acme", ok: true},
		{name: "empty value", payload: `{"tenant":""}`, field: "tenant", want: "", ok: true},
		{name: "missing field", payload: `{"plan":"gold"}`, field: "tenant"},
		{name: "not a string", payload: `{"tenant":42}`, field: "tenant"},
		{name: "nested object", payload: `{"tenant":{"name":"acme"}}`, field: "tenant"},
		{name: "not json", payload: `not-json`, field: "tenant"},
		{name: "json array", payload: `["acme"]`, field: "tenant"},
		{name: "nested field", payload: `{"user":{"id":"u1","plan":"gold"}}`, field: "user.id", want: "u1", ok: true},
		{name: "deeply nested field", payload: `{"a":{"b":{"c":"v"}}}`, field: "a.b.c", want: "v", ok: true},
		{name: "nested intermediate missing", payload: `{"user":{"id":"u1"}}`, field: "account.id"},
		{name: "intermediate is not an object", payload: `{"user":"u1"}`, field: "user.id"},
		{name: "empty path element", payload: `{"user":{"id":"u1"}}`, field: "user..id"},
		{name: "array index path", payload: `{"users":[{"id":"u1"}]}`, field: "users.0.id"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := jsonStringField(tc.payload, tc.field)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("jsonStringField(%q,%q) = (%q,%v), want (%q,%v)", tc.payload, tc.field, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestValidateCookieName(t *testing.T) {
	valid := []string{"_sm_user_id", "session", "SESSION_ID", "a.b", "id-1"}
	for _, name := range valid {
		if err := validateCookieName(name); err != nil {
			t.Errorf("validateCookieName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{"a b", "a;b", "a,b", "a=b", "a\tb", "a\nb"}
	for _, name := range invalid {
		if err := validateCookieName(name); err == nil {
			t.Errorf("validateCookieName(%q) = nil, want an error", name)
		}
	}

	// A configuration typo must fail at creation time.
	if _, err := GetSourceExtractor(&SourceCriterion{CookieName: "a b"}); err == nil {
		t.Error("expected an error for a cookie name that can never match")
	}
	if _, err := GetSourceExtractor(&SourceCriterion{CookieField: "tenant"}); err == nil {
		t.Error("expected an error for cookieField without cookieName")
	}
	if _, err := GetSourceExtractor(&SourceCriterion{CookieName: "session", CookieField: "user.id"}); err != nil {
		t.Fatalf("GetSourceExtractor with a nested field: %v", err)
	}
}

func TestCookieExtractorNestedField(t *testing.T) {
	extractor := makeCookieExtractor("session", "user.id")

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	// {"user":{"id":"u1","plan":"gold"}} percent-encoded
	req.Header.Set("Cookie", "session=%7B%22user%22%3A%7B%22id%22%3A%22u1%22%2C%22plan%22%3A%22gold%22%7D%7D")

	token, _, err := extractor.Extract(req)
	if err != nil || token != "u1" {
		t.Fatalf("Extract = (%q,%v), want u1", token, err)
	}
}

func TestCookieExtractor(t *testing.T) {
	extractor := makeCookieExtractor("session", "tenant")

	request := func(cookie string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		if cookie != "" {
			req.Header.Set("Cookie", cookie)
		}
		return req
	}

	cases := []struct {
		name   string
		cookie string
		want   string
	}{
		{name: "encoded json", cookie: `session=%7B%22tenant%22%3A%22acme%22%7D`, want: "acme"},
		{name: "raw cookie without field", cookie: `session=acme`, want: ""},
		{name: "missing cookie", cookie: `other=1`, want: ""},
		{name: "empty json value", cookie: `session=%7B%22tenant%22%3A%22%22%7D`, want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, amount, err := extractor.Extract(request(tc.cookie))
			if err != nil || amount != 1 || got != tc.want {
				t.Fatalf("Extract = (%q,%d,%v), want (%q,1,nil)", got, amount, err, tc.want)
			}
		})
	}
}

func TestGetSourceExtractorPolicies(t *testing.T) {
	request := func(header, value, remoteAddr string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		req.RemoteAddr = remoteAddr
		if value != "" {
			req.Header.Set(header, value)
		}
		return req
	}

	// shared (default)
	extractor, err := GetSourceExtractor(&SourceCriterion{RequestHeaderName: "X-K"})
	if err != nil {
		t.Fatalf("GetSourceExtractor: %v", err)
	}
	if token, _, err := extractor.Extract(request("X-K", "", "192.0.2.1:1")); token != "" || err != nil {
		t.Fatalf("shared: token=%q err=%v, want the shared (empty) source", token, err)
	}

	// clientIP
	extractor, err = GetSourceExtractor(&SourceCriterion{RequestHeaderName: "X-K", OnEmptySource: OnEmptySourceClientIP})
	if err != nil {
		t.Fatalf("GetSourceExtractor: %v", err)
	}
	if token, _, err := extractor.Extract(request("X-K", "", "192.0.2.1:1")); token != "192.0.2.1" || err != nil {
		t.Fatalf("clientIP: token=%q err=%v, want 192.0.2.1", token, err)
	}

	// deny
	extractor, err = GetSourceExtractor(&SourceCriterion{CookieName: "session", OnEmptySource: OnEmptySourceDeny})
	if err != nil {
		t.Fatalf("GetSourceExtractor: %v", err)
	}
	if _, _, err := extractor.Extract(request("", "", "192.0.2.1:1")); !errors.Is(err, ErrEmptySource) {
		t.Fatalf("deny: err = %v, want ErrEmptySource", err)
	}

	// denyOnError: the extractor reports the empty source, the middleware decides
	// (429 or pass-through) from the denyOnError flag.
	extractor, err = GetSourceExtractor(&SourceCriterion{RequestHeaderName: "X-K", OnEmptySource: OnEmptySourceDenyOnError})
	if err != nil {
		t.Fatalf("GetSourceExtractor: %v", err)
	}
	if _, _, err := extractor.Extract(request("X-K", "", "192.0.2.1:1")); !errors.Is(err, ErrEmptySource) {
		t.Fatalf("denyOnError: err = %v, want ErrEmptySource", err)
	}

	// invalid policy
	if _, err := GetSourceExtractor(&SourceCriterion{RequestHeaderName: "X-K", OnEmptySource: "explode"}); err == nil {
		t.Fatal("expected an error for an unknown onEmptySource")
	}

	// two criteria
	if _, err := GetSourceExtractor(&SourceCriterion{RequestHeaderName: "X-K", CookieName: "c"}); err == nil {
		t.Fatal("expected an error when two criteria are set")
	}
}

func TestIPStrategyAsFallback(t *testing.T) {
	// ipStrategy + another criterion is an error, unless it defines the
	// fallback of onEmptySource=clientIP.
	if _, err := GetSourceExtractor(&SourceCriterion{RequestHeaderName: "X-K", IPStrategy: &IPStrategy{Depth: 1}}); err == nil {
		t.Fatal("expected an error for ipStrategy combined with another criterion")
	}

	// Depth counts from the right of X-Forwarded-For: depth 2 is the client
	// when one proxy appended its own address.
	extractor, err := GetSourceExtractor(&SourceCriterion{
		RequestHeaderName: "X-K",
		IPStrategy:        &IPStrategy{Depth: 2},
		OnEmptySource:     OnEmptySourceClientIP,
	})
	if err != nil {
		t.Fatalf("GetSourceExtractor: %v", err)
	}

	// No header: the source comes from the X-Forwarded-For depth.
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")

	token, _, err := extractor.Extract(req)
	if err != nil || token != "203.0.113.7" {
		t.Fatalf("Extract = (%q,%v), want 203.0.113.7 from the X-Forwarded-For depth", token, err)
	}

	// With the header, the header wins.
	req.Header.Set("X-K", "tenant-a")
	if token, _, _ := extractor.Extract(req); token != "tenant-a" {
		t.Fatalf("token = %q, want tenant-a", token)
	}
}
