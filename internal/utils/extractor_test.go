package utils

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestJSONField(t *testing.T) {
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
		{name: "number", payload: `{"tenant":42}`, field: "tenant", want: "42", ok: true},
		{name: "big number keeps its text", payload: `{"id":12345678901234567890}`, field: "id", want: "12345678901234567890", ok: true},
		{name: "float", payload: `{"ratio":0.5}`, field: "ratio", want: "0.5", ok: true},
		{name: "boolean", payload: `{"admin":true}`, field: "admin", want: "true", ok: true},
		{name: "false boolean", payload: `{"admin":false}`, field: "admin", want: "false", ok: true},
		{name: "null", payload: `{"tenant":null}`, field: "tenant"},
		{name: "nested object", payload: `{"tenant":{"name":"acme"}}`, field: "tenant"},
		{name: "not json", payload: `not-json`, field: "tenant"},
		{name: "trailing garbage", payload: `{"tenant":"acme"} oops`, field: "tenant"},
		{name: "json array", payload: `["acme"]`, field: "tenant"},
		{name: "nested field", payload: `{"user":{"id":"u1","plan":"gold"}}`, field: "user.id", want: "u1", ok: true},
		{name: "deeply nested field", payload: `{"a":{"b":{"c":"v"}}}`, field: "a.b.c", want: "v", ok: true},
		{name: "nested intermediate missing", payload: `{"user":{"id":"u1"}}`, field: "account.id"},
		{name: "intermediate is not an object", payload: `{"user":"u1"}`, field: "user.id"},
		{name: "empty path element", payload: `{"user":{"id":"u1"}}`, field: "user..id"},
		{name: "array index path", payload: `{"users":[{"id":"u1"}]}`, field: "users.0.id", want: "u1", ok: true},
		{name: "bracket index", payload: `{"users":[{"id":"u1"},{"id":"u2"}]}`, field: "users[1].id", want: "u2", ok: true},
		{name: "array of scalars", payload: `{"roles":["admin","user"]}`, field: "roles.1", want: "user", ok: true},
		{name: "array of scalars by bracket", payload: `{"roles":["admin","user"]}`, field: "roles[0]", want: "admin", ok: true},
		{name: "nested arrays", payload: `{"m":[[1,2],[3,4]]}`, field: "m[1][0]", want: "3", ok: true},
		{name: "array of numbers", payload: `{"ids":[7,8]}`, field: "ids.1", want: "8", ok: true},
		{name: "numeric key of an object", payload: `{"0":"zero"}`, field: "0", want: "zero", ok: true},
		{name: "index out of range", payload: `{"roles":["admin"]}`, field: "roles.5"},
		{name: "index into a scalar", payload: `{"roles":"admin"}`, field: "roles.0"},
		{name: "not a number index", payload: `{"roles":["admin"]}`, field: "roles.first"},
		{name: "missing array", payload: `{"roles":["admin"]}`, field: "groups.0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := jsonField(tc.payload, tc.field)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("jsonField(%q,%q) = (%q,%v), want (%q,%v)", tc.payload, tc.field, got, ok, tc.want, tc.ok)
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
	if _, err := GetSourceExtractor(&SourceCriterion{CookieName: "session", CookieField: "users[0].id"}); err != nil {
		t.Fatalf("GetSourceExtractor with an array index: %v", err)
	}
}

func TestValidateFieldPath(t *testing.T) {
	valid := []string{"tenant", "user.id", "a.b.c", "users.0.id", "users[0].id", "m[0][1]", "roles[10]"}
	for _, field := range valid {
		if err := validateFieldPath("cookieField", field); err != nil {
			t.Errorf("validateFieldPath(%q) = %v, want nil", field, err)
		}
	}

	invalid := []string{"", "user..id", ".user", "user.", "items[]", "items[x]", "items[0", "items]", "a b", "a\tb"}
	for _, field := range invalid {
		if err := validateFieldPath("cookieField", field); err == nil {
			t.Errorf("validateFieldPath(%q) = nil, want an error", field)
		}
	}

	// Both cookieField and jwtClaim go through this validation.
	if _, err := GetSourceExtractor(&SourceCriterion{CookieName: "session", CookieField: "items[x]"}); err == nil {
		t.Error("expected an error for a non-numeric array index")
	}
	if _, err := GetSourceExtractor(&SourceCriterion{CookieName: "session", JWTClaim: "sub"}); err != nil {
		t.Fatalf("GetSourceExtractor with jwtClaim and cookieName: %v", err)
	}
	if _, err := GetSourceExtractor(&SourceCriterion{RequestHeaderName: "Authorization", JWTClaim: "sub"}); err != nil {
		t.Fatalf("GetSourceExtractor with jwtClaim and requestHeaderName: %v", err)
	}
	if _, err := GetSourceExtractor(&SourceCriterion{JWTClaim: "sub"}); err == nil {
		t.Error("expected an error for jwtClaim without a source to read the token from")
	}
	if _, err := GetSourceExtractor(&SourceCriterion{RequestHost: true, JWTClaim: "sub"}); err == nil {
		t.Error("expected an error for jwtClaim with requestHost")
	}
}

// testJWT builds an unsigned JWS compact token with the given payload.
func testJWT(payload string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	body := base64.RawURLEncoding.EncodeToString([]byte(payload))

	return header + "." + body + ".c2lnbmF0dXJl"
}

func TestCookieExtractorNestedField(t *testing.T) {
	extractor := makeCookieExtractor("session", "user.id", "")

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	// {"user":{"id":"u1","plan":"gold"}} percent-encoded
	req.Header.Set("Cookie", "session=%7B%22user%22%3A%7B%22id%22%3A%22u1%22%2C%22plan%22%3A%22gold%22%7D%7D")

	token, _, err := extractor.Extract(req)
	if err != nil || token != "u1" {
		t.Fatalf("Extract = (%q,%v), want u1", token, err)
	}
}

func TestCookieExtractor(t *testing.T) {
	extractor := makeCookieExtractor("session", "tenant", "")

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
		{name: "numeric field", cookie: `session=%7B%22tenant%22%3A42%7D`, want: "42"},
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

func TestCookieExtractorArrayField(t *testing.T) {
	request := func(cookie string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		req.Header.Set("Cookie", cookie)

		return req
	}

	// {"users":[{"id":"u1"},{"id":"u2"}]} percent-encoded, with both path forms.
	payload := `session=%7B%22users%22%3A%5B%7B%22id%22%3A%22u1%22%7D%2C%7B%22id%22%3A%22u2%22%7D%5D%7D`

	for _, field := range []string{"users.1.id", "users[1].id"} {
		extractor := makeCookieExtractor("session", field, "")

		token, _, err := extractor.Extract(request(payload))
		if err != nil || token != "u2" {
			t.Fatalf("cookieField %q: Extract = (%q,%v), want u2", field, token, err)
		}
	}

	// The index may be out of range: the empty-source policy applies.
	extractor := makeCookieExtractor("session", "users[9].id", "")
	if token, _, err := extractor.Extract(request(payload)); err != nil || token != "" {
		t.Fatalf("out of range index: Extract = (%q,%v), want an empty source", token, err)
	}
}

func TestCookieExtractorJWTClaim(t *testing.T) {
	token := testJWT(`{"sub":"u1","uid":7,"user":{"id":"u2"}}`)

	cases := []struct {
		name       string
		cookieName string
		cookie     string
		field      string
		claim      string
		want       string
	}{
		{
			name:       "claim of a jwt cookie",
			cookieName: "session",
			cookie:     "session=" + token,
			claim:      "sub",
			want:       "u1",
		},
		{
			name:       "numeric claim",
			cookieName: "session",
			cookie:     "session=" + token,
			claim:      "uid",
			want:       "7",
		},
		{
			name:       "nested claim",
			cookieName: "session",
			cookie:     "session=" + token,
			claim:      "user.id",
			want:       "u2",
		},
		{
			name:       "jwt inside a json cookie field (percent-encoded)",
			cookieName: "session",
			cookie:     "session=" + url.QueryEscape(`{"token":"`+token+`"}`),
			field:      "token",
			claim:      "sub",
			want:       "u1",
		},
		{
			name:       "missing claim",
			cookieName: "session",
			cookie:     "session=" + token,
			claim:      "tenant",
		},
		{
			name:       "not a jwt",
			cookieName: "session",
			cookie:     "session=acme",
			claim:      "sub",
		},
		{
			name:       "truncated jwt",
			cookieName: "session",
			cookie:     "session=eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1MSJ9",
			claim:      "sub",
		},
		{
			name:       "missing cookie",
			cookieName: "session",
			cookie:     "other=1",
			claim:      "sub",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			extractor := makeCookieExtractor(tc.cookieName, tc.field, tc.claim)

			req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
			req.Header.Set("Cookie", tc.cookie)

			got, _, err := extractor.Extract(req)
			if err != nil || got != tc.want {
				t.Fatalf("Extract = (%q,%v), want %q", got, err, tc.want)
			}
		})
	}
}

func TestCookieExtractorJWTClaimFromPaddedPayload(t *testing.T) {
	// Some producers pad the base64url payload: it must still be decoded.
	padded := base64.URLEncoding.EncodeToString([]byte(`{"sub":"u1"}`))
	token := "eyJhbGciOiJub25lIn0." + padded + ".c2lnbmF0dXJl"

	extractor := makeCookieExtractor("session", "", "sub")

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.Header.Set("Cookie", "session="+token)

	if token, _, err := extractor.Extract(req); err != nil || token != "u1" {
		t.Fatalf("Extract = (%q,%v), want u1 from a padded payload", token, err)
	}
}

func TestHeaderExtractorJWTClaim(t *testing.T) {
	token := testJWT(`{"sub":"u1"}`)

	request := func(value string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
		if value != "" {
			req.Header.Set("Authorization", value)
		}

		return req
	}

	for _, value := range []string{"Bearer " + token, "bearer " + token, token} {
		extractor := makeHeaderExtractor("Authorization", "sub")

		got, _, err := extractor.Extract(request(value))
		if err != nil || got != "u1" {
			t.Fatalf("Extract(%q) = (%q,%v), want u1", value, got, err)
		}
	}

	// Without a value, and without a JWT, the source stays empty.
	extractor := makeHeaderExtractor("Authorization", "sub")
	if got, _, _ := extractor.Extract(request("")); got != "" {
		t.Fatalf("empty header: token = %q, want an empty source", got)
	}
	if got, _, _ := extractor.Extract(request("Bearer not-a-jwt")); got != "" {
		t.Fatalf("not a jwt: token = %q, want an empty source", got)
	}

	// A plain header extractor is unaffected by the JWT support.
	plain := makeHeaderExtractor("X-K", "")
	if got, _, _ := plain.Extract(request("")); got != "" {
		t.Fatalf("plain header: token = %q, want an empty source", got)
	}
}

func TestGetSourceExtractorJWTEmptyPolicy(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.RemoteAddr = "192.0.2.1:1"
	req.Header.Set("Cookie", "session=acme")

	extractor, err := GetSourceExtractor(&SourceCriterion{
		CookieName:    "session",
		JWTClaim:      "sub",
		OnEmptySource: OnEmptySourceDeny,
	})
	if err != nil {
		t.Fatalf("GetSourceExtractor: %v", err)
	}

	if _, _, err := extractor.Extract(req); !errors.Is(err, ErrEmptySource) {
		t.Fatalf("err = %v, want ErrEmptySource for a cookie that is not a JWT", err)
	}

	// With clientIP, the fallback source keeps limiting the client.
	extractor, err = GetSourceExtractor(&SourceCriterion{
		CookieName:    "session",
		JWTClaim:      "sub",
		OnEmptySource: OnEmptySourceClientIP,
	})
	if err != nil {
		t.Fatalf("GetSourceExtractor: %v", err)
	}

	if token, _, err := extractor.Extract(req); err != nil || token != "192.0.2.1" {
		t.Fatalf("Extract = (%q,%v), want the client IP", token, err)
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
