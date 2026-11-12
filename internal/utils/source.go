package utils

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

// SourceExtractor extracts the source from the request, e.g. that may be client ip, or particular header that
// identifies the source. amount stands for amount of connections the source consumes, usually 1 for connection limiters
// error should be returned when source can not be identified.
type SourceExtractor interface {
	Extract(req *http.Request) (token string, amount int64, err error)
}

// ExtractorFunc extractor function type.
type ExtractorFunc func(req *http.Request) (token string, amount int64, err error)

// Extract extract from request.
func (f ExtractorFunc) Extract(req *http.Request) (string, int64, error) {
	return f(req)
}

// ExtractSource extract source function type.
type ExtractSource func(req *http.Request)

// makeCookieExtractor extracts the source from a cookie, optionally from a
// string field of a JSON cookie value (dot paths allowed).
func makeCookieExtractor(cookieName, cookieField string) SourceExtractor {
	return ExtractorFunc(func(req *http.Request) (string, int64, error) {
		cookie, err := req.Cookie(cookieName)
		if err != nil || cookie == nil {
			return "", 1, nil // missing cookie: the empty-source policy applies
		}

		value := cookie.Value
		if cookieField != "" {
			// A JSON payload cannot be sent raw inside a cookie: the quotes are
			// not valid in a cookie value, so clients (browsers, SDKs) send it
			// percent-encoded. Try the raw value first, then the decoded one.
			field, ok := jsonStringField(value, cookieField)
			if !ok {
				if decoded, err := url.QueryUnescape(value); err == nil {
					field, ok = jsonStringField(decoded, cookieField)
				}
			}
			if !ok {
				return "", 1, nil // not a JSON object, or missing/non-string field
			}

			value = field
		}

		return value, 1, nil
	})
}

// jsonStringField returns a string field of a JSON object. The field may be a
// dot path ("user.id"), so that a nested payload - the usual shape of a session
// cookie - can be used directly. ok is false when the payload is not a JSON
// object, when a path element is missing, or when the value is not a string.
func jsonStringField(payload, field string) (string, bool) {
	var object map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &object); err != nil {
		return "", false
	}

	parts := strings.Split(field, ".")
	var current interface{} = object

	for i, part := range parts {
		if part == "" {
			return "", false
		}

		level, ok := current.(map[string]interface{})
		if !ok {
			return "", false
		}

		raw, ok := level[part]
		if !ok {
			return "", false
		}

		if i == len(parts)-1 {
			value, ok := raw.(string)
			return value, ok
		}

		current = raw
	}

	return "", false
}

// makeHeaderExtractor builds the extractor of a request header.
func makeHeaderExtractor(header string) SourceExtractor {
	return ExtractorFunc(func(req *http.Request) (string, int64, error) {
		return req.Header.Get(header), 1, nil
	})
}
