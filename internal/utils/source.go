package utils

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
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
// field of a JSON cookie value, and optionally from a claim of a JWT held by
// that cookie or field.
func makeCookieExtractor(cookieName, cookieField, jwtClaim string) SourceExtractor {
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
			field, ok := jsonField(value, cookieField)
			if !ok {
				if decoded, err := url.QueryUnescape(value); err == nil {
					field, ok = jsonField(decoded, cookieField)
				}
			}
			if !ok {
				return "", 1, nil // not a JSON payload, or missing/unsupported field
			}

			value = field
		}

		if jwtClaim != "" {
			claim, ok := jwtClaimValue(value, jwtClaim)
			if !ok {
				return "", 1, nil // not a JWT, or missing/unsupported claim
			}

			value = claim
		}

		return value, 1, nil
	})
}

// makeHeaderExtractor builds the extractor of a request header, optionally from
// a claim of a JWT carried by that header ("Authorization: Bearer <jwt>").
func makeHeaderExtractor(header, jwtClaim string) SourceExtractor {
	return ExtractorFunc(func(req *http.Request) (string, int64, error) {
		value := req.Header.Get(header)

		if jwtClaim != "" {
			claim, ok := jwtClaimValue(value, jwtClaim)
			if !ok {
				return "", 1, nil
			}

			value = claim
		}

		return value, 1, nil
	})
}

// jsonField returns a field of a JSON payload as a source key. The field may be
// a dot path ("user.id"), address array elements by index ("users.0.id",
// "items[0].id", "a[0][1]"), or mix both. Scalars (string, number, boolean) are
// accepted, so that a numeric or boolean claim can be used directly; numbers
// keep the exact text of the payload. ok is false when the payload is not a
// single JSON value, when a path element is missing, or when the value is null,
// an object or an array.
func jsonField(payload, field string) (string, bool) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.UseNumber()

	var document interface{}
	if err := decoder.Decode(&document); err != nil {
		return "", false
	}

	// Reject trailing content: "{} garbage" is not a JSON payload.
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return "", false
	}

	parts := strings.Split(normalizeFieldPath(field), ".")

	current := document
	for _, part := range parts {
		if part == "" {
			return "", false
		}

		switch node := current.(type) {
		case map[string]interface{}:
			value, ok := node[part]
			if !ok {
				return "", false
			}
			current = value

		case []interface{}:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) {
				return "", false
			}
			current = node[index]

		default:
			// A scalar or null cannot be traversed any further.
			return "", false
		}
	}

	return scalarString(current)
}

// scalarString renders a JSON scalar as a rate limiting key. Numbers keep the
// exact text of the payload, which matters for identifiers that do not fit a
// float64 (snowflake ids, for instance).
func scalarString(value interface{}) (string, bool) {
	switch scalar := value.(type) {
	case string:
		return scalar, true
	case json.Number:
		return scalar.String(), true
	case bool:
		return strconv.FormatBool(scalar), true
	default:
		// null, an object or an array: not a usable source key on its own.
		return "", false
	}
}

// normalizeFieldPath turns the bracket form into the dot form, so that
// "items[0].id" and "items.0.id" are the same path. Index segments also resolve
// against objects, so a payload with numeric keys still works.
func normalizeFieldPath(field string) string {
	return strings.ReplaceAll(strings.ReplaceAll(field, "[", "."), "]", "")
}

// jwtClaimValue returns a claim of a JWS compact token
// ("header.payload.signature"). The signature is NOT verified: the claim is only
// used as a rate limiting key, so the token must be validated by another layer
// (see the README). A missing, empty or malformed token yields ok = false, and
// the empty-source policy then applies.
func jwtClaimValue(token, claim string) (string, bool) {
	parts := strings.Split(strings.TrimSpace(trimBearer(token)), ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return "", false
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		// Some producers pad the payload; base64url without padding is the norm.
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return "", false
		}
	}

	return jsonField(string(payload), claim)
}

// trimBearer drops a "Bearer " prefix (any case), so that the same option works
// for a raw token and for an Authorization header.
func trimBearer(value string) string {
	value = strings.TrimSpace(value)

	if len(value) >= len("bearer ") && strings.EqualFold(value[:len("bearer ")], "bearer ") {
		return strings.TrimSpace(value[len("bearer "):])
	}

	return value
}
