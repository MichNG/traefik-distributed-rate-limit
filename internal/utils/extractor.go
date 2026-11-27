package utils

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/MichNG/traefik-distributed-rate-limit/internal/ip"
)

// IPStrategy holds the IP strategy configuration used by Traefik to determine the client IP.
// More info: https://doc.traefik.io/traefik/v3.1/middlewares/http/ipallowlist/#ipstrategy
type IPStrategy struct {
	// Depth tells Traefik to use the X-Forwarded-For header and take the IP located at the depth position (starting from the right).
	Depth int `json:"depth,omitempty" toml:"depth,omitempty" yaml:"depth,omitempty" export:"true"`
	// ExcludedIPs configures Traefik to scan the X-Forwarded-For header and select the first IP not in the list.
	ExcludedIPs []string `json:"excludedIPs,omitempty" toml:"excludedIPs,omitempty" yaml:"excludedIPs,omitempty"`
	// TODO(mpl): I think we should make RemoteAddr an explicit field. For one thing, it would yield better documentation.
}

// Get an IP selection strategy.
// If nil return the RemoteAddr strategy
// else return a strategy based on the configuration using the X-Forwarded-For Header.
// Depth override the ExcludedIPs.
func (s *IPStrategy) Get() (ip.Strategy, error) {
	if s == nil {
		return &ip.RemoteAddrStrategy{}, nil
	}

	if s.Depth > 0 {
		return &ip.DepthStrategy{
			Depth: s.Depth,
		}, nil
	}

	if len(s.ExcludedIPs) > 0 {
		checker, err := ip.NewChecker(s.ExcludedIPs)
		if err != nil {
			return nil, err
		}
		return &ip.PoolStrategy{
			Checker: checker,
		}, nil
	}

	return &ip.RemoteAddrStrategy{}, nil
}

type SourceCriterion struct {
	IPStrategy *IPStrategy `json:"ipStrategy,omitempty" toml:"ipStrategy,omitempty" yaml:"ipStrategy,omitempty" mapstructure:"ipStrategy" export:"true"`
	// RequestHeaderName defines the name of the header used to group incoming requests.
	RequestHeaderName string `json:"requestHeaderName,omitempty" toml:"requestHeaderName,omitempty" yaml:"requestHeaderName,omitempty" mapstructure:"requestHeaderName" export:"true"`
	// RequestHost defines whether to consider the request Host as the source.
	RequestHost bool `json:"requestHost,omitempty" toml:"requestHost,omitempty" yaml:"requestHost,omitempty" mapstructure:"requestHost" export:"true"`

	// CookieName groups the requests by the value of a cookie, instead of
	// relying on a header set by another middleware (extension of this plugin).
	CookieName string `json:"cookieName,omitempty" toml:"cookieName,omitempty" yaml:"cookieName,omitempty" mapstructure:"cookieName"`
	// CookieField selects a field of a JSON cookie value instead of the whole
	// cookie value. Dot paths ("user.id"), array indexes ("users.0.id",
	// "items[0].id") and scalars (string, number, boolean) are supported.
	CookieField string `json:"cookieField,omitempty" toml:"cookieField,omitempty" yaml:"cookieField,omitempty" mapstructure:"cookieField"`
	// JWTClaim selects a claim of a JWT instead of the whole value: the token is
	// read from the cookie (after CookieField, if set) or from the request header
	// ("Authorization: Bearer <jwt>"). Dot paths and array indexes are supported,
	// as for CookieField. The signature is NOT verified - see the README.
	JWTClaim string `json:"jwtClaim,omitempty" toml:"jwtClaim,omitempty" yaml:"jwtClaim,omitempty" mapstructure:"jwtClaim"`
	// OnEmptySource defines what to do when the source cannot be determined
	// (missing cookie, missing header, absent JSON field, empty value):
	//   shared   - all such requests share one bucket (default, historical
	//              behaviour);
	//   clientIP - fall back to the client IP, so that rate limiting keeps
	//              working per client instead of grouping everybody;
	//   deny     - reject the request with 400: an unidentified client never
	//              consumes a shared bucket;
	//   denyOnError - handle the request exactly like a store error: 429 when
	//              denyOnError is true, and let it reach the backend (no rate
	//              limiting at all) when denyOnError is false.
	OnEmptySource string `json:"onEmptySource,omitempty" toml:"onEmptySource,omitempty" yaml:"onEmptySource,omitempty" mapstructure:"onEmptySource"`
}

// OnEmptySource policies.
const (
	OnEmptySourceShared      = "shared"
	OnEmptySourceClientIP    = "clientIP"
	OnEmptySourceDeny        = "deny"
	OnEmptySourceDenyOnError = "denyOnError"
)

// ErrEmptySource is returned when the source cannot be determined and the
// configured policy is OnEmptySourceDeny or OnEmptySourceDenyOnError. The
// middleware decides the response: 400 for the former, and 429/backend for the
// latter (following denyOnError).
var ErrEmptySource = errors.New("the source of the request can not be determined")

// GetSourceExtractor returns the SourceExtractor function corresponding to the given sourceMatcher.
// It defaults to a RemoteAddrStrategy IPStrategy if need be.
// It returns an error if more than one source criterion is provided.
func GetSourceExtractor(sourceMatcher *SourceCriterion) (SourceExtractor, error) {
	policy := OnEmptySourceShared
	if sourceMatcher != nil && sourceMatcher.OnEmptySource != "" {
		policy = sourceMatcher.OnEmptySource
	}

	var criteria int
	if sourceMatcher != nil {
		for _, set := range []bool{
			sourceMatcher.RequestHeaderName != "",
			sourceMatcher.RequestHost,
			sourceMatcher.CookieName != "",
		} {
			if set {
				criteria++
			}
		}

		if criteria > 1 {
			return nil, errors.New("requestHeaderName, requestHost and cookieName are mutually exclusive")
		}

		if sourceMatcher.CookieName != "" {
			if err := validateCookieName(sourceMatcher.CookieName); err != nil {
				return nil, err
			}
		} else if sourceMatcher.CookieField != "" {
			return nil, errors.New("cookieField requires cookieName")
		}

		if sourceMatcher.CookieField != "" {
			if err := validateFieldPath("cookieField", sourceMatcher.CookieField); err != nil {
				return nil, err
			}
		}

		if sourceMatcher.JWTClaim != "" {
			if sourceMatcher.CookieName == "" && sourceMatcher.RequestHeaderName == "" {
				return nil, errors.New("jwtClaim requires cookieName or requestHeaderName")
			}

			if err := validateFieldPath("jwtClaim", sourceMatcher.JWTClaim); err != nil {
				return nil, err
			}
		}

		// ipStrategy is either the criterion itself, or - with
		// onEmptySource=clientIP - the strategy used for the fallback source.
		if sourceMatcher.IPStrategy != nil && criteria == 1 && policy != OnEmptySourceClientIP {
			return nil, errors.New("ipStrategy and the other source criteria are mutually exclusive (use onEmptySource=clientIP to select the fallback strategy)")
		}
	}

	switch policy {
	case OnEmptySourceShared, OnEmptySourceClientIP, OnEmptySourceDeny, OnEmptySourceDenyOnError:
	default:
		return nil, fmt.Errorf("invalid onEmptySource %q: expected %q, %q, %q or %q",
			policy, OnEmptySourceShared, OnEmptySourceClientIP, OnEmptySourceDeny, OnEmptySourceDenyOnError)
	}

	if sourceMatcher == nil || (criteria == 0 && sourceMatcher.IPStrategy == nil) {
		sourceMatcher = &SourceCriterion{
			IPStrategy: &IPStrategy{},
		}
	}

	if sourceMatcher.IPStrategy != nil && criteria == 0 {
		// Pure IP criterion: no empty-source policy needed.
		sourceMatcher.OnEmptySource = OnEmptySourceShared
	}

	primary, fallbackIP, err := primaryExtractor(sourceMatcher)
	if err != nil {
		return nil, err
	}

	if policy == OnEmptySourceShared {
		return primary, nil
	}

	return ExtractorFunc(func(req *http.Request) (string, int64, error) {
		token, amount, err := primary.Extract(req)
		if err != nil || token != "" {
			return token, amount, err
		}

		if policy == OnEmptySourceDeny || policy == OnEmptySourceDenyOnError {
			return "", 0, ErrEmptySource
		}

		// OnEmptySourceClientIP: keep rate limiting per client instead of
		// grouping every unidentified client in one bucket.
		clientIP := fallbackIP.GetIP(req)
		if clientIP == "" {
			return "", 0, ErrEmptySource
		}

		return clientIP, amount, nil
	}), nil
}

// validateCookieName rejects names that can never match a cookie, so that a
// configuration typo fails at creation instead of silently producing an empty
// source (and, with it, a shared bucket).
func validateCookieName(name string) error {
	if strings.ContainsAny(name, " \t\r\n;,=") {
		return fmt.Errorf("invalid cookieName %q: it must not contain spaces, ';', ',', '=' or control characters", name)
	}

	return nil
}

// validateFieldPath rejects paths that can never resolve, so that a
// configuration typo fails at creation instead of silently producing an empty
// source (and, with it, a shared bucket). Both the dot form ("user.id",
// "users.0.id") and the bracket form ("items[0].id") are accepted.
func validateFieldPath(kind, field string) error {
	if strings.ContainsAny(field, " \t\r\n") {
		return fmt.Errorf("invalid %s %q: it must not contain spaces or control characters", kind, field)
	}

	for i := 0; i < len(field); i++ {
		switch field[i] {
		case '[':
			end := strings.IndexByte(field[i:], ']')
			if end < 0 {
				return fmt.Errorf("invalid %s %q: unbalanced array brackets", kind, field)
			}

			index := field[i+1 : i+end]
			if index == "" {
				return fmt.Errorf("invalid %s %q: empty array index", kind, field)
			}

			for j := 0; j < len(index); j++ {
				if index[j] < '0' || index[j] > '9' {
					return fmt.Errorf("invalid %s %q: array index %q is not a number", kind, field, index)
				}
			}

			i += end

		case ']':
			return fmt.Errorf("invalid %s %q: unbalanced array brackets", kind, field)
		}
	}

	for _, part := range strings.Split(normalizeFieldPath(field), ".") {
		if part == "" {
			return fmt.Errorf("invalid %s %q: empty path element", kind, field)
		}
	}

	return nil
}

// primaryExtractor builds the extractor of the configured criterion, plus the
// IP strategy used as the fallback when the source is empty.
//
// The IP strategy is the primary criterion only when no other criterion is set:
// with onEmptySource=clientIP it is usually just the fallback strategy.
func primaryExtractor(sourceMatcher *SourceCriterion) (SourceExtractor, ipStrategy, error) {
	// The IP strategy is the primary criterion only when no other criterion is
	// set; otherwise it is the fallback used by onEmptySource=clientIP.
	var strategy ipStrategy = &ip.RemoteAddrStrategy{}
	if sourceMatcher.IPStrategy != nil {
		configured, err := sourceMatcher.IPStrategy.Get()
		if err != nil {
			return nil, nil, err
		}
		strategy = configured
	}

	ipIsPrimary := sourceMatcher.IPStrategy != nil &&
		sourceMatcher.RequestHeaderName == "" &&
		!sourceMatcher.RequestHost &&
		sourceMatcher.CookieName == ""

	if ipIsPrimary {
		return ExtractorFunc(func(req *http.Request) (string, int64, error) {
			return strategy.GetIP(req), 1, nil
		}), strategy, nil
	}

	if sourceMatcher.RequestHeaderName != "" {
		return makeHeaderExtractor(sourceMatcher.RequestHeaderName, sourceMatcher.JWTClaim), strategy, nil
	}

	if sourceMatcher.RequestHost {
		return ExtractorFunc(func(req *http.Request) (string, int64, error) {
			return req.Host, 1, nil
		}), strategy, nil
	}

	if sourceMatcher.CookieName != "" {
		return makeCookieExtractor(sourceMatcher.CookieName, sourceMatcher.CookieField, sourceMatcher.JWTClaim), strategy, nil
	}

	return nil, nil, errors.New("no SourceCriterion criterion defined")
}

// ipStrategy is the IP selection strategy used as the fallback source.
type ipStrategy interface {
	GetIP(req *http.Request) string
}
