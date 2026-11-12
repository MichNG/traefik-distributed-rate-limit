// Package traefik_distributed_rate_limit implements the distributedRateLimit
// middleware: a token bucket rate limiter whose state lives in Redis, so that
// the limit is shared by every Traefik instance of the cluster.
//
// The configuration follows the Traefik Hub Distributed RateLimit schema
// (limit, period, burst, denyOnError, responseHeaders, sourceCriterion,
// store.redis.*), with a compatibility layer for the historical flat keys of
// this plugin (average, redisAddress, ...).
package traefik_distributed_rate_limit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/MichNG/traefik-distributed-rate-limit/internal/ip"
	"github.com/MichNG/traefik-distributed-rate-limit/internal/redis"
	"github.com/MichNG/traefik-distributed-rate-limit/internal/utils"
)

const (
	defaultPeriod      = time.Second
	defaultTimeout     = 2 * time.Second
	minSwitchInterval  = time.Second
	defaultBreakerTTL  = 15 * time.Second
	defaultBreakerFail = 3
)

// Config is the plugin configuration. Field names follow the Traefik Hub
// schema; the deprecated flat fields are kept for backward compatibility and
// are removed in v3.
type Config struct {
	// Limit is the maximum rate (in requests per Period) allowed for a given
	// source. 0 means no rate limiting. It defaults to 0.
	Limit int64 `json:"limit,omitempty" toml:"limit,omitempty" yaml:"limit,omitempty" mapstructure:"limit"`
	// Period, in combination with Limit, defines the actual maximum rate, such
	// as: r = Limit / Period. It defaults to 1s.
	Period string `json:"period,omitempty" toml:"period,omitempty" yaml:"period,omitempty" mapstructure:"period"`
	// Burst is the maximum number of requests allowed to arrive in the same
	// arbitrarily small period of time. It defaults to 1.
	Burst int64 `json:"burst,omitempty" toml:"burst,omitempty" yaml:"burst,omitempty" mapstructure:"burst"`
	// DenyOnError forces a 429 response when no rate limit decision can be
	// obtained (unreachable store, open circuit breaker). Set to false, the
	// request reaches the backend instead. It defaults to true.
	DenyOnError *bool `json:"denyOnError,omitempty" toml:"denyOnError,omitempty" yaml:"denyOnError,omitempty" mapstructure:"denyOnError"`
	// ResponseHeaders injects the X-RateLimit-* headers into the responses.
	ResponseHeaders bool `json:"responseHeaders,omitempty" toml:"responseHeaders,omitempty" yaml:"responseHeaders,omitempty" mapstructure:"responseHeaders"`
	// SourceCriterion defines what criterion is used to group requests as
	// originating from a common source.
	SourceCriterion *utils.SourceCriterion `json:"sourceCriterion,omitempty" toml:"sourceCriterion,omitempty" yaml:"sourceCriterion,omitempty" mapstructure:"sourceCriterion"`
	// Store configures the persistent storage of the token buckets.
	Store StoreConfig `json:"store,omitempty" toml:"store,omitempty" yaml:"store,omitempty" mapstructure:"store"`
	// WhitelistIPs is an extension of this plugin: addresses or CIDR ranges
	// that bypass rate limiting entirely.
	WhitelistIPs []string `json:"whitelistIPs,omitempty" toml:"whitelistIPs,omitempty" yaml:"whitelistIPs,omitempty" mapstructure:"whitelistIPs"`

	// Deprecated: Average is the v1 name of Limit.
	Average int64 `json:"average,omitempty" toml:"average,omitempty" yaml:"average,omitempty" mapstructure:"average"`
	// Deprecated: RedisAddress is the v1 single-instance address.
	RedisAddress string `json:"redisAddress,omitempty" toml:"redisAddress,omitempty" yaml:"redisAddress,omitempty" mapstructure:"redisAddress"`
	// Deprecated: RedisDB is the v1 database index.
	RedisDB uint `json:"redisDb,omitempty" toml:"redisDb,omitempty" yaml:"redisDb,omitempty" mapstructure:"redisDb"`
	// Deprecated: RedisPassword is the v1 password.
	RedisPassword string `json:"redisPassword,omitempty" toml:"redisPassword,omitempty" yaml:"redisPassword,omitempty" mapstructure:"redisPassword"`
	// Deprecated: RedisConnectionTimeout is the v1 timeout, in seconds.
	RedisConnectionTimeout int64 `json:"redisConnectionTimeout,omitempty" toml:"redisConnectionTimeout,omitempty" yaml:"redisConnectionTimeout,omitempty" mapstructure:"redisConnectionTimeout"`
	// Deprecated: BreakerThreshold is the v1 circuit breaker threshold.
	BreakerThreshold int64 `json:"breakerThreshold,omitempty" toml:"breakerThreshold,omitempty" yaml:"breakerThreshold,omitempty" mapstructure:"breakerThreshold"`
	// Deprecated: BreakerReattempt is the v1 circuit breaker reattempt delay,
	// in seconds.
	BreakerReattempt int64 `json:"breakerReattempt,omitempty" toml:"breakerReattempt,omitempty" yaml:"breakerReattempt,omitempty" mapstructure:"breakerReattempt"`
}

// StoreConfig is the `store` section.
type StoreConfig struct {
	Redis RedisStoreConfig `json:"redis,omitempty" toml:"redis,omitempty" yaml:"redis,omitempty" mapstructure:"redis"`
}

// RedisStoreConfig is the `store.redis` section.
type RedisStoreConfig struct {
	Endpoints []string `json:"endpoints,omitempty" toml:"endpoints,omitempty" yaml:"endpoints,omitempty" mapstructure:"endpoints"`
	Username  string   `json:"username,omitempty" toml:"username,omitempty" yaml:"username,omitempty" mapstructure:"username"`
	Password  string   `json:"password,omitempty" toml:"password,omitempty" yaml:"password,omitempty" mapstructure:"password"`
	Database  int      `json:"database,omitempty" toml:"database,omitempty" yaml:"database,omitempty" mapstructure:"database"`
	Timeout   string   `json:"timeout,omitempty" toml:"timeout,omitempty" yaml:"timeout,omitempty" mapstructure:"timeout"`
	// PoolSize is the maximum number of idle connections kept per node.
	PoolSize int `json:"poolSize,omitempty" toml:"poolSize,omitempty" yaml:"poolSize,omitempty" mapstructure:"poolSize"`
	// MaxActiveConns is the maximum number of connections opened at the same
	// time, in use or idle. It defaults to 10 * GOMAXPROCS.
	MaxActiveConns int `json:"maxActiveConns,omitempty" toml:"maxActiveConns,omitempty" yaml:"maxActiveConns,omitempty" mapstructure:"maxActiveConns"`
	// IdleTimeout stops the background master discovery and closes the idle
	// connections after that much inactivity; a later request starts them
	// again. 0 (default) disables it.
	IdleTimeout string `json:"idleTimeout,omitempty" toml:"idleTimeout,omitempty" yaml:"idleTimeout,omitempty" mapstructure:"idleTimeout"`
	// Cluster enables Redis Cluster. Not supported yet: configuring it fails
	// the middleware creation with an explicit message.
	Cluster *struct{} `json:"cluster,omitempty" toml:"cluster,omitempty" yaml:"cluster,omitempty" mapstructure:"cluster"`
	// Sentinel enables Redis Sentinel mode.
	Sentinel *SentinelStoreConfig `json:"sentinel,omitempty" toml:"sentinel,omitempty" yaml:"sentinel,omitempty" mapstructure:"sentinel"`
	// TLS secures the connections to Redis (and to the Sentinels).
	TLS *TLSStoreConfig `json:"tls,omitempty" toml:"tls,omitempty" yaml:"tls,omitempty" mapstructure:"tls"`
	// Breaker tunes the circuit breaker (extension of this plugin).
	Breaker *BreakerStoreConfig `json:"breaker,omitempty" toml:"breaker,omitempty" yaml:"breaker,omitempty" mapstructure:"breaker"`
}

// isEmpty reports whether no field of the new `store.redis` schema was set,
// in which case the deprecated flat keys still apply.
func (r RedisStoreConfig) isEmpty() bool {
	return len(r.Endpoints) == 0 &&
		r.Username == "" &&
		r.Password == "" &&
		r.Database == 0 &&
		r.Timeout == "" &&
		r.PoolSize == 0 &&
		r.MaxActiveConns == 0 &&
		r.IdleTimeout == "" &&
		r.Cluster == nil &&
		r.Sentinel == nil &&
		r.TLS == nil &&
		r.Breaker == nil
}

// SentinelStoreConfig is the `store.redis.sentinel` section.
type SentinelStoreConfig struct {
	MasterSet string `json:"masterSet,omitempty" toml:"masterSet,omitempty" yaml:"masterSet,omitempty" mapstructure:"masterSet"`
	Username  string `json:"username,omitempty" toml:"username,omitempty" yaml:"username,omitempty" mapstructure:"username"`
	Password  string `json:"password,omitempty" toml:"password,omitempty" yaml:"password,omitempty" mapstructure:"password"`
	// Watch subscribes to the Sentinel +switch-master events so that a
	// failover is applied before the next request fails. When disabled, the
	// failover is still handled, but only when a request hits the demoted
	// master (READONLY). Defaults to true.
	Watch *bool `json:"watch,omitempty" toml:"watch,omitempty" yaml:"watch,omitempty" mapstructure:"watch"`
}

// TLSStoreConfig is the `store.redis.tls` section. caBundle and ca are aliases
// (Traefik Hub and Traefik OSS naming respectively).
type TLSStoreConfig struct {
	CABundle           string `json:"caBundle,omitempty" toml:"caBundle,omitempty" yaml:"caBundle,omitempty" mapstructure:"caBundle"`
	CA                 string `json:"ca,omitempty" toml:"ca,omitempty" yaml:"ca,omitempty" mapstructure:"ca"`
	Cert               string `json:"cert,omitempty" toml:"cert,omitempty" yaml:"cert,omitempty" mapstructure:"cert"`
	Key                string `json:"key,omitempty" toml:"key,omitempty" yaml:"key,omitempty" mapstructure:"key"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty" toml:"insecureSkipVerify,omitempty" yaml:"insecureSkipVerify,omitempty" mapstructure:"insecureSkipVerify"`
}

// BreakerStoreConfig is the `store.redis.breaker` section (extension).
type BreakerStoreConfig struct {
	Threshold int64 `json:"threshold,omitempty" toml:"threshold,omitempty" yaml:"threshold,omitempty" mapstructure:"threshold"`
	Reattempt int64 `json:"reattempt,omitempty" toml:"reattempt,omitempty" yaml:"reattempt,omitempty" mapstructure:"reattempt"`
}

// CreateConfig creates the default plugin configuration.
func CreateConfig() *Config {
	return &Config{}
}

// ClusterRateLimit is the middleware handler.
type ClusterRateLimit struct {
	next http.Handler
	name string

	limiter          *Limiter
	sourceMatcher    utils.SourceExtractor
	whitelistChecker *ip.Checker
	ipStrategy       ip.Strategy

	limit           Limit
	denyOnError     bool
	responseHeaders bool

	// emptySourcePolicy is the configured sourceCriterion.onEmptySource value
	// (shared by default); when it is denyOnError, the response follows the
	// denyOnError flag, like a store error.
	emptySourcePolicy string

	logger *pluginLogger
}

// New creates a new distributedRateLimit middleware.
//
// The results are returned explicitly, and not by forwarding the call
// (`return newRateLimit(...)`): yaegi 0.16.1 loses the error of a forwarded
// multi-value call crossing the host/interpreted boundary, so Traefik would
// report a nil handler with a nil error ("invalid handler type: <nil>") and the
// operator would never see the configuration error.
func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
	handler, err := newRateLimit(ctx, next, config, name, nil)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

// newRateLimit is the testable constructor: a non-nil store replaces the real
// Redis client.
func newRateLimit(ctx context.Context, next http.Handler, config *Config, name string, store Store) (*ClusterRateLimit, error) {
	logger := newPluginLogger(name)

	warnings, err := config.normalize()
	if err != nil {
		return nil, err
	}
	for _, warning := range warnings {
		logger.warnf("%s", warning)
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	sourceMatcher, err := utils.GetSourceExtractor(config.SourceCriterion)
	if err != nil {
		return nil, err
	}

	var whitelistChecker *ip.Checker
	if len(config.WhitelistIPs) > 0 {
		whitelistChecker, err = ip.NewChecker(config.WhitelistIPs)
		if err != nil {
			return nil, fmt.Errorf("unable to create IP whitelist checker: %w", err)
		}
	}

	var ipStrategy ip.Strategy
	if config.SourceCriterion != nil && config.SourceCriterion.IPStrategy != nil {
		ipStrategy, err = config.SourceCriterion.IPStrategy.Get()
		if err != nil {
			return nil, fmt.Errorf("unable to create IP strategy: %w", err)
		}
	} else {
		ipStrategy = &ip.RemoteAddrStrategy{}
	}

	if store == nil {
		store, err = newStore(ctx, config, logger)
		if err != nil {
			return nil, err
		}
	}

	period, _ := parsePeriod(config.Period)
	limit := Limit{
		Rate:   config.Limit,
		Burst:  config.Burst,
		Period: period,
	}

	return &ClusterRateLimit{
		next:              next,
		name:              name,
		limiter:           NewLimiter(store, name, config.breakerConfig()),
		sourceMatcher:     sourceMatcher,
		whitelistChecker:  whitelistChecker,
		ipStrategy:        ipStrategy,
		limit:             limit,
		denyOnError:       config.denyOnError(),
		responseHeaders:   config.ResponseHeaders,
		emptySourcePolicy: emptySourcePolicy(config),
		logger:            logger,
	}, nil
}

// emptySourcePolicy returns the configured policy for an unknown source,
// defaulting to the historical behaviour (one shared bucket).
func emptySourcePolicy(config *Config) string {
	if config.SourceCriterion == nil || config.SourceCriterion.OnEmptySource == "" {
		return utils.OnEmptySourceShared
	}

	return config.SourceCriterion.OnEmptySource
}

// ServeHTTP implements http.Handler.
func (rl *ClusterRateLimit) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	// limit = 0 means no rate limiting.
	if rl.limit.Rate == 0 {
		rl.next.ServeHTTP(rw, req)
		return
	}

	if rl.isWhitelisted(req) {
		rl.next.ServeHTTP(rw, req)
		return
	}

	source, _, err := rl.sourceMatcher.Extract(req)
	if err != nil {
		if errors.Is(err, utils.ErrEmptySource) {
			// The source of the request is unknown: apply the configured policy
			// instead of silently sharing one bucket between all the
			// unidentified clients.
			switch rl.emptySourcePolicy {
			case utils.OnEmptySourceDenyOnError:
				// Handled like a store error: 429, or the request reaches the
				// backend when denyOnError is false.
				if rl.denyOnError {
					rl.logger.warnf("no source for the request and denyOnError is enabled: returning %d (%v)",
						http.StatusTooManyRequests, err)
					rl.writeTooManyRequests(rw, nil)
					return
				}

				rl.logger.warnf("no source for the request, denyOnError is disabled: the request is not rate limited (%v)", err)
				rl.next.ServeHTTP(rw, req)
				return

			default:
				rl.logger.warnf("rejecting the request: %v (onEmptySource=%s)", err, rl.emptySourcePolicy)
				http.Error(rw, "could not determine the rate limit source", http.StatusBadRequest)
				return
			}
		}

		rl.logger.errorf("could not extract the source of the request: %v", err)
		http.Error(rw, "could not extract source of request", http.StatusInternalServerError)
		return
	}

	res, err := rl.limiter.Allow(req.Context(), source, rl.limit)
	if err != nil {
		// No decision available: the denyOnError policy decides.
		rl.logger.errorf("no rate limit decision from the store on node %q: %v", rl.limiter.Node(), err)

		if rl.denyOnError {
			rl.writeTooManyRequests(rw, nil)
			return
		}

		rl.next.ServeHTTP(rw, req)
		return
	}

	if res.Allowed <= 0 {
		rl.writeTooManyRequests(rw, res)
		return
	}

	if rl.responseHeaders {
		rl.setRateLimitHeaders(rw, res)
	}

	rl.next.ServeHTTP(rw, req)
}

func (rl *ClusterRateLimit) isWhitelisted(req *http.Request) bool {
	if rl.whitelistChecker == nil {
		return false
	}

	clientIP := rl.ipStrategy.GetIP(req)
	if clientIP == "" {
		return false
	}

	contains, err := rl.whitelistChecker.Contains(clientIP)
	return err == nil && contains
}

func (rl *ClusterRateLimit) writeTooManyRequests(rw http.ResponseWriter, res *Result) {
	retryAfter := int64(1)
	if res != nil && res.RetryAfter > 0 {
		retryAfter = int64(res.RetryAfter/time.Second) + 1
	}
	rw.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))

	if rl.responseHeaders && res != nil {
		rl.setRateLimitHeaders(rw, res)
	}

	http.Error(rw, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
}

func (rl *ClusterRateLimit) setRateLimitHeaders(rw http.ResponseWriter, res *Result) {
	rw.Header().Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit.Burst, 10))
	rw.Header().Set("X-RateLimit-Period", res.Limit.Period.String())
	rw.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))

	reset := int64(0)
	if res.ResetAfter > 0 {
		reset = int64(math.Ceil(res.ResetAfter.Seconds()))
	}
	rw.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
}

// ---------------------------------------------------------------------------
// Configuration normalization and validation
// ---------------------------------------------------------------------------

// denyOnError returns the effective policy, defaulting to true like Traefik Hub.
func (c *Config) denyOnError() bool {
	if c.DenyOnError == nil {
		return true
	}
	return *c.DenyOnError
}

func (c *Config) breakerConfig() redis.BreakerConfig {
	cfg := redis.BreakerConfig{
		Threshold: defaultBreakerFail,
		Reattempt: defaultBreakerTTL,
	}

	if c.Store.Redis.Breaker != nil {
		if c.Store.Redis.Breaker.Threshold > 0 {
			cfg.Threshold = c.Store.Redis.Breaker.Threshold
		}
		if c.Store.Redis.Breaker.Reattempt > 0 {
			cfg.Reattempt = time.Duration(c.Store.Redis.Breaker.Reattempt) * time.Second
		}
	}

	return cfg
}

// normalize applies the defaults and folds the deprecated v1 keys into the new
// schema. It returns the list of deprecation warnings to log.
func (c *Config) normalize() ([]string, error) {
	var warnings []string

	// limit <- average
	if c.Limit == 0 && c.Average != 0 {
		c.Limit = c.Average
		warnings = append(warnings, `"average" is deprecated, use "limit" instead`)
	} else if c.Average != 0 && c.Average != c.Limit {
		warnings = append(warnings, `"average" is deprecated and ignored because "limit" is set`)
	}

	period, err := parsePeriod(c.Period)
	if err != nil {
		return warnings, err
	}
	if period <= 0 {
		return warnings, fmt.Errorf("period must be greater than 0, got %q", c.Period)
	}
	c.Period = period.String()

	if c.Burst < 1 {
		c.Burst = 1
	}

	// The new `store.redis` schema takes precedence: the deprecated flat keys
	// are only applied when the new section is entirely absent, so that a
	// half-migrated configuration never silently overrides the new values.
	if c.Store.Redis.isEmpty() {
		if c.RedisAddress != "" {
			c.Store.Redis.Endpoints = []string{c.RedisAddress}
			warnings = append(warnings, `"redisAddress" is deprecated, use "store.redis.endpoints" instead`)
		}
		if c.RedisDB != 0 {
			c.Store.Redis.Database = int(c.RedisDB)
			warnings = append(warnings, `"redisDb" is deprecated, use "store.redis.database" instead`)
		}
		if c.RedisPassword != "" {
			c.Store.Redis.Password = c.RedisPassword
			warnings = append(warnings, `"redisPassword" is deprecated, use "store.redis.password" instead`)
		}
		if c.RedisConnectionTimeout > 0 {
			c.Store.Redis.Timeout = (time.Duration(c.RedisConnectionTimeout) * time.Second).String()
			warnings = append(warnings, `"redisConnectionTimeout" is deprecated, use "store.redis.timeout" instead`)
		}
		if c.BreakerThreshold > 0 || c.BreakerReattempt > 0 {
			c.Store.Redis.Breaker = &BreakerStoreConfig{
				Threshold: c.BreakerThreshold,
				Reattempt: c.BreakerReattempt,
			}
			warnings = append(warnings, `"breakerThreshold"/"breakerReattempt" are deprecated, use "store.redis.breaker.threshold"/"reattempt" instead`)
		}
	} else {
		for _, legacy := range []struct {
			set  bool
			key  string
			repl string
		}{
			{c.RedisAddress != "", "redisAddress", "store.redis.endpoints"},
			{c.RedisDB != 0, "redisDb", "store.redis.database"},
			{c.RedisPassword != "", "redisPassword", "store.redis.password"},
			{c.RedisConnectionTimeout > 0, "redisConnectionTimeout", "store.redis.timeout"},
			{c.BreakerThreshold > 0 || c.BreakerReattempt > 0, "breakerThreshold/breakerReattempt", "store.redis.breaker"},
		} {
			if legacy.set {
				warnings = append(warnings, fmt.Sprintf("%q is deprecated and ignored because %q is configured", legacy.key, legacy.repl))
			}
		}
	}

	if c.Store.Redis.Breaker == nil {
		c.Store.Redis.Breaker = &BreakerStoreConfig{}
	}

	// Credentials may be given as "$ENV_VAR" instead of being inlined.
	if password, ok := expandEnv(c.Store.Redis.Password); !ok {
		warnings = append(warnings, fmt.Sprintf("store.redis.password references an environment variable that is not set: %s", c.Store.Redis.Password))
	} else {
		c.Store.Redis.Password = password
	}

	if c.Store.Redis.Sentinel != nil {
		if password, ok := expandEnv(c.Store.Redis.Sentinel.Password); !ok {
			warnings = append(warnings, fmt.Sprintf("store.redis.sentinel.password references an environment variable that is not set: %s", c.Store.Redis.Sentinel.Password))
		} else {
			c.Store.Redis.Sentinel.Password = password
		}
	}

	if c.Store.Redis.TLS != nil && c.Store.Redis.TLS.CABundle == "" && c.Store.Redis.TLS.CA != "" {
		// The OSS spelling is accepted as an alias.
		c.Store.Redis.TLS.CABundle = c.Store.Redis.TLS.CA
	}

	if c.Store.Redis.TLS == nil {
		c.Store.Redis.TLS = &TLSStoreConfig{}
	}

	return warnings, nil
}

// validate checks the configuration, so that mistakes are reported when the
// middleware is created rather than on the first request.
func (c *Config) validate() error {
	if c.Limit < 0 {
		return fmt.Errorf("limit must be >= 0 (0 means no rate limiting), got %d", c.Limit)
	}

	if c.Store.Redis.Cluster != nil {
		return errors.New("store.redis.cluster is not supported yet: use store.redis.sentinel or a single Redis instance")
	}

	if len(c.Store.Redis.Endpoints) == 0 {
		return errors.New("store.redis.endpoints must not be empty")
	}
	for _, endpoint := range c.Store.Redis.Endpoints {
		if _, _, err := net.SplitHostPort(endpoint); err != nil {
			return fmt.Errorf("invalid Redis endpoint %q: %w", endpoint, err)
		}
	}

	if c.Store.Redis.Sentinel != nil {
		if c.Store.Redis.Sentinel.MasterSet == "" {
			return errors.New("store.redis.sentinel.masterSet is required when store.redis.sentinel is used")
		}
	}

	if c.Store.Redis.PoolSize < 0 {
		return fmt.Errorf("store.redis.poolSize must be >= 0, got %d", c.Store.Redis.PoolSize)
	}
	if c.Store.Redis.MaxActiveConns < 0 {
		return fmt.Errorf("store.redis.maxActiveConns must be >= 0, got %d", c.Store.Redis.MaxActiveConns)
	}

	if _, err := c.timeout(); err != nil {
		return err
	}

	if _, err := c.idleTimeout(); err != nil {
		return err
	}

	// TLS material is validated eagerly: a broken CA/cert/key must not be
	// discovered on the first request.
	tlsCfg := c.tlsConfig()
	if _, err := tlsCfg.CreateTLSConfig(); err != nil {
		return fmt.Errorf("invalid store.redis.tls configuration: %w", err)
	}

	return nil
}

func (c *Config) timeout() (time.Duration, error) {
	if c.Store.Redis.Timeout == "" {
		return defaultTimeout, nil
	}

	d, err := time.ParseDuration(c.Store.Redis.Timeout)
	if err != nil {
		return 0, fmt.Errorf("invalid store.redis.timeout %q: %w", c.Store.Redis.Timeout, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("store.redis.timeout must be greater than 0, got %q", c.Store.Redis.Timeout)
	}

	return d, nil
}

// idleTimeout returns the idle stop delay (0 = disabled).
func (c *Config) idleTimeout() (time.Duration, error) {
	if c.Store.Redis.IdleTimeout == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(c.Store.Redis.IdleTimeout)
	if err != nil {
		return 0, fmt.Errorf("invalid store.redis.idleTimeout %q: %w", c.Store.Redis.IdleTimeout, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("store.redis.idleTimeout must be >= 0, got %q", c.Store.Redis.IdleTimeout)
	}

	return d, nil
}

// sentinelWatch reports whether the +switch-master subscription is enabled.
func (c *Config) sentinelWatch() bool {
	if c.Store.Redis.Sentinel == nil || c.Store.Redis.Sentinel.Watch == nil {
		return true
	}

	return *c.Store.Redis.Sentinel.Watch
}

func (c *Config) tlsConfig() *redis.TLSConfig {
	tlsCfg := c.Store.Redis.TLS
	if tlsCfg == nil {
		return nil
	}
	if tlsCfg.CABundle == "" && tlsCfg.Cert == "" && tlsCfg.Key == "" && !tlsCfg.InsecureSkipVerify {
		return nil
	}

	ca := tlsCfg.CABundle
	if ca == "" {
		ca = tlsCfg.CA
	}

	return &redis.TLSConfig{
		CA:                 ca,
		Cert:               tlsCfg.Cert,
		Key:                tlsCfg.Key,
		InsecureSkipVerify: tlsCfg.InsecureSkipVerify,
	}
}

// parsePeriod accepts a Go duration ("1s", "1m") and, for backward
// compatibility, a plain number of seconds ("10", the v1 format).
func parsePeriod(value string) (time.Duration, error) {
	if value == "" {
		return defaultPeriod, nil
	}

	if d, err := time.ParseDuration(value); err == nil {
		return d, nil
	}

	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid period %q: expected a duration like \"1s\" or a number of seconds", value)
	}

	return time.Duration(seconds) * time.Second, nil
}

// expandEnv resolves the "$VAR" indirection used for secrets. The returned
// boolean is false when the value references an environment variable that is
// not set, so that the caller can warn instead of silently using an empty
// password.
func expandEnv(value string) (string, bool) {
	if len(value) > 1 && value[0] == '$' {
		env, ok := os.LookupEnv(value[1:])
		return env, ok
	}
	return value, true
}

// ---------------------------------------------------------------------------
// Store construction
// ---------------------------------------------------------------------------

func newStore(ctx context.Context, c *Config, logger *pluginLogger) (Store, error) {
	timeout, err := c.timeout()
	if err != nil {
		return nil, err
	}

	tlsCfg := c.tlsConfig()

	idleTimeout, err := c.idleTimeout()
	if err != nil {
		return nil, err
	}

	clientCfg := redis.ClientConfig{
		Username:       c.Store.Redis.Username,
		Password:       c.Store.Redis.Password,
		Database:       c.Store.Redis.Database,
		ConnectTimeout: timeout,
		CommandTimeout: timeout,
		PoolSize:       c.Store.Redis.PoolSize,
		MaxActiveConns: c.Store.Redis.MaxActiveConns,
		IdleTimeout:    idleTimeout,
	}

	var provider redis.NodeProvider

	if c.Store.Redis.Sentinel != nil {
		tlsClientConfig, err := tlsCfg.CreateTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("invalid store.redis.tls configuration: %w", err)
		}

		resolver, err := redis.NewSentinelResolver(redis.SentinelConfig{
			Endpoints:   c.Store.Redis.Endpoints,
			MasterSet:   c.Store.Redis.Sentinel.MasterSet,
			Username:    c.Store.Redis.Sentinel.Username,
			Password:    c.Store.Redis.Sentinel.Password,
			DialTimeout: timeout,
			ReadTimeout: timeout,
		}, &redis.Dialer{TLS: tlsClientConfig, DialTimeout: timeout}, logger)
		if err != nil {
			return nil, fmt.Errorf("configuring Redis Sentinel: %w", err)
		}

		sentinelProvider, err := redis.NewSentinelProvider(resolver, c.sentinelWatch())
		if err != nil {
			return nil, err
		}

		// The subscription to +switch-master starts lazily, on the first
		// request, and is released when Traefik cancels the middleware context
		// (see redis.Client). A middleware that never serves traffic holds no
		// goroutine at all.
		logger.warnf("using Redis Sentinel, master set %q, endpoints %v, watch=%v",
			c.Store.Redis.Sentinel.MasterSet, c.Store.Redis.Endpoints, c.sentinelWatch())

		provider = sentinelProvider
	} else {
		staticProvider, err := redis.NewStaticProvider(c.Store.Redis.Endpoints, minSwitchInterval)
		if err != nil {
			return nil, err
		}
		provider = staticProvider
	}

	client, err := redis.NewClient(ctx, clientCfg, tlsCfg, provider, logger)
	if err != nil {
		return nil, err
	}

	return client, nil
}
