# Changelog

## 2.0.0

Initial release of `traefik-distributed-rate-limit`, a standalone Traefik
middleware plugin.

* distributed token bucket on a shared Redis store (single instance, endpoint
  list, Redis Sentinel with automatic master discovery and failover handling);
* Traefik Hub Distributed RateLimit configuration schema
  (`limit`, `period`, `burst`, `denyOnError`, `responseHeaders`,
  `sourceCriterion`, `store.redis.*`) with a compatibility layer for the legacy
  flat keys (`average`, `redisAddress`, ...);
* TLS/mTLS to Redis and to the Sentinels (`store.redis.tls.*`, semantics of the
  Traefik OSS client TLS);
* sources: client IP (`ipStrategy`), request host, header, cookie
  (`cookieName`, optional JSON `cookieField`), with an explicit policy for an
  unknown source (`onEmptySource`: `shared` | `clientIP` | `deny` | `denyOnError`);
* predictable behaviour on store failures (`denyOnError`, circuit breaker) and
  `X-RateLimit-*` response headers;
* correct lifecycle inside Traefik: lazy discovery start, release of every
  resource when the middleware instance is retired by a configuration reload,
  optional idle stop;
* stdlib-only runtime (required by Traefik's Yaegi interpreter) and
  `make yaegi-check` to catch interpreter incompatibilities before a release.
