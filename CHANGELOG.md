# Changelog

## 1.0.1 (2026-11-12)

### Fixed

* store errors keep their message when they cross the Yaegi boundary: a RESP
  error reply is a concrete interpreted type, and wrapping it with `%w` rendered
  `%!w(stdlib._fmt_Stringer=...)` instead of the server text. An authentication
  failure is now reported as
  `authenticating against Redis: WRONGPASS invalid username-password pair or user is disabled.`,
  for the data plane and for the Sentinels (`make yaegi-check` guards this).

### Added

* troubleshooting section in the README: credential pairs, readable store
  errors, and why the master reported by Sentinel can use a different port than
  the sentinel endpoints;
* `docker-compose.acl.yaml` (`make integration-acl`): a Sentinels + Redis ACL
  stand, with and without `store.redis.username`.

## 1.0.0

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
