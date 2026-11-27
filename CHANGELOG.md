# Changelog

## 1.1.0

### Added

* `sourceCriterion.cookieField` now walks arrays: a path may address an element by
  index in either form (`users.0.id`, `users[0].id`, `m[1][0]`), and the index also
  resolves against an object with numeric keys;
* `cookieField` accepts scalars, not only strings: a number keeps the exact text of
  the payload (a snowflake id is no longer rounded through a `float64`), and a
  boolean is read as `true`/`false`;
* `sourceCriterion.jwtClaim`: use a claim of a JWT as the source key. The token is
  read from the cookie (optionally from a JSON field of it) or from
  `requestHeaderName` (`Authorization: Bearer <jwt>`, prefix optional); the claim
  may be a dot path or an array index. **The signature is not verified** - the
  README explains when that is acceptable and when it is not;
* `cookieField` and `jwtClaim` are validated at creation time: a path that can
  never resolve (`items[x]`, `user..id`, `roles[0`) disables the middleware with an
  explicit error instead of silently producing an empty source;
* a p99 benchmark: in-process benchmarks of `ServeHTTP`
  (`go test -bench . -benchmem`) plus an end-to-end harness
  (`hack/bench/`, `make bench-e2e`) that measures one and two Traefik replicas on a
  shared Redis store. Results: `docs/16-benchmark-p99.md`;
* a nightly CI workflow (`.github/workflows/nightly.yml`, `make integration-nightly`):
  the unit suite, the Yaegi check and the docker stands (single Redis, cookie,
  ACL/Sentinel) plus a chaos scenario that fails a Sentinel master over under load
  and requires **zero** 5xx. See `docs/17-nightly-ci.md`.

### Changed

* the default `store.redis.poolSize` is now `maxActiveConns` instead of a fixed `5`.
  With 5 idle slots a burst of concurrent requests closed and re-dialed almost every
  connection: the benchmark measured 15 733 new connections in 12 s, then
  `connect: cannot assign requested address` (ephemeral port exhaustion), an open
  breaker and 137 996 × `429` out of 185 028 requests. An explicit `poolSize` keeps
  the old behaviour, including its cost;
* a non-string, non-scalar field of a JSON cookie payload (an object or an array)
  still yields an empty source, but a number or a boolean is now used as the key;
  if you relied on such a cookie being rejected by `onEmptySource: deny`, the
  request now passes instead of being rejected - pin the claim in `cookieField` to
  a scalar field to keep the old behaviour;
* `example/dynamic-cookie.yaml` gained `/cookie-array`, `/cookie-jwt` and
  `/jwt-header` routers, and `make integration-cookie` now exercises 11 routers.

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
