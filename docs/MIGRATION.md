# Migration v1 → v2

v2 renames the plugin to **`distributedRateLimit`** (`github.com/MichNG/traefik-distributed-rate-limit`)
and adopts the
[Traefik Hub Distributed RateLimit](https://doc.traefik.io/traefik-hub/api-gateway/reference/routing/http/middlewares/ref-distributed-rate-limit)
configuration schema. The v1 keys keep working (with a deprecation warning in
the Traefik log), so an upgrade does not require a configuration change — but
two behavior changes must be reviewed.

## 1. Configuration mapping

| v1 | v2 | Notes |
| :--- | :--- | :--- |
| `average` | `limit` | `0` still means "no rate limiting" |
| `period` (seconds, e.g. `10`) | `period` (duration, e.g. `10s`) | a bare number is still read as seconds |
| `burst` | `burst` | unchanged |
| `redisAddress` | `store.redis.endpoints` | now a list |
| `redisDb` | `store.redis.database` | |
| `redisPassword` | `store.redis.password` | `$ENV_VAR` expansion still supported |
| `redisConnectionTimeout` | `store.redis.timeout` | |
| `breakerThreshold` | `store.redis.breaker.threshold` | extension, not part of the Hub schema |
| `breakerReattempt` | `store.redis.breaker.reattempt` | extension |
| `whitelistIPs` | `whitelistIPs` | extension, unchanged |
| — | `store.redis.username` | new |
| — | `store.redis.sentinel.*` | new |
| — | `store.redis.tls.*` | new |
| — | `denyOnError` | new, **defaults to `true`** |
| — | `responseHeaders` | new |

New keys take precedence: if any `store.redis` field is set, the deprecated flat
keys are ignored (a warning is logged for each of them).

## 2. Behavior change: `denyOnError`

In v1, when Redis was unreachable the plugin **silently let every request
through** (fail-open) — the rate limit disappeared, without any log.

In v2 the default is the Traefik Hub policy, `denyOnError: true`: if no decision
can be obtained from the store, the middleware answers `429`.

* To keep the v1 behavior, set `denyOnError: false` explicitly.
* The 429 is deliberate: a rate limiter must not emit `5xx` and break the
  backend SLO.

Failures are now logged (`[distributed-ratelimit <middleware>] ERROR ...`), so
the degradation is visible.

## 3. Behavior change: Redis key format

v1 used `rate_<middleware><source>`; v2 aligns on the Traefik OSS format,
`rate:<middleware>:<source>`. After an upgrade, counters start from scratch
(one "free" burst per source). Roll out with this in mind if you rely on a very
low `limit`.

## 3.1 Extensions added in v2

These keys are **not** part of the Traefik Hub schema: they are extensions of
this plugin. All of them are optional, and the defaults keep the v1 behaviour.

| Key | Meaning | Default |
| :--- | :--- | :--- |
| `store.redis.poolSize` | maximum number of idle connections kept per node | `5` |
| `store.redis.maxActiveConns` | maximum number of connections opened at the same time (in use or idle) | `10 × GOMAXPROCS` |
| `store.redis.idleTimeout` | suspend the master discovery and close the idle connections after that much inactivity; a later request starts them again | `0` (disabled) |
| `store.redis.breaker.threshold` / `.reattempt` | consecutive store failures opening the circuit breaker, and the reattempt delay in seconds (replaces `breakerThreshold` / `breakerReattempt`) | `3` / `15s` |
| `store.redis.sentinel.watch` | subscribe to `+switch-master`; when `false`, a failover is only detected by the failing request (pull) | `true` |
| `whitelistIPs` | addresses or CIDR ranges that bypass rate limiting entirely | — |
| `sourceCriterion.cookieName` | group the requests by a cookie value, read by the plugin itself | `""` |
| `sourceCriterion.cookieField` | use a string field (dot path, e.g. `user.id`) of a JSON cookie value instead of the whole value | `""` |
| `sourceCriterion.onEmptySource` | what to do when the source cannot be determined: `shared` (one bucket for every unidentified request), `clientIP` (fall back to the client IP, combined with `sourceCriterion.ipStrategy`), `deny` (400 before touching the store), `denyOnError` (behave like a store error: 429 when `denyOnError` is true, pass-through when it is false) | `shared` |

## 4. Rollout checklist

1. Upgrade the static configuration: `experimental.plugins.distributedRateLimit`
   (the dynamic configuration key must be renamed too).
2. Decide on `denyOnError` **before** the rollout, and make sure your
   alerting tolerates `429`s if you keep the default.
3. If you use Sentinel, add `store.redis.sentinel.masterSet` and point
   `store.redis.endpoints` at the Sentinels.
4. If you use TLS, add `store.redis.tls.*` (`caBundle`/`ca`, `cert`, `key`,
   `insecureSkipVerify`).
5. Remove the deprecated keys once the warnings are gone from the logs; they
   will be dropped in v3.
6. Review the extensions above (`3.1`) if you rate limit by a cookie or a header:
   `sourceCriterion.onEmptySource` is the option that decides what happens to
   requests whose source is unknown - with the v1-style `shared` default they all
   end up in one bucket, which is rarely what you want for per-tenant limits.
