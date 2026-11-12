# distributedRateLimit

[![CI](https://github.com/MichNG/traefik-distributed-rate-limit/actions/workflows/ci.yml/badge.svg)](https://github.com/MichNG/traefik-distributed-rate-limit/actions/workflows/ci.yml)

`github.com/MichNG/traefik-distributed-rate-limit` - a Traefik middleware plugin
(distributed rate limiting on a shared Redis store). Standalone project: no
upstream fork history, module path and plugin name are its own.

Traefik comes with a built-in [rate limiter](https://doc.traefik.io/traefik/middlewares/http/ratelimit/) middleware, but its buckets are local to each Traefik instance: with several Traefik replicas (typically in Kubernetes HA deployments, behind a load balancer), the effective limit is multiplied by the number of replicas.

This plugin keeps the token bucket state in a **shared Redis store**, so the limit is enforced **cluster-wide**, and it follows the [Traefik Hub Distributed RateLimit](https://doc.traefik.io/traefik-hub/api-gateway/reference/routing/http/middlewares/ref-distributed-rate-limit) configuration schema (`limit`, `period`, `burst`, `denyOnError`, `responseHeaders`, `sourceCriterion`, `store.redis.*`).

Supported Redis topologies:

* single instance (with optional failover over a list of endpoints);
* **Redis Sentinel**: the master is discovered automatically (`store.redis.sentinel.masterSet`) and a failover is followed without any 5xx on the data path;
* TLS/mTLS to Redis and to the Sentinels.

> Redis Cluster is not supported yet: configuring `store.redis.cluster` fails the middleware creation with an explicit message.

## Installation

```yaml
# Static configuration
experimental:
  plugins:
    distributedRateLimit:
      moduleName: "github.com/MichNG/traefik-distributed-rate-limit"
      version: "v2.0.0"
```

For local development, mount the plugin directory and use `localPlugins`:

```yaml
experimental:
  localPlugins:
    distributedRateLimit:
      moduleName: github.com/MichNG/traefik-distributed-rate-limit
```

## Configuration

```yaml
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: test-distributedratelimit
  namespace: traefik
spec:
  plugin:
    distributedRateLimit:
      limit: 100
      period: 1s
      burst: 200
      denyOnError: true
      responseHeaders: true
      sourceCriterion:
        ipStrategy:
          excludedIPs:
            - 172.20.176.201
      store:
        redis:
          endpoints:
            - my-release-redis-master.default.svc.cluster.local:6379
          password: "$REDIS_PASSWORD"
          timeout: 500ms
```

File provider equivalent:

```yaml
http:
  middlewares:
    my-rate-limit:
      plugin:
        distributedRateLimit:
          limit: 50
          period: 1s
          burst: 100
          store:
            redis:
              endpoints: ["redis:6379"]
```

### Options

| Field | Description | Default |
| :--- | :--- | :--- |
| `limit` | Number of requests used to define the rate with `period`. `0` means no rate limiting. | `0` |
| `period` | Period of time used to define the rate (`1s`, `1m`, ...). | `1s` |
| `burst` | Maximum number of requests allowed at the very same moment. | `1` |
| `denyOnError` | Return `429` when no decision can be obtained from the store (unreachable Redis, open circuit breaker). `false` lets the request reach the backend. | `true` |
| `responseHeaders` | Inject `X-RateLimit-Limit`, `X-RateLimit-Period`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`. | `false` |
| `store.redis.endpoints` | Redis endpoints (or the Sentinel endpoints in Sentinel mode). | — (required) |
| `store.redis.username` | Username used to connect to Redis (ACL). | `""` |
| `store.redis.password` | Password used to connect to Redis. `$ENV_VAR` is expanded from the environment. | `""` |
| `store.redis.database` | Database index (also valid in Sentinel mode). | `0` |
| `store.redis.timeout` | Timeout applied to dial, read and write operations. | `2s` |
| `store.redis.poolSize` | Maximum number of idle connections kept per node. | `5` |
| `store.redis.maxActiveConns` | Maximum number of connections opened at the same time (in use or idle). The bound is enforced on the client side; while connections are being recycled, Redis may transiently see a few more sockets (an asynchronous TCP close). | `10 * GOMAXPROCS` |
| `store.redis.sentinel.masterSet` | Name of the Sentinel master set. | — (required with Sentinel) |
| `store.redis.sentinel.watch` | Subscribe to the Sentinel `+switch-master` events, so that a failover is applied before the next request fails. When disabled, failovers are still handled, but only when a request hits the demoted master. | `true` |
| `store.redis.sentinel.username` | Username for the Sentinel authentication (may differ from `username`). | `""` |
| `store.redis.sentinel.password` | Password for the Sentinel authentication (may differ from `password`). | `""` |
| `store.redis.idleTimeout` | Suspend the master discovery and close the idle connections after that much inactivity; a later request starts them again (`0` disables it). | `0` |
| `store.redis.tls.caBundle` | Custom CA bundle: a file path or the PEM content itself (`store.redis.tls.ca` is accepted as an alias). | `""` |
| `store.redis.tls.cert` | Client certificate (mTLS), file path or PEM content. | `""` |
| `store.redis.tls.key` | Client key (mTLS), file path or PEM content. | `""` |
| `store.redis.tls.insecureSkipVerify` | Skip the verification of the Redis server certificate. | `false` |
| `sourceCriterion.requestHost` | Use the request host as the source. | `false` |
| `sourceCriterion.requestHeaderName` | Header used to group incoming requests. | `""` |
| `sourceCriterion.cookieName` | **Extension**: group the requests by a cookie value (the cookie is read by the plugin itself, no rewriting middleware needed). | `""` |
| `sourceCriterion.cookieField` | **Extension**: take a string field of a JSON cookie value instead of the whole value. Dot paths are supported (`user.id`), the value is percent-decoded first. | `""` |
| `sourceCriterion.onEmptySource` | **Extension**: what to do when the source cannot be determined (missing cookie/header, absent JSON field, empty value): `shared` (all such requests share one bucket), `clientIP` (fall back to the client IP, combined with `ipStrategy`), `deny` (400 before touching the store), `denyOnError` (behave like a store error: 429 when `denyOnError` is true, and let the request reach the backend when it is false). | `shared` |
| `sourceCriterion.ipStrategy.depth` | Depth in `X-Forwarded-For` used to pick the client IP. | `0` |
| `sourceCriterion.ipStrategy.excludedIPs` | First `X-Forwarded-For` IP not in the list is the client IP. | — |
| `whitelistIPs` | **Extension**: IPs or CIDR ranges that bypass rate limiting entirely. | — |
| `store.redis.breaker.threshold` / `.reattempt` | **Extension**: consecutive store failures opening the circuit breaker, and the reattempt delay (seconds). | `3` / `15s` |

### Redis Sentinel

```yaml
store:
  redis:
    endpoints:
      - sentinel-0:26379
      - sentinel-1:26379
      - sentinel-2:26379
    password: "$REDIS_PASSWORD"
    sentinel:
      masterSet: mymaster
      username: sentinel-user
      password: "$SENTINEL_PASSWORD"
    tls:
      caBundle: /etc/ssl/redis-ca.crt
```

The master is resolved with `SENTINEL get-master-addr-by-name` and tracked through two complementary mechanisms:

1. a dedicated connection subscribes to `+switch-master`, so a failover is applied before the next request fails;
2. if that event is missed, a `READONLY`/`MASTERDOWN`/connection error triggers a single-flight re-resolution (rate limited to protect the Sentinels), and the command is transparently retried on the new master.

Connections are pooled per master generation and dropped on a switch; Lua scripts are re-loaded on the new master (and on `NOSCRIPT`) so that rate limiting never silently disappears after a failover. If the Sentinels are unreachable, the last known master keeps being used.

### Sources: by client IP, header, host or cookie

```yaml
sourceCriterion:
  # one of the following:
  ipStrategy: {depth: 2, excludedIPs: ["10.0.0.0/8"]}   # client IP from X-Forwarded-For
  requestHeaderName: X-Api-Key                          # a header, e.g. set by another middleware
  requestHost: true                                     # the request host
  cookieName: session                                   # a cookie read by the plugin itself
  cookieField: user.id                                  # optional: JSON field of the cookie (dot path)
  onEmptySource: deny                                   # shared | clientIP | deny | denyOnError
```

Notes:

* `cookieName` is validated at creation time: a name that can never match
  (space, `;`, `,`, `=` or control characters) disables the middleware with an
  explicit error instead of silently producing an empty source;
* a JSON cookie arrives **percent-encoded** (quotes are not valid inside a cookie
  value): `session=%7B%22tenant%22%3A%22acme%22%7D`. `cookieField` decodes it
  before parsing.
* when the source is a cookie or a header set by another plugin,
  `onEmptySource: denyOnError` is usually what you want: the request is then
  handled exactly like a store error (429 with `Retry-After`, or pass-through
  when `denyOnError: false`), and Redis is not queried at all for such requests.
* `onEmptySource` covers every "unknown source" case: missing cookie, missing
  header, absent JSON field, empty value. With the default `shared`, all such
  requests share one bucket (`rate:<middleware>:`); with `clientIP` they are
  limited per client IP (set `ipStrategy.depth` so that the real client is used:
  by default Traefik ignores a client-supplied `X-Forwarded-For`, configure
  `forwardedHeaders.trustedIPs`/`insecure` on the entry point); with `deny` they
  are rejected with `400` and never reach Redis.
* A third-party header-rewriting middleware in front of the rate limiter is
  **not recommended**: when the cookie is absent such a plugin usually leaves the
  header untouched, so a client can send its own value, pick its bucket, or even
  bypass the limit by rotating it. Prefer `cookieName` above; the analysis, the
  measurements and a hardening recipe for that chain are in
  [../docs/13-cookie-source-analysis.md](../docs/13-cookie-source-analysis.md).

## Lifecycle

The plugin follows the only lifecycle contract Traefik gives a middleware:

* the Sentinel `+switch-master` subscription starts **lazily, on the first
  request**, so a middleware that never serves traffic holds no goroutine;
* Traefik cancels the context it passed to `New()` when the middleware instance
  is replaced by a configuration reload: the plugin then waits for the in-flight
  commands (bounded by a 3 s grace period) and releases the subscription, the
  connections and the pool. This is visible in the Traefik log
  (`middleware instance retired (context cancelled) ...`);
* with `store.redis.idleTimeout`, an instance that stops receiving traffic also
  suspends its discovery and idle connections, and starts them again on the next
  request.

## Behavior on store errors

* `denyOnError: true` (default) → `429`, and the cause is logged;
* `denyOnError: false` → the request reaches the backend (fail-open, the v1 behavior).

The circuit breaker opens after `store.redis.breaker.threshold` consecutive failures and lets one probe through after `.reattempt` seconds.

Plugin messages are emitted through Traefik's interpreter logger, i.e. they
appear in the Traefik log at **debug** level: set `log.level: DEBUG` to see them
(`[distributed-ratelimit <middleware>] ERROR ...`). Messages are throttled to one
occurrence per template per 5 seconds.

## Troubleshooting

### The limiter silently stops working

With `denyOnError: false`, a request whose rate limit decision cannot be obtained
reaches the backend, and the middleware logs the reason at error level:

```
[distributed-ratelimit my-middleware@file] ERROR no rate limit decision from the
store on node "redis-master:6379": ... authenticating against Redis: WRONGPASS
invalid username-password pair or user is disabled.
```

The usual causes:

| In the log | Cause | Fix |
| :--- | :--- | :--- |
| `WRONGPASS ...` while the log node is the **master** | `store.redis.username` is missing while the Redis ACL requires a username: the plugin sends `AUTH <password>` (the `default` user) | set `store.redis.username` (keep `store.redis.password`) |
| `WRONGPASS ...` on a **Sentinel** node | `store.redis.sentinel.username` / `password` are missing or wrong | set them: they may differ from the data-plane pair |
| `NOAUTH Authentication required.` | the password is missing for that endpoint class | set the corresponding credentials |
| `TLS handshake with ...` | CA/cert/key or `insecureSkipVerify` | check `store.redis.tls.*` (a file path **or** inline PEM; `cert` and `key` must be set together) |
| `rate limit: no decision from the store` **without** a cause | the circuit breaker is open after `store.redis.breaker.threshold` consecutive failures | fix the root cause; the breaker closes after `.reattempt`, or on the first successful command |

With `denyOnError: true` the same failures are visible to the clients as `429`
(with `Retry-After`) instead of being silently passed through, which is usually
the better default for a rate limiter.

Two credential pairs are independent:

```yaml
store:
  redis:
    username: app             # AUTH app <password> against the master
    password: apppass
    sentinel:
      username: sentineluser  # AUTH sentineluser <password> against the Sentinels
      password: sentinelpass
```

**The address in the log is the master as reported by Sentinel**, and it may use a
different port than the Sentinel endpoints: with
`endpoints: ["sentinel-host:16379"]` and a Redis listening on `6379`, the log
legitimately shows `sentinel-host:6379`. The port always comes from
`SENTINEL get-master-addr-by-name`, never from the `endpoints` list.

A stand reproducing the ACL case (master behind an ACL user, Sentinels behind
their own credentials) is `docker-compose.acl.yaml` (`make integration-acl`).

## Credits

* The source-strategy helpers (`internal/ip`, `internal/utils`) are derived from
  [Traefik Proxy](https://github.com/traefik/traefik) (MIT, Copyright (c) Traefik Labs).
* The GCRA Lua script follows
  [redis-gcra](https://github.com/rwz/redis-gcra) by Pavel Pravosud (MIT).
* The configurable options follow the
  [Traefik Hub Distributed RateLimit](https://doc.traefik.io/traefik-hub/api-gateway/reference/routing/http/middlewares/ref-distributed-rate-limit)
  schema; the extensions of this plugin are marked as such.

## Migration from v1

See [docs/MIGRATION.md](docs/MIGRATION.md). In short: the flat keys (`average`, `redisAddress`, `redisDb`, `redisPassword`, `redisConnectionTimeout`, `breakerThreshold`, `breakerReattempt`) are still accepted and mapped onto the new schema with a deprecation warning, but **`denyOnError` defaults to `true`**, which changes the behavior when Redis is unavailable.

## Yaegi constraints

The plugin is interpreted by Traefik, and the interpreter is old on supported
Traefik releases (3.5 ships yaegi v0.16.1). Two rules follow:

* the runtime code may **only import the Go standard library** (the hand-written
  RESP client replaces `go-redis`, which is not interpretable);
* some constructs that compile with `go build` crash the interpreter or silently
  misbehave. Known cases:
  * calling a **variadic function stored in a struct field**
    (`type S struct{ logf func(string, ...interface{}) }`) makes yaegi 0.16.1
    panic while compiling the package (`index out of range` in
    `interp.run.go`, `callBin`). Logging seams are therefore interfaces
    (`internal/redis.Logger`), not function fields;
  * a multi-assignment of untyped `nil` to several **struct fields**
    (`r.a, r.b = nil, nil`) makes it panic with `reflect: New(nil)` at run time;
  * **forwarding a multi-value call** out of the exported `New`
    (`return newInner(...)`) **loses the error** across the
    host/interpreted boundary: Traefik then reports `invalid handler type: <nil>`
    instead of the configuration error. `New` therefore builds its results
    explicitly. `make yaegi-check` covers all three cases.

`make yaegi-check` compiles the plugin with yaegi v0.16.1 and drives a few
requests through it (self-contained, no Redis needed): run it before every
release, it catches this class of incompatibility in seconds.

## Development

`testify` and `mapstructure` are test-only dependencies: the latter mirrors
exactly the decoder that Traefik's plugin loader applies to the configuration.

Examples (`example/`): `dynamic.yaml` (client IP), `dynamic-header.yaml`
(header), `dynamic-sentinel.yaml` (Sentinel), `dynamic-cookie.yaml` (cookie and
header sources, with every `onEmptySource` policy),
`dynamic-sentinel-acl.yaml` (Sentinel + Redis ACL, with and without
`store.redis.username`).

```bash
./run-tests.sh -race      # gofmt + go vet + go test (works inside restricted sandboxes)
make yaegi-check          # loads the plugin with yaegi v0.16.1 (as Traefik does)
docker compose -f docker-compose.yaml up -d           # single Redis instance
docker compose -f docker-compose.sentinel.yaml up -d  # master + replica + 3 Sentinels
```

Benchmark (vegeta):

```bash
echo "GET http://localhost:8000/" | vegeta attack -duration=5s -rate=200 | tee results.bin | vegeta report
```
