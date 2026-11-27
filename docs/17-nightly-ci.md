# Nightly CI

The nightly pipeline (`.github/workflows/nightly.yml`) is the slow half of the
CI: it starts real Traefik + Redis containers, drives traffic through the plugin
and promotes a Redis replica while requests are in flight. The push/pull
pipeline (`.github/workflows/ci.yml`) stays fast and hermetic.

## What runs, and when

| Trigger | When |
| :--- | :--- |
| `schedule` | `27 0 * * *` — every night at 00:27 UTC (03:27 MSK); off the hour, because GitHub queues the `:00` runs |
| `workflow_dispatch` | manually, from the Actions tab or `gh workflow run nightly` |

| Job | Runs | Non-default host ports | Timeout |
| :--- | :--- | :--- | :--- |
| `unit` | `go test -race ./...`, `make yaegi-check` | — | 25 min |
| `integration` | `hack/ci/integration-single.sh`, `hack/ci/integration-cookie.sh`, `hack/ci/integration-acl.sh` | 28100/28180/16391, 28200/28280/16392, 28300/28380/16393 + 26393 | 40 min |
| `chaos` | `hack/ci/chaos-sentinel.sh` | 28400/28480/16394 + 26394 | 30 min |

Every check is non-interactive, waits for readiness instead of sleeping, and
fails with a non-zero exit code. When a job fails, the workflow uploads
`artifacts/` (14 days) and the scripts also print the tail of the stand logs to
the job log.

Requirements on the runner: `bash`, `curl`, `docker` with Compose v2, and the
Docker daemon. The load generator is a `curl` loop, so the stands do not need a
Go toolchain.

## What each stand proves

### `integration-single.sh` — a single Redis instance

* the bucket is drained by the plugin and the limit is applied
  (`limit: 5`, `burst: 10`, `period: 10s` → **10 × `200`, then `429`** — a full
  bucket passes `burst` requests, not `limit`);
* the rejection carries `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`
  and `X-RateLimit-Reset`; after the refill a request is accepted again and
  carries the same headers;
* the state really is shared: at least one `rate:*` key exists in Redis.

### `integration-cookie.sh` — the sources

* `cookie-value`, `cookie-field` (JSON field), `cookie-nested` (dot path),
  `cookie-array` (array element, `users[0].id`), `cookie-jwt` (claim of a JWT held
  by the cookie), `jwt-header` (claim of a JWT in `Authorization: Bearer …`),
  `cookie-fallback` (no cookie → client IP from `X-Forwarded-For`), `header`
  (`X-Api-Key`): each drains its own full bucket, `5 × 200` then `429`;
* two values of the same cookie get two independent buckets;
* `onEmptySource: denyOnError` with `denyOnError: true` → every request is `429`
  and the store is never touched; with `denyOnError: false` → `200` and no key
  either;
* a deliberately broken `cookieName` leaves the middleware **disabled** with the
  reason visible in the Traefik API, and the router answers `404`;
* an out-of-range array index (`{"users":[]}` with `users[0].id`) and a cookie
  that is not a JWT are empty sources: `400` with `onEmptySource: deny`;
* one `rate:*` key per source, and no key for the two empty-source routers.

> A JSON cookie must be URL-encoded (`session=%7B%22tenant%22%3A%22t1%22%7D`):
> Go's HTTP server rejects a raw `"` inside a `Cookie` header with `400` before
> the plugin sees the request.

### `integration-acl.sh` — two independent credential pairs

* the server side really requires both pairs: the master refuses anonymous
  commands (`NOAUTH`) and accepts `app/apppass`; a Sentinel refuses anonymous
  commands and accepts `sentineluser/sentinelpass`;
* with `store.redis.username` set the limiter works end to end
  (`limit: 1`, `burst: 2` → `2 × 200`, then `429` + headers) and the bucket
  (`rate:rl-with-user@file:…`) is on the master;
* with a password only the master answers `WRONGPASS`, which now surfaces
  readably in the Traefik log; `denyOnError: false` keeps serving (`200`, no key),
  `denyOnError: true` fails closed (`429`).

> In this stand the Sentinel ACL user has `+@all` but no channel permission
> (`&*`), so the push channel of the resolver logs
> `SUBSCRIBE: NOPERM No permissions to access a channel` and discovery falls back
> to the pull path. That is tolerated on purpose — the push path is exercised by
> the chaos stand, which has no ACL.

### `chaos-sentinel.sh` — a failover under load

1. a fresh source drains a full bucket (limiter works **before** the failover);
2. a background load starts and, while it runs, the script issues
   `SENTINEL FAILOVER mymaster`;
3. the load keeps running for 10 s after the promoted node reports `role:master`,
   and the whole window must contain **zero 5xx** (with `denyOnError: false` a
   store hiccup fails open, it never turns into a 500), with at least 200
   requests so that "zero 5xx" is not vacuous;
4. requests are still rejected after the switch, a second fresh source drains
   `10 × 200` → `429` against the **new** master, the old master reports
   `role:slave`, the bucket key exists on the new master, and the plugin logged
   `master switched from … to … (generation N)`.

## Reproducing locally

```bash
cd plugin

# single Redis: 10 x 200 -> 429 + headers + shared state
./hack/ci/integration-single.sh

# cookie/header sources (11 routers)
./hack/ci/integration-cookie.sh

# ACL credential pairs: data plane vs sentinel
./hack/ci/integration-acl.sh

# sentinel failover under load: zero 5xx
./hack/ci/chaos-sentinel.sh
```

Each script cleans its own stand up with a `trap` (even on failure) and names its
Compose project uniquely (`drl-nightly-<stand>-<run id>-<pid>`), so parallel runs
do not collide. Ports can be moved with the environment:

```bash
HTTP_PORT=29100 API_PORT=29180 REDIS_PORT=16399 SENTINEL_PORT=26399 \
  ./hack/ci/chaos-sentinel.sh
```

The Compose files parameterise **host** ports only; the container ports stay
`6379`/`:8000`/`:8080` (and `26379` for the Sentinels), exactly as
`example/*.yaml` expects.

## Reading the artifacts

`hack/ci/lib.sh` writes everything into `artifacts/<stand>/`:

| File | Content |
| :--- | :--- |
| `summary.txt` | the checks that passed, with the numbers (drain shape, headers, keys, failover timing) |
| `compose-ps.txt`, `compose-logs.txt` | container states and the full stand logs (collected on failure) |
| `requests-*.txt` | one HTTP status code per line, per phase (`requests-drain.txt`, `requests-during-failover.txt`, …) |
| `response-429.txt`, `response-200.txt`, … | raw response headers of the probed responses |
| `sentinel-state-*.txt` | `SENTINEL get-master-addr-by-name / master / sentinels / replicas` before, after and on failure |
| `redis-keys*.txt` | the `rate:*` keys of the stand |
| `logs-<service>.txt` | the logs of the service a log assertion greps (kept, so the evidence survives) |

On GitHub the same directory is attached to the failed job as
`nightly-integration-logs` / `nightly-chaos-logs`; the job log already contains
the last 40 stand log lines, which is usually enough to see what happened.

## Local reference run (2026-11-27)

| Stand | Result |
| :--- | :--- |
| single | 10 × 200 → 429; `Retry-After: 2`, `X-RateLimit-Limit: 10`, `Remaining: 0`; refill → 200; key `rate:distributed-ratelimit@file:` |
| cookie | 5 × 200 → 429 on all nine source routers (value, JSON field, nested field, array element, JWT from cookie, JWT from header, IP fallback, header); strict → 6 × 429; open → 6 × 200; empty array and non-JWT cookie → 400; typo middleware disabled (`invalid cookieName "bad name"…`), router → 404; keys per source |
| acl | 2 × 200 → 429 with `app/apppass`; `WRONGPASS` readable in the log; `/nouser` → 6 × 200 (fail-open), `/nouser-strict` → 6 × 429 (fail-closed); no key for the broken pair |
| chaos | 6 930 requests across the failover, **0 × 5xx**; Sentinel advertised `172.18.0.4:16394`, `redis-replica` became master after 12 s, `redis-master` became a replica; 10 × 200 → 429 before and after; bucket found on the new master |

## Notes for maintainers

* `hack/ci/lib.sh` is sourced by the stands; it holds the readiness waits, the
  assertion helpers and the artifact writers. Two details are deliberate:
  the logs are captured into a file **before** grepping (a `grep -q` on a pipe
  makes `docker compose logs` die with SIGPIPE, which `pipefail` reports as a
  failed check), and `while read` loops are guarded (an empty input returns 1,
  which `set -e` would treat as a failed stand).
* The drains assert a window that tolerates a token or two of refill, not an
  exact count, so a slow runner cannot produce a false positive.
* The Sentinel advertises the promoted node by IP, so anything that has to run
  inside a container uses the Compose service name; the script checks that the
  advertised IP belongs to the promoted container.
* Making a check deliberately impossible (for example requiring one `5xx` in the
  failover window) makes the script exit `1` with the reason, which is the point
  of the assertions.
