#!/usr/bin/env bash
# Nightly integration stand 3/4: Sentinels + Redis behind ACLs
# (docker-compose.acl.yaml).
#
# The stand reproduces a production setup with two independent credential pairs:
#   store.redis.username/password          -> the Redis data plane (app/apppass)
#   store.redis.sentinel.username/password -> the Sentinels (sentineluser/sentinelpass)
#
# Three routers of example/dynamic-sentinel-acl.yaml are checked: with the ACL
# user (the limiter works), with a password only (AUTH as "default" is refused
# with WRONGPASS, so the limiter fails open and no bucket is ever stored), and
# the same broken credentials with denyOnError=true (fail closed).
#
#   HTTP_PORT=28300 API_PORT=28380 REDIS_PORT=16393 SENTINEL_PORT=26393 \
#     hack/ci/integration-acl.sh
set -euo pipefail

STAND="integration-acl"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.acl.yaml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-drl-nightly-acl-${GITHUB_RUN_ID:-local}-$$}"
HTTP_PORT="${HTTP_PORT:-28300}"
API_PORT="${API_PORT:-28380}"
REDIS_PORT="${REDIS_PORT:-16393}"
SENTINEL_PORT="${SENTINEL_PORT:-26393}"
export HTTP_PORT API_PORT REDIS_PORT SENTINEL_PORT

# shellcheck source=hack/ci/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

MASTER_USER="app"
MASTER_PASSWORD="apppass"
SENTINEL_USER="sentineluser"
SENTINEL_PASSWORD="sentinelpass"

# rl-with-user: limit=1, burst=2, period=1s - a full bucket passes two requests.
MIN_PASS=2
MAX_PASS=4

on_exit() {
    local status=$?
    if ((status != 0)); then
        warn "stand ${STAND}: failed with exit ${status}, collecting the state"
        dump_stand_state
    fi
    stand_down
    exit "${status}"
}

master_cli() { # [redis-cli args...]
    compose exec -T redis-master redis-cli -p "${REDIS_PORT}" "$@" 2>&1 || true
}

sentinel_cli() { # SERVICE [redis-cli args...]
    local service="$1"
    shift
    compose exec -T "${service}" redis-cli -p 26379 "$@" 2>&1 || true
}

assert_redis_rejects_anonymous() { # OUTPUT DESCRIPTION
    grep -qE 'NOAUTH|WRONGPASS|ERR Client sent AUTH' <<<"$1" ||
        die "stand ${STAND}: ${2}: an unauthenticated command was accepted: ${1}"
    log "stand ${STAND}: ${2}: authentication is required ($(head -n 1 <<<"$1" | tr -d '\r'))"
}

assert_redis_pong() { # OUTPUT DESCRIPTION
    grep -qE '(^|\r?\n)PONG' <<<"$1" || die "stand ${STAND}: ${2}: expected PONG, got: ${1}"
    log "stand ${STAND}: ${2}: PONG"
}

main() {
    trap on_exit EXIT
    stand_up
    wait_tcp_port 127.0.0.1 "${REDIS_PORT}" 90 "redis master host port"
    wait_tcp_port 127.0.0.1 "${SENTINEL_PORT}" 90 "sentinel host port"
    wait_http_status "http://127.0.0.1:${API_PORT}/api/overview" 120 '^200$' "traefik api"

    local url="http://127.0.0.1:${HTTP_PORT}"
    wait_http_status "${url}/" 90 '^(200|429)$' "plugin route" \
        -H 'Host: localhost' -H 'X-Sm-User-Id: probe'

    init_summary
    summary "stand=${STAND} project=${COMPOSE_PROJECT} http=${HTTP_PORT} api=${API_PORT} redis=${REDIS_PORT} sentinel=${SENTINEL_PORT}"

    # 1. The two credential pairs are really independent on the server side.
    assert_redis_rejects_anonymous "$(master_cli PING)" "master requires an ACL user"
    assert_redis_pong "$(master_cli --user "${MASTER_USER}" -a "${MASTER_PASSWORD}" --no-auth-warning PING)" \
        "master accepts ${MASTER_USER}"
    assert_redis_rejects_anonymous "$(sentinel_cli sentinel-1 PING)" "sentinel requires its own user"
    assert_redis_pong "$(sentinel_cli sentinel-1 --user "${SENTINEL_USER}" -a "${SENTINEL_PASSWORD}" --no-auth-warning PING)" \
        "sentinel accepts ${SENTINEL_USER}"
    assert_log_contains reverse-proxy 'using Redis Sentinel, master set "mymaster"' \
        "sentinels are configured for discovery"

    # 2. With store.redis.username the limiter works end to end.
    # -p matters: this stand moves the Redis port, so the default 6379 would
    # silently talk to nothing.
    redis_flush_buckets redis-master -p "${REDIS_PORT}" --user "${MASTER_USER}" -a "${MASTER_PASSWORD}" --no-auth-warning
    check_drain acl-with-user "${url}/" 15 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' -H 'X-Sm-User-Id: nightly-acl'

    local rejected
    rejected="$(http_headers "${url}/" -H 'Host: localhost' -H 'X-Sm-User-Id: nightly-acl')"
    printf '%s\n' "${rejected}" >"$(tmp_file response-429-with-user.txt)"
    assert_status_line "${rejected}" 429 "with-acl-user rate limited response"
    assert_header "${rejected}" "Retry-After" "[0-9]+" "with-acl-user rate limited response"
    assert_header "${rejected}" "X-Ratelimit-Limit" "[0-9]+" "with-acl-user rate limited response"

    # 3. Password only: the master refuses AUTH as "default", the plugin reports
    #    it and - with denyOnError=false - keeps serving. No bucket is stored.
    local no_user
    no_user="$(tmp_file requests-without-user.txt)"
    run_requests "${no_user}" 6 "${url}/nouser" -H 'Host: localhost' -H 'X-Sm-User-Id: nightly-nouser'
    show_histogram "${no_user}" "without-user (password only, denyOnError=false)"
    assert_all_codes "${no_user}" 200 "missing store.redis.username fails open"
    assert_log_contains reverse-proxy 'WRONGPASS' "the authentication failure is readable"

    # 4. The same broken credentials with denyOnError=true fail closed.
    local strict
    strict="$(tmp_file requests-without-user-strict.txt)"
    run_requests "${strict}" 6 "${url}/nouser-strict" -H 'Host: localhost' -H 'X-Sm-User-Id: nightly-nouser'
    show_histogram "${strict}" "without-user-strict (password only, denyOnError=true)"
    assert_all_codes "${strict}" 429 "missing store.redis.username fails closed"

    # 5. Only the working router left a bucket behind.
    local keys
    keys="$(redis_keys redis-master -p "${REDIS_PORT}" --user "${MASTER_USER}" -a "${MASTER_PASSWORD}" --no-auth-warning)"
    printf '%s\n' "${keys}" >"$(tmp_file redis-keys.txt)"
    assert_key_present "${keys}" 'rl-with-user@file:nightly-acl' "ACL data plane"
    assert_key_absent "${keys}" 'rl-without-user' "broken credentials never stored a bucket"

    log "stand ${STAND}: OK"
    summary "result: OK"
}

main "$@"
