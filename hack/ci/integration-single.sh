#!/usr/bin/env bash
# Nightly integration stand 1/4: a single Redis instance (docker-compose.yaml).
#
# Checks that the plugin enforces the limit cluster-wide and decorates the
# responses with X-RateLimit-* / Retry-After.
#
# Host ports come from the environment, so several stands can run at the same
# time: the container ports stay the ones the example configuration expects
# (redis:6379 inside the network, :8000 for the entrypoint).
#
#   HTTP_PORT=28100 API_PORT=28180 REDIS_PORT=16391 hack/ci/integration-single.sh
set -euo pipefail

STAND="integration-single"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yaml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-drl-nightly-single-${GITHUB_RUN_ID:-local}-$$}"
HTTP_PORT="${HTTP_PORT:-28100}"
API_PORT="${API_PORT:-28180}"
REDIS_PORT="${REDIS_PORT:-16391}"
# The compose file interpolates these from the environment.
export HTTP_PORT API_PORT REDIS_PORT

# shellcheck source=hack/ci/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

on_exit() {
    local status=$?
    if ((status != 0)); then
        warn "stand ${STAND}: failed with exit ${status}, collecting the state"
        dump_stand_state
    fi
    stand_down
    exit "${status}"
}

# The example uses limit=5, burst=10, period=10s: a full bucket passes 10
# requests (= burst), the 11th is rejected. The bucket is flushed before the
# drain, so the probes above cannot skew the count; the window only tolerates a
# token or two refilled while the loop runs.
MIN_PASS=8
MAX_PASS=12

main() {
    trap on_exit EXIT
    stand_up
    wait_tcp_port 127.0.0.1 "${REDIS_PORT}" 60 "redis host port"
    wait_http_status "http://127.0.0.1:${API_PORT}/api/overview" 90 '^200$' "traefik api"

    local url="http://127.0.0.1:${HTTP_PORT}/"
    # A 5xx here means Traefik could not create the plugin middleware.
    wait_http_status "${url}" 60 '^(200|429)$' "plugin route" -H 'Host: localhost'

    init_summary
    summary "stand=${STAND} project=${COMPOSE_PROJECT} http=${HTTP_PORT} api=${API_PORT} redis=${REDIS_PORT}"

    # 1. Drain the bucket: burst requests pass, then the limit applies.
    local drain
    drain="$(tmp_file requests-drain.txt)"
    redis_flush_buckets redis
    run_requests "${drain}" 20 "${url}" -H 'Host: localhost'
    show_histogram "${drain}" "drain (limit 5, burst 10, period 10s)"
    assert_no_5xx "${drain}" "drain"
    assert_no_transport_errors "${drain}" "drain"
    assert_drained "${drain}" "${MIN_PASS}" "${MAX_PASS}" "drain"

    # 2. The rejection carries the headers clients are expected to read.
    local rejected headers
    rejected="$(http_headers "${url}" -H 'Host: localhost')"
    printf '%s\n' "${rejected}" >"$(tmp_file response-429.txt)"
    assert_status_line "${rejected}" 429 "rate limited response"
    assert_header "${rejected}" "Retry-After" "[0-9]+" "rate limited response"
    assert_header "${rejected}" "X-Ratelimit-Limit" "[0-9]+" "rate limited response"
    assert_header "${rejected}" "X-Ratelimit-Remaining" "0" "rate limited response"
    assert_header "${rejected}" "X-Ratelimit-Reset" "[0-9]+" "rate limited response"

    # 3. After the refill a request passes again, and it carries the headers too.
    sleep 6
    local passed
    passed="$(http_headers "${url}" -H 'Host: localhost')"
    printf '%s\n' "${passed}" >"$(tmp_file response-200.txt)"
    assert_status_line "${passed}" 200 "refilled request"
    assert_header "${passed}" "X-Ratelimit-Limit" "[0-9]+" "refilled request"
    assert_header "${passed}" "X-Ratelimit-Remaining" "[0-9]+" "refilled request"

    # 4. The bucket really lives in Redis, not in the plugin process.
    local keys
    keys="$(compose exec -T redis redis-cli --scan --pattern 'rate:*' 2>/dev/null | tr -d '\r' || true)"
    printf '%s\n' "${keys}" >"$(tmp_file redis-keys.txt)"
    [[ -n "${keys}" ]] || die "stand ${STAND}: no rate:* key in redis - the state is not shared"
    log "stand ${STAND}: redis keys: $(printf '%s' "${keys}" | tr '\n' ' ')"
    summary "redis keys: $(printf '%s' "${keys}" | tr '\n' ' ')"

    log "stand ${STAND}: OK"
    summary "result: OK"
}

main "$@"
