#!/usr/bin/env bash
# Nightly integration stand 2/4: cookie/header sources (docker-compose.cookie.yaml).
#
# Eleven routers of example/dynamic-cookie.yaml are exercised: the source of the
# bucket is a cookie value, a JSON field of a cookie, a nested JSON field, an
# array element of the payload, a claim of a JWT (from a cookie and from the
# Authorization header), the client IP (fallback), a header, and the two
# empty-source policies. A router with a deliberately broken cookieName must
# leave the middleware disabled with an explicit error.
#
#   HTTP_PORT=28200 API_PORT=28280 REDIS_PORT=16392 hack/ci/integration-cookie.sh
set -euo pipefail

STAND="integration-cookie"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.cookie.yaml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-drl-nightly-cookie-${GITHUB_RUN_ID:-local}-$$}"
HTTP_PORT="${HTTP_PORT:-28200}"
API_PORT="${API_PORT:-28280}"
REDIS_PORT="${REDIS_PORT:-16392}"
export HTTP_PORT API_PORT REDIS_PORT

# shellcheck source=hack/ci/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# Every router below uses limit=3, burst=5, period=10s, so a fresh bucket passes
# five requests and the sixth is rejected.
MIN_PASS=4
MAX_PASS=6

# JSON cookies must be URL-encoded: Go's HTTP server rejects a raw '"' inside a
# Cookie header with 400 before the plugin ever sees the request.
COOKIE_FIELD='session=%7B%22tenant%22%3A%22t1%22%7D'
COOKIE_NESTED='_sm_session=%7B%22user%22%3A%7B%22id%22%3A%22u9%22%7D%7D'
COOKIE_ARRAY='_sm_session=%7B%22users%22%3A%5B%7B%22id%22%3A%22uA%22%7D%5D%7D'
COOKIE_ARRAY_EMPTY='_sm_session=%7B%22users%22%3A%5B%5D%7D'
# {"sub":"nightly-jwt"} signed with a dummy value: the plugin reads the claim
# without verifying the signature (see the README, "Security: JWT claims").
JWT='eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJuaWdodGx5LWp3dCJ9.c2ln'

on_exit() {
    local status=$?
    if ((status != 0)); then
        warn "stand ${STAND}: failed with exit ${status}, collecting the state"
        dump_stand_state
    fi
    stand_down
    exit "${status}"
}

main() {
    trap on_exit EXIT
    stand_up
    wait_tcp_port 127.0.0.1 "${REDIS_PORT}" 60 "redis host port"
    wait_http_status "http://127.0.0.1:${API_PORT}/api/overview" 90 '^200$' "traefik api"

    local url="http://127.0.0.1:${HTTP_PORT}"
    wait_http_status "${url}/cookie-value" 60 '^(200|429)$' "cookie route" \
        -H 'Host: localhost' --cookie '_sm_user_id=probe'

    init_summary
    summary "stand=${STAND} project=${COMPOSE_PROJECT} http=${HTTP_PORT} api=${API_PORT} redis=${REDIS_PORT}"

    # The readiness probe spent a token of its own source; start from full buckets.
    redis_flush_buckets redis

    # 1. The bucket key follows the source.
    check_drain cookie-value "${url}/cookie-value" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' --cookie '_sm_user_id=u1'
    check_drain cookie-field "${url}/cookie-field" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' --cookie "${COOKIE_FIELD}"
    check_drain cookie-nested "${url}/cookie-nested" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' --cookie "${COOKIE_NESTED}"
    check_drain cookie-array "${url}/cookie-array" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' --cookie "${COOKIE_ARRAY}"
    check_drain cookie-jwt "${url}/cookie-jwt" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' --cookie "_sm_token=${JWT}"
    check_drain jwt-header "${url}/jwt-header" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' -H "Authorization: Bearer ${JWT}"
    check_drain header "${url}/header" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' -H 'X-Api-Key: nightly-header'

    # 2. No cookie with onEmptySource=clientIP: the fallback uses X-Forwarded-For.
    check_drain cookie-fallback "${url}/cookie-fallback" 20 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' -H 'X-Forwarded-For: 203.0.113.7'

    # 3. A second source value gets its own, still full bucket.
    local other
    other="$(tmp_file requests-cookie-other-source.txt)"
    run_requests "${other}" 3 "${url}/cookie-value" -H 'Host: localhost' --cookie '_sm_user_id=u2'
    assert_all_codes "${other}" 200 "second source u2 has its own bucket"

    # 4. Empty source, denyOnError=true: rejected without touching the store.
    local strict
    strict="$(tmp_file requests-cookie-strict.txt)"
    run_requests "${strict}" 6 "${url}/cookie-strict" -H 'Host: localhost'
    show_histogram "${strict}" "cookie-strict (no cookie, denyOnError=true)"
    assert_all_codes "${strict}" 429 "cookie-strict without a cookie"

    # 5. Empty source, denyOnError=false: fail-open, the request reaches the backend.
    local open
    open="$(tmp_file requests-cookie-open.txt)"
    run_requests "${open}" 6 "${url}/cookie-open" -H 'Host: localhost'
    show_histogram "${open}" "cookie-open (no cookie, denyOnError=false)"
    assert_all_codes "${open}" 200 "cookie-open without a cookie"

    # 5b. An empty source stays an empty source for the new path forms: an empty
    #     array, and a cookie that is not a JWT.
    local arrayjwt
    arrayjwt="$(tmp_file requests-cookie-array-empty.txt)"
    run_requests "${arrayjwt}" 2 "${url}/cookie-array" -H 'Host: localhost' --cookie "${COOKIE_ARRAY_EMPTY}"
    assert_all_codes "${arrayjwt}" 400 "cookie-array with an out-of-range index"

    arrayjwt="$(tmp_file requests-cookie-jwt-invalid.txt)"
    run_requests "${arrayjwt}" 2 "${url}/cookie-jwt" -H 'Host: localhost' --cookie '_sm_token=not-a-jwt'
    assert_all_codes "${arrayjwt}" 400 "cookie-jwt with a cookie that is not a JWT"

    # 6. Response headers on the cookie-field router (responseHeaders: true).
    local rejected
    rejected="$(http_headers "${url}/cookie-field" -H 'Host: localhost' --cookie "${COOKIE_FIELD}")"
    printf '%s\n' "${rejected}" >"$(tmp_file response-429-cookie-field.txt)"
    assert_status_line "${rejected}" 429 "cookie-field rate limited response"
    assert_header "${rejected}" "Retry-After" "[0-9]+" "cookie-field rate limited response"
    assert_header "${rejected}" "X-Ratelimit-Limit" "[0-9]+" "cookie-field rate limited response"
    assert_header "${rejected}" "X-Ratelimit-Remaining" "0" "cookie-field rate limited response"

    # 7. A broken cookieName must disable the middleware with the reason, and the
    #    router must not route to the backend.
    assert_middleware_disabled "${API_PORT}" "rl-cookie-typo@file" "invalid cookieName" "typo cookieName"
    local typo code
    typo="$(tmp_file requests-cookie-typo.txt)"
    run_requests "${typo}" 1 "${url}/cookie-typo" -H 'Host: localhost'
    code="$(head -n 1 "${typo}")"
    [[ "${code}" == "404" ]] || die "stand ${STAND}: a router with a disabled middleware answered ${code}, expected 404"
    log "stand ${STAND}: cookie-typo: middleware disabled, request answered 404"

    # 8. One key per source, and no key when the store was never used.
    local keys
    keys="$(redis_keys redis)"
    printf '%s\n' "${keys}" >"$(tmp_file redis-keys.txt)"
    assert_key_present "${keys}" 'rl-cookie-value@file:u1' "cookie value source"
    assert_key_present "${keys}" 'rl-cookie-value@file:u2' "second cookie value source"
    assert_key_present "${keys}" 'rl-cookie-field@file:t1' "JSON field source"
    assert_key_present "${keys}" 'rl-cookie-nested@file:u9' "nested JSON field source"
    assert_key_present "${keys}" 'rl-cookie-fallback@file:203.0.113.7' "client IP fallback source"
    assert_key_present "${keys}" 'rl-cookie-array@file:uA' "array element source"
    assert_key_present "${keys}" 'rl-cookie-jwt@file:nightly-jwt' "JWT claim source (cookie)"
    assert_key_present "${keys}" 'rl-jwt-header@file:nightly-jwt' "JWT claim source (header)"
    assert_key_present "${keys}" 'rl-header@file:nightly-header' "header source"
    assert_key_absent "${keys}" 'rl-cookie-strict' "cookie-strict never reached the store"
    assert_key_absent "${keys}" 'rl-cookie-open' "cookie-open never reached the store"

    log "stand ${STAND}: OK"
    summary "result: OK"
}

main "$@"
