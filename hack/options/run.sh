#!/usr/bin/env bash
#
# Options stand: loads every file of example/options/ through the file provider
# in directory mode and checks three things.
#
#   1. every "positive" middleware of the directory is created (the API reports
#      it enabled, with no error) - each option and each variant of its value is
#      covered by at least one middleware;
#   2. every "negative" configuration is refused with the documented message;
#   3. a few options are exercised for real: the burst/refill of a drain, the
#      bucket key of every source criterion, the whitelist bypass, the ACL
#      credentials (including $ENV_VAR expansion), the database index, the
#      failover over an endpoint list, the deprecation warnings of the legacy
#      keys, and the store error of an unreachable Redis.
#
# The middleware names are the contract between this script and the examples:
# they are listed below, so a renamed example fails the stand.
#
# Requirements: docker (compose), curl, openssl.
#
#   HTTP_PORT=29000 API_PORT=29080 ./hack/options/run.sh
#
# Artifacts (also on failure): artifacts/options/ - summary.txt, the middleware
# JSON, the request traces, the Redis keys, the Traefik log, the fixtures.
set -euo pipefail

STAND="options"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.options.yaml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-drl-options-${GITHUB_RUN_ID:-local}-$$}"
HTTP_PORT="${HTTP_PORT:-29000}"
API_PORT="${API_PORT:-29080}"
export HTTP_PORT API_PORT

# The ACL users of the generated redis.conf (see generate_fixtures).
REDIS_PASSWORD="optionspass"
APP_PASSWORD="apppass"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
ARTIFACTS="${ROOT}/artifacts/options"
CERTS="${ARTIFACTS}/certs"
SUMMARY="${ARTIFACTS}/summary.txt"

ENABLED_COUNT=0
DISABLED_COUNT=0
BEHAVIOUR_COUNT=0

cd "${ROOT}"

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
log() { printf '[%s] stand %s: %s\n' "$(date +%H:%M:%S)" "${STAND}" "$*"; }
warn() { printf '[%s] stand %s: WARNING: %s\n' "$(date +%H:%M:%S)" "${STAND}" "$*" >&2; }

die() {
    printf '[%s] stand %s: ERROR: %s\n' "$(date +%H:%M:%S)" "${STAND}" "$*" >&2
    exit 1
}

summary() { printf '%s\n' "$*" >>"${SUMMARY}"; }

init_summary() {
    mkdir -p "${ARTIFACTS}"
    {
        printf '# options stand %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf 'project=%s http=%s api=%s\n' "${COMPOSE_PROJECT}" "${HTTP_PORT}" "${API_PORT}"
    } >"${SUMMARY}"
}

tmp_file() { # NAME -> path, and the file is part of the artifacts
    local path="${ARTIFACTS}/$1"
    mkdir -p "$(dirname "${path}")"
    printf '%s' "${path}"
}

# ---------------------------------------------------------------------------
# Stand helpers
# ---------------------------------------------------------------------------
compose() { docker compose -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" "$@"; }

# redis_cli_on runs redis-cli on one of the two instances of the stand:
# `redis` (no credentials) and `redis-auth` (ACL, see generate_fixtures).
redis_cli_on() { # SERVICE [redis-cli args...]
    local service="$1"
    shift

    if [[ "${service}" == "redis-auth" ]]; then
        compose exec -T "${service}" redis-cli -a "${REDIS_PASSWORD}" --no-auth-warning "$@" 2>/dev/null | tr -d '\r'
        return
    fi

    compose exec -T "${service}" redis-cli "$@" 2>/dev/null | tr -d '\r'
}

redis_cli() { redis_cli_on redis "$@"; }

traefik_logs() { compose logs --no-color reverse-proxy 2>/dev/null || true; }

wait_for_http_status() { # URL TIMEOUT REGEX DESCRIPTION
    local url="$1" timeout="$2" expect="$3" what="$4"
    local deadline=$((SECONDS + timeout)) code
    while ((SECONDS < deadline)); do
        code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${url}" || true)"
        if [[ "${code}" =~ ${expect} ]]; then
            return 0
        fi
        sleep 1
    done
    die "${what}: ${url} did not answer ${expect} within ${timeout}s (last: ${code:-none})"
}

stand_up() {
    log "starting the stand (Traefik on :${HTTP_PORT}, API on :${API_PORT}, Redis internal)"
    generate_fixtures
    compose up -d --quiet-pull
}

stand_down() {
    compose down -v --remove-orphans >/dev/null 2>&1 || true
    # The fixtures are throw-away material: the next run regenerates them.
}

dump_state() {
    log "collecting the stand state into ${ARTIFACTS}"
    traefik_logs >"$(tmp_file compose-logs.txt)" || true
    redis_keys '*' >"$(tmp_file redis-keys.txt)" || true
    curl -s --max-time 10 "http://127.0.0.1:${API_PORT}/api/http/middlewares" \
        >"$(tmp_file middlewares.json)" 2>/dev/null || true
}

on_exit() {
    local status=$?
    if ((status != 0)); then
        warn "stand ${STAND}: failed with exit ${status}, collecting the state"
        dump_state
    fi
    stand_down
    exit "${status}"
}

# ---------------------------------------------------------------------------
# Fixtures: the TLS material of 06-store-tls.yaml and the Redis ACL users of
# the credential examples.
# ---------------------------------------------------------------------------
generate_fixtures() {
    mkdir -p "${CERTS}" "${ARTIFACTS}"

    if [[ ! -f "${CERTS}/redis-ca.crt" ]]; then
        log "generating the TLS fixtures (a throw-away CA and client certificate)"
        openssl ecparam -name prime256v1 -genkey -noout -out "${CERTS}/ca.key" 2>/dev/null
        openssl req -x509 -new -key "${CERTS}/ca.key" -days 3650 -subj "/CN=options-ca" \
            -out "${CERTS}/redis-ca.crt" 2>/dev/null
        openssl ecparam -name prime256v1 -genkey -noout -out "${CERTS}/client.key" 2>/dev/null
        openssl req -new -key "${CERTS}/client.key" -subj "/CN=options-client" \
            -out "${CERTS}/client.csr" 2>/dev/null
        openssl x509 -req -in "${CERTS}/client.csr" -CA "${CERTS}/redis-ca.crt" \
            -CAkey "${CERTS}/ca.key" -CAcreateserial -days 3650 -out "${CERTS}/client.crt" 2>/dev/null
    fi

    cat >"${ARTIFACTS}/redis.conf" <<EOF
# Generated by hack/options/run.sh: the two ACL users the examples refer to.
# "default" is the user of auth-password-only and of the \$ENV_VAR variants,
# "app" is the user of auth-user-pass and sentinel-creds.
user default on >${REDIS_PASSWORD} ~* &* +@all
user app on >${APP_PASSWORD} ~* &* +@all
EOF
}

# ---------------------------------------------------------------------------
# Middleware assertions
# ---------------------------------------------------------------------------
middleware_json() { # NAME
    local name="$1" json
    json="$(curl -s --max-time 10 "http://127.0.0.1:${API_PORT}/api/http/middlewares/${name}" || true)"
    [[ -n "${json}" ]] || die "the API returned nothing for ${name}"
    printf '%s\n' "${json}" >"$(tmp_file "middleware-${name//[^A-Za-z0-9_-]/_}.json")"
    printf '%s\n' "${json}"
}

assert_middleware_enabled() { # NAME
    local name="$1" json
    json="$(middleware_json "${name}")"

    grep -qE '"status":[[:space:]]*"disabled"' <<<"${json}" &&
        die "${name} must be enabled but is disabled: ${json}"
    grep -q '"error"' <<<"${json}" &&
        die "${name} must be enabled but carries an error: ${json}"

    ENABLED_COUNT=$((ENABLED_COUNT + 1))
    summary "enabled: ${name}"
}

assert_middleware_disabled() { # NAME EXPECTED_REGEX DESCRIPTION
    local name="$1" expect="$2" what="$3" json
    json="$(middleware_json "${name}")"

    grep -qE '"status":[[:space:]]*"disabled"' <<<"${json}" ||
        die "${what}: ${name} must be disabled: ${json}"
    grep -qE "${expect}" <<<"${json}" ||
        die "${what}: ${name} carries no error matching /${expect}/: ${json}"

    DISABLED_COUNT=$((DISABLED_COUNT + 1))
    summary "disabled: ${name} -> $(tr -d '\n' <<<"${json}" | cut -c1-220)"
}

# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------
run_requests() { # FILE COUNT URL [curl args...]
    local file="$1" count="$2" url="$3" i code
    shift 3
    : >"${file}"
    for ((i = 0; i < count; i++)); do
        code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@" "${url}" || echo 000)"
        printf '%s\n' "${code}" >>"${file}"
    done
}

assert_all_codes() { # FILE CODE DESCRIPTION
    local file="$1" code="$2" what="$3" bad
    bad="$(grep -vc "^${code}$" "${file}" || true)"
    [[ "${bad}" == "0" ]] ||
        die "${what}: $(tr '\n' ' ' <"${file}") (want every response to be ${code})"
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: $(wc -l <"${file}" | tr -d ' ') x ${code}"
}

# check_burst drains a fresh bucket and asserts the shape of the drain: the
# first MIN..MAX requests pass (a full bucket holds `burst` tokens) and the next
# one is rejected.
check_burst() { # NAME URL MIN MAX [curl args...]
    local name="$1" url="$2" min="$3" max="$4" file leading next
    shift 4
    file="$(tmp_file "requests-${name}.txt")"
    run_requests "${file}" 20 "${url}" "$@"

    leading="$(awk 'BEGIN{n=0; seen=0} $0=="200" {if (seen) {exit} n++; next} {seen=1} END{print n}' "${file}")"
    if ((leading < min || leading > max)); then
        die "${name}: ${leading} leading 200s, expected ${min}..${max}: $(tr '\n' ' ' <"${file}")"
    fi
    next="$(awk 'BEGIN{seen=0} $0=="200" && !seen {next} {print; exit}' "${file}")"
    [[ "${next}" == "429" ]] || die "${name}: the request after the drain answered ${next}, expected 429"
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "drain ${name}: ${leading} x 200 -> 429 (${min}..${max} accepted)"
}

http_headers() { # URL [curl args...]
    local url="$1"
    shift
    curl -s -D - -o /dev/null --max-time 10 "$@" "${url}" | tr -d '\r'
}

assert_header() { # RESPONSE_LINES NAME REGEX DESCRIPTION
    local response="$1" header="$2" expect="$3" what="$4"
    grep -qiE "^${header}: ${expect}" <<<"${response}" ||
        die "${what}: no ${header} matching /${expect}/ in: $(tr '\n' '|' <<<"${response}")"
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: ${header} matches /${expect}/"
}

assert_header_absent() { # RESPONSE_LINES NAME DESCRIPTION
    local response="$1" header="$2" what="$3"
    if grep -qiE "^${header}:" <<<"${response}"; then
        die "${what}: unexpected ${header} in: $(tr '\n' '|' <<<"${response}")"
    fi
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: no ${header}"
}

# ---------------------------------------------------------------------------
# Redis and log assertions
# ---------------------------------------------------------------------------
redis_keys() { # [redis-cli args...]
    redis_cli --scan --pattern '*' "$@" | sort
}

redis_keys_of() { # SERVICE
    redis_cli_on "$1" --scan --pattern '*' | sort
}

assert_key_present() { # KEYS_FILE PATTERN DESCRIPTION
    local file="$1" pattern="$2" what="$3"
    grep -qF "${pattern}" "${file}" || die "${what}: no key containing '${pattern}' in: $(tr '\n' ' ' <"${file}")"
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: key '${pattern}' present"
}

assert_key_match() { # KEYS_FILE REGEX DESCRIPTION
    local file="$1" pattern="$2" what="$3"
    grep -qE "${pattern}" "${file}" || die "${what}: no key matching /${pattern}/ in: $(tr '\n' ' ' <"${file}")"
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: key matching /${pattern}/ present"
}

assert_key_absent() { # KEYS_FILE PATTERN DESCRIPTION
    local file="$1" pattern="$2" what="$3"
    if grep -qF "${pattern}" "${file}"; then
        die "${what}: unexpected key '${pattern}' in: $(tr '\n' ' ' <"${file}")"
    fi
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: no key '${pattern}'"
}

assert_log_absent() { # REGEX DESCRIPTION
    local pattern="$1" what="$2" logs
    logs="$(traefik_logs)"
    if grep -qE "${pattern}" <<<"${logs}"; then
        die "${what}: '${pattern}' is present in the Traefik log"
    fi
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: log does not match /${pattern}/"
}

assert_log_contains() { # REGEX DESCRIPTION
    local pattern="$1" what="$2" logs
    logs="$(traefik_logs)"
    printf '%s\n' "${logs}" >"$(tmp_file logs-reverse-proxy.txt)"
    grep -qE "${pattern}" <<<"${logs}" || die "${what}: '${pattern}' not found in the Traefik log"
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "${what}: log matches /${pattern}/"
}

# ---------------------------------------------------------------------------
# The examples: the middleware name lists asserted below.
# ---------------------------------------------------------------------------
POSITIVE_MIDDLEWARES=(
    # 01-rate.yaml
    rate-defaults rate-zero-limit rate-limit-100 rate-period-500ms rate-period-1m
    rate-period-whole-seconds rate-burst-default rate-burst-explicit rate-limit-burst-combo
    rate-deny-on-error-true rate-deny-on-error-false rate-headers-true rate-headers-false
    # 02-sources.yaml
    src-ip-default src-ip-depth src-ip-excluded src-ip-depth-excluded src-header src-host
    src-cookie-value src-cookie-field-dot src-cookie-array-dot src-cookie-array-bracket
    src-cookie-scalar-number src-jwt-cookie src-jwt-cookie-field src-jwt-header-bearer
    src-jwt-header-raw src-empty-shared src-empty-clientip src-empty-deny
    src-empty-denyonerror-true src-empty-denyonerror-false src-empty-header-deny
    # 03-whitelist.yaml
    wl-single-ip wl-cidr wl-multiple
    # 04-store-basic.yaml
    db-defaults db-zero db-three db-failover-endpoints db-timeout-500ms db-timeout-1m
    db-pool-defaults db-pool-explicit db-pool-small db-idle-zero db-idle-30s
    db-failover-endpoints-available db-failover-healthy-first
    # 05-store-auth.yaml
    auth-user-pass auth-password-only auth-env-var auth-env-unset
    sentinel-master-set sentinel-creds sentinel-watch-true sentinel-watch-false
    # 06-store-tls.yaml
    tls-ca-path tls-ca-inline tls-ca-alias tls-mtls-path tls-mtls-inline
    tls-insecure-skip-verify tls-verify-server
    # 07-legacy.yaml
    legacy-all-flat-keys legacy-minimal legacy-new-keys-win legacy-timeout-only
    # 08-breaker.yaml
    brk-default brk-explicit-defaults brk-strict brk-extreme
)

# NAME|EXPECTED_REGEX_IN_THE_ERROR|DESCRIPTION
NEGATIVE_MIDDLEWARES=(
    'tls-cert-without-key|both TLS cert and key must be defined|cert without key'
    'tls-cert-file-missing|cert file does not exist|cert file missing'
    'bad-empty-endpoints|store\.redis\.endpoints must not be empty|empty endpoints'
    'bad-cluster|store\.redis\.cluster is not supported yet|cluster'
    'bad-cluster-empty-map|store\.redis\.cluster is not supported yet|cluster as an empty mapping'
    'bad-jwt-without-source|jwtClaim requires cookieName or requestHeaderName|jwtClaim without a source'
    'bad-cookiefield-without-cookiename|cookieField requires cookieName|cookieField without cookieName'
    'bad-nonnumeric-index|array index .* is not a number|non-numeric array index'
    'bad-negative-pool|store\.redis\.poolSize must be|negative poolSize'
    'bad-negative-limit|limit must be .*got -1|negative limit'
    'bad-period|invalid period .*10 seconds|unparsable period'
    'bad-zero-period|period must be greater than 0|zero period'
    'bad-emptysource-policy|invalid onEmptySource .*explode|unknown onEmptySource'
    'bad-two-criteria|requestHeaderName, requestHost and cookieName are mutually exclusive|two criteria'
    'bad-ip-strategy-with-criterion|ipStrategy and the other source criteria are mutually exclusive|ipStrategy with a criterion'
    'bad-timeout|invalid store\.redis\.timeout .*soon|unparsable timeout'
    'bad-idle-timeout|store\.redis\.idleTimeout must be|negative idleTimeout'
    'bad-whitelist-entry|unable to create IP whitelist checker|invalid whitelist entry'
    'bad-endpoint-format|invalid Redis endpoint .*redis|endpoint without a port'
    'bad-sentinel-without-masterset|store\.redis\.sentinel\.masterSet is required|sentinel without masterSet'
)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    trap on_exit EXIT

    init_summary
    stand_up

    wait_for_http_status "http://127.0.0.1:${API_PORT}/api/overview" 90 '^200$' "the Traefik API"
    wait_for_http_status "http://127.0.0.1:${HTTP_PORT}/mw/rate-defaults" 60 '^(200|429|404)$' "a route of the directory" \
        -H 'Host: localhost'

    # Every middleware of the directory must be known to Traefik (the file
    # provider reports them one by one, enabled or disabled).
    wait_for_http_status "http://127.0.0.1:${API_PORT}/api/http/middlewares/rate-defaults@file" 60 '^200$' \
        "the first middleware of the directory"

    log "checking the ${#POSITIVE_MIDDLEWARES[@]} positive middlewares"
    local name
    for name in "${POSITIVE_MIDDLEWARES[@]}"; do
        assert_middleware_enabled "${name}@file"
    done

    log "checking the ${#NEGATIVE_MIDDLEWARES[@]} negative middlewares"
    local entry
    for entry in "${NEGATIVE_MIDDLEWARES[@]}"; do
        IFS='|' read -r name expect what <<<"${entry}"
        assert_middleware_disabled "${name}@file" "${expect}" "${what}"
    done

    # A refused middleware is only built because 10-routers.yaml references it:
    # the router that uses it must then answer 404 and never reach the backend.
    local file
    file="$(tmp_file requests-negative-router.txt)"
    run_requests "${file}" 1 "http://127.0.0.1:${HTTP_PORT}/mw/bad-negative-limit" -H 'Host: localhost'
    assert_all_codes "${file}" 404 "a router with a disabled middleware answers 404"

    behaviour_checks
    log_checks

    log "OK: ${ENABLED_COUNT} middlewares enabled, ${DISABLED_COUNT} refused, ${BEHAVIOUR_COUNT} behavioural checks"
    summary ""
    summary "result: OK (${ENABLED_COUNT} enabled, ${DISABLED_COUNT} refused, ${BEHAVIOUR_COUNT} behavioural checks)"
}

behaviour_checks() {
    local url="http://127.0.0.1:${HTTP_PORT}"
    local host=(-H 'Host: localhost')
    local keys file response

    log "flushing both Redis instances so that every drain starts from a full bucket"
    redis_cli FLUSHALL >/dev/null
    redis_cli_on redis-auth FLUSHALL >/dev/null

    # --- 01: the shape of a bucket ----------------------------------------
    check_burst rate-limit-burst-combo "${url}/mw/rate-limit-burst-combo" 9 11 "${host[@]}"

    response="$(http_headers "${url}/mw/rate-limit-burst-combo" "${host[@]}")"
    printf '%s\n' "${response}" >"$(tmp_file response-rate-combo.txt)"
    assert_header "${response}" "X-Ratelimit-Limit" "10" "rate-combo headers"
    assert_header "${response}" "X-Ratelimit-Remaining" "0" "rate-combo headers"
    assert_header "${response}" "Retry-After" "[0-9]+" "rate-combo headers"

    # limit: 0 -> no rate limiting and no store call, whatever the load.
    file="$(tmp_file requests-rate-zero.txt)"
    run_requests "${file}" 20 "${url}/mw/rate-zero-limit" "${host[@]}"
    assert_all_codes "${file}" 200 "limit 0 is a pass-through"
    keys="$(tmp_file redis-keys-rate-zero.txt)"
    redis_keys >"${keys}"
    assert_key_absent "${keys}" 'rate:rate-zero-limit@file' "limit 0 never touches the store"

    # denyOnError against an unreachable store.
    file="$(tmp_file requests-rate-deny-true.txt)"
    run_requests "${file}" 3 "${url}/mw/rate-deny-on-error-true" "${host[@]}"
    assert_all_codes "${file}" 429 "denyOnError true: an unreachable store is a 429"
    file="$(tmp_file requests-rate-deny-false.txt)"
    run_requests "${file}" 3 "${url}/mw/rate-deny-on-error-false" "${host[@]}"
    assert_all_codes "${file}" 200 "denyOnError false: an unreachable store lets the request through"

    # burst omitted -> 1; burst explicit -> 10; responseHeaders on and off.
    check_burst rate-burst-default "${url}/mw/rate-burst-default" 1 1 "${host[@]}"
    check_burst rate-burst-explicit "${url}/mw/rate-burst-explicit" 9 11 "${host[@]}"

    # responseHeaders: the headers are checked on the *rejected* response, after
    # the bucket of each router has been drained by two requests.
    check_burst rate-headers-true "${url}/mw/rate-headers-true" 1 1 "${host[@]}"
    response="$(http_headers "${url}/mw/rate-headers-true" "${host[@]}")"
    printf '%s\n' "${response}" >"$(tmp_file response-rate-headers-true.txt)"
    assert_header "${response}" "X-Ratelimit-Limit" "1" "responseHeaders true"

    check_burst rate-headers-false "${url}/mw/rate-headers-false" 1 1 "${host[@]}"
    response="$(http_headers "${url}/mw/rate-headers-false" "${host[@]}")"
    printf '%s\n' "${response}" >"$(tmp_file response-rate-headers-false.txt)"
    assert_header_absent "${response}" "X-Ratelimit" "responseHeaders false"

    # period: a sub-second period refills in a sub-second window.
    file="$(tmp_file requests-rate-period-500ms.txt)"
    run_requests "${file}" 1 "${url}/mw/rate-period-500ms" "${host[@]}"
    assert_all_codes "${file}" 200 "period 500ms: the first request passes"
    file="$(tmp_file requests-rate-period-500ms-drained.txt)"
    run_requests "${file}" 1 "${url}/mw/rate-period-500ms" "${host[@]}"
    assert_all_codes "${file}" 429 "period 500ms: the bucket is empty"
    sleep 0.7
    file="$(tmp_file requests-rate-period-500ms-refilled.txt)"
    run_requests "${file}" 1 "${url}/mw/rate-period-500ms" "${host[@]}"
    assert_all_codes "${file}" 200 "period 500ms: the bucket refilled after 500ms"

    # --- 02: one key per source criterion ---------------------------------
    one_request_key_regex redis "src-ip-default" "${url}/mw/src-ip-default" '^rate:src-ip-default@file:[0-9a-fA-F:.]+$' "${host[@]}"
    one_request_key redis "src-ip-depth" "${url}/mw/src-ip-depth" 'rate:src-ip-depth@file:203.0.113.7' \
        "${host[@]}" -H 'X-Forwarded-For: 203.0.113.7'
    one_request_key redis "src-ip-excluded" "${url}/mw/src-ip-excluded" 'rate:src-ip-excluded@file:203.0.113.9' \
        "${host[@]}" -H 'X-Forwarded-For: 10.1.2.3, 203.0.113.9'
    one_request_key redis "src-ip-depth-excluded" "${url}/mw/src-ip-depth-excluded" 'rate:src-ip-depth-excluded@file:203.0.113.9' \
        "${host[@]}" -H 'X-Forwarded-For: 10.1.2.3, 203.0.113.9'
    one_request_key redis "src-header" "${url}/mw/src-header" 'rate:src-header@file:key-abc' \
        "${host[@]}" -H 'X-Api-Key: key-abc'
    one_request_key redis "src-host" "${url}/mw/src-host" 'rate:src-host@file:localhost' "${host[@]}"
    one_request_key redis "src-cookie-value" "${url}/mw/src-cookie-value" 'rate:src-cookie-value@file:u1' \
        "${host[@]}" --cookie '_sm_user_id=u1'
    one_request_key redis "src-cookie-field-dot" "${url}/mw/src-cookie-field-dot" 'rate:src-cookie-field-dot@file:u9' \
        "${host[@]}" --cookie 'session=%7B%22user%22%3A%7B%22id%22%3A%22u9%22%7D%7D'
    one_request_key redis "src-cookie-array-dot" "${url}/mw/src-cookie-array-dot" 'rate:src-cookie-array-dot@file:uA' \
        "${host[@]}" --cookie '_sm_session=%7B%22users%22%3A%5B%7B%22id%22%3A%22uA%22%7D%5D%7D'
    one_request_key redis "src-cookie-array-bracket" "${url}/mw/src-cookie-array-bracket" 'rate:src-cookie-array-bracket@file:uB' \
        "${host[@]}" --cookie '_sm_session=%7B%22users%22%3A%5B%7B%22id%22%3A%22uB%22%7D%5D%7D'
    one_request_key redis "src-cookie-scalar-number" "${url}/mw/src-cookie-scalar-number" 'rate:src-cookie-scalar-number@file:42' \
        "${host[@]}" --cookie 'session=%7B%22tenant%22%3A42%7D'

    # JWT claims: cookie, JSON field of a cookie, Authorization header with and
    # without the Bearer prefix. The signature is not verified - these are
    # dummy tokens (see the README).
    one_request_key redis "src-jwt-cookie" "${url}/mw/src-jwt-cookie" 'rate:src-jwt-cookie@file:uJ' \
        "${host[@]}" --cookie '_sm_token=eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1SiJ9.c2ln'
    one_request_key redis "src-jwt-cookie-field" "${url}/mw/src-jwt-cookie-field" 'rate:src-jwt-cookie-field@file:uK' \
        "${host[@]}" --cookie 'session=%7B%22token%22%3A%22eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1SyJ9.c2ln%22%7D'
    one_request_key redis "src-jwt-header-bearer" "${url}/mw/src-jwt-header-bearer" 'rate:src-jwt-header-bearer@file:uL' \
        "${host[@]}" -H 'Authorization: Bearer eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1TCJ9.c2ln'
    one_request_key redis "src-jwt-header-raw" "${url}/mw/src-jwt-header-raw" 'rate:src-jwt-header-raw@file:uM' \
        "${host[@]}" -H 'X-Jwt: eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJ1c2VyIjp7ImlkIjoidU0ifX0.c2ln'

    # onEmptySource: shared, clientIP, deny, denyOnError (both denyOnError
    # values), and the same policy on a header criterion.
    file="$(tmp_file requests-src-empty-shared.txt)"
    run_requests "${file}" 1 "${url}/mw/src-empty-shared" "${host[@]}"
    assert_all_codes "${file}" 200 "onEmptySource shared: no cookie, one shared bucket"
    keys="$(tmp_file redis-keys-empty.txt)"
    redis_keys >"${keys}"
    assert_key_present "${keys}" 'rate:src-empty-shared@file:' "onEmptySource shared: the empty source has its own key"

    one_request_key redis "src-empty-clientip" "${url}/mw/src-empty-clientip" 'rate:src-empty-clientip@file:198.51.100.5' \
        "${host[@]}" -H 'X-Forwarded-For: 198.51.100.5'

    file="$(tmp_file requests-src-empty-deny.txt)"
    run_requests "${file}" 2 "${url}/mw/src-empty-deny" "${host[@]}"
    assert_all_codes "${file}" 400 "onEmptySource deny: 400 before the store"
    file="$(tmp_file requests-src-empty-header-deny.txt)"
    run_requests "${file}" 2 "${url}/mw/src-empty-header-deny" "${host[@]}"
    assert_all_codes "${file}" 400 "onEmptySource deny with a header criterion: 400"

    file="$(tmp_file requests-src-empty-denyonerror-true.txt)"
    run_requests "${file}" 2 "${url}/mw/src-empty-denyonerror-true" "${host[@]}"
    assert_all_codes "${file}" 429 "onEmptySource denyOnError + denyOnError true: 429"
    file="$(tmp_file requests-src-empty-denyonerror-false.txt)"
    run_requests "${file}" 2 "${url}/mw/src-empty-denyonerror-false" "${host[@]}"
    assert_all_codes "${file}" 200 "onEmptySource denyOnError + denyOnError false: pass-through"

    keys="$(tmp_file redis-keys-empty-rejected.txt)"
    redis_keys >"${keys}"
    assert_key_absent "${keys}" 'rate:src-empty-deny@file' "onEmptySource deny never touches the store"
    assert_key_absent "${keys}" 'rate:src-empty-denyonerror-true@file' "onEmptySource denyOnError never touches the store"

    # --- 03: the whitelist bypasses rate limiting entirely ----------------
    file="$(tmp_file requests-wl-single.txt)"
    run_requests "${file}" 5 "${url}/mw/wl-single-ip" "${host[@]}" -H 'X-Forwarded-For: 203.0.113.7'
    assert_all_codes "${file}" 200 "whitelist: a single address is never limited"
    keys="$(tmp_file redis-keys-whitelist.txt)"
    redis_keys >"${keys}"
    assert_key_absent "${keys}" 'rate:wl-single-ip@file' "whitelist: the store is not called"

    check_burst wl-single-ip-not-whitelisted "${url}/mw/wl-single-ip" 1 1 "${host[@]}" -H 'X-Forwarded-For: 198.51.100.9'
    keys="$(tmp_file redis-keys-whitelist-limited.txt)"
    redis_keys >"${keys}"
    assert_key_present "${keys}" 'rate:wl-single-ip@file:198.51.100.9' "whitelist: another address is limited"

    file="$(tmp_file requests-wl-cidr.txt)"
    run_requests "${file}" 5 "${url}/mw/wl-cidr" "${host[@]}" -H 'X-Forwarded-For: 10.9.8.7'
    assert_all_codes "${file}" 200 "whitelist: a CIDR range is never limited"
    file="$(tmp_file requests-wl-multiple.txt)"
    run_requests "${file}" 5 "${url}/mw/wl-multiple" "${host[@]}" -H 'X-Forwarded-For: 198.51.100.77'
    assert_all_codes "${file}" 200 "whitelist: one of several entries is never limited"

    # --- 04: the store -----------------------------------------------------
    one_request_key_regex redis "db-defaults" "${url}/mw/db-defaults" '^rate:db-defaults@file:[0-9a-fA-F:.]+$' "${host[@]}"

    file="$(tmp_file requests-db-three.txt)"
    run_requests "${file}" 1 "${url}/mw/db-three" "${host[@]}"
    assert_all_codes "${file}" 200 "database 3: the request is served"
    keys="$(tmp_file redis-keys-db3.txt)"
    redis_cli -n 3 --scan --pattern '*' | sort >"${keys}"
    assert_key_match "${keys}" '^rate:db-three@file:[0-9a-fA-F:.]+$' "database 3 holds the bucket"
    keys="$(tmp_file redis-keys-db0.txt)"
    redis_keys >"${keys}"
    assert_key_absent "${keys}" 'rate:db-three@file' "database 3 keeps db0 clean"

    # A dead-first list fails over: the request that finds the dead endpoint is
    # retried on the healthy one, so the middleware keeps answering and the
    # bucket lives on the healthy endpoint.
    # One request is enough: limit=1/period=10s means the second one would be a
    # legitimate 429 of the bucket, not a failover failure.
    file="$(tmp_file requests-db-failover-dead-first.txt)"
    run_requests "${file}" 1 "${url}/mw/db-failover-endpoints" "${host[@]}"
    assert_all_codes "${file}" 200 "a dead-first endpoint list fails over to the healthy endpoint"
    keys="$(tmp_file redis-keys-db-failover.txt)"
    redis_keys >"${keys}"
    assert_key_present "${keys}" 'rate:db-failover-endpoints@file' \
        "a dead-first endpoint list: the bucket lives on the healthy endpoint"
    summary "a dial failure rotates store.redis.endpoints (failover to the second endpoint)"

    file="$(tmp_file requests-db-failover-available.txt)"
    run_requests "${file}" 1 "${url}/mw/db-failover-endpoints-available" "${host[@]}"
    assert_all_codes "${file}" 200 "an endpoint list with denyOnError false: the request is not rejected"

    one_request_key_regex redis "db-failover-healthy-first" "${url}/mw/db-failover-healthy-first" \
        '^rate:db-failover-healthy-first@file:[0-9a-fA-F:.]+$' "${host[@]}"
    one_request_key_regex redis "db-pool-small" "${url}/mw/db-pool-small" '^rate:db-pool-small@file:[0-9a-fA-F:.]+$' "${host[@]}"

    # --- 05: the credentials ----------------------------------------------
    one_request_key_regex redis-auth "auth-user-pass" "${url}/mw/auth-user-pass" '^rate:auth-user-pass@file:[0-9a-fA-F:.]+$' "${host[@]}"
    one_request_key_regex redis-auth "auth-password-only" "${url}/mw/auth-password-only" '^rate:auth-password-only@file:[0-9a-fA-F:.]+$' "${host[@]}"
    one_request_key_regex redis-auth "auth-env-var" "${url}/mw/auth-env-var" '^rate:auth-env-var@file:[0-9a-fA-F:.]+$' "${host[@]}"

    # --- 07: the legacy keys ----------------------------------------------
    # The legacy `redisDb` (2) of this example puts the bucket in another
    # database, which is also the proof that the legacy keys are applied.
    request_keys redis-auth "legacy-all-flat-keys" "${url}/mw/legacy-all-flat-keys" "${host[@]}" >/dev/null
    keys="$(tmp_file redis-keys-legacy-db2.txt)"
    redis_cli_on redis-auth -n 2 --scan --pattern '*' | sort >"${keys}"
    assert_key_match "${keys}" '^rate:legacy-all-flat-keys@file:[0-9a-fA-F:.]+$' \
        "legacy redisDb: the bucket lives in database 2"
    one_request_key_regex redis-auth "legacy-new-keys-win" "${url}/mw/legacy-new-keys-win" '^rate:legacy-new-keys-win@file:[0-9a-fA-F:.]+$' "${host[@]}"

    # --- 08: the breaker ---------------------------------------------------
    file="$(tmp_file requests-brk-extreme.txt)"
    run_requests "${file}" 3 "${url}/mw/brk-extreme" "${host[@]}"
    assert_all_codes "${file}" 429 "breaker threshold 1: the unreachable store is a 429 from the first failure"

    summary ""
    summary "behaviour: OK"
}

# one_request_key sends a single request and asserts that the bucket key of the
# given middleware exists in Redis (the source resolved as expected).
# request_keys sends one request and prints the path of the file holding the
# keys of the instance, so that a caller can assert on the source of the bucket.
request_keys() { # SERVICE NAME URL [curl args...]
    local service="$1" name="$2" url="$3" file keys
    shift 3
    file="$(tmp_file "requests-${name}.txt")"
    run_requests "${file}" 1 "${url}" "$@"
    assert_all_codes "${file}" 200 "${name}: the request is served"

    keys="$(tmp_file "redis-keys-${name}.txt")"
    redis_keys_of "${service}" >"${keys}"
    printf '%s' "${keys}"
}

one_request_key() { # SERVICE NAME URL EXPECTED_KEY [curl args...]
    local service="$1" name="$2" url="$3" pattern="$4" keys
    shift 4
    keys="$(request_keys "${service}" "${name}" "${url}" "$@")"
    assert_key_present "${keys}" "${pattern}" "${name}: source in the bucket key"
}

# one_request_key_regex is the same, for a source that depends on the network the
# stand runs on: the client address of a request that arrives through a
# published port is the gateway of the docker network (192.168.65.1 on Docker
# Desktop, 172.x on a Linux runner), not 127.0.0.1.
one_request_key_regex() { # SERVICE NAME URL REGEX [curl args...]
    local service="$1" name="$2" url="$3" pattern="$4" keys
    shift 4
    keys="$(request_keys "${service}" "${name}" "${url}" "$@")"
    assert_key_match "${keys}" "${pattern}" "${name}: source in the bucket key"
}

log_checks() {
    log "checking the log messages of the deprecated keys and the credentials"

    # The plugin reports its configuration warnings at debug level (interpreter
    # stdout), hence log.level: DEBUG in example/traefik-options.yaml.
    assert_log_contains '"average" is deprecated, use "limit" instead' "legacy average warning"
    assert_log_contains '"average" is deprecated and ignored because "limit" is set' \
        "legacy keys are ignored when the new schema is used (average)"
    assert_log_contains '"redisAddress" is deprecated, use "store\.redis\.endpoints" instead' "legacy redisAddress warning"
    # shellcheck disable=SC2016  # the regex needs a literal $
    assert_log_contains 'store\.redis\.password references an environment variable that is not set: \$REDIS_OPTIONS_MISSING' \
        "an unset \$ENV_VAR is reported"

    # Every deprecation warning of a middleware must reach the log: they all
    # share the "%s" template, so the logger throttles them by their rendered
    # message instead of by the template (the finding of the first pass, fixed
    # in this revision - see docs/18-options.md).
    assert_log_contains '"average" is deprecated' "legacy average warning is logged"
    assert_log_contains '"redisAddress" is deprecated' "legacy redisAddress warning is logged"
    assert_log_contains '"redisDb" is deprecated' "legacy redisDb warning is logged"
    assert_log_contains '"redisPassword" is deprecated' "legacy redisPassword warning is logged"
    assert_log_contains '"redisConnectionTimeout" is deprecated' "legacy redisConnectionTimeout warning is logged"
    assert_log_contains '"breakerThreshold"/"breakerReattempt" are deprecated' "legacy breaker warning is logged"
    summary "every deprecation warning of a middleware is logged (no longer collapsed by the throttle)"

    # No middleware of this directory is misconfigured, so the store must never
    # have refused an authentication (the ACL credentials of 05 are correct).
    local logs
    logs="$(traefik_logs)"
    if grep -q 'WRONGPASS' <<<"${logs}"; then
        die "an authentication error reached the log even though every credential example is configured correctly"
    fi
    BEHAVIOUR_COUNT=$((BEHAVIOUR_COUNT + 1))
    summary "no WRONGPASS in the log: the ACL credentials and \$ENV_VAR expansion work"
}

main "$@"
