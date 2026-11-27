#!/usr/bin/env bash
# shellcheck shell=bash
#
# Helpers shared by the nightly stands in hack/ci.
#
# A stand script sets (or lets the defaults in this file apply):
#   STAND            short stand name, also the artifacts sub-directory;
#   COMPOSE_FILE     compose file that describes the stand;
#   COMPOSE_PROJECT  unique compose project name (parallel runs must not clash);
#   ARTIFACTS_DIR    where the failure logs are collected.
#
# Only bash, curl and docker are used: the helpers run unchanged on a GitHub
# runner and on a workstation.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-${ROOT_DIR}/artifacts}"

log() { printf '[%s] %s\n' "$(date -u '+%H:%M:%S')" "$*"; }
warn() { printf '[%s] WARN: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }
die() {
    printf '[%s] ERROR: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

stand_dir() { printf '%s/%s' "${ARTIFACTS_DIR}" "${STAND}"; }
tmp_file() { printf '%s/%s' "$(stand_dir)" "$1"; }

compose() { docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" "$@"; }

stand_up() {
    require_cmd docker
    require_cmd curl
    mkdir -p "$(stand_dir)"
    log "stand ${STAND}: docker compose up (project ${COMPOSE_PROJECT}, ${COMPOSE_FILE})"
    compose up -d --quiet-pull
}

stand_down() {
    log "stand ${STAND}: docker compose down"
    compose down -v --remove-orphans >/dev/null 2>&1 || true
}

# dump_stand_state collects everything needed to understand a failure: the
# container states, the full logs and (optionally) a tail printed to stderr so
# that the GitHub job log is useful without downloading artifacts.
dump_stand_state() {
    local dir
    dir="$(stand_dir)"
    mkdir -p "${dir}"
    compose ps >"${dir}/compose-ps.txt" 2>&1 || true
    compose logs --no-color --timestamps >"${dir}/compose-logs.txt" 2>&1 || true
    log "stand ${STAND}: state collected in ${dir} (compose-ps.txt, compose-logs.txt)"
    warn "--- tail of stand ${STAND} logs ---"
    tail -n 40 "${dir}/compose-logs.txt" >&2 || true
}

init_summary() {
    mkdir -p "$(stand_dir)"
    : >"$(stand_dir)/summary.txt"
}

# summary prints to stdout and appends to the artifact, so the summary survives
# both a green run and a failure.
summary() {
    mkdir -p "$(stand_dir)"
    printf '%s\n' "$*" | tee -a "$(stand_dir)/summary.txt"
}

http_code() { # URL [curl args...]
    local url="$1"
    shift
    curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@" "${url}" 2>/dev/null || printf '000'
}

http_headers() { # URL [curl args...]
    local url="$1"
    shift
    curl -s -D - -o /dev/null --max-time 10 "$@" "${url}" 2>/dev/null || true
}

wait_tcp_port() { # HOST PORT TIMEOUT_S [DESCRIPTION]
    local host="$1" port="$2" timeout="$3"
    # A separate local: the words of a single `local` are expanded before the
    # builtin runs, so "${host}:${port}" would not see the assignments above.
    local what="${4:-${host}:${port}}"
    local start="${SECONDS}"
    while ! (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; do
        ((SECONDS - start >= timeout)) && die "stand ${STAND}: ${what} not reachable after ${timeout}s"
        sleep 1
    done
    log "stand ${STAND}: ${what} reachable after $((SECONDS - start))s"
}

wait_http_status() { # URL TIMEOUT_S EXPECTED_REGEX DESCRIPTION [curl args...]
    local url="$1" timeout="$2" expect="$3" what="$4"
    shift 4
    local start="${SECONDS}" code="000"
    while :; do
        code="$(http_code "${url}" "$@")"
        if [[ "${code}" =~ ${expect} ]]; then
            log "stand ${STAND}: ready - ${what} -> HTTP ${code} after $((SECONDS - start))s"
            return 0
        fi
        ((SECONDS - start >= timeout)) && die "stand ${STAND}: ${what} not ready after ${timeout}s (last HTTP ${code})"
        sleep 1
    done
}

# redis_flush_buckets drops the buckets of a stand, so that a check can start
# from a full bucket no matter how many requests the readiness probes sent.
redis_flush_buckets() { # SERVICE [REDIS_CLI ARGS...]
    local service="$1"
    shift
    local keys key
    keys="$(compose exec -T "${service}" redis-cli "$@" --scan --pattern 'rate:*' 2>/dev/null | tr -d '\r' || true)"
    # The guard matters: `while read` on empty input returns 1, which `set -e`
    # would treat as a failure of the stand.
    if [[ -n "${keys}" ]]; then
        while read -r key; do
            [[ -n "${key}" ]] || continue
            compose exec -T "${service}" redis-cli "$@" DEL "${key}" >/dev/null 2>&1 || true
        done <<<"${keys}"
    fi
}

# run_requests writes one HTTP status code per request, one per line, so that
# the result can be aggregated and archived.
run_requests() { # OUT_FILE COUNT URL [curl args...]
    local out="$1" count="$2" url="$3"
    shift 3
    : >"${out}"
    local i
    for ((i = 1; i <= count; i++)); do
        printf '%s\n' "$(http_code "${url}" "$@")" >>"${out}"
    done
}

# background_load keeps hammering URL until STOP_FILE appears. It is used by the
# chaos stand so that the traffic really spans the failover.
background_load() { # OUT_FILE STOP_FILE URL [curl args...]
    local out="$1" stop="$2" url="$3"
    shift 3
    : >"${out}"
    local workers="${LOAD_WORKERS:-6}"
    while [[ ! -f "${stop}" ]]; do
        seq 1 "${workers}" | xargs -P "${workers}" -I{} \
            curl -s -o /dev/null -w '%{http_code}\n' --max-time 5 "$@" "${url}" >>"${out}" 2>/dev/null || true
    done
}

show_histogram() { # FILE DESCRIPTION
    local file="$1" what="$2"
    log "stand ${STAND}: ${what}: $(wc -l <"${file}" | tr -d ' ') requests"
    sort "${file}" | uniq -c | sed 's/^/    /' | tee -a "$(stand_dir)/summary.txt" >&2
}

count_codes() { # FILE CODE
    local n
    n="$(grep -c -x "$2" "$1" 2>/dev/null || true)"
    printf '%s' "${n:-0}"
}

leading_200() { # FILE
    awk '{ if ($0 == "200") c++; else exit } END { print c+0 }' "$1"
}

assert_no_5xx() { # FILE DESCRIPTION
    local file="$1" what="$2" n
    n="$(awk '/^5[0-9][0-9]$/ { c++ } END { print c+0 }' "${file}")"
    ((n == 0)) || die "stand ${STAND}: ${what}: ${n} x 5xx - see $(stand_dir)"
    log "stand ${STAND}: ${what}: 0 x 5xx over $(wc -l <"${file}" | tr -d ' ') requests"
}

assert_no_transport_errors() { # FILE DESCRIPTION
    local file="$1" what="$2" n
    n="$(count_codes "${file}" 000)"
    ((n == 0)) || die "stand ${STAND}: ${what}: ${n} requests did not reach Traefik (curl 000)"
}

# assert_drained checks the classic "burst requests pass, then the limit kicks
# in" shape: a run of 200s followed by 429s, with the length of the run inside
# [min, max] (the bucket starts full at burst, and a slow runner may refill one
# or two tokens while the loop runs).
assert_drained() { # FILE MIN_OK MAX_OK DESCRIPTION
    local file="$1" min="$2" max="$3" what="$4"
    local ok n429
    ok="$(leading_200 "${file}")"
    n429="$(count_codes "${file}" 429)"
    ((n429 > 0)) || die "stand ${STAND}: ${what}: no 429 at all - the limiter is not enforced"
    ((ok >= min && ok <= max)) || die "stand ${STAND}: ${what}: ${ok} leading 200s, expected ${min}..${max}"
    log "stand ${STAND}: ${what}: ${ok} x 200 -> then 429 (${n429} x 429)"
    summary "${what}: ${ok} x 200 -> 429 (${n429} x 429)"
}

assert_code() { # FILE CODE MIN_COUNT DESCRIPTION
    local file="$1" code="$2" min="$3" what="$4" n
    n="$(count_codes "${file}" "${code}")"
    ((n >= min)) || die "stand ${STAND}: ${what}: ${n} x ${code}, expected at least ${min}"
    log "stand ${STAND}: ${what}: ${n} x ${code} (>= ${min})"
}

assert_status_line() { # HEADERS CODE DESCRIPTION
    local headers="$1" code="$2" what="$3"
    grep -qiE "^HTTP/[0-9.]+ ${code}([[:space:]]|$)" <<<"${headers}" ||
        die "stand ${STAND}: ${what}: expected HTTP ${code}, got '$(grep -i '^HTTP/' <<<"${headers}" | sed -n '1p' | tr -d '\r')'"
    log "stand ${STAND}: ${what}: $(grep -i '^HTTP/' <<<"${headers}" | sed -n '1p' | tr -d '\r')"
    summary "${what}: $(grep -i '^HTTP/' <<<"${headers}" | sed -n '1p' | tr -d '\r')"
}

assert_header() { # HEADERS NAME REGEX DESCRIPTION
    local headers="$1" name="$2" expect="$3" what="$4"
    if ! grep -qiE "^${name}: ${expect}" <<<"${headers}"; then
        die "stand ${STAND}: ${what}: header ${name} matching /${expect}/ not found (got: $(grep -i "^${name}:" <<<"${headers}" | tr -d '\r' || printf '<absent>'))"
    fi
    log "stand ${STAND}: ${what}: $(grep -i "^${name}:" <<<"${headers}" | sed -n '1p' | tr -d '\r')"
    summary "${what}: $(grep -i "^${name}:" <<<"${headers}" | sed -n '1p' | tr -d '\r')"
}

# check_drain drives COUNT requests to URL and asserts the "burst requests pass,
# then the limit applies" shape. NAME names the artifact file.
check_drain() { # NAME URL COUNT MIN_OK MAX_OK [curl args...]
    local name="$1" url="$2" count="$3" min="$4" max="$5"
    shift 5
    local file
    file="$(tmp_file "requests-${name}.txt")"
    run_requests "${file}" "${count}" "${url}" "$@"
    show_histogram "${file}" "${name}"
    assert_no_5xx "${file}" "${name}"
    assert_no_transport_errors "${file}" "${name}"
    assert_drained "${file}" "${min}" "${max}" "${name}"
}

code_histogram() { # FILE - "N x CODE" pairs on one line
    sort "$1" | uniq -c | awk '{ printf "%s x %s ", $1, $2 }'
}

assert_all_codes() { # FILE CODE DESCRIPTION
    local file="$1" code="$2" what="$3" total other
    total="$(wc -l <"${file}" | tr -d ' ')"
    other="$((total - $(count_codes "${file}" "${code}")))"
    ((other == 0)) ||
        die "stand ${STAND}: ${what}: ${other} of ${total} responses are not ${code} ($(code_histogram "${file}"))"
    log "stand ${STAND}: ${what}: all ${total} responses are ${code}"
    summary "${what}: ${total} x ${code}"
}

redis_keys() { # SERVICE [REDIS_CLI ARGS...]
    local service="$1"
    shift
    compose exec -T "${service}" redis-cli "$@" --scan --pattern 'rate:*' 2>/dev/null | tr -d '\r' | sort || true
}

assert_key_present() { # KEYS SUBSTRING DESCRIPTION
    grep -qF "$2" <<<"$1" || die "stand ${STAND}: ${3}: no key containing '${2}' (keys: $(tr '\n' ' ' <<<"$1"))"
    log "stand ${STAND}: ${3}: found key '$(grep -F "$2" <<<"$1" | sed -n '1p')'"
    summary "${3}: key '$(grep -F "$2" <<<"$1" | sed -n '1p')'"
}

assert_key_absent() { # KEYS SUBSTRING DESCRIPTION
    if grep -qF "$2" <<<"$1"; then
        die "stand ${STAND}: ${3}: unexpected key containing '${2}' (keys: $(tr '\n' ' ' <<<"$1"))"
    fi
    log "stand ${STAND}: ${3}: no key containing '${2}', as expected"
}

# assert_middleware_disabled checks that Traefik refused to create a middleware
# and reported why - the behaviour a misconfigured source criterion must have.
assert_middleware_disabled() { # API_PORT NAME ERROR_REGEX DESCRIPTION
    local api="$1" name="$2" expect="$3" what="$4" json
    json="$(curl -s --max-time 10 "http://127.0.0.1:${api}/api/http/middlewares/${name}" || true)"
    [[ -n "${json}" ]] || die "stand ${STAND}: ${what}: the API returned nothing for ${name}"
    printf '%s\n' "${json}" >"$(tmp_file "middleware-${name//[@:\/]/_}.json")"
    grep -qE '"status":[[:space:]]*"disabled"' <<<"${json}" ||
        die "stand ${STAND}: ${what}: ${name} is not disabled: ${json}"
    grep -qE "${expect}" <<<"${json}" ||
        die "stand ${STAND}: ${what}: ${name} carries no error matching /${expect}/: ${json}"
    log "stand ${STAND}: ${what}: ${name} is disabled with the expected error"
    summary "${what}: ${name} disabled"
}

# assert_log_contains greps the logs of a service. The logs are captured first:
# piping them straight into `grep -q` can abort `docker compose logs` with
# SIGPIPE, which `set -o pipefail` would report as a failure of the check.
assert_log_contains() { # SERVICE REGEX DESCRIPTION
    local service="$1" pattern="$2" what="$3" logs
    logs="$(compose logs --no-color "${service}" 2>/dev/null || true)"
    printf '%s\n' "${logs}" >"$(tmp_file "logs-${service}.txt")"
    grep -qE "${pattern}" <<<"${logs}" ||
        die "stand ${STAND}: ${what}: '${pattern}' not found in the ${service} logs"
    log "stand ${STAND}: ${what}: found '${pattern}' in the ${service} logs"
    summary "${what}: log matches /${pattern}/"
}
