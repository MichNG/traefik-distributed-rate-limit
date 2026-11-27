#!/usr/bin/env bash
# Nightly chaos stand 4/4: a Sentinel failover under load
# (docker-compose.sentinel.yaml).
#
# The stand runs redis-master + redis-replica + 3 Sentinels behind Traefik with
# the plugin in front. The script:
#
#   1. drains a fresh bucket to prove the limiter works before the failover;
#   2. starts a background load and, while it runs, asks a Sentinel to promote
#      the replica (SENTINEL FAILOVER mymaster);
#   3. keeps hammering until ten seconds after the new master has been seen, and
#      requires ZERO 5xx in the whole window (the data path must not notice the
#      switch: denyOnError=false fails open, it never fails with 500);
#   4. drains another fresh bucket after the switch: the limiter must still
#      enforce the limit against the new master, and the bucket must live there.
#
#   HTTP_PORT=28400 API_PORT=28480 REDIS_PORT=16394 SENTINEL_PORT=26394 \
#     hack/ci/chaos-sentinel.sh
set -euo pipefail

STAND="chaos-sentinel"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.sentinel.yaml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-drl-nightly-chaos-${GITHUB_RUN_ID:-local}-$$}"
HTTP_PORT="${HTTP_PORT:-28400}"
API_PORT="${API_PORT:-28480}"
REDIS_PORT="${REDIS_PORT:-16394}"
SENTINEL_PORT="${SENTINEL_PORT:-26394}"
export HTTP_PORT API_PORT REDIS_PORT SENTINEL_PORT

# shellcheck source=hack/ci/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

MASTER_SET="mymaster"
# dynamic-sentinel.yaml: limit=3, burst=10, period=10s.
MIN_PASS=8
MAX_PASS=12
# The whole failover window must carry a meaningful amount of traffic, otherwise
# "zero 5xx" would be vacuous.
MIN_LOAD_REQUESTS=200
# How long to keep loading after the new master has been detected.
POST_SWITCH_SECONDS=10

sentinel_cli() { # [redis-cli args...]
    compose exec -T sentinel-0 redis-cli -p 26379 "$@" 2>&1 || true
}

# sentinel_master prints the master reported by the Sentinel as "host port".
sentinel_master() {
    compose exec -T sentinel-0 redis-cli -p 26379 SENTINEL get-master-addr-by-name "${MASTER_SET}" 2>/dev/null |
        tr -d '\r' | awk 'NF { a[++n] = $0 } END { printf "%s %s", a[1], a[2] }'
}

save_sentinel_state() { # FILE
    {
        printf '### SENTINEL get-master-addr-by-name %s\n' "${MASTER_SET}"
        sentinel_cli SENTINEL get-master-addr-by-name "${MASTER_SET}"
        printf '\n### SENTINEL master %s\n' "${MASTER_SET}"
        sentinel_cli SENTINEL master "${MASTER_SET}"
        printf '\n### SENTINEL sentinels %s\n' "${MASTER_SET}"
        sentinel_cli SENTINEL sentinels "${MASTER_SET}"
        printf '\n### SENTINEL replicas %s\n' "${MASTER_SET}"
        sentinel_cli SENTINEL replicas "${MASTER_SET}"
    } >"$1" 2>&1
}

redis_role_of() { # SERVICE PORT
    compose exec -T "$1" redis-cli -p "$2" INFO replication 2>/dev/null |
        tr -d '\r' | awk -F: '/^role:/ { print $2 }'
}

# master_service prints redis-master or redis-replica, whichever reports
# role:master. The Sentinel may advertise the promoted node by IP, so anything
# that has to run inside a container uses the service name instead.
master_service() {
    local service
    for service in redis-master redis-replica; do
        if [[ "$(redis_role_of "${service}" "${REDIS_PORT}")" == "master" ]]; then
            printf '%s' "${service}"
            return 0
        fi
    done
    return 1
}

service_ip() { # SERVICE
    local id
    id="$(compose ps -q "$1" 2>/dev/null | sed -n '1p')"
    [[ -n "${id}" ]] || return 1
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' "${id}" 2>/dev/null |
        awk '{ print $1 }'
}

on_exit() {
    local status=$?
    if ((status != 0)); then
        warn "stand ${STAND}: failed with exit ${status}, collecting the state"
        save_sentinel_state "$(tmp_file sentinel-state-failure.txt)" || true
        dump_stand_state
    fi
    # The load generator is a child of this shell: do not leave it behind.
    [[ -n "${LOAD_PID:-}" ]] && kill "${LOAD_PID}" 2>/dev/null || true
    stand_down
    exit "${status}"
}

main() {
    trap on_exit EXIT
    stand_up
    wait_tcp_port 127.0.0.1 "${REDIS_PORT}" 90 "redis host port"
    wait_tcp_port 127.0.0.1 "${SENTINEL_PORT}" 90 "sentinel host port"
    wait_http_status "http://127.0.0.1:${API_PORT}/api/overview" 120 '^200$' "traefik api"

    local url="http://127.0.0.1:${HTTP_PORT}/"
    wait_http_status "${url}" 90 '^(200|429)$' "plugin route" -H 'Host: localhost' -H 'X-Api-Key: probe'

    # The Sentinel answers only once it has monitored the master at least once.
    local start="${SECONDS}" master=""
    while :; do
        master="$(sentinel_master)"
        [[ -n "${master// /}" ]] && break
        ((SECONDS - start >= 90)) && die "the Sentinel never reported a master"
        sleep 1
    done
    local pre_host pre_port pre_service
    read -r pre_host pre_port <<<"${master}"
    pre_service="$(master_service)" || die "stand ${STAND}: neither redis-master nor redis-replica is a master"

    init_summary
    summary "stand=${STAND} project=${COMPOSE_PROJECT} http=${HTTP_PORT} api=${API_PORT} redis=${REDIS_PORT} sentinel=${SENTINEL_PORT}"
    summary "master before the failover: ${pre_host}:${pre_port} (service ${pre_service}, role: $(redis_role_of "${pre_service}" "${REDIS_PORT}"))"
    save_sentinel_state "$(tmp_file sentinel-state-before.txt)"

    # 1. The limiter works before the failover (a fresh source starts full).
    check_drain chaos-before "${url}" 25 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' -H 'X-Api-Key: nightly-before'

    # 2. Load that spans the failover.
    local during stop
    during="$(tmp_file requests-during-failover.txt)"
    stop="$(tmp_file .stop-load)"
    rm -f "${stop}"
    log "stand ${STAND}: starting the background load (source nightly-chaos)"
    background_load "${during}" "${stop}" "${url}" -H 'Host: localhost' -H 'X-Api-Key: nightly-chaos' &
    LOAD_PID=$!

    sleep 3
    local t0 t1
    t0="$(date -u +%s)"
    log "stand ${STAND}: triggering SENTINEL FAILOVER ${MASTER_SET}"
    sentinel_cli SENTINEL FAILOVER "${MASTER_SET}" >"$(tmp_file sentinel-failover-trigger.txt)"

    local deadline=$((SECONDS + 120)) post=""
    while :; do
        post="$(sentinel_master)"
        [[ -n "${post// /}" && "${post}" != "${master}" ]] && break
        ((SECONDS >= deadline)) && die "stand ${STAND}: the Sentinel still reports ${post} as the master - the failover did not happen"
        sleep 1
    done
    t1="$(date -u +%s)"
    local post_host post_port
    read -r post_host post_port <<<"${post}"
    log "stand ${STAND}: the Sentinel advertises ${post_host}:${post_port} as the master (was ${pre_host}:${pre_port}) after $((t1 - t0))s"

    # The Sentinel switches its view immediately; the promoted node still has to
    # become writable. Wait for that too, so that the "after the switch" segment
    # really is the post-failover state.
    local promoted="" promoted_service="" deadline_p=$((SECONDS + 90))
    while :; do
        promoted_service="$(master_service || true)"
        if [[ -n "${promoted_service}" && "${promoted_service}" != "${pre_service}" ]]; then
            promoted="${promoted_service}"
            break
        fi
        ((SECONDS >= deadline_p)) && die "stand ${STAND}: no node was promoted to master within 90s"
        sleep 1
    done
    local t2
    t2="$(date -u +%s)"
    log "stand ${STAND}: ${promoted} reports role:master after $((t2 - t0))s"
    summary "master after the failover: Sentinel advertises ${post_host}:${post_port}, promoted container ${promoted}, writable after $((t2 - t0))s"

    # A Sentinel that advertises an IP must point at the promoted container.
    if [[ "${post_host}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        local ip
        ip="$(service_ip "${promoted}")"
        [[ "${ip}" == "${post_host}" ]] ||
            die "stand ${STAND}: the Sentinel advertises ${post_host} but ${promoted} is ${ip}"
        log "stand ${STAND}: ${promoted} really is ${post_host}"
    fi

    # Everything the load produced up to this point happened during the switch.
    local switched_at
    switched_at="$(wc -l <"${during}" | tr -d ' ')"
    log "stand ${STAND}: keeping the load running for ${POST_SWITCH_SECONDS}s after the switch"
    sleep "${POST_SWITCH_SECONDS}"
    touch "${stop}"
    wait "${LOAD_PID}" || true
    unset LOAD_PID

    local during_before during_after
    during_before="$(tmp_file requests-during-switch.txt)"
    during_after="$(tmp_file requests-after-switch.txt)"
    head -n "${switched_at}" "${during}" >"${during_before}"
    tail -n "+$((switched_at + 1))" "${during}" >"${during_after}"

    # 3. Zero 5xx across the failover - the point of the whole stand.
    local total
    total="$(wc -l <"${during}" | tr -d ' ')"
    ((total >= MIN_LOAD_REQUESTS)) ||
        die "stand ${STAND}: only ${total} requests were served during the failover, expected at least ${MIN_LOAD_REQUESTS}"
    show_histogram "${during_before}" "during the switch ($((switched_at)) requests)"
    show_histogram "${during_after}" "after the switch ($((total - switched_at)) requests)"
    assert_no_5xx "${during}" "the whole failover window"
    assert_no_transport_errors "${during}" "the whole failover window"
    summary "failover window: ${total} requests, 0 x 5xx"

    # Requests keep being rejected after the switch: the plugin talks to the new
    # master and the bucket state followed it.
    local n429
    n429="$(count_codes "${during_after}" 429)"
    ((n429 > 0)) || die "stand ${STAND}: no 429 after the switch - the limiter stopped enforcing"
    log "stand ${STAND}: after the switch: ${n429} x 429"
    summary "after the switch: ${n429} x 429 (limit still enforced)"

    # 4. A fresh source on the new master: the limit still applies, from scratch.
    check_drain chaos-after "${url}" 25 "${MIN_PASS}" "${MAX_PASS}" \
        -H 'Host: localhost' -H 'X-Api-Key: nightly-after'

    # 5. The new master is a real master, the old one became a replica, and the
    #    buckets live on the new master.
    local new_role old_role
    new_role="$(redis_role_of "${promoted}" "${REDIS_PORT}")"
    old_role="$(redis_role_of "${pre_service}" "${REDIS_PORT}")"
    [[ "${new_role}" == "master" ]] || die "stand ${STAND}: ${promoted} reports role=${new_role}, expected master"
    [[ "${old_role}" == "slave" ]] || die "stand ${STAND}: the previous master ${pre_service} reports role=${old_role}, expected slave"
    log "stand ${STAND}: ${promoted} role=${new_role}, ${pre_service} role=${old_role}"
    summary "roles after the failover: ${promoted}=${new_role}, ${pre_service}=${old_role}"

    local keys
    keys="$(redis_keys "${promoted}" -p "${REDIS_PORT}")"
    printf '%s\n' "${keys}" >"$(tmp_file redis-keys-new-master.txt)"
    assert_key_present "${keys}" 'rate:distributed-ratelimit@file:nightly-after' "bucket on the new master"
    save_sentinel_state "$(tmp_file sentinel-state-after.txt)"

    assert_log_contains reverse-proxy 'master switched from' "the plugin followed the failover"

    log "stand ${STAND}: OK"
    summary "result: OK"
}

main "$@"
