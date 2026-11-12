#!/usr/bin/env bash
# Runs the plugin test suite inside the DSH sandbox.
#
# The sandbox blocks Go's default caches (~/go, ~/Library/Caches/go-build) and
# occasionally denies the first module-cache access of a fresh shell, so all Go
# state is kept inside the workspace and a failed run is retried once.
set -uo pipefail

cd "$(dirname "$0")" || exit 1

export GOCACHE="$PWD/.gocache"
export GOTMPDIR="$PWD/.gotmp"
export GOMODCACHE="$PWD/.gomodcache"
export GOPATH="$PWD/.gopath"
export GOFLAGS="-mod=mod"
mkdir -p "$GOCACHE" "$GOTMPDIR" "$GOMODCACHE" "$GOPATH"

run() {
    gofmt -l . | grep -v gomodcache
    go vet ./... || return 1
    go test "$@" ./...
}

if run "$@"; then
    exit 0
fi

echo "--- first attempt failed, retrying (sandbox module-cache flakiness) ---"
run "$@"
