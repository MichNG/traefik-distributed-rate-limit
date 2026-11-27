.PHONY: test test-race cover lint yaegi-check integration integration-sentinel integration-cookie integration-acl integration-nightly chaos bench bench-e2e

# Runs gofmt, go vet and the whole test suite. The Go caches are kept inside
# the repository so that the target also works in restricted environments.
test:
	@./run-tests.sh -count=1

test-race:
	@./run-tests.sh -race -count=1

cover:
	@./run-tests.sh -count=1 -covermode=atomic -coverprofile=coverage.txt
	@go tool cover -html=coverage.txt -o cover.html

# Loads the plugin with the same Yaegi interpreter as Traefik (v0.16.1) and
# drives a few requests through it.
yaegi-check:
	@cd tools/yaegicheck && go run .

integration:
	@docker compose up -d

integration-sentinel:
	@docker compose -f docker-compose.sentinel.yaml up -d

integration-cookie:
	@docker compose -f docker-compose.cookie.yaml up -d

# Redis Sentinel behind an ACL user, with separate Sentinel credentials.
integration-acl:
	@docker compose -f docker-compose.acl.yaml up -d

# The docker stands the nightly workflow runs (same scripts, non-default host
# ports, every script brings its own stand up and down).
integration-nightly:
	@./hack/ci/integration-single.sh
	@./hack/ci/integration-cookie.sh
	@./hack/ci/integration-acl.sh

# Redis Sentinel failover under load: the data path must see zero 5xx.
chaos:
	@./hack/ci/chaos-sentinel.sh

# In-process benchmarks of the middleware (a fake RESP store, no docker).
bench:
	@./run-tests.sh -run '^$$' -bench . -benchmem

# End-to-end benchmark: real Traefik and Redis containers, client-side RPS and
# p50/p95/p99. Knobs: BENCH_WORKERS, BENCH_DURATION, BENCH_WARMUP, BENCH_KEEP.
bench-e2e:
	@./hack/bench/run.sh
