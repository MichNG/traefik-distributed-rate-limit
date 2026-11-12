.PHONY: test test-race cover lint yaegi-check integration integration-sentinel integration-cookie

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
