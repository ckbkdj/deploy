.PHONY: fmt test test-race vet check integration build local-up local-down remote-up observability-up doctor zip

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

vet:
	go vet ./...

check:
	@test -z "$$(gofmt -l .)" || (echo "run gofmt on:"; gofmt -l .; exit 1)
	@for f in scripts/*.sh; do bash -n "$$f"; done
	@node --check internal/webui/static/app.js
	@python3 scripts/validate-yaml.py

test-integration integration:
	./scripts/integration-test.sh

build:
	docker build --build-arg VERSION=$$(cat VERSION) --build-arg COMMIT=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown) -t newapi-risk-gateway:$$(cat VERSION) .

local-up:
	./scripts/bootstrap-local.sh

local-down:
	docker compose --env-file .env -f compose.local.yml down

remote-up:
	./scripts/bootstrap-remote.sh

observability-up:
	./scripts/deploy.sh observability-up

doctor:
	./scripts/doctor.sh local

zip:
	./scripts/package.sh
