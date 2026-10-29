.PHONY: check build standards sync-standards
SHELL := bash
ORG = https://github.com/junglestyle/.github

check: build
	golangci-lint run
	go test -race ./...
	go mod tidy -diff
	git ls-files -z '*.sh' | xargs -0 -r shellcheck

# The static binary RUN-1 asks for.
build:
	CGO_ENABLED=0 go build -o prometheus-tsdb-reader .

standards:
	@tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT && \
	git clone -q --depth 1 --branch main $(ORG) "$$tmp" && \
	"$$tmp/code-standards/sync.sh" check "$$tmp"

sync-standards:
	@tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT && \
	git clone -q --depth 1 --branch main $(ORG) "$$tmp" && \
	"$$tmp/code-standards/sync.sh" sync "$$tmp" "$$(git -C "$$tmp" rev-parse HEAD)"
