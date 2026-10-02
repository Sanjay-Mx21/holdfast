SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

GO       ?= go
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
PKG      := github.com/Sanjay-Mx21/holdfast
LDFLAGS  := -s -w -X $(PKG)/internal/platform/buildinfo.Version=$(VERSION) -X $(PKG)/internal/platform/buildinfo.Commit=$(COMMIT)
BINARIES := inventory queue booking holdfastctl contention fairness
IMAGES   := inventory queue booking holdfastctl
GOLANGCI_LINT_VERSION := v2.14.0
BUF_VERSION           := v1.73.0
SQLC_VERSION          := v1.31.1

# Local dependencies started by `make infra` / `make up`.
export HOLDFAST_TEST_VALKEY_ADDR  ?= localhost:6379
export HOLDFAST_TEST_POSTGRES_DSN ?= postgres://holdfast:holdfast@localhost:5432/holdfast?sslmode=disable
export HOLDFAST_TEST_KAFKA_BROKERS ?= localhost:29092

NAME     ?= Demo Concert
CAPACITY ?= 1000

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

deps: ## Resolve modules and write go.sum (run once after cloning, then commit go.sum)
	$(GO) mod tidy

deps-upgrade: ## Upgrade every dependency to its latest release
	$(GO) get -u ./... && $(GO) mod tidy

tools: ## Install golangci-lint, buf and sqlc
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	$(GO) install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)

gen: ## Generate Go code: protobuf contracts (buf) and SQL queries (sqlc); needs make tools
	buf lint && buf format -w && buf generate
	sqlc vet && sqlc generate

fmt: ## Format all Go code
	gofmt -w -s .

vet: ## go vet, including integration-tagged files
	$(GO) vet ./... && $(GO) vet -tags=integration ./...

lint: ## golangci-lint + migration checks
	golangci-lint run ./...
	./scripts/check-migrations.sh

test: ## Unit tests with the race detector (no external dependencies)
	$(GO) test -race -count=1 ./...

itest: infra ## Integration tests against PostgreSQL, Valkey and Kafka (starts them with Docker)
	$(GO) test -race -count=1 -tags=integration ./...

cover: ## Unit + integration coverage report (coverage.html)
	$(GO) test -count=1 -tags=integration -coverprofile=coverage.out ./... && $(GO) tool cover -html=coverage.out -o coverage.html

build: ## Build every binary into ./bin
	@mkdir -p bin
	@for b in $(BINARIES); do $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/$$b ./cmd/$$b || exit 1; done

keys: ## Generate dev key pairs in .local/keys: admission tokens, booking-svc's service tokens (idempotent)
	$(GO) run ./cmd/holdfastctl keys generate --out-dir .local/keys --if-missing
	$(GO) run ./cmd/holdfastctl keys generate --out-dir .local/keys --name booking --if-missing

infra: ## Start only PostgreSQL, Valkey and Kafka
	docker compose up -d --wait postgres valkey kafka

topics: ## Create every Kafka topic (idempotent; make up does it too)
	$(GO) run ./cmd/holdfastctl kafka topics --brokers "$(HOLDFAST_TEST_KAFKA_BROKERS)"

up: keys ## Start the whole local stack (migrations run automatically)
	docker compose up -d --build
	@# Prometheus, Grafana and the NGINX edge read their mounted config at start-up
	@# only; restart them so a changed scrape config, dashboard or route loads.
	docker compose restart prometheus grafana nginx

down: ## Stop the stack (keeps data volumes)
	docker compose down

clean: ## Stop the stack, delete volumes and build output
	docker compose down -v; rm -rf bin coverage.out coverage.html e1-results.json e6-results.json

migrate: ## Apply migrations to the local database
	$(GO) run ./cmd/holdfastctl migrate --dsn "$(HOLDFAST_TEST_POSTGRES_DSN)"

event: ## Create and provision an event: make event NAME="Coldplay Mumbai" CAPACITY=1000
	$(GO) run ./cmd/holdfastctl event create --name "$(NAME)" --capacity $(CAPACITY) \
		--dsn "$(HOLDFAST_TEST_POSTGRES_DSN)" --valkey "$(HOLDFAST_TEST_VALKEY_ADDR)"

token: ## Print a dev admission token: make -s token EVENT=<event id>
	@test -n "$(EVENT)" || (echo "usage: make -s token EVENT=<event id>" >&2 && exit 1)
	@$(GO) run ./cmd/holdfastctl token mint --event "$(EVENT)" 2>/dev/null

run-inventory: ## Run inventory-svc on the host using .env
	@test -f .env || (echo "copy .env.example to .env first" >&2 && exit 1)
	set -a && . ./.env && set +a && $(GO) run ./cmd/inventory

e1: ## Experiment E1: contention against local Valkey and PostgreSQL
	$(GO) run ./cmd/contention -mode all -valkey "$(HOLDFAST_TEST_VALKEY_ADDR)" -dsn "$(HOLDFAST_TEST_POSTGRES_DSN)" -json e1-results.json

fairness-e6: ## Experiment E6: fairness of the queue order (lottery before T0, FIFO after)
	$(GO) run ./cmd/fairness -valkey "$(HOLDFAST_TEST_VALKEY_ADDR)" -json e6-results.json

load-e2: ## Experiment E2: k6 stampede on the waiting room through the edge (needs make up and k6)
	bash loadtest/e2/run.sh

docker: ## Build container images for the deployable binaries
	@for s in $(IMAGES); do docker build --build-arg SERVICE=$$s --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t holdfast/$$s:$(VERSION) . || exit 1; done

.PHONY: help deps deps-upgrade tools fmt vet lint test itest cover build keys infra up down clean migrate event token run-inventory e1 fairness-e6 load-e2 topics gen docker
