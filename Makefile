# Domain Crawler — developer commands.
# Full environment description: docs/dev-environment.md

SHELL := /bin/bash

COMPOSE_FILE := deploy/docker/compose.yaml
COMPOSE := docker compose --project-directory . -f $(COMPOSE_FILE) --env-file .env

# app + testbed profiles — the standard developer scenario (make up)
APP_PROFILES := --profile app --profile testbed
ALL_PROFILES := --profile '*'

SERVICE ?=
NAME ?=
DOMAIN ?=
N ?= 1000000
FORCE ?= 0
# Resolvers for make check: primary — unbound testbed (test zones),
# fallbacks — public: the checker escalates to them on SERVFAIL/REFUSED
# (internal/checker/resolve.go), so one command checks both testbed
# domains (*.crawl.test) and real ones.
RESOLVER ?= 172.28.100.10:53
FALLBACK ?= 1.1.1.1:53,9.9.9.9:53

# Variables from .env that the Makefile itself needs
GO_VERSION     ?= $(shell sed -n 's/^GO_VERSION=\(.*\)/\1/p' .env 2>/dev/null)
ALPINE_VERSION ?= $(shell sed -n 's/^ALPINE_VERSION=\(.*\)/\1/p' .env 2>/dev/null)
PG_ADMIN_USER  ?= $(shell sed -n 's/^POSTGRES_ADMIN_USER=\(.*\)/\1/p' .env 2>/dev/null)
PG_DB          ?= $(shell sed -n 's/^POSTGRES_DB=\(.*\)/\1/p' .env 2>/dev/null)

BUILD_ARGS := --build-arg GO_VERSION=$(GO_VERSION) --build-arg ALPINE_VERSION=$(ALPINE_VERSION)

# Version/revision of the production images for crawler_build_info metrics (T-111):
# .git is excluded by .dockerignore, the values are computed on the calling machine
# and passed as build args (ARG BUILD_VERSION/BUILD_REVISION in the Dockerfile);
# without git (or outside a repository) — graceful fallback to unknown.
BUILD_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo unknown)
BUILD_REVISION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
VERSION_ARGS := --build-arg BUILD_VERSION=$(BUILD_VERSION) --build-arg BUILD_REVISION=$(BUILD_REVISION)

.DEFAULT_GOAL := help

define check_env
	@test -f .env || { echo 'ERROR: no .env file. Run: cp .env.example .env'; exit 1; }
endef

.PHONY: help
help: ## Show the command list
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------- lifecycle

.PHONY: up
up: ## Start infrastructure + testbed + apps
	$(check_env)
	mkdir -p var/incoming var/content
	$(MAKE) warm-cache
	$(COMPOSE) $(APP_PROFILES) up -d --build --wait

.PHONY: up-infra
up-infra: ## Only postgres, redis, unbound (services apply the schema on startup)
	$(check_env)
	$(COMPOSE) up -d --build --wait

.PHONY: up-internet
up-internet: ## Enable real DNS recursion (requires ALLOW_INTERNET=1 in .env)
	$(check_env)
	@grep -q '^ALLOW_INTERNET=1' .env || { echo \
	  'REFUSAL: internet access is enabled explicitly.';\
	  echo 'Add to .env:  ALLOW_INTERNET=1'; exit 1; }
	$(COMPOSE) $(APP_PROFILES) up -d --build --wait
	UNBOUND_MODE=internet UNBOUND_HEALTH_NAME=example.com \
		$(COMPOSE) up -d --no-deps --force-recreate unbound
	@echo 'WARNING: DNS now resolves into the real internet.'
	@echo 'The crawler User-Agent must carry a dev marker (CRAWLER_USER_AGENT in .env).'
	@echo 'Return to the isolated mode: make up'

.PHONY: reset-internet
reset-internet: ## Full environment reset and start with real DNS (ALLOW_INTERNET=1)
	$(check_env)
	@grep -q '^ALLOW_INTERNET=1' .env || { echo \
	  'REFUSAL: internet access is enabled explicitly (reset cancelled).';\
	  echo 'Add to .env:  ALLOW_INTERNET=1'; exit 1; }
	$(MAKE) clean FORCE=1
	rm -rf var/content/* var/incoming/processed/* var/incoming/failed/*
	$(MAKE) mod-download
	$(MAKE) warm-cache
	$(MAKE) up-internet

.PHONY: down
down: ## Stop the environment (volumes are kept)
	$(check_env)
	$(COMPOSE) $(ALL_PROFILES) down --remove-orphans

.PHONY: clean
clean: ## Stop the environment and remove all volumes (with confirmation)
	$(check_env)
	@if [ "$(FORCE)" != "1" ]; then \
	  read -r -p 'Remove all volumes (DB data, build caches, content)? [y/N] ' ans; \
	  [ "$$ans" = "y" ] || { echo 'Cancelled (or make clean FORCE=1).'; exit 1; }; \
	fi
	$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans

.PHONY: logs
logs: ## Service logs: make logs SERVICE=analyzer
	$(check_env)
	$(COMPOSE) logs --tail=200 -f $(SERVICE)

.PHONY: ps
ps: ## State of all services
	$(check_env)
	$(COMPOSE) $(ALL_PROFILES) ps -a

# --------------------------------------------------------------- build and code

.PHONY: build
build: ## Build dev images (hot reload)
	$(check_env)
	# All profiles: without them compose builds only the services without a profile
	# (unbound); go-dev (app+tools) and mockserver (testbed) would stay unbuilt.
	$(COMPOSE) $(ALL_PROFILES) build

# Shared build cache warm-up: one compilation of all services in tools (2G limit)
# instead of eight parallel air builds with a 256m limit — on a cold cache
# (first run / after make clean) parallel compilation does not fit in memory.
.PHONY: warm-cache
warm-cache: ## Warm up the shared Go build cache (one compilation in tools)
	$(check_env)
	$(COMPOSE) run --rm tools go build ./cmd/...

.PHONY: build-runtime
build-runtime: ## Build production images (runtime + render) for verification
	$(check_env)
	docker build $(BUILD_ARGS) $(VERSION_ARGS) -f deploy/docker/Dockerfile.go \
		--target runtime -t crawler/runtime:check .
	docker build $(BUILD_ARGS) $(VERSION_ARGS) -f deploy/docker/Dockerfile.render \
		--target render -t crawler/render:check .

GO_PKGS := ./cmd/... ./internal/... ./migrations/... ./test/... ./deploy/postgres

# gofmt paths (directories): the same coverage as lint — gofmt in CI is gated
# by golangci-lint (formatters.gofmt over GO_PKGS, including migrations/embed.go),
# make fmt must fix the same files, otherwise the gate catches what fmt does not.
GOFMT_PATHS := cmd internal test deploy/postgres migrations

.PHONY: test
test: ## Go unit tests (in a container)
	$(check_env)
	$(COMPOSE) run --rm tools go test $(GO_PKGS) -count=1

# -race for packages with goroutine loops (T-190): the regular make test
# runs without -race (CGO_ENABLED=0 in the image — production binaries are static);
# the race detector requires cgo — enabled for a single run container
# (-e CGO_ENABLED=1; gcc is present in tools — build-base). By default only
# packages with goroutine loops/workers — the race detector must
# see all of them (incident of Nov 10: races in monitor triage tests slipped
# past the local make test). The list grows as new
# concurrent packages appear (RACE_PKGS='./internal/a/... ./internal/b').
# Timing: the full set takes ~50 s (render/content with -race — the bulk).
RACE_PKGS ?= ./internal/monitor \
	./internal/scheduler \
	./internal/renderworker \
	./internal/httpworker \
	./internal/dnsworker \
	./internal/queue \
	./internal/render \
	./internal/content

.PHONY: test-race
test-race: ## Tests with the race detector (-race, internal/monitor by default)
	$(check_env)
	$(COMPOSE) run --rm -e CGO_ENABLED=1 tools go test -race $(RACE_PKGS) -count=1

.PHONY: test-integration
test-integration: ## Environment integration tests on the testbed
	$(check_env)
	# --build: HTTP-stage scenarios live in the mockserver source (scenario
	# virtual hosts, TLS certificates) — the image must be newer than the source;
	# the mockserver-build stage builds in seconds (stdlib-only).
	$(COMPOSE) --profile testbed --profile app up -d --build --wait \
		postgres redis unbound coredns testweb mockserver chromium
	# App services are not started, so the schema for tests is applied
	# by the CLI (the same migrations/ set); service runners are covered by TestMigrate_*.
	$(COMPOSE) run --rm tools sh -c \
	  'migrate -path /migrations -database "$$APP_PG_DSN" up'
	# ./cmd/crawlerctl — integration test of the T-243 fixture import
	# (CRAWLER_INTEGRATION gate, isolated schema in the same DB).
	$(COMPOSE) run --rm -e CRAWLER_INTEGRATION=1 tools \
		go test ./test/integration ./cmd/crawlerctl -count=1 -v -timeout 10m

# --------------------------------------------------------------- mutation testing
# How to read reports, gate on the threshold and flag false mutants —
# docs/mutation-testing.md.

# Default coverage — the "logic core": business-logic packages with
# hermetic, fast unit tests (no network/DB/Chromium). A full
# run takes ~2.5 h (2632 mutants, score 1.000); config and web are not included:
# hundreds of mutants there cover yaml binding and routing plumbing at low signal.
# To run a single package:
#   make mutate PKGS='./internal/httpfetch/...'
# CI (mutation.yml) splits this set into three parts for the weekly run
# by measured time and runs them as parallel jobs —
# full coverage on a hosted runner does not fit into the 6-hour job ceiling
# (render at ~29 s/mutant — a separate job). A local run is
# always the full set below (or a single package).
PKGS ?= ./internal/analyzer/... \
	./internal/dashboard/... ./internal/idn/... ./internal/model/... \
	./internal/monitor/... ./internal/queue/... ./internal/render/... \
	./internal/rules/... ./internal/scheduler/... ./internal/status/...
# go test timeout per mutant, seconds (package compilation + test)
MUTATE_TIMEOUT ?= 60
# mutation score threshold 0..1: below it — non-zero exit code (CI gate;
# locally 0 = report only)
MUTATION_MIN_SCORE ?= 0

.PHONY: mutate
mutate: ## Mutation testing with go-mutesting (report in var/mutation/)
	$(check_env)
	# The report directory is created on the host BEFORE compose run: on a clean
	# checkout (CI) var/ is missing, and the missing host paths of the bind mounts
	# (./var/incoming, ./var/content and the var/ parent) are created by the docker
	# daemon as root — after that the unprivileged container
	# (user DEV_UID) cannot create var/mutation inside the /src bind
	# (Permission denied; the mutation CI failed this way from the first run).
	mkdir -p var/mutation
	$(COMPOSE) run --rm \
		-e MUTATE_PKGS='$(PKGS)' \
		-e MUTATE_TIMEOUT='$(MUTATE_TIMEOUT)' \
		-e MUTATION_MIN_SCORE='$(MUTATION_MIN_SCORE)' \
		tools sh deploy/docker/scripts/mutate.sh

.PHONY: lint
lint: ## golangci-lint (in a container)
	$(check_env)
	$(COMPOSE) run --rm tools golangci-lint run $(GO_PKGS)

.PHONY: fmt
fmt: ## gofmt + go vet (in a container)
	$(check_env)
	$(COMPOSE) run --rm tools sh -c 'gofmt -l -w $(GOFMT_PATHS) && go vet $(GO_PKGS)'

.PHONY: mod-download
mod-download: ## Fetch Go modules into the shared cache (needs internet; tools has it)
	$(check_env)
	@# Without all: download only the modules needed to build the project packages.
	@# go mod download all would fetch the whole transitive graph (migrate requires
	@# drivers of every DBMS) and append their h1 hashes to go.sum (~350 lines of noise).
	$(COMPOSE) run --rm tools go mod download

# --------------------------------------------------------------- DB
# Services apply migrations automatically on startup (internal/migrate);
# the targets below are manual operations via the golang-migrate CLI in the tools container.

.PHONY: migrate-up
migrate-up: ## Apply migrations manually (usually not needed: services do it themselves)
	$(check_env)
	$(COMPOSE) run --rm tools sh -c \
	  'migrate -path /migrations -database "$$APP_PG_DSN" up'

.PHONY: migrate-down
migrate-down: ## Roll back all migrations (services have no rollback)
	$(check_env)
	$(COMPOSE) run --rm tools sh -c \
	  'migrate -path /migrations -database "$$APP_PG_DSN" down --all'

.PHONY: migrate-new
migrate-new: ## Create a migration pair: make migrate-new NAME=add_index
	$(check_env)
	@test -n "$(NAME)" || { echo 'Usage: make migrate-new NAME=migration_name'; exit 1; }
	$(COMPOSE) run --rm tools migrate create -ext sql -dir /migrations -seq $(NAME)

.PHONY: psql
psql: ## PostgreSQL console
	$(check_env)
	$(COMPOSE) exec postgres psql -U $(PG_ADMIN_USER) -d $(PG_DB)

.PHONY: redis-cli
redis-cli: ## Redis console
	$(check_env)
	$(COMPOSE) exec redis redis-cli

.PHONY: seed
seed: ## Test data: domain lists in var/incoming/<zone>/add/
	$(check_env)
	mkdir -p var/incoming
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl seed --incoming /data/incoming

.PHONY: seed-bulk
seed-bulk: ## Large synthetic set: make seed-bulk N=1000000
	$(check_env)
	@echo 'Generating N=$(N) domains into var/incoming/crawl.test/add/seed-bulk.txt'
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl seed --incoming /data/incoming --bulk $(N)

.PHONY: db-reset
db-reset: ## Full recreation of the project DB and volumes (with confirmation)
	$(check_env)
	@echo 'This is the same as make clean + make up (removes volumes, including caches).'
	$(MAKE) clean
	# After clean the shared gomodcache is empty, and the app services cannot
	# download modules (DNS — hermetic unbound only): warm it up from tools.
	$(MAKE) mod-download
	# Cold gocache: parallel air builds of eight services do not fit
	# in memory (256m limits) — warm the cache with one compilation in tools.
	$(MAKE) warm-cache
	$(MAKE) up

# --------------------------------------------------------------- testbed

.PHONY: testbed-gen
testbed-gen: ## Regenerate CoreDNS zones and Caddy fixtures from scenarios.yaml
	$(check_env)
	$(COMPOSE) run --rm tools go run ./cmd/testbedgen --root /src
	-$(COMPOSE) --profile testbed restart coredns testweb mockserver 2>/dev/null
	-$(COMPOSE) restart unbound 2>/dev/null

.PHONY: test-rules
test-rules: ## Run Laravel rules against the testdata/laravel-samples corpus (no network)
	$(check_env)
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl test-rules \
		--rules /rules --samples /testdata/laravel-samples

.PHONY: rules-reload
rules-reload: ## SIGHUP the analyzer — reload Laravel rules without a restart
	$(check_env)
	$(COMPOSE) exec -T analyzer pkill -HUP -f 'tmp/analyzer$$' \
		|| echo 'analyzer not running (make up)?'

.PHONY: reanalyze
reanalyze: ## Re-run result analysis (crawlerctl)
	$(check_env)
	$(COMPOSE) run --rm -e CRAWLER_ANALYZER_RULES_DIR=/rules \
		tools go run ./cmd/crawlerctl reanalyze

# --------------------------------------------------------------- debug / CLI

.PHONY: debug
debug: ## Run a service under delve: make debug SERVICE=dns-worker
	$(check_env)
	@test -n "$(SERVICE)" || { echo 'Usage: make debug SERVICE=dns-worker'; exit 1; }
	$(COMPOSE) run --rm --no-deps -p 127.0.0.1:2345:2345 $(SERVICE) \
		dlv debug ./cmd/$(SERVICE) \
		--headless --listen=0.0.0.0:2345 --api-version=2 --accept-multiclient

.PHONY: status
status: ## Zones and queues summary (crawlerctl status)
	$(check_env)
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl status

.PHONY: domain
domain: ## Domain info: make domain NAME=laravel-11-vite.crawl.test
	$(check_env)
	@test -n "$(NAME)" || { echo 'Usage: make domain NAME=example.crawl.test'; exit 1; }
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl domain "$(NAME)"

.PHONY: check
check: ## Live domain check: make check DOMAIN=example.com
	$(check_env)
	@test -n "$(DOMAIN)" || { echo 'Usage: make check DOMAIN=example.com'; exit 1; }
	$(COMPOSE) run --rm \
		-e CRAWLER_DNS_RESOLVERS=$(RESOLVER) \
		-e CRAWLER_DNS_FALLBACK_RESOLVERS=$(FALLBACK) \
		-e CRAWLER_ANALYZER_RULES_DIR=/rules \
		tools go run ./cmd/crawlerctl check "$(DOMAIN)"

.PHONY: shell
shell: ## Shell in a container: make shell [SERVICE=tools]
	$(check_env)
	@if [ -z "$(SERVICE)" ]; then \
	  $(COMPOSE) --profile tools up -d --no-deps tools; \
	  $(COMPOSE) exec tools sh; \
	else \
	  $(COMPOSE) exec $(SERVICE) sh; \
	fi
