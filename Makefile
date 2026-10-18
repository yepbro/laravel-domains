# Domain Crawler — команды разработчика.
# Полное описание окружения: docs/dev-environment.md

SHELL := /bin/bash

COMPOSE_FILE := deploy/docker/compose.yaml
COMPOSE := docker compose --project-directory . -f $(COMPOSE_FILE) --env-file .env

# Профиль app + testbed — стандартный сценарий разработчика (make up)
APP_PROFILES := --profile app --profile testbed
ALL_PROFILES := --profile '*'

SERVICE ?=
NAME ?=
N ?= 1000000
FORCE ?= 0

# Перенные из .env, нужные самому Makefile
GO_VERSION     ?= $(shell sed -n 's/^GO_VERSION=\(.*\)/\1/p' .env 2>/dev/null)
ALPINE_VERSION ?= $(shell sed -n 's/^ALPINE_VERSION=\(.*\)/\1/p' .env 2>/dev/null)
PG_ADMIN_USER  ?= $(shell sed -n 's/^POSTGRES_ADMIN_USER=\(.*\)/\1/p' .env 2>/dev/null)
PG_DB          ?= $(shell sed -n 's/^POSTGRES_DB=\(.*\)/\1/p' .env 2>/dev/null)

BUILD_ARGS := --build-arg GO_VERSION=$(GO_VERSION) --build-arg ALPINE_VERSION=$(ALPINE_VERSION)

.DEFAULT_GOAL := help

define check_env
	@test -f .env || { echo 'ОШИБКА: нет файла .env. Выполните: cp .env.example .env'; exit 1; }
endef

.PHONY: help
help: ## Показать список команд
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------- жизненный цикл

.PHONY: up
up: ## Запустить инфраструктуру + testbed + приложения
	$(check_env)
	mkdir -p var/incoming var/content
	$(MAKE) warm-cache
	$(COMPOSE) $(APP_PROFILES) up -d --build --wait

.PHONY: up-infra
up-infra: ## Только postgres, redis, unbound (схему применят сервисы при старте)
	$(check_env)
	$(COMPOSE) up -d --build --wait

.PHONY: up-internet
up-internet: ## Включить реальную рекурсию DNS (требует ALLOW_INTERNET=1 в .env)
	$(check_env)
	@grep -q '^ALLOW_INTERNET=1' .env || { echo \
	  'ОТКАЗ: выход в интернет включается явно.';\
	  echo 'Добавьте в .env:  ALLOW_INTERNET=1'; exit 1; }
	$(COMPOSE) $(APP_PROFILES) up -d --build --wait
	UNBOUND_MODE=internet UNBOUND_HEALTH_NAME=example.com \
		$(COMPOSE) up -d --no-deps --force-recreate unbound
	@echo 'ВНИМАНИЕ: DNS теперь резолвится в реальный интернет.'
	@echo 'User-Agent краулера должен содержать пометку dev (CRAWLER_USER_AGENT в .env).'
	@echo 'Возврат в изолированный режим: make up'

.PHONY: down
down: ## Остановить окружение (тома сохраняются)
	$(check_env)
	$(COMPOSE) $(ALL_PROFILES) down --remove-orphans

.PHONY: clean
clean: ## Остановить окружение и удалить все тома (с подтверждением)
	$(check_env)
	@if [ "$(FORCE)" != "1" ]; then \
	  read -r -p 'Удалить все тома (данные БД, кэши сборки, контент)? [y/N] ' ans; \
	  [ "$$ans" = "y" ] || { echo 'Отменено (или make clean FORCE=1).'; exit 1; }; \
	fi
	$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans

.PHONY: logs
logs: ## Логи сервиса: make logs SERVICE=analyzer
	$(check_env)
	$(COMPOSE) logs --tail=200 -f $(SERVICE)

.PHONY: ps
ps: ## Состояние всех сервисов
	$(check_env)
	$(COMPOSE) $(ALL_PROFILES) ps -a

# --------------------------------------------------------------- сборка и код

.PHONY: build
build: ## Собрать dev-образы (hot reload)
	$(check_env)
	# Все профили: без них compose собирает только сервисы без профиля
	# (unbound); go-dev (app+tools) и mockserver (testbed) остались бы несобранными.
	$(COMPOSE) $(ALL_PROFILES) build

# Прогрев общего кэша сборки: одна компиляция всех сервисов в tools (лимит 2G)
# вместо восьми параллельных air-сборок с лимитом 256m — на холодном кэше
# (первый запуск / после make clean) параллельная компиляция не влезает в память.
.PHONY: warm-cache
warm-cache: ## Прогреть общий кэш сборки Go (одна компиляция в tools)
	$(check_env)
	$(COMPOSE) run --rm tools go build ./cmd/...

.PHONY: build-runtime
build-runtime: ## Собрать боевые образы (runtime + render) для проверки
	$(check_env)
	docker build $(BUILD_ARGS) -f deploy/docker/Dockerfile.go \
		--target runtime -t crawler/runtime:check .
	docker build $(BUILD_ARGS) -f deploy/docker/Dockerfile.render \
		--target render -t crawler/render:check .

GO_PKGS := ./cmd/... ./internal/... ./migrations/... ./test/... ./deploy/postgres

# Пути gofmt (каталогами): тот же охват, что у линта — gofmt в CI гейтится
# golangci-lint (formatters.gofmt на GO_PKGS, включая migrations/embed.go),
# make fmt должен править те же файлы, иначе гейт ловит то, что fmt не чинит.
GOFMT_PATHS := cmd internal test deploy/postgres migrations

.PHONY: test
test: ## Модульные тесты Go (в контейнере)
	$(check_env)
	$(COMPOSE) run --rm tools go test $(GO_PKGS) -count=1

.PHONY: test-integration
test-integration: ## Интеграционные тесты окружения на testbed
	$(check_env)
	# --build: сценарии HTTP-этапа живут в исходнике mockserver (виртуальные
	# хосты сценариев, TLS-сертификаты) — образ обязан быть свежее исходника;
	# стадия mockserver-build собирается за секунды (stdlib-only).
	$(COMPOSE) --profile testbed --profile app up -d --build --wait \
		postgres redis unbound coredns testweb mockserver chromium
	# Сервисы приложения не запускаются, поэтому схему для тестов накатывает
	# CLI (тот же набор migrations/); раннер сервисов проверяют TestMigrate_*.
	$(COMPOSE) run --rm tools sh -c \
	  'migrate -path /migrations -database "$$APP_PG_DSN" up'
	$(COMPOSE) run --rm -e CRAWLER_INTEGRATION=1 tools \
		go test ./test/integration -count=1 -v -timeout 10m

.PHONY: lint
lint: ## golangci-lint (в контейнере)
	$(check_env)
	$(COMPOSE) run --rm tools golangci-lint run $(GO_PKGS)

.PHONY: fmt
fmt: ## gofmt + go vet (в контейнере)
	$(check_env)
	$(COMPOSE) run --rm tools sh -c 'gofmt -l -w $(GOFMT_PATHS) && go vet $(GO_PKGS)'

.PHONY: mod-download
mod-download: ## Докачать Go-модули в общий кэш (нужен интернет; tools его имеет)
	$(check_env)
	$(COMPOSE) run --rm tools go mod download all

# --------------------------------------------------------------- БД
# Сервисы применяют миграции автоматически при старте (internal/migrate);
# цели ниже — ручные операции через CLI golang-migrate в контейнере tools.

.PHONY: migrate-up
migrate-up: ## Применить миграции вручную (обычно не нужно: сервисы сами)
	$(check_env)
	$(COMPOSE) run --rm tools sh -c \
	  'migrate -path /migrations -database "$$APP_PG_DSN" up'

.PHONY: migrate-down
migrate-down: ## Откатить все миграции (у сервисов отката нет)
	$(check_env)
	$(COMPOSE) run --rm tools sh -c \
	  'migrate -path /migrations -database "$$APP_PG_DSN" down --all'

.PHONY: migrate-new
migrate-new: ## Создать пару миграций: make migrate-new NAME=add_index
	$(check_env)
	@test -n "$(NAME)" || { echo 'Использование: make migrate-new NAME=имя_миграции'; exit 1; }
	$(COMPOSE) run --rm tools migrate create -ext sql -dir /migrations -seq $(NAME)

.PHONY: psql
psql: ## Консоль PostgreSQL
	$(check_env)
	$(COMPOSE) exec postgres psql -U $(PG_ADMIN_USER) -d $(PG_DB)

.PHONY: redis-cli
redis-cli: ## Консоль Redis
	$(check_env)
	$(COMPOSE) exec redis redis-cli

.PHONY: seed
seed: ## Тестовые данные: списки доменов в var/incoming/<зона>/add/
	$(check_env)
	mkdir -p var/incoming
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl seed --incoming /data/incoming

.PHONY: seed-bulk
seed-bulk: ## Большой синтетический набор: make seed-bulk N=1000000
	$(check_env)
	@echo 'Генерация N=$(N) доменов в var/incoming/crawl.test/add/seed-bulk.txt'
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl seed --incoming /data/incoming --bulk $(N)

.PHONY: db-reset
db-reset: ## Полное пересоздание БД и томов проекта (с подтверждением)
	$(check_env)
	@echo 'Это то же самое, что make clean + make up (удаление томов, включая кэши).'
	$(MAKE) clean
	$(MAKE) up

# --------------------------------------------------------------- testbed

.PHONY: testbed-gen
testbed-gen: ## Регенерировать зоны CoreDNS и фикстуры Caddy из scenarios.yaml
	$(check_env)
	$(COMPOSE) run --rm tools go run ./cmd/testbedgen --root /src
	-$(COMPOSE) --profile testbed restart coredns testweb mockserver 2>/dev/null
	-$(COMPOSE) restart unbound 2>/dev/null

.PHONY: test-rules
test-rules: ## Прогон правил Laravel по корпусу testdata/laravel-samples (без сети)
	$(check_env)
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl test-rules \
		--rules /rules --samples /testdata/laravel-samples

.PHONY: rules-reload
rules-reload: ## SIGHUP анализатору — перезагрузить правила Laravel без рестарта
	$(check_env)
	$(COMPOSE) exec -T analyzer pkill -HUP -f 'tmp/analyzer$$' \
		|| echo 'analyzer не запущен (make up)?'

.PHONY: reanalyze
reanalyze: ## Запустить переанализ результатов (crawlerctl)
	$(check_env)
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl reanalyze

# --------------------------------------------------------------- отладка / CLI

.PHONY: debug
debug: ## Запустить сервис под delve: make debug SERVICE=dns-worker
	$(check_env)
	@test -n "$(SERVICE)" || { echo 'Использование: make debug SERVICE=dns-worker'; exit 1; }
	$(COMPOSE) run --rm --no-deps -p 127.0.0.1:2345:2345 $(SERVICE) \
		dlv debug ./cmd/$(SERVICE) \
		--headless --listen=0.0.0.0:2345 --api-version=2 --accept-multiclient

.PHONY: status
status: ## Сводка по зонам и очередям (crawlerctl status)
	$(check_env)
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl status

.PHONY: domain
domain: ## Информация о домене: make domain NAME=laravel-11-vite.crawl.test
	$(check_env)
	@test -n "$(NAME)" || { echo 'Использование: make domain NAME=example.crawl.test'; exit 1; }
	$(COMPOSE) run --rm tools go run ./cmd/crawlerctl domain "$(NAME)"

.PHONY: shell
shell: ## Оболочка в контейнере: make shell [SERVICE=tools]
	$(check_env)
	@if [ -z "$(SERVICE)" ]; then \
	  $(COMPOSE) --profile tools up -d --no-deps tools; \
	  $(COMPOSE) exec tools sh; \
	else \
	  $(COMPOSE) exec $(SERVICE) sh; \
	fi
