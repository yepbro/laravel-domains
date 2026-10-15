# Dev-окружение Domain Crawler

Воспроизводимое окружение разработки на Docker Compose: PostgreSQL, Redis,
локальный DNS-резолвер (unbound), изолированный тестовый стенд «мини-интернет»
(CoreDNS + Caddy + mockserver), headless-Chromium, мониторинг и Go-сервисы
с горячей перезагрузкой кода.

Соответствует `tasks/TZ_docker_dev_environment.md`.

## 1. Требования и быстрый старт

Требования к хосту:

- Linux (Ubuntu LTS), Docker Engine ≥ 24, Docker Compose v2 (`docker compose`);
- ≥ 8 ГБ RAM (рекомендуется 16), ≥ 20 ГБ свободного места;
- Go на хосте **не нужен** — всё собирается и тестируется в контейнерах.

Быстрый старт:

```bash
# 1. Клонировать репозиторий и войти
cd laravel-domains

# 2. Создать .env (UID/GID подставляются автоматически при желании)
cp .env.example .env

# 3. Поднять инфраструктуру + testbed + приложения (первый запуск: 5–15 мин)
make up

# 4. Проверить состояние (все healthcheck должны стать healthy)
make ps

# 5. Загрузить тестовые списки доменов в var/incoming/
make seed

# 6. Прогнать тесты окружения
make test-integration
```

Управление:

```bash
make logs SERVICE=analyzer   # логи конкретного сервиса
make psql                    # консоль PostgreSQL
make redis-cli               # консоль Redis
make shell                   # оболочка в tools-контейнере (Go-тулчейн)
make down                    # остановить (тома сохранятся)
make clean                   # остановить и удалить тома (с подтверждением)
```

## 2. Профили и типовые сценарии

Профили compose (`deploy/docker/compose.yaml`):

| Профиль | Что включает |
|---|---|
| (по умолчанию) | `postgres`, `redis`, `unbound`, разовая `migrate` |
| `testbed` | `coredns`, `testweb` (Caddy), `mockserver` |
| `app` | Go-сервисы (`ingest` … `monitor`, `render-worker`) + `chromium` |
| `monitoring` | `prometheus` (UI на http://127.0.0.1:9090) |
| `tools` | `adminer` (http://127.0.0.1:8080), `redis-insight` (http://127.0.0.1:5540), контейнер `tools` |

Типовые сценарии:

```bash
make up-infra        # только БД/Redis/DNS (работа над схемой или миграциями)
make up              # инфраструктура + testbed + приложения (стандартный день)
make test            # модульные тесты (в контейнере, Go на хосте не нужен)
make lint            # golangci-lint
make test-rules      # правила Laravel по корпусу testdata/laravel-samples
make test-integration# интеграционные тесты окружения на testbed
```

Мониторинг и инструменты поднимаются по требованию:

```bash
docker compose --project-directory . -f deploy/docker/compose.yaml --env-file .env \
  --profile monitoring --profile tools up -d
```

### Режим internet (осторожно)

По умолчанию окружение **изолировано от интернета**: unbound форвардит все
запросы в coredns, тестовые зоны отвечают авторитетно, всё остальное —
REFUSED. Реальная рекурсия включается явно:

```bash
# 1) в .env: ALLOW_INTERNET=1
make up-internet     # пересоздаст unbound в режиме рекурсии
```

Ограничения скорости из боевой конфигурации действуют и здесь; `User-Agent`
содержит пометку `dev` (переменная `CRAWLER_USER_AGENT` в `.env`).
Возврат в изолированный режим — `make up`.

## 3. Структура каталогов

```
deploy/docker/           compose-файлы, Dockerfile'ы, конфиги сервисов
  testbed/               тестовый стенд: сценарии, зоны, фикстуры, mockserver
migrations/              миграции golang-migrate (make migrate-new)
rules/                   правила Laravel (YAML), перезагрузка: make rules-reload
testdata/laravel-samples/  образцы страниц (index.html, headers.json, cookies.json, meta.yaml)
var/incoming/            наблюдаемая папка ingest: <зона>/add/*.txt, <зона>/delete/*.txt
var/content/             хранилище контента (dev)
tmp/                     артефакты hot-reload (air)
```

Тома Docker: `pg_data` (БД), `gomodcache`/`gocache` (кэши Go — быстрые
пересборки), `caddy_data` (TLS-сертификаты testweb), `prom_data`.

## 4. Как добавить сценарий testbed

1. Опишите домен в `deploy/docker/testbed/scenarios.yaml`:
   DNS-записи (`a: testweb|mockserver|blackhole`, `www`, `ns`, `wildcard`,
   `cname`, `cname_loop`, `special: servfail|timeout`) и сайт
   (`site.template`, схема http/https, поведение редиректов).
   В поле `expect` зафиксируйте ожидание — оно используется интеграционными
   тестами и документирует сценарий.
2. Для статических страниц шаблон (`generic`, `parked`, `redirect-chain`,
   `meta-refresh`, `js-redirect`, `spa`) сгенерирует фикстуру автоматически.
   Для динамических сценариев используйте `mockserver` (см. `cmd/mockserver`,
   при необходимости добавьте эндпоинт и пересоберите: `make build`).
3. `make testbed-gen` — перегенерирует зоны (`coredns/zones/`), `zones.conf`
   и `Caddyfile` с фикстурами; сервисы coredns/testweb перезапустятся.
4. Добавьте проверку в `test/integration/env_test.go`, если сценарий
   относится к критериям приёмки.

Образцы Laravel добавляются каталогом в `testdata/laravel-samples/<id>/`
(`index.html`, `headers.json`, `cookies.json`, `meta.yaml`) — домен
`<id>.crawl.test` создаётся автоматически при следующем `make testbed-gen`.

## 5. Правила Laravel

- Файлы правил: `rules/*.yaml` (формат — ТЗ 5.8 основного ТЗ, примеры там же).
- Изменение правил: правите YAML → `make rules-reload` (SIGHUP анализатору,
  без рестарта контейнера; air не следит за `rules/` специально).
- Проверка по корпусу без сети: `make test-rules` — прогон по
  `testdata/laravel-samples`, сверка `is_laravel`/диапазона версий с `meta.yaml`.
- Переанализ сохранённого контента: `make reanalyze` (после реализации analyzer).

## 6. Отладка, метрики, БД, очереди

Отладка (delve, порт 2345 на 127.0.0.1):

```bash
make debug SERVICE=dns-worker
# затем в IDE подключиться к 127.0.0.1:2345
```

Для постоянного запуска сервиса под отладчиком скопируйте
`deploy/docker/compose.override.example.yaml` → `compose.override.yaml`.

Метрики: каждый сервис отдаёт `/metrics` на своём порту (9100–9106), см.
`deploy/docker/prometheus/prometheus.yml`. UI Prometheus — после включения
профиля `monitoring`, http://127.0.0.1:9090/targets.

БД и очереди:

```bash
make psql         # PostgreSQL
make redis-cli    # Redis: XLEN q:dns, XINFO GROUPS q:dns и т.п.
# Adminer:        http://127.0.0.1:8080 (профиль tools)
# Redis Insight:  http://127.0.0.1:5540 (профиль tools)
```

## 7. Диагностика частых проблем

- **`make up` падает на healthcheck** — `make logs SERVICE=<имя>`; первое
  включение air-сборки может занимать до минуты (`start_period` это учитывает).
- **Нехватка памяти** — суммарные лимиты ≤ 8 ГБ; на слабых машинах уменьшите
  `MEM_*` в `.env` (postgres, chromium) — compose пересоздаст сервисы.
- **Права на bind-mount** — `DEV_UID`/`DEV_GID` в `.env` должны совпадать с
  вашим `id -u`/`id -g` (в `var/` пишет ingest как вы).
- **Конфликт портов** — порты на хосте настраиваются в `.env`
  (`PG_PORT`, `REDIS_PORT`, `PROMETHEUS_PORT`, ...), все биндятся на 127.0.0.1.
- **DNS не резолвится** — приложения используют unbound (172.28.0.10);
  проверьте `docker compose exec unbound nslookup <домен> 127.0.0.1`.
  Контейнер `tools` намеренно имеет обычный Docker-DNS (нужен proxy.golang.org);
  новые Go-зависимости качаются через `make mod-download`.
- **Новый модуль не находится при hot reload** — `GOFLAGS=-mod=readonly`:
  после правки `go.mod` выполните `make mod-download`, затем сервис
  пересоберётся сам.
- **Unhealthy migrate** — это одноразовый сервис (`restart: no`); статус
  `exited (0)` — норма. Смотрите `make logs SERVICE=migrate` при ошибках SQL.

## 8. Настройка хоста для нагрузочных тестов

Параметры контейнеров (заданы в compose): `ulimits.nofile=65535`,
`net.ipv4.ip_local_port_range=1024 65535` для воркеров.

Для нагрузочных тестов, приближенных к боевым (один IP, тысячи соединений),
на хосте дополнительно:

```bash
# эфемерные порты и переиспользование TIME_WAIT
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sysctl -w net.ipv4.tcp_tw_reuse=1

# conntrack: увеличить таблицу или исключить трафик краулера
sysctl -w net.netfilter.nf_conntrack_max=1048576

# UDP-буферы для DNS
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
```

Постоянно — через `/etc/sysctl.d/99-crawler.conf`. Настройки уровня ядра
хоста влияют на все контейнеры; в CI не требуются.
