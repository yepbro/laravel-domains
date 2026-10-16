# Domain Crawler

Сервис массовой проверки доменов и поиска сайтов на Laravel.
Технические задания: [`tasks/TZ_domain_crawler.md`](tasks/TZ_domain_crawler.md),
[`tasks/TZ_docker_dev_environment.md`](tasks/TZ_docker_dev_environment.md).

## Быстрый старт (dev)

Нужны Linux, Docker Engine ≥ 24 и Docker Compose v2. Go на хосте не требуется.

```bash
cp .env.example .env
make up          # инфраструктура + testbed + приложения (первый запуск 5–15 мин)
make ps          # дождаться healthy
make seed        # тестовые списки доменов → var/incoming/
make test        # модульные тесты
make test-integration  # проверка окружения на testbed
```

Полная инструкция: [`docs/dev-environment.md`](docs/dev-environment.md).
Работа с Docker (запуск/остановка, команды в контейнерах, тома, диагностика):
[`docs/docker.md`](docs/docker.md).
Миграции PostgreSQL (применение при старте сервисов, создание новых):
[`docs/migrations.md`](docs/migrations.md).
Схема PostgreSQL (`zones`, `subdomains`, `domains`, `subdomain_results`,
`contents`, `laravel_*`, `ingest_files`) и её типизация в Go
(`internal/model`): партиционирование, fillfactor, индексы, HOT-апдейты,
статусы домена, флаги, `dns_mask` — [`docs/schema.md`](docs/schema.md).
Слой БД (пул подключений `pgx` с бюджетом 20–50 соединений, потокочный
`COPY` пачками, пакетные `UPDATE … FROM unnest(...)`):
[`docs/db.md`](docs/db.md).
Конфигурация сервисов (единый YAML + переменные окружения, приоритеты,
валидация; пример — `config.example.yaml`): [`docs/configuration.md`](docs/configuration.md).
Список команд: `make help`.

## Структура

Соответствует приложению А основного ТЗ; пакеты `internal/` наполняются
по мере реализации этапов (ссылки на задачи — в `tasks/tasks.md`).

| Каталог | Назначение |
|---|---|
| `cmd/` | Go-сервисы (ingest, scheduler, dns-worker, http-worker, render-worker, analyzer, monitor, crawlerctl) и утилиты testbed (mockserver, testbedgen) |
| `internal/` | Общий код: каркас сервисов (`service`), конфигурация (`config`), доступ к PostgreSQL — пул и массовая запись (`db`), модель домена — статусы, флаги, маски субдоменов (`model`), метрики Prometheus (`metrics`), миграции при старте (`migrate`), движок правил Laravel (`rules`), каркасы `queue` (Redis Streams), `content` (zstd), `render` (Chromium), `watch` (fsnotify); пакеты idn, dnsclient, httpfetch — в своих задачах |
| `deploy/docker/` | Compose-окружение, Dockerfile'ы, конфиги, testbed (systemd-юниты и тюнинг ОС — этап 9 ТЗ) |
| `migrations/` | Миграции PostgreSQL (golang-migrate): встраиваются в бинарники и применяются сервисами при старте |
| `rules/` | Правила обнаружения Laravel (перезагрузка по SIGHUP) |
| `testdata/laravel-samples/` | Корпус образцов страниц (позитивные/негативные) |
| `test/integration/` | Интеграционные тесты dev-окружения на testbed |
| `var/` | Рабочие данные: `incoming/` (файлы доменов), `content/` |

## Зависимости

Библиотеки — по приложению Б основного ТЗ; все подключены в `go.mod`.

| Библиотека | Назначение | Где используется |
|---|---|---|
| `github.com/jackc/pgx/v5` | PostgreSQL: пул, COPY, пакетные UPDATE | `internal/migrate` (раннер миграций), `internal/db` (пул `pgxpool`, `COPY`, `UPDATE … FROM unnest`; T-007) |
| `github.com/golang-migrate/migrate/v4` | миграции схемы up/down, advisory-блокировка | `internal/migrate`, `migrations/embed.go` |
| `github.com/redis/go-redis/v9` | Redis Streams — очереди задач | `internal/queue` |
| `github.com/miekg/dns` | DNS-клиент | DNS-этап (T-034+) |
| `golang.org/x/net` (`idna`, `publicsuffix`) | нормализация IDN, публичные суффиксы | интеграция окружения; пакет `internal/idn` — T-009 |
| `github.com/chromedp/chromedp` | управление Chromium по CDP (рендер) | `internal/render` |
| `github.com/klauspost/compress/zstd` | сжатие контента | `internal/content` |
| `github.com/fsnotify/fsnotify` | наблюдение за папкой `incoming/` | `internal/watch` |
| `github.com/prometheus/client_golang` | метрики Prometheus | `internal/metrics` → `/metrics` каждого сервиса |
| `gopkg.in/yaml.v3` | конфиг и YAML-правила Laravel | `internal/config`, `internal/rules` |
