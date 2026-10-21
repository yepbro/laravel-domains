# Domain Crawler

[![CI](https://github.com/yepbro/laravel-domains/actions/workflows/ci.yml/badge.svg)](https://github.com/yepbro/laravel-domains/actions/workflows/ci.yml)
[![Go](https://img.shields.io/github/go-mod/go-version/yepbro/laravel-domains)](https://github.com/yepbro/laravel-domains/blob/master/go.mod)

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

Лендинг (профиль app) после старта — http://127.0.0.1:8081 (порт `WEB_PORT`
в `.env`, публикуется только на loopback). Операторский дашборд конвейера —
http://127.0.0.1:8090 (порт `DASHBOARD_PORT`; вход admin/admin — dev-креды,
см. [`docs/dev-environment.md`](docs/dev-environment.md)).

Живая проверка одного домена с хоста (DNS → HTTP/рендер → анализ Laravel,
read-only): `make check DOMAIN=example.com` — работает и для реальных
доменов, и для testbed (`*.crawl.test`): первичный резолвер — unbound
testbed, при SERVFAIL/REFUSED чекер эскалирует на публичные fallback-резолверы
(`internal/checker/resolve.go`). Форма лендинга
(`POST /api/check-domain`) проверяет реальные домены так же — сервис web
в dev получает публичные `CRAWLER_DNS_FALLBACK_RESOLVERS` (см.
[`docs/dev-environment.md`](docs/dev-environment.md) §6).

## Полноценное локальное тестирование с реальными доменами

По умолчанию dev-окружение герметично: unbound работает в режиме testbed и
на всё вне тестовых зон отвечает SERVFAIL. Для прогона полного цикла
конвейера (ingest → планировщик → DNS → HTTP → анализ) по списку реальных
доменов с настоящим DNS:

1. **Разрешить выход в интернет** — в `.env`:
   ```bash
   ALLOW_INTERNET=1
   ```
   User-Agent краулера должен нести dev-пометку (дефолт `.env` —
   `CRAWLER_USER_AGENT=DomainCrawler/dev …`, не убирайте её).

2. **Включить реальную рекурсию DNS и перезапустить стек**:
   ```bash
   make up-internet
   ```
   Unbound пересоздаётся в режиме internet — рекурсия с корневых серверов
   (ТЗ 5.4), остальные сервисы не пересоздаются.

3. **Подложить список доменов** — зона = имя каталога, создаётся
   автоматически; формат свободный (полное имя в строке, `#`-комментарии,
   строки выгрузок регистраторов — домен извлекается сам), контракт —
   [`docs/incoming-files.md`](docs/incoming-files.md):
   ```bash
   mkdir -p var/incoming/ru/add
   printf 'laravel.ru\nexample.ru\n' > var/incoming/ru/add/my-list.txt
   ```
   Ingest подхватывает файл сам и переносит его в `var/incoming/processed/`
   (ошибочные — в `failed/` с отчётом).

4. **Наблюдать конвейер**:
   ```bash
   make status                        # зоны, очереди, скорость
   make domain NAME=laravel.ru        # итог: субдомены, редиректы, score
   ```
   Дашборд оператора — http://127.0.0.1:8090. Разбор `q:dead` и повтор
   анализа — [`docs/runbooks.md`](docs/runbooks.md) §2 и `make reanalyze`.

5. **Вернуться в изолированный режим**:
   ```bash
   make up
   ```
   В режиме internet тестовые зоны (`*.test`) не резолвятся — прогон
   `make test-integration` и testbed-сценарии требуют возврата в testbed.

Проверка одного домена без постановки в конвейер — `make check DOMAIN=…`
(см. выше); живая проверка через форму лендинга — http://127.0.0.1:8081.

## Документация

Карта документации (та же таблица с пояснениями «когда читать» — в
[`docs/operations.md`](docs/operations.md) §1):

| Документ | Что внутри |
|---|---|
| [`docs/operations.md`](docs/operations.md) | эксплуатация: сценарии запуска (dev/бой), обновление и миграции, логи, метрики/алерты, чек-лист оператора |
| [`docs/dev-environment.md`](docs/dev-environment.md) | dev-окружение: профили compose, testbed, CLI `crawlerctl`, отладка, диагностика |
| [`docs/docker.md`](docs/docker.md) | шпаргалка Docker: запуск/остановка, команды в контейнерах, тома, диагностика |
| [`docs/configuration.md`](docs/configuration.md) | конфигурация: единый YAML + переменные окружения, приоритеты, валидация (пример — `config.example.yaml`) |
| [`docs/incoming-files.md`](docs/incoming-files.md) | контракт входных файлов `incoming/<зона>/add\|delete/*.txt`: валидация, отчёты, дедуп доставки (undo тем же списком) |
| [`docs/services.md`](docs/services.md) | каркас Go-сервисов: жизненный цикл, логирование, метрики и алерты |
| [`docs/migrations.md`](docs/migrations.md) | миграции PostgreSQL (применение при старте сервисов, создание новых) |
| [`docs/schema.md`](docs/schema.md) | схема PostgreSQL (`zones`, `subdomains`, `domains`, `subdomain_results`, `contents`, `laravel_*`, `ingest_files`): партиционирование, fillfactor, индексы, HOT-апдейты, `domain_info` |
| [`docs/db.md`](docs/db.md) | слой БД: пул `pgx` (бюджет 20–50 соединений), потокочный `COPY` пачками, пакетные `UPDATE … FROM unnest(...)`; тюнинг боевого PostgreSQL |
| [`docs/redis.md`](docs/redis.md) | Redis как транспорт очередей: dev/бой, бюджет памяти, `noeviction`, отказ от персистентности |
| [`docs/content.md`](docs/content.md) | хранилище контента: пак-файлы, словарь zstd, дедупликация, компактификация, порог диска |
| [`docs/render.md`](docs/render.md) | рендер: Chromium, изолированные контексты, ожидание готовности, съём DOM/cookies |
| [`docs/analyzer.md`](docs/analyzer.md) | движок правил Laravel: цели (cookie/заголовки/HTML/meta), свидетельства срабатываний, переанализ |
| [`docs/runbooks.md`](docs/runbooks.md) | процедурные инструкции: переобучение словаря zstd, разбор `q:dead`, переанализ |
| [`docs/mutation-testing.md`](docs/mutation-testing.md) | мутационное тестирование go-mutesting: охват и пороги `make mutate`, отчёты, гашение ложных мутантов, ночной CI-прогон |
| [`docs/deploy.md`](docs/deploy.md) | боевой деплой systemd: юниты, бюджет памяти, установка |
| [`docs/disks.md`](docs/disks.md) | диски боевого сервера: тома PostgreSQL и контента, SMART/TBW (ТЗ §10.3) |
| [`docs/os-tuning.md`](docs/os-tuning.md) | тюнинг ОС боевого сервера: sysctl — эфемерные порты, TIME_WAIT, nofile, conntrack, UDP-буферы DNS (ТЗ §10.2) |

Список команд: `make help`.

## CI

При каждом пуше в `master` и в каждом pull request GitHub Actions
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) прогоняет проверки —
те же make-цели через Docker Compose, что и локально, Go на хосте CI не нужен:

сборка образов (`make build`, слои кэшируются в GitHub Actions cache через
`deploy/docker/compose.ci.yaml`) → `make lint` → `make test` → `make test-rules`
→ `make test-integration` (testbed) → `make build-runtime`.

Статус — бейджем в шапке; при падении логи всех сервисов выгружаются в artifact
рана. Обновления GitHub Actions и Go-зависимостей поднимает Dependabot
([`.github/dependabot.yml`](.github/dependabot.yml)).

Мутационное тестирование в основной CI не входит (тяжёлая проверка):
ночью по `master` и вручную его гоняет отдельный workflow
[`.github/workflows/mutation.yml`](.github/workflows/mutation.yml)
(`make mutate` с порогом score, отчёты — в artifact;
[`docs/mutation-testing.md`](docs/mutation-testing.md)).

## Структура

Соответствует приложению А основного ТЗ; пакеты `internal/` наполняются
по мере реализации этапов (ссылки на задачи — в `tasks/tasks.md`,
выполненные — в архиве [`tasks/tasks_done.md`](tasks/tasks_done.md)).

| Каталог | Назначение |
|---|---|
| `cmd/` | Go-сервисы (ingest, scheduler, dns-worker, http-worker, render-worker, analyzer, monitor, web — публичный лендинг, dashboard — операторский дашборд, crawlerctl) и утилиты testbed (mockserver, testbedgen) |
| `internal/` | Общий код: каркас сервисов (`service`, [docs/services.md](docs/services.md)), конфигурация (`config`), доступ к PostgreSQL — пул и массовая запись (`db`), модель домена — статусы, флаги, маски субдоменов (`model`), нормализация имён — punycode/UTS-46, публичные суффиксы (`idn`), метрики Prometheus (`metrics`), миграции при старте (`migrate`), приём файлов — наблюдение и конвейер ingest (`watch`, `ingest`), планировщик — выборка/постановка/lease/перепроверки (`scheduler`), очереди Redis Streams (`queue`), DNS-клиент с лимитами (`dnsclient`) и цикл q:dns (`dnsworker`), HTTP-транспорт с лимитами и редиректами (`httpfetch`) и цикл q:http (`httpworker`), хранилище контента (`content`, [docs/content.md](docs/content.md)), рендер (`render`, [docs/render.md](docs/render.md)) и цикл q:render (`renderworker`), движок правил Laravel и цикл q:analyze (`rules`, `analyzer`, [docs/analyzer.md](docs/analyzer.md)), счётчики зон и алерты (`monitor`), сводка статуса конвейера — выборки и расчёты для CLI и дашборда (`status`), публичный лендинг — i18n, форма проверки, Tailwind-статика (`web`), операторский дашборд — авторизация и сборщик снимков состояния (`dashboard`, [docs/services.md](docs/services.md)) |
| `deploy/docker/` | Compose-окружение, Dockerfile'ы, конфиги, testbed |
| `deploy/systemd/` | Боевой деплой на целевом сервере: systemd-юниты сервисов, drop-in'ы бюджета пакетов ОС, окружение ([docs/deploy.md](docs/deploy.md)) |
| `deploy/nginx/` | Образец публичной точки входа: reverse-proxy с TLS перед лендингом `web` ([docs/deploy.md](docs/deploy.md) §6) |
| `deploy/sysctl/` | Тюнинг ядра боевого сервера (ТЗ §10.2): `99-crawler.conf` для `/etc/sysctl.d` + инструкция применения; расчёты — [docs/os-tuning.md](docs/os-tuning.md) |
| `deploy/smartd/` | Мониторинг SMART/TBW NVMe (ТЗ §10.3): конфиг smartd, обработчик предупреждений в syslog/journald, udev-правило и drop-in доступа алерта monitor к `/dev/nvme*`; расчёты — [docs/disks.md](docs/disks.md) |
| `migrations/` | Миграции PostgreSQL (golang-migrate): встраиваются в бинарники и применяются сервисами при старте |
| `rules/` | Правила обнаружения Laravel (перезагрузка по SIGHUP) |
| `testdata/laravel-samples/` | Корпус образцов страниц (позитивные/негативные) |
| `test/integration/` | Интеграционные тесты dev-окружения на testbed |
| `var/` | Рабочие данные: `incoming/` (файлы доменов и архив ingest: `processed/`, `failed/`), `content/` |

## Зависимости

Библиотеки — по приложению Б основного ТЗ; все подключены в `go.mod`.

| Библиотека | Назначение | Где используется |
|---|---|---|
| `github.com/jackc/pgx/v5` | PostgreSQL: пул, COPY, пакетные UPDATE | `internal/migrate` (раннер миграций), `internal/db` (пул `pgxpool`, `COPY`, `UPDATE … FROM unnest`; T-007) |
| `github.com/golang-migrate/migrate/v4` | миграции схемы up/down, advisory-блокировка | `internal/migrate`, `migrations/embed.go` |
| `github.com/redis/go-redis/v9` | Redis Streams — очереди задач | `internal/queue` |
| `github.com/miekg/dns` | DNS-клиент | DNS-этап (T-034+) |
| `golang.org/x/net` (`idna`, `publicsuffix`) | нормализация IDN (профиль Lookup, UTS-46), публичные суффиксы | `internal/idn` (T-009): punycode, зоны co.uk, is_idn; DNS/HTTP-этапы |
| `github.com/chromedp/chromedp` | управление Chromium по CDP (рендер) | `internal/render` |
| `github.com/klauspost/compress/zstd` | сжатие контента | `internal/content` |
| `github.com/fsnotify/fsnotify` | наблюдение за папкой `incoming/` | `internal/watch` |
| `github.com/prometheus/client_golang` | метрики Prometheus | `internal/metrics` → `/metrics` каждого сервиса |
| `gopkg.in/yaml.v3` | конфиг и YAML-правила Laravel | `internal/config`, `internal/rules` |
