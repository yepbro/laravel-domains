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
Список команд: `make help`.

## Структура

| Каталог | Назначение |
|---|---|
| `cmd/` | Go-сервисы (ingest, scheduler, dns-worker, http-worker, render-worker, analyzer, monitor, crawlerctl) и утилиты testbed (mockserver, testbedgen) |
| `internal/` | Общий код (каркас сервисов, движок правил Laravel) |
| `deploy/docker/` | Compose-окружение, Dockerfile'ы, конфиги, testbed |
| `migrations/` | Миграции PostgreSQL (golang-migrate) |
| `rules/` | Правила обнаружения Laravel (перезагрузка по SIGHUP) |
| `testdata/laravel-samples/` | Корпус образцов страниц (позитивные/негативные) |
| `var/` | Рабочие данные: `incoming/` (файлы доменов), `content/` |
