# Инструкция по работе с Docker

Практическая шпаргалка: запуск и остановка окружения, выполнение команд
внутри контейнеров, работа с томами и диагностика. Про архитектуру
окружения — [`dev-environment.md`](dev-environment.md), полный список
make-команд — `make help`.

## 1. Предпроверка

```bash
docker version --format '{{.Server.Version}}'   # Docker Engine ≥ 24
docker compose version                          # Compose v2 (плагин, не docker-compose)
cp .env.example .env                            # один раз; DEV_UID/DEV_GID = ваш id -u / id -g
```

Всё (сборка, тесты, линт, миграции) выполняется в контейнерах — Go на хосте
не нужен.

## 2. Запуск и остановка

### Стандартные сценарии (make)

```bash
make up          # инфраструктура + testbed + приложения; ждёт все healthchecks
make up-infra    # только postgres + redis + unbound (схему применят сервисы)
make down        # остановить всё; тома и данные сохраняются
make clean       # остановить И удалить все тома (БД, кэши, контент); спросит подтверждение
make clean FORCE=1   # то же без вопроса
make ps          # статус всех контейнеров
make logs SERVICE=analyzer   # логи одного сервиса (tail + follow)
```

- `make up` после правок Go-кода пересобирает только изменённое (hot reload
  работает дальше сам, через air).
- `make up` идемпотентен: повторный запуск ничего не ломает.
- `make clean && make up` поднимает окружение с нуля без ручных шагов.

### Прямые команды docker compose

Makefile — обёртка над compose. Канонический вызов из корня репозитория:

```bash
docker compose --project-directory . -f deploy/docker/compose.yaml --env-file .env <команда>
```

Удобно сделать алиас (добавьте в ~/.bashrc):

```bash
alias dc='docker compose --project-directory . -f deploy/docker/compose.yaml --env-file .env'
```

Дальше в примерах — `dc`.

```bash
dc ps -a                                  # все контейнеры, включая остановленные
dc logs -f --tail=100 unbound             # логи
dc restart coredns testweb                # перезапустить сервисы
dc stop http-worker                       # мягкая остановка одного сервиса
dc up -d --no-deps http-worker            # поднять один сервис без зависимостей
dc --profile monitoring up -d             # включить профиль (аналогично tools, app, testbed)
dc --profile '*' down --remove-orphans    # остановить всё под всеми профилями
dc --profile '*' down -v                  # + удалить тома (= make clean FORCE=1)
```

Важно: **не** запускайте `docker compose` из каталога `deploy/docker` без
`--project-directory .` — относительные пути (тома, сборка) разрешаются от
корня проекта, и без флага конфигурация не соберётся.

### Перезапуск отдельного сервиса после правок

Go-сервисы пересобираются сами (air следит за файлами). Остальные сервисы
перечитывают конфиг только при пересоздании:

```bash
dc restart unbound                 # конфиг unbound изменился
make testbed-gen                   # пересоздаёт зоны/фикстуры и перезапускает coredns/testweb/unbound
dc up -d --force-recreate testweb  # если правили compose-параметры сервиса
```

## 3. Команды внутри контейнеров

Два способа:

- `exec` — команда **в уже запущенном** контейнере (сервис должен работать);
- `run --rm` — **одноразовый контейнер** поверх образа сервиса (зависимости
  не поднимает; полезно для утилит).

```bash
# интерактивная оболочка
make shell                       # tools-контейнер (Go-тулчейн, есть интернет для модулей)
make shell SERVICE=analyzer      # оболочка внутри запущенного сервиса приложения

# то же напрямую
dc exec analyzer sh              # запущенный сервис
dc run --rm tools sh             # одноразовый tools

# разовые команды
dc exec postgres psql -U admin -d crawler -c '\dt'     # SQL одним вызовом
dc exec redis redis-cli XLEN q:dns                     # длина стрима
dc exec unbound nslookup same-site.crawl.test 127.0.0.1   # проверить резолвер
dc exec testweb wget -qO- http://127.0.0.1:2019/config/   # admin-API Caddy
dc run --rm tools go test ./internal/... -count=1       # тесты без make
```

Через make:

```bash
make psql                # интерактивная консоль PostgreSQL
make redis-cli           # интерактивная консоль Redis
make test                # модульные тесты (в tools)
make lint                # golangci-lint
make migrate-up          # применить миграции вручную (обычно не нужно: сервисы сами)
make migrate-new NAME=my_migration
make debug SERVICE=dns-worker   # сервис под delve, отладчик на 127.0.0.1:2345
```

Пользователь в контейнерах приложений — ваш UID/GID из `.env` (файлы,
создаваемые в bind-mount, принадлежат вам). HOME внутри — `/tmp`.

## 4. Тестовый стенд: как смотреть сценарии руками

Статические сайты testweb с хоста — через `curl --resolve` (порты
`127.0.0.1:8088`/`8443` из `.env`):

```bash
curl --resolve same-site.crawl.test:8088:127.0.0.1 http://same-site.crawl.test:8088/
curl -k --resolve spa-shell.crawl.test:8443:127.0.0.1 https://spa-shell.crawl.test:8443/
```

DNS-запросы к тестовым зонам — через запущенный unbound (из любого контейнера
сети; на хосте порт 53 не публикуется):

```bash
dc exec unbound nslookup parked-1.crawl.test 127.0.0.1
dc exec unbound nslookup example.com 127.0.0.1    # → ошибка: наружу в testbed не резолвится
```

«Злые» эндпоинты mockserver (внутри сети — напрямую по IP):

```bash
dc run --rm --no-deps tools wget -qO- http://172.28.100.13:8080/healthz
```

Полный список сценариев: `deploy/docker/testbed/scenarios.yaml`.

## 5. Томa и данные

| Том | Что внутри | Когда очищается |
|---|---|---|
| `crawler_pg_data` | база PostgreSQL | `make clean` |
| `crawler_gomodcache`, `crawler_gocache` | кэши Go (быстрая пересборка) | `make clean` |
| `crawler_caddy_data`, `crawler_caddy_config` | TLS-сертификаты testweb | `make clean` |
| `crawler_prom_data` | метрики Prometheus (7 дней) | `make clean` |

Данные вне томов: `var/incoming` (файлы доменов), `var/content` (контент) —
bind-mount из репозитория, в git не попадают.

```bash
docker volume ls | grep crawler
docker volume rm crawler_pg_data     # точечный сброс только БД
```

## 6. Сборка образов

```bash
make build            # dev-образы (crawler/go-dev, unbound, mockserver) — кэшируется
make build-runtime    # проверочные боевые образы: crawler/runtime:check, crawler/render:check
dc build tools        # собрать один сервис
dc build --no-cache unbound   # пересобрать без кэша (если «что-то странное»)
```

Обычный `make up` тоже собирает образы при необходимости (`--build`).
После смены версий в `.env` (`GO_VERSION`, `ALPINE_VERSION`, …) образы
пересоберутся автоматически при следующем `make up`/`make build`.

## 7. Диагностика Docker

```bash
dc ps -a                          # unhealthy/exited — первый взгляд
dc logs --tail=200 <сервис>       # почему падает
dc top analyzer                   # процессы внутри контейнера
docker stats --noheader           # потребление CPU/RAM всеми контейнерами
docker inspect crawler-analyzer-1 --format '{{.HostConfig.Memory}}'  # лимит
dc events                         # поток событий (restart, oom и т.п.)
```

Типовые проблемы:

- **`pool overlaps`** при создании сети — подсеть из compose занята другим
  проектом. Проверьте `docker network ls` + `docker network inspect <имя>`;
  при необходимости замените подсеть `172.28.100.0/24` в `deploy/docker/compose.yaml`,
  `deploy/docker/unbound/*.conf`, `deploy/docker/testbed/scenarios.yaml`,
  `test/integration/env_test.go`, `deploy/docker/config.dev.yaml` и выполните
  `make testbed-gen`.
- **Контейнер приложения в restart-цикле** — `dc logs <сервис>`: чаще всего
  это air с некомпилирующимся кодом; ошибка видна в логе, после правки всё
  пересоберётся само.
- **Permission denied в `var/`** — каталог создан root (например, Docker'ом
  при авто-создании bind-mount). Лечится: `sudo chown -R $(id -u):$(id -g) var`.
- **Порт занят на хосте** — поменяйте `PG_PORT`/`REDIS_PORT`/… в `.env`.
- **Место кончилось** — `docker system df`; чистить кэши сборок:
  `docker builder prune` (безопасно, пересборка дольше), тома проекта — только
  через `make clean`.
- **«Обвисшие» одноразовые контейнеры tools** — `dc rm -f tools-run-…` или
  `docker rm $(docker ps -aq --filter label=com.docker.compose.project=crawler --filter status=exited)`.

## 8. Сеть, адреса и порты

Сеть `crawler_crawler_net` (`172.28.100.0/24`), статические адреса:

| Адрес | Сервис |
|---|---|
| `172.28.100.2` | postgres |
| `172.28.100.3` | redis |
| `172.28.100.10` | unbound (DNS всех сервисов приложения) |
| `172.28.100.11` | coredns |
| `172.28.100.12` | testweb (все домены-фикстуры) |
| `172.28.100.13` | mockserver |
| `172.28.100.14` | chromium (CDP `ws://…:9222`) |
| `172.28.100.250` | «чёрная дыра» (сценарий закрытых портов) |

Порты на хосте (только `127.0.0.1`): `5432` PostgreSQL, `6379` Redis,
`9090` Prometheus, `8080` Adminer, `5540` Redis Insight, `8088`/`8443`
testweb HTTP/HTTPS. Метрики сервисов: `9100`–`9106` (только внутри сети).

Имена сервисов (`postgres`, `redis`, …) резолвятся Docker-DNS из любого
контейнера сети; у сервисов приложения весь остальной DNS идёт через unbound
(изоляция от интернета), а у `tools` — обычный (нужен proxy.golang.org:
новые зависимости качаются `make mod-download`).

## 9. Шпаргалка по сценариям

```bash
# повседневная работа
make up && make logs SERVICE=analyzer

# «упало, поднять всё заново с чистой БД»
make db-reset

# посмотреть состояние конкретного домена в тестовой зоне
make domain NAME=laravel-11-vite.crawl.test

# прогнать всё, что гоняет CI
make lint && make test && make test-rules && make test-integration && make build-runtime

# закончить день
make down
```
