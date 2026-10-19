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

### Что проверяют healthchecks (T-110)

- Инфраструктура — собственные проверки образов: `pg_isready` (postgres),
  `redis-cli ping` (redis), резолв тестового имени (unbound).
- Сервисы приложения (профиль app) — **liveness** `/livez` на порту метрик:
  контейнер здоров, пока процесс жив и отвечает HTTP. Readiness `/healthz`
  (пинг пула PostgreSQL, 503 при сбое) в healthcheck сознательно не
  используется: задачи воркеры тянут из Redis-очередей сами, а при
  недоступной БД стена unhealthy-воркеров лишь дублирует статус самого
  postgres. Проверить готовность вручную (порты метрик наружу не
  публикуются — изнутри контейнера сервиса или через tools):
  `dc exec scheduler wget -qO- http://127.0.0.1:9102/healthz`
  (порты — таблица «Метрики» в [`services.md`](services.md)).
- `tools` — healthcheck отключён: контейнер живёт как `sleep infinity` без
  HTTP-сервера, проверка была вечно красной (шум в `dc ps`, подвисший
  `up --wait` с профилем tools).
- `render` (профиль runtime) — healthcheck-а нет: образ headless-shell без
  HTTP-клиента (wget/curl); живость процесса держит `restart: unless-stopped`
  по коду выхода. Пробы доступны из сети crawler_net с хоста:
  `wget -qO- http://172.28.100.15:9105/healthz`.

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

Домены сценариев (`tarpit.crawl.test`, `huge-body.crawl.test`, `zip-bomb.crawl.test`,
`infinite-redirect`/`cycle-redirect`, `truncated`, `garbage-content`,
`bad-charset-*`, `cookies`, `slow-headers`, `tls-*`) резолвятся в mockserver и
обслуживаются как виртуальные хосты: запрос `/` по Host сценария получает его
«злой» ответ (карта маршрутов — `cmd/mockserver`, соответствует `mock_path` из
`scenarios.yaml`). TLS-сценарии получают по SNI сертификат со своим дефектом
(самоподписанный / просроченный / чужой SAN). Правки исходника mockserver
подхватываются `make test-integration` (образ пересобирается).

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

### Боевой render-контейнер (профиль runtime)

`make build-runtime` проверяет только сборку образов. Полный боевой
контейнер рендера поднимается профилем `runtime` — сервис `render`
(`crawler/render:local`): render-worker + собственный Chromium в одном
контейнере (`Dockerfile.render`; в образе `USER nobody`, в compose
запуск переопределён на `user: ${DEV_UID:-1000}:${DEV_GID:-1000}` —
воркер пишет пак-контент в host-bind `./var/content`, принадлежащий
`DEV_UID`, без ручного `chown`; непривилегированный не-root запуск
сохраняется — ТЗ §9/§10.1), с фиксированными ресурсными требованиями
ТЗ §3.2/§9/§10.1 (T-065):

```bash
dc --profile runtime up -d render   # поднимет и зависимости (postgres, redis)
docker inspect crawler-render-1 --format '{{.HostConfig.Memory}}'   # 2147483648 ≈ 2 ГБ (ТЗ 3.2)
dc --profile runtime stop render    # остановить
```

- `mem_limit: ${MEM_RENDER:-2048m}` — бюджет Chromium ≈ 2 ГБ (ТЗ 3.2);
  `mem_limit` — docker-аналог cgroup `MemoryMax` из ТЗ 10.1: OOM-killer
  не затрагивает PostgreSQL и остальные сервисы.
- `shm_size: 1gb`, `init: true` — как dev-сервису chromium (ТЗ 4.3).
- `cap_drop: ALL`, `no-new-privileges`, непривилегированный пользователь
  (в образе `nobody`, в compose — `DEV_UID`/`DEV_GID` под запись в
  host-bind `var/content`) — рендер недоверенных страниц без root (ТЗ §9).
- перезапуск Chromium каждые N страниц и по порогу RSS — изнутри воркера
  (`render.restart_every_pages`, `render.restart_memory_bytes`; конфиг —
  `deploy/docker/config.render.yaml`, `remote_url` пуст — боевой режим),
  жёсткий потолок — `mem_limit`; детали — [`render.md`](render.md),
  раздел «Перезапуск браузера и порог памяти».
- systemd-юниты с `MemoryMax`/`User=` (постановка вне Docker) —
  [`deploy/systemd/`](../deploy/systemd/), инструкция
  [`deploy.md`](deploy.md) (этап 9, ТЗ 10.1 — нативный боевой деплой:
  пакеты ОС, статические бинарники; docker-runtime остаётся вариантом
  для Docker-хостов).

Сервис потребляет `q:render` полным рабочим циклом (T-067: рендер в
изолированных контекстах, замена сырого HTML на итоговый DOM в
хранилище, публикация `q:analyze`) — конвейер описан в
[`render.md`](render.md) «Цикл очередей».

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
| `172.28.100.15` | render — боевой render-контейнер (профиль `runtime`) |
| `172.28.100.250` | «чёрная дыра» (сценарий закрытых портов) |

Порты на хосте (только `127.0.0.1`): `5432` PostgreSQL, `6379` Redis,
`9090` Prometheus, `8080` Adminer, `5540` Redis Insight, `8088`/`8443`
testweb HTTP/HTTPS, `8081` лендинг web (`WEB_PORT` в `.env`), `8090`
операторский дашборд dashboard (`DASHBOARD_PORT`; вход по паролю —
dev-креды в config.dev.yaml, см. [`dev-environment.md`](dev-environment.md)).
Метрики сервисов: `9100`–`9108`
(только внутри сети).

Имена сервисов (`postgres`, `redis`, …) резолвятся Docker-DNS из любого
контейнера сети; у сервисов приложения весь остальной DNS идёт через unbound
(изоляция от интернета), а у `tools` — обычный (нужен proxy.golang.org:
новые зависимости качаются `make mod-download`).

## 9. Unbound: режимы, память и fallback-резолверы

Конфиги — `deploy/docker/unbound/`; `Dockerfile.unbound` прогоняет оба через
`unbound-checkconf` при сборке образа. Режим задаётся переменной `UNBOUND_MODE`
и переключается только `make up` / `make up-internet` (см.
[`dev-environment.md`](dev-environment.md)).

| Режим | Конфиг | Поведение |
|---|---|---|
| `testbed` (по умолчанию) | `unbound.testbed.conf` | весь трафик форвардится в coredns (172.28.100.11): тестовые зоны отвечают авторитетно, остальное — REFUSED, наружу ничего не уходит (интеграционные тесты DNS-этапа) |
| `internet` (`make up-internet`) | `unbound.internet.conf` | полноценная рекурсия с корневых серверов, prefetch, кэш в бюджете ТЗ §3.2 |

Параметры режима `internet` (ТЗ §5.4, §3.2):

- **Рекурсия** — без `forward-zone`: unbound резолвит сам от корневых серверов
  (встроенные root hints). **Prefetch** (`prefetch: yes`): запись, запрошенная
  при остатке TTL < 10 %, обновляется до истечения срока — популярные записи
  не выпадают из кэша (формулировка ТЗ §5.4).
- **Кэш** ≈ 542 МБ — внутри окна ТЗ §5.4 (512–768 МБ):
  `msg-cache-size 128m` + `rrset-cache-size 384m` (≈3× msg — рекомендация
  NLnet Labs 2–4×) + `neg-cache-size 16m` (у краулера массово NXDOMAIN) +
  `key-cache-size 4m` (валидатор DNSSEC отключён, чистый `iterator`) +
  `infra-cache-numhosts 100000` (~10 МБ статистики RTT/таймаутов NS).
- **Память контейнера** — `mem_limit: ${MEM_UNBOUND:-700m}`: бюджет ТЗ §3.2
  «Unbound (кэш) ≈ 0,7 ГБ»; `mem_limit` — docker-аналог `MemoryMax` из §10.1.
  Кэш-таблицы растут лениво по заполнении (стартовый RSS ≈ 20 МБ), но лимит
  обязан вмещать полностью заполненный кэш: ~542 МБ + запас ~160 МБ на
  процесс, буферы потоков (`num-threads: 2` под нагрузку
  `dns.concurrency`/`dns.max_qps`) и фрагментацию аллокатора. Режим `testbed`
  занимает ~150 МБ и вписывается в любой лимит — при нехватке памяти на
  dev-машине в testbed можно вернуть `MEM_UNBOUND=256m` в `.env`.

### Почему публичные fallback-резолверы не в конфиге unbound

ТЗ §5.4: «Публичные резолверы — только как fallback (список в конфиге)».
Семантика unbound не позволяет реализовать это внутри резолвера:

- `forward-zone` с `name: "."` перехватывает **все** запросы — форвардеры
  становятся первичными для всего трафика;
- `forward-first: yes` меняет лишь направление отказа: при SERVFAIL от
  форвардера unbound повторяет запрос собственной рекурсией. Обратного
  направления «рекурсия → форвардер при SERVFAIL» в unbound нет — такой
  fallback реализуют на клиенте запросов.

Пропускать crawl (сотни миллионов доменов) через `1.1.1.1`/`9.9.9.9` первым
маршрутом противоречит букве ТЗ («только как fallback»), рискует упереться в
rate-limit публичных сервисов и обесценивает локальный кэш с prefetch.

Поэтому список живёт в конфиге приложения и потребляется **клиентом
dns-worker**: при SERVFAIL/таймауте локального unbound (`dns.resolvers`)
запрос повторяется напрямую в `dns.fallback_resolvers` (`internal/dnsclient`,
T-034). Поле и env-переопределение `CRAWLER_DNS_FALLBACK_RESOLVERS` есть с
T-006; дефолты по ТЗ §5.10 — `1.1.1.1:53`, `9.9.9.9:53` (общедоступные
anycast-резолверы двух независимых операторов — Cloudflare и Quad9). В
dev/testbed список пуст (`config.dev.yaml`) — изоляция от интернета
сохраняется; при `make up-internet` включается при необходимости той же
переменной. Подробности — [`configuration.md`](configuration.md), секция `dns`.

## 10. Шпаргалка по сценариям

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
