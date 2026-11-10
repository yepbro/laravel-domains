# Dev-окружение Domain Crawler

Воспроизводимое окружение разработки на Docker Compose: PostgreSQL, Redis,
локальный DNS-резолвер (unbound), изолированный тестовый стенд «мини-интернет»
(CoreDNS + Caddy + mockserver), headless-Chromium, мониторинг и Go-сервисы
с горячей перезагрузкой кода.

Соответствует `tasks/TZ_docker_dev_environment.md`.

Практическая работа с Docker (запуск/остановка, команды внутри контейнеров,
тома, диагностика) вынесена в отдельную шпаргалку: [`docker.md`](docker.md).
Эксплуатация в целом (запуск, обновление, метрики, процедуры) —
[`operations.md`](operations.md); контракт входных файлов —
[`incoming-files.md`](incoming-files.md).

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
make reset-internet          # полный сброс (тома, var/content, отчёты
                             # ingest) + старт с реальным DNS
```

## 2. Профили и типовые сценарии

Профили compose (`deploy/docker/compose.yaml`):

| Профиль | Что включает |
|---|---|
| (по умолчанию) | `postgres`, `redis`, `unbound` |
| `testbed` | `coredns`, `testweb` (Caddy), `mockserver` |
| `app` | Go-сервисы (`ingest` … `monitor`, `render-worker`) + публичный лендинг `web` (http://127.0.0.1:8081) + операторский дашборд `dashboard` (http://127.0.0.1:8090, креды dev — admin/admin) + `chromium` |
| `render-engines` | `lightpanda`, `moli` — альтернативные движки рендера (этап 35): по требованию, когда render-worker переключают с chromium (см. «Движки рендера» ниже) |
| `monitoring` | `prometheus` (UI на http://127.0.0.1:9090) |
| `tools` | `adminer` (http://127.0.0.1:8080), `redis-insight` (http://127.0.0.1:5540), контейнер `tools` |

Миграции PostgreSQL отдельного сервиса не имеют: каждый Go-сервис применяет
их сам при старте (см. [`migrations.md`](migrations.md)).

Типовые сценарии:

```bash
make up-infra        # только БД/Redis/DNS (работа над схемой или миграциями)
make up              # инфраструктура + testbed + приложения (стандартный день)
make test            # модульные тесты (в контейнере, Go на хосте не нужен)
make lint            # golangci-lint
make test-race       # детектор гонок -race для internal/monitor (CGO включается сам)
make test-rules      # правила Laravel по корпусу testdata/laravel-samples
make test-integration# интеграционные тесты окружения на testbed
make mutate          # мутационное тестирование (тяжёлое; mutation-testing.md)
```

Правило `-race` (T-190): пакеты с горутинными циклами (сейчас это
`internal/monitor` — slowquery/load/zonestats) держите чистыми под
детектором гонок — локально `make test-race`, тот же шаг есть в CI.
Штатный `make test` идёт без `-race` (CGO_ENABLED=0), поэтому гонки в
тестах ловит только эта цель; фейки, читаемые из горутины цикла,
синхронизируйте мьютексом (стиль `zonestatsFakePool`,
`fakeSlowSource` в slowqueries_test.go).

Мониторинг и инструменты поднимаются по требованию:

```bash
docker compose --project-directory . -f deploy/docker/compose.yaml --env-file .env \
  --profile monitoring --profile tools up -d
```

Стили лендинга (Tailwind v4) собираются в бинарь через go:embed:
`internal/web/static/app.css` — генерат и в git не хранится
(`.gitignore`). Для dev-просмотра с настоящими стилями соберите бандл
после правок шаблона/исходника — `make web-css` (одноразовый контейнер,
node не нужен; боевая docker-сборка делает это сама — стадия `css`
в Dockerfile.go) — и перезапустите сервис web: air следит только за
`.go`, на CSS сам не пересоберётся (`docker restart crawler-web-1`).

### Дашборд: dev-креды и перегенерация хэша

Операторский дашборд (compose-сервис `dashboard` профиля app, ТЗД)
публикуется на http://127.0.0.1:8090 (`DASHBOARD_PORT` в `.env`) и
требует входа: dev-креды — **admin / admin** (только dev-окружение;
в бою — `dashboard.username`/`password_hash`, docs/deploy.md §4.5).
Хэш лежит в секции `dashboard` файла `deploy/docker/config.dev.yaml`;
перегенерация при смене dev-пароля (stdin — пароль не остаётся в
истории shell):

```bash
printf 'admin' | docker compose --project-directory . \
    -f deploy/docker/compose.yaml --env-file .env \
    run --rm -T tools go run ./cmd/crawlerctl dashboard-secret
```

Полученную строку `pbkdf2-sha256$…` подставьте в `password_hash` (и
обновите комментарий с паролем); после рестарта сервиса
(`docker compose … restart dashboard`) новые креды действуют, все
cookie-сессии инвалидируются (ключ HMAC — производный ключ KDF, ТЗД §7.5).

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

### Движки рендера: lightpanda и moli (этап 35)

Помимо штатного `chromium` (профиль `app`, CDP на `ws://172.28.100.14:9222`)
dev-стек умеет поднимать оба альтернативных движка рендера — Lightpanda
([lightpanda-io/browser](https://github.com/lightpanda-io/browser),
официальный образ) и Moli ([lexmount/moli](https://github.com/lexmount/moli),
свой минимальный образ из релизного tarball — `deploy/docker/Dockerfile.moli`).
Оба говорят по CDP на том же эндпоинте `/json/version`, что и Chromium, и
поднимаются профилем `render-engines` — **по требованию**, не вместе с
`make up`: три браузера одновременно дев-машине не нужны.

Переключение движка render-worker'ом — правка двух ключей конфигурации
(`engine` + `remote_url`), без правки кода (ФТ-16 ТЗ по движкам рендера).
Здесь и ниже `dc` — алиас канонического вызова compose
([`docker.md`](docker.md) §2):

```bash
# 1) поднять контейнер движка (первый запуск собирает/качает образ)
dc --profile render-engines up -d lightpanda   # или: moli

# 2) переключить render-worker: в deploy/docker/config.dev.yaml заменить
#    render.engine и render.remote_url (закомментированные образцы там же):
#      lightpanda → engine: lightpanda + ws://172.28.100.16:9222
#      moli       → engine: moli + ws://172.28.100.17:9222

# 3) перезапустить воркер — air конфиг не перечитывает
dc restart render-worker
```

Порог перезапуска `render.restart_memory_bytes` в профиле уже `0`
(браузер в dev всегда внешний контейнер, RSS чужого процесса недоступен —
валидация требует 0 для moli/lightpanda), поэтому переключение — ровно
два ключа; адрес chromium при возврате ничего донастраивать не требует.

Возврат на chromium — вернуть `remote_url: "ws://172.28.100.14:9222"`
и закомментировать `engine` обратно (chromium — умолчание), перезапустить
воркер; контейнеры движков можно остановить
(`dc --profile render-engines stop lightpanda moli`).

Адреса в `crawler_net`: `lightpanda` — `172.28.100.16`, `moli` —
`172.28.100.17` (план адресов — [`docker.md`](docker.md) §8). Версии
закреплены в `.env` (ФТ-17): `LIGHTPANDA_IMAGE`, пара `MOLI_VERSION` +
`MOLI_SHA256` (контрольная сумма релизного ассета) и база образа
`DEBIAN_VERSION`; смена версии движка — отдельная процедура со
smoke-прогоном, не «поехало с latest». Эксплуатационные флаги (ФТ-18):
у Lightpanda телеметрия обязательно отключена переменной
`LIGHTPANDA_DISABLE_TELEMETRY` (в release-сборках она включена по умолчанию),
`--obey-robots` не передаётся; `--advertise-host 172.28.100.16` воркеру не
требуется (remote-аллокатор движка lightpanda подключается по
`render.remote_url` как есть, T-234) и оставлен как страховка сторонним
CDP-клиентам, которые ходят по рекламируемому `webSocketDebuggerUrl`;
Moli запускается `serve` с биндом на свой статический адрес
`172.28.100.17` (реклама `/json/version` строится от адреса бинда; воркер
с T-234 берёт из неё только путь browser-сессии, адрес подключения — из
`render.remote_url`), без `--layout` (геометрия не нужна) и без `--resource`
(ресурсные семейства блокирует сам этап рендера, T-062). Healthcheck обоих
движков — `GET /json/version` (внутри контейнера lightpanda проба идёт на
`127.0.0.1:9222`, moli — на свой статический адрес; из сети движки удобнее
проверять целиком:
`dc run --rm --no-deps tools wget -qO- http://172.28.100.16:9222/json/version`).

Per-engine живые тесты рендера (T-254): полный CDP-срез каждого движка
против зон `*.crawl.test` — изоляция контекста, съём DOM/cookies/заголовков,
перехват Fetch, SSRF-контур, UA-override; таблица движков, адреса —
`CHROMIUM_WS_URL`/`MOLI_WS_URL`/`LIGHTPANDA_WS_URL` (дефолты — адреса
стека; недоступный движок при адресе по умолчанию — SKIP; в
tools-контейнере `CHROMIUM_WS_URL` задан compose — недоступный chromium
даёт отказ, а не SKIP). Запуск и текущее состояние pinned-версий —
докомментарий `internal/render/engines_live_test.go` и раздел
[«Smoke движка при смене версии (ФТ-17)»](#smoke-движка-при-смене-версии-фт-17-t-254)
ниже.

### Фикстуры рендера и изолированный прогон этапа (T-242/T-243/T-244)

Стенд сравнения движков (ТЗ рендера §6.1) не гоняет домены через весь
конвейер: фикстура — дамп реальных задач q:render (строки subdomain_results
с финальными URL после SPA-вердикта), накатывается в тестовую БД и
публикуется прямо в q:render — рендер-этап тестируется изолированно, без
DNS/HTTP. Полный цикл (`dc` — алиас канонического вызова compose,
[`docker.md`](docker.md) §2; DSN дев-БД — по образцу `APP_PG_DSN` в
compose, пароль `APP_DB_PASSWORD` из `.env`):

```bash
# 1) выгрузка фикстуры (читает БД без записи; страты ok/failed/heavy,
#    smoke-подвыборка; дев-БД мала — квоты честно деградируют, отчёт
#    печатается и пишется в manifest.json)
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures export \
    --out /src/var/fixtures-render --ok 2000 --failed 500 --heavy 500

# 2) тестовая БД (отдельная от прода, ТЗ §6.1 п. 3). Первый шаг — dry-run:
#    без --force команда ничего не пишет, а печатает цель (user@host:port/
#    dbname из DSN, без пароля — сверьте, что это стенд!) и полный план
#    наката: строки по таблицам (вставка/обновление по существующим PK),
#    дубли ключей, что будет выметено из contents
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures import \
    --dir /src/var/fixtures-render

#    Накат — с явным --force (S-P3-5): накат перезаписывает zones/
#    domains/subdomain_results по PK и выметает contents — защита от
#    ошибочного DSN. Миграции дев-стека применяются сервисами при старте
#    (свежий make up) — накат идёт в пустую БД; идемпотентен по PK
#    (повтор не дублирует, а восстанавливает состояние дампа),
#    последовательности zones.id / domain_id_seq выставляются выше
#    максимума, контентные ссылки сбрасываются (contents не выгружался —
#    DOM пишет сам стенд)
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures import \
    --dir /src/var/fixtures-render --force

# 3) публикация задач в q:render (--publish требует --force): all — весь
#    дамп (полный прогон §5.1), smoke — домены smoke-подвыборки (смена
#    pinned-версий, §6.1 п. 6); накат и публикацию можно совместить
#    одним вызовом
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
    go run ./cmd/crawlerctl fixtures import \
    --dir /src/var/fixtures-render --force --publish all
```

Прогон: render-worker стенда читает тот же Redis (`redis.addr`), движок —
по переключателю выше (chromium/lightpanda/moli). Стенд фолбэка (воркер
B — сервис `render-fallback` профиля runtime): его экспозиция
`http://render-fallback:9105/metrics` добавляется строкой в
`dashboard.render_metrics_urls` (образец закомментирован в
`config.dev.yaml`), Prometheus дев-стека скрейпит её job'ом
`render-fallback`. Повторный прогон того же движка (контроль
вариативности, §5.1 п. 2): повторный `fixtures import --force` (сброс
строк к состоянию дампа — прогон воркера менял rendered/ссылки на контент/маркеры финализации;
заодно выметается осиротевший контент прогона) + очистка стримов
(`DEL q:render q:render-fallback q:analyze q:dead` в Redis стенда, при
остановленных воркерах — иначе DEL гоняется с их XADD/XACK) +
`--publish` заново — два прогона сравниваются на одном старте.

### Отчёт сравнения движков (T-244)

Прогон оставляет результаты в строках тестовой БД, но повторный накат
фикстуры сбрасывает их к состоянию дампа (и выметает контент прогона) —
поэтому между прогонами состояние «после прогона» фиксируется снапшотом,
а сравнивает снапшоты отчёт. Цикл стендового сравнения (T-247):

```bash
# 1) снапшот снятого прогона — СРАЗУ по завершении (пустая q:render,
#    фолбэк доработан) и ДО повторного import следующего прогона;
#    --metrics — /metrics render-worker стенда, --engine-rss-max —
#    макс. RSS контейнера движка за прогон (периодический опрос
#    docker stats --no-stream во время прогона;
#    в dev-стеке нет экспортёра памяти движков, без флага порог памяти
#    отчёта деградирует до «нет данных»)
dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" -e CRAWLER_REDIS_ADDR="$REDIS" tools \
    go run ./cmd/crawlerctl fixtures snapshot \
    --dir /src/var/fixtures-render --label chromium-1 --engine chromium \
    --metrics http://172.28.100.25:9105/metrics --engine-rss-max 734003200

# 2) следующий прогон (сброс+публикация из блока выше; движок — по
#    переключателю) → его снапшот: --label moli-1 --engine moli …

# 3) отчёт сравнения против порогов §5.1 п.4 (детекция <1%, успешность,
#    темп, память <⅓ бюджета chromium, 0 crashes) — по файлам снапшотов,
#    без инфраструктуры; --out сохраняет текст в протокол T-247,
#    --fail-exit — код 1 при любом «НЕ ПРОЙДЕН» (для скриптов)
dc run --rm tools \
    go run ./cmd/crawlerctl fixtures report \
    --baseline /src/var/fixtures-render/chromium-1.snapshot.json \
    --candidate /src/var/fixtures-render/moli-1.snapshot.json \
    --out /src/var/fixtures-render/report-moli-1.txt
```

Снапшот снимает: строки фикстуры из БД (успешные рендеры — `rendered` +
контентная ссылка: после наката `content_hash = NULL`, ссылку ставит
только DOM, записанный прогоном; длина/хэш DOM; узлы/скрипты — чтением
DOM из хранилища контента, `--no-dom` пропускает; вердикты анализатора
`laravel_findings` — материал дифа детекции), границы стрима q:analyze
по ID записей (темп, стр/с; стримы чистятся между прогонами — границы
принадлежат одному прогону) и — с `--metrics` — счётчики этапа: причины
финализаций по словарю `taskFinalizeReason` (net::ERR_\*), время
страницы (ср./p95 по гистограмме), browser-класс ошибок. Отчёт
выводит метрики §5.1 п. 3 и явные строки вердикта; живой DOM
недетерминирован — структурные метрики сравниваются нормализованно
(медианы отношений длина/узлы/скрипты, доля идентичных `content_hash`),
побайтовый диф не применяется.

Деградации честные, отчёт не выдумывает значения: отсутствующий источник
(анализатор не доработал хвост, Redis/`--metrics` не сняты, RSS движка
не задан) даёт строку «НЕТ ДАННЫХ» вместо вердикта. Один нюанс
эксплуатации: между прогонами перезапускайте render-worker стенда —
счётчики `/metrics` тогда равны ровно прогону; общий процесс для двух
снапшотов отчёт распознаёт по `process_start_time_seconds` и деградирует
завязанные на счётчики вердикты с предупреждением (успешность по строкам
БД и темп по стримам остаются валидными). Диф детекции сравнивает
вердикты одного набора правил: разошедшиеся `rules_version` между
прогонами — тоже честная деградация порога детекции; отсутствие строки
`laravel_findings` — штатное «признаков нет» (анализатор удаляет её при
нуле срабатываний), а не пробел — пропажа последнего сигнала ловится как
расхождение. Сравнимые строки — записанные прогоном (контент есть) и
проштампованные анализатором (`subdomain_results.rules_version` не NULL:
рендер обнуляет штамп, анализатор ставит после прохода по DOM) — снимайте
снапшот после доработки analyzer, иначе хвост исключается из дифа.

Ещё два требования к стенду: не включайте http-worker в его состав —
цикл retention контента (T-166) отбирает строки по `checked_at`
фикстуры (боевые даты) и может срезать контентные ссылки свежего прогона
до снапшота; `--engine-rss-max` — это максимум RSS контейнера движка,
снятый во время прогона (периодический опрос `docker stats --no-stream`;
мгновенный снимок после прогона занижает пик).

Изоляция стенда. Стенд — отдельный инстанс dev-стека (свои `postgres.dsn`
и `redis.addr` в его конфигурации): против его БД и Redis не должны
работать сервисы живого конвейера — scheduler+monitor (пересчитают
`zones.domains_pending` и переиздадут фикстурные домены через `q:dns` →
HTTP-эпизод перезапишет строки фикстуры), ingest, dns-worker, http-worker
(его цикл retention контента T-166 срежет `headers`/`cookies`/контентные
ссылки «старых» по `checked_at` строк между import и publish). Состав
стенда: postgres, redis, render-worker (тестируемый движок), analyzer
(вердикты нужны для дифа детекции, §5.1 п. 3), позже — воркер фолбэка.
Не публикуйте фикстуру в Redis живого конвейера: задачи уйдут боевому
render-воркеру. Выделенная БД Redis (например, 15 — приём
интеграционных тестов) тут не годится: `fixtures import` берёт адрес из
`redis.addr` и индекс БД не выбирает — нужен отдельный инстанс
(`CRAWLER_REDIS_ADDR` на него). Накат защищён от ошибочного DSN (S-P3-5):
без `--force` команда не пишет ничего — выводит цель (user@host:port/dbname
из DSN, без пароля) и план; `--force` печатает цель перед первой записью —
проверяйте её: `fixtures import` перезаписывает зоны/домены/результаты
по PK и выметает contents — это инструмент тестовой БД, не прода.

### Smoke движка при смене версии (ФТ-17, T-254)

Смена pinned-версии движка рендера — отдельная процедура, не «поехало с
latest» (ФТ-17 ТЗР движков): версии закреплены в `.env` — `LIGHTPANDA_IMAGE`,
пара `MOLI_VERSION` + `MOLI_SHA256` (контрольная сумма релизного ассета),
`CHROMEDP_HEADLESS_IMAGE` (последняя входит и в args `Dockerfile.render` —
профиль runtime пересобирается тем же пином). Процедура — два слоя: сначала
герметичный testbed-срез (интернета не требует), затем реальный smoke по
smoke-подвыборке фикстуры (§6.1 п. 6 ТЗР движков, ~300 доменов). Красный
любой шаг — откат переменной (образ прежней версии остаётся локально,
переподъём возвращает его) и разбор причины.

1. **Пересборка/переподъём профиля движка.** moli: сменить
   `MOLI_VERSION`/`MOLI_SHA256` → `dc --profile render-engines up -d moli`
   (тег образа несёт версию — compose пересоберёт сам); lightpanda: сменить
   `LIGHTPANDA_IMAGE` → тот же up; chromium: сменить `CHROMEDP_HEADLESS_IMAGE`
   → `dc up -d chromium` (+ пересборка render/render-fallback профиля
   runtime, если стенд проверяет и боевой образ). Здоровье — healthcheck
   `/json/version` в `dc ps` зелёный.
2. **Слой 1 — testbed-срез нового движка** (`TestEnginesLiveCDPSlice`,
   `internal/render/engines_live_test.go`; таблица движков: адреса
   `CHROMIUM_WS_URL`/`MOLI_WS_URL`/`LIGHTPANDA_WS_URL`, дефолты — адреса
   стека, недоступный движок — SKIP):

   ```bash
   dc run --use-aliases -e CRAWLER_RENDER_TEST=1 --rm tools \
       go test ./internal/render -run 'TestEnginesLiveCDPSlice/<движок>' -v -count=1
   ```

   Гейт среза печатает ответ `/json/version` — сверить версию в нём с новым
   пином. Красный срез — стоп-сигнал: новый движок не тянет CDP-инварианты
   этапа (инварианты — ТЗ 5.6/ФТ-5: изоляция контекста на задачу, съём
   DOM/cookies/заголовков, перехват Fetch, SSRF-контур, UA-override).
   Прецеденты pinned-версий на момент T-254/T-272 — в докомментарии
   файла: lightpanda 1.0.2 — один browser context на соединение (сессия
   не создаётся вовсе); moli — InvalidParams на `Fetch.enable` по типам
   Media/Font в v1.1.21/1.1.22 (каждая задача падала на включении
   перехвата); в v1.1.23 (пере-тест T-272, 2026-11-10) паттерны
   базового набора приняты, но перехват всё равно сломан: при
   включённом перехвате Fetch синхронный XHR падает на клиенте
   NetworkError, `failRequest` главной навигации оставляет вкладку
   неотзывчивой без ошибки загрузки; Stylesheet и в v1.1.23 не принят
   (CSS-режим `BlockStylesheet` валит весь вызов) — до починки
   апстрима срез по moli обязан быть красным (пере-тест по релизам,
   T-247).
3. **Реальный smoke по smoke-подвыборке.** Стенд — отдельный инстанс
   dev-стека в режиме internet (`ALLOW_INTERNET=1`, `make up-internet` —
   реальные домены требуют резолва наружу) с составом и изоляцией из
   раздела выше. Фикстура — свежий дамп с прода (`fixtures export`, T-242,
   на копии прода) или существующий дамп стенда. Порядок:

   ```bash
   # 0) baseline-chromium по той же smoke-выборке — один раз на дамп
   #    (переключатель движка — раздел «Движки рендера» выше; после
   #    дренирования q:render — снапшот; обновлять при смене дампа/правил)
   # 1) накат и публикация smoke-подвыборки (~300 доменов)
   #    ($APP_PG_DSN — DSN тестовой БД стенда по образцу compose,
   #    $REDIS — адрес Redis стенда; как и в блоке фикстур выше)
   dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" tools \
       go run ./cmd/crawlerctl fixtures import \
       --dir /src/var/fixtures-render --force --publish smoke
   # 2) render-worker стенда — в профиле тестируемого движка
   #    (engine + remote_url в конфиге стенда, dc restart render-worker),
   #    ожидание дренирования q:render (crawlerctl status / дашборд)
   # 3) снапшот прогона кандидата (метрики — /metrics render-worker стенда)
   dc run --rm -e CRAWLER_POSTGRES_DSN="$APP_PG_DSN" -e CRAWLER_REDIS_ADDR="$REDIS" tools \
       go run ./cmd/crawlerctl fixtures snapshot \
       --dir /src/var/fixtures-render --label moli-v1.1.22 --engine moli \
       --metrics http://172.28.100.25:9105/metrics
   # 4) отчёт против baseline-chromium (пороги §5.1 п.4 ТЗР движков)
   dc run --rm tools go run ./cmd/crawlerctl fixtures report \
       --baseline /src/var/fixtures-render/chromium-baseline.snapshot.json \
       --candidate /src/var/fixtures-render/moli-v1.1.22.snapshot.json
   ```

   На 300 доменах пороги §5.1 индикативны (полная процедура — стендовое
   сравнение T-247): критерий smoke — успешность/темп без деградации
   против baseline, 0 crashes ядра движка, память в порядке; между
   baseline и кандидатом перезапускайте render-worker (счётчики = ровно
   прогон) и повторяйте `import --force` + `--publish` (сброс строк и
   стримов — раздел «Фикстуры рендера» выше).
4. **Решение и фиксация.** Зелёные шаги 1–3 — пин обновлён (при смене
   дефолта — коммит `.env.example`), журнал — `docs/operations.md`; красный
   шаг — откат переменной, переподъём, при движковой причине — issue в
   апстрим движка (прецеденты — докомментарий `engines_live_test.go`).

Ручные шаги процедуры (инструмента нет, выполняет оператор): замер RSS
движка во время прогона — периодический `docker stats --no-stream` и
`--engine-rss-max` на снапшоте (экспортёра памяти движков в dev-стеке нет,
без флага порог памяти отчёта честно деградирует в «НЕТ ДАННЫХ»); контроль
дренирования очередей; обновление baseline-chromium при смене дампа или
правил (диф детекции сравнивает вердикты одного `rules_version`). Слой 2 —
именно процедура, не автотест: реальный интернет и живые домены в CI и
штатном `make test` не участвуют никогда.

## 3. Структура каталогов

```
deploy/docker/           compose-файлы, Dockerfile'ы, конфиги сервисов
  testbed/               тестовый стенд: сценарии, зоны, фикстуры, mockserver
migrations/              миграции golang-migrate: встраиваются в бинарники,
                        применяются сервисами при старте (make migrate-new)
rules/                   правила Laravel (YAML), перезагрузка: make rules-reload
testdata/laravel-samples/  образцы страниц (index.html, headers.json, cookies.json, meta.yaml)
var/incoming/            наблюдаемая папка ingest: <зона>/add|delete/*.txt и архивы
                         (.zip/.tar/.tar.gz/.tgz/.gz — docs/incoming-files.md §9)
var/incoming/processed/  архив обработанных файлов (структура зоны/операции сохраняется)
var/incoming/failed/     ошибочные файлы с лог-отчётом *.report рядом
var/content/             хранилище контента (dev)
tmp/                     артефакты hot-reload (air)
```

Тома Docker: `pg_data` (БД), `gomodcache`/`gocache` (кэши Go — быстрые
пересборки), `caddy_data` (TLS-сертификаты testweb), `prom_data`.

### Поток данных ingest (ТЗ 5.1)

Полный контракт входных файлов (формат, кодировка, отклонения, порог
доли ошибок, отчёты) — [`incoming-files.md`](incoming-files.md).

Сервис `ingest` наблюдает `var/incoming/` (fsnotify + периодический обход,
стабильность размера/mtime) и прогоняет каждый готовый файл через
конвейер: разбор пути `<зона>/add|delete/*.txt` или архива (T-133 —
распаковка потоковая, члены проходят те же ступени) → зона (создаётся
автоматически при первом файле) → один проход по файлу (построчный
разбор и sha256 одновременно) → валидация строк (невалидные — в лог
`rejected` с причиной, атрибут `service=rejected` в stderr сервиса) →
загрузка в staging-таблицу → применение (`add` — вставка/возврат из
deleted, `delete` — перевод в deleted) → отчёт в таблицу `ingest_files`
→ перемещение файла в архив:

| Итог файла | Куда | Что остаётся |
|---|---|---|
| применён | `processed/<зона>/<операция>/…` | строка `ingest_files` (sha256, счётчики) |
| дубль — точная повторная доставка (зона + действие + sha256) | `processed/…` | без нового отчёта — повторная обработка не выполняется |
| ошибочен (структура пути, зона, доля отказов > `ingest.max_error_ratio`) | `failed/…` | лог-отчёт `<файл>.report` рядом: причина и счётчики строк |
| сбой инфраструктуры (БД, чтение) | остаётся в `incoming/` | переработается после рестарта сервиса (at-least-once) |

Повторно не применяется только точная повторная доставка — та же зона,
действие и содержимое (T-113); то же содержимое из `delete/` после
`add/` (undo) или в другой зоне обрабатывается заново —
идемпотентность; пустые файлы и файлы из одних комментариев допустимы.
В сервисе также работает фоновая очистка результатов удалённых доменов
(проход раз в минуту пачками до 1000 доменов, T-020).

## 4. Как добавить сценарий testbed

1. Опишите домен в `deploy/docker/testbed/scenarios.yaml`:
   DNS-записи (`a: testweb|mockserver|blackhole`, `www`, `ns`, `wildcard`,
   `cname`, `cname_loop`, `a_many` — N различных A-записей 192.0.2.0/24:
   большой ответ режется в UDP без EDNS0 (TC=1) и требует TCP-fallback,
   `special: servfail|timeout`) и сайт
   (`site.template`, схема http/https, поведение редиректов).
   В поле `expect` зафиксируйте ожидание — оно используется интеграционными
   тестами и документирует сценарий.
2. Для статических страниц шаблон (`generic`, `parked`, `redirect-chain`,
   `meta-refresh`, `js-redirect`, `spa`) сгенерирует фикстуру автоматически.
   Для динамических сценариев используйте `mockserver` (см. `cmd/mockserver`,
   при необходимости добавьте эндпоинт и пересоберите: `make build`):
   сценарий `site: {template: mock, mock_path: …}` обслуживается по своему
   домену как виртуальный хост — «/» по Host сценария маршрутизируется на
   `mock_path` (карта хостов в `cmd/mockserver` правится вместе со
   сценариями), TLS-сценарии получают дефектный сертификат по SNI.
3. `make testbed-gen` — перегенерирует зоны (`coredns/zones/`), `zones.conf`
   и `Caddyfile` с фикстурами; сервисы coredns/testweb перезапустятся.
4. Добавьте проверку в `test/integration/env_test.go`, если сценарий
   относится к критериям приёмки.

Зоны стенда — многоуровневые (`crawl.test`, `co.test`, …): TLD-семантика
каталогов приёма (ТЗТ tld-only-zones §5.1) разрешает рабочие каталоги
`incoming/<tld>/`, поэтому все зоны сценариев перечислены в allowlist
`ingest.allowed_zone_paths` конфига dev-окружения
(`deploy/docker/config.dev.yaml`) — без записи файлы `make seed`
уходят в `failed/`. Новая зона сценариев — добавить её в allowlist
(имя — punycode нижним регистром, как в `zones.conf`); согласованность
страхуется юнит-тестом `TestSeedZonesAllowedInDevConfig`
(`cmd/crawlerctl/seed_test.go`). Продуктовый allowlist пуст.

Образцы Laravel добавляются каталогом в `testdata/laravel-samples/<id>/`
(`index.html`, `headers.json`, `cookies.json`, `meta.yaml`) — домен
`<id>.crawl.test` создаётся автоматически при следующем `make testbed-gen`;
заголовки из `headers.json` (в т.ч. `Set-Cookie` с куками Laravel) testweb
отдаёт вместе со страницей, поэтому анализ по кукам работает на e2e.

Генерат (`coredns/zones/`, `zones.conf`, `Caddyfile`, фикстуры `web/sites/`)
коммитится в репозиторий: CI собирает зоны из коммита. Рассинхрон с
`scenarios.yaml`/образцами ловит юнит-тест `TestGeneratedArtifactsInSync`
(`cmd/testbedgen/main_test.go`) — после правки сценариев или образцов
он падает до выполнения `make testbed-gen`. Состав стенда против
обязательного списка ТЗ 11.2 сверяется там же (`TestScenariosCoverTZ`):

| ТЗ 11.2 | Сценарий | Где реализовано |
|---|---|---|
| NXDOMAIN | `nxdomain-case` | зона без записей (coredns, авторитетный NXDOMAIN) |
| wildcard | `wildcard` | `*.wildcard` A в зоне + SPA-независимая страница (testweb) |
| тарпит | `tarpit` | mockserver `/tarpit`: тело со скоростью 512 Б/с |
| бесконечный редирект | `infinite-redirect` (+`cycle-redirect`) | mockserver `/redirect/infinite`, `/redirect/cycle/a` |
| гигантское тело | `huge-body` | mockserver `/huge`: Content-Length 1 ГиБ |
| SPA-страница | `spa-shell` | testweb: `#root` + `<noscript>` + JS-кука/контент |
| страницы Laravel разных версий | `<id>.crawl.test` для 17 образцов `testdata/laravel-samples` | testweb: статика + `headers.json` (куки `XSRF-TOKEN`/`*_session`/CMS-имена `__Secure-october_session`/`winter_session`, `Content-Type` и пр.), версии 5.8–12, стеки mix/vite/inertia/livewire, stateless-API/404-JSON/CMS-куки + 5 негативов |

Сверх обязательного списка стенд также эмулирует: SERVFAIL и таймаут DNS
(`servfail`, `dns-timeout`), NODATA у делегированного (`delegated-no-address`),
CNAME-цепочку и цикл, TCP-fallback (`tc-fallback`, 60 A-записей), закрытые
порты (`closed-ports`), варианты http/https и алиасы www, медленные заголовки,
zip-бомбу, обрыв соединения, мусорные байты, кривые кодировки, дефектные
TLS-сертификаты (самоподписанный/просроченный/чужой SAN), парковки для
дедупликации контента, общий IP для лимита соединений.

## 5. Правила Laravel

- Файлы правил: `rules/*.yaml` (формат, валидация и `rules_version` —
  [`configuration.md`](configuration.md), раздел `analyzer`; справочник —
  ТЗ 5.8 основного ТЗ). Рабочий набор — `rules/laravel.yaml` (22 правила;
  веса и версионные границы обоснованы в комментариях к каждому правилу).
- Изменение правил: правите YAML → `make rules-reload` (SIGHUP анализатору,
  без рестарта контейнера; air не следит за `rules/` специально).
- Проверка по корпусу без сети: `make test-rules` — прогон по
  `testdata/laravel-samples`, сверка `is_laravel`/диапазона версий и полного
  состава сработавших правил с `meta.yaml` (поле `rules`; `rules: []` — ни
  одно правило не должно сработать). Ту же сверку выполняет юнит-тест
  `internal/rules/laravel_test.go`.
- Формат `meta.yaml` образца: `is_laravel`, `ver_min`/`ver_max` — реальная
  версия образца (вычисленный диапазон обязан её покрывать), `rules` —
  полный состав сработавших правил; точные ожидания движка (T-075):
  `score` — noisy-OR весов правил из `rules`, округление до 3 знаков
  (сверка с допуском ±0.001), `calc_ver_min`/`calc_ver_max` — точные строки
  границ вычисленного диапазона (`""` — границы нет, в БД NULL),
  `conflict` — пустое пересечение диапазонов; плюс `note`/`source`/
  `obtained`. Сквозной корпус с этими ожиданиями гонит юнит-тест
  `internal/rules/corpus_test.go` (полная история «headers+cookies+HTML →
  EvaluateCtx → ScoreRules → IsLaravel(0.6)», негативные образцы,
  score-математика, конфликты диапазонов, лимиты HTML).
- Переанализ сохранённого контента: `make reanalyze` — находит результаты
  с устаревшим `rules_version` и ставит их в `q:analyze` (без повторного
  скачивания; детали — [`analyzer.md`](analyzer.md), раздел «Переанализ»).
  Тот же проход фоновой горутиной делает сервис analyzer: смена правил
  по `make rules-reload` запускает переанализ без рестарта. Пошаговая
  инструкция оператора (правка → проверка → применение → контроль) —
  [`runbooks.md`](runbooks.md) §3.

## 6. CLI crawlerctl

`crawlerctl` — CLI управления краулером (ТЗ 4.1). Каркас команд —
роутинг, флаги, валидация имён (internal/idn) и `--help` для каждой
команды (задача T-011); управление и dev-команды реализованы. Коды
возврата: 0 — успех, 1 — ошибка выполнения (в т.ч. «не найдено»),
2 — ошибка использования (аргументы/флаги).

```
crawlerctl status                        # сводка по зонам/очередям (T-079)
crawlerctl domain <имя> [--json|--content]
                                         # информация о домене (ТЗ 5.11):
                                         # статус, результаты, признаки
                                         # Laravel, контент; имя в любой
                                         # форме
crawlerctl subdomains list|add|disable|recheck
                                          # справочник субдоменов (T-083)
crawlerctl zones list|enable|disable|sync
                                          # управление зонами (T-084),
                                          # наполнение справочника из
                                          # реестра IANA (T-125)
crawlerctl reanalyze [--zone ИМЯ]        # переанализ по новым правилам
                                          # (T-074): постановка в q:analyze
                                          # без повторного скачивания
crawlerctl recheck <домен> [домен …]     # принудительная постановка домена
                                         # на проверку (T-084)
crawlerctl dict train <файл> [--samples ПУТЬ]…
                                         # обучение/переобучение словаря
                                         # сжатия zstd (ТЗ 5.7)
crawlerctl dashboard-secret [ПАРОЛЬ]     # хэш пароля операторского
                                          # дашборда (dashboard.password_hash,
                                          # ТЗД §7.2); пароль — аргумент
                                          # или stdin (аргумент остаётся
                                          # в истории shell)
crawlerctl near [--zone ИМЯ] [--min-score F]
                                          # находки Laravel в серой зоне
                                          # порога — материал калибровки
                                          # правил (T-115); порог по
                                          # умолчанию — analyzer.laravel_min_score
crawlerctl check <домен>                  # живая проверка домена конвейером
                                          # (T-119): DNS → HTTP (+рендер
                                          # SPA) → анализ; read-only —
                                          # ничего не пишет в БД и не
                                          # публикует в очереди (единственное
                                          # чтение — справочник субдоменов),
                                          # результат — только в stdout;
                                          # поставить домен на настоящий
                                          # цикл с записью в БД — recheck
crawlerctl counters list|recount|verify  # инкрементальные счётчики
                                           # конвейера (T-178): значения,
                                           # ручная реконсиляция (--zone),
                                           # сверка с живыми агрегатами
                                           # без записи (код 0/1); разбор
                                           # дрейфа — docs/runbooks.md §7
crawlerctl seed / test-rules             # dev-команды (make seed, make test-rules)
crawlerctl fixtures export --out КАТАЛОГ [--ok N] [--failed N] [--heavy N]
                                          # [--smoke N] [--weeks N] [--seed N]
                                          # выгрузка фикстуры рендера (T-242,
                                          # ТЗ рендера §6.1 п. 1–2): выборка
                                          # subdomain_results — задач q:render —
                                          # стратами ok/failed/heavy
                                          # пропорционально зонам + smoke-домены;
                                          # контент-хранилище не выгружается.
                                          # Дамп (zones/domains/
                                          # subdomain_results/smoke NDJSON +
                                          # manifest) — контракт наката T-243;
                                          # формат v1 — докомментарий
                                          # cmd/crawlerctl/fixtures.go. Читает
                                          # БД по postgres.dsn, записывает
                                          # только каталог --out
  crawlerctl fixtures import --dir КАТАЛОГ [--force] [--publish all|smoke]
                                          # накат дампа фикстуры в тестовую БД
                                          # (T-243, ТЗ рендера §6.1 п. 3–4). Накат —
                                          # запись без отката (перезапись по PK +
                                          # уборка contents): без --force — dry-run
                                          # (цель из DSN без пароля и полный план:
                                          # строки по таблицам вставка/обновление,
                                          # дубли ключей, выметаемый контент —
                                          # без единой записи), --force — накат
                                          # (цель и план печатаются до первой
                                          # записи): идемпотентно по PK из ключей
                                          # дампа (повторный накат восстанавливает
                                          # состояние дампа), zones — мимо
                                          # identity (OVERRIDING SYSTEM VALUE),
                                          # последовательности зон/доменов —
                                          # setval выше максимума, контентные
                                          # ссылки сбрасываются (contents не
                                          # выгружался — DOM пишет стенд).
                                          # --publish (только с --force) —
                                          # публикация задач фикстуры прямо
                                          # в q:render (без DNS/HTTP): all — весь
                                          # дамп, smoke — домены smoke-подвыборки;
                                          # дубли ключей пропускаются с подсчётом.
                                          # Пишет в БД по postgres.dsn и (с
                                          # --publish) в Redis по redis.addr.
                                          # Инструкция прогона стенда —
                                          # §2 «Фикстуры рендера»
  crawlerctl fixtures snapshot --dir КАТАЛОГ --label ИМЯ --engine ИМЯ
                                          # снапшот результатов прогона
                                          # (T-244, ТЗ рендера §6.1 п. 5):
                                          # строки фикстуры из БД (успешные
                                          # рендеры, длина/хэш/узлы/скрипты
                                          # DOM, вердикты анализатора),
                                          # границы q:analyze из Redis
                                          # (темп), с --metrics — счётчики
                                          # этапа (причины финализаций,
                                          # время страницы). Снимается
                                          # после прогона и до повторного
                                          # import; между прогонами
                                          # перезапускайте render-worker
                                          # (счётчики = ровно прогон)
  crawlerctl fixtures report --baseline ФАЙЛ --candidate ФАЙЛ
                                          # отчёт сравнения движков
                                          # (T-244, ТЗ рендера §5.1 п. 3–4)
                                          # по снапшотам: метрики сравнения
                                          # и явные строки вердикта против
                                          # порогов допуска к канарейке;
                                          # --fail-exit — код 1 для скриптов
                                          # при любом «НЕ ПРОЙДЕН»
```

Обучение словаря сжатия контента (ТЗ 5.7): `--samples` — файл или каталог
с `*.html`/`*.htm` (по умолчанию `testdata/laravel-samples`; флаг можно
повторять, образцы читаются до лимита корпуса 64 МиБ). Словарь записывается
атомарно и подключается конфигурацией `content.zstd_dictionary`; при
переобучении прежний словарь архивируется в реестр прежних словарей
(`dict-<id>.zstd` рядом с новым файлом) — записанный им контент остаётся
читаемым. Пример (словарь — в каталоге данных dev-окружения):

```
docker compose run --rm -v "$(pwd)/var/content:/out tools \
    go run ./cmd/crawlerctl dict train /out/dict.zst --samples testdata/laravel-samples
```

Полная процедура (когда переобучать, подключение, реестр прежних
словарей) — [`runbooks.md`](runbooks.md) §1.

Через make: `make status`, `make domain NAME=example.crawl.test`,
`make check DOMAIN=example.com` (живая проверка любого домена с хоста:
первичный резолвер — unbound testbed, при SERVFAIL/REFUSED (всё вне
тестовых зон — так отвечает герметичный unbound) чекер эскалирует на
публичные резервы `FALLBACK ?= 1.1.1.1:53,9.9.9.9:53`; поэтому одна
команда проверяет и `*.crawl.test`, и реальные домены — переключение
`RESOLVER` не нужно),
`make reanalyze`, `make seed`, `make test-rules`. Остальные команды —
`docker compose run --rm tools go run ./cmd/crawlerctl <команда>`
(см. `make shell`).

Форма лендинга (`POST /api/check-domain`, T-123) использует тот же
конвейер и в dev проверяет реальные домены: сервису web compose
прописывает публичные `CRAWLER_DNS_FALLBACK_RESOLVERS`, чекер
эскалирует на них при отказе unbound (семантика — «единичный ручной
вопрос», не массовый конвейер: dns-worker fallback-ов не получает,
герметичность краула не меняется).

`reanalyze` (T-074, ТЗ 5.8): находит `subdomain_results` с устаревшим
`rules_version` (NULL — никогда не анализированные) и пачками ставит их
в `q:analyze` — анализ пойдёт по сохранённому контенту, сайты не
загружаются заново. `--zone` ограничивает зону (имя в любой форме,
punycode/unicode); без флага — все зоны. Версия набора правил читается
из `analyzer.rules_dir` на момент запуска; счётчик постановок печатается
в stdout. Команда работает на стеке dev-окружения (PostgreSQL/Redis).

`status` (T-079, ТЗ 5.9): сводка для оператора — по зонам (счётчики
`domains_total`/`domains_checked` из таблицы `zones`, их обновляет сервис
monitor, T-078, плюс живой агрегат `domains` по `check_state`: pending без
учёта `deleted` — удалённые домены планировщик не выбирает, queued, done),
глубина очередей `q:dns`/`q:http`/`q:render`/`q:analyze`/`q:dead` (backlog групп потребления),
скорость и ETA завершения первого цикла: `(pending + queued) / скорость`.
Скорость доменов — средняя с начала суток по суточному счётчику проверок
`crawler_counters['checks:YYYY-MM-DD']` (T-179: живой подсчёт
`last_checked_at = current_date` был дорог — heap-fetch'ы свежих страниц;
короче 10 минут окно не используется — сразу после
полуночи оценка скачет); HTTP-результаты и рендеры — часовые корзины
темпа `crawler_counters['results:'/'rendered:YYYY-MM-DD"T"HH24']`
(T-203: суммируются предыдущий и текущий календарный час, скользящий
«последний час» из календарных корзин точно не собирается — скорость
делится на фактическую длину покрытого окна 1–2 ч, оценка «в секунду»
несмещённа; прежде живые подзапросы по `subdomain_results.checked_at`
деградировали heap-fetch'ами свежих страниц; собственной отметки
времени у рендера нет — считается доля `rendered` среди результатов
окна). Темп этапов DNS и анализа из БД не
измерим (нет отметок времени; завершение DNS+HTTP и есть «доменов/с») — их
метрики в Prometheus (T-076/T-077). `--json` — машинный вывод («н/д» —
null). Если Redis недоступен, сводка по БД всё равно печатается, команда
завершается кодом 1.

`domain` (T-081, ТЗ 5.11): поиск домена по имени — статус и признаки
Laravel одним вызовом. Имя — в любой форме (unicode/punycode, регистр,
точка на конце, пробелы по краям): командой нормализуется по ТЗ 5.2
(`internal/idn`), в БД хранится punycode. Данные — SQL-функция
`domain_info(text)` (миграция 000006, T-082): у CLI и psql одна точка
правды. Человекочитаемый вывод: зона (id, unicode), статус и состояние
проверки домена, дата последней проверки (`last_checked_at`), DNS-живые
субдомены (расшифровка `dns_mask`), таблица результатов по субдоменам
× вариантам (схема, HTTP-статус, итоговый URL, алиас `≡`/`→`, контент
DOM/HTML, ошибка с классом, время проверки) и признаки Laravel: score
с порогом, диапазон версий, субдомены, версия правил и срабатывания
с фрагментами-свидетельствами; слабые срабатывания (`is_laravel=false`,
score ниже порога) — отдельной секцией. Домены только со слабыми
срабатываниями записи в `laravel_domains` не имеют и функцией
`domain_info` не раскрываются (контракт T-082). `--json` — сырой ответ
функции (pretty-print); `--content` — вместо сводки вывести в stdout
байты контента выбранной записи как есть (приоритет — отрендеренный DOM,
иначе первая запись с контентом в порядке субдомен×вариант; сводка
и пометка о записи — в stderr, stdout перенаправляется в файл).
Читатель хранилища — тот же PackStore со словарём zstd, что у analyzer
(T-071), без записи; каталог `var/content` смонтирован и в контейнер
tools. `--json` и `--content` несовместимы. Неизвестный домен/зона —
понятное сообщение с указанием, что именно не найдено (зона есть, но
домена с меткой нет / зоны нет в справочнике), код 1; для `--json` —
`{"found":false,…}`; некорректное имя — код 2 до обращения к БД.
Примеры: `make domain NAME=laravel-11-vite.crawl.test`,
`docker compose run --rm tools go run ./cmd/crawlerctl domain --json
example.crawl.test`, `... domain --content example.crawl.test > page.html`.

`subdomains` (T-083, ТЗ 5.10, 13.1) — управление справочником проверяемых
субдоменов (таблица `subdomains`; id = бит `dns_mask`, максимум 63 — бит 63
знаковый, `''` — апекс с битом 0). `list` — таблица «бит / метка / акт»;
`add <метка>` — вставка строки свободным битом `max(id)+1` сразу
включённой, метка — одна DNS-метка (unicode → punycode); `disable
<метка|бит>` — `enabled = false` без смены бита и данных (маски стабильны,
эволюция без миграций; аргумент сперва ищется как метка, затем как номер
бита; апекс выключить нельзя — он основа проверки домена, ТЗ 5.4 п. 1).
DNS-воркер перечитывает справочник на каждую пачку — рестарт сервисов не
нужен. `recheck <метка> [--zone ИМЯ]` — запуск проверки нового субдомена
по уже проверенным доменам: done-домены зоны (без `--zone` — всех зон,
`deleted` исключаются) пачками по 5000 переводятся `check_state` в
`pending`, далее домены проходят штатный конвейер (выборка планировщиком →
`q:dns` → `q:http`) и DNS-этап проверяет их уже с новым субдоменом из
справочника; домены в pending/queued будут проверены с ним и так, темп
задаёт планировщик (backpressure `q:dns`). Прерванный дренаж можно
повторить — перевод идемпотентен. Команды работают на стеке dev-окружения
(PostgreSQL; `recheck` прерывается по Ctrl-C с частичным итогом).

`zones` (T-084, ТЗ 4.1, 5.3) — управление зонами проверки. `list` —
таблица «id / зона (punycode) / unicode / акт / всего / проверено»:
счётчики `domains_total`/`domains_checked` обновляет сервис monitor
(T-078). `enable|disable <зона>` — флаг `zones.is_active`: планировщик
(T-027) выбирает домены только активных зон, поэтому `disable`
приостанавливает проверку зоны (домены остаются pending), а `enable`
возвращает их в выборку без каких-либо datafix — состояние доменов
не менялось. Сами домены, их состояния и результаты disable не трогает
(ТЗ 13.1), приём файлов зоны (ingest) работает как прежде. Имя зоны —
в любой форме, как каталог `incoming/<зона>`; повторный enable/disable
идемпотентен («уже включена/выключена»), неизвестная зона — код 1.

`zones sync` (T-125, этап 14) — начальное наполнение и актуализация
справочника из машинно-читаемого среза реестра корневой зоны IANA
(`tlds-alpha-by-domain.txt`, ~1460 делегированных TLD, IDN — уже
в punycode, как `zones.name`). Новые зоны добавляются включёнными
(`is_active = true` — как автосоздание ingest'ом при первом файле,
ТЗ 5.1: пустая зона планировщику безразлична), существующие строки не
меняются вовсе (`is_active` — владение оператора, счётчики — monitor),
повторный запуск идемпотентен (ничего не меняет). Источник — `--url`
(по умолчанию канонический `https://data.iana.org/TLD/tlds-alpha-by-domain.txt`,
stdlib net/http с таймаутом) или заранее скачанный файл `--file`
(офлайн-режим: прод без исходящего интернета). `--deactivate-missing`
дополнительно выключает зоны БД, отсутствующие в срезе (retired TLD), —
имена выключенных печатаются в отчёте; без флага зоны БД не трогаются.
Ограничение dev/testbed: тестовые зоны сценариев (`crawl.test`,
`co.test`…) в срезе IANA нет — `--deactivate-missing` выключит и их
(файлы зоны по-прежнему принимаются, но планировщик не выбирает домены
выключенной зоны). Возврат — по зоне за вызовом: `crawlerctl zones
enable crawl.test` (и остальные зоны сценариев; enable принимает одну
зону) — или не использовать флаг в dev; продуктовый allowlist пуст,
на боевой записи проблемы нет.
Разбор: строки-комментарии `#` пропускаются (первая — версия файла,
выводится в отчёт), каждая строка валидируется `internal/idn`
(мусор — ошибка с номером строки, тихих пропусков нет), дубли в файле
сводятся. Отчёт: версия файла, прочитано/добавлено/уже было. Это
справочник имён TLD, а не источник доменов: домены попадают в систему
только через ingest (`var/incoming`). Пример офлайн-синхронизации:

```
curl -o /tmp/tlds.txt https://data.iana.org/TLD/tlds-alpha-by-domain.txt
docker compose run --rm -v /tmp/tlds.txt:/tlds.txt:ro tools \
    go run ./cmd/crawlerctl zones sync --file /tlds.txt --deactivate-missing
```

`recheck <домен> [домен …]` (T-084, ТЗ 4.1) — принудительная постановка
доменов на проверку («ручные задачи»), не дожидаясь интервала
перепроверки: имена (в любой форме) нормализуются по ТЗ 5.2, зона —
длиннейший суффикс имени из справочника zones (как `domain_info`),
найденные домены переводятся `check_state → pending` (сброс `queued_at`)
одним пакетным UPDATE по ключам PK. Публикацию в `q:dns` делает
планировщик — он владеет backpressure и lease (T-028); домен в pending
подберёт выборка T-027 в круговом порядке. Домен в работе (`queued`)
переводится как ручной возврат lease — проверится заново (дубль работы
ограничен, доставка at-least-once, ТЗ 3.4); уже ожидающий (`pending`)
остаётся в очереди. Удалённый (`deleted`) домен не проверяется — отказ
с сообщением (ТЗ 13.1); не найденные имена перечисляются в отказе
(нет зоны в справочнике / нет метки в зоне), код 1 возвращается при
хоть одном непоставленном домене (поставленные переводятся — частичный
итог виден в выводе). Для выключенной зоны перевод делается с пометкой
«домен ждёт включения зоны». Перевод идемпотентен, повтор команды
безопасен; Ctrl-C прерывает выполнение.

## 7. Отладка, метрики, БД, очереди

Отладка (delve, порт 2345 на 127.0.0.1):

```bash
make debug SERVICE=dns-worker
# затем в IDE подключиться к 127.0.0.1:2345
```

`make debug` запускает одноразовый run-контейнер сервиса с его статическим
адресом в crawler_net (план адресов — [`docker.md`](docker.md) §8):
предварительно остановите штатный контейнер сервиса
(`docker compose … stop <сервис>`), иначе run-контейнер не получит адрес.

Для постоянного запуска сервиса под отладчиком скопируйте
`deploy/docker/compose.override.example.yaml` → `compose.override.yaml`.

Метрики: каждый сервис отдаёт `/metrics` на своём порту (9100–9108, web —
9107, dashboard — 9108), см. `deploy/docker/prometheus/prometheus.yml`. UI
Prometheus — после включения профиля `monitoring`,
http://127.0.0.1:9090/targets.

БД и очереди:

```bash
make psql         # PostgreSQL
make redis-cli    # Redis: XLEN q:dns, XINFO GROUPS q:dns и т.п.
# Adminer:        http://127.0.0.1:8080 (профиль tools)
# Redis Insight:  http://127.0.0.1:5540 (профиль tools)
```

Интеграционные тесты устойчивости очередей (`test/integration/
resilience_test.go`, T-032) работают в выделенной БД Redis **15** (dev-конфиг
разрешает 16 БД; сервисы используют db 0): боевые имена стримов `q:*` в ней
создаются и вычищаются тестами, запущенные сервисы им не мешают.

Сквозной тест конвейера (`test/integration/e2e_test.go`, T-091; ТЗ 11.2,
критерий 11.4 п. 5) прогоняет весь путь «файл в `incoming` → ingest →
планировщик/`q:dns` → DNS-этап → `q:http` → HTTP-этап → `subdomain_results` →
`q:analyze` → анализатор → `laravel_findings`/`laravel_domains`» библиотечными
вызовами компонентов всех этапов (без запуска сервисов-процессов) и сверяет
итоги с ожиданиями образцов `testdata/laravel-samples` (meta.yaml). Устойчивость
к перезапуску PostgreSQL проверяется посреди конвейера эмуляцией: контейнер БД
общий для стека, рестарт инстанса из теста невозможен — вместо него обрываются
все соединения теста (`pg_terminate_backend` по метке `application_name` в DSN
пула): для клиентов это и есть перезапуск (соединения закрыты, данные живы),
пул переподключается, конвейер доделывает эпизоды, итоги сходятся.

### Приёмочные тесты (T-094)

`test/integration/acceptance_test.go` — явная карта приёмки по всем критериям
ТЗ §11.4: один критерий — один кейс `TestAcceptNN_*` (формулировка пункта —
комментарием над кейсом). Кейсы дополняют соседние файлы (e2e/http/resilience/
ingest/schema), а не дублируют их; карта покрытия:

| §11.4 | Критерий (суть) | Приёмочный кейс |
|---|---|---|
| п. 1 | add/delete + IDN-зоны, без дублей unicode/punycode | `TestAccept01_IngestAddDeleteIDN` |
| п. 2 | субдомены независимо; варианты и алиасы §5.5 | `TestAccept02_SubdomainsVariantsAliases` |
| п. 3 | SPA: DOM + cookies; HTML; дедуп/сжатие; чтение обратно | `TestAccept03_SPADomCookiesContent` |
| п. 4 | анализ (версии/score) + reanalyze без скачивания | `TestAccept04_ReanalyzeNoRefetch` |
| п. 5 | выборки из `laravel_domains`/`laravel_findings` | `TestAccept05_LaravelTablesQueries` |
| п. 6 | прогресс зон обновляется автоматически; метрики | `TestAccept06_ZoneProgressAutomatic` |
| п. 7 | суточный прогон в бюджете памяти на целевом сервере | вне dev-стенда — задача T-093, выполнена ([load-measurements.md](load-measurements.md) §13) |
| п. 8 | интервал перепроверки (6 месяцев, конфиг) | `TestAccept08_RecheckInterval` |
| п. 9 | рестарты сервисов: без потерь и «застрявших» (lease) | `TestAccept09_RestartNoStuckDomains` |
| п. 10 | поиск по имени в любой форме; понятное «не найдено» | `TestAccept10_DomainSearchByName` |
| п. 11 | реальные образцы заказчика через конвейер | `TestAccept11_CustomerSamplesPipeline` |

Особенности: переанализ (п. 4) проверяется копией набора правил с
никогда не срабатывающей пробой — версия набора меняется, выводы нет;
рендер SPA (п. 3) идёт через реальный `renderworker` на Chromium стека,
навигация подменяется на `http://` — внутренний CA Caddy не доверен
Chromium dev-стека (прод рендерит публичные сертификаты), live-тесты
рендера ходят так же; ожидания по образцам (пп. 4, 5, 10, 11) читаются
из `meta.yaml` корпуса (`rules.LoadSamples`) — приёмка не хранит копий
эталонов.


## 8. Диагностика частых проблем

- **`make up` падает на healthcheck** — `make logs SERVICE=<имя>`; первое
  включение air-сборки может занимать до минуты (`start_period` это учитывает).
- **После `make clean` первый `make up` долгий** — пустеют кэши Go: `make up`
  сначала прогревает общий кэш сборки в tools (`make warm-cache`), затем
  стартует сервисы; при желании модули можно докачать заранее `make mod-download`.
- **Схема БД в dirty-состоянии** — миграция прервалась на середине; сервис
  падает с понятной ошибкой. Восстановление: см. [`migrations.md`](migrations.md).
- **Нехватка памяти** — суммарные лимиты ≤ 8 ГБ; на слабых машинах уменьшите
  `MEM_*` в `.env` (postgres, chromium) — compose пересоздаст сервисы.
- **Права на bind-mount** — `DEV_UID`/`DEV_GID` в `.env` должны совпадать с
  вашим `id -u`/`id -g` (в `var/` пишет ingest как вы).
- **Конфликт портов** — порты на хосте настраиваются в `.env`
  (`PG_PORT`, `REDIS_PORT`, `PROMETHEUS_PORT`, ...), все биндятся на 127.0.0.1.
- **DNS не резолвится** — приложения используют unbound (172.28.100.10);
  проверьте `docker compose exec unbound nslookup <домен> 127.0.0.1`.
  Контейнер `tools` намеренно имеет обычный Docker-DNS (нужен proxy.golang.org);
  новые Go-зависимости качаются через `make mod-download`.
- **Новый модуль не находится при hot reload** — `GOFLAGS=-mod=readonly`:
  после правки `go.mod` выполните `make mod-download`, затем сервис
  пересоберётся сам.

## 9. Настройка хоста для нагрузочных тестов

Параметры контейнеров (заданы в compose): `ulimits.nofile=65535`,
`net.ipv4.ip_local_port_range=1024 65535` для воркеров.

Для нагрузочных тестов, приближенных к боевым (один IP, тысячи соединений),
на хосте примените комплект sysctl боевого сервера — расчёты и обоснование
в [`os-tuning.md`](os-tuning.md):

```bash
sudo install -m 0644 deploy/sysctl/99-crawler.conf /etc/sysctl.d/
sudo sysctl --system
```

Файл покрывает эфемерные порты и TIME_WAIT, nofile/fs.file-max, conntrack
(таблица + хэш) и UDP-буферы DNS. Инструкция, проверка и откат —
[`deploy/sysctl/README.md`](../deploy/sysctl/README.md). Настройки уровня
ядра хоста влияют на все контейнеры; в CI не требуются.
