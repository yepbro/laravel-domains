# Dev-окружение Domain Crawler

Воспроизводимое окружение разработки на Docker Compose: PostgreSQL, Redis,
локальный DNS-резолвер (unbound), изолированный тестовый стенд «мини-интернет»
(CoreDNS + Caddy + mockserver), headless-Chromium, мониторинг и Go-сервисы
с горячей перезагрузкой кода.

Соответствует `tasks/TZ_docker_dev_environment.md`.

Практическая работа с Docker (запуск/остановка, команды внутри контейнеров,
тома, диагностика) вынесена в отдельную шпаргалку: [`docker.md`](docker.md).

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
| (по умолчанию) | `postgres`, `redis`, `unbound` |
| `testbed` | `coredns`, `testweb` (Caddy), `mockserver` |
| `app` | Go-сервисы (`ingest` … `monitor`, `render-worker`) + `chromium` |

Миграции PostgreSQL отдельного сервиса не имеют: каждый Go-сервис применяет
их сам при старте (см. [`migrations.md`](migrations.md)).
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
migrations/              миграции golang-migrate: встраиваются в бинарники,
                        применяются сервисами при старте (make migrate-new)
rules/                   правила Laravel (YAML), перезагрузка: make rules-reload
testdata/laravel-samples/  образцы страниц (index.html, headers.json, cookies.json, meta.yaml)
var/incoming/            наблюдаемая папка ingest: <зона>/add/*.txt, <зона>/delete/*.txt
var/incoming/processed/  архив обработанных файлов (структура зоны/операции сохраняется)
var/incoming/failed/     ошибочные файлы с лог-отчётом *.report рядом
var/content/             хранилище контента (dev)
tmp/                     артефакты hot-reload (air)
```

Тома Docker: `pg_data` (БД), `gomodcache`/`gocache` (кэши Go — быстрые
пересборки), `caddy_data` (TLS-сертификаты testweb), `prom_data`.

### Поток данных ingest (ТЗ 5.1)

Сервис `ingest` наблюдает `var/incoming/` (fsnotify + периодический обход,
стабильность размера/mtime) и прогоняет каждый готовый файл через
конвейер: разбор пути `<зона>/add|delete/*.txt` → зона (создаётся
автоматически при первом файле) → один проход по файлу (построчный
разбор и sha256 одновременно) → валидация строк (невалидные — в лог
`rejected` с причиной, атрибут `service=rejected` в stderr сервиса) →
загрузка в staging-таблицу → применение (`add` — вставка/возврат из
deleted, `delete` — перевод в deleted) → отчёт в таблицу `ingest_files`
→ перемещение файла в архив:

| Итог файла | Куда | Что остаётся |
|---|---|---|
| применён | `processed/<зона>/<операция>/…` | строка `ingest_files` (sha256, счётчики) |
| дубль (sha256 уже в журнале) | `processed/…` | без нового отчёта — повторная обработка не выполняется |
| ошибочен (структура пути, зона, доля отказов > `ingest.max_error_ratio`) | `failed/…` | лог-отчёт `<файл>.report` рядом: причина и счётчики строк |
| сбой инфраструктуры (БД, чтение) | остаётся в `incoming/` | переработается после рестарта сервиса (at-least-once) |

Тот же файл (по содержимому, sha256) повторно не применяется —
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

Образцы Laravel добавляются каталогом в `testdata/laravel-samples/<id>/`
(`index.html`, `headers.json`, `cookies.json`, `meta.yaml`) — домен
`<id>.crawl.test` создаётся автоматически при следующем `make testbed-gen`.

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
  по `make rules-reload` запускает переанализ без рестарта.

## 6. CLI crawlerctl

`crawlerctl` — CLI управления краулером (ТЗ 4.1). Каркас команд готов
(задача T-011): роутинг, флаги, валидация имён (internal/idn) и `--help`
для каждой команды; функциональность подключается по мере реализации
этапов. Коды возврата: 0 — успех, 1 — ошибка/«не реализовано»,
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
crawlerctl zones list|enable|disable     # управление зонами (T-084)
crawlerctl reanalyze [--zone ИМЯ]        # переанализ по новым правилам
                                          # (T-074): постановка в q:analyze
                                          # без повторного скачивания
crawlerctl recheck <домен> [домен …]     # принудительная постановка домена
                                         # на проверку (T-084)
crawlerctl dict train <файл> [--samples ПУТЬ]…
                                         # обучение/переобучение словаря
                                         # сжатия zstd (ТЗ 5.7)
crawlerctl seed / test-rules             # dev-команды (make seed, make test-rules)
```

Обучение словаря сжатия контента (ТЗ 5.7): `--samples` — файл или каталог
с `*.html`/`*.htm` (по умолчанию `testdata/laravel-samples`; флаг можно
повторять, образцы читаются до лимита корпуса 64 МиБ). Словарь записывается
атомарно и подключается конфигурацией `content.zstd_dictionary`; страницы,
сжатые прежним словарём, для чтения требуют прежний словарь. Пример
(словарь — в каталоге данных dev-окружения):

```
docker compose run --rm -v "$(pwd)/var/content:/out tools \
    go run ./cmd/crawlerctl dict train /out/dict.zst --samples testdata/laravel-samples
```

Через make: `make status`, `make domain NAME=example.crawl.test`,
`make reanalyze`, `make seed`, `make test-rules`. Остальные команды —
`docker compose run --rm tools go run ./cmd/crawlerctl <команда>`
(см. `make shell`).

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
длины очередей `q:dns`/`q:http`/`q:render`/`q:analyze`/`q:dead` (XLEN),
скорость и ETA завершения первого цикла: `(pending + queued) / скорость`.
Скорость доменов — средняя с начала суток (`last_checked_at` имеет точность
дня, часового окна нет; короче 10 минут окно не используется — сразу после
полуночи оценка скачет); HTTP-результаты и рендеры — за последний час по
`subdomain_results.checked_at` (собственной отметки времени у рендера нет —
считается доля `rendered` в окне). Темп этапов DNS и анализа из БД не
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

## 7. Отладка, метрики, БД, очереди

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

Интеграционные тесты устойчивости очередей (`test/integration/
resilience_test.go`, T-032) работают в выделенной БД Redis **15** (dev-конфиг
разрешает 16 БД; сервисы используют db 0): боевые имена стримов `q:*` в ней
создаются и вычищаются тестами, запущенные сервисы им не мешают.

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
- **DNS не резолвится** — приложения используют unbound (172.28.0.10);
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
