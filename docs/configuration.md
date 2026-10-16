# Конфигурация сервисов

Единый YAML-файл + переопределение переменными окружения (ТЗ 5.10 основного
ТЗ). Пакет `internal/config`; пример файла — [`config.example.yaml`](../config.example.yaml)
в корне репозитория.

## Приоритет источников

Значение каждого параметра выбирается по цепочке:

```
переменная окружения  >  YAML-файл  >  значение по умолчанию
```

- **Пустое значение переменной считается незаданным** (перекрывать YAML
  «пустышкой» нельзя — очистите поле в самом YAML).
- Списки (`resolvers`, `subdomains`) переменная заменяет целиком.
- Все значения после наложения проходят валидацию (см. ниже); при любой
  проблеме сервис не стартует, перечисляя все ошибки сразу.

## Файл конфигурации

| Что | Как |
|---|---|
| Явный путь | переменная `CRAWLER_CONFIG` |
| Устаревшее имя | `APP_CONFIG` (используется, если `CRAWLER_CONFIG` не задана) |
| Автопоиск | если переменных нет — `config.yaml` в текущем каталоге, когда файл существует |
| Нет файла вовсе | конфигурация из значений по умолчанию + окружения (валидация всё равно обязательна: нужен `postgres.dsn`) |
| Явный путь указан, файла нет | ошибка запуска (молчаливых дефолтов не бывает) |
| Неизвестный ключ YAML | ошибка запуска (опечатки не игнорируются) |

Пример локального запуска из корня репозитория: `cp config.example.yaml config.yaml`
— файл подхватится автоматом, без переменных.

## Типы значений

| Тип | Форма в YAML | Форма в переменных | Пример |
|---|---|---|---|
| строка | YAML-строка | как есть | `/data/incoming` |
| целое | YAML-число | десятичное число | `50000` |
| булево | `true`/`false` | `strconv.ParseBool`: `true/false/1/0/T/F` | `false` |
| длительность | строка `time.ParseDuration` | та же | `5s`, `30m`, `24h`, `4380h` |
| размер | число байт или строка с единицей | та же | `2097152`, `512MB`, `1GiB` |
| вещественное | YAML-число | число с точкой | `0.6` |
| список | YAML-список | элементы через запятую, пробелы по краям обрезаются | `www,admin` |

Длительность числом (`total_timeout: 20`) — ошибка: единица измерения
была бы двусмысленной. Размеры: десятичные единицы (`KB`, `MB`, `GB`) —
степени 10³, двоичные (`KiB`, `MiB`, `GiB`) — степени 2¹⁰.

Для `subdomains` пустой элемент списка — апекс домена: в YAML это `""`,
в переменной — пустой элемент между запятыми (`CRAWLER_SUBDOMAINS=",www"`
апекс не задаёт; задаёт `",www"`). Значение переменной заменяет список
из YAML целиком — не забудьте апекс, если он нужен.

## Разделы и переменные

### `postgres`

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `dsn` | `CRAWLER_POSTGRES_DSN` | строка | — (**обязателен**) |
| `pool_size` | `CRAWLER_POSTGRES_POOL_SIZE` | целое | `20` |

`pool_size` — лимит соединений **одного сервиса** (пул `internal/db`,
T-007); рекомендация ТЗ — суммарно 20–50 соединений на все сервисы, то есть
при восьми сервисах задавайте 4–6 на сервис. Соединения открываются лениво.
Подробнее: [`db.md`](db.md).

### `redis`

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `addr` | `CRAWLER_REDIS_ADDR` | host:port | `127.0.0.1:6379` |

### `ingest` (ТЗ 5.1)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `dir` | `CRAWLER_INGEST_DIR` | путь | `/data/incoming` |
| `batch_size` | `CRAWLER_INGEST_BATCH_SIZE` | целое | `50000` |
| `stable_seconds` | `CRAWLER_INGEST_STABLE_SECONDS` | секунды | `10` |

### `scheduler` (ТЗ 5.3)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `recheck_interval` | `CRAWLER_SCHEDULER_RECHECK_INTERVAL` | длительность | `4380h` (≈ 6 мес.) |
| `batch_size` | `CRAWLER_SCHEDULER_BATCH_SIZE` | целое | `5000` (рекомендация: 1000–10000) |
| `queue_high_watermark` | `CRAWLER_SCHEDULER_QUEUE_HIGH_WATERMARK` | целое | `100000` (backpressure, ТЗ 4.2) |
| `lease` | `CRAWLER_SCHEDULER_LEASE` | длительность | `30m` |
| `temp_error_retry_after` | `CRAWLER_SCHEDULER_TEMP_ERROR_RETRY_AFTER` | длительность | `24h` |

### `dns` (ТЗ 5.4)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `resolvers` | `CRAWLER_DNS_RESOLVERS` | список host:port | `127.0.0.1:53` (unbound) |
| `fallback_resolvers` | `CRAWLER_DNS_FALLBACK_RESOLVERS` | список host:port | `1.1.1.1:53, 9.9.9.9:53` |
| `concurrency` | `CRAWLER_DNS_CONCURRENCY` | целое | `2000` |
| `max_qps` | `CRAWLER_DNS_MAX_QPS` | целое | `10000` |
| `ipv6` | `CRAWLER_DNS_IPV6` | булево | `true` (`false` — AAAA не запрашиваются) |

### `http` (ТЗ 5.5, таблица лимитов)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `concurrency` | `CRAWLER_HTTP_CONCURRENCY` | целое | `2000` |
| `per_ip_limit` | `CRAWLER_HTTP_PER_IP_LIMIT` | целое | `4` (рекомендация: 3–4) |
| `connect_timeout` | `CRAWLER_HTTP_CONNECT_TIMEOUT` | длительность | `5s` |
| `header_timeout` | `CRAWLER_HTTP_HEADER_TIMEOUT` | длительность | `10s` |
| `total_timeout` | `CRAWLER_HTTP_TOTAL_TIMEOUT` | длительность | `20s` |
| `max_redirects` | `CRAWLER_HTTP_MAX_REDIRECTS` | целое | `8` (`0` — не следовать) |
| `max_body_bytes` | `CRAWLER_HTTP_MAX_BODY_BYTES` | размер | `2097152` (2 MiB) |
| `user_agent` | `CRAWLER_HTTP_USER_AGENT` | строка | `DomainCrawler/1.0 (contact: admin@example.invalid)` |

`user_agent` обязателен: честный User-Agent с контактами (ТЗ 5.5, §9).

### `render` (ТЗ 5.6)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `remote_url` | `CRAWLER_RENDER_REMOTE_URL` | URL | `""` |
| `contexts` | `CRAWLER_RENDER_CONTEXTS` | целое | `3` (рекомендация: 2–4) |
| `timeout` | `CRAWLER_RENDER_TIMEOUT` | длительность | `25s` |
| `restart_every_pages` | `CRAWLER_RENDER_RESTART_EVERY_PAGES` | целое | `500` |

`remote_url` — расширение dev-окружения: адрес внешнего Chromium по CDP
(`ws://host:9222`); в боевом режиме воркер запускает браузер сам, поле пусто.

### `content` (ТЗ 5.7)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `dir` | `CRAWLER_CONTENT_DIR` | путь | `/data/content` |
| `pack_size` | `CRAWLER_CONTENT_PACK_SIZE` | размер | `512MB` (рекомендация: 512 МБ–1 ГБ) |
| `max_store_bytes` | `CRAWLER_CONTENT_MAX_STORE_BYTES` | размер | `2097152` (рекомендация: 1–2 МБ) |
| `zstd_dictionary` | `CRAWLER_CONTENT_ZSTD_DICTIONARY` | путь | `/data/content/dict.zst` (пусто — без словаря) |

### `analyzer` (ТЗ 5.8)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `rules_dir` | `CRAWLER_ANALYZER_RULES_DIR` | путь | `/etc/crawler/rules` |
| `laravel_min_score` | `CRAWLER_ANALYZER_LARAVEL_MIN_SCORE` | число | `0.6`, диапазон (0, 1] |

### `subdomains` (ТЗ 5.10)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| список | `CRAWLER_SUBDOMAINS` | список меток | `""` (апекс), `www`, `admin`, `crm`, `api`, `app` |

Начальное наполнение таблицы `subdomains`; управление на лету —
`crawlerctl subdomains add|disable` (T-083). Не более 63 записей (битовая
маска `dns_mask`). Регистр приводится к нижнему; каждая метка — DNS-метка
без точек (`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`), повторы запрещены.

### `monitor` (ТЗ 5.9)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `listen` | `CRAWLER_MONITOR_LISTEN` | [host]:port | `:9100` |
| `zone_stats_interval` | `CRAWLER_MONITOR_ZONE_STATS_INTERVAL` | длительность | `60s` (рекомендация: 30–60 с) |

### `log` (каркас сервисов)

| YAML | Переменная | Тип | По умолчанию |
|---|---|---|---|
| `level` | `CRAWLER_LOG_LEVEL` | `debug`\\|`info`\\|`warn`\\|`error` | `info` |

Уровень структурированного логгера `log/slog` каркаса `internal/service`
(T-012). Расширение к минимальному составу ТЗ 5.10: уровень нужен всем
сервисам сразу, а не отдельному этапу. Регистр значения не важен.

## Валидация

Проверяется всё сразу, ошибка перечисляет проблемы вида `секция.поле: что не так`:

- `postgres.dsn` непуст; `pool_size` 1..100;
- `redis.addr`, `dns.resolvers`, `dns.fallback_resolvers`, `monitor.listen` —
  корректные `host:port` (порт 1..65535); резолверы — хотя бы один;
- все таймауты/интервалы положительны; `ingest.batch_size` 1..1000000,
  `ingest.stable_seconds` 1..3600, `scheduler.batch_size` 1..100000,
  `queue_high_watermark` ≥ 1;
- `dns.concurrency`/`http.concurrency` 1..100000, `dns.max_qps` 1..1000000,
  `http.per_ip_limit` 1..64, `http.max_redirects` 0..100, `max_body_bytes` > 0;
- `http.user_agent` непуст;
- `render.contexts` 1..16, `restart_every_pages` 1..1000000,
  `remote_url` (если задан) — корректный URL;
- `content.pack_size` 1 MiB..8 GiB, `max_store_bytes` 1 байт..16 MiB;
- `analyzer.laravel_min_score` в (0, 1];
- `subdomains`: 1..63 записи, метки в нижнем регистре без точек, без повторов;
- пути (`ingest.dir`, `content.dir`, `analyzer.rules_dir`) непустые;
- `log.level` — одно из `debug`/`info`/`warn`/`error`.

## Устаревшие переменные (обратная совместимость)

| Переменная | Действие |
|---|---|
| `APP_PG_DSN` | переопределяет `postgres.dsn`, если не задана `CRAWLER_POSTGRES_DSN`; используется `deploy/docker/compose.yaml` и Makefile |
| `APP_CONFIG` | запасное имя переменной пути к файлу конфигурации |
| `METRICS_ADDR` | адрес healthz/metrics конкретного сервиса; для `monitor` значение по умолчанию — `monitor.listen` из конфигурации, для остальных — фиксированные порты dev-каркаса (`:9101`…`:9106`) |

Новые имена (`CRAWLER_*`) всегда важнее устаревших. Дополнять стоит только
ими; устаревшие поддерживаются, пока на них ссылается dev-окружение.

## Dev-окружение (Docker)

- [`deploy/docker/config.dev.yaml`](../deploy/docker/config.dev.yaml)
  монтируется в контейнеры как `/etc/crawler/config.yaml`; путь задаётся
  переменной `CRAWLER_CONFIG` (общий блок `x-go-common-env` в compose).
- DSN поверх YAML даёт `APP_PG_DSN` (собирается в compose из `.env`).
- `User-Agent` dev-краулера: `CRAWLER_USER_AGENT` из `.env` → переменная
  `CRAWLER_HTTP_USER_AGENT` в контейнерах (обязательная пометка dev).
- В docker-сети нет IPv6: в dev-конфиге `dns.ipv6: false`; fallback-резолверы
  пусты — окружение изолировано от интернета (см.
  [`dev-environment.md`](dev-environment.md), режим internet).

## Интеграция в код

- `config.Load()` — путь из `CRAWLER_CONFIG`/`APP_CONFIG` (или автопоиск
  `./config.yaml`), наложение окружения, валидация; вызывается в `main`
  каждого сервиса.
- `config.LoadFile(path)` — явный файл + окружение процесса.
- `config.LoadEnv(getenv)` — то же с инъекцией окружения (тесты).
- `config.Default()` — значения по умолчанию из ТЗ 5.10.
- `service.Options.Config` — загруженный конфиг передаётся в каркас
  (`internal/service`); DSN для миграций берётся из него (или из прямого
  `Options.PGDSN`), `log.level` — для логгера каркаса
  (`service.NewLogger`; сам `Run` тоже создаёт логгер, если `Options.Logger`
  не задан, и делает его дефолтным `slog.SetDefault`).
- `db.NewOptions(cfg.Postgres)` — секция `postgres` → параметры пула
  `internal/db` (`db.Open`; см. [`db.md`](db.md)).

Таблица привязок «переменная → поле» — `envSpecs()` в
`internal/config/load.go`; тесты: `internal/config/config_test.go`
(дефолты, YAML, окружение, приоритеты, валидация) и
`test/integration/config_test.go` (dev-окружение целиком).
