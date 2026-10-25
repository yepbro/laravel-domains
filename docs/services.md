# Каркас Go-сервисов

Пакет `internal/service` (T-001, T-007, T-012) — общая обвязка всех
долгоживущих сервисов проекта (всё, кроме CLI `crawlerctl` и утилит
testbed). `service.Run` выполняет за сервис стандартную последовательность
старта и остановки, а сервис подставляет только свою рабочую логику.

## Жизненный цикл

```
config.Load (в main)
  → service.Run(Options)
      логгер (slog) + slog.SetDefault
      → миграции PostgreSQL (advisory-блокировка, ТЗ 6.3)
      → пул БД (Options.OnDBPool, internal/db)
      → HTTP /livez + /healthz + /metrics (Options.MetricsAddr)
      → Options.OnRun(ctx)                — рабочая логика
      ⟵ SIGTERM/SIGINT | ошибка OnRun | ошибка HTTP | завершение OnRun
      остановка (ТЗ 3.4):
        1. отмена ctx рабочей логики и ожидание её возврата
           (бюджет Options.ShutdownTimeout, по умолчанию 10 с);
           повторный сигнал — принудительный выход без ожидания;
        2. HTTP-сервер метрик (дожидается начатых скрейпов);
        3. пул БД закрывается последним.
```

Ошибки жизненного цикла `Run` логирует сам (структурированно, с контекстом
сервиса) и возвращает вызывающему `main` — тому достаточно `os.Exit(1)`:
все `cmd/*/main.go` устроены одинаково.

## Логирование

`service.NewLogger(name, cfg)` — структурированный логгер stdlib `log/slog`
(новых зависимостей нет): текстовый формат на stderr, уровень из
`log.level` конфигурации (`CRAWLER_LOG_LEVEL`, по умолчанию `info`),
каждая запись помечена атрибутом `service=<имя>`. `Run` устанавливает
логгер дефолтным (`slog.SetDefault`), поэтому миграции и любой код сервиса
через `slog.Default`/`slog.Info` пишут в общий поток. Если `main` создал
логгер сам (чтобы логировать до `Run`), он передаёт его в `Options.Logger`.

## Опции для рабочей логики

| Опция | Назначение |
|---|---|
| `OnRun(ctx) error` | рабочая логика (воркеры этапов 2+): контекст отменяется при SIGTERM/SIGINT — по отмене доработать текущую задачу или вернуть её в очередь (ТЗ 3.4) и вернуться. Возврат без сигнала останавливает сервис: `nil` — чисто (одноразовые задачи), ошибка — с кодом 1 |
| `OnDBPool(pool)` | получить пул PostgreSQL (после миграций); пул закрывается каркасом при shutdown последним |
| `OnSIGHUP()` | перезагрузка правил Laravel без рестарта (`make rules-reload`, ТЗ 5.8) |
| `ExtraMux(mux)` | дополнительные хендлеры HTTP-сервера метрик |
| `Metrics` | готовый реестр Prometheus сервиса (T-077): main создаёт реестр под приборы рабочей логики до старта `OnRun`, каркас отдаёт его через `/metrics`; nil — каркас создаёт реестр сам |
| `Logger` | готовый логгер (иначе каркас создаст сам) |
| `ShutdownTimeout` | бюджет остановки (иначе 10 с) |

Схема сервиса с БД:

```go
cfg, err := config.Load()
if err != nil {
    log.Fatalf("[scheduler] конфигурация: %v", err)
}
logger := service.NewLogger("scheduler", cfg)
if err := service.Run(service.Options{
    Name:        "scheduler",
    Config:      cfg,
    Logger:      logger,
    MetricsAddr: service.EnvOr("METRICS_ADDR", ":9102"),
    OnDBPool:    func(pool *pgxpool.Pool) { svc = newWorker(pool, cfg, logger) },
    OnRun:       func(ctx context.Context) error { return svc.Run(ctx) },
}); err != nil {
    os.Exit(1)
}
```

## Метрики

ТЗ 5.9: HTTP-эндпоинт Prometheus на каждом сервисе. Каркас поднимает
`/livez`, `/healthz` и `/metrics` на `Options.MetricsAddr` (адрес `monitor` —
`monitor.listen` из конфигурации, ТЗ 5.10, важнее `METRICS_ADDR`; адреса
остальных сервисов — только `METRICS_ADDR`, dev-порты `:9101`…`:9108`).
Prometheus скрейпит каждый сервис отдельно (dev-конфиг
`deploy/docker/prometheus/prometheus.yml`, профиль `monitoring`):
`monitor.listen` — адрес HTTP-эндпоинта самого сервиса monitor, а не
агрегатор чужих метрик.

| Сервис | Адрес | Чем задаётся |
|---|---|---|
| monitor | `:9100` | `monitor.listen` (ТЗ 5.10), важнее `METRICS_ADDR` |
| ingest | `:9101` | `METRICS_ADDR` |
| scheduler | `:9102` | `METRICS_ADDR` |
| dns-worker | `:9103` | `METRICS_ADDR` |
| http-worker | `:9104` | `METRICS_ADDR` |
| render-worker | `:9105` | `METRICS_ADDR` |
| analyzer | `:9106` | `METRICS_ADDR` |
| web | `127.0.0.1:9107` | `METRICS_ADDR` (loopback: лендинг публичен на `web.listen`, метрики — нет, ТЗ §9) |
| dashboard | `127.0.0.1:9108` | `METRICS_ADDR` (loopback: дашборд публичен на `dashboard.listen`, метрики — нет, ТЗД §4.2) |

### Пробы живости и готовности (T-110)

| Проба | Семантика | Что проверяет | Потребитель |
|---|---|---|---|
| `/livez` | liveness: процесс жив | HTTP-цикл процесса отвечает; зависимости не трогает | compose-healthcheck сервисов приложения (`docker ps`, `up --wait`), оператор |
| `/healthz` | readiness: зависимости | пул PostgreSQL коротким Ping (503 при недоступности, C-03); сервис без пула — всегда 200 | оператор (`curl 127.0.0.1:91xx/healthz` в systemd-деплое, `dc exec` в dev) |

`/livez` сознательно не ходит в БД/Redis: их недоступность — не смерть
процесса, и рестартовать воркеры при сбое PostgreSQL значило бы лечить
симптом. Отдельного признака «фоновый цикл не паниковал» каркас не
держит: паника/ошибка/завершение `OnRun` и так останавливают весь
процесс (`Run` возвращается, оркестратор рестартует по политике) —
состояние «цикл умер, процесс жив» не живёт дольше бюджета остановки.

`/healthz` в compose-healthcheck не стоит (T-110): задачи воркеры тянут
из Redis-стримов сами — маршрутизатора «впускать трафик» нет, а при
недоступной БД стена unhealthy-воркеров лишь дублировала бы статус
самого postgres (у него свой healthcheck). Redis в пробу также не
входит: клиенты Redis живут внутри `OnRun` воркеров (каркас их не
видит), лёгкой готовой проверки нет, а втягивание зависимости в путь
пробы меняло бы семантику readiness; доступность Redis для конвейера
видна по `crawler_queue_depth` и логам. Для мониторинга живость сервиса
фиксирует метрика `up` скрейпов Prometheus (`/metrics`), пробы он не
читает. Systemd-деплой healthcheck-ов и watchdog-а не имеет
(`Restart=on-failure` по коду выхода, T-085) — обе пробы там доступны
оператору на loopback.

Реестр метрик — `metrics.New(name)` (T-076): собственный
`*prometheus.Registry` на сервис (одноимённые метрики разных сервисов не
конфликтуют) с метриками каркаса — `crawler_service_up{service}`,
`crawler_build_info{service,mode,version,revision}` (mode: `dev-stub` —
dev-сборки без инъекции, `prod` — боевые сборки через `-ldflags -X
…buildMode=prod`; version/revision — версия и короткий хеш коммита
сборки, инъекцируются теми же `-X …buildVersion=…`/`…buildRevision=…`
из git: `git describe --tags --always --dirty` и `git rev-parse --short
HEAD`, при недоступном git — `unknown`; dev-сборки — `dev` и пустая
ревизия, пустое значение метки в PromQL эквивалентно её отсутствию) и
коллекторами Go/runtime и
процесса (uptime выводится из `process_start_time_seconds` в PromQL).

Реестр — фабрика метрик рабочей логики: `Counter`/`Gauge`/`Histogram`
(+ `Must`-варианты) создают вектор с единой первой меткой `service` и
возвращают его каррированным — значение `service` подставляется
автоматически, вызывающий передаёт только значения остальных метк:

```go
tasks := registry.MustCounter(prometheus.CounterOpts{
    Name: "crawler_stage_tasks_total",
    Help: "Обработанные домены по этапам конвейера.",
}, metrics.LabelStage)
tasks.WithLabelValues("dns").Inc() // service подставится сама
```

Имена метрик начинаются с `crawler_`, счётчики — с суффиксом `_total`;
HELP/TYPE экспортируются Prometheus из `Name`/`Help` — документация
метрики пишется там же, где создаётся прибор. Единые имена меток —
константы пакета: `LabelService` (вручную не передаётся), `LabelStage`
(этап конвейера: dns/http/render/analyze), `LabelQueue` (имя очереди:
`q:dns`/`q:http`/`q:render`/`q:analyze`/`q:dead`), `LabelType` (тип
ошибки).

Повторная регистрация идентичной метрики (то же имя, help и метки) — не
ошибка: возвращается уже зарегистрированный вектор, поэтому конструкторы
приборов можно вызывать многократно в тестах (прометеевский
`AlreadyRegisteredError` не паникует; гистограммы — корзины первого
вызова выигрывают, `Buckets` не входят в дескриптор). Конфликт описаний
— то же имя с другим help, набором меток или типом — ошибка
(`Must`-варианты паникуют): это программная ошибка, а не штатный
случай. Зарезервированная метка `service` и дубликаты меток
отклоняются ошибкой до создания вектора.

Сами приборы этапов (ТЗ 5.9: пропускная способность по этапам, длины
очередей, ошибки по типам, доля рендера, время ответа, глубина backlog,
память воркеров) регистрируют сервисы через `NewMetrics`-конструкторы
своих пакетов воркеров (`Options.Metrics` — реестр сервиса; `nil` —
изолированные сборки без метрик) и коллекторы
(`internal/metrics/collect.go`, `internal/scheduler/backlog.go`).

### Метрики рабочей логики (T-077)

| Метрика | Тип | Метки | Экспортирует | Точка инкремента |
|---|---|---|---|---|
| `crawler_stage_tasks_total` | counter | `stage` | dns-worker (`dns`), http-worker (`http`), render-worker (`render`), analyzer (`analyze`) | завершённая задача этапа: домен (dns, http) или запись субдомен×вариант (render, analyze) |
| `crawler_stage_seconds` | histogram | `stage` | те же четыре | длительность той же задачи (корзины свои по этапу) |
| `crawler_stage_errors_total` | counter | `stage`, `type` | http-worker (`timeout`/`redirects`/`tls`/`body`/`network` — классы ErrorClass, §8), render-worker (`task`/`context`/`browser` — классы render.Class) | каждая ошибочная проверка протокола / неподтверждённая доставка рендера |
| `crawler_domain_status_total` | counter | `type` | dns-worker (`not_delegated`/`delegated_no_address`/`temp_error`/`invalid` — диагноз ядовитой строки, T-109), http-worker (`alive`/`no_http`/`temp_error`) | финальный статус домена терминальной ветки эпизода (ТЗ 6.2) |
| `crawler_laravel_near_findings` | gauge | — | monitor (T-115) | находки Laravel в серой зоне порога `[min_score/2, min_score)` — материал калибровки правил |
| `crawler_laravel_appeared_total` / `crawler_laravel_lost_total` | gauge | — | monitor (T-118) | кумулятивные появления/исчезновения строк `laravel_domains` (журнал `laravel_history`, свежие эпизоды HTTP); монотонны — counter-семантика |
| `crawler_redis_memory_bytes` | gauge | `kind` | monitor (T-154) | тот же проход, что `crawler_redis_memory_ratio`: `kind=used` — `used_memory`, `kind=max` — `maxmemory` (0 — потолка нет) из `INFO memory` |
| `crawler_http_response_seconds` | histogram | — | http-worker | каждая доступная некэшированная проверка: время до заголовков итоговой страницы (ResponseMS, ТЗ 5.5 п. 7); кэш-попадания сети не касаются |
| `crawler_http_per_ip_wait_seconds` | histogram | — | http-worker (T-165) | каждое успешное взятие слота per-IP семафора соединений с чистым временем ожидания очереди (мгновенный слот — микросекунды; отменённые ожидания не наблюдаются — их считает классификация ошибок); единица — диал (задача делает несколько подключений); корзина `le="2"` — граница критерия head-of-line парковочных IP: доля ожиданий > 2 с = `1 − rate(…_bucket{le="2"})/rate(…_count)`, норма < 5 % на смеси боя; материал для `http.per_ip_overrides` |
| `crawler_http_dead_host_cutoff_tasks_total` | counter | — | http-worker (T-164) | задачи q:http, завершённые dead-host cutoff: apex и www упали на connect-фазе (connection refused/connect-таймаут, оба протокола), остальные субдомены не проверялись; контроль критерия mean задачи ≤ 7 с — `crawler_stage_seconds{stage="http"}` |
| `crawler_http_dead_host_cutoff_skipped_checks_total` | counter | — | http-worker (T-164) | проверки (субдомен × протокол), не выполненные из-за cutoff, — сэкономленный пул задачи; ошибками этапа не считаются (`crawler_stage_errors_total` серии `cutoff` не имеет); доля ложных cutoff (<1 %, критерий) меряется контрольной выборкой по биту `dead_host_cutoff` в `domains.flags` |
| `crawler_render_records_total` | counter | `rendered` | http-worker (`false` — записи результата), render-worker (`true` — заменённые на DOM) | доля рендера = `rate(…{rendered="true"}) / rate(…{rendered="false"})` |
| `crawler_render_tarpit_finalized_total` | counter | — | render-worker (T-148) | запись финализирована без рендера по исчерпании `render.timeout_attempts`: итог — сырой HTML HTTP-фазы, публикация q:analyze, XACK (в q:dead тарпиты не попадают); рост — доля таймаут-тарпитов на рендере |
| `crawler_render_cert_finalized_total` | counter | — | render-worker (T-159) | запись финализирована без рендера по постоянной ошибке TLS-сертификата хоста (`net::ERR_CERT_AUTHORITY_INVALID`/`ERR_CERT_COMMON_NAME_INVALID`, с T-172 и `ERR_CERT_DATE_INVALID` — замер: 0 успешных ретраев из 321), с первой доставки: cert-класс не жжёт попытки доставок и не попадает в q:dead; рост при нулевом приросте q:dead — политика работает |
| `crawler_render_task_finalized_total` | counter | `reason` | render-worker (T-172) | запись финализирована без рендера по ошибке класса задачи рендера (отказ соединения, load error — свойство хоста) на последней доставке `redis.max_attempts`: итог — сырой HTML HTTP-фазы, публикация q:analyze, XACK; метка reason — код `net::ERR_*` (например `net::ERR_CONNECTION_REFUSED`), `timeout` или `other`: состав того, что раньше уносило записи в q:dead |
| `crawler_ingest_bad_hashes_total` | counter | — | ingest | итог каждого прохода CleanupDeleted (хук `CleanupOptions.OnPass`, T-099): удалённые результаты с `content_hash` неверной длины; в норме не растёт вовсе — рост = ошибка писателя результатов (T-056) |
| `crawler_content_retention_rows_total` / `_refs_total` / `_contents_total` / `_freed_bytes_total` | counter | — | http-worker (T-166) | итог каждого прохода RetentionLoop (хук `RetentionOptions.OnPass`): строки `subdomain_results`, очищенные от контента/jsonb, срезанные ссылки `content_hash`, удалённые строки `contents` и их сжатые байты (место физически возвращает компактификация паков T-045 — сходимость видно по `disk_avail`); останов роста при работающем цикле = candidates кончились (плато) |
| `crawler_content_retention_skips_total` | counter | — | http-worker (T-166) | пропуски прохода по интерлоку: глубина q:analyze у порога `content.retention_analyze_watermark` — анализатор не успевает, тела и headers/cookies ещё нужны; рост = анализ отстаёт, диск продолжает расти |
| `crawler_content_retention_pending_errors_total` | counter | — | http-worker (T-166/T-167) | пропуски прохода по ошибке чтения непотреблённых ключей q:render/q:analyze (консервативно — записи очередей не тронуты); рост = защита не может прочитать стримы, разбираться сразу: без неё окно чистки ждущих записей открыто |
| `crawler_content_retention_deferred_total` | counter | `stream` | http-worker (T-166/T-167) | записи, исключённые из чистки: сообщения ждут потребителей стрима (`q:render` — рендер SPA, `q:analyze` — анализ; запись может ждать оба — считается в каждом); растёт при застое очередей — сигнал дренировать потребителей (runbooks §5.2) |
| `crawler_content_retention_pass_seconds` / `crawler_content_retention_passes_total{result}` | histogram / counter | `result` | http-worker (T-166) | длительность и число проходов retention (`ok` — дренаж выполнен, `skipped` — интерлок, `skipped_pending` — ошибка чтения защиты; алерты по `rate(passes{result="skipped"})` и `rate(passes{result="skipped_pending"})`) |
| `crawler_content_retention_bad_hashes_total` / `crawler_content_retention_missing_refs_total` | counter | — | http-worker (T-166) | аномалии учёта: срезанные ссылки с хэшем неверной длины / без строки `contents`; в норме не растут вовсе — рост = ошибка писателя (T-056) или расхождение refcount (T-044) |
| `crawler_analyzer_laravel_unfound_total` | counter | — | analyzer (T-187) | снятия Laravel-находки переанализом (доставка без fresh): домен покинул `laravel_domains` без свежего эпизода HTTP; каждому отвечает точечный дренаж контента домена (`content.DrainDomain`) |
| `crawler_analyzer_unfound_drained_rows_total` | counter | — | analyzer (T-187) | строки `subdomain_results`, очищенные точечным дренажом при снятии находки переанализом: без дренажа остались бы позади курсора retention до редких catch-up-кругов (docs/content.md) |
| `crawler_analyzer_unfound_drain_errors_total` | counter | — | analyzer (T-187) | неудачи точечного дренажа (Warn в логе): записи домена остались позади курсора — подберут редкие catch-up-круги T-185 либо перепроверка T-030; рост = разбирать причину сбоя |
| `crawler_unit_oom_events` | gauge | `unit`, `kind` | monitor (T-145) | проход алертов (5 мин): счётчики memory.events cgroup v2 юнитов `crawler-*.service` (kind=oom/oom_kill); обнуляются пересозданием cgroup на рестарте юнита — gauge, не counter; рост = события OOM (алерт unit_oom) |
| `crawler_unit_memory_high_events` | gauge | `unit` | monitor (T-162) | тот же проход, что unit_oom: счётчик memory.events high (попадания за мягкий потолок MemoryHigh — троттлинг reclaim'ом); накапливается за жизнь cgroup, осмыслен рост — алерт `memory_high` при росте ≥2 проходов подряд (T-162: предвестник фаталов http-worker, T-157) |
| `crawler_psi_cpu_some_percent` | gauge | `kind` | monitor (T-162) | проход алертов: some-доля времени задержки CPU из `/proc/pressure/cpu` хоста (kind=avg10/avg60/avg300); устойчивый avg300 >90 % — голод планировщика (алерт `psi_cpu`, T-162), ядро без PSI — ряда нет |
| `crawler_unit_restarts` | gauge | `unit` | monitor (T-145) | тот же проход: `NRestarts` юнитов (крэш-рестарты с последнего старта/`reset-failed`); источник обоих — `systemctl show` + чтение cgroupfs (internal/monitor/unitevents.go) |
| `crawler_queue_depth` | gauge | `queue` | dns-worker (`q:dns`), http-worker (`q:http`), render-worker (`q:render`), analyzer (`q:analyze`), monitor (`q:dead`, T-154) | коллектор глубины потребляемого стрима раз в 15 с (backlog группы потребления: `min(lag, XLEN)` + `pending` — лаг ограничен длиной стрима против фантома Redis 7.0.x после XTRIM недоставленного, T-168; `metrics.CollectQueueLoop`, источник — гейт backpressure T-026); `q:dead` публикует monitor с прохода алерта `queue_dead` (групп у карантина нет — XLEN, обёртка `monitor.NewDeadDepthMetrics`) |
| `crawler_render_pending_domains` | gauge | — | monitor (T-175) | цикл рендер-долга (`monitor.render_debt_interval`, 5 мин): домены с битом `render_pending` — счётчик `crawler_counters['render_pending']` (миграция 000015) ведут переходы бита в транзакции перехода; авторитетное значение периодически перезаписывает реконсиляция T-178 (наблюдаемость без seq scan по 41,8 млн строк; лаг `q:render` — `crawler_queue_depth{queue="q:render"}`, долг = сумма — алерт `render_debt` и строка отчёта нагрузки T-173) |
| `crawler_queue_dead_buried_total` | counter | `queue` | dns-worker, http-worker, render-worker, analyzer (T-154) | каждый перевод записи в q:dead (`Reliab.bury` после публикации Dead и XACK исходной записи; хук `ReliabOptions.OnBury` ← `metrics.NewDeadBuriedHook`) — метка `queue` — стрим-источник умершей задачи; темп смертей — `rate()`, неразобранный остаток — глубина q:dead монитора |
| `crawler_redis_memory_ratio` | gauge | — | monitor (T-154) | проход алертов: `used_memory/maxmemory` из `INFO memory` (обёртка источника `monitor.NewRedisMemoryMetrics`); при noeviction — прокси-метрика предстоящих OOM-отказов записи (алерт `redis_memory` при ≥ 0.8); `maxmemory` не задан — ряд не пишется |
| `crawler_backlog_domains` | gauge | `state` | scheduler (`pending`, `queued`) | суммы инкрементальных счётчиков зон (T-178: `sum(zones.domains_pending/queued)`, базовое множество `status <> deleted`) с периодом `scheduler.backlog_interval` (по умолчанию 5 мин) — `scheduler.BacklogLoop`: чтение справочника вместо прохода по 47 млн строк (ТЗИ §8.4) |
| `crawler_backlog_pass_seconds` / `crawler_backlog_skipped_ticks_total` | histogram / counter | — | scheduler (T-161) | каждый проход backlog: длительность (проход — суммы справочника, T-178) и пропущенные тики single-flight (проход дольше интервала — накопившийся тик не запускает новый проход, Warn в журнале); рост пропусков — поднимать `scheduler.backlog_interval` |
| `crawler_counters_recount_seconds` / `crawler_zonestats_skipped_ticks_total` | histogram / counter | — | monitor (T-161/T-178) | каждый проход совмещённой реконсиляции счётчиков (агрегаты domains/laravel_domains/laravel_history → zones + crawler_counters): длительность (корзины 0.5с…~8.5 мин — замер §12: десятки секунд на 47 млн строк) и пропущенные тики single-flight; `crawler_counters_recount_seconds` — переименование `crawler_zonestats_pass_seconds` (T-178) |
| `crawler_counters_drift` | gauge | `counter` | monitor (T-178) | дрейф инкрементальных счётчиков на проходе реконсиляции: \|старое − новое\| по позициям `zone:<id>:<колонка>` / `global:<имя>` / `daily:checks` (статусные строки вне дрейфа — пишутся одной реконсиляцией); стационар стремится к нулю (проход — три оператора под READ COMMITTED, единичный транзиентный дрейф под нагрузкой нормален), устойчивый ненулевой — пропущенный переход или ручной SQL мимо кода (Warn в журнале, разбор — `docs/runbooks.md`); исключение — `daily:checks` (T-179): позиция потоковая (события записи результата HTTP против доменов дня всех веток конвейера), ненулевая под живой нагрузкой — наблюдается gauge, в Warn не входит |
| `crawler_pg_slow_queries` | gauge | — | monitor (T-177) | проход цикла медленных запросов: клиентские операторы PostgreSQL, выполняемые дольше `monitor.slow_query_min_duration` на момент снимка `pg_stat_activity` (собственные агрегаты monitor исключены `application_name`); hold-last при сбое источника |
| `crawler_pg_slow_queries_total` | counter | `app` | monitor (T-177) | обнаруженные медленные запросы по `application_name` сервиса (новые операторы дольше порога, дебаунс pid+query_start); дублируют Telegram-сообщение метками Prometheus |
| `crawler_dashboard_snapshots_total` | counter | `result` | dashboard (`ok`/`error`, ТЗД §10) | каждый проход цикла сборки снимков: `error` — проход с деградированными секциями (снимок публикуется, причины — Warn в логе и карта `errors` /api/status) |
| `crawler_dashboard_snapshot_seconds` | histogram | — | dashboard | длительность того же прохода (корзины 50 мс…~3 мин: проход — полные агрегаты `internal/status`) |

Решения (зафиксированы):

- **Глубина очередей** живёт у потребителей стрима: backlog группы
  потребления (XINFO GROUPS + XLEN через гейт backpressure, клиент Redis
  у каждого сервиса уже есть; лаг группы ограничен длиной стрима —
  кламп против фантомного лага Redis 7.0.x после обрезки недоставленного,
  T-168), а привязка к потребителю гарантирует «очередь читается ⇔
  глубина публикуется». «Сырой» XLEN для этого не годится: он считает и
  подтверждённую историю до обрезки MAXLEN. Все читатели глубины
  (гейт, `crawlerctl status`, `crawler_queue_depth`, алерт `queue_frozen`
  T-162 и строка «📬 Очереди» отчёта нагрузки T-173 — с T-188) считают
  один и тот же бэклог общим расчётом `queue.GroupDepth`
  (backpressure.go): наблюдение и гейт не имеют права разойтись о том,
  есть ли работа — расхождение 25.10 (отчёт печатал сырой XLEN и
  завышал q:render в ~5 раз при остановленном TrimLoop) закрыто именно
  ею; стрим без групп и `q:dead` — XLEN.
  `q:dead` потребителя не имеет (ручной разбор, ТЗ 4.2) — с T-154 его
  глубину публикует сам monitor (XLEN через ту же обёртку гейта,
  `NewDeadDepthMetrics`, только ряд `queue="q:dead"`: дубль рабочих
  стримов из monitor удваивал бы ряды воркеров в PromQL), сверяясь с
  подсветкой дашборда (T-126/T-144) и алертом `queue_dead`;
  до появления наблюдателя глубину показывал только `crawlerctl
  status` (T-079).
- **Глубина backlog** (pending/queued из БД) — в scheduler: он владелец
  состояний `domains` (постановка T-028, возврат lease T-029,
  перепроверки T-030/T-031), пул БД и периодический цикл уже его.
  Длина стримов и backlog — разные метрики: Redis усечётся MAXLEN и
  теряется при сбое (восстановит планировщик, ТЗ 3.4), БД — полный
  объём невыполненной работы.
- **Память воркеров** — без собственных метрик: коллектор Go/runtime
  (T-076) уже отдаёт `go_memstats_*` (heap alloc/inuse/sys, GC),
  process-коллектор — `process_resident_memory_bytes` (RSS — именно
  его ограничивает systemd `MemoryMax`, ТЗ 3.2). Дублировать их
  `runtime.MemStats`-снимком нет смысла.
- Счётчики статусов/записей считаются по завершённому эпизоду
  (после успешной записи в БД до XACK, ТЗ 4.2): гонка с удалением
  домена (§13.1) не различается, повторные доставки at-least-once
  считаются честной работой (ТЗ 3.4).
- DNS-этап не типизирует ошибки обменов отдельным счётчиком: сбои
  резолвера сводятся к `temp_error` (T-036), счётчик финальных
  статусов и есть типизация итога; analyzer пассивен (ТЗ 5.8) — его
  результаты (findings, is_laravel) — данные БД, не метрики процесса.

## Рабочая логика сервисов

Все семь конвейерных сервисов (плюс публичный `web` этапа 13 — ниже)
реализованы и подключаются через `OnRun`/`OnDBPool`
(сервис без `OnRun` тоже валиден: `/livez`, `/healthz` и `/metrics`
подняты, он
живёт до SIGTERM/SIGINT и корректно останавливается — каркас не требует
рабочей логики).

`ingest` (этап 1, T-013…T-021): его
`OnRun` наблюдает `ingest.dir` через `internal/watch` и прогоняет готовые
файлы через конвейер приёма `internal/ingest` (разбор пути → зона →
разбор с sha256 → валидация → staging → применение → отчёт `ingest_files`
→ архив `processed/`/`failed/`), плюс фоновые очистки — удалённых доменов
(T-020) и осиротевших партий staging по возрасту `batch_id` (T-098),
одним циклом CleanupDeleted; контракт файлов и отчёты — в
[`incoming-files.md`](incoming-files.md). Итоги проходов очистки
экспортируются метрикой `crawler_ingest_bad_hashes_total` (T-099):
счётчик мусорных `content_hash` растёт только при ошибке писателя
результатов и читается алертом мониторинга (раздел об алертах ниже). `scheduler` (этап 2,
T-027…T-031): его `OnRun` держит главный цикл постановки пачек — выборка
pending с чередованием зон (`DispatchBatch`, T-027; круг 1 читает только
активные зоны с работой — фильтр `domains_pending > 0`, T-183: диспетчер
становится читателем инкрементальных счётчиков зон, ТЗИ §8 — семантика
колонки совпадает с предикатом LATERAL-выборки, проба пустой зоны и так
вернула бы 0 строк; квота раунда дозаполнения ограничена остатком пачки)
→ перевод в `queued`
с lease (`queued_at`) одним пакетным UPDATE с охраной pending (тем же
оператором снимается staging-бит `http_published` — T-108: каждый новый
эпизод начинается с чистой пометки публикации) и публикация
пачки `DNSBatch` в `q:dns` (`EnqueueLoop`/`EnqueueBatch`, T-028; порядок
«перевод → публикация» и гарантии — комментарии
`internal/scheduler/enqueue.go`), — фоновые циклы возврата доменов с
истёкшим lease (`LeaseWatch`, T-029), суточной перепроверки
(`RecheckDaily`, T-030 — не возвращает ядовитые строки `invalid`,
T-109) и повторных проверок `temp_error`
(`RetryTempErrors`, T-031; выборка пачки кандидатов — seq scan по
`domains` с ранним остановом `LIMIT` и закреплением плана `SET LOCAL` в
транзакции выборки: без закрепления планировщик садится на частичный
`domains_recheck_idx` и вырождает окно «старше 24 ч» в марафон — запрос
№1 журнала медленных 25.10, T-191; до выборки — guard точечным чтением
счётчика `crawler_counters['status_temp_error']` (ведёт реконсиляция
T-178): стационар без `temp_error` не трогает `domains` вовсе). Рядом
с ними — коллектор глубины backlog
`BacklogLoop` (T-077): gauge `crawler_backlog_domains{pending|queued}`
суммами инкрементальных счётчиков зон (T-178: справочник вместо
прохода по 47 млн строк, ТЗИ §8.4) с периодом
`scheduler.backlog_interval` (по умолчанию 5 мин; коридор 30с..1ч).
Проходы single-flight (T-161): выполняются в горутине цикла — новый не
начинается, пока не завершился предыдущий, а проход дольше интервала
сбрасывает накопившийся тик ticker'а, Warn и
`crawler_backlog_skipped_ticks_total` поднимают шум. Переводы
состояний (enqueue/lease/recheck/retry) ведут дельты счётчиков зон
`domains_pending`/`domains_queued`/`domains_checked` CTE того же
оператора (T-178): переход и дельта атомарны, точность восстанавливает
реконсиляция monitor.

`dns-worker` (этап 3, T-036…T-039): его `OnRun` выполняет
цикл `dnsworker.Run` (`internal/dnsworker/result.go`):

- потребление пачек `q:dns` группой потребителей `dns-worker` (по одной
  записи-пачке за чтение), обработка каждой доставки паттерном
  `queue.Processing` (T-025) — XACK только после записи результата в БД;
- на каждую доставку: перечитывается справочник `subdomains` (изменения
  `crawlerctl` подхватываются без рестарта), пачка фильтруется одним
  SELECT по актуальности (`check_state <> done`, `status <> deleted`) —
  повторная доставка обрабатывает только недоработанные домены
  (идемпотентность, ТЗ 3.4); поэтому проверка добавленного субдомена по
  уже проверенным доменам — перевод `done → pending` командой
  `crawlerctl subdomains recheck` (T-083): домен снова проходит конвейер,
  а DNS-этап берёт субдомены из свежего справочника;
- эпизод домена (параллельно до `dns.concurrency`): `CheckApex` (T-036)
  → субдомены `CheckSubdomains` и wildcard-детект
  `DetectWildcard`/`MarkWildcardSubdomains` (T-037/T-038) → решение
  (`DecideEpisode`): терминальные ветки `not_delegated`/`temp_error`/
  `delegated_no_address` пишут `domains.status`, найденные адреса —
  задача `q:http` с адресами и CNAME (без терминального статуса: итог
  `alive`/`no_http` ставит HTTP-этап);
- порядок «публикация `q:http` → запись `done`» (обоснование — в
  комментарии `internal/dnsworker/result.go`): сбой записи возвращает
  эпизод повторной доставкой, а инвариант «`done` ⇒ задача опубликована»
  делает фильтр актуальности страховкой от потери задачи. Единица
  повтора — домен, не пачка (T-108, DNS-04): сразу после успешной
  публикации задача помечается в строке домена битом `http_published`
  (`domains.flags`, снятие — записью результата и постановкой
  планировщика `pending → queued`); повторная доставка пачки по биту
  пропускает публикацию опубликованных доменов и доводит их до записи —
  дублей `q:http` (полная повторная HTTP-проверка) нет. Окно дубля
  сузилось с «сбой записи у любого домена пачки × попытки доставки»
  (до `redis.max_attempts × |пачка|` задач) до краха воркера между XADD
  и одним UPDATE пометки (или отказа БД в нём — пометка non-fatal);
- запись — одним оператором на домен (CTE, T-178): `dns_mask`, статус
  (терминальные ветки), `check_state = done`, `queued_at = NULL`,
  `last_checked_at`, сброс `temp_attempts` на не-`temp_error` итоге
  (контракт T-031); тем же оператором снимается бит `http_published`
  (T-108); охраны `status <> deleted` (§13.1) и `check_state <> done`
  (идемпотентность); CTE ведёт дельты счётчиков зоны по старому
  состоянию строки (главный переход конвейера queued/pending → done:
  предчтение `FOR UPDATE` → `queued −1`/`pending −1` + `checked +1`,
  ревью P1-1) — внешний `SELECT count(*)` отдаёт число переведённых;
- домен с пустым/пробельным именем в пачке — ядовитая строка
  (`ErrEmptyDomain`, мусорная/порченая строка `domains`): терминальный
  диагноз `invalid` (T-109) отдельным UPDATE (`check_state = done`,
  lease завершён) + Warn; остальные домены пачки обрабатываются,
  доставка подтверждается. Перепроверка (T-030) `invalid` не возвращает
  — строка не циклится `queued → lease → dispatch` бесконечно, разбор —
  ручной (runbook 2.4);
- обмены — цепь `dnsclient.FallbackResolver` (локальный `Client`
  с пулом UDP-сокетов, T-034, + резервные клиенты `dns.fallback_resolvers`,
  переключение при транспортном отказе, T-107) под `LimitResolver`
  (`dns.concurrency`/`dns.max_qps`, адаптив, `dns.ipv6`, T-035); старт
  сервиса — синтетическая проба цепи (fail-fast на всех мёртвых, Warn
  на частичном отказе, T-107); публикация `q:http` — под гейтом
  backpressure (`WatermarkPublisher`, T-026).

`http-worker` (этап 5, T-053): его `OnRun` выполняет цикл
`httpworker.Run` (`internal/httpworker/worker.go`):

- потребление `q:http` группой потребителей `http-worker` пачками до 16
  записей (`http.task_consume`) скользящим циклом (T-139): следующее
  чтение — при освобождении слота задачи (чтение идёт вперёд обработки
  не глубже пары пачек), до 192 доменных задач одновременно на экземпляр
  (`http.task_concurrency`; дефолт T-139 256 понижен до 192 по
  стабильности памяти — сутки на 256 слотах дали два фатала юнита,
  load-measurements.md §12, T-157, возврат 256 — по замеру после T-163;
  глобальный потолок соединений и RPS держит транспорт, T-049), каждая
  доставка — паттерном `queue.Processing`
  (T-025): XACK после полного цикла обработки домена — после сбора
  результата, сохранения контента (T-056), записи в БД и публикации
  `q:analyze` (T-058, хук `Options.OnResult`) — порядок «XACK только
  после записи в БД» (ТЗ 4.2);
- задача — домен целиком (T-024): каждый субдомен × (`https`, `http`)
  проверяется цепочкой редиректов `httpfetch.FetchChain` (T-048) на
  транспорте с лимитами (T-049), параллельно внутри задачи пулом 8
  проверок (`http.task_per_domain`; субдомены одного домена живут на
  одних и тех же IP — реальный параллелизм подключения к одному IP уже
  ограничен per-IP семафором 3–4, ТЗ 5.5); префиксы парковщиков могут
  получать повышенный лимит (`http.per_ip_overrides`, T-165:
  longest-prefix, наблюдаемость ожидания — метрикой ниже), head-of-line
  парковочных IP (находка T-092 §3) виден по гистограмме
  `crawler_http_per_ip_wait_seconds` — доля ожиданий > 2 с (корзина
  `le="2"`) против критерия < 5 % на смеси боя;
- dead-host cutoff (T-164, `http.dead_host_cutoff`): при включённом
  флаге проверки идут двумя волнами — apex и www первыми, и если оба
  упали на connect-фазе (connection refused или connect-таймаут по
  обоим протоколам — таймауты именно connect, не TLS/заголовки/общий;
  падение хоста редиректа не считается), остальные субдомены задачи не
  проверяются: записи — по §8 («нет ответа»: записей нет), статус
  домена решают выполненные проверки головы (refused → `no_http`,
  connect-таймауты → `temp_error` с перепроверкой T-031 — транзиентный
  ложный cutoff самозалечивается). Наблюдаемость: бит
  `dead_host_cutoff` в `domains.flags` (ставится/снимается записью
  результата последнего эпизода), счётчики
  `crawler_http_dead_host_cutoff_tasks_total` /
  `crawler_http_dead_host_cutoff_skipped_checks_total` и запись в
  журнале — контрольная выборка ложных cutoff (повторная полная
  проверка cutoff-доменов, критерий < 1 %) берётся по ним; выключенный
  флаг — одна волна пула, прежнее поведение;
- общий кэш «итоговый URL → результат» в пределах задачи домена
  (ТЗ 5.5 п. 6): итоговый URL каждой завершённой проверки кладётся в
  кэш, стартовый URL каждой следующей сверяется с ним — bare→www после
  редиректа на тот же URL страницу повторно не скачивает; ключ —
  схема+хост+путь без query/порта (согласован с критерием вариантов
  T-054: счётчиковые `?i=N` не разваливают дедуп; схема входит — http и
  https одного адреса это разные скачивания);
- тело читается с потолком, приводится к UTF-8 (T-051), от него
  вычисляются хэш `content.HashOf` (критерий вариантов/алиасов) и
  SPA-вердикт (T-057), переносятся заголовки/cookies итогового ответа;
  байты в памяти слота не удерживаются (T-147): каждая скачанная
  страница немедленно уходит ранним `Put` в `ContentStore`
  (`Options.Store`) — дедупликация алиасов идёт по хэшам, тела записям
  не нужны; в задаче остаются метаданные и ≤ `http.task_per_domain`
  буферов читаемых тел (память слота ≈1–1.5 MiB против ≈3.34 до
  T-147, замер T-092 §11); пустое тело (`Content-Length: 0`) —
  валидный контент: хэш пустых байтов, дедуп как у любой страницы,
  `content_hash` записи жив (T-138);
- память слота наблюдаема (T-163): `/metrics` (9104) экспортирует
  занятые слоты `crawler_http_tasks_inflight` (знаменатель «память
  юнита / слот» при замере критерия возврата 256), транзитные байты
  тел в обработке `crawler_http_body_bytes_inflight` (окно чтение →
  UTF-8 → хэш → SPA → ранний Put) и распределение размеров тел
  `crawler_http_body_bytes`; мягкий потолок кучи Go —
  `http.mem_limit` (сборка вызывает `debug.SetMemoryLimit`): у порога
  GC сам возвращает страницы ОС вместо reclaim-штормов ядра на
  MemoryHigh юнита и OOM-kill на MemoryMax (фаталы T-157);
- контент записывается ранним `Put` каждой страницы (T-147), сверка
  ссылок — `ContentSaver` в хуке `OnResult` до записи результата
  (T-056/T-058: паки T-042, словарь zstd T-043, `OpenPackStore` +
  `DiskWatchLoop` T-046 горутиной): записи-оригиналы классов алиасов
  наследуют свои ссылки `Put`, лишние (свёрнутые варианты, проигравшие
  классы, повторные скачивания) снимаются `Delete` — дедуп
  двухуровневая (§8): классы алиасов задачи хранят контент один раз,
  одинаковые страницы разных доменов — одним блоком по хэшу;
  переполнение диска и лимит `max_store_bytes` не роняют результат —
  запись с `content_hash = NULL` и признаком «контент не сохранён»
  (подробнее — `docs/content.md`);
- вариант-логика `DecideVariants` (T-054) и алиасы `DetectAliases`
  (T-055) собирают записи результата; ошибки субдоменов классифицируются
  (таймаут/лимит-цикл редиректов/TLS/тело/сеть, ТЗ §8) и не роняют
  домен — все субдомены молчат ⇒ записей нет;
- запись результата домена — `Recorder` (T-058, `record.go`) в том же
  хуке после `ContentSaver`: одной транзакцией `UPDATE domains` (статус
  `alive` при записях / `temp_error` при таймаутах / `no_http`, `done`,
  `queued_at = NULL`, `last_checked_at = now()`, охрана
  `status <> deleted` — §8/§13.1) → `DELETE` старых строк субдомен×вариант
  c `RETURNING content_hash` → −1 `refcount` каждого старого хэша
  (`SwapRefTx`, T-044; новые уже имеют +1 от `Put` T-056) → `INSERT`
  новых строк unnest-пачкой со всеми полями (§5.5 п. 7: адреса — 16
  октетов на адрес, IPv4 в v4-mapped форме, CNAME, цепочка редиректов,
  итоговый URL, TLS/заголовки/cookies — jsonb, время ответа, код ошибки,
  `rendered = false` — рендер T-067); после фиксации — публикация
  `q:analyze` по `Result`-сообщению на запись (§5.5 п. 9) под гейтом
  backpressure; публикация `q:render` по SPA-записям (T-066,
  `renderq.go`) — неблокирующая: ёмкость стрима проверяется до
  транзакции одним чтением глубины (`BackpressureGate.Len`:
  `min(lag, XLEN)` + `pending` групп — кламп фантомного лага T-168,
  XLEN — пока групп нет; порог — отдельный `render.queue_high_watermark`
  (T-174; 0 — общий `scheduler.queue_high_watermark`), эффективное
  значение — с клампом `min(watermark, MAXLEN/2)`, как у гейта
  публикации T-026, считает проводка `renderQueueWatermark`
  в `cmd/http-worker`),
  свободно — публикация сразу после фиксации мимо гейта, переполнение —
  флаг `render_pending` той же транзакцией результата (со сдвигом +1
  счётчика рендер-долга `crawler_counters['render_pending']`, T-175 —
  миграция 000015; охрана бита в UPDATE делает RowsAffected индикатором
  перехода: повторная доставка с уже стоящим флагом счётчик не
  наращивает), эпизод завершается без задержки XACK (§5.6: рендер не
  задерживает остальные этапы; §7: домен `done` после DNS/HTTP, рендер
  дополняет асинхронно); повторная доставка перезаписывает результат
  идемпотентно (§3.4);
- домены с флагом `render_pending` дорабатывает фоновый проход
  `RequeueRenderPending` (T-066, горутина в `OnRun`, раз в минуту,
  пачки по 100): повторный SPA-вердикт по сохранённому контенту записей
  (`DetectSPA` детерминирован — тот же вердикт, что при записи;
  алиасы наследуют вердикт оригинала), при свободной `q:render` —
  публикация `Result`-ключей и снятие флага с декрементом счётчика долга
  одной транзакцией (T-175; порядок «публикация → снятие» —
  at-least-once §3.4); выборка пачки — без PK-порядка, seq scan с ранним
  остановом LIMIT и закреплением плана `SET LOCAL` в транзакции выборки,
  стационар без долга не трогает `domains` вовсе — guard точечным чтением
  счётчика долга (T-181; детали — [`render.md`](render.md)); переполнение
  рендера не роняет и не задерживает DNS/HTTP-этапы.
- тем же `OnRun` запускается цикл retention контента
  (`content.RetentionLoop`, T-166, горутиной рядом с компактификацией
  T-045, раз в час `content.retention_interval`): записи
  `subdomain_results` старше `content.retention`, не Laravel-находки и
  не ждущие рендера теряют `content_hash` и тяжёлые jsonb одной
  транзакцией с уменьшением `contents.refcount` (паспорт строки
  остаётся); интерлок — глубина `q:analyze` у порога (гейт T-026):
  анализатор не успевает — проход пропускается; точная защита —
  записи, чьи сообщения ещё не потреблены `q:render`/`q:analyze`,
  исключаются из пачек по снимку стримов (адаптер над Redis-клиентом
  гейта, fix T-166/T-167; метрики `crawler_content_retention_*`);
  механика и критерии —
  [`content.md`](content.md) «Retention контента и трим не-находок».
- хосты редиректов резолвятся «через тот же резолвер» (ТЗ 5.5 п. 2):
  `dnsclient` с лимитами T-035 → адаптер `httpworker.NewDNSLookup` →
  кэш `CachingResolver` (T-047); A и AAAA опрашиваются параллельно с
  общим ctx — латентность резолва max(A, AAAA), не сумма, сбой/отказ
  одного типа не роняет резолв при живом другом, итог стабилен:
  адреса A, затем AAAA (T-101); TLS — `InsecureSkipVerify` с сохранением
  атрибутов сертификата (`TLSInfo`, T-050).

`render-worker` (этап 6, T-067): его `OnRun` поднимает менеджер
поколений Chromium (`render.NewManager` + `Manager.Start` — внешний по
`render.remote_url` или собственный процесс, T-061; перезапуски по
страницам/памяти и замена мёртвого поколения — T-065), хранилище
контента (то же, что у http-worker:
`OpenPackStore` + словарь zstd + `DiskWatchLoop`, T-042/T-043/T-046) и
цикл `renderworker.Run` (`internal/renderworker`):

- потребление `q:render` группой потребителей `render-worker` — скользящее
  (T-142, образец T-139 http-worker): следующее чтение происходит при
  освобождении слота доставки (пачка XREADGROUP до 16 записей + буфер
  предвыборки — чтение идёт вперёд обработки не глубже пары пачек),
  параллельно до max(4, `render.contexts`) доставок на экземпляр
  (сборка передаёт `max(DefaultConcurrency, render.contexts)` — слоты
  пула не простаивают при `contexts` > 4), каждая
  доставка — паттерном `queue.Processing` (T-025): XACK только после
  записи результата в БД и публикации `q:analyze`; Consume доставок
  подряд без единого подтверждения (систематический сбой обработки) —
  пауза-предохранитель чтения с удвоением (чтение новых доставок
  прекращается — не прочитанные не расходуют попытки; уже прочитанные
  остаются в PEL и вернутся клеймом по `redis.claim_min_idle`);
- задача — `Result`-ключ строки `subdomain_results` (T-024; публикуют
  HTTP-этап по SPA-вердиктам T-057/T-066 и проход доработки): итоговый
  URL читается из строки, страница рендерится в свежем изолированном
  профиле (блокировка ресурсов T-062, ожидание готовности T-063, съём
  DOM/cookies/заголовков T-064);
- DOM заменяет сырой HTML в хранилище (ТЗ 5.6, §5.7): `Put` в то же
  хранилище контента (+1 ссылке), затем одной транзакцией строка
  получает `rendered = true`, новую ссылку `content_hash`, итоговые
  URL/cookies/заголовки рендера и `rules_version = NULL` (контент
  сменился — прошлое заключение анализатора не отражает новый), а старая
  ссылка снимается `SwapRefTx` (T-044);
- после фиксации — публикация `q:analyze` тем же ключом (§7: рендер —
  асинхронное дополнение, анализ повторяется уже по итоговому DOM) под
  гейтом backpressure (`WatermarkPublisher`, T-026);
- идемпотентность повторной доставки (ТЗ 3.4): повторный рендер
  перезаписывает контент и refcount (баланс T-058); гонка с перепроверкой
  закрыта оптимистической охраной UPDATE по `content_hash` — изменившаяся
  строка не перезаписывается, ссылка `Put` снимается;
- недоступный Chromium (зонд `Manager.Alive`) — прекращение новых
  чтений (непрочитанные доставки остаются в стриме, не в PEL);
  прочитанные в буфер предвыборки дорабатываются и при неудаче
  возвращаются клеймом; мёртвое поколение зонд перезапускает сам
  (реконнект после рестарта Chromium), перезапуск браузера каждые N
  страниц и по памяти — T-065.

Скорость этапа диктует Chromium (слоты `render.contexts`, замер
T-092 §4: ≈5.1 с/страница; дефолт 8 контекстов, T-169, — потолок
≈1.6 стр/с против спроса ≈1.2 стр/с при свободном гейте, §12),
переполнение `q:render` HTTP-этап не блокирует: записи помечаются
`render_pending`
и дорабатываются проходом позже (ТЗ 5.6, T-066) — скользящее
потребление лишь не добавляет к узкому горлу браузера барьер окна
чтения (хвост страниц до `render.timeout` не держит слоты праздными).

`analyzer` (этап 7, T-071): его `OnRun` выполняет цикл
`analyzer.Run` (`internal/analyzer`; подробный конвейер — в
[`analyzer.md`](analyzer.md)):

- потребление `q:analyze` группой потребителей `analyzer` окном до 16
  записей, параллельно до 8 доставок на экземпляр, каждая — паттерном
  `queue.Processing` (T-025): XACK только после записи в БД;
- набор правил обязан загрузиться при старте (`rules.NewReloader` по
  `analyzer.rules_dir`, ошибка — код 1): без валидного набора analyzer
  молча не находил бы Laravel; SIGHUP (`make rules-reload`) перезагружает
  каталог на живом сервисе (`Options.OnSIGHUP` → `HandleSIGHUP`), каждая
  доставка берёт актуальный снапшот через `Set()`;
- задача — `Result`-ключ строки `subdomain_results` (T-024; публикуют
  HTTP-этап T-058 и render-worker T-067): вход анализа (заголовки/cookies
  jsonb, `content_hash`) читается из строки, контент — из того же
  хранилища PackStore (словарь zstd совпадает с пишущими этапами);
  `Set.EvaluateCtx` с `DefaultLimits` (T-069) — все срабатывания любого
  правила независимо от порога, `is_laravel = score ≥
  analyzer.laravel_min_score` (по умолчанию 0.6, §13.1);
- запись — одной транзакцией: `UPDATE subdomain_results.rules_version`
  (штамп версии набора для переанализа T-074; охрана `content_hash` и
  «домен не deleted» — 0 строк: успех без записи, стиль render-worker)
  → upsert `laravel_findings` (score, диапазон версий с NULL при
  конфликте, `signals` jsonb, `is_laravel`, `rules_version`) либо DELETE
  при пустом итоге (§3.5 перезапись) → пересчёт агрегата `laravel_domains`
  (T-072: максимальный score, объединённый диапазон версий,
  `found_on_mask` — бит № = `subdomains.id` у уверенных срабатываний;
  advisory-блокировка домена согласует параллельные доставки); при
  смене наличия строки и свежем эпизоде HTTP (`Result.Fresh` в задаче —
  ставят HTTP/рендер-этапы) той же транзакцией пишется журнал детекции
  `laravel_history` appeared/lost (T-118: переанализ по старому
  контенту историю не пишет — смена правил не факт о сайте); повторные
  доставки идемпотентны (ТЗ 3.4) — детали в
  [`analyzer.md`](analyzer.md);
- analyzer ничего не публикует (конец конвейера §7) — гейт backpressure
  не нужен, очередь читается и подтверждается напрямую.

`monitor` (этап 8, T-078/T-178): его `OnRun` выполняет цикл
`monitor.ZoneStatsLoop` (`internal/monitor/zonestats.go`) —
периодическую реконсиляцию инкрементальных счётчиков конвейера:

- один совмещённый проход раз в `monitor.zone_stats_interval` (по
  умолчанию 5 минут) — единственный полный проход `domains` в
  стационаре (T-178, замер §12: живые агрегаты зон/статусов/backlog и
  часовой проход истории входили в большую часть CPU postgres): в одной
  транзакции перезаписываются колонки зон `domains_total` /
  `domains_pending` / `domains_queued` / `domains_checked` /
  `domains_laravel` (агрегат `domains` FILTER по `check_state` GROUP BY
  `zone_id` — partitionwise, плюс агрегат `laravel_domains`) и строки
  глобальных скаляров `crawler_counters` (`status_<имя>` по легенде
  статусов, кумулятивы `laravel_appeared_total`/`laravel_lost_total`,
  `render_pending`), а также суточные строки проверок
  `checks:YYYY-MM-DD` и её temp_error-числителя
  `temp_error:YYYY-MM-DD` — FILTER-колонками `last_checked_at =
  current_date` (× `status = temp_error` у числителя) того же скана
  (T-179/T-182, цена нуля; имя дня вычисляет БД — отдельными upsert
  после скаляров, в порядке `checks:` → `temp_error:`); upsert строк
  создаёт их первый проход (суточные — и первые инкременты суток);
- между проходами счётчики ведут дельты переходов в транзакциях самих
  переходов (T-178: DNS-этап — главный переход queued/pending → done,
  CTE с `FOR UPDATE` в записи результата; scheduler enqueue/lease/
  recheck/retry — CTE того же оператора; ingest add/delete; CLI recheck
  и submodules recheck; запись результата http-worker поверх done —
  без дельт (предчтение `FOR UPDATE`); analyzer — `domains_laravel` ±1
  и инкремент кумулятивов; запись результата http-worker также
  инкрементирует суточный счётчик проверок `+1` на эпизод и его
  числитель `temp_error:` `+1` на temp_error-итог — T-179/T-182):
  откат транзакции откатывает пару. Суточные строки — семантика
  «темп работы»: инкременты считают события записи результата HTTP,
  живое окно — домены дня всех веток (DNS-терминальные завершения
  эпизода HTTP не имеют), повторные эпизоды дают +1 при живом нуле;
  числитель temp_error расходится с окном вдобавок ретраями, закрывшими
  ошибку (событие остаётся в истории). Позиции `daily:checks` /
  `daily:temp_error` потоковые (без стационарного нуля), в
  агрегированный Warn дрейфа не входят, наблюдаются gauge;
- дрейф |старое − новое| каждой позиции — gauge
  `crawler_counters_drift{counter}` (стационар стремится к нулю:
  проход — три оператора под READ COMMITTED, единичный транзиентный
  дрейф под живой нагрузкой нормален; статусные строки вне дрейфа —
  пишутся одной реконсиляцией) и Warn при ненулевом дрейфе (первый
  успешный проход цикла дрейгом не считается — заполняет нули после
  миграции/простоя); устойчивый дрейф — пропущенный переход или ручной
  SQL мимо кода, разбор — `docs/runbooks.md`, ручное восстановление —
  `crawlerctl counters recount|verify` (T-178);
- проходы single-flight (T-161): в горутине цикла новый проход не
  начинается, пока не завершился предыдущий, а проход дольше интервала
  (рост таблицы) сбрасывает накопившийся тик ticker'а — Warn,
  `crawler_counters_recount_seconds` (переименование
  `crawler_zonestats_pass_seconds`, T-178) и
  `crawler_zonestats_skipped_ticks_total` поднимают шум;
- семантика (ТЗ 11.4 п. 6 «всего / проверено»; базовое множество ВСЕХ
  колонок зон — `status <> deleted`, унификация T-178, ТЗИ §4.1):
  `domains_total` — живые домены зоны, `domains_pending` /
  `domains_queued` — ожидают выборки / в работе с lease, —
  `domains_checked` — проверенные в текущем цикле (`check_state =
  done`, ТЗ 6.1): возрастная перепроверка (ТЗ 5.3) переводит done →
  pending, и счётчики отслеживают прогресс нового цикла; все считаются
  по одному базовому множеству, поэтому `pending + queued + checked =
  total`;
- первый проход — сразу при старте (свежие счётчики после простоя и
  заполнение нулей после миграции), ошибка прохода — Warn, цикл живёт;
  отмена ctx — чистый nil (graceful shutdown, ТЗ 3.4). Счётчики читают
  `crawlerctl status` (T-079), дашборд (без JOIN domains, T-178),
  backlog-цикл scheduler (суммы справочника) и выбор размера партиций
  (`docs/schema.md`). Прогресс зон наблюдается алертами monitor (T-080,
  цикл ниже); счётчики очередей/backlog/этапов конвейера собирают сами
  сервисы (таблица выше, T-077).

Третий цикл того же сервиса — счётчик серой зоны Laravel-детекции
(`internal/monitor/nearstats.go`, T-115): gauge
`crawler_laravel_near_findings` — текущее число `laravel_findings` с
`analyzer.laravel_min_score/2 ≤ score < min_score` («почти-срабатывания»:
признаки есть, порог не пройден). Период — 1 ч (не 30–60 с пересчёта
зон): проход — агрегат по большим партициям `laravel_findings`, а
материал калибровки правил не требует минутной свежести; разбор тех же
строк поимённо — `crawlerctl near`. Стабильная серая зона — норма
(слабые признаки частых не-Laravel стеков), всплеск после смены правил —
сигнал деградации точности (материал T-116/T-117).

Четвёртый цикл — счётчики истории детекции
(`internal/monitor/historystats.go`, T-118): пара gauge
`crawler_laravel_appeared_total` / `crawler_laravel_lost_total` —
кумулятивные числа переходов из append-only журнала `laravel_history`
(появление/исчезновение строки `laravel_domains` по свежим эпизодам
HTTP). Значения монотонны (журнал append-only) — gauge с
counter-семантикой корректен для `rate()`/`increase()`; период — 1 ч,
как у серой зоны: тренды детекции — материал дашборда, не оперативный
сигнал конвейера.

Цикл медленных запросов PostgreSQL того же сервиса
(`internal/monitor/slowqueries.go`, T-177): раз в
`monitor.slow_query_interval` (по умолчанию 10 с) опрашивает
`pg_stat_activity` — клиентские операторы, выполняемые дольше
`monitor.slow_query_min_duration` (3 с — порог боевого лога
`log_min_duration_statement`, [db.md](db.md) §«Мониторинг»), уходят
одним сообщением в Telegram той же доставкой, что алерты (шапка с
числом новых и «всего в работе», три самых долгих строки
«длительность [сервис] pid: текст»; Warn в журнал и метрики
`crawler_pg_slow_queries` / `crawler_pg_slow_queries_total`). Дебаунс —
pid+query_start: оператор сообщается один раз при первом наблюдении,
завершившийся покидает базу; собственные агрегаты monitor (UPDATE по
47 млн строк каждые 5 мин, замер §12 — штатная нагрузка) исключены
`application_name` пула, метки сервисов в сообщении — оттуда же. Снимок
мгновенный, а не поток событий: эпизоды короче интервала цикла ловятся
частично, дольше — всегда; полный посмертный след — журнал сервера.
Для текста чужих сессий роли БД нужен `pg_read_all_stats` (deploy.md
§4.1) — без гранта сообщение честно помечает «текст недоступен».

Второй цикл того же сервиса — оценка алертов минимального набора ТЗ 5.9
(`internal/monitor/alerts.go`, T-080; ТЗ 8, ТЗ 10.3), период — тот же
`monitor.zone_stats_interval` (одна пара циклов мониторинга):

- **рост доли ошибок DNS** — доля `temp_error` среди проверок текущих
  суток по суточным строкам `crawler_counters` `temp_error:YYYY-MM-DD` /
  `checks:YYYY-MM-DD` — два точечных PK-чтения (T-182; `temp_error` —
  исход DNS-этапа после исчерпания ретраев, ТЗ 5.4; NXDOMAIN →
  `not_delegated` — валидный результат, не ошибка). Семантика — события
  записей результатов: ретраи и дубли доставки остаются в истории
  (доля устойчива к ним, абсолюты строк чинит проход реконсиляции);
  окно — сутки, имя дня вычисляет БД (`current_date`, точность дня у
  `last_checked_at` — ТЗ 6.1). Включение при доле ≥ 30 %, снятие ниже
  20 % (гистерезис); при < 100 событиях знаменателя доля — шум пустого
  окна, алерт «не измеряется». Прежний живой агрегат по окну суток
  (скан `domains` по `last_checked_at` с чтением `status` из кучи)
  убран: журнал медленных боя 24.10 фиксировал 33 оценки длиннее 3 с
  (медиана 27,7 с) — запрос №6, статус в индексе быть не может
  (HOT-инвариант, docs/schema.md);
- **переполнение очередей** — глубина рабочих стримов `q:dns`/`q:http`/
  `q:render`/`q:analyze` (backlog групп: `min(lag, XLEN)` + `pending` —
  лаг ограничен длиной стрима, кламп фантомного лага T-168;
  `BackpressureGate.Len`; `q:dead` гейтом не ограничивается и не
  проверяется) против эффективного порога стрима —
  `queue.StreamWatermark(scheduler.queue_high_watermark, redis.maxlen,
  стрим)`, не выше половины предела MAXLEN (T-141: `q:dns` — 125 пачек,
  штучные — 75 000 при дефолтах); тот же порог, что у гейта публикации
  (T-026) и подсветки очередей дашборда (`queue_watermarks`, T-144).
  Срабатывание при len ≥ порога — одновременно с закрытием гейта у
  стримов, публикуемых через него (исключение — `q:render`: ворота
  ёмкости там отдельные — `render.queue_high_watermark` с клампом
  MAXLEN/2 (T-174; 0 — общий), закрытие не ждёт — домен получает флаг
  `render_pending` (T-066); порог алерта для `q:render` остаётся общим
  (75 тыс. при дефолтах) и срабатывает позже закрытия ворот — сигнал,
  что дренаж долга не успевает); снятие ниже 90 % порога (гистерезис
  против дребезга длин у порога);
- **остановка прогресса** — `zones.updated_at` (пишет `ZoneStatsLoop`)
  старше 5 × `zone_stats_interval` (25 мин на дефолтных 5 мин): проход
  по десяткам млн строк — минуты, единичный пропуск периода — не авария.
  Оговорка самоконтроля: метку
  пишет сам monitor — полная смерть процесса видна по метрике
  `crawler_service_up` каркаса (T-012), а не этим алертом;
- **нехватка места на диске** — statfs каталога `content.dir` против
  `content.disk_free_percent`: та же формула, что пауза `Put` контента
  (T-046), — алерт срабатывает ровно тогда, когда запись контента
  останавливается (ТЗ 8);
- **износ NVMe (SMART)** — `percentage_used` из health-лога NVMe (ТЗ
  10.3): устройства обнаруживаются в sysfs, износ читается ioctl-ом
  admin-команды без nvme-cli; алерт при ≥ 90 % ресурса (значения >100 %
  — ресурс исчерпан — тоже), в detail — TBW устройства. В Docker-контейнере
  sysfs видит устройства хоста, но `/dev/nvme*` не проброшен — алерт
  честно «не измеряется» (Debug, без ложного срабатывания и без Warn):
  на боевом systemd-хосте слой включается доступом юнита к устройствам
  (группа `nvme-smart`, T-089) рядом с хостовым smartd —
  [`disks.md`](disks.md) «Мониторинг SMART и TBW»;
- **мусорные content_hash очистки** (T-099) — всплеск счётчика
  `crawler_ingest_bad_hashes_total`: зонд читает экспозицию `/metrics`
  сервиса ingest (`monitor.ingest_metrics_url`, тот же адрес, что
  скрейпит Prometheus) раз в период алертов, окно — 15 проходов
  (≈15 мин), порог — 10 новых мусорных строк. В норме инвариант писателя
  результатов держит 16 октет (ТЗ 6.1) и счётчик не растёт вовсе: рост —
  программная ошибка писателя (T-056), данные не теряются (refcount по
  мусорной ссылке не менялся, ING-P2-4); снятие — старением окна без
  продолжения потока. Ошибка зонда (ingest остановлен, метрики нет) —
  «не измеряется» с Warn: смерть самого ingest видна по
  `crawler_service_up`, не этим алертом;
- **OOM и рестарты юнитов crawler** (T-145) — счётчики memory.events
  cgroup v2 (`oom`/`oom_kill`) и `NRestarts` всех юнитов
  `crawler-*.service`: событие — незапланированный простой конвейера
  (ТЗ 3.4) и симптом невыполнения бюджета памяти юнита
  ([`deploy.md`](deploy.md) §2; мягкий потолок MemoryHigh http-worker —
  тот же T-145). Срабатывание при любом событии, один Warn на эпизод;
  эпизод — от события до сброса состояния юнита: `systemctl
  reset-failed`/новый старт обнуляет NRestarts (у живого юнита oom/
  oom_kill держатся до пересоздания cgroup перезапуском), деплой
  сбрасывает всё сам (stop → reset-failed → start, deploy-install.sh).
  Источник — `systemctl show 'crawler-*.service' -p Id -p NRestarts`
  (свойства менеджера читаются непривилегированно) + прямое чтение
  `/sys/fs/cgroup/system.slice/<юнит>/memory.events` (файлы cgroup
  world-readable, systemd не экспортирует их как D-Bus-свойства);
  обёртка источника зеркалит снимок в метрики
  `crawler_unit_oom_events`/`crawler_unit_restarts`. Ошибки источника
  делятся на классы: окружение без systemd (dev-контейнер) — тихое
  «не измеряется» (как SMART), боевой отказ (systemd/D-Bus/cgroupfs
  сломан) — Warn каждого прохода;
- **неразобранный q:dead** (T-154) — глубина карантина ненулевая
  3 прохода подряд (≈3 мин): «устойчиво ненулевой», а не миг единичной
  смерти — при живом разборе единичная смерть штатна; сработавший
  алерт держит любая ненулевая глубина, снятие — пустой карантин
  (разбор + XTRIM, runbooks §2). Источник глубины — тот же гейт, что у
  алерта очередей: потребителей у `q:dead` нет, Len читает XLEN;
  обёртка `NewDeadDepthMetrics` заодно зеркалит её в
  `crawler_queue_depth{queue="q:dead"}`. Согласован с подсветкой q:dead
  дашборда (T-126/T-144: глубина > 0) и счётчиком смертей
  `crawler_queue_dead_buried_total`: счётчик — темп (rate) смертей,
  глубина — сток; эпизод 22.10 (5507 смертей,
  docs/load-measurements.md §11.1) растянулся клеймом на часы — сток
  держит сигнал всё это время, темп виден ретроспективно;
- **память Redis** (T-154) — `used_memory/maxmemory` из `INFO memory`
  (источник `RedisInfo` того же клиента, что у гейта глубин; обёртка
  `NewRedisMemoryMetrics` зеркалит `crawler_redis_memory_ratio`/
  `_bytes`) ≥ 80 % — срабатывание, < 75 % — снятие (гистерезис):
  при noeviction (политика бюджета, ТЗ 3.2) заполнение maxmemory —
  отказ записи XADD («OOM command not allowed», инциденты T-093/22.10),
  прямого счётчика отказов `INFO` не отдаёт — доля с запасом 20 %
  (межобрезочные остатки TrimLoop, фрагментация, буферы) — прокси
  предстоящих отказов. `maxmemory` 0 (потолка нет) — «не измеряется»:
  отношение не определено;
- **абсолютный минимум места на диске** (T-162) — свободные байты
  каталога контента против `monitor.alerts.disk_free_min_bytes`
  (15 GiB, Warn) и `disk_free_crit_bytes` (10 GiB, эскалация отдельным
  алертом `disk_free_crit`): процентный порог
  `content.disk_free_percent` на большом томе поздний — 10 % от 79 ГБ
  это 8 ГБ ≈ 5 ч до заполнения при темпе ≈36 GiB/сут (замер §12),
  абсолютный минимум даёт запас на разбор независимо от размера тома.
  Срабатывание строгое (<), гистерезиса нет — место при инциденте
  убывает монотонно, дребезг у порога нечему двигать (как у
  процентного);
- **троттлинг MemoryHigh** (T-162, алерт `memory_high`) — рост счётчика
  `memory.events high` юнита между проходами (источник — тот же
  `systemctl show` + cgroup v2, что у unit_oom; обёртка зеркалит
  `crawler_unit_memory_high_events`): каждое попадание за мягкий
  потолок — принудительный reclaim/задержка, счётчик накапливается за
  жизнь cgroup (http-worker с MemoryHigh=850M набирает >100 тыс. за
  сутки) — порог не значение, а рост. Устойчивость — рост
  `monitor.alerts.mem_high_passes` проходов подряд (2 × 5 мин = 10 мин):
  одиночный всплеск — штатная работа мягкого потолка, устойчивый
  троттлинг — предвестник фаталов http-worker (T-157: рестарты по памяти
  до OOM-kill) и запрос пересмотра бюджета (deploy.md §2). Снятие —
  первый проход без роста;
- **замороженная очередь** (T-162, алерт `queue_frozen`) —
  last-delivered-id группы рабочего стрима не движется
  ≥ `monitor.alerts.queue_freeze_window` (15 мин) при глубине > 0:
  живой потребитель двигает ID каждым XREADGROUP (пачки в секунды,
  ТЗ 3.3), стоящий ID при непустом backlog — записи не доставляются
  вовсе: потребитель мёртв или валится на первой записи (яд T-159).
  Источник — XINFO GROUPS + XLEN того же клиента (`RedisStreamGroups`),
  глубина группы — общий расчёт с гейтом (`queue.GroupDepth`:
  `min(lag, XLEN)` + pending, кламп T-168): застрявший фантомный лаг
  инцидента 22–23.10 (гейт закрыт
  15,5 ч при 40,3 млн pending) этот алерт поймал бы сам фактом
  остановки доставок. Ошибка источника память трекинга не трогает;
  первая запись группы живёт только при глубине > 0 — простой пустой
  очереди «время без доставок» не копит;
- **PSI CPU** (T-162, алерт `psi_cpu`) — some avg300 из
  `/proc/pressure/cpu` хоста (источник `OSPSI`, обёртка зеркалит
  `crawler_psi_cpu_some_percent`) выше
  `monitor.alerts.psi_cpu_percent` (90 %) устойчиво
  `monitor.alerts.psi_cpu_window` (30 мин): some — задержка хотя бы
  одной задачи (утилизация 100 % без очереди — норма CPU-bound
  конвейера), замер §12 — рабочий голод планировщика 91–95 %, на
  котором тонут все этапы разом (T-158 понизил вес CPU monitor'а —
  собственные проходы голод не приближают). Окно отбрасывает разовые
  шпильки (деплой, ретро-пачки); ядро без PSI — тихое
  «не измеряется» (как SMART в контейнере);
- **накопление рендер-долга** (T-175, алерт `render_debt`) — долг
  (домены с флагом `render_pending` по счётчику
  `crawler_counters['render_pending']`, миграция 000015, + лаг стрима
  `q:render`; источник — пул БД и гейт глубин, те же, что у алертов
  DNS/очередей) выше `monitor.alerts.render_debt_domains` (50 тыс.) И
  растёт `monitor.alerts.render_debt_passes` проходов подряд
  (2 × 5 мин = 10 мин): очередь переполнена и приток SPA обгоняет
  дренаж — алерт `queue_frozen` (T-162) такой случай не ловит, доставки
  идут; долг 443 тыс. морозки 24.10 нашли ручным SQL, этот алерт делает
  накопление видимым (проходы серии — период цикла алертов
  `zone_stats_interval`, по умолчанию 2 × 5 мин = 10 мин).
  Срабатывание привязано к росту, снятие — к уровню: сработавший алерт
  держится, пока долг не стечёт ниже 90 % порога (гистерезис; защёлка
  трекинга — перерыв источника «не измеряется» эпизод не рвёт, ревью
  T-175) — долг это уровень (флаги + лаг), лаг колеблется на
  сотни-тысячи за проход при дренаже ~1–2 стр/с, снятие «по не-росту»
  качало бы эпизод 🚨/✅ на стационарном высоком долге. Триггер роста —
  любая положительная дельта (минимальной величины нет: темп
  накопления инцидента ~600/проход сопоставим с шумом лага — порог
  величины не разделяет сигнал и шум; долг выше порога легитимно
  держит эпизод до дренажа, момент начала меняют только колебания);
  стагнация без серии роста не будит оператора — её видно строкой
  «Рендер-долг» отчёта нагрузки T-173 и gauge
  `crawler_render_pending_domains`, первый проход после рестарта —
  база. Отсутствие строки счётчика (ручная правка таблицы) — «не
  измеряется» с Warn, а не тихий ноль.

Доставка алертов оператору (T-162, ТЗ 5.9/10.4): переходы состояний
(срабатывание/снятие) уходят в Telegram тем же ботом и группой, что
извещения CI/CD — monitor зовёт exec `telegram-notify.sh --message`
(хук `AlertsOptions.OnPass`, `TelegramNotifier` в notify.go), секреты
`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` читает из EnvironmentFile
юнита. Дебаунс — эпизод алерта доставляется ровно двумя сообщениями:
🚨 при первом срабатывании, «✅ Восстановлено» при снятии; повторные
проходы, возврат в firing после перерыва источника и первый проход
(рестарт monitor) — тишина. Нет секретов или скрипта — доставка
выключена, алерты остаются Warn в journald (настройка — operations.md
§4.3, скрипт — ci-cd.md §«Извещения Telegram»).

Сигнал фронтовой (на переходах, стиль `DiskWatchLoop` в internal/content):
Warn при срабатывании, Info при снятии; «не измеряется» — Debug. Ошибки
источников (БД, Redis, statfs, зонд ingest, группы стримов) — Warn
каждого прохода, цикл живёт; исключение — источники, чья недоступность
ожидаема в части окружений (SMART без `/dev/nvme*` в контейнере,
события юнитов systemd в dev-контейнере, T-145; PSI на ядре без
CONFIG_PSI, T-162) — тихая деградация «не измеряется». Состояние
прохода — снапшот `AlertsResult` (ок/сработал/не измеряется +
детали) в хук `AlertsOptions.OnPass`: лог + поле, собственный
gauge-реестр в monitor не заводится — метрики алертов в Prometheus заведёт
слой T-077 поверх `internal/metrics`, чтобы не дублировать реестры и метки.

`web` (этап 13, T-121/T-122; продуктовое расширение вне основного ТЗ —
перенос из larasites): публичный лендинг. Каркас — `cmd/web`
(`config.Load` → `service.Run`, пул через `OnDBPool`, метрики/проб —
loopback `127.0.0.1:9107`); публичный слушатель `web.listen` —
собственный `http.Server` внутри OnRun с graceful stop по отмене ctx:
каркас даёт один HTTP-порт на сервис и вешает туда `/metrics`,
выставлять который наружу нельзя (ТЗ §9) — прецедент управления
жизненным циклом внутри OnRun. Роуты: `GET /` — редирект на дефолтную
локаль `/ru`, `GET /{ru|en}` — лендинг (T-122: перенос разметки
larasites — hero с SVG-радаром, карточки статистики со счётчиками
count-up, облако зон топ-50 с лог-масштабом и коллизиями, таблица зон
топ-10 с сортировкой, how-it-works, форма ручной проверки, последние
находки с бейджами субдоменов и диапазоном версий, контакты/Telegram;
SEO canonical/hreflang/OG/Twitter, счётчик Яндекс.Метрики), данные —
кэши T-120 + laravel_history («находки за 24 ч», T-118), `POST
/api/check-domain` — живая проверка из формы (T-123), `GET /healthz` —
readiness ping PostgreSQL (503), `GET /robots.txt` и
`GET /sitemap.xml` — служебные файлы поисковых роботов (T-124:
robots — статика из embed, sitemap — минимальный набор локалей,
абсолютные URL собираются из запроса — адрес публикации в конфиге
не хранится), `GET /static/*` — статика (go:embed). Шаблоны —
`html/template` + `go:embed`, новых зависимостей нет.

CSS лендинга — Tailwind v4 без node: исходник
`internal/web/static/app.src.css` (классы сканируются `@source`-ом
шаблона), собранный бандл в git не хранится (`.gitignore` — свежий
клон даёт страницу без стилей, не 500). Сборка: стадия `css`
в `deploy/docker/Dockerfile.go` (alpine + standalone-CLI tailwindcss,
версия — `TAILWINDCSS_VERSION` в `.env`; генерат кладётся в
build-стадии до `go build` — go:embed встраивает настоящий CSS) или
`make web-css` для dev-просмотра (одноразовый docker run).

Конфигурация — секция `web` (`listen`/`rate_limit`/`timeout`,
[`configuration.md`](configuration.md)). Развёртывание (T-124):
dev — compose-сервис `web` профиля app (порт лендинга публикуется на
`127.0.0.1` хоста, `WEB_PORT`); бой — юнит `crawler-web.service`
(docs/deploy.md) за reverse-proxy с TLS (первый образец —
`deploy/nginx/`).

Подробности браузерного слоя (изоляция, блокировка ресурсов, ожидание,
съём результата) — [`render.md`](render.md); live-проверки против
compose-сервиса `chromium` — флагом `CRAWLER_RENDER_TEST=1`.

`dashboard` (этап 15, ТЗД; T-128 — каркас и авторизация): внутренний
операторский дашборд конвейера — единая страница состояния «одним
взглядом» (прогресс, скорость, очереди, находки, здоровье сервисов),
строго read-only. Каркас — `cmd/dashboard` (`config.Load` →
`service.Run`, пул через `OnDBPool`, метрики/пробы — loopback
`127.0.0.1:9108`, ТЗД §4.2); публичный слушатель `dashboard.listen` —
собственный `http.Server` внутри OnRun с graceful stop по отмене ctx
(прецедент `cmd/web`, T-121) и таймаутами ТЗД §6.3 (ReadHeader 5 с,
Read 10 с, Write 15 с, Idle 60 с). Стартовая проверка кредов (ТЗД
§7.7): пустые `dashboard.username`/`password_hash` (или неразборимый
хэш) — отказ запуска до миграций и слушателей; общий `config.Validate`
креды не проверяет (иначе требование коснулось бы всех сервисов).

Роуты (ТЗД §6.1): `GET /` (страница), `GET /assets/*` (статика) и
`GET /api/status` (JSON последнего снимка, T-129; до первого успешного
сбора — 503 `snapshot_not_ready`, §5.9) — за авторизацией;
`GET /healthz` (ping PostgreSQL, 503 при сбое), `GET /livez`,
`GET /robots.txt` (`Disallow: /` — внутренний сервис) — без
авторизации. Не GET/HEAD — 405; все ответы несут
`X-Content-Type-Options: nosniff` и CSP ТЗД §6.3.

Авторизация (ТЗД §7, T-128): HTTP Basic — имя constant-time, пароль —
PBKDF2-сверка формата T-127 (`internal/dashboard/auth`,
`subtle.ConstantTimeCompare`, §7.4). После успешной сверки выдаётся
cookie-сессия `<expiry>.<HMAC-SHA256(expiry, производный ключ KDF)>`
(§7.5): HttpOnly, SameSite=Lax, 24 ч; быстрая проверка подписи снимает
стоимость KDF (полсекунды на 600 000 итераций) с периодического опроса;
ключом служит ключ хэша — смена `password_hash` инвалидирует все
сессии, отдельный серверный секрет не нужен. Per-IP защита от подбора
(§7.6): 5 неудачных сверок в течение минуты — 429 + `Retry-After` до
конца окна (in-memory, по образцу токен-бакета T-123; успешная сверка
сбрасывает окно IP); валидная cookie проверяется до окна — оператор с
живой сессией не заперт чужими неудачами со своего адреса. Неудачные
сверки — Warn с IP-адресом, без кредов (ТЗД §9.2). За reverse-proxy
адрес клиента несёт `X-Real-IP` (доверие — только от loopback-пира,
как у формы лендинга T-123).

Сборщик снимков (T-129, ТЗД §4.3–4.4): фоновый цикл
`dashboard.Collector.Loop` (`internal/dashboard/collector.go`) рядом с
публичным слушателем в `OnRun` — первый сбор сразу при старте, далее с
периодом `dashboard.refresh` (коридор 1 с…5 мин, дефолт 5 с); снимок
атомарно заменяет предыдущий (`atomic.Pointer`), `/api/status` отдаёт
последний готовый — нагрузка на БД фиксирована частотой refresh и не
зависит от числа вкладок (ТЗД §3.2.3). До первого **успешного** сбора —
прохода, в котором жив каркас сводки (зоны/итоги, §5.1) — `/api/status`
отвечает 503 `snapshot_not_ready` (§5.9). Иерархия готовности — как у
`crawlerctl status`: Redis, лежащий с самого старта, не прячет сводку
по БД (очереди «н/д» обслуживаются с первого прохода — приёмка §12.3
работает и для рестарта при уже лежащем Redis), лежащая БД держит 503
и метрику `result=error` до восстановления. Позже деградированные
снимки обслуживаются: замена атомарная, отказ секции не роняет выдачу.

Цена прохода — выборки `internal/status` (те же, что платит
`crawlerctl status` на каждый запуск: сводка зон и распределение
статусов — точечные чтения справочника и `crawler_counters` (T-178,
без прохода `domains`); темп — суточный счётчик проверок
`checks:YYYY-MM-DD` точечным лукапом (T-179; прежде живой подсчёт
`last_checked_at = current_date` деградировал heap-fetch'ами свежих
страниц — запрос №1 журнала медленных 24.10) и дешёвые часовые окна
`subdomain_results` по индексу; окно 24 ч Laravel — по индексу
`laravel_domains_updated_at_idx`; «последние ingest-файлы» — top-N
сортировка журнала). Тяжёлые секции (зоны, темп, статусы) проходят
через TTL-кэш `status.Cache` (T-161,
`dashboard.status_cache_ttl`, по умолчанию 60 с — коридор 1–5 мин):
в окне TTL проходы `refresh` переиспользуют последний ответ БД;
ошибки не кэшируются — деградация секции остаётся видимой. Лёгкие
секции (последние находки по индексу, журнал ingest) кэш обходит.

Секции снимка (§5): зоны/итоги/темп/скорости/ETA и глубины стримов —
общая библиотека `internal/status` (те же SQL и расчёты, что у
`crawlerctl status`, T-126; Redis — `BackpressureGate.Len`, как monitor;
клиент Redis сервис создаёт сам по секции `redis` конфигурации — как
`cmd/monitor`); распределение `domains` по `status` (§5.5),
Laravel-находки — всего/за 24 ч/последние 10 с JOIN зон по индексу
`laravel_domains_updated_at_idx` (§5.6) и последние 10 `ingest_files`
(§5.7) — тоже `internal/status` (один источник SQL сводки, §4.5).
JSON-форма `/api/status` (§6.2): общие секции — канонический
wire-контракт `status.SummaryJSON` (его же отдаёт `crawlerctl status
--json` — поля не расходятся), плюс `statuses`/`laravel`/`ingest`/
`health`/`errors`/`built_at` и `queue_watermarks` — эффективные пороги
backpressure по рабочим стримам (§5.4; с T-144 вместо скалярного
`queue_watermark`, оставлявшего порог без клампа): для каждого стрима
без `q:dead` — `queue.StreamWatermark(scheduler.queue_high_watermark,
redis.maxlen, стрим)`, тот же единый расчёт, что у гейта публикации
(T-026) и алерта переполнения очередей монитора (T-080): порог стрима
не выше половины его предела MAXLEN (T-141: при дефолтах `q:dns` —
125 пачек, штучные — 75 000; 0 — гейт отключён) — данные конфигурации,
не деградируют. Для `q:render` это порог алерта/подсветки: ворота
ёмкости рендера — отдельный `render.queue_high_watermark` (T-174,
дефолт 10 тыс. — закрываются раньше подсветки; наблюдаемость
рендер-долга — T-175). Недоступные секции — `null`, причины — в карте
`errors`; `Cache-Control: no-store`.

Деградация (§5.9): секции собираются независимо — ошибка БД, Redis или
пробы не роняет остальные; метка `result=error` у
`crawler_dashboard_snapshots_total` и Warn с картой причин поднимают
шум, цикл живёт. Пробы `/livez` (§5.8) из `dashboard.probes` идут
параллельно горутинами с таймаутом 2 с (errgroup не использован —
`golang.org/x/sync` в go.mod косвенная, новых зависимостей ТЗД §3.2.1
не вводит); недоступный/медленный сервис — данные (`up:false` с
причиной), а не ошибка секции; пустая карта проб — секция «н/д».

Метрики собственного сервиса (§10): `crawler_dashboard_snapshots_total{result}`
и `crawler_dashboard_snapshot_seconds` — реестр создаёт `cmd/dashboard`
(таблица выше).

Фронт (T-130): страница `/` и статика `/assets/*` — артефакты
`go:embed` (`internal/dashboard/assets/`: `page.html`, `app.js`,
`style.css`), без внешних CDN и шрифтов (ТЗД §3.2.2 — окружение без
интернета; CSP §6.3 пускает только свои скрипты, стили и соединения);
рендер целиком клиентский, сервер HTML не шаблонизирует. Content-Type
статики задан явной картой (не системным mime-файлом — его окружение
меняет `.js` на `text/javascript`), имя вне карты — 404. Разметка (ru,
§6.5): карточки итогов и скоростей — всего/проверено %/в работе
(pending+queued)/дом. в с (окно суток)/HTTP-рез. в с и рендеры в с
(окно часа)/ETA цикла/Laravel-находок всего и за 24 ч (§5.1–5.2);
очереди — пять стримов с историей-спарклайном и подсветкой по каждому
рабочему стриму (без `q:dead` — мёртвые задачи не отставание конвейера)
против его эффективного порога из `queue_watermarks` — приближение
от 90 % и превышение, как алерт `queue_overflow` монитора (T-144:
после клампа по MAXLEN пороги стримов различны, а глубины — в разных
единицах: q:dns считается пачками; сумма глубин против одного порога
не осмысленна), а также ненулевого `q:dead` (§5.4); таблица зон с
прогресс-барами,
активностью и pending/queued/done (§5.3); распределение статусов —
полоса с легендой-счётчиками по легенде ТЗ 6.2 (§5.5); последние 10
Laravel-находок — домен, зона, score, диапазон версий, обновление
(§5.6); последние 10 ingest-файлов с счётчиками строк (§5.7); чипы
здоровья сервисов up/down с причиной в подсказке (§5.8). Деградация
(§5.9): секция с ошибкой в снимке — «н/д» с причиной (подсказка),
значения без окна (скорость/ETA) — тоже «н/д»; 503 до первого снимка —
индикатор «снимок готовится…». `app.js` опрашивает `/api/status` каждые
~5 с (период — `meta[name=dashboard-refresh]` страницы, может отличаться
от `dashboard.refresh` сервера — клиент показывает последний готовый
снимок), историю скоростей и глубин держит в памяти (~120 точек ≈ 10 мин)
и рисует спарклайны SVG polyline; 401 — остановка опроса и перезагрузка
страницы: `fetch` не показывает браузерный Basic-диалог, а повторный
запрос самого документа получает `WWW-Authenticate` — браузер предлагает
повторный вход или молча переотправляет кэшированные креды (§6.4).
Развёртывание (T-131, ТЗД §11): dev — compose-сервис `dashboard`
профиля app (клон go-common: `SERVICE=dashboard`, `METRICS_PORT=9108`,
`METRICS_ADDR=:9108` в docker-сети для Prometheus; порт публикуется на
`127.0.0.1:8090` хоста — `DASHBOARD_PORT`; `mem_limit` 256m; креды и
пробы — секция `dashboard` в `deploy/docker/config.dev.yaml`,
dev-пароль — docs/dev-environment.md); бой — юнит
`crawler-dashboard.service` (User=`crawler-dashboard`, Group=`crawler`,
`CRAWLER_DASHBOARD_LISTEN=127.0.0.1:8090`, `MemoryMax=128M`,
docs/deploy.md) за reverse-proxy с TLS (ТЗД §9.1); Prometheus — job
`dashboard` на :9108 (`deploy/docker/prometheus/prometheus.yml`).
Конфигурация — секция `dashboard`
([`configuration.md`](configuration.md)).
