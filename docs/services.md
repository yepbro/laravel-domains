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
      → HTTP /healthz + /metrics (Options.MetricsAddr)
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

Каждый сервис получает собственный реестр Prometheus (`internal/metrics`)
с меткой `service`: базовые `crawler_service_up` и `crawler_build_info`,
коллекторы Go/runtime (uptime выводится из `process_start_time_seconds`).
Метрики этапов добавляются в этот же реестр задачами этапов (T-076/T-077).

## Поведение заглушек

Пока рабочая логика не реализована (до этапов 2–7), сервисы запускаются
без `OnRun`/`OnDBPool`: подняты `/healthz` и `/metrics`, сервис живёт до
SIGTERM/SIGINT и корректно останавливается — healthcheck в compose остаётся
зелёным. Исключение — `ingest` (этап 1 завершён, T-013…T-021): его
`OnRun` наблюдает `ingest.dir` через `internal/watch` и прогоняет готовые
файлы через конвейер приёма `internal/ingest` (разбор пути → зона →
разбор с sha256 → валидация → staging → применение → отчёт `ingest_files`
→ архив `processed/`/`failed/`), плюс фоновая очистка удалённых доменов
(CleanupDeleted, T-020); поведение каталогов и отчёты — в
[`dev-environment.md`](dev-environment.md). И `scheduler` (этап 2,
T-027…T-031): его `OnRun` держит главный цикл постановки пачек — выборка
pending с чередованием зон (`DispatchBatch`, T-027) → перевод в `queued`
с lease (`queued_at`) одним пакетным UPDATE с охраной pending и публикация
пачки `DNSBatch` в `q:dns` (`EnqueueLoop`/`EnqueueBatch`, T-028; порядок
«перевод → публикация» и гарантии — комментарии
`internal/scheduler/enqueue.go`), — и фоновые циклы суточной перепроверки
(`RecheckDaily`, T-030) и повторных проверок `temp_error`
(`RetryTempErrors`, T-031); возврат доменов с истёкшим lease подключается
задачей T-029.

И `dns-worker` (этап 3, T-036…T-039) — не заглушка: его `OnRun` выполняет
цикл `dnsworker.Run` (`internal/dnsworker/result.go`):

- потребление пачек `q:dns` группой потребителей `dns-worker` (по одной
  записи-пачке за чтение), обработка каждой доставки паттерном
  `queue.Processing` (T-025) — XACK только после записи результата в БД;
- на каждую доставку: перечитывается справочник `subdomains` (изменения
  `crawlerctl` подхватываются без рестарта), пачка фильтруется одним
  SELECT по актуальности (`check_state <> done`, `status <> deleted`) —
  повторная доставка обрабатывает только недоработанные домены
  (идемпотентность, ТЗ 3.4);
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
  делает фильтр актуальности страховкой от потери задачи;
- запись — одним UPDATE на домен: `dns_mask`, статус (терминальные
  ветки), `check_state = done`, `queued_at = NULL`, `last_checked_at`,
  сброс `temp_attempts` на не-`temp_error` итоге (контракт T-031);
  охраны `status <> deleted` (§13.1) и `check_state <> done`
  (идемпотентность);
- обмены — `dnsclient.Client` (пул UDP-сокетов, T-034) под
  `LimitResolver` (`dns.concurrency`/`dns.max_qps`, адаптив, `dns.ipv6`,
  T-035); публикация `q:http` — под гейтом backpressure
  (`WatermarkPublisher`, T-026).
