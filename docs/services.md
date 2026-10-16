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
