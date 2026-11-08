// Сервис render-worker (ТЗ 5.6, §5.7, §7; T-067): этап рендера.
// Потребляет задачи стрима потребления (дефолт q:render; Result — ключ
// строки subdomain_results,
// T-024; публикуют HTTP-этап при SPA-вердиктах T-057/T-066 и проход
// доработки renderq.go) группой потребителей; итоговый URL читается из
// строки, страница рендерится браузером Chromium под менеджером поколений
// (internal/render.Manager, T-065: один браузер T-061, блокировка ресурсов
// T-062, ожидание готовности T-063, съём DOM/cookies/заголовков T-064,
// перезапуск по страницам/памяти и реконнект после потери Chromium),
// DOM заменяет сырой HTML в хранилище контента (ContentStore.Put,
// T-056/T-042), строка получает rendered = true, итоговые
// URL/cookies/заголовки рендера и новую ссылку на контент одной
// транзакцией со свопом refcount старого (SwapRefTx, T-044); после
// фиксации запись публикуется в q:analyze на переанализ уже итогового
// контента (§7). Подтверждение q:render — паттерном Processing (XACK
// только после записи в БД, T-025); повторные доставки идемпотентны
// (перезапись, ТЗ 3.4). Публикация q:analyze — под гейтом backpressure
// (WatermarkPublisher, T-026). При включённом render.fallback.enabled
// (T-238) движко-зависимые финализации (тарпит T-148, класс задачи
// T-172) публикуются в q:render-fallback вместо маркера — доработка
// chromium-воркером B (ТЗР этапа 35 §4.4). Воркер B — этот же бинарь
// другой сборкой (T-240, ФТ-12/§4.5): RENDER_WORKER_STREAM=q:render-
// fallback переключает стрим потребления и группу (render-fallback-
// worker), темп — render.fallback.worker_rate за скользящее окно
// renderworker.RateWindow (ФТ-13); развёртывание — docs/deploy.md §2.3.
// Перезапуск браузера — менеджер T-065;
// при недоступном Chromium цикл лишь приостанавливает потребление
// (зонд Manager.Alive — реконнект и перезапуск делает сам зонд).
// Каркас — internal/service: миграции, пул БД (OnDBPool), пробы /livez + /healthz
// и /metrics, graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[render-worker] конфигурация: %v", err)
	}
	logger := service.NewLogger("render-worker", cfg)

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate);
	// им пользуется и хранилище контента (строки contents, T-042), и
	// замена ссылки в subdomain_results (T-067). Реестр метрик — здесь
	// же (T-077): приборы этапа рендера и коллектор длины q:render
	// получают его до старта OnRun, каркас отдаёт тот же реестр через
	// /metrics.
	reg := metrics.New("render-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "render-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9105"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика render-worker: Chromium под менеджером поколений
// (T-065: внешний по render.remote_url или собственный процесс T-061,
// перезапуски по страницам/памяти и замена мёртвого поколения; контексты
// задач — изолированные профили) → хранилище контента (то же, что у
// HTTP-этапа: паки T-042 + словарь zstd T-043 + контроль диска T-046) →
// цикл q:render с приборами этапа (T-077) и коллектор длины q:render
// (метрика длин очередей, ТЗ 5.9). Завершение — отмена ctx (graceful
// shutdown, ТЗ 3.4): задачи дорабатываются (каждая ограничена
// render.timeout) или возвращаются очереди повторной доставкой
// (Processing, T-025); фоновые циклы (DiskWatchLoop, коллектор q:render)
// гасятся производной отменой и джойнятся до defer-закрытий rdb/store
// (T-232).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("render-worker: пул БД не получен (postgres.dsn обязателен)")
	}

	// Chromium — менеджер поколений (T-065): он владеет текущим «браузер +
	// пул», перезапускает браузер по порогам страниц/RSS и заменяет
	// поколение после потери Chromium (реконнект). ctx OnRun — контекст
	// жизненного цикла всех поколений (T-061: браузер живёт, пока жив
	// контекст стартового вызова). Старт — синхронно: ошибка старта —
	// ошибка OnRun (код 1, systemd Restart=on-failure), без браузера этап
	// не работает; дальнейшие перезапуски — без остановки потребления.
	manager := render.NewManager(ctx, render.NewOptions(*cfg))
	defer manager.Close()
	if err := manager.Start(); err != nil {
		return err
	}

	// Хранилище контента (T-056, ТЗ 5.7) — то же, что у HTTP-этапа:
	// словарь zstd (T-043) активируется до открытия, отсутствующий файл —
	// не ошибка; лимиты — секция content (max_store_bytes — ErrTooLarge
	// на раздутом DOM, disk_free_percent — ErrStoreFull, ТЗ 8).
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:             cfg.Content.Dir,
		PackSize:        cfg.Content.PackSize.B(),
		MaxStoreBytes:   cfg.Content.MaxStoreBytes.B(),
		DiskFreePercent: cfg.Content.DiskFreePercent,
		// OnStoreFull — хук алерта T-080 (метрики): до него события
		// «место ниже порога» видны в Warn-логах DiskWatchLoop.
	})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	// Словарь против новейшего пака (T-043): прежний словарь реестра
	// даёт читаемость старых кадров (T-103); расхождение без файла
	// в реестре видно сразу при старте.
	content.WarnDictMismatch(store.Dir(), logger)

	// Очередь: потребление стрима воркера (stream, дефолт q:render) и
	// XACK проходят прозрачно, публикация
	// q:analyze — под гейтом backpressure (T-026: порог общий для
	// гейтируемых стримов; как у dns-worker с q:http и http-worker с
	// q:analyze; ворота ёмкости q:render — отдельные,
	// render.queue_high_watermark, T-174).
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()

	// Джойн фоновых циклов (T-232, инвариант http-worker T-223):
	// производная отмена + WaitGroup. Каркас отменяет ctx OnRun только
	// после его возврата (internal/service: cancelRun после onRunDone) —
	// ранний выход run из конструктора ниже (renderworker.New) или паника
	// обязаны останавливать циклы сами, иначе defer-закрытия rdb/store
	// (LIFO ниже) закроют ресурсы под серединой прохода DiskWatchLoop
	// (store) или запроса глубины q:render (gate.Len → rdb); до T-232
	// оба цикла жили fire-and-forget. Джойн — defer, зарегистрированный
	// ПОСЛЕ defer-закрытий выше → исполняется ДО них и до закрытия
	// менеджера Chromium (LIFO). WaitGroup, а не канал ёмкости: джойн
	// ждёт ровно запущенное (Add/Done по месту запуска) — ранний выход с
	// частью циклов не виснет на «недостающих» приёмах. Ошибки циклов
	// отбрасываются сознательно: каждый цикл логирует свои сбои сам.
	// Инварианты закреплены структурным тестом TestRunBackgroundJoin
	// (main_test.go, разборщик — internal/bgjoin).
	ctx, cancel := context.WithCancel(ctx)
	var bg sync.WaitGroup
	defer func() {
		// Штатно ctx уже отменён каркасом к моменту возврата run;
		// ранний выход/паника гасят ещё живые циклы здесь — до
		// defer-закрытий (LIFO).
		cancel()
		bg.Wait()
	}()

	// Контроль свободного места каталога контента (T-046, ТЗ 8):
	// периодический наблюдатель горутиной (не зависит от цикла задач),
	// Warn и событие при входе в паузу записи/восстановлении; завершается
	// с отменой ctx.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = content.DiskWatchLoop(ctx, store, content.DiskWatchOptions{Log: logger})
	}()

	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // пределы обрезки — для порога гейта (T-026/T-025)
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	}), gate)

	// Возврат зависших доставок стрима потребления (T-025): RecoverLoop
	// в воркере —
	// клейм XAUTOCLAIM, исчерпание попыток — q:dead; параметры надёжности
	// и темпа — секция redis (max_attempts, claim_min_idle,
	// recover_batch, recover_interval — T-153). OnBury — счётчик смертей
	// crawler_queue_dead_buried_total (T-154): bury молчаливым быть не
	// должно (пост-мортем 22.10).
	rel := queue.NewReliab(rdb, q, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
		OnBury:      metrics.NewDeadBuriedHook(reg),
		// OnBuryUnidentified — карантин неидентифицируемых записей
		// (без task_id): crawler_queue_dead_unidentified_total
		// (T-260, ФТ-6.1) — терминальная ветка bury.
		OnBuryUnidentified: metrics.NewDeadUnidentifiedHook(reg),
	})

	// Стрим потребления воркера (T-236, ТЗ §4.1 п.3/ФТ-12): дефолт —
	// q:render; воркер фолбэка (ТЗ §4.5) — тот же бинарь другой сборкой:
	// стрим q:render-fallback (константа queue.StreamRenderFallback —
	// задача T-237). Переключатель сборки — переменная окружения
	// RENDER_WORKER_STREAM (как METRICS_ADDR — параметр запуска, а не
	// поля конфигурации: стрим потребления — параметр сборки, ТЗ §4.1
	// п.3; профиль воркера B — deploy/docker/config.render-fallback.yaml,
	// docs/deploy.md §2.3). Допустимы только стримы рендер-воркера
	// (q:render/q:render-fallback): чужой стрим гонял бы рендер чужой
	// группой потребителей. Группа — каноническая пара стрима
	// (queue.WorkingGroups, инвариант «одна consumer group на рабочий
	// стрим», T-155): основная сборка — DefaultGroup, воркер B —
	// render-fallback-worker.
	stream := service.EnvOr("RENDER_WORKER_STREAM", renderworker.DefaultStream)
	if stream != renderworker.DefaultStream && stream != queue.StreamRenderFallback {
		return fmt.Errorf("render-worker: стрим потребления %q недопустим: стримы рендер-воркера — q:render (основная сборка) и q:render-fallback (воркер B, ФТ-12/T-237)", stream)
	}
	group := queue.WorkingGroups()[stream]

	// Длина потребляемого стрима (stream) → crawler_queue_depth (T-077):
	// источник глубины — гейт (тот же клиент Redis), завершается отменой ctx.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{stream}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// Опции воркера — одни на сборку (стиль http-worker): Worker хранит
	// их для ProcessResult, цикл Run берёт те же Group/Consume/Alive.
	// Темп потребления (T-240, ФТ-13): потолок включается на стриме
	// фолбэка — сборке воркера B: render.fallback.worker_rate (T-233)
	// записей за скользящее окно renderworker.RateWindow, батч-режим
	// «собрать и доработать» без наращивания chromium-мощности. Основная
	// сборка (q:render) читает без потолка; воркер B держит темп и при
	// выключенном render.fallback.enabled — дорабатывает накопленное в
	// q:render-fallback до пуста (ФТ-14).
	rate := 0
	if stream == queue.StreamRenderFallback {
		rate = cfg.Render.Fallback.WorkerRate
	}
	opts := renderworker.Options{
		// Стрим потребления (T-236, ФТ-12): чтение цикла Run и клейм
		// RecoverLoop — одно значение с коллектором глубины выше;
		// дефолт q:render, сборка воркера фолбэка переопределяет.
		Stream: stream,
		// Группа — каноническая пара стрима (queue.WorkingGroups):
		// render-worker (основная сборка) / render-fallback-worker (B).
		Group:   group,
		Recover: rel, // возврат зависших задач (T-025)
		// Темп возврата зависших (T-153): пачка/интервал клейма —
		// redis.recover_*; инвариант с claim_min_idle (рендер — самый
		// медленный последовательный обход) — docs/configuration.md.
		RecoverBatch:    cfg.Redis.RecoverBatch,
		RecoverInterval: cfg.Redis.RecoverInterval.D(),
		Metrics:         reg, // приборы этапа: задачи, длительности, ошибки классов, rendered (T-077)
		// Движок сборки (render.engine, T-234) → метка engine метрик
		// этапа (T-235, ФТ-15): то же значение, что у
		// render.NewOptions выше (менеджер рендера), — движок сборки
		// один, метки метрик различают движки канареечных окон.
		Engine: cfg.Render.Engine,
		Log:    logger,
		// Зонд браузера: мёртвое поколение менеджер заменяет прямо в зонде
		// (Manager.Alive, T-065 — реконнект после потери Chromium), здесь —
		// только пауза потребления, пока старт не удастся: доставки
		// остаются в стриме, не в PEL, и не расходуют попытки.
		Alive: manager.Alive,
		// Доставок в работе — не меньше слотов пула: без этого
		// render.contexts > 4 не ускорял бы этап (параллелизм цикла
		// остался бы 4, лишние слоты пула простаивали; ревью T-142).
		Concurrency: max(renderworker.DefaultConcurrency, cfg.Render.Contexts),
		// Тарпит-бюджет (T-148): таймауты страницы не жгут общий лимит
		// доставок — после render.timeout_attempts таймаутов запись
		// финализируется без рендера (сырой HTML + q:analyze), не q:dead.
		TimeoutAttempts: cfg.Render.TimeoutAttempts,
		// Последняя попытка × класс задачи (T-172): ошибка рендера класса
		// task на доставке № redis.max_attempts финализирует запись без
		// рендера вместо q:dead — то же значение, что у Reliab выше
		// (граница захоронения), чтобы политика успевала до перевода.
		MaxAttempts: cfg.Redis.MaxAttempts,
		// Тарпит-счётчик (T-261, ФТ-7): решение тарпит-финализации — по
		// числу тарпит-классифицированных доставок эпизода (мощность
		// Redis-множества render:tarpit:{task_id} ≥ render.timeout
		// _attempts, TTL 48 ч), а не по номеру доставки PEL: смешанные
		// серии (инфра-сбой между тарпитами) не финализируют на попытку
		// раньше. Тот же клиент Redis, что у очереди; потеря Redis —
		// деградация к номеру доставки (поведение до T-261).
		Tarpits: queue.NewTarpitCounter(rdb),
		// Фолбэк-публикация (T-238, ТЗ 4.4, ФТ-14): render.fallback.enabled
		// (T-233) — тарпиты T-148 и класс задачи T-172 на последней
		// доставке публикуются в q:render-fallback вместо маркера
		// render_failed_at; false (дефолт) — финализации как сегодня.
		// Гейт/бюджет решения (T-239, ФТ-9): порог стрика инфра-ошибок
		// класса браузера и потолок доли публикаций за окно — из
		// render.fallback (T-233); состояния (стрик, окно) ведёт воркер.
		// Темп воркера B — T-240; воркер B (стрим q:render-fallback)
		// фолбэк не публикует — рекурсия запрещена (ФТ-10), проверяется
		// в publishFallback по Stream; движок сборки chromium (включая
		// пустое значение render.engine) тоже не публикует (S-P1-1,
		// ТЗ §6.2 п.2/§6.4 п.1) — шаг 1 приёмки и откат гасят
		// публикацию без смены флага.
		Fallback: renderworker.FallbackOptions{
			Enabled: cfg.Render.Fallback.Enabled,
			Gate: renderworker.FallbackGate{
				Threshold: cfg.Render.Fallback.HealthGateStreak,
			},
			Budget: renderworker.FallbackBudget{
				Ceiling: cfg.Render.Fallback.SharePercent,
			},
		},
		// Потолок темпа потребления (T-240, ФТ-13; комментарий у rate
		// выше): воркер B — render.fallback.worker_rate за окно
		// renderworker.RateWindow, основная сборка — 0 (без потолка).
		Rate: rate,
	}
	w, err := renderworker.New(pool, q, store, manager, opts)
	if err != nil {
		return err
	}
	// Главный цикл живёт до отмены ctx (graceful shutdown); джойн
	// фоновых циклов — defer выше: любой выход run (штатный, ранний из
	// конструктора, паника) сначала гасит и дожидается циклы, и лишь
	// затем LIFO закрывает rdb/store/менеджера Chromium.
	return renderworker.Run(ctx, w, opts)
}
