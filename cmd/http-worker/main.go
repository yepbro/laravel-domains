// Сервис http-worker (ТЗ 5.5, 4.2, 3.4): HTTP-этап проверки доменов.
// Потребляет задачи q:http (HTTPTask T-024: домен + DNS-живые субдомены с
// адресами и CNAME) группой потребителей; для каждого субдомена — проверки
// https и http цепочками редиректов (internal/httpfetch: транспорт с
// лимитами T-049, редиректы и meta refresh T-048, TLS-атрибуты T-050,
// кодировки T-051, честный UA T-052), параллельно внутри задачи с общим
// кэшем «итоговый URL → результат» (ТЗ 5.5 п. 6); вариант-логика T-054,
// алиасы T-055 (сравнение по хэшам), SPA-эвристика T-057 — internal/
// httpworker (T-053). Каждая скачанная страница немедленно сохраняется
// ранним Put в ContentStore (T-147: паки T-042, словарь zstd T-043, дедуп
// по хэшу §8; тела в памяти слота не удерживаются — контроль диска T-046 —
// DiskWatchLoop); сверка ссылок ранних Put — ContentSaver (T-056) в хуке
// Options.OnResult до XACK: оригиналы наследуют свои ссылки, лишние
// снимаются Delete, затем результат домена записывается в
// subdomain_results одной транзакцией со свопом ссылок на контент
// (Recorder T-058: статус alive/no_http/temp_error, домен done, §7) и
// каждая запись публикуется в q:analyze (Result, T-024) — вся цепочка
// до XACK (ТЗ 4.2), подтверждение q:http — паттерном Processing (T-025).
// SPA-записи публикуются в q:render (T-066, renderq.go): свободная
// очередь — сразу после фиксации, переполнение — флаг render_pending той
// же транзакцией без задержки XACK (§5.6), доработка — фоновым проходом
// RequeueRenderPending (повторный SPA-вердикт по сохранённому контенту).
// Хосты редиректов резолвятся «через тот же резолвер» (ТЗ 5.5 п. 2):
// dnsclient с лимитами T-035 → адаптер httpworker.DNSLookup → кэш
// CachingResolver. Каркас — internal/service: миграции, пул БД,
// пробы /livez + /healthz, /metrics, graceful shutdown.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[http-worker] конфигурация: %v", err)
	}
	logger := service.NewLogger("http-worker", cfg)

	// Мягкий потолок памяти Go-рантайма (T-163, http.mem_limit): GC
	// удерживает кучу ниже порога сам, не доводя anon до MemoryHigh/
	// MemoryMax юнита — reclaim-штормы ядра и OOM-kill дороже лишних
	// циклов GC (фаталы T-157: 120 095 событий MemoryHigh, пик
	// 900.5 MiB = MemoryMax). Ставится до рабочих аллокаций; page cache
	// записи паков в учёт рантайма не входит — потолок ниже MemoryHigh
	// с запасом (дефолт 768 MiB против 850M/900M). 0 — выключено.
	if limit := cfg.HTTP.MemLimit; limit > 0 {
		debug.SetMemoryLimit(limit.B())
		logger.Info("мягкий потолок памяти Go-рантайма", "limit", limit.String())
	}

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate);
	// им пользуется и хранилище контента (строки contents, T-042/T-056),
	// и запись результата (subdomain_results/domains, T-058). Реестр
	// метрик — здесь же (T-077): приборы HTTP-этапа и коллектор длины
	// q:http получают его до старта OnRun, каркас отдаёт тот же реестр
	// через /metrics.
	reg := metrics.New("http-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "http-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9104"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика http-worker: клиент DNS (резолв хостов редиректов,
// ТЗ 5.5 п. 2) под лимитами T-035 → транспорт httpfetch с лимитами T-049
// и TLS без проверки сертификата (ТЗ 5.5 п. 3: контент нужен всегда,
// результат проверки сертификата сохраняет TLSInfo T-050) → хранилище
// контента (T-056/T-147: паки T-042 + словарь T-043 + контроль диска
// T-046 — ранние Put страниц и сверка ссылок) → воркер (кэш итоговых
// URL, варианты, алиасы, SPA) с приборами этапа (T-077) и коллектор
// длины q:http (метрика длин очередей, ТЗ 5.9) → цикл q:http. Завершение
// — отмена ctx (graceful shutdown, ТЗ 3.4): начатые задачи дорабатывают
// или возвращаются очереди повторной доставкой (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("http-worker: пул БД не получен (postgres.dsn обязателен)")
	}

	client, err := dnsclient.NewClient(ctx, dnsclient.NewOptions(cfg.DNS))
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	resolve, err := dnsclient.NewLimitResolver(client, dnsclient.NewLimitOptions(cfg.DNS))
	if err != nil {
		return err
	}

	// Транспорт этапа: NewOptions включает лимиты всегда (T-049) и
	// подставляет дефолтный честный UA (T-052); резолв хостов редиректов —
	// тем же резолвером DNS-этапа с кэшем (ТЗ 5.5 п. 2, CachingResolver
	// T-047, адаптер httpworker.DNSLookup); InsecureSkipVerify — ТЗ 5.5
	// п. 3.
	opts := httpfetch.NewOptions(cfg.HTTP)
	opts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	opts.Resolver = httpfetch.NewCachingResolver(
		httpworker.NewDNSLookup(resolve, cfg.DNS.IPv6), httpfetch.CacheOptions{})
	fetcher := httpfetch.NewFetcher(opts)
	defer fetcher.CloseIdleConnections()

	// Хранилище контента (T-056, ТЗ 5.7): словарь zstd (T-043)
	// активируется до открытия — блоки сжимаются активным словарём
	// (CompressWithDict), его ID попадает в заголовок создаваемых паков;
	// отсутствующий файл — не ошибка (сжатие без словаря). Лимиты —
	// секция content: max_store_bytes (ErrTooLarge), disk_free_percent
	// (ErrStoreFull, ТЗ 8), pack_size (ротация).
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:             cfg.Content.Dir,
		PackSize:        cfg.Content.PackSize.B(),
		MaxStoreBytes:   cfg.Content.MaxStoreBytes.B(),
		DiskFreePercent: cfg.Content.DiskFreePercent,
		// OnStoreFull — хук алерта T-080 (метрики): до него события
		// «место ниже порога» видны в Warn-логах DiskWatchLoop и
		// ContentSaver.
	})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	// Словарь против новейшего пака (T-043): прежний словарь реестра
	// даёт читаемость старых кадров (T-103); расхождение без файла
	// в реестре видно сразу при старте.
	content.WarnDictMismatch(store.Dir(), logger)

	// Фоновые циклы сервиса (наблюдатель диска, коллектор длин очередей,
	// проход доработки рендера) живут до отмены ctx; их результаты
	// забираются в bg после главного цикла — до defer-закрытий store,
	// rdb и транспорта ниже (стиль cmd/scheduler): незаджойненная
	// горутина могла трогать закрытые PackStore/Redis/Fetcher.
	bg := make(chan error, 3)

	// Контроль свободного места каталога контента (T-046, ТЗ 8):
	// периодический наблюдатель горутиной (не зависит от цикла задач),
	// Warn и событие при входе в паузу записи/восстановлении; завершается
	// с отменой ctx.
	go func() { bg <- content.DiskWatchLoop(ctx, store, content.DiskWatchOptions{Log: logger}) }()

	// Фоновая компактификация паков (T-045, ТЗ 5.7): единственный
	// механизм физического освобождения места и удаления мёртвых строк
	// contents — без неё ErrStoreFull не снимается никогда. Цикл один на
	// хранилище — здесь, рядом с DiskWatchLoop (проходам не нужен
	// параллелизм; одновременный запуск во втором писателе формально
	// допустим, но бесполезен). Завершается с отменой ctx.
	go func() { _ = content.CompactLoop(ctx, store, content.CompactOptions{Log: logger}) }()

	saver, err := httpworker.NewContentSaver(store, logger)
	if err != nil {
		return err
	}

	// Очередь: потребление q:http и публикация q:analyze — под гейтом
	// backpressure (T-026: порог единый для рабочих стримов; Consume/Ack
	// проходят прозрачно, как у dns-worker). Публикация q:render идёт
	// мимо WatermarkPublisher, через ту же RedisQueue (T-066, §5.6):
	// ожидание ёмкости задержало бы XACK задачи q:http — при переполнении
	// домен получает флаг render_pending вместо публикации (renderq.go).
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // пределы обрезки — для порога гейта (T-026/T-025)
		Log:       logger,
	})
	rawQ := queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	})
	q := queue.NewWatermarkPublisher(rawQ, gate)

	// Возврат зависших доставок q:http (T-025): RecoverLoop в воркере —
	// клейм XAUTOCLAIM, исчерпание попыток — q:dead; параметры надёжности
	// и темпа — секция redis (max_attempts, claim_min_idle,
	// recover_batch, recover_interval — T-153). OnBury — счётчик смертей
	// crawler_queue_dead_buried_total (T-154): bury молчаливым быть не
	// должно (пост-мортем 22.10).
	rel := queue.NewReliab(rdb, rawQ, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
		OnBury:      metrics.NewDeadBuriedHook(reg),
	})

	// Очередь рендера: неблокирующая проверка ёмкости (глубина через гейт
	// T-026, общий порог scheduler.queue_high_watermark) + публикация
	// без ожидания ёмкости.
	renderQ, err := httpworker.NewRenderQueue(rawQ, gate, int64(cfg.Scheduler.QueueHighWatermark), logger)
	if err != nil {
		return err
	}

	// Длина потребляемого стрима q:http → crawler_queue_depth (T-077):
	// источник глубины — гейт (тот же клиент Redis), завершается отменой ctx.
	go func() {
		bg <- metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamHTTP}, metrics.DefaultQueueCollectInterval, logger)
	}()

	recorder, err := httpworker.NewRenderRecorder(pool, q, renderQ, logger)
	if err != nil {
		return err
	}

	// Проход доработки рендера (T-066, §5.6 «дорабатываются позже»):
	// домены с флагом render_pending → повторный SPA-вердикт по
	// сохранённому контенту → публикация q:render при свободной очереди.
	// Отдельной горутиной в том же OnRun (образец CleanupDeleted
	// T-020/T-021): не зависит от цикла задач q:http, завершается
	// отменой ctx.
	go func() {
		bg <- httpworker.RequeueRenderPending(ctx, pool, store, renderQ, httpworker.RequeueOptions{Log: logger})
	}()

	// Retention контента и трим jsonb не-находок (T-166, ТЗ 5.7: контент
	// хранится, пока нужно переприменять правила; решение оператора
	// 2026-10-23 — диск без новых мощностей): записи старше
	// content.retention, не Laravel-находки и не ждущие рендера теряют
	// content_hash (−1 contents.refcount той же транзакцией, нулевые
	// строки удаляются) и тяжёлые jsonb; паспорт строки остаётся для
	// domain_info и дашборда. Интерлок — глубина q:analyze (гейт T-026):
	// анализатор не успевает — тела ещё нужны, проход пропускается;
	// порог 0 в конфиге — общий scheduler.queue_high_watermark (тем же
	// критерием «строго ниже порога» работает гейт публикации q:analyze).
	// Точная защита (T-166/T-167): записи, чьи сообщения ещё не потреблены
	// q:render/q:analyze, исключаются из пачек по снимку стримов —
	// retentionPending ниже, тем же Redis-клиентом, что и гейт.
	// Цикл здесь же, где компактификация: purge лишь готовит мёртвые
	// блоки, место возвращает её часовой проход; второму владельцу он не
	// нужен (охраны делают параллельный запуск безопасным, но
	// бесполезным). Завершается отменой ctx.
	retentionWatermark := int64(cfg.Content.RetentionAnalyzeWatermark)
	if retentionWatermark <= 0 {
		retentionWatermark = int64(cfg.Scheduler.QueueHighWatermark)
	}
	go func() {
		_ = content.RetentionLoop(ctx, pool, content.RetentionOptions{
			Retention:        cfg.Content.Retention.D(),
			Interval:         cfg.Content.RetentionInterval.D(),
			BatchSize:        cfg.Content.RetentionBatch,
			Depth:            analyzeDepth{gate: gate},
			Pending:          retentionPending{rdb: rdb},
			AnalyzeWatermark: retentionWatermark,
			Log:              logger,
			OnPass:           content.NewRetentionMetrics(reg).Observe,
		})
	}()

	opts2 := httpworker.Options{
		// Задачный параллелизм цикла Run (T-139): скользящее потребление
		// q:http, значения — http.task_* из конфига (дефолты по замерам
		// T-092; транспортные потолки — http.concurrency/per_ip_limit/
		// max_rps выше, в httpfetch.NewOptions).
		Concurrency:  cfg.HTTP.TaskConcurrency,
		Consume:      cfg.HTTP.TaskConsume,
		PerDomain:    cfg.HTTP.TaskPerDomain,
		MaxRedirects: cfg.HTTP.MaxRedirects, // 0 — не следовать (T-006/T-048)
		// Dead-host cutoff (T-164): мёртвые apex+www не жгут слоты
		// задачи на субдоменах — http.dead_host_cutoff.
		DeadHostCutoff: cfg.HTTP.DeadHostCutoff,
		Store:          store, // раннее сохранение тел страниц (T-147)
		Recover:        rel,   // возврат зависших задач q:http (T-025)
		// Темп возврата зависших (T-153): пачка/интервал клейма —
		// redis.recover_*.
		RecoverBatch:    cfg.Redis.RecoverBatch,
		RecoverInterval: cfg.Redis.RecoverInterval.D(),
		Metrics:         reg, // приборы этапа: задачи, ошибки, статусы, время ответа, rendered (T-077)
		Log:             logger,
		// Цепочка OnResult (внутри обработки задачи, до XACK, ТЗ 4.2):
		// сохранение контента (T-056) → запись subdomain_results со свопом
		// ссылок contents (T-058/T-044) → публикация q:analyze (T-058,
		// §5.5 п. 9). Сбой любого звена — доставка без подтверждения,
		// повторная доставка переигрывает цепочку идемпотентно (ТЗ 3.4).
		OnResult: func(c context.Context, res *httpworker.DomainResult) error {
			if err := saver.Save(c, res); err != nil {
				return err
			}
			_, err := recorder.RecordAndPublish(c, res)
			return err
		},
	}
	w, err := httpworker.New(httpworker.ChainFetch(fetcher), opts2)
	if err != nil {
		return err
	}

	err = httpworker.Run(ctx, q, w, opts2)
	// Главный цикл завершён отменой ctx — фоновые циклы видят ту же
	// отмену и заканчиваются сами; дождаться их до defer-закрытий
	// store/rdb/соединений транспорта (гонка времени жизни ресурсов).
	<-bg
	<-bg
	<-bg
	return err
}

// analyzeDepth — глубина q:analyze для интерлока retention (T-166):
// адаптер гейта backpressure (T-026) к узкому интерфейсу
// content.RetentionDepth — пакет контента не знает Redis-специфики
// гейта (стиль RenderLen, renderq.go).
type analyzeDepth struct{ gate *queue.BackpressureGate }

func (a analyzeDepth) AnalyzeDepth(ctx context.Context) (int64, error) {
	return a.gate.Len(ctx, queue.StreamAnalyze)
}

// retentionPendingRedis — окно в go-redis для чтения непотреблённого
// хвоста стрима: XINFO GROUPS (last-delivered-id групп) и XRANGE.
// Узкое, как redisDepth гейта (T-026): *redis.Client удовлетворяет,
// юнит-тест (main_test.go) подменяет фейком; внутренний интерфейс
// пакета internal/queue не расширяется — там параллельные задачи.
type retentionPendingRedis interface {
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
	XRangeN(ctx context.Context, key, start, stop string, count int64) *redis.XMessageSliceCmd
}

// compile-time: клиент очереди — источник защиты retention.
var _ retentionPendingRedis = (*redis.Client)(nil)

// Константы чтения хвоста стрима.
const (
	// pendingFieldData — единственное поле записи стрима: JSON сообщения
	// (fieldData redisq.go; литерал здесь — поле не экспортировано,
	// дублируется с комментарием-якорем).
	pendingFieldData = "data"

	// pendingScanBatch — записей в одном XRANGE: хвост при застое —
	// десятки тысяч (лаг q:render ≈99 тыс., runbooks §5), пагинация
	// держит пиковый слайс умеренным; итоговое множество ключей всё
	// равно вмещает весь хвост — цена точной защиты.
	pendingScanBatch = 1000
)

// retentionPending — ключи записей, чьи сообщения ждут потребителей,
// для точной защиты retention (content.RetentionPending, T-166/T-167):
// адаптер над Redis-клиентом гейта (тем же клиентом читает глубину
// BackpressureGate.Len — одна точка правды о стримах). Читает
// недоставленный группе хвост: XRANGE от last-delivered-id группы
// (эксклюзивно, «(id») до '+'. XREADGROUP ">" запрещён — он доставил бы
// записи в pending группы и сдвинул last-delivered-id, сломав реального
// потребителя. Групп у стрима нет — недоставлено всё содержимое: XRANGE
// с начала (та же семантика, что «глубина = XLEN» у гейта). Сообщения
// разбираются как queue.Result (формат T-024 — публикация record.go и
// renderSPAResults); чужое тело пропускается: в стрим очереди пишет
// только marshalMessage (валидированные форматы).
type retentionPending struct {
	rdb retentionPendingRedis
}

func (p retentionPending) PendingKeys(ctx context.Context, stream string) (map[content.RetentionPendingKey]struct{}, error) {
	keys := make(map[content.RetentionPendingKey]struct{})
	groups, err := p.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// Стрима нет — «no such key» (не пустой список), как у гейта:
		// ни публикаций, ни групп — ждать потребителей нечего.
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			return keys, nil
		}
		return nil, fmt.Errorf("http-worker: защита retention: группы %s: %w", stream, err)
	}
	if len(groups) == 0 {
		if err := scanPendingKeys(ctx, p.rdb, stream, "-", keys); err != nil {
			return nil, err
		}
		return keys, nil
	}
	// Хвост, недоставленный хоть одной группе, — от минимального
	// last-delivered-id: интервал каждой группы — (её id, +∞), объединение
	// интервалов с разными краями — интервал от минимума; один XRANGE
	// вместо по группе (в бою группа одна — вырожденный случай).
	minID := ""
	for _, g := range groups {
		if minID == "" || pendingIDLess(g.LastDeliveredID, minID) {
			minID = g.LastDeliveredID
		}
	}
	// Эксклюзивный нижний край "(" + id: записи строго после
	// last-delivered-id; у только что созданной группы id "0-0" —
	// весь стрим.
	if err := scanPendingKeys(ctx, p.rdb, stream, "("+minID, keys); err != nil {
		return nil, err
	}
	return keys, nil
}

// pendingIDLess — сравнение идентификаторов записей стрима "ms-seq"
// численно по частям: лексикографическое сравнение ошибочно ("999-0"
// больше "1000-0" — то же замечание у trackProgress, redisq.go;
// LastDeliveredID новой группы "0-0" короче реальных id).
func pendingIDLess(a, b string) bool {
	ams, aseq, aerr := splitPendingID(a)
	bms, bseq, berr := splitPendingID(b)
	if aerr != nil || berr != nil {
		return a < b // неразбираемые — лексикографически: детерминизм важнее точности
	}
	if ams != bms {
		return ams < bms
	}
	return aseq < bseq
}

// splitPendingID разбирает "ms-seq" в пару int64.
func splitPendingID(id string) (ms, seq int64, err error) {
	s1, s2, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0, fmt.Errorf("http-worker: защита retention: идентификатор записи %q не вида ms-seq", id)
	}
	if ms, err = strconv.ParseInt(s1, 10, 64); err != nil {
		return 0, 0, err
	}
	if seq, err = strconv.ParseInt(s2, 10, 64); err != nil {
		return 0, 0, err
	}
	return ms, seq, nil
}

// scanPendingKeys дочитывает хвост стрима от start (включительно) до '+'
// пачками pendingScanBatch, разбирая поле data в ключ записи.
func scanPendingKeys(ctx context.Context, rdb retentionPendingRedis, stream, start string, keys map[content.RetentionPendingKey]struct{}) error {
	for {
		msgs, err := rdb.XRangeN(ctx, stream, start, "+", pendingScanBatch).Result()
		if err != nil {
			return fmt.Errorf("http-worker: защита retention: xrange %s: %w", stream, err)
		}
		for _, m := range msgs {
			var raw []byte
			switch v := m.Values[pendingFieldData].(type) {
			case string:
				raw = []byte(v)
			case []byte:
				raw = v
			default:
				continue // поля data нет — не сообщение очереди
			}
			var r queue.Result
			if json.Unmarshal(raw, &r) != nil {
				continue // чужое тело: стримы очереди пишет только marshalMessage
			}
			if r.ZoneID <= 0 || r.DomainID <= 0 {
				continue // нулевые идентификаторы — не Result (validate требует ≥ 1)
			}
			keys[content.RetentionPendingKey{
				Zone: r.ZoneID, Domain: r.DomainID,
				Sub: r.SubdomainID, Variant: int16(r.Variant),
			}] = struct{}{}
		}
		if int64(len(msgs)) < pendingScanBatch {
			return nil // хвост прочитан целиком
		}
		start = "(" + msgs[len(msgs)-1].ID // следующая страница — после последней
	}
}
