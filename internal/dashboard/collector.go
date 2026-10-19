// Сборщик снимков (ТЗД §4.3–4.4, T-129): фоновый цикл с периодом
// dashboard.refresh — первый сбор сразу при старте (свежий снимок после
// простоя/рестарта, стиль monitor.ZoneStatsLoop), далее по ticker;
// собранный снимок атомарно заменяет предыдущий (atomic.Pointer —
// хендлеры читают без блокировок).
//
// Источники (ТЗД §4.3, все read-only):
//
//   - PostgreSQL — секции internal/status (зоны/темп — те же SQL, что у
//     crawlerctl status, T-126; статусы/Laravel/ingest — §5.5–5.7);
//   - Redis — глубина пяти стримов через тот же узкий API Len
//     (BackpressureGate, ТЗД §4.3.2): клиент собирает cmd/dashboard по
//     конфигу redis — как cmd/monitor; порог гейта не участвует (только
//     Len, стиль crawlerctl status);
//   - пробы /livez сервисов конвейера из dashboard.probes — параллельно
//     (горутины + WaitGroup; errgroup не используется — golang.org/x/sync
//     в go.mod косвенная, ТЗД §3.2.1 новых зависимостей не вводит),
//     таймаут 2 с на пробу (§5.8).
//
// Ошибка секции прерывает только её (снимок публикуется с деградацией,
// §5.9); ошибка прохода целиком — только отмена ctx (остановка сервиса).
// «Успешный сбор» (ТЗД §5.9: до первого /api/status отвечает 503) —
// первый проход с живым каркасом сводки (зоны/итоги §5.1): иерархия та
// же, что у crawlerctl status — недоступный Redis не прячет сводку по
// БД (очереди «н/д», приёмка §12.3 работает и для рестарта при лежащем
// Redis), лежащая БД держит 503 и метрику result=error до
// восстановления — снимок без каркаса зон бессодержателен. Latch
// готовности срабатывает один раз — позже деградированные снимки
// обслуживаются (§4.4 атомарная замена + §5.9 не роняет выдачу).
package dashboard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/status"
)

// DefaultRefreshInterval — период сборки при нулевом Interval (зеркалит
// дефолт dashboard.refresh, ТЗД §8.1).
const DefaultRefreshInterval = 5 * time.Second

// probeTimeout — бюджет одной пробы /livez (ТЗД §5.8: 2 с).
const probeTimeout = 2 * time.Second

// probeBodyLimit — сколько тела пробы вычитать до закрытия (connection
// reuse; /livez каркаса — байты «ok»).
const probeBodyLimit = 4 << 10

// Метки результата сборки (§10).
const (
	snapshotResultOK    = "ok"
	snapshotResultError = "error"
)

// CollectorOptions — параметры сборщика; нулевые значения заменяются
// дефолтами (стиль monitor.ZoneStatsOptions).
type CollectorOptions struct {
	// Interval — период цикла (DefaultRefreshInterval); рабочее значение —
	// dashboard.refresh из конфига.
	Interval time.Duration
	// Probes — имя → базовый URL метрик сервиса (dashboard.probes): проба
	// идёт на base + "/livez" (каркас internal/service, ТЗД §4.3.3).
	// nil/пусто — секция здоровья «н/д» (Health = nil, не ошибка).
	Probes map[string]string
	// QueueWatermark — порог backpressure scheduler.queue_high_watermark
	// для подсветки очередей (§5.4); 0 — не настроен (гейт отключён).
	QueueWatermark int64
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — реестр сервиса (метрики §10); nil — без метрик (шов
	// тестов; прецедент monitor.NewNearGauge).
	Metrics *metrics.Registry
	// Now — часы момента сборки (built_at); nil — time.Now.
	Now func() time.Time
	// HTTPClient — клиент проб; nil — свой с таймаутом probeTimeout.
	// Шов тестов: короткий таймаут вместо ожидания двух секунд.
	HTTPClient *http.Client
}

func (o CollectorOptions) withDefaults() CollectorOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRefreshInterval
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Collector — фоновый сборщик снимков: собирает секции независимо,
// публикует атомарно. Безопасен для одновременного использования: Loop
// пишет, Current читают хендлеры.
type Collector struct {
	pool    db.DB
	lens    status.StreamLener
	opt     CollectorOptions
	client  *http.Client
	metrics *collectorMetrics

	snap  atomic.Pointer[Snapshot]
	ready atomic.Bool
}

// NewCollector собирает сборщик: pool — пул БД (обязателен), lens —
// глубины стримов Redis (nil — секция очередей всегда деградирована:
// FetchQueueLens честно откажет), cfg — секция dashboard (период/пробы).
func NewCollector(pool db.DB, lens status.StreamLener, cfg config.Dashboard, opts CollectorOptions) (*Collector, error) {
	if pool == nil {
		return nil, errors.New("dashboard: сборщик снимков: пул БД не задан")
	}
	if cfg.Refresh.D() > 0 && opts.Interval == 0 {
		opts.Interval = cfg.Refresh.D()
	}
	if opts.Probes == nil {
		opts.Probes = cfg.Probes
	}
	o := opts.withDefaults()
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: probeTimeout,
			// Проба не следует редиректам: /livez отвечает 200 прямо,
			// а чужой Location не должен уводить исходящее соединение
			// на неконфигурированный хост (ТЗД §9.3 — только настроенные
			// пробы); редирект остаётся ответом != 200 → down с причиной.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &Collector{
		pool:    pool,
		lens:    lens,
		opt:     o,
		client:  client,
		metrics: newCollectorMetrics(o.Metrics),
	}, nil
}

// Current — последний снимок для /api/status; nil, пока не было ни
// одного успешного сбора (§5.9 — каркас сводки ещё не собирался).
func (c *Collector) Current() *Snapshot {
	if !c.ready.Load() {
		return nil
	}
	return c.snap.Load()
}

// CollectOnce — один проход сборки: секции независимо (§5.9), готовый к
// отдаче снимок. Отмена ctx посреди прохода оставляет собранные секции
// с ошибками контекста в карте errors — Loop такой снимок не публикует.
func (c *Collector) CollectOnce(ctx context.Context) *Snapshot {
	now := c.opt.Now()
	snap := &Snapshot{
		Errors:         map[string]string{},
		BuiltAt:        rfc3339(now),
		QueueWatermark: c.opt.QueueWatermark, // §5.4: подсветка порога — данные, не секция
	}

	// Зоны (§5.1/§5.3) → зоны + итоги; вход скоростей (pending+queued).
	// SummaryJSON встроена в Snapshot — поля присваиваются напрямую.
	var tot status.Totals
	zonesOK := false
	zones, err := status.FetchZoneStats(ctx, c.pool)
	if err != nil {
		snap.Errors["zones"] = err.Error()
	} else {
		zonesOK = true
		tot = status.ComputeTotals(zones)
		snap.Zones = status.ZonesToJSON(zones)
		t := status.TotalsToJSON(tot)
		snap.Totals = &t
	}

	// Темп (§5.2).
	var thr status.Throughput
	thrOK := false
	thr, err = status.FetchThroughput(ctx, c.pool)
	if err != nil {
		snap.Errors["throughput"] = err.Error()
	} else {
		thrOK = true
		t := status.ThroughputToJSON(thr)
		snap.Throughput = &t
	}

	// Скорости/ETA (§5.1–5.2): производные обеих секций — деградация
	// любой оставляет rates null.
	if zonesOK && thrOK {
		r := status.RatesToJSON(status.ComputeRates(thr, tot.Pending, tot.Queued))
		snap.Rates = &r
	}

	// Очереди (§5.4): nil-карта → null в JSON.
	lens, err := status.FetchQueueLens(ctx, c.lens)
	if err != nil {
		snap.Errors["queues"] = err.Error()
	} else {
		snap.Queues = lens
	}

	// Статусы доменов (§5.5).
	if snap.Statuses, err = status.FetchDomainStatuses(ctx, c.pool); err != nil {
		snap.Statuses = nil
		snap.Errors["statuses"] = err.Error()
	}

	// Laravel-находки (§5.6).
	if laravel, err := status.FetchLaravelStats(ctx, c.pool); err != nil {
		snap.Errors["laravel"] = err.Error()
	} else {
		snap.Laravel = &LaravelJSON{
			Total:   laravel.Total,
			Last24h: laravel.Last24h,
			Recent:  make([]LaravelItemJSON, 0, len(laravel.Recent)),
		}
		for _, it := range laravel.Recent {
			snap.Laravel.Recent = append(snap.Laravel.Recent, LaravelItemJSON{
				Domain: it.Domain, Zone: it.Zone, Score: it.Score,
				VerMin: it.VerMin, VerMax: it.VerMax, UpdatedAt: rfc3339(it.UpdatedAt),
			})
		}
	}

	// Журнал ingest (§5.7).
	if reports, err := status.FetchIngestReports(ctx, c.pool); err != nil {
		snap.Errors["ingest"] = err.Error()
	} else {
		snap.Ingest = make([]IngestFileJSON, 0, len(reports))
		for _, r := range reports {
			snap.Ingest = append(snap.Ingest, IngestFileJSON{
				Path: r.Path, Action: r.Action,
				LinesTotal: r.LinesTotal, Added: r.Added, Existed: r.Existed,
				Deleted: r.Deleted, Rejected: r.Rejected,
				ProcessedAt: rfc3339Zero(r.ProcessedAt), // NULL processed_at — пусто
			})
		}
	}

	// Здоровье сервисов (§5.8): пробы параллельно; пустой список проб —
	// секция «н/д» (null), не ошибка.
	snap.Health = c.probeAll(ctx)
	return snap
}

// probeAll — пробы всех сервисов параллельно (§5.8), ответ отсортирован
// по имени пробы (детерминированный JSON). Отмена ctx прерывает и сами
// пробы (контекст запроса): собранные down-записи уходят вместе
// с отменённым проходом — он не публикуется.
func (c *Collector) probeAll(ctx context.Context) []ProbeJSON {
	if len(c.opt.Probes) == 0 {
		return nil
	}
	names := make([]string, 0, len(c.opt.Probes))
	for name := range c.opt.Probes {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]ProbeJSON, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = c.probeOne(ctx, name, c.opt.Probes[name])
		}()
	}
	wg.Wait()
	return out
}

// probeOne — одна проба: GET base+"/livez" с таймаутом клиента
// (probeTimeout). up — 200 OK; недоступность, таймаут и не-200 — down
// с причиной (§5.8 «недоступен или таймаут»). URL собирается
// JoinPath'ом: корректно и для базы с хвостовым слэшем, и для базы
// с путём/query (валидация конфигурации допускает).
func (c *Collector) probeOne(ctx context.Context, name, base string) ProbeJSON {
	u, err := url.Parse(base)
	if err != nil {
		return ProbeJSON{Name: name, Error: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.JoinPath("livez").String(), nil)
	if err != nil {
		return ProbeJSON{Name: name, Error: err.Error()}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return ProbeJSON{Name: name, Error: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, probeBodyLimit))
	if resp.StatusCode != http.StatusOK {
		return ProbeJSON{Name: name, Error: fmt.Sprintf("HTTP %d", resp.StatusCode)}
	}
	return ProbeJSON{Name: name, Up: true}
}

// Loop — фоновый цикл сборки (ТЗД §4.4): первый сбор сразу, далее с
// периодом Interval; снимок атомарно заменяется. Ошибки секций — Warn с
// картой причин и метрика result=error, цикл живёт (контракт OnRun);
// отмена ctx — чистый nil (graceful shutdown). Сборка длиннее периода
// просто сдвигает следующий тик (ticker не накапливает пропуски).
func (c *Collector) Loop(ctx context.Context) error {
	c.opt.Log.Info("цикл сборки снимков дашборда",
		"interval", c.opt.Interval.String(), "probes", len(c.opt.Probes))

	pass := func() {
		started := time.Now()
		snap := c.CollectOnce(ctx)
		if ctx.Err() != nil {
			return // отмена посреди прохода — часть остановки, не снимок
		}
		result := snapshotResultOK
		if len(snap.Errors) > 0 {
			result = snapshotResultError
		}
		// Успешный сбор — жив каркас сводки (Totals есть ⇔ секция зон
		// собрана): очереди/пробы могут деградировать и дальше — их «н/д»
		// обслуживаются (§5.9), а не держат 503.
		if snap.Totals != nil {
			c.ready.Store(true)
		}
		c.snap.Store(snap)
		c.metrics.observe(result, time.Since(started).Seconds())
		if result == snapshotResultError {
			c.opt.Log.Warn("сборка снимка дашборда с ошибками секций", "errors", snap.Errors)
		} else {
			c.opt.Log.Debug("сборка снимка дашборда", "built_at", snap.BuiltAt)
		}
	}

	pass()
	ticker := time.NewTicker(c.opt.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// --- метрики собственного сервиса (ТЗД §10) -----------------------------------

// collectorMetrics — crawler_dashboard_snapshots_total{result} и
// crawler_dashboard_snapshot_seconds; nil-приёмник — no-op (сборщик без
// реестра, стиль monitor.NewNearGauge).
type collectorMetrics struct {
	snapshots *prometheus.CounterVec
	seconds   prometheus.ObserverVec
}

// newCollectorMetrics — приборы в реестре reg (nil — nil). Корзины
// гистограммы — экспонента от 50 мс до ~3 минут: сборка — это полные
// проходы агрегатов (status §«агрегаты и окна»), на боевом корпусе
// секунды, дефолтные корзины prometheus (потолок 10 с) малы.
func newCollectorMetrics(reg *metrics.Registry) *collectorMetrics {
	if reg == nil {
		return nil
	}
	return &collectorMetrics{
		snapshots: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_dashboard_snapshots_total",
			Help: "Сборки снимка дашборда по результату: ok — все секции живы, error — есть деградированные (снимок публикуется, ТЗД §5.9).",
		}, "result"),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_dashboard_snapshot_seconds",
			Help:    "Длительность сборки снимка дашборда (один проход всех секций, ТЗД §4.4).",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 13),
		}),
	}
}

// observe — фиксация прохода; nil — no-op.
func (m *collectorMetrics) observe(result string, seconds float64) {
	if m == nil {
		return
	}
	m.snapshots.WithLabelValues(result).Inc()
	m.seconds.WithLabelValues().Observe(seconds)
}
