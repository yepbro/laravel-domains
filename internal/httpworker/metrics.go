// Приборы HTTP-этапа (T-077, ТЗ 5.9): пропускная способность (счётчик
// задач и гистограмма длительностей), ошибки проверок по типам
// (ErrorClass → LabelType: timeout/redirects/tls/body/network, §8),
// финальные статусы доменов (alive/no_http/temp_error, §7), время
// ответа (гистограмма ResponseMS итоговой страницы, ТЗ 5.5 п. 7) и доля
// рендера — счётчик записей rendered=false (пишет HTTP-этап; ветку
// rendered=true наращивает render-worker тем же счётчиком, доля —
// отношением rate в PromQL). Счётчики статусов и записей считаются по
// завершённому эпизоду (после успешного OnResult — записи зафиксированы
// транзакцией T-058): гонка с удалением домена (§13.1) не различается,
// at-least-once повторы учитываются как работа (ТЗ 3.4).
//
// Память слота (T-163, критерий ≤1.5 MiB/слот — docs/load-measurements.md
// §12): тела в слоте не удерживаются (ранний Put T-147), остаток —
// транзитные буферы читаемых тел и метаданные; приборы ниже делают
// остаток измеримым сэмплером: занятые слоты (знаменатель «память/слот»),
// байты тел в обработке прямо сейчас и распределение размеров тел (смесь,
// определяющая пик). Мягкий потолок кучи — http.mem_limit (сборка
// cmd/http-worker, debug.SetMemoryLimit): GC удерживает anon ниже
// MemoryHigh юнита, не отдавая память reclaim-штормам ядра (фаталы
// T-157 случились именно на границе потолков cgroup).
//
// Ожидание per-IP семафора (T-165): head-of-line парковочных IP
// (per_ip_limit при FIFO-очереди растягивает задачи к 20 с, находка
// T-092 §3) сделано измеримым — гистограмма ожидания слота
// crawler_http_per_ip_wait_seconds (наблюдения подставляет сборка
// сервиса как httpfetch.LimitOptions.OnPerIPWait); доля ожиданий > 2 с —
// критерий активации мер против head-of-line (< 5 % — норма).
package httpworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// Корзины гистограмм этапа. Время ответа — до заголовков итоговой
// страницы (мс ResponseMS, ТЗ 5.5 п. 7): локальные сайты десятки
// миллисекунд, таймауты этапа (connect 5 с, общий ~10 с, ТЗ 5.10)
// обрезают хвост корзинами 5/10 с. Длительность задачи домена — до всех
// проверок субдоменов параллельным пулом с per-IP-лимитами (T-049):
// от долей секунды до десятков секунд на «молчащие» домены с таймаутами.
// Размер тела (T-163) — от пустых страниц парковок до потолка
// max_body_bytes (2 МиБ): корзины степеней четвёрки, хвост обрезан
// лимитом чтения тела. Ожидание per-IP семафора (T-165) — от
// микросекунд свободного слота до хвоста total_timeout (~20 с):
// корзина le=2 выделена границей — доля ожиданий > 2 с (критерий
// T-165: < 5 % на смеси боя) читается напрямую как
// 1 − rate(…_bucket{le="2"})/rate(…_count).
var (
	httpResponseBuckets  = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	httpSecondsBuckets   = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	httpBodyBuckets      = []float64{1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20}
	httpPerIPWaitBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30}
)

// Metrics — приборы HTTP-этапа; строится NewMetrics из реестра сервиса,
// nil — все методы no-op (изолированные сборки). Потокобезопасен:
// задачи идут параллельно, приборы prometheus синхронизированы.
type Metrics struct {
	tasks     prometheus.Counter     // crawler_stage_tasks_total{stage="http"}
	seconds   prometheus.Observer    // crawler_stage_seconds{stage="http"}
	errors    *prometheus.CounterVec // crawler_stage_errors_total{stage,type}
	statuses  *prometheus.CounterVec // crawler_domain_status_total{type}
	response  prometheus.Observer    // crawler_http_response_seconds
	rendered  *prometheus.CounterVec // crawler_render_records_total{rendered}
	slots     prometheus.Gauge       // crawler_http_tasks_inflight (T-163)
	bodyHold  prometheus.Gauge       // crawler_http_body_bytes_inflight (T-163)
	bodySize  prometheus.Observer    // crawler_http_body_bytes (T-163)
	perIPWait prometheus.Observer    // crawler_http_per_ip_wait_seconds (T-165)
	cutoff    prometheus.Counter     // crawler_http_dead_host_cutoff_tasks_total (T-164)
	cutSkip   prometheus.Counter     // crawler_http_dead_host_cutoff_skipped_checks_total (T-164)
	dbRetried prometheus.Counter     // crawler_db_deadlock_retried_total (T-268)
	late      prometheus.Counter     // crawler_render_late_republish_total (T-259)
	lateErrs  prometheus.Counter     // crawler_render_late_errors_total (T-273)
}

// NewMetrics регистрирует приборы HTTP-этапа в реестре reg (каррированные
// по service, фиксированные метки значений — по месту); nil — nil.
// Идемпотентен (T-076): повторный вызов делит те же коллекторы.
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные задачи этапов конвейера: dns — домены, http — домены, render/analyze — записи субдомен×вариант.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageHTTP),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Длительность обработки одной задачи этапа (эпизод домена/записи).",
			Buckets: httpSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageHTTP),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_errors_total",
			Help: "Ошибки этапов по типам: http — классы проверок (timeout, redirects, tls, body, network), render — классы рендера (task, context, browser).",
		}, metrics.LabelStage, metrics.LabelType),
		statuses: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_domain_status_total",
			Help: "Финальные статусы доменов по итогам эпизодов (ТЗ 6.2): DNS-этап — not_delegated/delegated_no_address/temp_error, HTTP-этап — alive/no_http/temp_error.",
		}, metrics.LabelType),
		response: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_http_response_seconds",
			Help:    "Время ответа итоговой страницы HTTP-проверки (до заголовков, без кэш-попаданий).",
			Buckets: httpResponseBuckets,
		}).WithLabelValues(),
		rendered: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_records_total",
			Help: "Записи результатов по признаку rendered: false пишет HTTP-этап, true — render-worker; доля рендера — отношение rate.",
		}, "rendered"),
		slots: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_http_tasks_inflight",
			Help: "Занятые слоты задач q:http прямо сейчас (обработки в полёте, включая клеймы RecoverLoop): знаменатель арифметики «память юнита / слот» при замере критерия T-163.",
		}).WithLabelValues(),
		bodyHold: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_http_body_bytes_inflight",
			Help: "Байты тел страниц, находящихся в обработке прямо сейчас (чтение→UTF-8→хэш→SPA→ранний Put, T-147/T-163): транзитная составляющая памяти слота; удерживаемых тел нет — пики этой метрики и есть буферы чтения.",
		}).WithLabelValues(),
		bodySize: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_http_body_bytes",
			Help:    "Размеры прочитанных тел итоговых страниц (байт): смесь, определяющая пик транзитных буферов слота (T-163); хвост обрезан http.max_body_bytes.",
			Buckets: httpBodyBuckets,
		}).WithLabelValues(),
		perIPWait: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_http_per_ip_wait_seconds",
			Help:    "Ожидание слота per-IP семафора соединений перед установкой соединения (T-165): каждое успешное взятие слота, включая мгновенное. Доля ожиданий > 2 с (критерий T-165: < 5% на смеси боя) — 1 − rate(…_bucket{le=\"2\"})/rate(…_count); корзина le=2 — граница критерия. Диал — единица наблюдения: задача домена делает несколько подключений (субдомены × протоколы × ходы цепочки).",
			Buckets: httpPerIPWaitBuckets,
		}).WithLabelValues(),
		cutoff: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_http_dead_host_cutoff_tasks_total",
			Help: "Задачи q:http, завершённые dead-host cutoff (T-164): apex и www упали на connect-фазе (connection refused/connect-таймаут, оба протокола), остальные субдомены не проверялись; записи — по §8, домен несёт бит dead_host_cutoff. Контроль критерия mean задачи ≤ 7 с — crawler_stage_seconds{stage=\"http\"}.",
		}).WithLabelValues(),
		cutSkip: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_http_dead_host_cutoff_skipped_checks_total",
			Help: "Проверки (субдомен × протокол), не выполненные из-за dead-host cutoff (T-164), — сэкономленный пул задачи. Доля ложных cutoff (<1%, критерий T-164) меряется контрольной выборкой по битам dead_host_cutoff в domains.flags.",
		}).WithLabelValues(),
		// Счётчик 40P01-повторов транзакции записи (T-268): единые
		// имя/HELP для всех сервисов — конструктор internal/metrics.
		dbRetried: metrics.NewDBDeadlockRetried(reg),
		late: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_late_republish_total",
			Help: "Переиздания q:render поздним переиздателем (T-259, ТЗ-fsm ФТ-5.3): «вечно молодые» записи (rendered = false, final_url не пуст, без маркера render_failed_at) старше 6 ч без бита render_pending и без непотреблённых сообщений q:render/q:render-fallback — потерянные MAXLEN-усечением и сбоями публикации задачи; стационарно не растёт, рост после инцидента — дренаж потерь, спад до нуля — дренаж завершён.",
		}).WithLabelValues(),
		lateErrs: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_late_errors_total",
			Help: "Ошибочные проходы позднего переиздателя рендера (T-273): выборка/снимок/издание пачки не завершились — курсор не двигается, пачка повторится после backoff. До T-273 ошибки были видны только в журнале (инцидент 09–10.11.2026: 70+ WARN-ов за сутки при нулевой метрике переизданий); рост = переиздатель не работает, при render_pending > 0 рендер-долг не вернётся в q:render — искать заголовок Warn в journalctl.",
		}).WithLabelValues(),
	}
}

// Episode — задача q:http завершена успешно: +1 задача этапа и её
// длительность; финальный статус домена (recordStatus, §7); ошибки
// проверок по классам (§8), время ответа доступных некэшированных
// проверок; записи результата — в счётчик rendered=false (доля рендера,
// ТЗ 5.9). Кэш-попадания (Cached) временем ответа не наблюдаются — сеть
// не трогалась (ResponseMS кэша — перенесённое измерение первоисточника);
// суб-миллисекундные ответы честно ложатся в нижнюю корзину.
// Dead-host cutoff (T-164): задача с признаком Cutoff — в отдельный
// счётчик, пропущенные проверки (класс ErrCutoff) — в счётчик
// сэкономленных проверок; ошибками этапа они не считаются — проверка
// не выполнялась.
func (m *Metrics) Episode(res *DomainResult, d time.Duration) {
	if m == nil || res == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
	m.statuses.WithLabelValues(recordStatus(res).String()).Inc()
	m.rendered.WithLabelValues("false").Add(float64(res.RecordCount()))
	if res.Cutoff {
		m.cutoff.Inc()
	}
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for _, check := range []SchemeCheck{sub.HTTPS, sub.HTTP} {
			if check.ErrClass == ErrCutoff {
				m.cutSkip.Inc()
				continue
			}
			if check.ErrClass != ErrNone {
				m.errors.WithLabelValues(metrics.StageHTTP, check.ErrClass.String()).Inc()
			}
			if check.Available && !check.Cached {
				m.response.Observe(float64(check.ResponseMS) / 1000)
			}
		}
	}
}

// SlotEnter/SlotLeave — занятость слота задачи (T-163): processDelivery
// занимает слот на всё время обработки (обе точки входа — диспетчер Run
// и клеймы RecoverLoop), пара метода обязана сходиться к нулю на пустом
// конвейере. Счётчик — знаменатель «память юнита / слот» при замере
// критерия сэмплером (docs/load-measurements.md §12, T-157/T-163).
func (m *Metrics) SlotEnter() {
	if m == nil {
		return
	}
	m.slots.Inc()
}

// SlotLeave — освобождение слота (см. SlotEnter).
func (m *Metrics) SlotLeave() {
	if m == nil {
		return
	}
	m.slots.Dec()
}

// BodyHeld — байты тела, находящегося в обработке (T-163): checkScheme
// держит прочитанное тело от завершения чтения до раннего Put
// (UTF-8/хэш/SPA/сжатие); +n при входе в окно, −n на выходе. Плюс к
// нулю сходится всегда (defer по выходу из окна удержания).
func (m *Metrics) BodyHeld(n int64) {
	if m == nil {
		return
	}
	m.bodyHold.Add(float64(n))
}

// BodySize — размер успешно прочитанного тела (T-163): распределение
// смеси (парковки — десятки КиБ, тяжёлые страницы — до потолка
// max_body_bytes) объясняет пики bodyHold при замере памяти слота.
func (m *Metrics) BodySize(n int) {
	if m == nil {
		return
	}
	m.bodySize.Observe(float64(n))
}

// PerIPWait — ожидание слота per-IP семафора (T-165): наблюдение
// гистограммы после каждой успешной выдачи слота; подключается сборка
// сервиса (cmd/http-worker) как httpfetch.LimitOptions.OnPerIPWait.
func (m *Metrics) PerIPWait(wait time.Duration) {
	if m == nil {
		return
	}
	m.perIPWait.Observe(wait.Seconds())
}

// DBDeadlockRetried — транзакция БД повторена после дедлока 40P01
// (T-268): +1 к счётчику; хук db.RetryDeadlock записателя результата
// (attempt — номер повтора, ряд один, метка зарезервирована за сервисом).
func (m *Metrics) DBDeadlockRetried(attempt int) {
	if m == nil {
		return
	}
	m.dbRetried.Inc()
}

// LateRepublished — поздний переиздатель опубликовал n переизданий
// q:render (T-259, ФТ-5.3): +n к crawler_render_late_republish_total;
// вызывается проходом LateOnce по факту публикации (Full/ошибка не
// считаются — издания не было).
func (m *Metrics) LateRepublished(n int) {
	if m == nil || n <= 0 {
		return
	}
	m.late.Add(float64(n))
}

// LateError — ошибочный проход позднего переиздателя (T-273): +1 к
// crawler_render_late_errors_total; вызывается циклом RenderLateLoop
// рядом с Warn (отмена ctx — штатная остановка, не считается).
func (m *Metrics) LateError() {
	if m == nil {
		return
	}
	m.lateErrs.Inc()
}
