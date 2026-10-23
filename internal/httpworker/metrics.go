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
// лимитом чтения тела.
var (
	httpResponseBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	httpSecondsBuckets  = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	httpBodyBuckets     = []float64{1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20}
)

// Metrics — приборы HTTP-этапа; строится NewMetrics из реестра сервиса,
// nil — все методы no-op (изолированные сборки). Потокобезопасен:
// задачи идут параллельно, приборы prometheus синхронизированы.
type Metrics struct {
	tasks    prometheus.Counter     // crawler_stage_tasks_total{stage="http"}
	seconds  prometheus.Observer    // crawler_stage_seconds{stage="http"}
	errors   *prometheus.CounterVec // crawler_stage_errors_total{stage,type}
	statuses *prometheus.CounterVec // crawler_domain_status_total{type}
	response prometheus.Observer    // crawler_http_response_seconds
	rendered *prometheus.CounterVec // crawler_render_records_total{rendered}
	slots    prometheus.Gauge       // crawler_http_tasks_inflight (T-163)
	bodyHold prometheus.Gauge       // crawler_http_body_bytes_inflight (T-163)
	bodySize prometheus.Observer    // crawler_http_body_bytes (T-163)
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
	}
}

// Episode — задача q:http завершена успешно: +1 задача этапа и её
// длительность; финальный статус домена (recordStatus, §7); ошибки
// проверок по классам (§8), время ответа доступных некэшированных
// проверок; записи результата — в счётчик rendered=false (доля рендера,
// ТЗ 5.9). Кэш-попадания (Cached) временем ответа не наблюдаются — сеть
// не трогалась (ResponseMS кэша — перенесённое измерение первоисточника);
// суб-миллисекундные ответы честно ложатся в нижнюю корзину.
func (m *Metrics) Episode(res *DomainResult, d time.Duration) {
	if m == nil || res == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
	m.statuses.WithLabelValues(recordStatus(res).String()).Inc()
	m.rendered.WithLabelValues("false").Add(float64(res.RecordCount()))
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for _, check := range []SchemeCheck{sub.HTTPS, sub.HTTP} {
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
