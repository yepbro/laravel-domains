// Приборы публичной формы проверки домена (T-221): исходы запросов
// POST /api/check-domain, сбои по классам и длительность обработки —
// здоровая работа формы видна из Prometheus (:9107), а не только из
// journald. Семантика меток — как у приборов этапов (T-077): service
// подставляет фабрика реестра, result/type локальны для формы.
package web

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// Значения метки result счётчика crawler_web_checks_total — исход
// запроса формы, ровно один на запрос.
const (
	// checkResultOK — 200: живая проверка прошла.
	checkResultOK = "ok"
	// checkResultStoredOnly — 200: живая проверка не удалась, ответ
	// собран из сохранённых данных конвейера.
	checkResultStoredOnly = "stored_only"
	// checkResultFailed — 502: живая проверка не удалась, сохранённых
	// данных нет.
	checkResultFailed = "failed"
	// checkResultBusy — 503: семафор одновременных живых проверок полон
	// (T-220), быстрый отказ с Retry-After.
	checkResultBusy = "busy"
	// checkResultRateLimited — 429: исчерпан рейт-лимит формы.
	checkResultRateLimited = "rate_limited"
	// checkResultInvalid — 422: мусорный ввод (формат landing.js).
	checkResultInvalid = "invalid"
)

// Значения метки type счётчика crawler_web_check_errors_total —
// класс сбоя формы (метка этапов LabelStage занята конвейером
// dns/http/render/analyze и для локальных рядов не используется).
const (
	// checkErrLive — конвейер живой проверки не удался (не перегруз;
	// перегруз — штатный исход busy, не ошибка).
	checkErrLive = "live"
	// checkErrJournal — INSERT web_checks (T-120) не удался: журнал
	// ручных проверок теряет строку (до T-221 терял молча).
	checkErrJournal = "journal"
	// checkErrNonIP — адрес клиента не IP (пир unix-сокета без валидного
	// заголовка прокси): запись web_checks пропущена — колонка
	// ip_address inet NOT NULL не принимает произвольную строку.
	checkErrNonIP = "non_ip"
)

// webCheckSecondsBuckets — корзины длительности запроса формы: потолок
// web.timeout 60 с (DNS + HTTP обеих схем + рендер SPA), лёгкие домены
// — единицы секунд.
var webCheckSecondsBuckets = []float64{0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 120}

// Metrics — приборы формы; строится NewMetrics из реестра сервиса,
// nil (реестра нет — изолированные тесты) — все методы no-op.
// Потокобезопасен: приборы prometheus синхронизированы.
type Metrics struct {
	checks  *prometheus.CounterVec // crawler_web_checks_total{result}
	errors  *prometheus.CounterVec // crawler_web_check_errors_total{stage}
	seconds prometheus.Observer    // crawler_web_check_seconds
}

// NewMetrics регистрирует приборы формы в реестре reg; nil — nil.
// Идемпотентен (T-076): повторный вызов делит те же коллекторы.
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		checks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_web_checks_total",
			Help: "Запросы формы проверки домена по исходам: ok — живая проверка прошла (200), stored_only — живая сбоила, ответ из сохранённых данных (200), failed — живая сбоила без сохранённых данных (502), busy — перегруз семафора живых проверок (503), rate_limited — рейт-лимит формы (429), invalid — мусорный ввод (422).",
		}, "result"),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_web_check_errors_total",
			Help: "Сбои формы проверки по классам: live — конвейер живой проверки не удался (перегруз — исход busy, не ошибка), journal — INSERT web_checks не удался (строка журнала потеряна), non_ip — адрес клиента не IP (unix-сокет без валидного заголовка прокси), запись журнала пропущена.",
		}, metrics.LabelType),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_web_check_seconds",
			Help:    "Полное время обработки запроса формы, прошедшего лимит и валидацию (stored-чтение, живая проверка, запись журнала; 429/422/перегруз 503 не наблюдаются — работы не выполняли).",
			Buckets: webCheckSecondsBuckets,
		}).WithLabelValues(),
	}
}

// Outcome — исход запроса формы: +1 в счётчик по result.
func (m *Metrics) Outcome(result string) {
	if m == nil {
		return
	}
	m.checks.WithLabelValues(result).Inc()
}

// Failure — сбой формы по классу: +1 в счётчик по type.
func (m *Metrics) Failure(kind string) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(kind).Inc()
}

// Duration — длительность обработки запроса, дошедшего до живой
// проверки.
func (m *Metrics) Duration(d time.Duration) {
	if m == nil {
		return
	}
	m.seconds.Observe(d.Seconds())
}
