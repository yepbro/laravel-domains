// Приборы этапа анализа (T-077, ТЗ 5.9): пропускная способность — счётчик
// задач и гистограмма длительностей. Отдельных счётчиков ошибок/статусов
// этап не имеет: анализ пассивен (запросов наружу нет, ТЗ 5.8), сбой
// доставки — повтор уровня очереди, а его результаты (findings,
// is_laravel) — данные БД, не метрики процесса.
package analyzer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// analyzeSecondsBuckets — корзины длительности задачи анализа: RE2-матчинг
// по ограниченному входу — микросекунды (T-069), пара операторов БД —
// миллисекунды; корзины покрывают и аномалии (контент на пределе
// лимитов, медленная БД).
var analyzeSecondsBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}

// Metrics — приборы этапа анализа; строится NewMetrics из реестра
// сервиса, nil — все методы no-op (изолированные сборки).
// Потокобезопасен: доставки идут параллельно, приборы prometheus
// синхронизированы.
type Metrics struct {
	tasks   prometheus.Counter  // crawler_stage_tasks_total{stage="analyze"}
	seconds prometheus.Observer // crawler_stage_seconds{stage="analyze"}
}

// NewMetrics регистрирует приборы этапа в реестре reg (каррированные по
// service и stage); nil — nil. Идемпотентен (T-076).
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные задачи этапов конвейера: dns — домены, http — домены, render/analyze — записи субдомен×вариант.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageAnalyze),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Длительность обработки одной задачи этапа (эпизод домена/записи).",
			Buckets: analyzeSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageAnalyze),
	}
}

// Task — задача q:analyze обработана успешно (включая штатный отказ —
// строки нет или она ушла из-под задачи): +1 задача этапа, длительность
// в гистограмму.
func (m *Metrics) Task(d time.Duration) {
	if m == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
}
