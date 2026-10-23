// Метрики цикла retention (T-166, ТЗ 5.9): счётчики очищенных строк
// и байтов, гистограмма длительности прохода и счётчик пропусков
// интерлока — материал алертов «диск растёт» и «анализатор не успевает».
//
// Архитектурно — push, не полл: итоги проходов — возвращаемые значения
// RetentionOnce, долговременного состояния нет. Цикл RetentionLoop
// отдаёт итог хуком RetentionOptions.OnPass, коллектор переводит его в
// приборы — тот же шов, что CleanupOptions.OnPass (T-020/T-099),
// пакет retention.go зависимостей на метрики не получает.
package content

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// retentionSecondsBuckets — корзины длительности прохода: пачка до
// 50 тыс. строк (content.retention_batch) — секунды на живой БД, дренаж
// из ~13 пачек — десятки секунд; хвост (бэкфилл T-167, VACUUM-соседство)
// закрывают минуты.
var retentionSecondsBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// RetentionMetrics — приборы цикла retention: счётчики монотонно растут
// итогами проходов, гистограмма наблюдается каждый проход.
type RetentionMetrics struct {
	rows          prometheus.Counter     // crawler_content_retention_rows_total
	refs          prometheus.Counter     // crawler_content_retention_refs_total
	contents      prometheus.Counter     // crawler_content_retention_contents_total
	freed         prometheus.Counter     // crawler_content_retention_freed_bytes_total
	skips         prometheus.Counter     // crawler_content_retention_skips_total
	pendingErrors prometheus.Counter     // crawler_content_retention_pending_errors_total
	badHashes     prometheus.Counter     // crawler_content_retention_bad_hashes_total
	missing       prometheus.Counter     // crawler_content_retention_missing_refs_total
	seconds       prometheus.Observer    // crawler_content_retention_pass_seconds
	passes        *prometheus.CounterVec // crawler_content_retention_passes_total{result}
	deferred      *prometheus.CounterVec // crawler_content_retention_deferred_total{stream}
}

// NewRetentionMetrics регистрирует приборы retention в реестре reg;
// nil — nil (сервис без реестра — изолированные сборки). Идемпотентен
// (T-076): повторный вызов делит те же коллекторы.
func NewRetentionMetrics(reg *metrics.Registry) *RetentionMetrics {
	if reg == nil {
		return nil
	}
	return &RetentionMetrics{
		rows: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_rows_total",
			Help: "Строки subdomain_results, очищенные retention (T-166): срезаны content_hash и/или тяжёлые jsonb не-находок старше content.retention; паспорт строки остаётся.",
		}).WithLabelValues(),
		refs: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_refs_total",
			Help: "Срезанные ссылки content_hash (T-166): каждой соответствует −1 contents.refcount той же транзакцией.",
		}).WithLabelValues(),
		contents: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_contents_total",
			Help: "Удалённые строки contents, лишившиеся последней ссылки (T-166); физическое место блоков возвращает компактификация паков — сходимость видно по crawler_content_* / disk_avail.",
		}).WithLabelValues(),
		freed: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_freed_bytes_total",
			Help: "Место (сжатые байты) блоков, лишившихся ссылок (T-166): счётчик права на место, фактическое освобождение — компактификация паков (T-045).",
		}).WithLabelValues(),
		skips: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_skips_total",
			Help: "Пропуски retention по интерлоку: глубина q:analyze у порога content.retention_analyze_watermark — анализатор не успевает, тела и headers/cookies ещё нужны; рост = анализ отстаёт, диск продолжает расти.",
		}).WithLabelValues(),
		pendingErrors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_pending_errors_total",
			Help: "Пропуски retention по ошибке чтения непотреблённых ключей q:render/q:analyze (консервативно: записи очередей не тронуты, проход отложен); рост = защита не может прочитать стримы — Redis/доступ, разбираться сразу: без защиты окно чистки ждущих записей открыто.",
		}).WithLabelValues(),
		badHashes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_bad_hashes_total",
			Help: "Срезанные retention ссылки с content_hash неверной длины (16 октет, ТЗ 6.1): refcount по ним не менялся; в норме не растёт вовсе — рост = ошибка писателя результатов (T-056), аналог crawler_ingest_bad_hashes_total.",
		}).WithLabelValues(),
		missing: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_missing_refs_total",
			Help: "Срезанные retention ссылки без строки contents — расхождение учёта refcount (T-044); в норме не растёт: рост = баг учёта, разбираться по инвариантам T-044.",
		}).WithLabelValues(),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_content_retention_pass_seconds",
			Help:    "Длительность одного прохода retention (дренаж пачками, T-166).",
			Buckets: retentionSecondsBuckets,
		}).WithLabelValues(),
		passes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_passes_total",
			Help: "Проходы retention по итогам: ok — дренаж выполнен (в т.ч. пустой), skipped — интерлок q:analyze (тела ещё нужны), skipped_pending — ошибка чтения непотреблённых q:render/q:analyze (проход отложен); error-проходы не считаются — их видит Warn в логе.",
		}, "result"),
		deferred: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_deferred_total",
			Help: "Записи, исключённые retention из чистки: сообщения ждут потребителей стрима (q:render — рендер SPA, q:analyze — анализ; запись может ждать оба — считается в каждом). Растёт при застое очередей рядом с queue-метриками — сигнал дренировать потребителей (runbooks §5.2).",
		}, "stream"),
	}
}

// Observe — итог одного прохода (подключается хуком
// RetentionOptions.OnPass): счётчики и длительность. Нулевой итог
// значения не меняет; nil-приёмник — no-op (сервис без реестра), чтобы
// сборка подключала хук безусловно. Skipped учитывается меткой
// passes{result="skipped"} и счётчиком skips, SkippedPending — меткой
// passes{result="skipped_pending"} и счётчиком pendingErrors; пропуски
// посреди дренажа (интерлок/ошибка после части пачек) не теряют уже
// сделанной работы — Rows/Refs/Contents/Freed/Deferred зачитываются и
// в них, пачки-то зафиксированы в БД.
func (m *RetentionMetrics) Observe(res RetentionStats, d time.Duration) {
	if m == nil {
		return
	}
	if res.Skipped {
		m.skips.Inc()
		m.passes.WithLabelValues("skipped").Inc()
		m.observeWork(res)
		return
	}
	if res.SkippedPending {
		m.pendingErrors.Inc()
		m.passes.WithLabelValues("skipped_pending").Inc()
		m.observeWork(res)
		return
	}
	m.passes.WithLabelValues("ok").Inc()
	m.seconds.Observe(d.Seconds())
	m.observeWork(res)
}

// observeWork — счётчики сделанной работы прохода (общая часть веток
// ok/skipped/skipped_pending; BadHashes/Missing — только при наличии).
func (m *RetentionMetrics) observeWork(res RetentionStats) {
	m.rows.Add(float64(res.Rows))
	m.refs.Add(float64(res.Refs))
	m.contents.Add(float64(res.Contents))
	m.freed.Add(float64(res.FreedBytes))
	for stream, n := range res.Deferred {
		if n > 0 {
			m.deferred.WithLabelValues(stream).Add(float64(n))
		}
	}
	if res.BadHashes > 0 {
		m.badHashes.Add(float64(res.BadHashes))
	}
	if res.Missing > 0 {
		m.missing.Add(float64(res.Missing))
	}
}
