// The HTTP stage's instruments (T-077, spec 5.9): throughput (the task
// counter and the duration histogram), check errors by type
// (ErrorClass → LabelType: timeout/redirects/tls/body/network, §8),
// the final domain statuses (alive/no_http/temp_error, §7), the response
// time (the ResponseMS histogram of the final page, spec 5.5 item 7) and the
// render share — the rendered=false records counter (written by the HTTP stage; the
// rendered=true branch is incremented by the render-worker with the same counter, the share —
// as the rate ratio in PromQL). The status and record counters are counted over
// a completed episode (after a successful OnResult — the records are committed
// by the T-058 transaction): the race with domain deletion (§13.1) is not distinguished,
// at-least-once repeats are counted as work (spec 3.4).
//
// The slot memory (T-163, the ≤1.5 MiB/slot criterion — docs/load-measurements.md
// §12): bodies are not held in the slot (the early Put of T-147), the remainder —
// the transit buffers of the bodies being read and the metadata; the instruments
// below make the remainder measurable by the sampler: the busy slots (the "memory/slot"
// denominator), the body bytes in processing right now and the body size
// distribution (the mix defining the peak). The soft heap cap — http.mem_limit (the
// cmd/http-worker build, debug.SetMemoryLimit): the GC keeps anon below
// the unit's MemoryHigh without giving the memory to the kernel's reclaim storms (the
// T-157 fatals happened exactly at the boundary of the cgroup caps).
//
// The per-IP semaphore wait (T-165): the head-of-line parking IPs
// (per_ip_limit with a FIFO queue stretches tasks to 20 s, the finding
// of T-092 §3) made measurable — the slot wait histogram
// crawler_http_per_ip_wait_seconds (the observations are plugged in by the
// service build as httpfetch.LimitOptions.OnPerIPWait); the share of waits > 2 s —
// the activation criterion of the head-of-line countermeasures (< 5% — the norm).
package httpworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// The stage histograms' buckets. The response time — up to the final
// page's headers (the ResponseMS ms, spec 5.5 item 7): local sites are tens of
// milliseconds, the stage timeouts (connect 5 s, the overall ~10 s, spec 5.10)
// trim the tail with the 5/10 s buckets. The domain task duration — up to all
// the subdomain checks by the parallel pool with per-IP limits (T-049):
// from fractions of a second to tens of seconds on "silent" domains with timeouts.
// The body size (T-163) — from the parkings' empty pages to the
// max_body_bytes cap (2 MiB): the powers-of-four buckets, the tail truncated
// by the body read limit. The per-IP semaphore wait (T-165) — from
// the microseconds of a free slot to the total_timeout tail (~20 s):
// the le=2 bucket is singled out by the boundary — the share of waits > 2 s (the
// T-165 criterion: < 5% on the production mix) is read directly as
// 1 − rate(…_bucket{le="2"})/rate(…_count).
var (
	httpResponseBuckets  = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	httpSecondsBuckets   = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	httpBodyBuckets      = []float64{1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20}
	httpPerIPWaitBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30}
)

// Metrics — the HTTP stage's instruments; built by NewMetrics from the service's
// registry, nil — all methods are no-ops (isolated builds). Concurrency-safe:
// tasks run in parallel, the prometheus instruments are synchronized.
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

// NewMetrics registers the HTTP stage's instruments in the reg registry (carried
// by service, the fixed label values — at the site); nil — nil.
// Idempotent (T-076): a repeated call shares the same collectors.
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
		//		// The 40P01 transaction-retry counter (T-268): the name/HELP
		//		// shared by all services — the internal/metrics constructor.
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

// Episode — a q:http task completed successfully: +1 stage task and its
// duration; the domain's final status (recordStatus, §7); the check
// errors by class (§8), the response time of the available non-cached
// checks; the result records — into the rendered=false counter (the render share,
// spec 5.9). Cache hits (Cached) are not observed by the response time — the network
// was not touched (the cache's ResponseMS — the primary source's measurement carried over);
// sub-millisecond responses honestly land in the lowest bucket.
// Dead-host cutoff (T-164): a task with the Cutoff sign — into a separate
// counter, the skipped checks (the ErrCutoff class) — into the
// saved checks counter; they do not count as stage errors — the check
// was not performed.
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

// SlotEnter/SlotLeave — the task slot occupancy (T-163): processDelivery
// takes the slot for the whole processing time (both entry points — the Run dispatcher
// and the RecoverLoop claims), the method pair must converge to zero on an idle
// pipeline. The counter — the "unit memory / slot" denominator when measuring
// the criterion by the sampler (docs/load-measurements.md §12, T-157/T-163).
func (m *Metrics) SlotEnter() {
	if m == nil {
		return
	}
	m.slots.Inc()
}

// SlotLeave — the slot release (see SlotEnter).
func (m *Metrics) SlotLeave() {
	if m == nil {
		return
	}
	m.slots.Dec()
}

// BodyHeld — the bytes of the body in processing (T-163): checkScheme
// holds the read body from completing the read to the early Put
// (UTF-8/hash/SPA/compression); +n on entering the window, −n on exiting. The
// plus converges to zero always (defer on leaving the hold window).
func (m *Metrics) BodyHeld(n int64) {
	if m == nil {
		return
	}
	m.bodyHold.Add(float64(n))
}

// BodySize — the size of a successfully read body (T-163): the distribution of
// the mix (parkings — tens of KiB, heavy pages — up to the
// max_body_bytes cap) explains the bodyHold peaks when measuring the slot memory.
func (m *Metrics) BodySize(n int) {
	if m == nil {
		return
	}
	m.bodySize.Observe(float64(n))
}

// PerIPWait — the per-IP semaphore slot wait (T-165): the histogram
// observation after each successful slot grant; wired by the
// service build (cmd/http-worker) as httpfetch.LimitOptions.OnPerIPWait.
func (m *Metrics) PerIPWait(wait time.Duration) {
	if m == nil {
		return
	}
	m.perIPWait.Observe(wait.Seconds())
}

// DBDeadlockRetried — the DB transaction retried after a 40P01 deadlock
// (T-268): +1 to the counter; the db.RetryDeadlock hook of the result
// writer (attempt — the retry number, one row, the label reserved for the service).
func (m *Metrics) DBDeadlockRetried(attempt int) {
	if m == nil {
		return
	}
	m.dbRetried.Inc()
}

// LateRepublished — the late re-publisher published n q:render
// re-publications (T-259, FR-5.3): +n to crawler_render_late_republish_total;
// called by the LateOnce pass upon the fact of publication (Full/error do not
// count — there was no publication).
func (m *Metrics) LateRepublished(n int) {
	if m == nil || n <= 0 {
		return
	}
	m.late.Add(float64(n))
}

// LateError — an erroneous late re-publisher pass (T-273): +1 to
// crawler_render_late_errors_total; called by the RenderLateLoop loop
// next to the Warn (a ctx cancellation — a graceful stop, not counted).
func (m *Metrics) LateError() {
	if m == nil {
		return
	}
	m.lateErrs.Inc()
}
