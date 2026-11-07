// Приборы этапа рендера (T-077, ТЗ 5.9): пропускная способность (счётчик
// задач и гистограмма длительностей), ошибки по классам render.Class
// (task/context/browser — реакция этапа на сбой, T-061/T-063: повтор
// страницы, контекста или перезапуск браузера), доля рендера — ветка
// rendered=true счётчика crawler_render_records_total (ветку false
// наращивает HTTP-этап после записи результата; доля — отношением rate
// в PromQL), тарпит-финализации без рендера (T-148), финализации по
// постоянной ошибке сертификата хоста (T-159), финализации по DNS-отказу
// хоста (T-211), финализации ошибки класса задачи на последней попытке
// с меткой причины (T-172), публикации в очередь фолбэка с меткой
// причины (T-238, ТЗ §4.6 п.2 — числитель fallback-рейта, ФТ-15),
// события приостановки фолбэк-публикации с меткой причины gate|budget
// (T-239, ФТ-9, ТЗ §4.6 п.2), потребление фолбэка воркером B с меткой
// исхода rendered|finalized|skipped (T-245, ТЗ §4.6 п.2, ФТ-11/ФТ-15 —
// баланс published = consumed + остаток q:render-fallback, шаг 4
// приёмки ТЗ §6.2); метка движка engine на доле рендера,
// тарпит-финализациях (S-P2-4: тарпит — движко-зависимый отказ) и
// финализациях класса задачи (T-235, ТЗ §4.6 п.1, ФТ-15:
// кардинальность 3 — chromium/moli/lightpanda, значение — движок
// сборки воркера, Options.Engine ← render.engine; существующие
// агрегации без engine не меняются); предсоздание нулевых серий
// замкнутых словарей suspended{gate,budget} и
// consumed{rendered,finalized,skipped} (S-P1-4): семейства видны в
// экспозиции с первого старта, первое событие счётчика не теряется.
package renderworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/render"
)

// Package-уровневые константы ниже — словарь причин приостановки.
const (
	// fbReasonGate — health-гейт (ФТ-9а): стрик инфра-ошибок класса
	// браузера достиг render.fallback.health_gate_streak — основной
	// движок нездоров, фолбэк-публикация приостановлена до
	// восстановления (T-239).
	fbReasonGate = "gate"
	// fbReasonBudget — бюджет доли (ФТ-9б): доля публикаций за окно
	// ушла за потолок render.fallback.share_percent — следующее окно
	// без публикаций (T-239).
	fbReasonBudget = "budget"
)

// Словарь исходов потребления фолбэка (T-245, ФТ-15): метка result
// счётчика crawler_render_fallback_consumed_total — чем закончилась
// доставка q:render-fallback у воркера B.
const (
	// fbResultRendered — доработка успешна (ТЗ §4.5 п.2): DOM записан,
	// rendered = true, публикация q:analyze.
	fbResultRendered = "rendered"
	// fbResultFinalized — фолбэк исчерпан (ФТ-10): запись финализирована
	// с маркером render_failed_at, публикация q:analyze (пути тарпита
	// T-148 / класса задачи T-172 / cert T-159 / DNS T-211 воркера B).
	fbResultFinalized = "finalized"
	// fbResultSkipped — штатный отказ без побочных эффектов: строки нет
	// или без итогового URL (перепроверка переписала эпизод) — доставка
	// подтверждается без рендера и маркера.
	fbResultSkipped = "skipped"
)

// renderSecondsBuckets — корзины длительности задачи рендера: страница
// рендерится в изолированном контексте с ожиданием готовности до
// render.timeout (по умолчанию 25 с, ТЗ 5.6/5.10), быстрые статические
// страницы — единицы секунд.
var renderSecondsBuckets = []float64{0.5, 1, 2.5, 5, 10, 15, 20, 25, 30}

// Metrics — приборы этапа рендера; строится NewMetrics из реестра
// сервиса, nil — все методы no-op (изолированные сборки).
// Потокобезопасен: доставки идут параллельно, приборы prometheus
// синхронизированы.
type Metrics struct {
	tasks    prometheus.Counter     // crawler_stage_tasks_total{stage="render"}
	seconds  prometheus.Observer    // crawler_stage_seconds{stage="render"}
	errors   *prometheus.CounterVec // crawler_stage_errors_total{stage,type}
	rendered prometheus.Counter     // crawler_render_records_total{rendered="true",engine} (T-235)
	tarpits  *prometheus.CounterVec // crawler_render_tarpit_finalized_total{engine} (T-148/S-P2-4)
	certs    prometheus.Counter     // crawler_render_cert_finalized_total (T-159)
	dns      prometheus.Counter     // crawler_render_dns_finalized_total (T-211)
	tasksFin *prometheus.CounterVec // crawler_render_task_finalized_total{reason,engine} (T-172/T-235)
	fallback *prometheus.CounterVec // crawler_render_fallback_published_total{reason} (T-238)
	suspend  *prometheus.CounterVec // crawler_render_fallback_suspended_total{reason} (T-239)
	consumed *prometheus.CounterVec // crawler_render_fallback_consumed_total{result} (T-245)
	engine   string                 // значение метки engine — движок сборки воркера (T-235)
}

// NewMetrics регистрирует приборы этапа в реестре reg (каррированные по
// service и stage); nil — nil. Идемпотентен (T-076). engine — значение
// метки движка (T-235, ТЗ §4.6 п.1, ФТ-15) на crawler_render_records_total
// и crawler_render_task_finalized_total: движок сборки воркера
// (Options.Engine ← render.engine, словарь config.Engine*), кардинальность
// 3 — chromium/moli/lightpanda; словарь и связку «движок × remote_url ×
// порог памяти» уже проверили config.Validate и render.Options.validate,
// воркер значение не перепроверяет — неизвестный движок сборка рендера
// отвергает раньше старта воркера. Пустое значение — chromium, как у
// render.Options.Engine (сборки без конфигурации); нормализация здесь —
// кардинальность метки держит владелец приборов. Замкнутые словари
// событий приостановки (gate|budget) и потребления фолбэка
// (rendered|finalized|skipped) предсоздаются нулевыми сериями (S-P1-4,
// решение оператора): семейства видны в экспозиции с первого старта —
// алерты/панель не ждут первого события и не теряют его (до
// предсоздания первая точка ленивого счётчика базировала prev зонда и
// событие 0→1 пропускалось); открытые словари (reason кодов net::ERR_* —
// fallback{reason}, tasksFin{reason,engine}) предсоздать нельзя и не
// нужно: коды не перечислимы, серия рождается первой финализацией с
// этим кодом.
func NewMetrics(reg *metrics.Registry, engine string) *Metrics {
	if reg == nil {
		return nil
	}
	if engine == "" {
		engine = config.EngineChromium
	}
	m := &Metrics{
		engine: engine,
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные задачи этапов конвейера: dns — домены, http — домены, render/analyze — записи субдомен×вариант.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageRender),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Длительность обработки одной задачи этапа (эпизод домена/записи).",
			Buckets: renderSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageRender),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_errors_total",
			Help: "Ошибки этапов по типам: http — классы проверок (timeout, redirects, tls, body, network), render — классы рендера (task, context, browser).",
		}, metrics.LabelStage, metrics.LabelType),
		rendered: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_records_total",
			Help: "Записи результатов по признаку rendered: false пишет HTTP-этап, true — render-worker; доля рендера — отношение rate. Метка engine — движок рендера сборки воркера (chromium|moli|lightpanda, render.engine, T-235): у ветки false HTTP-этапа метки нет, агрегации и доля рендера без engine не меняются.",
		}, "rendered", metrics.LabelEngine).WithLabelValues("true", engine),
		tarpits: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_tarpit_finalized_total",
			Help: "Записи, финализированные без рендера после исчерпания render.timeout_attempts (T-148): итог — сырой HTML HTTP-фазы, запись уходит в q:analyze; рост — доля таймаут-тарпитов на рендере. Метка engine — движок рендера сборки воркера (chromium|moli|lightpanda, render.engine, S-P2-4): тарпит — движко-зависимый отказ (зависшая на лёгком движке страница может открыться в chromium), метка атрибутирует его движку; агрегации без engine не меняются.",
		}, metrics.LabelEngine),
		certs: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_cert_finalized_total",
			Help: "Записи, финализированные без рендера по постоянной ошибке TLS-сертификата хоста — net::ERR_CERT_AUTHORITY_INVALID / net::ERR_CERT_COMMON_NAME_INVALID (T-159): итог — сырой HTML HTTP-фазы, запись уходит в q:analyze, в q:dead cert-класс не попадает.",
		}).WithLabelValues(),
		dns: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_dns_finalized_total",
			Help: "Записи, финализированные без рендера по DNS-отказу хоста — net::ERR_NAME_NOT_RESOLVED (T-211): итог — сырой HTML HTTP-фазы, запись уходит в q:analyze, в q:dead DNS-класс не попадает.",
		}).WithLabelValues(),
		tasksFin: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_task_finalized_total",
			Help: "Записи, финализированные без рендера по ошибке класса задачи рендера на последней доставке (T-172): итог — сырой HTML HTTP-фазы, запись уходит в q:analyze, в q:dead не попадает. Метка reason — код net::ERR_* (например net::ERR_CONNECTION_REFUSED), timeout или other: состав того, что раньше уносило записи в q:dead. Метка engine — движок рендера сборки воркера (chromium|moli|lightpanda, render.engine, T-235): агрегации без engine не меняются.",
		}, "reason", metrics.LabelEngine),
		fallback: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_fallback_published_total",
			Help: "Задачи, опубликованные в очередь фолбэка q:render-fallback из путей финализации без рендера (T-238, ТЗ 4.4): движко-зависимые причины (тарпит T-148, класс задачи T-172 на последней доставке) дорабатывает chromium-воркер B, запись остаётся без маркера render_failed_at. Метка reason — словарь crawler_render_task_finalized_total (код net::ERR_*, timeout, other); числитель fallback-рейта (ФТ-15).",
		}, "reason"),
		suspend: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_fallback_suspended_total",
			Help: "События приостановки фолбэк-публикации (T-239, ФТ-9), не состояние — каждое защёлкивание одна точка: reason=gate — health-гейт, стрик инфра-ошибок класса браузера (основной движок недоступен) достиг render.fallback.health_gate_streak, публикация приостановлена до восстановления (успешный рендер обнуляет стрик; ошибки классов задачи/контекста стрик не трогают); reason=budget — бюджет доли, доля публикаций за окно в 1000 доставок этапа ушла за потолок render.fallback.share_percent, следующее окно без публикаций. Пока приостановка держится, кандидаты фолбэка финализируются штатно с маркером render_failed_at (как при выключенном фолбэке); browser-класс ждёт в redelivery q:render в пределах окна ретраев redis.max_attempts. Рост — сигнал деградации основного движка (алерты ТЗ 4.6 п.3). Серии замкнутого словаря gate|budget предсозданы нулевыми (S-P1-4): семейство в экспозиции с первого старта, события считает только вооружённая сборка (движок-кандидат, q:render — fallbackArmed).",
		}, "reason"),
		consumed: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_fallback_consumed_total",
			Help: "Задачи q:render-fallback, потреблённые chromium-воркером B (T-245, ТЗ 4.5/4.6 п.2, ФТ-11/ФТ-15), по исходу доставки, завершившейся обработкой без ошибки: result=rendered — доработка успешна (DOM записан, rendered=true, публикация q:analyze, ТЗ 4.5 п.2); result=finalized — фолбэк исчерпан, запись закрыта путём финализации (маркер render_failed_at + публикация q:analyze, ФТ-10: пути тарпита T-148 / класса задачи T-172 / cert T-159 / DNS T-211; маркер ставится, пока строка принадлежит эпизоду — гонка с перепроверкой T-058 закрывает запись без маркера); result=skipped — штатный отказ без побочных эффектов (строки нет, без итогового URL или охрана UPDATE отсекла — строка ушла из-под задачи). Счёт — в обработчике до XACK уровнем очереди: сбой Ack или повторная доставка дают дубль — допуск единиц at-least-once (как у окна бюджета T-239); ошибочные доставки не считаются — живут в PEL до клейма либо bury в q:dead. Баланс-чек приёмки (ТЗ 6.2 п.4): crawler_render_fallback_published_total (воркер A, T-238) = сумма этого счётчика + остаток q:render-fallback — crawler_queue_depth{queue=\"q:render-fallback\"} воркера B (backlog группы: непрочитанный лаг + PEL; держится до потолка MAXLEN стрима) + increase(crawler_queue_dead_buried_total{queue=\"q:render-fallback\"}) + increase(crawler_queue_dead_unidentified_total{queue=\"q:render-fallback\"}) (неидентифицируемые записи без task_id, T-260 — стационарно ноль). Считает только сборка воркера B (Options.Stream = q:render-fallback, ФТ-12): серии замкнутого словаря трёх исходов предсозданы нулевыми у всех сборок (S-P1-4 — семейство в экспозиции с первого старта), растут только у воркера B: у основной сборки живут нулями.",
		}, "result"),
	}
	// Предсоздание нулевых серий замкнутых словарей (S-P1-4): оба
	// семейства экспортируются с момента старта (нулями), алерт бюджета
	// и панель не ждут первого события; растут серии только в
	// вооружённой сборке (suspend — движок-кандидат, consumed — воркер
	// B), у остальных сборок живут нулями.
	m.suspend.WithLabelValues(fbReasonGate).Add(0)
	m.suspend.WithLabelValues(fbReasonBudget).Add(0)
	m.consumed.WithLabelValues(fbResultRendered).Add(0)
	m.consumed.WithLabelValues(fbResultFinalized).Add(0)
	m.consumed.WithLabelValues(fbResultSkipped).Add(0)
	return m
}

// Task — задача q:render обработана успешно (включая штатный отказ —
// строки нет или ушла из-под задачи): +1 задача этапа, длительность
// в гистограмму.
func (m *Metrics) Task(d time.Duration) {
	if m == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
}

// Rendered — DOM записан в строку (rendered = true, ТЗ 5.6): +1 в ветку
// rendered=true счётчика доли рендера (с меткой engine сборки, T-235).
func (m *Metrics) Rendered() {
	if m == nil {
		return
	}
	m.rendered.Inc()
}

// TarpitFinalized — запись финализирована без рендера по исчерпании
// лимита таймаутов (T-148): +1 в счётчик тарпит-финализаций с меткой
// engine сборки (S-P2-4) — движко-зависимый отказ атрибутируется
// движку (материал тарпит-рейта движка на панели).
func (m *Metrics) TarpitFinalized() {
	if m == nil {
		return
	}
	m.tarpits.WithLabelValues(m.engine).Inc()
}

// CertFinalized — запись финализирована без рендера по постоянной ошибке
// сертификата хоста (T-159): +1 в счётчик cert-финализаций — материал
// критерия «суточный прирост q:dead от cert-класса = 0» (рост счётчика
// при нулевом приросте q:dead — политика работает).
func (m *Metrics) CertFinalized() {
	if m == nil {
		return
	}
	m.certs.Inc()
}

// DNSFinalized — запись финализирована без рендера по DNS-отказу хоста
// (T-211): +1 в счётчик DNS-финализаций — материал критерия «−2
// доставки и −20 мин PEL на мёртвом хосте» (замер T-207: 252+186
// финализаций в сутки; рост счётчика при нулевом приросте q:dead —
// политика работает).
func (m *Metrics) DNSFinalized() {
	if m == nil {
		return
	}
	m.dns.Inc()
}

// TaskFinalized — запись финализирована без рендера по ошибке класса
// задачи на последней доставке (T-172): +1 в счётчик финализаций с
// меткой причины (код net::ERR_* / timeout / other) и меткой engine
// сборки (T-235) — материал критерия «суточный прирост q:dead от
// q:render = 0» и видимости состава причин (что именно доезжало до
// q:dead прежде).
func (m *Metrics) TaskFinalized(reason string) {
	if m == nil {
		return
	}
	m.tasksFin.WithLabelValues(reason, m.engine).Inc()
}

// FallbackPublished — задача опубликована в q:render-fallback из пути
// финализации без рендера (T-238): +1 с меткой причины (словарь
// taskFinalizeReason — код net::ERR_* / timeout / other, тот же, что у
// TaskFinalized). Числитель fallback-рейта (ФТ-15: доля публикаций
// против потока этапа — штатный индикатор здоровья основного движка).
// Дубль доставки (стрим уже ждёт запись) методом не считается —
// публикация не выполнялась (publishFallback).
func (m *Metrics) FallbackPublished(reason string) {
	if m == nil {
		return
	}
	m.fallback.WithLabelValues(reason).Inc()
}

// FallbackSuspended — событие приостановки фолбэк-публикации (T-239,
// ФТ-9, ТЗ §4.6 п.2): +1 с меткой причины (замкнутый словарь
// fbReasonGate/fbReasonBudget — «gate»/«budget»). Событие, не
// состояние: одна точка на защёлкивание приостановки — гейт закрылся
// стриком инфра-ошибок класса браузера (движок нездоров) или окно
// бюджета перерасходовало потолок доли; пока приостановка держится,
// кандидаты финализируются штатно с маркером. Материал алертов
// деградации основного движка (ТЗ §4.6 п.3).
func (m *Metrics) FallbackSuspended(reason string) {
	if m == nil {
		return
	}
	m.suspend.WithLabelValues(reason).Inc()
}

// FallbackConsumed — доставка q:render-fallback обработана воркером B
// без ошибки (T-245, ТЗ §4.6 п.2, ФТ-15): +1 с меткой исхода (замкнутый
// словарь fbResult*): rendered — доработка успешна, finalized — запись
// закрыта путём финализации после исчерпания фолбэка (ФТ-10), skipped —
// штатный отказ без побочных эффектов. Знаменатель баланса published =
// consumed + остаток q:render-fallback + похороненные в q:dead (шаг 4
// приёмки ТЗ §6.2): доставка без ошибки уходит из PEL стрима XACK'ом
// уровнем очереди (счёт до него — дубль от сбоя XACK допустим, единицы
// at-least-once), ошибочная — живёт в PEL до клейма либо bury (в баланс
// — членом crawler_queue_dead_buried_total). Вызывает только сборка
// воркера B (Options.Stream = q:render-fallback, worker.fbConsume).
func (m *Metrics) FallbackConsumed(result string) {
	if m == nil {
		return
	}
	m.consumed.WithLabelValues(result).Inc()
}

// Error — задача рендера не подтверждена (вернётся повторной доставкой):
// +1 в счётчик ошибок этапа по классу render.ClassOf.
func (m *Metrics) Error(class render.Class) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(metrics.StageRender, class.String()).Inc()
}
