// Снимок состояния конвейера (ТЗД §4.4, §5, T-129): единичный срез всех
// секций данных дашборда. Собирается фоновым циклом (collector.go) и
// атомарно заменяет предыдущий; HTTP-хендлеры отдают последний готовый
// снимок — нагрузка на БД не зависит от числа открытых вкладок (ТЗД
// §3.2.3).
//
// Деградация (ТЗД §5.9): секции собираются независимо, ошибка секции
// (БД, Redis) не роняет остальные — секция получает null в JSON, причина
// — в карте errors с текстом; интерфейс показывает «н/д». Значения
// внутри секций, которые не определились (скорость без окна, ETA без
// скорости), — null по тем же правилам, что в CLI (internal/status).
//
// Пробы живости (ТЗД §5.8) — не секция с ошибкой: недоступный или
// медленный сервис — данные («имя + up/down»), а не деградация; причина
// down лежит в самой строке пробы.
package dashboard

import (
	"time"

	"crawler/internal/status"
)

// LaravelItemJSON — строка «последние находки» (ТЗД §5.6): домен, зона,
// score, диапазон версий (пусто — не определён), updated_at (RFC3339).
type LaravelItemJSON struct {
	Domain    string  `json:"domain"`
	Zone      string  `json:"zone"`
	Score     float32 `json:"score"`
	VerMin    string  `json:"ver_min,omitempty"`
	VerMax    string  `json:"ver_max,omitempty"`
	UpdatedAt string  `json:"updated_at"`
}

// LaravelJSON — секция Laravel-находок (ТЗД §5.6): всего, за 24 часа,
// последние 10.
type LaravelJSON struct {
	Total   int64             `json:"total"`
	Last24h int64             `json:"last_24h"`
	Recent  []LaravelItemJSON `json:"recent"`
}

// IngestFileJSON — строка «последние ingest-файлы» (ТЗД §5.7): файл,
// действие add/delete (пусто — NULL), счётчики, processed_at (RFC3339;
// пусто — NULL).
type IngestFileJSON struct {
	Path        string `json:"path"`
	Action      string `json:"action,omitempty"`
	LinesTotal  int64  `json:"lines_total"`
	Added       int64  `json:"added"`
	Existed     int64  `json:"existed"`
	Deleted     int64  `json:"deleted"`
	Rejected    int64  `json:"rejected"`
	ProcessedAt string `json:"processed_at"`
}

// ProbeJSON — строка здоровья сервисов (ТЗД §5.8): имя пробы из
// dashboard.probes, up/down и причина down (недоступен, таймаут,
// не-200) — оператору нужен «почему», а не только факт.
type ProbeJSON struct {
	Name  string `json:"name"`
	Up    bool   `json:"up"`
	Error string `json:"error,omitempty"`
}

// RenderEngineJSON — строка панели движков (ТЗ рендера §4.6 п.4, T-246):
// темп и успешность одного движка рендера по счётчикам этапа с меткой
// engine (T-235): rendered — crawler_render_records_total{rendered=
// "true",engine}, finalized — Σ crawler_render_task_finalized_total по
// причинам (движко-зависимый класс задачи T-172; тарпит/cert/DNS без
// метки движка в долю не входят — cert/DNS движко-независимы по ТЗ
// рендера, тарпит недоатрибутирован, оговорка — docs/render.md).
// Кумулятив — с рестарта воркеров (метрики процесса), темп — между
// проходами сборщика.
type RenderEngineJSON struct {
	// Engine — движок сборки рендер-воркера (chromium|moli|lightpanda,
	// render.engine).
	Engine string `json:"engine"`
	// RenderedTotal — успешные рендеры движка с рестарта воркера.
	RenderedTotal int64 `json:"rendered_total"`
	// FinalizedTotal — финализации класса задачи с меткой движка.
	FinalizedTotal int64 `json:"finalized_total"`
	// SuccessPct — доля успешных среди атрибутированных движком
	// завершений (rendered/(rendered+finalized)); nil — завершений ещё
	// нет (молодой воркер).
	SuccessPct *float64 `json:"success_pct"`
	// RenderedPerSec — темп успешных рендеров между проходами сборщика
	// (0 — первый проход или рестарт воркера: базы дельты нет).
	RenderedPerSec float64 `json:"rendered_per_sec"`
}

// RenderFallbackJSON — блок фолбэка панели движков (T-246): публикации
// воркера A (crawler_render_fallback_published_total, T-238), потребление
// воркером B (crawler_render_fallback_consumed_total по исходам, T-245)
// и остаток — глубина q:render-fallback (тот же общий расчёт глубины,
// что у секции очередей).
type RenderFallbackJSON struct {
	// PublishedTotal — публикации в q:render-fallback с рестарта воркера A.
	PublishedTotal int64 `json:"published_total"`
	// PublishedPerSec — темп публикаций между проходами (0 — без базы).
	PublishedPerSec float64 `json:"published_per_sec"`
	// ConsumedTotal — потреблено воркером B с рестарта.
	ConsumedTotal int64 `json:"consumed_total"`
	// ConsumedPerSec — темп потребления между проходами (0 — без базы);
	// пара к PublishedPerSec — сравнение темпов и есть диагностика
	// «воркер B не успевает» (алерт render_fallback_depth, T-246).
	ConsumedPerSec float64 `json:"consumed_per_sec"`
	// ConsumedByResult — потребление по исходам delivered
	// (rendered|finalized|skipped).
	ConsumedByResult map[string]int64 `json:"consumed_by_result"`
	// Depth — глубина q:render-fallback (backlog группы render-fallback-
	// worker); nil — Redis недоступен (глубина «н/д», секция жива).
	Depth *int64 `json:"depth"`
}

// Snapshot — снимок состояния (ТЗД §5): готовый к отдаче документ.
// Общие секции зон/итогов/очередей/темпа/скоростей (§5.1–5.4) —
// status.SummaryJSON (wire-контракт CLI --json, ТЗД §6.2): встраивание
// поднимает поля zones/totals/queues/throughput/rates на верхний уровень.
// nil-секции — ошибки сбора (§5.9), причины — в errors; built_at — момент
// сборки (RFC3339), фронт показывает свежесть снимка.
type Snapshot struct {
	status.SummaryJSON
	Statuses map[string]int64  `json:"statuses"` // §5.5: имена ТЗ 6.2 → счётчик; nil — ошибка секции
	Laravel  *LaravelJSON      `json:"laravel"`  // §5.6; nil — ошибка секции
	Ingest   []IngestFileJSON  `json:"ingest"`   // §5.7; nil — ошибка секции
	Health   []ProbeJSON       `json:"health"`   // §5.8; nil — пробы не заданы («н/д», не ошибка)
	Errors   map[string]string `json:"errors"`   // §5.9: секция → причина; пуста, когда все живы
	BuiltAt  string            `json:"built_at"`

	// QueueWatermarks — эффективные пороги backpressure рабочих стримов
	// (§5.4): для каждого стрима без q:dead (гейтом не ограничивается)
	// — queue.StreamWatermark(scheduler.queue_high_watermark,
	// redis.maxlen, стрим), тот же единый расчёт, что у гейта
	// публикации (T-026) и алерта переполнения очередей монитора
	// (T-080): порог не выше половины предела MAXLEN стрима (T-141:
	// q:dns — 125 пачек, штучные — 75 000 при дефолтах). Фронту нужен
	// для подсветки приближения глубины стрима к порогу; 0 — гейт
	// отключён. Значение конфигурации (пересчитывается на каждом
	// проходе), секцией не является и деградировать не может.
	QueueWatermarks map[string]int64 `json:"queue_watermarks"`

	// Render — панель движков рендера и фолбэка (ТЗ рендера §4.6 п.4,
	// T-246): темп/успешность по engine из экспозиций /metrics
	// рендер-воркеров (dashboard.render_metrics_urls) и fallback-рейт —
	// published/consumed/глубина q:render-fallback. nil — адреса не
	// заданы («н/д», не ошибка — стиль Health) или секция деградировала
	// (причина — в errors).
	Render *RenderJSON `json:"render"`
}

// RenderJSON — секция панели движков: строки по движкам (темп/успешность)
// и блок фолбэка.
type RenderJSON struct {
	// Engines — движки, замеченные в экспозициях воркеров (порядок —
	// по имени, детерминированный JSON); пусто — воркеры без рендеров.
	Engines []RenderEngineJSON `json:"engines"`
	// Fallback — published/consumed/глубина фолбэка.
	Fallback RenderFallbackJSON `json:"fallback"`
}

// rfc3339 — единый формат моментов времени снимка.
func rfc3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// rfc3339Zero — как rfc3339, но zero-время (NULL колонки схемы) — пустая
// строка: «0001-01-01T00:00:00Z» в интерфейсе — шум.
func rfc3339Zero(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return rfc3339(t)
}

// SnapshotSource — источник готового снимка для /api/status: цикл сборки
// T-129 (*Collector). nil-ответ — снимка ещё нет: до первого успешного
// сбора /api/status отвечает 503 (ТЗД §5.9). Отдельный интерфейс, а не
// *Collector напрямую: каркас T-128 и его тесты работают без сборки.
type SnapshotSource interface {
	Current() *Snapshot
}
