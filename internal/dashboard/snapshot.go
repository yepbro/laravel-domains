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

	// QueueWatermark — порог backpressure scheduler.queue_high_watermark
	// (§5.4): фронту нужен для подсветки приближения суммы глубин к порогу
	// (0 — гейт отключён). Значение конфигурации, секцией не является и
	// деградировать не может.
	QueueWatermark int64 `json:"queue_watermark"`
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
