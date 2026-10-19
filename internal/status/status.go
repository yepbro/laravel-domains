// Package status — общая библиотека сводки конвейера (ТЗД §4.5, T-126):
// SQL-выборки, типы и расчёты статуса, используемые и CLI `crawlerctl
// status` (T-079, ТЗ 5.9), и операторским дашбордом (T-129) — один
// источник правды о смысле чисел; дублирование SQL между потребителями
// запрещено (ТЗД §4.5).
//
// Источники — PostgreSQL (счётчики zones + агрегаты domains + окна
// темпа) и Redis (глубина стримов — backlog групп потребления); всё
// читается разово, без состояния. Пакет не знает про вывод: таблица/JSON
// остаются в CLI, снимки — в дашборде.
//
// Что измеримо из БД (решения зафиксированы, остальное — честно не
// показывается, ТЗ 5.9 «скорость… ожидаемое время завершения цикла»):
//
//   - прогресс зон — счётчики zones.domains_total/domains_checked (их
//     обновляет сервис monitor, ТЗ 5.9) плюс живой агрегат domains по
//     check_state: pending без учёта deleted (удалённый домен планировщик
//     не выбирает, T-028 — ждать его проверки нечего), queued и done без
//     фильтра статуса;
//   - скорость доменов — средняя с начала суток: last_checked_at имеет
//     точность дня (date, ТЗ 6.1), часового окна нет; короче 10 минут
//     окно не используется (сразу после полуночи оценка скачет);
//   - HTTP-результаты и рендеры — за последний час по checked_at
//     (timestamptz пишет http-worker, T-058); собственной отметки времени
//     у рендера нет — считается доля rendered среди результатов окна;
//   - темп DNS-этапа отдельно и этапа анализа из БД не измерим (нет
//     отметок времени; завершение DNS+HTTP и есть «доменов/с») — их
//     метрики в Prometheus (T-076/T-077);
//   - ETA первого цикла = (pending + queued) / скорость доменов, защита
//     от деления на ноль — скорости нет или она 0 → «н/д».
//
// Агрегаты и окна — полные проходы таблиц (частичный domains_recheck_idx
// доменам T-104 не покрывает окна сводки — фильтра по check_state в
// запросе нет): для разовой диагностики допустимо; постоянные
// потребители не повторяют проходы на каждый запрос — дашборд кэширует
// снимки фоновым циклом (ТЗД §4.4).
package status

import (
	"context"
	"errors"
	"fmt"
	"math"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// ---------------------------------------------------------------- SQL

// zoneStatsSQL — сводка по зонам одним проходом: счётчики zones +
// агрегат domains по check_state (LEFT JOIN — зона без доменов остаётся
// в сводке). Коды check_state/status — константы model, вшитые в текст
// (стиль scheduler/dispatch.go); count(d.id) игнорирует строку зоны без
// доменов, FILTER — NULL-колонки непрошедших условий LEFT JOIN.
var zoneStatsSQL = fmt.Sprintf(
	"SELECT z.id, z.name, coalesce(z.name_unicode, '') AS name_unicode, "+
		"z.is_active, z.domains_total, z.domains_checked, "+
		"count(d.id) FILTER (WHERE d.check_state = %d AND d.status <> %d) AS pending, "+
		"count(d.id) FILTER (WHERE d.check_state = %d) AS queued, "+
		"count(d.id) FILTER (WHERE d.check_state = %d) AS done "+
		"FROM zones AS z LEFT JOIN domains AS d ON d.zone_id = z.id "+
		"GROUP BY z.id ORDER BY z.name",
	int16(model.CheckStatePending), int16(model.StatusDeleted),
	int16(model.CheckStateQueued), int16(model.CheckStateDone))

// throughputSQL — замеры темпа одним запросом. now() — время транзакции:
// оба интервала согласованы внутри одного выполнения; current_date и
// date_trunc — одна таймзона сервера БД, как и now() в записи
// last_checked_at (T-058). done_today считает проверки, а не домены:
// повторные проверки одного домена за сутки сходятся в «темп работы».
const throughputSQL = "SELECT " +
	"(SELECT count(*) FROM domains WHERE last_checked_at = current_date) AS done_today, " +
	"extract(epoch FROM (now() - date_trunc('day', now())))::float8 AS elapsed_sec, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour') AS results_hour, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour' AND rendered) AS rendered_hour"

// ---------------------------------------------------------------- данные

// ZoneStats — строка сводки зоны.
type ZoneStats struct {
	ID             int16
	Name           string // punycode — каноническая форма (ТЗ 5.2)
	NameUnicode    string // "" — NULL; только отображение (в JSON)
	IsActive       bool
	DomainsTotal   int64 // счётчик zones.domains_total (обновляет monitor)
	DomainsChecked int64 // счётчик zones.domains_checked (обновляет monitor)
	Pending        int64 // агрегат domains: ожидает выборки (без deleted)
	Queued         int64 // в работе (lease)
	Done           int64 // DNS/HTTP-этап завершён (ТЗ 7)
}

// Throughput — замеры темпа из БД (одно выполнение throughputSQL).
type Throughput struct {
	DoneToday    int64   // проверок доменов с начала суток (last_checked_at)
	ElapsedSec   float64 // прошло с начала суток по часам БД
	ResultsHour  int64   // HTTP-результатов за последний час (checked_at)
	RenderedHour int64   // из них с итоговым DOM (rendered)
}

// FetchZoneStats выполняет zoneStatsSQL и собирает строки зон.
func FetchZoneStats(ctx context.Context, pool db.DB) ([]ZoneStats, error) {
	if pool == nil {
		return nil, errors.New("status: зоны: пул не задан")
	}
	rows, err := pool.Query(ctx, zoneStatsSQL)
	if err != nil {
		return nil, fmt.Errorf("status: зоны: %w", err)
	}
	defer rows.Close()
	var zones []ZoneStats
	for rows.Next() {
		var z ZoneStats
		if err := rows.Scan(&z.ID, &z.Name, &z.NameUnicode, &z.IsActive,
			&z.DomainsTotal, &z.DomainsChecked, &z.Pending, &z.Queued, &z.Done); err != nil {
			return nil, fmt.Errorf("status: зоны: чтение: %w", err)
		}
		zones = append(zones, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("status: зоны: %w", err)
	}
	return zones, nil
}

// FetchThroughput выполняет throughputSQL (ровно одна строка — скалярный
// SELECT без FROM).
func FetchThroughput(ctx context.Context, pool db.DB) (Throughput, error) {
	if pool == nil {
		return Throughput{}, errors.New("status: темп: пул не задан")
	}
	rows, err := pool.Query(ctx, throughputSQL)
	if err != nil {
		return Throughput{}, fmt.Errorf("status: темп: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return Throughput{}, errors.New("status: темп: пустой ответ")
	}
	var t Throughput
	if err := rows.Scan(&t.DoneToday, &t.ElapsedSec, &t.ResultsHour, &t.RenderedHour); err != nil {
		return Throughput{}, fmt.Errorf("status: темп: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return Throughput{}, fmt.Errorf("status: темп: %w", err)
	}
	return t, nil
}

// ---------------------------------------------------------------- очереди

// StreamLener — глубина стримов; *queue.BackpressureGate (T-026)
// удовлетворяет интерфейсу. Отдельный интерфейс, а не go-redis напрямую:
// тесты подменяют фейком, транспорт заменяем (ТЗ 4.2).
type StreamLener interface {
	Len(ctx context.Context, stream string) (int64, error)
}

// Streams — рабочие стримы очереди в порядке вывода (ТЗ 4.2).
var Streams = []string{
	queue.StreamDNS, queue.StreamHTTP, queue.StreamRender, queue.StreamAnalyze, queue.StreamDead,
}

// FetchQueueLens собирает глубину очередей. Ошибка любого стрима
// останавливает сбор: Redis один, повторные обращения к недоступному
// серверу бессмысленны.
func FetchQueueLens(ctx context.Context, lenner StreamLener) (map[string]int64, error) {
	if lenner == nil {
		return nil, errors.New("status: очереди: клиент не задан")
	}
	lens := make(map[string]int64, len(Streams))
	for _, s := range Streams {
		n, err := lenner.Len(ctx, s)
		if err != nil {
			return nil, err
		}
		lens[s] = n
	}
	return lens, nil
}

// ---------------------------------------------------------------- расчёты

const (
	// MinSpeedWindowSec — минимальное окно оценки скорости доменов: сразу
	// после полуночи среднее «с начала суток» нестабильно (окно в минуты
	// при тысячах проверок в секунду, ТЗ 3.3).
	MinSpeedWindowSec = 600
	// speedWindowSec — фиксированное окно часовых счётчиков (checked_at).
	speedWindowSec = 3600.0
)

// Totals — суммы по зонам (итого таблицы и вход расчёта ETA).
type Totals struct {
	DomainsTotal   int64
	DomainsChecked int64
	Pending        int64
	Queued         int64
	Done           int64
}

// ComputeTotals суммирует строки зон.
func ComputeTotals(zones []ZoneStats) Totals {
	var t Totals
	for _, z := range zones {
		t.DomainsTotal += z.DomainsTotal
		t.DomainsChecked += z.DomainsChecked
		t.Pending += z.Pending
		t.Queued += z.Queued
		t.Done += z.Done
	}
	return t
}

// Rates — производные скорости/ETA. Чистая функция от замеров:
// короткие окна и нулевые скорости не дают «н/д»-оценок (защита от
// деления на ноль — флаги Known, а не паника).
type Rates struct {
	DomainsPerSec float64 // средняя с начала суток
	DomainsKnown  bool    // окно достаточно (≥ MinSpeedWindowSec)
	ResultsPerSec float64 // HTTP-результатов/с за час
	RenderPerSec  float64 // рендеров/с за час
	Remaining     int64   // pending + queued — объём первого цикла
	ETASec        int64
	ETAKnown      bool
}

// ComputeRates выводит скорости и ETA первого цикла. Пустой цикл
// (нечего проверять) завершён независимо от скорости;ETA при остатке
// не меньше секунды — остаток в одну домену не должен выглядеть «0с».
func ComputeRates(t Throughput, pending, queued int64) Rates {
	r := Rates{
		ResultsPerSec: float64(t.ResultsHour) / speedWindowSec,
		RenderPerSec:  float64(t.RenderedHour) / speedWindowSec,
		Remaining:     pending + queued,
	}
	if r.Remaining == 0 {
		r.ETAKnown, r.ETASec = true, 0
	}
	if t.ElapsedSec >= MinSpeedWindowSec {
		r.DomainsKnown = true
		r.DomainsPerSec = float64(t.DoneToday) / t.ElapsedSec
		if r.Remaining > 0 && r.DomainsPerSec > 0 {
			r.ETASec = int64(math.Round(float64(r.Remaining) / r.DomainsPerSec))
			if r.ETASec < 1 {
				r.ETASec = 1
			}
			r.ETAKnown = true
		}
	}
	return r
}

// FormatDuration — округлый человекочитаемый интервал: «45с», «13м»,
// «5ч 20м», «3д 4ч» (сутки — потолок единиц: недели скрывают масштаб
// хуже, чем трёхзначные дни).
func FormatDuration(sec int64) string {
	switch {
	case sec < 60:
		return fmt.Sprintf("%dс", sec)
	case sec < 3600:
		return fmt.Sprintf("%dм", (sec+30)/60)
	case sec < 86400:
		return fmt.Sprintf("%dч %02dм", sec/3600, (sec%3600)/60)
	default:
		return fmt.Sprintf("%dд %dч", sec/86400, (sec%86400)/3600)
	}
}
