// Package status — общая библиотека сводки конвейера (ТЗД §4.5, T-126):
// SQL-выборки, типы и расчёты статуса, используемые и CLI `crawlerctl
// status` (T-079, ТЗ 5.9), и операторским дашбордом (T-129) — один
// источник правды о смысле чисел; дублирование SQL между потребителями
// запрещено (ТЗД §4.5).
//
// Источники — PostgreSQL (счётчики zones + агрегаты domains + окна
// темпа) и Redis (глубина стримов — backlog групп потребления); всё
// читается разово, без состояния. Пакет не знает про вывод: таблица/JSON
// остаются в CLI, снимки — в дашборде. Секции дашборда без аналога в CLI
// (распределение статусов, Laravel-находки, журнал ingest — ТЗД §5.5–5.7)
// живут здесь же: SQL сводки — один источник правды, а деградация секций
// и снимок целиком — сборка дашборда (ТЗД §4.4, §5.9).
//
// Что измеримо из БД (решения зафиксированы, остальное — честно не
// показывается, ТЗ 5.9 «скорость… ожидаемое время завершения цикла»):
//
//   - прогресс зон — счётчики zones.domains_total/domains_pending/
//     domains_queued/domains_checked (дельты переходов + реконсиляция
//     monitor, T-178; базовое множество — status <> deleted, ТЗИ §4.1),
//     читаются справочником без прохода domains;
//   - скорость доменов — средняя с начала суток по суточному счётчику
//     проверок checks:YYYY-MM-DD (T-179; last_checked_at имеет точность
//     дня, ТЗ 6.1, часового окна нет); короче 10 минут окно не
//     используется (сразу после полуночи оценка скачет);
//   - HTTP-результаты и рендеры — за последний час по checked_at
//     (timestamptz пишет http-worker, T-058); собственной отметки времени
//     у рендера нет — считается доля rendered среди результатов окна;
//   - темп DNS-этапа отдельно и этапа анализа из БД не измерим (нет
//     отметок времени; завершение DNS+HTTP и есть «доменов/с») — их
//     метрики в Prometheus (T-076/T-077);
//   - ETA первого цикла = (pending + queued) / скорость доменов, защита
//     от деления на ноль — скорости нет или она 0 → «н/д».
//
// Окна и распределения: часовые окна темпа (subdomain_results.
// checked_at) обслуживает индекс T-120 — правый край диапазона,
// стоимость пропорциональна окну, а не таблице; суточное окно темпа —
// инкрементальный счётчик checks:YYYY-MM-DD (T-179: живой подсчёт
// last_checked_at = current_date деградировал heap-fetch'ами свежих
// страниц — замер §2 TZ_slow_done_today); распределения и состояния —
// инкрементальные счётчики (T-178: zones + crawler_counters, дельты
// переходов + реконсиляция раз в monitor.zone_stats_interval — единственный
// полный проход domains в стационаре). Дашборд кэширует снимки фоновым
// циклом (ТЗД §4.4, TTL-кэш T-161 — теперь он кэширует дешёвые чтения,
// защищая от трафика между refresh-циклами), его период
// (dashboard.refresh) задаёт цену мониторинга.
package status

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// ---------------------------------------------------------------- SQL

// zoneStatsSQL — сводка по зонам из счётчиков справочника (T-178, ТЗИ
// §8.1): без JOIN domains — полный проход 47 млн строк ушёл из путей
// чтения, сводка читает тысячи строк зон; счётчики ведут дельты
// переходов (ТЗИ §6) и реконсиляция monitor (ТЗИ §7). Базовое множество
// всех колонок — status <> deleted (унификация ТЗИ §4.1: queued/done
// прежде живого агрегата считались без фильтра статуса — удалённый с
// зависшей арендой попадал в queued; теперь удалённый домен не является
// ни работой, ни прогрессом).
var zoneStatsSQL = "SELECT z.id, z.name, coalesce(z.name_unicode, '') AS name_unicode, " +
	"z.is_active, z.domains_total, z.domains_pending, z.domains_queued, z.domains_checked " +
	"FROM zones AS z ORDER BY z.name"

// throughputSQL — замеры темпа одним запросом. now() — время транзакции:
// оба интервала согласованы внутри одного выполнения; current_date и
// date_trunc — одна таймзона сервера БД, как и now() в записи
// last_checked_at (T-058). done_today — точечное чтение суточного
// счётчика crawler_counters['checks:YYYY-MM-DD'] (T-179): имя дня
// вычисляет БД тем же current_date, что и окно реконсиляции, — Go-часы
// в имени не участвуют; счётчик ведут инкременты записи результата
// (httpworker/record.go), авторитет — реконсиляция monitor. coalesce
// СНАРУЖИ подзапроса: до первой записи суток подзапрос пуст (NULL), а не
// строка с NULL-значением — часы простоя конвейера показывают честный
// ноль (TZ_slow_done_today §4/§7). done_today считает события записи
// результатов, а не домены: повторные эпизоды того же домена за сутки
// сходятся в «темп работы» (расхождение с живым count(*) — §5.1 ТЗ,
// восстанавливается реконсиляцией). Часовые окна results_hour/
// rendered_hour остаются живыми подзапросами по subdomain_results —
// окно маленькое, индекс subdomain_results_checked_idx отрабатывает как
// задумано (замер §2 ТЗ: 0,07 мс).
const throughputSQL = "SELECT " +
	"coalesce((SELECT value FROM crawler_counters " +
	"WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')), 0) AS done_today, " +
	"extract(epoch FROM (now() - date_trunc('day', now())))::float8 AS elapsed_sec, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour') AS results_hour, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour' AND rendered) AS rendered_hour"

// domainStatusSQL — распределение domains по статусам (ТЗД §5.5) из
// строк status_* таблицы crawler_counters (T-178, ТЗИ §8.2): строки
// пишет реконсиляция monitor (статус-распределение не инкрементируется
// на переходах — старые статусы не пинируются предикатами операторов,
// ТЗИ §13.3; FILTER-колонки в реконсиляционном скане бесплатны),
// свежесть — monitor.zone_stats_interval (5 мин; прежде живой агрегат
// кэшировался минутой T-161). LIKE-шаблон с экранированным
// подчёркиванием: имя строки — status_<имя>, односимвольная маска _
// ловила бы чужие имена. ORDER BY name — детерминированный порядок
// строк (имена и полнота легенды — в FetchDomainStatuses).
const domainStatusSQL = "SELECT name, value FROM crawler_counters WHERE name LIKE 'status\\_%' ORDER BY name"

// laravelTotalsSQL — счётчики Laravel-находок (ТЗД §5.6): всего — сумма
// инкрементальных счётчиков зон (T-178, ТЗИ §8.3: sum(zones.
// domains_laravel), как читает лендинг с T-122; дельты пишет analyzer,
// авторитет — реконсиляция), окно 24 часа — живой агрегат по
// laravel_domains (обновлится при пересчёте агрегата, ТЗ 5.8) по индексу
// laravel_domains_updated_at_idx (T-120): цена пропорциональна окну, а
// не таблице (рамки ТЗИ §3.1). Окно 24 часа — то же, что у «находок за
// 24 ч» лендинга (T-118/T-120), но по laravel_domains — ТЗД §5.6 явно
// называет эту таблицу: секция показывает живой агрегат, а не журнал
// переходов.
const laravelTotalsSQL = "SELECT (SELECT coalesce(sum(domains_laravel), 0) FROM zones), " +
	"count(*) FILTER (WHERE updated_at >= now() - interval '24 hours') " +
	"FROM laravel_domains"

// laravelRecentSQL — последние находки (ТЗД §5.6): домен собирается из
// label и зоны (JOIN по ключу laravel_domains), сортировка и LIMIT — по
// индексу laravel_domains_updated_at_idx (T-120). ver_min/ver_max
// NULL-able (диапазон мог не определиться) — FetchLaravelStats
// разворачивает в пустые строки.
const laravelRecentSQL = "SELECT d.label || '.' || z.name AS domain, z.name AS zone, " +
	"ld.score, ld.ver_min, ld.ver_max, ld.updated_at " +
	"FROM laravel_domains AS ld " +
	"JOIN domains AS d ON d.zone_id = ld.zone_id AND d.id = ld.domain_id " +
	"JOIN zones AS z ON z.id = ld.zone_id " +
	"ORDER BY ld.updated_at DESC LIMIT 10"

// ingestRecentSQL — последние обработанные файлы (ТЗД §5.7). NULLS LAST
// обязателен: DESC в PostgreSQL по умолчанию поднимает NULL первыми —
// строка без processed_at (схема допускает) не должна возглавлять
// «последние»; id DESC — детерминированный порядок внутри одной секунды.
const ingestRecentSQL = "SELECT path, action, lines_total, added, existed, deleted, rejected, processed_at " +
	"FROM ingest_files ORDER BY processed_at DESC NULLS LAST, id DESC LIMIT 10"

// ---------------------------------------------------------------- данные

// ZoneStats — строка сводки зоны.
type ZoneStats struct {
	ID             int16
	Name           string // punycode — каноническая форма (ТЗ 5.2)
	NameUnicode    string // "" — NULL; только отображение (в JSON)
	IsActive       bool
	DomainsTotal   int64 // счётчик zones.domains_total (дельты + реконсиляция, T-178)
	DomainsChecked int64 // счётчик zones.domains_checked (дельты + реконсиляция, T-178)
	Pending        int64 // счётчик zones.domains_pending: ожидает выборки
	Queued         int64 // счётчик zones.domains_queued: в работе (lease)
	Done           int64 // DNS/HTTP-этап завершён (ТЗ 7) — теперь тот же
	// счётчик domains_checked (T-178: прежде живой агрегат считал done
	// без фильтра статуса — унификация базового множества ТЗИ §4.1
	// закрыла расхождение в пользу реконсиляции); поле сохранено —
	// wire-формат JSON (§5.3) показывает обе колонки.
}

// Throughput — замеры темпа из БД (одно выполнение throughputSQL).
type Throughput struct {
	DoneToday    int64   // событий записи результатов с начала суток (счётчик checks:, T-179)
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
			&z.DomainsTotal, &z.Pending, &z.Queued, &z.DomainsChecked); err != nil {
			return nil, fmt.Errorf("status: зоны: чтение: %w", err)
		}
		z.Done = z.DomainsChecked // одна колонка счётчика — обе семантики (T-178)
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

// ---------------------------------------------------------------- данные секций дашборда (ТЗД §5.5–5.7)

// LaravelRecent — строка «последние находки» (ТЗД §5.6): домен, зона,
// score, диапазон версий (пустая строка — NULL: диапазон не определён),
// момент обновления.
type LaravelRecent struct {
	Domain    string
	Zone      string
	Score     float32
	VerMin    string // "" — NULL
	VerMax    string // "" — NULL
	UpdatedAt time.Time
}

// LaravelStats — секция Laravel-находок (ТЗД §5.6): всего, за 24 часа,
// последние 10.
type LaravelStats struct {
	Total   int64
	Last24h int64
	Recent  []LaravelRecent // пустой срез — находок нет (не ошибка)
}

// IngestReport — строка «последние ingest-файлы» (ТЗД §5.7): файл,
// действие («add»/«delete» по model.Action; код вне таблицы ТЗ 6.1 —
// имя «Action(N)», NULL — пусто), счётчики строк и момент обработки.
type IngestReport struct {
	Path        string // "" — NULL
	Action      string
	LinesTotal  int64
	Added       int64
	Existed     int64
	Deleted     int64
	Rejected    int64
	ProcessedAt time.Time // zero — NULL
}

// FetchDomainStatuses — распределение domains по статусам (ТЗД §5.5)
// из строк status_* crawler_counters (T-178). Ключи — имена по ТЗ 6.2
// основного ТЗ (model.Statuses); статусы без строк входят с нулём —
// полная легенда для отображения без знания таблицы на клиенте; строки
// с именем вне легенды (неизвестный код схемы) не отображаются —
// реконсиляция пишет только model.Statuses, чужая строка — ручная
// правка таблицы (runbooks §6).
func FetchDomainStatuses(ctx context.Context, pool db.DB) (map[string]int64, error) {
	if pool == nil {
		return nil, errors.New("status: статусы: пул не задан")
	}
	rows, err := pool.Query(ctx, domainStatusSQL)
	if err != nil {
		return nil, fmt.Errorf("status: статусы: %w", err)
	}
	defer rows.Close()
	counts := make(map[string]int64, len(model.Statuses))
	for _, s := range model.Statuses {
		counts[s.String()] = 0
	}
	const statusPrefix = "status_"
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			return nil, fmt.Errorf("status: статусы: чтение: %w", err)
		}
		if _, ok := counts[strings.TrimPrefix(name, statusPrefix)]; !ok {
			continue // имя вне легенды model.Statuses — комментарий выше
		}
		counts[strings.TrimPrefix(name, statusPrefix)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("status: статусы: %w", err)
	}
	return counts, nil
}

// FetchLaravelStats собирает секцию Laravel-находок (ТЗД §5.6): счётчики
// одним проходом laravel_domains, последние 10 — отдельным запросом с
// JOIN (индекс T-120). Ошибка любого запроса — ошибка секции целиком
// (вызывающий кладёт null, ТЗД §5.9). Первый result set закрывается ДО
// второго запроса (стиль web.FetchLandingData): скалярный SELECT не
// дочитывается до конца сам — удержание set'а занимало бы соединение
// пула на всё время второго запроса (при pool_size=1 — дедлок).
func FetchLaravelStats(ctx context.Context, pool db.DB) (LaravelStats, error) {
	if pool == nil {
		return LaravelStats{}, errors.New("status: laravel: пул не задан")
	}
	var st LaravelStats
	rows, err := pool.Query(ctx, laravelTotalsSQL)
	if err != nil {
		return LaravelStats{}, fmt.Errorf("status: laravel: %w", err)
	}
	if !rows.Next() {
		// Пустой набор скалярного агрегата — аномалия: различаем ошибку
		// уровня строк (реальная причина ценнее синтетической) и честное
		// «пусто».
		err := rows.Err()
		rows.Close()
		if err != nil {
			return LaravelStats{}, fmt.Errorf("status: laravel: %w", err)
		}
		return LaravelStats{}, errors.New("status: laravel: пустой ответ агрегата")
	}
	if err := rows.Scan(&st.Total, &st.Last24h); err != nil {
		rows.Close()
		return LaravelStats{}, fmt.Errorf("status: laravel: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return LaravelStats{}, fmt.Errorf("status: laravel: %w", err)
	}
	rows.Close()

	rows, err = pool.Query(ctx, laravelRecentSQL)
	if err != nil {
		return LaravelStats{}, fmt.Errorf("status: laravel: последние: %w", err)
	}
	defer rows.Close()
	st.Recent = make([]LaravelRecent, 0, 10)
	for rows.Next() {
		var (
			it      LaravelRecent
			verMin  *string
			verMax  *string
			updated time.Time
		)
		if err := rows.Scan(&it.Domain, &it.Zone, &it.Score, &verMin, &verMax, &updated); err != nil {
			return LaravelStats{}, fmt.Errorf("status: laravel: последние: чтение: %w", err)
		}
		if verMin != nil {
			it.VerMin = *verMin
		}
		if verMax != nil {
			it.VerMax = *verMax
		}
		it.UpdatedAt = updated
		st.Recent = append(st.Recent, it)
	}
	if err := rows.Err(); err != nil {
		return LaravelStats{}, fmt.Errorf("status: laravel: последние: %w", err)
	}
	return st, nil
}

// FetchIngestReports — последние 10 записей журнала ingest_files
// (ТЗД §5.7). NULL-колонки схемы (path/action/счётчики/processed_at)
// разворачиваются в нулевые значения — журнал пишет конвейер всегда
// полными строками (internal/ingest RecordFile), NULL легален только
// для вручную вставленных строк.
func FetchIngestReports(ctx context.Context, pool db.DB) ([]IngestReport, error) {
	if pool == nil {
		return nil, errors.New("status: ingest: пул не задан")
	}
	rows, err := pool.Query(ctx, ingestRecentSQL)
	if err != nil {
		return nil, fmt.Errorf("status: ingest: %w", err)
	}
	defer rows.Close()
	out := make([]IngestReport, 0, 10)
	for rows.Next() {
		var (
			it        IngestReport
			path      *string
			action    *int16
			lines     *int64
			added     *int64
			existed   *int64
			deleted   *int64
			rejected  *int64
			processed *time.Time
		)
		if err := rows.Scan(&path, &action, &lines, &added, &existed, &deleted,
			&rejected, &processed); err != nil {
			return nil, fmt.Errorf("status: ingest: чтение: %w", err)
		}
		if path != nil {
			it.Path = *path
		}
		if action != nil {
			it.Action = model.Action(*action).String()
		}
		it.LinesTotal, it.Added, it.Existed = derefInt64(lines), derefInt64(added), derefInt64(existed)
		it.Deleted, it.Rejected = derefInt64(deleted), derefInt64(rejected)
		if processed != nil {
			it.ProcessedAt = *processed
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("status: ingest: %w", err)
	}
	return out, nil
}

// derefInt64 — значение указателя или 0 (NULL-счётчик ingest_files).
func derefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
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
			// Кламп «минимум 1с»: до строки ETASec = Round(остаток/
			// скорость) ≥ 0 при положительных операндах; ноль и единица
			// дают 1, большее не меняется — «< 1» и «<= 1»/«< 2»
			// неразличимы ни на одном достижимом значении.
			// mutator-disable-next-line expression/comparison, numbers/incrementer
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
