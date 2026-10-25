// Package monitor — служебный сервис мониторинга (ТЗ 5.9). Файл
// zonestats — реконсиляция инкрементальных счётчиков конвейера (T-178,
// ТЗИ tasks/TZ_incremental_counters.md §7): один совмещённый проход раз
// в monitor.zone_stats_interval — единственный полный проход domains в
// стационаре (замер §12 docs/load-measurements.md: живые агрегаты
// zoneStatsSQL/domainStatusSQL/backlogSQL/historyStatsSQL — большая
// часть CPU postgres; T-161 смягчил интервалами/кэшом, T-178 убрал
// сами проходы из путей чтения — читатели идут по счётчикам, ТЗИ §8).
//
// Проход авторитетен: значение каждого счётчика перезаписывается целиком
// агрегатом, инкременты переходов (ТЗИ §6, scheduler/ingest/httpworker/
// analyzer) дают свежесть между проходами. Состав прохода (одна
// транзакция, один снапшот — согласованность всех позиций на один
// момент):
//
//   - zones: domains_total/domains_pending/domains_queued/
//     domains_checked — один FILTER-агрегат domains GROUP BY zone_id
//     (партиционный ключ — partitionwise aggregate, как T-078),
//     domains_laravel — агрегат laravel_domains (T-120). Базовое
//     множество всех колонок — status <> deleted (унификация ТЗИ §4.1:
//     удалённый домен не является ни работой, ни прогрессом);
//   - crawler_counters: строки status_<имя> по model.Statuses (все
//     строки domains, включая deleted — легенда полная, ТЗИ §4.2),
//     render_pending — счётчик бита T-175 (тот же предикат, что бэкфилл
//     миграции 000015 и инкременты renderq: без фильтра статуса —
//     удалённый с битом остаётся в долге, пока проход доработки его не
//     снимет), laravel_appeared_total/laravel_lost_total — агрегат
//     laravel_history (кумулятивы T-118), checks:YYYY-MM-DD — суточный
//     счётчик проверок (T-179, TZ_slow_done_today §6: живой аналог
//     прежнего подзапроса count(*) … WHERE last_checked_at =
//     current_date; имя дня вычисляет БД — отдельным оператором после
//     скаляров, в unnest-пачку с Go-именем не кладётся: рассинхрон
//     таймзоны Go/БД молча развёл бы строку и окно FILTER),
//     temp_error:YYYY-MM-DD — суточный числитель доли temp_error
//     (T-182, TZ_slow_dns_share §6: та же FILTER-цена нуля, знаменатель
//     алерта доли — строка checks:);
//   - дрейф |старое − новое| каждой позиции: до перезаписи читаются
//     прежние значения, после — новые (транзакция видит собственную
//     запись); ненулевой дрейф — gauge crawler_counters_drift + Warn
//     (ТЗИ §7.1.3). Первый проход цикла дрейфом не считается: после
//     миграции/простоя он заполняет нули (ТЗИ §5).
//
// Скан domains в проходе один: per-zone агрегат dom (FILTER по
// check_state без фильтра статуса) обслуживает и колонки зон
// (total/pending/queued/checked — status <> deleted в FILTER), и
// глобальное распределение статусов + бит рендера (суммы по зонам), и
// суточные окна проверок с temp_error-числителем (FILTER
// last_checked_at = current_date × статус, T-179/T-182) —
// дополнительная цена FILTER-колонок ноль (ТЗИ §13.3).
//
// Семантика колонок зон — комментарий пакета ниже; изменения против
// T-078 — только расширение (pending/queued), существующие колонки
// читают лендинг (T-122), crawlerctl status, партиционирование.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// DefaultZoneStatsInterval — период реконсиляции: 5 минут. Исходные
// 60 с (верх коридора ТЗ 5.9) рассчитаны на малый корпус: замер §12
// docs/load-measurements.md (47 млн строк) — UPDATE-агрегат в тройке
// главных статей CPU postgres; T-161 поднял до 5 мин, T-178 оставил
// период реконсиляции тем же: в стационаре это единственный полный
// проход domains (критерий ТЗИ §1.2), дрейф между проходами обязан быть
// нулевым — по замеру дрейфа оператор может разредить интервал до
// 15–30 мин (ТЗИ §7.1). Коридор ТЗ остаётся доступным конфигом
// monitor.zone_stats_interval; тот же период у цикла алертов
// (DefaultAlertsInterval) — порог остановки прогресса
// AlertStallFactor × интервал растёт пропорционально.
const DefaultZoneStatsInterval = 5 * time.Minute

// ---------------------------------------------------------------- SQL

// statusAggCols — легенда статусов и их колонки-позиции в per-zone
// агрегате domainsAggSQL: ОДИН источник для генерации SQL (FILTER-колонки)
// и чтения в Go (порядок колонок результата) — рассинхронизация легенды
// model.Statuses и колонок невозможна по построению (ревью P3-4).
var statusAggCols = []struct {
	status model.Status
	col    string
}{
	{model.StatusNew, "f_new"},
	{model.StatusNotDelegated, "f_not_delegated"},
	{model.StatusDelegatedNoAddress, "f_delegated_no_address"},
	{model.StatusNoHTTP, "f_no_http"},
	{model.StatusAlive, "f_alive"},
	{model.StatusTempError, "f_temp_error"},
	{model.StatusDeleted, "f_deleted"},
	{model.StatusInvalid, "f_invalid"},
}

func init() {
	// Сверка по индексам, а не только по длине (ревью R2 P3-6): замена
	// одного статуса другим держала бы длину равной — легенда разъехалась
	// бы молча, агрегат писал бы чужие значения.
	if len(statusAggCols) != len(model.Statuses) {
		panic("monitor: statusAggCols не совпадает с model.Statuses по длине — легенда агрегата неполна")
	}
	for i, c := range statusAggCols {
		if c.status != model.Statuses[i] {
			panic(fmt.Sprintf("monitor: statusAggCols[%d] = %v, want %v (порядок model.Statuses)", i, c.status, model.Statuses[i]))
		}
	}
}

// domainsAggSQL — per-zone агрегат живого распределения, статусов и бита
// рендера ОДНИМ сканом domains (ТЗИ §7.1): FILTER-вариант count считает
// все позиции за один проход. Коды check_state/status/flags — константы
// model, вшитые в текст литералами (коды схемы, а не данные — стиль
// lease.go); GROUP BY по ключу партиционирования (zone_id в листах
// HASH-субпартиций T-005 входит в PK) позволяет partitionwise
// aggregate. Фильтра зоны нет: агрегат обслуживает и колонки зон, и
// глобальные суммы (статусы, бит рендера) — фильтр зон ограничивал бы
// глобальные счётчики (ТЗИ §9: recount --zone глобальные счётчики
// пересчитывает всегда). f_render — предикат бэкфилла 000015 и
// инкрементов T-175 (без фильтра статуса: удалённый с битом остаётся
// в долге, пока проход доработки его не снимет). f_checks_today —
// суточное окно проверок (T-179): тот же предикат
// last_checked_at = current_date, что у прежнего подзапроса done_today
// throughputSQL (включая строки deleted — живой аналог их не фильтровал);
// FILTER-колонка — цена нуля на уже идущем скане (ТЗИ §13.3), сумма по
// зонам — значение строки checks:YYYY-MM-DD. f_temp_today — числитель
// доли temp_error суток (T-182, TZ_slow_dns_share §6): то же окно
// × статус temp_error, FILTER-цена нуля (живой аналог прежнего dnsShareSQL
// считал окно целиком и ходил в heap за статусом — запрос №6 журнала
// медленных 24.10), сумма по зонам — значение строки
// temp_error:YYYY-MM-DD.
var domainsAggSQL = func() string {
	cols := make([]string, 0, len(statusAggCols)+7)
	cols = append(cols,
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d) AS total", int16(model.StatusDeleted)),
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d AND check_state = %d) AS pending", int16(model.StatusDeleted), int16(model.CheckStatePending)),
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d AND check_state = %d) AS queued", int16(model.StatusDeleted), int16(model.CheckStateQueued)),
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d AND check_state = %d) AS checked", int16(model.StatusDeleted), int16(model.CheckStateDone)))
	for _, c := range statusAggCols {
		cols = append(cols, fmt.Sprintf("count(*) FILTER (WHERE status = %d) AS %s", int16(c.status), c.col))
	}
	cols = append(cols,
		fmt.Sprintf("count(*) FILTER (WHERE (flags & %d) <> 0) AS f_render", int16(model.FlagRenderPending)),
		"count(*) FILTER (WHERE last_checked_at = current_date) AS f_checks_today",
		fmt.Sprintf("count(*) FILTER (WHERE last_checked_at = current_date AND status = %d) AS f_temp_today", int16(model.StatusTempError)))
	return "SELECT zone_id, " + strings.Join(cols, ", ") + " FROM domains GROUP BY zone_id"
}()

// zoneAggRow — строка per-zone агрегата (колонки domainsAggSQL): значения
// состояний и легенда статусов по порядку statusAggCols + бит рендера +
// суточное окно проверок (T-179) и его temp_error-числитель (T-182).
type zoneAggRow struct {
	zone                            int16
	total, pending, queued, checked int64
	statuses                        []int64 // по порядку statusAggCols
	render                          int64
	checksToday                     int64 // FILTER last_checked_at = current_date (T-179)
	tempToday                       int64 // FILTER last_checked_at = current_date AND status = temp_error (T-182)
}

// recountAggQuerySQL — чтение per-zone агрегата прохода (единственный
// полный скан domains реконсиляции, ТЗИ §7.1).
var recountAggQuerySQL = "SELECT * FROM (" + domainsAggSQL + ") AS agg ORDER BY zone_id"

// laravelAggQuerySQL — агрегат Laravel-счётчиков зон (T-120): строки
// laravel_domains уже прошли порог analyzer (§13.1), отдельный порог
// не нужен.
const laravelAggQuerySQL = "SELECT zone_id, count(*) AS laravel FROM laravel_domains GROUP BY zone_id ORDER BY zone_id"

// historyAggSQL — кумулятивы appeared/lost журнала детекции. Размен
// ТЗИ (§6.5 «убирают часовой seq scan» против §7.1.2 «агрегат
// laravel_history в проходе»): журнал append-only и на порядок меньше
// domains (события — только переходы Laravel-находимок), проход раз в
// zone_stats_interval по нему — секунды против десятков секунд скана
// domains; инкременты analyzer дают свежесть, агрегат — авторитет.
const historyAggSQL = "SELECT count(*) FILTER (WHERE appeared) AS h_appeared, " +
	"count(*) FILTER (WHERE NOT appeared) AS h_lost FROM laravel_history"

// recountZonesSQL — перезапись колонок зон значениями агрегата из Go
// (unnest-пачка; зона без живых доменов получает нули — значение в Go
// вычислено с нулями по отсутствию в агрегате). Блокирует строки зон
// ПЕРВЫМИ — до строк crawler_counters (recountCountersSQL): порядок
// «зоны → счётчики» общий с писателями переходов (httpworker/record.go,
// analyzer) и исключает циклы блокировок (ревью P3-2). updated_at —
// момент прохода (живость monitor, ТЗ 5.9 «остановка прогресса»).
// RETURNING — новые значения колонок для дрейфа (PG16 без RETURNING
// OLD: старые читаются до оператора в той же транзакции).
const recountZonesSQL = "UPDATE zones AS z " +
	"SET domains_total = u.total, " +
	"domains_pending = u.pending, " +
	"domains_queued = u.queued, " +
	"domains_checked = u.checked, " +
	"domains_laravel = u.laravel, " +
	"updated_at = $6 " +
	"FROM unnest($1::int2[], $2::int8[], $3::int8[], $4::int8[], $5::int8[], $7::int8[]) " +
	"AS u(id, total, pending, queued, checked, laravel) " +
	"WHERE z.id = u.id " +
	"RETURNING z.id, z.domains_total, z.domains_pending, z.domains_queued, z.domains_checked, z.domains_laravel"

// recountCountersSQL — upsert глобальных скаляров значениями из Go
// (строки создаёт первый проход, ТЗИ §5); блокирует строки
// crawler_counters ПОСЛЕДНИМИ — после зон (порядок против дедлоков,
// комментарий recountZonesSQL).
const recountCountersSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
	"SELECT u.name, u.value, $3 " +
	"FROM unnest($1::text[], $2::int8[]) AS u(name, value) " +
	"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
	"RETURNING name, value"

// readZonesSQL — старые значения колонок зон до перезаписи (дрейф).
// $1 — фильтр зоны (NULL — все).
const readZonesSQL = "SELECT id, domains_total, domains_pending, domains_queued, domains_checked, domains_laravel " +
	"FROM zones WHERE ($1::int2 IS NULL OR id = $1)"

// readCountersSQL — старые значения глобальных счётчиков до upsert
// (дрейф); отсутствующая строка — старое 0 (первый проход заполняет).
const readCountersSQL = "SELECT name, value FROM crawler_counters WHERE name = ANY($1)"

// readDailyChecksSQL — старое значение строки суточного счётчика
// проверок до перезаписи (дрейф, T-179): имя дня вычисляет БД
// (current_date) — та же таймзона, что у FILTER-колонки f_checks_today
// прохода и now() записи last_checked_at (T-058); пустой результат —
// строки дня ещё нет (создаёт инкремент записи или этот проход —
// заполнение, не дрейф, прецедент «первый проход заполняет», ТЗИ §5).
const readDailyChecksSQL = "SELECT value FROM crawler_counters " +
	"WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')"

// readDailyTempSQL — старое значение строки суточного числителя
// temp_error до перезаписи (дрейф, T-182): пара readDailyChecksSQL,
// строка temp_error:YYYY-MM-DD (инкремент записи результата с вердиктом
// temp_error, TZ_slow_dns_share §5); пустой результат — строки дня ещё
// нет (первый temp_error-эпизод суток или проход создаст) — заполнение,
// не дрейф.
const readDailyTempSQL = "SELECT value FROM crawler_counters " +
	"WHERE name = 'temp_error:' || to_char(current_date, 'YYYY-MM-DD')"

// recountDailyChecksSQL — перезапись строки текущего дня значением
// агрегата прохода (T-179, TZ_slow_done_today §6): value := сумма
// FILTER-колонок f_checks_today per-zone агрегата, имя дня — снова БД
// (рассинхрон таймзоны Go/БД развёл бы строку и окно). Блокировка
// строки — последней, после зон и глобальных скаляров (порядок против
// дедлоков, комментарий recountZonesSQL; общий с инкрементом записи
// результата record.go: зоны → render_pending → checks:). RETURNING —
// новое значение для дрейфа.
const recountDailyChecksSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
	"VALUES ('checks:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, $2) " +
	"ON CONFLICT (name) DO UPDATE " +
	"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
	"RETURNING value"

// recountDailyTempSQL — перезапись суточного числителя temp_error
// значением агрегата прохода (T-182, TZ_slow_dns_share §6): value := сумма
// FILTER-колонок f_temp_today; пара recountDailyChecksSQL — блокировка
// ПОСЛЕ строки знаменателя (тот же порядок, что у инкрементов record.go:
// checks: → temp_error: — без циклов блокировок), RETURNING — новое
// значение для дрейфа. Строку дня без temp_error-событий проход создаёт
// значением окна (может быть нулём) — симметрия знаменателю, читатель
// доли до прохода видит coalesce-ноль инкрементов.
const recountDailyTempSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
	"VALUES ('temp_error:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, $2) " +
	"ON CONFLICT (name) DO UPDATE " +
	"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
	"RETURNING value"

// CounterNames — имена строк глобальных счётчиков, поддерживаемых
// реконсиляцией (легенда для чтения/сверки). Суточные строки
// checks:YYYY-MM-DD и temp_error:YYYY-MM-DD (T-179/T-182) сюда не
// входят: имя дня вычисляет БД (readDailyChecksSQL/recountDailyChecksSQL
// и их temp-пары), в unnest-пачке с Go-именами их нет.
func CounterNames() []string {
	names := make([]string, 0, len(model.Statuses)+3)
	for _, s := range model.Statuses {
		names = append(names, "status_"+s.String())
	}
	return append(names, "render_pending", "laravel_appeared_total", "laravel_lost_total")
}

// driftCounterNames — глобальные строки с ДЕЛЬТА-ведомой семантикой:
// инкременты переходов поддерживают значение между проходами, дрейф в
// стационаре обязан быть нулевым (ТЗИ §7.1.3, §11). Строки status_*
// сюда не входят: распределение статусов пишется ТОЛЬКО реконсиляцией
// (ТЗИ §13.3 — старые статусы переходов не пинируются предикатами
// операторов), под живой нагрузкой их «дрейф» — freshwater лаг
// интервала, а не аномалия: Warn/gauge по ним шумели бы на каждом
// проходе, verify — никогда не сходился бы без допуска. Суточные строки
// checks:/temp_error: (T-179/T-182) — дельта-ведомые, но с потоковой
// семантикой расхождения (события записи результата HTTP против доменов
// дня всех веток конвейера, TZ_slow_done_today §5.1): их позиции
// daily:checks/daily:temp_error ведутся в проходе отдельно (имя дня
// вычисляет БД), а verify их не сверяет — под нагрузкой расхождение
// равно разнице потоков этапов с прошлого прохода (свежесть, не
// аномалия).
func driftCounterNames() []string {
	return []string{"render_pending", "laravel_appeared_total", "laravel_lost_total"}
}

// dailyChecksCounter — метка позиции суточного счётчика проверок в
// дрейфе прохода (T-179, TZ_slow_done_today §8). Потоковая, а не
// нулевая в стационаре: не участвует в агрегированном Warn
// (reportDrift), наблюдается только gauge.
const dailyChecksCounter = "daily:checks"

// dailyTempCounter — метка позиции суточного числителя temp_error в
// дрейфе прохода (T-182, TZ_slow_dns_share §6): та же потоковая
// семантика, что dailyChecksCounter (числитель и знаменатель расходятся
// с живым окном вместе — повторы эпизодов суток), вне Warn, наблюдается
// gauge.
const dailyTempCounter = "daily:temp_error"

// dailyStreamingPositions — суточные позиции потоковой семантики
// (T-179/T-182): инкременты считают события записи результатов HTTP,
// живое окно прохода — домены дня всех веток конвейера; стационарного
// нуля нет, агрегированный Warn дрейфа их не поднимает (шум под живой
// нагрузкой), gauge экспонирует.
func dailyStreamingPositions() []string {
	return []string{dailyChecksCounter, dailyTempCounter}
}

// ---------------------------------------------------------------- проход

// ZoneStatsDB — пул для прохода: db.DB с транзакциями (дрейф и
// перезапись — один снапшот); *pgxpool.Pool удовлетворяет интерфейсу
// (стиль RetryDB/RecordDB).
type ZoneStatsDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ ZoneStatsDB = (*pgxpool.Pool)(nil)

// ZoneStatsOptions — параметры реконсиляции; нулевые значения
// заменяются дефолтами (withDefaults, стиль scheduler.RecheckOptions).
type ZoneStatsOptions struct {
	// Interval — период цикла ZoneStatsLoop (DefaultZoneStatsInterval);
	// рабочее значение — monitor.zone_stats_interval из конфига. На разовый
	// ZoneStatsOnce не влияет.
	Interval time.Duration
	// Now — источник времени для zones.updated_at и crawler_counters.
	// updated_at; nil — time.Now. Тесты подставляют фиксированные часы
	// (стиль scheduler, T-030).
	Now func() time.Time
	// Zone — фильтр разового прохода (recount --zone, ТЗИ §9):
	// перезаписываются колонки только выбранной зоны, глобальные
	// счётчики пересчитываются всегда. nil — все зоны; на цикл
	// ZoneStatsLoop не влияет (периодический проход всегда полный).
	Zone *int16
	// Log — логгер цикла ZoneStatsLoop; nil — записи подавлены
	// (slog.DiscardHandler). ZoneStatsOnce не логирует — итог возвращается
	// ZoneStatsResult.
	Log *slog.Logger
}

// withDefaults заменяет неположительные значения дефолтами (Now
// проверяется отдельно: функций — nil).
func (o ZoneStatsOptions) withDefaults() ZoneStatsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultZoneStatsInterval
	}
	return o
}

// CounterPosition — позиция реконсиляции: имя для метрики/CLI
// (zone:<id>:pending, global:status_alive — ТЗИ §11) и пара
// старое/новое. New — записанное значение, Old — прочитанное до
// перезаписи (у verify — живой агрегат против сохранённого).
type CounterPosition struct {
	Counter string
	Old     int64
	New     int64
}

// Drift — |старое − новое| позиции.
func (p CounterPosition) Drift() int64 {
	d := p.New - p.Old
	if d < 0 {
		return -d
	}
	return d
}

// ZoneStatsResult — итог одного прохода ZoneStatsOnce.
type ZoneStatsResult struct {
	// Zones — зоны, чьи колонки перезаписаны (число строк RETURNING —
	// каждая строка зон обновляется один раз).
	Zones int64
	// Positions — все позиции прохода (колонки зон + глобальные
	// скаляры) со старым/новым значениями; Positions[i].Drift() > 0 —
	// дрейф. Пустой может быть только у прохода с фильтром несуществующей
	// зоны.
	Positions []CounterPosition
}

// zoneColumnPositions — метрики позиций колонок зоны (порядок —
// колонки readZonesSQL/recountZonesSQL RETURNING, без id).
var zoneColumnPositions = [...]string{"total", "pending", "queued", "checked", "laravel"}

// ZoneStatsOnce выполняет один совмещённый проход реконсиляции (ТЗИ
// §7.1) в одной транзакции: (1) читает старые значения зон, счётчиков
// и суточных строк (проверок и temp_error, T-179/T-182); (2) единственный
// полный скан domains — per-zone агрегат (recountAggQuerySQL, включая
// FILTER-колонки last_checked_at = current_date, T-179/T-182), агрегаты
// laravel_domains и laravel_history; (3) перезаписывает колонки
// зонunnest-пачкой значений (блокировки зон — первыми); (4) upsert-ит
// глобальные скаляры (блокировки crawler_counters — после зон, порядок
// против дедлоков — recountZonesSQL) и суточные строки (последними, в
// порядке checks: → temp_error:); RETURNING операторов даёт новые
// значения для дрейфа. Проход идемпотентен (ТЗ 3.4): повтор даёт те же
// значения; сбой откатывает транзакцию целиком — половинчатых состояний
// не бывает. Фильтр Zone (recount --zone) ограничивает перезапись зон;
// глобальные скаляры и суточные строки пересчитываются всегда (ТЗИ §9).
func ZoneStatsOnce(ctx context.Context, pool ZoneStatsDB, opts ZoneStatsOptions) (ZoneStatsResult, error) {
	if pool == nil {
		return ZoneStatsResult{}, errors.New("monitor: реконсиляция счётчиков: пул не задан")
	}
	o := opts.withDefaults()
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return ZoneStatsResult{}, fmt.Errorf("monitor: реконсиляция счётчиков: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	oldZones, err := readZonePositions(ctx, tx, o.Zone)
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldCounters, err := readCounterValues(ctx, tx, CounterNames())
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// Суточные строки — тем же блоком старых чтений (снапшот один): имя
	// дня вычисляет БД (T-179/T-182), присутствие — признак заполнения
	// (дрейф только у существующей строки).
	oldDaily, dailyExists, err := readDailyValue(ctx, tx, readDailyChecksSQL, "суточная строка проверок")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldTemp, tempExists, err := readDailyValue(ctx, tx, readDailyTempSQL, "суточная строка temp_error")
	if err != nil {
		return ZoneStatsResult{}, err
	}

	// Единственный полный скан domains: per-zone агрегат + смежные.
	agg, err := readZoneAgg(ctx, tx)
	if err != nil {
		return ZoneStatsResult{}, err
	}
	lar, err := readLaravelAgg(ctx, tx)
	if err != nil {
		return ZoneStatsResult{}, err
	}
	hAppeared, hLost, err := readHistoryAgg(ctx, tx)
	if err != nil {
		return ZoneStatsResult{}, err
	}

	// Колонки зон: значения — агрегат, зона без живых доменов — нули.
	// Порядок обхода детерминирован (slice zoneIDs отсортирован).
	zoneIDs := make([]int16, 0, len(oldZones))
	for id := range oldZones {
		zoneIDs = append(zoneIDs, id)
	}
	sort.Slice(zoneIDs, func(i, j int) bool { return zoneIDs[i] < zoneIDs[j] })
	zTotal := make([]int64, len(zoneIDs))
	zPending := make([]int64, len(zoneIDs))
	zQueued := make([]int64, len(zoneIDs))
	zChecked := make([]int64, len(zoneIDs))
	zLaravel := make([]int64, len(zoneIDs))
	aggByZone := make(map[int16]*zoneAggRow, len(agg))
	for i := range agg {
		aggByZone[agg[i].zone] = &agg[i]
	}
	larByZone := make(map[int16]int64, len(lar))
	for _, l := range lar {
		larByZone[l.zone] = l.laravel
	}
	for i, id := range zoneIDs {
		if a := aggByZone[id]; a != nil {
			zTotal[i], zPending[i], zQueued[i], zChecked[i] = a.total, a.pending, a.queued, a.checked
		}
		zLaravel[i] = larByZone[id]
	}
	newZones, err := writeZonePositions(ctx, tx, zoneIDs, zTotal, zPending, zQueued, zChecked, zLaravel, now())
	if err != nil {
		return ZoneStatsResult{}, err
	}

	// Глобальные скаляры: суммы per-zone агрегата + кумулятивы журнала.
	globals := globalCounters(agg, hAppeared, hLost)
	names := make([]string, 0, len(globals))
	values := make([]int64, 0, len(globals))
	for _, name := range CounterNames() { // детерминированный порядок
		names = append(names, name)
		values = append(values, globals[name])
	}
	newCounters, err := writeGlobalCounters(ctx, tx, names, values, now())
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// Суточные строки — последними записями прохода (T-179/T-182):
	// значение — сумма FILTER-колонок f_checks_today/f_temp_today, строка
	// дня — всегда глобальная (фильтр Zone касался только колонок зон);
	// порядок «checks: → temp_error:» общий с инкрементами record.go —
	// без циклов блокировок. Ротация суток — просто новая строка: проход
	// после полуночи создаёт её значением окна, вчерашняя остаётся
	// историей темпа.
	var checksToday, tempToday int64
	for i := range agg {
		checksToday += agg[i].checksToday
		tempToday += agg[i].tempToday
	}
	newDaily, err := writeDailyCounter(ctx, tx, recountDailyChecksSQL, checksToday, now(), "суточная строка проверок")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newTemp, err := writeDailyCounter(ctx, tx, recountDailyTempSQL, tempToday, now(), "суточная строка temp_error")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ZoneStatsResult{}, fmt.Errorf("monitor: реконсиляция счётчиков: фиксация: %w", err)
	}

	res := ZoneStatsResult{Zones: int64(len(newZones)), Positions: make([]CounterPosition, 0, len(newZones)*len(zoneColumnPositions)+len(driftCounterNames())+len(dailyStreamingPositions()))}
	for _, id := range zoneIDs {
		nz, ok := newZones[id]
		if !ok {
			continue // RETURNING не вернул (зону удалили мимо кода) — старая остаётся
		}
		oz := oldZones[id]
		newVals := [...]int64{nz.total, nz.pending, nz.queued, nz.checked, nz.laravel}
		oldVals := [...]int64{oz.total, oz.pending, oz.queued, oz.checked, oz.laravel}
		for i, col := range zoneColumnPositions {
			res.Positions = append(res.Positions, CounterPosition{
				Counter: fmt.Sprintf("zone:%d:%s", id, col),
				Old:     oldVals[i],
				New:     newVals[i],
			})
		}
	}
	// Позиции дрейфа — только дельта-ведомые счётчики (driftCounterNames):
	// status_* исключены (пишутся одной реконсиляцией — их «дрейф» это
	// свежесть интервала, не аномалия).
	for _, name := range driftCounterNames() {
		res.Positions = append(res.Positions, CounterPosition{
			Counter: "global:" + name,
			Old:     oldCounters[name],
			New:     newCounters[name],
		})
	}
	// Суточные позиции (T-179/T-182): дрейф — потоковая разница семантик,
	// а не аномалия: инкременты считают события записи результата
	// HTTP-этапа, живое окно — домены дня всех веток (DNS-терминальные
	// завершения — not_delegated и пр. — эпизода HTTP не имеют), между
	// ними — повторные эпизоды суток. Числитель temp_error расходится
	// вдобавок ретраями, закрывшими ошибку (событие осталось в истории).
	// Стационарного нуля нет (в отличие от render_pending), позиции
	// наблюдаемые. Строки дня ещё не было — заполнение, не дрейф
	// (Old := New): расхождение с нулём считало бы in-flight записи
	// DNS-этапа, к инкрементам отношения не имеющие (прецедент —
	// стартовый проход цикла, ТЗИ §7.1.3).
	dailyOld := oldDaily
	if !dailyExists {
		dailyOld = newDaily
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: dailyChecksCounter,
		Old:     dailyOld,
		New:     newDaily,
	})
	tempOld := oldTemp
	if !tempExists {
		tempOld = newTemp
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: dailyTempCounter,
		Old:     tempOld,
		New:     newTemp,
	})
	return res, nil
}

// laravelAggRow — строка агрегата laravel_domains.
type laravelAggRow struct {
	zone    int16
	laravel int64
}

// readZoneAgg — per-zone агрегат domainsAggSQL: колонки в порядке
// домена, затем легенда statusAggCols, бит рендера, суточное окно
// проверок и его temp_error-числитель (T-179/T-182). Приёмник — db.DB
// (транзакция прохода или пул verify: агрегат один, точка правды —
// ТЗИ §9).
func readZoneAgg(ctx context.Context, q db.DB) ([]zoneAggRow, error) {
	rows, err := q.Query(ctx, recountAggQuerySQL)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: агрегат: %w", err)
	}
	defer rows.Close()
	var out []zoneAggRow
	for rows.Next() {
		var r zoneAggRow
		r.statuses = make([]int64, len(statusAggCols))
		dest := make([]any, 0, 5+len(statusAggCols)+3)
		dest = append(dest, &r.zone, &r.total, &r.pending, &r.queued, &r.checked)
		for i := range r.statuses {
			dest = append(dest, &r.statuses[i])
		}
		dest = append(dest, &r.render, &r.checksToday, &r.tempToday)
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: агрегат: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: агрегат: %w", err)
	}
	return out, nil
}

// readLaravelAgg — агрегат laravel_domains по зонам.
func readLaravelAgg(ctx context.Context, tx pgx.Tx) ([]laravelAggRow, error) {
	rows, err := tx.Query(ctx, laravelAggQuerySQL)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: laravel-агрегат: %w", err)
	}
	defer rows.Close()
	var out []laravelAggRow
	for rows.Next() {
		var r laravelAggRow
		if err := rows.Scan(&r.zone, &r.laravel); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: laravel-агрегат: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: laravel-агрегат: %w", err)
	}
	return out, nil
}

// readHistoryAgg — кумулятивы appeared/lost журнала детекции.
func readHistoryAgg(ctx context.Context, tx pgx.Tx) (appeared, lost int64, err error) {
	rows, err := tx.Query(ctx, historyAggSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: пустой ответ агрегата")
	}
	if err := rows.Scan(&appeared, &lost); err != nil {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: %w", err)
	}
	return appeared, lost, nil
}

// writeZonePositions — перезапись колонок зонunnest-пачкой; возврат —
// новые значения колонок по RETURNING.
func writeZonePositions(ctx context.Context, tx pgx.Tx, ids []int16, total, pending, queued, checked, laravel []int64, at time.Time) (map[int16]zonePositionRow, error) {
	out := make(map[int16]zonePositionRow, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := tx.Query(ctx, recountZonesSQL, ids, total, pending, queued, checked, at, laravel)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int16
		var r zonePositionRow
		if err := rows.Scan(&id, &r.total, &r.pending, &r.queued, &r.checked, &r.laravel); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: чтение: %w", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: %w", err)
	}
	if int64(len(out)) != int64(len(ids)) {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: перезаписано %d из %d (строки зон обязательны)", len(out), len(ids))
	}
	return out, nil
}

// writeGlobalCounters — upsert глобальных скаляров; возврат — новые
// значения по RETURNING.
func writeGlobalCounters(ctx context.Context, tx pgx.Tx, names []string, values []int64, at time.Time) (map[string]int64, error) {
	rows, err := tx.Query(ctx, recountCountersSQL, names, values, at)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64, len(names))
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: чтение: %w", err)
		}
		out[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: %w", err)
	}
	if len(out) != len(names) {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: перезаписано %d из %d строк", len(out), len(names))
	}
	return out, nil
}

// readDailyValue — старое значение суточной строки (T-179 checks:,
// T-182 temp_error:) в транзакции прохода: sqlQ — точечный PK-лукап с
// именем дня, вычисляемым БД; what — контекст ошибок. exists различает
// «0» от «строки дня ещё нет» (заполнение — не дрейф). Пустой набор без
// ошибки rows.Err — честное отсутствие строки.
func readDailyValue(ctx context.Context, tx pgx.Tx, sqlQ, what string) (value int64, exists bool, err error) {
	rows, err := tx.Query(ctx, sqlQ)
	if err != nil {
		return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	defer rows.Close()
	if !rows.Next() {
		// Сначала сбой чтения: обрыв соединения — не «строки нет»
		// (стиль renderDebtCount).
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
		}
		return 0, false, nil
	}
	if err := rows.Scan(&value); err != nil {
		return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: чтение: %w", what, err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	return value, true, nil
}

// writeDailyCounter — перезапись суточной строки значением агрегата
// прохода (recountDailyChecksSQL/recountDailyTempSQL, T-179/T-182);
// возврат — новое значение по RETURNING (дрейф). Upsert создаёт строку
// первого прохода дня (или исправляет созданную инкрементами — проход
// авторитетен).
func writeDailyCounter(ctx context.Context, tx pgx.Tx, sqlQ string, value int64, at time.Time, what string) (int64, error) {
	rows, err := tx.Query(ctx, sqlQ, value, at)
	if err != nil {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: пустой ответ upsert", what)
	}
	var fresh int64
	if err := rows.Scan(&fresh); err != nil {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: чтение: %w", what, err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	return fresh, nil
}

// globalCounters — глобальные скаляры из per-zone агрегата (суммы
// легенды статусов и бита рендера — один скан domains уже выполнен)
// и кумулятивов журнала. Одна точка правды соответствия «зона →
// глобальные суммы» для реконсиляции и verify (ТЗИ §9).
func globalCounters(agg []zoneAggRow, appeared, lost int64) map[string]int64 {
	out := make(map[string]int64, len(statusAggCols)+3)
	for i, c := range statusAggCols {
		out["status_"+c.status.String()] = 0
		for j := range agg {
			out["status_"+c.status.String()] += agg[j].statuses[i]
		}
	}
	var render int64
	for j := range agg {
		render += agg[j].render
	}
	out["render_pending"] = render
	out["laravel_appeared_total"] = appeared
	out["laravel_lost_total"] = lost
	return out
}

// zonePositionRow — значения колонок одной зоны (старые или новые).
type zonePositionRow struct {
	total, pending, queued, checked, laravel int64
}

// readZonePositions — текущие значения колонок зон (в транзакции
// прохода: до оператора — старые, после — собственная запись).
func readZonePositions(ctx context.Context, tx pgx.Tx, zone *int16) (map[int16]zonePositionRow, error) {
	rows, err := tx.Query(ctx, readZonesSQL, zone)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение зон: %w", err)
	}
	defer rows.Close()
	out := make(map[int16]zonePositionRow)
	for rows.Next() {
		var id int16
		var r zonePositionRow
		if err := rows.Scan(&id, &r.total, &r.pending, &r.queued, &r.checked, &r.laravel); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение зон: строка: %w", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение зон: %w", err)
	}
	return out, nil
}

// readCounterValues — текущие значения глобальных счётчиков по именам
// (отсутствующая строка — 0: первый проход после миграции заполняет,
// ТЗИ §5).
func readCounterValues(ctx context.Context, tx pgx.Tx, names []string) (map[string]int64, error) {
	rows, err := tx.Query(ctx, readCountersSQL, names)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение счётчиков: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64, len(names))
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение счётчиков: строка: %w", err)
		}
		out[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение счётчиков: %w", err)
	}
	return out, nil
}

// ZoneStatsLoop — периодический цикл реконсиляции (ТЗ 5.9, ТЗИ §7.2):
// ZoneStatsOnce по ticker, первый проход — сразу при старте (свежие
// счётчики после простоя/рестарта и заполнение нулей после миграции —
// потому первый проход дрейгом не считается: Warn/gauge по нему не
// поднимаются, ТЗИ §7.1.3), а не ждёт тика. Ошибка прохода прерывает
// только его: логируется Warn, цикл живёт (контракт OnRun, T-012) —
// очередной проход повторит по актуальному состоянию. Отмена ctx —
// чистый выход с nil (graceful shutdown, ТЗ 3.4). Итог прохода —
// Debug, а не Info: проход успешен всегда, когда жива БД.
//
// Single-flight (T-161): проходы выполняются в горутине самого цикла —
// новый проход структурно не может начаться, пока не завершился
// предыдущий; проход дольше интервала (рост таблицы, замер §12)
// сбрасывает накопившийся тик ticker'а (иначе новый проход стартовал бы
// сразу за медленным — непрерывный агрегат), Warn и счётчик
// crawler_zonestats_skipped_ticks_total поднимают шум. reg — реестр
// метрик сервиса (метрики прохода и дрейфа; nil — цикл без метрик,
// пересчёт живёт: смысл цикла — реконсиляция, а не приборы).
func ZoneStatsLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts ZoneStatsOptions) error {
	if pool == nil {
		return errors.New("monitor: реконсиляция счётчиков: пул не задан")
	}
	txPool, ok := pool.(ZoneStatsDB)
	if !ok {
		return errors.New("monitor: реконсиляция счётчиков: пул без транзакций (ZoneStatsDB)")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := newZoneStatsLoopMetrics(reg)
	log.Info("цикл реконсиляции счётчиков конвейера",
		"interval", o.Interval.String())

	// Тикер до стартового прохода: тики отсчитываются от старта цикла,
	// дренаж накопившегося тика в pass видит один и тот же тикер.
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	first := true
	pass := func() {
		started := time.Now()
		res, err := ZoneStatsOnce(ctx, txPool, o)
		switch {
		// Тело ветки пустое (только комментарий): branch/case подменяет
		// его на noop-EmptyStmt, печать которого идентична оригиналу, —
		// мутант с пустым диффом, тестом не убить; подавление генерации —
		// единственная альтернатива блэклисту чексумм (§4).
		// mutator-disable-next-line branch/case
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход реконсиляции счётчиков", "err", err)
		default:
			reportDrift(log, m, res, first)
			log.Debug("проход реконсиляции счётчиков", "zones", res.Zones)
			// «Первый» снимается только успешным проходом (ревью P3-5):
			// сбойный старт (БД ещё не готова) счётчиков не заполнил —
			// следующий проход всё ещё «заполняющий», его разница не
			// дрейф (ТЗИ §7.1.3).
			first = false
		}
		took := time.Since(started)
		m.observe(took.Seconds())
		if took > o.Interval {
			// Single-flight: сбросить тик, накопившийся за медленный
			// проход, — иначе новый проход стартовал бы сразу за этим
			// (select/default — тика может и не быть: Warn честен
			// по длительности, дренаж в этом случае no-op).
			select {
			case <-ticker.C:
			default:
			}
			log.Warn("проход реконсиляции дольше интервала — накопившийся тик пропущен",
				"took", took.String(), "interval", o.Interval.String())
			m.skip()
		}
	}

	pass()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// reportDrift — наблюдение дрейфа (ТЗИ §7.1.3): gauge каждой позиции
// (стационар — 0), Warn при ненулевом дрейфе любой позиции — КРОМЕ
// суточных строк (T-179/T-182): их «дрейф» — потоковая разница семантик
// (события записи HTTP против доменов дня всех веток), ненулевая под
// живой нагрузкой почти всегда — Warn каждые 5 минут хоронил бы сигнал
// остальных позиций под шумом (отступление от §8 TZ_slow_done_today,
// писавшегося в предположении «дрейф = повторы, доля мала»;
// наблюдаемость позиций — gauge). Первый проход цикла (fillFirst) не
// наблюдается вовсе: после миграции/простоя он заполняет нули — старые
// значения не дрейф, а отсутствие счётчиков (ТЗИ §5); метрики по нему
// не поднимаются, со второго прохода gauge экспонирует все позиции
// (включая нулевые). Warn агрегирует: проход по 2 тыс. зон с полным
// дрейфом (рестарт на обнулённых счётчиках) не должен флудить журнал —
// счётчик позиций, сумма |Δ| и первые пять имён; полный список —
// crawlerctl counters verify (ТЗИ §9).
func reportDrift(log *slog.Logger, m *zoneStatsLoopMetrics, res ZoneStatsResult, fillFirst bool) {
	if fillFirst {
		return
	}
	if m != nil {
		for _, p := range res.Positions {
			m.drift.WithLabelValues(p.Counter).Set(float64(p.Drift()))
		}
	}
	streaming := dailyStreamingPositions()
	var drifted []CounterPosition
	var sum int64
	for _, p := range res.Positions {
		if slices.Contains(streaming, p.Counter) || p.Drift() == 0 {
			continue
		}
		drifted = append(drifted, p)
		sum += p.Drift()
	}
	if len(drifted) == 0 {
		return
	}
	top := make([]string, 0, 5)
	for i, p := range drifted {
		if i == 5 {
			break
		}
		top = append(top, fmt.Sprintf("%s: %d → %d", p.Counter, p.Old, p.New))
	}
	log.Warn("реконсиляция: дрейф счётчиков (инкременты разошлись с агрегатом; следующий проход уже перезаписал)",
		"positions", len(drifted), "sum_abs", sum, "top", top)
}

// zoneStatsLoopMetrics — приборы цикла реконсиляции (T-161/T-178):
// длительность прохода, счётчик пропущенных тиков и gauge дрейфа;
// nil-приёмник — no-op (цикл без реестра, стиль scheduler.backlogLoopMetrics).
type zoneStatsLoopMetrics struct {
	seconds prometheus.ObserverVec
	skipped *prometheus.CounterVec
	drift   *prometheus.GaugeVec
}

// newZoneStatsLoopMetrics — приборы в реестре reg (nil — nil). Корзины —
// экспонента 0.5 с…~8.5 мин: проход — агрегат по всем доменам (замер
// §12 — десятки секунд на 47 млн строк), дефолтные корзины Prometheus
// (потолок 10 с) малы. crawler_counters_recount_seconds — переименование
// crawler_zonestats_pass_seconds (T-178, ТЗИ §7.2: проход совмещённый),
// корзины прежние.
func newZoneStatsLoopMetrics(reg *metrics.Registry) *zoneStatsLoopMetrics {
	if reg == nil {
		return nil
	}
	return &zoneStatsLoopMetrics{
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_counters_recount_seconds",
			Help:    "Длительность совмещённого прохода реконсиляции счётчиков (агрегат domains/laravel_domains/laravel_history → zones + crawler_counters, T-078/T-178).",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 11),
		}),
		skipped: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_zonestats_skipped_ticks_total",
			Help: "Пропущенные тики цикла реконсиляции: проход длился дольше интервала, накопившийся тик не запускает новый проход (single-flight, T-161).",
		}),
		drift: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_counters_drift",
			Help: "Дрейф инкрементальных счётчиков на проходе реконсиляции: |старое − новое| по позициям (zone:<id>:колонка, global:<имя>; daily:checks и daily:temp_error — суточный темп проверок и его temp_error-числитель, T-179/T-182). Стационар — 0, устойчивый ненулевой дрейф — пропущенный переход или ручной SQL мимо кода (разбор — docs/runbooks.md, T-178); исключение — суточные позиции daily:*: они потоковые (события записи HTTP против доменов дня всех веток конвейера), ненулевая под живой нагрузкой — читается как наблюдаемая величина, в Warn не входит.",
		}, "counter"),
	}
}

// observe — фиксация длительности прохода; nil — no-op.
func (m *zoneStatsLoopMetrics) observe(seconds float64) {
	if m == nil {
		return
	}
	m.seconds.WithLabelValues().Observe(seconds)
}

// skip — пропуск тика; nil — no-op.
func (m *zoneStatsLoopMetrics) skip() {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues().Inc()
}

// ---------------------------------------------------------------- verify

// verifyZonesSQL — сверка колонок зон с живым агрегатом без записи
// (ТЗИ §9): сохранённые значения рядом с агрегатом; COALESCE — зона без
// живых доменов вне агрегата. $1 — фильтр зоны (NULL — все).
var verifyZonesSQL = "SELECT z.id, " +
	"z.domains_total, COALESCE(a.total, 0), " +
	"z.domains_pending, COALESCE(a.pending, 0), " +
	"z.domains_queued, COALESCE(a.queued, 0), " +
	"z.domains_checked, COALESCE(a.checked, 0), " +
	"z.domains_laravel, COALESCE(l.laravel, 0) " +
	"FROM zones AS z " +
	"LEFT JOIN (" + domainsAggSQL + ") AS a ON a.zone_id = z.id " +
	"LEFT JOIN (SELECT zone_id, count(*) AS laravel FROM laravel_domains GROUP BY zone_id) AS l ON l.zone_id = z.id " +
	"WHERE ($1::int2 IS NULL OR z.id = $1) ORDER BY z.id"

// VerifyOptions — параметры сверки; используется crawlerctl counters
// verify (ТЗИ §9) и интеграционными тестами (готовая verify-логика,
// ТЗИ §12.2).
type VerifyOptions struct {
	// Zone — фильтр зоны (nil — все зоны; глобальные счётчики сверяются
	// всегда полностью).
	Zone *int16
	// Tolerance — допуск расхождения (по умолчанию 0 — счётчики обязаны
	// сходиться точно; > 0 — для сверки под живой нагрузкой, когда между
	// чтением сохранённого и живого агрегата проходят инкременты).
	Tolerance int64
}

// VerifyCounters сверяет счётчики с живыми агрегатами БЕЗ записи
// (ТЗИ §9): сохранённые значения zones/crawler_counters против
// агрегатов domains/laravel_domains/laravel_history (тот же per-zone
// агрегат и функция глобальных сумм, что у реконсиляции, — одна точка
// правды, ТЗИ §9). Возвращает позиции с расхождением выше допуска
// (Old — сохранённое, New — живой агрегат); пустой срез — счётчики
// сходятся. Снимок не атомарен между сохранённым и живым чтением —
// под живой нагрузкой единичные расхождения в темпе переходов закрывает
// tolerance.
func VerifyCounters(ctx context.Context, pool db.DB, opts VerifyOptions) ([]CounterPosition, error) {
	if pool == nil {
		return nil, errors.New("monitor: сверка счётчиков: пул не задан")
	}

	var out []CounterPosition

	rows, err := pool.Query(ctx, verifyZonesSQL, opts.Zone)
	if err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: зоны: %w", err)
	}
	for rows.Next() {
		var id int16
		var vals [10]int64
		dest := make([]any, len(vals)+1)
		dest[0] = &id
		for i := range vals {
			dest[i+1] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			rows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: зоны: чтение: %w", err)
		}
		for i, col := range zoneColumnPositions {
			old, fresh := vals[2*i], vals[2*i+1]
			if absDiff(old, fresh) > opts.Tolerance {
				out = append(out, CounterPosition{
					Counter: fmt.Sprintf("zone:%d:%s", id, col),
					Old:     old,
					New:     fresh,
				})
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("monitor: сверка счётчиков: зоны: %w", err)
	}
	rows.Close()

	// Живые глобальные скаляры: тот же агрегат и та же функция сумм,
	// что у реконсиляции (одна точка правды соответствия).
	agg, err := readZoneAgg(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: %w", err)
	}
	var appeared, lost int64
	{
		hrows, err := pool.Query(ctx, historyAggSQL)
		if err != nil {
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: %w", err)
		}
		if !hrows.Next() {
			hrows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: пустой ответ агрегата")
		}
		if err := hrows.Scan(&appeared, &lost); err != nil {
			hrows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: чтение: %w", err)
		}
		if err := hrows.Err(); err != nil {
			hrows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: %w", err)
		}
		hrows.Close()
	}
	live := globalCounters(agg, appeared, lost)
	stored, err := readStoredCounters(ctx, pool)
	if err != nil {
		return nil, err
	}
	for _, name := range driftCounterNames() {
		// Только дельта-ведомые скаляры: распределение статусов пишется
		// только реконсиляцией (ТЗИ §13.3) — сверка «сохранённое против
		// живого» под нагрузкой расходится на темп статусов, это
		// свежесть интервала, не дрейф. Суточные строки checks:/temp_error:
		// (T-179/T-182) — тоже мимо сверки: инкременты считают события
		// записи результата HTTP, живой агрегат — домены дня всех веток
		// (TZ_slow_done_today §5.1); под нагрузкой расхождение равно
		// разнице потоков этапов с прошлого прохода — наблюдается
		// позициями daily:* прохода реконсиляции, сверка с допуском 0
		// сигналила бы штатную работу.
		if absDiff(stored[name], live[name]) > opts.Tolerance {
			out = append(out, CounterPosition{
				Counter: "global:" + name,
				Old:     stored[name],
				New:     live[name],
			})
		}
	}
	return out, nil
}

// readStoredCounters — сохранённые строки crawler_counters (сверка:
// отсутствующая строка — 0).
const storedCountersSQL = "SELECT name, value FROM crawler_counters"

func readStoredCounters(ctx context.Context, pool db.DB) (map[string]int64, error) {
	rows, err := pool.Query(ctx, storedCountersSQL)
	if err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: сохранённые: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, fmt.Errorf("monitor: сверка счётчиков: сохранённые: чтение: %w", err)
		}
		out[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: сохранённые: %w", err)
	}
	return out, nil
}

// absDiff — |a − b|.
func absDiff(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}
