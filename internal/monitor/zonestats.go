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
//     миграции 000015 и инкременты renderq: без фильтра статуса; бит
//     удаляемого домена снимает применение delete той же транзакцией с
//     декрементом, T-186 — редкие рассинхроны гасит этот проход),
//     laravel_appeared_total/laravel_lost_total — агрегат
//     laravel_history (кумулятивы T-118), checks:YYYY-MM-DD — суточный
//     счётчик проверок (T-179, TZ_slow_done_today §6: живой аналог
//     прежнего подзапроса count(*) … WHERE last_checked_at =
//     current_date; имя дня вычисляет БД — отдельным оператором после
//     скаляров, в unnest-пачку с Go-именем не кладётся: рассинхрон
//     таймзоны Go/БД молча развёл бы строку и окно FILTER),
//     temp_error:YYYY-MM-DD — суточный числитель доли temp_error
//     (T-182, TZ_slow_dns_share §6: та же FILTER-цена нуля, знаменатель
//     алерта доли — строка checks:), корзины зрелости ретрая
//     retry_mature:YYYY-MM-DD (T-202: зрелые кандидаты temp_error по
//     датам эпизодов — guard тяжёлой выборки ретрая; FILTER-колонки
//     «зрелые сегодня/сегодняшние эпизоды», устаревшие корзины проход
//     удаляет — живут две строки, вчерашняя и сегодняшняя), часовые
//     корзины темпа results:/rendered:YYYY-MM-DD"T"HH24 (T-203: окна
//     «последний час» темпа дашборда; живые подзапросы по
//     subdomain_results деградировали heap-fetch'ами — контрольная
//     точка этапа 27; агрегат окна двух живых часов по checked_at —
//     FILTER-позиции prev/cur, rendered — асинхронный флаг этапа
//     рендера в часе ЭПИЗОДА строки; проход держит две корзины каждой
//     семьи — предыдущий час (хвост окна чтения: поздние рендеры
//     дорабатывают его весь текущий час) и текущий, устаревшие
//     удаляет) и часовые корзины неанализированных хвостов переанализа
//     reanalyze:YYYY-MM-DD"T"HH24 (T-204: строки с rules_version IS
//     NULL по часам эпизодов — guard часового тика переанализа; те же
//     FILTER-позиции prev/cur агрегата окна, две живые корзины,
//     устаревшие удаляет);
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
// last_checked_at = current_date × статус, T-179/T-182), и корзины
// зрелости ретрая temp_error по датам эпизодов (T-202) —
// дополнительная цена FILTER-колонок ноль (ТЗИ §13.3). Часовые корзины
// темпа (T-203) и хвостов переанализа (T-204) считаются ОТДЕЛЬНЫМ
// агрегатом subdomain_results по окну двух живых часов (index-диапазон
// checked_at — цена пропорциональна окну, в ≤ 2 раза больше прежнего
// ежеминутного читателя темпа, здесь — раз в zone_stats_interval).
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

// DefaultZoneStatsInterval — период реконсиляции: 30 минут. Исходные
// 60 с (верх коридора ТЗ 5.9) рассчитаны на малый корпус: замер §12
// docs/load-measurements.md (47 млн строк) — UPDATE-агрегат в тройке
// главных статей CPU postgres; T-161 поднял до 5 мин, T-178 оставил
// период реконсиляции тем же: в стационаре это единственный полный
// проход domains (критерий ТЗИ §1.2). Решение оператора 26.10 (T-199)
// по заготовке ТЗИ §7.1 «после приработки волны интервал можно
// разредить до 15–30 мин»: замер окна медленных запросов после деплоя
// 3bffc62 (25.10 18:16 → 26.10 03:05 UTC) — реконсиляция единственный
// оставшийся полный проход, 48 % суммарного медленного времени БД
// (100 событий >3 с, медиана 39 с, max 501 с) при проходе раз в 5 мин;
// интервал разрежен до 30 мин, дрейф между проходами обязан быть
// нулевым — по замеру дрейфа оператор видит, когда интервал можно
// вернуть короче. Коридор ТЗ остаётся доступным конфигом
// monitor.zone_stats_interval; тот же период у цикла алертов —
// порог остановки прогресса AlertStallFactor × интервал растёт
// пропорционально (25 мин → 2,5 ч).
const DefaultZoneStatsInterval = 30 * time.Minute

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
// инкрементов T-175 (без фильтра статуса: бит удаляемых доменов снимает
// применение delete той же транзакцией, T-186, — предикат остаётся
// статус-нейтральным, как бэкфилл и инкременты). f_checks_today —
// суточное окно проверок (T-179): тот же предикат
// last_checked_at = current_date, что у прежнего подзапроса done_today
// throughputSQL (включая строки deleted — живой аналог их не фильтровал);
// FILTER-колонка — цена нуля на уже идущем скане (ТЗИ §13.3), сумма по
// зонам — значение строки checks:YYYY-MM-DD. f_temp_today — числитель
// доли temp_error суток (T-182, TZ_slow_dns_share §6): то же окно
// × статус temp_error, FILTER-цена нуля (живой аналог прежнего dnsShareSQL
// считал окно целиком и ходил в heap за статусом — запрос №6 журнала
// медленных 24.10), сумма по зонам — значение строки
// temp_error:YYYY-MM-DD. f_retry_mature_today/f_retry_mature_past —
// корзины зрелости ретрая temp_error (T-202): зрелые кандидаты
// (status = temp_error × check_state = done) по датам эпизодов —
// сегодняшние эпизоды (last_checked_at = current_date, зреют к полуночи
// следующего дня) и созревшая когорта (last_checked_at <
// current_date — окно ретрая «старше 24 ч» по date-колонке зреет
// когортами в полночь); суммы по зонам — значения корзин
// retry_mature:YYYY-MM-DD, читаемых guard тяжёлой выборки ретрая
// (scheduler/retry.go, retryMatureSQL), FILTER-цена нуля на том же
// скане.
var domainsAggSQL = func() string {
	cols := make([]string, 0, len(statusAggCols)+9)
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
		fmt.Sprintf("count(*) FILTER (WHERE last_checked_at = current_date AND status = %d) AS f_temp_today", int16(model.StatusTempError)),
		fmt.Sprintf("count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at = current_date) AS f_retry_mature_today", int16(model.StatusTempError), int16(model.CheckStateDone)),
		fmt.Sprintf("count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at < current_date) AS f_retry_mature_past", int16(model.StatusTempError), int16(model.CheckStateDone)))
	return "SELECT zone_id, " + strings.Join(cols, ", ") + " FROM domains GROUP BY zone_id"
}()

// zoneAggRow — строка per-zone агрегата (колонки domainsAggSQL): значения
// состояний и легенда статусов по порядку statusAggCols + бит рендера +
// суточное окно проверок (T-179), его temp_error-числитель (T-182) и
// корзины зрелости ретрая (T-202: сегодняшние/созревшие эпизоды
// temp_error × done).
type zoneAggRow struct {
	zone                            int16
	total, pending, queued, checked int64
	statuses                        []int64 // по порядку statusAggCols
	render                          int64
	checksToday                     int64 // FILTER last_checked_at = current_date (T-179)
	tempToday                       int64 // FILTER last_checked_at = current_date AND status = temp_error (T-182)
	retryToday                      int64 // FILTER status = temp_error AND done AND last_checked_at = current_date (T-202)
	retryPast                       int64 // FILTER status = temp_error AND done AND last_checked_at < current_date (T-202)
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

// hourlyAggSQL — живой агрегат часовых корзин (T-203): окно двух
// живых часов по subdomain_results (границы вычисляет БД —
// date_trunc('hour', now()), индекс checked_at обслуживает диапазон),
// FILTER-позиции — те же предикаты, что у прежних живых подзапросов
// results_hour/rendered_hour throughputSQL (та же граница «последний
// час», календарная: prev — строки часа [trunc(now−1ч), trunc(now)),
// cur — [trunc(now), …]; WHERE — нижняя граница окна). rendered —
// асинхронный флаг этапа рендера: позиция часа учитывает строки,
// отрендеренные ПОЗЖЕ своей проверки — как и инкремент renderworker
// (час эпизода строки), потому корзины сходятся с живым count(*)
// по календарному часу. prev_reanalyze/cur_reanalyze (T-204) — хвосты
// переанализа: строки с rules_version IS NULL (никогда не
// анализировались — вставка записи результата или замена контента
// рендером) — предикат guard'а тикового прохода
// (analyzer/reanalyze.go); цена FILTER-колонки ноль на том же скане.
// Скалярный агрегат без GROUP BY — ровно одна строка (пустое окно —
// нули, не пустой набор).
const hourlyAggSQL = "SELECT " +
	"count(*) FILTER (WHERE checked_at < date_trunc('hour', now())) AS prev_results, " +
	"count(*) FILTER (WHERE checked_at < date_trunc('hour', now()) AND rendered) AS prev_rendered, " +
	"count(*) FILTER (WHERE checked_at >= date_trunc('hour', now())) AS cur_results, " +
	"count(*) FILTER (WHERE checked_at >= date_trunc('hour', now()) AND rendered) AS cur_rendered, " +
	"count(*) FILTER (WHERE checked_at < date_trunc('hour', now()) AND rules_version IS NULL) AS prev_reanalyze, " +
	"count(*) FILTER (WHERE checked_at >= date_trunc('hour', now()) AND rules_version IS NULL) AS cur_reanalyze " +
	"FROM subdomain_results " +
	"WHERE checked_at >= date_trunc('hour', now() - interval '1 hour')"

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

// Корзины зрелости ретрая temp_error (T-202). Имя — дата эпизода
// (last_checked_at записи результата temp_error): корзина становится
// «зрелой» (суммируется guard'ом retryMatureSQL планировщика) в полночь
// следующего дня — момент созревания когорты при date-точности колонки.
// Проход поддерживает РОВНО ДВЕ живые корзины — вчерашнюю (зрелая
// когорта, value := FILTER f_retry_mature_past) и сегодняшнюю
// (эпизоды суток, value := FILTER f_retry_mature_today): старшие
// корзины удаляет (устаревший учёт дренажей прошлых суток и хвостовые
// декременты дат эпизодов старше вчерашней — их значение уже в сумме
// вчерашней корзины, пересчитанной агрегатом). Имена вычисляет БД
// (current_date/current_date − 1) — рассинхрон таймзоны Go/БД развёл бы
// корзину и окно FILTER (стиль T-179). Порядок блокировок — последними
// в проходе, после суточных строк, в порядке «очистка → зрелая →
// сегодняшняя»: общий с писателями (record.go: … → retry_mature:;
// retry.go: декремент корзины last_checked_at; monitor — здесь), без
// циклов блокировок. Оговорка (ревью T-202): порядок выполнения
// нессылаемых DML-CTE одного оператора PostgreSQL документацией не
// гарантируется («executed concurrently… unpredictable») — сегодня
// исполняются в порядке перечисления (зоны раньше счётчиков у всех
// писателей), что и даёт отсутствие циклов; при смене поведения PG
// смотреть сюда (радикальное лечение — отдельные операторы транзакции).
const (
	// readRetryPastSQL — старое значение зрелой корзины (вчерашней) до
	// перезаписи (дрейф, T-202); пустой результат — корзины ещё нет
	// (заполнение, не дрейф).
	readRetryPastSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD')"

	// readRetryTodaySQL — старое значение сегодняшней корзины (дрейф,
	// T-202); пустой результат — корзины нет до первого temp_error-эпизода
	// суток (инкремент или проход создаст).
	readRetryTodaySQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')"

	// retryMatureCleanupSQL — удаление корзин старше вчерашней: их
	// когорты уже слиты в зрелую корзину перезаписью ниже (агрегат
	// f_retry_mature_past считает статусы целиком, а не по датам), а
	// строки попадают в диапазон суммы guard'а — хвосты декрементов
	// дат старых эпизодов с отрицательными значениями искажали бы сумму
	// до ближайшего прохода; корзины — живое расписание дренажа, не
	// история темпа (в отличие от суточных строк, T-179 §10.4).
	// RETURNING — для единого Query-протокола прохода (значения не
	// читаются).
	retryMatureCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'retry_mature:' " +
		"AND name < 'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD') " +
		"RETURNING value"

	// recountRetryPastSQL — перезапись зрелой корзины значением
	// FILTER-колонки f_retry_mature_past (сумма по зонам), RETURNING —
	// новое значение для дрейфа.
	recountRetryPastSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountRetryTodaySQL — перезапись сегодняшней корзины значением
	// FILTER-колонки f_retry_mature_today (сумма по зонам); пара
	// recountRetryPastSQL, блокировка — последней из корзин.
	recountRetryTodaySQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"
)

// Корзины неанализированных хвостов переанализа (T-204):
// reanalyze:YYYY-MM-DD"T"HH24 — строки subdomain_results с
// rules_version IS NULL (никогда не анализировались) по часам ЭПИЗОДА
// (checked_at). Guard часового тика переанализа
// (analyzer/reanalyze.go, reanalyzeGuardSQL) суммирует корзины старше
// текущего часа: незрелый in-flight-поток (запись → q:analyze → штамп —
// секунды) живёт в корзине текущего часа и guard не открывает;
// пережившая границу часа без штампа публикация — потерянный хвост —
// открывает, тик вычитывает окно и публикует. Стационар без потерь
// стоит одну сумму индексного диапазона из пары строк — тяжёлая
// выборка окна (heap-визит ~84 тыс. строк/ч ради неиндексируемого
// rules_version, HOT-инвариант) не выполняется вовсе (уточнение того
// же класса, что T-191 → T-202: наивный счётчик «записано без
// анализа» не отличает хвосты от непрерывного потока — зрелость дают
// ЧАСОВЫЕ корзины, прецедент retry_mature). Инкременты — транзакции
// записи результата (record.go, час now() = checked_at строк) и замены
// контента рендером (renderworker, час эпизода из RETURNING —
// рендер-опоздание воскрешает корзину старого часа, guard открывается
// впустую: хвост за курсором, следующий проход корзину удаляет);
// декремент — штамп анализатора только устаревших строк (охрана
// версии, worker.go): повторные доставки не двоят, штампы старой
// версии (дренажи смены правил) дают отрицательный дрейф до прохода —
// держит guard закрытым на время дренажа, у тика там своя работа.
// Проход поддерживает РОВНО ДВЕ живые корзины — предыдущий час
// (пересчитывается, а не только текущая: поздние декременты штампов
// дорабатывают её весь текущий час) и текущий; устаревшие удаляет
// (по образцу retry_mature/hourly-семей T-202/T-203). Порядок
// блокировок — между корзинами ретрая и rendered-темпа (единый
// порядок семей всех писателей «retry_mature: → reanalyze: →
// rendered: → results:» — конвенция, НЕ лексикографика имён
// ('retry_mature:' лексикографически последний: 't' > 'a'/'n'/'s');
// от дедлоков важна лишь согласованность порядка у всех писателей),
// внутри семьи «очистка → prev → cur»: общий с писателями (record.go:
// … → retry_mature: → reanalyze: → results:; renderworker:
// reanalyze: → rendered:; analyzer: единственная точка reanalyze:) —
// без циклов. Guard видит суммы живых корзин сразу после фиксации.
const (
	// readHourlyReanalyzePrevSQL — старое значение корзины хвостов
	// предыдущего часа до перезаписи (дрейф, T-204); пустой результат —
	// корзины ещё нет (до первого инкремента/прохода; заполнение, не
	// дрейф).
	readHourlyReanalyzePrevSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyReanalyzeCurSQL — старое значение корзины хвостов
	// текущего часа (дрейф, T-204); пара readHourlyReanalyzePrevSQL.
	readHourlyReanalyzeCurSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

	// hourlyReanalyzeCleanupSQL — удаление корзин хвостов старше
	// предыдущего часа: живой хвост часа H обслуживается тиком часа H+1
	// (корзина H = prev, guard открыт); не подобранный за два часа
	// (сбойные тики) хвост за границей корзин — остаточный класс
	// время-формы (T-193 §4.2): рестарт с якорем 24 ч или полный дренаж
	// смены правил. RETURNING — протокол Query прохода (значения не
	// читаются).
	hourlyReanalyzeCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'reanalyze:' " +
		"AND name < 'reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24') " +
		"RETURNING value"

	// recountHourlyReanalyzePrevSQL — перезапись корзины хвостов
	// предыдущего часа значением FILTER-позиции prev_reanalyze агрегата
	// hourlyAggSQL, RETURNING — новое значение для дрейфа.
	recountHourlyReanalyzePrevSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyReanalyzeCurSQL — перезапись корзины хвостов текущего
	// часа (FILTER-позиция cur_reanalyze); блокировка — после
	// предыдущей (возрастание имён).
	recountHourlyReanalyzeCurSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"
)

// Часовые корзины темпа (T-203): results:YYYY-MM-DD"T"HH24 — строки
// subdomain_results, вставленные транзакцией записи результата HTTP-этапа
// (инкремент httpworker/record.go, дельта — RowsAffected INSERT-пачки;
// час корзины — date_trunc('hour', now()) той же транзакции, checked_at
// строк — тот же now()), rendered:… — строки, переведённые в
// rendered = true этапом рендера (инкремент renderworker/worker.go по
// checked_at строки из RETURNING UPDATE — час ЭПИЗОДА, не рендера: так
// считало живое окно прежнего читателя). Замена живых подзапросов
// results_hour/rendered_hour throughputSQL (count(*) по
// checked_at >= now() − 1 ч): index-only scan свежих страниц окна
// деградировал heap-fetch'ами — контрольная точка этапа 27 (медиана
// 6,0 с > 3 с, 108 событий; тот же механизм, что у суточного окна
// T-179 — замер §2 TZ_slow_done_today). Корзина — календарный час:
// скользящее окно «последний час» читатель (status.throughputSQL)
// собирает суммой ДВУХ живых корзин — предыдущей и текущей, делитель
// темпа — фактическая длина покрытого окна; сходимость корзин с живым
// count(*) гарантируется по календарному часу — здесь (hourlyAggSQL,
// FILTER-позиции prev/cur). Проход держит РОВНО ДВЕ живые корзины
// каждой семьи: предыдущий час — «хвост окна чтения» (поздние
// rendered-инкременты строк прошлого часа дорабатывают его корзину
// весь текущий час — потому пересчитывается, а не только текущая) и
// текущий час; устаревшие корзины удаляет (по образцу retry_mature,
// T-202 — корзины живое окно темпа, не история). Имена вычисляет БД
// (date_trunc от now()) — рассинхрон таймзоны Go/БД развёл бы корзину
// и окно агрегата (стиль T-179/T-202). Порядок блокировок — финал
// прохода после корзин ретрая, семьи лексикографически
// «rendered: → results:», внутри семьи «очистка → предыдущая →
// текущая» (возрастание имён: час лексикографичен): общий с писателями
// (record.go: … → retry_mature: → results: последним; renderworker:
// единственная точка rendered:; monitor — здесь), без циклов.
// Пограничный случай: rendered-инкремент опоздания > 1 ч (строка часа
// H−2 отрендерена в часе H) воскрешает корзину H−2 upsert'ом — читатель
// её не читает (IN-список двух живых часов), следующий проход удаляет;
// живому окну она не принадлежала и прежде (окно «последний час»).
const (
	// readHourlyRenderedPrevSQL — старое значение корзины rendered-темпа
	// предыдущего часа до перезаписи (дрейф, T-203); пустой результат —
	// корзины ещё нет (до первого rendered-эпизода часа; заполнение, не
	// дрейф).
	readHourlyRenderedPrevSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyRenderedCurSQL — старое значение корзины rendered-темпа
	// текущего часа (дрейф, T-203); пара readHourlyRenderedPrevSQL.
	readHourlyRenderedCurSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'rendered:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyResultsPrevSQL — старое значение корзины темпа
	// результатов предыдущего часа (дрейф, T-203).
	readHourlyResultsPrevSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyResultsCurSQL — старое значение корзины темпа
	// результатов текущего часа (дрейф, T-203).
	readHourlyResultsCurSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

	// hourlyRenderedCleanupSQL — удаление корзин rendered-темпа старше
	// предыдущего часа: их строки вне живого окна чтения (читатель
	// суммирует prev+cur), значение хвоста не нужно; поздние инкременты
	// устаревших часов см. комментарий семейства. RETURNING — протокол
	// Query прохода (значения не читаются).
	hourlyRenderedCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'rendered:' " +
		"AND name < 'rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24') " +
		"RETURNING value"

	// hourlyResultsCleanupSQL — удаление корзин темпа результатов старше
	// предыдущего часа; пара hourlyRenderedCleanupSQL (results-инкременты
	// всегда текущего часа — опозданий не бывает, хвост — только строки,
	// пересчитанные в prev).
	hourlyResultsCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'results:' " +
		"AND name < 'results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24') " +
		"RETURNING value"

	// recountHourlyRenderedPrevSQL — перезапись корзины rendered-темпа
	// предыдущего часа значением FILTER-позиции prev_rendered агрегата
	// hourlyAggSQL, RETURNING — новое значение для дрейфа.
	recountHourlyRenderedPrevSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyRenderedCurSQL — перезапись корзины rendered-темпа
	// текущего часа (FILTER-позиция cur_rendered); блокировка — после
	// предыдущей (возрастание имён).
	recountHourlyRenderedCurSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('rendered:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyResultsPrevSQL — перезапись корзины темпа результатов
	// предыдущего часа (FILTER-позиция prev_results); семья results: —
	// после rendered: (лексикографика префиксов — порядок блокировок).
	recountHourlyResultsPrevSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyResultsCurSQL — перезапись корзины темпа результатов
	// текущего часа (FILTER-позиция cur_results); финал прохода.
	recountHourlyResultsCurSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"
)

// CounterNames — имена строк глобальных счётчиков, поддерживаемых
// реконсиляцией (легенда для чтения/сверки). Суточные строки
// checks:YYYY-MM-DD и temp_error:YYYY-MM-DD (T-179/T-182), корзины
// зрелости ретрая retry_mature:YYYY-MM-DD (T-202) и часовые корзины
// темпа results:/rendered: и хвостов переанализа reanalyze:
// (T-203/T-204) сюда не входят: имена дней/часов вычисляет БД
// (readDailyChecksSQL/recountDailyChecksSQL, их temp-пары, пары корзин
// ретрая и часовых корзин), в unnest-пачке с Go-именами их нет.
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

// retryMaturePastCounter — метка зрелой корзины ретрая temp_error в
// дрейфе прохода (T-202): инкременты/декременты считают события
// (эпизоды записи результата против ретраев), живое окно FILTER —
// домены (status = temp_error × done × дата); дубль-доставки и
// повторные эпизоды суток дают +1 к живому нулю, кандидаты, ушедшие
// мимо ретрая (удаление temp_error-домена delete-файлом, возрастная
// перепроверка T-030), оставляют +1 в корзине до прохода — потоковая
// семантика (как daily:*, TZ_slow_done_today §5.1), вне Warn,
// наблюдается gauge.
const retryMaturePastCounter = "retry_mature:past"

// retryMatureTodayCounter — метка сегодняшней корзины ретрая temp_error
// (T-202): та же потоковая семантика, что retryMaturePastCounter
// (сегодняшние эпизоды: инкременты — события, FILTER — домены).
const retryMatureTodayCounter = "retry_mature:today"

// dailyStreamingPositions — суточные позиции потоковой семантики
// (T-179/T-182): инкременты считают события записи результатов HTTP,
// живое окно прохода — домены дня всех веток конвейера; стационарного
// нуля нет, агрегированный Warn дрейфа их не поднимает (шум под живой
// нагрузкой), gauge экспонирует.
func dailyStreamingPositions() []string {
	return []string{dailyChecksCounter, dailyTempCounter}
}

// retryMatureStreamingPositions — позиции корзин зрелости ретрая
// (T-202) той же потоковой семантики, что суточные строки: события
// эпизодов против живых доменов фильтра; в Warn не входят, наблюдается
// gauge (комментарий retryMaturePastCounter).
func retryMatureStreamingPositions() []string {
	return []string{retryMaturePastCounter, retryMatureTodayCounter}
}

// hourlyRenderedPrevCounter — метка корзины rendered-темпа предыдущего
// часа в дрейфе прохода (T-203): инкременты renderworker считают
// события (переводы строк в rendered), живой агрегат — строки окна;
// повторный рендер уже rendered-строки (повторная доставка, DOM тем же
// хэшем) и перезаписи эпизодов (запись результата удаляет отрендеренные
// строки) дают расхождение до прохода — потоковая семантика (как
// daily:*, TZ_slow_done_today §5.1), вне Warn, наблюдается gauge.
const hourlyRenderedPrevCounter = "rendered:prev"

// hourlyRenderedCurCounter — метка корзины rendered-темпа текущего часа
// (T-203): та же потоковая семантика, что hourlyRenderedPrevCounter.
const hourlyRenderedCurCounter = "rendered:cur"

// hourlyResultsPrevCounter — метка корзины темпа результатов
// предыдущего часа (T-203): инкременты записи результата считают
// события вставки строк, живой агрегат — строки (перезапись эпизода
// удаляет старые строки окна, не декрементируя корзину) — потоковая
// семантика, вне Warn, наблюдается gauge.
const hourlyResultsPrevCounter = "results:prev"

// hourlyResultsCurCounter — метка корзины темпа результатов текущего
// часа (T-203); пара hourlyResultsPrevCounter.
const hourlyResultsCurCounter = "results:cur"

// hourlyReanalyzePrevCounter — метка корзины неанализированных хвостов
// переанализа предыдущего часа в дрейфе прохода (T-204): инкременты
// записи/рендера и декременты штампов считают события переходов, живой
// агрегат — строки с rules_version IS NULL; перезапись эпизода
// (record.go DELETE без декремента), рендер непроштампованной строки
// (двойной инкремент одного хвоста) и штампы старой версии (дренажи)
// дают расхождение до прохода — потоковая семантика (семейство
// T-202/T-203), вне Warn, наблюдается gauge.
const hourlyReanalyzePrevCounter = "reanalyze:prev"

// hourlyReanalyzeCurCounter — метка корзины хвостов переанализа
// текущего часа (T-204); пара hourlyReanalyzePrevCounter.
const hourlyReanalyzeCurCounter = "reanalyze:cur"

// hourlyStreamingPositions — позиции часовых корзин темпа (T-203) и
// хвостов переанализа (T-204) потоковой семантики: события вставки
// строк/перевода в rendered/штампа против живых строк окна;
// стационарного нуля нет (дубль-доставки, перезаписи эпизодов,
// дренажи), агрегированный Warn дрейфа их не поднимает (шум под живой
// нагрузкой), gauge экспонирует.
func hourlyStreamingPositions() []string {
	return []string{hourlyReanalyzePrevCounter, hourlyReanalyzeCurCounter,
		hourlyRenderedPrevCounter, hourlyRenderedCurCounter, hourlyResultsPrevCounter, hourlyResultsCurCounter}
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
// §7.1) в одной транзакции: (1) читает старые значения зон, счётчиков,
// суточных строк (проверок и temp_error, T-179/T-182), корзин зрелости
// ретрая (T-202), часовых корзин темпа (T-203) и корзин хвостов
// переанализа (T-204); (2) единственный полный скан domains — per-zone
// агрегат (recountAggQuerySQL, включая FILTER-колонки last_checked_at
// = current_date и корзины зрелости, T-179/T-182/T-202), агрегаты
// laravel_domains и laravel_history и агрегат часовых корзин
// (hourlyAggSQL — окно двух живых часов subdomain_results, позиции
// темпа T-203 и хвостов переанализа T-204); (3) перезаписывает колонки
// зон unnest-пачкой значений (блокировки зон — первыми); (4) upsert-ит
// глобальные скаляры (блокировки crawler_counters — после зон, порядок
// против дедлоков — recountZonesSQL), суточные строки (в порядке
// checks: → temp_error:), корзины зрелости ретрая (очистка устаревших
// → зрелая → сегодняшняя, T-202), корзины хвостов переанализа и
// часовые корзины темпа (финал прохода: reanalyze-семья, затем
// rendered-семья, затем results-семья, в каждой «очистка →
// предыдущая → текущая», T-203/T-204); RETURNING операторов даёт новые
// значения для дрейфа. Проход идемпотентен (ТЗ 3.4): повтор даёт те же
// значения; сбой откатывает транзакцию целиком — половинчатых
// состояний не бывает. Фильтр Zone (recount --zone) ограничивает
// перезапись зон; глобальные скаляры, суточные строки и корзины
// пересчитываются всегда (ТЗИ §9).
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
	// Корзины зрелости ретрая (T-202) — тем же блоком: присутствие —
	// признак заполнения (корзину создаёт инкремент или этот проход).
	oldRetryPast, retryPastExists, err := readDailyValue(ctx, tx, readRetryPastSQL, "зрелая корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldRetryToday, retryTodayExists, err := readDailyValue(ctx, tx, readRetryTodaySQL, "сегодняшняя корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// Часовые корзины темпа (T-203) — тем же блоком: присутствие —
	// признак заполнения (корзину создаёт инкремент записи/рендера или
	// этот проход), имя часа вычисляет БД.
	oldHourRenderedPrev, hourRenderedPrevExists, err := readDailyValue(ctx, tx, readHourlyRenderedPrevSQL, "корзина rendered-темпа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourRenderedCur, hourRenderedCurExists, err := readDailyValue(ctx, tx, readHourlyRenderedCurSQL, "корзина rendered-темпа текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourResultsPrev, hourResultsPrevExists, err := readDailyValue(ctx, tx, readHourlyResultsPrevSQL, "корзина темпа результатов предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourResultsCur, hourResultsCurExists, err := readDailyValue(ctx, tx, readHourlyResultsCurSQL, "корзина темпа результатов текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// Корзины хвостов переанализа (T-204) — тем же блоком: присутствие —
	// признак заполнения (корзину создаёт инкремент записи/рендера,
	// декремент штампа или этот проход), имя часа вычисляет БД.
	oldHourReanalyzePrev, hourReanalyzePrevExists, err := readDailyValue(ctx, tx, readHourlyReanalyzePrevSQL, "корзина хвостов переанализа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourReanalyzeCur, hourReanalyzeCurExists, err := readDailyValue(ctx, tx, readHourlyReanalyzeCurSQL, "корзина хвостов переанализа текущего часа")
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
	// Агрегат часовых корзин темпа (T-203): окно двух живых часов
	// subdomain_results —FILTER-позиции prev/cur обеих семей.
	hourly, err := readHourlyAgg(ctx, tx)
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
	// Корзины зрелости ретрая (T-202) — последними записями прохода:
	// устаревшие корзины удаляются (значение зрелой когорты несут
	// целиком в вчерашней корзине), затем перезаписываются зрелая
	// (FILTER f_retry_mature_past) и сегодняшняя (f_retry_mature_today)
	// — суммы тех же FILTER-колонок per-zone агрегата; порядок
	// «очистка → зрелая → сегодняшняя» замыкает порядок блокировок
	// писателей (record.go/dnsworker: инкремент сегодняшней; retry:
	// декремент дат эпизодов) — без циклов. Guard тяжёлой выборки
	// ретрая видит сумму зрелых корзин сразу после фиксации.
	if err := cleanupBaskets(ctx, tx, retryMatureCleanupSQL, "очистка корзин ретрая"); err != nil {
		return ZoneStatsResult{}, err
	}
	var retryTodaySum, retryPastSum int64
	for i := range agg {
		retryTodaySum += agg[i].retryToday
		retryPastSum += agg[i].retryPast
	}
	newRetryPast, err := writeDailyCounter(ctx, tx, recountRetryPastSQL, retryPastSum, now(), "зрелая корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newRetryToday, err := writeDailyCounter(ctx, tx, recountRetryTodaySQL, retryTodaySum, now(), "сегодняшняя корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// Корзины хвостов переанализа (T-204) — за корзинами ретрая, перед
	// корзинами rendered-темпа (порядок блокировок семей
	// «retry_mature: → reanalyze: → rendered: → results:», комментарий
	// семейства): устаревшие корзины удаляются, затем перезаписываются
	// prev (FILTER-позиция prev_reanalyze — поздние декременты штампов
	// дорабатывают её весь текущий час) и cur. Guard тика переанализа
	// видит суммы живых корзин сразу после фиксации.
	if err := cleanupBaskets(ctx, tx, hourlyReanalyzeCleanupSQL, "очистка корзин хвостов переанализа"); err != nil {
		return ZoneStatsResult{}, err
	}
	newHourReanalyzePrev, err := writeDailyCounter(ctx, tx, recountHourlyReanalyzePrevSQL, hourly.prevReanalyze, now(), "корзина хвостов переанализа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newHourReanalyzeCur, err := writeDailyCounter(ctx, tx, recountHourlyReanalyzeCurSQL, hourly.curReanalyze, now(), "корзина хвостов переанализа текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// Часовые корзины темпа (T-203) — финал прохода: устаревшие корзины
	// удаляются (вне окна чтения), затем перезаписываются живые —
	// prev (хвост окна чтения: поздние rendered-инкременты дорабатывают
	// его весь текущий час) и cur; семьи в лексикографике префиксов
	// «rendered: → results:», внутри — «очистка → prev → cur» — порядок
	// блокировок общий с писателями (record.go: … → results: последним;
	// renderworker: rendered:) — без циклов. Читатель темпа
	// (status.throughputSQL) видит суммы живых корзин сразу после
	// фиксации.
	if err := cleanupBaskets(ctx, tx, hourlyRenderedCleanupSQL, "очистка корзин rendered-темпа"); err != nil {
		return ZoneStatsResult{}, err
	}
	newHourRenderedPrev, err := writeDailyCounter(ctx, tx, recountHourlyRenderedPrevSQL, hourly.prevRendered, now(), "корзина rendered-темпа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newHourRenderedCur, err := writeDailyCounter(ctx, tx, recountHourlyRenderedCurSQL, hourly.curRendered, now(), "корзина rendered-темпа текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	if err := cleanupBaskets(ctx, tx, hourlyResultsCleanupSQL, "очистка корзин темпа результатов"); err != nil {
		return ZoneStatsResult{}, err
	}
	newHourResultsPrev, err := writeDailyCounter(ctx, tx, recountHourlyResultsPrevSQL, hourly.prevResults, now(), "корзина темпа результатов предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newHourResultsCur, err := writeDailyCounter(ctx, tx, recountHourlyResultsCurSQL, hourly.curResults, now(), "корзина темпа результатов текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ZoneStatsResult{}, fmt.Errorf("monitor: реконсиляция счётчиков: фиксация: %w", err)
	}

	res := ZoneStatsResult{Zones: int64(len(newZones)), Positions: make([]CounterPosition, 0, len(newZones)*len(zoneColumnPositions)+len(driftCounterNames())+len(dailyStreamingPositions())+len(retryMatureStreamingPositions())+len(hourlyStreamingPositions()))}
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
	// Корзины зрелости ретрая (T-202): дрейф — потоковая разница семантик
	// (события эпизодов/ретраев против живых доменов фильтра — дубль-доставки,
	// повторные эпизоды суток, кандидаты мимо ретрая), не аномалия:
	// стационарного нуля нет, позиции наблюдаемые, в Warn не входят
	// (retryMatureStreamingPositions). Корзины не было — заполнение, не
	// дрейф (Old := New), прецедент стартового прохода (ТЗИ §7.1.3).
	retryPastOld := oldRetryPast
	if !retryPastExists {
		retryPastOld = newRetryPast
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: retryMaturePastCounter,
		Old:     retryPastOld,
		New:     newRetryPast,
	})
	retryTodayOld := oldRetryToday
	if !retryTodayExists {
		retryTodayOld = newRetryToday
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: retryMatureTodayCounter,
		Old:     retryTodayOld,
		New:     newRetryToday,
	})
	// Часовые корзины темпа (T-203): дрейф — потоковая разница семантик
	// (события вставки строк/перевода в rendered против живых строк окна
	// — дубль-доставки, перезаписи эпизодов, повторные рендеры), не
	// аномалия: стационарного нуля нет, позиции наблюдаемые, в Warn не
	// входят (hourlyStreamingPositions). Корзины не было — заполнение,
	// не дрейф (Old := New), прецедент стартового прохода (ТЗИ §7.1.3).
	appendHourly := func(counter string, old int64, exists bool, newV int64) {
		if !exists {
			old = newV
		}
		res.Positions = append(res.Positions, CounterPosition{
			Counter: counter,
			Old:     old,
			New:     newV,
		})
	}
	appendHourly(hourlyRenderedPrevCounter, oldHourRenderedPrev, hourRenderedPrevExists, newHourRenderedPrev)
	appendHourly(hourlyRenderedCurCounter, oldHourRenderedCur, hourRenderedCurExists, newHourRenderedCur)
	appendHourly(hourlyResultsPrevCounter, oldHourResultsPrev, hourResultsPrevExists, newHourResultsPrev)
	appendHourly(hourlyResultsCurCounter, oldHourResultsCur, hourResultsCurExists, newHourResultsCur)
	// Корзины хвостов переанализа (T-204): та же потоковая семантика —
	// события переходов против живых NULL-строк окна (комментарий
	// hourlyReanalyzePrevCounter); корзины не было — заполнение, не
	// дрейф (Old := New), прецедент стартового прохода (ТЗИ §7.1.3).
	appendHourly(hourlyReanalyzePrevCounter, oldHourReanalyzePrev, hourReanalyzePrevExists, newHourReanalyzePrev)
	appendHourly(hourlyReanalyzeCurCounter, oldHourReanalyzeCur, hourReanalyzeCurExists, newHourReanalyzeCur)
	return res, nil
}

// laravelAggRow — строка агрегата laravel_domains.
type laravelAggRow struct {
	zone    int16
	laravel int64
}

// readZoneAgg — per-zone агрегат domainsAggSQL: колонки в порядке
// домена, затем легенда statusAggCols, бит рендера, суточное окно
// проверок, его temp_error-числитель (T-179/T-182) и корзины зрелости
// ретрая (T-202). Приёмник — db.DB
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
		dest := make([]any, 0, 5+len(statusAggCols)+5)
		dest = append(dest, &r.zone, &r.total, &r.pending, &r.queued, &r.checked)
		for i := range r.statuses {
			dest = append(dest, &r.statuses[i])
		}
		dest = append(dest, &r.render, &r.checksToday, &r.tempToday, &r.retryToday, &r.retryPast)
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

// hourlyAggRow — FILTER-позиции агрегата часовых корзин (hourlyAggSQL,
// T-203/T-204): prev/cur семей темпа (results — все строки окна часа,
// rendered — с флагом итогового DOM) и хвостов переанализа (строки с
// rules_version IS NULL).
type hourlyAggRow struct {
	prevResults, prevRendered   int64
	curResults, curRendered     int64
	prevReanalyze, curReanalyze int64
}

// readHourlyAgg — агрегат часовых корзин (T-203/T-204): скалярный
// агрегат без GROUP BY обязан вернуть ровно одну строку (пустое окно —
// нули); пустой набор — обрыв протокола, ошибка.
func readHourlyAgg(ctx context.Context, tx pgx.Tx) (hourlyAggRow, error) {
	rows, err := tx.Query(ctx, hourlyAggSQL)
	if err != nil {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: %w", err)
	}
	defer rows.Close()
	var r hourlyAggRow
	if !rows.Next() {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: пустой ответ агрегата")
	}
	if err := rows.Scan(&r.prevResults, &r.prevRendered, &r.curResults, &r.curRendered,
		&r.prevReanalyze, &r.curReanalyze); err != nil {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: %w", err)
	}
	return r, nil
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

// cleanupBaskets — удаление устаревших корзин (DELETE … RETURNING,
// T-202/T-203-стиль): retryMatureCleanupSQL/hourlyRenderedCleanupSQL/
// hourlyResultsCleanupSQL. RETURNING value — протокол Query (проход не
// использует Exec), значения не читаются: живые корзины ниже
// перезаписываются агрегатом целиком, удалённые — вне окна чтения.
func cleanupBaskets(ctx context.Context, tx pgx.Tx, sqlQ, what string) error {
	rows, err := tx.Query(ctx, sqlQ)
	if err != nil {
		return fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	defer rows.Close()
	var discarded int64
	for rows.Next() {
		// Значение удалённой корзины не читается: живые позиции
		// переписываются агрегатом целиком.
		if err := rows.Scan(&discarded); err != nil {
			return fmt.Errorf("monitor: реконсиляция счётчиков: %s: чтение: %w", what, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	return nil
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
// суточных строк (T-179/T-182), корзин зрелости ретрая (T-202) и
// часовых корзин темпа (T-203): их «дрейф» — потоковая разница семантик
// (события записи HTTP против доменов дня всех веток), ненулевая под
// живой нагрузкой почти всегда — Warn каждым проходом (каждые 30 мин)
// хоронил бы сигнал
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
	streaming := append(append(dailyStreamingPositions(), retryMatureStreamingPositions()...), hourlyStreamingPositions()...)
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
			Help: "Дрейф инкрементальных счётчиков на проходе реконсиляции: |старое − новое| по позициям (zone:<id>:колонка, global:<имя>; daily:checks и daily:temp_error — суточный темп проверок и его temp_error-числитель, T-179/T-182). Стационар — 0, устойчивый ненулевой дрейф — пропущенный переход или ручной SQL мимо кода (разбор — docs/runbooks.md, T-178); исключения — потоковые позиции (считают события против живых доменов/строк, ненулевые под живой нагрузкой — наблюдаемая величина, в Warn не входят): суточные daily:* (T-179/T-182), корзины зрелости ретрая retry_mature:past/retry_mature:today (T-202), часовые корзины темпа rendered:prev/rendered:cur/results:prev/results:cur (T-203 — prev/cur: предыдущий и текущий календарный час) и хвостов переанализа reanalyze:prev/reanalyze:cur (T-204).",
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
		// (T-179/T-182), корзины зрелости ретрая retry_mature: (T-202) и
		// часовые корзины темпа results:/rendered: (T-203) и хвостов
		// переанализа reanalyze: (T-204) — тоже мимо сверки: инкременты
		// считают события (записи результата, переводы в rendered,
		// штампы версий), живой агрегат — строки окна; под нагрузкой
		// расхождение равно разнице потоков с прошлого прохода —
		// наблюдается позициями daily:*/retry_mature:*/rendered:* /
		// results:*/reanalyze:* прохода реконсиляции, сверка с допуском 0
		// сигналила бы штатную работу (у корзин ретрая/темпа/хвостов
		// вдобавок проход держит только две живые строки на семью и
		// свёртывает старые, поимённая сверка дат/часов не определена).
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
