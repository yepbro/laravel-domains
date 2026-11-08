// Scheduler: компенсатор «статус повис» (T-270, ТЗ tasks/TZ-fsm.md §3
// ФТ-2.2) — ранний recheck доменов status = new ∧ check_state = done,
// перевод Р15 спецификации автомата check_state (checkstate.go).
//
// Аномалия (О6 без О7–О9, общая таблица статусов internal/model/enums.go):
// DNS-этап записал ветку «есть адреса» (check_state → done, статус
// остаётся new — итог решает HTTP-этап), а задача q:http потеряна уже
// ПОСЛЕ XADD — доставки не будет (рестарт Redis без персистентности,
// docs/redis.md §5; гибель записей в OOM-окно,
// docs/load-measurements.md §11.1 — когорта инцидентной недели
// 22–29.10.2026, 1865 доменов, вскрыта gauge T-256). Строка выпадает из
// всех штатных возвратов: lease-проход T-029 видит только queued,
// возрастная перепроверка T-030 созревает по recheck_interval (4380 ч ≈
// полгода) — класс потерь открыт для повторения.
//
// Компенсатор возвращает такие строки в pending — перевыберет диспетчер
// (T-027), эпизод пройдёт конвейер заново. Переиздать потерянное
// сообщение q:http нельзя: HTTPTask несёт адреса субдоменов DNS-эпизода,
// нигде не хранящиеся (internal/queue/message.go) — возврат в pending
// через штатный конвейер единственно возможен, повторная проверка
// идемпотентна (at-least-once, ТЗ 3.4). Дубль задачи после оживания
// стоявшего воркера (поздний клейм PEL) гасится идемпотентностью
// записи HTTP-этапа — асимметрия А3: recordDomainSQL пишет итог БЕЗ
// охраны done, перезаписывает эпизод DELETE+INSERT и переиздаёт
// q:analyze (httpworker/record.go).
//
// Порог — окно gauge: 24 ч по date-колонке (ТЗ 6.1) — когорта зреет в
// полночь (date < now() − 24 ч эквивалентно date < current_date,
// прецедент retrySelectSQL T-031 и корзин T-202, комментарий
// zonestats.go) — компенсатор и gauge видят одну и ту же когорту.
// Плата date-гранулярности — полуночный борт: минимальный возраст
// строки в когорте — МИНУТЫ (эпизод в 23:59 в 00:05 уже «старше
// суток»), окно пересекается с живыми доставками q:http (клейм PEL
// claim_min_idle 10 мин × max_attempts 3 — задача легально в полёте
// до ~30 мин, internal/queue/reliab.go). Дубль задачи полуночного
// борта гасится асимметрией А3: запись HTTP-итога идёт без охраны
// done, перезаписывает эпизод DELETE+INSERT и переиздаёт q:analyze —
// корректность сохраняется, цена — повторный эпизод. Стационарно
// q:http-бэклог — минуты: полуночная когорта пустеет до первого тика
// (guard закрыт нулём ближайшей реконсиляции); инцидентный бэклог,
// переживший полночь, дренируется однократно с дублем — ожидаемый
// всплеск счётчика, не потери (runbooks §2.10). Строки СЕГОДНЯШНИХ
// эпизодов компенсатор не видит вовсе: сегодняшние in-flight задачи
// не трогаются.
//
// Пустое множество — дешёвая проба (guard, стиль T-202 retryMatureSQL):
// до выборки читается строка crawler_counters['new_done_stale'] —
// уровень аномалии, который пишет ТОЛЬКО реконсиляция monitor (T-256:
// FILTER-колонка f_new_done_stale прохода zonestats.go, инкрементов
// нет), окно фильтра идентично окну выборки. Стационарно (когорта
// вылечена) ноль закрывает guard — domains не сканируется вовсе,
// часовой тик стоит один PK-лукап; свежесть уровня —
// monitor.zone_stats_interval (30 мин), суммарная задержка ремонта ≤
// интервал цикла + интервал реконсиляции. Дрейф guard (счётчик видит
// когорту, выборка — нет) — один холостой seq scan за тик до ближайшей
// реконсиляции (остаточный случай TZ T-191 §4.3).
//
// Выборка пачки — seq scan с ранним остановом LIMIT, закреплённый SET
// LOCAL в транзакции выборки (T-191, образец retryCandidates/T-181):
// индекса по предикату status = new сознательно нет (status в ключе или
// предикате индекса ломал бы HOT массовых апдейтов статуса,
// docs/schema.md), а частичный domains_recheck_idx (check_state = done,
// T-104) планировщику «доказуем» — слепая оценка обещает тысячи строк,
// и диапазонный скан вырождается в марафон по миллионам done-строк с
// heap-визитом каждой (запрос №1 журнала медленных 25.10: медиана 74,8 с
// — комментарий retrySelectSQL). Перевод — ОТДЕЛЬНОЙ транзакцией
// PK-пробами (форма leaseNullUpdateSQL, T-200): закрепление плана не
// наследуется переводу, unnest → PK-пробы индексны по построению.
//
// Wiring в cmd/scheduler — фоновый цикл StaleRequeueLoop (T-270).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

const (
	// StaleRequeueWindow — порог возраста last_checked_at аномалии
	// «статус повис». НЕ конфигурируется: порог обязан совпадать с окном
	// gauge new_done_stale (ФТ-2.2: last_checked_at < current_date —
	// «24 ч» канонического вида проекта на date-колонке, зреет когортами
	// в полночь, прецедент T-031/T-202; полуночный борт и его плата —
	// комментарий в шапке файла), больший бессмысленно задерживал бы
	// ремонт и развёл бы компенсатор с gauge.
	StaleRequeueWindow = 24 * time.Hour

	// DefaultStaleRequeueBatch — доменов в одной пачке перевода (задача
	// T-270: ≤1000): пачка ограничивает транзакцию перевода — блокировки
	// удерживаются минимум времени (стиль T-020).
	DefaultStaleRequeueBatch = 1000

	// DefaultStaleRequeueInterval — период цикла StaleRequeueLoop (задача
	// T-270: 1 ч): ремонт не срочный (когорта уже старше суток), в
	// стационаре тик стоит один PK-лукап guard.
	DefaultStaleRequeueInterval = time.Hour
)

// staleRequeueGuardSQL — дешёвая проба пустого множества (стиль guard
// T-202/retryMatureSQL): точечный PK-лукап строки new_done_stale —
// уровня аномалии, который пишет только реконсиляция monitor (T-256),
// окно FILTER идентично окну выборки компенсатора (date <
// current_date ≡ date < now() − 24 ч). coalesce-подзапрос гарантирует
// ровно одну строку (стиль dnsDailyCountersSQL): до первого прохода
// реконсиляции строки нет — честный ноль, guard закрыт (источник уровня
// — monitor, без него компенсатор честно спит). Значение ≤ 0 — когорты
// нет, тяжёлая выборка не выполняется вовсе.
const staleRequeueGuardSQL = "SELECT coalesce((SELECT value FROM crawler_counters WHERE name = 'new_done_stale'), 0)"

// staleRequeueSelectSQL — пачка аномальных строк: status = new ∧
// check_state = done ∧ last_checked_at старше порога (NULL отметки
// сравнением отсеиваются: писатели done ставят её всегда, NULL у
// new∧done — мусор ручного SQL, не потерянная задача — та же семантика
// FILTER у gauge, zonestats.go). Коды схемы — литералами через model.*
// (стиль leasenull.go). ORDER BY отсутствует сознательно (прецедент
// retrySelectSQL, T-191/T-181): порядок пачки не важен — охраны перевода
// перепроверяют состояние на момент блокировки, повторный проход
// безопасен; сортировка держала бы индексный доступ к таблице. Форму
// плана (seq scan с ранним остановом LIMIT) закрепляет транзакция
// выборки SET LOCAL-ами (staleRequeuePlan*SQL): без закрепления
// планировщик садится на «доказуемый» частичный domains_recheck_idx и
// вырождает окно в марафон по миллионам done-строк (замер 25.10 —
// комментарий retrySelectSQL).
var staleRequeueSelectSQL = fmt.Sprintf(
	"SELECT d.zone_id, d.id FROM domains AS d "+
		"WHERE d.status = %d AND d.check_state = %d AND d.last_checked_at < $1 "+
		"LIMIT $2",
	int16(model.StatusNew), int16(model.CheckStateDone))

// Закрепление формы плана выборки (T-191, прецедент T-181): SET LOCAL в
// транзакции выборки (staleRequeueBatch) отключает индексные формы
// доступа к domains до оператора выборки — пачка за доли секунды
// независимо от статистики (оценке планировщика верить нельзя — см.
// staleRequeueSelectSQL). Действие SET LOCAL — только эта транзакция;
// перевод идёт отдельной, PK-пробы перевода не затронуты.
const (
	staleRequeuePlanNoIndexScanSQL  = "SET LOCAL enable_indexscan = off"
	staleRequeuePlanNoBitmapScanSQL = "SET LOCAL enable_bitmapscan = off"
	staleRequeuePlanNoIOSQL         = "SET LOCAL enable_indexonlyscan = off"
)

// staleRequeueUpdateSQL — перевод выбранной пачки в pending PK-пробами
// (guard-оператор вида leaseNullUpdateSQL/leaseUpdateSQL, T-200):
// планировщик ведёт от unnest и пробирует domains по PK (zone_id, id),
// скана со стороны UPDATE нет. Guard-фильтры те же, что в выборке, и
// выполняются на момент БЛОКИРОВКИ строки: HTTP-итог, записанный между
// выборкой и блокировкой (status перестаёт быть new — запись HTTP-этапа
// асимметрией А3 идёт без охраны done), delete-файл (status = deleted),
// победивший конкурент (recheck T-030, ретрай T-031, ручной recheck Р11)
// строку не переводят — идемпотентность повторных проходов (И6
// спецификации). Побочные действия Р15: queued_at = NULL (у done уже
// NULL — единая форма переводов в pending, И2); статус-итог и
// temp_attempts сохраняются (new — не temp_error, перепишет новая
// проверка); бит http_published НЕ трогается — снимет постановка Р1
// нового эпизода (И4: возврат в работу — только через pending).
// Дельты счётчиков зон (checked −n / pending +n — предикат пинирует
// старое состояние done, T-178) — CTE zc по RETURNING zone_id; zc
// ссылается на upd — порядок исполнения гарантирован, зон и счётчиков в
// одном операторе не смешивается (инвариант порядка блокировок
// docs/schema.md).
var staleRequeueUpdateSQL = fmt.Sprintf(
	"WITH upd AS (UPDATE domains AS t "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) "+
		"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
		"AND t.check_state = %d AND t.status = %d AND t.last_checked_at < $4 "+
		"RETURNING t.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_checked = z.domains_checked - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateDone), int16(model.StatusNew))

// StaleRequeueDB — пул для компенсатора: db.DB с транзакциями (стиль
// RetryDB/LeaseDB). Выборка и перевод — РАЗДЕЛЬНЫЕ транзакции: выборка
// закрепляет форму плана (SET LOCAL seq scan с ранним остановом),
// перевод unnest→PK-пробами не должен наследовать закрепление; каждая
// соблюдает порядок блокировок «domains → zones» (счётчиков проход не
// трогает — инвариант docs/schema.md); *pgxpool.Pool удовлетворяет
// интерфейсу (сессии наследуют LockTimeout T-192 и NoParallelGather
// T-171 из AfterConnect пула).
type StaleRequeueDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ StaleRequeueDB = (*pgxpool.Pool)(nil)

// StaleRequeueOptions — параметры компенсатора; нулевые значения
// заменяются дефолтами (withDefaults, стиль RecheckOptions). Порог
// StaleRequeueWindow константой, не полем: обязан совпадать с окном
// gauge (см. комментарий константы).
type StaleRequeueOptions struct {
	// BatchSize — максимум доменов в одной пачке
	// (DefaultStaleRequeueBatch).
	BatchSize int
	// Interval — период цикла StaleRequeueLoop; на разовый
	// StaleRequeueOnce не влияет.
	Interval time.Duration
	// Now — источник времени порога now() − StaleRequeueWindow; nil —
	// time.Now (стиль RecheckOptions: точность порога — сутки, секунды
	// расхождения часов процесса с БД несущественны).
	Now func() time.Time
	// Log — логгер цикла StaleRequeueLoop; nil — записи подавлены
	// (slog.DiscardHandler). StaleRequeueOnce не логирует — итог
	// возвращается StaleRequeueResult.
	Log *slog.Logger
	// Metrics — реестр Prometheus сервиса: счётчик
	// crawler_scheduler_stale_requeued_total (T-270); nil — без метрик
	// (изолированные сборки/тесты).
	Metrics *metrics.Registry
}

// withDefaults заменяет неположительные значения дефолтами (Now
// проверяется отдельно: функций — nil).
func (o StaleRequeueOptions) withDefaults() StaleRequeueOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultStaleRequeueBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultStaleRequeueInterval
	}
	return o
}

// StaleRequeueResult — итог одного прохода StaleRequeueOnce.
type StaleRequeueResult struct {
	// Domains — строки, переведённые в pending ЭТИМ проходом (по RETURNING
	// перевода, после охран — стиль LeaseNullResult).
	Domains int64
	// Selected — строк в пачке выборки: LIMIT заполнен целиком — за ней
	// ещё есть, дренаж StaleRequeueLoop продолжает следующей пачкой.
	Selected int
}

// staleRequeueRow — строка пачки: ключ домена (состояний и отметок
// хранить не нужно — охраны перевода перепроверяют всё на месте).
type staleRequeueRow struct {
	zone int16
	id   int64
}

// staleRequeueMetrics — счётчик компенсатора (T-270); nil — no-op (стиль
// leaseNullMetrics).
type staleRequeueMetrics struct {
	requeued prometheus.Counter
}

// newStaleRequeueMetrics — счётчик в реестре reg (nil — nil): имя/HELP —
// точка правды документации метрики (docs/services.md); регистрация
// идемпотентна (T-076) — построение на каждый проход безопасно.
func newStaleRequeueMetrics(reg *metrics.Registry) *staleRequeueMetrics {
	if reg == nil {
		return nil
	}
	return &staleRequeueMetrics{
		requeued: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_scheduler_stale_requeued_total",
			Help: "Компенсатор «статус повис» (ФТ-2.2 ТЗ-fsm, T-270, переход Р15 спецификации check_state): перевод check_state done → pending доменов status=new с last_checked_at старше суток — задача q:http потеряна после XADD (рестарт Redis без персистентности, OOM-окно), lease/возрастная перепроверка строку не возвращают. Каждое событие — одна переведённая строка (по RETURNING после охран); стационарно ноль, рост — потери q:http (уровень — gauge crawler_domains_new_done_stale; у границы суток непустой q:http даёт штатный полуночный борт живых доставок — дубль эпизода гасится асимметрией А3, не потери), разбор docs/runbooks.md §2.10.",
		}).WithLabelValues(),
	}
}

// add — n переведённых строк; nil — no-op.
func (m *staleRequeueMetrics) add(n int64) {
	if m == nil {
		return
	}
	m.requeued.Add(float64(n))
}

// StaleRequeueOnce выполняет один проход компенсатора (ФТ-2.2, T-270):
// дешёвая проба guard по строке new_done_stale (закрыта — пустой
// результат без касания domains), до opts.BatchSize аномальных строк
// выбираются пачкой (seq scan с ранним остановом, закрепление SET LOCAL)
// и ОТДЕЛЬНОЙ транзакцией PK-пробами переводятся в check_state = pending
// — дельты счётчиков зон ведёт CTE оператора перевода (T-178), счётчик
// crawler_scheduler_stale_requeued_total растёт на фактически
// переведённых (opts.Metrics; nil — без метрик). Повторный проход
// безопасен (ТЗ 3.4): переведённые перестают удовлетворять отбору,
// охраны перепроверяют состояние на момент блокировки. Пустая пачка —
// no-op без перевода (дрейф guard: когорты нет, один холостой скан за
// тик — до ближайшей реконсиляции).
func StaleRequeueOnce(ctx context.Context, pool StaleRequeueDB, opts StaleRequeueOptions) (StaleRequeueResult, error) {
	if pool == nil {
		return StaleRequeueResult{}, errors.New("scheduler: компенсатор «статус повис»: пул не задан")
	}
	o := opts.withDefaults()

	// Guard пустого множества (стиль T-202): когорты нет — domains не
	// трогаем. Отрицательного уровня не бывает (строка — абсолют
	// реконсиляции, инкрементов нет), но guard обязан молчать и на нём.
	stale, err := staleRequeueGuard(ctx, pool)
	if err != nil {
		return StaleRequeueResult{}, err
	}
	if stale <= 0 {
		return StaleRequeueResult{}, nil
	}

	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	cutoff := now().Add(-StaleRequeueWindow)

	batch, err := staleRequeueBatch(ctx, pool, cutoff, o.BatchSize)
	if err != nil {
		return StaleRequeueResult{}, err
	}
	if len(batch) == 0 {
		return StaleRequeueResult{}, nil
	}

	// Порог один на выборку и перевод (симметрия recheck.go): гонка со
	// свежей записью результата не переведёт строку, чей возраст/судьба
	// изменились между транзакциями.
	met := newStaleRequeueMetrics(o.Metrics)
	n, err := staleRequeueTranslate(ctx, pool, batch, cutoff, o.Metrics)
	if err != nil {
		return StaleRequeueResult{}, err
	}
	met.add(n)
	return StaleRequeueResult{Domains: n, Selected: len(batch)}, nil
}

// staleRequeueGuard — дешёвая проба пустого множества (staleRequeueGuardSQL):
// уровень new_done_stale из crawler_counters; одна строка гарантирована
// coalesce-подзапросом — пустой набор есть обрыв протокола, а не «ноль».
func staleRequeueGuard(ctx context.Context, pool StaleRequeueDB) (int64, error) {
	rows, err := pool.Query(ctx, staleRequeueGuardSQL)
	if err != nil {
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: проба уровня new_done_stale: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: проба уровня new_done_stale: %w", err)
		}
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: проба уровня new_done_stale: пустой ответ агрегата")
	}
	var n int64
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: проба уровня new_done_stale: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: проба уровня new_done_stale: %w", err)
	}
	return n, nil
}

// staleRequeueBatch — пачка аномальных строк (staleRequeueSelectSQL) в
// собственной транзакции с закреплением плана (SET LOCAL ×3 до SELECT —
// стиль retryCandidates T-191): seq scan по domains с ранним остановом
// LIMIT. Транзакция — только оболочка закрепления: SELECT читает
// зафиксированные данные, фиксация/откат по содержанию равнозначны.
func staleRequeueBatch(ctx context.Context, pool StaleRequeueDB, cutoff time.Time, limit int) ([]staleRequeueRow, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: транзакция выборки: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, staleRequeuePlanNoIndexScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, staleRequeuePlanNoBitmapScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, staleRequeuePlanNoIOSQL); err != nil {
		return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: закрепление плана: %w", err)
	}

	rows, err := tx.Query(ctx, staleRequeueSelectSQL, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: выборка пачки: %w", err)
	}
	defer rows.Close()

	var out []staleRequeueRow
	for rows.Next() {
		var r staleRequeueRow
		if err := rows.Scan(&r.zone, &r.id); err != nil {
			return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: выборка пачки: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: выборка пачки: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("scheduler: компенсатор «статус повис»: фиксация выборки: %w", err)
	}
	return out, nil
}

// staleRequeueTranslate — перевод пачки PK-пробами (staleRequeueUpdateSQL)
// в собственной транзакции под 40P01-ретраем (T-268, страховка поверх
// инварианта порядка блокировок: повтор безопасен — охраны идемпотентны);
// reg — реестр для счётчика повторов crawler_db_deadlock_retried_total
// (общий конструктор прохода ретрая; nil — no-op), возвращает число
// переведённых строк по RETURNING (после охран).
func staleRequeueTranslate(ctx context.Context, pool StaleRequeueDB, batch []staleRequeueRow, cutoff time.Time, reg *metrics.Registry) (int64, error) {
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	for i, r := range batch {
		zones[i], ids[i] = r.zone, r.id
	}
	dbRetried := newRetryMetrics(reg)
	var n int64
	if err := db.RetryDeadlock(ctx, db.DefaultDeadlockRetry, dbRetried.dbDeadlockRetried, func() error {
		var err error
		n, err = staleRequeueTranslateOnce(ctx, pool, zones, ids, cutoff)
		return err
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// staleRequeueTranslateOnce — ОДНА попытка транзакции перевода (точка
// повтора 40P01 — staleRequeueTranslate).
func staleRequeueTranslateOnce(ctx context.Context, pool StaleRequeueDB, zones []int16, ids []int64, cutoff time.Time) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: транзакция перевода: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, staleRequeueUpdateSQL,
		int16(model.CheckStatePending),
		zones, ids,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: перевод пачки: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: перевод пачки: чтение: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: перевод пачки: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("scheduler: компенсатор «статус повис»: фиксация перевода: %w", err)
	}
	return n, nil
}

// StaleRequeueLoop — часовой цикл компенсатора «статус повис» (T-270):
// дренаж всех созревших пачками StaleRequeueOnce, затем ожидание тика.
// Первый дренаж — сразу при старте (проход идемпотентен и подбирает
// накопленное за простой — стиль стартового дренажа T-020/T-030).
// Ошибка прохода прерывает только текущий дренаж: логируется Warn, цикл
// живёт (контракт OnRun, T-012), очередной тик повторит — переведённое
// не возвращается дважды. Отмена ctx — чистый выход с nil (graceful
// shutdown, ТЗ 3.4).
func StaleRequeueLoop(ctx context.Context, pool StaleRequeueDB, opts StaleRequeueOptions) error {
	if pool == nil {
		return errors.New("scheduler: компенсатор «статус повис»: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("компенсатор «статус повис»: ранний recheck new∧done",
		"interval", o.Interval.String(),
		"batch", o.BatchSize,
		"window", StaleRequeueWindow.String())

	// drain переводит всю когорту: пачки повторяются, пока проходит
	// целиком (LIMIT заполнен — за ней ещё есть) И переводит строки;
	// неполная пачка либо нулевой перевод (guard отсеял всё, конкурент
	// уже перевёл) — множество исчерпано, дренаж конечен.
	drain := func() {
		var total int64
		for {
			res, err := StaleRequeueOnce(ctx, pool, o)
			if err != nil {
				// Отмена во время прохода — часть остановки, не тревога;
				// прочая ошибка — Warn, очередной дренаж повторит.
				if ctx.Err() == nil {
					log.Warn("дренаж компенсатора «статус повис»",
						"err", err, "requeued", total)
				}
				return
			}
			total += res.Domains
			if res.Domains == 0 || res.Selected < o.BatchSize {
				if total > 0 {
					// Каждая переведённая строка — потерянная задача q:http:
					// событие видно счётчику и gauge; в журнал — итог дренажа.
					log.Info("компенсатор «статус повис»: домены возвращены в pending",
						"domains", total)
				}
				return
			}
		}
	}

	drain()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			drain()
		}
	}
}
