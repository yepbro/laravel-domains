// Scheduler: возврат доменов с истёкшим lease (T-029; ТЗ 5.3, 3.4;
// форма прохода — SELECT пачки + перевод PK-пробами и курсор дренажа,
// T-200, TZ_slow_lease_return.md).
//
// Постановка в очередь (T-028) переводит домен в check_state = queued и
// отмечает queued_at — с этого момента действует аренда (lease): воркер
// обязан записать результат за срок scheduler.lease (по умолчанию
// 30 минут, ТЗ 5.3). Если результата нет — воркер погиб, потеря Redis
// (источник истины — PostgreSQL, ТЗ 3.4), сбой публикации — домен
// «зависает» в queued и вне цикла проверок. Проход LeaseOnce возвращает
// такие домены в pending: их перевыберет диспетчер (T-027) и поставит в
// очередь заново (T-028) — доставка at-least-once, повтор безопасен.
//
// «Без результата» встроено в условие отбора: воркер, записавший результат,
// переводит домен в done (контракты записи результатов, ТЗ 5.4/5.5) — строка
// перестаёт удовлетворять check_state = queued и не выбирается, сколько бы
// ни было ей лет. Честному воркеру, не успевшему за срок аренды, возврат не
// мешает: таймауты этапов — секунды против минут lease, а повторная
// доставка задачи идемпотентна (ТЗ 3.4).
//
// Условия возврата: check_state = queued И queued_at старше now() − lease.
// NULL queued_at сравнением отсеивается: queued без отметки времени —
// нарушенный инвариант T-028, переводить его по возрасту бессмысленно.
// Статус deleted исключается (ТЗ 13.1, как в перепроверке T-030):
// удалённый домен выпадает из цикла проверок целиком — диспетчер его не
// выбирает (охрана T-027), возвращать его в pending значило бы накапливать
// мёртвые строки в частичном индексе domains_pending_idx. Воскрешение
// add-файлом (T-018) само ставит new/pending и сбрасывает queued_at,
// из какого бы состояния домен ни возвращался.
//
// Проход — ДВА оператора в одной транзакции: выборка пачки (leaseSelectSQL)
// и перевод (leaseUpdateSQL, UPDATE … FROM unnest PK-пробами). До T-200
// пачка и перевод были одним CTE-оператором — и UPDATE-сторона его плана
// (Update → Nested Loop) сканировала ВЕСЬ истёкший диапазон lease-индекса
// без LIMIT: стоимость пачки росла с размером диапазона, а не пачки, и
// диапазон читался дважды за пачку (подзапрос + охраняемый join;
// WITH AS MATERIALIZED план не менял — замер ТЗ T-200 §2.1, запрос №3
// окна 25–26.10: 75 событий, медиана 8,1 с, max 111 с). Два оператора —
// единственный способ убрать lease-скан со стороны UPDATE (ТЗ §6.1):
// unnest-форма пакетного UPDATE (db.Update-стиль, T-028) ведёт планировщика
// от unnest, и domains пробируется по PK; атомарность «выборка = перевод»
// возвращает транзакция (ТЗ 3.4) — обе операции одного прохода идут через
// pool.Begin. Guard-перепроверки отбора (check_state/status/queued_at) в
// UPDATE сохранены: гонка с воркером, записавшим результат между выборкой и
// блокировкой строки, не возвращает уже готовый домен. Переведённые строки
// перестают удовлетворять отбору: повторные проходы идемпотентны (ТЗ 3.4).
// Фильтр deleted обязан жить в самой выборке (а не только в охране
// перевода): иначе мёртвые строки могли бы заполнять лимит пачки и закрывать
// живым возврат (голодание).
//
// Курсор дренажа (T-200, ТЗ §3.2/§4.1): между вакуумами lease-индекс хранит
// мёртвые версии переведённых аренд (замер §2.2: 273 МБ индекса при ~1 тыс.
// живых записей; автовакуум масштабного фактора 0,02 × 42,3 млн строк
// чистит префикс 2–3 раза в сутки) — без курсора каждая пачка КАЖДОГО
// дренажа начинала скан с начала индекса и перечитывала один и тот же
// мёртвый префикс (штормовой дренаж 25.10 — 14 пачек × префикс). Курсор
// (queued_at, zone_id, id) — ключ индекса целиком — живёт в состоянии цикла
// LeaseWatch и переживает пачки и дренажи: мёртвый префикс читается один
// раз за жизнь процесса; рестарт сбрасывает курсор — стартовый полный
// дренаж сохраняется (T-012/T-029). Корректность движения — ТЗ §4.1: всё с
// ключом ≤ курсора уже возвращено (истёкшесть зависит только от queued_at);
// неистёкшие на момент прохода лежат правее порога — правее курсора — и
// состарятся впереди него; повторная постановка пишет свежий queued_at
// правее порога и курсора; deleted-строки левее курсора перепрыгиваются
// навсегда — их и не должно возвращать.
//
// Wiring в cmd/scheduler — сборка сервиса (T-028/поздние задачи).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
)

const (
	// DefaultLeaseTimeout — срок аренды queued-домена: старше — результат
	// не дождаться, домен возвращается в pending (ТЗ 5.3: «по умолчанию
	// 30 мин»). Зеркалит дефолт scheduler.lease (internal/config, T-006);
	// рабочий цикл получает значение из конфига.
	DefaultLeaseTimeout = 30 * time.Minute

	// DefaultLeaseBatch — доменов в одной пачке возврата: в диапазоне пачек
	// планировщика (ТЗ 5.3: 1000–10 000), совпадает с дефолтами выборки
	// (T-027) и перепроверки (T-030). Пачка ограничивает одну транзакцию
	// прохода — блокировки удерживаются минимум времени (стиль T-020);
	// размер не меняется формой T-200 (ТЗ §4.3).
	DefaultLeaseBatch = 5000
)

// leaseSelectSQL — выборка пачки истёкших аренд (T-200, ТЗ §4.1): до LIMIT
// строк check_state = queued, не deleted, queued_at старше порога — по
// индексу domains_lease_idx (T-104): ORDER BY (queued_at, zone_id, id)
// совпадает с ключом индекса, скан читает с начала индекса только истёкшие
// аренды и останавливается на первой неистёкшей; самые старые возвращаются
// первыми. Код check_state и статус deleted — литералы через model.*
// (предикат частичного индекса обязан быть доказуем для планировщика,
// стиль dispatch.go; параметр в предикате убивал бы domains_lease_idx в
// generic-плане подготовленного выражения).
//
// Композитный курсор row-value > (queued_at, zone_id, id) — пагинация
// дренажа (стиль retentionSelectSQL T-185 / reanalyzeTickSQL T-193): пачки
// детерминированы, без пропусков и повторов; ключ целиком (уникальный
// доменный ключ в хвосте) — продолжение строго с места остановки. Курсор
// переживает пачки и дренажи в состоянии LeaseWatch — мёртвый префикс
// индекса между вакуумами читается один раз за жизнь процесса, а не каждой
// пачкой (ТЗ §2.2/§3.2); корректность движения — комментарий файла (ТЗ
// §4.1). Нулевой курсор (старт/рестарт) — с начала индекса, как до T-200.
// queued_at в списке колонок — курсор следующей пачки (T-185).
//
// Порог аренды считается часами СЕРВЕРА БД: queued_at пишется серверным
// now() (enqueueUpdate), расхождение часов процесса планировщика с часами
// БД вперёд больше срока аренды возвращало бы в pending только что
// поставленные домены (дубли работы), назад — задерживало возврат.
// now() подставляется параметром только в тестах (фиксированные часы
// LeaseOptions.Now); в бою параметр NULL — COALESCE берёт now() сервера,
// в транзакции прохода он стабилен (момент Begin). Срок передан секундами
// через make_interval — интервал в одной точке правды, без клиентской
// арифметики timestamp.
var leaseSelectSQL = fmt.Sprintf(
	"SELECT zone_id, id, queued_at FROM domains "+
		"WHERE check_state = %d AND status <> %d "+
		"AND queued_at < COALESCE($1::timestamptz, now()) - make_interval(secs => $2) "+
		"AND (queued_at, zone_id, id) > ($3::timestamptz, $4::int2, $5::int8) "+
		"ORDER BY queued_at, zone_id, id LIMIT $6",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// leaseUpdateSQL — перевод выбранной пачки в pending PK-пробами (T-200,
// ТЗ §4.2): пакетный UPDATE … FROM unnest (db.Update-стиль, T-028) —
// планировщик ведёт от unnest и пробирует domains по PK (zone_id, id),
// альтернативного lease-скана на стороне UPDATE больше нет (диагноз ТЗ
// §2.1: единый CTE-оператор держал Update → Nested Loop, внешняя сторона
// — index scan всего истёкшего диапазона без LIMIT, соединение Join
// Filter'ом; AS MATERIALIZED план не менял). Стоимость перевода ∝ пачке,
// а не размеру истёкшего диапазона. Отметка аренды сбрасывается — у
// pending нет lease (инвариант T-018/T-030), остальные колонки не меняются.
//
// Guard-фильтры (check_state/status/queued_at) — те же, что в выборке, и
// выполняются на момент БЛОКИРОВКИ строки (стиль перепроверки T-030):
// воркер, записавший результат между выборкой и блокировкой, меняет
// состояние строки — охрана не пропускает её к переводу, семантика гонок
// прежняя. now() транзакции — см. leaseSelectSQL: порог выборки и перевода
// один и тот же. Коды схемы — литералами через model.*.
//
// Дельты счётчиков зон (queued −n / pending +n) — CTE zc того же оператора
// по RETURNING zone_id переведённых строк (T-178, ТЗИ §6.2: предикат
// пинирует старое состояние queued); внешний SELECT отдаёт zone_id
// переведённых — LeaseOnce считает итог прохода по RETURNING, то есть
// строки, переведённые именно этим проходом (guard мог отсеять часть
// выбранной пачки).
var leaseUpdateSQL = fmt.Sprintf(
	"WITH upd AS (UPDATE domains AS t "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) "+
		"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
		"AND t.check_state = %d AND t.status <> %d "+
		"AND t.queued_at < COALESCE($4::timestamptz, now()) - make_interval(secs => $5) "+
		"RETURNING t.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_queued = z.domains_queued - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// LeaseDB — пул для возврата lease: db.DB, из которого можно начать
// транзакцию (стиль RetryDB T-191 / RetentionDB T-185). Выборка пачки и
// перевод — одна транзакция (ТЗ 3.4, T-200 §4.3): атомарность пары
// операторов эквивалентна прежнему одному оператору; *pgxpool.Pool
// удовлетворяет интерфейсу (сессия наследует LockTimeout T-192 и
// NoParallelGather T-171 из AfterConnect пула).
type LeaseDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ LeaseDB = (*pgxpool.Pool)(nil)

// LeaseOptions — параметры возврата; нулевые значения заменяются
// дефолтами (withDefaults, стиль RecheckOptions/ingest.CleanupOptions).
type LeaseOptions struct {
	// Timeout — срок аренды (DefaultLeaseTimeout, ТЗ 5.3): queued_at
	// старше now() − Timeout → возврат в pending; рабочее значение —
	// scheduler.lease из конфига.
	Timeout time.Duration
	// BatchSize — максимум доменов в одной пачке (DefaultLeaseBatch).
	BatchSize int
	// Interval — период цикла LeaseWatch; на разовый LeaseOnce не влияет.
	// Ноль/отрицательное — Timeout/2 (см. DefaultLeaseInterval-комментарий
	// в LeaseWatch).
	Interval time.Duration
	// Now — фиксированные часы для тестов: подставляются в SQL параметром
	// вместо now() сервера (nil в бою — порог считают часы БД,
	// см. leaseSelectSQL).
	Now func() time.Time
	// Log — логгер цикла LeaseWatch; nil — записи подавлены
	// (slog.DiscardHandler). LeaseOnce не логирует — итог возвращается
	// LeaseResult.
	Log *slog.Logger
}

// withDefaults заменяет неположительные значения дефолтами (Now
// проверяется отдельно: функций — nil).
func (o LeaseOptions) withDefaults() LeaseOptions {
	if o.Timeout <= 0 {
		o.Timeout = DefaultLeaseTimeout
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultLeaseBatch
	}
	if o.Interval <= 0 {
		// Половина срока аренды: см. обоснование в LeaseWatch. Вырожденный
		// Timeout ≤ 1 нс делением даёт 0 — тикеру нужен положительный
		// интервал, поэтому минимум 1 нс.
		o.Interval = o.Timeout / 2
		// Эквивалентный мутант (T-M06): `o.Interval <= 1` ведёт себя как
		// `<= 0` — после деления на 2 здесь не бывает значений из (0, 1 нс)
		// кроме ровно 1 нс (Timeout 2–3 нс), а тело присваивает те же
		// 1 нс (no-op); значения ≥ 2 нс обе границы пропускают.
		// mutator-disable-next-line numbers/incrementer
		if o.Interval <= 0 {
			o.Interval = time.Nanosecond
		}
	}
	return o
}

// LeaseResult — итог одного прохода LeaseOnce.
type LeaseResult struct {
	// Domains — домены, возвращённые в pending ЭТИМ проходом: счёт по
	// RETURNING перевода (не больше пачки и не больше выборки — guard мог
	// отсеять строки, чья судьба изменилась между выборкой и блокировкой).
	Domains int64
	// Selected — строк в пачке выборки: LIMIT заполнен целиком — истёкшие
	// за курсором ещё есть, дренаж LeaseWatch продолжается следующей
	// пачкой. Число переводов больше не критерий остановки: guard отсеял
	// записанные воркером результаты — курсор всё равно ушёл за пачку, и
	// возврат к ней был бы холостым чтением.
	Selected int
}

// LeaseCursor — курсор дренажа по времени (T-200): последняя посещённая
// строка (queued_at, zone_id, id), переживающая пачки и дренажи цикла
// LeaseWatch (образец retentionCursor T-185 / ReanalyzeCursor T-193).
// Время ведёт: новые аренды старше порога впереди; доменный ключ
// детерминирует порядок внутри одинаковых отметок. Нулевое значение —
// «с начала» индекса (стартовый полный дренаж, рестарт процесса);
// time.Time{} меньше любой реальной queued_at, нулевые zone/id — любого
// реального ключа. Поля не экспортированы: курсор двигает только
// LeaseOnce, владелец — цикл LeaseWatch (одна горутина, блокировки не
// нужны — стиль retentionState T-185); внешние вызовы держат значение
// целиком (нулевая структура-литерал доступна и без полей).
type LeaseCursor struct {
	queuedAt time.Time
	zone     int16
	id       int64
}

// leaseRow — строка пачки: ключ домена и отметка queued_at — курсор
// следующей пачки (T-200).
type leaseRow struct {
	zone     int16
	id       int64
	queuedAt time.Time
}

// LeaseOnce выполняет один проход (ТЗ 5.3, T-200 §§4.1–4.3): до
// opts.BatchSize доменов со check_state = queued и queued_at старше
// opts.Timeout (не deleted, без результата) выбираются по курсору cursor и
// в той же транзакции переводятся в check_state = pending PK-пробами —
// отметка аренды сбрасывается, остальные колонки не меняются; CTE оператора
// перевода ведёт дельты счётчиков зон (T-178). Порог считает сервер БД
// (см. leaseSelectSQL); повторный проход безопасен (ТЗ 3.4): переведённые
// строки отбор больше не выбирает.
//
// cursor — состояние дренажа: после успешной фиксации пачки двигается за
// её последнюю строку (по ключу выборки); пустая пачка не двигает; ошибка
// любого оператора не двигает — повтор прохода берёт пачку заново.
// Guard-отсеянные выборкой строки позади курсора не возвращаются: их
// судьба изменилась между выборкой и блокировкой (воркер записал результат,
// повторная постановка пишет свежий queued_at правее курсора — ТЗ §4.1),
// под старым ключом их больше нет. nil — разовый вызов с нулевого курсора
// без сохранения (стиль ReanalyzeTick): стартовый полный дренаж
// T-012/T-029 сохраняется и рестартом цикла.
func LeaseOnce(ctx context.Context, pool LeaseDB, cursor *LeaseCursor, opts LeaseOptions) (LeaseResult, error) {
	if pool == nil {
		return LeaseResult{}, errors.New("scheduler: возврат истёкших lease: пул не задан")
	}
	o := opts.withDefaults()
	if cursor == nil {
		cursor = &LeaseCursor{} // разовый вызов: с начала индекса, без сохранения
	}

	// Часы процесса — только для тестов (фиксированное «сейчас»); в бою
	// NULL — единственные часы это now() сервера БД (в транзакции прохода
	// стабилен: момент Begin).
	var now *time.Time
	if o.Now != nil {
		t := o.Now()
		now = &t
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("scheduler: возврат истёкших lease: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-020/T-044).
	// Прерванный проход не оставляет половинчатых изменений.
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := leaseSelect(ctx, tx, o, *cursor, now)
	if err != nil {
		return LeaseResult{}, err
	}
	if len(batch) == 0 {
		// Пустая пачка: переводить нечего (UPDATE не выполняется), курсор
		// не двигается; транзакция только читала — откат содержательно
		// равнозначен фиксации (стиль T-181).
		return LeaseResult{}, nil
	}

	n, err := leaseTranslate(ctx, tx, o, batch, now)
	if err != nil {
		return LeaseResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseResult{}, fmt.Errorf("scheduler: возврат истёкших lease: фиксация: %w", err)
	}
	// Курсор — за последнюю строку пачки, только после успешной фиксации
	// (сбой транзакции повторит пачку целиком). Строки пачки, отсеянные
	// guard-ом перевода, перепрыгиваются: их состояние изменилось между
	// выборкой и блокировкой — под старым ключом их больше нет (ТЗ §4.1).
	last := batch[len(batch)-1]
	cursor.queuedAt, cursor.zone, cursor.id = last.queuedAt, last.zone, last.id
	return LeaseResult{Domains: n, Selected: len(batch)}, nil
}

// leaseSelect — пачка истёкших аренд после курсора after (leaseSelectSQL)
// в транзакции прохода: ключи доменов и отметки queued_at — аргументы
// перевода и курсор следующей пачки.
func leaseSelect(ctx context.Context, tx pgx.Tx, o LeaseOptions, after LeaseCursor, now *time.Time) ([]leaseRow, error) {
	rows, err := tx.Query(ctx, leaseSelectSQL,
		now,
		o.Timeout.Seconds(),
		after.queuedAt, after.zone, after.id,
		o.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("scheduler: возврат истёкших lease: выборка пачки: %w", err)
	}
	defer rows.Close()

	var out []leaseRow
	for rows.Next() {
		var r leaseRow
		if err := rows.Scan(&r.zone, &r.id, &r.queuedAt); err != nil {
			return nil, fmt.Errorf("scheduler: возврат истёкших lease: выборка пачки: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: возврат истёкших lease: выборка пачки: %w", err)
	}
	return out, nil
}

// leaseTranslate — перевод пачки PK-пробами (leaseUpdateSQL) в транзакции
// прохода; возвращает число переведённых строк по RETURNING (после охран —
// как RETURNING cleanup, T-020: строки, чью запись увидела охрана, не
// считаются).
func leaseTranslate(ctx context.Context, tx pgx.Tx, o LeaseOptions, batch []leaseRow, now *time.Time) (int64, error) {
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	for i, r := range batch {
		zones[i], ids[i] = r.zone, r.id
	}
	rows, err := tx.Query(ctx, leaseUpdateSQL,
		int16(model.CheckStatePending),
		zones, ids,
		now,
		o.Timeout.Seconds(),
	)
	if err != nil {
		return 0, fmt.Errorf("scheduler: возврат истёкших lease: перевод пачки: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return 0, fmt.Errorf("scheduler: возврат истёкших lease: перевод пачки: чтение: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: возврат истёкших lease: перевод пачки: %w", err)
	}
	return n, nil
}

// LeaseWatch — фоновый цикл возврата доменов с истёкшей арендой: дренаж
// всех созревших пачками LeaseOnce, затем ожидание тика. Первый дренаж —
// сразу при старте: проход идемпотентен и подбирает зависшее за простой
// сервиса (остановка/рестарт не оставляют «застрявших» доменов — ТЗ 3.4,
// 11.4 п. 9; стиль стартового дренажа T-020/T-030). Ошибка пачки
// прерывает только текущий дренаж: логируется Warn, цикл живёт (контракт
// OnRun, T-012), очередной дренаж продолжит — переведённое не
// возвращается дважды. Отмена ctx — чистый выход с nil (graceful
// shutdown, ТЗ 3.4).
//
// Курсор дренажа — состояние цикла, переживающее пачки и дренажи (T-200,
// стиль retentionCursor T-185): мёртвый префикс lease-индекса читается
// один раз за жизнь процесса, а не каждой пачкой каждого дренажа (ТЗ
// §2.2/§3.2); рестарт процесса (новый запуск цикла) начинает с нулевого
// курсора — стартовый полный дренаж сохраняется. Курсор трогает только
// цикл (одна горутина тикера) — блокировки не нужны.
//
// Интервал проверок — lease/2 (а не фикс): баланс «скорость возврата vs
// нагрузка». Проход с индексом domains_lease_idx (T-104) стоит столько,
// сколько в нём зависших доменов (штатно единицы строк), но фикс при
// lease 30 мин всё равно давал бы десятки холостых проходов за срок ради
// выигрыша в минуты возврата, а при коротком настраиваемом lease и вовсе
// «переспал» бы сам срок. Половина срока — две проверки за аренду:
// истёкший lease обнаруживается не позже чем через полсрока после
// истечения, всего домен зависнет не дольше чем на полтора срока.
func LeaseWatch(ctx context.Context, pool LeaseDB, opts LeaseOptions) error {
	if pool == nil {
		return errors.New("scheduler: возврат истёкших lease: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("возврат доменов с истёкшим lease",
		"interval", o.Interval.String(),
		"timeout", o.Timeout.String(),
		"batch", o.BatchSize)

	// Курсор дренажа цикла (T-200): ноль при старте — первый дренаж
	// проходит всё созревшее с начала индекса, дальше пачки продолжают
	// друг друга через дренажи.
	var cursor LeaseCursor

	// drain возвращает все созревшие домены: пачки повторяются, пока
	// выборка проходит целиком (LIMIT заполнен — за курсором ещё есть
	// истёкшие); неполная пачка — множество исчерпано. Критерий — размер
	// выборки, а не число переводов: guard мог отсеять пачку целиком
	// (результаты записаны между выборкой и блокировкой), но курсор ушёл
	// за неё — возвращаться к пройденному незачем.
	drain := func() {
		var total int64
		for {
			res, err := LeaseOnce(ctx, pool, &cursor, o)
			if err != nil {
				// Отмена во время прохода — часть остановки, не тревога;
				// прочая ошибка — Warn, очередной дренаж повторит.
				if ctx.Err() == nil {
					log.Warn("дренаж возврата истёкших lease",
						"err", err, "returned", total)
				}
				return
			}
			total += res.Domains
			if res.Selected < o.BatchSize {
				if total > 0 {
					log.Info("дренаж возврата истёкших lease", "domains", total)
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
