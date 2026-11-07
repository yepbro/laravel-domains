// Scheduler: терминальная страховка строк queued с NULL queued_at (T-255,
// ТЗ tasks/TZ-fsm.md §3 ФТ-1.2) — ремонтный проход цикла LeaseWatch
// (lease.go). Инвариант T-028 «queued ⇒ queued_at NOT NULL» (И1
// спецификации автомата check_state, checkstate.go) нарушается багом
// писателя или ручным SQL — сегодня такая строка зависает в queued
// навсегда: основной lease-проход отбирает по возрасту queued_at, NULL
// сравнением отсеивается (lease.go leaseSelectSQL), диспетчер выбирает
// только pending, записи результатов приходят только на доставки задач,
// которых у строки нет. Страховка возвращает такие строки в pending —
// перевыберет диспетчер (T-027), повторная проверка идемпотентна
// (at-least-once, ТЗ 3.4): состояние перестаёт зависеть от выживания
// чужой отметки времени.
//
// Проход — ДВА оператора в одной транзакции (форма основного lease-прохода
// T-200, без его курсора: множество NULL-строк не растёт повторными
// постановками — свежая постановка пишет now(), правее любого порога):
// выборка пачки (leaseNullSelectSQL) и перевод PK-пробами
// (leaseNullUpdateSQL — guard-оператор того же вида, что leaseUpdateSQL).
// Форма основного lease-запроса и пригодность domains_lease_idx (T-104)
// не меняются: выборка страховки — отдельный оператор, идущий по ТОМУ ЖЕ
// частичному индексу (queued_at IS NULL — поисковое условие на ведущей
// колонке ключа, NULL-записи лежат в индексе компактным диапазоном),
// ORDER BY совпадает с ключом индекса — пачки без сортировки.
//
// Поведение штатных аренд не меняется: у строк с ненулевой queued_at
// охрана IS NULL перевода не пропускает, основной проход их и не видит.
// Счётчик crawler_scheduler_lease_null_returned_total — по RETURNING
// перевода: каждое событие — одна возвращённая строка; стационарно ноль
// (инвариант держат все писатели), ненулевой стационар — сигнал
// оператору (docs/runbooks.md §2.9): где-то пишут queued без отметки.

package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// leaseNullSelectSQL — выборка пачки строк queued с NULL queued_at (не
// deleted): нарушенный инвариант И1 (checkstate.go). Идёт по частичному
// индексу domains_lease_idx (T-104): предикат check_state = 1 имплицирован
// частичным индексом, queued_at IS NULL — поисковое условие на ведущей
// колонке ключа (btree хранит NULL-записи, IS NULL — индексное равенство),
// ORDER BY (queued_at, zone_id, id) совпадает с ключом — скан читает
// только NULL-диапазон каждой партиции и останавливается по LIMIT; ключ
// целиком детерминирует пачки (стиль leaseSelectSQL). Коды схемы —
// литералами через model.* (предикат частичного индекса обязан быть
// доказуем для планировщика; параметр убивал бы индекс в generic-плане
// подготовленного выражения — стиль dispatch.go). deleted исключён в
// самой выборке (как в leaseSelectSQL): мёртвая строка не должна
// занимать лимит пачки и закрывать возврат живым.
var leaseNullSelectSQL = fmt.Sprintf(
	"SELECT zone_id, id FROM domains "+
		"WHERE check_state = %d AND status <> %d AND queued_at IS NULL "+
		"ORDER BY queued_at, zone_id, id LIMIT $1",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// leaseNullUpdateSQL — перевод выбранной пачки в pending PK-пробами
// (guard-оператор вида leaseUpdateSQL, T-200): unnest-пакетный UPDATE,
// планировщик ведёт от unnest и пробирует domains по PK (zone_id, id) —
// скан со стороны UPDATE нет. Guard-фильтры те же, что в выборке, и
// выполняются на момент БЛОКИРОВКИ строки: гонка с записью результата
// (queued → done) или ручной постановкой отметки между выборкой и
// блокировкой не переводит строку, чья судьба изменилась. Сброс
// queued_at = NULL — единая форма с остальными переводами в pending
// (И2): у строки уже NULL, присваивание семантически no-op, но держит
// оператор самодостаточным (не порождает особой ветки). Дельты счётчиков
// зон (queued −n / pending +n) — CTE zc по RETURNING zone_id
// переведённых строк (T-178, ТЗИ §6.2: предикат пинирует старое
// состояние queued); внешний SELECT — LeaseNullOnce считает итог по
// RETURNING (guard мог отсеять часть пачки).
var leaseNullUpdateSQL = fmt.Sprintf(
	"WITH upd AS (UPDATE domains AS t "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) "+
		"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
		"AND t.check_state = %d AND t.status <> %d AND t.queued_at IS NULL "+
		"RETURNING t.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_queued = z.domains_queued - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// LeaseNullResult — итог одного прохода LeaseNullOnce.
type LeaseNullResult struct {
	// Domains — строки, возвращённые в pending ЭТИМ проходом (по RETURNING
	// перевода, после охран — стиль LeaseResult).
	Domains int64
	// Selected — строк в пачке выборки: LIMIT заполнен целиком — за ней
	// ещё есть, дренаж LeaseWatch продолжается следующей пачкой.
	Selected int
}

// leaseNullRow — строка пачки страховки: ключ домена (queued_at у всех
// NULL — курсора нет, хранить нечего).
type leaseNullRow struct {
	zone int16
	id   int64
}

// leaseNullMetrics — счётчик ремонтного прохода (T-255); nil — no-op
// (стиль retryMetrics).
type leaseNullMetrics struct {
	returned prometheus.Counter
}

// newLeaseNullMetrics — счётчик в реестре reg (nil — nil): имя/HELP —
// точка правды документации метрики (docs/services.md); регистрация
// идемпотентна (T-076) — построение на каждый проход безопасно, вектор
// делится между LeaseWatch и прямыми вызовами LeaseNullOnce.
func newLeaseNullMetrics(reg *metrics.Registry) *leaseNullMetrics {
	if reg == nil {
		return nil
	}
	return &leaseNullMetrics{
		returned: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_scheduler_lease_null_returned_total",
			Help: "Возврат в pending строк check_state=queued с NULL queued_at ремонтным проходом цикла LeaseWatch (T-255, ФТ-1.2): нарушенный инвариант T-028 «queued ⇒ queued_at NOT NULL» — вне основного lease-прохода навсегда (NULL отсеивается сравнением возраста). Стационарно ноль; ненулевой стационар — ручной SQL или баг писателя постановки: разбор docs/runbooks.md §2.9.",
		}).WithLabelValues(),
	}
}

// add — n возвращённых строк; nil — no-op.
func (m *leaseNullMetrics) add(n int64) {
	if m == nil {
		return
	}
	m.returned.Add(float64(n))
}

// LeaseNullOnce выполняет один ремонтный проход (ФТ-1.2): до
// opts.BatchSize строк check_state = queued с NULL queued_at (не deleted)
// выбираются и в той же транзакции переводятся в check_state = pending
// PK-пробами — дельты счётчиков зон ведёт CTE оператора перевода (T-178);
// счётчик crawler_scheduler_lease_null_returned_total растёт на число
// фактически переведённых (opts.Metrics; nil — без метрик). Повторный
// проход безопасен (ТЗ 3.4): переведённые строки перестают удовлетворять
// отбору. Пустая пачка — no-op без перевода и фиксации (стиль LeaseOnce).
// Форма основного lease-запроса не затрагивается (lease.go не меняется).
func LeaseNullOnce(ctx context.Context, pool LeaseDB, opts LeaseOptions) (LeaseNullResult, error) {
	if pool == nil {
		return LeaseNullResult{}, errors.New("scheduler: страховка queued без queued_at: пул не задан")
	}
	o := opts.withDefaults()
	met := newLeaseNullMetrics(o.Metrics)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return LeaseNullResult{}, fmt.Errorf("scheduler: страховка queued без queued_at: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-020/T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := leaseNullSelect(ctx, tx, o.BatchSize)
	if err != nil {
		return LeaseNullResult{}, err
	}
	if len(batch) == 0 {
		// Переводить нечего: UPDATE не выполняется, транзакция только
		// читала (стиль LeaseOnce).
		return LeaseNullResult{}, nil
	}

	n, err := leaseNullTranslate(ctx, tx, batch)
	if err != nil {
		return LeaseNullResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseNullResult{}, fmt.Errorf("scheduler: страховка queued без queued_at: фиксация: %w", err)
	}
	met.add(n)
	return LeaseNullResult{Domains: n, Selected: len(batch)}, nil
}

// leaseNullSelect — пачка строк queued/NULL queued_at (leaseNullSelectSQL)
// в транзакции прохода: ключи доменов — аргументы перевода.
func leaseNullSelect(ctx context.Context, tx pgx.Tx, limit int) ([]leaseNullRow, error) {
	rows, err := tx.Query(ctx, leaseNullSelectSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: страховка queued без queued_at: выборка пачки: %w", err)
	}
	defer rows.Close()

	var out []leaseNullRow
	for rows.Next() {
		var r leaseNullRow
		if err := rows.Scan(&r.zone, &r.id); err != nil {
			return nil, fmt.Errorf("scheduler: страховка queued без queued_at: выборка пачки: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: страховка queued без queued_at: выборка пачки: %w", err)
	}
	return out, nil
}

// leaseNullTranslate — перевод пачки PK-пробами (leaseNullUpdateSQL) в
// транзакции прохода; возвращает число переведённых строк по RETURNING
// (после охран — стиль leaseTranslate).
func leaseNullTranslate(ctx context.Context, tx pgx.Tx, batch []leaseNullRow) (int64, error) {
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	for i, r := range batch {
		zones[i], ids[i] = r.zone, r.id
	}
	rows, err := tx.Query(ctx, leaseNullUpdateSQL,
		int16(model.CheckStatePending),
		zones, ids,
	)
	if err != nil {
		return 0, fmt.Errorf("scheduler: страховка queued без queued_at: перевод пачки: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return 0, fmt.Errorf("scheduler: страховка queued без queued_at: перевод пачки: чтение: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: страховка queued без queued_at: перевод пачки: %w", err)
	}
	return n, nil
}
