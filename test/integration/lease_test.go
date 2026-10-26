// Интеграционные тесты возврата истёкших lease в форме T-200 (ТЗ
// TZ_slow_lease_return.md §7.2) против живого PostgreSQL: зрелая аренда
// возвращается пачкой PK-пробами (повторный проход — 0), запись результата
// воркером между выборкой и переводом не возвращается (guard-гонка),
// курсор дренажа не пропускает созревающие позже записи (малый lease, два
// прохода с паузой), дельты счётчиков зон сходятся с T-178-реконсиляцией;
// EXPLAIN-контракты обеих сторон прохода — предикат композитного курсора
// обслуживается листом domains_lease_idx (T-104), перевод ведёт от unnest
// с PK-пробами domains без lease-скана диапазона (§4.2, ревью T-200
// P3-1). Юнит-уровень (SQL-контракты, курсор, цикл на фейках) —
// internal/scheduler/lease_test.go. Уровень вызовов — библиотечные, без
// запуска сервисов (стиль reanalyze_test.go); изоляция — withTestSchema
// (search_path в DSN; ВСЕ подключения теста — один пул этой схемы).
// Прогон — make test-integration (после вливания, из основного дерева).
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/scheduler"
)

// leaseSeedQueued — домен в queued с отметкой аренды age назад (make_interval
// — детерминированный ввод без парсинга строк интервалов); возвращает id.
func leaseSeedQueued(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, label string, ageMinutes int) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO domains (zone_id, label, check_state, status, queued_at)
		 VALUES ($1, $2, $3, $4, now() - make_interval(mins => $5)) RETURNING id`,
		zone, label, int16(model.CheckStateQueued), int16(model.StatusNew), ageMinutes,
	).Scan(&id); err != nil {
		t.Fatalf("домен %s: %v", label, err)
	}
	return id
}

// leaseCheckState — check_state домена (quote-значение для сообщений).
func leaseCheckState(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, id int64) int16 {
	t.Helper()
	var state int16
	if err := pool.QueryRow(ctx,
		"SELECT check_state FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&state); err != nil {
		t.Fatalf("чтение домена %d: %v", id, err)
	}
	return state
}

// TestLease_MatureReturnedRepeatZero — сценарий §7.2: зрелая аренда
// возвращается в pending одним проходом (пачка PK-пробами, счёт по
// RETURNING), повторный проход с тем же курсором — 0 (переведённые строки
// выпадают из отбора); дельты зон queued −n / pending +n сходятся с
// T-178-реконсиляцией (VerifyCounters после эпизода).
func TestLease_MatureReturnedRepeatZero(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета (ревью T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease.test")
	labels := []string{"m1", "m2", "m3", "m4"}
	for i := range labels {
		leaseSeedQueued(ctx, t, pool, zone, labels[i], 60+i) // все старше lease
	}

	// Реконсиляция T-178 — заполняет нули счётчиков зоны (4 queued);
	// после возврата сверка обязана сойтись и без новой реконсиляции:
	// дельты ведёт CTE того же оператора перевода.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	var cursor scheduler.LeaseCursor
	res, err := scheduler.LeaseOnce(ctx, pool, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res.Domains != int64(len(labels)) || res.Selected != len(labels) {
		t.Fatalf("итог = %+v, want Domains=Selected=%d", res, len(labels))
	}
	if n := countDomains(t, pool,
		fmt.Sprintf("zone_id = %d AND check_state = %d", zone, int16(model.CheckStatePending))); n != len(labels) {
		t.Fatalf("pending после возврата: %d, хочу %d", n, len(labels))
	}
	if n := countDomains(t, pool,
		fmt.Sprintf("zone_id = %d AND queued_at IS NOT NULL", zone)); n != 0 {
		t.Errorf("queued_at не сброшен у возвращённых: %d, хочу 0", n)
	}
	countersVerifyEmpty(t, pool, "после lease (дельты T-178)")

	// Повторный проход с тем же курсором — пустая пачка: переведённые
	// строки больше не удовлетворяют отбору (идемпотентность, ТЗ 3.4).
	repeat, err := scheduler.LeaseOnce(ctx, pool, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("повторный LeaseOnce: %v", err)
	}
	if repeat != (scheduler.LeaseResult{}) {
		t.Fatalf("повторный проход = %+v, want нулевой", repeat)
	}
	countersVerifyEmpty(t, pool, "после повторного прохода")
}

// --- гонка: результат записан между выборкой и переводом -------------------

// leaseRacePool — обёртка пула для сценария гонки (§7.2): в ТОЙ ЖЕ
// транзакции прохода между SELECT пачки и UPDATE перевода пишет результат
// «воркер» по жертве пачки — guard перевода обязан её отсеять. Оба
// оператора идентифицируются префиксами текста (SQL пакета не экспортирован);
// неожидаемый оператор — ошибка: контракт прохода ломается громко.
type leaseRacePool struct {
	*pgxpool.Pool

	victimZone int16
	victimID   int64
}

var _ scheduler.LeaseDB = (*leaseRacePool)(nil)

func (p *leaseRacePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &leaseRaceTx{Tx: tx, pool: p}, nil
}

// leaseRaceTx — транзакция с точкой внедрения гонки.
type leaseRaceTx struct {
	pgx.Tx

	pool *leaseRacePool
}

func (t *leaseRaceTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch {
	case strings.HasPrefix(sql, "SELECT zone_id, id, queued_at FROM domains"):
		return t.Tx.Query(ctx, sql, args...)
	case strings.HasPrefix(sql, "WITH upd AS (UPDATE domains AS t"):
		// «Воркер» записывает результат между выборкой и переводом
		// (контракт записи: queued → done, отметка аренды сброшена);
		// транзакция прохода видит собственную правку — guard обязан
		// отсеять жертву, счёт RETURNING её не считает.
		if _, err := t.Exec(ctx,
			"UPDATE domains SET check_state = $1, queued_at = NULL WHERE zone_id = $2 AND id = $3",
			int16(model.CheckStateDone), t.pool.victimZone, t.pool.victimID); err != nil {
			return nil, fmt.Errorf("leaseRaceTx: запись результата воркера: %w", err)
		}
		return t.Tx.Query(ctx, sql, args...)
	default:
		return nil, fmt.Errorf("leaseRaceTx: неожидаемый SQL прохода: %s", sql)
	}
}

// TestLease_WorkerRaceGuard — сценарий §7.2: запись результата между
// выборкой и переводом — домен не возвращается (остаётся done), остальные
// строки пачки переводятся; курсор всё равно уходит за пачку.
func TestLease_WorkerRaceGuard(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета (ревью T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease-race.test")
	first := leaseSeedQueued(ctx, t, pool, zone, "r1", 90)
	victim := leaseSeedQueued(ctx, t, pool, zone, "r2", 60) // ключ пачки — средний
	last := leaseSeedQueued(ctx, t, pool, zone, "r3", 30)

	// Реконсиляция T-178 — зона считает 3 queued: после гонки прямая
	// сверка дельт частично переведённой пачки (ревью T-200 P3-2).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	race := &leaseRacePool{Pool: pool, victimZone: zone, victimID: victim}
	var cursor scheduler.LeaseCursor
	res, err := scheduler.LeaseOnce(ctx, race, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce с гонкой: %v", err)
	}
	if res.Domains != 2 || res.Selected != 3 {
		t.Fatalf("итог = %+v, want Domains=2 (жертва отсеяна guard), Selected=3", res)
	}
	if state := leaseCheckState(ctx, t, pool, zone, victim); state != int16(model.CheckStateDone) {
		t.Errorf("жертва гонки: check_state = %d, хочу done — результат воркера не затёрт возвратом", state)
	}
	for _, id := range []int64{first, last} {
		if state := leaseCheckState(ctx, t, pool, zone, id); state != int16(model.CheckStatePending) {
			t.Errorf("домен %d: check_state = %d, хочу pending — соседние строки пачки возвращены", id, state)
		}
	}

	// Дельты зон учли ТОЛЬКО переведённые строки (T-178): queued 3−2=1,
	// pending 0+2=2 — guard-отсеянная жертва в дельты не входит. Остаток
	// queued (1 против 0 живых) — прямой SQL-воркер теста обходит
	// инкрементальные дельты (результат пишет контракт записи T-058, здесь
	// упрощение); выравнивает реконсиляция — здесь пинируется ровно
	// «дельты == RETURNING перевода».
	var queued, pending int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_queued, domains_pending FROM zones WHERE id = $1", zone,
	).Scan(&queued, &pending); err != nil {
		t.Fatalf("чтение счётчиков зоны: %v", err)
	}
	if queued != 1 || pending != 2 {
		t.Errorf("дельты зоны = queued %d / pending %d, want 1/2 — дельты ведут ровно RETURNING перевода",
			queued, pending)
	}

	// Курсор ушёл за пачку: повторный проход (уже без гонки) ничего не
	// находит — отсеянная судьба жертвы изменилась, возврат к пачке
	// ничего бы не дал.
	repeat, err := scheduler.LeaseOnce(ctx, pool, &cursor, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("повторный LeaseOnce: %v", err)
	}
	if repeat != (scheduler.LeaseResult{}) {
		t.Fatalf("повторный проход = %+v, want нулевой — курсор за пачкой", repeat)
	}
}

// TestLease_CursorNotSkipLaterMatured — сценарий §7.2: курсор не
// пропускает записи, состарившиеся ПОЗЖЕ прохода (малый lease, проходы с
// паузой): свежая аренда правее порога не выбирается и не двигает курсор,
// состарев за паузу — возвращается следующим проходом (её ключ правее
// курсора, ТЗ §4.1).
func TestLease_CursorNotSkipLaterMatured(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета (ревью T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease-cursor.test")
	old := leaseSeedQueued(ctx, t, pool, zone, "c1", 30) // зрелая при малом lease

	// Малый lease 2 с: бюджет «свежести» аренды устойчив к паузам раннера
	// (пауза > lease между посевом и проходом 2 состарила бы аренду —
	// ложный провал; ревью T-200 P2-1).
	opts := scheduler.LeaseOptions{Timeout: 2 * time.Second}
	var cursor scheduler.LeaseCursor

	// Проход 1: только зрелая; курсор — за неё.
	first, err := scheduler.LeaseOnce(ctx, pool, &cursor, opts)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if first.Domains != 1 || first.Selected != 1 {
		t.Fatalf("проход 1 = %+v, want 1/1", first)
	}
	if state := leaseCheckState(ctx, t, pool, zone, old); state != int16(model.CheckStatePending) {
		t.Fatalf("зрелая аренда не возвращена: check_state = %d", state)
	}

	// Свежая аренда: правее порога — второй проход пуст, курсор стоит.
	fresh := leaseSeedQueued(ctx, t, pool, zone, "c2", 0)
	second, err := scheduler.LeaseOnce(ctx, pool, &cursor, opts)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if second != (scheduler.LeaseResult{}) {
		t.Fatalf("проход 2 = %+v, want нулевой — аренда ещё не истекла", second)
	}
	if state := leaseCheckState(ctx, t, pool, zone, fresh); state != int16(model.CheckStateQueued) {
		t.Fatalf("живая аренда тронута: check_state = %d, хочу queued", state)
	}

	// Пауза больше lease: свежая состарилась — проход 3 возвращает её,
	// курсор не пропустил (ключ свежей правее курсора прохода 1).
	time.Sleep(2500 * time.Millisecond)
	third, err := scheduler.LeaseOnce(ctx, pool, &cursor, opts)
	if err != nil {
		t.Fatalf("проход 3: %v", err)
	}
	if third.Domains != 1 || third.Selected != 1 {
		t.Fatalf("проход 3 = %+v, want 1/1 — состарившаяся позже запись не пропущена курсором", third)
	}
	if state := leaseCheckState(ctx, t, pool, zone, fresh); state != int16(model.CheckStatePending) {
		t.Errorf("состарившаяся аренда не возвращена: check_state = %d, хочу pending", state)
	}
}

// TestLease_CursorPredicateIndex — EXPLAIN-контракты обеих сторон прохода
// (стиль reanalyze_test.go). SELECT (§4.1): предикат композитного курсора +
// порог старения обслуживаются листовым экземпляром domains_lease_idx
// (T-104), не сортировкой/полным сканом. UPDATE (§4.2, ревью T-200 P3-1):
// ядро диагноза ТЗ §2.1 — перевод обязан вестись от unnest (Function
// Scan) и пробировать domains по PK (domains_pkey), lease-скана истёкшего
// диапазона на стороне UPDATE нет (§3.1/§6.1). На игрушечном объёме seq
// scan дешевле индекса — enable_seqscan = off проверяет индексируемость
// предикатов и форму плана, а не выбор по стоимости (стоимость — журнал
// медленных T-177 на бою).
func TestLease_CursorPredicateIndex(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета (ревью T-200 P2-1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "lease-explain.test")
	id := leaseSeedQueued(ctx, t, pool, zone, "e1", 90)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("SET LOCAL: %v", err)
	}

	// SELECT-сторона: форма leaseSelectSQL с литералами (параметры в
	// EXPLAIN требуют подготовленного оператора — стиль reanalyze_test.go):
	// порог 30 мин, курсор 2 часа назад с нулевой PK-частью, LIMIT пачки.
	selPlan := leaseExplain(ctx, t, tx, fmt.Sprintf(
		"SELECT zone_id, id, queued_at FROM domains "+
			"WHERE check_state = %d AND status <> %d "+
			"AND queued_at < now() - make_interval(secs => %d) "+
			"AND (queued_at, zone_id, id) > (now() - interval '2 hours', 0::int2, 0::int8) "+
			"ORDER BY queued_at, zone_id, id LIMIT %d",
		int16(model.CheckStateQueued), int16(model.StatusDeleted),
		int(scheduler.DefaultLeaseTimeout.Seconds()), scheduler.DefaultLeaseBatch))
	// domains партиционирована: имя родителя domains_lease_idx на листы
	// не наследуется — PG материализует дочерние индексы как
	// <партиция>_<колонки>_idx (замер: domains_default_queued_at_zone_id_
	// id_idx на DEFAULT; check_state = 1 в Index Cond нет — имплицирован
	// предикатом частичного индекса). Assert по суффиксу колонок ключа,
	// инвариантному к разбиению.
	if !strings.Contains(selPlan, "queued_at_zone_id_id_idx") {
		t.Errorf("предикат композитного курсора не обслуживается листовым экземпляром domains_lease_idx:\n%s",
			selPlan)
	}

	// UPDATE-сторона: форма leaseUpdateSQL с литералами (unnest-ключи —
	// засеянный домен, порог 30 мин). Порядок join на тестовом объёме не
	// пинится: с горстью строк планировщик вправе вести от листа
	// lease-индекса (диапазон — единицы строк, дешевле PK-проб; на боевой
	// 42-млн таблице он ведёт от unnest с PK-пробами — TZ §2.1/§3.1,
	// prod-план охраняется журналом медленных T-177 §7.3, текст оператора —
	// юнит-контрактом leaseUpdateSQL). Стабильные инварианты теста:
	// unnest-пакет присутствует в плане (PG16 печатает Function Scan по
	// алиасу u, не по имени функции) и нет деградации в seq scan по domains.
	updPlan := leaseExplain(ctx, t, tx, fmt.Sprintf(
		"WITH upd AS (UPDATE domains AS t "+
			"SET check_state = %d, queued_at = NULL "+
			"FROM unnest(ARRAY[%d::int2], ARRAY[%d::int8]) AS u(zone_id, id) "+
			"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
			"AND t.check_state = %d AND t.status <> %d "+
			"AND t.queued_at < now() - make_interval(secs => %d) "+
			"RETURNING t.zone_id), "+
			"zc AS (UPDATE zones AS z "+
			"SET domains_queued = z.domains_queued - c.n, "+
			"domains_pending = z.domains_pending + c.n "+
			"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
			"WHERE z.id = c.zone_id) "+
			"SELECT zone_id FROM upd",
		int16(model.CheckStatePending), zone, id,
		int16(model.CheckStateQueued), int16(model.StatusDeleted),
		int(scheduler.DefaultLeaseTimeout.Seconds())))
	if !strings.Contains(updPlan, "Function Scan on u") {
		t.Errorf("в плане перевода нет unnest-пакета (Function Scan on u):\n%s", updPlan)
	}
	if strings.Contains(updPlan, "Seq Scan on domains") {
		t.Errorf("перевод пачки деградировал в seq scan по domains:\n%s", updPlan)
	}
}

// leaseExplain — текст плана EXPLAIN (COSTS OFF) запроса в транзакции tx.
func leaseExplain(ctx context.Context, t *testing.T, tx pgx.Tx, query string) string {
	t.Helper()
	rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+query)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("EXPLAIN: чтение: %v", err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	return plan.String()
}
