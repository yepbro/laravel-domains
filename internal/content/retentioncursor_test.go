// Юнит-тесты персистентного курсора дренажа retention (T-274,
// retentioncursor.go + проводка в retentionPass/planCircle/RetentionLoop):
// round-trip (upsert в ТРАНЗАКЦИИ пачки — атомарность «пачка ⟺ курсор»),
// главный сценарий «рестарт продолжает с курсора, а не с нуля»
// (RetentionLoop читает состояние при старте), позиция в БД не двигается
// за отложенные защитой строки (потеря списка ретраев восполняется
// перечитыванием окна), план круга чистит состояние в момент плана и
// подавляет сохранения до применения, некорректное состояние → старт с
// нуля, отсутствие таблицы (42P01) — работа без персистентности.
// Фейковый пул — retention_test.go (строка курсора — cursorRow,
// применяется в Commit).
package content

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/model"
)

// runLoopPass — запуск RetentionLoop до первого завершённого прохода
// (стартовый дренаж) и остановка: единица сценариев «рестарта» — цикл
// читает персистентный курсор ДО первого прохода.
func runLoopPass(t *testing.T, pool *retentionFakePool, opts RetentionOptions) RetentionStats {
	t.Helper()
	passed := make(chan RetentionStats, 1)
	opts.OnPass = func(s RetentionStats, _ time.Duration) { passed <- s }
	opts.Retention = DefaultRetention
	opts.Interval = time.Hour // тикера не ждём — только стартовый проход
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()
	var stats RetentionStats
	select {
	case stats = <-passed:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("стартовый проход цикла не завершился")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	return stats
}

// TestRetentionPass_CursorSavedInBatchTx — round-trip: закоммиченная
// пачка фиксирует курсор СВОЕЙ транзакцией (upsert — последний оператор
// tx чистки), позиция равна последней строке прохода.
func TestRetentionPass_CursorSavedInBatchTx(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход: %v", err)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want {
		t.Fatalf("строка курсора = %+v, want %+v (последняя строка прохода)", pool.cursorRow, want)
	}
	if pool.cursorSaves != 1 || pool.begins != 2 {
		t.Fatalf("upsert'ов = %d (транзакций %d), want 1 upsert в tx пачки (2 транзакции: оболочка + чистка)",
			pool.cursorSaves, pool.begins)
	}
	// Атомарность: upsert — в ТРАНЗАКЦИИ ЧИСТКИ, последним оператором
	// (после purge/decref/delete, до Commit).
	tx := pool.txs[1]
	n := len(tx.calls)
	if n < 2 || tx.calls[n-1].sql != retentionCursorSaveSQL {
		t.Fatalf("последний оператор tx чистки = %v, want upsert курсора", tx.calls[n-1].sql)
	}
	if !tx.committed {
		t.Fatal("tx пачки не зафиксирована")
	}
}

// TestRetentionPass_CursorSaveFailsWithBatch — сбой upsert'а откатывает
// ПАЧКУ ЦЕЛИКОМ (атомарность в обе стороны): ни строки, ни курсора —
// повтор прохода идемпотентен с прежней позиции.
func TestRetentionPass_CursorSaveFailsWithBatch(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.cursorSaveErr = errors.New("бд недоступна")
	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err == nil ||
		!strings.Contains(err.Error(), "сохранение курсора") {
		t.Fatalf("ошибка = %v, want сбой сохранения курсора", err)
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("курсор = %+v (saves=%d), want не зафиксирован", pool.cursorRow, pool.cursorSaves)
	}
	for _, r := range pool.results {
		if r.hash == nil || !r.jsonb {
			t.Fatalf("строка %v очищена при откаченной пачке", r.key)
		}
	}
	if st.cursor != (retentionCursor{}) {
		t.Fatalf("in-memory курсор = %+v, want нулевой (пачка не зафиксирована)", st.cursor)
	}
}

// TestRetentionPass_DeferredBatchKeepsCursor — пачка с отложенными
// защитой очередей строками позицию в БД НЕ двигает (ревью T-274, P1):
// потеря списка ретраев при рестарте восполняется перечитыванием
// защитного окна — ключи отложатся заново. Полная и частичная отложка.
func TestRetentionPass_DeferredBatchKeepsCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0))},
		analyze: []map[RetentionPendingKey]struct{}{{}},
	}
	st := &retentionState{persist: true}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("Rows = %d, want 0 (вся пачка отложена)", stats.Rows)
	}
	// Персистентная позиция на месте (состояния нет — как до прохода):
	// рестарт перечитает окно и отложит ключи заново. In-memory курсор
	// двигается как раньше — зонд вернёт ключи в пределах процесса.
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want нет фиксации за отложенной пачкой",
			pool.cursorRow, pool.cursorSaves)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if st.cursor != want {
		t.Fatalf("in-memory курсор = %+v, want %+v (двигается и без чистки)", st.cursor, want)
	}

	// Частичная отложка: чистка идёт, но позиция в БД не двигается —
	// отложенный ключ не перешагивается персистентно.
	pool2 := drainPool(now)
	opts2 := retentionDefaultOpts()
	opts2.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0), {}},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}
	st2 := &retentionState{persist: true}
	stats2, err := retentionPass(context.Background(), pool2, opts2, st2)
	if err != nil {
		t.Fatalf("проход (частичная отложка): %v", err)
	}
	if stats2.Rows != 1 {
		t.Fatalf("Rows = %d, want 1 (sub=1 очищена, sub=0 отложена)", stats2.Rows)
	}
	if pool2.cursorRow != nil || pool2.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want нет фиксации: пачка перешагнула отложенный ключ",
			pool2.cursorRow, pool2.cursorSaves)
	}

	// Восстановление «рестартом»: защита выключена, новый процесс читает
	// прежнюю позицию БД (её нет — нулевой курсор) и вычищает обе строки;
	// теперь пачка без отложек — позиция фиксируется.
	pool.queries = nil
	statsRestart, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), &retentionState{persist: true})
	if err != nil {
		t.Fatalf("проход восстановления: %v", err)
	}
	if statsRestart.Rows != 2 {
		t.Fatalf("Rows = %d, want 2 (обе отложенные строки вычищены «рестартом»)", statsRestart.Rows)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("отложенная запись не вычищена после «рестарта» — потеря за курсором")
	}
	if pool.cursorRow == nil || pool.cursorSaves != 1 {
		t.Fatalf("строка курсора = %+v (saves=%d), want фиксация пачки без отложек",
			pool.cursorRow, pool.cursorSaves)
	}
}

// TestRetentionPass_CircleResetsPersistedCursor — принудительный
// catch-up-круг (обнуление счётчика рендер-долга) чистит персистентное
// состояние ДО обхода с нуля: рестарт посреди круга продолжит круг, а
// не вернётся к старому курсору; круговые пачки пишут позицию заново.
func TestRetentionPass_CircleResetsPersistedCursor(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	// 10 — под рендер-долгом (флаг): запись защищена, курсор её пройдёт;
	// 20 — обычная не-находка.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{flags: int16(model.FlagRenderPending)}
	pool.domains[retentionDomKey{zone: 1, id: 20}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	pool.counters["render_pending"] = 5

	st := &retentionState{persist: true}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 1 (эпоха долга): %v", err)
	}
	if pool.cursorRow == nil {
		t.Fatal("проход 1 не зафиксировал курсор")
	}

	// Долг снят: счётчик обнулился — проход 2 применяет catch-up-круг.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.counters["render_pending"] = 0
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2 (снятие долга): %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (догнали запись эпохи долга)", stats.Rows)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("args прохода 2 = %v, want catch-up-круг с нулевого курсора", pool.queries[0].args)
	}
	// Состояние сброшено (DELETE) и переписано круговой пачкой: круг шёл
	// с нуля, последняя посещённая строка — запись домена 10 (домен 20
	// уже чист с прохода 1, вне множества).
	if pool.cursorDelete != 1 {
		t.Fatalf("удалений строки курсора = %d, want 1 (круг чистит персистентное состояние)", pool.cursorDelete)
	}
	want := retentionCursor{checkedAt: old, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != want {
		t.Fatalf("строка курсора после круга = %+v, want %+v (позиция круговой пачки)", pool.cursorRow, want)
	}
}

// TestRetentionLoop_ResumesFromPersistedCursor — ГЛАВНЫЙ тест T-274:
// «рестарт» (новый RetentionLoop) читает сохранённый курсор и продолжает
// дренаж с него, а не с нуля — первая выборка стартового прохода идёт
// от сохранённой позиции, записи позади курсора не посещаются.
func TestRetentionLoop_ResumesFromPersistedCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	// «Прошлый процесс» остановился на первой строке.
	saved := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &saved

	stats := runLoopPass(t, pool, RetentionOptions{BatchSize: 100})
	if stats.Rows != 1 {
		t.Fatalf("стартовый проход = %+v, want 1 строка (только sub=1 впереди курсора)", stats)
	}
	wantArgs := []any{DefaultRetention.String(), saved.checkedAt, saved.key.zone, saved.key.domain, saved.key.sub, saved.key.variant, 100}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("первая выборка = %v, want продолжение от сохранённого курсора %v", pool.queries[0].args, wantArgs)
	}
	// sub=0 позади курсора — не тронута; курсор после прохода — на sub=1.
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("запись позади курсора очищена вопреки продолжению с курсора")
	}
	after := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if pool.cursorRow == nil || *pool.cursorRow != after {
		t.Fatalf("строка курсора после прохода = %+v, want %+v", pool.cursorRow, after)
	}
}

// TestRetentionLoop_InvalidPersistedCursor — некорректное состояние
// (отметка в будущем сверх допуска часов): Warn и прежнее поведение —
// старт с нулевого курсора, полный catch-up.
func TestRetentionLoop_InvalidPersistedCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	bad := retentionCursor{checkedAt: time.Now().Add(time.Hour), key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &bad

	var buf bytes.Buffer
	opts := RetentionOptions{BatchSize: 100, Log: retentionTestLogger(&buf)}
	stats := runLoopPass(t, pool, opts)
	if stats.Rows != 2 {
		t.Fatalf("стартовый проход = %+v, want 2 строки (полный catch-up с нуля)", stats)
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("первая выборка = %v, want нулевой курсор", pool.queries[0].args)
	}
	if !strings.Contains(buf.String(), "некорректен") {
		t.Errorf("лог = %q, want Warn о некорректном состоянии", buf.String())
	}
	// Некорректная строка перезаписана первым же коммитом пачки.
	if pool.cursorRow == nil || pool.cursorRow.checkedAt.Equal(bad.checkedAt) {
		t.Fatalf("строка курсора после прохода = %+v, want перезаписана пачкой", pool.cursorRow)
	}
}

// TestRetentionLoop_NoCursorTable — БД до миграции 000020 (42P01):
// Info, персистентность выключается на процесс — дренаж работает,
// сохранения нет (деградация к поведению T-273, не поломка).
func TestRetentionLoop_NoCursorTable(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.cursorErr = &pgconn.PgError{Code: "42P01"} // undefined_table

	var buf bytes.Buffer
	opts := RetentionOptions{BatchSize: 100, Log: retentionTestLogger(&buf)}
	stats := runLoopPass(t, pool, opts)
	if stats.Rows != 2 {
		t.Fatalf("стартовый проход = %+v, want 2 строки (дренаж работает без таблицы)", stats)
	}
	if pool.cursorSaves != 0 {
		t.Fatalf("upsert'ов курсора = %d, want 0 (персистентность выключена)", pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "таблицы персистентного курсора нет") {
		t.Errorf("лог = %q, want Info об отсутствующей таблице", buf.String())
	}
}

// TestRetentionLoop_NoCursorTableMidRun — таблица исчезла после старта
// (сброс посреди работы): первый сбой пачки выключает персистентность
// (Info), проход завершается без ошибки, чистка повторяется следующим.
func TestRetentionLoop_NoCursorTableMidRun(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.cursorSaveErr = &pgconn.PgError{Code: "42P01"} // upsert падает в tx пачки

	var buf bytes.Buffer
	opts := RetentionOptions{BatchSize: 100, Log: retentionTestLogger(&buf)}
	stats := runLoopPass(t, pool, opts)
	if stats.Rows != 0 {
		t.Fatalf("стартовый проход = %+v, want 0 строк (пачка откатана целиком)", stats)
	}
	if pool.cursorSaves != 0 {
		t.Fatalf("upsert'ов = %d, want 0", pool.cursorSaves)
	}
	// Персистентность выключена, строки целы — повторный проход чистит.
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), &retentionState{}); err != nil {
		t.Fatalf("повторный проход: %v", err)
	}
	if !strings.Contains(buf.String(), "таблицы персистентного курсора нет") {
		t.Errorf("лог = %q, want Info об отсутствующей таблице", buf.String())
	}
}

// TestRetentionOnce_DoesNotTouchCursor — разовый проход (runbooks T-167
// путь А, изолированные сборки) не читает и не пишет персистентное
// состояние цикла: изолированный дренаж не двигает позицию владельца.
func TestRetentionOnce_DoesNotTouchCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	saved := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	pool.cursorRow = &saved

	if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if pool.cursorSaves != 0 || pool.cursorDelete != 0 {
		t.Fatalf("upsert'ов=%d удалений=%d, want 0/0 (разовый проход без персистентности)",
			pool.cursorSaves, pool.cursorDelete)
	}
	if pool.cursorRow == nil || *pool.cursorRow != saved {
		t.Fatalf("строка курсора = %+v, want нетронутой %+v", pool.cursorRow, saved)
	}
}

// TestRetentionCursorLoadContract — тексты SQL персистентного курсора
// закреплены якорями (контракт миграции 000020: строка одна, upsert по
// PK, DELETE при кругах).
func TestRetentionCursorLoadContract(t *testing.T) {
	if retentionCursorLoadSQL != "SELECT checked_at, zone_id, domain_id, subdomain_id, variant FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorLoadSQL разошёлся с контрактом миграции 000020:\n%s", retentionCursorLoadSQL)
	}
	for _, want := range []string{
		"INSERT INTO retention_drain_cursor",
		"ON CONFLICT (id) DO UPDATE SET",
		"updated_at = EXCLUDED.updated_at",
	} {
		if !strings.Contains(retentionCursorSaveSQL, want) {
			t.Errorf("retentionCursorSaveSQL потерял фрагмент %q:\n%s", want, retentionCursorSaveSQL)
		}
	}
	if retentionCursorDeleteSQL != "DELETE FROM retention_drain_cursor WHERE id = 1" {
		t.Fatalf("retentionCursorDeleteSQL разошёлся с контрактом:\n%s", retentionCursorDeleteSQL)
	}
}

// TestRetentionLoadCursor_ZeroRow — все-нулевая строка (ручной SQL)
// трактуется как «состояния нет», а не как позиция год-один.
func TestRetentionLoadCursor_ZeroRow(t *testing.T) {
	pool := drainPool(retentionTestNow())
	pool.cursorRow = &retentionCursor{}
	cur, ok, err := retentionLoadCursor(context.Background(), pool)
	if err != nil || ok || cur != (retentionCursor{}) {
		t.Fatalf("load = (%+v, %v, %v), want пустое состояние без ошибки", cur, ok, err)
	}
}

// TestRetentionPass_OverflowPlanResetsCursorAndSuppressesSaves — план
// круга посреди прохода (переполнение списка ретраев): строка курсора
// удаляется В МОМЕНТ плана (рестарт в окне между планом и применением
// попадает в полный catch-up, ревью T-274 P2), а последующие пачки того
// же прохода позицию не resurrect-ят (сохранение подавлено до
// применения круга); следующий проход применяет круг с нуля.
func TestRetentionPass_OverflowPlanResetsCursorAndSuppressesSaves(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	// A, B — под защитой (переполняют предел 1), C — чистый кандидат
	// ПОСЛЕ переполнения: её пачка не должна воскресить строку курсора.
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0}, checkedAt: old.Add(2 * time.Hour), hash: retentionHash16(0x33), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}
	protected := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0), pendingKey(1, 10, 2, 0))
	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // пачка = строка: A → предел, B → план, C → чистка
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{protected, protected, protected, protected},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}, {}, {}},
	}

	var buf bytes.Buffer
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{persist: true, retryLimit: 1}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход: %v", err)
	}
	// A отложена (список), B переполнила (план круга), C — тоже защищена
	// в снимке: Rows=0, план стоит, applying — следующий проход.
	if stats.Rows != 0 || !st.circle {
		t.Fatalf("проход = %+v (circle=%v), want 0 строк и план круга", stats, st.circle)
	}
	if pool.cursorDelete != 1 {
		t.Fatalf("удалений строки курсора = %d, want 1 (план круга чистит БД в момент плана)", pool.cursorDelete)
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want подавлено до применения круга", pool.cursorRow, pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "переполнен") {
		t.Errorf("лог = %q, want Warn о переполнении", buf.String())
	}

	// Следующий проход применяет круг с нулевого курсора: все три строки
	// снова кандидаты (защита всё ещё стоит — ключи откладываются заново,
	// круг пересобирает множество).
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 2 (круг): %v", err)
	}
	if len(pool.queries) == 0 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(1)) {
		t.Fatalf("args прохода 2 = %v, want catch-up-круг с нулевого курсора", pool.queries[0].args)
	}
}

// TestRetentionPass_PlanResetFailureWarns — сбой отражения плана круга
// в БД — Warn без ошибки прохода (лучший шанс: круг в памяти состоится;
// рестарт в окне деградирует к продолжению со старой позиции — дороже,
// без потери данных). План круга не потребляется, строка... остаётся,
// сохранение всё равно подавлено.
func TestRetentionPass_PlanResetFailureWarns(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	protected := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0))
	opts := retentionDefaultOpts()
	opts.BatchSize = 1
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{protected, protected, protected},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}, {}},
	}
	// Фиксация сброса падает (commitErr бьёт по не-select транзакциям —
	// reset-tx плана круга).
	pool.commitErr = errors.New("бд недоступна")

	var buf bytes.Buffer
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{persist: true, retryLimit: 1}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("сбой отражения плана — не ошибка прохода: %v", err)
	}
	if !st.circle {
		t.Fatal("план круга потерян при сбое отражения в БД")
	}
	if pool.cursorRow != nil || pool.cursorSaves != 0 {
		t.Fatalf("строка курсора = %+v (saves=%d), want без воскресения (сохранение подавлено)", pool.cursorRow, pool.cursorSaves)
	}
	if !strings.Contains(buf.String(), "не отражён в БД") {
		t.Errorf("лог = %q, want Warn о неотражённом плане", buf.String())
	}
}
