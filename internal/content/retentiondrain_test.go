// Юнит-тесты состояния дренажа retention (T-185, retentiondrain.go):
// курсор по времени переживает проходы (строка посещается один раз за
// жизнь, следующий проход продолжает с места остановки), точечные
// предложения ретраев защиты очередей (зонд по PK, эвикция
// не-кандидатов, bound списка), catch-up-круги (обнуление счётчика
// рендер-долга T-175, fallback-сутки при отсутствии строки, переполнение
// списка), возврат перепроверённой строки в дренаж после старения.
// Фейковый пул — retention_test.go (включая счётчик crawler_counters).
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
	"crawler/internal/queue"
)

// drainPool — пул с одним живым доменом zone=1 (без флагов) и двумя
// остаревшими кандидатами sub=0/sub=1: база сценариев дренажа.
func drainPool(now time.Time) *retentionFakePool {
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x22), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 2000}
	return pool
}

// zeroArgs — ожидаемые аргументы выборки с нулевого курсора.
func zeroArgs(limit int) []any {
	return []any{DefaultRetention.String(), time.Time{}, int16(0), int64(0), int16(0), int16(0), limit}
}

// --- курсор цикла (ТЗ §4.2) ---------------------------------------------------

// TestRetentionPass_CursorAcrossPasses — курсор переживает проходы:
// первый дренаж останавливается на последней строке окна, часовой
// проход продолжает с места остановки и не перечитывает пройденное;
// состарившаяся за час запись подбирается без полного круга.
func TestRetentionPass_CursorAcrossPasses(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	// Третья запись свежа (5,5 ч < 6 ч окна) — не кандидат первого прохода.
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: now.Add(-5*time.Hour - 30*time.Minute), hash: retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("проход 1: Rows = %d, want 2 (sub=0, sub=1)", stats.Rows)
	}
	want := retentionCursor{checkedAt: pool.results[1].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}}
	if st.cursor != want {
		t.Fatalf("курсор после прохода 1 = %+v, want %+v (последняя строка окна)", st.cursor, want)
	}

	// Час спустя sub=2 состарилась (5,5 ч > 6 ч − 1 ч окна) — проход
	// продолжает от курсора, без полного круга.
	pool.now = now.Add(time.Hour)
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (только состаревшаяся sub=2)", stats.Rows)
	}
	if len(pool.queries) != 1 {
		t.Fatalf("SELECT'ов прохода 2 = %d, want 1", len(pool.queries))
	}
	wantArgs := []any{DefaultRetention.String(), want.checkedAt, want.key.zone, want.key.domain, want.key.sub, want.key.variant, 100}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("args прохода 2 = %v, want курсор прохода 1 %v", pool.queries[0].args, wantArgs)
	}
}

// TestRetentionPass_InterlockKeepsCursor — остановка интерлоком среди
// дренажа сохраняет прогресс: зафиксированные пачки продвинули курсор,
// следующий проход продолжает за ними, а не с нуля.
func TestRetentionPass_InterlockKeepsCursor(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: pool.results[1].checkedAt.Add(time.Hour), hash: retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}

	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // пачка = строка
	depth := &fakeDepth{n: 0}
	opts.Depth = depth
	opts.AnalyzeWatermark = 100
	var selects int
	pool.onSelect = func() {
		selects++
		if selects >= 1 {
			depth.mu.Lock()
			depth.n = 150 // анализатор перестал успевать после первой пачки
			depth.mu.Unlock()
		}
	}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 1 || !stats.Skipped {
		t.Fatalf("проход 1 = %+v, want одна пачка и пропуск интерлоком", stats)
	}
	want := retentionCursor{checkedAt: pool.results[0].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	if st.cursor != want {
		t.Fatalf("курсор после остановки = %+v, want %+v (первая строка)", st.cursor, want)
	}

	// Глубина спала: дренаж продолжается с курсора — строки 1 и 2.
	depth.mu.Lock()
	depth.n = 0
	depth.mu.Unlock()
	pool.onSelect = nil // час глубины прошёл — гонки больше нет
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 2 || stats.Skipped {
		t.Fatalf("проход 2 = %+v, want 2 строки без пропуска", stats)
	}
	wantArgs := []any{DefaultRetention.String(), want.checkedAt, want.key.zone, want.key.domain, want.key.sub, want.key.variant, 1}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Fatalf("args прохода 2 = %v, want продолжение от курсора %v", pool.queries[0].args, wantArgs)
	}
}

// TestRetentionPass_RewrittenReEnters — перезапись recheck (DELETE+INSERT,
// fresh checked_at и новый контент, T-058) пропускается охраной в текущем
// окне и входит в дренаж заново, когда состарится: fresh отметка всегда
// впереди курсора (ТЗ §3.2).
func TestRetentionPass_RewrittenReEnters(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	// Перезапись между выборкой и чисткой: свежая отметка, новый контент.
	pool.onSelect = func() {
		r := pool.results[0]
		r.checkedAt, r.hash = now.Add(-time.Hour), retentionHash16(0x99)
		pool.results[0] = r
	}
	pool.contents[retentionHash(0x99)] = retentionFakeContent{refc: 1, clen: 1500}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("проход 1: Rows = %d, want 0 (охрана возраста)", stats.Rows)
	}

	// Шесть часов спустя перезапись состарилась — дренаж подбирает её.
	pool.now = now.Add(6*time.Hour + time.Minute)
	pool.onSelect = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 || stats.FreedBytes != 1500 {
		t.Fatalf("проход 2 = %+v, want чистка перезаписанной строки (новый контент 0x99)", stats)
	}
}

// --- ретраи защиты очередей (ТЗ §4.3) -----------------------------------------

// TestRetentionPass_RetryOffer — ключ, отложенный снимком q:render,
// предлагается точечным зондом в следующем проходе: защита ушла —
// чистится по месту, список пустеет; Forward-дренаж при этом продолжает
// от своего курсора.
func TestRetentionPass_RetryOffer(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render: []map[RetentionPendingKey]struct{}{
			pendingKey(1, 10, 0, 0), // снимок 1: sub=0 ждёт q:render
			{},                      // снимок 2: защита ушла
		},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 1 || stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("проход 1 = %+v, want sub=1 очищена, sub=0 отложена", stats)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d ключей, want 1", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("защищённая запись очищена в проходе 1")
	}

	// Проход 2: зонд предложил ключ, защита ушла — чистка на месте.
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (зонд очистил sub=0)", stats.Rows)
	}
	if len(pool.queries) != 2 || pool.queries[0].sql != retentionProbeSQL || pool.queries[1].sql != retentionSelectSQL {
		t.Fatalf("обращения прохода 2: %d (первый = %q), want зонд THEN выборка", len(pool.queries), pool.queries[0].sql)
	}
	probe := pool.queries[0]
	if !reflect.DeepEqual(probe.args[:4], []any{
		[]int16{1}, []int64{10}, []int16{0}, []int16{0},
	}) || probe.args[4] != DefaultRetention.String() {
		t.Errorf("args зонда = %v, want ключ sub=0 с границей старения", probe.args)
	}
	if len(st.retries) != 0 {
		t.Fatalf("список ретраев после прохода 2 = %d ключей, want 0", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("запись не очищена зондом прохода 2")
	}
}

// TestRetentionPass_RetryStillProtected — ключ, всё ещё ждущий
// потребителей, зондом не чистится и остаётся в списке на следующий
// проход (до снятия защиты); исключение наблюдаемо в Deferred.
func TestRetentionPass_RetryStillProtected(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	protected := pendingKey(1, 10, 0, 0)
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{protected, protected}, // снимки 1 и 2
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("проход 2: Rows = %d, want 0 (ключ всё ещё защищён)", stats.Rows)
	}
	if stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("Deferred = %v, want повторное исключение ключа зонда", stats.Deferred)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 (ключ держится до снятия защиты)", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil {
		t.Fatal("защищённая запись очищена вопреки защите")
	}
}

// TestRetentionPass_RetryEviction — ключ, переставший быть кандидатом
// (домен стал Laravel-находкой), зонд не возвращает — ключ выпадает из
// списка, чистки нет (ТЗ §4.3: «исключая из предложения те, чья защита
// ушла и очистка прошла» — и симметричные случаи).
func TestRetentionPass_RetryEviction(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0), {}},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 после прохода 1", len(st.retries))
	}

	// Домен стал находкой: зонд не вернёт ключ — эвикция без чистки.
	pool.laravel[retentionDomKey{zone: 1, id: 10}] = true
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("проход 2 = %+v, want пусто (находка — не кандидат)", stats)
	}
	if len(st.retries) != 0 {
		t.Fatalf("список ретраев = %d, want 0 (эвикция не-кандидата)", len(st.retries))
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
		t.Fatal("запись находки затронута чисткой")
	}
}

// TestRetentionPass_RetryBound — переполнение списка ретраев: Warn и
// консервативный catch-up-круг следующим проходом (список пересоберёт
// сам круг); новый список ключей не растёт сверх предела.
func TestRetentionPass_RetryBound(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	for sub := int16(0); sub < 3; sub++ {
		pool.results = append(pool.results, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old.Add(time.Duration(sub) * time.Hour), hash: retentionHash16(byte(0x10 + sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(0x10+sub))] = retentionFakeContent{refc: 1, clen: 1000}
	}
	allProtected := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 1, 0), pendingKey(1, 10, 2, 0))
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{allProtected, allProtected},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	var buf bytes.Buffer
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{retryLimit: 1}
	opts.BatchSize = 1 // пачка = строка: переполнение встречается в нескольких пачках
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if !st.circle || len(st.retries) != 1 {
		t.Fatalf("после прохода 1: circle=%v, retries=%d (want запланированный круг, предел 1)", st.circle, len(st.retries))
	}
	// Warn — ровно один за эпизод переполнения (ревью T-185, P3-1 R2):
	// вторая и третья пачки с ждущими ключами молчат, план уже стоит.
	// Считается текст Warn: причина круга в Info-строке прохода 2 тоже
	// содержит слово «переполнен» — это не Warn.
	const warnOverflow = "переполнен — следующий проход catch-up-кругом"
	if got := strings.Count(buf.String(), warnOverflow); got != 1 {
		t.Fatalf("Warn о переполнении = %d за проход 1, want 1", got)
	}

	// Следующий проход — catch-up-круг с нулевого курсора: список
	// очищен, выборка снова с начала; застой держится — ключи
	// откладываются заново (регресс полных кругов ограничен эпизодами
	// застоя у watermark'ов), новый эпизод — новый Warn.
	pool.queries = nil
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if len(pool.queries) < 1 || pool.queries[0].sql != retentionSelectSQL {
		t.Fatalf("обращения прохода 2 = %d, want выборки круга (зонд не звать: список очищен)", len(pool.queries))
	}
	if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(1)) {
		t.Fatalf("args прохода 2 = %v, want нулевой курсор catch-up-круга", pool.queries[0].args)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев круга = %d, want 1 (переполнение на прежнем пределе)", len(st.retries))
	}
	if got := strings.Count(buf.String(), warnOverflow); got != 2 {
		t.Fatalf("Warn о переполнении = %d за два прохода, want 2 (по одному на эпизод)", got)
	}
}

// --- catch-up по рендер-долгу (ТЗ §4.4) ---------------------------------------

// TestRetentionPass_DebtCatchUp — записи под флагом render_pending молча
// отфильтровываются выборкой (курсор проходит их): обнуление счётчика
// долга T-175 — переход в 0 — планирует catch-up-круг с нулевого
// курсора, догоняющий всё, что копилось за эпоху долга.
func TestRetentionPass_DebtCatchUp(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	// 10 — под рендер-долгом (флаг, не rendered): запись защищена;
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

	var buf bytes.Buffer
	opts := retentionDefaultOpts()
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1 (эпоха долга): %v", err)
	}
	if stats.Rows != 1 || !st.debtSeen || st.circle {
		t.Fatalf("проход 1 = %+v (debtSeen=%v): только домен 20, круг не планируется", stats, st.debtSeen)
	}

	// Долг дренирован: флаг снят, счётчик обнулился.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.counters["render_pending"] = 0
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 2 (снятие долга): %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("проход 2: Rows = %d, want 1 (догнали запись домена 10)", stats.Rows)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("args прохода 2 = %v, want catch-up-круг с нулевого курсора", pool.queries[0].args)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Fatal("запись эпохи долга не очищена catch-up-кругом")
	}
	if !strings.Contains(buf.String(), "обнулился") {
		t.Errorf("лог = %q, want причина круга — обнуление счётчика", buf.String())
	}
}

// TestRetentionPass_DebtFallback — строки счётчика нет (БД до деплоя
// T-175): fallback — круг раз в сутки от последнего круга, между ними
// проходы продолжают от курсора; якорь — стартовый круг процесса.
func TestRetentionPass_DebtFallback(t *testing.T) {
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
	delete(pool.counters, "render_pending")

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if st.lastCircle.IsZero() {
		t.Fatal("стартовый круг не зафиксирован якорем fallback-суток")
	}

	// Час спустя: новой записи нет, круг НЕ планируется (сутки не вышли)
	// — проход один пустой SELECT от курсора.
	pool.now = now.Add(time.Hour)
	pool.results = append(pool.results, retentionRow{
		key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		// Остарела к этому моменту (11,5 ч > 5 ч границы), но сидит ПОЗАДИ
		// курсора прохода 1 (11 ч): до круга не трогается вовсе.
		checkedAt: now.Add(-11*time.Hour - 30*time.Minute), hash: retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 0 || len(pool.queries) != 1 {
		t.Fatalf("проход 2 = %+v (%d SELECT'ов), want пустой проход от курсора без круга", stats, len(pool.queries))
	}
	if reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatal("проход 2 пошёл с нулевого курсора вопреки неистёкшим суткам fallback")
	}

	// Сутки спустя — fallback-круг с нулевого курсора подбирает хвост.
	st.lastCircle = time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour)
	pool.queries = nil
	stats, err = retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 3: %v", err)
	}
	if stats.Rows != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("проход 3 = %+v, args=%v: want fallback-круг, подбирающий sub=2", stats, pool.queries[0].args)
	}
}

// TestRetentionPass_DebtReadError — ошибка чтения счётчика — не ошибка
// прохода: Warn, состояние долга держится (якорь fallback свеж — круг не
// срабатывает), дренаж идёт штатно.
func TestRetentionPass_DebtReadError(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.counters["render_pending"] = 5
	pool.debtErr = errors.New("бд недоступна")

	var buf bytes.Buffer
	opts := retentionDefaultOpts()
	opts.Log = retentionTestLogger(&buf)
	// Якорь не ставится вручную: его обязана поставить инициализация
	// состояния в retentionPass — иначе (мутация «убрать блок») fallback
	// немедленно применяет круг и сбрасывает debtSeen (ревью T-185,
	// P3-1 R3).
	st := &retentionState{debtSeen: true}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("stats = %+v, want штатный дренаж при ошибке счётчика", stats)
	}
	if !st.debtSeen || st.circle {
		t.Fatalf("состояние долга: debtSeen=%v circle=%v, want держится без круга", st.debtSeen, st.circle)
	}
	if st.lastCircle.IsZero() {
		t.Fatal("якорь fallback-суток не поставлен при создании состояния")
	}
	if !strings.Contains(buf.String(), "не прочитан") {
		t.Errorf("лог = %q, want Warn о нечитаемом счётчике", buf.String())
	}
}

// TestRetentionPass_DebtAnchorRefresh — якорь fallback-суток двигают
// успешные чтения счётчика: сутки «с прошлого круга» истекли, но чтение
// удалось — круг не поднимается, курсор не сбрасывается; транзиентный
// сбой следующего прохода тоже (якорь свеж) — fallback значит
// «недоступен сутки ПОДРЯД» (ревью T-185, P3-2 R3).
func TestRetentionPass_DebtAnchorRefresh(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	anchor := retentionCursor{checkedAt: now.Add(-time.Hour), key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}}
	st := &retentionState{
		cursor:     anchor,                                                      // дренаж уже прошёл эту точку
		lastCircle: time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour), // «сутки истекли»
	}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if st.circle || st.cursor != anchor {
		t.Fatalf("после прохода 1: circle=%v cursor=%+v — успешное чтение обязано обновить якорь, круг не поднимается", st.circle, st.cursor)
	}

	// Следующий час — сбой чтения: якорь свеж, круг не поднимается.
	pool.debtErr = errors.New("бд недоступна")
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if st.circle || st.cursor != anchor {
		t.Fatalf("транзиентный сбой чтения поднял круг: circle=%v cursor=%+v", st.circle, st.cursor)
	}
}

// TestRetentionPass_DebtReadErrorFallback — стойко недоступный счётчик
// (сбой чтения) включает страховку fallback-суток: якорь истёк — круг с
// нулевого курсора подбирает и строку, утраченную позади курсора (ревью
// T-185, P3-3/P2-1 R2).
func TestRetentionPass_DebtReadErrorFallback(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if st.cursor == (retentionCursor{}) {
		t.Fatal("проход 1 не продвинул курсор")
	}

	// Строка позади курсора (класс утечки P1-1): круг страховки обязан
	// её вычистить.
	pool.results = append(pool.results, retentionRow{
		key:       retentionKey{zone: 1, domain: 10, sub: 2, variant: 0},
		checkedAt: now.Add(-11*time.Hour - 30*time.Minute), // позади курсора прохода 1
		hash:      retentionHash16(0x33), jsonb: true,
	})
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 3000}

	pool.debtErr = errors.New("бд недоступна")
	st.lastCircle = time.Now().Add(-DefaultRetentionFallbackCircle - time.Hour)
	pool.queries = nil
	stats, err := retentionPass(context.Background(), pool, retentionDefaultOpts(), st)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if stats.Rows != 1 || stats.FreedBytes != 3000 {
		t.Fatalf("проход 2 = %+v, want вычищена утраченная за курсором строка", stats)
	}
	if len(pool.queries) != 1 || !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
		t.Fatalf("args прохода 2 = %v, want выборка fallback-круга с нулевого курсора", pool.queries[0].args)
	}
	// Курсор после круга — на подобранной строке (дренаж продолжился
	// штатно с нулевой точки).
	want := retentionCursor{checkedAt: pool.results[2].checkedAt, key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 0}}
	if st.cursor != want {
		t.Fatalf("курсор после прохода 2 = %+v, want %+v (подобранная строка круга)", st.cursor, want)
	}
}

// TestRetentionPass_DebtTableMissing — БД до деплоя миграции 000015:
// undefined_table (42P01) — не сбой чтения, а «счётчика нет» — ветка
// fallback без Warn, один Info на процесс (ревью T-185, P3-2/P3-4).
func TestRetentionPass_DebtTableMissing(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	pool.debtErr = &pgconn.PgError{Code: "42P01"} // undefined_table

	var buf bytes.Buffer
	opts := retentionDefaultOpts()
	opts.Log = retentionTestLogger(&buf)
	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("проход 1: stats = %+v, want штатный дренаж", stats)
	}
	// Второй проход того же стенда — Info не повторяется.
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if strings.Contains(buf.String(), "не прочитан") {
		t.Errorf("лог = %q: absent-table обязан молчать (Warn только для сбоев)", buf.String())
	}
	if got := strings.Count(buf.String(), "таблицы счётчика рендер-долга нет"); got != 1 {
		t.Errorf("Info об отсутствующей таблице = %d, want 1 (один раз на процесс)", got)
	}
}

// --- контракты ----------------------------------------------------------------

// TestRetentionDebtSQLContract — точечное чтение счётчика — тот же
// оператор, что guard прохода доработки рендера (renderDebtCounterSQL,
// T-181/T-175): литерал дублирован якорем, расхождение контракта
// ловится здесь.
func TestRetentionDebtSQLContract(t *testing.T) {
	if retentionDebtSQL != "SELECT value FROM crawler_counters WHERE name = 'render_pending'" {
		t.Fatalf("retentionDebtSQL разошёлся с контрактом T-175/T-181:\n%s", retentionDebtSQL)
	}
}

// TestRetentionOnce_EachFromZero — разовый RetentionOnce состояния не
// держит: каждый вызов начинает с нулевого курсора (первая выборка
// каждого прохода — ноль; runbooks T-167 путь А, изолированные сборки).
func TestRetentionOnce_EachFromZero(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	for pass := 1; pass <= 2; pass++ {
		if _, err := RetentionOnce(context.Background(), pool, opts); err != nil {
			t.Fatalf("проход %d: %v", pass, err)
		}
		if !reflect.DeepEqual(pool.queries[0].args, zeroArgs(100)) {
			t.Fatalf("проход %d: первая выборка = %v, want нулевой курсор (разовый проход)", pass, pool.queries[0].args)
		}
		pool.queries = nil
	}
}

// TestRetentionSQLShape — регрессионный якорь формы SQL дренажа (ТЗ §5.1
// «текст retentionSelectSQL»; ревью T-185, P3-4): курсор row-value по
// ВРЕМЕНИ с PK-хвостом, ORDER BY тем же ключом, LIMIT последним; зонд —
// LATERAL по PK с LIMIT 1 (закрепление точечного плана). Возврат к
// PK-порядку (диагноз запроса №13) или потеря детерминизма ловится тут.
func TestRetentionSQLShape(t *testing.T) {
	for _, want := range []string{
		"WHERE r.checked_at < now() - $1::interval",
		"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) " +
			"> ($2::timestamptz, $3::int2, $4::int8, $5::int2, $6::int2)",
		"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant",
		"LIMIT $7",
	} {
		if !strings.Contains(retentionSelectSQL, want) {
			t.Errorf("retentionSelectSQL потерял фрагмент дренажа по времени:\n%s\n--- нет:\n%s", retentionSelectSQL, want)
		}
	}
	// Курсор по PK (прежний порядок) обязан уйти.
	if strings.Contains(retentionSelectSQL, "ORDER BY r.zone_id, r.domain_id") {
		t.Errorf("retentionSelectSQL вернул PK-порядок (диагноз запроса №13):\n%s", retentionSelectSQL)
	}
	for _, want := range []string{
		"JOIN LATERAL (",
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id",
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant",
		"LIMIT 1) AS r ON true",
	} {
		if !strings.Contains(retentionProbeSQL, want) {
			t.Errorf("retentionProbeSQL потерял фрагмент точечного зонда:\n%s\n--- нет:\n%s", retentionProbeSQL, want)
		}
	}
	if strings.Contains(retentionProbeSQL, "ORDER BY") {
		t.Errorf("retentionProbeSQL не нуждается в порядке:\n%s", retentionProbeSQL)
	}
}

// TestRetentionPass_EqualTimestampsBoundary — главный риск потери строк
// пагинации: пачки строк одной транзакции делят отметку checked_at
// (T-058), граница пачки режет такую группу — PK-хвост курсора обязан
// вернуть остаток группы следующей пачкой (ревью T-185, P3-4).
func TestRetentionPass_EqualTimestampsBoundary(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	for sub := int16(0); sub < 3; sub++ {
		pool.results = append(pool.results, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(0x10 + sub)), jsonb: true, // одна отметка на всех
		})
		pool.contents[retentionHash(byte(0x10+sub))] = retentionFakeContent{refc: 1, clen: 1000}
	}

	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // пачка = строка: группа режется дважды
	st := &retentionState{}
	stats, err := retentionPass(context.Background(), pool, opts, st)
	if err != nil {
		t.Fatalf("проход: %v", err)
	}
	if stats.Rows != 3 {
		t.Fatalf("Rows = %d, want 3 (все строки равной отметки подобраны)", stats.Rows)
	}
	if len(pool.queries) != 4 { // три пачки + пустая выборка конца
		t.Fatalf("SELECT'ов = %d, want 4", len(pool.queries))
	}
	// Курсоры пачек 2 и 3 — та же отметка, ключ строго растёт: курсор
	// выборки i+1 — последняя строка пачки i (sub = 0, затем 1).
	for i := 1; i < 3; i++ {
		args := pool.queries[i].args
		if !args[1].(time.Time).Equal(old) {
			t.Fatalf("пачка %d: курсор-время = %v, want общая отметка %v", i+1, args[1], old)
		}
		if args[4].(int16) != int16(i-1) {
			t.Fatalf("пачка %d: курсор sub = %v, want %d (PK-хвост за последней строкой предыдущей)", i+1, args[4], i-1)
		}
	}
}

// TestRetentionPass_RetryPurgeFailureKeepsList — сбой чистки зонда
// сохраняет прежний список ретраев: неочищенные ключи не теряются позади
// курсора, следующий проход повторит зонд идемпотентно (утверждение
// сообщения коммита; ревью T-185, P3-4).
func TestRetentionPass_RetryPurgeFailureKeepsList(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0), {}},
		analyze: []map[RetentionPendingKey]struct{}{{}, {}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 после прохода 1", len(st.retries))
	}

	// Чистка зонда падает: список обязан уцелеть до следующего прохода.
	pool.purgeErr = errors.New("бд недоступна")
	if _, err := retentionPass(context.Background(), pool, opts, st); err == nil {
		t.Fatal("проход 2 обязан упасть ошибкой чистки")
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев после сбоя чистки = %d, want 1 (ключи не теряются)", len(st.retries))
	}
}

// TestRetentionPass_NoMidPassProbe — зонд предлагается строго в начале
// прохода: ключи, отложенные текущим проходом, предлагаются следующим
// (ревью T-185, P3-2 — контракт комментария retentionPass).
func TestRetentionPass_NoMidPassProbe(t *testing.T) {
	now := retentionTestNow()
	pool := drainPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0)},
		analyze: []map[RetentionPendingKey]struct{}{{}},
	}

	st := &retentionState{}
	if _, err := retentionPass(context.Background(), pool, opts, st); err != nil {
		t.Fatalf("проход: %v", err)
	}
	if len(st.retries) != 1 {
		t.Fatalf("список ретраев = %d, want 1 (ключ отложен текущим проходом)", len(st.retries))
	}
	for i, q := range pool.queries {
		if q.sql == retentionProbeSQL {
			t.Fatalf("зонд выполнен среди дренажа (обращение %d) вопреки «один раз за проход, до дренажа»", i)
		}
	}
}
