// Юнит-тесты точной защиты retention — исключения из пачек записей,
// чьи сообщения ждут потребителей q:render/q:analyze (T-166/T-167,
// retentionpending.go): фейковый источник ключей поверх фейкового пула
// retention_test.go. Проверяются исключение ключей обоих стримов (в т.ч.
// когда SQL уже вернул записи кандидатами), пропуск пачки целиком без
// транзакции, обновление снимка при длинном дренаже, консервативный
// пропуск прохода при ошибке чтения стримов (первый снимок и обновление).
package content

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"crawler/internal/queue"
)

// retentionTestNow — фиксированное «сейчас» проходов (стиль retention_test.go).
func retentionTestNow() time.Time {
	return time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
}

// fakePending — источник непотреблённых ключей с заготовленными
// снимками: render/analyze — последовательности множеств по снимкам
// (последний повторяется), errFrom — с какого снимка (1-based, снимок
// считается по обращению к q:render — первый стрим чтения) отвечать
// ошибкой Redis; 0 — никогда. calls — порядок обращений по стримам.
type fakePending struct {
	mu      sync.Mutex
	render  []map[RetentionPendingKey]struct{}
	analyze []map[RetentionPendingKey]struct{}
	errFrom int
	snap    int
	calls   []string
}

// compile-time: фейк — источник защиты для тестов без Redis.
var _ RetentionPending = (*fakePending)(nil)

func (f *fakePending) PendingKeys(_ context.Context, stream string) (map[RetentionPendingKey]struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, stream)
	if stream == queue.StreamRender {
		f.snap++
	}
	if f.errFrom > 0 && f.snap >= f.errFrom {
		return nil, errors.New("redis недоступен")
	}
	seq := &f.analyze
	if stream == queue.StreamRender {
		seq = &f.render
	}
	if len(*seq) == 0 {
		return map[RetentionPendingKey]struct{}{}, nil
	}
	m := (*seq)[0]
	if len(*seq) > 1 {
		*seq = (*seq)[1:]
	}
	return m, nil
}

// pendingKey — ключ защиты из координат записи.
func pendingKey(zone int16, domain int64, sub, variant int16) map[RetentionPendingKey]struct{} {
	return map[RetentionPendingKey]struct{}{{
		Zone: zone, Domain: domain, Sub: sub, Variant: variant,
	}: {}}
}

// TestRetentionOnce_PendingExclude — записи, чьи сообщения ждут
// потребителей q:render/q:analyze, исключаются из чистки, хотя выборка
// вернула их кандидатами: контент, jsonb и refcount не тронуты; запись,
// ждущая оба стрима сразу, считается в каждом (наблюдаемость «кто
// держит»); не-ожидающие записи пачки чистятся штатно.
func TestRetentionOnce_PendingExclude(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	old := now.Add(-2 * DefaultRetention)
	// Два дополнительных кандидата: (10,1,0) ждёт оба стрима,
	// (10,2,1) — только q:analyze; чистые кандидаты seed — (10,0,0)
	// и rendered-запись домена 40.
	pool.results = append(pool.results,
		retentionRow{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		retentionRow{key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 1}, checkedAt: old, hash: retentionHash16(0x22), jsonb: true},
	)
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 1, clen: 1000}
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 1000}

	opts := retentionDefaultOpts()
	// Снимок 1 (единственный): (10,1,0) ждёт оба стрима, (10,2,1) — q:analyze.
	opts.Pending = &fakePending{
		render:  []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 1, 0)},
		analyze: []map[RetentionPendingKey]struct{}{mergePending(pendingKey(1, 10, 1, 0), pendingKey(1, 10, 2, 1))},
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 2 || stats.Refs != 2 {
		t.Fatalf("stats = %+v, want Rows=Refs=2 (только не-ожидающие: 10 и 40)", stats)
	}
	if stats.Deferred[queue.StreamRender] != 1 || stats.Deferred[queue.StreamAnalyze] != 2 {
		t.Fatalf("Deferred = %v, want q:render=1 (запись 10/1/0), q:analyze=2 (10/1/0 и 10/2/1)", stats.Deferred)
	}
	if stats.Skipped || stats.SkippedPending {
		t.Fatalf("stats = %+v, want проход без пропусков", stats)
	}
	// Записи, ждущие очередей, не тронуты: контент и jsonb на месте,
	// refcount живых строк не менялся.
	for _, key := range []retentionKey{
		{zone: 1, domain: 10, sub: 1, variant: 0},
		{zone: 1, domain: 10, sub: 2, variant: 1},
	} {
		if r := pool.row(key); r == nil || r.hash == nil || !r.jsonb {
			t.Errorf("запись %v, ждущая очередей, очищена: %+v", key, r)
		}
	}
	if _, ok := pool.contents[retentionHash(0x11)]; !ok {
		t.Error("contents 0x11 (ждёт очередей) удалён")
	}
	if _, ok := pool.contents[retentionHash(0x22)]; !ok {
		t.Error("contents 0x22 (ждёт очередей) удалён")
	}
	// SQL вернул ждущие записи кандидатами — исключение сделала защита.
	if len(pool.queries) == 0 {
		t.Fatal("выборок нет — тест не проверил исключение кандидатов")
	}
}

// TestRetentionOnce_PendingAllExcluded — пачка целиком ждёт очередей:
// транзакция не открывается, дренаж завершается штатно (курсор прошёл
// выборку), записи не тронуты.
func TestRetentionOnce_PendingAllExcluded(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	// Один кандидат, и его сообщение ждёт q:render.
	pool.results = pool.results[:1]
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{
		render: []map[RetentionPendingKey]struct{}{pendingKey(1, 10, 0, 0)},
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("stats = %+v, want пустой дренаж без транзакций", stats)
	}
	if stats.Deferred[queue.StreamRender] != 1 {
		t.Fatalf("Deferred = %v, want q:render=1", stats.Deferred)
	}
	if pool.begins != 0 {
		t.Fatalf("транзакций = %d, want 0 (чистить нечего)", pool.begins)
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
		t.Fatalf("ждущая запись тронута: %+v", r)
	}
}

// TestRetentionOnce_PendingRefresh — снимок обновляется при длинном
// дренаже (DefaultRetentionPendingRefresh выборок): запись, сообщение
// которой попало в стрим после первого снимка, защищена обновлением —
// без него чистилась бы (гонка переиздания T-066, retentionpending.go).
func TestRetentionOnce_PendingRefresh(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	var rows []retentionRow
	for sub := int16(0); sub < 26; sub++ {
		rows = append(rows, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(sub))] = retentionFakeContent{refc: 1, clen: 100}
	}
	pool.results = rows

	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // дренаж пачками-строками: 26 выборок
	second := mergePending(pendingKey(1, 10, 0, 0), pendingKey(1, 10, 25, 0))
	opts.Pending = &fakePending{
		render: []map[RetentionPendingKey]struct{}{
			pendingKey(1, 10, 0, 0), // снимок 1: ждёт первая запись
			second,                  // снимок 2 (обновление): и последняя — сообщение опубликовано после первого снимка
		},
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	// Чистятся 24 записи: sub 1..24; sub 0 — первый снимок, sub 25 —
	// обновлённый (сообщение опубликовано после первого снимка).
	if stats.Rows != 24 {
		t.Fatalf("Rows = %d, want 24 (исключены sub=0 и sub=25)", stats.Rows)
	}
	for _, sub := range []int16{0, 25} {
		if r := pool.row(retentionKey{zone: 1, domain: 10, sub: sub, variant: 0}); r.hash == nil || !r.jsonb {
			t.Errorf("запись sub=%d, защищённая обновлением снимка, очищена", sub)
		}
	}
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 12, variant: 0}); r.hash != nil {
		t.Errorf("запись sub=12 без защиты не очищена")
	}
	p := opts.Pending.(*fakePending)
	if len(p.calls) != 4 {
		t.Fatalf("обращений к стримам = %v, want 4 (два снимка × q:render+q:analyze)", p.calls)
	}
}

// TestRetentionOnce_PendingAfterInterlock — порядок проверок: интерлок
// q:analyze раньше первого снимка стримов — при закрытом интерлоке
// тяжёлое чтение хвостов не выполняется вовсе (и его таймаут не даёт
// ложного SkippedPending вместо честного Skipped).
func TestRetentionOnce_PendingAfterInterlock(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	opts := retentionDefaultOpts()
	opts.AnalyzeWatermark = 100
	depth := &fakeDepth{n: 150}
	opts.Depth = depth
	pending := &fakePending{errFrom: 1} // даже недоступный источник не читается
	opts.Pending = pending

	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if !stats.Skipped || stats.SkippedPending {
		t.Fatalf("stats = %+v, want Skipped интерлока (не SkippedPending)", stats)
	}
	if len(pending.calls) != 0 {
		t.Fatalf("снимок при закрытом интерлоке читался: %v", pending.calls)
	}
	if len(pool.queries) != 0 {
		t.Fatalf("SELECT'ов = %d, want 0", len(pool.queries))
	}
}

// TestRetentionOnce_PendingReadError — ошибка чтения стримов ДО первой
// выборки: консервативный пропуск прохода — не ошибка, БД не тронута,
// итог уходит в OnPass (SkippedPending — метрика passes{skipped_pending}
// и pending_errors_total, Warn в цикле).
func TestRetentionOnce_PendingReadError(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	opts := retentionDefaultOpts()
	opts.Pending = &fakePending{errFrom: 1} // первый же снимок — ошибка

	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v, want консервативный пропуск без ошибки", err)
	}
	if !stats.SkippedPending || stats.Rows != 0 {
		t.Fatalf("stats = %+v, want SkippedPending без работы", stats)
	}
	if len(pool.queries) != 0 || pool.begins != 0 {
		t.Fatalf("пропущенный проход обязан не трогать БД: SELECT'ов %d, транзакций %d", len(pool.queries), pool.begins)
	}
}

// TestRetentionOnce_PendingRefreshError — ошибка обновления снимка среди
// дренажа: сделанные пачки остаются (пачки атомарны сами по себе),
// остаток прохода пропущен консервативно.
func TestRetentionOnce_PendingRefreshError(t *testing.T) {
	now := retentionTestNow()
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	var rows []retentionRow
	for sub := int16(0); sub < 26; sub++ {
		rows = append(rows, retentionRow{
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(sub))] = retentionFakeContent{refc: 1, clen: 100}
	}
	pool.results = rows

	opts := retentionDefaultOpts()
	opts.BatchSize = 1
	opts.Pending = &fakePending{errFrom: 2} // снимок 1 пуст, обновление — ошибка

	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v, want отложенный остаток без ошибки", err)
	}
	if stats.Rows != 25 || !stats.SkippedPending {
		t.Fatalf("stats = %+v, want 25 зафиксированных пачек и SkippedPending", stats)
	}
	// 25-я запись (sub=24) чистилась, 26-я (sub=25) не тронута — проход
	// остановлен до её пачки.
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 25, variant: 0}); r.hash == nil {
		t.Error("запись после точки ошибки очищена — остаток обязан ждать следующего прохода")
	}
}

// TestRetentionOnce_PendingDisabled — nil-источник: защита выключена,
// поведение прохода прежнее (все кандидаты чистятся).
func TestRetentionOnce_PendingDisabled(t *testing.T) {
	now := retentionTestNow()
	pool := retentionSeedPool(now)
	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 2 || len(stats.Deferred) != 0 {
		t.Fatalf("stats = %+v, want прежние 2 строки без Deferred", stats)
	}
}

// mergePending объединяет множества ключей (снимок фейка).
func mergePending(ms ...map[RetentionPendingKey]struct{}) map[RetentionPendingKey]struct{} {
	out := make(map[RetentionPendingKey]struct{})
	for _, m := range ms {
		for k := range m {
			out[k] = struct{}{}
		}
	}
	return out
}
