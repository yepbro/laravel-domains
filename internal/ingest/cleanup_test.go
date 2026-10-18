// Юнит-тесты фоновой очистки удалённых доменов (T-020, ТЗ 5.1, 13.1, 8)
// на фейковом pgx-пуле с транзакциями: точные SQL-тексты и параметры
// (DELETE subdomain_results / UPDATE contents refcount), пачечность,
// агрегация хэшей, повторные проходы, пустой набор, ошибки и отмена ctx,
// цикл CleanupDeleted. Поведение против живой БД — интеграционные тесты
// ingest (T-023).
package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- фейковый пул с транзакциями -----------------------------------------

// cleanupCall — записанный SQL-вызов с аргументами.
type cleanupCall struct {
	sql  string
	args []any
}

// cleanupFakeRows — pgx.Rows с заготовленными строками; Scan позиционный,
// в порядке колонок SQL.
type cleanupFakeRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error // Err() после исчерпания строк
}

func cleanupRows(rows ...[]any) *cleanupFakeRows {
	return &cleanupFakeRows{rows: rows}
}

func (r *cleanupFakeRows) Close() { r.closed = true }

func (r *cleanupFakeRows) Err() error { return r.err }

func (r *cleanupFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *cleanupFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *cleanupFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *cleanupFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("cleanupFakeRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("cleanupFakeRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(row[i]))
	}
	return nil
}

func (r *cleanupFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *cleanupFakeRows) RawValues() [][]byte { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая позиционным Scan.
func (r *cleanupFakeRows) Conn() *pgx.Conn      { return nil }
func (r *cleanupFakeRows) TypeMap() *pgtype.Map { return nil }

// cleanupFakePool — CleanupDB без БД: очереди заготовленных результатов
// SELECT кандидатов и DELETE … RETURNING, ошибки по шагам прохода,
// протокол вызовов. Всё под замком: цикл CleanupDeleted работает в
// горутине, тест ожидает прогресс по счётчикам.
type cleanupFakePool struct {
	mu         sync.Mutex
	selects    []*cleanupFakeRows // очередь результатов SELECT (кончилась — пустой набор)
	deletes    []*cleanupFakeRows // очередь результатов DELETE … RETURNING
	selectErrs []error            // очередь ошибок SELECT (nil — проход без ошибки)
	deleteErr  error
	updateErr  error
	beginErr   error
	commitErr  error

	execs     []pipeExec // результаты Exec по очереди — зачистка осиротевших партий staging (T-098)
	execI     int
	execCalls []cleanupCall // Exec'ы через пул

	queries []cleanupCall // SELECT'ы через пул
	begins  int
	txs     []*cleanupFakeTx
}

func (p *cleanupFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

// Exec обслуживает зачистку осиротевших партий (CleanupStaleBatches,
// T-098): результат — по очереди, конец очереди — DELETE 0 (нечего
// чистить). Вызов записывается до проверки ctx — как Query: после
// cancel() цикл может начать ещё один проход (выбор ветви select
// случаен), и не записанный Exec расшатывал бы соответствие «проходов
// цикла = зачисток» в тестах. Сам CleanupOnce по пулу не Exec'ит —
// это проверяют тесты прохода (execCalls пуст).
func (p *cleanupFakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execCalls = append(p.execCalls, cleanupCall{sql: sql, args: args})
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	i := p.execI
	p.execI++
	if i < len(p.execs) {
		if p.execs[i].err != nil {
			return pgconn.CommandTag{}, p.execs[i].err
		}
		return p.execs[i].tag, nil
	}
	return pgconn.NewCommandTag("DELETE 0"), nil
}

func (p *cleanupFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, cleanupCall{sql: sql, args: args})
	if len(p.selectErrs) > 0 {
		err := p.selectErrs[0]
		p.selectErrs = p.selectErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(p.selects) > 0 {
		rows := p.selects[0]
		p.selects = p.selects[1:]
		return rows, nil
	}
	return cleanupRows(), nil
}

func (p *cleanupFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	tx := &cleanupFakeTx{pool: p}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// stats — снимок счётчиков под замком (ожидание прогресса в тестах цикла).
type cleanupStats struct {
	selects int
	begins  int
	commits int
}

func (p *cleanupFakePool) stats() cleanupStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := cleanupStats{selects: len(p.queries), begins: p.begins}
	for _, tx := range p.txs {
		if tx.committed {
			s.commits++
		}
	}
	return s
}

// cleanupFakeTx — транзакция фейкового пула: помнит вызовы и исход.
type cleanupFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool       *cleanupFakePool
	calls      []cleanupCall
	committed  bool
	rolledBack bool
}

func (t *cleanupFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, cleanupCall{sql: sql, args: args})
	if t.pool.updateErr != nil {
		return pgconn.CommandTag{}, t.pool.updateErr
	}
	// UPDATE с unnest-массивами: число затронутых строк = число хэшей.
	hashes := args[0].([][]byte)
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", len(hashes))), nil
}

func (t *cleanupFakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, cleanupCall{sql: sql, args: args})
	if t.pool.deleteErr != nil {
		return nil, t.pool.deleteErr
	}
	if len(t.pool.deletes) > 0 {
		rows := t.pool.deletes[0]
		t.pool.deletes = t.pool.deletes[1:]
		return rows, nil
	}
	return cleanupRows(), nil
}

func (t *cleanupFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.committed = true
	return nil
}

func (t *cleanupFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if !t.committed {
		t.rolledBack = true
	}
	return nil
}

// compile-time: фейковый пул — CleanupDB для тестов без БД.
var _ CleanupDB = (*cleanupFakePool)(nil)

// --- ожидаемые SQL-тексты (контракт) -------------------------------------

const wantCleanupSelect = "SELECT d.zone_id, d.id FROM domains d " +
	"WHERE d.status = $1 " +
	"AND EXISTS (SELECT 1 FROM subdomain_results r " +
	"WHERE r.zone_id = d.zone_id AND r.domain_id = d.id) " +
	"ORDER BY d.zone_id, d.id LIMIT $2"

const wantCleanupDelete = "DELETE FROM subdomain_results r " +
	"USING unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
	"AND EXISTS (SELECT 1 FROM domains d " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id AND d.status = $3) " +
	"RETURNING r.zone_id, r.domain_id, r.content_hash"

const wantCleanupUpdate = "UPDATE contents SET refcount = GREATEST(refcount - h.n, 0) " +
	"FROM unnest($1::bytea[], $2::int4[]) AS h(hash, n) " +
	"WHERE contents.hash = h.hash"

// wantStaleStageSQL — контракт зачистки осиротевших партий (T-098):
// диапазон по префиксу batch_id (StageTable.Sanitize — «ingest_stage»).
const wantStaleStageSQL = "DELETE FROM \"ingest_stage\" WHERE batch_id < $1"

// cleanupHash16 — 16-октетный content_hash с заданным первым байтом.
func cleanupHash16(b byte) []byte {
	h := make([]byte, contentHashLen)
	h[0] = b
	return h
}

// --- CleanupOnce ---------------------------------------------------------

func TestCleanupOnceEmpty(t *testing.T) {
	pool := &cleanupFakePool{} // очередь SELECT пуста — кандидатов нет

	res, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
	if err != nil {
		t.Fatalf("CleanupOnce: %v", err)
	}
	if res != (CleanupResult{}) {
		t.Errorf("итог = %+v, want нулевой: чистить нечего", res)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != wantCleanupSelect {
		t.Errorf("SELECT = %v, want один запрос кандидатов", pool.queries)
	}
	if pool.begins != 0 {
		t.Errorf("транзакций = %d, want 0: пустой набор не открывает транзакцию", pool.begins)
	}
}

func TestCleanupOnceSQLAndFlow(t *testing.T) {
	hashA := cleanupHash16(0xaa)
	hashB := cleanupHash16(0xbb)
	pool := &cleanupFakePool{
		selects: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10)},
			[]any{int16(1), int64(11)},
			[]any{int16(2), int64(20)},
		)},
		deletes: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10), hashA},
			[]any{int16(1), int64(10), nil}, // результат без контента
			[]any{int16(1), int64(11), hashA},
			[]any{int16(2), int64(20), hashB},
		)},
	}

	res, err := CleanupOnce(context.Background(), pool, CleanupOptions{BatchSize: 7})
	if err != nil {
		t.Fatalf("CleanupOnce: %v", err)
	}

	// Выборка кандидатов: статус deleted и лимит пачки.
	if len(pool.queries) != 1 {
		t.Fatalf("SELECT'ов = %d, want 1", len(pool.queries))
	}
	if pool.queries[0].sql != wantCleanupSelect {
		t.Errorf("SQL выборки:\n%s\nwant:\n%s", pool.queries[0].sql, wantCleanupSelect)
	}
	if want := []any{int16(6), 7}; !reflect.DeepEqual(pool.queries[0].args, want) {
		t.Errorf("аргументы выборки = %v, want %v (status deleted, лимит пачки)", pool.queries[0].args, want)
	}

	// Транзакция: удаление результатов + уменьшение refcount.
	if pool.begins != 1 || len(pool.txs) != 1 {
		t.Fatalf("транзакций = %d, want 1", pool.begins)
	}
	tx := pool.txs[0]
	if len(tx.calls) != 2 {
		t.Fatalf("вызовов в транзакции = %d, want 2 (DELETE + UPDATE)", len(tx.calls))
	}
	if tx.calls[0].sql != wantCleanupDelete {
		t.Errorf("SQL удаления:\n%s\nwant:\n%s", tx.calls[0].sql, wantCleanupDelete)
	}
	if want := []any{[]int16{1, 1, 2}, []int64{10, 11, 20}, int16(6)}; !reflect.DeepEqual(tx.calls[0].args, want) {
		t.Errorf("аргументы удаления = %v, want %v", tx.calls[0].args, want)
	}
	if tx.calls[1].sql != wantCleanupUpdate {
		t.Errorf("SQL refcount:\n%s\nwant:\n%s", tx.calls[1].sql, wantCleanupUpdate)
	}
	// Хэши агрегированы по числу удалённых ссылок, NULL пропущен,
	// порядок детерминированный.
	if want := []any{[][]byte{hashA, hashB}, []int32{2, 1}}; !reflect.DeepEqual(tx.calls[1].args, want) {
		t.Errorf("аргументы refcount = %v, want %v", tx.calls[1].args, want)
	}

	want := CleanupResult{Domains: 3, Results: 4, Contents: 2}
	if res != want {
		t.Errorf("итог = %+v, want %+v", res, want)
	}
	if !tx.committed || tx.rolledBack {
		t.Errorf("транзакция: committed=%v rolledBack=%v, want фиксация без отката", tx.committed, tx.rolledBack)
	}
	// Проход очистки доменов не пишет в пул напрямую: Exec — только
	// канал зачистки осиротевших партий (T-098).
	if len(pool.execCalls) != 0 {
		t.Errorf("Exec'ы через пул = %v, want нет: CleanupOnce работает в транзакции", pool.execCalls)
	}
}

// TestCleanupOnceDefaultBatch — нулевой размер пачки заменяется дефолтом.
func TestCleanupOnceDefaultBatch(t *testing.T) {
	pool := &cleanupFakePool{}

	if _, err := CleanupOnce(context.Background(), pool, CleanupOptions{}); err != nil {
		t.Fatalf("CleanupOnce: %v", err)
	}
	got := pool.queries[0].args[1]
	if got != DefaultCleanupBatch {
		t.Errorf("лимит пачки = %v, want дефолт %d", got, DefaultCleanupBatch)
	}
}

// TestCleanupOnceRepeatPasses — вычищенный домен не выбирается: повторный
// проход видит пустой набор и не открывает транзакцию.
func TestCleanupOnceRepeatPasses(t *testing.T) {
	pool := &cleanupFakePool{
		selects: []*cleanupFakeRows{cleanupRows([]any{int16(3), int64(30)})},
		deletes: []*cleanupFakeRows{cleanupRows([]any{int16(3), int64(30), nil})},
	}

	first, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
	if err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	if first.Domains != 1 || first.Results != 1 {
		t.Fatalf("первый проход = %+v, want 1 домен и 1 результат", first)
	}

	second, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
	if err != nil {
		t.Fatalf("второй проход: %v", err)
	}
	if second != (CleanupResult{}) {
		t.Errorf("второй проход = %+v, want нулевой", second)
	}
	if len(pool.queries) != 2 {
		t.Errorf("SELECT'ов = %d, want 2 (по одному на проход)", len(pool.queries))
	}
	if pool.begins != 1 {
		t.Errorf("транзакций = %d, want 1: повторный проход не открывает", pool.begins)
	}
}

// TestCleanupOnceBadHash — content_hash не 16 октетов (контракт contents,
// ТЗ 6.1) не роняет проход (poison pill, ING-P2-4): строка уже удалена,
// refcount по ней не меняется, транзакция фиксируется, аномалия видна в
// CleanupResult.BadHashes.
func TestCleanupOnceBadHash(t *testing.T) {
	t.Run("мусорный хэш рядом с корректным", func(t *testing.T) {
		pool := &cleanupFakePool{
			selects: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10)})},
			deletes: []*cleanupFakeRows{cleanupRows(
				[]any{int16(1), int64(10), make([]byte, 15)},
				[]any{int16(1), int64(10), cleanupHash16(0xaa)},
			)},
		}

		res, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
		if err != nil {
			t.Fatalf("CleanupOnce: %v, want проход без ошибки", err)
		}
		if res.BadHashes != 1 || res.Domains != 1 || res.Results != 2 || res.Contents != 1 {
			t.Errorf("итог = %+v, want bad_hashes 1, domains 1, results 2, contents 1", res)
		}
		tx := pool.txs[0]
		if len(tx.calls) != 2 {
			t.Fatalf("вызовов в транзакции %d, want 2 (DELETE + UPDATE по корректному хэшу)", len(tx.calls))
		}
		if want := []any{[][]byte{cleanupHash16(0xaa)}, []int32{1}}; !reflect.DeepEqual(tx.calls[1].args, want) {
			t.Errorf("аргументы refcount = %v, want только корректный хэш", tx.calls[1].args)
		}
		if !tx.committed || tx.rolledBack {
			t.Errorf("транзакция: committed=%v rolledBack=%v, want фиксация без отката", tx.committed, tx.rolledBack)
		}
	})
	t.Run("все хэши некорректны — проход продолжается", func(t *testing.T) {
		pool := &cleanupFakePool{
			selects: []*cleanupFakeRows{cleanupRows(
				[]any{int16(1), int64(10)},
				[]any{int16(2), int64(20)},
			)},
			deletes: []*cleanupFakeRows{cleanupRows(
				[]any{int16(1), int64(10), make([]byte, 3)},
				[]any{int16(2), int64(20), nil},
			)},
		}

		res, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
		if err != nil {
			t.Fatalf("CleanupOnce: %v, want проход без ошибки", err)
		}
		if res.BadHashes != 1 || res.Domains != 2 || res.Contents != 0 {
			t.Errorf("итог = %+v, want bad_hashes 1, domains 2, contents 0", res)
		}
		// Ссылок для refcount нет — UPDATE не выполняется, но проход
		// фиксируется: домены вычищены, следующие проходы их не выберут.
		tx := pool.txs[0]
		if len(tx.calls) != 1 {
			t.Fatalf("вызовов в транзакции %d, want 1 (только DELETE)", len(tx.calls))
		}
		if !tx.committed {
			t.Error("транзакция не зафиксирована — кандидаты выберутся снова")
		}
	})
}

func TestCleanupOnceErrors(t *testing.T) {
	newPool := func() *cleanupFakePool {
		return &cleanupFakePool{
			selects: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10)})},
			deletes: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10), cleanupHash16(1)})},
		}
	}

	tests := []struct {
		name    string
		pool    *cleanupFakePool
		context string // ожидаемый фрагмент сообщения
	}{
		{"ошибка SELECT", &cleanupFakePool{selectErrs: []error{errors.New("бд недоступна")}}, "выборка кандидатов"},
		{"ошибка Begin", func() *cleanupFakePool {
			p := newPool()
			p.beginErr = errors.New("бд недоступна")
			return p
		}(), "транзакция"},
		{"ошибка DELETE", func() *cleanupFakePool {
			p := newPool()
			p.deleteErr = errors.New("бд недоступна")
			return p
		}(), "удаление результатов"},
		{"ошибка UPDATE", func() *cleanupFakePool {
			p := newPool()
			p.updateErr = errors.New("бд недоступна")
			return p
		}(), "refcount"},
		{"ошибка Commit", func() *cleanupFakePool {
			p := newPool()
			p.commitErr = errors.New("бд недоступна")
			return p
		}(), "фиксация"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CleanupOnce(context.Background(), tt.pool, CleanupOptions{})
			if err == nil {
				t.Fatal("ожидалась ошибка")
			}
			if !strings.Contains(err.Error(), "ingest: очистка удалённых доменов") ||
				!strings.Contains(err.Error(), tt.context) ||
				!strings.Contains(err.Error(), "бд недоступна") {
				t.Errorf("сообщение = %q, want контекст %q и исходная ошибка", err, tt.context)
			}
			for _, tx := range tt.pool.txs {
				if tx.committed {
					t.Errorf("транзакция закоммичена при ошибке %q", tt.name)
				}
			}
		})
	}
}

// TestCleanupOnceCtxCanceled — отмена контекста проходит через оба шага
// прохода как ошибка (errors.Is), транзакция не фиксируется.
func TestCleanupOnceCtxCanceled(t *testing.T) {
	selectPool := &cleanupFakePool{selectErrs: []error{context.Canceled}}
	if _, err := CleanupOnce(context.Background(), selectPool, CleanupOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка выборки = %v, want context.Canceled", err)
	}

	deletePool := &cleanupFakePool{
		selects: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10)})},
		deletes: []*cleanupFakeRows{cleanupRows()},
	}
	deletePool.deleteErr = context.Canceled
	if _, err := CleanupOnce(context.Background(), deletePool, CleanupOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка удаления = %v, want context.Canceled", err)
	}
	if deletePool.txs[0].committed {
		t.Error("транзакция закоммичена при отмене контекста")
	}
}

func TestCleanupOnceNilPool(t *testing.T) {
	if _, err := CleanupOnce(context.Background(), nil, CleanupOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// --- CleanupStaleBatches (T-098) -----------------------------------------

// TestCleanupStaleBatchesSQLAndArg — контракт зачистки: SQL-диапазон по
// префиксу batch_id и точная граница «now − StaleBatchAge» (фиксированное
// now делает аргумент детерминированным). Семантика «старая партия
// удаляется, свежая живёт» диапазона против живой БД — интеграционные
// тесты ingest; здесь — кодировка границы (TestBatchIDAgeOrdering).
func TestCleanupStaleBatchesSQLAndArg(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	pool := &cleanupFakePool{execs: []pipeExec{{tag: pgconn.NewCommandTag("DELETE 7")}}}

	rows, err := CleanupStaleBatches(context.Background(), pool, now)
	if err != nil {
		t.Fatalf("CleanupStaleBatches: %v", err)
	}
	if rows != 7 {
		t.Errorf("удалено строк = %d, want 7 (из CommandTag)", rows)
	}
	if len(pool.execCalls) != 1 {
		t.Fatalf("Exec'ов = %d, want 1", len(pool.execCalls))
	}
	call := pool.execCalls[0]
	if call.sql != wantStaleStageSQL {
		t.Errorf("SQL зачистки:\n%s\nwant:\n%s", call.sql, wantStaleStageSQL)
	}
	wantArg := batchIDBefore(now.Add(-StaleBatchAge))
	if !reflect.DeepEqual(call.args, []any{wantArg[:]}) {
		t.Errorf("граница зачистки = %x, want %x (batch_idBefore(now − %s))",
			call.args[0], wantArg[:], StaleBatchAge)
	}
}

// TestCleanupStaleBatchesNoop — пустой staging: DELETE 0 без ошибки,
// лишних обращений нет.
func TestCleanupStaleBatchesNoop(t *testing.T) {
	pool := &cleanupFakePool{} // очередь Exec пуста — DELETE 0

	rows, err := CleanupStaleBatches(context.Background(), pool, time.Now())
	if err != nil {
		t.Fatalf("CleanupStaleBatches: %v", err)
	}
	if rows != 0 {
		t.Errorf("удалено строк = %d, want 0: чистить нечего", rows)
	}
	if len(pool.execCalls) != 1 || pool.begins != 0 {
		t.Errorf("обращений = %v (транзакций %d), want один Exec без транзакции",
			pool.execCalls, pool.begins)
	}
}

func TestCleanupStaleBatchesErrors(t *testing.T) {
	if _, err := CleanupStaleBatches(context.Background(), nil, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}

	boom := &cleanupFakePool{execs: []pipeExec{{err: errors.New("бд недоступна")}}}
	if _, err := CleanupStaleBatches(context.Background(), boom, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "ingest: зачистка осиротевших партий") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка = %v, want контекст зачистки и исходная ошибка", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := &cleanupFakePool{}
	if _, err := CleanupStaleBatches(ctx, canceled, time.Now()); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}

	// Граница возраста до 1970 года — отказ до обращения к БД: uint64-переход
	// границы делал бы зачистку срезающей весь staging (P1 раунда 3:
	// проверяется именно now−StaleBatchAge, а не now — часы ровно на
	// эпохе Unix дают отрицательную границу при положительном now).
	preEpoch := &cleanupFakePool{}
	if _, err := CleanupStaleBatches(context.Background(), preEpoch, time.Unix(0, 0)); err == nil ||
		!strings.Contains(err.Error(), "до 1970 года") {
		t.Errorf("ошибка = %v, want отказ «граница до 1970 года» для эпохи Unix", err)
	}
	if _, err := CleanupStaleBatches(context.Background(), preEpoch, time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC)); err == nil ||
		!strings.Contains(err.Error(), "до 1970 года") {
		t.Errorf("ошибка = %v, want отказ «граница до 1970 года»", err)
	}
	if len(preEpoch.execCalls) != 0 {
		t.Errorf("Exec'ы = %v, want нет: отказ до обращения к БД", preEpoch.execCalls)
	}

	// Сутки спустя после эпохи граница положительна — зачистка работает
	// даже с «сдвинутыми» часами (контракт не требует 2026 года).
	epochPlus := &cleanupFakePool{execs: []pipeExec{{tag: pgconn.NewCommandTag("DELETE 2")}}}
	now := time.Unix(2*86400+3600, 0) // 1970-01-03 01:00 UTC
	rows, err := CleanupStaleBatches(context.Background(), epochPlus, now)
	if err != nil {
		t.Fatalf("CleanupStaleBatches(эпоха+2 суток): %v", err)
	}
	if rows != 2 {
		t.Errorf("удалено строк = %d, want 2", rows)
	}
	wantArg := batchIDBefore(now.Add(-StaleBatchAge))
	if want := []any{wantArg[:]}; !reflect.DeepEqual(epochPlus.execCalls[0].args, want) {
		t.Errorf("граница = %x, want %x", epochPlus.execCalls[0].args[0], want[0])
	}
}

// --- CleanupOptions ------------------------------------------------------

func TestCleanupOptionsDefaults(t *testing.T) {
	got := CleanupOptions{}.withDefaults()
	want := CleanupOptions{BatchSize: DefaultCleanupBatch, Interval: DefaultCleanupInterval}
	if got != want {
		t.Errorf("withDefaults = %+v, want %+v", got, want)
	}

	set := CleanupOptions{BatchSize: 5, Interval: time.Second}.withDefaults()
	if set.BatchSize != 5 || set.Interval != time.Second {
		t.Errorf("withDefaults затирает заданные значения: %+v", set)
	}

	neg := CleanupOptions{BatchSize: -1, Interval: -time.Second}.withDefaults()
	if neg != want {
		t.Errorf("withDefaults(отрицательные) = %+v, want дефолты %+v", neg, want)
	}
}

// --- CleanupDeleted ------------------------------------------------------

// TestCleanupDeletedLoop — первый проход сразу, ошибки прохода не
// останавливают цикл, отмена ctx возвращает nil (контракт OnRun, T-012).
// Теми же проходами идёт зачистка осиротевших партий staging (T-098):
// первый проход удаляет строки (Warn с числом), второй — ошибка БД
// (Warn, цикл жив), дальше — DELETE 0 без записей.
func TestCleanupDeletedLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := newCleanupTestLogger(&buf)

	pool := &cleanupFakePool{
		// Проход 1 (сразу после старта) — работа; проход 2 — ошибка БД;
		// дальше — пустые наборы.
		selects: []*cleanupFakeRows{
			cleanupRows([]any{int16(1), int64(10)}),
			cleanupRows(),
		},
		selectErrs: []error{nil, errors.New("бд недоступна")},
		deletes: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10), cleanupHash16(0xaa)},
			[]any{int16(1), int64(10), make([]byte, 9)}, // poison pill: Warn, проход жив
		)},
		// Зачистка staging: проход 1 — 5 осиротевших строк, проход 2 —
		// ошибка БД, дальше — DELETE 0 (очередь кончилась).
		execs: []pipeExec{
			{tag: pgconn.NewCommandTag("DELETE 5")},
			{err: errors.New("бд недоступна")},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CleanupDeleted(ctx, pool, CleanupOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Ждём трёх проходов (сразу + минимум два тика) и останавливаем.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats().selects < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до третьего прохода: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("CleanupDeleted: %v, want nil при отмене ctx", err)
	}

	st := pool.stats()
	if st.begins != 1 || st.commits != 1 {
		t.Errorf("транзакций %d, коммитов %d, want 1 и 1: ошибочный и пустые проходы не открывают", st.begins, st.commits)
	}
	// Каждый проход — одна зачистка staging, ошибки её не прерывают.
	pool.mu.Lock()
	sweeps := len(pool.execCalls)
	pool.mu.Unlock()
	if sweeps != st.selects {
		t.Errorf("зачисток staging = %d, want = проходам цикла (%d)", sweeps, st.selects)
	}

	log := buf.String()
	for _, want := range []string{
		"фоновая очистка удалённых доменов", // старт
		"проход очистки удалённых доменов",  // итог первого прохода
		"domains=1",                 // счётчики прохода
		"некорректным content_hash", // poison pill — Warn (ING-P2-4)
		"бд недоступна",             // ошибка второго прохода
		"удалены осиротевшие строки staging", // зачистка: Warn с числом строк (T-098)
		"rows=5",                               // счётчик зачистки
		"older_than=" + StaleBatchAge.String(), // порог возраста
		"зачистка осиротевших партий staging", // ошибка зачистки — Warn, цикл жив
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

func TestCleanupDeletedNilPool(t *testing.T) {
	err := CleanupDeleted(context.Background(), nil, CleanupOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// newCleanupTestLogger — текстовый логгер в буфер (проверка записей цикла).
func newCleanupTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}
