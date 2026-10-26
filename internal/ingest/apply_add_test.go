// Юнит-тесты применения партии add (T-018, ТЗ 5.1; чанкинг T-205) без БД
// (стиль stage_test.go / cleanup_test.go): точные SQL-тексты и параметры,
// движение курсора пачками по PK staging (конфликтная пачка тоже
// закрывается), накопление дельт зон и один итоговый UPDATE, транзакция
// (Begin/Commit, откат при сбое шага), счётчики, границы (партия меньше
// пачки, пустая партия), повторное применение (идемпотентность, ТЗ 3.4),
// ошибки шагов, инвариант счётчиков, отмена ctx. Реальное применение —
// интеграционные тесты ingest (T-023).
package ingest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"crawler/internal/idn"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// --- фейковый пул с транзакциями ------------------------------------------

// addStageRow — строка партии в модели staging фейка.
type addStageRow struct {
	lineNo int64
	zoneID int16
}

// applyAddPool — ApplyDB для ApplyAdd без БД: моделирует строки партии в
// staging (staged) и отвечает на границу пачки (addChunkBoundSQL) и
// RETURNING вставки (insertAddSQL) по диапазону курсора — конфликтами
// считаются строки с line_no >= conflictFrom (0 — конфликтов нет, всё
// вставляется); revive/count — заготовленные числа. Ошибки шагов — по
// именованным полям; протокол ops фиксирует порядок применения.
// Как и реальный pgx, отказывается работать с отменённым контекстом.
type applyAddPool struct {
	ops  []string // порядок: "begin", "commit", "query:<sql>", "exec:<sql>"
	args [][]any  // аргументы операций в порядке вызовов

	staged       []addStageRow // строки партии (модель staging, порядок line_no)
	conflictFrom int64         // line_no, с которого строки уже существуют (конфликт)

	revived int64 // ответ reviveAddSQL
	uniques int64 // ответ countAddSQL

	beginErr     error
	boundErr     error
	insertErr    error
	zoneDeltaErr error
	reviveErr    error
	countErr     error
	clearErr     error
	commitErr    error

	begins     int
	commits    int
	rollbacks  int
	txArgs     [][]any // аргументы операций транзакции (zoneDelta/clear)
	zoneCall   int     // индекс аргументов итогового UPDATE зон (последний вызов)
	zoneCalled bool
}

var _ ApplyDB = (*applyAddPool)(nil)

func (p *applyAddPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("applyAddPool: CopyFrom не должен вызываться")
}

// Exec и Query пула применению не нужны (всё — в транзакции): отказ, если
// что-то вызвано мимо Begin.
func (p *applyAddPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("applyAddPool: Exec вне транзакции не должен вызываться")
}

func (p *applyAddPool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("applyAddPool: Query вне транзакции не должен вызываться")
}

// stageRange — строки партии модели staging с line_no в (after, to];
// to >= 1<<62 — без верхней границы (граница пачки).
func (p *applyAddPool) stageRange(after, to int64) []addStageRow {
	var res []addStageRow
	for _, r := range p.staged {
		if r.lineNo > after && r.lineNo <= to {
			res = append(res, r)
		}
	}
	return res
}

func (p *applyAddPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	p.begins++
	p.ops = append(p.ops, "begin")
	return &applyAddTx{pool: p}, nil
}

// applyAddTx — транзакция фейкового пула: все операции идут через пул
// (протокол общий), ошибки — по именованным полям пула.
type applyAddTx struct {
	pgx.Tx // остальные методы pgx.Tx применение не использует

	pool      *applyAddPool
	committed bool
}

func (t *applyAddTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := t.pool
	p.ops = append(p.ops, "query:"+sql)
	p.args = append(p.args, args)
	switch sql {
	case addChunkBoundSQL:
		if p.boundErr != nil {
			return nil, p.boundErr
		}
		after, limit := args[2].(int64), args[3].(int)
		var bound int64
		rows := p.stageRange(after, 1<<62)
		if n := len(rows); n > limit {
			rows = rows[:limit]
		}
		if len(rows) > 0 {
			bound = rows[len(rows)-1].lineNo
		}
		return cleanupRows([]any{bound}), nil
	case insertAddSQL:
		if p.insertErr != nil {
			return nil, p.insertErr
		}
		from, to := args[2].(int64), args[3].(int64)
		out := cleanupRows()
		for _, r := range p.stageRange(from, to) {
			if p.conflictFrom > 0 && r.lineNo >= p.conflictFrom {
				continue // существующий домен: ON CONFLICT DO NOTHING
			}
			out.rows = append(out.rows, []any{r.zoneID})
		}
		return out, nil
	case reviveAddSQL:
		if p.reviveErr != nil {
			return nil, p.reviveErr
		}
		return cleanupRows([]any{p.revived}), nil
	case countAddSQL:
		if p.countErr != nil {
			return nil, p.countErr
		}
		return cleanupRows([]any{p.uniques}), nil
	}
	return nil, errors.New("applyAddPool: неожидаемый запрос: " + sql)
}

func (t *applyAddTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	p := t.pool
	p.ops = append(p.ops, "exec:"+sql)
	p.txArgs = append(p.txArgs, args)
	switch sql {
	case zoneDeltaAddSQL:
		p.zoneCalled = true
		p.zoneCall = len(p.txArgs) - 1
		if p.zoneDeltaErr != nil {
			return pgconn.CommandTag{}, p.zoneDeltaErr
		}
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case clearStageSQL:
		if p.clearErr != nil {
			return pgconn.CommandTag{}, p.clearErr
		}
		return pgconn.NewCommandTag("DELETE 0"), nil
	}
	return pgconn.CommandTag{}, errors.New("applyAddPool: неожиданный Exec: " + sql)
}

func (t *applyAddTx) Commit(context.Context) error {
	p := t.pool
	if p.commitErr != nil {
		return p.commitErr
	}
	p.commits++
	t.committed = true
	p.ops = append(p.ops, "commit")
	return nil
}

func (t *applyAddTx) Rollback(context.Context) error {
	// После Commit откат — no-op (как ErrTxClosed реального pgx).
	if !t.committed {
		t.pool.rollbacks++
	}
	return nil
}

// stageLines — модель партии: строки 1..n в одной зоне.
func stageLines(n int, zone int16) []addStageRow {
	rows := make([]addStageRow, n)
	for i := range rows {
		rows[i] = addStageRow{lineNo: int64(i + 1), zoneID: zone}
	}
	return rows
}

// applyAddOps — ожидаемый порядок операций применения с вставками:
// транзакция → пачки (граница, вставка) до исчерпания → дельты зон →
// возврат → подсчёт → очистка → фиксация.
func applyAddOps(chunks int) []string {
	ops := []string{"begin"}
	for i := 0; i < chunks; i++ {
		ops = append(ops, "query:"+addChunkBoundSQL, "query:"+insertAddSQL)
	}
	ops = append(ops,
		"query:"+addChunkBoundSQL, // партия исчерпана
		"exec:"+zoneDeltaAddSQL,
		"query:"+reviveAddSQL,
		"query:"+countAddSQL,
		"exec:"+clearStageSQL,
		"commit",
	)
	return ops
}

// ---------------------------------------------------------------- сценарии

func TestApplyAdd(t *testing.T) {
	b := validBatch(t) // зона 7, add — как после EnsureZone и Stage
	pool := &applyAddPool{
		staged:       stageLines(10, 7), // партия одной пачкой (10 < 50 000)
		conflictFrom: 6,                 // строки 6–10 уже существуют
		revived:      3,
		uniques:      10,
	}

	res, err := ApplyAdd(context.Background(), pool, b, DefaultBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 5 || res.Revived != 3 || res.Existing != 2 {
		t.Errorf("результат = %+v, want Inserted 5, Revived 3, Existing 2 (уникальные 10)", res)
	}
	if res.Total() != 10 {
		t.Errorf("Total() = %d, want 10", res.Total())
	}

	// Порядок: транзакция → граница/вставка пачки → граница (пусто) →
	// дельты зон → возврат → подсчёт → очистка → фиксация.
	if got, want := strings.Join(pool.ops, "\n"), strings.Join(applyAddOps(1), "\n"); got != want {
		t.Errorf("порядок операций:\n%s\nwant:\n%s", got, want)
	}
	if pool.begins != 1 || pool.commits != 1 || pool.rollbacks != 0 {
		t.Errorf("транзакций: begin %d, commit %d, rollback %d, want 1/1/0", pool.begins, pool.commits, pool.rollbacks)
	}

	// Пачка адресует свою партию и диапазон курсора от нуля до границы.
	wantArgs := []any{[]byte(b.ID[:]), int16(model.ActionAdd), int64(0), int64(10)}
	if !reflect.DeepEqual(pool.args[1], wantArgs) {
		t.Errorf("аргументы вставки = %v, want [batch_id, action=%d, line_no > 0, line_no <= 10]", pool.args[1], int16(model.ActionAdd))
	}
	// Дельты зон — один итоговый оператор по накопленному zone_id→n.
	if !pool.zoneCalled {
		t.Fatal("итоговый UPDATE дельт зон не выполнен")
	}
	if !reflect.DeepEqual(pool.txArgs[pool.zoneCall], []any{[]int16{7}, []int64{5}}) {
		t.Errorf("аргументы дельт зон = %v, want зоны [7] × 5", pool.txArgs[pool.zoneCall])
	}
}

// TestApplyAdd_Chunks — партия больше пачки (T-205): 12 строк пачками по
// 5 — диапазоны (0,5], (5,10], (10,12]; Inserted — сумма вставок пачек,
// дельты зон — один оператор по накоплениям всех пачек.
func TestApplyAdd_Chunks(t *testing.T) {
	b := validBatch(t)
	// Две зоны: строки 1–6 в зоне 7, 7–12 в зоне 9.
	staged := stageLines(6, 7)
	staged = append(staged, stageLines(6, 9)...)
	for i := 6; i < len(staged); i++ {
		staged[i].lineNo = int64(i + 1)
	}
	pool := &applyAddPool{staged: staged, uniques: 12}

	res, err := ApplyAdd(context.Background(), pool, b, 5)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 12 || res.Revived != 0 || res.Existing != 0 {
		t.Errorf("результат = %+v, want Inserted 12 (сумма пачек), Revived/Existing 0", res)
	}
	if got, want := strings.Join(pool.ops, "\n"), strings.Join(applyAddOps(3), "\n"); got != want {
		t.Errorf("порядок операций:\n%s\nwant:\n%s", got, want)
	}

	// Диапазоны пачек продолжают друг друга по PK staging.
	wantRanges := [][2]int64{{0, 5}, {5, 10}, {10, 12}}
	for i, want := range wantRanges {
		args := pool.args[1+2*i] // вставки чередуются с границами
		got := [2]int64{args[2].(int64), args[3].(int64)}
		if got != want {
			t.Errorf("пачка %d: диапазон (курсор, граница] = %v, want %v", i+1, got, want)
		}
	}

	// Дельты двух зон накоплены всеми пачками и применены одним
	// оператором: зона 7 × 6, зона 9 × 6, порядок zone_id детерминирован.
	if !pool.zoneCalled {
		t.Fatal("итоговый UPDATE дельт зон не выполнен")
	}
	if !reflect.DeepEqual(pool.txArgs[pool.zoneCall], []any{[]int16{7, 9}, []int64{6, 6}}) {
		t.Errorf("аргументы дельт зон = %v, want зоны [7 9] × [6 6]", pool.txArgs[pool.zoneCall])
	}
}

// TestApplyAdd_ConflictChunkAdvances — пачка из одних конфликтов
// (ON CONFLICT DO NOTHING пропустил всё) обязана закрывать курсор по
// диапазону staging, а не по вставленным строкам — иначе применение
// партии из сплошных существующих доменов зациклилось бы (T-205).
func TestApplyAdd_ConflictChunkAdvances(t *testing.T) {
	b := validBatch(t)
	pool := &applyAddPool{
		staged:       stageLines(8, 7),
		conflictFrom: 1, // все строки партии уже существуют
		uniques:      8,
	}

	res, err := ApplyAdd(context.Background(), pool, b, 3)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 0 || res.Revived != 0 || res.Existing != 8 {
		t.Errorf("результат = %+v, want конфликтная партия: Inserted/Revived 0, Existing 8", res)
	}
	// Три пачки по диапазону (0,3], (3,6], (6,8] — курсор дошёл до конца;
	// вставок нет — дельты зон не применяются вовсе.
	want := append([]string{"begin"},
		"query:"+addChunkBoundSQL, "query:"+insertAddSQL,
		"query:"+addChunkBoundSQL, "query:"+insertAddSQL,
		"query:"+addChunkBoundSQL, "query:"+insertAddSQL,
		"query:"+addChunkBoundSQL, // партия исчерпана
		"query:"+reviveAddSQL,
		"query:"+countAddSQL,
		"exec:"+clearStageSQL,
		"commit",
	)
	if got := strings.Join(pool.ops, "\n"); got != strings.Join(want, "\n") {
		t.Errorf("порядок операций:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
	if pool.zoneCalled {
		t.Error("итоговый UPDATE дельт зон выполнен без вставок, want no-op")
	}
}

// TestApplyAdd_EmptyBatch — пустая партия (или уже применённая, staging
// пуст): данные не меняются, результат нулевой; транзакция открывается,
// вставки нет, дельты зон не применяются.
func TestApplyAdd_EmptyBatch(t *testing.T) {
	pool := &applyAddPool{}

	res, err := ApplyAdd(context.Background(), pool, validBatch(t), DefaultBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	if res != (AddResult{}) {
		t.Errorf("результат = %+v, want нулевой", res)
	}
	want := []string{
		"begin",
		"query:" + addChunkBoundSQL, // партия исчерпана сразу
		"query:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
		"commit",
	}
	if got := strings.Join(pool.ops, "\n"); got != strings.Join(want, "\n") {
		t.Errorf("порядок операций:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
	if pool.zoneCalled {
		t.Error("пустая партия не трогает счётчики зон")
	}
}

// TestApplyAdd_Reapply — повторное применение той же партии (ТЗ 3.4):
// после первой попытки все домены существуют — повторная загрузка тех же
// строк конфликтует, вставок и дельт нет, данные сходятся к тому же
// итогу (счётчики повторного прогона отражают текущее состояние).
func TestApplyAdd_Reapply(t *testing.T) {
	b := validBatch(t)
	pool := &applyAddPool{staged: stageLines(10, 7), uniques: 10}

	first, err := ApplyAdd(context.Background(), pool, b, 4)
	if err != nil {
		t.Fatal(err)
	}
	if first.Inserted != 10 || first.Existing != 0 {
		t.Fatalf("первое применение = %+v, want Inserted 10", first)
	}

	// Партия загружена заново тем же batch_id (Stage затирает свою
	// партию), домены уже в БД — сплошные конфликты.
	pool.staged = stageLines(10, 7)
	pool.conflictFrom = 1
	second, err := ApplyAdd(context.Background(), pool, b, 4)
	if err != nil {
		t.Fatal(err)
	}
	if second.Inserted != 0 || second.Revived != 0 || second.Existing != 10 {
		t.Errorf("повторное применение = %+v, want no-op: Inserted/Revived 0, Existing 10", second)
	}
}

func TestApplyAdd_Validation(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		batch     Batch
		batchSize int
	}{
		{"нулевой идентификатор", Batch{ZoneID: 1, Action: model.ActionAdd}, DefaultBatchSize},
		{"партия delete", Batch{ID: id, ZoneID: 1, Action: model.ActionDelete}, DefaultBatchSize},
		{"неизвестная операция", Batch{ID: id, ZoneID: 1, Action: model.Action(3)}, DefaultBatchSize},
		{"нулевой размер пачки", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyAddPool{}
			if _, err := ApplyAdd(context.Background(), pool, tc.batch, tc.batchSize); err == nil {
				t.Fatal("ожидалась ошибка валидации")
			}
			if len(pool.ops) != 0 || pool.begins != 0 {
				t.Errorf("невалидная партия дошла до БД: %v", pool.ops)
			}
		})
	}
}

// TestApplyAdd_StepErrors — сбой каждого шага возвращает обёрнутую
// ошибку, транзакция откатывается (строки остаются в staging, повторное
// применение безопасно, ТЗ 3.4), очистка и фиксация не выполняются.
func TestApplyAdd_StepErrors(t *testing.T) {
	boom := errors.New("соединение потеряно")
	cases := []struct {
		name    string
		set     func(*applyAddPool)
		wantOps int    // обращений до сбоя включительно
		want    string // фрагмент ожидаемой ошибки
	}{
		{"сбой транзакции", func(p *applyAddPool) { p.beginErr = boom }, 0, "транзакция"},
		{"сбой границы пачки", func(p *applyAddPool) { p.boundErr = boom }, 2, "граница пачки"},
		{"сбой вставки", func(p *applyAddPool) { p.insertErr = boom }, 3, "вставка новых доменов"},
		{"сбой дельт зон", func(p *applyAddPool) { p.zoneDeltaErr = boom }, 5, "дельты счётчиков зон"},
		{"сбой возврата", func(p *applyAddPool) { p.reviveErr = boom }, 6, "возврат удалённых"},
		{"сбой подсчёта", func(p *applyAddPool) { p.countErr = boom }, 7, "подсчёт доменов"},
		{"сбой очистки", func(p *applyAddPool) { p.clearErr = boom }, 8, "очистка партии"},
		{"сбой фиксации", func(p *applyAddPool) { p.commitErr = boom }, 8, "фиксация"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyAddPool{staged: stageLines(10, 7), uniques: 10}
			tc.set(pool)

			_, err := ApplyAdd(context.Background(), pool, validBatch(t), DefaultBatchSize)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !errors.Is(err, boom) {
				t.Fatalf("err = %v, want обёрнутая ошибка с %q", err, tc.want)
			}
			if len(pool.ops) != tc.wantOps {
				t.Errorf("обращений к БД %d (%v), want %d — после сбоя продолжения нет", len(pool.ops), pool.ops, tc.wantOps)
			}
			if pool.commits != 0 {
				t.Error("сбойный шаг зафиксировал транзакцию")
			}
			if pool.begins > 0 && pool.rollbacks != 1 {
				t.Errorf("откатов %d, want 1 (открытая транзакция откатывается)", pool.rollbacks)
			}
			if strings.Contains(strings.Join(pool.ops, "\n"), "commit") {
				t.Error("сбой применения завершился фиксацией")
			}
		})
	}
}

// TestApplyAdd_CountInvariant — подсчёт меньше вставленного и
// возвращённого: счётчики расходятся, применение признаётся ошибочным,
// транзакция откатывается.
func TestApplyAdd_CountInvariant(t *testing.T) {
	pool := &applyAddPool{
		staged:  stageLines(5, 7),
		revived: 3,
		uniques: 1, // 1 < 5 + 3
	}

	_, err := ApplyAdd(context.Background(), pool, validBatch(t), DefaultBatchSize)
	if err == nil || !strings.Contains(err.Error(), "счётчики расходятся") {
		t.Fatalf("err = %v, want инвариант счётчиков", err)
	}
	if pool.commits != 0 || pool.rollbacks != 1 {
		t.Errorf("транзакция: commit %d, rollback %d, want 0/1", pool.commits, pool.rollbacks)
	}
}

// TestApplyAdd_ContextCanceled — отменённый контекст отказывает на
// открытии транзакции (как реальный pgx), партия не применяется.
func TestApplyAdd_ContextCanceled(t *testing.T) {
	pool := &applyAddPool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ApplyAdd(ctx, pool, validBatch(t), DefaultBatchSize)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(pool.ops) != 0 {
		t.Errorf("обращений к БД быть не должно, got %v", pool.ops)
	}
}

// ---------------------------------------------------------------- контракт SQL

const (
	wantAddChunkBoundSQL = "SELECT COALESCE(max(t.line_no), 0) FROM (" +
		"SELECT s.line_no FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 " +
		"ORDER BY s.line_no LIMIT $4) AS t"

	wantAddInsertSQL = "INSERT INTO domains (zone_id, label, flags) " +
		"SELECT s.zone_id, s.label, CASE WHEN s.label ILIKE 'xn--%' OR s.label ILIKE '%.xn--%' THEN 1 ELSE 0 END " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 AND s.line_no <= $4 " +
		"ON CONFLICT DO NOTHING RETURNING zone_id"

	wantAddZoneDeltaSQL = "UPDATE zones AS z " +
		"SET domains_total = z.domains_total + d.n, " +
		"domains_pending = z.domains_pending + d.n " +
		"FROM unnest($1::int2[], $2::int8[]) AS d(zone_id, n) " +
		"WHERE z.id = d.zone_id"

	wantAddReviveSQL = "WITH rev AS (UPDATE domains AS d " +
		"SET status = 0, check_state = 0, queued_at = NULL " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 " +
		"AND d.zone_id = s.zone_id AND d.label = s.label " +
		"AND d.status = 6 RETURNING d.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_total = z.domains_total + c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM rev GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM rev"

	wantAddCountSQL = "SELECT count(*) FROM (" +
		"SELECT DISTINCT s.zone_id, s.label FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2) AS t"
)

// TestApplyAdd_SQLContract — точные тексты SQL применения и связь
// литералов с константами модели: курсор пачек опирается на PK staging
// (batch_id, line_no, миграция 000004), ON CONFLICT — на уникальный
// (zone_id, label) партиций domains (миграция 000001).
func TestApplyAdd_SQLContract(t *testing.T) {
	if addChunkBoundSQL != wantAddChunkBoundSQL {
		t.Errorf("addChunkBoundSQL = %q,\nwant %q", addChunkBoundSQL, wantAddChunkBoundSQL)
	}
	if insertAddSQL != wantAddInsertSQL {
		t.Errorf("insertAddSQL = %q,\nwant %q", insertAddSQL, wantAddInsertSQL)
	}
	if zoneDeltaAddSQL != wantAddZoneDeltaSQL {
		t.Errorf("zoneDeltaAddSQL = %q,\nwant %q", zoneDeltaAddSQL, wantAddZoneDeltaSQL)
	}
	if reviveAddSQL != wantAddReviveSQL {
		t.Errorf("reviveAddSQL = %q,\nwant %q", reviveAddSQL, wantAddReviveSQL)
	}
	if countAddSQL != wantAddCountSQL {
		t.Errorf("countAddSQL = %q,\nwant %q", countAddSQL, wantAddCountSQL)
	}

	// Литералы в текстах выше обязаны следовать за кодами модели (ТЗ 6.2,
	// 5.2 п. 9): при изменении кодов тексты пересобираются сами — тест
	// ловит рассинхронизацию рукописного литерала.
	if int16(model.StatusNew) != 0 || int16(model.StatusDeleted) != 6 ||
		int16(model.CheckStatePending) != 0 || int16(model.FlagIDN) != 1 {
		t.Errorf("коды модели изменились (new=%d deleted=%d pending=%d is_idn=%d): обновите литералы в wantAdd*SQL",
			int16(model.StatusNew), int16(model.StatusDeleted), int16(model.CheckStatePending), int16(model.FlagIDN))
	}

	up := readMigration(t, "000001_init.up.sql")
	if !strings.Contains(up, "UNIQUE (zone_id, label)") {
		t.Error("в миграции 000001 нет UNIQUE (zone_id, label) — ON CONFLICT DO NOTHING нечему конфликтовать (ТЗ 5.1)")
	}
	// Курсор пачек — PK staging: без него ORDER BY line_no не обслуживается
	// индексом и граница пачки читала бы всю партию (T-205).
	stage := readMigration(t, "000004_ingest_staging.up.sql")
	if !strings.Contains(stage, "PRIMARY KEY (batch_id, line_no)") {
		t.Error("в миграции 000004 нет PRIMARY KEY (batch_id, line_no) — курсор пачек применения не обслуживается ключом (T-205)")
	}
}

// TestInsertAddIDNConditionEquivalent — условие is_idn в insertAddSQL
// ('xn--%' OR '%.xn--%') эквивалентно idn.HasXNLabel (ТЗ 5.2 п. 9) на
// простых и составных метках (ING-P1-1): «любая метка имени начинается
// с xn--».
func TestInsertAddIDNConditionEquivalent(t *testing.T) {
	prefixFold := func(s, p string) bool {
		return len(s) >= len(p) && strings.EqualFold(s[:len(p)], p)
	}
	ilike := func(label string) bool { // семантика 'xn--%' OR '%.xn--%'
		return prefixFold(label, "xn--") ||
			strings.Contains(strings.ToLower(label), ".xn--")
	}
	labels := []string{
		"example", "xn--e1afmkfd",
		"shop.xn--p1ai", "xn--p1ai.shop", // расходились до фикса
		"xn--80aairftm.xn--p1ai", "sub.example", "sub.xn--e1afmkfd.example",
		"XN--E1AFMKFD", "sub.XN--e1afmkfd",
	}
	for _, label := range labels {
		if got, want := ilike(label), idn.HasXNLabel(label); got != want {
			t.Errorf("условие is_idn(%q) = %v, want %v (idn.HasXNLabel)", label, got, want)
		}
	}
}
