// Юнит-тесты применения партии add (T-018, ТЗ 5.1) без БД (стиль
// stage_test.go / zones_test.go): точные SQL-тексты и параметры, счётчики,
// фиксированный порядок «применить → очистить», пустая партия, повторное
// применение (идемпотентность, ТЗ 3.4), ошибки шагов и инвариант
// счётчиков, отмена ctx. Реальное применение — интеграционные тесты
// ingest (T-023).
package ingest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// applyAddPool — db.DB для ApplyAdd: записывает порядок операций, SQL и
// аргументы; Exec (очистка партии) возвращает заготовленные
// CommandTag/ошибку по очереди вызовов, Query (CTE вставки/возврата с
// дельтами зон T-178 и подсчёт) — заготовленные числа строк по очереди.
// Как и реальный pgx, отказывается работать с отменённым контекстом.
type applyAddPool struct {
	ops       []string // порядок: "exec:<sql>", "query:<sql>"
	execSQL   []string
	execArgs  [][]any
	querySQL  []string
	queryArgs [][]any
	execTags  []pgconn.CommandTag // результат каждого Exec по очереди
	execErrs  []error             // ошибка i-го Exec (nil — успех)
	counts    []int64             // результат каждого Query по очереди
	queryErrs []error             // ошибка i-го Query (nil — успех)
}

var _ db.DB = (*applyAddPool)(nil)

func (p *applyAddPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("applyAddPool: CopyFrom не должен вызываться")
}

func (p *applyAddPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	i := len(p.execSQL)
	p.execSQL = append(p.execSQL, sql)
	p.execArgs = append(p.execArgs, args)
	p.ops = append(p.ops, "exec:"+sql)
	if i < len(p.execErrs) && p.execErrs[i] != nil {
		return pgconn.CommandTag{}, p.execErrs[i]
	}
	if i < len(p.execTags) {
		return p.execTags[i], nil
	}
	return pgconn.CommandTag{}, nil
}

func (p *applyAddPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i := len(p.querySQL)
	p.querySQL = append(p.querySQL, sql)
	p.queryArgs = append(p.queryArgs, args)
	p.ops = append(p.ops, "query:"+sql)
	if i < len(p.queryErrs) && p.queryErrs[i] != nil {
		return nil, p.queryErrs[i]
	}
	count := int64(0)
	if i < len(p.counts) {
		count = p.counts[i]
	}
	return &applyAddRows{vals: []int64{count}}, nil
}

// applyAddRows — pgx.Rows ответа подсчёта: одна строка, одна колонка
// count (int8). Коллектор pgx читает её позиционно (RowTo[int64]).
type applyAddRows struct {
	vals   []int64
	i      int
	closed bool
}

func (r *applyAddRows) Next() bool {
	if r.closed || r.i >= len(r.vals) {
		return false
	}
	r.i++
	return true
}

func (r *applyAddRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.vals) {
		return pgx.ErrNoRows
	}
	d, ok := dest[0].(*int64)
	if !ok || len(dest) != 1 {
		return errors.New("applyAddRows: приёмник Scan — один *int64")
	}
	*d = r.vals[r.i-1]
	return nil
}

func (r *applyAddRows) Close() { r.closed = true }

func (r *applyAddRows) Err() error                    { return nil }
func (r *applyAddRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *applyAddRows) FieldDescriptions() []pgconn.FieldDescription {
	return []pgconn.FieldDescription{{Name: "count"}}
}

func (r *applyAddRows) Values() ([]any, error) { return []any{r.vals[r.i-1]}, nil }
func (r *applyAddRows) RawValues() [][]byte    { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая коллектором.
func (r *applyAddRows) Conn() *pgx.Conn      { return nil }
func (r *applyAddRows) TypeMap() *pgtype.Map { return nil }

// applyAddOps — ожидаемый порядок операций применения (вставка → возврат
// удалённых → подсчёт → очистка). Вставка и возврат — CTE-операторы с
// дельтами счётчиков зон (T-178): результат читается Query (внешний
// SELECT числа строк), очистка — Exec.
func applyAddOps() []string {
	return []string{
		"query:" + insertAddSQL,
		"query:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
	}
}

func TestApplyAdd(t *testing.T) {
	b := validBatch(t) // зона 7, add — как после EnsureZone и Stage
	pool := &applyAddPool{
		execTags: []pgconn.CommandTag{
			pgconn.NewCommandTag("DELETE 10"), // партия очищена
		},
		counts: []int64{5, 3, 10}, // вставлено новых, возвращено из deleted, уникальных меток
	}

	res, err := ApplyAdd(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 5 || res.Revived != 3 || res.Existing != 2 {
		t.Errorf("результат = %+v, want Inserted 5, Revived 3, Existing 2 (уникальные 10 − 5 − 3)", res)
	}
	if res.Total() != 10 {
		t.Errorf("Total() = %d, want 10", res.Total())
	}

	// Порядок: применить (вставка, возврат, подсчёт) → очистить.
	if got := strings.Join(pool.ops, "\n"); got != strings.Join(applyAddOps(), "\n") {
		t.Errorf("порядок операций:\n%s\nwant:\n%s", got, strings.Join(applyAddOps(), "\n"))
	}

	// Вставка, возврат и подсчёт адресуют свою партию и только строки add.
	wantArgs := []any{[]byte(b.ID[:]), int16(model.ActionAdd)}
	for i, name := range []string{"вставка", "возврат", "подсчёт"} {
		if !reflect.DeepEqual(pool.queryArgs[i], wantArgs) {
			t.Errorf("аргументы %s = %v, want [batch_id, action=%d]", name, pool.queryArgs[i], int16(model.ActionAdd))
		}
	}
	// Очистка — ClearBatch своей партии.
	if len(pool.execArgs) != 1 || !reflect.DeepEqual(pool.execArgs[0][0], []byte(b.ID[:])) {
		t.Errorf("очистка = %v, want ClearBatch(batch_id)", pool.execArgs)
	}
}

// TestApplyAdd_EmptyBatch — пустая партия (или уже применённая, staging
// пуст): данные не меняются, результат нулевой, но порядок операций тот
// же — подсчёт и очистка выполняются и на пустом множестве.
func TestApplyAdd_EmptyBatch(t *testing.T) {
	pool := &applyAddPool{counts: []int64{0}}

	res, err := ApplyAdd(context.Background(), pool, validBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	if res != (AddResult{}) {
		t.Errorf("результат = %+v, want нулевой", res)
	}
	if got := strings.Join(pool.ops, "\n"); got != strings.Join(applyAddOps(), "\n") {
		t.Errorf("порядок операций:\n%s\nwant:\n%s", got, strings.Join(applyAddOps(), "\n"))
	}
}

// TestApplyAdd_Reapply — повторное применение той же партии (ТЗ 3.4):
// после первой попытки вставка конфликтует (CTE считает 0), возврат уже
// выполнен — данные не меняются, счётчики повторного прогона нулевые.
func TestApplyAdd_Reapply(t *testing.T) {
	pool := &applyAddPool{
		execTags: []pgconn.CommandTag{
			pgconn.NewCommandTag("DELETE 10"),
			pgconn.NewCommandTag("DELETE 0"), // staging уже пуст
		},
		counts: []int64{5, 3, 10, 0, 0, 0},
	}
	b := validBatch(t)

	if _, err := ApplyAdd(context.Background(), pool, b); err != nil {
		t.Fatal(err)
	}
	res, err := ApplyAdd(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if res != (AddResult{}) {
		t.Errorf("повторное применение = %+v, want no-op с нулевым результатом", res)
	}
}

func TestApplyAdd_Validation(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		batch Batch
	}{
		{"нулевой идентификатор", Batch{ZoneID: 1, Action: model.ActionAdd}},
		{"партия delete", Batch{ID: id, ZoneID: 1, Action: model.ActionDelete}},
		{"неизвестная операция", Batch{ID: id, ZoneID: 1, Action: model.Action(3)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyAddPool{}
			if _, err := ApplyAdd(context.Background(), pool, tc.batch); err == nil {
				t.Fatal("ожидалась ошибка валидации")
			}
			if len(pool.ops) != 0 {
				t.Errorf("невалидная партия дошла до БД: %v", pool.ops)
			}
		})
	}
}

// TestApplyAdd_StepErrors — сбой каждого шага возвращает обёрнутую ошибку
// без очистки: строки остаются в staging, повторное применение безопасно
// (ТЗ 3.4). Счётчики завершённых шагов сохраняются в результате.
func TestApplyAdd_StepErrors(t *testing.T) {
	boom := errors.New("соединение потеряно")
	cases := []struct {
		name      string
		queryErrs []error // по одному на Query: вставка, возврат, подсчёт
		execErrs  []error // очистка
		wantOps   int     // обращений к БД до сбоя включительно
		want      string  // фрагмент ожидаемой ошибки
	}{
		{"сбой вставки", []error{boom, nil, nil}, nil, 1, "вставка новых доменов"},
		{"сбой возврата", []error{nil, boom, nil}, nil, 2, "возврат удалённых"},
		{"сбой подсчёта", []error{nil, nil, boom}, nil, 3, "подсчёт доменов"},
		{"сбой очистки", nil, []error{boom}, 4, "очистка партии"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyAddPool{
				execTags: []pgconn.CommandTag{
					pgconn.NewCommandTag("DELETE 10"),
				},
				execErrs:  tc.execErrs,
				queryErrs: tc.queryErrs,
				counts:    []int64{5, 3, 10},
			}
			res, err := ApplyAdd(context.Background(), pool, validBatch(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) || !errors.Is(err, boom) {
				t.Fatalf("err = %v, want обёрнутая ошибка с %q", err, tc.want)
			}
			if len(pool.ops) != tc.wantOps {
				t.Errorf("обращений к БД %d (%v), want %d — после сбоя продолжения нет", len(pool.ops), pool.ops, tc.wantOps)
			}
			if strings.Contains(strings.Join(pool.ops, "\n"), "exec:"+clearStageSQL) && tc.wantOps < 4 {
				t.Error("партия очищена несмотря на сбой применения")
			}
			if tc.wantOps > 1 && res.Inserted != 5 {
				t.Errorf("Inserted = %d, want 5 (шаг завершён до сбоя)", res.Inserted)
			}
			if tc.wantOps > 2 && res.Revived != 3 {
				t.Errorf("Revived = %d, want 3 (шаг завершён до сбоя)", res.Revived)
			}
		})
	}
}

func TestApplyAdd_CountError(t *testing.T) {
	pool := &applyAddPool{
		execTags: []pgconn.CommandTag{
			pgconn.NewCommandTag("DELETE 10"),
		},
		counts:    []int64{5, 3, 0},
		queryErrs: []error{nil, nil, errors.New("соединение потеряно")},
	}

	res, err := ApplyAdd(context.Background(), pool, validBatch(t))
	if err == nil || !strings.Contains(err.Error(), "подсчёт доменов") {
		t.Fatalf("err = %v, want ошибка подсчёта", err)
	}
	if len(pool.ops) != 3 {
		t.Errorf("обращений к БД %d, want 3 (подсчёт последним, без очистки)", len(pool.ops))
	}
	if res.Inserted != 5 || res.Revived != 3 || res.Existing != 0 {
		t.Errorf("результат = %+v, want завершённые шаги в счётчиках", res)
	}
}

// TestApplyAdd_CountInvariant — подсчёт меньше вставленного и
// возвращённого: счётчики расходятся, применение признаётся ошибочным,
// партия не очищается.
func TestApplyAdd_CountInvariant(t *testing.T) {
	pool := &applyAddPool{
		execTags: []pgconn.CommandTag{
			pgconn.NewCommandTag("DELETE 10"),
		},
		counts: []int64{5, 3, 1}, // уникальных 1 < 5 + 3
	}

	_, err := ApplyAdd(context.Background(), pool, validBatch(t))
	if err == nil || !strings.Contains(err.Error(), "счётчики расходятся") {
		t.Fatalf("err = %v, want инвариант счётчиков", err)
	}
	if len(pool.ops) != 3 {
		t.Errorf("обращений к БД %d, want 3 (без очистки)", len(pool.ops))
	}
}

// TestApplyAdd_ContextCanceled — отменённый контекст отказывает на
// первом же обращении к БД (как реальный pgx), партия не применяется и
// не очищается.
func TestApplyAdd_ContextCanceled(t *testing.T) {
	pool := &applyAddPool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ApplyAdd(ctx, pool, validBatch(t))
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(pool.ops) != 0 {
		t.Errorf("обращений к БД быть не должно, got %v", pool.ops)
	}
}

// ---------------------------------------------------------------- контракт SQL

const (
	wantAddInsertSQL = "WITH ins AS (INSERT INTO domains (zone_id, label, flags) " +
		"SELECT s.zone_id, s.label, CASE WHEN s.label ILIKE 'xn--%' OR s.label ILIKE '%.xn--%' THEN 1 ELSE 0 END " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 " +
		"ON CONFLICT DO NOTHING RETURNING zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_total = z.domains_total + c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM ins GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM ins"

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
// литералов с константами модели: ON CONFLICT опирается на уникальный
// (zone_id, label) партиций domains (миграция 000001).
func TestApplyAdd_SQLContract(t *testing.T) {
	if insertAddSQL != wantAddInsertSQL {
		t.Errorf("insertAddSQL = %q,\nwant %q", insertAddSQL, wantAddInsertSQL)
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
