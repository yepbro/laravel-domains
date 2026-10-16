// Юнит-тесты применения delete без БД (стиль stage_test.go/zones_test.go):
// текст SQL и параметры, счётчики, порядок «применить → очистить», пустая
// партия, идемпотентность повтора (ТЗ 3.4), ошибки применения и очистки,
// отмена ctx. Реальное поведение против живой БД — интеграционные тесты
// ingest (T-023).
package ingest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
	"crawler/internal/model"
)

// applyDeleteCall — записанное обращение к пулу: SQL и аргументы.
type applyDeleteCall struct {
	sql  string
	args []any
}

// applyDeletePool — db.DB для тестов применения delete: помнит порядок
// обращений (query применения, exec очистки) с SQL и аргументами, выдаёт
// заготовленные строки счётчиков по очереди. Строки — переиспользованный
// fakeRows из zones_test.go (та же пара Query/Scan в одном пакете).
type applyDeletePool struct {
	ops      []string // порядок обращений: "query:<sql>", "exec:<sql>"
	queries  []applyDeleteCall
	execs    []applyDeleteCall
	results  []fakeRows
	execTag  pgconn.CommandTag
	queryErr error
	execErr  error
}

var _ db.DB = (*applyDeletePool)(nil)

func (p *applyDeletePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *applyDeletePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.execs = append(p.execs, applyDeleteCall{sql: sql, args: args})
	p.ops = append(p.ops, "exec:"+sql)
	if p.execErr != nil {
		return pgconn.CommandTag{}, p.execErr
	}
	return p.execTag, nil
}

func (p *applyDeletePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.queries = append(p.queries, applyDeleteCall{sql: sql, args: args})
	p.ops = append(p.ops, "query:"+sql)
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	rows := p.results[0]
	p.results = p.results[1:]
	return &rows, nil
}

// validDeleteBatch — валидная delete-партия для тестов применения.
func validDeleteBatch(t *testing.T) Batch {
	t.Helper()
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	return Batch{ID: id, ZoneID: 7, Action: model.ActionDelete}
}

// wantApplyArgs — аргументы запроса применения: идентификатор партии,
// код статуса deleted и код операции delete (в параметрах, а не в тексте
// SQL — точные значения model закрепляются тестом).
func wantApplyArgs(b Batch) []any {
	return []any{b.ID[:], int16(model.StatusDeleted), int16(model.ActionDelete)}
}

func TestApplyDelete(t *testing.T) {
	b := validDeleteBatch(t)
	pool := &applyDeletePool{
		results: []fakeRows{zoneRows([]any{int64(5), int64(2)})},
		execTag: pgconn.NewCommandTag("DELETE 7"),
	}

	res, err := ApplyDelete(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{Deleted: 5, NotFound: 2}) {
		t.Errorf("счётчики = %+v, want {5 2}", res)
	}

	// Один запрос применения с точными SQL и параметрами.
	if len(pool.queries) != 1 || pool.queries[0].sql != applyDeleteSQL {
		t.Fatalf("запросы = %+v, want один applyDeleteSQL", pool.queries)
	}
	if !reflect.DeepEqual(pool.queries[0].args, wantApplyArgs(b)) {
		t.Errorf("аргументы = %#v, want %#v", pool.queries[0].args, wantApplyArgs(b))
	}

	// Затем одна очистка своей партии (StageTable — общий источник).
	if len(pool.execs) != 1 || pool.execs[0].sql != clearStageSQL {
		t.Fatalf("exec = %+v, want один clearStageSQL", pool.execs)
	}
	if !reflect.DeepEqual(pool.execs[0].args, []any{b.ID[:]}) {
		t.Errorf("аргументы очистки = %#v, want идентификатор партии", pool.execs[0].args)
	}

	// Порядок: применить → очистить (очистка до применения теряла бы
	// строки партии при сбое применения).
	if len(pool.ops) != 2 || !strings.HasPrefix(pool.ops[0], "query:") || !strings.HasPrefix(pool.ops[1], "exec:") {
		t.Errorf("порядок обращений = %v, want [query применение, exec очистка]", pool.ops)
	}
}

// TestApplyDeleteSQLContract — точный текст применения: перенос статуса
// по ключу (zone_id, label) из строк своей партии, без охраны по текущему
// статусу (уже deleted остаётся deleted — идемпотентность, ТЗ 3.4/13.1),
// домен остаётся в БД (нет DELETE), счётчики по строкам staging.
func TestApplyDeleteSQLContract(t *testing.T) {
	want := `WITH applied AS (
	UPDATE domains AS d
	SET status = $2
	FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.zone_id = s.zone_id
	  AND d.label = s.label
	RETURNING d.zone_id, d.label
)
SELECT
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	JOIN applied AS a USING (zone_id, label)
	WHERE s.batch_id = $1 AND s.action = $3) AS deleted,
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND NOT EXISTS (SELECT 1 FROM domains AS d
	                  WHERE d.zone_id = s.zone_id AND d.label = s.label)) AS not_found`
	if applyDeleteSQL != want {
		t.Errorf("applyDeleteSQL = %q, want точное совпадение", applyDeleteSQL)
	}

	// Таблица staging — из StageTable, как COPY и очистка партии.
	if !strings.Contains(applyDeleteSQL, `FROM `+StageTable.Sanitize()+` AS s`) {
		t.Errorf("SQL = %q, want обращение к staging через StageTable", applyDeleteSQL)
	}
	// Только перевод статуса: домен остаётся в БД (ТЗ 13.1), результаты и
	// контент чистит T-020 — здесь их нет.
	if !strings.Contains(applyDeleteSQL, "UPDATE domains") || strings.Contains(applyDeleteSQL, "DELETE FROM domains") {
		t.Errorf("SQL = %q, want только UPDATE domains", applyDeleteSQL)
	}
	// Без охраны по текущему статусу: переводится любой статус, уже
	// удалённый считается удалённым (счётчик Deleted).
	if strings.Contains(applyDeleteSQL, "status <>") || strings.Contains(applyDeleteSQL, "status !=") {
		t.Errorf("SQL = %q, неожидаемая охрана по текущему статусу", applyDeleteSQL)
	}
}

// TestApplyDeleteEmptyBatch — партия без строк (файл из одних пустых
// строк/комментариев): no-op с нулевыми счётчиками, очистка выполняется.
func TestApplyDeleteEmptyBatch(t *testing.T) {
	pool := &applyDeletePool{results: []fakeRows{zoneRows([]any{int64(0), int64(0)})}}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{}) {
		t.Errorf("счётчики = %+v, want нулевые", res)
	}
	if len(pool.queries) != 1 {
		t.Errorf("запросов = %d, want 1", len(pool.queries))
	}
	if len(pool.execs) != 1 {
		t.Errorf("очисток = %d, want 1 (партия покидает staging и пустой)", len(pool.execs))
	}
}

// TestApplyDeleteIdempotentRepeat — повторное применение после очистки —
// no-op: staging партии пуст, счётчики нулевые, ошибок нет (ТЗ 3.4).
func TestApplyDeleteIdempotentRepeat(t *testing.T) {
	b := validDeleteBatch(t)
	pool := &applyDeletePool{results: []fakeRows{
		zoneRows([]any{int64(3), int64(0)}), // первое применение
		zoneRows([]any{int64(0), int64(0)}), // повтор: партия уже очищена
	}}

	first, err := ApplyDelete(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if first != (DeleteResult{Deleted: 3, NotFound: 0}) {
		t.Fatalf("первое применение = %+v, want {3 0}", first)
	}

	second, err := ApplyDelete(context.Background(), pool, b)
	if err != nil {
		t.Fatalf("повторное применение: %v", err)
	}
	if second != (DeleteResult{}) {
		t.Errorf("повтор = %+v, want нулевые счётчики (no-op)", second)
	}
	if len(pool.queries) != 2 || len(pool.execs) != 2 {
		t.Errorf("обращений: запросов %d, очисток %d, want 2 и 2", len(pool.queries), len(pool.execs))
	}
}

// TestApplyDeleteValidation — программные ошибки партии отбиваются до БД:
// партия без идентификатора и партия не-delete (иначе очистка в конце
// ApplyDelete молча уничтожила бы неприменённую add-партию).
func TestApplyDeleteValidation(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		b    Batch
	}{
		{"нулевой идентификатор", Batch{ZoneID: 1, Action: model.ActionDelete}},
		{"партия add", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}},
		{"нулевой код операции", Batch{ID: id, ZoneID: 1, Action: 0}},
		{"неизвестный код операции", Batch{ID: id, ZoneID: 1, Action: model.Action(9)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{}
			if _, err := ApplyDelete(context.Background(), pool, tc.b); err == nil {
				t.Fatal("ожидалась ошибка валидации")
			}
			if len(pool.ops) != 0 {
				t.Errorf("невалидная партия дошла до БД: %v", pool.ops)
			}
		})
	}
}

// TestApplyDeleteApplyError — ошибка применения (в том числе отмена ctx)
// прерывает ApplyDelete до очистки: партия остаётся в staging для
// повтора (ТЗ 3.4).
func TestApplyDeleteApplyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"отмена контекста", context.Canceled},
		{"ошибка запроса", errors.New("connection refused")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{queryErr: tc.err}

			_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
			if err == nil || !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want обёрнутая %v", err, tc.err)
			}
			if !strings.Contains(err.Error(), "применение delete") {
				t.Errorf("err = %v, want контекст «применение delete»", err)
			}
			if len(pool.execs) != 0 {
				t.Errorf("очисток = %d, want 0 (очистка только после применения)", len(pool.execs))
			}
		})
	}
}

// TestApplyDeleteBadCounters — защита от развала контракта запроса:
// отсутствие строки счётчиков или лишние колонки — ошибка, очистка
// не выполняется.
func TestApplyDeleteBadCounters(t *testing.T) {
	cases := []struct {
		name   string
		result fakeRows
		want   string
	}{
		{"нет строки", zoneRows(), "счётчики не возвращены"},
		{"лишняя колонка", zoneRows([]any{int64(1), int64(2), int64(3)}), "применение delete"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{results: []fakeRows{tc.result}}

			_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ошибку с %q", err, tc.want)
			}
			if len(pool.execs) != 0 {
				t.Errorf("очисток = %d, want 0", len(pool.execs))
			}
		})
	}
}

// TestApplyDeleteClearError — ошибка очистки не отменяет применённый
// delete: счётчики возвращаются вместе с ошибкой, повтор доведёт очистку.
func TestApplyDeleteClearError(t *testing.T) {
	pool := &applyDeletePool{
		results: []fakeRows{zoneRows([]any{int64(4), int64(1)})},
		execErr: errors.New("deadlock detected"),
	}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err == nil || !strings.Contains(err.Error(), "deadlock detected") {
		t.Fatalf("err = %v, want ошибка очистки", err)
	}
	if !strings.Contains(err.Error(), "ingest: очистка партии") {
		t.Errorf("err = %v, want контекст очистки партии", err)
	}
	if res != (DeleteResult{Deleted: 4, NotFound: 1}) {
		t.Errorf("счётчики = %+v, want {4 1}: применение состоялось", res)
	}
}
