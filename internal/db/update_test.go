// Юнит-тесты конструктора пакетных UPDATE … FROM unnest(...): точный текст
// SQL (ключи, SET из unnest, фиксированные выражения, охрана, RETURNING),
// валидация параллельных массивов значений, пустая пачка, порядок
// аргументов. Реальное выполнение — test/integration/db_test.go.
package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeExecer запоминает SQL и аргументы Exec.
type fakeExecer struct {
	sql    string
	args   []any
	rows   int64 // RowsAffected для Exec
	err    error
	called bool
}

func (f *fakeExecer) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (f *fakeExecer) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.called = true
	f.sql = sql
	f.args = args
	if f.err != nil {
		return pgconn.CommandTag{}, f.err
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", f.rows)), nil
}

func (f *fakeExecer) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }

// schedulerUpdate — типовой запрос планировщика (ТЗ 5.3): перевод
// выбранных доменов в queued с отметкой времени, только из pending.
func schedulerUpdate() *UpdateBuilder {
	return Update("domains").
		Key(Column{Name: "zone_id", Type: "int2"}).
		Key(Column{Name: "id", Type: "int8"}).
		Set(Column{Name: "check_state", Type: "int2"}).
		SetExpr("queued_at = now()").
		Where("t.check_state = 0")
}

func TestUpdate_SQL(t *testing.T) {
	want := `UPDATE "domains" AS t ` +
		`SET "check_state" = u."check_state", queued_at = now() ` +
		`FROM unnest($1::int2[], $2::int8[], $3::int2[]) AS u("zone_id", "id", "check_state") ` +
		`WHERE t."zone_id" = u."zone_id" AND t."id" = u."id" AND t.check_state = 0`

	sql, cols, err := schedulerUpdate().build()
	if err != nil {
		t.Fatal(err)
	}
	if sql != want {
		t.Errorf("SQL:\n got  %s\n want %s", sql, want)
	}
	if strings.Join(colNames(cols), ",") != "zone_id,id,check_state" {
		t.Errorf("колонки значений = %v, want [zone_id id check_state] (ключи, затем SET)", colNames(cols))
	}
}

func TestUpdate_SQLReturning(t *testing.T) {
	sql, cols, err := schedulerUpdate().
		Returning("t.zone_id", "t.id").
		build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(sql, ` RETURNING t.zone_id, t.id`) {
		t.Errorf("SQL без RETURNING: %s", sql)
	}
	if len(cols) != 3 {
		t.Errorf("Returning не должно добавлять колонки значений: %v", colNames(cols))
	}
}

func TestUpdate_SQLSchemaQualifiedTable(t *testing.T) {
	sql, _, err := Update("public.domains").
		Key(Column{Name: "id", Type: "int8"}).
		SetExpr("status = 5").
		build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sql, `UPDATE "public"."domains" AS t`) {
		t.Errorf("SQL = %s, want схема экранируется по частям", sql)
	}
}

func TestUpdate_BuildValidation(t *testing.T) {
	cases := []struct {
		name     string
		b        *UpdateBuilder
		problems []string
	}{
		{"без ключей", Update("domains").SetExpr("a = 1"), []string{"ключи"}},
		{"без присваиваний", Update("domains").Key(Column{Name: "id", Type: "int8"}), []string{"присваивания"}},
		{"без таблицы", Update("").Key(Column{Name: "id", Type: "int8"}).SetExpr("a = 1"), []string{"таблица"}},
		{"без типа колонки", Update("domains").Key(Column{Name: "id"}).SetExpr("a = 1"), []string{"SQL-тип"}},
		{"без имени колонки", Update("domains").Key(Column{Name: "", Type: "int8"}).SetExpr("a = 1"), []string{"пустым именем"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tc.b.build()
			if err == nil {
				t.Fatal("ожидалась ошибка сборки")
			}
			for _, want := range tc.problems {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v, want содержит %q", err, want)
				}
			}
		})
	}
}

func TestUpdate_Exec(t *testing.T) {
	f := &fakeExecer{rows: 41}
	zones := []any{int16(1), int16(1)}
	ids := []any{int64(10), int64(11)}
	states := []any{int16(1), int16(1)}

	n, err := schedulerUpdate().Exec(context.Background(), f, zones, ids, states)
	if err != nil {
		t.Fatal(err)
	}
	if n != f.rows {
		t.Errorf("RowsAffected = %d, want %d", n, f.rows)
	}
	if !f.called {
		t.Fatal("Exec не дошёл до БД")
	}
	if len(f.args) != 3 {
		t.Fatalf("аргументов %d, want 3 (по одному массиву на колонку)", len(f.args))
	}
	gotZones, ok := f.args[0].([]any)
	if !ok || !reflect.DeepEqual(gotZones, zones) {
		t.Errorf("аргумент 0 = %#v, want массив зоны %v", f.args[0], zones)
	}
	gotStates, ok := f.args[2].([]any)
	if !ok || !reflect.DeepEqual(gotStates, states) {
		t.Errorf("аргумент 2 = %#v, want массив состояний %v", f.args[2], states)
	}
	if !strings.Contains(f.sql, "unnest($1::int2[], $2::int8[], $3::int2[])") {
		t.Errorf("SQL = %s, want приведение типов массивов", f.sql)
	}
}

func TestUpdate_ExecEmptyBatch(t *testing.T) {
	f := &fakeExecer{}
	n, err := schedulerUpdate().Exec(context.Background(), f, []any{}, []any{}, []any{})
	if err != nil || n != 0 {
		t.Fatalf("(%d, %v), want (0, nil)", n, err)
	}
	if f.called {
		t.Error("пустая пачка не должна обращаться к БД")
	}
}

func TestUpdate_ExecArgsValidation(t *testing.T) {
	f := &fakeExecer{}

	cases := []struct {
		name    string
		columns [][]any
		want    string
	}{
		{"мало массивов", [][]any{{}, {}}, "нужно 3"},
		{"разная длина", [][]any{{1, 2}, {1}, {1, 2}}, "параллельными"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := schedulerUpdate().Exec(context.Background(), f, tc.columns...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want содержит %q", err, tc.want)
			}
			if f.called {
				t.Error("невалидные аргументы дошли до БД")
			}
		})
	}
}

func TestUpdate_ExecReturningMismatch(t *testing.T) {
	f := &fakeExecer{}
	// Exec при заданном Returning — ошибка: строки результата потерялись бы.
	if _, err := schedulerUpdate().Returning("t.id").Exec(context.Background(), f, []any{1}, []any{1}, []any{1}); err == nil || !strings.Contains(err.Error(), "Query") {
		t.Errorf("err = %v, want подсказка использовать Query", err)
	}
	// Query без Returning — ошибка.
	if _, err := schedulerUpdate().Query(context.Background(), f, []any{1}, []any{1}, []any{1}); err == nil || !strings.Contains(err.Error(), "Returning") {
		t.Errorf("err = %v, want подсказка задать Returning", err)
	}
}

func TestUpdate_QueryEmptyBatch(t *testing.T) {
	f := &fakeExecer{}
	if _, err := schedulerUpdate().Returning("t.id").Query(context.Background(), f, []any{}, []any{}, []any{}); err == nil {
		t.Error("Query с пустой пачкой: ожидалась ошибка (RETURNING не вернёт строк)")
	}
	if f.called {
		t.Error("запрос не должен был выполниться")
	}
}

func TestUpdate_ExecError(t *testing.T) {
	f := &fakeExecer{err: errors.New("boom")}
	if _, err := schedulerUpdate().Exec(context.Background(), f, []any{1}, []any{1}, []any{1}); err == nil || !strings.Contains(err.Error(), "UPDATE domains") {
		t.Errorf("err = %v, want имя таблицы в обёрнутой ошибке", err)
	}
}

func colNames(cols []Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}
