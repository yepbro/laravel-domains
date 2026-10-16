// Юнит-тесты хелперов COPY без БД: валидация аргументов, разбиение потока
// на пачки, обработка ошибок источника и отмена контекста между пачками.
// Реальное копирование — test/integration/db_test.go.
package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var testTable = pgx.Identifier{"public", "items"}

// fakeCopier записывает размеры пачек, переданных в CopyFrom.
type fakeCopier struct {
	batches []int
	err     error // если задана — возвращается каждым вызовом CopyFrom
}

func (f *fakeCopier) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, src pgx.CopyFromSource) (int64, error) {
	n := int64(0)
	for src.Next() {
		if _, err := src.Values(); err != nil {
			return 0, err
		}
		n++
	}
	if err := src.Err(); err != nil {
		return 0, err
	}
	if f.err != nil {
		return 0, f.err
	}
	f.batches = append(f.batches, int(n))
	return n, nil
}

func (f *fakeCopier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (f *fakeCopier) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }

// rowsIter — итератор по готовым строкам с подсчётом вызовов.
type rowsIter struct {
	rows    [][]any
	nextIdx int
	onNext  func(i int) // побочный эффект перед выдачей строки i (отмена и т.п.)
}

func (it *rowsIter) next() ([]any, error) {
	i := it.nextIdx
	if i >= len(it.rows) {
		return nil, io.EOF
	}
	if it.onNext != nil {
		it.onNext(i)
	}
	it.nextIdx++
	return it.rows[i], nil
}

func simpleRows(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int16(1), int64(i), fmt.Sprintf("row-%d", i)}
	}
	return rows
}

func TestCopyFrom_Validation(t *testing.T) {
	f := &fakeCopier{}
	ctx := context.Background()

	if n, err := CopyFrom(ctx, f, testTable, []string{"zone_id"}, nil); err != nil || n != 0 {
		t.Errorf("пустая пачка: (%d, %v), want (0, nil) без обращения к БД", n, err)
	}
	if len(f.batches) != 0 {
		t.Errorf("пустая пачка привела к вызовам БД: %v", f.batches)
	}

	if _, err := CopyFrom(ctx, f, testTable, nil, simpleRows(1)); err == nil || !strings.Contains(err.Error(), "колонок") {
		t.Errorf("пустой список колонок: err = %v, want ошибка про колонки", err)
	}

	bad := [][]any{{1, 2, 3}}
	if _, err := CopyFrom(ctx, f, testTable, []string{"a", "b"}, bad); err == nil || !strings.Contains(err.Error(), "строка 0") {
		t.Errorf("несоответствие строки колонкам: err = %v, want ошибка со строкой 0", err)
	}
	if len(f.batches) != 0 {
		t.Errorf("невалидная пачка дошла до БД: %v", f.batches)
	}
}

func TestCopyFrom_SingleBatch(t *testing.T) {
	f := &fakeCopier{}
	n, err := CopyFrom(context.Background(), f, testTable, []string{"a", "b", "c"}, simpleRows(3))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("скопировано %d, want 3", n)
	}
	if strings.Join(ints(f.batches), ",") != "3" {
		t.Errorf("пачки = %v, want [3]", f.batches)
	}
}

func TestCopyFromStream_Batching(t *testing.T) {
	f := &fakeCopier{}
	it := &rowsIter{rows: simpleRows(105)}
	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 10, it.next)
	if err != nil {
		t.Fatal(err)
	}
	if n != 105 {
		t.Errorf("скопировано %d, want 105", n)
	}
	// 10 полных пачок + хвост из 5 строк; память ограничена batchSize.
	if strings.Join(ints(f.batches), ",") != "10,10,10,10,10,10,10,10,10,10,5" {
		t.Errorf("пачки = %v, want 10×10 + 5", f.batches)
	}
}

func TestCopyFromStream_BatchSmallerThanSource(t *testing.T) {
	f := &fakeCopier{}
	it := &rowsIter{rows: simpleRows(7)}
	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 1000, it.next)
	if err != nil || n != 7 {
		t.Fatalf("(%d, %v), want (7, nil)", n, err)
	}
	if strings.Join(ints(f.batches), ",") != "7" {
		t.Errorf("пачки = %v, want [7] (одна неполная)", f.batches)
	}
}

func TestCopyFromStream_InvalidBatchSize(t *testing.T) {
	for _, size := range []int{0, -1} {
		f := &fakeCopier{}
		if _, err := CopyFromStream(context.Background(), f, testTable, []string{"a"}, size, (&rowsIter{rows: simpleRows(1)}).next); err == nil {
			t.Errorf("batchSize %d: ожидалась ошибка", size)
		}
		if len(f.batches) != 0 {
			t.Errorf("batchSize %d: копирование дошло до БД", size)
		}
	}
}

func TestCopyFromStream_SourceError(t *testing.T) {
	f := &fakeCopier{}
	boom := errors.New("источник сломался")
	it := &rowsIter{rows: simpleRows(30)}
	// Источник падает на строке 22, не дотянув до сброса третьей пачки.
	next := func() ([]any, error) {
		if it.nextIdx == 22 {
			return nil, boom
		}
		return it.next()
	}

	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 10, next)
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("err = %v, want обёрнутая %v", err, boom)
	}
	if !strings.Contains(err.Error(), "скопировано 20") {
		t.Errorf("err = %v, want упоминание 20 закоммиченных строк (2 полные пачки)", err)
	}
	if n != 20 {
		t.Errorf("скопировано %d, want 20 (только законченные пачки)", n)
	}
	if strings.Join(ints(f.batches), ",") != "10,10" {
		t.Errorf("пачки = %v, want [10,10]", f.batches)
	}
}

func TestCopyFromStream_RowMismatch(t *testing.T) {
	f := &fakeCopier{}
	rows := [][]any{{1, 2, 3}, {1, 2}} // вторая строка короткая
	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 10, (&rowsIter{rows: rows}).next)
	if err == nil || !strings.Contains(err.Error(), "строка 1") {
		t.Errorf("err = %v, want ошибка про строку 1", err)
	}
	if n != 0 || len(f.batches) != 0 {
		t.Errorf("n = %d, пачки = %v: невалидная пачка не должна попадать в БД", n, f.batches)
	}
}

func TestCopyFromStream_CancelBetweenBatches(t *testing.T) {
	f := &fakeCopier{}
	ctx, cancel := context.WithCancel(context.Background())

	it := &rowsIter{rows: simpleRows(100)}
	it.onNext = func(i int) {
		if i == 30 { // строка 31: пачки 1–3 (строки 0–29) уже закоммичены
			cancel()
		}
	}

	n, err := CopyFromStream(ctx, f, testTable, []string{"a", "b", "c"}, 10, it.next)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n != 30 {
		t.Errorf("скопировано %d, want 30 (отмена до сброса 4-й пачки)", n)
	}
	if strings.Join(ints(f.batches), ",") != "10,10,10" {
		t.Errorf("пачки = %v, want [10,10,10]", f.batches)
	}
}

func TestCopyFromStream_CopyError(t *testing.T) {
	f := &fakeCopier{err: errors.New("relation does not exist")}
	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 2, (&rowsIter{rows: simpleRows(5)}).next)
	if err == nil || !strings.Contains(err.Error(), "relation does not exist") {
		t.Fatalf("err = %v, want обёрнутая ошибка COPY", err)
	}
	if !strings.Contains(err.Error(), "items") {
		t.Errorf("err = %v, want имя таблицы в ошибке", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0 (первая же пачка упала)", n)
	}
}

func ints(vs []int) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = fmt.Sprint(v)
	}
	return out
}
