// Unit tests of the COPY helpers without a DB: argument validation,
// splitting the stream into chunks, source error handling and context
// cancellation between chunks. Real copying — test/integration/db_test.go.
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

// fakeCopier records the sizes of the chunks passed to CopyFrom.
type fakeCopier struct {
	batches []int
	err     error // if set, returned by every CopyFrom call
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

// rowsIter — an iterator over ready rows with call counting.
type rowsIter struct {
	rows    [][]any
	nextIdx int
	onNext  func(i int) // side effect before yielding row i (cancellation etc.)
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
		t.Errorf("an empty chunk: (%d, %v), want (0, nil) without touching the DB", n, err)
	}
	if len(f.batches) != 0 {
		t.Errorf("an empty chunk led to DB calls: %v", f.batches)
	}

	if _, err := CopyFrom(ctx, f, testTable, nil, simpleRows(1)); err == nil || !strings.Contains(err.Error(), "column") {
		t.Errorf("an empty column list: err = %v, want a columns error", err)
	}

	bad := [][]any{{1, 2, 3}}
	if _, err := CopyFrom(ctx, f, testTable, []string{"a", "b"}, bad); err == nil || !strings.Contains(err.Error(), "row 0") {
		t.Errorf("a row does not match the columns: err = %v, want an error about row 0", err)
	}
	if len(f.batches) != 0 {
		t.Errorf("an invalid chunk reached the DB: %v", f.batches)
	}
}

func TestCopyFrom_SingleBatch(t *testing.T) {
	f := &fakeCopier{}
	n, err := CopyFrom(context.Background(), f, testTable, []string{"a", "b", "c"}, simpleRows(3))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("copied %d, want 3", n)
	}
	if strings.Join(ints(f.batches), ",") != "3" {
		t.Errorf("chunks = %v, want [3]", f.batches)
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
		t.Errorf("copied %d, want 105", n)
	}
	// 10 full chunks + a tail of 5 rows; memory is bounded by batchSize.
	if strings.Join(ints(f.batches), ",") != "10,10,10,10,10,10,10,10,10,10,5" {
		t.Errorf("chunks = %v, want 10×10 + 5", f.batches)
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
		t.Errorf("chunks = %v, want [7] (one incomplete)", f.batches)
	}
}

func TestCopyFromStream_InvalidBatchSize(t *testing.T) {
	for _, size := range []int{0, -1} {
		f := &fakeCopier{}
		if _, err := CopyFromStream(context.Background(), f, testTable, []string{"a"}, size, (&rowsIter{rows: simpleRows(1)}).next); err == nil {
			t.Errorf("batchSize %d: want an error", size)
		}
		if len(f.batches) != 0 {
			t.Errorf("batchSize %d: the copy reached the DB", size)
		}
	}
}

func TestCopyFromStream_SourceError(t *testing.T) {
	f := &fakeCopier{}
	boom := errors.New("the source is broken")
	it := &rowsIter{rows: simpleRows(30)}
	// The source fails on row 22, short of the third chunk's flush.
	next := func() ([]any, error) {
		if it.nextIdx == 22 {
			return nil, boom
		}
		return it.next()
	}

	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 10, next)
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("err = %v, want the wrapped %v", err, boom)
	}
	if !strings.Contains(err.Error(), "copied 20") {
		t.Errorf("err = %v, want a mention of 20 committed rows (2 full chunks)", err)
	}
	if n != 20 {
		t.Errorf("copied %d, want 20 (only finished chunks)", n)
	}
	if strings.Join(ints(f.batches), ",") != "10,10" {
		t.Errorf("chunks = %v, want [10,10]", f.batches)
	}
}

func TestCopyFromStream_RowMismatch(t *testing.T) {
	f := &fakeCopier{}
	rows := [][]any{{1, 2, 3}, {1, 2}} // the second row is short
	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 10, (&rowsIter{rows: rows}).next)
	if err == nil || !strings.Contains(err.Error(), "row 1") {
		t.Errorf("err = %v, want an error about row 1", err)
	}
	if n != 0 || len(f.batches) != 0 {
		t.Errorf("n = %d, chunks = %v: an invalid chunk must not reach the DB", n, f.batches)
	}
}

func TestCopyFromStream_CancelBetweenBatches(t *testing.T) {
	f := &fakeCopier{}
	ctx, cancel := context.WithCancel(context.Background())

	it := &rowsIter{rows: simpleRows(100)}
	it.onNext = func(i int) {
		if i == 30 { // row 31: chunks 1–3 (rows 0–29) are already committed
			cancel()
		}
	}

	n, err := CopyFromStream(ctx, f, testTable, []string{"a", "b", "c"}, 10, it.next)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n != 30 {
		t.Errorf("copied %d, want 30 (cancelled before the 4th chunk is flushed)", n)
	}
	if strings.Join(ints(f.batches), ",") != "10,10,10" {
		t.Errorf("chunks = %v, want [10,10,10]", f.batches)
	}
}

func TestCopyFromStream_CopyError(t *testing.T) {
	f := &fakeCopier{err: errors.New("relation does not exist")}
	n, err := CopyFromStream(context.Background(), f, testTable, []string{"a", "b", "c"}, 2, (&rowsIter{rows: simpleRows(5)}).next)
	if err == nil || !strings.Contains(err.Error(), "relation does not exist") {
		t.Fatalf("err = %v, want a wrapped COPY error", err)
	}
	if !strings.Contains(err.Error(), "items") {
		t.Errorf("err = %v, want the table name in the error", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0 (the very first chunk failed)", n)
	}
}

func ints(vs []int) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = fmt.Sprint(v)
	}
	return out
}
