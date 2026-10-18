// Юнит-тесты идемпотентности приёма на уровне доменов (T-097, вариант D;
// ТЗ 3.4, 5.1): файловый дедуп по sha256 — оптимизация, корректность
// повторной обработки гарантируют операции над доменами. Сценарии:
// повторная обработка того же файла после сбоя записи отчёта (журнал
// пуст — дубль не находится, применение обязано сойтись к тому же
// состоянию, отчёт пишется ровно один раз) и сбой очистки партии в ветках
// порога ошибок и дубля — discard досыпает очистку собственным
// контекстом, строки не остаются в staging. Повторное применение партий
// add/delete без БД — TestApplyAdd_Reapply / TestApplyDeleteIdempotentRepeat;
// реальная сходимость против живой БД — интеграционные тесты
// идемпотентности (test/integration/ingest_idempotency_test.go).
package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestPipelineReapplyAfterLostReport — идемпотентность уровня доменов
// (T-097): сбой между применением и записью отчёта (окно RecordFile) —
// файл остаётся в incoming и обрабатывается повторно, но журнал пуст,
// дубль по sha256 не находится. Повторное применение обязано быть no-op
// для доменов (INSERT 0 / UPDATE 0 — вставленные первой попыткой
// конфликтуют, возвращать некого), отчёт пишётся ровно один раз.
func TestPipelineReapplyAfterLostReport(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\nshop.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))

	// Первая попытка: применение полностью прошло, отчёт не записан.
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой
		{tag: pgconn.NewCommandTag("INSERT 2")}, // example, shop
		{tag: pgconn.NewCommandTag("UPDATE 0")}, // возвращать некого
		{tag: pgconn.NewCommandTag("DELETE 2")}, // очистка после применения
	}
	pt.pool.uniques = 2
	pt.files.recErr = errors.New("журнал недоступен")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.files.recErr) {
		t.Fatalf("первая попытка: err = %v, want ошибка журнала", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("первая попытка: итог = %s, want left и файл в incoming", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Fatal("отчёт не должен был записаться (сбой журнала)")
	}

	// Повторная обработка того же файла (at-least-once, ТЗ 3.4): домены
	// уже добавлены первой попыткой — применение сходится к no-op.
	pt.files.recErr = nil
	pt.pool = &pipePool{
		execs: []pipeExec{
			{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой
			{tag: pgconn.NewCommandTag("INSERT 0")}, // конфликт уникальности
			{tag: pgconn.NewCommandTag("UPDATE 0")}, // охрана status = deleted
			{tag: pgconn.NewCommandTag("DELETE 2")}, // очистка после применения
		},
		uniques: 2,
	}
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir: pt.dir, BatchSize: 10, Now: func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	pt.pipe = pipe

	out, err = pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("повторная обработка: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("повторная обработка: итог = %s, want applied", out.Kind)
	}
	// Состояние доменов неизменно: добавлять нечего, всё «уже было»
	// (счётчики повтора отражают текущее состояние, ТЗ 3.4).
	if out.Report.Added != 0 || out.Report.Revived != 0 || out.Report.Existed != 2 {
		t.Errorf("счётчики повтора = added %d, revived %d, existed %d; want 0/0/2",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}

	// Отчёт — ровно один, повтор не дублирует журнал.
	if len(pt.files.records) != 1 {
		t.Fatalf("записей в журнале = %d, want 1", len(pt.files.records))
	}
	rec := pt.files.records[0]
	wantRec := FileRecord{
		ZoneID: 1, Action: model.ActionAdd, Path: "com/add/list.txt", SHA256: sha[:],
		LinesTotal: 2, Existed: 2, ProcessedAt: pt.now,
	}
	if !reflect.DeepEqual(rec, wantRec) {
		t.Errorf("запись журнала = %+v, want %+v", rec, wantRec)
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")) {
		t.Error("файл не перемещён в processed")
	}
}

// TestPipelineDuplicateClearFailureDiscards — сбой очистки партии в ветке
// дубля (T-097): clearBatch с уже отменённым ctx (graceful shutdown) или
// кратковременным сбоем БД не должен оставлять строки партии в staging —
// discard ретраит очистку собственным контекстом.
func TestPipelineDuplicateClearFailureDiscards(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))
	processed := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	path := "com/add/list.txt"
	zone, action := int16(1), model.ActionAdd
	pt.files = newFakeFileStore(model.IngestFile{
		ID: 5, ZoneID: &zone, Action: &action, Path: &path,
		SHA256: sha[:], ProcessedAt: &processed,
	})

	boom := errors.New("очистка партии не прошла")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой (Stage)
		{err: boom},                             // очистка партии дубля — сбой
		// discard: третья очистка — успела
	}
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir: pt.dir, BatchSize: 10, Now: func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want ошибка очистки партии", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("итог = %s, want left и файл в incoming", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Error("дубль не должен писать второй отчёт")
	}

	// Очистка → COPY → сбой очистки дубля → discard-ретрай собственной
	// очисткой: строки партии не остаются в staging.
	want := []string{
		"exec:" + clearStageSQL,
		"copy:1",
		"exec:" + clearStageSQL,
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}
}

// TestPipelineFailedRatioClearFailureDiscards — тот же инвариант для
// ветки порога ошибок (T-016): файл ошибочен, партия отбрасывается;
// сбой отбрасывания ретраится discard'ом.
func TestPipelineFailedRatioClearFailureDiscards(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "a.org\nb.org\nc.com\n") // 2/3 > 0.1

	boom := errors.New("очистка партии не прошла")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой (Stage)
		{err: boom},                             // отбрасывание партии — сбой
		// discard: третья очистка — успела
	}

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want ошибка отбрасывания партии", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("итог = %s, want left и файл в incoming", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Error("отчёт ошибочного файла не пишется в журнал")
	}

	want := []string{
		"exec:" + clearStageSQL,
		"copy:1",
		"exec:" + clearStageSQL,
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}
}
