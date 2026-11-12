// Unit tests of domain-level ingest idempotency (T-097, option D;
// spec 3.4, 5.1): the file dedup by sha256 — an optimization; the
// correctness of reprocessing is guaranteed by the domain operations.
// Scenarios: reprocessing the same file after a report record failure
// (the journal is empty — no duplicate is found, the application must
// converge to the same state, the report is written exactly once) and a
// batch cleanup failure in the error-threshold and duplicate branches —
// the discard retries the cleanup with its own context, the rows are not
// left in staging. Re-applying add/delete batches without a DB —
// TestApplyAdd_Reapply / TestApplyDeleteIdempotentRepeat; the real
// convergence against a live DB — the idempotency integration tests
// (test/integration/ingest_idempotency_test.go).
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

// TestPipelineReapplyAfterLostReport — domain-level idempotency (T-097):
// (T-097): a failure between the application and the report record (the
// RecordFile window) — the file stays in incoming and is reprocessed,
// but the journal is empty, no duplicate is found by sha256. The repeated
// application must be a no-op for the domains (INSERT 0 / UPDATE 0 — the
// ones inserted by the first attempt conflict, there is nothing to
// revive), the report is written exactly once.
func TestPipelineReapplyAfterLostReport(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\nshop.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))

	// The first attempt: the application fully went through, the report
	// was not recorded.
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading
		{tag: pgconn.NewCommandTag("INSERT 2")}, // example, shop
		{tag: pgconn.NewCommandTag("UPDATE 0")}, // nothing to revive
		{tag: pgconn.NewCommandTag("DELETE 2")}, // the cleanup after the application
	}
	pt.pool.uniques = 2
	pt.files.recErr = errors.New("the journal is unavailable")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.files.recErr) {
		t.Fatalf("the first attempt: err = %v, want the journal error", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("the first attempt: the outcome = %s, want left and the file in incoming", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Fatal("the report must not have been written (the journal failed)")
	}

	// Reprocessing the same file (at-least-once, spec 3.4): the domains
	// were already added by the first attempt — the application converges
	// to a no-op.
	pt.files.recErr = nil
	pt.pool = &pipePool{
		execs: []pipeExec{
			{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading
			{tag: pgconn.NewCommandTag("INSERT 0")}, // a uniqueness conflict
			{tag: pgconn.NewCommandTag("UPDATE 0")}, // the status = deleted guard
			{tag: pgconn.NewCommandTag("DELETE 2")}, // the cleanup after the application
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
		t.Fatalf("the reprocessing: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("the reprocessing: the outcome = %s, want applied", out.Kind)
	}
	// The domain state is unchanged: nothing to add, everything "already
	// existed" (the retry counters reflect the current state, spec 3.4).
	if out.Report.Added != 0 || out.Report.Revived != 0 || out.Report.Existed != 2 {
		t.Errorf("the repeat counters = added %d, revived %d, existed %d; want 0/0/2",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}

	// The report — exactly one, the retry does not duplicate the journal.
	if len(pt.files.records) != 1 {
		t.Fatalf("journal records = %d, want 1", len(pt.files.records))
	}
	rec := pt.files.records[0]
	wantRec := FileRecord{
		ZoneID: 1, Action: model.ActionAdd, Path: "com/add/list.txt", SHA256: sha[:],
		LinesTotal: 2, Existed: 2, ProcessedAt: pt.now,
	}
	if !reflect.DeepEqual(rec, wantRec) {
		t.Errorf("the journal record = %+v, want %+v", rec, wantRec)
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")) {
		t.Error("the file was not moved to processed")
	}
}

// TestPipelineDuplicateClearFailureDiscards — a batch cleanup failure in
// the duplicate branch (T-097): a clearBatch with an already cancelled
// ctx (a graceful shutdown) or a momentary DB failure must not leave the
// batch rows in staging — the discard retries the cleanup with its own
// context.
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

	boom := errors.New("the batch cleanup failed")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		{err: boom},                             // the duplicate's batch cleanup — a failure
		// the discard: the third cleanup — went through
	}
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir: pt.dir, BatchSize: 10, Now: func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the batch cleanup error", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("the outcome = %s, want left and the file in incoming", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Error("a duplicate must not write a second report")
	}

	// The cleanup → COPY → the duplicate's cleanup failure → a discard
	// retry with its own cleanup: the batch rows are not left in staging.
	want := []string{
		"exec:" + clearStageSQL,
		"copy:1",
		"exec:" + clearStageSQL,
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("the call order =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}
}

// TestPipelineFailedRatioClearFailureDiscards — the same invariant for
// the error threshold branch (T-016): the file is bad, the batch is
// discarded; a discard failure is retried by the discard.
func TestPipelineFailedRatioClearFailureDiscards(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "a.org\nb.org\nc.com\n") // 2/3 > 0.1

	boom := errors.New("the batch cleanup failed")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		{err: boom},                             // the batch discard — a failure
		// the discard: the third cleanup — went through
	}

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the batch discard error", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("the outcome = %s, want left and the file in incoming", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Error("the bad file's report is not written to the journal")
	}

	want := []string{
		"exec:" + clearStageSQL,
		"copy:1",
		"exec:" + clearStageSQL,
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("the call order =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}
}
