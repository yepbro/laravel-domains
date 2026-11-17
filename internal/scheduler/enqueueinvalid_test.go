// Unit tests of the data-level guard of batch enqueueing (T-309; the
// hy4-flash-free bulk review, the scheduler stage P3-3) on the fakes of
// enqueue_test.go: a poison row (the name is empty or longer than 253 octets,
// spec 5.2 item 4) at the head of a zone queue does NOT block the batch —
// the valid rows are moved and published, the poison ones are terminally
// diagnosed (status = invalid, the R16 transition); the exact SQL text of
// the diagnosis and parameters; the outcome counters (Invalid/Diagnosed,
// by RETURNING after the guards); a diagnosis failure does not block the
// enqueue of the valid remainder; the loop continues with the following
// batches, the crawler_scheduler_enqueue_invalid_total counter grows, the
// Warn is logged. Behavior against a live DB — integration tests.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// enqueuePoisonRow — a batch row with a name over the spec 5.2 limit
// (254 octets > enqueueMaxNameLen).
func enqueuePoisonRow() DispatchRow {
	name := strings.Repeat("a", 254)
	if len(name) <= enqueueMaxNameLen {
		panic("enqueuePoisonRow: the name must be over the limit")
	}
	return DispatchRow{DomainID: 100, ZoneID: 1, Name: name}
}

// --- EnqueueBatch: the filter + the terminal diagnosis -------------------

// TestEnqueueBatch_InvalidHeadDoesNotBlock — the headline scenario of T-309:
// a poison row at the head of the batch (the dispatcher selects the zone
// queue prefix by id, the row would get into EVERY batch) does not reject
// the batch: the valid rows are moved and published, the poison one is
// terminally diagnosed by its exact key; before T-309 the whole batch was
// an error and the cycle repeated the same refusal forever.
func TestEnqueueBatch_InvalidHeadDoesNotBlock(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		enqueuePoisonRow(), // the head of the zone queue
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 201, ZoneID: 2, Name: "b.example.net"},
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 2, Published: 2, Skipped: 0, Invalid: 1, Diagnosed: 1}) {
		t.Errorf("result = %+v, want {Queued:2 Published:2 Skipped:0 Invalid:1 Diagnosed:1}", res)
	}

	calls := pool.enqueuePoolCalls()
	if len(calls) != 2 || calls[0].kind != enqueueKindDiagnose || calls[1].kind != enqueueKindUpdate {
		t.Fatalf("DB calls = %v, want the diagnosis, then the move of the valid remainder", calls)
	}
	// The diagnosis keys — exactly the poison row (typed slices, the
	// leasenull style), the move and the publication do not touch it.
	if !reflect.DeepEqual(calls[0].args, []any{[]int16{1}, []int64{100}}) {
		t.Errorf("diagnosis args = %v, want [zone_id[], domain_id[]] of the poison row", calls[0].args)
	}
	wantMoveArgs := []any{
		[]any{int16(1), int16(2)},
		[]any{int64(101), int64(201)},
	}
	if !reflect.DeepEqual(calls[1].args, wantMoveArgs) {
		t.Errorf("move args = %v, want %v — only the valid rows", calls[1].args, wantMoveArgs)
	}

	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 {
		t.Fatalf("publications %d, want 1", len(pubs))
	}
	want := []struct {
		id   int64
		name string
	}{{101, "a.example.com"}, {201, "b.example.net"}}
	got := pubs[0].msg.Domains
	if len(got) != len(want) {
		t.Fatalf("domains = %+v, want the %d valid ones", got, len(want))
	}
	for i, w := range want {
		if got[i].DomainID != w.id || got[i].Name != w.name {
			t.Errorf("domains[%d] = %+v, want id=%d name=%q (the poison row is not published)", i, got[i], w.id, w.name)
		}
	}
}

// TestEnqueueBatch_AllInvalid — the whole batch is poison: no move and no
// publication at all (an empty DNSBatch is forbidden by the queue contract),
// the outcome is the diagnosis alone, without an error — the batch is
// "processed", the cycle does not go into the IdleWait error pause.
func TestEnqueueBatch_AllInvalid(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		enqueuePoisonRow(),
		{DomainID: 101, ZoneID: 1, Name: ""}, // an empty name: unreachable from dispatch, guarded anyway
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 0, Published: 0, Skipped: 0, Invalid: 2, Diagnosed: 2}) {
		t.Errorf("result = %+v, want {Invalid:2 Diagnosed:2} and the rest zero", res)
	}
	calls := pool.enqueuePoolCalls()
	if len(calls) != 1 || calls[0].kind != enqueueKindDiagnose {
		t.Fatalf("DB calls = %v, want a single diagnosis without a move", calls)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications %d, want 0", len(pubs))
	}
}

// TestEnqueueBatch_DiagnoseGuardFilters — the diagnosis guards filtered out
// the row (fixed by manual SQL in the race window between the selection and
// the lock): Diagnosed < Invalid by RETURNING, no error — the row leaves
// pending by itself, the next selection no longer sees it.
func TestEnqueueBatch_DiagnoseGuardFilters(t *testing.T) {
	pool := &enqueueFakePool{
		diagnoses: []*enqueueFakeRows{enqueueFakeRowsOf()}, // RETURNING is empty
	}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		enqueuePoisonRow(),
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 1, Published: 1, Skipped: 0, Invalid: 1, Diagnosed: 0}) {
		t.Errorf("result = %+v, want {Queued:1 Published:1 Invalid:1 Diagnosed:0}", res)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 1 || len(pubs[0].msg.Domains) != 1 {
		t.Errorf("publications = %+v, want the valid row published", pubs)
	}
}

// TestEnqueueBatch_DiagnoseErrorDoesNotBlock — a diagnosis failure does not
// block the enqueue of the valid remainder (the publication-failure style):
// the rows are moved and published, the error is returned alongside the
// successful result — the loop Warns and repeats the batch (the filter is
// idempotent); the poison row remains pending, res.Invalid shows it.
func TestEnqueueBatch_DiagnoseErrorDoesNotBlock(t *testing.T) {
	pool := &enqueueFakePool{diagnoseErr: errors.New("the db blipped")}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		enqueuePoisonRow(),
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err == nil || !errors.Is(err, pool.diagnoseErr) {
		t.Fatalf("err = %v, want the diagnosis error wrapped", err)
	}
	if !strings.Contains(err.Error(), "diagnosing") {
		t.Errorf("message = %q, want the diagnosis context", err)
	}
	if res != (EnqueueResult{Queued: 1, Published: 1, Skipped: 0, Invalid: 1, Diagnosed: 0}) {
		t.Errorf("result = %+v, want {Queued:1 Published:1 Invalid:1 Diagnosed:0} — the valid part succeeded", res)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 1 || len(pubs[0].msg.Domains) != 1 {
		t.Errorf("publications = %+v, want the valid row published despite the diagnosis error", pubs)
	}
}

// TestEnqueueBatch_DiagnoseErrorAllInvalid — the failure path of an
// all-poison batch: no move and no publication (there is nothing valid),
// the error is returned with the outcome res — the loop Warns and repeats
// the batch; the filter is idempotent.
func TestEnqueueBatch_DiagnoseErrorAllInvalid(t *testing.T) {
	pool := &enqueueFakePool{diagnoseErr: errors.New("the db blipped")}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		enqueuePoisonRow(),
		{DomainID: 101, ZoneID: 1, Name: strings.Repeat("b", 300)},
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err == nil || !errors.Is(err, pool.diagnoseErr) {
		t.Fatalf("err = %v, want the diagnosis error wrapped", err)
	}
	if res != (EnqueueResult{Queued: 0, Published: 0, Skipped: 0, Invalid: 2, Diagnosed: 0}) {
		t.Errorf("result = %+v, want {Invalid:2 Diagnosed:0} and the rest zero", res)
	}
	calls := pool.enqueuePoolCalls()
	if len(calls) != 1 || calls[0].kind != enqueueKindDiagnose {
		t.Fatalf("DB calls = %v, want a single (failed) diagnosis without a move", calls)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications %d, want 0", len(pubs))
	}
}

// TestEnqueueBatch_DiagnoseErrorRaceAllLost — the diagnosis failed AND the
// whole valid remainder lost the race (RETURNING of the move is empty):
// the diagErr is returned together with the honest outcome — no
// publication of an empty message, the valid rows await the next cycle
// (they remained pending).
func TestEnqueueBatch_DiagnoseErrorRaceAllLost(t *testing.T) {
	pool := &enqueueFakePool{
		diagnoseErr: errors.New("the db blipped"),
		updates:     []*enqueueFakeRows{enqueueFakeRowsOf()}, // the move moved nothing
	}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		enqueuePoisonRow(),
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err == nil || !errors.Is(err, pool.diagnoseErr) {
		t.Fatalf("err = %v, want the diagnosis error (the move lost the race, not an error of its own)", err)
	}
	if res != (EnqueueResult{Queued: 0, Published: 0, Skipped: 1, Invalid: 1, Diagnosed: 0}) {
		t.Errorf("result = %+v, want {Skipped:1 Invalid:1} and the rest zero", res)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("publications %d, want 0 (nothing moved)", len(pubs))
	}
}

// ------------------------------------------------------------ SQL contract

// wantEnqueueInvalidSQL — the terminal diagnosis of poison rows: keys by the
// domains PK, the invalid/done codes as literals, the R13 assignments
// (temp_attempts, the http_published bit, last_checked_at, queued_at = NULL),
// guard re-checks (pending, not deleted) with the length re-check at the
// moment the row is locked (the correlated zone name probe); the zone
// counter deltas pending −n / checked +n by the same statement (T-178).
const wantEnqueueInvalidSQL = "WITH upd AS (UPDATE domains AS t " +
	"SET status = 7::int2, temp_attempts = 0, flags = (t.flags & -5)::int2, " +
	"check_state = 2, queued_at = NULL, last_checked_at = now() " +
	"FROM unnest($1::int2[], $2::int8[]) AS u(zone_id, id) " +
	"WHERE t.zone_id = u.zone_id AND t.id = u.id " +
	"AND t.check_state = 0 AND t.status <> 6 " +
	"AND octet_length(t.label) + 1 + (SELECT octet_length(z.name) FROM zones AS z WHERE z.id = t.zone_id) > 253 " +
	"RETURNING t.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_pending = z.domains_pending - c.n, " +
	"domains_checked = z.domains_checked + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT zone_id FROM upd"

// TestEnqueueInvalid_SQLContract — the exact diagnosis text: the literals are
// tied to the model codes and the schema of migrations 000001/000016 (label,
// the zones reference table, the counter columns).
func TestEnqueueInvalid_SQLContract(t *testing.T) {
	pool := &enqueueFakePool{}
	if _, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{},
		[]DispatchRow{enqueuePoisonRow()}); err != nil {
		t.Fatal(err)
	}
	calls := pool.enqueuePoolCalls()
	if len(calls) != 1 || calls[0].kind != enqueueKindDiagnose {
		t.Fatalf("DB calls = %v, want a single diagnosis", calls)
	}
	if calls[0].sql != wantEnqueueInvalidSQL {
		t.Fatalf("SQL = %q,\nwant %q", calls[0].sql, wantEnqueueInvalidSQL)
	}

	// The text literals must follow the model codes (spec 6.1/6.2): when
	// the codes change the test catches the desync.
	if int16(model.StatusInvalid) != 7 || int16(model.CheckStateDone) != 2 || int16(model.CheckStatePending) != 0 {
		t.Errorf("the model codes changed (pending=%d done=%d invalid=%d): update the literals in wantEnqueueInvalidSQL",
			int16(model.CheckStatePending), int16(model.CheckStateDone), int16(model.StatusInvalid))
	}
	if int16(^int16(model.FlagHTTPPublished)) != -5 {
		t.Errorf("the http_published clear mask = %d, want -5: update the literals in wantEnqueueInvalidSQL",
			int16(^int16(model.FlagHTTPPublished)))
	}

	// The diagnosis references the schema: the label, last_checked_at and the
	// zones reference table with domains_checked (migration 000001), the
	// pending counter column (migration 000016 — the same check as the move
	// contract in enqueue_test.go).
	up := dispatchReadMigration(t, "000001_init.up.sql")
	for _, col := range []string{"label", "last_checked_at", "CREATE TABLE zones", "domains_checked"} {
		if !strings.Contains(up, col) {
			t.Errorf("migration 000001 has no %q — the diagnosis references it", col)
		}
	}
	mig := dispatchReadMigration(t, "000016_zone_pipeline_counters.up.sql")
	if !strings.Contains(mig, "domains_pending") {
		t.Errorf("migration 000016 has no zones.domains_pending — the diagnosis deltas reference the column")
	}
}

// --- the enqueue loop: the cycle continues -------------------------------

// enqueueInvalidDiagnosedValue — the value of the diagnosis counter in the
// registry (the leaseNullReturnedValue style).
func enqueueInvalidDiagnosedValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_scheduler_enqueue_invalid_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// TestEnqueueLoop_InvalidHeadCycleContinues — the T-309 cycle scenario: the
// first selection returns the zone's head with an over-long label plus a
// valid row, the following ones — only valid rows (the diagnosed row left
// pending). The loop does not stall: both batches are published; the
// diagnosis runs exactly once; the counter grows by the diagnosed row; the
// Warn is in the log. Before T-309 the first batch was an error, the cycle
// repeated it every IdleWait and the publications never happened.
func TestEnqueueLoop_InvalidHeadCycleContinues(t *testing.T) {
	// The zone's head: the label gives the full name of 250 + 1 + 11 = 262
	// octets > 253 (the name is assembled by the selection, dispatch.go).
	reg := metrics.New("scheduler")
	pool := &enqueueFakePool{
		zones: [][]any{{int16(1), "example.com"}},
		dispatches: []*enqueueFakeRows{
			enqueueFakeRowsOf(
				[]any{int16(1), int64(100), strings.Repeat("a", 250)}, // the poison head
				[]any{int16(1), int64(101), "a"},
			),
			enqueueFakeRowsOf(
				[]any{int16(1), int64(102), "b"}, // the queue after the diagnosis
			),
		},
		notify: make(chan struct{}, 64),
	}
	q := &enqueueFakeQueue{notify: make(chan struct{}, 64)}
	var buf strings.Builder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{
		Interval: time.Millisecond,
		IdleWait: time.Hour, // an error pause would stall the test — the batches must succeed
		Log:      slog.New(slog.NewTextHandler(&buf, nil)),
		Metrics:  reg,
	})

	enqueueWaitFor(t, 5*time.Second, func() bool {
		return len(q.enqueueQueuePublishes()) >= 2
	}, "the second batch after the poison head of the zone")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on ctx cancellation")
	}

	pubs := q.enqueueQueuePublishes()
	if len(pubs) < 2 {
		t.Fatalf("publications = %d, want ≥2 — the cycle did not stall on the poison row", len(pubs))
	}
	if d := pubs[0].msg.Domains; len(d) != 1 || d[0].DomainID != 101 || d[0].Name != "a.example.com" {
		t.Errorf("the first publication = %+v, want only the valid a.example.com", d)
	}
	if d := pubs[1].msg.Domains; len(d) != 1 || d[0].DomainID != 102 || d[0].Name != "b.example.com" {
		t.Errorf("the second publication = %+v, want b.example.com", d)
	}

	// The diagnosis ran exactly once — the row left pending (a repeated
	// selection of the same row would run it again every batch).
	diag := 0
	for _, c := range pool.enqueuePoolCalls() {
		if c.kind == enqueueKindDiagnose {
			diag++
		}
	}
	if diag != 1 {
		t.Errorf("diagnosis calls = %d, want 1 (the diagnosed row leaves pending)", diag)
	}
	if got := enqueueInvalidDiagnosedValue(t, reg); got != 1 {
		t.Errorf("the crawler_scheduler_enqueue_invalid_total counter = %v, want 1 (by RETURNING)", got)
	}
	for _, want := range []string{"level=WARN", "unenqueueable rows skipped and terminally diagnosed"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log does not contain %q:\n%s", want, buf.String())
		}
	}
}
