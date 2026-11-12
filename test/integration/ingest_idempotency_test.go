// Integration tests of domain-level ingest idempotence (T-097, option D;
// spec 3.4, 5.1): redelivery and reprocessing of the same file do not
// change the domain state — the sha256 file dedup is only an optimization,
// the correctness is guaranteed by the operations over domains (ON CONFLICT
// by (zone_id, label), the revive guard by status = deleted, the unguarded
// delete UPDATE, the refcount decrease strictly by the actually deleted
// references).
//
// The "the same file processed twice" scenario is modeled by a failure in
// the window between applying and writing the report (at-least-once, spec
// 3.4): the journal row is rolled back, the file returns to incoming — the
// pipeline applies it again, and the DB state (domains, staging, journal)
// converges to the same result. The real convergence against a live DB is
// the subject of these checks; the unit level (the operation order,
// counters on fakes) — internal/ingest/idempotency_test.go.
package integration

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"crawler/internal/ingest"
	"crawler/internal/model"
)

// domainSnapshot — the full observed state of the schema's domains in a
// deterministic order: comparing the "before/after" snapshots of
// reprocessing is the check that the DB state is unchanged (T-097).
func (e *ingestEnv) domainSnapshot(t *testing.T) []string {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `
		SELECT z.name, d.label, d.status, d.check_state, d.flags,
		       coalesce(d.queued_at::text, '-'),
		       coalesce(d.last_checked_at::text, '-')
		FROM domains AS d JOIN zones AS z ON z.id = d.zone_id
		ORDER BY z.name, d.label`)
	if err != nil {
		t.Fatalf("снимок доменов: %v", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name, label, queued, checked string
		var status, checkState, flags int16
		if err := rows.Scan(&name, &label, &status, &checkState, &flags, &queued, &checked); err != nil {
			t.Fatalf("снимок доменов: чтение: %v", err)
		}
		out = append(out, fmt.Sprintf("%s/%s status=%d check=%d flags=%d queued=%s checked=%s",
			name, label, status, checkState, flags, queued, checked))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("снимок доменов: %v", err)
	}
	return out
}

// dropJournal imitates a failure in the window between applying and writing
// the report: the ingest_files row is not committed (rolled back), the file
// will return to incoming by redelivery.
func (e *ingestEnv) dropJournal(t *testing.T) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(), "DELETE FROM ingest_files"); err != nil {
		t.Fatalf("откат журнала ingest_files: %v", err)
	}
}

// assertStateUnchanged — the DB state after reprocessing matches the
// expectation: the domains are unchanged, staging is empty, the journal has
// exactly wantFiles rows.
func (e *ingestEnv) assertStateUnchanged(t *testing.T, want []string, wantFiles int) {
	t.Helper()
	if got := e.domainSnapshot(t); !reflect.DeepEqual(got, want) {
		t.Errorf("состояние доменов изменилось:\nполучено:\n%s\nwant:\n%s",
			joinLines(got), joinLines(want))
	}
	if n := e.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage = %d, want 0 (партии покидают staging)", n)
	}
	if n := e.countOf(t, "ingest_files"); n != wantFiles {
		t.Errorf("записей в ingest_files = %d, want %d", n, wantFiles)
	}
}

// joinLines — a snapshot as multi-line text for readable messages.
func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out
}

// TestIngest_ReapplySameFileConverges — the main T-097 scenario: "the same
// file processed twice — the DB state is unchanged". A failure after
// applying, before writing the report → a redelivery of the same content →
// a reapplication. Add converges via ON CONFLICT (everything "was already
// there"), delete — by moving the same rows to deleted again (the counters
// are stable); the report ends up exactly one.
func TestIngest_ReapplySameFileConverges(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	// --- add: the first processing.
	add := fileContent("example.crawl.test", "shop.crawl.test")
	if out := env.process(t, "crawl.test/add/list.txt", add); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}
	afterAdd := env.domainSnapshot(t)
	if len(afterAdd) != 2 {
		t.Fatalf("доменов после add = %d, want 2", len(afterAdd))
	}

	// A failure in the application/report window: the journal is empty, the file is delivered again.
	env.dropJournal(t)
	out := env.process(t, "crawl.test/add/list.txt", add)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторный add: итог = %q, want applied (дубль по sha256 не находится — журнал пуст)", out.Kind)
	}
	// The reapplication is a no-op for the domains: the counters reflect
	// the current state (everything "was already there"), not the first
	// attempt.
	if out.Report.Added != 0 || out.Report.Revived != 0 || out.Report.Existed != 2 {
		t.Errorf("повторный add: added %d, revived %d, existed %d; want 0/0/2",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}
	env.assertStateUnchanged(t, afterAdd, 1)

	// --- delete: the first processing (one domain exists, one does not).
	del := fileContent("example.crawl.test", "ghost.crawl.test")
	if out := env.process(t, "crawl.test/delete/x.txt", del); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}
	afterDelete := env.domainSnapshot(t)

	// The same failure and redelivery: the already deleted domain stays
	// deleted (an UPDATE without a status guard — the "deleted" counter is
	// stable), the missing one — "not found".
	env.dropJournal(t)
	out = env.process(t, "crawl.test/delete/x.txt", del)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторный delete: итог = %q, want applied", out.Kind)
	}
	if out.Report.Deleted != 1 || out.Report.NotFound != 1 {
		t.Errorf("повторный delete: deleted %d, not_found %d; want 1/1 (счётчики стабильны)",
			out.Report.Deleted, out.Report.NotFound)
	}
	env.assertStateUnchanged(t, afterDelete, 1)
}

// TestIngest_ReviveCycleIdempotent — the delete → add (revival) cycle of
// the same domain converges: revive returns a deleted domain to
// new/pending; re-adding an already revived one is a no-op (the status =
// deleted guard); a repeated delete returns it to deleted without changing
// the other columns.
func TestIngest_ReviveCycleIdempotent(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}

	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("example.crawl.test", "# снятие")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}
	afterDelete := env.domainSnapshot(t)
	d, ok := env.domain(t, "crawl.test", "example")
	if !ok || d.status != int16(model.StatusDeleted) {
		t.Fatalf("после delete: домен = %+v, want status deleted", d)
	}

	// The revival: revived, new/pending, no lease.
	out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test", "# возврат"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("возврат: итог = %q, want applied", out.Kind)
	}
	if out.Report.Added != 1 || out.Report.Revived != 1 || out.Report.Existed != 0 {
		t.Errorf("возврат: added %d, revived %d, existed %d; want 1/1/0",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}
	d, ok = env.domain(t, "crawl.test", "example")
	if !ok || d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
		t.Fatalf("после возврата: домен = %+v, want new/pending", d)
	}
	if d.queuedAt != nil {
		t.Error("после возврата queued_at не сброшен")
	}
	afterRevive := env.domainSnapshot(t)

	// Re-adding the already revived domain — a no-op: the state is the
	// same, the counters say "was already there".
	out = env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test", "# снова"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторный add: итог = %q, want applied", out.Kind)
	}
	if out.Report.Added != 0 || out.Report.Revived != 0 || out.Report.Existed != 1 {
		t.Errorf("повторный add: added %d, revived %d, existed %d; want 0/0/1",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}
	env.assertStateUnchanged(t, afterRevive, 4)

	// A repeated delete — the same state as after the first one.
	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("example.crawl.test", "# повторное снятие")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторное delete: итог = %q, want applied", out.Kind)
	}
	env.assertStateUnchanged(t, afterDelete, 5)
}

// TestIngest_SameContentCrossActionApplied — an undo with the same list
// (T-113; overrides the T-097 pinning "the dedup contract boundary"): the
// dedup is relaxed to an exact redelivery (zone_id, action, sha256) — the
// same list from delete/ after add/ is NOW processed: the domains are
// deleted, the journal gains a delete row. A subsequent exact redelivery
// of the add file with the same content is a duplicate (skip).
func TestIngest_SameContentCrossActionApplied(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	content := fileContent("example.crawl.test", "shop.crawl.test")
	if out := env.process(t, "crawl.test/add/list.txt", content); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}

	// An undo with the same list: the action is different — there is no
	// exact redelivery, the file is applied, the domains are deleted.
	out := env.process(t, "crawl.test/delete/list.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("тот же список из delete/: итог = %q, want applied (T-113: другое действие — не дубль)", out.Kind)
	}
	if out.Report.Deleted != 2 {
		t.Errorf("undo: deleted = %d, want 2 (оба домена списка)", out.Report.Deleted)
	}
	for _, label := range []string{"example", "shop"} {
		if d, ok := env.domain(t, "crawl.test", label); !ok || d.status != int16(model.StatusDeleted) {
			t.Errorf("после undo: домен %s = %+v, want status deleted", label, d)
		}
	}
	if n := env.countOf(t, "ingest_files"); n != 2 {
		t.Errorf("записей в ingest_files = %d, want 2 (add и undo-delete)", n)
	}

	// An exact redelivery of the add file with the same content — a
	// duplicate: no application happens, the journal does not grow.
	if out := env.process(t, "crawl.test/add/list.txt", content); out.Kind != ingest.OutcomeDuplicate {
		t.Fatalf("повторный add тем же содержимым: итог = %q, want duplicate", out.Kind)
	}
	if n := env.countOf(t, "ingest_files"); n != 2 {
		t.Errorf("записей в ingest_files = %d, want 2 (дубль не отчётится)", n)
	}
}

// TestIngest_CleanupIdempotent — the background cleanup of deleted domains
// is idempotent (T-020, spec 13.1/8): a repeat pass does not select the
// purged ones and does not touch the refcount; the decrease is strictly by
// the actually deleted references; the full cycle delete → cleanup →
// revive → new results → delete → cleanup converges to consistent
// refcounts.
func TestIngest_CleanupIdempotent(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)
	ctx := context.Background()

	if out := env.process(t, "crawl.test/add/list.txt",
		fileContent("a.crawl.test", "b.crawl.test", "c.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}

	// The domain ids for seeding the results.
	domainKey := func(label string) (int16, int64) {
		var zoneID int16
		var id int64
		if err := env.pool.QueryRow(ctx, `
			SELECT d.zone_id, d.id FROM domains AS d
			JOIN zones AS z ON z.id = d.zone_id
			WHERE z.name = $1 AND d.label = $2`, "crawl.test", label,
		).Scan(&zoneID, &id); err != nil {
			t.Fatalf("домен %s: %v", label, err)
		}
		return zoneID, id
	}
	za, a := domainKey("a")
	zb, b := domainKey("b")
	zc, c := domainKey("c")
	if za != zb || zb != zc {
		t.Fatalf("зоны доменов разошлись: %d/%d/%d", za, zb, zc)
	}

	// The content: h1 is held by a, b, c (refcount 3), h2 — by b only (refcount 1).
	h1 := make([]byte, 16)
	h1[15] = 0x01
	h2 := make([]byte, 16)
	h2[15] = 0x02
	if _, err := env.pool.Exec(ctx, `
		INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount)
		VALUES ($1, 1, 0, 10, 20, 3), ($2, 1, 16, 5, 5, 1)`, h1, h2); err != nil {
		t.Fatalf("посев contents: %v", err)
	}

	// The results: a → h1; b → h1 and h2; c → h1 (a live domain — not deleted).
	addResult := func(domainID int64, subID int16, hash []byte) {
		if _, err := env.pool.Exec(ctx, `
			INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, checked_at, content_hash)
			VALUES ($1, $2, $3, 2, now(), $4)`, za, domainID, subID, hash); err != nil {
			t.Fatalf("посев результата %d/%d: %v", domainID, subID, err)
		}
	}
	addResult(a, 0, h1)
	addResult(b, 0, h1)
	addResult(b, 1, h2)
	addResult(c, 0, h1)

	// refcounts — the observed state of contents.
	refcounts := func() map[string]int {
		rows, err := env.pool.Query(ctx, "SELECT encode(hash, 'hex'), refcount FROM contents ORDER BY hash")
		if err != nil {
			t.Fatalf("чтение refcount: %v", err)
		}
		defer rows.Close()
		out := map[string]int{}
		for rows.Next() {
			var h string
			var n int
			if err := rows.Scan(&h, &n); err != nil {
				t.Fatalf("чтение refcount: %v", err)
			}
			out[h] = n
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("чтение refcount: %v", err)
		}
		return out
	}

	// Deleting a and b: their results (3 rows) must go away, the references
	// withdrawn; the reference of the live c is kept.
	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("a.crawl.test", "b.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}

	first, err := ingest.CleanupOnce(ctx, env.pool, ingest.CleanupOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("первый проход очистки: %v", err)
	}
	if first.Domains != 2 || first.Results != 3 || first.Contents != 2 {
		t.Errorf("первый проход = %+v, want 2 домена, 3 результата, 2 контента", first)
	}
	want := map[string]int{
		fmt.Sprintf("%x", h1): 1, // the references of a and b withdrawn, c alive
		fmt.Sprintf("%x", h2): 0, // the only reference (b) withdrawn
	}
	if got := refcounts(); !reflect.DeepEqual(got, want) {
		t.Errorf("refcount после очистки = %v, want %v", got, want)
	}

	// A repeat pass — a no-op: the purged ones are not selected, the
	// refcount is not decreased again (idempotence, spec 3.4).
	second, err := ingest.CleanupOnce(ctx, env.pool, ingest.CleanupOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("второй проход очистки: %v", err)
	}
	if second != (ingest.CleanupResult{}) {
		t.Errorf("второй проход = %+v, want no-op", second)
	}
	if got := refcounts(); !reflect.DeepEqual(got, want) {
		t.Errorf("refcount после повтора = %v, want прежние %v", got, want)
	}

	// The full cycle: reviving a, the worker writes a new result with a
	// reference to h2 (refcount +1), the repeated delete and cleanup
	// withdraw it back — the counters converge.
	if out := env.process(t, "crawl.test/add/one.txt",
		fileContent("a.crawl.test", "# возврат")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("возврат: итог = %q, want applied", out.Kind)
	}
	addResult(a, 0, h2)
	if _, err := env.pool.Exec(ctx,
		"UPDATE contents SET refcount = refcount + 1 WHERE hash = $1", h2); err != nil {
		t.Fatalf("инкремент refcount: %v", err)
	}
	if got := refcounts(); got[fmt.Sprintf("%x", h2)] != 1 {
		t.Errorf("refcount h2 после нового результата = %v, want 1", got)
	}

	if out := env.process(t, "crawl.test/delete/y.txt",
		fileContent("a.crawl.test", "# повторное снятие")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторное delete: итог = %q, want applied", out.Kind)
	}
	third, err := ingest.CleanupOnce(ctx, env.pool, ingest.CleanupOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("третий проход очистки: %v", err)
	}
	if third.Domains != 1 || third.Results != 1 {
		t.Errorf("третий проход = %+v, want 1 домен, 1 результат", third)
	}
	want[fmt.Sprintf("%x", h2)] = 0
	if got := refcounts(); !reflect.DeepEqual(got, want) {
		t.Errorf("refcount после цикла = %v, want %v", got, want)
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage = %d, want 0", n)
	}
}
