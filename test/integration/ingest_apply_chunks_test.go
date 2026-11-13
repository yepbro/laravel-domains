// Integration tests of applying add in batches (T-205, spec 5.1/3.5): the
// staging PK cursor cuts a batch into chunks of ingest.batch_size, the
// whole application is one transaction. Checked against live PostgreSQL: a
// file larger than several chunks is applied fully, the zone counters
// converge with direct counts over domains, reapplying the same batch (the
// same — with the same batch_id and rows) is a no-op with zero deltas.
// Isolation — as in ingest_test.go: migrations in an own schema
// (migratedSchema).
package integration

import (
	"context"
	"fmt"
	"io"
	"testing"

	"crawler/internal/ingest"
	"crawler/internal/model"
)

// TestIngest_ApplyAddChunked — a zone-less add file (the NRD feed style,
// T-205) larger than three chunks (BatchSize 4) is applied fully: all
// domains in the DB, each zone's counters converge with direct counts over
// domains (the chunk deltas are accumulated and applied by one statement).
func TestIngest_ApplyAddChunked(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t) // BatchSize 4: the COPY and the application are in chunks of 4

	// 11 domains in two zones: 3 application chunks (4 + 4 + 3). The zones
	// are real TLDs (com/org): for a zone-less file the row zone is the
	// PSL suffix (idn.Result.Zone); "crawl.test"/"walk.test" would give a
	// single test zone with compound labels (checked with Normalize) —
	// like the NRD feed, the file mixes zones in one list.
	lines := make([]string, 0, 11)
	for i := 1; i <= 6; i++ {
		lines = append(lines, fmt.Sprintf("chunk%02d.com", i))
	}
	for i := 1; i <= 5; i++ {
		lines = append(lines, fmt.Sprintf("chunk%02d.org", i))
	}

	out := env.process(t, "add/chunks.txt", fileContent(lines...))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("the outcome = %q, want applied", out.Kind)
	}
	if out.Report.Added != 11 || out.Report.Existed != 0 || out.Report.Revived != 0 {
		t.Errorf("the counters = added %d, existed %d, revived %d; want 11/0/0 (the batch applied in full)",
			out.Report.Added, out.Report.Existed, out.Report.Revived)
	}
	if n := env.countOf(t, "domains"); n != 11 {
		t.Errorf("domains = %d, want 11 (no chunk lost)", n)
	}

	// Zone counters against direct counts: the chunk deltas (total and
	// pending) must converge with reality — one zone per row of each chunk.
	rows, err := env.pool.Query(context.Background(), `
		SELECT z.name, z.domains_total, z.domains_pending,
		       (SELECT count(*) FROM domains AS d WHERE d.zone_id = z.id),
		       (SELECT count(*) FROM domains AS d
		        WHERE d.zone_id = z.id AND d.check_state = $1)
		FROM zones AS z ORDER BY z.name`,
		int16(model.CheckStatePending))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var seen int
	for rows.Next() {
		var (
			name                              string
			total, pending, totalDir, pendDir int64
		)
		if err := rows.Scan(&name, &total, &pending, &totalDir, &pendDir); err != nil {
			t.Fatal(err)
		}
		seen++
		if total != totalDir || pending != pendDir {
			t.Errorf("zone %s: the counters total/pending = %d/%d, the direct counts = %d/%d — the batch deltas diverged",
				name, total, pending, totalDir, pendDir)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Errorf("zones = %d, want 2 (com, org)", seen)
	}

	// The batch has left staging (apply → clean up, T-017).
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("rows in ingest_stage = %d, want 0", n)
	}
}

// zoneCounters — the observed counters of all zones (a before/after comparison).
func (e *ingestEnv) zoneCounters(t *testing.T) map[int16][2]int64 {
	t.Helper()
	rows, err := e.pool.Query(context.Background(),
		"SELECT id, domains_total, domains_pending FROM zones ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	m := make(map[int16][2]int64)
	for rows.Next() {
		var id int16
		var c [2]int64
		if err := rows.Scan(&id, &c[0], &c[1]); err != nil {
			t.Fatal(err)
		}
		m[id] = c
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return m
}

// TestIngest_ApplyAddReapplyNoop — reapplying the same batch (T-205, spec
// 3.4): Stage with the same batch_id wipes and copies the batch again, the
// repeated ApplyAdd runs into all-out conflicts — Inserted 0, the zone
// deltas are zero (the counters unchanged), staging is cleaned.
func TestIngest_ApplyAddReapplyNoop(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)
	ctx := context.Background()

	// The zones — as the pipeline's EnsureZone (T-014).
	store := ingest.NewZoneStore(env.pool)
	zc, err := ingest.EnsureZone(ctx, store, "crawl.test")
	if err != nil {
		t.Fatal(err)
	}
	zw, err := ingest.EnsureZone(ctx, store, "walk.test")
	if err != nil {
		t.Fatal(err)
	}

	// A batch of 10 rows in two zones in chunks of 4 (3 application chunks).
	id, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	batch := ingest.Batch{ID: id, Action: model.ActionAdd}
	rows := make([]ingest.Row, 10)
	for i := range rows {
		zone := zc.ID
		if i >= 6 {
			zone = zw.ID
		}
		rows[i] = ingest.Row{Label: fmt.Sprintf("row%02d", i+1), ZoneID: zone, LineNo: int64(i + 1)}
	}
	stage := func() (int64, error) {
		i := 0
		return ingest.Stage(ctx, env.pool, batch, 4, func() (ingest.Row, error) {
			if i == len(rows) {
				return ingest.Row{}, io.EOF
			}
			r := rows[i]
			i++
			return r, nil
		})
	}

	staged, err := stage()
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if staged != 10 {
		t.Fatalf("staged rows = %d, want 10", staged)
	}
	res, err := ingest.ApplyAdd(ctx, env.pool, batch, 4)
	if err != nil {
		t.Fatalf("ApplyAdd: %v", err)
	}
	if res.Inserted != 10 || res.Revived != 0 || res.Existing != 0 {
		t.Fatalf("the first application = %+v, want Inserted 10 (3 chunks)", res)
	}
	before := env.zoneCounters(t)

	// Restaging the same batch with the same batch_id (overwriting staging)
	// and reapplying: everything is already in the DB.
	if _, err := stage(); err != nil {
		t.Fatalf("Stage again: %v", err)
	}
	res2, err := ingest.ApplyAdd(ctx, env.pool, batch, 4)
	if err != nil {
		t.Fatalf("ApplyAdd again: %v", err)
	}
	if res2.Inserted != 0 || res2.Revived != 0 || res2.Existing != 10 {
		t.Errorf("the repeat application = %+v, want a no-op: Inserted/Revived 0, Existing 10", res2)
	}
	after := env.zoneCounters(t)
	if len(after) != len(before) {
		t.Fatalf("zones = %d after the repeat, were %d", len(after), len(before))
	}
	for id, c := range before {
		if after[id] != c {
			t.Errorf("zone %d: the counters changed %v → %v — the repeat must be a no-op (zero deltas)", id, c, after[id])
		}
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("rows in ingest_stage = %d, want 0 (the batch cleaned)", n)
	}
	if n := env.countOf(t, "domains"); n != 10 {
		t.Errorf("domains = %d, want 10 (the repeat does not duplicate)", n)
	}
}
