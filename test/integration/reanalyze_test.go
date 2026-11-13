// Integration tests of the tick pass of cursor-time re-analysis
// (T-193; the TZ_slow_reanalyze_tick.md spec §7.2) against live
// PostgreSQL: the hourly tick must read a fresh slice with the
// composite cursor (checked_at, PK) backed by
// subdomain_results_checked_idx (T-171), the start anchor is
// now() − 24 h (the older residual class is the territory of the full
// drain), the cursor survives passes; the full drain on a rules change
// stays the from-scratch PK form and finds stale rows outside the
// window. Tail bucket guard (T-204): a tick without mature tails does
// not read the window at all — the reanalyze: buckets lead the
// record/render increments, the stamp decrement and the monitor
// reconciliation (the FILTER positions of the live window); the bucket
// scenario — test/integration/counters_test.go. Unit level (SQL
// contracts, the anchor, cursor advancement, the loop on fakes) —
// internal/analyzer/reanalyze_test.go. The call level — library-level,
// without starting services (the render_requeue_test.go style);
// isolation — withTestSchema (search_path in the DSN; ALL connections
// of the test — one pool of that schema), publication — a capturing
// queue stub (the counters_test.go style): the selection and
// publication are checked, not Redis. Run — make test-integration
// (after merging, from the main tree).
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
)

// reanQ — a queue.Queue: records Result publications to q:analyze
// (count and content only; Redis is not needed — the stream is checked
// by e2e/acceptance).
type reanQ struct {
	publishes []queue.Result
}

var _ queue.Queue = (*reanQ)(nil)

func (q *reanQ) Publish(_ context.Context, stream string, msg queue.Message) (string, error) {
	if stream != queue.StreamAnalyze {
		return "", fmt.Errorf("reanQ: publishing to %s, need %s", stream, queue.StreamAnalyze)
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("reanQ: publishing %T, need queue.Result", msg)
	}
	q.publishes = append(q.publishes, res)
	return fmt.Sprintf("entry-%d", len(q.publishes)), nil
}

func (q *reanQ) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	panic("reanQ: Consume is not used")
}

func (q *reanQ) Ack(context.Context, string, string, ...string) error {
	panic("reanQ: Ack is not used")
}

// reanSeedRow — a subdomain_results row of one domain: the PK differs
// by subdomain; version nil — NULL (never analyzed, T-058), otherwise
// the set stamp; age — the age of the checked_at mark (whole hours,
// make_interval — deterministic input without parsing interval
// strings).
func reanSeedRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool,
	zone int16, domain int64, sub int16, version *int32, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO subdomain_results
		 (zone_id, domain_id, subdomain_id, scheme, checked_at, rules_version)
		 VALUES ($1, $2, $3, 2, now() - make_interval(hours => $4), $5)`,
		zone, domain, sub, int(age/time.Hour), version); err != nil {
		t.Fatalf("insert subdomain_results (sub %d): %v", sub, err)
	}
}

// reanBasketValue — the value of the re-analysis tail bucket of the
// given hour age (T-204; the DB computes the hour name with the same
// expression as the pass operators).
func reanBasketValue(ctx context.Context, t *testing.T, pool *pgxpool.Pool, age time.Duration) int64 {
	t.Helper()
	var v int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'reanalyze:' || to_char(date_trunc('hour', now() - make_interval(hours => $1)), 'YYYY-MM-DD\"T\"HH24')",
		int(age/time.Hour)).Scan(&v); err != nil {
		t.Fatalf("tail bucket (%v ago): %v", age, err)
	}
	return v
}

// reanSkipHourBoundary — the tail bucket scenarios are tied to the
// "previous hour" from now() (the seed and the reconciliation window):
// a calendar hour change in the middle of the test shifts the window —
// the seed becomes two hours old, reconciliation does not see it, the
// asserts fail. Right at the hour boundary the test is skipped: ~1% of
// runs is cheaper than a rare flake (review T-204, P3-4).
func reanSkipHourBoundary(t *testing.T) {
	t.Helper()
	if left := time.Until(time.Now().Truncate(time.Hour).Add(time.Hour)); left < 45*time.Second {
		t.Skipf("calendar hour boundary in %v — the previous-hour window is unstable", left)
	}
}

// TestReanalyze_TickCursor — the §7.2 scenario with the tail bucket
// guard (T-204): the monitor pass materializes the buckets with the
// live window (a fresh row without analysis lands in the previous-hour
// bucket), the guard opens — the tick walks the time form and publishes
// the tail; the analyzed row is not selected; a repeated tick does not
// re-select (the cursor is past the row, not the version); a rules
// change — the full drain from scratch finds the old row outside the
// lookback too. Plus the EXPLAIN contract: the composite cursor
// predicate is served by the subdomain_results_checked_idx index (on a
// toy volume a seq scan is cheaper — hence enable_seqscan = off: the
// indexability of the predicate is checked, not the cost-based choice;
// the cost — the T-177 journal in production).
func TestReanalyze_TickCursor(t *testing.T) {
	skipUnlessIntegration(t)
	reanSkipHourBoundary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("applying migrations to the isolated schema: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the test schema: %v", err)
	}
	t.Cleanup(pool.Close)

	// Reference data: a zone + a domain; three rows of one domain (the
	// PK — the subdomain):
	// fresh — a new episode without analysis (NULL, 1 h ago — the
	//   bucket hour of the monitor pass live window: the guard must see
	//   it);
	// ancient — a stale version 7 three days ago: outside the lookback
	//   anchor, the territory of the full drain only (§4.2);
	// done — analyzed by the current version (1 h ago): not selected.
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('reanalyze.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var domain int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'example') RETURNING id", zone,
	).Scan(&domain); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	const (
		subFresh   int16 = 1
		subAncient int16 = 2
		subDone    int16 = 3
		vCurrent         = int32(42)
		vOld             = int32(7)
	)
	reanSeedRow(ctx, t, pool, zone, domain, subFresh, nil, time.Hour)
	old := vOld
	reanSeedRow(ctx, t, pool, zone, domain, subAncient, &old, 72*time.Hour)
	current := vCurrent
	reanSeedRow(ctx, t, pool, zone, domain, subDone, &current, time.Hour)

	// The reconciliation pass (T-204): the tail buckets are materialized
	// by the live window aggregate — fresh (NULL, an hour ago) lands in
	// the previous-hour bucket, the tick guard opens; done (the current
	// version) and ancient (outside the window) are not counted in any
	// bucket.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if got := reanBasketValue(ctx, t, pool, time.Hour); got != 1 {
		t.Fatalf("previous-hour tail bucket = %d, want 1 (the fresh NULL row)", got)
	}

	q := &reanQ{}
	var cursor analyzer.ReanalyzeCursor

	t.Run("tick from the anchor picks up a fresh unanalyzed row", func(t *testing.T) {
		res, err := analyzer.ReanalyzeTick(ctx, pool, q, vCurrent, &cursor, analyzer.ReanalyzeOptions{})
		if err != nil {
			t.Fatalf("ReanalyzeTick: %v", err)
		}
		// fresh — yes; done (the current version) and ancient (outside
		// the window) — no.
		if res.Selected != 1 || res.Published != 1 {
			t.Fatalf("outcome = %+v, want 1/1 (only the fresh NULL)", res)
		}
		got := q.publishes[0]
		if got.ZoneID != zone || got.DomainID != domain || got.SubdomainID != subFresh ||
			got.Variant != model.VariantMain || got.Name != "example.reanalyze.test" {
			t.Errorf("publication = %+v, want the fresh row key", got)
		}
	})

	t.Run("a repeated tick does not re-select", func(t *testing.T) {
		// The row is still not stamped (rules_version NULL): the cursor
		// excludes it, not the version — the window is exhausted (the
		// guard is still open by the bucket: the selection runs, there
		// are no publications).
		res, err := analyzer.ReanalyzeTick(ctx, pool, q, vCurrent, &cursor, analyzer.ReanalyzeOptions{})
		if err != nil {
			t.Fatalf("repeat ReanalyzeTick: %v", err)
		}
		if res.Selected != 0 || res.Published != 0 || len(q.publishes) != 1 {
			t.Fatalf("repeat outcome = %+v (%d publications), want 0/0/1 — no re-selection",
				res, len(q.publishes))
		}
	})

	t.Run("rules change — the from-scratch drain finds stale rows outside the lookback", func(t *testing.T) {
		// The PK form from the zero cursor: fresh (NULL) and ancient
		// (vOld) — stale against the same current version, done — not.
		res, err := analyzer.ReanalyzeDrain(ctx, pool, q, vCurrent, nil, analyzer.ReanalyzeOptions{})
		if err != nil {
			t.Fatalf("ReanalyzeDrain: %v", err)
		}
		if res.Selected != 2 || res.Published != 2 {
			t.Fatalf("drain = %+v, want 2/2 (fresh + ancient outside the lookback)", res)
		}
		subs := map[int16]bool{}
		for _, m := range q.publishes[1:] {
			subs[m.SubdomainID] = true
		}
		if !subs[subAncient] {
			t.Errorf("the drain did not find ancient (subdomain %d) — outside the lookback", subAncient)
		}
		if subs[subDone] {
			t.Errorf("the drain selected done (the current version) — it must not be selected")
		}
	})

	t.Run("the cursor predicate is served by subdomain_results_checked_idx", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		// On three rows a seq scan is cheaper than the index — disable it
		// and check the indexability of the predicate (the §4.1 shape),
		// not the planner's cost-based choice: at production volume the
		// index window is fractions of a second against 19.9 s of the
		// full scan (spec §2).
		if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			t.Fatalf("SET LOCAL: %v", err)
		}
		// The time form with literals (parameters in EXPLAIN require a
		// prepared statement — the rqBatchSQL style): the anchor —
		// now() − 24 h, the zero PK part.
		explain := "EXPLAIN (COSTS OFF) " + fmt.Sprintf(
			"SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.checked_at "+
				"FROM subdomain_results AS r "+
				"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id "+
				"WHERE r.rules_version IS DISTINCT FROM %d::int4 "+
				"AND d.status <> %d::int2 "+
				"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) "+
				"> (now() - interval '%d hours', 0::int2, 0::int8, 0::int2, 0::int2) "+
				"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant "+
				"LIMIT 5000",
			vCurrent, int16(model.StatusDeleted),
			int(analyzer.DefaultReanalyzeLookback/time.Hour))
		rows, err := tx.Query(ctx, explain)
		if err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("EXPLAIN: reading: %v", err)
			}
			plan.WriteString(line)
			plan.WriteByte('\n')
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		// The parent index subdomain_results_checked_idx (migration 0014)
		// is partitioned: on the DEFAULT leaf the instance materializes
		// as subdomain_results_default_checked_at_idx (and
		// <leaf>_checked_at_idx on zonal ones) — the assert matches the
		// name suffix, invariant to the split.
		if !strings.Contains(plan.String(), "_checked_at_idx") {
			t.Errorf("the composite cursor predicate is not served by the leaf instance of subdomain_results_checked_idx:\n%s",
				plan.String())
		}
	})
}
