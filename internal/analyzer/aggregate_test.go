// Unit tests of the domain-level aggregation (T-072; spec 5.8 "at the
// domain level — an aggregate (the maximum score, the union range, the
// subdomain mask)", §6.1 laravel_domains, §13.1, 3.4, 3.5) — on the
// worker_test.go fake pool extended with live laravel_domains semantics
// (the SELECT of the domain's confident hits, upsert/DELETE by PK).
// Checked: the SQL contracts and parameter types, the maximum score, the
// version range union (NULL bounds and the T-070 conflict, the numeric
// version order), the subdomain bit mask, the domain row upsert/delete,
// repeat delivery idempotency, a deleted domain, step failures and ctx
// cancellation, "no confident hits — no row".
package analyzer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- fake pool: laravel_domains (anFakePool extension) ---------------------

// anDomainKey is the laravel_domains PK in the fake.
type anDomainKey struct {
	zone   int16
	domain int64
}

// anDomain is a laravel_domains row written by aggregateUpsertSQL.
type anDomain struct {
	score        float32
	verMin       *string
	verMax       *string
	foundOnMask  model.DNSMask
	rulesVersion int32
	updatedAt    time.Time
}

// Query implements aggregateSelectSQL in the fake transaction: the
// domain's confident hits (is_laravel) in PK order — deterministically,
// like a PostgreSQL partition leaf; the transaction's own writes (this
// delivery's findings) are visible — the fake's Exec changes the state
// directly, Rollback restores the snapshot. The aggScanErr/aggRowsErr
// and onClose injections are for the T-M10 mutation tests (row
// Scan/Err failures, Close control).
func (t *anFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != aggregateSelectSQL {
		return nil, fmt.Errorf("anFakeTx: unexpected SQL: %s", sql)
	}
	if t.pool.aggSelectErr != nil {
		return nil, t.pool.aggSelectErr
	}
	key := anDomainKey{zone: args[0].(int16), domain: args[1].(int64)}
	keys := make([]anRowKey, 0, len(t.pool.findings))
	for k, f := range t.pool.findings {
		if k.zone == key.zone && k.domain == key.domain && f.isLaravel {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].subdomain != keys[j].subdomain {
			return keys[i].subdomain < keys[j].subdomain
		}
		return keys[i].variant < keys[j].variant
	})
	rows := make([][]any, 0, len(keys))
	for _, k := range keys {
		f := t.pool.findings[k]
		rows = append(rows, []any{k.subdomain, f.score, f.verMin, f.verMax})
	}
	t.pool.j.record(anEvAggSelect)
	return &anAggRows{rows: rows, scanErr: t.pool.aggScanErr, err: t.pool.aggRowsErr,
		onClose: func() { t.pool.j.record(anEvAggClose) }}, nil
}

// anAggRows is pgx.Rows with the domain's confident hits (subdomain_id,
// score, ver_min, ver_max); scanErr/err/onClose are T-M10 injections.
type anAggRows struct {
	rows    [][]any
	i       int
	scanErr error
	err     error
	onClose func()
}

func (r *anAggRows) Close() {
	if r.onClose != nil {
		r.onClose()
	}
}
func (r *anAggRows) Err() error                    { return r.err }
func (r *anAggRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *anAggRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *anAggRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *anAggRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.i == 0 || r.i > len(r.rows) || len(dest) != 4 {
		return fmt.Errorf("anAggRows: Scan without Next or %d destinations (need 4)", len(dest))
	}
	row := r.rows[r.i-1]
	// NULL → the nil destination stays untouched (the pgx semantics: a
	// *string for a bound stays nil).
	for i, v := range row {
		if v == nil {
			continue
		}
		switch d := dest[i].(type) {
		case *int16:
			*d = v.(int16)
		case *float32:
			*d = v.(float32)
		case **string:
			*d = v.(*string)
		default:
			return fmt.Errorf("anAggRows: destination[%d] %T", i, dest[i])
		}
	}
	return nil
}

func (r *anAggRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *anAggRows) RawValues() [][]byte    { return nil }
func (r *anAggRows) Conn() *pgx.Conn        { return nil }
func (r *anAggRows) TypeMap() *pgtype.Map   { return nil }

// anDomainSnapshot is a snapshot of the laravel_domains row.
func anDomainSnapshot(p *anFakePool, zone int16, domain int64) (anDomain, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d, ok := p.domains[anDomainKey{zone, domain}]; ok {
		return *d, true
	}
	return anDomain{}, false
}

// anSeedFinding is a ready-made hit of a foreign (already committed)
// episode: old findings need no subdomain_results row of their own.
func anSeedFinding(p *anFakePool, key anRowKey, score float32, verMin, verMax *string, isLaravel bool) {
	p.findings[key] = &anFinding{score: score, verMin: verMin, verMax: verMax, isLaravel: isLaravel, rulesVersion: 999}
}

// anStr is a pointer to a string (a fixture range bound).
func anStr(s string) *string { return &s }

// --- SQL contract ----------------------------------------------------------

// TestAggregate_SQLContract — the exact aggregation SQL text against
// migration 000001 (laravel_domains: PK (zone_id, domain_id), score
// real, ver_min/ver_max text, found_on_mask bigint, rules_version
// integer NOT NULL, updated_at timestamptz NOT NULL) and §13.1 (the is_laravel threshold).
func TestAggregate_SQLContract(t *testing.T) {
	if !strings.Contains(aggregateLockSQL, "pg_advisory_xact_lock($1::int8)") {
		t.Error("lock: no domain advisory lock until the end of the transaction")
	}
	if !strings.Contains(aggregateSelectSQL, "FROM laravel_findings") {
		t.Error("select: the aggregate reads from laravel_findings")
	}
	if !strings.Contains(aggregateSelectSQL, "AND is_laravel") {
		t.Error("select: no confident-hits filter (the laravel_domains §13.1 threshold)")
	}
	if !strings.Contains(aggregateUpsertSQL,
		"ON CONFLICT (zone_id, domain_id) DO UPDATE") {
		t.Error("upsert: no conflict on the laravel_domains PK (§3.5 rewrite)")
	}
	for _, col := range []string{"score", "ver_min", "ver_max", "found_on_mask", "rules_version", "updated_at"} {
		if !strings.Contains(aggregateUpsertSQL, col) {
			t.Errorf("upsert: no %s column (§6.1)", col)
		}
	}
	if !strings.Contains(aggregateUpsertSQL, "$3::float4") {
		t.Error("upsert: score is not cast to real (float4)")
	}
	if !strings.Contains(aggregateUpsertSQL, "$6::int8") {
		t.Error("upsert: found_on_mask is not cast to bigint (the DNSMask mask)")
	}
	if !strings.Contains(aggregateUpsertSQL, "now()") {
		t.Error("upsert: updated_at is not the server transaction time (the record.go style)")
	}
	if !strings.Contains(aggregateDeleteSQL, "DELETE FROM laravel_domains") {
		t.Error("delete: does not delete the laravel_domains row")
	}
}

// --- scanAggregate: range union, mask, maximum -----------------------------

func TestAggregate_ScanSemantics(t *testing.T) {
	cases := []struct {
		name string
		rows [][]any // (subdomain_id, score, ver_min, ver_max)
		want domainAggregate
	}{
		{
			name: "a single hit — the range as is",
			rows: [][]any{{int16(1), float32(0.7), anStr("5.3"), anStr("7.x")}},
			want: domainAggregate{score: 0.7, verMin: anStr("5.3"), verMax: anStr("7.x"),
				foundOnMask: mustMask(t, 1)},
		},
		{
			name: "numeric version order: min(10,5.3)=5.3, max(9.x,11)=11",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("10"), anStr("9.x")},
				{int16(2), float32(0.6), anStr("5.3"), anStr("11")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("5.3"), verMax: anStr("11"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "the maximum score is not at the range's lower bound",
			rows: [][]any{
				{int16(1), float32(0.9), anStr("9.19"), anStr("11")},
				{int16(2), float32(0.95), anStr("5.3"), anStr("7.x")},
			},
			want: domainAggregate{score: 0.95, verMin: anStr("5.3"), verMax: anStr("11"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "a NULL upper bound propagates: the union is unbounded above",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("9.19"), nil},
				{int16(2), float32(0.6), anStr("5.3"), anStr("7.x")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("5.3"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "a single hit's conflict (both NULL, T-070) — the domain range is undefined",
			rows: [][]any{
				{int16(1), float32(0.7), nil, nil},
				{int16(2), float32(0.6), anStr("9.19"), anStr("11")},
			},
			want: domainAggregate{score: 0.7, foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "all hits without bounds — both bounds NULL",
			rows: [][]any{
				{int16(1), float32(0.7), nil, nil},
				{int16(2), float32(0.6), nil, anStr("7.x")},
			},
			want: domainAggregate{score: 0.7, foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "equal bounds in a different notation — the first by PK",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("9.19"), anStr("9.19.0")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("9.19"), verMax: anStr("9.19.0"),
				foundOnMask: mustMask(t, 1)},
		},
		{
			name: "numerically equal bounds of neighboring rows — the first by PK (min and max)",
			rows: [][]any{
				{int16(1), float32(0.7), anStr("9.19"), anStr("9.19.0")},
				{int16(2), float32(0.6), anStr("9.19.0"), anStr("9.19")},
			},
			want: domainAggregate{score: 0.7, verMin: anStr("9.19"), verMax: anStr("9.19.0"),
				foundOnMask: mustMask(t, 1, 2)},
		},
		{
			name: "a negative score (junk in the DB) — the first row sets the maximum",
			rows: [][]any{
				{int16(1), float32(-0.5), nil, nil},
			},
			want: domainAggregate{score: -0.5, foundOnMask: mustMask(t, 1)},
		},
		{
			name: "variants of one subdomain collapse into a bit; bit 62 is allowed",
			rows: [][]any{
				{int16(5), float32(0.61), anStr("10"), anStr("11")},
				{int16(5), float32(0.62), anStr("10"), anStr("11")},
				{int16(62), float32(0.6), anStr("10"), anStr("11")},
			},
			want: domainAggregate{score: 0.62, verMin: anStr("10"), verMax: anStr("11"),
				foundOnMask: mustMask(t, 5, 62)},
		},
		{
			name: "an empty set — an empty aggregate",
			rows: nil,
			want: domainAggregate{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scanAggregate(&anAggRows{rows: tc.rows})
			if err != nil {
				t.Fatalf("scanAggregate: %v", err)
			}
			if got.findings != len(tc.rows) {
				t.Fatalf("findings = %d, want %d", got.findings, len(tc.rows))
			}
			if got.score != tc.want.score {
				t.Errorf("score = %v, want %v (the maximum)", got.score, tc.want.score)
			}
			if got.foundOnMask != tc.want.foundOnMask {
				t.Errorf("found_on_mask = %d (%v), want %d (%v)",
					int64(got.foundOnMask), got.foundOnMask.IDs(),
					int64(tc.want.foundOnMask), tc.want.foundOnMask.IDs())
			}
			if !anStrEq(got.verMin, tc.want.verMin) {
				t.Errorf("ver_min = %v, want %v", anStrDeref(got.verMin), anStrDeref(tc.want.verMin))
			}
			if !anStrEq(got.verMax, tc.want.verMax) {
				t.Errorf("ver_max = %v, want %v", anStrDeref(got.verMax), anStrDeref(tc.want.verMax))
			}
		})
	}
}

// TestAggregate_InvalidSubdomainID — a bit number outside 0..62 (junk in
// the DB) is an error, the mask is not silently corrupted (model.DNSMask).
func TestAggregate_InvalidSubdomainID(t *testing.T) {
	_, err := scanAggregate(&anAggRows{rows: [][]any{{int16(63), float32(0.7), nil, nil}}})
	if err == nil {
		t.Fatal("subdomain_id 63 (bit 63 is the sign bit) — an error is expected")
	}
}

// --- through the worker: transaction, upsert/delete, idempotency -----------

// TestAggregate_ThroughWorker — the end-to-end contract: the domain
// findings (written by the delivery + committed earlier) → an advisory
// lock → the SELECT of confident hits → the laravel_domains upsert
// (the maximum score, the union range, the mask, the set version) →
// COMMIT; the order is per the journal. A foreign domain and weak hits do not enter the aggregate.
func TestAggregate_ThroughWorker(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	// The delivery: subdomain 1 (XSRF + session + welcome) → score ≈ 0.7231,
	// the range [5.3, 7.x], is_laravel.
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])

	// An earlier committed confident hit of subdomain 2: the upper bound
	// is undefined — the union is unbounded above.
	anSeedFinding(f.pool, anRowKey{3, 42, 2, 0}, 0.7, anStr("9.19"), nil, true)
	// A weak hit of subdomain 3 — below the threshold, outside the aggregate (§13.1).
	anSeedFinding(f.pool, anRowKey{3, 42, 3, 0}, 0.3, anStr("4.0"), anStr("4.1"), false)
	// A confident hit of a foreign domain — not included.
	anSeedFinding(f.pool, anRowKey{3, 43, 1, 0}, 0.99, anStr("1.0"), anStr("2.0"), true)

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}

	// The order: findings → lock → read → aggregate → commit; the
	// aggregation rows are closed after the read (the rows.Close cut
	// mutant, T-M10).
	for _, pair := range [][2]anEventKind{
		{anEvUpsert, anEvLock}, {anEvLock, anEvAggSelect},
		{anEvAggSelect, anEvAggClose}, {anEvAggClose, anEvAggUpsert},
		{anEvAggUpsert, anEvCommit},
	} {
		if f.pool.j.indexOf(pair[0]) > f.pool.j.indexOf(pair[1]) {
			t.Fatalf("the order %v after %v is broken: %v", pair[1], pair[0], f.pool.j.snapshot())
		}
	}

	row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain)
	if !ok {
		t.Fatal("the laravel_domains row was not written")
	}
	// The maximum score: noisy-OR(0.25, 0.35, 0.45) ≈ 0.7231 > 0.7.
	wantScore := float32(1 - (1-0.25)*(1-0.35)*(1-0.45))
	if row.score != wantScore {
		t.Errorf("score = %v, want the maximum %v", row.score, wantScore)
	}
	// The union [5.3, 7.x] ∪ [9.19, NULL) = [5.3, NULL).
	if row.verMin == nil || *row.verMin != "5.3" {
		t.Errorf("ver_min = %v, want 5.3 (the min of the lower bounds)", anStrDeref(row.verMin))
	}
	if row.verMax != nil {
		t.Errorf("ver_max = %v, want NULL (the union is unbounded above)", *row.verMax)
	}
	// The mask: subdomains 1 (the delivery) and 2 (earlier); 3 — weak, 43 — foreign.
	if want := mustMask(t, 1, 2); row.foundOnMask != want {
		t.Errorf("found_on_mask = %d (%v), want %d (%v)",
			int64(row.foundOnMask), row.foundOnMask.IDs(), int64(want), want.IDs())
	}
	if row.rulesVersion != 1001 {
		t.Errorf("rules_version = %d, want 1001 (the recalculation's set version)", row.rulesVersion)
	}
	if row.updatedAt.IsZero() {
		t.Error("updated_at is not set")
	}
}

// TestAggregate_NoConfidentDeletesRow — no confident hits: the domain
// row must be gone (the §13.1 threshold), an existing one is deleted
// (§3.5) — both from a weak delivery outcome and from an empty one (the finding removed).
func TestAggregate_NoConfidentDeletesRow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cookies *string
		content []byte
	}{
		{"a weak hit below the threshold", anCookies(t, "XSRF-TOKEN"), nil}, // 0.25 < 0.6
		{"no hits at all", anCookies(t, "PHPSESSID"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
			f.pool.seedResult(anKey, anHeaders(t), tc.cookies, nil)
			// A past episode found Laravel — the domain row exists.
			f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
				score: 0.9, verMin: anStr("5.3"), verMax: anStr("7.x"),
				foundOnMask: mustMask(t, 1, 2), rulesVersion: 999, updatedAt: time.Now(),
			}

			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
				t.Fatal("the laravel_domains row was not deleted (no confident hits)")
			}
			if f.pool.j.indexOf(anEvAggDelete) == -1 {
				t.Fatal("DELETE laravel_domains did not run")
			}
		})
	}
}

// TestAggregate_RepeatDelivery — a repeated delivery is idempotent
// (spec 3.4): the upsert rewrites the same row with the same values.
func TestAggregate_RepeatDelivery(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	anSeedFinding(f.pool, anRowKey{3, 42, 2, 0}, 0.7, anStr("9.19"), anStr("11"), true)

	for i := 0; i < 2; i++ {
		if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
			t.Fatalf("delivery #%d: %v", i+1, err)
		}
	}
	row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain)
	if !ok {
		t.Fatal("the laravel_domains row was not written")
	}
	if want := mustMask(t, 1, 2); row.foundOnMask != want {
		t.Fatalf("found_on_mask = %d, want %d", int64(row.foundOnMask), int64(want))
	}
	if row.verMin == nil || *row.verMin != "5.3" || row.verMax == nil || *row.verMax != "11" {
		t.Fatalf("the range = [%v, %v], want [5.3, 11]",
			anStrDeref(row.verMin), anStrDeref(row.verMax))
	}
	if n := countEvents(f.pool.j.snapshot(), anEvAggUpsert); n != 1 {
		t.Fatalf("agg upsert ran %d times (want 1 — a same-version no-op mark repeat, T-204)", n)
	}
}

// TestAggregate_DomainDeleted — the domain is deleted (§8/§13.1): the
// stamp guard → 0 rows → no write; the aggregation does not run at
// all, the existing domain row is untouched (the laravel_* background
// cleanup will purge it, the post-T-020 plan).
func TestAggregate_DomainDeleted(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	stale := anDomain{score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now()}
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &stale
	f.pool.mutateBeforeTx = func(p *anFakePool) {
		p.statuses[anKey] = int16(model.StatusDeleted)
	}

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil || wrote {
		t.Fatalf("ProcessResult = %v, %v; want success without a write", wrote, err)
	}
	if f.pool.j.indexOf(anEvLock) != -1 || f.pool.j.indexOf(anEvAggUpsert) != -1 || f.pool.j.indexOf(anEvAggDelete) != -1 {
		t.Fatal("the aggregation ran for a deleted domain")
	}
	if row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); !ok || row.rulesVersion != 999 {
		t.Fatal("the existing laravel_domains row was touched for a deleted domain")
	}
}

// TestAggregate_Errors — a failure of each aggregation step: a delivery
// error (no XACK), the state rolls back — the findings and the domain
// row; want is the exact wrap substring (text pins against the
// return-cut mutants, T-M10); fresh is the delivery of a fresh HTTP
// episode (the appeared/lost history branches, T-118).
func TestAggregate_Errors(t *testing.T) {
	seed := func(t *testing.T) *anFixture {
		f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
		hash := anTestHash(7)
		f.store.blocks[hash] = anPageHTML()
		f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
		stale := anDomain{score: 0.5, foundOnMask: mustMask(t, 2), rulesVersion: 999, updatedAt: time.Now()}
		f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &stale
		return f
	}
	cases := []struct {
		name   string
		inj    func(f *anFixture)
		want   string
		fresh  bool
		hadRow bool // whether the domain row existed before the failure (the appeared branch starts without it)
	}{
		{"domain lock", func(f *anFixture) { f.pool.lockErr = errors.New("lock boom") }, "", false, true},
		{"reading hits", func(f *anFixture) { f.pool.aggSelectErr = errors.New("select boom") }, "", false, true},
		{"reading hits (row Scan)", func(f *anFixture) { f.pool.aggScanErr = errors.New("scan boom") },
			"reading hits", false, true},
		{"reading hits (rows Err)", func(f *anFixture) { f.pool.aggRowsErr = errors.New("rows boom") },
			"reading hits", false, true},
		{"junk subdomain in findings", func(f *anFixture) {
			anSeedFinding(f.pool, anRowKey{3, 42, 63, 0}, 0.7, nil, nil, true)
		}, "reading hits", false, true},
		{"reading domain row", func(f *anFixture) { f.pool.existsErr = errors.New("exists boom") },
			"reading domain row", false, true},
		{"writing domain row", func(f *anFixture) { f.pool.aggUpsertErr = errors.New("agg boom") },
			"writing domain row: agg boom", false, true},
		{"writing domain row: RowsAffected", func(f *anFixture) { f.pool.aggUpsertRowsAff = 2 },
			"writing domain row: 2 rows affected", false, true},
		{"deleting domain row", func(f *anFixture) {
			// A delivery outcome without hits → the delete branch.
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
			f.pool.aggUpsertErr = errors.New("must not be called")
			f.pool.aggDeleteErr = errors.New("agg del boom")
		}, "", false, true},
		{"history write (lost)", func(f *anFixture) {
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil) // no signs
			f.pool.aggHistErr = errors.New("hist boom")
		}, "history write (lost)", true, true},
		{"history write (appeared)", func(f *anFixture) {
			delete(f.pool.domains, anDomainKey{anKey.zone, anKey.domain}) // there was no row
			f.pool.aggHistErr = errors.New("hist boom")
		}, "history write (appeared)", true, false},
		{"commit", func(f *anFixture) { f.pool.commitErr = errors.New("commit boom") }, "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := seed(t)
			tc.inj(f)
			task := anTask
			task.Fresh = tc.fresh
			if _, err := f.w.ProcessResult(context.Background(), task); err == nil {
				t.Fatal("a delivery error is expected")
			} else if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want the substring %q", err, tc.want)
			}
			// The rollback: the domain row's previous state survived.
			row, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain)
			if tc.hadRow != ok || (ok && row.rulesVersion != 999) {
				t.Fatalf("the domain row changed on failure: %+v (ok=%v)", row, ok)
			}
			if _, ok := anFindingSnapshot(f.pool, anKey); ok {
				t.Fatal("findings were written on aggregation failure")
			}
		})
	}
}

// TestAggregate_CtxCancelled — a ctx cancellation aborts the aggregation:
// an error, nothing is written (a graceful shutdown, spec 3.4).
func TestAggregate_CtxCancelled(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	tx, err := f.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	unfound, err := aggregateDomain(ctx, tx, anKey.zone, anKey.domain, 1001, false)
	if err == nil {
		t.Fatal("a cancelled ctx — an aggregation error")
	}
	if unfound {
		t.Error("an aggregation failure cannot signal a find removal (T-187)")
	}
}

// mustMask is the fixture mask (a failure is a test error, not a business one).
func mustMask(t *testing.T, ids ...int) model.DNSMask {
	t.Helper()
	m, err := model.NewDNSMask(ids...)
	if err != nil {
		t.Fatalf("NewDNSMask: %v", err)
	}
	return m
}

// --- the appeared/lost detection history (T-118) ---------------------------

// anHistorySnapshot is a snapshot of the fixture transition journal.
func anHistorySnapshot(p *anFakePool) []anHistoryRec {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]anHistoryRec(nil), p.history...)
}

// TestAggregate_History — the laravel_history transition journal
// semantics (T-118): a record only on a fresh HTTP episode
// (queue.Result.Fresh) — a row appears → appeared=true, disappears →
// false; a recalculation without a presence change writes no history;
// re-analysis (fresh=false) writes nothing — a rule change is not a fact about the site.
func TestAggregate_History(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	ctx := context.Background()

	runTask := func(task queue.Result) {
		t.Helper()
		if _, err := f.w.ProcessResult(ctx, task); err != nil {
			t.Fatalf("ProcessResult: %v", err)
		}
	}

	// Episode 1 (fresh): a page with signs → the row appeared.
	laravelHash := anTestHash(7)
	f.store.blocks[laravelHash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), laravelHash[:])
	task := anTask
	task.Fresh = true
	runTask(task)

	hist := anHistorySnapshot(f.pool)
	if len(hist) != 1 || !hist[0].appeared || hist[0].domain != anKey.domain {
		t.Fatalf("the history after the appearance = %+v, want one appeared=true record", hist)
	}

	// Episode 2 (fresh): the same signs — the row exists, no transition.
	runTask(task)
	if hist = anHistorySnapshot(f.pool); len(hist) != 1 {
		t.Fatalf("a recalculation without a presence change: the history = %+v, want no change", hist)
	}

	// Episode 3 (fresh): a page without signs — the findings are empty,
	// the row is deleted → appeared=false.
	plainHash := anTestHash(8)
	f.store.blocks[plainHash] = []byte(`<html><body>nothing here</body></html>`)
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t), plainHash[:])
	runTask(task)

	hist = anHistorySnapshot(f.pool)
	if len(hist) != 2 || hist[1].appeared {
		t.Fatalf("the history after the disappearance = %+v, want a second appeared=false record", hist)
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Fatal("the laravel_domains row was not deleted")
	}

	// A re-analysis of the appearance (fresh=false): the findings will
	// appear, the row will be written — the history does not grow (a rule change is not a fact about the site).
	rKey := anRowKey{zone: 3, domain: 43, subdomain: 1, variant: 0}
	f.pool.seedResult(rKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), laravelHash[:])
	rTask := queue.Result{
		ZoneID: rKey.zone, DomainID: rKey.domain,
		SubdomainID: rKey.subdomain, Variant: 0, Name: "reanalyzed.example",
	}
	runTask(rTask)
	if _, ok := anDomainSnapshot(f.pool, rKey.zone, rKey.domain); !ok {
		t.Fatal("the re-analyzed domain's row was not written")
	}
	if hist = anHistorySnapshot(f.pool); len(hist) != 2 {
		t.Fatalf("the re-analysis appearance writes history: %+v, want no change (fresh=false)", hist)
	}

	// A re-analysis of the disappearance (fresh=false): the findings are
	// empty, the row is deleted — the history does not grow.
	f.pool.seedResult(rKey, anHeaders(t), anCookies(t), plainHash[:])
	runTask(rTask)
	if _, ok := anDomainSnapshot(f.pool, rKey.zone, rKey.domain); ok {
		t.Fatal("the re-analyzed domain's row was not deleted")
	}
	if hist = anHistorySnapshot(f.pool); len(hist) != 2 {
		t.Fatalf("the re-analysis disappearance writes history: %+v, want no change (fresh=false)", hist)
	}

	// An empty fresh episode without a row (DELETE 0): no transition — no record.
	runTask(task)
	if hist = anHistorySnapshot(f.pool); len(hist) != 2 {
		t.Fatalf("deleting a missing row writes history: %+v, want no change", hist)
	}
}

// TestAggregate_Counters — the incremental counters of the
// laravel_domains row transitions (T-178, the incremental-counters spec
// §6.5): zones.domains_laravel ±1 on EVERY presence transition
// (including re-analysis — the reconciliation counts rows regardless of
// origin), the crawler_counters appeared/lost cumulatives — only on
// fresh (the cumulative values match the laravel_history records).
// The invariants are checked against the fake's history journal.
func TestAggregate_Counters(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	ctx := context.Background()

	runTask := func(task queue.Result) {
		t.Helper()
		if _, err := f.w.ProcessResult(ctx, task); err != nil {
			t.Fatalf("ProcessResult: %v", err)
		}
	}

	laravelHash := anTestHash(7)
	f.store.blocks[laravelHash] = anPageHTML()
	plainHash := anTestHash(8)
	f.store.blocks[plainHash] = []byte(`<html><body>nothing here</body></html>`)

	// An appearance (fresh): domain 3 → zone +1, appeared +1.
	task := anTask // the anKey zone (3)
	task.Fresh = true
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), laravelHash[:])
	runTask(task)
	if got := f.pool.zoneLaravel[anKey.zone]; got != 1 {
		t.Fatalf("domains_laravel of the zone after the appearance = %d, want 1", got)
	}
	if got := f.pool.counters["laravel_appeared_total"]; got != 1 {
		t.Fatalf("laravel_appeared_total = %d, want 1", got)
	}
	if got := f.pool.counters["laravel_lost_total"]; got != 0 {
		t.Fatalf("laravel_lost_total = %d, want 0", got)
	}

	// A disappearance (fresh): zone −1, lost +1.
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t), plainHash[:])
	runTask(task)
	if got := f.pool.zoneLaravel[anKey.zone]; got != 0 {
		t.Fatalf("domains_laravel of the zone after the disappearance = %d, want 0", got)
	}
	if got := f.pool.counters["laravel_lost_total"]; got != 1 {
		t.Fatalf("laravel_lost_total = %d, want 1", got)
	}

	// A repeated episode without a presence change — no deltas.
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), laravelHash[:])
	runTask(task)
	if got := f.pool.zoneLaravel[anKey.zone]; got != 1 {
		t.Fatalf("domains_laravel after the repeated appearance = %d, want 1", got)
	}

	// A re-analysis of the disappearance (fresh=false): the row is
	// deleted — zone −1 (the set changes), the lost cumulative does not grow (no history record).
	rTask := task
	rTask.Fresh = false
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t), plainHash[:])
	runTask(rTask)
	if got := f.pool.zoneLaravel[anKey.zone]; got != 0 {
		t.Fatalf("domains_laravel after the re-analysis disappearance = %d, want 0 (a transition without fresh)", got)
	}
	if got := f.pool.counters["laravel_lost_total"]; got != 1 {
		t.Fatalf("laravel_lost_total after the re-analysis = %d, want 1 (no history is written)", got)
	}

	// The cumulatives match the history journal (the incremental-counters spec §6.5 invariant).
	hist := anHistorySnapshot(f.pool)
	var appeared, lost int64
	for _, h := range hist {
		if h.appeared {
			appeared++
		} else {
			lost++
		}
	}
	if appeared != f.pool.counters["laravel_appeared_total"] || lost != f.pool.counters["laravel_lost_total"] {
		t.Fatalf("the cumulatives (%d/%d) diverged from the history journal (%d/%d)",
			f.pool.counters["laravel_appeared_total"], f.pool.counters["laravel_lost_total"], appeared, lost)
	}

	// The zone row is missing (a manual reference-table edit): the shift
	// fails loudly — silent drift is unacceptable.
	f.pool.zoneNoRow = true
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), laravelHash[:])
	if _, err := f.w.ProcessResult(ctx, anTask); err == nil || !strings.Contains(err.Error(), "Laravel zone counter") {
		t.Fatalf("missing zone row: err = %v, want a zone counter error", err)
	}
	f.pool.zoneNoRow = false
}

// anStrEq / anStrDeref — comparison and printing of NULL bounds.
func anStrEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func anStrDeref(s *string) any {
	if s == nil {
		return "NULL"
	}
	return *s
}
