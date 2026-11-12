// Unit tests of the point drain of a domain's content (T-187,
// retentiondomain.go): closing the residual loss class behind the cursor —
// removing a Laravel find by re-analysis returns the domain's aged records
// to the retention candidates behind the cursor (T-185); the drain purges
// them by the PK prefix (zone_id, domain_id) with the regular T-166 purge
// machinery. The fake pool lives in retention_test.go. The task's control
// scenario: after the drain the domain has no candidates left (behind the
// cursor and at all).
package content

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/model"
)

// --- SQL contract ---------------------------------------------------------------

// TestDrainDomain_SQLContract — the exact text of the domain drain
// selection: addressing by the PK prefix (no cursor and no LIMIT — the
// domain's rows are bounded by subdomains 0..62 × variants), the same
// selection filters as retentionSelectSQL (age, content/jsonb, render
// flag, deleted, non-find), batch determinism by PK order.
func TestDrainDomain_SQLContract(t *testing.T) {
	renderPending := fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0)", int16(model.FlagRenderPending))
	deleted := fmt.Sprintf("AND d.status <> %d", int16(model.StatusDeleted))
	for _, frag := range []string{
		"FROM subdomain_results AS r",
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id",
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8",
		"AND r.checked_at < now() - $3::interval",
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
			"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL",
		renderPending,
		deleted,
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld",
		"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant",
	} {
		if !strings.Contains(retentionDomainSQL, frag) {
			t.Errorf("domain drain selection: the fragment %q missing (the T-166 selection filters)", frag)
		}
	}
	for _, bad := range []string{"LIMIT", "> ($2::timestamptz"} {
		if strings.Contains(retentionDomainSQL, bad) {
			t.Errorf("domain drain selection: an extra fragment %q (point-by-PK, no cursor/limit)", bad)
		}
	}
}

// --- the task's control scenario -----------------------------------------------

// TestDrainDomain_ControlScenario — re-analysis removed the domain's find:
// its aged records (behind the drain cursor — the cycle cursor will not
// return to them) are purged point-wise — the reference and jsonb trimmed,
// refcount decreased, contents without references deleted; other domains
// are untouched.
func TestDrainDomain_ControlScenario(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	// Domain 10 — a former find (its laravel_domains row was removed by
	// re-analysis), three aged records: two with content (sharing one —
	// deduplication), one with jsonb only.
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{status: int16(model.StatusAlive)}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old.Add(time.Hour), hash: retentionHash16(0x11), jsonb: true},
		{key: retentionKey{zone: 1, domain: 10, sub: 2, variant: 1}, checkedAt: old.Add(2 * time.Hour), jsonb: true},
		// The neighboring domain 20 of the same zone — untouched.
		{key: retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x33), jsonb: true},
	}
	pool.domains[retentionDomKey{zone: 1, id: 20}] = retentionFakeDomain{status: int16(model.StatusAlive)}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 2, clen: 5000}
	pool.contents[retentionHash(0x33)] = retentionFakeContent{refc: 1, clen: 7000}

	stats, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err != nil {
		t.Fatalf("DrainDomain: %v", err)
	}
	if stats.Rows != 3 || stats.Refs != 2 || stats.Contents != 1 || stats.FreedBytes != 5000 {
		t.Fatalf("stats = %+v, want Rows=3 Refs=2 Contents=1 FreedBytes=5000", stats)
	}
	for _, sub := range []int16{0, 1, 2} {
		r := pool.row(retentionKey{zone: 1, domain: 10, sub: sub, variant: map[int16]int16{0: 0, 1: 0, 2: 1}[sub]})
		if r == nil || r.hash != nil || r.jsonb {
			t.Errorf("domain 10, sub=%d: the record not purged: %+v", sub, r)
		}
	}
	if _, ok := pool.contents[retentionHash(0x11)]; ok {
		t.Error("contents 0x11 (both references trimmed) must be removed")
	}
	if r := pool.row(retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}); r == nil || r.hash == nil || !r.jsonb {
		t.Errorf("foreign domain 20 touched: %+v", r)
	}
	if c, ok := pool.contents[retentionHash(0x33)]; !ok || c.refc != 1 {
		t.Errorf("contents 0x33 (a foreign domain) touched: %+v", c)
	}
	// Control criterion: the domain has no candidates left AT ALL —
	// a repeated drain is empty, no transaction is opened.
	pool.queries, pool.txCalls = nil, nil
	stats, err = DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err != nil {
		t.Fatalf("a repeated DrainDomain: %v", err)
	}
	if stats.Rows != 0 || pool.begins != 1 {
		t.Fatalf("repeat = %+v (transactions %d), want an empty result without a new transaction", stats, pool.begins)
	}
}

// --- selection filters -----------------------------------------------------------

// TestDrainDomain_Filters — the T-166 criteria in the point selection:
// a find keeps its content always, render_pending without rendered waits
// for the render, deleted is T-020 territory, fresh and already trimmed
// records are not candidates.
func TestDrainDomain_Filters(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		domain int64
	}{
		{"the domain is a find again (re-analysis returned it)", 20},
		{"render_pending, not rendered", 30},
		{"deleted — T-020 purges it", 50},
		{"a fresh record — not aged out", 60},
		{"already purged — nothing to purge", 70},
	}
	pool := retentionSeedPool(now)
	// 70: an aged record without a reference and jsonb (e.g. a previous
	// drain).
	pool.domains[retentionDomKey{zone: 1, id: 70}] = retentionFakeDomain{status: int16(model.StatusAlive)}
	pool.results = append(pool.results, retentionRow{
		key: retentionKey{zone: 1, domain: 70, sub: 0, variant: 0}, checkedAt: now.Add(-2 * DefaultRetention),
	})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stats, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, tc.domain)
			if err != nil {
				t.Fatalf("DrainDomain: %v", err)
			}
			if stats.Rows != 0 || stats.Batches != 0 {
				t.Fatalf("stats = %+v, want an empty result (the records protected by the selection filters)", stats)
			}
		})
	}
}

// TestDrainDomain_ArgsAndNoCursor — the selection parameters: the domain
// keys and the aging boundary as an interval; the cycle's drain cursor is
// not involved.
func TestDrainDomain_ArgsAndNoCursor(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	pool := drainPool(now) // domain 1/10, two aged candidates

	if _, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10); err != nil {
		t.Fatalf("DrainDomain: %v", err)
	}
	if len(pool.queries) != 1 {
		t.Fatalf("SELECTs = %d, want 1", len(pool.queries))
	}
	want := []any{int16(1), int64(10), DefaultRetention.String()}
	if !reflect.DeepEqual(pool.queries[0].args, want) {
		t.Fatalf("args = %v, want %v (zone, domain, window)", pool.queries[0].args, want)
	}
	// The purge batch — the domain keys in PK order, the same boundary.
	if len(pool.txCalls) == 0 || pool.txCalls[0].sql != retentionPurgeSQL {
		t.Fatalf("the transaction's first statement = %+v, want purge", pool.txCalls[0])
	}
	purge := pool.txCalls[0].args
	if !reflect.DeepEqual(purge[:4], []any{[]int16{1, 1}, []int64{10, 10}, []int16{0, 1}, []int16{0, 0}}) {
		t.Errorf("purge keys = %v, want the domain rows in PK order", purge[:4])
	}
	if purge[5] != DefaultRetention.String() {
		t.Errorf("the purge boundary = %v, want %s", purge[5], DefaultRetention)
	}
}

// --- empty outcome and failures --------------------------------------------------

// TestDrainDomain_EmptyNoTx — no candidates: a single SELECT, no
// transaction is opened (nothing to purge).
func TestDrainDomain_EmptyNoTx(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results = nil
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{status: int16(model.StatusAlive)}

	stats, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err != nil {
		t.Fatalf("DrainDomain: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("stats = %+v, want an empty result", stats)
	}
	if len(pool.queries) != 1 || pool.begins != 0 {
		t.Fatalf("calls = %d (transactions %d), want one SELECT without a transaction", len(pool.queries), pool.begins)
	}
}

// TestDrainDomain_Errors — step failures: the selection and purge errors
// are returned (the caller logs them without bringing the delivery down —
// T-187); a disabled window and a nil pool are a loud usage error.
func TestDrainDomain_Errors(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	if _, err := DrainDomain(context.Background(), nil, DefaultRetention, 1, 10); err == nil {
		t.Error("nil pool: an error expected")
	}
	if _, err := DrainDomain(context.Background(), retentionSeedPool(now), 0, 1, 10); err == nil {
		t.Error("retention = 0: an error expected (content is kept forever)")
	}

	pool := retentionSeedPool(now)
	pool.selectErr = errors.New("select boom")
	if _, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10); err == nil ||
		!strings.Contains(err.Error(), "domain drain") {
		t.Fatalf("selection error = %v, want the drain context", err)
	}

	pool = retentionSeedPool(now)
	pool.purgeErr = errors.New("purge boom")
	_, err := DrainDomain(context.Background(), pool, DefaultRetention, 1, 10)
	if err == nil || !strings.Contains(err.Error(), "domain 1/10 drain") {
		t.Fatalf("purge error = %v, want the context with the domain key", err)
	}
	// The T-044 guards: the purge failure rolled the transaction back —
	// the rows are untouched.
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r == nil || r.hash == nil {
		t.Error("the batch failure left changes in the rows (the transaction did not roll back)")
	}
}
