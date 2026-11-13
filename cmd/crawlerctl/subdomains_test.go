// Unit tests for crawlerctl subdomains (T-083, spec 5.10, 13.1) on a fake
// pool (db.DB) — without a DB. Checked: the exact SQL texts (a contract,
// the status_test.go style) and the fake's live semantics: the reference
// table (label uniqueness, assignment of the free bit max(id)+1, the
// maximum 63 — bit 63 is the sign bit; disable changes only enabled, the
// bit and the masks are stable), starting a check of a new subdomain
// (done → pending in batches with the check_state/status guard and the
// zone filter, deleted is excluded, the affected counter), errors (no
// subdomain, disabled, zone not found, DB failure, drain interruption).
// Behavior against live PostgreSQL — the dev environment
// (docker compose run --rm tools go run ./cmd/crawlerctl).
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
	"crawler/internal/model"
)

// --- SQL contracts ---------------------------------------------------------

// The want texts duplicate the constants: a SQL change without updating
// the expectations (or vice versa) fails the test loudly.
const (
	wantSubdomainsSelect = "SELECT id, label, enabled FROM subdomains ORDER BY id"
	wantSubdomainByLabel = "SELECT id, label, enabled FROM subdomains WHERE label = $1"
	wantSubdomainByID    = "SELECT id, label, enabled FROM subdomains WHERE id = $1"
	wantSubdomainNextID  = "SELECT coalesce(max(id), -1) + 1 FROM subdomains"
	wantSubdomainInsert  = "INSERT INTO subdomains (id, label, enabled) VALUES ($1, $2, true) RETURNING id"
	wantSubdomainDisable = "UPDATE subdomains SET enabled = false WHERE id = $1 RETURNING id, label, enabled"

	wantSubRecheckAll = "WITH b AS (SELECT zone_id, id FROM domains " +
		"WHERE check_state = $2 AND status <> $3 " +
		"ORDER BY zone_id, id LIMIT $4), " +
		"upd AS (UPDATE domains AS d SET check_state = $1, queued_at = NULL " +
		"FROM b WHERE d.zone_id = b.zone_id AND d.id = b.id " +
		"AND d.check_state = $2 AND d.status <> $3 " +
		"RETURNING d.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_checked = z.domains_checked - c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM upd"

	wantSubRecheckZone = "WITH b AS (SELECT zone_id, id FROM domains " +
		"WHERE check_state = $2 AND status <> $3 AND zone_id = $4 " +
		"ORDER BY zone_id, id LIMIT $5), " +
		"upd AS (UPDATE domains AS d SET check_state = $1, queued_at = NULL " +
		"FROM b WHERE d.zone_id = b.zone_id AND d.id = b.id " +
		"AND d.check_state = $2 AND d.status <> $3 AND d.zone_id = $4 " +
		"RETURNING d.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_checked = z.domains_checked - c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM upd"

	// wantSubZoneIDByName — the analyzer.ZoneIDByName contract (reused by
	// recheck for --zone): the text is pinned here so that a change of a
	// foreign query does not pass silently.
	wantSubZoneIDByName = "SELECT id FROM zones WHERE name = $1"
)

func TestSubdomainsSQLContract(t *testing.T) {
	for sql, want := range map[string]string{
		subdomainsSelectSQL:     wantSubdomainsSelect,
		subdomainByLabelSQL:     wantSubdomainByLabel,
		subdomainByIDSQL:        wantSubdomainByID,
		subdomainNextIDSQL:      wantSubdomainNextID,
		subdomainInsertSQL:      wantSubdomainInsert,
		subdomainDisableSQL:     wantSubdomainDisable,
		subdomainRecheckAllSQL:  wantSubRecheckAll,
		subdomainRecheckZoneSQL: wantSubRecheckZone,
	} {
		if sql != want {
			t.Errorf("the SQL contract:\ngot %s\nwant %s", sql, want)
		}
	}
}

// --- fake pool (db.DB) --------------------------------------------------------

// subdomSeed — a fake domain for the done → pending move semantics.
type subdomSeed struct {
	zone   int16
	id     int64
	state  model.CheckState
	status model.Status
}

// subCall — a pool call, for contract checks.
type subCall struct {
	sql  string
	args []any
}

// subdomainsPool — a db.DB without a DB: the live semantics of the
// subdomains command SQL — the reference table (SELECT by label/bit,
// max(id)+1, INSERT, UPDATE enabled), zones by name (the analog of
// analyzer.ZoneIDByName) and the done → pending move over domains (a
// selection with LIMIT, the check_state/status guard, the zone filter,
// ORDER BY zone_id, id).
type subdomainsPool struct {
	subs    []subdomainRow
	zones   map[string]int16
	domains []subdomSeed

	mu           sync.Mutex
	queryErr     error
	execErr      error
	execErrAfter int // the first N Exec calls succeed, then execErr (a drain interruption)
	queries      []subCall
	execs        []subCall
}

var _ db.DB = (*subdomainsPool)(nil)

// newSubdomainsPool — the initial filling of migration 000001 and two zones.
func newSubdomainsPool() *subdomainsPool {
	return &subdomainsPool{
		subs: []subdomainRow{
			{0, "", true}, {1, "www", true}, {2, "admin", true},
			{3, "crm", true}, {4, "api", true}, {5, "app", true},
		},
		zones: map[string]int16{"crawl.test": 7, "example.test": 9},
	}
}

func (p *subdomainsPool) seedSub(s subdomainRow) { p.subs = append(p.subs, s) }

func (p *subdomainsPool) seedDomain(zone int16, id int64, st model.CheckState, status model.Status) {
	p.domains = append(p.domains, subdomSeed{zone: zone, id: id, state: st, status: status})
}

// subByID — a reference table row by bit, for summary checks.
func (p *subdomainsPool) subByID(id int16) subdomainRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.subs {
		if s.id == id {
			return s
		}
	}
	return subdomainRow{}
}

func (p *subdomainsPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("subdomainsPool: CopyFrom must not be called")
}

func (p *subdomainsPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("subdomainsPool: unexpected Exec: %s (recheck — a CTE via Query, T-178)", sql)
}

// recheck — the semantics of the subquery and the outer WHERE: the
// candidates are done and not deleted [of the zone], ORDER BY
// (zone_id, id), LIMIT; the selected ones are moved to pending. Returns
// the moved count.
func (p *subdomainsPool) recheck(done, deleted int16, zone int16, limit int) int64 {
	cands := make([]subdomSeed, 0, len(p.domains))
	for _, d := range p.domains {
		if int16(d.state) == done && int16(d.status) != deleted && (zone == 0 || d.zone == zone) {
			cands = append(cands, d)
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].zone != cands[j].zone {
			return cands[i].zone < cands[j].zone
		}
		return cands[i].id < cands[j].id
	})
	if len(cands) > limit {
		cands = cands[:limit]
	}
	for _, c := range cands {
		for i := range p.domains {
			if p.domains[i].zone == c.zone && p.domains[i].id == c.id {
				p.domains[i].state = model.CheckStatePending
			}
		}
	}
	return int64(len(cands))
}

func (p *subdomainsPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, subCall{sql, append([]any(nil), args...)})
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	// The recheck move statements — CTEs with an outer SELECT count (T-178):
	// counted in the execs protocol (the error/drain hooks).
	case subdomainRecheckAllSQL:
		p.execs = append(p.execs, subCall{sql, append([]any(nil), args...)})
		if p.execErr != nil && (p.execErrAfter == 0 || len(p.execs) > p.execErrAfter) {
			return nil, p.execErr
		}
		n := p.recheck(args[1].(int16), args[2].(int16), 0, args[3].(int))
		return statusRowsOf([]any{n}), nil
	case subdomainRecheckZoneSQL:
		p.execs = append(p.execs, subCall{sql, append([]any(nil), args...)})
		if p.execErr != nil && (p.execErrAfter == 0 || len(p.execs) > p.execErrAfter) {
			return nil, p.execErr
		}
		n := p.recheck(args[1].(int16), args[2].(int16), args[3].(int16), args[4].(int))
		return statusRowsOf([]any{n}), nil
	case subdomainsSelectSQL:
		rows := make([][]any, len(p.subs))
		for i, s := range p.subs {
			rows[i] = []any{s.id, s.label, s.enabled}
		}
		return statusRowsOf(rows...), nil
	case subdomainByLabelSQL:
		for _, s := range p.subs {
			if s.label == args[0].(string) {
				return statusRowsOf([]any{s.id, s.label, s.enabled}), nil
			}
		}
		return statusRowsOf(), nil
	case subdomainByIDSQL:
		for _, s := range p.subs {
			if s.id == args[0].(int16) {
				return statusRowsOf([]any{s.id, s.label, s.enabled}), nil
			}
		}
		return statusRowsOf(), nil
	case subdomainNextIDSQL:
		max := int16(-1)
		for _, s := range p.subs {
			if s.id > max {
				max = s.id
			}
		}
		return statusRowsOf([]any{max + 1}), nil
	case subdomainInsertSQL:
		id, label := args[0].(int16), args[1].(string)
		for _, s := range p.subs {
			if s.id == id {
				return nil, fmt.Errorf("subdomainsPool: the PK %d is already taken", id)
			}
			if s.label == label {
				return nil, fmt.Errorf(`subdomainsPool: the label %q already exists (UNIQUE)`, label)
			}
		}
		p.subs = append(p.subs, subdomainRow{id: id, label: label, enabled: true})
		return statusRowsOf([]any{id}), nil
	case subdomainDisableSQL:
		for i := range p.subs {
			if p.subs[i].id == args[0].(int16) {
				p.subs[i].enabled = false
				s := p.subs[i]
				return statusRowsOf([]any{s.id, s.label, s.enabled}), nil
			}
		}
		return statusRowsOf(), nil
	case wantSubZoneIDByName:
		if id, ok := p.zones[args[0].(string)]; ok {
			return statusRowsOf([]any{id}), nil
		}
		return statusRowsOf(), nil
	default:
		return nil, fmt.Errorf("subdomainsPool: unexpected SQL Query: %s", sql)
	}
}

// --- list ---------------------------------------------------------------------

func TestSubdomainsListRun(t *testing.T) {
	pool := newSubdomainsPool()
	pool.subs[2].enabled = false // admin is disabled

	var stdout bytes.Buffer
	if err := subdomainsListRun(context.Background(), pool, &stdout); err != nil {
		t.Fatalf("subdomainsListRun: %v", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "bit  label") {
		t.Errorf("the output without the header:\n%s", got)
	}
	// Rows in bit order (the apex '' is shown with its own marker); act —
	// the last character of the row: "+" enabled, "-" disabled.
	lines := strings.Split(strings.TrimSpace(got), "\n")
	rows := []struct{ prefix, mark string }{
		{"  0  (apex)", "+"},
		{"  1  www", "+"},
		{"  2  admin", "-"},
		{"  3  crm", "+"},
		{"  4  api", "+"},
		{"  5  app", "+"},
	}
	for i, w := range rows {
		if len(lines) <= i+1 {
			t.Fatalf("line %d is missing:\n%s", i+1, got)
		}
		if !strings.HasPrefix(lines[i+1], w.prefix) || !strings.HasSuffix(lines[i+1], w.mark) {
			t.Errorf("line %d = %q, want the prefix %q and the act %q", i+1, lines[i+1], w.prefix, w.mark)
		}
	}
	if !strings.Contains(got, "total 6 of 63, enabled 5") {
		t.Errorf("the output without the total/enabled summary:\n%s", got)
	}
	if len(pool.execs) != 0 {
		t.Errorf("list only reads, Exec was not expected: %v", pool.execs)
	}
}

func TestSubdomainsListRunQueryError(t *testing.T) {
	pool := newSubdomainsPool()
	pool.queryErr = errors.New("connection failure")
	err := subdomainsListRun(context.Background(), pool, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "subdomains catalog") {
		t.Fatalf("err = %v, want the catalog context", err)
	}
}

// --- add ----------------------------------------------------------------------

func TestSubdomainsAddRun(t *testing.T) {
	pool := newSubdomainsPool()

	var stdout bytes.Buffer
	if err := subdomainsAddRun(context.Background(), pool, "shop", &stdout); err != nil {
		t.Fatalf("subdomainsAddRun: %v", err)
	}
	// The free bit — max(id)+1 = 6; the row is inserted enabled.
	if want := (subdomainRow{6, "shop", true}); pool.subs[len(pool.subs)-1] != want {
		t.Errorf("the row inserted %+v, want %+v", pool.subs[len(pool.subs)-1], want)
	}
	for _, want := range []string{"subdomain shop added: bit 6", "subdomains recheck shop"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("the output does not contain %q:\n%s", want, stdout.String())
		}
	}
}

func TestSubdomainsAddRunDuplicate(t *testing.T) {
	pool := newSubdomainsPool()
	err := subdomainsAddRun(context.Background(), pool, "www", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "already in the catalog: bit 1, enabled") {
		t.Fatalf("err = %v, want \"already in the catalog ... bit 1\"", err)
	}
	// No insert happened: only the label and free-bit checks, none reached
	// the insert.
	for _, c := range pool.queries {
		if c.sql == subdomainInsertSQL {
			t.Errorf("a duplicate label must not insert a row: %v", c)
		}
	}

	// A disabled label — the same rejection, with the state named.
	pool2 := newSubdomainsPool()
	pool2.subs[1].enabled = false
	err = subdomainsAddRun(context.Background(), pool2, "www", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "bit 1, disabled") {
		t.Fatalf("err = %v, want \"bit 1, disabled\"", err)
	}
}

func TestSubdomainsAddRunMax63(t *testing.T) {
	// The reference table is full: bits 0..62 (63 rows) — no next bit
	// (bit 63 is the sign bit), no insert is performed.
	pool := newSubdomainsPool()
	pool.subs = nil
	for id := int16(0); id < 63; id++ {
		pool.subs = append(pool.subs, subdomainRow{id, fmt.Sprintf("s%d", id), true})
	}
	err := subdomainsAddRun(context.Background(), pool, "shop", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "out of range 0..62") {
		t.Fatalf("err = %v, want the 0..62 range refusal (out of range)", err)
	}
	if len(pool.subs) != 63 {
		t.Errorf("the catalog changed: %d rows, want 63", len(pool.subs))
	}
}

func TestSubdomainsAddRunInsertError(t *testing.T) {
	pool := newSubdomainsPool()
	pool.queryErr = errors.New("no connection")
	// The error shows up at the first query (the label check).
	err := subdomainsAddRun(context.Background(), pool, "shop", &bytes.Buffer{})
	if err == nil {
		t.Fatal("a DB error was expected")
	}
}

// --- disable ------------------------------------------------------------------

func TestSubdomainsDisableRun(t *testing.T) {
	pool := newSubdomainsPool()

	var stdout bytes.Buffer
	if err := subdomainsDisableRun(context.Background(), pool, "www", &stdout); err != nil {
		t.Fatalf("subdomainsDisableRun: %v", err)
	}
	if pool.subs[1].enabled {
		t.Error("www must be disabled after disable")
	}
	// The bit is stable: the row stayed in place, id/label unchanged.
	if pool.subs[1] != (subdomainRow{1, "www", false}) {
		t.Errorf("the www row = %+v, want {1 www false}", pool.subs[1])
	}
	if !strings.Contains(stdout.String(), "www (bit 1) disabled") {
		t.Errorf("the output without the summary:\n%s", stdout.String())
	}
	// Masks/data are not touched: no UPDATE domains was executed at all.
	if len(pool.execs) != 0 {
		t.Errorf("disable does not touch domains, Exec was not expected: %v", pool.execs)
	}
}

func TestSubdomainsDisableRunByID(t *testing.T) {
	pool := newSubdomainsPool()

	var stdout bytes.Buffer
	if err := subdomainsDisableRun(context.Background(), pool, "3", &stdout); err != nil {
		t.Fatalf("disable by the bit number: %v", err)
	}
	if pool.subs[3] != (subdomainRow{3, "crm", false}) {
		t.Errorf("the crm row = %+v, want {3 crm false}", pool.subs[3])
	}
	if !strings.Contains(stdout.String(), "crm (bit 3) disabled") {
		t.Errorf("the output without the summary:\n%s", stdout.String())
	}
}

func TestSubdomainsDisableRunLabelPriority(t *testing.T) {
	// A numeric label exists — it takes precedence over the bit number.
	pool := newSubdomainsPool()
	pool.seedSub(subdomainRow{7, "3", true})

	var stdout bytes.Buffer
	if err := subdomainsDisableRun(context.Background(), pool, "3", &stdout); err != nil {
		t.Fatalf("disable of a numeric label: %v", err)
	}
	bit3, bit7 := pool.subByID(3), pool.subByID(7)
	if bit3 != (subdomainRow{3, "crm", true}) || bit7 != (subdomainRow{7, "3", false}) {
		t.Errorf("the label \"3\" disabled (bit 7), crm (bit 3) untouched: crm=%+v label3=%+v", bit3, bit7)
	}
	if !strings.Contains(stdout.String(), "3 (bit 7) disabled") {
		t.Errorf("the output without the summary:\n%s", stdout.String())
	}
}

func TestSubdomainsDisableRunNotFound(t *testing.T) {
	pool := newSubdomainsPool()
	err := subdomainsDisableRun(context.Background(), pool, "nope", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "subdomain \"nope\" not found in the catalog") {
		t.Fatalf("err = %v, want \"not found in the catalog\"", err)
	}
	// A numeric argument missing from the occupied bits — also not found.
	err = subdomainsDisableRun(context.Background(), pool, "42", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v, want \"not found\"", err)
	}
}

// TestSubdomainsDisableRunBitOutOfRange — a numeric argument outside the
// bit range 0..62: an int16 truncation must not silently map it to a
// foreign bit (65537 → www, bit 1) — "not found", the reference table is
// not touched.
func TestSubdomainsDisableRunBitOutOfRange(t *testing.T) {
	pool := newSubdomainsPool()
	for _, arg := range []string{"65537", "131073", "100", "-1", "65536"} {
		err := subdomainsDisableRun(context.Background(), pool, arg, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("disable %q: err = %v, want \"not found\"", arg, err)
		}
	}
	if pool.subs[1] != (subdomainRow{1, "www", true}) {
		t.Errorf("www must not change: %+v", pool.subs[1])
	}
	if pool.subs[0] != (subdomainRow{0, "", true}) {
		t.Errorf("the apex must not change: %+v", pool.subs[0])
	}
}

func TestSubdomainsDisableRunApexGuard(t *testing.T) {
	pool := newSubdomainsPool()
	for _, arg := range []string{"", "0", "  "} {
		err := subdomainsDisableRun(context.Background(), pool, arg, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "the apex (bit 0)") {
			t.Errorf("disable %q: err = %v, want the apex disabling refusal", arg, err)
		}
	}
	if pool.subs[0] != (subdomainRow{0, "", true}) {
		t.Errorf("the apex changed: %+v", pool.subs[0])
	}
}

func TestSubdomainsDisableRunAlreadyDisabled(t *testing.T) {
	pool := newSubdomainsPool()
	pool.subs[2].enabled = false // admin is already disabled

	var stdout bytes.Buffer
	if err := subdomainsDisableRun(context.Background(), pool, "admin", &stdout); err != nil {
		t.Fatalf("a repeated disable — not an error: %v", err)
	}
	if !strings.Contains(stdout.String(), "already disabled") {
		t.Errorf("the output does not report \"already disabled\":\n%s", stdout.String())
	}
	// The idempotent branch — before any write: no UPDATE was executed at all.
	for _, c := range pool.queries {
		if c.sql == subdomainDisableSQL {
			t.Errorf("\"already disabled\" must not do an UPDATE: %v", c)
		}
	}
}

// --- recheck ------------------------------------------------------------------

func TestSubdomainsRecheckRunTranslate(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)        // moved
	pool.seedDomain(7, 2, model.CheckStateDone, model.StatusDeleted)      // deleted — the guard
	pool.seedDomain(7, 3, model.CheckStatePending, model.StatusAlive)     // already in the cycle
	pool.seedDomain(7, 4, model.CheckStateQueued, model.StatusAlive)      // in work (lease)
	pool.seedDomain(9, 5, model.CheckStateDone, model.StatusNotDelegated) // moved
	pool.seedDomain(9, 6, model.CheckStateDone, model.StatusTempError)    // moved (temp_error — a candidate)

	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "all", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "subdomain www (bit 1), zone: all") || !strings.Contains(got, "moved to pending: 3") {
		t.Errorf("the output without the summary:\n%s", got)
	}
	// Semantics: done and not deleted → pending; the other states and
	// statuses are not touched.
	want := []model.CheckState{model.CheckStatePending, model.CheckStateDone,
		model.CheckStatePending, model.CheckStateQueued, model.CheckStatePending, model.CheckStatePending}
	for i, w := range want {
		if pool.domains[i].state != w {
			t.Errorf("domain %d: check_state = %v, want %v", i+1, pool.domains[i].state, w)
		}
	}

	// The parameter contract: the pending/done/deleted codes and the batch size.
	if len(pool.execs) != 1 {
		t.Fatalf("Exec calls = %d, want 1 (a partial batch finishes the drain)", len(pool.execs))
	}
	call := pool.execs[0]
	if call.sql != subdomainRecheckAllSQL {
		t.Errorf("SQL = %s, want the move without a zone", call.sql)
	}
	if len(call.args) != 4 {
		t.Fatalf("args = %v, want 4", call.args)
	}
	for i, want := range []any{int16(model.CheckStatePending), int16(model.CheckStateDone), int16(model.StatusDeleted), subdomainRecheckBatch} {
		if call.args[i] != want {
			t.Errorf("args[%d] = %v (%T), want %v (%T)", i, call.args[i], call.args[i], want, want)
		}
	}
}

func TestSubdomainsRecheckRunZone(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)
	pool.seedDomain(9, 2, model.CheckStateDone, model.StatusAlive) // another zone — not touched

	zone := int16(7)
	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", &zone, "crawl.test", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "moved to pending: 1") {
		t.Errorf("the output without the zone summary:\n%s", stdout.String())
	}
	if pool.domains[0].state != model.CheckStatePending || pool.domains[1].state != model.CheckStateDone {
		t.Errorf("the zone is not filtered: %+v", pool.domains)
	}
	call := pool.execs[0]
	if call.sql != subdomainRecheckZoneSQL || call.args[3] != zone || call.args[4] != subdomainRecheckBatch {
		t.Errorf("exec = %v args=%v, want the zone SQL with zone=%d", call.sql, call.args, zone)
	}
}

func TestSubdomainsRecheckRunBatches(t *testing.T) {
	// Draining in batches: 2·5000 + 1 domains — three passes, a partial
	// batch ends the cycle (the RecheckOnce style, T-030).
	pool := newSubdomainsPool()
	const total = 2*subdomainRecheckBatch + 1
	for i := int64(1); i <= total; i++ {
		pool.seedDomain(7, i, model.CheckStateDone, model.StatusAlive)
	}

	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "all", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(stdout.String(), fmt.Sprintf("moved to pending: %d", total)) {
		t.Errorf("the output without the counter %d:\n%s", total, stdout.String())
	}
	if len(pool.execs) != 3 {
		t.Errorf("passes = %d, want 3 (2 full batches + 1 partial)", len(pool.execs))
	}
	for _, d := range pool.domains {
		if d.state != model.CheckStatePending {
			t.Fatalf("domain %d was not moved: %+v", d.id, d)
		}
	}
}

func TestSubdomainsRecheckRunEmpty(t *testing.T) {
	pool := newSubdomainsPool() // no domains

	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "all", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "no done domains to recheck") {
		t.Errorf("the output does not report \"no domains\":\n%s", stdout.String())
	}
}

func TestSubdomainsRecheckRunUnknownSubdomain(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)

	err := subdomainsRecheckRun(context.Background(), pool, "ghost", nil, "all", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "is not in the catalog — run subdomains add ghost first") {
		t.Fatalf("err = %v, want the add hint", err)
	}
	if len(pool.execs) != 0 {
		t.Errorf("the move must not be executed: %v", pool.execs)
	}

	// A disabled subdomain is pointless to check — the DNS worker skips
	// it (spec 5.4 item 2).
	pool.subs[1].enabled = false
	err = subdomainsRecheckRun(context.Background(), pool, "www", nil, "all", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "is disabled") {
		t.Fatalf("err = %v, want the disabled refusal", err)
	}
}

func TestSubdomainsRecheckRunErrors(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)

	pool.execErr = errors.New("no connection")
	err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "all", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "moving done → pending") {
		t.Fatalf("err = %v, want the move context", err)
	}

	// An interruption after the first full batch: a partial total in the error.
	pool2 := newSubdomainsPool()
	const total = 2 * subdomainRecheckBatch
	for i := int64(1); i <= total; i++ {
		pool2.seedDomain(7, i, model.CheckStateDone, model.StatusAlive)
	}
	pool2.execErrAfter, pool2.execErr = 1, errors.New("ctx cancelled")
	err = subdomainsRecheckRun(context.Background(), pool2, "www", nil, "all", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("moved to pending %d domains, interrupted", subdomainRecheckBatch)) {
		t.Fatalf("err = %v, want the first batch's partial total", err)
	}
}

// --- commands via run ----------------------------------------------------------

// TestRunSubdomainsExecution — the subdomains command execution path
// without infrastructure: a deterministic configuration failure (code 1)
// with the command context — input validation runs earlier (usage errors —
// code 2).
func TestRunSubdomainsExecution(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "list — no configuration", args: []string{"subdomains", "list"}, code: exitError, inErr: "subdomains list: config:"},
		{name: "add — no configuration", args: []string{"subdomains", "add", "www"}, code: exitError, inErr: "subdomains add www: config:"},
		{name: "disable — no configuration", args: []string{"subdomains", "disable", "www"}, code: exitError, inErr: "subdomains disable www: config:"},
		{name: "recheck — no configuration", args: []string{"subdomains", "recheck", "www"}, code: exitError, inErr: "subdomains recheck www: config:"},
		{name: "recheck — the label normalizes before the configuration", args: []string{"subdomains", "recheck", "МосквА"}, code: exitError, inErr: "subdomains recheck xn--80adxhks: config:"},
		{name: "recheck no configuration with a zone", args: []string{"subdomains", "recheck", "--zone", "crawl.test", "www"}, code: exitError, inErr: "subdomains recheck www: config:"},
		{name: "recheck without a label", args: []string{"subdomains", "recheck"}, code: exitUsage, inErr: "specify a subdomain label"},
		{name: "recheck two positionals", args: []string{"subdomains", "recheck", "a", "b"}, code: exitUsage, inErr: "specify a subdomain label"},
		{name: "recheck an empty label", args: []string{"subdomains", "recheck", ""}, code: exitUsage, inErr: "the apex"},
		{name: "recheck an invalid label (after --)", args: []string{"subdomains", "recheck", "--", "-bad-"}, code: exitUsage, inErr: "invalid subdomain label"},
		{name: "recheck an invalid zone", args: []string{"subdomains", "recheck", "--zone", "bad!!zone", "www"}, code: exitUsage, inErr: "invalid zone name"},
		{name: "recheck --zone without a value", args: []string{"subdomains", "recheck", "--zone"}, code: exitUsage, inErr: "subdomains recheck:"},
		{name: "recheck an unknown flag", args: []string{"subdomains", "recheck", "--nope", "www"}, code: exitUsage, inErr: "subdomains recheck:"},
		{name: "list an extra argument", args: []string{"subdomains", "list", "foo"}, code: exitUsage, inErr: "extra arguments"},
		{name: "recheck --help", args: []string{"subdomains", "recheck", "--help"}, code: exitOK, inOut: "checking a subdomain over the already checked domains"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) code = %d, want %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout does not contain %q:\n%s", tc.args, tc.inOut, stdout.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr does not contain %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
		})
	}
}
