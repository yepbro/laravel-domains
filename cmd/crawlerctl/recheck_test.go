// Unit tests for crawlerctl recheck (T-084, spec 4.1, 5.3) on a fake pool
// (db.DB) — without a DB. The exact SQL texts are checked (a contract, the
// subdomains_test.go style) along with the fake's live semantics: batch
// name resolution (unnest + WITH ORDINALITY, LEFT JOIN of zones/domains —
// not-found ones are not lost, the argument order is preserved), moving
// the found ones to pending with a single UPDATE over PK keys with the
// status <> deleted guard and RETURNING of the keys, the per-line summary
// (not found: no zone / no label; deleted — a rejection; the old
// check_state), a partial success — code 1, DB errors.
// Behavior against live PostgreSQL — the dev environment (make shell).
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
)

// --- SQL contracts ---------------------------------------------------------

const wantRecheckLookup = "SELECT n.ord, " +
	"coalesce(z.id, 0) AS zone_id, coalesce(z.name, '') AS zone_name, coalesce(z.is_active, false) AS zone_on, " +
	"coalesce(d.id, 0) AS domain_id, coalesce(d.label, '') AS label, " +
	"coalesce(d.check_state, 0) AS check_state, coalesce(d.status, 0) AS status " +
	"FROM unnest($1::text[]) WITH ORDINALITY AS n(name, ord) " +
	"LEFT JOIN LATERAL (" +
	"SELECT id, name, is_active FROM zones AS z " +
	"WHERE n.name LIKE '%.' || z.name " +
	"ORDER BY length(z.name) DESC, z.name LIMIT 1" +
	") AS z ON true " +
	"LEFT JOIN domains AS d ON d.zone_id = z.id AND d.label = substr(n.name, 1, length(n.name) - length(z.name) - 1) " +
	"ORDER BY n.ord"

const wantRecheckUpdate = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM unnest($2::int2[], $3::int8[]) AS k(zone_id, domain_id) " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id AND d.status <> $4 " +
	"RETURNING d.zone_id, d.id"

// wantRecheckStates — the FOR UPDATE pre-read of the old states (T-178).
const wantRecheckStates = "SELECT d.zone_id, d.id, d.check_state " +
	"FROM domains AS d " +
	"JOIN unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"ON d.zone_id = k.zone_id AND d.id = k.domain_id " +
	"WHERE d.status <> $3 " +
	"FOR UPDATE OF d"

func TestRecheckSQLContract(t *testing.T) {
	if recheckLookupSQL != wantRecheckLookup {
		t.Errorf("SQL-контракт lookup:\nполучен %s\nожидался %s", recheckLookupSQL, wantRecheckLookup)
	}
	if recheckUpdateSQL != wantRecheckUpdate {
		t.Errorf("SQL-контракт update:\nполучен %s\nожидался %s", recheckUpdateSQL, wantRecheckUpdate)
	}
	if recheckStatesSQL != wantRecheckStates {
		t.Errorf("SQL-контракт состояний:\nполучен %s\nожидался %s", recheckStatesSQL, wantRecheckStates)
	}
}

// --- fake pool (db.DB) --------------------------------------------------------

// recheckZoneSeed — a fake zone.
type recheckZoneSeed struct {
	id     int16
	name   string
	active bool
}

// recheckDomainSeed — a fake domain (label + zone; queuedAt mimics
// queued_at: it is reset by the move to pending).
type recheckDomainSeed struct {
	zone     int16
	id       int64
	label    string
	state    model.CheckState
	status   model.Status
	queuedAt bool
}

// recheckPool — a db.DB without a DB: the live semantics of the recheck
// SQL — resolution of (zone, label) → rows (LEFT JOIN, ord), moving keys
// to pending (the deleted guard, RETURNING of the actually moved ones).
type recheckPool struct {
	zones   []recheckZoneSeed
	domains []recheckDomainSeed

	mu            sync.Mutex
	queryErr      error
	queryErrAfter int // the first N Query calls succeed, then queryErr (a move failure)
	onUpdate      func()
	queries       []subCall
	execs         []subCall // transaction Execs (the zone counter shift, T-178)
}

var _ db.DB = (*recheckPool)(nil)

func newRecheckPool() *recheckPool {
	return &recheckPool{
		zones: []recheckZoneSeed{
			{id: 7, name: "crawl.test", active: true},
			{id: 9, name: "off.test", active: false},
		},
	}
}

func (p *recheckPool) seed(zone int16, id int64, label string, st model.CheckState, status model.Status) {
	p.domains = append(p.domains, recheckDomainSeed{zone: zone, id: id, label: label, state: st, status: status, queuedAt: st == model.CheckStateQueued})
}

func (p *recheckPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("recheckPool: CopyFrom не должен вызываться")
}

func (p *recheckPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return pgconn.CommandTag{}, fmt.Errorf("recheckPool: неожидаемый Exec: %s", sql)
}

func (p *recheckPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, subCall{sql, append([]any(nil), args...)})
	if p.queryErr != nil && (p.queryErrAfter == 0 || len(p.queries) > p.queryErrAfter) {
		return nil, p.queryErr
	}
	switch sql {
	case recheckLookupSQL:
		return p.lookupRows(args[0].([]string)), nil
	case recheckStatesSQL:
		return p.statesRows(args[0].([]int16), args[1].([]int64), args[2].(int16)), nil
	case recheckUpdateSQL:
		if p.onUpdate != nil {
			p.onUpdate() // a race between the lookup and the UPDATE (e.g. a delete file)
		}
		return p.updateRows(args[0].(int16), args[1].([]int16), args[2].([]int64), args[3].(int16)), nil
	default:
		return nil, fmt.Errorf("recheckPool: неожидаемый SQL Query: %s", sql)
	}
}

// Begin — the move transaction (T-178: the state pre-read, the move and
// the zone counter deltas are atomic).
func (p *recheckPool) Begin(context.Context) (pgx.Tx, error) {
	return &recheckPoolTx{pool: p}, nil
}

// recheckPoolTx — a transaction over the fake: Query/Exec go into the
// pool protocol (Exec — the zone counter shift), Commit/Rollback are counted.
type recheckPoolTx struct {
	pgx.Tx
	pool     *recheckPool
	commits  int
	rollback int
}

func (t *recheckPoolTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.pool.Query(ctx, sql, args...)
}

func (t *recheckPoolTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.execs = append(t.pool.execs, subCall{sql, append([]any(nil), args...)})
	if sql != db.ZoneCounterShiftSQL {
		return pgconn.CommandTag{}, fmt.Errorf("recheckPool: неожидаемый SQL Exec: %s", sql)
	}
	ids := args[0].([]int16)
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", len(ids))), nil
}

func (t *recheckPoolTx) Commit(context.Context) error   { t.commits++; return nil }
func (t *recheckPoolTx) Rollback(context.Context) error { t.rollback++; return nil }

// statesRows applies the recheckStatesSQL semantics (T-178): the old
// states of rows by keys with the status <> deleted guard (FOR UPDATE).
func (p *recheckPool) statesRows(zones []int16, ids []int64, deleted int16) pgx.Rows {
	var rows [][]any
	for i, zone := range zones {
		id := ids[i]
		for _, d := range p.domains {
			if d.zone == zone && d.id == id && int16(d.status) != deleted {
				rows = append(rows, []any{zone, id, int16(d.state)})
				break
			}
		}
	}
	return statusRowsOf(rows...)
}

// lookupRows applies the recheckLookupSQL semantics: the zone — the
// longest name suffix from the reference table (LATERAL LIMIT 1), the
// label — the name without "."+zone; a missing zone/label — zeros
// (coalesce), the order — the argument order (ord).
func (p *recheckPool) lookupRows(names []string) pgx.Rows {
	rows := make([][]any, len(names))
	for i, name := range names {
		var zoneID int16
		var zoneName string
		var zoneOn bool
		for _, z := range p.zones {
			if strings.HasSuffix(name, "."+z.name) && len(z.name) > len(zoneName) {
				zoneID, zoneName, zoneOn = z.id, z.name, z.active
			}
		}
		var domID int64
		var state, status int16
		label := ""
		if zoneName != "" {
			label = strings.TrimSuffix(name, "."+zoneName)
			for _, d := range p.domains {
				if d.zone == zoneID && d.label == label {
					domID, state, status = d.id, int16(d.state), int16(d.status)
					break
				}
			}
		}
		rows[i] = []any{int64(i + 1), zoneID, zoneName, zoneOn, domID, label, state, status}
	}
	return statusRowsOf(rows...)
}

// updateRows applies the recheckUpdateSQL semantics: a move to pending
// over PK keys with a re-check of status <> deleted; returns the keys of
// the actually moved rows (RETURNING). queued_at is reset.
func (p *recheckPool) updateRows(pending int16, zones []int16, ids []int64, deleted int16) pgx.Rows {
	var rows [][]any
	for i, zone := range zones {
		id := ids[i]
		for j := range p.domains {
			d := &p.domains[j]
			if d.zone == zone && d.id == id && int16(d.status) != deleted {
				d.state = model.CheckState(pending)
				d.queuedAt = false
				rows = append(rows, []any{zone, id})
				break
			}
		}
	}
	return statusRowsOf(rows...)
}

// domain — a fake domain row by key, for summary checks.

// namesOf — normalized punycode names (ord matches the input).
func namesOf(names ...string) []string {
	res := make([]string, len(names))
	for i, n := range names {
		r, err := idn.Normalize(n)
		if err != nil {
			panic(fmt.Sprintf("тестовое имя %q: %v", n, err))
		}
		res[i] = r.ASCII
	}
	return res
}

// --- move ----------------------------------------------------------------------

func TestRecheckRunTranslate(t *testing.T) {
	pool := newRecheckPool()
	pool.seed(7, 1, "done", model.CheckStateDone, model.StatusAlive)       // the main case
	pool.seed(7, 2, "pending", model.CheckStatePending, model.StatusAlive) // already waiting — idempotent
	pool.seed(7, 3, "queued", model.CheckStateQueued, model.StatusAlive)   // in work — as a lease refund
	pool.seed(7, 4, "temp", model.CheckStateDone, model.StatusTempError)   // temp_error — a candidate
	pool.seed(7, 5, "gone", model.CheckStateDone, model.StatusDeleted)     // deleted — a rejection

	var stdout bytes.Buffer
	err := recheckRun(context.Background(), pool,
		namesOf("done.crawl.test", "pending.crawl.test", "queued.crawl.test", "temp.crawl.test", "gone.crawl.test"),
		&stdout)
	if err == nil || !strings.Contains(err.Error(), "не поставлены: gone.crawl.test") {
		t.Fatalf("ошибка = %v, хочу отказ по gone.crawl.test (частичный успех)", err)
	}
	got := stdout.String()

	// The found ones are moved: one line per domain with the old state.
	for name, was := range map[string]string{
		"done.crawl.test":    "done",
		"pending.crawl.test": "pending",
		"queued.crawl.test":  "queued",
		"temp.crawl.test":    "done",
	} {
		if !strings.Contains(got, fmt.Sprintf("%s: поставлен на проверку (было check_state: %s)", name, was)) {
			t.Errorf("вывод не содержит «%s … было %s»:\n%s", name, was, got)
		}
	}
	if !strings.Contains(got, "gone.crawl.test: удалён (deleted)") {
		t.Errorf("вывод не содержит отказ deleted:\n%s", got)
	}
	if !strings.Contains(got, "переведено в проверку (pending): 4 из 5") {
		t.Errorf("вывод без итога:\n%s", got)
	}

	// Move semantics: check_state = pending, queued_at reset
	// (including the one taken into work), deleted untouched.
	for i, want := range []model.CheckState{model.CheckStatePending, model.CheckStatePending,
		model.CheckStatePending, model.CheckStatePending, model.CheckStateDone} {
		if d := pool.domains[i]; d.state != want {
			t.Errorf("домен %d: check_state = %v, хочу %v", i+1, d.state, want)
		}
	}
	if pool.domains[2].queuedAt {
		t.Error("queued_at не сброшен у переведённого из queued")
	}
	if !pool.domains[4].queuedAt && pool.domains[4].state != model.CheckStateDone {
		t.Errorf("deleted-домен изменился: %+v", pool.domains[4])
	}

	// The UPDATE parameter contract: the pending code, the PK keys, the deleted guard.
	upd := pool.queries[len(pool.queries)-1]
	if upd.sql != recheckUpdateSQL {
		t.Fatalf("SQL = %s, хочу перевод", upd.sql)
	}
	if got, want := upd.args[0], int16(model.CheckStatePending); got != want {
		t.Errorf("args[0] = %v, хочу %v", got, want)
	}
	if got, want := upd.args[1], []int16{7, 7, 7, 7}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("args[1] = %v, хочу зоны %v", got, want)
	}
	if got, want := upd.args[2], []int64{1, 2, 3, 4}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("args[2] = %v, хочу id %v", got, want)
	}
	if got, want := upd.args[3], int16(model.StatusDeleted); got != want {
		t.Errorf("args[3] = %v, хочу %v", got, want)
	}
}

func TestRecheckRunAllTranslated(t *testing.T) {
	pool := newRecheckPool()
	pool.seed(7, 1, "example", model.CheckStateDone, model.StatusAlive)

	var stdout bytes.Buffer
	if err := recheckRun(context.Background(), pool, namesOf("EXAMPLE.Crawl.Test."), &stdout); err != nil {
		t.Fatalf("recheckRun: %v (нормализованное имя должно находиться)", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "example.crawl.test: поставлен на проверку (было check_state: done)") {
		t.Errorf("вывод не содержит итог:\n%s", got)
	}
	if !strings.Contains(got, "переведено в проверку (pending): 1 из 1") {
		t.Errorf("вывод без итога 1 из 1:\n%s", got)
	}
	if !strings.Contains(got, "домены проверит планировщик") {
		t.Errorf("вывод без подсказки о планировщике:\n%s", got)
	}
}

func TestRecheckRunNotFound(t *testing.T) {
	pool := newRecheckPool()
	pool.seed(7, 1, "example", model.CheckStateDone, model.StatusAlive)

	var stdout bytes.Buffer
	err := recheckRun(context.Background(), pool, namesOf("ghost.crawl.test", "x.nowhere.test"), &stdout)
	if err == nil || !strings.Contains(err.Error(), "не поставлены: ghost.crawl.test, x.nowhere.test") {
		t.Fatalf("ошибка = %v, хочу список не найденных", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "ghost.crawl.test: не найден: в зоне crawl.test нет домена с меткой ghost") {
		t.Errorf("вывод не уточняет «нет метки в зоне»:\n%s", got)
	}
	if !strings.Contains(got, "x.nowhere.test: не найден: зоны nowhere.test нет в справочнике") {
		t.Errorf("вывод не уточняет «нет зоны» (длиннейший суффикс имени):\n%s", got)
	}
	if !strings.Contains(got, "переведено в проверку (pending): 0 из 2") {
		t.Errorf("вывод без итога 0 из 2:\n%s", got)
	}
	// No move happened — the name resolution only.
	if len(pool.queries) != 1 {
		t.Errorf("обращений = %d, хочу 1 (только lookup)", len(pool.queries))
	}
}

func TestRecheckRunInactiveZone(t *testing.T) {
	// The zone is disabled (zones disable): the domain is moved but the
	// check waits for the zone to be enabled — a warning in the domain line.
	pool := newRecheckPool()
	pool.seed(9, 1, "paused", model.CheckStateDone, model.StatusAlive)

	var stdout bytes.Buffer
	if err := recheckRun(context.Background(), pool, namesOf("paused.off.test"), &stdout); err != nil {
		t.Fatalf("recheckRun: %v (перевод выключенной зоны — не ошибка)", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "зона выключена, домен ждёт включения (crawlerctl zones enable off.test)") {
		t.Errorf("вывод без предупреждения о выключенной зоне:\n%s", got)
	}
	if pool.domains[0].state != model.CheckStatePending {
		t.Errorf("домен не переведён: %+v", pool.domains[0])
	}
}

func TestRecheckRunDuplicates(t *testing.T) {
	// A repeated name in the arguments: both resolutions find the same
	// domain, the move by key applies once, the output — one line per argument.
	pool := newRecheckPool()
	pool.seed(7, 1, "dup", model.CheckStateDone, model.StatusAlive)

	var stdout bytes.Buffer
	if err := recheckRun(context.Background(), pool, namesOf("dup.crawl.test", "dup.crawl.test"), &stdout); err != nil {
		t.Fatalf("recheckRun: %v", err)
	}
	got := stdout.String()
	if strings.Count(got, "dup.crawl.test: поставлен на проверку") != 2 {
		t.Errorf("вывод без построчного итога для каждого аргумента:\n%s", got)
	}
	if !strings.Contains(got, "переведено в проверку (pending): 1 из 2") {
		t.Errorf("итог должен считать переведённые строки, не аргументы:\n%s", got)
	}
}

func TestRecheckRunRaceDeleted(t *testing.T) {
	// The status changed to deleted between the lookup and the UPDATE: the
	// status <> deleted guard skips the row, RETURNING does not return it —
	// the domain ends up in the rejection as "not enqueued".
	pool := newRecheckPool()
	pool.seed(7, 1, "racy", model.CheckStateDone, model.StatusAlive)
	pool.onUpdate = func() { pool.domains[0].status = model.StatusDeleted }

	var stdout bytes.Buffer
	err := recheckRun(context.Background(), pool, namesOf("racy.crawl.test"), &stdout)
	if err == nil || !strings.Contains(err.Error(), "не поставлены: racy.crawl.test") {
		t.Fatalf("ошибка = %v, хочу отказ из-за гонки с delete", err)
	}
	if !strings.Contains(stdout.String(), "не поставлен: статус изменился на deleted") {
		t.Errorf("вывод не объясняет пропуск:\n%s", stdout.String())
	}
}

func TestRecheckRunErrors(t *testing.T) {
	pool := newRecheckPool()
	pool.seed(7, 1, "example", model.CheckStateDone, model.StatusAlive)

	pool.queryErr = errors.New("нет соединения")
	err := recheckRun(context.Background(), pool, namesOf("example.crawl.test"), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "разрешение имён доменов") {
		t.Fatalf("ошибка = %v, хочу контекст разрешения имён", err)
	}

	pool2 := newRecheckPool()
	pool2.seed(7, 1, "example", model.CheckStateDone, model.StatusAlive)
	pool2.queryErrAfter, pool2.queryErr = 1, errors.New("нет соединения")
	err = recheckRun(context.Background(), pool2, namesOf("example.crawl.test"), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "перевод доменов в pending") {
		t.Fatalf("ошибка = %v, хочу контекст перевода", err)
	}
}

// --- joinNames ------------------------------------------------------------------

func TestJoinNames(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"a"}, "a"},
		{[]string{"a", "b", "c"}, "a, b, c"},
		{[]string{"a", "b", "c", "d"}, "a, b, c (+1)"},
		{[]string{"a", "b", "c", "d", "e"}, "a, b, c (+2)"},
	}
	for _, tc := range cases {
		if got := joinNames(tc.in); got != tc.want {
			t.Errorf("joinNames(%q) = %q, хочу %q", tc.in, got, tc.want)
		}
	}
}

// --- command via run -------------------------------------------------------------

// TestRunRecheckExecution — the recheck execution path without
// infrastructure: a deterministic configuration failure (code 1) with the
// command context — name validation runs earlier (usage errors — code 2).
func TestRunRecheckExecution(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "нет конфигурации", args: []string{"recheck", "example.crawl.test"}, code: exitError, inErr: "recheck example.crawl.test: config:"},
		{name: "имя нормализуется до конфигурации", args: []string{"recheck", "МосквА.рф"}, code: exitError, inErr: "recheck xn--80adxhks.xn--p1ai: config:"},
		{name: "список имён в контексте", args: []string{"recheck", "a.crawl.test", "b.crawl.test"}, code: exitError, inErr: "recheck a.crawl.test, b.crawl.test: config:"},
		{name: "без домена", args: []string{"recheck"}, code: exitUsage, inErr: "укажите домен"},
		{name: "невалидное имя", args: []string{"recheck", "ok.crawl.test", "bad!!name"}, code: exitUsage, inErr: "некорректное имя домена"},
		{name: "неизвестный флаг", args: []string{"recheck", "--nope", "a.crawl.test"}, code: exitUsage, inErr: "recheck:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) код = %d, хочу %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr не содержит %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
		})
	}
}
