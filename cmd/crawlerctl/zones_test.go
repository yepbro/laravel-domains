// Unit tests for crawlerctl zones (T-084, spec 4.1, 5.3) on a fake pool
// (db.DB) — without a DB. Checked: the exact SQL texts (a contract, the
// subdomains_test.go style) and the fake's live semantics: the reference
// table (list by ascending name, NULL/matching name_unicode — "—", the
// counters, the totals row), enabling/disabling by name (normalize runs
// first, the UPDATE changes only is_active, a repeat — "already
// enabled/disabled", domains not touched), errors (an unknown zone, a DB
// failure). Behavior against live PostgreSQL — the dev environment
// (make shell).
package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
)

// --- SQL contracts ---------------------------------------------------------

// The want texts duplicate the constants: a SQL change without updating
// the expectations (or vice versa) fails the test loudly.
const (
	wantZonesSelect = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
		"is_active, domains_total, domains_checked FROM zones ORDER BY name"

	wantZoneByName = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
		"is_active, domains_total, domains_checked FROM zones WHERE name = $1"

	wantZoneSetActive = "UPDATE zones SET is_active = $1 WHERE id = $2 " +
		"RETURNING id, name, coalesce(name_unicode, '') AS name_unicode, is_active"
)

func TestZonesSQLContract(t *testing.T) {
	for sql, want := range map[string]string{
		zonesSelectSQL:   wantZonesSelect,
		zoneByNameSQL:    wantZoneByName,
		zoneSetActiveSQL: wantZoneSetActive,
	} {
		if sql != want {
			t.Errorf("SQL-контракт:\nполучен %s\nожидался %s", sql, want)
		}
	}
}

// --- fake pool (db.DB) --------------------------------------------------------

// zonesPool — a db.DB without a DB: the live semantics of the zones
// command SQL — the reference table (SELECT of the whole list and by
// name) and the is_active UPDATE by id (RETURNING of the row; only the
// flag changes).
type zonesPool struct {
	zones []zoneRow

	mu       sync.Mutex
	queryErr error
	queries  []subCall
	execs    []subCall
}

var _ db.DB = (*zonesPool)(nil)

func newZonesPool() *zonesPool {
	return &zonesPool{
		zones: []zoneRow{
			{id: 7, name: "crawl.test", nameUnicode: "", active: true, total: 120, checked: 80},
			{id: 9, name: "example.test", nameUnicode: "", active: false, total: 10, checked: 0},
			{id: 11, name: "xn--80adxhks", nameUnicode: "москва", active: true, total: 3, checked: 3},
		},
	}
}

func (p *zonesPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("zonesPool: CopyFrom не должен вызываться")
}

func (p *zonesPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execs = append(p.execs, subCall{sql, append([]any(nil), args...)})
	return pgconn.CommandTag{}, errors.New("zonesPool: неожидаемый Exec: " + sql)
}

func (p *zonesPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, subCall{sql, append([]any(nil), args...)})
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	case zonesSelectSQL:
		sorted := append([]zoneRow(nil), p.zones...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
		rows := make([][]any, len(sorted))
		for i, z := range sorted {
			rows[i] = []any{z.id, z.name, z.nameUnicode, z.active, z.total, z.checked}
		}
		return statusRowsOf(rows...), nil
	case zoneByNameSQL:
		for _, z := range p.zones {
			if z.name == args[0].(string) {
				return statusRowsOf([]any{z.id, z.name, z.nameUnicode, z.active, z.total, z.checked}), nil
			}
		}
		return statusRowsOf(), nil
	case zoneSetActiveSQL:
		active, id := args[0].(bool), args[1].(int16)
		for i := range p.zones {
			if p.zones[i].id == id {
				p.zones[i].active = active // only is_active changes
				z := p.zones[i]
				return statusRowsOf([]any{z.id, z.name, z.nameUnicode, z.active}), nil
			}
		}
		return statusRowsOf(), nil
	default:
		return nil, errors.New("zonesPool: неожидаемый SQL Query: " + sql)
	}
}

// --- list ---------------------------------------------------------------------

func TestZonesListRun(t *testing.T) {
	pool := newZonesPool()

	var stdout bytes.Buffer
	if err := zonesListRun(context.Background(), pool, &stdout); err != nil {
		t.Fatalf("zonesListRun: %v", err)
	}
	got := stdout.String()

	// Rows by ascending name (the first — crawl.test); unicode — only the
	// one differing from punycode (ASCII zones without name_unicode — "—");
	// act — the last character of the row.
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 1+len(pool.zones)+1 { // header + zones + totals
		t.Fatalf("строк %d, хочу %d:\n%s", len(lines), len(pool.zones)+2, got)
	}
	if !strings.HasPrefix(lines[0], "id") || !strings.Contains(lines[0], "зона") {
		t.Errorf("строка 0 = %q, хочу заголовок таблицы", lines[0])
	}
	if !strings.Contains(lines[1], "7") || !strings.Contains(lines[1], "crawl.test") {
		t.Errorf("строка 1 = %q, хочу зону id 7 crawl.test", lines[1])
	}
	if !strings.Contains(lines[1], "—") || strings.Contains(lines[1], "москва") {
		t.Errorf("unicode-колонка ASCII-зоны должна быть «—»: %q", lines[1])
	}
	// "act" — the column after unicode; total/checked come further, so
	// the mark is looked up by the row fields.
	if fields := strings.Fields(lines[1]); len(fields) < 4 || fields[3] != "+" {
		t.Errorf("активная зона — акт «+»: %q", lines[1])
	}
	if fields := strings.Fields(lines[2]); len(fields) < 4 || fields[3] != "-" {
		t.Errorf("выключенная зона — акт «-»: %q", lines[2])
	}
	if !strings.Contains(lines[3], "xn--80adxhks") || !strings.Contains(lines[3], "москва") {
		t.Errorf("unicode-зона показывает обе формы: %q", lines[3])
	}
	if !strings.Contains(got, "зон 3, включено 2, доменов всего 133, проверено 83") {
		t.Errorf("вывод без итога:\n%s", got)
	}
	if len(pool.execs) != 0 {
		t.Errorf("list только читает, Exec не ждали: %v", pool.execs)
	}
}

func TestZonesListRunQueryError(t *testing.T) {
	pool := newZonesPool()
	pool.queryErr = errors.New("бой соединения")
	err := zonesListRun(context.Background(), pool, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "справочник зон") {
		t.Fatalf("ошибка = %v, хочу контекст справочника", err)
	}
}

// --- enable / disable ----------------------------------------------------------

func TestZonesToggleRun(t *testing.T) {
	pool := newZonesPool()

	// Disabling an active zone.
	var stdout bytes.Buffer
	if err := zonesToggleRun(context.Background(), pool, "disable", "crawl.test", &stdout); err != nil {
		t.Fatalf("zonesToggleRun disable: %v", err)
	}
	if !strings.Contains(stdout.String(), "crawl.test (id 7) выключена") {
		t.Errorf("вывод disable не содержит итог:\n%s", stdout.String())
	}
	if pool.zones[0].active {
		t.Error("crawl.test должна быть выключена после disable")
	}
	// Domains are not touched: no UPDATE domains was executed at all
	// (only the lookup by name and the UPDATE zones).
	for _, c := range pool.queries {
		if strings.Contains(c.sql, "FROM domains") {
			t.Errorf("toggle не должен трогать domains: %v", c)
		}
	}
	upd := pool.queries[len(pool.queries)-1]
	if upd.sql != zoneSetActiveSQL || upd.args[0] != false || upd.args[1] != int16(7) {
		t.Errorf("UPDATE = %v %v, хочу is_active=false, id=7", upd.sql, upd.args)
	}

	// Enabling a disabled zone — with the unicode name in the message.
	stdout.Reset()
	if err := zonesToggleRun(context.Background(), pool, "enable", "example.test", &stdout); err != nil {
		t.Fatalf("zonesToggleRun enable: %v", err)
	}
	if !strings.Contains(stdout.String(), "example.test (id 9) включена") {
		t.Errorf("вывод enable не содержит итог:\n%s", stdout.String())
	}
	if !pool.zones[1].active {
		t.Error("example.test должна быть включена после enable")
	}
}

func TestZonesToggleRunUnicodeZone(t *testing.T) {
	// A zone with name_unicode: the messages show both forms.
	pool := newZonesPool()
	pool.zones[0].active = false

	var stdout bytes.Buffer
	if err := zonesToggleRun(context.Background(), pool, "enable", "xn--80adxhks", &stdout); err != nil {
		t.Fatalf("zonesToggleRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "xn--80adxhks (москва) (id 11) уже включена") {
		t.Errorf("вывод не содержит «уже включена» с unicode-именем:\n%s", stdout.String())
	}
	// The state matches — no UPDATE was executed (idempotency).
	last := pool.queries[len(pool.queries)-1]
	if last.sql == zoneSetActiveSQL {
		t.Errorf("повторный enable не должен обновлять: %v", last)
	}
}

func TestZonesToggleRunAlready(t *testing.T) {
	pool := newZonesPool()

	var stdout bytes.Buffer
	if err := zonesToggleRun(context.Background(), pool, "disable", "example.test", &stdout); err != nil {
		t.Fatalf("повторный disable — не ошибка: %v", err)
	}
	if !strings.Contains(stdout.String(), "уже выключена") {
		t.Errorf("вывод не сообщает «уже выключена»:\n%s", stdout.String())
	}
}

func TestZonesToggleRunNotFound(t *testing.T) {
	pool := newZonesPool()
	err := zonesToggleRun(context.Background(), pool, "enable", "nope.test", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "зона nope.test не найдена в справочнике") {
		t.Fatalf("ошибка = %v, хочу «не найдена в справочнике»", err)
	}
	for _, c := range pool.queries {
		if c.sql == zoneSetActiveSQL {
			t.Errorf("неизвестная зона не должна обновляться: %v", c)
		}
	}
}

func TestZonesToggleRunQueryError(t *testing.T) {
	pool := newZonesPool()
	pool.queryErr = errors.New("нет соединения")
	err := zonesToggleRun(context.Background(), pool, "disable", "crawl.test", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "зона по имени crawl.test") {
		t.Fatalf("ошибка = %v, хочу контекст поиска зоны", err)
	}
}

// --- commands via run -----------------------------------------------------------

// TestRunZonesExecution — the zones command execution path without
// infrastructure: a deterministic configuration failure (code 1) with the
// command context — input validation runs earlier (usage errors — code 2).
func TestRunZonesExecution(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "list — нет конфигурации", args: []string{"zones", "list"}, code: exitError, inErr: "zones list: config:"},
		{name: "enable — нет конфигурации", args: []string{"zones", "enable", "crawl.test"}, code: exitError, inErr: "zones enable crawl.test: config:"},
		{name: "disable — нет конфигурации", args: []string{"zones", "disable", "crawl.test"}, code: exitError, inErr: "zones disable crawl.test: config:"},
		{name: "enable — зона нормализуется до конфигурации", args: []string{"zones", "enable", "МосквА."}, code: exitError, inErr: "zones enable xn--80adxhks: config:"},
		{name: "list лишний аргумент", args: []string{"zones", "list", "foo"}, code: exitUsage, inErr: "лишние аргументы"},
		{name: "enable без зоны", args: []string{"zones", "enable"}, code: exitUsage, inErr: "укажите зону"},
		{name: "enable две зоны", args: []string{"zones", "disable", "a.test", "b.test"}, code: exitUsage, inErr: "укажите зону"},
		{name: "enable невалидная зона", args: []string{"zones", "enable", "bad!!zone"}, code: exitUsage, inErr: "некорректное имя зоны"},
		{name: "enable неизвестный флаг", args: []string{"zones", "enable", "--nope", "crawl.test"}, code: exitUsage, inErr: "zones enable:"},
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
