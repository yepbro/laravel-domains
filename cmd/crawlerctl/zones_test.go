// Юнит-тесты crawlerctl zones (T-084, ТЗ 4.1, 5.3) на фейковом пуле
// (db.DB) — без БД. Проверяются точные SQL-тексты (контракт, стиль
// subdomains_test.go) и живая семантика фейка: справочник (list по
// возрастанию имени, NULL/совпадающий name_unicode — «—», счётчики,
// итоговая строка), включение/выключение по имени (normalize идёт
// раньше, UPDATE меняет только is_active, повтор — «уже включена/
// выключена», домены не трогаются), ошибки (неизвестная зона, сбой БД).
// Поведение против живого PostgreSQL — dev-окружение (make shell).
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

// --- SQL-контракты ----------------------------------------------------------

// want-тексты дублируют константы: изменение SQL без обновления ожиданий
// (или наоборот) валит тест громко.
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

// --- фейковый пул (db.DB) -----------------------------------------------------

// zonesPool — db.DB без БД: живая семантика SQL команды zones — справочник
// по имени (SELECT всего списка и по имени) и UPDATE is_active по id
// (RETURNING строки; меняется только флаг).
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
				p.zones[i].active = active // меняется только is_active
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

	// Строки по возрастанию имени (первая — crawl.test); unicode — только
	// отличающийся от punycode (ASCII-зоны без name_unicode — «—»);
	// акт — последний символ строки.
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 1+len(pool.zones)+1 { // заголовок + зоны + итог
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
	// «акт» — колонка после unicode; всего/проверено идут дальше,
	// поэтому маркер ищется по полям строки.
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

	// disable активной зоны.
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
	// Домены не трогаются: UPDATE domains не выполнялся вовсе (только
	// lookup по имени и UPDATE zones).
	for _, c := range pool.queries {
		if strings.Contains(c.sql, "FROM domains") {
			t.Errorf("toggle не должен трогать domains: %v", c)
		}
	}
	upd := pool.queries[len(pool.queries)-1]
	if upd.sql != zoneSetActiveSQL || upd.args[0] != false || upd.args[1] != int16(7) {
		t.Errorf("UPDATE = %v %v, хочу is_active=false, id=7", upd.sql, upd.args)
	}

	// enable выключенной зоны — с unicode-именем в сообщении.
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
	// Зона с name_unicode: сообщения показывают обе формы.
	pool := newZonesPool()
	pool.zones[0].active = false

	var stdout bytes.Buffer
	if err := zonesToggleRun(context.Background(), pool, "enable", "xn--80adxhks", &stdout); err != nil {
		t.Fatalf("zonesToggleRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "xn--80adxhks (москва) (id 11) уже включена") {
		t.Errorf("вывод не содержит «уже включена» с unicode-именем:\n%s", stdout.String())
	}
	// Состояние совпадает — UPDATE не выполнялся (идемпотентность).
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

// --- команды через run --------------------------------------------------------

// TestRunZonesExecution — путь выполнения команд zones без инфраструктуры:
// детерминированный отказ конфигурации (код 1) с контекстом команды —
// валидация входа идёт раньше (ошибки использования — код 2).
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
