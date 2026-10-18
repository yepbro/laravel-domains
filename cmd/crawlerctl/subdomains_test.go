// Юнит-тесты crawlerctl subdomains (T-083, ТЗ 5.10, 13.1) на фейковом
// пуле (db.DB) — без БД. Проверяются точные SQL-тексты (контракт, стиль
// status_test.go) и живая семантика фейка: справочник (уникальность
// метки, назначение свободного бита max(id)+1, максимум 63 — бит 63
// знаковый; disable меняет только enabled, бит и маски стабильны),
// запуск проверки нового субдомена (done → pending пачками с охраной
// check_state/status и фильтром зоны, deleted исключается, счётчик
// затронутых), ошибки (нет субдомена, выключен, зона не найдена, сбой
// БД, прерывание дренажа). Поведение против живого PostgreSQL —
// dev-окружение (docker compose run --rm tools go run ./cmd/crawlerctl).
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

// --- SQL-контракты ----------------------------------------------------------

// want-тексты дублируют константы: изменение SQL без обновления ожиданий
// (или наоборот) валит тест громко.
const (
	wantSubdomainsSelect = "SELECT id, label, enabled FROM subdomains ORDER BY id"
	wantSubdomainByLabel = "SELECT id, label, enabled FROM subdomains WHERE label = $1"
	wantSubdomainByID    = "SELECT id, label, enabled FROM subdomains WHERE id = $1"
	wantSubdomainNextID  = "SELECT coalesce(max(id), -1) + 1 FROM subdomains"
	wantSubdomainInsert  = "INSERT INTO subdomains (id, label, enabled) VALUES ($1, $2, true) RETURNING id"
	wantSubdomainDisable = "UPDATE subdomains SET enabled = false WHERE id = $1 RETURNING id, label, enabled"

	wantSubRecheckAll = "UPDATE domains AS d SET check_state = $1, queued_at = NULL " +
		"FROM (SELECT zone_id, id FROM domains " +
		"WHERE check_state = $2 AND status <> $3 " +
		"ORDER BY zone_id, id LIMIT $4) AS b " +
		"WHERE d.zone_id = b.zone_id AND d.id = b.id " +
		"AND d.check_state = $2 AND d.status <> $3"

	wantSubRecheckZone = "UPDATE domains AS d SET check_state = $1, queued_at = NULL " +
		"FROM (SELECT zone_id, id FROM domains " +
		"WHERE check_state = $2 AND status <> $3 AND zone_id = $4 " +
		"ORDER BY zone_id, id LIMIT $5) AS b " +
		"WHERE d.zone_id = b.zone_id AND d.id = b.id " +
		"AND d.check_state = $2 AND d.status <> $3 AND d.zone_id = $4"

	// wantSubZoneIDByName — контракт analyzer.ZoneIDByName (переиспользуется
	// recheck для --zone): текст зафиксирован здесь, чтобы смена чужого
	// запроса не прошла молча.
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
			t.Errorf("SQL-контракт:\nполучен %s\nожидался %s", sql, want)
		}
	}
}

// --- фейковый пул (db.DB) -----------------------------------------------------

// subdomSeed — домен фейка для семантики перевода done → pending.
type subdomSeed struct {
	zone   int16
	id     int64
	state  model.CheckState
	status model.Status
}

// subCall — обращение к пулу для контрактных проверок.
type subCall struct {
	sql  string
	args []any
}

// subdomainsPool — db.DB без БД: живая семантика SQL команды subdomains —
// справочник (SELECT по метке/биту, max(id)+1, INSERT, UPDATE enabled),
// зоны по имени (аналог analyzer.ZoneIDByName) и перевод done → pending
// по domains (выборка с LIMIT, охраной check_state/status, фильтром
// зоны, ORDER BY zone_id, id).
type subdomainsPool struct {
	subs    []subdomainRow
	zones   map[string]int16
	domains []subdomSeed

	mu           sync.Mutex
	queryErr     error
	execErr      error
	execErrAfter int // первые N Exec успешны, затем execErr (прерывание дренажа)
	queries      []subCall
	execs        []subCall
}

var _ db.DB = (*subdomainsPool)(nil)

// newSubdomainsPool — начальное наполнение миграции 000001 и две зоны.
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

// subByID — строка справочника по биту для проверок итога.
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
	return 0, errors.New("subdomainsPool: CopyFrom не должен вызываться")
}

func (p *subdomainsPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execs = append(p.execs, subCall{sql, append([]any(nil), args...)})
	if p.execErr != nil && (p.execErrAfter == 0 || len(p.execs) > p.execErrAfter) {
		return pgconn.CommandTag{}, p.execErr
	}
	switch sql {
	case subdomainRecheckAllSQL:
		n := p.recheck(args[1].(int16), args[2].(int16), 0, args[3].(int))
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
	case subdomainRecheckZoneSQL:
		n := p.recheck(args[1].(int16), args[2].(int16), args[3].(int16), args[4].(int))
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("subdomainsPool: неожидаемый Exec: %s", sql)
	}
}

// recheck — семантика подзапроса и внешнего WHERE: кандидаты — done
// и не deleted [зоны], ORDER BY (zone_id, id), LIMIT; выбранные
// переводятся в pending. Возвращает число переведённых.
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
				return nil, fmt.Errorf("subdomainsPool: PK %d уже занят", id)
			}
			if s.label == label {
				return nil, fmt.Errorf(`subdomainsPool: метка %q уже есть (UNIQUE)`, label)
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
		return nil, fmt.Errorf("subdomainsPool: неожидаемый SQL Query: %s", sql)
	}
}

// --- list ---------------------------------------------------------------------

func TestSubdomainsListRun(t *testing.T) {
	pool := newSubdomainsPool()
	pool.subs[2].enabled = false // admin выключен

	var stdout bytes.Buffer
	if err := subdomainsListRun(context.Background(), pool, &stdout); err != nil {
		t.Fatalf("subdomainsListRun: %v", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "бит  метка") {
		t.Errorf("вывод без заголовка:\n%s", got)
	}
	// Строки по порядку битов (апекс '' показан как «(апекс)»); акт —
	// последний символ строки: «+» включён, «-» выключен.
	lines := strings.Split(strings.TrimSpace(got), "\n")
	rows := []struct{ prefix, mark string }{
		{"  0  (апекс)", "+"},
		{"  1  www", "+"},
		{"  2  admin", "-"},
		{"  3  crm", "+"},
		{"  4  api", "+"},
		{"  5  app", "+"},
	}
	for i, w := range rows {
		if len(lines) <= i+1 {
			t.Fatalf("строка %d отсутствует:\n%s", i+1, got)
		}
		if !strings.HasPrefix(lines[i+1], w.prefix) || !strings.HasSuffix(lines[i+1], w.mark) {
			t.Errorf("строка %d = %q, хочу префикс %q и акт %q", i+1, lines[i+1], w.prefix, w.mark)
		}
	}
	if !strings.Contains(got, "всего 6 из 63, включено 5") {
		t.Errorf("вывод без итога «всего/включено»:\n%s", got)
	}
	if len(pool.execs) != 0 {
		t.Errorf("list только читает, Exec не ждали: %v", pool.execs)
	}
}

func TestSubdomainsListRunQueryError(t *testing.T) {
	pool := newSubdomainsPool()
	pool.queryErr = errors.New("бой соединения")
	err := subdomainsListRun(context.Background(), pool, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "справочник субдоменов") {
		t.Fatalf("ошибка = %v, хочу контекст справочника", err)
	}
}

// --- add ----------------------------------------------------------------------

func TestSubdomainsAddRun(t *testing.T) {
	pool := newSubdomainsPool()

	var stdout bytes.Buffer
	if err := subdomainsAddRun(context.Background(), pool, "shop", &stdout); err != nil {
		t.Fatalf("subdomainsAddRun: %v", err)
	}
	// Свободный бит — max(id)+1 = 6; строка вставлена включённой.
	if want := (subdomainRow{6, "shop", true}); pool.subs[len(pool.subs)-1] != want {
		t.Errorf("вставлена строка %+v, хочу %+v", pool.subs[len(pool.subs)-1], want)
	}
	for _, want := range []string{"субдомен shop добавлен: бит 6", "subdomains recheck shop"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("вывод не содержит %q:\n%s", want, stdout.String())
		}
	}
}

func TestSubdomainsAddRunDuplicate(t *testing.T) {
	pool := newSubdomainsPool()
	err := subdomainsAddRun(context.Background(), pool, "www", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "уже есть в справочнике: бит 1, включён") {
		t.Fatalf("ошибка = %v, хочу «уже есть … бит 1»", err)
	}
	// Вставки не было: только проверка метки и свободного бита не дошли.
	for _, c := range pool.queries {
		if c.sql == subdomainInsertSQL {
			t.Errorf("дубль метки не должен вставлять строку: %v", c)
		}
	}

	// Выключенная метка — тот же отказ, состояние указано.
	pool2 := newSubdomainsPool()
	pool2.subs[1].enabled = false
	err = subdomainsAddRun(context.Background(), pool2, "www", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "бит 1, выключен") {
		t.Fatalf("ошибка = %v, хочу «бит 1, выключен»", err)
	}
}

func TestSubdomainsAddRunMax63(t *testing.T) {
	// Справочник заполнен: биты 0..62 (63 строки) — следующего бита нет
	// (бит 63 знаковый), вставка не выполняется.
	pool := newSubdomainsPool()
	pool.subs = nil
	for id := int16(0); id < 63; id++ {
		pool.subs = append(pool.subs, subdomainRow{id, fmt.Sprintf("s%d", id), true})
	}
	err := subdomainsAddRun(context.Background(), pool, "shop", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "вне диапазона 0..62") {
		t.Fatalf("ошибка = %v, хочу отказ диапазона 0..62", err)
	}
	if len(pool.subs) != 63 {
		t.Errorf("справочник изменился: %d строк, хочу 63", len(pool.subs))
	}
}

func TestSubdomainsAddRunInsertError(t *testing.T) {
	pool := newSubdomainsPool()
	pool.queryErr = errors.New("нет соединения")
	// Ошибка проявляется на первом же обращении (проверка метки).
	err := subdomainsAddRun(context.Background(), pool, "shop", &bytes.Buffer{})
	if err == nil {
		t.Fatal("ожидалась ошибка БД")
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
		t.Error("www должен быть выключен после disable")
	}
	// Бит стабилен: строка осталась на месте, id/метка не менялись.
	if pool.subs[1] != (subdomainRow{1, "www", false}) {
		t.Errorf("строка www = %+v, хочу {1 www false}", pool.subs[1])
	}
	if !strings.Contains(stdout.String(), "www (бит 1) выключен") {
		t.Errorf("вывод не содержит итог:\n%s", stdout.String())
	}
	// Маски/данные не трогаются: UPDATE domains не выполнялся вовсе.
	if len(pool.execs) != 0 {
		t.Errorf("disable не трогает domains, Exec не ждали: %v", pool.execs)
	}
}

func TestSubdomainsDisableRunByID(t *testing.T) {
	pool := newSubdomainsPool()

	var stdout bytes.Buffer
	if err := subdomainsDisableRun(context.Background(), pool, "3", &stdout); err != nil {
		t.Fatalf("disable по номеру бита: %v", err)
	}
	if pool.subs[3] != (subdomainRow{3, "crm", false}) {
		t.Errorf("строка crm = %+v, хочу {3 crm false}", pool.subs[3])
	}
	if !strings.Contains(stdout.String(), "crm (бит 3) выключен") {
		t.Errorf("вывод не содержит итог:\n%s", stdout.String())
	}
}

func TestSubdomainsDisableRunLabelPriority(t *testing.T) {
	// Цифровая метка существует — она важнее номера бита.
	pool := newSubdomainsPool()
	pool.seedSub(subdomainRow{7, "3", true})

	var stdout bytes.Buffer
	if err := subdomainsDisableRun(context.Background(), pool, "3", &stdout); err != nil {
		t.Fatalf("disable цифровой метки: %v", err)
	}
	bit3, bit7 := pool.subByID(3), pool.subByID(7)
	if bit3 != (subdomainRow{3, "crm", true}) || bit7 != (subdomainRow{7, "3", false}) {
		t.Errorf("выключена метка «3» (бит 7), crm (бит 3) не тронут: crm=%+v метка3=%+v", bit3, bit7)
	}
	if !strings.Contains(stdout.String(), "3 (бит 7) выключен") {
		t.Errorf("вывод не содержит итог:\n%s", stdout.String())
	}
}

func TestSubdomainsDisableRunNotFound(t *testing.T) {
	pool := newSubdomainsPool()
	err := subdomainsDisableRun(context.Background(), pool, "nope", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), `субдомен "nope" не найден в справочнике`) {
		t.Fatalf("ошибка = %v, хочу «не найден в справочнике»", err)
	}
	// Числовой аргумент мимо занятых битов — тоже не найден.
	err = subdomainsDisableRun(context.Background(), pool, "42", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "не найден") {
		t.Fatalf("ошибка = %v, хочу «не найден»", err)
	}
}

// TestSubdomainsDisableRunBitOutOfRange — числовой аргумент вне диапазона
// битов 0..62: усечение int16 не должно молча сводить его к чужому биту
// (65537 → www, бит 1) — «не найден», справочник не тронут.
func TestSubdomainsDisableRunBitOutOfRange(t *testing.T) {
	pool := newSubdomainsPool()
	for _, arg := range []string{"65537", "131073", "100", "-1", "65536"} {
		err := subdomainsDisableRun(context.Background(), pool, arg, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "не найден") {
			t.Errorf("disable %q: ошибка = %v, хочу «не найден»", arg, err)
		}
	}
	if pool.subs[1] != (subdomainRow{1, "www", true}) {
		t.Errorf("www не должен меняться: %+v", pool.subs[1])
	}
	if pool.subs[0] != (subdomainRow{0, "", true}) {
		t.Errorf("апекс не должен меняться: %+v", pool.subs[0])
	}
}

func TestSubdomainsDisableRunApexGuard(t *testing.T) {
	pool := newSubdomainsPool()
	for _, arg := range []string{"", "0", "  "} {
		err := subdomainsDisableRun(context.Background(), pool, arg, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "апекс (бит 0)") {
			t.Errorf("disable %q: ошибка = %v, хочу запрет выключения апекса", arg, err)
		}
	}
	if pool.subs[0] != (subdomainRow{0, "", true}) {
		t.Errorf("апекс изменился: %+v", pool.subs[0])
	}
}

func TestSubdomainsDisableRunAlreadyDisabled(t *testing.T) {
	pool := newSubdomainsPool()
	pool.subs[2].enabled = false // admin уже выключен

	var stdout bytes.Buffer
	if err := subdomainsDisableRun(context.Background(), pool, "admin", &stdout); err != nil {
		t.Fatalf("повторный disable — не ошибка: %v", err)
	}
	if !strings.Contains(stdout.String(), "уже выключен") {
		t.Errorf("вывод не сообщает «уже выключен»:\n%s", stdout.String())
	}
}

// --- recheck ------------------------------------------------------------------

func TestSubdomainsRecheckRunTranslate(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)        // переводится
	pool.seedDomain(7, 2, model.CheckStateDone, model.StatusDeleted)      // deleted — охрана
	pool.seedDomain(7, 3, model.CheckStatePending, model.StatusAlive)     // уже в цикле
	pool.seedDomain(7, 4, model.CheckStateQueued, model.StatusAlive)      // в работе (lease)
	pool.seedDomain(9, 5, model.CheckStateDone, model.StatusNotDelegated) // переводится
	pool.seedDomain(9, 6, model.CheckStateDone, model.StatusTempError)    // переводится (temp_error — кандидат)

	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "все", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "субдомен www (бит 1), зона: все") || !strings.Contains(got, "переведено в проверку (pending): 3") {
		t.Errorf("вывод не содержит итог:\n%s", got)
	}
	// Семантика: done и не deleted → pending; остальные состояния и
	// статусы не тронуты.
	want := []model.CheckState{model.CheckStatePending, model.CheckStateDone,
		model.CheckStatePending, model.CheckStateQueued, model.CheckStatePending, model.CheckStatePending}
	for i, w := range want {
		if pool.domains[i].state != w {
			t.Errorf("домен %d: check_state = %v, хочу %v", i+1, pool.domains[i].state, w)
		}
	}

	// Контракт параметров: коды pending/done/deleted и размер пачки.
	if len(pool.execs) != 1 {
		t.Fatalf("Exec обращений = %d, хочу 1 (частичная пачка завершает дренаж)", len(pool.execs))
	}
	call := pool.execs[0]
	if call.sql != subdomainRecheckAllSQL {
		t.Errorf("SQL = %s, хочу перевод без зоны", call.sql)
	}
	if len(call.args) != 4 {
		t.Fatalf("аргументы = %v, хочу 4", call.args)
	}
	for i, want := range []any{int16(model.CheckStatePending), int16(model.CheckStateDone), int16(model.StatusDeleted), subdomainRecheckBatch} {
		if call.args[i] != want {
			t.Errorf("аргумент[%d] = %v (%T), хочу %v (%T)", i, call.args[i], call.args[i], want, want)
		}
	}
}

func TestSubdomainsRecheckRunZone(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)
	pool.seedDomain(9, 2, model.CheckStateDone, model.StatusAlive) // другая зона — не трогается

	zone := int16(7)
	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", &zone, "crawl.test", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "переведено в проверку (pending): 1") {
		t.Errorf("вывод не содержит итог зоны:\n%s", stdout.String())
	}
	if pool.domains[0].state != model.CheckStatePending || pool.domains[1].state != model.CheckStateDone {
		t.Errorf("зона не отфильтрована: %+v", pool.domains)
	}
	call := pool.execs[0]
	if call.sql != subdomainRecheckZoneSQL || call.args[3] != zone || call.args[4] != subdomainRecheckBatch {
		t.Errorf("exec = %v args=%v, хочу зонный SQL с zone=%d", call.sql, call.args, zone)
	}
}

func TestSubdomainsRecheckRunBatches(t *testing.T) {
	// Дренаж пачками: 2·5000 + 1 доменов — три прохода, частичная пачка
	// завершает цикл (стиль RecheckOnce, T-030).
	pool := newSubdomainsPool()
	const total = 2*subdomainRecheckBatch + 1
	for i := int64(1); i <= total; i++ {
		pool.seedDomain(7, i, model.CheckStateDone, model.StatusAlive)
	}

	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "все", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(stdout.String(), fmt.Sprintf("переведено в проверку (pending): %d", total)) {
		t.Errorf("вывод не содержит счётчик %d:\n%s", total, stdout.String())
	}
	if len(pool.execs) != 3 {
		t.Errorf("проходов = %d, хочу 3 (2 полных пачки + 1 частичная)", len(pool.execs))
	}
	for _, d := range pool.domains {
		if d.state != model.CheckStatePending {
			t.Fatalf("домен %d не переведён: %+v", d.id, d)
		}
	}
}

func TestSubdomainsRecheckRunEmpty(t *testing.T) {
	pool := newSubdomainsPool() // доменов нет

	var stdout bytes.Buffer
	if err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "все", &stdout); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(stdout.String(), "готовых к перепроверке доменов нет") {
		t.Errorf("вывод не сообщает «нет доменов»:\n%s", stdout.String())
	}
}

func TestSubdomainsRecheckRunUnknownSubdomain(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)

	err := subdomainsRecheckRun(context.Background(), pool, "ghost", nil, "все", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "нет в справочнике — сначала subdomains add ghost") {
		t.Fatalf("ошибка = %v, хочу подсказку add", err)
	}
	if len(pool.execs) != 0 {
		t.Errorf("перевод не должен выполняться: %v", pool.execs)
	}

	// Выключенный субдомен проверять бессмысленно — DNS-воркер его
	// пропускает (ТЗ 5.4 п. 2).
	pool.subs[1].enabled = false
	err = subdomainsRecheckRun(context.Background(), pool, "www", nil, "все", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "выключен") {
		t.Fatalf("ошибка = %v, хочу отказ выключенного", err)
	}
}

func TestSubdomainsRecheckRunErrors(t *testing.T) {
	pool := newSubdomainsPool()
	pool.seedDomain(7, 1, model.CheckStateDone, model.StatusAlive)

	pool.execErr = errors.New("нет соединения")
	err := subdomainsRecheckRun(context.Background(), pool, "www", nil, "все", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "перевод done → pending") {
		t.Fatalf("ошибка = %v, хочу контекст перевода", err)
	}

	// Прерывание после первой полной пачки: частичный итог в ошибке.
	pool2 := newSubdomainsPool()
	const total = 2 * subdomainRecheckBatch
	for i := int64(1); i <= total; i++ {
		pool2.seedDomain(7, i, model.CheckStateDone, model.StatusAlive)
	}
	pool2.execErrAfter, pool2.execErr = 1, errors.New("ctx отменён")
	err = subdomainsRecheckRun(context.Background(), pool2, "www", nil, "все", &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("переведено в pending %d доменов, прервано", subdomainRecheckBatch)) {
		t.Fatalf("ошибка = %v, хочу частичный итог первой пачки", err)
	}
}

// --- команды через run --------------------------------------------------------

// TestRunSubdomainsExecution — путь выполнения команд subdomains без
// инфраструктуры: детерминированный отказ конфигурации (код 1) с контекстом
// команды — валидация входа идёт раньше (ошибки использования — код 2).
func TestRunSubdomainsExecution(t *testing.T) {
	t.Setenv("CRAWLER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))

	cases := []runCase{
		{name: "list — нет конфигурации", args: []string{"subdomains", "list"}, code: exitError, inErr: "subdomains list: config:"},
		{name: "add — нет конфигурации", args: []string{"subdomains", "add", "www"}, code: exitError, inErr: "subdomains add www: config:"},
		{name: "disable — нет конфигурации", args: []string{"subdomains", "disable", "www"}, code: exitError, inErr: "subdomains disable www: config:"},
		{name: "recheck — нет конфигурации", args: []string{"subdomains", "recheck", "www"}, code: exitError, inErr: "subdomains recheck www: config:"},
		{name: "recheck — метка нормализуется до конфигурации", args: []string{"subdomains", "recheck", "МосквА"}, code: exitError, inErr: "subdomains recheck xn--80adxhks: config:"},
		{name: "recheck нет конфигурации с зоной", args: []string{"subdomains", "recheck", "--zone", "crawl.test", "www"}, code: exitError, inErr: "subdomains recheck www: config:"},
		{name: "recheck без метки", args: []string{"subdomains", "recheck"}, code: exitUsage, inErr: "укажите метку субдомена"},
		{name: "recheck два позиционных", args: []string{"subdomains", "recheck", "a", "b"}, code: exitUsage, inErr: "укажите метку субдомена"},
		{name: "recheck пустая метка", args: []string{"subdomains", "recheck", ""}, code: exitUsage, inErr: "апекс"},
		{name: "recheck невалидная метка (после --)", args: []string{"subdomains", "recheck", "--", "-bad-"}, code: exitUsage, inErr: "некорректная метка субдомена"},
		{name: "recheck невалидная зона", args: []string{"subdomains", "recheck", "--zone", "bad!!zone", "www"}, code: exitUsage, inErr: "некорректное имя зоны"},
		{name: "recheck --zone без значения", args: []string{"subdomains", "recheck", "--zone"}, code: exitUsage, inErr: "subdomains recheck:"},
		{name: "recheck неизвестный флаг", args: []string{"subdomains", "recheck", "--nope", "www"}, code: exitUsage, inErr: "subdomains recheck:"},
		{name: "list лишний аргумент", args: []string{"subdomains", "list", "foo"}, code: exitUsage, inErr: "лишние аргументы"},
		{name: "recheck --help", args: []string{"subdomains", "recheck", "--help"}, code: exitOK, inOut: "проверка субдомена по уже проверенным доменам"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("run(%q) код = %d, хочу %d; stderr:\n%s", tc.args, code, tc.code, stderr.String())
			}
			if tc.inOut != "" && !strings.Contains(stdout.String(), tc.inOut) {
				t.Errorf("run(%q) stdout не содержит %q:\n%s", tc.args, tc.inOut, stdout.String())
			}
			if tc.inErr != "" && !strings.Contains(stderr.String(), tc.inErr) {
				t.Errorf("run(%q) stderr не содержит %q:\n%s", tc.args, tc.inErr, stderr.String())
			}
		})
	}
}
