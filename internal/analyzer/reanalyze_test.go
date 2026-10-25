// Юнит-тесты переанализа (T-074, ТЗ 5.8 «Переанализ», §11.4 п. 4) на
// фейковых пуле/очереди/источнике правил — без БД, Redis и файловой
// системы. Проверяются точный SQL-текст и параметры выборок обеих форм
// (PK-форма дренажа: IS DISTINCT FROM, охрана deleted §13.1, фильтр
// зоны, курсор пачки, LIMIT; время-форма тика T-193: композитный курсор
// (checked_at, PK), ORDER BY по тому же ключу, якорь now−lookback),
// живая семантика фильтров фейка, пачечность дренажа без повторов,
// курсор тика между проходами (продвижение пачкой, пустая пачка не
// двигает), пустое множество, идемпотентность повторного прохода,
// ошибки и отмена ctx, цикл (стартовый дренаж PK-формой, Warn с
// продолжением, тиковый проход время-формой по свежему срезу,
// внеочередной дренаж по смене правил, чистый выход). Поведение против живых PostgreSQL/Redis —
// интеграционные тесты этапа 7 (test/integration/reanalyze_test.go).
package analyzer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// --- фейковая очередь --------------------------------------------------------

// reanPublish — записанная публикация.
type reanPublish struct {
	stream string
	msg    queue.Result
}

// reanQueue — queue.Queue: фиксирует публикации Result в q:analyze
// (сбои по индексу), Consume паркуется до отмены ctx, Ack — заглушка.
type reanQueue struct {
	errs []error // ошибка публикации по индексу (nil — успешно)

	mu        sync.Mutex
	publishes []reanPublish
}

var _ queue.Queue = (*reanQueue)(nil)

func (q *reanQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok || stream != queue.StreamAnalyze {
		return "", fmt.Errorf("reanQueue: публикация %T в %s (нужен queue.Result в %s)",
			msg, stream, queue.StreamAnalyze)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, reanPublish{stream: stream, msg: res})
	q.mu.Unlock()
	if i < len(q.errs) && q.errs[i] != nil {
		return "", q.errs[i]
	}
	return fmt.Sprintf("entry-%d", i+1), nil
}

func (q *reanQueue) Consume(ctx context.Context, _, _, _ string, _ int) ([]queue.Delivery, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *reanQueue) Ack(context.Context, string, string, ...string) error { return nil }

// snapshot / count — снимки под замком (цикл работает в горутине,
// тест ожидает прогресс по счётчику).
func (q *reanQueue) snapshot() []reanPublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]reanPublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *reanQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.publishes)
}

// --- фейковый пул (db.DB) ----------------------------------------------------

// reanCall — записанный вызов Query с аргументами.
type reanCall struct {
	sql  string
	args []any
}

// reanDomKey — ключ строки domains в фейке.
type reanDomKey struct {
	zone int16
	id   int64
}

// reanDomain — строка domains, которую видит выборка (JOIN).
type reanDomain struct {
	label  string
	status model.Status
}

// reanRow — строка subdomain_results: ключ, rules_version (nil — NULL,
// никогда не анализировалась) и checked_at (нулевая — только для строк
// PK-формы; время-форма сеет через seedRowAt).
type reanRow struct {
	key       resultKey
	version   *int32
	checkedAt time.Time
}

// reanPool — db.DB без БД: живая семантика выборки переанализа (JOIN
// domains/zones, IS DISTINCT FROM, status <> deleted, фильтр зоны,
// курсор row-value >, ORDER BY по ключу, LIMIT) и поиска зоны по имени;
// SQL сопоставляется ТОЧНО (неожидаемый — ошибка, тест падает громко).
// Поля *ScanErr/*RowsErr — инъекции сбоев Scan/Err строк (T-M10).
type reanPool struct {
	zones      map[int16]string
	zoneByName map[string]int16
	domains    map[reanDomKey]*reanDomain
	rows       []reanRow

	selectScanErr error
	selectRowsErr error
	zoneScanErr   error
	zoneRowsErr   error

	mu       sync.Mutex
	queryErr error // настраиваемый сбой любой выборки
	queries  []reanCall
}

var _ db.DB = (*reanPool)(nil)

func newReanPool() *reanPool {
	return &reanPool{
		zones:      make(map[int16]string),
		zoneByName: make(map[string]int16),
		domains:    make(map[reanDomKey]*reanDomain),
	}
}

func (p *reanPool) seedZone(id int16, name string) {
	p.zones[id] = name
	p.zoneByName[name] = id
}

func (p *reanPool) seedDomain(zone int16, id int64, label string, status model.Status) {
	p.domains[reanDomKey{zone: zone, id: id}] = &reanDomain{label: label, status: status}
}

func (p *reanPool) seedRow(zone int16, domain int64, sub int16, variant model.Variant, version *int32) {
	p.rows = append(p.rows, reanRow{key: resultKey{zone: zone, domain: domain, sub: sub, variant: variant}, version: version})
}

// seedRowAt — строка с отметкой checked_at (время-форма, T-193): курсор
// и порядок тика идут по (checked_at, PK). Под замком: тесты цикла
// подсевают свежие эпизоды в работающую горутину (loop-тесты T-193).
func (p *reanPool) seedRowAt(zone int16, domain int64, sub int16, variant model.Variant, version *int32, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows = append(p.rows, reanRow{
		key:       resultKey{zone: zone, domain: domain, sub: sub, variant: variant},
		version:   version,
		checkedAt: at,
	})
}

// markAnalyzed имитирует воркер T-071: все строки получают текущую
// версию набора (фиксация rules_version по обработанному сообщению).
func (p *reanPool) markAnalyzed(version int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.rows {
		v := version
		p.rows[i].version = &v
	}
}

func (p *reanPool) setQueryErr(err error) {
	p.mu.Lock()
	p.queryErr = err
	p.mu.Unlock()
}

func (p *reanPool) calls() []reanCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]reanCall(nil), p.queries...)
}

func (p *reanPool) queryCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

func (p *reanPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("reanPool: CopyFrom не должен вызываться")
}

func (p *reanPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("reanPool: Exec не должен вызываться")
}

func (p *reanPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, reanCall{sql: sql, args: args})
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	switch sql {
	case reanalyzeSelectSQL:
		return p.selectRows(args)
	case reanalyzeTickSQL:
		return p.tickSelectRows(args)
	case zoneIDByNameSQL:
		if len(args) != 1 {
			return nil, fmt.Errorf("reanPool: зона по имени: параметров %d, нужно 1", len(args))
		}
		name, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("reanPool: зона по имени: параметр %T, нужен string", args[0])
		}
		id, found := p.zoneByName[name]
		if !found {
			return &reanRows{err: p.zoneRowsErr}, nil // пустой ответ (с инъекцией Err)
		}
		return &reanRows{rows: [][]any{{id}}, scanErr: p.zoneScanErr, err: p.zoneRowsErr}, nil
	default:
		return nil, fmt.Errorf("reanPool: неожидаемый SQL Query: %s", sql)
	}
}

// selectRows применяет семантику reanalyzeSelectSQL к сидам.
func (p *reanPool) selectRows(args []any) (pgx.Rows, error) {
	if len(args) != 8 {
		return nil, fmt.Errorf("reanPool: выборка: параметров %d, нужно 8", len(args))
	}
	version, okV := args[0].(int32)
	deleted, okD := args[1].(int16)
	if !okV || !okD {
		return nil, fmt.Errorf("reanPool: выборка: версия %T (%v), статус %T (%v)", args[0], args[0], args[1], args[1])
	}
	if deleted != int16(model.StatusDeleted) {
		return nil, fmt.Errorf("reanPool: выборка: статус-охрана %d, нужен код deleted %d (§13.1)",
			deleted, model.StatusDeleted)
	}
	var zone *int16
	if z, ok := args[2].(int16); ok {
		zone = &z
	} else if args[2] != nil {
		return nil, fmt.Errorf("reanPool: выборка: зона %T (%v), нужен int16 или nil", args[2], args[2])
	}
	cz, ok1 := args[3].(int16)
	cd, ok2 := args[4].(int64)
	cs, ok3 := args[5].(int16)
	cv, ok4 := args[6].(int16)
	limit, ok5 := args[7].(int)
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return nil, fmt.Errorf("reanPool: выборка: курсор/LIMIT %v", args[3:])
	}
	if limit <= 0 {
		return nil, fmt.Errorf("reanPool: выборка: LIMIT %d ≤ 0", limit)
	}
	cursor := resultKey{zone: cz, domain: cd, sub: cs, variant: model.Variant(cv)}

	var sel []reanRow
	for _, r := range p.rows {
		d, ok := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		if !ok {
			continue // JOIN domains: строки без домена не выбираются
		}
		if d.status == model.StatusDeleted {
			continue // §13.1
		}
		if r.version != nil && *r.version == version {
			continue // rules_version = текущей — актуальна
		}
		if zone != nil && r.key.zone != *zone {
			continue // фильтр зоны
		}
		if !reanKeyGreater(r.key, cursor) {
			continue // курсор пагинации
		}
		sel = append(sel, r)
	}
	// ORDER BY zone_id, domain_id, subdomain_id, variant (пузырёк:
	// пачки маленькие) + LIMIT.
	for i := 0; i < len(sel); i++ {
		for j := i + 1; j < len(sel); j++ {
			if reanKeyGreater(sel[i].key, sel[j].key) {
				sel[i], sel[j] = sel[j], sel[i]
			}
		}
	}
	if len(sel) > limit {
		sel = sel[:limit]
	}
	rows := make([][]any, len(sel))
	for i, r := range sel {
		d := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, int16(r.key.variant), d.label + "." + p.zones[r.key.zone]}
	}
	out := reanRowsOf(rows...)
	out.scanErr, out.err = p.selectScanErr, p.selectRowsErr
	return out, nil
}

// reanKeyGreater — row-value сравнение ключей «>» (лексикографическое,
// как (a,b,c,d) > (x,y,z,w) в PostgreSQL).
func reanKeyGreater(a, b resultKey) bool {
	switch {
	case a.zone != b.zone:
		return a.zone > b.zone
	case a.domain != b.domain:
		return a.domain > b.domain
	case a.sub != b.sub:
		return a.sub > b.sub
	default:
		return a.variant > b.variant
	}
}

// reanCursorGreater — row-value сравнение курсора время-формы «>» по
// (checked_at, PK), как (t, a,b,c,d) > (u, w,x,y,z) в PostgreSQL:
// время ведёт, PK детерминирует внутри одинаковых отметок.
func reanCursorGreater(r reanRow, after ReanalyzeCursor) bool {
	if !r.checkedAt.Equal(after.checkedAt) {
		return r.checkedAt.After(after.checkedAt)
	}
	return reanKeyGreater(r.key, after.key)
}

// tickSelectRows применяет семантику reanalyzeTickSQL к сидам (T-193):
// тот же отбор, что PK-форма, но курсор и порядок — по (checked_at, PK).
func (p *reanPool) tickSelectRows(args []any) (pgx.Rows, error) {
	if len(args) != 9 {
		return nil, fmt.Errorf("reanPool: выборка тика: параметров %d, нужно 9", len(args))
	}
	version, okV := args[0].(int32)
	deleted, okD := args[1].(int16)
	if !okV || !okD {
		return nil, fmt.Errorf("reanPool: выборка тика: версия %T (%v), статус %T (%v)", args[0], args[0], args[1], args[1])
	}
	if deleted != int16(model.StatusDeleted) {
		return nil, fmt.Errorf("reanPool: выборка тика: статус-охрана %d, нужен код deleted %d (§13.1)",
			deleted, model.StatusDeleted)
	}
	var zone *int16
	if z, ok := args[2].(int16); ok {
		zone = &z
	} else if args[2] != nil {
		return nil, fmt.Errorf("reanPool: выборка тика: зона %T (%v), нужен int16 или nil", args[2], args[2])
	}
	afterAt, okT := args[3].(time.Time)
	cz, ok1 := args[4].(int16)
	cd, ok2 := args[5].(int64)
	cs, ok3 := args[6].(int16)
	cv, ok4 := args[7].(int16)
	limit, ok5 := args[8].(int)
	if !okT || !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return nil, fmt.Errorf("reanPool: выборка тика: курсор/LIMIT %v", args[3:])
	}
	if limit <= 0 {
		return nil, fmt.Errorf("reanPool: выборка тика: LIMIT %d ≤ 0", limit)
	}
	after := ReanalyzeCursor{
		checkedAt: afterAt,
		key:       resultKey{zone: cz, domain: cd, sub: cs, variant: model.Variant(cv)},
	}

	var sel []reanRow
	for _, r := range p.rows {
		d, ok := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		if !ok {
			continue // JOIN domains: строки без домена не выбираются
		}
		if d.status == model.StatusDeleted {
			continue // §13.1
		}
		if r.version != nil && *r.version == version {
			continue // rules_version = текущей — актуальна
		}
		if zone != nil && r.key.zone != *zone {
			continue // фильтр зоны
		}
		if !reanCursorGreater(r, after) {
			continue // композитный курсор (checked_at, PK)
		}
		sel = append(sel, r)
	}
	// ORDER BY checked_at, zone_id, domain_id, subdomain_id, variant
	// (пузырёк: пачки маленькие) + LIMIT.
	for i := 0; i < len(sel); i++ {
		for j := i + 1; j < len(sel); j++ {
			gt := false
			if !sel[i].checkedAt.Equal(sel[j].checkedAt) {
				gt = sel[i].checkedAt.After(sel[j].checkedAt)
			} else {
				gt = reanKeyGreater(sel[i].key, sel[j].key)
			}
			if gt {
				sel[i], sel[j] = sel[j], sel[i]
			}
		}
	}
	if len(sel) > limit {
		sel = sel[:limit]
	}
	rows := make([][]any, len(sel))
	for i, r := range sel {
		d := p.domains[reanDomKey{zone: r.key.zone, id: r.key.domain}]
		rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, int16(r.key.variant),
			d.label + "." + p.zones[r.key.zone], r.checkedAt}
	}
	out := reanRowsOf(rows...)
	out.scanErr, out.err = p.selectScanErr, p.selectRowsErr
	return out, nil
}

// reanRows — pgx.Rows с заготовленными строками (Scan — позиционный,
// типы int16/int64/string); scanErr/err — инъекции сбоев Scan/Err (T-M10).
type reanRows struct {
	rows    [][]any
	i       int
	scanErr error
	err     error
}

func reanRowsOf(rows ...[]any) *reanRows { return &reanRows{rows: rows} }

func (r *reanRows) Close()                                       {}
func (r *reanRows) Err() error                                   { return r.err }
func (r *reanRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *reanRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *reanRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *reanRows) RawValues() [][]byte                          { return nil }
func (r *reanRows) Conn() *pgx.Conn                              { return nil }
func (r *reanRows) TypeMap() *pgtype.Map                         { return nil }

func (r *reanRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *reanRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("reanRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("reanRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *int64:
			*dst = v.(int64)
		case *string:
			*dst = v.(string)
		case *time.Time:
			*dst = v.(time.Time)
		default:
			return fmt.Errorf("reanRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// --- фейковый источник правил ---------------------------------------------

// reanSource — RulesSource: переключаемый текущий набор и канал
// уведомлений, который наполняет тест. Поле set под замком: цикл читает
// его из своей горутины, тест переключает из своей (как Reloader, T-068).
type reanSource struct {
	mu   sync.Mutex
	set  *rules.Set
	next chan *rules.Set
}

func newReanSource(version int32) *reanSource {
	return &reanSource{set: &rules.Set{Version: version}, next: make(chan *rules.Set, 1)}
}

func (s *reanSource) Set() *rules.Set {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set
}

func (s *reanSource) Subscribe() (<-chan *rules.Set, func()) {
	return s.next, func() {}
}

// switchTo меняет набор и уведомляет подписчиков (как Reloader.Reload).
func (s *reanSource) switchTo(version int32) {
	s.mu.Lock()
	s.set = &rules.Set{Version: version}
	set := s.set
	s.mu.Unlock()
	select {
	case s.next <- set:
	default:
	}
}

// reanBuf — потокобезопасный bytes.Buffer: цикл пишет через slog, тест
// читает содержимое до остановки цикла (ожидание прогресса по логу).
type reanBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *reanBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *reanBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// --- ожидаемый SQL-текст (контракт) ---------------------------------------

const wantReanalyzeSelect = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::int2, $5::int8, $6::int2, $7::int2) " +
	"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $8"

// wantReanalyzeTickSQL — контракт время-формы (T-193 §4.1): тот же
// отбор, композитный курсор (checked_at, PK) + ORDER BY по тому же
// ключу + LIMIT; checked_at в списке колонок — курсор следующей пачки.
const wantReanalyzeTickSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name, r.checked_at " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::timestamptz, $5::int2, $6::int8, $7::int2, $8::int2) " +
	"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $9"

const wantZoneIDByName = "SELECT id FROM zones WHERE name = $1"

// reanV — вспомогательный указатель на версию (сид строк).
func reanV(v int32) *int32 { return &v }

// --- ReanalyzeOnce ---------------------------------------------------------

// TestReanalyzeOnceSQL — точный SQL-текст и параметры: версия набора,
// охрана deleted (§13.1), зона NULL (все), нулевой курсор, LIMIT пачки;
// по каждой строке — Result с ключом и именем в q:analyze.
func TestReanalyzeOnceSQL(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(3, "crawl.test")
	pool.seedDomain(3, 7, "example", model.StatusAlive)
	pool.seedRow(3, 7, 0, model.VariantHTTP, nil) // NULL — никогда не анализировалась
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, int32(42), nil, ReanalyzeOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("выборок = %d, want 1", len(calls))
	}
	if calls[0].sql != wantReanalyzeSelect {
		t.Errorf("SQL выборки:\n%s\nwant:\n%s", calls[0].sql, wantReanalyzeSelect)
	}
	if reanalyzeSelectSQL != wantReanalyzeSelect {
		t.Errorf("константа разошлась с контрактом теста")
	}
	wantArgs := []any{
		int32(42),                              // версия набора
		int16(model.StatusDeleted),             // §13.1
		nil,                                    // зона — все
		int16(0), int64(0), int16(0), int16(0), // нулевой курсор — с начала
		100, // LIMIT пачки
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("аргументы = %#v, want %#v", calls[0].args, wantArgs)
	}

	if res.Selected != 1 || res.Published != 1 {
		t.Errorf("итог = %+v, want 1/1", res)
	}
	pubs := q.snapshot()
	if len(pubs) != 1 {
		t.Fatalf("публикаций = %d, want 1", len(pubs))
	}
	want := queue.Result{
		ZoneID: 3, DomainID: 7, SubdomainID: 0,
		Variant: model.VariantHTTP, Name: "example.crawl.test",
	}
	if pubs[0].stream != queue.StreamAnalyze || pubs[0].msg != want {
		t.Errorf("публикация = %s %+v, want %s %+v", pubs[0].stream, pubs[0].msg, queue.StreamAnalyze, want)
	}
}

// TestReanalyzeOnceZoneParam — фильтр зоны: параметр int16, строки чужих
// зон не выбираются.
func TestReanalyzeOnceZoneParam(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "a.test")
	pool.seedZone(2, "b.test")
	pool.seedDomain(1, 1, "one", model.StatusAlive)
	pool.seedDomain(2, 2, "two", model.StatusAlive)
	pool.seedRow(1, 1, 0, model.VariantMain, nil)
	pool.seedRow(2, 2, 0, model.VariantMain, nil)
	q := &reanQueue{}

	zone := int16(2)
	res, err := ReanalyzeOnce(context.Background(), pool, q, 1, &zone, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if args := pool.calls()[0].args; args[2] != int16(2) {
		t.Errorf("параметр зоны = %v (%T), want int16(2)", args[2], args[2])
	}
	if res.Selected != 1 || res.Published != 1 {
		t.Fatalf("итог = %+v, want только строка зоны 2", res)
	}
	if msg := q.snapshot()[0].msg; msg.ZoneID != 2 || msg.Name != "two.b.test" {
		t.Errorf("публикация = %+v, want строка зоны 2", msg)
	}
}

// TestReanalyzeOnceFilters — семантика отбора: NULL и устаревшая версия
// выбираются, совпадающая — нет; deleted-домен и строка без домена
// (JOIN) не выбираются.
func TestReanalyzeOnceFilters(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "never", model.StatusAlive)     // NULL — до T-071
	pool.seedDomain(1, 2, "stale", model.StatusAlive)     // старая версия
	pool.seedDomain(1, 3, "fresh", model.StatusAlive)     // текущая версия
	pool.seedDomain(1, 4, "deleted", model.StatusDeleted) // §13.1
	pool.seedDomain(1, 5, "gone", model.StatusAlive)      // домен строки 5 ниже
	pool.seedRow(1, 1, 0, model.VariantMain, nil)
	pool.seedRow(1, 2, 0, model.VariantMain, reanV(7))
	pool.seedRow(1, 3, 0, model.VariantMain, reanV(42))
	pool.seedRow(1, 4, 0, model.VariantMain, nil)
	pool.seedRow(1, 9, 0, model.VariantMain, nil) // без строки domains
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 42, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if res.Selected != 2 || res.Published != 2 {
		t.Fatalf("итог = %+v, want 2/2 (NULL + устаревшая)", res)
	}
	for i, want := range []string{"never.test", "stale.test"} {
		if got := q.snapshot()[i].msg.Name; got != want {
			t.Errorf("публикация[%d] = %s, want %s", i, got, want)
		}
	}
}

// TestReanalyzeOnceBatchLimit — пачка обрезается LIMIT по порядку ключа.
func TestReanalyzeOnceBatchLimit(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 5; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if res.Selected != 3 || res.Published != 3 {
		t.Fatalf("итог = %+v, want пачка 3", res)
	}
	if got := q.snapshot()[2].msg.Name; got != "d3.test" {
		t.Errorf("третья публикация = %s, want d3.test (порядок ключа)", got)
	}
}

// TestReanalyzeOnceEmpty — пустое множество: одна выборка, нули, ни одной
// публикации.
func TestReanalyzeOnceEmpty(t *testing.T) {
	pool := newReanPool()
	q := &reanQueue{}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 5, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if res != (ReanalyzeResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	if n := q.count(); n != 0 {
		t.Errorf("публикаций = %d, want 0", n)
	}
}

// TestReanalyzeOnceDefaults — нулевые опции: пачка DefaultReanalyzeBatch
// (литерал 5000 — пин против мутантов ±1 константы, T-M10: сравнение
// с символом константы менялось бы вместе с мутантом).
func TestReanalyzeOnceDefaults(t *testing.T) {
	pool := newReanPool()
	if _, err := ReanalyzeOnce(context.Background(), pool, &reanQueue{}, 1, nil, ReanalyzeOptions{}); err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	if got := pool.calls()[0].args[7]; got != 5000 {
		t.Errorf("LIMIT = %v, want дефолт 5000", got)
	}
}

// --- ReanalyzeDrain --------------------------------------------------------

// TestReanalyzeDrainBatches — дренаж пачками до исчерпания: курсор
// передаётся в следующую выборку, публикуются все строки по одному разу
// (без повторов внутри прохода), итог — суммы.
func TestReanalyzeDrainBatches(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 7; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{}

	res, err := ReanalyzeDrain(context.Background(), pool, q, 1, nil, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("ReanalyzeDrain: %v", err)
	}
	if res.Selected != 7 || res.Published != 7 {
		t.Fatalf("итог = %+v, want 7/7", res)
	}

	calls := pool.calls()
	if len(calls) != 3 { // 3 + 3 + 1 (неполная — стоп)
		t.Fatalf("выборок = %d, want 3", len(calls))
	}
	// Курсор второй пачки — последняя строка первой, третьей — последняя
	// второй: row-value параметры растут.
	if got := calls[1].args[4]; got != int64(3) {
		t.Errorf("курсор 2-й пачки domain = %v, want 3", got)
	}
	if got := calls[2].args[4]; got != int64(6) {
		t.Errorf("курсор 3-й пачки domain = %v, want 6", got)
	}

	pubs := q.snapshot()
	if len(pubs) != 7 {
		t.Fatalf("публикаций = %d, want 7 (без повторов)", len(pubs))
	}
	for i, p := range pubs {
		if want := int64(i + 1); p.msg.DomainID != want {
			t.Errorf("публикация[%d].DomainID = %d, want %d (порядок ключа)", i, p.msg.DomainID, want)
		}
	}
}

// TestReanalyzeDrainExactBatch — кратное пачке множество: одна
// контрольная выборка после последней полной пачки (неполная=пустая —
// стоп), публикаций ровно по числу строк.
func TestReanalyzeDrainExactBatch(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 6; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{}

	res, err := ReanalyzeDrain(context.Background(), pool, q, 1, nil, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("ReanalyzeDrain: %v", err)
	}
	if res.Selected != 6 || res.Published != 6 {
		t.Fatalf("итог = %+v, want 6/6", res)
	}
	if n := len(pool.calls()); n != 3 {
		t.Errorf("выборок = %d, want 3 (2 полные + контрольная)", n)
	}
}

// TestReanalyzeIdempotent — после фиксации воркером T-071 текущей версии
// повторный дренаж ничего не находит: выборка+публикация идемпотентны.
func TestReanalyzeIdempotent(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedDomain(1, 2, "b", model.StatusAlive)
	pool.seedRow(1, 1, 0, model.VariantMain, nil)
	pool.seedRow(1, 2, 0, model.VariantMain, reanV(7))
	q := &reanQueue{}

	first, err := ReanalyzeDrain(context.Background(), pool, q, 42, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("первый дренаж: %v", err)
	}
	if first.Published != 2 {
		t.Fatalf("первый дренаж = %+v, want 2", first)
	}

	pool.markAnalyzed(42) // воркер зафиксировал версию по обеим строкам
	q2 := &reanQueue{}
	second, err := ReanalyzeDrain(context.Background(), pool, q2, 42, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("второй дренаж: %v", err)
	}
	if second != (ReanalyzeResult{}) {
		t.Errorf("повторный дренаж = %+v, want нулевой (идемпотентность)", second)
	}
	if n := q2.count(); n != 0 {
		t.Errorf("публикаций повторно = %d, want 0", n)
	}
}

// --- ReanalyzeTick (время-форма, T-193) -----------------------------------

// TestReanalyzeTickSQL — SQL-контракт время-формы и якорь старта: точный
// текст (композитный курсор (checked_at, PK), ORDER BY по тому же ключу,
// LIMIT, checked_at в колонках), первая выборка нулевого курсора идёт с
// якоря now() − DefaultReanalyzeLookback с нулевой PK-частью (§4.2);
// публикация — Result с ключом и именем в q:analyze.
func TestReanalyzeTickSQL(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(3, "crawl.test")
	pool.seedDomain(3, 7, "example", model.StatusAlive)
	before := time.Now().Add(-DefaultReanalyzeLookback)
	pool.seedRowAt(3, 7, 0, model.VariantHTTP, nil, time.Now().Add(-time.Minute)) // NULL + свежая
	q := &reanQueue{}

	res, err := ReanalyzeTick(context.Background(), pool, q, int32(42), &ReanalyzeCursor{}, ReanalyzeOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("ReanalyzeTick: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 1 { // одна пачка (1 < 100) — короткая, стоп
		t.Fatalf("выборок = %d, want 1", len(calls))
	}
	if calls[0].sql != wantReanalyzeTickSQL {
		t.Errorf("SQL выборки тика:\n%s\nwant:\n%s", calls[0].sql, wantReanalyzeTickSQL)
	}
	if reanalyzeTickSQL != wantReanalyzeTickSQL {
		t.Errorf("константа разошлась с контрактом теста")
	}
	args := calls[0].args
	if args[0] != int32(42) || args[1] != int16(model.StatusDeleted) || args[2] != nil {
		t.Errorf("аргументы отбора = %#v, want версия 42, deleted, зона nil", args[:3])
	}
	anchor, ok := args[3].(time.Time)
	if !ok {
		t.Fatalf("курсор времени = %T (%v), нужен time.Time", args[3], args[3])
	}
	// Якорь = now − lookback (допуск минуты на медленный тест).
	if d := anchor.Sub(before); d < -time.Minute || d > time.Minute {
		t.Errorf("якорь = %v, want now − %v (±1 мин, расхождение %v)",
			anchor, DefaultReanalyzeLookback, d)
	}
	if args[4] != int16(0) || args[5] != int64(0) || args[6] != int16(0) || args[7] != int16(0) {
		t.Errorf("PK-часть якоря = %v, want нули", args[4:8])
	}
	if args[8] != 100 {
		t.Errorf("LIMIT = %v, want 100", args[8])
	}

	if res.Selected != 1 || res.Published != 1 {
		t.Fatalf("итог = %+v, want 1/1", res)
	}
	pubs := q.snapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации = %+v, want одна в q:analyze", pubs)
	}
	if want := (queue.Result{
		ZoneID: 3, DomainID: 7, SubdomainID: 0,
		Variant: model.VariantHTTP, Name: "example.crawl.test",
	}); pubs[0].msg != want {
		t.Errorf("публикация = %+v, want %+v", pubs[0].msg, want)
	}
}

// TestReanalyzeTickAnchorWindow — якорь ограничивает окно: старше
// lookback не отбирается (территория полного дренажа), свежие NULL и
// устаревшие внутри окна — отбираются; проанализированная текущей
// версией — нет.
func TestReanalyzeTickAnchorWindow(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "fresh", model.StatusAlive)
	pool.seedDomain(1, 2, "stale", model.StatusAlive)
	pool.seedDomain(1, 3, "done", model.StatusAlive)
	pool.seedDomain(1, 4, "ancient", model.StatusAlive)
	now := time.Now()
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, now.Add(-time.Hour))                               // новый эпизод без анализа
	pool.seedRowAt(1, 2, 0, model.VariantMain, reanV(7), now.Add(-2*time.Hour))                        // устаревшая внутри окна
	pool.seedRowAt(1, 3, 0, model.VariantMain, reanV(42), now.Add(-time.Minute))                       // актуальная
	pool.seedRowAt(1, 4, 0, model.VariantMain, reanV(7), now.Add(-DefaultReanalyzeLookback-time.Hour)) // вне окна
	q := &reanQueue{}

	res, err := ReanalyzeTick(context.Background(), pool, q, 42, &ReanalyzeCursor{}, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick: %v", err)
	}
	if res.Selected != 2 || res.Published != 2 {
		t.Fatalf("итог = %+v, want 2/2 (NULL + устаревшая в окне)", res)
	}
	// Порядок пачки — по (checked_at, PK): устаревшая (2 ч) впереди
	// свежей (1 ч).
	for i, want := range []string{"stale.test", "fresh.test"} {
		if got := q.snapshot()[i].msg.Name; got != want {
			t.Errorf("публикация[%d] = %s, want %s", i, got, want)
		}
	}
}

// TestReanalyzeTickCursor — курсор живёт между проходами и продвигается
// пачкой: 5 строк пачками по 2 (2+2+1), курсор каждой выборки — последняя
// строка предыдущей (время ведёт, PK детерминирует внутри отметки);
// повторный тик по исчерпанному окну — одна пустая выборка тем же
// курсором (пустая пачка не двигает).
func TestReanalyzeTickCursor(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	base := time.Now().Add(-time.Hour)
	for i := int64(1); i <= 5; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		// Одинаковая отметка у d1/d2 — PK детерминирует порядок внутри.
		at := base.Add(time.Duration(i) * time.Minute)
		if i == 2 {
			at = base.Add(time.Duration(1) * time.Minute)
		}
		pool.seedRowAt(1, i, 0, model.VariantMain, nil, at)
	}
	q := &reanQueue{}
	var cur ReanalyzeCursor

	res, err := ReanalyzeTick(context.Background(), pool, q, 1, &cur, ReanalyzeOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("ReanalyzeTick: %v", err)
	}
	if res.Selected != 5 || res.Published != 5 {
		t.Fatalf("итог = %+v, want 5/5", res)
	}
	calls := pool.calls()
	if len(calls) != 3 { // 2 + 2 + 1 (короткая — стоп)
		t.Fatalf("выборок = %d, want 3", len(calls))
	}
	// Курсор 2-й пачки — последняя строка 1-й (d2, отметка base+1м),
	// 3-й — последняя 2-й (d4).
	if gotAt, _ := calls[1].args[3].(time.Time); !gotAt.Equal(base.Add(time.Minute)) {
		t.Errorf("курсор 2-й пачки checked_at = %v, want %v", gotAt, base.Add(time.Minute))
	}
	if calls[1].args[5] != int64(2) {
		t.Errorf("курсор 2-й пачки domain = %v, want 2", calls[1].args[5])
	}
	if gotAt, _ := calls[2].args[3].(time.Time); !gotAt.Equal(base.Add(4 * time.Minute)) {
		t.Errorf("курсор 3-й пачки checked_at = %v, want %v", gotAt, base.Add(4*time.Minute))
	}
	if calls[2].args[5] != int64(4) {
		t.Errorf("курсор 3-й пачки domain = %v, want 4", calls[2].args[5])
	}
	// Курсор после прохода — последняя строка последней пачки.
	if !cur.checkedAt.Equal(base.Add(5*time.Minute)) || cur.key.domain != 5 {
		t.Errorf("курсор после прохода = %v/%d, want %v/5",
			cur.checkedAt, cur.key.domain, base.Add(5*time.Minute))
	}

	// Повторный тик: окно исчерпано — одна пустая выборка курсором
	// последней строки (пустая пачка не двигает).
	res2, err := ReanalyzeTick(context.Background(), pool, q, 1, &cur, ReanalyzeOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("повторный ReanalyzeTick: %v", err)
	}
	if res2 != (ReanalyzeResult{}) {
		t.Errorf("повторный тик = %+v, want нулевой (окно исчерпано)", res2)
	}
	calls2 := pool.calls()[3:]
	if len(calls2) != 1 {
		t.Fatalf("выборок повторного тика = %d, want 1", len(calls2))
	}
	if gotAt, _ := calls2[0].args[3].(time.Time); !gotAt.Equal(base.Add(5 * time.Minute)) {
		t.Errorf("курсор пустого тика checked_at = %v, want прежний %v",
			gotAt, base.Add(5*time.Minute))
	}
	if calls2[0].args[5] != int64(5) {
		t.Errorf("курсор пустого тика domain = %v, want 5", calls2[0].args[5])
	}
	// Новая строка впереди курсора — следующий тик подберёт.
	pool.seedDomain(1, 6, "d6", model.StatusAlive)
	pool.seedRowAt(1, 6, 0, model.VariantMain, nil, base.Add(time.Hour))
	res3, err := ReanalyzeTick(context.Background(), pool, q, 1, &cur, ReanalyzeOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("тик после свежей строки: %v", err)
	}
	if res3.Selected != 1 || res3.Published != 1 {
		t.Fatalf("итог = %+v, want 1/1 (свежий эпизод впереди курсора)", res3)
	}
	if msg := q.snapshot()[5].msg; msg.DomainID != 6 {
		t.Errorf("публикация свежего эпизода = %+v, want домен 6", msg)
	}
}

// TestReanalyzeTickPublishError — сбой публикации посреди пачки не
// двигает курсор за сбойную пачку: повторный тик выбирает её строки
// снова (at-least-once, дубликат ограничен пачкой).
func TestReanalyzeTickPublishError(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	base := time.Now().Add(-time.Hour)
	for i := int64(1); i <= 3; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRowAt(1, i, 0, model.VariantMain, nil, base.Add(time.Duration(i)*time.Minute))
	}
	var cur ReanalyzeCursor
	res, err := ReanalyzeTick(context.Background(), pool,
		&reanQueue{errs: []error{nil, errors.New("redis ушёл")}}, 1, &cur, ReanalyzeOptions{BatchSize: 3})
	if err == nil || !strings.Contains(err.Error(), "redis ушёл") {
		t.Fatalf("ошибка = %v, want причину публикации", err)
	}
	if res.Selected != 3 || res.Published != 1 {
		t.Errorf("итог = %+v, want 3/1 (до сбоя)", res)
	}
	// Курсор не продвинулся: первая пачка не опубликована целиком.
	anchor := time.Now().Add(-DefaultReanalyzeLookback)
	if d := cur.checkedAt.Sub(anchor); d < -time.Minute || d > time.Minute {
		t.Errorf("курсор после сбоя публикации = %v, want якорь %v (пачка не продвинута)",
			cur.checkedAt, anchor)
	}

	// Повторный тик с тем же курсором выбирает сбойную пачку снова.
	q2 := &reanQueue{}
	res2, err := ReanalyzeTick(context.Background(), pool, q2, 1, &cur, ReanalyzeOptions{BatchSize: 3})
	if err != nil {
		t.Fatalf("повторный тик: %v", err)
	}
	if res2.Selected != 3 || res2.Published != 3 {
		t.Fatalf("повторный итог = %+v, want 3/3 (пачка повторена)", res2)
	}
}

// TestReanalyzeTickCancel — отмена ctx до прохода: чистая ошибка
// контекста без двойной обёртки (пин текста, стиль TestReanalyzeDrainCancel).
func TestReanalyzeTickCancel(t *testing.T) {
	pool := newReanPool()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := ReanalyzeTick(ctx, pool, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}
	if err != nil && err.Error() != "analyzer: переанализ: context canceled" {
		t.Errorf("ошибка = %q, want точная обёртка «analyzer: переанализ: context canceled»", err)
	}
	if res.Published != 0 {
		t.Errorf("итог = %+v, want без публикаций", res)
	}
}

// TestReanalyzeTickNilCursor — nil курсор: проход с якоря без сохранения
// (разовый вызов), panic нет.
func TestReanalyzeTickNilCursor(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}

	res, err := ReanalyzeTick(context.Background(), pool, q, 1, nil, ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeTick(nil): %v", err)
	}
	if res.Selected != 1 || res.Published != 1 {
		t.Fatalf("итог = %+v, want 1/1", res)
	}
}

// --- ошибки и отмена -------------------------------------------------------

// TestReanalyzePublishError — сбой публикации посреди пачки: ошибка
// обёрнута с именем домена и позицией записи (пин «запись N/M» против
// мутантов i±1, T-M10), итог содержит опубликованное до сбоя.
func TestReanalyzePublishError(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 2; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	q := &reanQueue{errs: []error{nil, errors.New("redis ушёл")}}

	res, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "analyzer: переанализ: публикация q:analyze") ||
		!strings.Contains(err.Error(), "запись 2/2") ||
		!strings.Contains(err.Error(), "d2.test") ||
		!strings.Contains(err.Error(), "redis ушёл") {
		t.Errorf("ошибка = %v, want контекст публикации, позицию 2/2, домен и причину", err)
	}
	if res.Selected != 2 || res.Published != 1 {
		t.Errorf("итог = %+v, want 2/1 (до сбоя)", res)
	}

	// Дренаж не глотает ошибку публикации своей пачки (собственный
	// return, отличимый от прохода Once; мутант вырезания, T-M10):
	// ошибка наружу, опубликованное до сбоя — в итоге.
	drainPool := newReanPool()
	drainPool.seedZone(1, "test")
	for i := int64(1); i <= 2; i++ {
		drainPool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		drainPool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	drainRes, err := ReanalyzeDrain(context.Background(), drainPool,
		&reanQueue{errs: []error{nil, errors.New("redis ушёл")}}, 1, nil, ReanalyzeOptions{})
	if err == nil || !strings.Contains(err.Error(), "redis ушёл") {
		t.Errorf("дренаж: ошибка = %v, want причину публикации (не глотается)", err)
	}
	if drainRes.Selected != 2 || drainRes.Published != 1 {
		t.Errorf("дренаж: итог = %+v, want 2/1 (до сбоя)", drainRes)
	}
}

// TestReanalyzeSelectError — ошибка выборки обёрнута; отмена ctx
// сохраняется errors.Is.
func TestReanalyzeSelectError(t *testing.T) {
	pool := newReanPool()
	pool.setQueryErr(errors.New("бд недоступна"))
	_, err := ReanalyzeOnce(context.Background(), pool, &reanQueue{}, 1, nil, ReanalyzeOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "analyzer: переанализ: выборка") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка = %v, want контекст выборки и исходная ошибка", err)
	}

	// Дренаж не глотает ошибку выборки (мутант вырезания return, T-M10):
	// итог «как есть», ошибка наружу.
	pool2 := newReanPool()
	pool2.setQueryErr(errors.New("бд недоступна"))
	if _, err := ReanalyzeDrain(context.Background(), pool2, &reanQueue{}, 1, nil, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "analyzer: переанализ: выборка") {
		t.Errorf("дренаж: ошибка = %v, want контекст выборки (не глотается)", err)
	}

	pool.setQueryErr(context.Canceled)
	if _, err := ReanalyzeOnce(context.Background(), pool, &reanQueue{}, 1, nil, ReanalyzeOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}
}

// TestReanalyzeRowErrors — сбои Scan и Err строк выборки: ошибки обёрнуты
// контекстом «чтение» (мутанты вырезания return, T-M10), публикаций нет.
func TestReanalyzeRowErrors(t *testing.T) {
	seed := func() *reanPool {
		pool := newReanPool()
		pool.seedZone(1, "test")
		pool.seedDomain(1, 1, "a", model.StatusAlive)
		pool.seedRow(1, 1, 0, model.VariantMain, nil)
		return pool
	}
	pool := seed()
	pool.selectScanErr = errors.New("скан-бум")
	q := &reanQueue{}
	if _, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "analyzer: переанализ: выборка: чтение: скан-бум") {
		t.Errorf("Scan: ошибка = %v, want «выборка: чтение: скан-бум»", err)
	}
	if n := q.count(); n != 0 {
		t.Errorf("публикаций при сбое Scan = %d, want 0", n)
	}

	pool = seed()
	pool.selectRowsErr = errors.New("строк-бум")
	if _, err := ReanalyzeOnce(context.Background(), pool, q, 1, nil, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "analyzer: переанализ: выборка: строк-бум") {
		t.Errorf("Err: ошибка = %v, want «выборка: строк-бум» (без «чтение»)", err)
	}
	if n := q.count(); n != 0 {
		t.Errorf("публикаций при сбое Err = %d, want 0", n)
	}
}

// TestReanalyzeDrainCancel — отмена ctx между пачками: чистый выход с
// ошибкой контекста (без двойной обёртки «выборка» — пин текста против
// мутанта вырезания return, T-M10), опубликованное остаётся в итоге.
func TestReanalyzeDrainCancel(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	for i := int64(1); i <= 4; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRow(1, i, 0, model.VariantMain, nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // пачки нет: первая же выборка вернёт ctx.Err()
	q := &reanQueue{}

	res, err := ReanalyzeDrain(ctx, pool, q, 1, nil, ReanalyzeOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}
	if err != nil && err.Error() != "analyzer: переанализ: context canceled" {
		t.Errorf("ошибка = %q, want точная обёртка «analyzer: переанализ: context canceled»", err)
	}
	if res.Published != 0 {
		t.Errorf("итог = %+v, want без публикаций", res)
	}
}

// TestReanalyzeNilArgs — nil-зависимости отклоняются с понятной ошибкой
// (все три входа: разовый проход, дренаж, цикл).
func TestReanalyzeNilArgs(t *testing.T) {
	for _, check := range []struct {
		name string
		call func() error
		want string
	}{
		{"проход без пула", func() error {
			_, err := ReanalyzeOnce(context.Background(), nil, &reanQueue{}, 1, nil, ReanalyzeOptions{})
			return err
		}, "пул не задан"},
		{"проход без очереди", func() error {
			_, err := ReanalyzeOnce(context.Background(), newReanPool(), nil, 1, nil, ReanalyzeOptions{})
			return err
		}, "очередь не задана"},
		{"дренаж без пула", func() error {
			_, err := ReanalyzeDrain(context.Background(), nil, &reanQueue{}, 1, nil, ReanalyzeOptions{})
			return err
		}, "пул не задан"},
		{"дренаж без очереди", func() error {
			_, err := ReanalyzeDrain(context.Background(), newReanPool(), nil, 1, nil, ReanalyzeOptions{})
			return err
		}, "очередь не задана"},
		{"цикл без пула", func() error {
			return ReanalyzeLoop(context.Background(), nil, &reanQueue{}, newReanSource(1), ReanalyzeOptions{})
		}, "пул не задан"},
		{"цикл без очереди", func() error {
			return ReanalyzeLoop(context.Background(), newReanPool(), nil, newReanSource(1), ReanalyzeOptions{})
		}, "очередь не задана"},
		{"цикл без источника правил", func() error {
			return ReanalyzeLoop(context.Background(), newReanPool(), &reanQueue{}, nil, ReanalyzeOptions{})
		}, "источник набора правил не задан"},
		{"тик без пула", func() error {
			_, err := ReanalyzeTick(context.Background(), nil, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{})
			return err
		}, "пул не задан"},
		{"тик без очереди", func() error {
			_, err := ReanalyzeTick(context.Background(), newReanPool(), nil, 1, &ReanalyzeCursor{}, ReanalyzeOptions{})
			return err
		}, "очередь не задана"},
	} {
		if err := check.call(); err == nil || !strings.Contains(err.Error(), check.want) {
			t.Errorf("%s: ошибка = %v, want %q", check.name, err, check.want)
		}
	}
}

// --- опции -----------------------------------------------------------------

func TestReanalyzeOptionsDefaults(t *testing.T) {
	got := (ReanalyzeOptions{}).withDefaults()
	if got.Interval != DefaultReanalyzeInterval || got.BatchSize != DefaultReanalyzeBatch {
		t.Errorf("withDefaults = %v/%v, want %v/%v",
			got.Interval, got.BatchSize, DefaultReanalyzeInterval, DefaultReanalyzeBatch)
	}
	got = ReanalyzeOptions{Interval: -time.Minute, BatchSize: -1}.withDefaults()
	if got.Interval != DefaultReanalyzeInterval || got.BatchSize != DefaultReanalyzeBatch {
		t.Errorf("withDefaults(отрицательные) = %v/%v, want дефолты",
			got.Interval, got.BatchSize)
	}
	keep := ReanalyzeOptions{Interval: time.Minute, BatchSize: 10}.withDefaults()
	if keep.Interval != time.Minute || keep.BatchSize != 10 {
		t.Errorf("withDefaults затирает заданные значения: %v/%v", keep.Interval, keep.BatchSize)
	}
	// Единичные значения валидны и не заменяются дефолтами (граница
	// «<= 0 → <= 1», T-M10): 1 нс и 1 строка — рабочие настройки.
	unit := ReanalyzeOptions{Interval: 1, BatchSize: 1}.withDefaults()
	if unit.Interval != 1 || unit.BatchSize != 1 {
		t.Errorf("withDefaults(единичные) = %v/%v, want 1/1", unit.Interval, unit.BatchSize)
	}
}

// --- ZoneIDByName ----------------------------------------------------------

func TestZoneIDByName(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(3, "crawl.test")

	id, found, err := ZoneIDByName(context.Background(), pool, "crawl.test")
	if err != nil || !found || id != 3 {
		t.Fatalf("ZoneIDByName = %d, %v, %v; want 3, true, nil", id, found, err)
	}
	calls := pool.calls()
	if len(calls) != 1 || calls[0].sql != wantZoneIDByName {
		t.Fatalf("SQL = %v, want точный контракт %s", calls, wantZoneIDByName)
	}
	if zoneIDByNameSQL != wantZoneIDByName {
		t.Errorf("константа разошлась с контрактом теста")
	}

	// Зоны нет: id ровно 0 и found=false (мутанты «return 0» → ±1, T-M10).
	id, found, err = ZoneIDByName(context.Background(), pool, "nope.test")
	if err != nil || found || id != 0 {
		t.Errorf("несуществующая зона = %d, %v, %v; want 0, false, nil", id, found, err)
	}

	fail := newReanPool()
	fail.setQueryErr(errors.New("бд недоступна"))
	id, _, err = ZoneIDByName(context.Background(), fail, "x.test")
	if err == nil || !strings.Contains(err.Error(), "analyzer: зона по имени") {
		t.Errorf("ошибка = %v, want контекст задачи", err)
	}
	if id != 0 {
		t.Errorf("id при ошибке = %d, want 0 (мутанты «return 0» → ±1, T-M10)", id)
	}

	// Сбой Scan найденной зоны: ошибка «чтение», found=false, id=0
	// (мутанты вырезания return и «return 0» → ±1, T-M10).
	scan := newReanPool()
	scan.seedZone(3, "crawl.test")
	scan.zoneScanErr = errors.New("скан-бум")
	id, found, err = ZoneIDByName(context.Background(), scan, "crawl.test")
	if err == nil || !strings.Contains(err.Error(), "analyzer: зона по имени: чтение: скан-бум") {
		t.Errorf("Scan: ошибка = %v, want «зона по имени: чтение: скан-бум»", err)
	}
	if found || id != 0 {
		t.Errorf("Scan: id/found = %d, %v; want 0, false", id, found)
	}

	// Сбой Err строк пустой выборки: ошибка контекста задачи, id=0.
	rows := newReanPool()
	rows.zoneRowsErr = errors.New("строк-бум")
	id, found, err = ZoneIDByName(context.Background(), rows, "x.test")
	if err == nil || !strings.Contains(err.Error(), "analyzer: зона по имени: строк-бум") {
		t.Errorf("Err: ошибка = %v, want «зона по имени: строк-бум»", err)
	}
	if found || id != 0 {
		t.Errorf("Err: id/found = %d, %v; want 0, false", id, found)
	}

	id, _, err = ZoneIDByName(context.Background(), nil, "x.test")
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
	if id != 0 {
		t.Errorf("id без пула = %d, want 0", id)
	}
}

// --- ReanalyzeLoop ---------------------------------------------------------

// reanWait — ожидание условия с дедлайном (цикл работает в горутине).
func reanWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("цикл не дошёл: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestReanalyzeLoopImmediateDrain — стартовый ПОЛНЫЙ дренаж выполняется
// сразу: устаревшее за простоем (включая сменившиеся за простоем
// правила) публикуется до первого тика — PK-формой с нулевого курсора
// (§4.3). Без Options.Log: nil-логгер обязан заменяться заглушкой —
// Info «проход переанализа» по публикации дренажа на nil-указателе
// паниковал бы (мутант вырезания подстановки Discard, T-M10).
func TestReanalyzeLoopImmediateDrain(t *testing.T) {
	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, newReanSource(42), ReanalyzeOptions{
			Interval: time.Hour, // тик не наступит: только стартовый дренаж
		})
	}()

	reanWait(t, "стартовый дренаж", func() bool { return q.count() >= 1 })
	if n := pool.queryCount(); n != 1 {
		t.Errorf("выборок = %d, want 1: без тика повторных проходов нет", n)
	}
	call := pool.calls()[0]
	if call.sql != reanalyzeSelectSQL {
		t.Errorf("SQL стартового прохода — PK-форма дренажа:\n%s\nwant:\n%s",
			call.sql, reanalyzeSelectSQL)
	}
	// Курсор дренажа — нулевой PK (с начала множества).
	if call.args[3] != int16(0) || call.args[4] != int64(0) {
		t.Errorf("курсор стартового дренажа = %v, want нулевой PK", call.args[3:5])
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil при отмене ctx", err)
	}
}

// TestReanalyzeLoopTickAndTrigger — тикер повторяет проход время-формой
// (страховка): первый тик заякоренным курсором перепубликует ещё не
// проштампованные строки в окне (дубликаты in-flight, T-071), курсор
// проходит их — дальнейшие тики окно не перечитывают; смена набора
// правил — внеочередной ПОЛНЫЙ дренаж PK-формой с нуля; отмена ctx —
// чистый nil.
func TestReanalyzeLoopTickAndTrigger(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedDomain(1, 2, "b", model.StatusAlive)
	// Обе строки — в окне якоря первого тика (NULL-версии, не проштампованы).
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-2*time.Hour))
	pool.seedRowAt(1, 2, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}
	src := newReanSource(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, src, ReanalyzeOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Стартовый дренаж: обе строки published по одному разу.
	reanWait(t, "стартовый дренаж", func() bool { return q.count() >= 2 })

	// Первый тиковый проход время-формой: окно якоря (24 ч) вмещает обе
	// не проштампованные строки — перепубликация in-flight (at-least-once),
	// курсор проходит за последнюю (b).
	reanWait(t, "тиковый проход", func() bool { return q.count() >= 4 })
	var tickSeen bool
	for _, c := range pool.calls() {
		if c.sql == reanalyzeTickSQL {
			tickSeen = true
		}
	}
	if !tickSeen {
		t.Errorf("тиковый проход не ходил время-формой (SQL не замечен)")
	}

	// Дальнейшие тики: обе строки ПОЗАДИ курсора — переизбрания нет
	// (время-курсор, а не полный круг по версии).
	reanWait(t, "тиковые выборки после исчерпания окна", func() bool { return pool.queryCount() >= 4 })
	if n := q.count(); n != 4 {
		t.Errorf("публикаций после исчерпания окна = %d, want 4 (без переизбрания)", n)
	}

	src.switchTo(2) // SIGHUP: внеочередной ПОЛНЫЙ дренаж с новой версией
	reanWait(t, "внеочередной проход по смене правил", func() bool {
		return strings.Contains(buf.String(), "внеочередной переанализ") &&
			strings.Contains(buf.String(), "rules_version=2")
	})
	// Дренаж — PK-форма с нулевого курсора: обе строки (устаревшие против
	// версии 2) опубликованы снова.
	reanWait(t, "публикации дренажа", func() bool { return q.count() >= 6 })
	drained := false
	for _, c := range pool.calls() {
		if c.sql != reanalyzeSelectSQL {
			continue
		}
		drained = true
		if c.args[3] != int16(0) || c.args[4] != int64(0) {
			t.Errorf("курсор дренажа = %v, want нулевой PK (форма с нуля)", c.args[3:5])
		}
	}
	if !drained {
		t.Errorf("среди выборок нет PK-формы: смена правил обязана идти дренажом с нуля")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil при отмене ctx", err)
	}
}

// TestReanalyzeLoopErrorSurvives — ошибка прохода (БД недоступна) — Warn,
// цикл живёт: после восстановления тиковый проход публикует; отмена —
// чистый nil. Курсор остаётся нулевым (якорь), восстановление — окно
// lookback, ничего не теряется (§4.2).
func TestReanalyzeLoopErrorSurvives(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}

	pool.setQueryErr(errors.New("бд недоступна")) // стартовый проход падает

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, newReanSource(1), ReanalyzeOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	reanWait(t, "Warn об ошибке прохода", func() bool {
		return strings.Contains(buf.String(), "level=WARN") &&
			strings.Contains(buf.String(), "бд недоступна")
	})

	pool.setQueryErr(nil) // БД вернулась
	reanWait(t, "проход после восстановления", func() bool { return q.count() >= 1 })

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil при отмене ctx", err)
	}
}

// TestReanalyzeLoopCancelDuringDrain — отмена во время прохода — не
// тревога: без Warn, чистый nil (контракт OnRun, T-012).
func TestReanalyzeLoopCancelDuringDrain(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.setQueryErr(context.Canceled)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до старта: первый же проход вернёт отмену

	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, &reanQueue{}, newReanSource(1), ReanalyzeOptions{Log: logger})
	}()

	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil при отмене ctx", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("отмена во время прохода — Warn:\n%s", buf.String())
	}
}

// TestReanalyzeLoopNilSet — источник без набора: пропуск с Warn, цикл
// живёт (после появления набора проход работает).
func TestReanalyzeLoopNilSet(t *testing.T) {
	var buf reanBuf
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := newReanPool()
	pool.seedZone(1, "test")
	pool.seedDomain(1, 1, "a", model.StatusAlive)
	pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
	q := &reanQueue{}
	src := &reanSource{next: make(chan *rules.Set, 1)} // Set() = nil

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, src, ReanalyzeOptions{
			Interval: time.Hour,
			Log:      logger,
		})
	}()

	reanWait(t, "Warn о пустом наборе", func() bool {
		return strings.Contains(buf.String(), "набор правил не загружен")
	})
	if n := q.count(); n != 0 {
		t.Errorf("публикаций = %d, want 0 без набора", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil при отмене ctx", err)
	}
}

// TestReanalyzeLoopPassInfo — порог Info «проход переанализа» по итогам
// дренажа: пустой проход молчит, проход с публикациями пишет итог
// (мутанты «Published > 0» ±/вырезание и удаление самого Info, T-M10).
// Вторая выборка доказывает завершение первого прохода (дренажи
// последовательны), к моменту проверки лог уже решён.
func TestReanalyzeLoopPassInfo(t *testing.T) {
	t.Run("пустой проход молчит", func(t *testing.T) {
		var buf reanBuf
		pool := newReanPool() // пусто: дренаж без публикаций
		q := &reanQueue{}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- ReanalyzeLoop(ctx, pool, q, newReanSource(1), ReanalyzeOptions{
				Interval: 2 * time.Millisecond,
				Log:      slog.New(slog.NewTextHandler(&buf, nil)),
			})
		}()

		reanWait(t, "второй проход", func() bool { return pool.queryCount() >= 2 })
		if strings.Contains(buf.String(), `msg="проход переанализа"`) {
			t.Errorf("Info о пустом проходе записан:\n%s", buf.String())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("ReanalyzeLoop: %v", err)
		}
	})

	t.Run("проход с публикациями пишет Info", func(t *testing.T) {
		var buf reanBuf
		pool := newReanPool()
		pool.seedZone(1, "test")
		pool.seedDomain(1, 1, "a", model.StatusAlive)
		pool.seedRowAt(1, 1, 0, model.VariantMain, nil, time.Now().Add(-time.Minute))
		q := &reanQueue{}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- ReanalyzeLoop(ctx, pool, q, newReanSource(1), ReanalyzeOptions{
				Interval: 2 * time.Millisecond,
				Log:      slog.New(slog.NewTextHandler(&buf, nil)),
			})
		}()

		reanWait(t, "второй проход", func() bool { return pool.queryCount() >= 2 })
		if log := buf.String(); !strings.Contains(log, `msg="проход переанализа"`) || !strings.Contains(log, "published=1") {
			t.Errorf("Info о проходе с публикацией не записан:\n%s", log)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("ReanalyzeLoop: %v", err)
		}
	})
}

// TestReanalyzeLoopClosedSets — закрытый канал подписки не зацикливает
// select: ветка гасится (sets = nil), цикл продолжает жить по тикеру и
// выходит только отменой. Убивает мутант вырезания ветки целиком (тот
// льёт поток drain/логов «внеочередного переанализа» по закрытому
// каналу); continue → break в select эквивалентен и погашен аннотацией
// в reanalyze.go. Пустой пул: корректный цикл после закрытия не делает
// ни выборки, ни записи в лог.
func TestReanalyzeLoopClosedSets(t *testing.T) {
	var buf reanBuf
	pool := newReanPool()
	q := &reanQueue{}
	src := newReanSource(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, src, ReanalyzeOptions{
			Interval: time.Hour, // тик не наступит: после закрытия циклу нечего делать
			Log:      slog.New(slog.NewTextHandler(&buf, nil)),
		})
	}()

	reanWait(t, "стартовый проход", func() bool { return pool.queryCount() >= 1 })
	if !strings.Contains(buf.String(), "цикл переанализа запущен") {
		t.Errorf("Info запуска не записан:\n%s", buf.String())
	}
	close(src.next) // Reloader ушёл, не отменив подписку

	// Окно 100 мс: мутанты успевают себя проявить (горячий цикл с
	// drain на каждую итерацию или мгновенный выход из цикла).
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("цикл завершился по закрытому каналу: %v", err)
	default:
	}
	if strings.Contains(buf.String(), "внеочередной переанализ") {
		t.Errorf("закрытый канал трактован как смена набора:\n%s", buf.String())
	}
	if n := pool.queryCount(); n != 1 {
		t.Fatalf("выборок после закрытия канала = %d, want 1 (без внеочередных проходов)", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ReanalyzeLoop: %v, want nil при отмене ctx", err)
	}
}
