// Юнит-тесты переполнения очереди рендера (T-066; ТЗ 5.6, §7) на фейковых
// пуле/очереди/хранилище — без БД, Redis и сети. Собственные фейковые пулы
// с префиксом renderq (граница задачи: record_test.go T-058 не правится):
// проверяются точные SQL и параметры прохода, флаг render_pending в той
// же транзакции результата, порядок «фиксация → публикация q:render»,
// неблокирующая публикация (XACK не ждёт ёмкости), повторный SPA-вердикт
// по сохранённому контенту, алиасы, ошибки/отмена ctx, цикл прохода.
// Поведение против живых PostgreSQL/Redis — интеграционные тесты.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- протокол событий --------------------------------------------------------

// renderqJournal — потокобезопасный протокол операций (стиль recordJournal).
type renderqJournal struct {
	mu     sync.Mutex
	events []string
	// done — неблокируемый сигнал на каждое событие вида (синхронизация
	// цикла прохода в горутине).
	done chan string
}

func (j *renderqJournal) record(e string) {
	j.mu.Lock()
	j.events = append(j.events, e)
	j.mu.Unlock()
	if j.done != nil {
		select {
		case j.done <- e:
		default:
		}
	}
}

func (j *renderqJournal) indexOf(e string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, v := range j.events {
		if v == e {
			return i
		}
	}
	return -1
}

func (j *renderqJournal) count(e string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, v := range j.events {
		if v == e {
			n++
		}
	}
	return n
}

// snapshot — копия всех событий (проверка последовательностей шагов).
func (j *renderqJournal) snapshot() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]string, len(j.events))
	copy(out, j.events)
	return out
}

// --- фейковые RenderLen и очередь --------------------------------------------

// renderqLen — RenderLen: длина по счётчику вызовов (seq — ответы по
// порядку, дальше повторяется последний), настраиваемый сбой.
type renderqLen struct {
	mu    sync.Mutex
	seq   []int64
	err   error
	calls int
}

func (l *renderqLen) Len(ctx context.Context, stream string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.err != nil {
		return 0, l.err
	}
	if stream != queue.StreamRender {
		return 0, fmt.Errorf("renderqLen: стрим %s, ожидался %s", stream, queue.StreamRender)
	}
	if len(l.seq) == 0 {
		return 0, nil
	}
	n := l.seq[0]
	if len(l.seq) > 1 {
		l.seq = l.seq[1:]
	}
	return n, nil
}

func (l *renderqLen) callsCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// renderqPublish — записанная публикация.
type renderqPublish struct {
	stream string
	msg    queue.Result
}

// renderqQueue — queue.Queue: фиксирует публикации Result (сбои по
// индексу), Consume паркуется до отмены ctx, Ack — заглушка.
type renderqQueue struct {
	j *renderqJournal

	errs []error

	mu        sync.Mutex
	publishes []renderqPublish
}

var _ queue.Queue = (*renderqQueue)(nil)

func (q *renderqQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("renderqQueue: неожидаемое сообщение %T (нужен queue.Result)", msg)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, renderqPublish{stream: stream, msg: res})
	q.mu.Unlock()
	if q.j != nil {
		q.j.record("publish:" + stream)
	}
	if i < len(q.errs) && q.errs[i] != nil {
		return "", q.errs[i]
	}
	return fmt.Sprintf("entry-%s-%d", stream, i+1), nil
}

func (q *renderqQueue) Consume(ctx context.Context, _, _, _ string, _ int) ([]queue.Delivery, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *renderqQueue) Ack(context.Context, string, string, ...string) error { return nil }

func (q *renderqQueue) snapshot() []renderqPublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]renderqPublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

// renderqContent — RenderContent: тело по хэшу, настраиваемый сбой.
type renderqContent struct {
	mu     sync.Mutex
	bodies map[content.Hash][]byte
	errOn  map[content.Hash]error
}

func (c *renderqContent) Get(ctx context.Context, hash content.Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.errOn[hash]; err != nil {
		return nil, err
	}
	body, ok := c.bodies[hash]
	if !ok {
		return nil, fmt.Errorf("renderqContent: блока %s нет", hash)
	}
	return body, nil
}

// --- фейковый пул прохода (RecordDB): domains/zones/subdomain_results --------

// renderqDomKey — ключ строки domains в фейке прохода.
type renderqDomKey struct {
	zone int16
	id   int64
}

// renderqDomain — строка domains, которую читает и меняет проход.
type renderqDomain struct {
	flags  model.Flags
	status model.Status
	label  string
}

// renderqRow — строка subdomain_results для прохода.
type renderqRow struct {
	subdomain int16
	variant   model.Variant
	hash      []byte // nil — content_hash NULL
	aliasSub  *int16 // nil — alias_subdomain_id NULL
	rendered  bool
	finalURL  string // "" — NULL (фильтр final_url IS NOT NULL)
}

// renderqPool — RecordDB прохода без БД: живая семантика фильтров SQL
// (флаг render_pending, status <> deleted, rendered = false,
// final_url IS NOT NULL), смена флага с охраной бита и счётчик долга
// crawler_counters (T-175: строка render_pending, ±1 в транзакции
// снятия — как в живом RequeueOnce); SQL сопоставляется ТОЧНО
// (неожидаемый — ошибка, тест падает громко). RecordDB, а не db.DB
// (T-175): проход снимает флаг транзакцией со счётчиком.
type renderqPool struct {
	j *renderqJournal

	zones   map[int16]string
	domains map[renderqDomKey]*renderqDomain
	rows    map[renderqDomKey][]renderqRow
	// order — порядок вставки доменов: «физический порядок кучи» для
	// выборки без ORDER BY (T-181) — seq scan возвращает строки
	// раскладкой, а не PK; сид-порядок детерминирует пачки в тестах.
	order    []renderqDomKey
	counters map[string]int64 // T-175: crawler_counters ('render_pending')

	queryErrs    map[string]error
	beforeHook   func() // вызывается перед первым Exec транзакции снятия (симуляция гонок)
	counterNoRow bool   // строка счётчика отсутствует (guard и UPDATE 0 — ошибка вызывающего)

	mu       sync.Mutex
	queries  int
	clears   int
	clearTag int64 // RowsAffected снятия флага (0 — честный ответ)
}

var _ RecordDB = (*renderqPool)(nil)

func newRenderqPool() *renderqPool {
	return &renderqPool{
		j:         &renderqJournal{},
		zones:     make(map[int16]string),
		domains:   make(map[renderqDomKey]*renderqDomain),
		rows:      make(map[renderqDomKey][]renderqRow),
		counters:  map[string]int64{"render_pending": 0},
		queryErrs: make(map[string]error),
	}
}

func (p *renderqPool) seedDomain(zone int16, id int64, label string, flags model.Flags, status model.Status) *renderqDomain {
	d := &renderqDomain{flags: flags, status: status, label: label}
	p.domains[renderqDomKey{zone: zone, id: id}] = d
	p.order = append(p.order, renderqDomKey{zone: zone, id: id})
	// Инвариант счётчика (T-175): он равен числу строк с битом — как
	// бэкфилл миграции 000015 (без фильтра по status: удалённый с битом
	// домен в счётчике остаётся, снимает бит только проход доработки).
	if flags.Test(model.FlagRenderPending) {
		p.counters["render_pending"]++
	}
	return d
}

func (p *renderqPool) seedRow(zone int16, id int64, sub int16, variant model.Variant, hash []byte, aliasSub *int16, rendered bool, finalURL string) {
	p.rows[renderqDomKey{zone: zone, id: id}] = append(p.rows[renderqDomKey{zone: zone, id: id}],
		renderqRow{subdomain: sub, variant: variant, hash: hash, aliasSub: aliasSub, rendered: rendered, finalURL: finalURL})
}

func (p *renderqPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("renderqPool: CopyFrom не должен вызываться")
}

func (p *renderqPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("renderqPool: Exec вне транзакции не должен вызываться (снятие — транзакцией, T-175)")
}

// Begin — снапшот доменов и счётчика (стиль renderqRecPool): откат
// возвращает оба, фиксация оставляет изменения.
func (p *renderqPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	snapDomains := make(map[renderqDomKey]*renderqDomain, len(p.domains))
	for k, v := range p.domains {
		c := *v
		snapDomains[k] = &c
	}
	snapCounters := make(map[string]int64, len(p.counters))
	for k, v := range p.counters {
		snapCounters[k] = v
	}
	p.j.record("begin")
	return &renderqTx{
		pool: p, j: p.j,
		snapDomains: snapDomains, snapCounters: snapCounters,
	}, nil
}

// renderqTx — транзакция фейка прохода: снятие флага и сдвиг счётчика
// долга (T-175), откат возвращает снапшот.
type renderqTx struct {
	pgx.Tx

	pool         *renderqPool
	j            *renderqJournal
	snapDomains  map[renderqDomKey]*renderqDomain
	snapCounters map[string]int64
	committed    bool
}

func (t *renderqTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if err := t.pool.queryErrs[sql]; err != nil {
		return pgconn.CommandTag{}, err
	}
	switch sql {
	case renderPlanNoIndexScanSQL, renderPlanNoBitmapScanSQL, renderPlanNoIOSQL:
		// Закрепление плана выборки (T-181): SET LOCAL ×3 до SELECT —
		// последовательность проверяет TestRequeueOnceSelectPlan.
		t.j.record("plan")
		return pgconn.NewCommandTag("SET"), nil
	case renderClearPendingSQL:
		if t.pool.beforeHook != nil {
			t.pool.beforeHook()
			t.pool.beforeHook = nil
		}
		if len(args) != 2 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: снятие флага: параметров %d, нужно 2", len(args))
		}
		zone, okZ := args[0].(int16)
		id, okI := args[1].(int64)
		if !okZ || !okI {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: снятие флага: параметры %T/%T, нужно int16/int64", args[0], args[1])
		}
		t.pool.clears++
		d, ok := t.pool.domains[renderqDomKey{zone: zone, id: id}]
		if !ok || !d.flags.Test(model.FlagRenderPending) {
			return pgconn.NewCommandTag("UPDATE 0"), nil // охрана бита
		}
		d.flags.Clear(model.FlagRenderPending)
		t.j.record("clear")
		if t.pool.clearTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", t.pool.clearTag)), nil
		}
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case renderCounterDeltaSQL:
		if len(args) != 1 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: счётчик: параметров %d, нужно 1", len(args))
		}
		delta, ok := args[0].(int64)
		if !ok {
			return pgconn.CommandTag{}, fmt.Errorf("renderqPool: счётчик: параметр %T, нужно int64", args[0])
		}
		if t.pool.counterNoRow {
			return pgconn.NewCommandTag("UPDATE 0"), nil // строки нет (ручная правка)
		}
		t.pool.counters["render_pending"] += delta
		t.j.record("debtcount")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("renderqPool: неожидаемый SQL Exec: %s", sql)
	}
}

func (t *renderqTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.committed = true
	t.j.record("commit")
	return nil
}

// Query — выборка пачки доменов ВНУТРИ транзакции закрепления плана
// (T-181): renderSelectPending зовёт tx.Query, не пул — фейк повторяет
// живую точку вызова (пул напрямую пачку не читает).
func (t *renderqTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if err := t.pool.queryErrs[sql]; err != nil {
		return nil, err
	}
	t.pool.queries++
	switch sql {
	case renderPendingDomainsSQL:
		if len(args) != 1 {
			return nil, fmt.Errorf("renderqPool: домены: параметров %d, нужно 1 (LIMIT)", len(args))
		}
		limit, ok := args[0].(int)
		if !ok || limit <= 0 {
			return nil, fmt.Errorf("renderqPool: домены: LIMIT %v (%T)", args[0], args[0])
		}
		t.j.record("select")
		// Порядок — «физический» (order, порядок вставки): SQL без
		// ORDER BY, seq scan возвращает раскладку кучи (T-181).
		rows := make([][]any, 0, limit)
		for _, k := range t.pool.order {
			if len(rows) == limit {
				break
			}
			d, ok := t.pool.domains[k]
			if !ok || !d.flags.Test(model.FlagRenderPending) || d.status == model.StatusDeleted {
				continue // WHERE бита и status <> deleted
			}
			rows = append(rows, []any{k.zone, k.id, d.label + "." + t.pool.zones[k.zone]})
		}
		return renderqRowsOf(rows...), nil
	default:
		return nil, fmt.Errorf("renderqPool: неожидаемый SQL Query в транзакции: %s", sql)
	}
}

func (t *renderqTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.committed {
		return nil
	}
	t.pool.domains, t.pool.counters = t.snapDomains, t.snapCounters
	return nil
}

func (p *renderqPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.queryErrs[sql]; err != nil {
		return nil, err
	}
	p.queries++
	switch sql {
	case renderDebtCounterSQL:
		// Guard прохода (T-181): точечное чтение счётчика долга; строка
		// обязана быть (миграция 000015) — её отсутствие эмулируется
		// пустым набором (renderDebtCount ошибается).
		if p.counterNoRow {
			return renderqRowsOf(), nil
		}
		return renderqRowsOf([]any{p.counters["render_pending"]}), nil
	case renderPendingRowsSQL:
		if len(args) != 2 {
			return nil, fmt.Errorf("renderqPool: записи: параметров %d, нужно 2", len(args))
		}
		zone, okZ := args[0].(int16)
		id, okI := args[1].(int64)
		if !okZ || !okI {
			return nil, fmt.Errorf("renderqPool: записи: параметры %T/%T, нужно int16/int64", args[0], args[1])
		}
		src := p.rows[renderqDomKey{zone: zone, id: id}]
		rows := make([][]any, 0, len(src))
		for _, r := range src {
			if r.rendered || r.finalURL == "" {
				continue // rendered = false AND final_url IS NOT NULL
			}
			var alias any
			if r.aliasSub != nil {
				sub := *r.aliasSub
				alias = &sub
			}
			var hash any
			if r.hash != nil {
				hash = r.hash
			}
			rows = append(rows, []any{r.subdomain, int16(r.variant), hash, alias})
		}
		// ORDER BY subdomain_id, variant (src уже в порядке вставки —
		// сортировка не зависит от него).
		for i := 0; i < len(rows); i++ {
			for j := i + 1; j < len(rows); j++ {
				a, b := rows[i], rows[j]
				if a[0].(int16) > b[0].(int16) || (a[0].(int16) == b[0].(int16) && a[1].(int16) > b[1].(int16)) {
					rows[i], rows[j] = rows[j], rows[i]
				}
			}
		}
		return renderqRowsOf(rows...), nil
	default:
		return nil, fmt.Errorf("renderqPool: неожидаемый SQL Query: %s", sql)
	}
}

// renderqRows — pgx.Rows с заготовленными строками (Scan — позиционный,
// типы int16/int64/string/[]byte/*int16).
type renderqRows struct {
	rows [][]any
	i    int
}

func renderqRowsOf(rows ...[]any) *renderqRows { return &renderqRows{rows: rows} }

func (r *renderqRows) Close()                                       {}
func (r *renderqRows) Err() error                                   { return nil }
func (r *renderqRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *renderqRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *renderqRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *renderqRows) RawValues() [][]byte                          { return nil }
func (r *renderqRows) Conn() *pgx.Conn                              { return nil }
func (r *renderqRows) TypeMap() *pgtype.Map                         { return nil }

func (r *renderqRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *renderqRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("renderqRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("renderqRows: приёмников %d, колонок %d", len(dest), len(row))
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
		case *[]byte:
			if v == nil {
				*dst = nil
			} else {
				*dst = v.([]byte)
			}
		case **int16:
			if v == nil {
				*dst = nil
			} else {
				sub := *v.(*int16)
				*dst = &sub
			}
		default:
			return fmt.Errorf("renderqRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// --- фейковый RecordDB записи результата (с флагом) ---------------------------

// renderqRecDomain — строка domains фейка записи: как recordFakeDomain,
// плюс flags (renderqRecPool применяет renderSetPendingSQL).
type renderqRecDomain struct {
	status       model.Status
	checkState   model.CheckState
	queuedAt     *time.Time
	lastChecked  bool
	tempAttempts int16
	flags        model.Flags
}

// renderqRecPool — RecordDB без БД: семантика транзакции записи результата
// (record.go) с флагом render_pending. Фейк T-058 (record_test.go) флаг не
// отслеживает и править его нельзя (граница задачи) — потому собственная
// копия: domains (UPDATE статуса, OR флага), subdomain_results (DELETE …
// RETURNING, INSERT-пачка), contents (refcount, SwapRefTx T-044),
// транзакции-снапшоты.
type renderqRecPool struct {
	j *renderqJournal

	domains  map[recordDomainKey]*renderqRecDomain
	rows     map[recordDomainKey][]recordFakeRow
	refc     map[content.Hash]int32
	counters map[string]int64 // T-175: crawler_counters ('render_pending')

	failExec     map[string]error
	counterNoRow bool // строки счётчика нет (UPDATE 0 — ошибка записи)
	deleteErr    error
	commitErr    error
	beginErr     error

	mu sync.Mutex
}

var _ RecordDB = (*renderqRecPool)(nil)

func newRenderqRecPool() *renderqRecPool {
	return &renderqRecPool{
		j:        &renderqJournal{},
		domains:  make(map[recordDomainKey]*renderqRecDomain),
		rows:     make(map[recordDomainKey][]recordFakeRow),
		refc:     make(map[content.Hash]int32),
		counters: map[string]int64{"render_pending": 0}, // строка миграции 000015
		failExec: make(map[string]error),
	}
}

func (p *renderqRecPool) seedDomain(zone int16, id int64, status model.Status, flags model.Flags) *renderqRecDomain {
	d := &renderqRecDomain{status: status, checkState: model.CheckStateQueued, flags: flags, tempAttempts: 2}
	now := time.Now()
	d.queuedAt = &now
	p.domains[recordDomainKey{zone: zone, id: id}] = d
	// Инвариант счётчика (T-175): равен числу строк с битом (бэкфилл
	// миграции 000015).
	if flags.Test(model.FlagRenderPending) {
		p.counters["render_pending"]++
	}
	return d
}

func (p *renderqRecPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("renderqRecPool: CopyFrom не должен вызываться")
}

func (p *renderqRecPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("renderqRecPool: Exec вне транзакции")
}

func (p *renderqRecPool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("renderqRecPool: Query вне транзакции")
}

func (p *renderqRecPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	snapDomains := make(map[recordDomainKey]*renderqRecDomain, len(p.domains))
	for k, v := range p.domains {
		c := *v
		snapDomains[k] = &c
	}
	snapRows := make(map[recordDomainKey][]recordFakeRow, len(p.rows))
	for k, v := range p.rows {
		snapRows[k] = append([]recordFakeRow(nil), v...)
	}
	snapRefc := make(map[content.Hash]int32, len(p.refc))
	for k, v := range p.refc {
		snapRefc[k] = v
	}
	snapCounters := make(map[string]int64, len(p.counters))
	for k, v := range p.counters {
		snapCounters[k] = v
	}
	p.j.record("begin")
	return &renderqRecTx{
		pool: p, j: p.j,
		snapDomains: snapDomains, snapRows: snapRows, snapRefc: snapRefc,
		snapCounters: snapCounters,
	}, nil
}

type renderqRecTx struct {
	pgx.Tx

	pool         *renderqRecPool
	j            *renderqJournal
	snapDomains  map[recordDomainKey]*renderqRecDomain
	snapRows     map[recordDomainKey][]recordFakeRow
	snapRefc     map[content.Hash]int32
	snapCounters map[string]int64
	committed    bool
}

func (t *renderqRecTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if err := t.pool.failExec[sql]; err != nil {
		return pgconn.CommandTag{}, err
	}
	switch sql {
	case recordDomainSQL:
		zone := args[0].(int16)
		id := args[1].(int64)
		status := model.Status(args[2].(int16))
		d, ok := t.pool.domains[recordDomainKey{zone: zone, id: id}]
		if !ok || d.status == model.StatusDeleted {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		d.status = status
		d.checkState = model.CheckStateDone
		d.queuedAt = nil
		d.lastChecked = true
		if status != model.StatusTempError {
			d.tempAttempts = 0
		}
		t.j.record("domains")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case renderSetPendingSQL:
		if len(args) != 2 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: флаг: параметров %d, нужно 2", len(args))
		}
		zone, okZ := args[0].(int16)
		id, okI := args[1].(int64)
		if !okZ || !okI {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: флаг: параметры %T/%T", args[0], args[1])
		}
		d, ok := t.pool.domains[recordDomainKey{zone: zone, id: id}]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		// Охрана бита (T-175): RowsAffected — индикатор перехода 0→1,
		// повторная установка (флаг уже стоит) переходом не считается.
		if d.flags.Test(model.FlagRenderPending) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		d.flags.Set(model.FlagRenderPending)
		t.j.record("setflag")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case renderCounterDeltaSQL:
		if len(args) != 1 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: счётчик: параметров %d, нужно 1", len(args))
		}
		delta, ok := args[0].(int64)
		if !ok {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: счётчик: параметр %T, нужно int64", args[0])
		}
		if t.pool.counterNoRow {
			return pgconn.NewCommandTag("UPDATE 0"), nil // строки нет (ручная правка)
		}
		t.pool.counters["render_pending"] += delta
		t.j.record("debtcount")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordZoneShiftSQL:
		// Дельта счётчиков зоны (T-178): значения фейку не важны —
		// запись сценария долга рендера их не проверяет; строка зоны
		// «существует» всегда.
		t.j.record("zoneshift")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordInsertSQL:
		if len(args) != 18 {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: INSERT: параметров %d, нужно 18", len(args))
		}
		zone := args[0].([]int16)
		domain := args[1].([]int64)
		sub := args[2].([]int16)
		variant := args[3].([]int16)
		scheme := args[4].([]int16)
		httpStatus := args[5].([]*int16)
		finalURL := args[6].([]*string)
		aliasKind := args[7].([]int16)
		aliasSub := args[8].([]*int16)
		addrs := args[9].([][]byte)
		cname := args[10].([]*string)
		tls := args[11].([]*string)
		redirects := args[12].([]*string)
		headers := args[13].([]*string)
		cookies := args[14].([]*string)
		hash := args[15].([][]byte)
		errCode := args[16].([]*int16)
		responseMS := args[17].([]*int32)

		n := len(zone)
		rows := make([]recordFakeRow, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, recordFakeRow{
				subdomainID: sub[i], variant: model.Variant(variant[i]),
				scheme: model.Scheme(scheme[i]), httpStatus: httpStatus[i],
				finalURL: finalURL[i], aliasKind: model.AliasKind(aliasKind[i]),
				aliasSub: aliasSub[i], addrs: addrs[i], cname: cname[i],
				tls: tls[i], redirects: redirects[i], headers: headers[i],
				cookies: cookies[i], rendered: false,
				contentHash: hash[i], errCode: errCode[i], responseMS: responseMS[i],
			})
		}
		key := recordDomainKey{zone: zone[0], id: domain[0]}
		t.pool.rows[key] = append(t.pool.rows[key], rows...)
		t.j.record("insert")
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", n)), nil
	default:
		if !strings.HasPrefix(sql, recordDelRefSQLPrefix) {
			return pgconn.CommandTag{}, fmt.Errorf("renderqRecPool: неожидаемый SQL: %s", sql)
		}
		var h content.Hash
		copy(h[:], args[0].([]byte))
		if t.pool.refc[h] <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		t.pool.refc[h]--
		t.j.record("swap")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
}

// QueryRow — предчтение состояний строки домена (recordSelectStateSQL,
// T-178): значения фейку не важны (сценарии долга статуса строки не
// меняют), absent — ErrNoRows.
func (t *renderqRecTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordSelectStateSQL {
		return recordStateRow{err: fmt.Errorf("renderqRecPool: неожидаемый QueryRow: %s", sql)}
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	d, ok := t.pool.domains[key]
	if !ok {
		return recordStateRow{err: pgx.ErrNoRows}
	}
	return recordStateRow{status: int16(d.status), state: int16(d.checkState)}
}

func (t *renderqRecTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordDeleteSQL {
		return nil, fmt.Errorf("renderqRecPool: неожидаемый SQL Query: %s", sql)
	}
	if t.pool.deleteErr != nil {
		return nil, t.pool.deleteErr
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	old := t.pool.rows[key]
	delete(t.pool.rows, key)
	t.j.record("delete")

	out := make([][]any, len(old))
	for i, r := range old {
		if r.contentHash == nil {
			out[i] = []any{nil}
		} else {
			hash := make([]byte, len(r.contentHash))
			copy(hash, r.contentHash)
			out[i] = []any{hash}
		}
	}
	return recordFakeRowsOf(out...), nil
}

func (t *renderqRecTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.committed = true
	t.j.record("commit")
	return nil
}

func (t *renderqRecTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.committed {
		return nil
	}
	t.pool.domains, t.pool.rows, t.pool.refc = t.snapDomains, t.snapRows, t.snapRefc
	t.pool.counters = t.snapCounters
	return nil
}

// --- фикстуры -----------------------------------------------------------------

// renderqSPABody — SPA-оболочка (T-057: мало видимого текста, корневой
// контейнер, noscript с требованием JS).
var renderqSPABody = []byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`)

// renderqFlatBody — обычная страница: видимого текста много, признаков SPA нет.
var renderqFlatBody = []byte("<html><body><p>" + strings.Repeat("обычная страница с видимым текстом ", 30) + "</p></body></html>")

// renderqResult — результат домена с тремя субдоменами: апекс — SPA
// (публикация q:render), www — обычная страница (без публикации), api —
// вердикта нет (nil). Возвращает хэши тел для фейка хранилища.
func renderqResult() (*DomainResult, content.Hash, content.Hash) {
	hSPA := content.HashOf(renderqSPABody)
	hFlat := content.HashOf(renderqFlatBody)
	spa := &httpfetch.SPAVerdict{SPA: true, Reasons: []httpfetch.SPAReason{httpfetch.SPAReasonRootContainer}}
	flat := &httpfetch.SPAVerdict{SPA: false}
	res := &DomainResult{
		ZoneID: 5, DomainID: 11, Name: "spa.example",
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "", Hostname: "spa.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 0, Label: "",
					FinalURL: "https://spa.example/", Host: "spa.example", Path: "/",
					HTTPStatus: 200, ContentHash: &hSPA, StoreContent: true,
					SPA: spa,
				}},
			},
			{
				SubdomainID: 1, Label: "www", Hostname: "www.spa.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 1, Label: "www",
					FinalURL: "https://spa.example/", Host: "spa.example", Path: "/",
					HTTPStatus: 200, ContentHash: &hFlat, StoreContent: true,
					SPA: flat,
				}},
			},
			{
				SubdomainID: 2, Label: "api", Hostname: "api.spa.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 2, Label: "api",
					FinalURL: "https://api.spa.example/", Host: "api.spa.example", Path: "/",
					HTTPStatus: 200,
					// SPA nil — страница без вердикта (ошибка загрузки тела).
				}},
			},
		},
	}
	return res, hSPA, hFlat
}

// renderqRecorder — записатель с очередью рендера над фейками.
func renderqRecorder(t *testing.T, pool *renderqRecPool, q *renderqQueue, ln *renderqLen) *Recorder {
	t.Helper()
	rq, err := NewRenderQueue(q, ln, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	r, err := NewRenderRecorder(pool, q, rq, nil)
	if err != nil {
		t.Fatalf("NewRenderRecorder: %v", err)
	}
	return r
}

// --- чистая логика -------------------------------------------------------------

// TestRenderSPAResults — в q:render идут ключи только SPA-записей
// (субдомен×вариант), включая алиас с собственным вердиктом; nil и
// SPA=false не идут.
func TestRenderSPAResults(t *testing.T) {
	spa := &httpfetch.SPAVerdict{SPA: true}
	aliasSPA := &httpfetch.SPAVerdict{SPA: true}
	flat := &httpfetch.SPAVerdict{SPA: false}
	res := &DomainResult{
		ZoneID: 5, DomainID: 11, Name: "spa.example",
		Subdomains: []SubdomainResult{
			{SubdomainID: 0, Records: []Record{
				{Variant: model.VariantMain, SubdomainID: 0, SPA: spa},
			}},
			{SubdomainID: 1, Records: []Record{
				{Variant: model.VariantMain, SubdomainID: 1, SPA: aliasSPA},
			}},
			{SubdomainID: 2, Records: []Record{
				{Variant: model.VariantMain, SubdomainID: 2, SPA: flat},
			}},
			{SubdomainID: 3, Records: []Record{
				{Variant: model.VariantHTTP, SubdomainID: 3}, // SPA nil
			}},
		},
	}
	keys := renderSPAResults(res)
	if len(keys) != 2 {
		t.Fatalf("ключей %d, ожидалось 2 (SPA-записи)", len(keys))
	}
	for i, want := range []int16{0, 1} {
		if keys[i].SubdomainID != want || keys[i].Variant != model.VariantMain {
			t.Errorf("ключ %d: субдомен %d вариант %v, ожидался %d/%v", i, keys[i].SubdomainID, keys[i].Variant, want, model.VariantMain)
		}
		if keys[i].ZoneID != 5 || keys[i].DomainID != 11 || keys[i].Name != "spa.example" {
			t.Errorf("ключ %d: идентичность %+v", i, keys[i])
		}
	}

	empty := renderSPAResults(&DomainResult{ZoneID: 5, DomainID: 11, Name: "x"})
	if len(empty) != 0 {
		t.Fatalf("без записей ключей %d, ожидалось 0", len(empty))
	}
}

// TestRenderQueueHasCapacity — ёмкость: строго ниже порога (len ==
// watermark — уже переполнение, ТЗ 4.2); watermark <= 0 отключает
// проверку без обращения к Redis (семантика T-026).
func TestRenderQueueHasCapacity(t *testing.T) {
	q := &renderqQueue{}
	cases := []struct {
		wm   int64
		n    int64
		want bool
	}{
		{wm: 100, n: 0, want: true},
		{wm: 100, n: 99, want: true},
		{wm: 100, n: 100, want: false},
		{wm: 100, n: 1000, want: false},
		{wm: 1, n: 0, want: true},
	}
	for _, tc := range cases {
		ln := &renderqLen{seq: []int64{tc.n}}
		rq, err := NewRenderQueue(q, ln, tc.wm, nil)
		if err != nil {
			t.Fatalf("NewRenderQueue: %v", err)
		}
		got, err := rq.HasCapacity(context.Background())
		if err != nil {
			t.Fatalf("wm=%d n=%d: %v", tc.wm, tc.n, err)
		}
		if got != tc.want {
			t.Errorf("wm=%d n=%d: ёмкость %v, ожидалось %v", tc.wm, tc.n, got, tc.want)
		}
	}

	// Отключенный гейт — без XLEN.
	ln := &renderqLen{}
	rq, err := NewRenderQueue(q, ln, 0, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	got, err := rq.HasCapacity(context.Background())
	if err != nil || !got {
		t.Fatalf("watermark 0: %v/%v, ожидалось true/nil", got, err)
	}
	if n := ln.callsCount(); n != 0 {
		t.Fatalf("watermark 0: обращений к XLEN %d, ожидалось 0", n)
	}

	// Ошибка XLEN — ошибка вызывающему (решение — за Recorder/проходом).
	lnErr := &renderqLen{err: errors.New("redis down")}
	rq, err = NewRenderQueue(q, lnErr, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	if _, err := rq.HasCapacity(context.Background()); err == nil {
		t.Fatal("ошибка XLEN не возвращена")
	}
}

// TestRenderQueuePublish — публикация идёт в q:render без проверки
// ёмкости; сбой возвращается вызывающему.
func TestRenderQueuePublish(t *testing.T) {
	q := &renderqQueue{}
	rq, err := NewRenderQueue(q, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	msgs := []queue.Result{
		{ZoneID: 5, DomainID: 11, SubdomainID: 0, Variant: model.VariantMain, Name: "a.example"},
		{ZoneID: 5, DomainID: 11, SubdomainID: 1, Variant: model.VariantMain, Name: "a.example"},
	}
	if err := rq.Publish(context.Background(), msgs); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	pub := q.snapshot()
	if len(pub) != 2 {
		t.Fatalf("публикаций %d, ожидалось 2", len(pub))
	}
	for i, p := range pub {
		if p.stream != queue.StreamRender {
			t.Errorf("публикация %d: стрим %s, ожидался %s", i, p.stream, queue.StreamRender)
		}
		if p.msg.SubdomainID != msgs[i].SubdomainID {
			t.Errorf("публикация %d: сообщение %+v", i, p.msg)
		}
	}

	qFail := &renderqQueue{errs: []error{nil, errors.New("xadd failed")}}
	rqFail, err := NewRenderQueue(qFail, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	if err := rqFail.Publish(context.Background(), msgs); err == nil {
		t.Fatal("сбой XADD не возвращён")
	}
	if len(qFail.snapshot()) != 2 {
		t.Fatalf("до сбоя опубликовано %d из 2", len(qFail.snapshot())-1)
	}
}

// TestNewRenderQueueValidation — nil-зависимости — ошибка (тихая потеря
// публикаций незаметна для оператора).
func TestNewRenderQueueValidation(t *testing.T) {
	if _, err := NewRenderQueue(nil, &renderqLen{}, 100, nil); err == nil {
		t.Fatal("очередь nil: ожидалась ошибка")
	}
	if _, err := NewRenderQueue(&renderqQueue{}, nil, 100, nil); err == nil {
		t.Fatal("источник длины nil: ожидалась ошибка")
	}
}

// --- Recorder + рендер-конвейер ----------------------------------------------

// TestRecorderRenderPublishes — свободная очередь: q:render публикуется
// по SPA-записям ПОСЛЕ фиксации транзакции; флаг не ставится; q:analyze —
// по всем записям (контракт T-058 не меняется).
func TestRecorderRenderPublishes(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, model.FlagIDN)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{0}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}

	pub := q.snapshot()
	var analyze, render int
	for _, p := range pub {
		switch p.stream {
		case queue.StreamAnalyze:
			analyze++
		case queue.StreamRender:
			render++
			if p.msg.SubdomainID != 0 {
				t.Errorf("q:render: субдомен %d, ожидался только SPA-апекс 0", p.msg.SubdomainID)
			}
		default:
			t.Errorf("публикация в неожидаемый стрим %s", p.stream)
		}
	}
	if analyze != 3 || render != 1 {
		t.Fatalf("публикаций q:analyze=%d q:render=%d, ожидалось 3/1", analyze, render)
	}

	// Публикация q:render — после фиксации; флаг не ставится.
	if i := pool.j.indexOf("commit"); i < 0 || pool.j.indexOf("publish:"+queue.StreamRender) < i {
		t.Fatalf("порядок нарушен: commit=%d publish:render=%d", pool.j.indexOf("commit"), pool.j.indexOf("publish:"+queue.StreamRender))
	}
	if c := pool.j.count("setflag"); c != 0 {
		t.Fatalf("флаг ставился %d раз при свободной очереди", c)
	}
	if f := pool.domains[recordDomainKey{zone: 5, id: 11}].flags; f != model.FlagIDN {
		t.Fatalf("flags=%s, ожидался только is_idn", f)
	}
}

// TestRecorderRenderOverflowFlags — переполнение q:render: публикаций
// q:render нет, флаг render_pending ставится ТОЙ ЖЕ транзакцией (между
// UPDATE статуса и фиксацией), эпизод завершается успешно — XACK не
// задерживается (§5.6); q:analyze публикуется как обычно.
func TestRecorderRenderOverflowFlags(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, model.FlagIDN)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}} // len == watermark — переполнение
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v (переполнение рендера не задерживает эпизод)", ok, err)
	}

	if c := pool.j.count("publish:" + queue.StreamRender); c != 0 {
		t.Fatalf("публикаций q:render при переполнении: %d", c)
	}
	if c := pool.j.count("publish:" + queue.StreamAnalyze); c != 3 {
		t.Fatalf("публикаций q:analyze %d, ожидалось 3", c)
	}

	// Флаг — внутри транзакции: после UPDATE статуса, до фиксации.
	// Сдвиг счётчика — последним оператором до Commit (T-175: окно
	// блокировки строки счётчика минимально), +1 ровно на переход.
	dom, set, commit := pool.j.indexOf("domains"), pool.j.indexOf("setflag"), pool.j.indexOf("commit")
	if dom < 0 || set < dom || commit < set {
		t.Fatalf("порядок нарушен: domains=%d setflag=%d commit=%d", dom, set, commit)
	}
	insert, debt := pool.j.indexOf("insert"), pool.j.indexOf("debtcount")
	if insert < set || debt < insert || commit < debt {
		t.Fatalf("порядок нарушен: setflag=%d insert=%d debtcount=%d commit=%d (сдвиг счётчика — после INSERT, до Commit)",
			set, insert, debt, commit)
	}
	if got := pool.counters["render_pending"]; got != 1 {
		t.Fatalf("счётчик долга = %d, ожидался 1 (ровно один переход)", got)
	}
	f := pool.domains[recordDomainKey{zone: 5, id: 11}].flags
	if !f.Test(model.FlagRenderPending) || !f.Test(model.FlagIDN) {
		t.Fatalf("flags=%s, ожидался is_idn|render_pending", f)
	}
}

// TestRecorderRenderLenError — недоступный XLEN трактуется как
// переполнение: флаг спасает задачи рендера, эпизод не падает.
func TestRecorderRenderLenError(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{err: errors.New("redis: xlen")}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}
	if pool.j.count("setflag") != 1 {
		t.Fatalf("флаг при ошибке XLEN не поставлен")
	}
	if pool.j.count("publish:"+queue.StreamRender) != 0 {
		t.Fatalf("публикаций q:render при ошибке XLEN: %d", pool.j.count("publish:"+queue.StreamRender))
	}
	if !pool.domains[recordDomainKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг render_pending не установлен")
	}
}

// TestRecorderRenderNoSPA — без SPA-записей ёмкость не проверяется,
// флаг не ставится и не стирается (черновой флаг снимет проход).
func TestRecorderRenderNoSPA(t *testing.T) {
	pool := newRenderqRecPool()
	dom := pool.seedDomain(5, 11, model.StatusNew, model.FlagRenderPending) // черновой флаг
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{}
	res, _, _ := renderqResult()
	for i := range res.Subdomains {
		for j := range res.Subdomains[i].Records {
			res.Subdomains[i].Records[j].SPA = &httpfetch.SPAVerdict{SPA: false}
		}
	}

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}
	if n := ln.callsCount(); n != 0 {
		t.Fatalf("ёмкость проверялась %d раз без SPA-записей", n)
	}
	if pool.j.count("setflag") != 0 || pool.j.count("publish:"+queue.StreamRender) != 0 {
		t.Fatal("без SPA-записей не должно быть ни флага, ни q:render")
	}
	if !dom.flags.Test(model.FlagRenderPending) {
		t.Fatal("черновой флаг стёрт записью результата — этим занимается проход")
	}
}

// TestRecorderWithoutRender — NewRecorder без очереди рендера (render
// nil): поведение T-058 без изменений (обратная совместимость сборок).
func TestRecorderWithoutRender(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	q := &renderqQueue{j: pool.j}
	res, _, _ := renderqResult()

	r, err := NewRecorder(pool, q, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	ok, err := r.RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}
	if pool.j.count("publish:"+queue.StreamAnalyze) != 3 {
		t.Fatalf("q:analyze: %d, ожидалось 3", pool.j.count("publish:"+queue.StreamAnalyze))
	}
	if pool.j.count("publish:"+queue.StreamRender) != 0 || pool.j.count("setflag") != 0 {
		t.Fatal("без очереди рендера не должно быть ни q:render, ни флага")
	}
}

// TestRecorderRenderDeletedDomain — удалённому домену результат не
// пишется (§8/§13.1): ни флага, ни публикаций.
func TestRecorderRenderDeletedDomain(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusDeleted, 0)
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}} // переполнение — флаг всё равно не ставится
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, ожидалось false/nil", ok, err)
	}
	if pool.j.count("setflag") != 0 || pool.j.count("publish:"+queue.StreamAnalyze) != 0 {
		t.Fatal("удалённому домену — ни флага, ни публикаций")
	}
}

// TestRecorderRenderSetFlagFailure — сбой оператора флага откатывает
// транзакцию целиком: строк и флага нет, доставка вернётся.
func TestRecorderRenderSetFlagFailure(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	pool.failExec[renderSetPendingSQL] = errors.New("sql: флаг")
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, ожидалась ошибка", ok, err)
	}
	if c := pool.j.count("commit"); c != 0 {
		t.Fatalf("транзакция зафиксирована при сбое флага (%d)", c)
	}
	dom := pool.domains[recordDomainKey{zone: 5, id: 11}]
	if dom.flags != 0 {
		t.Fatalf("flags=%s — откат не вернул исходное состояние", dom.flags)
	}
	if len(pool.rows[recordDomainKey{zone: 5, id: 11}]) != 0 {
		t.Fatal("строки вставлены при откатившейся транзакции")
	}
}

// TestRecorderRenderFlagAlreadySet — охрана бита в SET (T-175): повторная
// доставка эпизода при уже стоящем флаге — RowsAffected 0 без ошибки
// (семантика OR идемпотентна), счётчик долга не наращивается: только
// реальные переходы 0→1.
func TestRecorderRenderFlagAlreadySet(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, model.FlagRenderPending) // бит уже стоит
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}} // переполнение — флаг ставится снова
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v (повторная установка бита — не ошибка)", ok, err)
	}
	if c := pool.j.count("setflag"); c != 0 {
		t.Fatalf("переходов setflag %d, ожидался 0 (бит уже стоял)", c)
	}
	if c := pool.j.count("debtcount"); c != 0 {
		t.Fatalf("сдвигов счётчика %d, ожидался 0 (перехода не было)", c)
	}
	if got := pool.counters["render_pending"]; got != 1 {
		t.Fatalf("счётчик = %d, ожидался 1 (только сид)", got)
	}
}

// TestRecorderRenderCounterAtomic — счётчик долга в ТОЙ ЖЕ транзакции,
// что и флаг (T-175, §3.4): сбой сдвига счётчика откатывает транзакцию
// целиком — ни строк, ни флага, ни счётчика.
func TestRecorderRenderCounterAtomic(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	pool.failExec[renderCounterDeltaSQL] = errors.New("sql: счётчик")
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, ожидалась ошибка (счётчик обязан фиксироваться с флагом)", ok, err)
	}
	if c := pool.j.count("commit"); c != 0 {
		t.Fatalf("транзакция зафиксирована при сбое счётчика (%d)", c)
	}
	dom := pool.domains[recordDomainKey{zone: 5, id: 11}]
	if dom.flags != 0 {
		t.Fatalf("flags=%s — откат не вернул бит", dom.flags)
	}
	if got := pool.counters["render_pending"]; got != 0 {
		t.Fatalf("счётчик = %d — откат не вернул счётчик", got)
	}
	if len(pool.rows[recordDomainKey{zone: 5, id: 11}]) != 0 {
		t.Fatal("строки вставлены при откатившейся транзакции")
	}
}

// TestRecorderRenderCounterMissingRow — строки 'render_pending' в
// crawler_counters нет (возможна только ручная правка, runbooks §6):
// ошибка откатывает транзакцию — молчаливый рассинхрон счётчика долга
// недопустим, gauge не должен лгать.
func TestRecorderRenderCounterMissingRow(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	pool.counterNoRow = true
	q := &renderqQueue{j: pool.j}
	ln := &renderqLen{seq: []int64{100}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, ожидалась ошибка отсутствия строки счётчика", ok, err)
	}
	if c := pool.j.count("commit"); c != 0 {
		t.Fatalf("транзакция зафиксирована без строки счётчика (%d)", c)
	}
	if pool.domains[recordDomainKey{zone: 5, id: 11}].flags != 0 {
		t.Fatal("флаг поставлен без счётчика — рассинхрон")
	}
}

// TestRecorderRenderPublishFailure — сбой публикации q:render (после
// фиксации): ошибка возвращает доставку — повторный эпизод переиграет
// решение (ёмкость проверится заново), дубликаты ограничены попытками.
func TestRecorderRenderPublishFailure(t *testing.T) {
	pool := newRenderqRecPool()
	pool.seedDomain(5, 11, model.StatusNew, 0)
	q := &renderqQueue{j: pool.j, errs: []error{nil, nil, nil, errors.New("xadd")}}
	ln := &renderqLen{seq: []int64{0}}
	res, _, _ := renderqResult()

	ok, err := renderqRecorder(t, pool, q, ln).RecordAndPublish(context.Background(), res)
	if err == nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v, ожидалась ошибка при зафиксированном результате", ok, err)
	}
	if pool.j.count("commit") != 1 {
		t.Fatal("результат обязан быть зафиксирован до публикации")
	}
	if pool.j.count("setflag") != 0 {
		t.Fatal("флаг ставится только при переполнении")
	}
}

// --- проход доработки ---------------------------------------------------------

// renderqPassKit — набор фейков прохода: пул, контент, очередь рендера.
func renderqPassKit(t *testing.T, lens ...int64) (*renderqPool, *renderqContent, *renderqQueue, *renderqLen, *RenderQueue) {
	t.Helper()
	pool := newRenderqPool()
	pool.zones[5] = "example"
	body := &renderqContent{bodies: make(map[content.Hash][]byte), errOn: make(map[content.Hash]error)}
	q := &renderqQueue{j: pool.j}
	var ln *renderqLen
	if len(lens) > 0 {
		ln = &renderqLen{seq: lens}
	} else {
		ln = &renderqLen{}
	}
	rq, err := NewRenderQueue(q, ln, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	return pool, body, q, ln, rq
}

// TestRequeueOncePublishes — проход дорабатывает: SPA-контент → ключ
// записи; алиас наследует вердикт оригинала; обычная страница и записи
// без контента не переиздаются; отрендеренные исключаются; флаг снят
// после публикации; is_idn сохраняется.
func TestRequeueOncePublishes(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hFlat := content.HashOf(renderqFlatBody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	body.bodies[hFlat] = renderqFlatBody

	pool.seedDomain(5, 11, "spa", model.FlagRenderPending|model.FlagIDN, model.StatusAlive)
	aliasTo := int16(0)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedRow(5, 11, 1, model.VariantMain, nil, &aliasTo, false, "https://spa.example/")
	pool.seedRow(5, 11, 2, model.VariantMain, hFlat[:], nil, false, "https://spa.example/")
	pool.seedRow(5, 11, 3, model.VariantMain, nil, nil, false, "https://spa.example/") // контент не сохранён
	pool.seedRow(5, 11, 4, model.VariantMain, hSPA[:], nil, true, "https://spa.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Full || res.Selected != 1 || res.Cleared != 1 || res.Published != 2 {
		t.Fatalf("итог %+v, ожидался selected=1 cleared=1 published=2 full=false", res)
	}

	pub := q.snapshot()
	if len(pub) != 2 {
		t.Fatalf("публикаций %d, ожидалось 2 (запись + алиас)", len(pub))
	}
	for i, want := range []int16{0, 1} {
		p := pub[i]
		if p.stream != queue.StreamRender {
			t.Errorf("публикация %d: стрим %s", i, p.stream)
		}
		if p.msg.SubdomainID != want || p.msg.Variant != model.VariantMain {
			t.Errorf("публикация %d: ключ (%d,%v), ожидался (%d,%v)", i, p.msg.SubdomainID, p.msg.Variant, want, model.VariantMain)
		}
		if p.msg.ZoneID != 5 || p.msg.DomainID != 11 || p.msg.Name != "spa.example" {
			t.Errorf("публикация %d: идентичность %+v", i, p.msg)
		}
	}

	dom := pool.domains[renderqDomKey{zone: 5, id: 11}]
	if dom.flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг не снят")
	}
	if !dom.flags.Test(model.FlagIDN) {
		t.Fatalf("flags=%s — is_idn потерян при снятии", dom.flags)
	}
}

// TestRequeueOnceFullAtStart — полная очередь: проход не читает БД
// (одно XLEN), флаги не трогает.
func TestRequeueOnceFullAtStart(t *testing.T) {
	pool, body, q, ln, rq := renderqPassKit(t, 100) // len == watermark
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if !res.Full || res.Selected != 0 {
		t.Fatalf("итог %+v, ожидался full=true selected=0", res)
	}
	if ln.callsCount() != 1 {
		t.Fatalf("обращений к XLEN %d, ожидалось 1", ln.callsCount())
	}
	if len(q.snapshot()) != 0 {
		t.Fatal("при полной очереди публикаций быть не должно")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят при полной очереди")
	}
}

// TestRequeueOnceFullMidPass — очередь заполнилась между доменами:
// обработанные доработаны, оставшиеся сохранили флаг.
func TestRequeueOnceFullMidPass(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, _, rq := renderqPassKit(t, 0, 0, 100) // старт свободен, после первого домена — полна
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedDomain(5, 12, "two", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 12, 0, model.VariantMain, hSPA[:], nil, false, "https://two.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if !res.Full || res.Selected != 2 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("итог %+v, ожидался full selected=2 cleared=1 published=1", res)
	}
	if n := len(q.snapshot()); n != 1 {
		t.Fatalf("публикаций %d, ожидалась 1 (только первый домен)", n)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 12}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг второго домена снят при полной очереди")
	}
}

// TestRequeueOnceNoSPA — SPA-записей не нашлось: публикаций нет, флаг
// снят (черновой флаг).
func TestRequeueOnceNoSPA(t *testing.T) {
	hFlat := content.HashOf(renderqFlatBody)
	pool, body, q, _, rq := renderqPassKit(t, 0)
	body.bodies[hFlat] = renderqFlatBody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hFlat[:], nil, false, "https://spa.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Published != 0 || res.Cleared != 1 {
		t.Fatalf("итог %+v, ожидался published=0 cleared=1", res)
	}
	if len(q.snapshot()) != 0 {
		t.Fatal("публикаций без SPA-записей быть не должно")
	}
}

// TestRequeueOnceContentError — недоступный блок: запись пропускается
// (Warn), проход продолжается без ошибки, но флаг домена СОХРАНЯЕТСЯ:
// транзиентный сбой чтения не должен терять SPA-задачу до перепроверки
// T-030 — следующий тик прохода повторит домен.
func TestRequeueOnceContentError(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.errOn[hSPA] = errors.New("pack: блок не читается")
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v (ошибка чтения — не ошибка прохода)", err)
	}
	if res.Published != 0 || res.Cleared != 0 {
		t.Fatalf("итог %+v, ожидался published=0 cleared=0 (флаг сохранён)", res)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят при нечитаемом контенте — SPA-задача потеряна до T-030")
	}
}

// TestRequeueOnceContentErrorPartial — сбой чтения одной записи не мешает
// остальным: читаемая SPA-запись публикуется, но флаг домена держится,
// пока не прочитаны все (следующий проход доберёт нечитаемую).
func TestRequeueOnceContentErrorPartial(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hDead := content.HashOf([]byte("блок, который не читается"))
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	body.errOn[hDead] = errors.New("pack: блок не читается")
	pool.seedDomain(5, 13, "mix", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 13, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedRow(5, 13, 1, model.VariantMain, hDead[:], nil, false, "https://down.example/")

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Published != 1 || res.Cleared != 0 {
		t.Fatalf("итог %+v, ожидался published=1 cleared=0 (флаг при нечитаемой записи сохранён)", res)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 13}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят, хотя одна запись не прочитана")
	}
}

// TestRequeueOnceSelection — выборка: только домены с флагом, удалённые
// исключены (§13.1), LIMIT пачки; пустой набор — пустой итог.
func TestRequeueOnceSelection(t *testing.T) {
	pool, body, _, ln, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusDeleted) // вне цикла
	pool.seedDomain(5, 12, "live", 0, model.StatusAlive)                        // без флага

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 0 || res.Cleared != 0 || res.Published != 0 || res.Full {
		t.Fatalf("итог %+v, ожидался пустой", res)
	}
	if n := ln.callsCount(); n != 1 {
		t.Fatalf("XLEN %d, ожидался 1", n)
	}
}

// TestRequeueOnceBatchLimit — LIMIT пачки без ORDER BY (T-181): выборка
// берёт первые limit строк «физического» порядка, остальные домены
// сохраняют флаг до следующего прохода (детерминизм пачек не нужен —
// снятие флага исключает повторы).
func TestRequeueOnceBatchLimit(t *testing.T) {
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "one", model.FlagRenderPending, model.StatusAlive)
	pool.seedDomain(5, 12, "two", model.FlagRenderPending, model.StatusAlive)
	pool.seedDomain(5, 13, "three", model.FlagRenderPending, model.StatusAlive)

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 2 || res.Full {
		t.Fatalf("итог %+v, ожидался selected=2 (LIMIT) full=false", res)
	}
	// Первые два по порядку вставки сняты (черновой флаг — без SPA-записей),
	// третий ждёт следующего прохода.
	for _, id := range []int64{11, 12} {
		if pool.domains[renderqDomKey{zone: 5, id: id}].flags.Test(model.FlagRenderPending) {
			t.Fatalf("домен %d не снят первым проходом", id)
		}
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 13}].flags.Test(model.FlagRenderPending) {
		t.Fatal("домен за пределами LIMIT снят — пачка больше лимита")
	}
}

// TestRequeueOnceSelectPlan — закрепление формы плана выборки (T-181):
// пачка читается в явной транзакции строго последовательностью
// SET LOCAL ×3 → SELECT → COMMIT (реплика диаграммы §2 ТЗ: без
// закрепления планировщик берёт index scan по PK — запрос №2 журнала
// медленных 24.10); выборка идёт через транзакцию, не пул.
func TestRequeueOnceSelectPlan(t *testing.T) {
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)

	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	ev := pool.j.snapshot()
	sel := pool.j.indexOf("select")
	if sel < 4 {
		t.Fatalf("select в журнале на позиции %d — нет истории шагов: %v", sel, ev)
	}
	if ev[sel-4] != "begin" || ev[sel-3] != "plan" || ev[sel-2] != "plan" || ev[sel-1] != "plan" {
		t.Fatalf("перед SELECT нет транзакции закрепления плана: %v", ev)
	}
	if len(ev) <= sel+1 || ev[sel+1] != "commit" {
		t.Fatalf("после SELECT нет фиксации: %v", ev)
	}
}

// TestRequeueRenderPendingDrainNoProgress — стартовый дренаж не крутится
// вхолостую по пачке без прогресса (T-181): контент не читается (keep),
// флаги стоят — очередная выборка вернула бы те же домены; без остановки
// цикл молотил бы БД без темпителя (дорогая выборка журнала медленных
// больше не замедляет его сама).
func TestRequeueRenderPendingDrainNoProgress(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool := newRenderqPool()
	pool.zones[5] = "example"
	body := &renderqContent{
		bodies: map[content.Hash][]byte{},
		errOn:  map[content.Hash]error{hSPA: errors.New("pack: блок не читается")},
	}
	pool.j.done = make(chan string, 64)
	q := &renderqQueue{j: pool.j}
	rq, err := NewRenderQueue(q, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RequeueRenderPending(ctx, pool, body, rq,
			RequeueOptions{Interval: time.Hour, BatchSize: 1})
	}()

	// Ждём выборки стартового прохода.
	deadline := time.After(3 * time.Second)
	for pool.j.count("select") == 0 {
		select {
		case <-deadline:
			t.Fatal("стартовый дренаж не выполнил выборку")
		case <-pool.j.done:
		case <-time.After(time.Millisecond):
		}
	}
	// Пачка полная (BatchSize = 1), флаг не снят (контент не читается):
	// дренаж обязан остановиться — следующий тик не раньше Interval.
	time.Sleep(50 * time.Millisecond)
	if c := pool.j.count("select"); c != 1 {
		t.Fatalf("выборок %d за стартовый проход, ожидалась 1 (пачка без прогресса — ждать тика)", c)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят при нечитаемом контенте")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RequeueRenderPending: %v (отмена — чистый nil)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
}

// TestRequeueOnceGuardZeroDebt — guard счётчиком долга (T-181): при
// нулевом счётчике тяжёлая выборка не выполняется вовсе — даже если
// флаги живы (рассинхрон ручного SQL: дренаж стоит до пересчёта,
// runbooks §6; честный стационар — то же поведение без чтения domains).
func TestRequeueOnceGuardZeroDebt(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, q, ln, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.counters["render_pending"] = 0 // ручной рассинхрон: флаг жив, счётчик нулевой

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Selected != 0 || res.Cleared != 0 || res.Published != 0 || res.Full {
		t.Fatalf("итог %+v, ожидался пустой (guard: долга нет)", res)
	}
	if c := pool.j.count("select"); c != 0 {
		t.Fatalf("тяжёлый SELECT выполнен %d раз при нулевом счётчике долга", c)
	}
	if c := pool.j.count("plan"); c != 0 {
		t.Fatalf("закрепление плана выполнено %d раз при нулевом счётчике долга", c)
	}
	if n := len(q.snapshot()); n != 0 {
		t.Fatalf("публикаций %d при нулевом счётчике долга", n)
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят при нулевом счётчике — guard обязан держать дренаж до пересчёта")
	}
	if n := ln.callsCount(); n != 1 {
		t.Fatalf("обращений к XLEN %d, ожидалось 1 (guard — после проверки ёмкости)", n)
	}

	// Отрицательный счётчик (невозможен кодом — только ручная правка):
	// тот же ранний возврат «долга нет» — тихий баг счётчика не должен
	// запускать дренаж по отрицательному долгу.
	pool.counters["render_pending"] = -1
	res, err = RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce (отрицательный счётчик): %v", err)
	}
	if res.Selected != 0 || res.Cleared != 0 || res.Published != 0 {
		t.Fatalf("итог %+v при отрицательном счётчике, ожидался пустой", res)
	}
	if c := pool.j.count("select"); c != 0 {
		t.Fatalf("тяжёлый SELECT выполнен %d раз при отрицательном счётчике", c)
	}
}

// TestRequeueOnceGuardCounter — guard-чтение счётчика: строка есть —
// точечный Lookup по имени; долг есть — выборка выполняется (счётчик
// согласован с числом флагов: инвариант бэкфилла миграции 000015).
func TestRequeueOnceGuardCounter(t *testing.T) {
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedDomain(5, 12, "two", model.FlagRenderPending, model.StatusAlive)
	if got := pool.counters["render_pending"]; got != 2 {
		t.Fatalf("сид счётчика = %d, ожидался 2", got)
	}

	// Два прохода: первый снимает оба флага (черновые — без SPA-записей),
	// счётчик к нулю; второй — guard: тяжёлой выборки нет.
	for i := 1; i <= 2; i++ {
		res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
		if err != nil {
			t.Fatalf("проход %d: %v", i, err)
		}
		switch i {
		case 1:
			if res.Selected != 2 || res.Cleared != 2 {
				t.Fatalf("проход 1: итог %+v, ожидался selected=2 cleared=2", res)
			}
		case 2:
			if res.Selected != 0 || res.Cleared != 0 {
				t.Fatalf("проход 2: итог %+v, ожидался пустой (долг исчерпан)", res)
			}
			if c := pool.j.count("select"); c != 1 {
				t.Fatalf("тяжёлых SELECT за два прохода %d, ожидался 1 (второй — guard)", c)
			}
		}
	}
}

// TestRequeueOnceClearRace — охрана бита при снятии: конкурент уже снял
// флаг — RowsAffected 0 без ошибки, домен не засчитан.
func TestRequeueOnceClearRace(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	// Между выборкой и снятием конкурент снял флаг.
	pool.beforeHook = func() {
		pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Clear(model.FlagRenderPending)
	}

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Cleared != 0 {
		t.Fatalf("итог %+v: снятие без строки засчитано", res)
	}
}

// TestRequeueOnceCounterInvariant — снятие флага декрементирует счётчик
// долга той же транзакцией (T-175): после прохода счётчик равен числу
// строк с битом (инвариант бэкфилла миграции 000015); домен с
// нечитаемым контентом (флаг сохранён) и удалённый (вне цикла) в
// счётчике остаются.
func TestRequeueOnceCounterInvariant(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	hDead := content.HashOf([]byte("блок, который не читается"))
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	body.errOn[hDead] = errors.New("pack: блок не читается")

	// Доработан (снятие −1), оставлен (контент не читается), удалён с
	// битом (вне цикла — бит и счётчик держат).
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.seedDomain(5, 12, "stuck", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 12, 0, model.VariantMain, hDead[:], nil, false, "https://down.example/")
	pool.seedDomain(5, 13, "gone", model.FlagRenderPending, model.StatusDeleted)
	if got := pool.counters["render_pending"]; got != 3 {
		t.Fatalf("сид счётчика = %d, ожидался 3", got)
	}

	res, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{})
	if err != nil {
		t.Fatalf("RequeueOnce: %v", err)
	}
	if res.Cleared != 1 {
		t.Fatalf("итог %+v, ожидался cleared=1", res)
	}
	// Инвариант: счётчик == число строк с битом (включая удалённую —
	// бэкфилл миграции фильтра по status не имеет).
	want := int64(0)
	for _, d := range pool.domains {
		if d.flags.Test(model.FlagRenderPending) {
			want++
		}
	}
	if got := pool.counters["render_pending"]; got != want {
		t.Fatalf("счётчик = %d, число строк с битом = %d (рассинхрон)", got, want)
	}
	if want != 2 {
		t.Fatalf("строк с битом после прохода = %d, ожидалось 2 (stuck+gone)", want)
	}
}

// TestRequeueOnceCounterFailure — сбой сдвига счётчика откатывает и
// снятие флага (одна транзакция, T-175): проход возвращает ошибку,
// домен остаётся с битом и в счётчике — следующий тик повторит пару.
func TestRequeueOnceCounterFailure(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.queryErrs[renderCounterDeltaSQL] = errors.New("sql: счётчик")

	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("сбой счётчика не возвращён ошибкой прохода")
	}
	dom := pool.domains[renderqDomKey{zone: 5, id: 11}]
	if !dom.flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят без счётчика — рассинхрон (откат обязан вернуть бит)")
	}
	if got := pool.counters["render_pending"]; got != 1 {
		t.Fatalf("счётчик = %d, ожидался 1 (откат вернул)", got)
	}
}

// TestRequeueOnceCounterMissingRow — строки счётчика нет (ручная
// правка): guard прохода (T-181) ошибается ДО выборки — молчаливый
// рассинхрон сделал бы и gauge, и сам guard ложью; флаг не снят — пара
// бит+счётчик не расходится.
func TestRequeueOnceCounterMissingRow(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool, body, _, _, rq := renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.counterNoRow = true

	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("отсутствие строки счётчика не возвращено ошибкой")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят при отсутствующей строке счётчика — рассинхрон")
	}
}

// TestRequeueOnceErrors — ошибки выборки/снятия возвращаются вызывающему
// (цикл.Warn), флаг домена остаётся.
func TestRequeueOnceErrors(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)

	// Ошибка тяжёлой выборки: долг есть (флаг + счётчик) — guard
	// пропускает к выборке, сбой SELECT возвращается вызывающему.
	pool, body, _, _, rq := renderqPassKit(t, 0)
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.queryErrs[renderPendingDomainsSQL] = errors.New("sql: select")
	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("ошибка выборки не возвращена")
	}

	// Сбой закрепления плана (SET LOCAL) — ошибка прохода, без SELECT.
	pool2, body2, _, _, rq2 := renderqPassKit(t, 0)
	pool2.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool2.queryErrs[renderPlanNoIndexScanSQL] = errors.New("sql: set local")
	if _, err := RequeueOnce(context.Background(), pool2, body2, rq2, RequeueOptions{}); err == nil {
		t.Fatal("ошибка SET LOCAL не возвращена")
	}
	if c := pool2.j.count("select"); c != 0 {
		t.Fatalf("SELECT выполнен %d раз при сбое закрепления плана", c)
	}

	// Сбой guard-чтения счётчика — ошибка прохода, без выборки.
	pool3, body3, _, _, rq3 := renderqPassKit(t, 0)
	pool3.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool3.queryErrs[renderDebtCounterSQL] = errors.New("sql: counter")
	if _, err := RequeueOnce(context.Background(), pool3, body3, rq3, RequeueOptions{}); err == nil {
		t.Fatal("ошибка чтения счётчика не возвращена")
	}
	if c := pool3.j.count("select"); c != 0 {
		t.Fatalf("SELECT выполнен %d раз при сбое guard", c)
	}

	pool, body, _, _, rq = renderqPassKit(t, 0)
	body.bodies[hSPA] = renderqSPABody
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")
	pool.queryErrs[renderClearPendingSQL] = errors.New("sql: update")
	if _, err := RequeueOnce(context.Background(), pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("ошибка снятия флага не возвращена")
	}
	if !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending) {
		t.Fatal("флаг снят при сбое оператора снятия")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RequeueOnce(ctx, pool, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("отмена ctx не возвращена")
	}
}

// TestRequeueOnceValidation — nil-зависимости — ошибка.
func TestRequeueOnceValidation(t *testing.T) {
	pool := newRenderqPool()
	body := &renderqContent{}
	rq, err := NewRenderQueue(&renderqQueue{}, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	if _, err := RequeueOnce(context.Background(), nil, body, rq, RequeueOptions{}); err == nil {
		t.Fatal("пул nil: ожидалась ошибка")
	}
	if _, err := RequeueOnce(context.Background(), pool, nil, rq, RequeueOptions{}); err == nil {
		t.Fatal("хранилище nil: ожидалась ошибка")
	}
	if _, err := RequeueOnce(context.Background(), pool, body, nil, RequeueOptions{}); err == nil {
		t.Fatal("очередь рендера nil: ожидалась ошибка")
	}
}

// TestRequeueRenderPendingLoop — цикл: стартовый дренаж сразу (флаг
// доработан до первого тика), отмена ctx — чистый nil.
func TestRequeueRenderPendingLoop(t *testing.T) {
	hSPA := content.HashOf(renderqSPABody)
	pool := newRenderqPool()
	pool.zones[5] = "example"
	body := &renderqContent{bodies: map[content.Hash][]byte{hSPA: renderqSPABody}, errOn: make(map[content.Hash]error)}
	pool.j.done = make(chan string, 16)
	q := &renderqQueue{j: pool.j}
	rq, err := NewRenderQueue(q, &renderqLen{}, 100, nil)
	if err != nil {
		t.Fatalf("NewRenderQueue: %v", err)
	}
	pool.seedDomain(5, 11, "spa", model.FlagRenderPending, model.StatusAlive)
	pool.seedRow(5, 11, 0, model.VariantMain, hSPA[:], nil, false, "https://spa.example/")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RequeueRenderPending(ctx, pool, body, rq, RequeueOptions{Interval: 5 * time.Millisecond})
	}()

	// Ждём снятия флага (стартовый дренаж — до первого тика).
	deadline := time.After(3 * time.Second)
	for {
		pool.mu.Lock()
		cleared := !pool.domains[renderqDomKey{zone: 5, id: 11}].flags.Test(model.FlagRenderPending)
		pool.mu.Unlock()
		if cleared {
			break
		}
		select {
		case <-deadline:
			t.Fatal("флаг не снят: стартовый дренаж не доработал домен")
		case <-pool.j.done:
		case <-time.After(time.Millisecond):
		}
	}
	if len(q.snapshot()) != 1 {
		t.Fatalf("публикаций %d, ожидалась 1", len(q.snapshot()))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RequeueRenderPending: %v (отмена — чистый nil)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
}

// TestRenderSQLContract — литералы схемы и структура операторов (стиль
// SQL-контракта record_test.go).
func TestRenderSQLContract(t *testing.T) {
	if !strings.Contains(renderSetPendingSQL, "SET flags = (t.flags | 2)::int2") ||
		!strings.Contains(renderSetPendingSQL, "WHERE t.zone_id = $1::int2 AND t.id = $2::int8") ||
		!strings.Contains(renderSetPendingSQL, "AND (t.flags & 2) = 0") {
		t.Fatalf("renderSetPendingSQL разошёлся с контрактом §6.1/T-008/T-175:\n%s", renderSetPendingSQL)
	}
	if !strings.Contains(renderPendingDomainsSQL, "(d.flags & 2) <> 0") ||
		!strings.Contains(renderPendingDomainsSQL, "d.status <> 6") ||
		!strings.Contains(renderPendingDomainsSQL, "LIMIT $1") ||
		strings.Contains(renderPendingDomainsSQL, "ORDER BY") {
		t.Fatalf("renderPendingDomainsSQL разошёлся с контрактом T-181 (без PK-порядка, LIMIT — пачка):\n%s", renderPendingDomainsSQL)
	}
	if renderPlanNoIndexScanSQL != "SET LOCAL enable_indexscan = off" ||
		renderPlanNoBitmapScanSQL != "SET LOCAL enable_bitmapscan = off" {
		t.Fatalf("закрепление плана выборки разошлось с контрактом T-181:\n%s\n%s",
			renderPlanNoIndexScanSQL, renderPlanNoBitmapScanSQL)
	}
	if !strings.Contains(renderDebtCounterSQL, "SELECT value FROM crawler_counters") ||
		!strings.Contains(renderDebtCounterSQL, "WHERE name = 'render_pending'") {
		t.Fatalf("renderDebtCounterSQL разошёлся с контрактом T-181/T-175:\n%s", renderDebtCounterSQL)
	}
	if !strings.Contains(renderPendingRowsSQL, "rendered = false") ||
		!strings.Contains(renderPendingRowsSQL, "final_url IS NOT NULL") ||
		!strings.Contains(renderPendingRowsSQL, "ORDER BY subdomain_id, variant") {
		t.Fatalf("renderPendingRowsSQL разошёлся с контрактом §6.1:\n%s", renderPendingRowsSQL)
	}
	if !strings.Contains(renderClearPendingSQL, "(t.flags & -3)::int2") ||
		!strings.Contains(renderClearPendingSQL, "(t.flags & 2) <> 0") {
		t.Fatalf("renderClearPendingSQL разошёлся с контрактом T-008:\n%s", renderClearPendingSQL)
	}
	if !strings.Contains(renderCounterDeltaSQL, "SET value = c.value + $1::int8") ||
		!strings.Contains(renderCounterDeltaSQL, "WHERE c.name = 'render_pending'") {
		t.Fatalf("renderCounterDeltaSQL разошёлся с контрактом T-175:\n%s", renderCounterDeltaSQL)
	}
}
