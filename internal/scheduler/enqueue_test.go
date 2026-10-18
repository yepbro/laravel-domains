// Юнит-тесты постановки пачек планировщиком (T-028, ТЗ 5.3, 4.2, 3.4) на
// фейковых pgx-пуле и очереди (стиль dispatch_test.go / retry_test.go):
// точный SQL-текст перевода и параметры, охрана pending и queued_at,
// публикация ровно переведённых строк (гонка — переведены не все),
// группировка пачки в DNSBatch (имена из выборки, порядок сохраняется),
// ошибки БД/чтения/публикации, пустая пачка, валидация, отмена ctx;
// smoke цикла EnqueueLoop (выборка → перевод → публикация → пауза,
// ожидание при пустой пачке, продолжение после ошибок). Поведение против
// живой БД/Redis — интеграционные тесты (T-032).
package scheduler

import (
	"context"
	"errors"
	"fmt"
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
)

// --- фейковые пул и очередь ----------------------------------------------
//
// Имена с префиксом enqueue: в пакете параллельно живут фейки задач
// T-027/T-030/T-031 (dispatchPool, retryFakePool), общих хелперов здесь
// не заводят.

// enqueueCallKind — шаг цикла, обслуженный Query.
type enqueueCallKind string

const (
	enqueueKindZones    enqueueCallKind = "zones"    // SELECT активных зон (T-027)
	enqueueKindDispatch enqueueCallKind = "dispatch" // SELECT очередей зон (T-027)
	enqueueKindUpdate   enqueueCallKind = "update"   // UPDATE … RETURNING (T-028)
)

// enqueueCall — записанный вызов Query.
type enqueueCall struct {
	kind enqueueCallKind
	sql  string
	args []any
}

// enqueueFakeRows — pgx.Rows с заготовленными строками; Scan позиционный
// (в порядке колонок SQL), как у retryFakeRows.
type enqueueFakeRows struct {
	rows    [][]any
	i       int
	closed  bool
	err     error // Err() после исчерпания строк
	scanErr error // ошибка Scan, если задана
}

func enqueueFakeRowsOf(rows ...[]any) *enqueueFakeRows {
	return &enqueueFakeRows{rows: rows}
}

func (r *enqueueFakeRows) Close() { r.closed = true }

func (r *enqueueFakeRows) Err() error { return r.err }

func (r *enqueueFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *enqueueFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *enqueueFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *enqueueFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("enqueueFakeRows: Scan без Next")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("enqueueFakeRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(row[i]))
	}
	return nil
}

func (r *enqueueFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *enqueueFakeRows) RawValues() [][]byte    { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая позиционным Scan.
func (r *enqueueFakeRows) Conn() *pgx.Conn      { return nil }
func (r *enqueueFakeRows) TypeMap() *pgtype.Map { return nil }

// enqueueFakePool — db.DB без БД: Query различает запросы выборки
// (справочник зон и очереди зон, dispatch.go) и перевод (UPDATE) по
// тексту, выдаёт заготовленные результаты по шагам. Очередь выборки
// исчерпана — пустой набор (pending пуст); очередь перевода исчерпана —
// переводятся все ключи пачки (нет гонки). Всё под замком: цикл
// EnqueueLoop работает в горутине.
type enqueueFakePool struct {
	mu          sync.Mutex
	zones       [][]any // ответ справочника активных зон (nil — зон нет)
	dispatches  []*enqueueFakeRows
	updates     []*enqueueFakeRows
	dispatchErr error // липовая ошибка выборки (зоны/очереди)
	updateErr   error // липовая ошибка перевода
	calls       []enqueueCall
	notify      chan struct{} // если задан — неблокирующий сигнал на каждый Query
}

func (p *enqueueFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("enqueueFakePool: CopyFrom не должен вызываться")
}

func (p *enqueueFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("enqueueFakePool: Exec не должен вызываться")
}

func (p *enqueueFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	kind := enqueueKindDispatch
	switch {
	case strings.HasPrefix(sql, "UPDATE"):
		kind = enqueueKindUpdate
	case strings.HasPrefix(sql, "SELECT id, name FROM zones"):
		kind = enqueueKindZones
	}
	p.calls = append(p.calls, enqueueCall{kind: kind, sql: sql, args: args})
	if p.notify != nil {
		select {
		case p.notify <- struct{}{}:
		default:
		}
	}

	if kind == enqueueKindZones {
		if p.dispatchErr != nil {
			return nil, p.dispatchErr
		}
		// Справочник мал и стабилен — один и тот же ответ на каждый
		// вызов (строки не расходуются, в отличие от очередей).
		return enqueueFakeRowsOf(p.zones...), nil
	}

	if kind == enqueueKindDispatch {
		if p.dispatchErr != nil {
			return nil, p.dispatchErr
		}
		if len(p.dispatches) > 0 {
			rows := p.dispatches[0]
			p.dispatches = p.dispatches[1:]
			return rows, nil
		}
		return enqueueFakeRowsOf(), nil // pending пуст
	}

	if p.updateErr != nil {
		return nil, p.updateErr
	}
	if len(p.updates) > 0 {
		rows := p.updates[0]
		p.updates = p.updates[1:]
		return rows, nil
	}
	// Дефолт: гонки нет — переведены все ключи пачки (args: []any зоны,
	// []any идентификаторы; колонки конструктора db.Update — []any).
	zones := args[0].([]any)
	ids := args[1].([]any)
	rows := make([][]any, len(zones))
	for i := range zones {
		rows[i] = []any{zones[i], ids[i]}
	}
	return enqueueFakeRowsOf(rows...), nil
}

var _ db.DB = (*enqueueFakePool)(nil)

// enqueuePoolCalls — снимок протокола вызовов.
func (p *enqueueFakePool) enqueuePoolCalls() []enqueueCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]enqueueCall, len(p.calls))
	copy(out, p.calls)
	return out
}

// enqueuePublish — записанная попытка публикации.
type enqueuePublish struct {
	stream string
	msg    queue.DNSBatch
	err    error // ошибка, возвращённая Publish (nil — успех)
}

// enqueueFakeQueue — queue.Queue для постановки: Publish проверяет
// контекст, запоминает сообщение и стрим, раздаёт заготовленные ошибки по
// одной на вызов; Consume/Ack постановке не нужны.
type enqueueFakeQueue struct {
	mu        sync.Mutex
	publishes []enqueuePublish
	errs      []error // очередь ошибок (пусто — успех)
	notify    chan struct{}
}

var _ queue.Queue = (*enqueueFakeQueue)(nil)

func (q *enqueueFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var err error
	if len(q.errs) > 0 {
		err = q.errs[0]
		q.errs = q.errs[1:]
	}
	m, _ := msg.(queue.DNSBatch)
	q.publishes = append(q.publishes, enqueuePublish{stream: stream, msg: m, err: err})
	if q.notify != nil {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("id-%d", len(q.publishes)), nil
}

func (q *enqueueFakeQueue) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	return nil, errors.New("enqueueFakeQueue: Consume не должен вызываться")
}

func (q *enqueueFakeQueue) Ack(context.Context, string, string, ...string) error {
	return errors.New("enqueueFakeQueue: Ack не должен вызываться")
}

// enqueueQueuePublishes — снимок попыток публикации.
func (q *enqueueFakeQueue) enqueueQueuePublishes() []enqueuePublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]enqueuePublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

// enqueueWaitSignal ждёт сигнала с таймаутом (событие фейка или отмена).
func enqueueWaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("не дождались события: %s", what)
	}
}

// enqueueWaitFor опрашивает условие до таймаута.
func enqueueWaitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("условие не наступило за %s: %s", timeout, what)
}

// --- EnqueueBatch ---------------------------------------------------------

// enqueueBatch3 — пачка из трёх доменов двух зон (порядок выборки —
// чередование зон T-027).
func enqueueBatch3() []DispatchRow {
	return []DispatchRow{
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 201, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 102, ZoneID: 1, Name: "c.example.com"},
	}
}

func TestEnqueueBatch(t *testing.T) {
	pool := &enqueueFakePool{} // перевод без гонки: все ключи пачки
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 3, Published: 3, Skipped: 0}) {
		t.Errorf("итог = %+v, want {Queued:3 Published:3 Skipped:0}", res)
	}

	// Один пакетный UPDATE с охраной pending и queued_at (контракт SQL —
	// TestEnqueue_SQLContract), ключи — параллельные массивы зоны/домена.
	calls := pool.enqueuePoolCalls()
	if len(calls) != 1 || calls[0].kind != enqueueKindUpdate {
		t.Fatalf("обращения к БД = %v, want единственный UPDATE", calls)
	}
	wantArgs := []any{
		[]any{int16(1), int16(2), int16(1)},
		[]any{int64(101), int64(201), int64(102)},
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("аргументы = %v, want [zone_id[], domain_id[]] = %v", calls[0].args, wantArgs)
	}

	// Публикация: одно сообщение DNSBatch в q:dns, домены — переведённые
	// строки в порядке выборки (имя из выборки: label || '.' || зона).
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 {
		t.Fatalf("публикаций %d, want 1 (пачка — одно сообщение, ТЗ 4.2)", len(pubs))
	}
	if pubs[0].stream != queue.StreamDNS {
		t.Errorf("стрим = %q, want %q", pubs[0].stream, queue.StreamDNS)
	}
	wantDomains := []queue.DNSDomain{
		{ZoneID: 1, DomainID: 101, Name: "a.example.com"},
		{ZoneID: 2, DomainID: 201, Name: "b.example.net"},
		{ZoneID: 1, DomainID: 102, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(pubs[0].msg.Domains, wantDomains) {
		t.Errorf("домены = %+v, want %+v (порядок выборки сохраняется)", pubs[0].msg.Domains, wantDomains)
	}
}

// TestEnqueueBatch_RaceNotAllTranslated — гонка: между выборкой (T-027)
// и переводом часть строк ушла из pending (возврат lease T-029, второй
// планировщик). RETURNING отдал только переведённые — публикация строится
// ровно по ним, проигравшие ждут следующего цикла (at-least-once, ТЗ 3.4).
func TestEnqueueBatch_RaceNotAllTranslated(t *testing.T) {
	// Переведены первый и третий (зона 1), второй (зона 2) — уже не pending.
	pool := &enqueueFakePool{
		updates: []*enqueueFakeRows{enqueueFakeRowsOf(
			[]any{int16(1), int64(101)},
			[]any{int16(1), int64(102)},
		)},
	}
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 2, Published: 2, Skipped: 1}) {
		t.Errorf("итог = %+v, want {Queued:2 Published:2 Skipped:1}", res)
	}

	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 {
		t.Fatalf("публикаций %d, want 1", len(pubs))
	}
	want := []queue.DNSDomain{
		{ZoneID: 1, DomainID: 101, Name: "a.example.com"},
		{ZoneID: 1, DomainID: 102, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(pubs[0].msg.Domains, want) {
		t.Errorf("домены = %+v, want %+v — непереведённая строка не публикуется", pubs[0].msg.Domains, want)
	}
}

// TestEnqueueBatch_RaceAllLost — вся пачка проиграла гонку: перевода нет,
// публикация не выполняется вовсе (пустая DNSBatch запрещена очередью).
func TestEnqueueBatch_RaceAllLost(t *testing.T) {
	pool := &enqueueFakePool{updates: []*enqueueFakeRows{enqueueFakeRowsOf()}}
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 0, Published: 0, Skipped: 3}) {
		t.Errorf("итог = %+v, want {Queued:0 Published:0 Skipped:3}", res)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("публикаций %d, want 0", len(pubs))
	}
}

// TestEnqueueBatch_DuplicateKeys — дубликат ключа в пачке публикуется
// один раз (ключ расходуется при первом совпадении).
func TestEnqueueBatch_DuplicateKeys(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}
	batch := []DispatchRow{
		{DomainID: 1, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 1, ZoneID: 1, Name: "a.example.com"},
	}

	res, err := EnqueueBatch(context.Background(), pool, q, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res != (EnqueueResult{Queued: 1, Published: 1, Skipped: 1}) {
		t.Errorf("итог = %+v, want {Queued:1 Published:1 Skipped:1}", res)
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 || len(pubs[0].msg.Domains) != 1 {
		t.Fatalf("публикации = %+v, want один домен", pubs)
	}
}

// TestEnqueueBatch_EmptyBatch — пустая пачка: ни БД, ни очереди.
func TestEnqueueBatch_EmptyBatch(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}

	res, err := EnqueueBatch(context.Background(), pool, q, nil)
	if err != nil || res != (EnqueueResult{}) {
		t.Fatalf("(%+v, %v), want ({0 0 0}, nil)", res, err)
	}
	if calls := pool.enqueuePoolCalls(); len(calls) != 0 {
		t.Errorf("обращений к БД = %d, want 0", len(calls))
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("публикаций = %d, want 0", len(pubs))
	}
}

// TestEnqueueBatch_Validation — nil пула/очереди и недопустимые строки
// отвергаются до обращения к БД: публикация q:dns отвергла бы такие
// значения, а перевод к этому моменту уже был бы зафиксирован.
func TestEnqueueBatch_Validation(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}

	if _, err := EnqueueBatch(context.Background(), nil, q, enqueueBatch3()); err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("nil пул: err = %v, want «пул не задан»", err)
	}
	if _, err := EnqueueBatch(context.Background(), pool, nil, enqueueBatch3()); err == nil || !strings.Contains(err.Error(), "очередь не задана") {
		t.Errorf("nil очередь: err = %v, want «очередь не задана»", err)
	}

	cases := []struct {
		name string
		row  DispatchRow
		want string
	}{
		{"зона 0", DispatchRow{DomainID: 1, Name: "a.example.com"}, "zone_id 0"},
		{"домен 0", DispatchRow{ZoneID: 1, Name: "a.example.com"}, "domain_id 0"},
		{"пустое имя", DispatchRow{ZoneID: 1, DomainID: 1}, "пустое имя"},
		{"длинное имя", DispatchRow{ZoneID: 1, DomainID: 1, Name: strings.Repeat("a", 254) + ".com"}, "длиннее 253"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &enqueueFakePool{}
			_, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, []DispatchRow{{ZoneID: 1, DomainID: 1, Name: "ok.example.com"}, tc.row})
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "scheduler:") {
				t.Fatalf("err = %v, want описание с %q и контекстом «scheduler:»", err, tc.want)
			}
			if calls := pool.enqueuePoolCalls(); len(calls) != 0 {
				t.Errorf("невалидная пачка дошла до БД: %v", calls)
			}
		})
	}
}

// TestEnqueueBatch_UpdateError — ошибка перевода: ничего не опубликовано,
// ошибка обёрнута контекстом задачи.
func TestEnqueueBatch_UpdateError(t *testing.T) {
	boom := errors.New("база ушла на обед")
	pool := &enqueueFakePool{updateErr: boom}
	q := &enqueueFakeQueue{}

	_, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want обёрнутая «база ушла на обед»", err)
	}
	if !strings.Contains(err.Error(), "scheduler: постановка пачки") {
		t.Errorf("сообщение = %q, want контекст «scheduler: постановка пачки»", err)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("публикаций = %d, want 0 (перевод не состоялся)", len(pubs))
	}
}

// TestEnqueueBatch_ReadErrors — ошибка чтения RETURNING (Scan и Err)
// прерывают постановку: публикация по неполному списку переведённых
// потеряла бы домены, попавшие в queued.
func TestEnqueueBatch_ReadErrors(t *testing.T) {
	boom := errors.New("декодирование сорвалось")

	pool := &enqueueFakePool{updates: []*enqueueFakeRows{
		{rows: [][]any{{int16(1), int64(101)}, {int16(1), int64(102)}}, scanErr: boom},
	}}
	_, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, enqueueBatch3()[:2])
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "чтение переведённых") {
		t.Fatalf("err = %v, want обёрнутая ошибка чтения RETURNING", err)
	}

	pool = &enqueueFakePool{updates: []*enqueueFakeRows{
		{rows: [][]any{{int16(1), int64(101)}}, err: boom},
	}}
	_, err = EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, enqueueBatch3()[:2])
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want ошибка итерации (rows.Err)", err)
	}
}

// TestEnqueueBatch_PublishError — сбой публикации: перевод уже зафиксирован
// и не откатывается (комментарий enqueue.go: XADD мог удаться, откат дал бы
// дубли в работе) — итог отдаёт Queued, строк ждут возврата по lease
// (T-029), попытка публикации одна.
func TestEnqueueBatch_PublishError(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{errs: []error{errors.New("redis лёг")}}

	res, err := EnqueueBatch(context.Background(), pool, q, enqueueBatch3())
	if err == nil || !strings.Contains(err.Error(), "q:dns") {
		t.Fatalf("err = %v, want ошибка публикации с контекстом q:dns", err)
	}
	if res.Queued != 3 || res.Published != 0 {
		t.Errorf("итог = %+v, want {Queued:3 Published:0} — перевод зафиксирован, публикации нет", res)
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 || pubs[0].err == nil {
		t.Fatalf("публикации = %+v, want одна неудачная попытка", pubs)
	}
}

// TestEnqueueBatch_ContextCanceled — отменённый контекст отказывает до
// обращений (как реальный pgx и go-redis).
func TestEnqueueBatch_ContextCanceled(t *testing.T) {
	pool := &enqueueFakePool{}
	q := &enqueueFakeQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := EnqueueBatch(ctx, pool, q, enqueueBatch3())
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls := pool.enqueuePoolCalls(); len(calls) != 0 {
		t.Errorf("обращений к БД = %d, want 0", len(calls))
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("публикаций = %d, want 0", len(pubs))
	}
}

// ------------------------------------------------------------ контракт SQL

const wantEnqueueUpdateSQL = `UPDATE "domains" AS t ` +
	`SET check_state = 1, queued_at = now() ` +
	`FROM unnest($1::int2[], $2::int8[]) AS u("zone_id", "id") ` +
	`WHERE t."zone_id" = u."zone_id" AND t."id" = u."id" AND t.check_state = 0 ` +
	`RETURNING t.zone_id, t.id`

// TestEnqueue_SQLContract — точный текст перевода: ключи по PK domains,
// check_state литералом кода queued, queued_at = now() (сервер БД, единый
// момент lease пачки), охрана pending, RETURNING ключей фактически
// переведённых строк. Литералы связаны с кодами модели и схемой миграции
// 000001 (queued_at, PK).
func TestEnqueue_SQLContract(t *testing.T) {
	pool := &enqueueFakePool{}
	if _, err := EnqueueBatch(context.Background(), pool, &enqueueFakeQueue{}, enqueueBatch3()[:1]); err != nil {
		t.Fatal(err)
	}
	calls := pool.enqueuePoolCalls()
	if len(calls) != 1 || calls[0].sql != wantEnqueueUpdateSQL {
		t.Fatalf("SQL = %q,\nwant %q", calls[0].sql, wantEnqueueUpdateSQL)
	}

	// Литералы текста обязаны следовать за кодами модели (ТЗ 6.1): при
	// изменении кодов тест ловит рассинхронизацию.
	if int16(model.CheckStateQueued) != 1 || int16(model.CheckStatePending) != 0 {
		t.Errorf("коды модели изменились (pending=%d queued=%d): обновите литералы в wantEnqueueUpdateSQL",
			int16(model.CheckStatePending), int16(model.CheckStateQueued))
	}

	up := dispatchReadMigration(t, "000001_init.up.sql")

	// queued_at — колонка lease (ТЗ 6.1); UPDATE ссылается на схему.
	if !strings.Contains(up, "queued_at") {
		t.Error("в миграции 000001 нет domains.queued_at — отметка lease ссылается на схему")
	}
	// Ключи пачки — PK таблицы: UPDATE адресует строки парой (zone_id, id).
	if !strings.Contains(up, "PRIMARY KEY (zone_id, id)") {
		t.Error("в миграции 000001 нет PK domains (zone_id, id) — ключи перевода обязаны совпадать с ним")
	}
}

// --- EnqueueLoop ------------------------------------------------------------

// enqueueLoopRun запускает цикл в горутине и возвращает канал результата.
func enqueueLoopRun(ctx context.Context, pool *enqueueFakePool, q *enqueueFakeQueue, o EnqueueOptions) <-chan error {
	done := make(chan error, 1)
	go func() { done <- EnqueueLoop(ctx, pool, q, o) }()
	return done
}

// TestEnqueueLoop — smoke сервиса на фейках: круг «выборка → перевод →
// публикация q:dns», затем пауза; отмена ctx — чистый nil (контракт OnRun,
// T-012). Паузы большого размера: после первой пачки цикл гарантированно
// ждёт — тест детерминирован.
func TestEnqueueLoop(t *testing.T) {
	pool := &enqueueFakePool{
		zones: [][]any{
			{int16(1), "example.com"},
			{int16(2), "example.net"},
		},
		dispatches: []*enqueueFakeRows{enqueueFakeRowsOf(
			[]any{int16(1), int64(101), "a"},
			[]any{int16(2), int64(201), "b"},
		)},
		notify: make(chan struct{}, 8),
	}
	q := &enqueueFakeQueue{notify: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{
		BatchSize: 1000,
		Interval:  time.Hour, // одна пачка — затем пауза до отмены
		IdleWait:  time.Hour,
	})

	enqueueWaitSignal(t, pool.notify, "выборка пачки")
	enqueueWaitSignal(t, pool.notify, "перевод пачки")
	enqueueWaitSignal(t, q.notify, "публикация q:dns")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл вернул %v, want nil (отмена ctx — чистый выход)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не остановился по отмене ctx")
	}

	calls := pool.enqueuePoolCalls()
	if len(calls) != 3 || calls[0].kind != enqueueKindZones ||
		calls[1].kind != enqueueKindDispatch || calls[2].kind != enqueueKindUpdate {
		t.Fatalf("обращения = %v, want зоны, выборка, затем перевод", calls)
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) != 1 {
		t.Fatalf("публикаций %d, want 1", len(pubs))
	}
	want := []queue.DNSDomain{
		{ZoneID: 1, DomainID: 101, Name: "a.example.com"},
		{ZoneID: 2, DomainID: 201, Name: "b.example.net"},
	}
	if !reflect.DeepEqual(pubs[0].msg.Domains, want) {
		t.Errorf("домены = %+v, want %+v", pubs[0].msg.Domains, want)
	}
}

// TestEnqueueLoop_EmptyBatchWaits — пустая выборка: публикаций нет, цикл
// ждёт пополнения (recheck T-030 / retry T-031 / lease T-029 / ingest),
// отмена прерывает ожидание.
func TestEnqueueLoop_EmptyBatchWaits(t *testing.T) {
	pool := &enqueueFakePool{notify: make(chan struct{}, 8)} // pending пуст
	q := &enqueueFakeQueue{notify: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{
		Interval: time.Hour,
		IdleWait: time.Hour,
	})

	enqueueWaitSignal(t, pool.notify, "выборка (пустая)")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл вернул %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не остановился по отмене ctx")
	}
	if calls := pool.enqueuePoolCalls(); len(calls) != 1 || calls[0].kind != enqueueKindZones {
		t.Errorf("обращения = %v, want один запрос зон без выборки очередей и перевода", calls)
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("публикаций = %d, want 0", len(pubs))
	}
}

// TestEnqueueLoop_DispatchError — ошибка выборки не останавливает цикл:
// Warn в лог, пауза, новый круг; выход — только отмена ctx.
func TestEnqueueLoop_DispatchError(t *testing.T) {
	pool := &enqueueFakePool{
		dispatchErr: errors.New("база моргнула"),
		notify:      make(chan struct{}, 8),
	}
	q := &enqueueFakeQueue{notify: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{Interval: time.Hour, IdleWait: time.Hour})
	enqueueWaitSignal(t, pool.notify, "неудачная выборка")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл вернул %v, want nil (ошибка шага не останавливает сервис)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не остановился по отмене ctx")
	}
	if pubs := q.enqueueQueuePublishes(); len(pubs) != 0 {
		t.Errorf("публикаций = %d, want 0", len(pubs))
	}
}

// TestEnqueueLoop_PublishErrorContinues — сбой публикации: переведённые
// строки остаются в queued (вернёт lease, T-029), цикл после паузы
// продолжает работу и публикует следующие пачки.
func TestEnqueueLoop_PublishErrorContinues(t *testing.T) {
	// Фейки хранят курсор чтения — каждой выборке свой экземпляр строк
	// (колонки очередей зон: zone_id, id, label — имя собирает выборка).
	batch := func() *enqueueFakeRows {
		return enqueueFakeRowsOf(
			[]any{int16(1), int64(101), "a"},
			[]any{int16(2), int64(201), "b"},
		)
	}
	pool := &enqueueFakePool{
		zones: [][]any{
			{int16(1), "example.com"},
			{int16(2), "example.net"},
		},
		dispatches: []*enqueueFakeRows{batch(), batch()},
		notify:     make(chan struct{}, 64),
	}
	q := &enqueueFakeQueue{
		errs:   []error{errors.New("redis моргнул")},
		notify: make(chan struct{}, 64),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := enqueueLoopRun(ctx, pool, q, EnqueueOptions{
		Interval: time.Millisecond,
		IdleWait: time.Millisecond,
	})

	// Первая пачка — неудачная публикация, вторая — успешная: цикл жив.
	enqueueWaitFor(t, 5*time.Second, func() bool {
		return len(q.enqueueQueuePublishes()) >= 2
	}, "две попытки публикации (после ошибки цикл продолжил)")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл вернул %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не остановился по отмене ctx")
	}
	pubs := q.enqueueQueuePublishes()
	if len(pubs) < 2 || pubs[0].err == nil || pubs[1].err != nil {
		t.Fatalf("публикации = %+v, want первая с ошибкой, вторая успешная", pubs)
	}
}

// TestEnqueueLoop_Validation — nil пула/очереди: ошибка до старта цикла
// (сборка сервиса падает внятно, а не молча).
func TestEnqueueLoop_Validation(t *testing.T) {
	if err := EnqueueLoop(context.Background(), nil, &enqueueFakeQueue{}, EnqueueOptions{}); err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("nil пул: err = %v, want «пул не задан»", err)
	}
	if err := EnqueueLoop(context.Background(), &enqueueFakePool{}, nil, EnqueueOptions{}); err == nil || !strings.Contains(err.Error(), "очередь не задана") {
		t.Errorf("nil очередь: err = %v, want «очередь не задана»", err)
	}
}
