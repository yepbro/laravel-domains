// Юнит-тесты render-worker (T-067; ТЗ 5.6, §5.7, §7, 3.4) на фейковых
// пуле, хранилище, рендере и очереди — без БД, Redis, Chromium и сети.
// Фейковый пул живёт семантикой subdomain_results (SELECT цели, UPDATE с
// оптимистической охраной content_hash) и contents (refcount для
// SwapRefTx, T-044); фейковое хранилище связано с пулом таблицей contents
// (Put +1, Delete −1) — баланс ссылок проверяется сквозняком. Проверяются
// точные SQL и параметры UPDATE, порядок «рендер → Put → транзакция →
// публикация q:analyze → XACK», идемпотентность повторной доставки,
// гонки (строка ушла из-под задачи), сбои шагов и отмена ctx; smoke —
// цикл Run. Живой конвейер рендера (Chromium + БД + Redis) —
// интеграционные тесты поздних задач.
package renderworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/content"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// --- протокол событий (порядок «рендер → Put → транзакция → публикация») -----

// rwEventKind — тип события протокола; порядок проверяется индексами.
type rwEventKind string

const (
	rwEvRender  rwEventKind = "render"
	rwEvPut     rwEventKind = "put"
	rwEvDelete  rwEventKind = "delete"
	rwEvBegin   rwEventKind = "begin"
	rwEvUpdate  rwEventKind = "update"
	rwEvSwap    rwEventKind = "swapref"
	rwEvCommit  rwEventKind = "commit"
	rwEvPublish rwEventKind = "publish"
	rwEvAck     rwEventKind = "ack"
)

// rwJournal — потокобезопасный протокол (цикл Run работает в горутине).
type rwJournal struct {
	mu     sync.Mutex
	events []rwEventKind

	// ackNotify — неблокирующий сигнал на каждое Ack (синхронизация smoke).
	ackNotify chan struct{}
}

func (j *rwJournal) record(k rwEventKind) {
	j.mu.Lock()
	j.events = append(j.events, k)
	j.mu.Unlock()
}

func (j *rwJournal) snapshot() []rwEventKind {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]rwEventKind, len(j.events))
	copy(out, j.events)
	return out
}

func (j *rwJournal) indexOf(k rwEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e == k {
			return i
		}
	}
	return -1
}

func (j *rwJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- фейковый pgx-пул: subdomain_results + contents --------------------------

// rwRowKey — PK subdomain_results в фейке.
type rwRowKey struct {
	zone      int16
	domain    int64
	subdomain int16
	variant   int16
}

// rwRow — строка subdomain_results, которую читает renderSelectSQL и
// меняет renderUpdateSQL.
type rwRow struct {
	finalURL *string
	hash     []byte // nil — NULL (контента нет, T-056)
	rendered bool
	cookies  *string
	headers  *string
}

// rwFakePool — RenderDB без БД: живая семантика subdomain_results
// (SELECT по PK, UPDATE с охраной content_hash IS NOT DISTINCT FROM) и
// contents (refcount с охраной > 0 — sqlMetaDelRef из SwapRefTx, T-044).
// Транзакция работает на снапшоте: Rollback восстанавливает состояние,
// Commit оставляет (конкурентные транзакции не моделируются — их
// сериализует PostgreSQL).
type rwFakePool struct {
	j *rwJournal

	rows map[rwRowKey]*rwRow
	refc map[content.Hash]int32

	queryErr  error
	updateErr error
	commitErr error
	beginErr  error

	// mutateBeforeUpdate — хук: вызывается перед UPDATE внутри транзакции
	// (симуляция перезаписи строки перепроверкой между SELECT и UPDATE).
	mutateBeforeUpdate func(p *rwFakePool)

	mu sync.Mutex
}

func newRWFakePool() *rwFakePool {
	return &rwFakePool{
		j:    &rwJournal{},
		rows: make(map[rwRowKey]*rwRow),
		refc: make(map[content.Hash]int32),
	}
}

var _ RenderDB = (*rwFakePool)(nil)

func (p *rwFakePool) seedRow(key rwRowKey, finalURL string, hash *content.Hash) {
	r := &rwRow{rendered: false}
	if finalURL != "" {
		r.finalURL = &finalURL
	}
	if hash != nil {
		r.hash = hash[:]
		p.refc[*hash]++
	}
	p.rows[key] = r
}

func (p *rwFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("rwFakePool: CopyFrom не должен вызываться")
}

func (p *rwFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("rwFakePool: Exec вне транзакции")
}

// rwFakeRows — pgx.Rows с заготовленными строками SELECT (final_url,
// content_hash).
type rwFakeRows struct {
	rows [][]any
	i    int
}

func (r *rwFakeRows) Close()                                       {}
func (r *rwFakeRows) Err() error                                   { return nil }
func (r *rwFakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *rwFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *rwFakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *rwFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) || len(dest) != 2 {
		return fmt.Errorf("rwFakeRows: Scan без Next или приёмников %d (нужно 2)", len(dest))
	}
	row := r.rows[r.i-1]
	// final_url: NULL → nil-приёмник остаётся нетронутым (pgx semantics:
	// *string остаётся nil).
	if row[0] != nil {
		if d, ok := dest[0].(**string); ok {
			*d = row[0].(*string)
		} else {
			return fmt.Errorf("rwFakeRows: приёмник[0] %T, ожидается **string", dest[0])
		}
	}
	if row[1] != nil {
		if d, ok := dest[1].(*[]byte); ok {
			*d = row[1].([]byte)
		} else {
			return fmt.Errorf("rwFakeRows: приёмник[1] %T, ожидается *[]byte", dest[1])
		}
	}
	return nil
}

func (r *rwFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *rwFakeRows) RawValues() [][]byte    { return nil }
func (r *rwFakeRows) Conn() *pgx.Conn        { return nil }
func (r *rwFakeRows) TypeMap() *pgtype.Map   { return nil }

func (p *rwFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sql != renderSelectSQL {
		return nil, fmt.Errorf("rwFakePool: неожидаемый SQL: %s", sql)
	}
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	key := rwRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
	r, ok := p.rows[key]
	if !ok {
		return &rwFakeRows{}, nil
	}
	var hash any
	if r.hash != nil {
		h := make([]byte, len(r.hash))
		copy(h, r.hash)
		hash = h
	}
	return &rwFakeRows{rows: [][]any{{r.finalURL, hash}}}, nil
}

func (p *rwFakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	// Хук «перепроверка перезаписала строку» выполняется до снапшота:
	// чужая транзакция уже зафиксирована — наш Rollback её не откатывает.
	if p.mutateBeforeUpdate != nil {
		p.mutateBeforeUpdate(p)
	}
	snapRows := make(map[rwRowKey]*rwRow, len(p.rows))
	for k, v := range p.rows {
		c := *v
		snapRows[k] = &c
	}
	snapRefc := make(map[content.Hash]int32, len(p.refc))
	for k, v := range p.refc {
		snapRefc[k] = v
	}
	p.j.record(rwEvBegin)
	return &rwFakeTx{pool: p, snapRows: snapRows, snapRefc: snapRefc}, nil
}

// rwDelRefSQLPrefix — префикс sqlMetaDelRef из content/pack.go (T-042):
// константа не экспортирована, точный текст поддерживается вручную;
// расхождение ломает тест громко — синхронизируйте с pack.go.
const rwDelRefSQLPrefix = "UPDATE contents SET refcount = refcount - 1"

type rwFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx этап не использует

	pool     *rwFakePool
	snapRows map[rwRowKey]*rwRow
	snapRefc map[content.Hash]int32
	commit   bool
}

func (t *rwFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch {
	case sql == renderUpdateSQL:
		if t.pool.updateErr != nil {
			return pgconn.CommandTag{}, t.pool.updateErr
		}
		key := rwRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
		finalURL := args[4].(string)
		newHash := args[5].([]byte)
		cookies := args[6].(*string)
		headers := args[7].(*string)
		oldHash := args[8].([]byte)
		r, ok := t.pool.rows[key]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		// Охрана content_hash IS NOT DISTINCT FROM $9.
		same := (r.hash == nil && oldHash == nil) ||
			(r.hash != nil && oldHash != nil && string(r.hash) == string(oldHash))
		if !same {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		r.rendered = true
		r.finalURL = &finalURL
		h := make([]byte, len(newHash))
		copy(h, newHash)
		r.hash = h
		r.cookies, r.headers = cookies, headers
		t.pool.j.record(rwEvUpdate)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case strings.HasPrefix(sql, rwDelRefSQLPrefix):
		// sqlMetaDelRef (SwapRefTx, T-044): −1 с охраной refcount > 0.
		var h content.Hash
		copy(h[:], args[0].([]byte))
		if t.pool.refc[h] <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil // ErrRefIntegrity у вызывающего
		}
		t.pool.refc[h]--
		t.pool.j.record(rwEvSwap)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("rwFakePool: неожидаемый SQL: %s", sql)
	}
}

func (t *rwFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.commit = true
	t.pool.j.record(rwEvCommit)
	return nil
}

func (t *rwFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.commit {
		return nil // откат после Commit — no-op
	}
	t.pool.rows, t.pool.refc = t.snapRows, t.snapRefc
	return nil
}

// --- фейковое хранилище контента (связано с contents пула) -------------------

// rwFakeStore — ContentStore: Put и Delete живут общей с пулом таблицей
// contents (refcount), моделируя PackStore над той же БД (T-042).
type rwFakeStore struct {
	j *rwJournal
	p *rwFakePool

	putErr    error
	deleteErr error

	mu      sync.Mutex
	puts    [][]byte
	deletes []content.Hash
}

var _ ContentStore = (*rwFakeStore)(nil)

func (s *rwFakeStore) Put(ctx context.Context, data []byte) (content.PutResult, error) {
	if err := ctx.Err(); err != nil {
		return content.PutResult{}, err
	}
	h := content.HashOf(data)
	s.mu.Lock()
	s.puts = append(s.puts, append([]byte(nil), data...))
	s.mu.Unlock()
	s.p.mu.Lock()
	created := s.p.refc[h] == 0
	s.p.refc[h]++
	s.p.mu.Unlock()
	s.j.record(rwEvPut)
	if s.putErr != nil {
		return content.PutResult{}, s.putErr
	}
	return content.PutResult{Hash: h, Created: created}, nil
}

func (s *rwFakeStore) Delete(ctx context.Context, h content.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.deletes = append(s.deletes, h)
	s.mu.Unlock()
	s.p.mu.Lock()
	n := s.p.refc[h]
	if n > 0 {
		s.p.refc[h]--
	}
	s.p.mu.Unlock()
	s.j.record(rwEvDelete)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if n == 0 {
		return content.ErrNotFound
	}
	return nil
}

func (s *rwFakeStore) putsSnapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.puts))
	copy(out, s.puts)
	return out
}

func (s *rwFakeStore) deletesSnapshot() []content.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]content.Hash, len(s.deletes))
	copy(out, s.deletes)
	return out
}

// --- фейковый рендер ---------------------------------------------------------

// rwFakeRenderer — Renderer: возвращает заготовленный вывод по URL.
type rwFakeRenderer struct {
	j *rwJournal

	output  render.RenderOutput
	err     error
	panicOn string // URL задач, на которых Run паникует (тест R-04)

	mu    sync.Mutex
	tasks []render.RenderTask
}

var _ Renderer = (*rwFakeRenderer)(nil)

func (r *rwFakeRenderer) Run(ctx context.Context, task render.RenderTask) (render.RenderOutput, error) {
	r.mu.Lock()
	r.tasks = append(r.tasks, task)
	r.mu.Unlock()
	r.j.record(rwEvRender)
	if err := ctx.Err(); err != nil {
		return render.RenderOutput{}, err
	}
	if r.panicOn != "" && task.URL == r.panicOn {
		panic("фейк рендера: паника в задаче " + task.URL)
	}
	if r.err != nil {
		return render.RenderOutput{}, r.err
	}
	return r.output, nil
}

func (r *rwFakeRenderer) tasksSnapshot() []render.RenderTask {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]render.RenderTask, len(r.tasks))
	copy(out, r.tasks)
	return out
}

// --- фейковая очередь --------------------------------------------------------

// rwPublishRec — записанная публикация.
type rwPublishRec struct {
	stream string
	msg    queue.Result
}

// rwFakeQueue — queue.Queue: Publish фиксирует Result-сообщения (и может
// падать по счётчику), Consume раздаёт заготовленные доставки один раз и
// дальше паркуется до отмены ctx, Ack фиксируется и сигнализирует.
type rwFakeQueue struct {
	j *rwJournal

	deliveries []queue.Delivery

	publishErrs []error

	mu        sync.Mutex
	publishes []rwPublishRec
	acked     []string
	consumes  int
}

var _ queue.Queue = (*rwFakeQueue)(nil)

func (q *rwFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("rwFakeQueue: неожидаемое сообщение %T (нужен queue.Result)", msg)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, rwPublishRec{stream: stream, msg: res})
	q.mu.Unlock()
	q.j.record(rwEvPublish)
	if i < len(q.publishErrs) && q.publishErrs[i] != nil {
		return "", q.publishErrs[i]
	}
	return fmt.Sprintf("entry-analyze-%d", i+1), nil
}

func (q *rwFakeQueue) Consume(ctx context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	q.consumes++
	var out []queue.Delivery
	if n > 0 && len(q.deliveries) > 0 {
		take := n
		if take > len(q.deliveries) {
			take = len(q.deliveries)
		}
		out = append(out, q.deliveries[:take]...)
		q.deliveries = q.deliveries[take:]
	}
	q.mu.Unlock()
	if len(out) > 0 {
		return out, nil
	}
	<-ctx.Done() // реальная очередь блокируется в XREADGROUP
	return nil, ctx.Err()
}

func (q *rwFakeQueue) Ack(ctx context.Context, _, _ string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, ids...)
	q.mu.Unlock()
	for range ids {
		q.j.record(rwEvAck)
	}
	q.j.signalAck()
	return nil
}

func (q *rwFakeQueue) publishesSnapshot() []rwPublishRec {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]rwPublishRec, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *rwFakeQueue) ackedSnapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, len(q.acked))
	copy(out, q.acked)
	return out
}

func (q *rwFakeQueue) consumesCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// --- сборщики ----------------------------------------------------------------

// rwTestHash — различимые хэши: rwTestHash(1), (2), …
func rwTestHash(n byte) content.Hash {
	var h content.Hash
	h[0] = n
	return h
}

// rwFixture — собранный набор фейков и воркер.
type rwFixture struct {
	pool     *rwFakePool
	store    *rwFakeStore
	renderer *rwFakeRenderer
	q        *rwFakeQueue
	w        *Worker
}

func newRWFixture(t *testing.T) *rwFixture {
	t.Helper()
	pool := newRWFakePool()
	store := &rwFakeStore{j: pool.j, p: pool}
	renderer := &rwFakeRenderer{j: pool.j}
	q := &rwFakeQueue{j: pool.j}
	w, err := New(pool, q, store, renderer, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &rwFixture{pool: pool, store: store, renderer: renderer, q: q, w: w}
}

// rwKey — ключ строки фикстуры.
var rwKey = rwRowKey{zone: 3, domain: 42, subdomain: 1, variant: 0}

// rwTask — задача q:render на ключ фикстуры.
var rwTask = queue.Result{
	ZoneID: rwKey.zone, DomainID: rwKey.domain,
	SubdomainID: rwKey.subdomain, Variant: 0, Name: "spa.example",
}

// rwDelivery — валидная доставка с Result-сообщением.
func rwDelivery(id string) queue.Delivery {
	payload, err := json.Marshal(rwTask)
	if err != nil {
		panic(err)
	}
	return queue.Delivery{Stream: queue.StreamRender, ID: id, Payload: payload}
}

// rwOutput — типовой результат рендера.
func rwOutput(dom string) render.RenderOutput {
	return render.RenderOutput{
		FinalURL: "https://spa.example/home",
		Title:    "SPA",
		DOM:      dom,
		Cookies: []render.ExtractCookie{
			{Name: "js_session", Value: "v1", Path: "/", HTTPOnly: true},
		},
		ResponseHeaders: http.Header{"X-Rendered": []string{"yes"}},
	}
}

// rwRefc — снапшот refcount пула.
func rwRefc(p *rwFakePool) map[content.Hash]int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[content.Hash]int32, len(p.refc))
	for k, v := range p.refc {
		out[k] = v
	}
	return out
}

// rwRowSnapshot — снапшот строки.
func rwRowSnapshot(p *rwFakePool, key rwRowKey) rwRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.rows[key]; ok {
		return *r
	}
	return rwRow{}
}

// --- ProcessResult -----------------------------------------------------------

// TestRenderWorker_HappyPath — сквозной контракт: SELECT цели → рендер
// итогового URL строки → Put DOM → транзакция (UPDATE + SwapRef старой
// ссылки) → Commit → публикация q:analyze тем же ключом; баланс refcount:
// старый контент −1, DOM +1.
func TestRenderWorker_HappyPath(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	dom := "<html><body>rendered</body></html>"
	f.renderer.output = rwOutput(dom)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !written {
		t.Fatal("строка должна быть записана (written=true)")
	}

	// Рендер получил итоговый URL строки.
	tasks := f.renderer.tasksSnapshot()
	if len(tasks) != 1 || tasks[0].URL != "https://spa.example/" {
		t.Fatalf("рендер: задачи %+v, нужен один URL https://spa.example/", tasks)
	}

	// Строка: rendered=true, DOM-хэш, итоговый URL и данные рендера.
	domHash := content.HashOf([]byte(dom))
	row := rwRowSnapshot(f.pool, rwKey)
	if !row.rendered {
		t.Error("rendered должен быть true (ТЗ 5.6)")
	}
	if row.finalURL == nil || *row.finalURL != "https://spa.example/home" {
		t.Errorf("final_url = %v, нужен итоговый URL рендера (JS-редиректы)", row.finalURL)
	}
	if row.hash == nil || string(row.hash) != string(domHash[:]) {
		t.Errorf("content_hash = %v, нужен хэш DOM %s", row.hash, domHash)
	}
	if row.cookies == nil || !strings.Contains(*row.cookies, "js_session") {
		t.Errorf("cookies = %v, нужен набор рендера (все cookies, ТЗ 5.6)", row.cookies)
	}
	if row.headers == nil || !strings.Contains(*row.headers, "X-Rendered") {
		t.Errorf("headers = %v, нужны заголовки итоговой навигации (ТЗ 5.8)", row.headers)
	}

	// Баланс refcount: сырой HTML −1 (был 1 → 0), DOM +1.
	refc := rwRefc(f.pool)
	if refc[raw] != 0 {
		t.Errorf("refcount сырого HTML = %d, нужен 0 (ссылка снята, ТЗ 5.7)", refc[raw])
	}
	if refc[domHash] != 1 {
		t.Errorf("refcount DOM = %d, нужен 1", refc[domHash])
	}

	// Публикация q:analyze — тем же ключом, после Commit.
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze", pubs)
	}
	if pubs[0].msg.ZoneID != rwTask.ZoneID || pubs[0].msg.DomainID != rwTask.DomainID ||
		pubs[0].msg.SubdomainID != rwTask.SubdomainID || pubs[0].msg.Variant != rwTask.Variant {
		t.Errorf("q:analyze ключ %+v не совпадает с задачей", pubs[0].msg)
	}
	if i, j := f.pool.j.indexOf(rwEvCommit), f.pool.j.indexOf(rwEvPublish); i < 0 || j < 0 || i > j {
		t.Errorf("публикация до Commit: commit=%d publish=%d", i, j)
	}
	// Порядок: рендер → Put → транзакция.
	ev := f.pool.j.snapshot()
	wantOrder := []rwEventKind{rwEvRender, rwEvPut, rwEvBegin, rwEvUpdate, rwEvSwap, rwEvCommit, rwEvPublish}
	last := -1
	for _, w := range wantOrder {
		i := f.pool.j.indexOf(w)
		if i < 0 {
			t.Fatalf("событие %s не наступило: %v", w, ev)
		}
		if i < last {
			t.Fatalf("нарушен порядок %v: %s на позиции %d после %d", ev, w, i, last)
		}
		last = i
	}
}

// TestRenderWorker_SQLContract — точный текст SELECT и UPDATE с
// литералами ТЗ (rendered = true, rules_version = NULL).
func TestRenderWorker_SQLContract(t *testing.T) {
	selectWant := "SELECT final_url, content_hash FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"
	if renderSelectSQL != selectWant {
		t.Errorf("renderSelectSQL =\n%s\nхочу\n%s", renderSelectSQL, selectWant)
	}
	updateWant := "UPDATE subdomain_results AS r " +
		"SET rendered = true, final_url = $5::text, content_hash = $6::bytea, " +
		"cookies = $7::jsonb, headers = $8::jsonb, rules_version = NULL " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.content_hash IS NOT DISTINCT FROM $9::bytea"
	if renderUpdateSQL != updateWant {
		t.Errorf("renderUpdateSQL =\n%s\nхочу\n%s", renderUpdateSQL, updateWant)
	}
}

// TestRenderWorker_NoRow — строки нет: успех без рендера, Put и публикации.
func TestRenderWorker_NoRow(t *testing.T) {
	f := newRWFixture(t)
	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v (строки нет — не ошибка)", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("ренер не должен запускаться: %+v", tasks)
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не должен вызываться: %d", len(puts))
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_NoFinalURL — итогового URL нет (NULL): рендерить
// нечего, успех без записи.
func TestRenderWorker_NoFinalURL(t *testing.T) {
	f := newRWFixture(t)
	f.pool.seedRow(rwKey, "", nil) // final_url NULL, контента нет
	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("рендер не должен запускаться: %+v", tasks)
	}
}

// TestRenderWorker_SameContent — DOM побайтово равен сохранённому
// контенту: Put и своп не выполняются, строка получает rendered=true.
func TestRenderWorker_SameContent(t *testing.T) {
	f := newRWFixture(t)
	dom := "<html><body>stable</body></html>"
	domHash := content.HashOf([]byte(dom))
	f.pool.seedRow(rwKey, "https://spa.example/", &domHash)
	f.renderer.output = rwOutput(dom)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !written {
		t.Fatal("written должен быть true")
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не нужен при том же контенте: %d вызовов", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvSwap); i >= 0 {
		t.Errorf("своп ссылок не нужен при том же контенте (событие на %d)", i)
	}
	refc := rwRefc(f.pool)
	if refc[domHash] != 1 {
		t.Errorf("refcount = %d, нужен 1 (без лишних +1)", refc[domHash])
	}
	if !rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("rendered должен быть true")
	}
}

// TestRenderWorker_AliasRow — алиасная строка (content_hash NULL, контент
// у оригинала, T-055): рендер по своему URL, DOM — своя ссылка, снимать
// нечего.
func TestRenderWorker_AliasRow(t *testing.T) {
	f := newRWFixture(t)
	f.pool.seedRow(rwKey, "https://spa.example/", nil) // hash NULL
	dom := "<html>alias dom</html>"
	f.renderer.output = rwOutput(dom)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !written {
		t.Fatal("written должен быть true")
	}
	if i := f.pool.j.indexOf(rwEvSwap); i >= 0 {
		t.Errorf("своп при NULL старой ссылки не нужен (событие на %d)", i)
	}
	refc := rwRefc(f.pool)
	domHash := content.HashOf([]byte(dom))
	if refc[domHash] != 1 {
		t.Errorf("refcount DOM = %d, нужен 1", refc[domHash])
	}
}

// TestRenderWorker_StaleRow — строка перезаписана между SELECT и UPDATE
// (перепроверка домена): охрана отсекает запись, ссылка Put снимается,
// публикаций нет, доставка подтверждается (false, nil).
func TestRenderWorker_StaleRow(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	dom := "<html>fresh dom</html>"
	f.renderer.output = rwOutput(dom)
	// Перепроверка T-058 переписала строку до нашей транзакции: старые
	// ссылки сняты её DELETE, новая строка ссылается на свежий контент.
	fresh := rwTestHash(2)
	f.pool.mutateBeforeUpdate = func(p *rwFakePool) {
		r := p.rows[rwKey]
		p.refc[raw]-- // DELETE старых строк эпизода (SwapRef, T-058)
		r.hash = fresh[:]
		p.refc[fresh]++ // INSERT новых строк
	}

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v (строка ушла из-под задачи — не ошибка)", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	// Ссылка Put снята: DOM не висит, чужой контент цел.
	refc := rwRefc(f.pool)
	domHash := content.HashOf([]byte(dom))
	if refc[domHash] != 0 {
		t.Errorf("refcount DOM = %d, нужен 0 (ссылка Put снята Delete)", refc[domHash])
	}
	if refc[fresh] != 1 {
		t.Errorf("refcount чужого контента = %d, нужен 1", refc[fresh])
	}
	if refc[raw] != 0 {
		t.Errorf("refcount старого контента = %d, нужен 0 (снят эпизодом перепроверки)", refc[raw])
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
	if i := f.pool.j.indexOf(rwEvCommit); i >= 0 {
		t.Errorf("Commit не должен выполняться (событие на %d)", i)
	}
	if deletes := f.store.deletesSnapshot(); len(deletes) != 1 || deletes[0] != domHash {
		t.Errorf("Delete: %+v, нужен один %s", deletes, domHash)
	}
}

// TestRenderWorker_RepeatDelivery — идемпотентность повторной доставки:
// повторный рендер перезаписывает контент и refcount, баланс сходится
// без утечек (ТЗ 3.4, стиль T-058).
func TestRenderWorker_RepeatDelivery(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	dom1 := "<html>render 1</html>"
	f.renderer.output = rwOutput(dom1)
	if _, err := f.w.ProcessResult(context.Background(), rwTask); err != nil {
		t.Fatalf("первая обработка: %v", err)
	}
	dom2 := "<html>render 2</html>"
	f.renderer.output = rwOutput(dom2)
	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err != nil {
		t.Fatalf("повторная обработка: %v", err)
	}
	if !written {
		t.Fatal("повторная обработка должна записать строку")
	}

	refc := rwRefc(f.pool)
	h1, h2 := content.HashOf([]byte(dom1)), content.HashOf([]byte(dom2))
	if refc[raw] != 0 {
		t.Errorf("refcount сырого HTML = %d, нужен 0", refc[raw])
	}
	if refc[h1] != 0 {
		t.Errorf("refcount DOM первой доставки = %d, нужен 0 (снят при перезаписи)", refc[h1])
	}
	if refc[h2] != 1 {
		t.Errorf("refcount DOM второй доставки = %d, нужен 1", refc[h2])
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 2 {
		t.Errorf("публикаций q:analyze = %d, нужно 2 (после каждой записи)", len(pubs))
	}
	if !rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("rendered должен быть true")
	}
}

// TestRenderWorker_RenderError — ошибка рендера: ничего не записано,
// ошибка наружу (доставка без XACK вернётся повторной доставкой).
func TestRenderWorker_RenderError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = &render.Error{Class: render.ClassTask, URL: "https://spa.example/", Err: errors.New("таймаут")}

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка рендера обязана возвращаться")
	}
	if written {
		t.Error("written должен быть false")
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не должен вызываться: %d", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("транзакция не должна открываться (событие на %d)", i)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_EmptyOutput — пустой DOM или итоговый URL —
// страховка контракта T-064: строка не трогается, ошибка задачи.
func TestRenderWorker_EmptyOutput(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = render.RenderOutput{FinalURL: "https://spa.example/x"} // DOM пуст

	if _, err := f.w.ProcessResult(context.Background(), rwTask); err == nil {
		t.Fatal("пустой DOM — ошибка")
	}
	f.renderer.output = render.RenderOutput{DOM: "<html/>"} // FinalURL пуст
	if _, err := f.w.ProcessResult(context.Background(), rwTask); err == nil {
		t.Fatal("пустой итоговый URL — ошибка")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("rendered должен остаться false")
	}
}

// TestRenderWorker_PutError — сбой Put (ErrTooLarge): ошибка без
// транзакции; повторная доставка разыграет сценарий заново, постоянные
// ошибки исчерпают попытки и уйдут в q:dead (T-025).
func TestRenderWorker_PutError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>big dom</html>")
	f.store.putErr = fmt.Errorf("%w: DOM больше лимита", content.ErrTooLarge)

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil || !errors.Is(err, content.ErrTooLarge) {
		t.Fatalf("Put ErrTooLarge обязан возвращаться: %v", err)
	}
	if written {
		t.Error("written должен быть false")
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("транзакция не должна открываться (событие на %d)", i)
	}
}

// TestRenderWorker_UpdateError — сбой UPDATE: откат транзакции, ссылка
// Put остаётся утечкой в безопасную сторону (T-044).
func TestRenderWorker_UpdateError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	dom := "<html>dom</html>"
	f.renderer.output = rwOutput(dom)
	f.pool.updateErr = errors.New("нет соединения")

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка UPDATE обязана возвращаться")
	}
	if written {
		t.Error("written должен быть false")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка не изменилась (откат)")
	}
	refc := rwRefc(f.pool)
	domHash := content.HashOf([]byte(dom))
	if refc[domHash] != 1 || refc[raw] != 1 {
		t.Errorf("refcount после отката: dom=%d raw=%d, хочу 1/1 (Put не откатывается — безопасная утечка)",
			refc[domHash], refc[raw])
	}
}

// TestRenderWorker_SwapIntegrity — расхождение учёта (у старого контента
// нет живой ссылки): ErrRefIntegrity с откатом (T-044).
func TestRenderWorker_SwapIntegrity(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.pool.refc[raw] = 0 // ссылка мертва — учёт разошёлся
	f.renderer.output = rwOutput("<html>dom</html>")

	_, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil || !errors.Is(err, content.ErrRefIntegrity) {
		t.Fatalf("ожидался ErrRefIntegrity: %v", err)
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка не изменилась (откат ErrRefIntegrity)")
	}
}

// TestRenderWorker_CommitError — сбой фиксации: ошибка, строка не
// изменилась, ссылка Put — утечкой в безопасную сторону.
func TestRenderWorker_CommitError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>dom</html>")
	f.pool.commitErr = errors.New("сервер закрыл соединение")

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка Commit обязана возвращаться")
	}
	if written {
		t.Error("written должен быть false")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка не изменилась (откат)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикации после Commit не должно быть: %+v", pubs)
	}
}

// TestRenderWorker_PublishError — сбой публикации q:analyze при
// зафиксированной строке: written=true + ошибка — доставка вернётся
// повторной доставкой и опубликует заново (at-least-once, ТЗ 3.4).
func TestRenderWorker_PublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>dom</html>")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	written, err := f.w.ProcessResult(context.Background(), rwTask)
	if err == nil {
		t.Fatal("ошибка публикации обязана возвращаться")
	}
	if !written {
		t.Error("written должен быть true — строка зафиксирована до публикации")
	}
	if !rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("строка записана (Commit до публикации)")
	}
}

// TestRenderWorker_SelectError — сбой чтения цели: ошибка без рендера.
func TestRenderWorker_SelectError(t *testing.T) {
	f := newRWFixture(t)
	f.pool.queryErr = errors.New("база недоступна")
	if _, err := f.w.ProcessResult(context.Background(), rwTask); err == nil {
		t.Fatal("ошибка SELECT обязана возвращаться")
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("рендер не должен запускаться: %+v", tasks)
	}
}

// TestRenderWorker_NewValidation — nil-зависимости отклоняются.
func TestRenderWorker_NewValidation(t *testing.T) {
	pool := newRWFakePool()
	store := &rwFakeStore{j: pool.j, p: pool}
	renderer := &rwFakeRenderer{j: pool.j}
	q := &rwFakeQueue{j: pool.j}
	cases := []struct {
		name string
		db   RenderDB
		q    queue.Queue
		s    ContentStore
		r    Renderer
	}{
		{"без БД", nil, q, store, renderer},
		{"без очереди", pool, nil, store, renderer},
		{"без хранилища", pool, q, nil, renderer},
		{"без рендера", pool, q, store, nil},
	}
	for _, c := range cases {
		if _, err := New(c.db, c.q, c.s, c.r, Options{}); err == nil {
			t.Errorf("%s: New должен отклонять nil-зависимость", c.name)
		}
	}
}

// --- цикл Run ----------------------------------------------------------------

// TestRenderWorker_RunSmoke — цикл: доставки обрабатываются, XACK после
// публикации; ядовитая запись — без XACK (вернётся повторной доставкой,
// T-025); отмена ctx — чистый nil.
func TestRenderWorker_RunSmoke(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html>dom</html>")

	key2 := rwRowKey{zone: rwKey.zone, domain: rwKey.domain, subdomain: 2, variant: 0}
	raw2 := rwTestHash(2)
	f.pool.seedRow(key2, "https://two.example/", &raw2)
	task2 := queue.Result{ZoneID: key2.zone, DomainID: key2.domain, SubdomainID: key2.subdomain, Variant: 0, Name: "two.example"}
	payload2, _ := json.Marshal(task2)

	f.pool.j.ackNotify = make(chan struct{}, 4)
	f.q.deliveries = []queue.Delivery{
		rwDelivery("e1"),
		{Stream: queue.StreamRender, ID: "e2", Payload: []byte("{битый json")},
		{Stream: queue.StreamRender, ID: "e3", Payload: payload2},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{}) }()

	// Ждём два XACK (e1, e3); e2 — ядовитая, не подтверждается.
	for want := 2; want > 0; {
		select {
		case <-f.pool.j.ackNotify:
			want--
		case <-time.After(5 * time.Second):
			t.Fatal("XACK не наступил")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}

	acked := f.q.ackedSnapshot()
	if len(acked) != 2 {
		t.Fatalf("XACK = %v, нужны e1 и e3 (e2 ядовитая — без XACK)", acked)
	}
	got := map[string]bool{acked[0]: true, acked[1]: true}
	if !got["e1"] || !got["e3"] {
		t.Errorf("XACK = %v, нужны e1 и e3 (e2 ядовитая — без XACK)", acked)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 {
		t.Fatalf("публикаций q:analyze = %d, нужно 2", len(pubs))
	}
	if i, j := f.pool.j.indexOf(rwEvPublish), f.pool.j.indexOf(rwEvAck); i < 0 || j < 0 || i > j {
		t.Errorf("XACK должен следовать за публикацией: publish=%d ack=%d", i, j)
	}
}

// TestRenderWorker_RunBrowserDead — недоступный браузер: очередь не
// читается (доставки остаются в стриме, не в PEL), цикл ждёт и чисто
// завершается по отмене ctx.
func TestRenderWorker_RunBrowserDead(t *testing.T) {
	f := newRWFixture(t)
	f.q.deliveries = []queue.Delivery{rwDelivery("e1")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, f.w, Options{Alive: func() bool { return false }})
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if n := f.q.consumesCount(); n != 0 {
		t.Errorf("Consume вызван %d раз при мёртвом браузере — очередь читаться не должна", n)
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Errorf("рендер не должен запускаться: %+v", tasks)
	}
}

// TestRenderWorker_RunPanicRecovered — паника обработки одной доставки
// (рендер/ProcessResult) не роняет процесс (инвариант каркаса, R-04):
// паникующая запись не подтверждается, соседние доставки окна
// дорабатываются и подтверждаются.
func TestRenderWorker_RunPanicRecovered(t *testing.T) {
	f := newRWFixture(t)
	// Две строки: рендер первой паникует (как CDP посреди страницы),
	// вторая — обычная доставка того же окна.
	f.pool.seedRow(rwKey, "https://spa.example/", nil)
	key2 := rwRowKey{zone: rwKey.zone, domain: rwKey.domain, subdomain: 2, variant: 0}
	f.pool.seedRow(key2, "https://two.example/", nil)
	f.renderer.panicOn = "https://spa.example/"
	f.renderer.output = rwOutput("<html>dom</html>")

	task2 := queue.Result{ZoneID: key2.zone, DomainID: key2.domain, SubdomainID: key2.subdomain, Variant: 0, Name: "two.example"}
	payload2, err := json.Marshal(task2)
	if err != nil {
		t.Fatal(err)
	}
	f.pool.j.ackNotify = make(chan struct{}, 4)
	f.q.deliveries = []queue.Delivery{rwDelivery("e1"),
		{Stream: queue.StreamRender, ID: "e2", Payload: payload2}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{}) }()

	// e2 обязан доработать и подтвердиться, несмотря на панику e1.
	select {
	case <-f.pool.j.ackNotify:
	case <-time.After(5 * time.Second):
		t.Fatal("XACK здоровой доставки не наступил (паника убила цикл?)")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	acked := f.q.ackedSnapshot()
	if len(acked) != 1 || acked[0] != "e2" {
		t.Errorf("XACK = %v, нужен только e2 (паникующая e1 — без XACK)", acked)
	}
	if !rwRowSnapshot(f.pool, key2).rendered {
		t.Error("здоровая доставка не записана")
	}
	if rwRowSnapshot(f.pool, rwKey).rendered {
		t.Error("паникующая доставка не должна записывать строку")
	}
}

// TestRenderWorker_RunValidation — nil-воркер отклоняется.
func TestRenderWorker_RunValidation(t *testing.T) {
	if err := Run(context.Background(), nil, Options{}); err == nil {
		t.Fatal("Run должен отклонять nil-воркер")
	}
}
