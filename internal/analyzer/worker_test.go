// Юнит-тесты analyzer (T-071; ТЗ 5.8, §7, §13.1, 3.4) на фейковых пуле,
// хранилище контента, источнике набора правил и очереди — без БД, Redis
// и сети. Фейковый пул живёт семантикой subdomain_results (SELECT входа
// анализа, UPDATE-штамп rules_version с оптимистической охраной
// content_hash и статусом домена), domains (status <> deleted) и
// laravel_findings (upsert по PK / DELETE); фейковый ContentStore отдаёт
// контент по хэшу. Проверяются точные SQL и параметры, порядок «SELECT →
// Get контента → EvaluateCtx → транзакция (штамп → findings) → XACK»,
// все срабатывания независимо от порога и is_laravel, ветки без
// контента/без срабатываний, конфликт версий, исчезнувшая строка,
// повторная доставка, смена набора (SIGHUP), сбои шагов, отмена ctx;
// smoke — цикл Run.
package analyzer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/content"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// --- протокол событий (порядок шагов обработки) -------------------------------

// anEventKind — тип события протокола; порядок проверяется индексами.
type anEventKind string

const (
	anEvGet       anEventKind = "get"        // чтение контента
	anEvBegin     anEventKind = "begin"      // открытие транзакции
	anEvMark      anEventKind = "mark"       // UPDATE-штамп rules_version
	anEvUpsert    anEventKind = "upsert"     // INSERT ... ON CONFLICT laravel_findings
	anEvDelete    anEventKind = "delete"     // DELETE laravel_findings
	anEvLock      anEventKind = "lock"       // advisory-блокировка домена (T-072)
	anEvAggSelect anEventKind = "agg_select" // SELECT findings домена для агрегации
	anEvAggUpsert anEventKind = "agg_upsert" // upsert laravel_domains
	anEvAggDelete anEventKind = "agg_delete" // DELETE laravel_domains
	anEvCommit    anEventKind = "commit"
	anEvAck       anEventKind = "ack"
)

// anJournal — потокобезопасный протокол (цикл Run работает в горутине).
type anJournal struct {
	mu     sync.Mutex
	events []anEventKind

	// ackNotify — неблокирующий сигнал на каждое Ack (синхронизация smoke).
	ackNotify chan struct{}
}

func (j *anJournal) record(k anEventKind) {
	j.mu.Lock()
	j.events = append(j.events, k)
	j.mu.Unlock()
}

func (j *anJournal) snapshot() []anEventKind {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]anEventKind, len(j.events))
	copy(out, j.events)
	return out
}

func (j *anJournal) indexOf(k anEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e == k {
			return i
		}
	}
	return -1
}

func (j *anJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- фейковый pgx-пул: subdomain_results + domains + laravel_findings ---------

// anRowKey — PK subdomain_results и laravel_findings в фейке.
type anRowKey struct {
	zone      int16
	domain    int64
	subdomain int16
	variant   int16
}

// anResultRow — строка subdomain_results, которую читает analyzeSelectSQL
// и меняет analyzeMarkSQL.
type anResultRow struct {
	headers      *string // jsonb
	cookies      *string // jsonb
	hash         []byte  // nil — NULL (контента нет)
	rulesVersion *int32
}

// anFinding — строка laravel_findings, которую пишет findingUpsertSQL.
type anFinding struct {
	score        float32
	verMin       *string
	verMax       *string
	signals      string // jsonb-текст
	isLaravel    bool
	rulesVersion int32
}

// anFakePool — AnalyzerDB без БД: живая семантика subdomain_results
// (SELECT по PK, UPDATE-штамп с охранами content_hash IS NOT DISTINCT
// FROM и «домен не deleted»), domains (status), laravel_findings
// (upsert/DELETE по PK) и laravel_domains (upsert/DELETE по PK домена,
// SELECT уверенных срабатываний — агрегация T-072). Транзакция работает
// на снапшоте: Rollback восстанавливает состояние, Commit оставляет
// (конкурентные транзакции не моделируются — их сериализует PostgreSQL).
type anFakePool struct {
	j *anJournal

	results  map[anRowKey]*anResultRow
	statuses map[anRowKey]int16 // status домена по ключу строки
	findings map[anRowKey]*anFinding
	domains  map[anDomainKey]*anDomain

	queryErr     error
	beginErr     error
	markErr      error
	upsertErr    error
	deleteErr    error
	lockErr      error
	aggSelectErr error
	aggUpsertErr error
	aggDeleteErr error
	commitErr    error

	// mutateBeforeTx — хук: вызывается перед снапшотом транзакции
	// (симуляция перезаписи строки перепроверкой/рендером между SELECT
	// и UPDATE, либо удаления домена).
	mutateBeforeTx func(p *anFakePool)

	mu sync.Mutex
}

func newANFakePool() *anFakePool {
	return &anFakePool{
		j:        &anJournal{},
		results:  make(map[anRowKey]*anResultRow),
		statuses: make(map[anRowKey]int16),
		findings: make(map[anRowKey]*anFinding),
		domains:  make(map[anDomainKey]*anDomain),
	}
}

var _ AnalyzerDB = (*anFakePool)(nil)

// seedResult — строка subdomain_results домена со статусом alive.
func (p *anFakePool) seedResult(key anRowKey, headers, cookies *string, hash []byte) {
	p.results[key] = &anResultRow{headers: headers, cookies: cookies, hash: hash}
	p.statuses[key] = int16(model.StatusAlive)
}

func (p *anFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("anFakePool: CopyFrom не должен вызываться")
}

func (p *anFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("anFakePool: Exec вне транзакции")
}

// anFakeRows — pgx.Rows с заготовленными строками SELECT (headers,
// cookies, content_hash).
type anFakeRows struct {
	rows [][]any
	i    int
}

func (r *anFakeRows) Close()                        {}
func (r *anFakeRows) Err() error                    { return nil }
func (r *anFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *anFakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}
func (r *anFakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *anFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) || len(dest) != 3 {
		return fmt.Errorf("anFakeRows: Scan без Next или приёмников %d (нужно 3)", len(dest))
	}
	row := r.rows[r.i-1]
	// NULL → nil-приёмник остаётся нетронутым (семантика pgx: *string
	// и *[]byte остаются nil).
	for i, v := range row {
		if v == nil {
			continue
		}
		switch d := dest[i].(type) {
		case **string:
			*d = v.(*string)
		case *[]byte:
			*d = v.([]byte)
		default:
			return fmt.Errorf("anFakeRows: приёмник[%d] %T", i, dest[i])
		}
	}
	return nil
}

func (r *anFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *anFakeRows) RawValues() [][]byte    { return nil }
func (r *anFakeRows) Conn() *pgx.Conn        { return nil }
func (r *anFakeRows) TypeMap() *pgtype.Map   { return nil }

func (p *anFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sql != analyzeSelectSQL {
		return nil, fmt.Errorf("anFakePool: неожидаемый SQL: %s", sql)
	}
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
	r, ok := p.results[key]
	if !ok {
		return &anFakeRows{}, nil
	}
	var hash []byte
	if r.hash != nil {
		hash = append([]byte(nil), r.hash...)
	}
	return &anFakeRows{rows: [][]any{{r.headers, r.cookies, hash}}}, nil
}

func (p *anFakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	// Хук «строку перезаписал другой эпизод» выполняется до снапшота:
	// чужая транзакция уже зафиксирована — наш Rollback её не откатывает.
	if p.mutateBeforeTx != nil {
		p.mutateBeforeTx(p)
	}
	snapResults := make(map[anRowKey]*anResultRow, len(p.results))
	for k, v := range p.results {
		c := *v
		snapResults[k] = &c
	}
	snapFindings := make(map[anRowKey]*anFinding, len(p.findings))
	for k, v := range p.findings {
		c := *v
		snapFindings[k] = &c
	}
	snapDomains := make(map[anDomainKey]*anDomain, len(p.domains))
	for k, v := range p.domains {
		c := *v
		snapDomains[k] = &c
	}
	p.j.record(anEvBegin)
	return &anFakeTx{pool: p, snapResults: snapResults, snapFindings: snapFindings, snapDomains: snapDomains}, nil
}

type anFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx этап не использует

	pool         *anFakePool
	snapResults  map[anRowKey]*anResultRow
	snapFindings map[anRowKey]*anFinding
	snapDomains  map[anDomainKey]*anDomain
	commit       bool
}

func (t *anFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch sql {
	case analyzeMarkSQL:
		key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
		if t.pool.markErr != nil {
			return pgconn.CommandTag{}, t.pool.markErr
		}
		version := args[4].(int32)
		hash := args[5].([]byte)
		r, ok := t.pool.results[key]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		// Охрана content_hash IS NOT DISTINCT FROM $6.
		same := (r.hash == nil && hash == nil) ||
			(r.hash != nil && hash != nil && string(r.hash) == string(hash))
		if !same {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		if t.pool.statuses[key] == int16(model.StatusDeleted) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		v := version
		r.rulesVersion = &v
		t.pool.j.record(anEvMark)
		return pgconn.NewCommandTag("UPDATE 1"), nil

	case findingUpsertSQL:
		key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
		if t.pool.upsertErr != nil {
			return pgconn.CommandTag{}, t.pool.upsertErr
		}
		t.pool.findings[key] = &anFinding{
			score:        args[4].(float32),
			verMin:       args[5].(*string),
			verMax:       args[6].(*string),
			signals:      args[7].(string),
			isLaravel:    args[8].(bool),
			rulesVersion: args[9].(int32),
		}
		t.pool.j.record(anEvUpsert)
		return pgconn.NewCommandTag("INSERT 0 1"), nil

	case findingDeleteSQL:
		key := anRowKey{zone: args[0].(int16), domain: args[1].(int64), subdomain: args[2].(int16), variant: args[3].(int16)}
		if t.pool.deleteErr != nil {
			return pgconn.CommandTag{}, t.pool.deleteErr
		}
		delete(t.pool.findings, key)
		t.pool.j.record(anEvDelete)
		return pgconn.NewCommandTag("DELETE 1"), nil

	case aggregateLockSQL:
		if t.pool.lockErr != nil {
			return pgconn.CommandTag{}, t.pool.lockErr
		}
		if args[0].(int64) == 0 {
			return pgconn.CommandTag{}, errors.New("anFakePool: ключ блокировки не задан")
		}
		t.pool.j.record(anEvLock)
		return pgconn.NewCommandTag("SELECT 1"), nil

	case aggregateUpsertSQL:
		key := anDomainKey{zone: args[0].(int16), domain: args[1].(int64)}
		if t.pool.aggUpsertErr != nil {
			return pgconn.CommandTag{}, t.pool.aggUpsertErr
		}
		t.pool.domains[key] = &anDomain{
			score:        args[2].(float32),
			verMin:       args[3].(*string),
			verMax:       args[4].(*string),
			foundOnMask:  model.DNSMask(args[5].(int64)),
			rulesVersion: args[6].(int32),
			updatedAt:    time.Now(),
		}
		t.pool.j.record(anEvAggUpsert)
		return pgconn.NewCommandTag("INSERT 0 1"), nil

	case aggregateDeleteSQL:
		key := anDomainKey{zone: args[0].(int16), domain: args[1].(int64)}
		if t.pool.aggDeleteErr != nil {
			return pgconn.CommandTag{}, t.pool.aggDeleteErr
		}
		_, ok := t.pool.domains[key]
		delete(t.pool.domains, key)
		tag := "DELETE 0"
		if ok {
			tag = "DELETE 1"
		}
		t.pool.j.record(anEvAggDelete)
		return pgconn.NewCommandTag(tag), nil

	default:
		return pgconn.CommandTag{}, fmt.Errorf("anFakePool: неожидаемый SQL: %s", sql)
	}
}

func (t *anFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.commit = true
	t.pool.j.record(anEvCommit)
	return nil
}

func (t *anFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.commit {
		return nil // откат после Commit — no-op
	}
	t.pool.results, t.pool.findings, t.pool.domains = t.snapResults, t.snapFindings, t.snapDomains
	return nil
}

// --- фейковое хранилище контента ----------------------------------------------

// anFakeStore — ContentGetter: Get по хэшу.
type anFakeStore struct {
	j *anJournal

	blocks map[content.Hash][]byte
	getErr error

	mu sync.Mutex
}

var _ ContentGetter = (*anFakeStore)(nil)

func (s *anFakeStore) Get(ctx context.Context, h content.Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.j.record(anEvGet)
	if s.getErr != nil {
		return nil, s.getErr
	}
	data, ok := s.blocks[h]
	if !ok {
		return nil, content.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

// --- фейковый источник набора правил ------------------------------------------

// anFakeSets — SetSource со сменяемым снапшотом (моделирует Reloader:
// стартовый набор + SIGHUP-релоад).
type anFakeSets struct {
	mu  sync.Mutex
	set *rules.Set
}

var _ SetSource = (*anFakeSets)(nil)

func (f *anFakeSets) Set() *rules.Set {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.set
}

func (f *anFakeSets) swap(set *rules.Set) {
	f.mu.Lock()
	f.set = set
	f.mu.Unlock()
}

// --- фейковая очередь -----------------------------------------------------------

// anFakeQueue — queue.Queue: потребитель q:analyze; Publish этапу не
// нужен (конец конвейера §7) — вызов громко ломает тест.
type anFakeQueue struct {
	j *anJournal

	deliveries []queue.Delivery
	consumeErr error

	mu       sync.Mutex
	acked    []string
	consumes int
}

var _ queue.Queue = (*anFakeQueue)(nil)

func (q *anFakeQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", errors.New("anFakeQueue: Publish не должен вызываться (analyzer не публикует)")
}

func (q *anFakeQueue) Consume(ctx context.Context, stream, group, consumer string, n int) ([]queue.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	q.consumes++
	if stream != queue.StreamAnalyze {
		q.mu.Unlock()
		return nil, fmt.Errorf("anFakeQueue: неожидаемый стрим %s", stream)
	}
	if group != DefaultGroup {
		q.mu.Unlock()
		return nil, fmt.Errorf("anFakeQueue: неожидаемая группа %s", group)
	}
	if consumer == "" {
		q.mu.Unlock()
		return nil, errors.New("anFakeQueue: пустой consumer")
	}
	var out []queue.Delivery
	if q.consumeErr != nil {
		err := q.consumeErr
		q.mu.Unlock()
		return nil, err
	}
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

func (q *anFakeQueue) Ack(ctx context.Context, _, _ string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, ids...)
	q.mu.Unlock()
	for range ids {
		q.j.record(anEvAck)
	}
	q.j.signalAck()
	return nil
}

func (q *anFakeQueue) ackedSnapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, len(q.acked))
	copy(out, q.acked)
	return out
}

func (q *anFakeQueue) consumesCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// --- сборщики -------------------------------------------------------------------

// anTestHash — различимые хэши: anTestHash(1), (2), …
func anTestHash(n byte) content.Hash {
	var h content.Hash
	h[0] = n
	return h
}

// anTestHashN — байты различимого хэша для колонки content_hash фикстуры.
func anTestHashN(n byte) []byte {
	h := anTestHash(n)
	out := make([]byte, len(h))
	copy(out, h[:])
	return out
}

// anSet — набор правил фикстуры: cookie XSRF-TOKEN (0.25), кука со
// суффиксом _session (regex, 0.35), html welcome (0.45, диапазон
// [5.3, 7.x]); версия задаётся отдельно (SIGHUP-релоад меняет её).
func anSet(version int32) *rules.Set {
	return &rules.Set{
		Rules: []rules.Rule{
			{ID: "cookie_xsrf_token", Target: rules.TargetCookieName, Match: "XSRF-TOKEN", Weight: 0.25},
			{ID: "cookie_session_suffix", Target: rules.TargetCookieName, Regex: `^[a-z0-9_]+_session$`, Weight: 0.35},
			{ID: "html_welcome", Target: rules.TargetHTML, Regex: `<div class="content">`, Weight: 0.45,
				VerMin: "5.3", VerMax: "7.x"},
		},
		Version: version,
	}
}

// anConflictSet — правила с непересекающимися диапазонами версий.
func anConflictSet(version int32) *rules.Set {
	return &rules.Set{
		Rules: []rules.Rule{
			{ID: "a_old", Target: rules.TargetHTML, Regex: `Old-App`, Weight: 0.5, VerMax: "6.x"},
			{ID: "b_new", Target: rules.TargetHTML, Regex: `New-App`, Weight: 0.5, VerMin: "10"},
		},
		Version: version,
	}
}

// anJSON — сериализация для jsonb-полей фикстуры.
func anJSON(t *testing.T, v any) *string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("anJSON: %v", err)
	}
	s := string(b)
	return &s
}

// anFixture — собранный набор фейков и воркер.
type anFixture struct {
	pool  *anFakePool
	store *anFakeStore
	sets  *anFakeSets
	q     *anFakeQueue
	w     *Worker
}

func newANFixture(t *testing.T, set *rules.Set, opts Options) *anFixture {
	t.Helper()
	pool := newANFakePool()
	store := &anFakeStore{j: pool.j, blocks: make(map[content.Hash][]byte)}
	sets := &anFakeSets{set: set}
	q := &anFakeQueue{j: pool.j}
	w, err := New(pool, q, store, sets, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &anFixture{pool: pool, store: store, sets: sets, q: q, w: w}
}

// anKey — ключ строки фикстуры.
var anKey = anRowKey{zone: 3, domain: 42, subdomain: 1, variant: 0}

// anTask — задача q:analyze на ключ фикстуры.
var anTask = queue.Result{
	ZoneID: anKey.zone, DomainID: anKey.domain,
	SubdomainID: anKey.subdomain, Variant: 0, Name: "laravel.example",
}

// anDelivery — валидная доставка с Result-сообщением.
func anDelivery(id string) queue.Delivery {
	payload, err := json.Marshal(anTask)
	if err != nil {
		panic(err)
	}
	return queue.Delivery{Stream: queue.StreamAnalyze, ID: id, Payload: payload}
}

// anHeaders — jsonb заголовков типовой строки.
func anHeaders(t *testing.T) *string {
	return anJSON(t, map[string][]string{
		"Content-Type": {"text/html; charset=UTF-8"},
		"Set-Cookie":   {"XSRF-TOKEN=abc; path=/", "laravel_session=xyz; path=/"},
	})
}

// anCookies — jsonb cookies типовой строки: имя/значение + атрибуты,
// которые движок игнорирует (контракт T-058/T-067).
func anCookies(t *testing.T, names ...string) *string {
	t.Helper()
	type cookie struct {
		Name     string `json:"name"`
		Value    string `json:"value"`
		Path     string `json:"path"`
		Domain   string `json:"domain"`
		Secure   bool   `json:"secure"`
		HTTPOnly bool   `json:"http_only"`
	}
	out := make([]cookie, 0, len(names))
	for _, n := range names {
		out = append(out, cookie{Name: n, Value: "eyJpdiI6", Path: "/", HTTPOnly: true})
	}
	return anJSON(t, out)
}

// anPageHTML — страница с welcome-признаком (правило html_welcome).
func anPageHTML() []byte {
	return []byte(`<html><body><div class="content"><div class="title">Laravel</div></div></body></html>`)
}

// anFindingSnapshot — снапшот строки findings.
func anFindingSnapshot(p *anFakePool, key anRowKey) (anFinding, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f, ok := p.findings[key]; ok {
		return *f, true
	}
	return anFinding{}, false
}

// anResultSnapshot — снапшот строки subdomain_results.
func anResultSnapshot(p *anFakePool, key anRowKey) (anResultRow, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.results[key]; ok {
		return *r, true
	}
	return anResultRow{}, false
}

// --- ProcessResult -----------------------------------------------------------

// TestAnalyzer_HappyPath — сквозной контракт: SELECT входа → Get контента
// → EvaluateCtx → транзакция (штамп rules_version → INSERT findings со
// всеми срабатываниями) → COMMIT; порядок шагов — по журналу.
func TestAnalyzer_HappyPath(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if !wrote {
		t.Fatal("ожидана запись findings")
	}

	// Порядок: контент → транзакция → штамп → findings → фиксация.
	events := f.pool.j.snapshot()
	for _, pair := range [][2]anEventKind{
		{anEvGet, anEvBegin}, {anEvBegin, anEvMark}, {anEvMark, anEvUpsert}, {anEvUpsert, anEvCommit},
	} {
		if f.pool.j.indexOf(pair[0]) > f.pool.j.indexOf(pair[1]) {
			t.Fatalf("порядок событий %v после %v нарушен: %v", pair[1], pair[0], events)
		}
	}

	// Штамп версии набора на строке результата.
	row, ok := anResultSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка результата исчезла")
	}
	if row.rulesVersion == nil || *row.rulesVersion != 1001 {
		t.Fatalf("rules_version строки = %v, нужно 1001", row.rulesVersion)
	}

	// Findings: все срабатывания (три правила), score noisy-OR
	// 1-(1-0.25)(1-0.35)(1-0.45) ≈ 0.7231 ≥ 0.6 → is_laravel,
	// диапазон [5.3, 7.x] из html-правила.
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка laravel_findings не записана")
	}
	wantScore := float32(1 - (1-0.25)*(1-0.35)*(1-0.45))
	if finding.score != wantScore {
		t.Fatalf("score = %v, нужно %v", finding.score, wantScore)
	}
	if !finding.isLaravel {
		t.Fatalf("is_laravel = false при score %v ≥ 0.6", finding.score)
	}
	if finding.verMin == nil || *finding.verMin != "5.3" {
		t.Fatalf("ver_min = %v, нужно 5.3", finding.verMin)
	}
	if finding.verMax == nil || *finding.verMax != "7.x" {
		t.Fatalf("ver_max = %v, нужно 7.x", finding.verMax)
	}
	if finding.rulesVersion != 1001 {
		t.Fatalf("rules_version findings = %d, нужно 1001", finding.rulesVersion)
	}

	// signals: [{rule, weight, evidence}, …] — все три срабатывания,
	// свидетельства формата T-069.
	var sigs []map[string]any
	if err := json.Unmarshal([]byte(finding.signals), &sigs); err != nil {
		t.Fatalf("signals не JSON: %v\n%s", err, finding.signals)
	}
	if len(sigs) != 3 {
		t.Fatalf("signals: %d записей, нужно 3 (%s)", len(sigs), finding.signals)
	}
	byRule := make(map[string]map[string]any, len(sigs))
	for _, s := range sigs {
		byRule[s["rule"].(string)] = s
	}
	for _, id := range []string{"cookie_xsrf_token", "cookie_session_suffix", "html_welcome"} {
		if _, ok := byRule[id]; !ok {
			t.Fatalf("в signals нет правила %s: %s", id, finding.signals)
		}
	}
	if ev := byRule["cookie_xsrf_token"]["evidence"]; ev != "cookie XSRF-TOKEN" {
		t.Fatalf("свидетельство cookie_xsrf_token = %v", ev)
	}
}

// TestAnalyzer_SQLContract — точный текст SQL этапа (контракт против
// миграцией 000001: PK laravel_findings (zone_id, domain_id, subdomain_id,
// variant), score real, ver_min/ver_max text, signals jsonb, is_laravel
// boolean, rules_version integer NOT NULL).
func TestAnalyzer_SQLContract(t *testing.T) {
	if !strings.Contains(analyzeSelectSQL, "SELECT headers, cookies, content_hash FROM subdomain_results") {
		t.Error("select: не читает вход анализа (headers/cookies/content_hash)")
	}
	if !strings.Contains(analyzeMarkSQL, "SET rules_version = $5::int4") {
		t.Error("mark: нет штампа rules_version")
	}
	if !strings.Contains(analyzeMarkSQL, "content_hash IS NOT DISTINCT FROM $6::bytea") {
		t.Error("mark: нет охраны актуальности content_hash")
	}
	if !strings.Contains(analyzeMarkSQL, fmt.Sprintf("d.status = %d", int16(model.StatusDeleted))) {
		t.Error("mark: нет охраны «домен не deleted» (§8/§13.1)")
	}
	if !strings.Contains(findingUpsertSQL,
		"ON CONFLICT (zone_id, domain_id, subdomain_id, variant) DO UPDATE") {
		t.Error("upsert: нет конфликта по PK laravel_findings (§3.5 перезапись)")
	}
	for _, col := range []string{"score", "ver_min", "ver_max", "signals", "is_laravel", "rules_version"} {
		if !strings.Contains(findingUpsertSQL, col) {
			t.Errorf("upsert: нет колонки %s (§6.1)", col)
		}
	}
	if !strings.Contains(findingUpsertSQL, "$5::float4") {
		t.Error("upsert: score не приводится к real (float4)")
	}
	if !strings.Contains(findingUpsertSQL, "$8::jsonb") {
		t.Error("upsert: signals не приводятся к jsonb")
	}
	if !strings.Contains(findingDeleteSQL, "DELETE FROM laravel_findings") {
		t.Error("delete: не удаляет строку laravel_findings")
	}
}

// TestAnalyzer_AllMatchesBelowThreshold — все срабатывания пишутся
// независимо от порога; слабый набор ниже порога → is_laravel = false
// (§13.1: отделять уверенные срабатывания от слабых).
func TestAnalyzer_AllMatchesBelowThreshold(t *testing.T) {
	// Только XSRF-TOKEN 0.25 < 0.6.
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), nil) // контента нет

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("слабые срабатывания обязаны записываться независимо от порога")
	}
	if finding.isLaravel {
		t.Fatal("is_laravel = true при score 0.25 < 0.6")
	}
	if finding.score != 0.25 {
		t.Fatalf("score = %v, нужно 0.25", finding.score)
	}
	if !strings.Contains(finding.signals, "cookie_xsrf_token") {
		t.Fatalf("signals не содержат сработавшее правило: %s", finding.signals)
	}
}

// TestAnalyzer_ThresholdBoundary — порог нестрогий: score ≥ min
// (равенство на границе — Laravel, T-070); MinScore ≤ 0 заменяется
// DefaultMinScore.
func TestAnalyzer_ThresholdBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		minScore float64
		want     bool
	}{
		{"равенство на границе", 0.25, true},
		{"чуть выше score", 0.26, false},
		{"нулевая опция — дефолт 0.6", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{MinScore: tc.minScore})
			f.pool.seedResult(anKey, nil, anCookies(t, "XSRF-TOKEN"), nil)
			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			finding, ok := anFindingSnapshot(f.pool, anKey)
			if !ok {
				t.Fatal("строка findings не записана")
			}
			if finding.isLaravel != tc.want {
				t.Fatalf("is_laravel = %v при score 0.25, min %v", finding.isLaravel, tc.minScore)
			}
		})
	}
}

// TestAnalyzer_VersionConflict — пустое пересечение диапазонов:
// ver_min/ver_max NULL, обе границы пустые (T-070).
func TestAnalyzer_VersionConflict(t *testing.T) {
	f := newANFixture(t, anConflictSet(2002), Options{})
	hash := anTestHash(9)
	f.store.blocks[hash] = []byte("Old-App New-App")
	f.pool.seedResult(anKey, nil, nil, hash[:])

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка findings не записана (оба правила сработали)")
	}
	if finding.score != 0.75 { // noisy-OR(0.5, 0.5)
		t.Fatalf("score = %v, нужно 0.75", finding.score)
	}
	if finding.verMin != nil || finding.verMax != nil {
		t.Fatalf("конфликт диапазонов: границы обязаны быть NULL, получены %v/%v", finding.verMin, finding.verMax)
	}
}

// TestAnalyzer_NoSignalsDeletesRow — ни одно правило не сработало:
// строка не пишется, существующая (прошлый анализ) удаляется — §3.5
// перезапись; штамп rules_version ставится и на «пустой» итог.
func TestAnalyzer_NoSignalsDeletesRow(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	stale := "stale"
	f.pool.findings[anKey] = &anFinding{score: 0.9, verMin: &stale, isLaravel: true, rulesVersion: 999}
	hash := anTestHash(5)
	f.store.blocks[hash] = []byte("<html><body>чужой фреймворк без признаков</body></html>")
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), hash[:])

	// «Пустой» итог — тоже итог: транзакция фиксируется (штамп + DELETE),
	// но строка findings не пишется.
	if wrote, err := f.w.ProcessResult(context.Background(), anTask); err != nil || !wrote {
		t.Fatalf("ProcessResult = %v, %v; нужен успех с записью (штамп/удаление)", wrote, err)
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("прошлая строка findings не удалена (§3.5 перезапись)")
	}
	if f.pool.j.indexOf(anEvDelete) == -1 {
		t.Fatal("DELETE laravel_findings не выполнялся")
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion == nil || *row.rulesVersion != 1001 {
		t.Fatalf("rules_version строки = %v, нужен штамп 1001 и на пустом итоге", row.rulesVersion)
	}
}

// TestAnalyzer_NoContent — контента нет (NULL content_hash, контент
// потерян, пустой DOM) — анализ по заголовкам и кукам (ТЗ 5.8: правила по
// шести целям, html/meta — не весь анализ).
func TestAnalyzer_NoContent(t *testing.T) {
	cases := []struct {
		name  string
		hash  []byte
		block []byte // nil — контента в хранилище нет (ErrNotFound)
	}{
		{"NULL content_hash", nil, nil},
		{"контент потерян", anTestHashN(3), nil},
		{"пустой DOM", anTestHashN(4), []byte{}},
		{"урезанный хэш", []byte{1, 2, 3}, []byte("ignored")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newANFixture(t, anSet(1001), Options{})
			if tc.block != nil {
				var h content.Hash
				copy(h[:], tc.hash)
				f.store.blocks[h] = tc.block
			}
			f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), tc.hash)

			if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
				t.Fatalf("ProcessResult: %v", err)
			}
			finding, ok := anFindingSnapshot(f.pool, anKey)
			if !ok {
				t.Fatal("куки обязаны срабатывать без контента")
			}
			if !strings.Contains(finding.signals, "cookie_xsrf_token") {
				t.Fatalf("куки не проанализированы: %s", finding.signals)
			}
			if strings.Contains(finding.signals, "html_welcome") {
				t.Fatalf("html-правило сработало без контента: %s", finding.signals)
			}
		})
	}
}

// TestAnalyzer_RowMissing — строки нет (домен перепроверен без этого
// ключа, строки вычищены): успех без записи, findings не пишутся
// (стиль renderworker: 0 rows → успех).
func TestAnalyzer_RowMissing(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("без строки findings не пишутся")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны без строки результата")
	}
	if f.pool.j.indexOf(anEvBegin) != -1 {
		t.Fatal("без строки транзакция не открывается")
	}
}

// TestAnalyzer_NoRulesSet — nil-снапшот SetSource (у Reloader не бывает;
// защита для альтернативных реализаций, симметрия с ReanalyzeLoop):
// пропуск без паники и без записи; строка без штампа вернётся
// переанализом (T-074).
func TestAnalyzer_NoRulesSet(t *testing.T) {
	f := newANFixture(t, nil, Options{})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), nil)

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("без набора правил запись невозможна")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны без набора правил")
	}
	if f.pool.j.indexOf(anEvBegin) != -1 {
		t.Fatal("без набора правил транзакция не открывается")
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion != nil {
		t.Fatal("штамп без анализа ставить нельзя — переанализ вернётся к строке")
	}
}

// TestAnalyzer_StaleRow — строка перезаписана другим эпизодом между
// SELECT и UPDATE (перепроверка DELETE+INSERT, рендер сменил контент):
// охрана content_hash → 0 строк → успех без записи; у нового эпизода
// своя публикация q:analyze.
func TestAnalyzer_StaleRow(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	old := anTestHash(1)
	f.store.blocks[old] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), old[:])

	f.pool.mutateBeforeTx = func(p *anFakePool) {
		if r, ok := p.results[anKey]; ok {
			newHash := anTestHash(2)
			r.hash = newHash[:]
			r.rulesVersion = nil // T-067: смена контента сбрасывает штамп
		}
	}

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("ушедшая из-под задачи строка не перезаписывается")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны по неактуальной строке")
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion != nil {
		t.Fatalf("штамп поставлен неактуальному эпизоду: %v", row.rulesVersion)
	}
	if f.pool.j.indexOf(anEvCommit) != -1 {
		t.Fatal("0 строк — транзакция откатывается без фиксации")
	}
}

// TestAnalyzer_DomainDeleted — домен удалён во время анализа (§8/§13.1):
// охрана статуса → 0 строк → успех без записи findings.
func TestAnalyzer_DomainDeleted(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f.pool.mutateBeforeTx = func(p *anFakePool) {
		p.statuses[anKey] = int16(model.StatusDeleted) // delete-файл успел раньше записи
	}

	wrote, err := f.w.ProcessResult(context.Background(), anTask)
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if wrote {
		t.Fatal("удалённому домену результат не записывается (§8)")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны удалённому домену")
	}
}

// TestAnalyzer_RepeatDelivery — повторная доставка идемпотентна (ТЗ
// 3.4): upsert перезаписывает ту же строку, штамп ставится тем же
// набором; XACK потерявшего ответ воркера безопасен.
func TestAnalyzer_RepeatDelivery(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	for i := 0; i < 2; i++ {
		if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
			t.Fatalf("доставка №%d: %v", i+1, err)
		}
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("строка findings не записана")
	}
	if finding.rulesVersion != 1001 {
		t.Fatalf("rules_version = %d, нужно 1001", finding.rulesVersion)
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion == nil || *row.rulesVersion != 1001 {
		t.Fatalf("rules_version строки = %v, нужно 1001", row.rulesVersion)
	}
	if n := countEvents(f.pool.j.snapshot(), anEvUpsert); n != 2 {
		t.Fatalf("upsert выполнен %d раз (нужно 2 — по одному на доставку)", n)
	}
}

// TestAnalyzer_RulesReload — смена набора (SIGHUP без рестарта): каждая
// доставка берёт актуальный снапшот — новая версия в штампе и findings.
func TestAnalyzer_RulesReload(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)

	// Первый анализ: признаков нет (PHPSESSID не Laravel) — строка не
	// пишется, штамп 1001.
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("по старому набору срабатываний нет")
	}

	// SIGHUP: набор с дополнительным правилом и новой версией.
	reloaded := anSet(2002)
	reloaded.Rules = append(reloaded.Rules, rules.Rule{
		ID: "cookie_php_sessid", Target: rules.TargetCookieName, Match: "PHPSESSID", Weight: 0.4,
	})
	f.sets.swap(reloaded)

	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult после релоада: %v", err)
	}
	finding, ok := anFindingSnapshot(f.pool, anKey)
	if !ok {
		t.Fatal("после релоада правило обязано срабатывать")
	}
	if !strings.Contains(finding.signals, "cookie_php_sessid") {
		t.Fatalf("сработало не новое правило: %s", finding.signals)
	}
	if finding.rulesVersion != 2002 {
		t.Fatalf("rules_version findings = %d, нужно 2002 (актуальный набор)", finding.rulesVersion)
	}
	row, _ := anResultSnapshot(f.pool, anKey)
	if row.rulesVersion == nil || *row.rulesVersion != 2002 {
		t.Fatalf("rules_version строки = %v, нужно 2002", row.rulesVersion)
	}
}

// TestAnalyzer_Errors — сбой каждого шага: ошибка доставки (без XACK,
// повторной доставкой), состояние откатывается.
func TestAnalyzer_Errors(t *testing.T) {
	seed := func(t *testing.T) *anFixture {
		f := newANFixture(t, anSet(1001), Options{})
		hash := anTestHash(7)
		f.store.blocks[hash] = anPageHTML()
		f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
		return f
	}
	cases := []struct {
		name string
		inj  func(f *anFixture)
	}{
		{"чтение строки", func(f *anFixture) { f.pool.queryErr = errors.New("select boom") }},
		{"чтение контента", func(f *anFixture) { f.store.getErr = errors.New("disk boom") }},
		{"открытие транзакции", func(f *anFixture) { f.pool.beginErr = errors.New("begin boom") }},
		{"штамп rules_version", func(f *anFixture) { f.pool.markErr = errors.New("mark boom") }},
		{"запись findings", func(f *anFixture) { f.pool.upsertErr = errors.New("upsert boom") }},
		{"удаление findings", func(f *anFixture) {
			f.pool.upsertErr = errors.New("не должен вызываться")
			f.pool.deleteErr = errors.New("delete boom")
			f.sets.swap(&rules.Set{Rules: []rules.Rule{anSet(1).Rules[0]}, Version: 1})
			f.pool.seedResult(anKey, nil, anCookies(t, "PHPSESSID"), nil) // ни одного срабатывания
		}},
		{"фиксация", func(f *anFixture) { f.pool.commitErr = errors.New("commit boom") }},
		{"битый jsonb заголовков", func(f *anFixture) {
			bad := "{не-json"
			f.pool.results[anKey] = &anResultRow{headers: &bad}
		}},
		{"битый jsonb cookies", func(f *anFixture) {
			bad := "[не-json"
			f.pool.results[anKey] = &anResultRow{cookies: &bad}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := seed(t)
			tc.inj(f)
			if _, err := f.w.ProcessResult(context.Background(), anTask); err == nil {
				t.Fatal("ожидана ошибка доставки")
			}
			// Состояние откатилось: findings нет, штамп не стоит.
			if _, ok := anFindingSnapshot(f.pool, anKey); ok {
				t.Fatal("findings записаны при сбое")
			}
			if row, ok := anResultSnapshot(f.pool, anKey); ok && row.rulesVersion != nil {
				t.Fatal("штамп поставлен при сбое")
			}
		})
	}
}

// TestAnalyzer_CtxCancelled — отмена контекста прерывает обработку
// (graceful shutdown): ошибка без записи.
func TestAnalyzer_CtxCancelled(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.w.ProcessResult(ctx, anTask); err == nil {
		t.Fatal("отменённый ctx — ошибка")
	}
	if _, ok := anFindingSnapshot(f.pool, anKey); ok {
		t.Fatal("findings записаны при отменённом ctx")
	}
}

// --- разбор jsonb --------------------------------------------------------------

func TestParseHeadersJSON(t *testing.T) {
	if h, err := parseHeadersJSON(nil); err != nil || h != nil {
		t.Fatalf("NULL: %v %v", h, err)
	}
	empty := ""
	if h, err := parseHeadersJSON(&empty); err != nil || h != nil {
		t.Fatalf("пусто: %v %v", h, err)
	}
	in := `{"X-Laravel": ["yes"], "Set-Cookie": ["a=1", "b=2"]}`
	h, err := parseHeadersJSON(&in)
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if len(h) != 2 || len(h["Set-Cookie"]) != 2 || h["X-Laravel"][0] != "yes" {
		t.Fatalf("значения: %v", h)
	}
	bad := "{"
	if _, err := parseHeadersJSON(&bad); err == nil {
		t.Fatal("битый JSON — ошибка")
	}
}

func TestParseCookiesJSON(t *testing.T) {
	if c, err := parseCookiesJSON(nil); err != nil || c != nil {
		t.Fatalf("NULL: %v %v", c, err)
	}
	in := `[{"name":"a","value":"1","path":"/","http_only":true}]`
	c, err := parseCookiesJSON(&in)
	if err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if len(c) != 1 || c[0].Name != "a" || c[0].Value != "1" {
		t.Fatalf("cookies: %v", c)
	}
	bad := "["
	if _, err := parseCookiesJSON(&bad); err == nil {
		t.Fatal("битый JSON — ошибка")
	}
}

func TestMarshalSignals(t *testing.T) {
	if s, err := marshalSignals(nil); err != nil || s != "" {
		t.Fatalf("пустой набор: %q %v", s, err)
	}
	in := []rules.Evidence{{Rule: "r", Weight: 0.5, Evidence: "e"}}
	s, err := marshalSignals(in)
	if err != nil {
		t.Fatalf("marshalSignals: %v", err)
	}
	want := `[{"rule":"r","weight":0.5,"evidence":"e"}]`
	if s != want {
		t.Fatalf("формат signals = %s, нужно %s (эскиз §6.1: нижний регистр)", s, want)
	}
}

// --- конструирование ----------------------------------------------------------

func TestNewValidation(t *testing.T) {
	pool := newANFakePool()
	store := &anFakeStore{j: pool.j, blocks: map[content.Hash][]byte{}}
	sets := &anFakeSets{set: anSet(1)}
	q := &anFakeQueue{j: pool.j}
	if _, err := New(nil, q, store, sets, Options{}); err == nil {
		t.Fatal("без БД: ожидана ошибка")
	}
	if _, err := New(pool, nil, store, sets, Options{}); err == nil {
		t.Fatal("без очереди: ожидана ошибка")
	}
	if _, err := New(pool, q, nil, sets, Options{}); err == nil {
		t.Fatal("без хранилища: ожидана ошибка")
	}
	if _, err := New(pool, q, store, nil, Options{}); err == nil {
		t.Fatal("без набора правил: ожидана ошибка")
	}
	if _, err := New(pool, q, store, sets, Options{}); err != nil {
		t.Fatalf("полный набор: %v", err)
	}
}

func TestOptionsDefaults(t *testing.T) {
	o := Options{Consumer: "c"}.withDefaults()
	if o.Concurrency != DefaultConcurrency || o.Consume != DefaultConsume || o.Group != DefaultGroup {
		t.Fatalf("дефолты: %+v", o)
	}
	if o.Consumer != "c" {
		t.Fatalf("явный Consumer затёрт: %s", o.Consumer)
	}
	o = Options{}.withDefaults()
	if !strings.HasPrefix(o.Consumer, "analyzer-") {
		t.Fatalf("Consumer по умолчанию: %s", o.Consumer)
	}
	if o.Log != nil {
		t.Fatal("нулевой Log остаётся nil (log() подставляет Discard)")
	}
	if o.log() == nil {
		t.Fatal("log() обязан возвращать логгер")
	}
}

// --- processDelivery и цикл Run ------------------------------------------------

func TestProcessDelivery_PoisonMessage(t *testing.T) {
	f := newANFixture(t, anSet(1), Options{})
	del := queue.Delivery{Stream: queue.StreamAnalyze, ID: "bad", Payload: []byte("{не-json")}
	if err := f.w.processDelivery(context.Background(), del); err == nil {
		t.Fatal("ядовитая запись — ошибка (queue.dead через попытки, T-025)")
	}
}

// TestRun_Smoke — цикл: Consume окна → Processing (анализ → Ack);
// после окна пустых чтений цикл ждёт ctx; всё подтверждено.
func TestRun_Smoke(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	f.q.j.ackNotify = make(chan struct{}, 8)
	hash := anTestHash(7)
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN"), hash[:])
	f.q.deliveries = []queue.Delivery{anDelivery("e1"), anDelivery("e2")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{Consumer: "test"}) }()

	// Обе доставки подтверждены после записи в БД (Processing).
	for i := 0; i < 2; i++ {
		select {
		case <-f.q.j.ackNotify:
		case <-time.After(5 * time.Second):
			t.Fatal("доставка не подтверждена")
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
	if got := f.q.ackedSnapshot(); len(got) != 2 || got[0] == got[1] {
		t.Fatalf("подтверждены %v, нужно e1 и e2 (порядок параллелен — не важен)", got)
	}
	// Запись в БД строго до XACK: mark предшествует ack.
	if f.pool.j.indexOf(anEvMark) > f.pool.j.indexOf(anEvAck) {
		t.Fatal("XACK раньше записи в БД")
	}
	if n := countEvents(f.pool.j.snapshot(), anEvUpsert); n != 2 {
		t.Fatalf("upsert выполнен %d раз, нужно 2", n)
	}
}

// TestRun_FailingDelivery — ошибка обработки не останавливает цикл и не
// подтверждает доставку (вернётся XAUTOCLAIM, T-025).
func TestRun_FailingDelivery(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{})
	f.q.j.ackNotify = make(chan struct{}, 8)
	f.pool.queryErr = errors.New("select boom") // каждая доставка падает
	f.q.deliveries = []queue.Delivery{anDelivery("e1")}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f.w, Options{Consumer: "test"}) }()

	// Цикл жив: после неудачной доставки снова читает очередь.
	deadline := time.After(5 * time.Second)
	for f.q.consumesCount() < 2 {
		select {
		case <-deadline:
			t.Fatal("цикл не продолжил чтение после ошибки доставки")
		case <-time.After(time.Millisecond):
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
	if got := f.q.ackedSnapshot(); len(got) != 0 {
		t.Fatalf("упавшая доставка подтверждена: %v", got)
	}
}

func TestRun_Validation(t *testing.T) {
	if err := Run(context.Background(), nil, Options{}); err == nil {
		t.Fatal("Run без воркера — ошибка")
	}
}

// countEvents — сколько раз событие встречается в журнале.
func countEvents(events []anEventKind, k anEventKind) int {
	n := 0
	for _, e := range events {
		if e == k {
			n++
		}
	}
	return n
}
