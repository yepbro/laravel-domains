// Юнит-тесты retention контента и трима jsonb не-находок (T-166; ТЗ 5.7,
// 5.5, 6.1, 3.4) на фейковом pgx-пуле с транзакциями: критерии отбора
// (возраст, Laravel-находка, render_pending/rendered, deleted), точные
// SQL-тексты и параметры, агрегация decref, инварианты refcount под
// сбоем между UPDATE и decref (образец T-044), интерлок q:analyze,
// дренаж пачками, цикл. Поведение против живой БД — интеграционные
// тесты (test/integration).
package content

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

	"crawler/internal/model"
)

// --- фейковый пул с транзакциями retention ------------------------------------

// retentionCall — записанный SQL-вызов с аргументами.
type retentionCall struct {
	sql  string
	args []any
}

// retentionRows — pgx.Rows с заготовленными строками; Scan позиционный,
// в порядке колонок SQL (стиль cleanupFakeRows).
type retentionRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error
}

func retentionResultRows(rows ...[]any) *retentionRows {
	return &retentionRows{rows: rows}
}

func (r *retentionRows) Close() { r.closed = true }

func (r *retentionRows) Err() error { return r.err }

func (r *retentionRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *retentionRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *retentionRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *retentionRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("retentionRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("retentionRows: приёмников %d, колонок %d", len(dest), len(row))
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

func (r *retentionRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *retentionRows) RawValues() [][]byte { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая позиционным Scan.
func (r *retentionRows) Conn() *pgx.Conn      { return nil }
func (r *retentionRows) TypeMap() *pgtype.Map { return nil }

// retentionRowKey — ключ строки subdomain_results (PK, ТЗ 6.1).
type retentionRowKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant int16
}

// retentionRow — строка subdomain_results: возраст, ссылка на контент,
// признак наличия тяжёлых jsonb (любая из колонок) и rendered.
type retentionRow struct {
	key       retentionRowKey
	checkedAt time.Time
	hash      []byte // nil — ссылки нет (T-056)
	jsonb     bool   // есть headers/cookies/tls/redirects/addrs/cname
	rendered  bool
}

// retentionDomKey — ключ домена.
type retentionDomKey struct {
	zone int16
	id   int64
}

// retentionFakeDomain — строка domains, нужная выборке: флаги и статус.
type retentionFakeDomain struct {
	flags  int16
	status int16
}

// retentionFakeContent — строка contents.
type retentionFakeContent struct {
	refc int32
	clen int32
}

// retentionFakePool — RetentionDB без БД: таблицы subdomain_results/
// domains/laravel_domains/contents в памяти, операторы retention —
// по семантике SQL (охраны, RETURNING, GREATEST), транзакции копят
// изменения и применяют их только в Commit (откат честно отменяет).
// Отказы по шагам — краш-окна инвариантов T-044.
type retentionFakePool struct {
	mu       sync.Mutex
	now      time.Time // серверное now() для границ старения
	results  []retentionRow
	domains  map[retentionDomKey]retentionFakeDomain
	laravel  map[retentionDomKey]bool
	contents map[Hash]retentionFakeContent

	selectErr error
	beginErr  error
	purgeErr  error
	decrefErr error
	deleteErr error
	commitErr error

	// onSelect — хук между выборкой и чисткой (гонки перезаписи): вызывается
	// после отбора кандидатов, до возврата строк.
	onSelect func()

	queries   []retentionCall // SELECT'ы через пул
	txCalls   []retentionCall // операторы через транзакции
	begins    int
	commits   int
	rollbacks int
}

func newRetentionFakePool(now time.Time) *retentionFakePool {
	return &retentionFakePool{
		now:      now,
		domains:  make(map[retentionDomKey]retentionFakeDomain),
		laravel:  make(map[retentionDomKey]bool),
		contents: make(map[Hash]retentionFakeContent),
	}
}

// compile-time: фейковый пул — RetentionDB для тестов без БД.
var _ RetentionDB = (*retentionFakePool)(nil)

func (p *retentionFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

// Exec через пул retention не использует — все операторы в транзакции.
func (p *retentionFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec через пул не должен вызываться")
}

// cutoff — граница старения «now() − interval» (Duration.String()
// круглится через ParseDuration — roundtrip-свойство).
func (p *retentionFakePool) cutoff(interval string) time.Time {
	d, err := time.ParseDuration(interval)
	if err != nil {
		panic("retention fake: некорректный interval " + interval)
	}
	return p.now.Add(-d)
}

func (p *retentionFakePool) row(key retentionRowKey) *retentionRow {
	for i := range p.results {
		if p.results[i].key == key {
			return &p.results[i]
		}
	}
	return nil
}

// candidate — проходит ли строка фильтрам выборки (JOIN domains, возраст,
// наличие контента/jsonb, рендер, удалённость, Laravel-находка).
func (p *retentionFakePool) candidate(r *retentionRow, cutoff time.Time) bool {
	d, ok := p.domains[retentionDomKey{zone: r.key.zone, id: r.key.domain}]
	if !ok {
		return false // JOIN domains
	}
	if !r.checkedAt.Before(cutoff) {
		return false
	}
	if r.hash == nil && !r.jsonb {
		return false // чистить нечего
	}
	if !r.rendered && (d.flags&int16(model.FlagRenderPending)) != 0 {
		return false // ждёт рендера (проход доработки T-066 перечитает контент)
	}
	if d.status == int16(model.StatusDeleted) {
		return false
	}
	if p.laravel[retentionDomKey{zone: r.key.zone, id: r.key.domain}] {
		return false
	}
	return true
}

// Query обслуживает выборку кандидатов: фильтры, курсор row-value >,
// порядок PK, LIMIT.
func (p *retentionFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, retentionCall{sql: sql, args: args})
	if sql != retentionSelectSQL {
		return nil, fmt.Errorf("retention fake: неожидаемый SELECT %q", sql)
	}
	if p.selectErr != nil {
		return nil, p.selectErr
	}
	cutoff := p.cutoff(args[0].(string))
	after := retentionRowKey{
		zone:    args[1].(int16),
		domain:  args[2].(int64),
		sub:     args[3].(int16),
		variant: args[4].(int16),
	}
	limit := args[5].(int)

	var sel []*retentionRow
	for i := range p.results {
		r := &p.results[i]
		if !p.candidate(r, cutoff) {
			continue
		}
		if !retentionKeyAfter(r.key, after) {
			continue
		}
		sel = append(sel, r)
	}
	// ORDER BY PK (выборка уже по возрастанию ключа — сортировка копии).
	sorted := make([]*retentionRow, len(sel))
	copy(sorted, sel)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && retentionKeyLess(sorted[j].key, sorted[j-1].key); j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	if len(sorted) > limit {
		sorted = sorted[:limit]
	}
	// Снимок SELECT материализуется ДО хука: перезапись в окне гонки
	// меняет строки после чтения (как в реальном конвейере recheck).
	rows := make([][]any, len(sorted))
	for i, r := range sorted {
		rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, r.key.variant, r.hash}
	}
	if p.onSelect != nil {
		p.onSelect() // гонка перезаписи между выборкой и чисткой
	}
	return retentionResultRows(rows...), nil
}

// retentionKeyLess/After — сравнение ключа строки (row-value, ТЗ 6.1).
func retentionKeyLess(a, b retentionRowKey) bool {
	if a.zone != b.zone {
		return a.zone < b.zone
	}
	if a.domain != b.domain {
		return a.domain < b.domain
	}
	if a.sub != b.sub {
		return a.sub < b.sub
	}
	return a.variant < b.variant
}

func retentionKeyAfter(a, after retentionRowKey) bool { return retentionKeyLess(after, a) }

func (p *retentionFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return &retentionFakeTx{pool: p}, nil
}

// retentionFakeTx — транзакция: чистки/уменьшения копятся в оверлее и
// применяются только в Commit (PostgreSQL видит собственные изменения,
// потому операторы транзакции читают оверлей).
type retentionFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool    *retentionFakePool
	purged  map[retentionRowKey]retentionRow // очищенные строки
	updated map[Hash]retentionFakeContent    // уменьшенные refcount
	deleted map[Hash]struct{}                // удалённые contents
}

// view-доступ к состоянию с учётом собственных изменений транзакции.
func (t *retentionFakeTx) row(key retentionRowKey) *retentionRow {
	if r, ok := t.purged[key]; ok {
		return &r
	}
	return t.pool.row(key)
}

func (t *retentionFakeTx) content(h Hash) (retentionFakeContent, bool) {
	if _, ok := t.deleted[h]; ok {
		return retentionFakeContent{}, false
	}
	if c, ok := t.updated[h]; ok {
		return c, true
	}
	c, ok := t.pool.contents[h]
	return c, ok
}

// Query — операторы чистки с RETURNING (retentionPurgeSQL,
// retentionDecrefSQL).
func (t *retentionFakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
	switch sql {
	case retentionPurgeSQL:
		if t.pool.purgeErr != nil {
			return nil, t.pool.purgeErr
		}
		cutoff := t.pool.cutoff(args[5].(string))
		zones := args[0].([]int16)
		domains := args[1].([]int64)
		subs := args[2].([]int16)
		variants := args[3].([]int16)
		hashes := args[4].([][]byte)
		var out [][]any
		for i := range zones {
			key := retentionRowKey{zone: zones[i], domain: domains[i], sub: subs[i], variant: variants[i]}
			r := t.row(key)
			if r == nil {
				continue // строка исчезла (перепроверка переписала) — охрана
			}
			if !r.checkedAt.Before(cutoff) {
				continue // перезаписана свежим эпизодом — охрана возраста
			}
			if !retentionBytesEqual(r.hash, hashes[i]) {
				continue // контент сменился — охрана content_hash
			}
			if t.purged == nil {
				t.purged = make(map[retentionRowKey]retentionRow)
			}
			clean := *r
			clean.hash, clean.jsonb = nil, false
			t.purged[key] = clean
			out = append(out, []any{hashes[i]}) // RETURNING k.hash
		}
		return retentionResultRows(out...), nil

	case retentionDecrefSQL:
		if t.pool.decrefErr != nil {
			return nil, t.pool.decrefErr
		}
		hashes := args[0].([][]byte)
		counts := args[1].([]int32)
		var out [][]any
		for i := range hashes {
			var h Hash
			copy(h[:], hashes[i])
			c, ok := t.content(h)
			if !ok {
				continue // нет строки contents — Missing (расхождение учёта)
			}
			refc := c.refc - counts[i] // GREATEST(..., 0)
			if refc < 0 {
				refc = 0
			}
			if t.updated == nil {
				t.updated = make(map[Hash]retentionFakeContent)
			}
			t.updated[h] = retentionFakeContent{refc: refc, clen: c.clen}
			out = append(out, []any{hashes[i], refc, c.clen})
		}
		return retentionResultRows(out...), nil

	default:
		return nil, fmt.Errorf("retention fake: неожидаемый tx-SELECT %q", sql)
	}
}

// Exec — удаление contents без ссылок (retentionDeleteSQL).
func (t *retentionFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
	if sql != retentionDeleteSQL {
		return pgconn.CommandTag{}, fmt.Errorf("retention fake: неожидаемый tx-Exec %q", sql)
	}
	if t.pool.deleteErr != nil {
		return pgconn.CommandTag{}, t.pool.deleteErr
	}
	hashes := args[0].([][]byte)
	var n int64
	for _, hb := range hashes {
		var h Hash
		copy(h[:], hb)
		if c, ok := t.content(h); ok && c.refc <= 0 {
			if t.deleted == nil {
				t.deleted = make(map[Hash]struct{})
			}
			t.deleted[h] = struct{}{}
			n++
		}
	}
	return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
}

func (t *retentionFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.commits++
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	for key, r := range t.purged {
		if live := t.pool.row(key); live != nil {
			*live = r
		}
	}
	for h := range t.deleted {
		delete(t.pool.contents, h)
	}
	for h, c := range t.updated {
		if _, ok := t.pool.contents[h]; ok {
			t.pool.contents[h] = c
		}
	}
	return nil
}

func (t *retentionFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.rollbacks++
	return nil
}

// retentionBytesEqual — семантика IS NOT DISTINCT FROM для bytea.
func retentionBytesEqual(a, b []byte) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return string(a) == string(b)
}

// retentionHash16 — тестовый 16-октетный хэш с байтом-заполнителем.
func retentionHash16(b byte) []byte {
	h := make([]byte, HashLen)
	for i := range h {
		h[i] = b
	}
	return h
}

// retentionTestLogger — логгер в буфер (проверка Warn/Info цикла).
func retentionTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// retentionSeed — типовой набор: старая не-находка с контентом и jsonb,
// домен-находка, render_pending, deleted, свежая запись.
func retentionSeedPool(now time.Time) *retentionFakePool {
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{flags: 0, status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 20}] = retentionFakeDomain{flags: 0, status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 30}] = retentionFakeDomain{flags: int16(model.FlagRenderPending), status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 40}] = retentionFakeDomain{flags: int16(model.FlagRenderPending), status: int16(model.StatusAlive)}
	pool.domains[retentionDomKey{zone: 1, id: 50}] = retentionFakeDomain{flags: 0, status: int16(model.StatusDeleted)}
	pool.domains[retentionDomKey{zone: 1, id: 60}] = retentionFakeDomain{flags: 0, status: int16(model.StatusAlive)}
	pool.laravel[retentionDomKey{zone: 1, id: 20}] = true
	pool.results = []retentionRow{
		// 10: обычная не-находка — чистится.
		{key: retentionRowKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xAA), jsonb: true},
		// 20: Laravel-находка — контент хранится всегда.
		{key: retentionRowKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xBB), jsonb: true},
		// 30: render_pending и не rendered — ждёт рендера.
		{key: retentionRowKey{zone: 1, domain: 30, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xCC), jsonb: true},
		// 40: render_pending, но rendered — рендер уже заменил контент.
		{key: retentionRowKey{zone: 1, domain: 40, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xDD), jsonb: true, rendered: true},
		// 50: deleted — территория очистки T-020.
		{key: retentionRowKey{zone: 1, domain: 50, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xEE), jsonb: true},
		// 60: свежая запись — не остарела.
		{key: retentionRowKey{zone: 1, domain: 60, sub: 0, variant: 0}, checkedAt: now.Add(-time.Minute), hash: retentionHash16(0xFF), jsonb: true},
	}
	pool.contents[retentionHash(0xAA)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xBB)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xCC)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xDD)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xEE)] = retentionFakeContent{refc: 1, clen: 7000}
	pool.contents[retentionHash(0xFF)] = retentionFakeContent{refc: 1, clen: 7000}
	return pool
}

// retentionHash — Hash из байта-заполнителя.
func retentionHash(b byte) Hash {
	var h Hash
	for i := range h {
		h[i] = b
	}
	return h
}

// retentionDefaultOpts — типовые параметры прохода с выключенным
// интерлоком (тесты критериев отбора и SQL).
func retentionDefaultOpts() RetentionOptions {
	return RetentionOptions{Retention: DefaultRetention, BatchSize: 100}
}

// --- критерии отбора ---------------------------------------------------------

// TestRetentionOnce_Selection — критерии T-166: чистятся только остаревшие
// записи не-находок, не ждущие рендера, у живых доменов; паспортные
// колонки не восстанавливаются (строка остаётся).
func TestRetentionOnce_Selection(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 2 || stats.Refs != 2 || stats.Contents != 2 || stats.FreedBytes != 2*7000 {
		t.Fatalf("stats = %+v, want Rows=2 Refs=2 Contents=2 FreedBytes=14000 (домены 10 и 40)", stats)
	}
	if stats.BadHashes != 0 || stats.Missing != 0 || stats.Skipped {
		t.Errorf("аномалий быть не должно: %+v", stats)
	}
	for _, dom := range []int64{10, 40} {
		r := pool.row(retentionRowKey{zone: 1, domain: dom, sub: 0, variant: 0})
		if r == nil || r.hash != nil || r.jsonb {
			t.Errorf("домен %d: запись не очищена: %+v", dom, r)
		}
	}
	for _, dom := range []int64{20, 30, 50, 60} {
		r := pool.row(retentionRowKey{zone: 1, domain: dom, sub: 0, variant: 0})
		if r == nil || r.hash == nil || !r.jsonb {
			t.Errorf("домен %d: запись обязана остаться нетронутой: %+v", dom, r)
		}
	}
	if _, ok := pool.contents[retentionHash(0xAA)]; ok {
		t.Error("contents 0xAA (refcount 1 → 0) обязан быть удалён")
	}
	if c, ok := pool.contents[retentionHash(0xBB)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xBB (находка) тронут: %+v", c)
	}
	if c, ok := pool.contents[retentionHash(0xCC)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xCC (render_pending) тронут: %+v", c)
	}
	if c, ok := pool.contents[retentionHash(0xFF)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xFF (свежая) тронут: %+v", c)
	}
}

// TestRetentionOnce_OnlyJSONB — запись без контента (T-056), но с jsonb:
// трим идёт, refcount не трогается, транзакция фиксируется.
func TestRetentionOnce_OnlyJSONB(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results[0].hash = nil // у первой записи контента нет
	_, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	r := pool.row(retentionRowKey{zone: 1, domain: 10, sub: 0, variant: 0})
	if r == nil || r.jsonb {
		t.Errorf("запись без контента: jsonb не зачищен: %+v", r)
	}
	for h, c := range pool.contents {
		if c.refc != 1 {
			t.Errorf("contents %x: refcount изменился без ссылки: %+v", h[:2], c)
		}
	}
}

// TestRetentionOnce_AlreadyClean — пустая выборка (всё чисто): один
// SELECT, транзакция не открывается.
func TestRetentionOnce_AlreadyClean(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results = nil

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Errorf("stats = %+v, want пустой проход", stats)
	}
	if len(pool.queries) != 1 || pool.begins != 0 {
		t.Errorf("обращений = %d (транзакций %d), want один SELECT без транзакции", len(pool.queries), pool.begins)
	}
}

// --- SQL-контракт ------------------------------------------------------------

// TestRetentionOnce_SQLContract — точные тексты операторов и параметры:
// граница старения interval, курсор row-value, LIMIT пачки; ключи и
// прочитанные хэши — в UPDATE; агрегация decref — по хэшам.
func TestRetentionOnce_SQLContract(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	// Оставить двух кандидатов с разными хэшами — проверить пачку.
	keep := []retentionRow{pool.results[0], pool.results[3]}
	pool.results = keep

	if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if len(pool.queries) != 1 {
		t.Fatalf("SELECT'ов = %d, want 1 (одна неполная пачка)", len(pool.queries))
	}
	sel := pool.queries[0]
	if sel.sql != retentionSelectSQL {
		t.Errorf("SQL выборки изменён")
	}
	wantArgs := []any{
		DefaultRetention.String(),
		int16(0), int64(0), int16(0), int16(0), // нулевой курсор первой пачки
		100,
	}
	if !reflect.DeepEqual(sel.args, wantArgs) {
		t.Errorf("args выборки = %v, want %v", sel.args, wantArgs)
	}

	if len(pool.txCalls) != 3 {
		t.Fatalf("операторов транзакции = %d, want 3 (purge, decref, delete)", len(pool.txCalls))
	}
	purge := pool.txCalls[0]
	if purge.sql != retentionPurgeSQL {
		t.Errorf("SQL purge изменён")
	}
	if !reflect.DeepEqual(purge.args[:5], []any{
		[]int16{1, 1}, []int64{10, 40}, []int16{0, 0}, []int16{0, 0},
		[][]byte{retentionHash16(0xAA), retentionHash16(0xDD)},
	}) {
		t.Errorf("args purge (ключи/хэши) = %v", purge.args[:5])
	}
	if purge.args[5] != DefaultRetention.String() {
		t.Errorf("граница purge = %v, want %s", purge.args[5], DefaultRetention)
	}

	decref := pool.txCalls[1]
	if decref.sql != retentionDecrefSQL {
		t.Errorf("SQL decref изменён")
	}
	if !reflect.DeepEqual(decref.args, []any{
		[][]byte{retentionHash16(0xAA), retentionHash16(0xDD)},
		[]int32{1, 1},
	}) {
		t.Errorf("args decref = %v (хэши отсортированы, счётчики по ссылкам)", decref.args)
	}

	del := pool.txCalls[2]
	if del.sql != retentionDeleteSQL {
		t.Errorf("SQL delete изменён")
	}
	if !reflect.DeepEqual(del.args, []any{
		[][]byte{retentionHash16(0xAA), retentionHash16(0xDD)},
	}) {
		t.Errorf("args delete = %v (оба refcount дошли до нуля)", del.args)
	}
}

// TestRetentionOnce_DedupAggregation — один контент у нескольких записей
// (дедупликация, ТЗ 5.7): уменьшение кратно числу срезанных ссылок,
// строка contents умирает только с последней.
func TestRetentionOnce_DedupAggregation(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionRowKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11)},
		{key: retentionRowKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old, hash: retentionHash16(0x11)},
	}
	pool.contents[retentionHash(0x11)] = retentionFakeContent{refc: 2, clen: 5000}

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Refs != 2 || stats.Contents != 1 || stats.FreedBytes != 5000 {
		t.Fatalf("stats = %+v, want Refs=2 Contents=1 FreedBytes=5000", stats)
	}
	if _, ok := pool.contents[retentionHash(0x11)]; ok {
		t.Error("contents с refcount 2 и двумя срезанными ссылками обязан быть удалён")
	}
}

// TestRetentionOnce_SharedContentLives — контент, на который ссылается
// ещё не остаревшая запись, не умирает (refcount > 0 после decref).
func TestRetentionOnce_SharedContentLives(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	// Свежая запись 60 делит контент 0xAA с остаревшей 10.
	pool.results[5].hash = retentionHash16(0xAA)
	pool.contents[retentionHash(0xAA)] = retentionFakeContent{refc: 2, clen: 7000}

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Contents != 1 {
		t.Fatalf("Contents = %d, want 1 (только 0xDD домена 40)", stats.Contents)
	}
	if c, ok := pool.contents[retentionHash(0xAA)]; !ok || c.refc != 1 {
		t.Errorf("contents 0xAA после decref = %+v (ok=%v), want refcount 1 (ссылка свежей записи жива)", c, ok)
	}
}

// --- аномалии учёта ----------------------------------------------------------

// TestRetentionOnce_BadHashes — срезанная ссылка неверной длины: строка
// очищена, refcount по ней не менялся, проход не роняется (poison pill,
// стиль ING-P2-4).
func TestRetentionOnce_BadHashes(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.results[0].hash = make([]byte, 9) // 9 октет вместо 16 (ТЗ 6.1)

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.BadHashes != 1 || stats.Refs != 2 || stats.Rows != 2 {
		t.Fatalf("stats = %+v, want BadHashes=1 при Rows=Refs=2", stats)
	}
	if len(pool.txCalls) != 3 {
		t.Fatalf("операторов = %d, want 3 (purge, decref и delete только по валидному 0xDD)", len(pool.txCalls))
	}
	decref := pool.txCalls[1]
	hashes := decref.args[0].([][]byte)
	if len(hashes) != 1 || !retentionBytesEqual(hashes[0], retentionHash16(0xDD)) {
		t.Errorf("decref по хэшам %v, want только валидный 0xDD", hashes)
	}
	if r := pool.row(retentionRowKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
		t.Errorf("строка с кривым хэшем не очищена: %+v", r)
	}
}

// TestRetentionOnce_Missing — срезанная ссылка без строки contents:
// расхождение учёта — счётчик, не ошибка пачки.
func TestRetentionOnce_Missing(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	delete(pool.contents, retentionHash(0xAA)) // ссылка есть, contents нет

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Missing != 1 {
		t.Fatalf("stats = %+v, want Missing=1", stats)
	}
	if stats.Contents != 1 { // 0xDD домена 40 удалён штатно
		t.Errorf("Contents = %d, want 1", stats.Contents)
	}
}

// --- инварианты refcount под сбоем (T-044) -----------------------------------

// TestRetentionOnce_FailureRollback — краш-окна транзакции пачки: сбой
// любого шага (purge / decref / delete / commit) откатывает всё —
// строки и refcount не расходятся. Сбой именно МЕЖДУ UPDATE и decref
// (decrefErr) — предмет инварианта T-044: срезанная без decref ссылка
// навсегда ломала бы учёт.
func TestRetentionOnce_FailureRollback(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		mut  func(*retentionFakePool)
	}{
		{"сбой purge", func(p *retentionFakePool) { p.purgeErr = errors.New("бд") }},
		{"сбой между UPDATE и decref", func(p *retentionFakePool) { p.decrefErr = errors.New("бд") }},
		{"сбой между decref и delete", func(p *retentionFakePool) { p.deleteErr = errors.New("бд") }},
		{"сбой фиксации", func(p *retentionFakePool) { p.commitErr = errors.New("бд") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := retentionSeedPool(now)
			tc.mut(pool)
			if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err == nil {
				t.Fatalf("ожидается ошибка прохода")
			}
			// Инвариант: ничего не изменилось — строки целы (контент и
			// jsonb на месте), refcount цел, contents живы.
			for _, r := range pool.results {
				if r.hash == nil || !r.jsonb {
					t.Fatalf("строка %v очищена при откаченной транзакции", r.key)
				}
			}
			for h, c := range pool.contents {
				if c.refc != 1 {
					t.Fatalf("contents %x: refcount = %d, want 1 (откат decref)", h[:2], c.refc)
				}
			}
			if len(pool.contents) != 6 {
				t.Fatalf("contents = %d строк, want 6 (ничего не удалено)", len(pool.contents))
			}
		})
	}
}

// TestRetentionOnce_RaceRewritten — перезапись результата между выборкой
// и чисткой (recheck T-030): двойная охрана (возраст + content_hash)
// отсекает чужой эпизод — 0 затронутых строк, ничего не сломано.
func TestRetentionOnce_RaceRewritten(t *testing.T) {
	fresh := func(r *retentionRow) { // перезапись DELETE+INSERT (T-058)
		r.checkedAt = r.checkedAt.Add(3 * DefaultRetention) // fresh now()
	}
	t.Run("свежий эпизод", func(t *testing.T) {
		now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
		pool := retentionSeedPool(now)
		pool.results = pool.results[:1]
		pool.onSelect = func() { fresh(&pool.results[0]) }
		stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
		if err != nil {
			t.Fatalf("RetentionOnce: %v", err)
		}
		if stats.Rows != 0 || stats.Refs != 0 || stats.Batches != 1 {
			t.Fatalf("stats = %+v, want пустая зафиксированная пачка", stats)
		}
		if r := pool.row(retentionRowKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
			t.Fatalf("перезаписанная строка тронута: %+v", r)
		}
	})
	t.Run("контент сменился", func(t *testing.T) {
		now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
		pool := retentionSeedPool(now)
		pool.results = pool.results[:1]
		pool.onSelect = func() { pool.results[0].hash = retentionHash16(0x99) }
		stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
		if err != nil {
			t.Fatalf("RetentionOnce: %v", err)
		}
		if stats.Rows != 0 || stats.Refs != 0 {
			t.Fatalf("stats = %+v, want 0 строк (охрана content_hash)", stats)
		}
	})
}

// --- интерлок q:analyze ------------------------------------------------------

// fakeDepth — источник глубины q:analyze с заготовленным ответом.
type fakeDepth struct {
	mu    sync.Mutex
	n     int64
	err   error
	calls int
}

func (f *fakeDepth) AnalyzeDepth(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.n, f.err
}

// TestRetentionOnce_Interlock — глубина q:analyze у порога: проход
// пропущен, БД не тронута; ниже порога — работа идёт; ошибка чтения —
// ошибка прохода (консервативно); порог ≤ 0 / nil-источник — выключен.
func TestRetentionOnce_Interlock(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	opts := retentionDefaultOpts()
	opts.AnalyzeWatermark = 100

	pool := retentionSeedPool(now)
	depth := &fakeDepth{n: 100}
	opts.Depth = depth
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if !stats.Skipped || stats.Depth != 100 || stats.Rows != 0 {
		t.Fatalf("stats = %+v, want Skipped с Depth=100 без работы", stats)
	}
	if len(pool.queries) != 0 || pool.begins != 0 {
		t.Fatalf("пропущенный проход обязан не трогать БД: SELECT'ов %d, транзакций %d", len(pool.queries), pool.begins)
	}

	pool = retentionSeedPool(now)
	depth = &fakeDepth{n: 99} // строго ниже порога — как гейт T-026
	opts.Depth = depth
	if stats, err := RetentionOnce(context.Background(), pool, opts); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	} else if stats.Skipped || stats.Rows != 2 {
		t.Fatalf("stats = %+v, want работа при глубине ниже порога", stats)
	}

	pool = retentionSeedPool(now)
	depth = &fakeDepth{err: errors.New("redis недоступен")}
	opts.Depth = depth
	if _, err := RetentionOnce(context.Background(), pool, opts); err == nil ||
		!strings.Contains(err.Error(), "интерлок q:analyze") {
		t.Fatalf("ошибка = %v, want контекст интерлока", err)
	}
	if len(pool.queries) != 0 {
		t.Fatalf("ошибка глубины: БД не трогать (SELECT'ов %d)", len(pool.queries))
	}

	// Выключенный интерлок: порог ≤ 0 — источник не опрашивается вовсе.
	pool = retentionSeedPool(now)
	depth = &fakeDepth{n: 1 << 30}
	opts.Depth = depth
	opts.AnalyzeWatermark = 0
	if _, err := RetentionOnce(context.Background(), pool, opts); err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if depth.calls != 0 {
		t.Fatalf("выключенный интерлок не опрашивает глубину (вызовов %d)", depth.calls)
	}
}

// TestRetentionOnce_InterlockMidDrain — интерлок проверяется на каждой
// пачке дренажа: глубина выросла посреди прохода — сделанное остаётся
// (пачки атомарны сами по себе), остаток ждёт следующего прохода.
func TestRetentionOnce_InterlockMidDrain(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	opts := retentionDefaultOpts()
	opts.BatchSize = 1 // пачка = строка: первая чистится, вторая — интерлок
	depth := &fakeDepth{n: 0}
	opts.Depth = depth
	opts.AnalyzeWatermark = 100
	var selects int
	pool.onSelect = func() {
		selects++
		if selects >= 1 {
			depth.mu.Lock()
			depth.n = 150 // анализатор перестал успевать после первой пачки
			depth.mu.Unlock()
		}
	}
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 1 || !stats.Skipped || stats.Depth != 150 {
		t.Fatalf("stats = %+v, want одна пачка до интерлока", stats)
	}
}

// --- дренаж ------------------------------------------------------------------

// TestRetentionOnce_Drain — дренаж пачками: полная пачка → следующая
// с курсором за последней строкой; неполная — стоп; LIMIT — размер пачки.
func TestRetentionOnce_Drain(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	var rows []retentionRow
	for sub := int16(0); sub < 3; sub++ {
		rows = append(rows, retentionRow{
			key:       retentionRowKey{zone: 1, domain: 10, sub: sub, variant: 0},
			checkedAt: old, hash: retentionHash16(byte(0x10 + sub)), jsonb: true,
		})
		pool.contents[retentionHash(byte(0x10+sub))] = retentionFakeContent{refc: 1, clen: 1000}
	}
	pool.results = rows

	opts := retentionDefaultOpts()
	opts.BatchSize = 2
	stats, err := RetentionOnce(context.Background(), pool, opts)
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 3 || stats.Batches != 2 {
		t.Fatalf("stats = %+v, want 3 строки двумя пачками", stats)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("SELECT'ов = %d, want 2", len(pool.queries))
	}
	// Курсор второй пачки — за последним ключом первой (sub=1).
	second := pool.queries[1]
	if want := []any{DefaultRetention.String(), int16(1), int64(10), int16(1), int16(0), 2}; !reflect.DeepEqual(second.args, want) {
		t.Errorf("args второй пачки = %v, want %v", second.args, want)
	}
}

// --- ошибки и отмена ---------------------------------------------------------

func TestRetentionOnceErrors(t *testing.T) {
	if _, err := RetentionOnce(context.Background(), nil, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}

	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	boom := retentionSeedPool(now)
	boom.selectErr = errors.New("бд недоступна")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "выборка кандидатов") {
		t.Errorf("ошибка = %v, want контекст выборки", err)
	}

	boom = retentionSeedPool(now)
	boom.beginErr = errors.New("бд недоступна")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "транзакция") {
		t.Errorf("ошибка = %v, want контекст транзакции", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := retentionSeedPool(now)
	if _, err := RetentionOnce(ctx, canceled, retentionDefaultOpts()); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка = %v, want context.Canceled", err)
	}
}

// --- опции и цикл ------------------------------------------------------------

func TestRetentionOptionsDefaults(t *testing.T) {
	got := RetentionOptions{}.withDefaults()
	want := RetentionOptions{Interval: DefaultRetentionInterval, BatchSize: DefaultRetentionBatch}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("withDefaults = %+v, want %+v", got, want)
	}
	set := RetentionOptions{Interval: time.Minute, BatchSize: 5}.withDefaults()
	if set.Interval != time.Minute || set.BatchSize != 5 {
		t.Errorf("withDefaults затирает заданные значения: %+v", set)
	}
}

// TestRetentionLoop_Disabled — retention = 0: цикл не поднимается,
// возвращает nil (контракт OnRun).
func TestRetentionLoop_Disabled(t *testing.T) {
	var buf bytes.Buffer
	pool := retentionSeedPool(time.Now())
	if err := RetentionLoop(context.Background(), pool, RetentionOptions{Log: retentionTestLogger(&buf)}); err != nil {
		t.Fatalf("RetentionLoop: %v", err)
	}
	if len(pool.queries) != 0 {
		t.Fatalf("SELECT'ов = %d, want 0 (retention выключен)", len(pool.queries))
	}
	if !strings.Contains(buf.String(), "retention контента выключен") {
		t.Errorf("лог = %q, want запись о выключенном retention", buf.String())
	}
}

// TestRetentionLoop — стартовый дренаж сразу, итог прохода уходит хуком
// OnPass (метрики), отмена ctx возвращает nil (контракт OnRun, T-012).
func TestRetentionLoop(t *testing.T) {
	var buf bytes.Buffer
	pool := retentionSeedPool(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	opts := RetentionOptions{
		Retention: DefaultRetention,
		Interval:  time.Hour, // тикера не ждём — проверяем стартовый дренаж
		BatchSize: 100,
		Log:       retentionTestLogger(&buf),
	}
	passed := make(chan RetentionStats, 4)
	opts.OnPass = func(s RetentionStats, _ time.Duration) { passed <- s }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()
	// Стартовый дренаж завершён до отмены — итог детерминирован.
	select {
	case s := <-passed:
		if s.Rows != 2 {
			t.Fatalf("итог стартового прохода = %+v, want 2 строки", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("стартовый проход не завершился")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RetentionLoop: %v", err)
	}
	if !strings.Contains(buf.String(), "цикл retention контента запущен") ||
		!strings.Contains(buf.String(), "проход retention контента") {
		t.Errorf("лог = %q, want старт цикла и итог прохода", buf.String())
	}
	if pool.commits != 1 {
		t.Errorf("фиксаций = %d, want 1", pool.commits)
	}
}

// TestRetentionLoop_NilPool — nil-пул — ошибка запуска (сборка сервиса
// обязана упасть заметно, не молча).
func TestRetentionLoop_NilPool(t *testing.T) {
	if err := RetentionLoop(context.Background(), nil, RetentionOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}
