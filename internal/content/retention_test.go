// Юнит-тесты retention контента и трима jsonb не-находок (T-166; ТЗ 5.7,
// 5.5, 6.1, 3.4) на фейковом pgx-пуле с транзакциями: критерии отбора
// (возраст, Laravel-находка, render_pending/rendered, deleted), точные
// SQL-тексты и параметры, агрегация decref, инварианты refcount под
// сбоем между UPDATE и decref (образец T-044), интерлок q:analyze,
// дренаж пачками, цикл. ТЗ T-271: tx-оболочка закрепления плана выборки
// (SET LOCAL ×4 → SELECT → COMMIT), потолок SelectTimeout, гейт тяжёлых
// сканов и backoff цикла после ошибки. Поведение против живой БД —
// интеграционные тесты (test/integration).
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

	"crawler/internal/db"
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

// retentionRow — строка subdomain_results: возраст, ссылка на контент,
// признак наличия тяжёлых jsonb (любая из колонок) и rendered.
type retentionRow struct {
	key       retentionKey
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
	counters map[string]int64 // crawler_counters (T-175); nil-значения не поддерживаются — нет строки = нет ключа

	selectErr error
	beginErr  error
	purgeErr  error
	decrefErr error
	deleteErr error
	commitErr error
	debtErr   error // отказ чтения счётчика рендер-долга
	// planErr — отказ закрепления плана (SET LOCAL, T-271); selectBlock —
	// «зависший» SELECT выборки: ответ только по ctx-дедлайну (тест
	// ctx-таймаута T-271); selectBeginErr/selectCommitErr — отказ
	// Begin/Commit tx-оболочки выборки («транзакция/фиксация выборки»).
	//
	// beginErr/commitErr — инъекции транзакции ЧИСТКИ, не tx-оболочки
	// выборки (ревью T-271 P2-2): с T-271 первой транзакцией прохода
	// стала оболочка закрепления, и без разведения ролей кейс «сбой
	// фиксации» TestRetentionOnce_FailureRollback падал бы на Commit
	// оболочки — до чистки, выхолащивая инвариант T-044 (проверки
	// «строки/refcount/contents целы» проходили бы тривиально, на пустой
	// пачке). Ветки оболочки — selectBeginErr/selectCommitErr здесь же,
	// кейсы TestRetentionOnceErrors.
	planErr         error
	selectBlock     bool
	selectBeginErr  error
	selectCommitErr error

	// selectDelay — задержка тяжёлой выборки (тест семантики backoff
	// T-273: проход длится дольше тика интервала); selectsAt — метки
	// времени вызовов выборки дренажа (старты проходов).
	selectDelay time.Duration
	selectsAt   []time.Time

	// onSelect — хук между выборкой и чисткой (гонки перезаписи): вызывается
	// после отбора кандидатов, до возврата строк.
	onSelect func()

	queries   []retentionCall // SELECT'ы дренажа/зонда (в tx-оболочке закрепления — T-271; без чтения счётчика)
	debtReads int             // обращения к счётчику рендер-долга
	txCalls   []retentionCall // операторы чистки через транзакции (purge/decref/delete)
	txs       []*retentionFakeTx
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
		// Строка счётчика есть (миграция 000015): ноль долга — дефолт
		// боевой БД после деплоя T-175.
		counters: map[string]int64{"render_pending": 0},
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

func (p *retentionFakePool) row(key retentionKey) *retentionRow {
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

// Query обслуживает чтения пула: счётчик рендер-долга (точечная строка),
// зонд ретраев (PK = ANY ключей, фильтры отбора без курсора) и точечный
// дренаж домена (T-187). Тяжёлая выборка дренажа (retentionSelectSQL) с
// T-271 идёт в транзакции закрепления плана — tx.Query (retentionFakeTx).
func (p *retentionFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch sql {
	case retentionDebtSQL:
		p.debtReads++
		if p.debtErr != nil {
			return nil, p.debtErr
		}
		n, ok := p.counters["render_pending"]
		if !ok {
			return retentionResultRows(), nil // строки счётчика нет
		}
		return retentionResultRows([]any{n}), nil

	case retentionDomainSQL:
		p.queries = append(p.queries, retentionCall{sql: sql, args: args})
		if p.selectErr != nil {
			return nil, p.selectErr
		}
		zone, domain := args[0].(int16), args[1].(int64)
		cutoff := p.cutoff(args[2].(string))
		var sel []*retentionRow
		for i := range p.results {
			r := &p.results[i]
			if r.key.zone != zone || r.key.domain != domain {
				continue // префикс PK (zone_id, domain_id)
			}
			if !p.candidate(r, cutoff) {
				continue
			}
			sel = append(sel, r)
		}
		// ORDER BY PK — детерминированные аргументы пачки.
		sorted := make([]*retentionRow, len(sel))
		copy(sorted, sel)
		for i := 1; i < len(sorted); i++ {
			for j := i; j > 0 && sorted[j].key.less(sorted[j-1].key); j-- {
				sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			}
		}
		rows := make([][]any, len(sorted))
		for i, r := range sorted {
			rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, r.key.variant, r.hash, r.checkedAt}
		}
		return retentionResultRows(rows...), nil

	case retentionProbeSQL:
		p.queries = append(p.queries, retentionCall{sql: sql, args: args})
		cutoff := p.cutoff(args[4].(string))
		zones := args[0].([]int16)
		domains := args[1].([]int64)
		subs := args[2].([]int16)
		variants := args[3].([]int16)
		var rows [][]any
		for i := range zones { // порядок входных ключей (сортирует зонд)
			key := retentionKey{zone: zones[i], domain: domains[i], sub: subs[i], variant: variants[i]}
			r := p.row(key)
			if r == nil || !p.candidate(r, cutoff) {
				continue // не кандидат: чистить нечего / fresh / находка / рендер
			}
			rows = append(rows, []any{r.key.zone, r.key.domain, r.key.sub, r.key.variant, r.hash})
		}
		return retentionResultRows(rows...), nil

	default:
		return nil, fmt.Errorf("retention fake: неожидаемый SELECT %q", sql)
	}
}

// retentionRowLess/After — сравнение строки с курсором по (checked_at, PK),
// row-value порядок выборки (T-185).
func retentionRowLess(a, b *retentionRow) bool {
	if !a.checkedAt.Equal(b.checkedAt) {
		return a.checkedAt.Before(b.checkedAt)
	}
	return a.key.less(b.key)
}

func retentionRowAfter(r *retentionRow, after retentionCursor) bool {
	if !r.checkedAt.Equal(after.checkedAt) {
		return r.checkedAt.After(after.checkedAt)
	}
	return after.key.less(r.key)
}

func (p *retentionFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	// Разведение инъекций по ролям транзакций (ревью T-271 P2-2): первая
	// транзакция прохода — tx-оболочка выборки (selectBeginErr), вторая —
	// транзакция чистки пачки (beginErr). Счётчик корректен на свежем
	// фейке: RetentionOnce открывает оболочку, затем чистку.
	if p.begins == 1 && p.selectBeginErr != nil {
		return nil, p.selectBeginErr
	}
	if p.begins > 1 && p.beginErr != nil {
		return nil, p.beginErr
	}
	tx := &retentionFakeTx{pool: p}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// retentionFakeTx — транзакция: чистки/уменьшения копятся в оверлее и
// применяются только в Commit (PostgreSQL видит собственные изменения,
// потому операторы транзакции читают оверлей). С T-271 транзакция
// обслуживает и tx-оболочку выборки: SET LOCAL закрепления плана
// (retentionFakeTx.Exec) и SELECT пачки (retentionFakeTx.Query); calls —
// упорядоченный журнал операторов транзакции (контракт порядка
// закрепления, стиль retryFakeTx.calls).
type retentionFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool    *retentionFakePool
	purged  map[retentionKey]retentionRow // очищенные строки
	updated map[Hash]retentionFakeContent // уменьшенные refcount
	deleted map[Hash]struct{}             // удалённые contents

	calls      []retentionCall // все операторы транзакции по порядку
	isSelect   bool            // tx-оболочка закрепления (первый Exec — SET LOCAL)
	committed  bool
	rolledBack bool
}

// view-доступ к состоянию с учётом собственных изменений транзакции.
func (t *retentionFakeTx) row(key retentionKey) *retentionRow {
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

// Query — операторы с RETURNING (retentionPurgeSQL, retentionDecrefSQL)
// и тяжёлая выборка дренажа (retentionSelectSQL — с T-271 в транзакции
// закрепления плана; фильтры, курсор row-value, LIMIT — та же семантика).
func (t *retentionFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	// «Зависший» SELECT (тест ctx-таймаута T-271): ответ только по
	// дедлайну выборки, до журналов и семантики.
	if sql == retentionSelectSQL && t.pool.selectBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if sql == retentionSelectSQL {
		// Момент старта прохода и задержка выборки (семантика backoff
		// T-273): сон вне мьютекса — пул остаётся доступен тесту.
		t.pool.mu.Lock()
		t.pool.selectsAt = append(t.pool.selectsAt, time.Now())
		delay := t.pool.selectDelay
		t.pool.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, retentionCall{sql: sql, args: args})
	switch sql {
	case retentionSelectSQL:
		t.pool.queries = append(t.pool.queries, retentionCall{sql: sql, args: args})
		if t.pool.selectErr != nil {
			return nil, t.pool.selectErr
		}
		cutoff := t.pool.cutoff(args[0].(string))
		after := retentionCursor{
			checkedAt: args[1].(time.Time),
			key: retentionKey{
				zone:    args[2].(int16),
				domain:  args[3].(int64),
				sub:     args[4].(int16),
				variant: args[5].(int16),
			},
		}
		limit := args[6].(int)

		var sel []*retentionRow
		for i := range t.pool.results {
			r := &t.pool.results[i]
			if !t.pool.candidate(r, cutoff) {
				continue
			}
			if !retentionRowAfter(r, after) {
				continue
			}
			sel = append(sel, r)
		}
		// ORDER BY (checked_at, PK) — детерминированные пачки (T-185).
		sorted := make([]*retentionRow, len(sel))
		copy(sorted, sel)
		for i := 1; i < len(sorted); i++ {
			for j := i; j > 0 && retentionRowLess(sorted[j], sorted[j-1]); j-- {
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
			rows[i] = []any{r.key.zone, r.key.domain, r.key.sub, r.key.variant, r.hash, r.checkedAt}
		}
		if t.pool.onSelect != nil {
			t.pool.onSelect() // гонка перезаписи между выборкой и чисткой
		}
		return retentionResultRows(rows...), nil

	case retentionPurgeSQL:
		t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
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
			key := retentionKey{zone: zones[i], domain: domains[i], sub: subs[i], variant: variants[i]}
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
				t.purged = make(map[retentionKey]retentionRow)
			}
			clean := *r
			clean.hash, clean.jsonb = nil, false
			t.purged[key] = clean
			out = append(out, []any{hashes[i]}) // RETURNING k.hash
		}
		return retentionResultRows(out...), nil

	case retentionDecrefSQL:
		t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
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

// Exec — SET LOCAL закрепления плана выборки (T-271) и удаление contents
// без ссылок (retentionDeleteSQL).
func (t *retentionFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, retentionCall{sql: sql, args: args})
	switch {
	case sql == retentionPlanNoSeqScanSQL || sql == retentionPlanNoHashJoinSQL ||
		sql == retentionPlanNoMergeJoinSQL || sql == retentionPlanNoBitmapScanSQL ||
		strings.HasPrefix(sql, "SET LOCAL statement_timeout = "):
		// Закрепление плана (T-271): потолок statement_timeout — Sprintf
		// по SelectTimeout, потому префиксом. Исполнивший SET LOCAL —
		// tx-оболочка выборки (метка для commitErr-инъекции чистки).
		t.isSelect = true
		if t.pool.planErr != nil {
			return pgconn.CommandTag{}, t.pool.planErr
		}
		return pgconn.NewCommandTag("SET"), nil
	case sql == retentionDeleteSQL:
		t.pool.txCalls = append(t.pool.txCalls, retentionCall{sql: sql, args: args})
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
	default:
		return pgconn.CommandTag{}, fmt.Errorf("retention fake: неожидаемый tx-Exec %q", sql)
	}
}

func (t *retentionFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	// Разведение инъекций по ролям (ревью T-271 P2-2): tx-оболочка
	// выборки (isSelect — исполненный SET LOCAL) — selectCommitErr,
	// фиксация транзакции чистки — commitErr (инвариант T-044: сбой
	// Commit откатывает purge/decref/delete).
	if t.isSelect && t.pool.selectCommitErr != nil {
		return t.pool.selectCommitErr
	}
	if !t.isSelect && t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.pool.commits++
	t.committed = true
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
	// Откат после Commit — ErrTxClosed, как у pgx (ревью T-271 P3-3):
	// defer Rollback есть у каждой транзакции прохода, успешные проходы
	// откатами не считаются (прод-код ошибку игнорирует — `_ =`).
	if t.committed {
		return pgx.ErrTxClosed
	}
	t.pool.rollbacks++
	t.rolledBack = true
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
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xAA), jsonb: true},
		// 20: Laravel-находка — контент хранится всегда.
		{key: retentionKey{zone: 1, domain: 20, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xBB), jsonb: true},
		// 30: render_pending и не rendered — ждёт рендера.
		{key: retentionKey{zone: 1, domain: 30, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xCC), jsonb: true},
		// 40: render_pending, но rendered — рендер уже заменил контент.
		{key: retentionKey{zone: 1, domain: 40, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xDD), jsonb: true, rendered: true},
		// 50: deleted — территория очистки T-020.
		{key: retentionKey{zone: 1, domain: 50, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0xEE), jsonb: true},
		// 60: свежая запись — не остарела.
		{key: retentionKey{zone: 1, domain: 60, sub: 0, variant: 0}, checkedAt: now.Add(-time.Minute), hash: retentionHash16(0xFF), jsonb: true},
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
		r := pool.row(retentionKey{zone: 1, domain: dom, sub: 0, variant: 0})
		if r == nil || r.hash != nil || r.jsonb {
			t.Errorf("домен %d: запись не очищена: %+v", dom, r)
		}
	}
	for _, dom := range []int64{20, 30, 50, 60} {
		r := pool.row(retentionKey{zone: 1, domain: dom, sub: 0, variant: 0})
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
	r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0})
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
// SELECT в tx-оболочке закрепления плана (T-271), транзакция чистки
// не открывается.
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
	if len(pool.queries) != 1 || pool.begins != 1 {
		t.Errorf("обращений = %d (транзакций %d), want один SELECT в tx-оболочке закрепления", len(pool.queries), pool.begins)
	}
}

// --- SQL-контракт ------------------------------------------------------------

// TestRetentionOnce_SQLContract — точные тексты операторов и параметры:
// граница старения interval, курсор row-value, LIMIT пачки; ключи и
// прочитанные хэши — в UPDATE; агрегация decref — по хэшам. С T-271
// выборка идёт в tx-оболочке закрепления плана: строгий порядок
// BEGIN → SET LOCAL seq/hash/merge/bitmap off → SET LOCAL statement_timeout
// → SELECT → COMMIT (реплика T-191 retry_test.go / T-181 renderq_test.go;
// без закрепления деградировавший план клал PostgreSQL OOM — инцидент
// 08–09.11.2026).
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
		t.Fatalf("SELECT'ов дренажа = %d, want 1 (одна неполная пачка; чтение счётчика долга не в счёт)", len(pool.queries))
	}
	sel := pool.queries[0]
	if sel.sql != retentionSelectSQL {
		t.Errorf("SQL выборки изменён")
	}
	wantArgs := []any{
		DefaultRetention.String(),
		time.Time{}, int16(0), int64(0), int16(0), int16(0), // нулевой курсор первой пачки (время + PK)
		100,
	}
	if !reflect.DeepEqual(sel.args, wantArgs) {
		t.Errorf("args выборки = %v, want %v", sel.args, wantArgs)
	}

	// Оболочка закрепления (T-271): первая транзакция — SET LOCAL ×5 +
	// SELECT, фиксация без отката; тексты — литералы пакета (потолок
	// statement_timeout — дефолт 3600 с T-273: таймаут обязан накрывать
	// первый catch-up-проход с нулевого курсора, а не только штатные
	// пачки замера T-201; bitmap off — доводка T-271: битмап-путь
	// ломает порядок курсора).
	if retentionPlanNoSeqScanSQL != "SET LOCAL enable_seqscan = off" ||
		retentionPlanNoHashJoinSQL != "SET LOCAL enable_hashjoin = off" ||
		retentionPlanNoMergeJoinSQL != "SET LOCAL enable_mergejoin = off" ||
		retentionPlanNoBitmapScanSQL != "SET LOCAL enable_bitmapscan = off" ||
		retentionSelectTimeoutFmt != "SET LOCAL statement_timeout = '%dms'" {
		t.Fatalf("тексты закрепления плана изменены (контракт интеграционного EXPLAIN-теста)")
	}
	if len(pool.txs) != 2 {
		t.Fatalf("транзакций = %d, want 2 (tx-оболочка выборки + tx чистки пачки)", len(pool.txs))
	}
	pin := pool.txs[0]
	wantOrder := []string{
		retentionPlanNoSeqScanSQL,
		retentionPlanNoHashJoinSQL,
		retentionPlanNoMergeJoinSQL,
		retentionPlanNoBitmapScanSQL,
		fmt.Sprintf(retentionSelectTimeoutFmt, DefaultRetentionSelectTimeout.Milliseconds()),
		retentionSelectSQL,
	}
	if len(pin.calls) != len(wantOrder) {
		t.Fatalf("операторов tx-оболочки = %d, want %d (SET LOCAL ×5 + SELECT): %v", len(pin.calls), len(wantOrder), pin.calls)
	}
	for i, sql := range wantOrder {
		if pin.calls[i].sql != sql {
			t.Errorf("шаг %d tx-оболочки = %s, want %s (порядок SET LOCAL ×5 → SELECT)", i, pin.calls[i].sql, sql)
		}
	}
	if !pin.committed || pin.rolledBack {
		t.Errorf("tx-оболочка: committed=%v rolledBack=%v, want фиксация без отката", pin.committed, pin.rolledBack)
	}
	if pool.txs[1].committed != true {
		t.Error("транзакция чистки пачки не зафиксирована")
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

// --- закрепление плана, таймауты, гейт (T-271) ---------------------------------

// TestRetentionOnce_PlanError — сбой закрепления плана (SET LOCAL) —
// ошибка прохода ДО выборки: SELECT не выполнялся, пачка не коммитится,
// чистка не открывается (стиль renderq_test.go TestRequeueOncePlanError).
func TestRetentionOnce_PlanError(t *testing.T) {
	now := time.Date(2026, 11, 8, 20, 43, 0, 0, time.UTC)
	pool := retentionSeedPool(now)
	pool.planErr = errors.New("потеря соединения: unexpected EOF")

	if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "закрепление плана") {
		t.Fatalf("ошибка = %v, want контекст закрепления плана", err)
	}
	if len(pool.queries) != 0 {
		t.Errorf("SELECT'ов = %d, want 0 (закрепление упало до выборки)", len(pool.queries))
	}
	if len(pool.txCalls) != 0 {
		t.Errorf("операторов чистки = %d, want 0", len(pool.txCalls))
	}
}

// TestRetentionOnce_SelectCtxTimeout — зависшая выборка режется
// клиентским ctx-дедлайном SelectTimeout (T-271): серверный
// statement_timeout и ctx — один потолок двумя уровнями; обёрнутая
// ошибка с контекстом «выборка кандидатов», повтор идемпотентен.
func TestRetentionOnce_SelectCtxTimeout(t *testing.T) {
	pool := retentionSeedPool(time.Now())
	pool.selectBlock = true // SELECT не отвечает до отмены ctx

	opts := retentionDefaultOpts()
	opts.SelectTimeout = 40 * time.Millisecond
	if _, err := RetentionOnce(context.Background(), pool, opts); err == nil ||
		!errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "выборка кандидатов") {
		t.Fatalf("ошибка = %v, want обёрнутый context.DeadlineExceeded «выборка кандидатов»", err)
	}
}

// TestRetentionSelectGate — тяжёлая выборка под общим гейтом сканов
// (db.ScanGate, T-271): занятый гейт держит выборку ДО Begin (соединение
// пула не занимается в очереди), освобождение пропускает её; взаимное
// исключение двух держателей — TestScanGate в internal/db.
func TestRetentionSelectGate(t *testing.T) {
	pool := retentionSeedPool(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	gate := make(db.ScanGate, 1)
	if err := gate.Acquire(context.Background()); err != nil {
		t.Fatalf("занятие гейта тестом: %v", err)
	}
	opts := retentionDefaultOpts()
	opts.Gate = gate

	done := make(chan error, 1)
	go func() {
		_, err := retentionSelect(context.Background(), pool, opts, retentionCursor{})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("выборка прошла под занятым гейтом: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	pool.mu.Lock()
	begins := pool.begins
	pool.mu.Unlock()
	if begins != 0 {
		t.Fatalf("Begin под занятым гейтом: транзакций %d, want 0 (гейт до Begin)", begins)
	}
	gate.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("выборка после освобождения гейта: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("выборка не прошла после освобождения гейта")
	}
}

// TestRetentionLoop_Backoff — ошибка прохода переводит тикер в backoff
// (T-271): тик интервала больше не перезапускает проход
// немедленно (инцидент 08–09.11.2026 — проход в только что поднятый
// после OOM-килла PostgreSQL); после паузы проход повторяется, успех
// возвращает штатный интервал.
func TestRetentionLoop_Backoff(t *testing.T) {
	var buf bytes.Buffer
	pool := retentionSeedPool(time.Date(2026, 11, 8, 20, 43, 0, 0, time.UTC))
	pool.mu.Lock()
	pool.selectErr = errors.New("потеря соединения: unexpected EOF")
	pool.mu.Unlock()
	opts := RetentionOptions{
		Retention:    DefaultRetention,
		Interval:     10 * time.Millisecond,  // без backoff — ~10 проходов на 100 мс
		ErrorBackoff: 250 * time.Millisecond, // пауза ошибочного тика
		BatchSize:    100,
		Log:          retentionTestLogger(&buf),
	}
	passes := func() int {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return pool.debtReads // одно чтение счётчика = один стартовавший проход
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()

	// Стартовый проход ошибся; тики минутного... 10-мс интервала не
	// перезапускают его в пределах паузы backoff.
	time.Sleep(80 * time.Millisecond)
	if n := passes(); n != 1 {
		cancel()
		t.Fatalf("проходов за паузу backoff = %d, want 1 (только стартовый; без backoff ~8)", n)
	}
	// Ошибка снята: тик паузы повторяет проход, успех возвращает
	// штатный интервал — проходы идут дальше.
	pool.mu.Lock()
	pool.selectErr = nil
	pool.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for passes() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := passes(); n < 3 {
		cancel()
		t.Fatalf("проходов после паузы = %d, want ≥3 (успех возвращает штатный интервал)", n)
	}
	// Лог читается только после остановки цикла: горутина цикла пишет в
	// тот же буфер (ревью T-271 P2-1 — bytes.Buffer не потокобезопасен).
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("лог = %q, want Warn ошибочного прохода", buf.String())
	}
}

// TestRetentionLoop_BackoffEveryErrorResetsTicker — семантика backoff
// (T-273, главный диагностический пробел T-271): reset тикера после
// КАЖДОЙ ошибки. До T-273 reset выполнялся только на первую (условие
// !backoff): якорем тикера оставался конец первого ошибочного прохода,
// и повторные ошибки шли паузой «backoff − длительность прохода»
// (проход 600 с при backoff 15 мин — реальная пауза 300 с), каденс
// WARN-ов маскировал длительность проходов. Проход здесь длится дольше
// тика интервала (selectDelay 150 мс > interval 40 мс) и ошибается:
// старты проходов обязаны идти с паузой ≥ длительность прохода +
// backoff (гарантия снизу: сон и тикер не заканчиваются раньше срока).
func TestRetentionLoop_BackoffEveryErrorResetsTicker(t *testing.T) {
	pool := retentionSeedPool(time.Date(2026, 11, 9, 14, 13, 0, 0, time.UTC))
	pool.mu.Lock()
	pool.selectErr = errors.New("content: retention: выборка кандидатов: timeout: context deadline exceeded")
	pool.selectDelay = 150 * time.Millisecond
	pool.mu.Unlock()
	opts := RetentionOptions{
		Retention:    DefaultRetention,
		Interval:     40 * time.Millisecond,
		ErrorBackoff: 500 * time.Millisecond,
		BatchSize:    100,
	}
	attempts := func() []time.Time {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return append([]time.Time(nil), pool.selectsAt...)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()

	// Стартовый проход + два повторных за щедрый бюджет (пауза 650 мс).
	deadline := time.Now().Add(5 * time.Second)
	var starts []time.Time
	for {
		starts = attempts()
		if len(starts) >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	if len(starts) < 3 {
		t.Fatalf("стартов прохода = %d, want ≥3 (стартовый + два повторных)", len(starts))
	}
	// Пауза между стартами ≥ проход (150 мс) + backoff (500 мс) − запас
	// на планировщик (50 мс). Прошлая семантика давала ровно backoff
	// (500 мс) между повторными ошибками — якорь тикера не переанкорился.
	const minGap = 150 + 500 - 50
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < minGap*time.Millisecond {
			t.Errorf("пауза между проходами %d→%d = %v, want ≥ %v (проход + backoff: reset после каждой ошибки)",
				i-1, i, gap, minGap*time.Millisecond)
		}
	}
}

// TestRetentionLoop_OnError — хук ошибки прохода (T-273): вызывается на
// каждом неудачном проходе (вместе с Warn), на успешных — нет; счётчик
// ошибок цикла (crawler_content_retention_errors_total) растёт ровно на
// число неудачных проходов — до T-273 ошибки были видны только в
// journalctl (метрики :9104 — нули при 70+ WARN-ах за сутки).
func TestRetentionLoop_OnError(t *testing.T) {
	pool := retentionSeedPool(time.Date(2026, 11, 9, 14, 13, 0, 0, time.UTC))
	pool.mu.Lock()
	pool.selectErr = errors.New("потеря соединения: unexpected EOF")
	pool.mu.Unlock()

	errs := make(chan error, 16)
	opts := RetentionOptions{
		Retention:    DefaultRetention,
		Interval:     20 * time.Millisecond,
		ErrorBackoff: 60 * time.Millisecond,
		BatchSize:    100,
		OnError:      func(err error) { errs <- err },
	}
	count := func() int {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return len(pool.selectsAt)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RetentionLoop(ctx, pool, opts) }()

	// Стартовый проход и первый повтор — ошибки: по вызову хука на each.
	deadline := time.Now().Add(3 * time.Second)
	for count() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Ошибка снята: успешные проходы хук не зовут.
	pool.mu.Lock()
	pool.selectErr = nil
	pool.mu.Unlock()
	errored := count()
	deadline = time.Now().Add(3 * time.Second)
	for count() < errored+2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не завершился после отмены ctx")
	}
	close(errs)
	n := 0
	var last error
	for err := range errs {
		n++
		last = err
	}
	// Вызовов ровно по числу неудачных проходов: errored попыток шли с
	// активной ошибкой (+1 — проход, стартовавший до снятия ошибки, но
	// прочитавший её позже); успешные проходы хука не добавляют.
	if n < errored || n > errored+1 {
		t.Fatalf("вызовов OnError = %d, want %d..%d (по одному на каждый неудачный проход)", n, errored, errored+1)
	}
	if last == nil || !strings.Contains(last.Error(), "unexpected EOF") {
		t.Errorf("OnError получил %v, want ошибку прохода", last)
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
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x11)},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old, hash: retentionHash16(0x11)},
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

// TestRetentionOnce_ClampDesync — клэмп GREATEST при рассинхроне учёта
// (T-265, инвариант И1 автомата refcount, спецификация — doc-комментарий
// pack.go, переход П9): агрегированное срезание больше фактического
// refcount (две остаревшие ссылки на контент со счётчиком 1) не уводит
// счётчик в минус: 1 − 2 клэмпится в 0, строка удаляется как лишившаяся
// последней ссылки (retention минует tombstone), проход не падает.
// Отрицательный refcount отвергал бы и CHECK contents_refcount_check
// (миграция 000019) — клэмп гарантирует, что массовый проход его не
// триггерит. Поведение GREATEST против живой PostgreSQL —
// test/integration (TestContent_RefcountClampDesync).
func TestRetentionOnce_ClampDesync(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	pool := newRetentionFakePool(now)
	old := now.Add(-2 * DefaultRetention)
	pool.domains[retentionDomKey{zone: 1, id: 10}] = retentionFakeDomain{}
	pool.results = []retentionRow{
		{key: retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}, checkedAt: old, hash: retentionHash16(0x22)},
		{key: retentionKey{zone: 1, domain: 10, sub: 1, variant: 0}, checkedAt: old, hash: retentionHash16(0x22)},
	}
	// Рассинхрон: держателей 2, счётчик 1 (ссылка уже снята иным путём).
	pool.contents[retentionHash(0x22)] = retentionFakeContent{refc: 1, clen: 5000}

	stats, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts())
	if err != nil {
		t.Fatalf("RetentionOnce при рассинхроне: %v", err)
	}
	if stats.Refs != 2 || stats.Contents != 1 || stats.FreedBytes != 5000 {
		t.Fatalf("stats = %+v, want Refs=2 Contents=1 FreedBytes=5000 (клэмп 1−2 → 0, строка удалена)", stats)
	}
	if c, ok := pool.contents[retentionHash(0x22)]; ok {
		t.Errorf("contents после клэмпа = %+v, want удалена (refcount дошёл до нуля)", c)
	}
	for _, sub := range []int16{0, 1} {
		r := pool.row(retentionKey{zone: 1, domain: 10, sub: sub, variant: 0})
		if r == nil || r.hash != nil {
			t.Errorf("запись sub=%d не очищена: %+v", sub, r)
		}
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
	if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash != nil {
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
// навсегда ломала бы учёт. Overlay-глубины (ревью T-271 P2-2) —
// «сколько чистка успела наработать в транзакции до сбоя»: без них
// кейс «сбой фиксации» мог падать на Commit tx-оболочки выборки (до
// чистки), и проверки «строки/refcount/contents целы» проходили бы
// тривиально, на пустой пачке — ожидаемые глубины закрепляют шаг сбоя.
func TestRetentionOnce_FailureRollback(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		mut     func(*retentionFakePool)
		purged  int // строки, срезанные до сбоя (overlay tx чистки)
		decrefd int // contents с уменьшенным refcount
		deleted int // contents, намеченные к удалению
	}{
		{"сбой purge", func(p *retentionFakePool) { p.purgeErr = errors.New("бд") }, 0, 0, 0},
		{"сбой между UPDATE и decref", func(p *retentionFakePool) { p.decrefErr = errors.New("бд") }, 2, 0, 0},
		{"сбой между decref и delete", func(p *retentionFakePool) { p.deleteErr = errors.New("бд") }, 2, 2, 0},
		{"сбой фиксации", func(p *retentionFakePool) { p.commitErr = errors.New("бд") }, 2, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := retentionSeedPool(now)
			tc.mut(pool)
			if _, err := RetentionOnce(context.Background(), pool, retentionDefaultOpts()); err == nil {
				t.Fatalf("ожидается ошибка прохода")
			}
			// Структура прохода (T-271): tx-оболочка выборки, затем
			// транзакция чистки; инъекции чистки (ревью P2-2) обязаны
			// падать во второй — сбой в оболочке оставил бы чистку
			// неоткрытой, а проверки ниже — тривиально пустыми.
			if len(pool.txs) != 2 {
				t.Fatalf("транзакций прохода = %d, want 2 (оболочка выборки + чистка)", len(pool.txs))
			}
			tx := pool.txs[1]
			if len(tx.purged) != tc.purged || len(tx.updated) != tc.decrefd || len(tx.deleted) != tc.deleted {
				t.Fatalf("overlay чистки = purge:%d decref:%d delete:%d, want %d/%d/%d — сбой не в шаге кейса",
					len(tx.purged), len(tx.updated), len(tx.deleted), tc.purged, tc.decrefd, tc.deleted)
			}
			if !tx.rolledBack {
				t.Fatal("транзакция чистки не откачена после сбоя")
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
		if r := pool.row(retentionKey{zone: 1, domain: 10, sub: 0, variant: 0}); r.hash == nil || !r.jsonb {
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
			key:       retentionKey{zone: 1, domain: 10, sub: sub, variant: 0},
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
	// Курсор второй пачки — (checked_at, PK) за последней строкой первой
	// (sub=1): время ведёт, PK детерминирует порядок равных отметок.
	second := pool.queries[1]
	want := []any{DefaultRetention.String(), old, int16(1), int64(10), int16(1), int16(0), 2}
	if !reflect.DeepEqual(second.args, want) {
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

	// Отказ Begin транзакции ЧИСТКИ (второй транзакции прохода; первая —
	// tx-оболочка выборки; разведение инъекций, ревью T-271 P2-2):
	// префикс «retention: транзакция:» отличает ветку чистки от
	// «транзакции выборки» — старая проверка Contains("транзакция")
	// маскировала, какая из транзакций отказала.
	boom = retentionSeedPool(now)
	boom.beginErr = errors.New("бд недоступна")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.HasPrefix(err.Error(), "content: retention: транзакция: ") {
		t.Errorf("ошибка = %v, want отказ Begin транзакции чистки («retention: транзакция:»)", err)
	}

	// Отказ Begin tx-оболочки выборки (первая транзакция прохода, T-271).
	boom = retentionSeedPool(now)
	boom.selectBeginErr = errors.New("бд недоступна")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "транзакция выборки") {
		t.Errorf("ошибка = %v, want «транзакция выборки»", err)
	}

	// Отказ Commit tx-оболочки: пачка не закоммичена, чистка не
	// открывается — повтор идемпотентен (курсор не двигался).
	boom = retentionSeedPool(now)
	boom.selectCommitErr = errors.New("бд недоступна")
	if _, err := RetentionOnce(context.Background(), boom, retentionDefaultOpts()); err == nil ||
		!strings.Contains(err.Error(), "фиксация выборки") {
		t.Errorf("ошибка = %v, want «фиксация выборки»", err)
	}
	if len(boom.txs) != 1 || len(boom.txCalls) != 0 {
		t.Errorf("после сбоя фиксации оболочки: транзакций = %d, операторов чистки = %d, want 1/0", len(boom.txs), len(boom.txCalls))
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
	want := RetentionOptions{
		Interval:      DefaultRetentionInterval,
		BatchSize:     DefaultRetentionBatch,
		SelectTimeout: DefaultRetentionSelectTimeout,
		ErrorBackoff:  DefaultRetentionErrorBackoff,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("withDefaults = %+v, want %+v", got, want)
	}
	if DefaultRetentionSelectTimeout != 3600*time.Second {
		t.Errorf("DefaultRetentionSelectTimeout = %s, want 3600s (потолок catch-up-прохода с нулевого курсора, T-273: 600 с убивали первый проход до коммита — вечная петля)", DefaultRetentionSelectTimeout)
	}
	if DefaultRetentionErrorBackoff != 15*time.Minute {
		t.Errorf("DefaultRetentionErrorBackoff = %s, want 15m (инцидентный backoff T-271)", DefaultRetentionErrorBackoff)
	}
	set := RetentionOptions{Interval: time.Minute, BatchSize: 5,
		SelectTimeout: time.Minute, ErrorBackoff: time.Hour}.withDefaults()
	if set.Interval != time.Minute || set.BatchSize != 5 ||
		set.SelectTimeout != time.Minute || set.ErrorBackoff != time.Hour {
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
	// Две фиксации: tx-оболочка закрепления выборки (T-271) + tx чистки.
	if pool.commits != 2 {
		t.Errorf("фиксаций = %d, want 2 (закрепление + чистка)", pool.commits)
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
