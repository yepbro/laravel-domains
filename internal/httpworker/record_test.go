// Юнит-тесты записи результата HTTP-эпизода и публикации q:analyze (T-058;
// ТЗ 5.5 пп. 7, 9, §6.1, §7, §8, 3.4) на фейковых пуле и очереди — без БД,
// Redis и сети. Фейковый пул живёт семантикой таблиц (domains, contents,
// subdomain_results) с транзакциями-снапшотами: проверяются точные SQL и
// параметры, порядок «запись → публикация q:analyze», границы транзакции
// со свопом ссылок (SwapRefTx, T-044), статусы alive/no_http/temp_error,
// охрана deleted (§8/§13.1), идемпотентный повтор доставки, сбои шагов и
// отмена ctx; smoke — цикл Run с цепочкой OnResult «ContentSaver →
// запись → публикация» (сборка cmd/http-worker). Поведение против живых
// PostgreSQL/Redis — интеграционные тесты (T-032, T-046).
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

// --- протокол событий (порядок «запись → публикация → ack») -----------------

// recordEventKind — тип события протокола; порядок проверяется индексами.
type recordEventKind string

const (
	recordEvBegin   recordEventKind = "begin"
	recordEvDomains recordEventKind = "update-domains"
	recordEvDelete  recordEventKind = "delete"
	recordEvSwap    recordEventKind = "swapref"
	recordEvInsert  recordEventKind = "insert"
	recordEvShift   recordEventKind = "zone-shift"     // дельта счётчиков зоны (T-178)
	recordEvChecks  recordEventKind = "checks-bump"    // дельта суточного счётчика проверок (T-179)
	recordEvTempErr recordEventKind = "temp-err-bump"  // дельта суточного числителя temp_error (T-182)
	recordEvMature  recordEventKind = "mature-bump"    // дельта корзины зрелости ретрая (T-202)
	recordEvRean    recordEventKind = "reanalyze-bump" // дельта корзины неанализированных хвостов (T-204)
	recordEvResults recordEventKind = "results-bump"   // дельта часовой корзины темпа (T-203)
	recordEvCommit  recordEventKind = "commit"
	recordEvPublish recordEventKind = "publish"
	recordEvAck     recordEventKind = "ack"
)

// recordJournal — потокобезопасный протокол (цикл Run работает в горутине).
type recordJournal struct {
	mu     sync.Mutex
	events []recordEventKind

	// ackNotify — неблокирующий сигнал на каждое Ack (синхронизация smoke).
	ackNotify chan struct{}
}

func (j *recordJournal) record(k recordEventKind) {
	j.mu.Lock()
	j.events = append(j.events, k)
	j.mu.Unlock()
}

func (j *recordJournal) snapshot() []recordEventKind {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]recordEventKind, len(j.events))
	copy(out, j.events)
	return out
}

func (j *recordJournal) count(k recordEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, e := range j.events {
		if e == k {
			n++
		}
	}
	return n
}

// indexOf возвращает индекс первого события вида (0-based) или −1.
func (j *recordJournal) indexOf(k recordEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e == k {
			return i
		}
	}
	return -1
}

func (j *recordJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- фейковый pgx-пул: три таблицы + транзакции-снапшоты --------------------

// recordDomainKey — ключ строки domains в фейке.
type recordDomainKey struct {
	zone int16
	id   int64
}

// recordFakeDomain — строка domains, которую меняет recordDomainSQL.
type recordFakeDomain struct {
	status       model.Status
	checkState   model.CheckState
	queuedAt     *time.Time
	lastChecked  bool
	tempAttempts int16
	cutoff       bool // бит dead_host_cutoff последнего эпизода (T-164)
	flags        model.Flags
}

// recordFakeRow — строка subdomain_results: колонки INSERT-пачки.
type recordFakeRow struct {
	subdomainID int16
	variant     model.Variant
	scheme      model.Scheme
	httpStatus  *int16
	finalURL    *string
	aliasKind   model.AliasKind
	aliasSub    *int16
	addrs       []byte
	cname       *string
	tls         *string
	redirects   *string
	headers     *string
	cookies     *string
	rendered    bool
	contentHash []byte
	errCode     *int16
	responseMS  *int32
}

// recordFakePool — RecordDB без БД: живая семантика domains (охрана
// status <> deleted у UPDATE), subdomain_results (DELETE … RETURNING,
// INSERT-пачка), contents (refcount с охраной > 0), счётчиков зон
// (recordZoneShiftSQL, T-178), суточного счётчика проверок, его
// temp_error-числителя (recordChecksBumpSQL/recordTempErrBumpSQL,
// T-179/T-182), корзины зрелости ретрая (recordRetryMatureBumpSQL,
// T-202) и часовой корзины темпа (recordResultsBumpSQL, T-203 — имена
// дней/часов вычисляет БД, фейк ведёт по одному значению «строк текущих
// суток/часа»). Транзакция работает
// на снапшоте: Rollback восстанавливает состояние пула, Commit оставляет
// (достаточно для последовательных вызовов записи; конкурентные
// транзакции фейк не моделирует — их сериализует PostgreSQL).
type recordFakePool struct {
	j *recordJournal

	domains map[recordDomainKey]*recordFakeDomain
	rows    map[recordDomainKey][]recordFakeRow
	refc    map[content.Hash]int32
	zones   map[int16]*recordFakeZone
	checks  int64
	tempErr int64
	// retryMature — корзина зрелости ретрая текущих суток (T-202): имя
	// вычисляет БД, фейк ведёт одно значение (стиль суточных строк).
	retryMature int64
	// reanalyze — корзина неанализированных хвостов текущего часа
	// (T-204): имя вычисляет БД, фейк ведёт одно значение.
	reanalyze int64
	// results — часовая корзина темпа текущего часа (T-203): имя
	// вычисляет БД, фейк ведёт одно значение (стиль суточных строк).
	results int64

	begins   int
	txs      int // создано транзакций
	beginErr error

	failExec   map[string]error // сбой Exec по точному SQL
	deleteErr  error
	commitErr  error
	insertTag  int64 // RowsAffected INSERT (0 — честный ответ)
	matureTag  int64 // RowsAffected upsert корзины зрелости (0 — честный ответ)
	reanTag    int64 // RowsAffected upsert корзины хвостов (0 — честный ответ)
	resultsTag int64 // RowsAffected upsert корзины темпа (0 — честный ответ)

	mu sync.Mutex
}

// recordFakeZone — строка zones: колонки инкрементальных счётчиков
// (T-178); сдвигает recordZoneShiftSQL.
type recordFakeZone struct {
	total, pending, queued, checked int64
}

func newRecordFakePool() *recordFakePool {
	return &recordFakePool{
		j:       &recordJournal{},
		domains: make(map[recordDomainKey]*recordFakeDomain),
		rows:    make(map[recordDomainKey][]recordFakeRow),
		refc:    make(map[content.Hash]int32),
		zones:   make(map[int16]*recordFakeZone),
	}
}

// recordSeedZone — зона с заданными счётчиками (T-178); зоны без записи —
// строка справочника отсутствует, сдвиг честно ответит UPDATE 0.
func (p *recordFakePool) recordSeedZone(id int16, total, pending, queued, checked int64) *recordFakeZone {
	z := &recordFakeZone{total: total, pending: pending, queued: queued, checked: checked}
	p.zones[id] = z
	return z
}

// checksValue — значение суточного счётчика проверок (T-179) под замком.
func (p *recordFakePool) checksValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checks
}

// tempErrValue — значение суточного числителя temp_error (T-182) под
// замком.
func (p *recordFakePool) tempErrValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tempErr
}

// retryMatureValue — значение корзины зрелости ретрая (T-202) под замком.
func (p *recordFakePool) retryMatureValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.retryMature
}

// reanalyzeValue — значение корзины неанализированных хвостов (T-204)
// под замком.
func (p *recordFakePool) reanalyzeValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reanalyze
}

// resultsValue — значение часовой корзины темпа (T-203) под замком.
func (p *recordFakePool) resultsValue() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.results
}

var _ RecordDB = (*recordFakePool)(nil)

// recordSeedDomain — домен в очередном состоянии (перед записью эпизода).
func (p *recordFakePool) recordSeedDomain(zone int16, id int64, status model.Status, tempAttempts int16) *recordFakeDomain {
	d := &recordFakeDomain{status: status, checkState: model.CheckStateQueued, tempAttempts: tempAttempts}
	now := time.Now()
	d.queuedAt = &now
	p.domains[recordDomainKey{zone: zone, id: id}] = d
	if _, ok := p.zones[zone]; !ok {
		// Зона справочника для дельт счётчиков (T-178): нулевые counters
		// — тесты, которым нужны значения, сеют явно recordSeedZone.
		p.recordSeedZone(zone, 0, 0, 0, 0)
	}
	return d
}

func (p *recordFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("recordFakePool: CopyFrom не должен вызываться (INSERT — unnest-пачкой)")
}

func (p *recordFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("recordFakePool: Exec вне транзакции")
}

func (p *recordFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("recordFakePool: Query вне транзакции")
}

func (p *recordFakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	// Снапшот: значения-структуры копируются, слайсы строк разделяют
	// указатели неизменяемых полей — транзакция только добавляет/удаляет
	// элементы, не мутирует старые.
	snapDomains := make(map[recordDomainKey]*recordFakeDomain, len(p.domains))
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
	snapZones := make(map[int16]*recordFakeZone, len(p.zones))
	for k, v := range p.zones {
		c := *v
		snapZones[k] = &c
	}
	snapChecks := p.checks
	snapTempErr := p.tempErr
	snapMature := p.retryMature
	snapResults := p.results
	p.txs++
	p.j.record(recordEvBegin)
	return &recordFakeTx{
		pool: p, j: p.j,
		snapDomains: snapDomains, snapRows: snapRows, snapRefc: snapRefc, snapZones: snapZones,
		snapChecks: snapChecks, snapTempErr: snapTempErr, snapMature: snapMature, snapResults: snapResults,
	}, nil
}

// recordFakeTx — транзакция над фейком: операторы применяются к пулу
// сразу, Rollback откатывает к снапшоту, Commit фиксирует.
type recordFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx запись не использует

	pool        *recordFakePool
	j           *recordJournal
	snapDomains map[recordDomainKey]*recordFakeDomain
	snapRows    map[recordDomainKey][]recordFakeRow
	snapRefc    map[content.Hash]int32
	snapZones   map[int16]*recordFakeZone
	snapChecks  int64
	snapTempErr int64
	snapMature  int64
	snapResults int64
	committed   bool
}

// recordFakeRow — pgx.Row предчтения состояний (T-178): два int16
// (status, check_state) или ошибка (pgx.ErrNoRows — строки нет).
type recordStateRow struct {
	status, state int16
	err           error
}

func (r recordStateRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 2 {
		return fmt.Errorf("recordStateRow: приёмников %d, нужно 2", len(dest))
	}
	*dest[0].(*int16) = r.status
	*dest[1].(*int16) = r.state
	return nil
}

// QueryRow — точечное предчтение состояний FOR UPDATE (recordSelectStateSQL,
// T-178): строка домена любой прозрачности статуса — блокировка «есть»;
// отсутствующая строка — ErrNoRows.
func (t *recordFakeTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordSelectStateSQL {
		return recordStateRow{err: fmt.Errorf("recordFakePool: неожидаемый QueryRow: %s", sql)}
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	d, ok := t.pool.domains[key]
	if !ok {
		return recordStateRow{err: pgx.ErrNoRows}
	}
	return recordStateRow{status: int16(d.status), state: int16(d.checkState)}
}

// recordDelRefSQLPrefix — префикс sqlMetaDelRef из content/pack.go
// (T-042): константа не экспортирована, точный текст поддерживается
// вручную; расхождение ломает тест громко — синхронизируйте с pack.go.
const recordDelRefSQLPrefix = "UPDATE contents SET refcount = refcount - 1"

func (t *recordFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if err, ok := t.pool.failExec[sql]; ok {
		return pgconn.CommandTag{}, err
	}
	switch sql {
	case recordDomainSQL:
		zone := args[0].(int16)
		id := args[1].(int64)
		status := model.Status(args[2].(int16))
		cutoff := args[3].(bool) // бит dead_host_cutoff (T-164)
		key := recordDomainKey{zone: zone, id: id}
		d, ok := t.pool.domains[key]
		if !ok || d.status == model.StatusDeleted {
			return pgconn.NewCommandTag("UPDATE 0"), nil // охрана §8/§13.1
		}
		d.status = status
		d.checkState = model.CheckStateDone
		d.queuedAt = nil
		d.lastChecked = true
		if status == model.StatusTempError {
			// CASE из recordDomainSQL: temp_error сохраняет счётчик.
		} else {
			d.tempAttempts = 0
		}
		// CASE flags из recordDomainSQL (T-164): cutoff-эпизод ставит
		// бит, обычный снимает; прочие биты не трогает.
		if cutoff {
			d.flags.Set(model.FlagDeadHostCutoff)
		} else {
			d.flags.Clear(model.FlagDeadHostCutoff)
		}
		d.cutoff = d.flags.IsDeadHostCutoff()
		t.j.record(recordEvDomains)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordInsertSQL:
		if len(args) != 18 {
			return pgconn.CommandTag{}, fmt.Errorf("recordFakePool: INSERT: параметров %d, нужно 18", len(args))
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
				cookies: cookies[i], rendered: false, // литерал false в SELECT
				contentHash: hash[i], errCode: errCode[i], responseMS: responseMS[i],
			})
			if domain[i] == 0 {
				return pgconn.CommandTag{}, errors.New("recordFakePool: INSERT: domain_id 0")
			}
		}
		key := recordDomainKey{zone: zone[0], id: domain[0]}
		t.pool.rows[key] = append(t.pool.rows[key], rows...)
		t.j.record(recordEvInsert)
		if t.pool.insertTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.insertTag)), nil
		}
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", n)), nil
	case recordZoneShiftSQL:
		// Дельта счётчиков зоны по старому состоянию строки (T-178):
		// зона + дельты pending/queued/checked (total запись результата
		// не меняет; нулевые дельты — колонка не тронута).
		zone := args[0].(int16)
		z, ok := t.pool.zones[zone]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil // строка зоны обязательна
		}
		z.pending += args[1].(int64)
		z.queued += args[2].(int64)
		z.checked += args[3].(int64)
		t.j.record(recordEvShift)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case recordChecksBumpSQL:
		// Инкремент суточного счётчика проверок (T-179): upsert строки
		// дня — имя вычисляет БД, фейк ведёт одно значение. RowsAffected
		// upsert'а всегда 1 (строку создаёт конфликт-ветвь).
		t.pool.checks += args[0].(int64)
		t.j.record(recordEvChecks)
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordTempErrBumpSQL:
		// Инкремент суточного числителя temp_error (T-182): та же пара —
		// оператор выполняют только temp_error-эпизоды (вызывающий
		// гарантирует статус), фейк ведёт одно значение строки дня.
		t.pool.tempErr += args[0].(int64)
		t.j.record(recordEvTempErr)
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordRetryMatureBumpSQL:
		// Инкремент корзины зрелости ретрая (T-202): третья пара —
		// выполняют только temp_error-эпизоды (статус гарантирует
		// вызывающий), фейк ведёт одно значение корзины суток;
		// matureTag — страховочная ветка RowsAffected ≠ 1.
		t.pool.retryMature += args[0].(int64)
		t.j.record(recordEvMature)
		if t.pool.matureTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.matureTag)), nil
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordReanalyzeBumpSQL:
		// Инкремент корзины неанализированных хвостов (T-204): дельта —
		// вставленные строки эпизода (0 — оператор не вызывается вовсе:
		// все вставленные строки несут rules_version = NULL), фейк ведёт
		// одно значение корзины часа; reanTag — страховочная ветка
		// RowsAffected ≠ 1.
		t.pool.reanalyze += args[0].(int64)
		t.j.record(recordEvRean)
		if t.pool.reanTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.reanTag)), nil
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case recordResultsBumpSQL:
		// Инкремент часовой корзины темпа (T-203): финальная пара —
		// дельта равна числу вставленных строк эпизода (0 — оператор не
		// вызывается вовсе), фейк ведёт одно значение корзины часа;
		// resultsTag — страховочная ветка RowsAffected ≠ 1.
		t.pool.results += args[0].(int64)
		t.j.record(recordEvResults)
		if t.pool.resultsTag != 0 {
			return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", t.pool.resultsTag)), nil
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	default:
		if !strings.HasPrefix(sql, recordDelRefSQLPrefix) {
			return pgconn.CommandTag{}, fmt.Errorf("recordFakePool: неожидаемый SQL: %s", sql)
		}
		// sqlMetaDelRef (SwapRefTx, T-044): −1 с охраной refcount > 0.
		var h content.Hash
		copy(h[:], args[0].([]byte))
		if t.pool.refc[h] <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil // ErrRefIntegrity у вызывающего
		}
		t.pool.refc[h]--
		t.j.record(recordEvSwap)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
}

func (t *recordFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if sql != recordDeleteSQL {
		return nil, fmt.Errorf("recordFakePool: неожидаемый SQL: %s", sql)
	}
	if t.pool.deleteErr != nil {
		return nil, t.pool.deleteErr
	}
	key := recordDomainKey{zone: args[0].(int16), id: args[1].(int64)}
	old := t.pool.rows[key]
	delete(t.pool.rows, key)
	t.j.record(recordEvDelete)

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

func (t *recordFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.committed = true
	t.j.record(recordEvCommit)
	return nil
}

func (t *recordFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.committed {
		return nil // откат после Commit — no-op
	}
	t.pool.domains, t.pool.rows, t.pool.refc, t.pool.zones = t.snapDomains, t.snapRows, t.snapRefc, t.snapZones
	t.pool.checks = t.snapChecks
	t.pool.tempErr = t.snapTempErr
	t.pool.retryMature = t.snapMature
	t.pool.results = t.snapResults
	return nil
}

// recordFakeRows — pgx.Rows с заготовленными строками (одна колонка
// content_hash); позиционный Scan в *[]byte (стиль resultFakeRows T-039).
type recordFakeRows struct {
	rows [][]any
	i    int
}

func recordFakeRowsOf(rows ...[]any) *recordFakeRows { return &recordFakeRows{rows: rows} }

func (r *recordFakeRows) Close()                                       {}
func (r *recordFakeRows) Err() error                                   { return nil }
func (r *recordFakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *recordFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *recordFakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *recordFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("recordFakeRows: Scan без Next")
	}
	if len(dest) != 1 {
		return fmt.Errorf("recordFakeRows: приёмников %d, колонка одна (content_hash)", len(dest))
	}
	switch d := dest[0].(type) {
	case *[]byte:
		if r.rows[r.i-1][0] == nil {
			*d = nil
		} else {
			*d = r.rows[r.i-1][0].([]byte)
		}
	default:
		return fmt.Errorf("recordFakeRows: приёмник %T, ожидается *[]byte", dest[0])
	}
	return nil
}

func (r *recordFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *recordFakeRows) RawValues() [][]byte    { return nil }
func (r *recordFakeRows) Conn() *pgx.Conn        { return nil }
func (r *recordFakeRows) TypeMap() *pgtype.Map   { return nil }

// --- фейковая очередь -------------------------------------------------------

// recordPublishRec — записанная публикация.
type recordPublishRec struct {
	stream string
	msg    queue.Result
}

// recordFakeQueue — queue.Queue: Publish фиксирует Result-сообщения (и
// может падать по очереди), Consume раздаёт заготовленные доставки один
// раз и дальше паркуется до отмены ctx, Ack фиксируется и сигнализирует.
type recordFakeQueue struct {
	j *recordJournal

	deliveries []queue.Delivery

	publishErrs []error

	mu        sync.Mutex
	publishes []recordPublishRec
	acked     []string
}

var _ queue.Queue = (*recordFakeQueue)(nil)

func (q *recordFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("recordFakeQueue: неожидаемое сообщение %T (нужен queue.Result)", msg)
	}
	q.mu.Lock()
	i := len(q.publishes)
	q.publishes = append(q.publishes, recordPublishRec{stream: stream, msg: res})
	q.mu.Unlock()
	q.j.record(recordEvPublish)
	if i < len(q.publishErrs) && q.publishErrs[i] != nil {
		return "", q.publishErrs[i]
	}
	return fmt.Sprintf("entry-analyze-%d", i+1), nil
}

func (q *recordFakeQueue) Consume(ctx context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
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

func (q *recordFakeQueue) Ack(ctx context.Context, _, _ string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, ids...)
	q.mu.Unlock()
	for range ids {
		q.j.record(recordEvAck)
	}
	q.j.signalAck()
	return nil
}

func (q *recordFakeQueue) publishesSnapshot() []recordPublishRec {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]recordPublishRec, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *recordFakeQueue) ackedSnapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, len(q.acked))
	copy(out, q.acked)
	return out
}

// --- сборщики ---------------------------------------------------------------

// recordTestHash — различимые хэши: recordTestHash(1), (2), …
func recordTestHash(n byte) content.Hash {
	var h content.Hash
	h[0] = n
	return h
}

// recordRichResult — результат с двумя субдоменами и всеми полями записей
// (§5.5 п. 7): апекс — полный набор (TLS, цепочка, заголовки, cookies,
// хэш сохранённого контента), www — алиас-редирект на апекс с CNAME,
// обрезанным телом (ErrBody при живом статусе) и NULL content_hash
// (контент класса хранится у оригинала, T-055/T-056).
func recordRichResult() (*DomainResult, map[string]content.Hash) {
	hApex := recordTestHash(1)
	final := "https://rich.example/"
	aliasTo := int16(0)
	res := &DomainResult{
		ZoneID: 7, DomainID: 91, Name: "rich.example",
		Subdomains: []SubdomainResult{
			{
				SubdomainID: 0, Label: "", Hostname: "rich.example",
				Addrs: []netip.Addr{
					netip.MustParseAddr("192.0.2.1"),
					netip.MustParseAddr("2001:db8::1"),
				},
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 0, Label: "",
					FinalURL: final, Host: "rich.example", Path: "/",
					HTTPStatus: 200,
					Header: http.Header{
						"Content-Type": []string{"text/html; charset=utf-8"},
						"Server":       []string{"nginx"},
					},
					Cookies: []*http.Cookie{
						{Name: "laravel_session", Value: "s3", Path: "/", Secure: true, HttpOnly: true},
						{Name: "XSRF-TOKEN", Value: "tok"},
					},
					TLS: &httpfetch.TLSInfo{
						Valid:             true,
						SubjectCommonName: "rich.example",
						IssuerCommonName:  "R11",
						NotBefore:         time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
						NotAfter:          time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
						DNSNames:          []string{"rich.example", "www.rich.example"},
						ChainLength:       2,
					},
					ResponseMS: 123,
					Redirects: []httpfetch.RedirectStep{
						{URL: "http://rich.example/", Status: 301},
						{URL: final, Status: 200},
					},
					ContentHash:  &hApex,
					StoreContent: true,
					ErrClass:     ErrNone,
				}},
			},
			{
				SubdomainID: 1, Label: "www", Hostname: "www.rich.example",
				Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
				CNAME: "rich.example",
				Records: []Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					SubdomainID: 1, Label: "www",
					FinalURL: final, Host: "rich.example", Path: "/",
					HTTPStatus:       200,
					Header:           http.Header{"Server": []string{"nginx"}},
					ResponseMS:       456,
					Redirects:        []httpfetch.RedirectStep{{URL: "https://www.rich.example/", Status: 301}, {URL: final, Status: 200}},
					Truncated:        true,
					ContentNotStored: true, // обрезанное тело не хранится (T-056)
					AliasKind:        model.AliasRedirect,
					AliasSubdomainID: &aliasTo,
					ErrClass:         ErrBody,
				}},
			},
		},
	}
	// Фейк-хэш: content.HashOf не нужен, важна различимость.
	return res, map[string]content.Hash{"apex": hApex}
}

// recordSilentResult — домен без записей: все проверки промолчали.
func recordSilentResult(timeout bool) *DomainResult {
	class := ErrNetwork
	if timeout {
		class = ErrTimeout
	}
	return &DomainResult{
		ZoneID: 7, DomainID: 92, Name: "silent.example",
		Subdomains: []SubdomainResult{{
			SubdomainID: 0, Label: "", Hostname: "silent.example",
			Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.9")},
			HTTPS: SchemeCheck{Scheme: model.SchemeHTTPS, ErrClass: class},
			HTTP:  SchemeCheck{Scheme: model.SchemeHTTP, ErrClass: class},
		}},
	}
}

func recordRecorder(t *testing.T, pool *recordFakePool, q *recordFakeQueue) *Recorder {
	t.Helper()
	r, err := NewRecorder(pool, q, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	return r
}

// --- полнота полей и порядок операций ----------------------------------------

// TestRecordWritesAllColumns — полный результат: строки subdomain_results
// несут все колонки (§5.5 п. 7), домен — alive/done со сбросом lease и
// temp_attempts, q:analyze — по Result на запись; порядок «запись →
// фиксация → публикация»; адреса упакованы по 16 октетов.
func TestRecordWritesAllColumns(t *testing.T) {
	pool := newRecordFakePool()
	pool.recordSeedDomain(7, 91, model.StatusNew, 3)
	zone := pool.recordSeedZone(7, 10, 2, 3, 5) // счётчики зоны до записи (T-178)
	q := &recordFakeQueue{j: pool.j}
	res, hashes := recordRichResult()

	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}

	// Дельта счётчиков зоны той же транзакцией (T-178, ТЗИ §6.4): домен
	// был queued → queued −1, checked +1, total/pending не тронуты.
	if zone.queued != 2 || zone.checked != 6 || zone.total != 10 || zone.pending != 2 {
		t.Fatalf("счётчики зоны = %+v, want {10 2 2 6} (queued −1, checked +1)", *zone)
	}

	// Суточный счётчик проверок (T-179): один эпизод — +1, а не +2 по
	// числу записей субдомен×вариант (дельта — RowsAffected UPDATE
	// домена, не INSERT-пачки); дельта в той же транзакции — после
	// сдвига зоны, до фиксации. Числитель temp_error (T-182) живым
	// (alive) эпизодом не тронут — оператор выполняют только
	// temp_error-итоги.
	if got := pool.checksValue(); got != 1 {
		t.Fatalf("суточный счётчик = %d, want 1 (один эпизод из двух записей)", got)
	}
	if got := pool.tempErrValue(); got != 0 {
		t.Fatalf("суточный числитель temp_error = %d, want 0 (эпизод alive)", got)
	}
	if shift, checks := pool.j.indexOf(recordEvShift), pool.j.indexOf(recordEvChecks); shift > checks || checks > pool.j.indexOf(recordEvCommit) {
		t.Fatalf("порядок счётчиков: shift=%d checks=%d commit=%d (зона → checks → фиксация)",
			shift, checks, pool.j.indexOf(recordEvCommit))
	}

	dom := pool.domains[recordDomainKey{zone: 7, id: 91}]
	if dom.status != model.StatusAlive || dom.checkState != model.CheckStateDone {
		t.Fatalf("домен: status=%v check_state=%v, ожидалось alive/done", dom.status, dom.checkState)
	}
	if dom.queuedAt != nil || !dom.lastChecked {
		t.Fatalf("домен: queued_at=%v last_checked=%v, ожидался NULL/заполнен", dom.queuedAt, dom.lastChecked)
	}
	if dom.tempAttempts != 0 {
		t.Fatalf("temp_attempts=%d, ожидался сброс в 0 (контракт T-031)", dom.tempAttempts)
	}

	rows := pool.rows[recordDomainKey{zone: 7, id: 91}]
	if len(rows) != 2 {
		t.Fatalf("строк %d, ожидалось 2", len(rows))
	}
	apex, www := rows[0], rows[1]

	if apex.subdomainID != 0 || apex.variant != model.VariantMain || apex.scheme != model.SchemeHTTPS {
		t.Fatalf("апекс: ключ (%d,%v,%v)", apex.subdomainID, apex.variant, apex.scheme)
	}
	if apex.httpStatus == nil || *apex.httpStatus != 200 {
		t.Fatalf("апекс: http_status=%v", apex.httpStatus)
	}
	if apex.finalURL == nil || *apex.finalURL != "https://rich.example/" {
		t.Fatalf("апекс: final_url=%v", apex.finalURL)
	}
	if apex.aliasKind != model.AliasNone || apex.aliasSub != nil {
		t.Fatalf("апекс: алиасы %v/%v", apex.aliasKind, apex.aliasSub)
	}
	// addrs: v4-mapped (::ffff:192.0.2.1) + IPv6, по 16 октетов.
	if len(apex.addrs) != 32 {
		t.Fatalf("апекс: addrs %d байт, ожидалось 32 (2×16)", len(apex.addrs))
	}
	want4 := netip.MustParseAddr("192.0.2.1").As16()
	want6 := netip.MustParseAddr("2001:db8::1").As16()
	if string(apex.addrs[:16]) != string(want4[:]) || string(apex.addrs[16:]) != string(want6[:]) {
		t.Fatalf("апекс: addrs не совпали с упаковкой v4-mapped+IPv6: %x", apex.addrs)
	}
	if apex.cname != nil {
		t.Fatalf("апекс: cname=%v, ожидался NULL", *apex.cname)
	}
	for col, v := range map[string]*string{"tls": apex.tls, "redirects": apex.redirects, "headers": apex.headers, "cookies": apex.cookies} {
		if v == nil {
			t.Fatalf("апекс: %s = NULL, ожидался jsonb", col)
		}
	}
	if !strings.Contains(*apex.tls, `"subject_cn":"rich.example"`) || !strings.Contains(*apex.tls, `"dns_names"`) {
		t.Fatalf("апекс: tls jsonb: %s", *apex.tls)
	}
	if !strings.Contains(*apex.redirects, `"url":"http://rich.example/","status":301`) {
		t.Fatalf("апекс: redirects jsonb: %s", *apex.redirects)
	}
	if !strings.Contains(*apex.headers, `"Content-Type":["text/html; charset=utf-8"]`) {
		t.Fatalf("апекс: headers jsonb: %s", *apex.headers)
	}
	if !strings.Contains(*apex.cookies, `"name":"laravel_session"`) || !strings.Contains(*apex.cookies, `"http_only":true`) {
		t.Fatalf("апекс: cookies jsonb: %s", *apex.cookies)
	}
	if apex.rendered {
		t.Fatal("апекс: rendered=true, рендер — T-067")
	}
	wantApex := hashes["apex"]
	if string(apex.contentHash) != string(wantApex[:]) {
		t.Fatalf("апекс: content_hash %x, ожидался %x", apex.contentHash, wantApex)
	}
	if apex.errCode != nil {
		t.Fatalf("апекс: error=%v, ожидался NULL (ErrNone)", *apex.errCode)
	}
	if apex.responseMS == nil || *apex.responseMS != 123 {
		t.Fatalf("апекс: response_ms=%v", apex.responseMS)
	}

	// www: алиас-редирект на апекс, CNAME, обрезанное тело — код ошибки
	// и NULL-хэш (T-056).
	if www.aliasKind != model.AliasRedirect || www.aliasSub == nil || *www.aliasSub != 0 {
		t.Fatalf("www: алиас %v/%v", www.aliasKind, www.aliasSub)
	}
	if www.cname == nil || *www.cname != "rich.example" {
		t.Fatalf("www: cname=%v", www.cname)
	}
	if www.contentHash != nil {
		t.Fatal("www: content_hash у обрезанного тела, ожидался NULL")
	}
	if www.errCode == nil || *www.errCode != int16(ErrBody) {
		t.Fatalf("www: error=%v, ожидался ErrBody", www.errCode)
	}
	if www.responseMS == nil || *www.responseMS != 456 {
		t.Fatalf("www: response_ms=%v", www.responseMS)
	}
	if len(www.addrs) != 16 {
		t.Fatalf("www: addrs %d байт, ожидалось 16", len(www.addrs))
	}

	// SQL-контракт: литералы схемы и структура операторов.
	if !strings.Contains(recordDomainSQL, "check_state = 2") ||
		!strings.Contains(recordDomainSQL, "queued_at = NULL") ||
		!strings.Contains(recordDomainSQL, "last_checked_at = now()") ||
		!strings.Contains(recordDomainSQL, "t.status <> 6") ||
		!strings.Contains(recordDomainSQL, "temp_attempts = CASE") {
		t.Fatalf("recordDomainSQL разошёлся с контрактом §6.1/T-031:\n%s", recordDomainSQL)
	}
	if !strings.Contains(recordInsertSQL, "false, z.content_hash, z.error, z.response_ms, now(), NULL") ||
		!strings.Contains(recordInsertSQL, "::jsonb[]") || !strings.Contains(recordInsertSQL, "::bytea[]") {
		t.Fatalf("recordInsertSQL разошёлся с колонками §6.1:\n%s", recordInsertSQL)
	}
	// T-210: render_failed_at в INSERT не входит — перезапись эпизода
	// (DELETE + INSERT) сбрасывает маркер «рендер невозможен»: новый
	// контент имеет право на рендер (DEFAULT NULL).
	if strings.Contains(recordInsertSQL, "render_failed_at") {
		t.Fatalf("recordInsertSQL пишет render_failed_at — маркер T-210 обязан сбрасываться перезаписью эпизода (DEFAULT NULL):\n%s", recordInsertSQL)
	}
	if !strings.Contains(recordDeleteSQL, "RETURNING content_hash") {
		t.Fatalf("recordDeleteSQL без RETURNING content_hash (своп ссылок T-044):\n%s", recordDeleteSQL)
	}

	// Публикация: после фиксации транзакции, по сообщению на запись.
	pubs := q.publishesSnapshot()
	if len(pubs) != 2 {
		t.Fatalf("публикаций q:analyze %d, ожидалось 2", len(pubs))
	}
	for i, want := range []struct {
		sub     int16
		variant model.Variant
	}{{0, model.VariantMain}, {1, model.VariantMain}} {
		got := pubs[i]
		if got.stream != queue.StreamAnalyze {
			t.Fatalf("публикация %d: стрим %s", i, got.stream)
		}
		if got.msg.ZoneID != 7 || got.msg.DomainID != 91 || got.msg.SubdomainID != want.sub ||
			got.msg.Variant != want.variant || got.msg.Name != "rich.example" {
			t.Fatalf("публикация %d: %+v", i, got.msg)
		}
	}
	ev := pool.j.snapshot()
	for _, pair := range [][2]recordEventKind{
		{recordEvDomains, recordEvDelete}, {recordEvDelete, recordEvInsert},
		{recordEvInsert, recordEvCommit}, {recordEvCommit, recordEvPublish},
	} {
		if pool.j.indexOf(pair[0]) > pool.j.indexOf(pair[1]) {
			t.Fatalf("порядок событий нарушен: %v после %v (%v)", pair[0], pair[1], ev)
		}
	}
}

// --- статусы домена (§5.5 п. 9, §7) ------------------------------------------

func TestRecordStatuses(t *testing.T) {
	cases := []struct {
		name string
		res  *DomainResult
		want model.Status
	}{
		{"записи есть — alive", func() *DomainResult { r, _ := recordRichResult(); return r }(), model.StatusAlive},
		{"молчит постоянно — no_http", recordSilentResult(false), model.StatusNoHTTP},
		{"молчит с таймаутами — temp_error", recordSilentResult(true), model.StatusTempError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRecordFakePool()
			pool.recordSeedDomain(tc.res.ZoneID, tc.res.DomainID, model.StatusNew, 2)
			q := &recordFakeQueue{j: pool.j}
			if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), tc.res); err != nil {
				t.Fatalf("RecordAndPublish: %v", err)
			}
			dom := pool.domains[recordDomainKey{zone: tc.res.ZoneID, id: tc.res.DomainID}]
			if dom.status != tc.want {
				t.Fatalf("status=%v, ожидался %v", dom.status, tc.want)
			}
			if dom.tempAttempts != 0 && tc.want != model.StatusTempError {
				t.Fatalf("temp_attempts=%d при не-temp_error итоге", dom.tempAttempts)
			}
		})
	}

	t.Run("temp_error сохраняет счётчик повторов", func(t *testing.T) {
		pool := newRecordFakePool()
		res := recordSilentResult(true)
		pool.recordSeedDomain(res.ZoneID, res.DomainID, model.StatusNew, 4)
		q := &recordFakeQueue{j: pool.j}
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err != nil {
			t.Fatalf("RecordAndPublish: %v", err)
		}
		if got := pool.domains[recordDomainKey{zone: res.ZoneID, id: res.DomainID}].tempAttempts; got != 4 {
			t.Fatalf("temp_attempts=%d, temp_error обязан сохранить накопленное (T-031)", got)
		}
	})

	t.Run("без записей строки не вставляются, старые вычищаются", func(t *testing.T) {
		pool := newRecordFakePool()
		res := recordSilentResult(false)
		key := recordDomainKey{zone: res.ZoneID, id: res.DomainID}
		pool.recordSeedDomain(res.ZoneID, res.DomainID, model.StatusAlive, 0)
		oldHash := recordTestHash(9)
		pool.rows[key] = []recordFakeRow{{subdomainID: 0, variant: model.VariantMain, contentHash: oldHash[:]}}
		pool.refc[oldHash] = 1
		q := &recordFakeQueue{j: pool.j}
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err != nil {
			t.Fatalf("RecordAndPublish: %v", err)
		}
		if len(pool.rows[key]) != 0 {
			t.Fatalf("строк после no_http: %d, ожидалось 0 (§3.5 — только последний результат)", len(pool.rows[key]))
		}
		if pool.j.count(recordEvInsert) != 0 {
			t.Fatal("INSERT при пустом результате не нужен")
		}
		if pool.refc[oldHash] != 0 {
			t.Fatalf("refcount старого контента %d, ожидался 0", pool.refc[oldHash])
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Fatalf("публикаций %d — публиковать нечего", got)
		}
	})
}

// TestRecordCutoffFlag — бит dead_host_cutoff последнего эпизода (T-164):
// cutoff-результат ставит бит, обычный снимает (контрольная выборка ложных
// cutoff-ов выбирает домены по маске бита); прочие биты не затрагиваются.
func TestRecordCutoffFlag(t *testing.T) {
	pool := newRecordFakePool()
	key := recordDomainKey{zone: 7, id: 92}
	dom := pool.recordSeedDomain(key.zone, key.id, model.StatusNew, 0)
	dom.flags.Set(model.FlagRenderPending) // чужой бит обязан уцелеть
	q := &recordFakeQueue{j: pool.j}

	cut := recordSilentResult(false)
	cut.Cutoff = true
	if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), cut); err != nil {
		t.Fatalf("RecordAndPublish (cutoff): %v", err)
	}
	if !dom.cutoff || !dom.flags.IsDeadHostCutoff() {
		t.Fatalf("после cutoff-эпизода бит dead_host_cutoff не стоит: flags=%s", dom.flags)
	}
	if !dom.flags.IsRenderPending() {
		t.Fatalf("CASE flags снял чужой бит render_pending: flags=%s", dom.flags)
	}

	// Обычный эпизод (полная проверка при перепроверке T-030) снимает бит.
	full := recordSilentResult(false)
	if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), full); err != nil {
		t.Fatalf("RecordAndPublish (полный эпизод): %v", err)
	}
	if dom.cutoff || dom.flags.IsDeadHostCutoff() {
		t.Fatalf("после полного эпизода бит dead_host_cutoff стоит: flags=%s", dom.flags)
	}
	if !dom.flags.IsRenderPending() {
		t.Fatalf("CASE flags снял чужой бит render_pending: flags=%s", dom.flags)
	}

	// SQL-контракт: бит и маска — литералами модели.
	if !strings.Contains(recordDomainSQL, fmt.Sprintf("(t.flags | %d)::int2", int16(model.FlagDeadHostCutoff))) ||
		!strings.Contains(recordDomainSQL, fmt.Sprintf("(t.flags & %d)::int2", int16(^int16(model.FlagDeadHostCutoff)))) {
		t.Fatalf("recordDomainSQL без CASE битa dead_host_cutoff (T-164):\n%s", recordDomainSQL)
	}
}

// TestRecordTempErrCounter — суточный числитель temp_error (T-182) и
// корзина зрелости ретрая (T-202): инкремент — только у temp_error-итога
// эпизода (та же транзакция, после знаменателя checks:, до фиксации —
// порядок общий с проходом реконсиляции); no_http-итог строку дня не
// трогает; дубль-доставка temp_error-эпизода — ещё одно событие в
// числителе, знаменателе и корзине одновременно (доля устойчива,
// корзину между проходами выравнивает декремент ретрая, абсолюты чинит
// реконсиляция, TZ_slow_dns_share §5 / T-202).
func TestRecordTempErrCounter(t *testing.T) {
	pool := newRecordFakePool()
	q := &recordFakeQueue{j: pool.j}
	r := recordRecorder(t, pool, q)

	// temp_error-эпизод: +1 знаменатель, +1 числитель, +1 корзина;
	// инкременты — между знаменателем и фиксацией, корзина — за
	// числителем (порядок блокировок прохода реконсиляции).
	temp := recordSilentResult(true)
	pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
	if ok, err := r.RecordAndPublish(context.Background(), temp); err != nil || !ok {
		t.Fatalf("temp_error-эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 1 {
		t.Fatalf("после temp_error-эпизода checks = %d, want 1", got)
	}
	if got := pool.tempErrValue(); got != 1 {
		t.Fatalf("после temp_error-эпизода temp_error = %d, want 1", got)
	}
	if got := pool.retryMatureValue(); got != 1 {
		t.Fatalf("после temp_error-эпизода retry_mature = %d, want 1", got)
	}
	checks, tempBump := pool.j.indexOf(recordEvChecks), pool.j.indexOf(recordEvTempErr)
	matureBump, commit := pool.j.indexOf(recordEvMature), pool.j.indexOf(recordEvCommit)
	if tempBump < checks || matureBump < tempBump || matureBump > commit {
		t.Fatalf("порядок: checks=%d temp=%d mature=%d commit=%d (checks → temp_error → retry_mature → фиксация)",
			checks, tempBump, matureBump, commit)
	}

	// no_http-эпизод: знаменатель растёт, числитель и корзина не тронуты —
	// операторы выполняют только temp_error-итоги.
	silent := recordSilentResult(false)
	pool.recordSeedDomain(silent.ZoneID, silent.DomainID, model.StatusNew, 0)
	if ok, err := r.RecordAndPublish(context.Background(), silent); err != nil || !ok {
		t.Fatalf("no_http-эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 2 {
		t.Fatalf("после no_http-эпизода checks = %d, want 2", got)
	}
	if got := pool.tempErrValue(); got != 1 {
		t.Fatalf("после no_http-эпизода temp_error = %d, want 1 (не temp_error-итог не инкрементирует)", got)
	}
	if got := pool.retryMatureValue(); got != 1 {
		t.Fatalf("после no_http-эпизода retry_mature = %d, want 1 (зреют только temp_error-эпизоды)", got)
	}
	if n := pool.j.count(recordEvTempErr); n != 1 {
		t.Fatalf("инкрементов temp_error = %d, want 1", n)
	}
	if n := pool.j.count(recordEvMature); n != 1 {
		t.Fatalf("инкрементов корзины = %d, want 1", n)
	}

	// Дубль-доставка temp_error-эпизода: +1 в числитель, знаменатель и
	// корзину вместе — событие записи, дрейф с живым доменным счётом
	// сводит реконсиляция (T-202).
	if ok, err := r.RecordAndPublish(context.Background(), temp); err != nil || !ok {
		t.Fatalf("повтор temp_error-эпизода: ok=%v err=%v", ok, err)
	}
	if got := pool.checksValue(); got != 3 {
		t.Fatalf("после повтора checks = %d, want 3", got)
	}
	if got := pool.tempErrValue(); got != 2 {
		t.Fatalf("после повтора temp_error = %d, want 2 (по одному событию на запись)", got)
	}
	if got := pool.retryMatureValue(); got != 2 {
		t.Fatalf("после повтора retry_mature = %d, want 2 (по одному событию на запись)", got)
	}
}

// TestRecordResultsCounter — часовая корзина темпа (T-203): дельта —
// вставленные строки эпизода (не эпизоды: домен с двумя записями даёт
// +2, молчаливый — 0 и оператор не выполняется вовсе); порядок — финал
// цепочки дельт счётчика (после retry_mature: и reanalyze:, перед
// фиксацией), общий с проходом реконсиляции.
func TestRecordResultsCounter(t *testing.T) {
	pool := newRecordFakePool()
	q := &recordFakeQueue{j: pool.j}
	r := recordRecorder(t, pool, q)

	// Богатый эпизод: две записи субдомен×вариант → корзины +2/+2
	// (хвосты переанализа T-204 — те же строки с rules_version = NULL,
	// темп T-203 — те же строки как события вставки).
	rich, _ := recordRichResult()
	pool.recordSeedDomain(rich.ZoneID, rich.DomainID, model.StatusNew, 0)
	if ok, err := r.RecordAndPublish(context.Background(), rich); err != nil || !ok {
		t.Fatalf("богатый эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 2 {
		t.Fatalf("после богатого эпизода results = %d, want 2 (две вставленные строки)", got)
	}
	if got := pool.reanalyzeValue(); got != 2 {
		t.Fatalf("после богатого эпизода reanalyze = %d, want 2 (каждая вставленная строка — хвост до штампа, T-204)", got)
	}
	resultsBump, commit := pool.j.indexOf(recordEvResults), pool.j.indexOf(recordEvCommit)
	matureBump := pool.j.indexOf(recordEvMature)
	reanBump := pool.j.indexOf(recordEvRean)
	if resultsBump > commit || (matureBump >= 0 && resultsBump < matureBump) || reanBump > resultsBump {
		t.Fatalf("порядок: mature=%d reanalyze=%d results=%d commit=%d (… → retry_mature: → reanalyze: → results: → фиксация)",
			matureBump, reanBump, resultsBump, commit)
	}

	// Молчаливый эпизод (temp_error, без записей): строк нет — операторы
	// корзин не выполняются вовсе (нулевые upsert-проходы не греют
	// блокировки часов).
	silent := recordSilentResult(true)
	pool.recordSeedDomain(silent.ZoneID, silent.DomainID, model.StatusNew, 1)
	if ok, err := r.RecordAndPublish(context.Background(), silent); err != nil || !ok {
		t.Fatalf("молчаливый эпизод: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 2 {
		t.Fatalf("после молчаливого эпизода results = %d, want 2 (эпизод без строк корзину не трогает)", got)
	}
	if got := pool.reanalyzeValue(); got != 2 {
		t.Fatalf("после молчаливого эпизода reanalyze = %d, want 2 (эпизод без строк корзину не трогает)", got)
	}
	if n := pool.j.count(recordEvResults); n != 1 {
		t.Fatalf("инкрементов корзины темпа = %d, want 1 (нулевая дельта не выполняется)", n)
	}
	if n := pool.j.count(recordEvRean); n != 1 {
		t.Fatalf("инкрементов корзины хвостов = %d, want 1 (нулевая дельта не выполняется)", n)
	}

	// Повтор богатого эпизода (дубль-доставка): ещё +2 события вставки в
	// обе корзины — живое окно насчитало бы те же 2 строки, разницу
	// семантик сводит реконсиляция (потоковая, как у суточных строк).
	// Повторная доставка переигрывает и сохранение контента (стиль
	// TestRecordIdempotentRepeat): повторный Put даёт +1 refcount.
	_, hashes := recordRichResult()
	pool.refc[hashes["apex"]] = 2
	if ok, err := r.RecordAndPublish(context.Background(), rich); err != nil || !ok {
		t.Fatalf("повтор богатого эпизода: ok=%v err=%v", ok, err)
	}
	if got := pool.resultsValue(); got != 4 {
		t.Fatalf("после повтора results = %d, want 4 (по событиям вставки на доставку)", got)
	}
	if got := pool.reanalyzeValue(); got != 4 {
		t.Fatalf("после повтора reanalyze = %d, want 4 (по событиям вставки на доставку)", got)
	}
}

// TestRecordReanalyzeCounterErrors — страховочная ветка RowsAffected ≠ 1
// upsert корзины хвостов (T-204): ошибка шага откатывает транзакцию
// целиком (строк и статуса нет), доставка вернётся повтором.
func TestRecordReanalyzeCounterErrors(t *testing.T) {
	pool := newRecordFakePool()
	pool.reanTag = 2 // чужой SQL: затронуто 2 строки
	rich, _ := recordRichResult()
	pool.recordSeedDomain(rich.ZoneID, rich.DomainID, model.StatusNew, 0)
	q := &recordFakeQueue{j: pool.j}

	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), rich)
	if err == nil || !strings.Contains(err.Error(), "корзина хвостов переанализа") {
		t.Fatalf("ошибка = %v, want контекст корзины хвостов", err)
	}
	if ok {
		t.Fatal("при сбое оператора результат не считается записанным")
	}
	if _, exists := pool.rows[recordDomainKey{zone: rich.ZoneID, id: rich.DomainID}]; exists {
		t.Fatal("строки эпизода записаны при сбое корзины (откат §3.4)")
	}
}

// --- охрана deleted (§8/§13.1) ----------------------------------------------

func TestRecordDeletedGuard(t *testing.T) {
	pool := newRecordFakePool()
	res, _ := recordRichResult()
	key := recordDomainKey{zone: res.ZoneID, id: res.DomainID}
	dom := pool.recordSeedDomain(res.ZoneID, res.DomainID, model.StatusAlive, 1)
	dom.status = model.StatusDeleted
	pool.rows[key] = []recordFakeRow{{subdomainID: 0, variant: model.VariantMain}}
	q := &recordFakeQueue{j: pool.j}

	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
	if err != nil {
		t.Fatalf("удалённый домен — не ошибка: %v", err)
	}
	if ok {
		t.Fatal("запись обязана сообщить «не записано»")
	}
	if dom.status != model.StatusDeleted || dom.checkState != model.CheckStateQueued {
		t.Fatalf("домен изменён: %v/%v", dom.status, dom.checkState)
	}
	if len(pool.rows[key]) != 1 {
		t.Fatalf("старые строки тронуты (%d), удалённому результат не пишется", len(pool.rows[key]))
	}
	if pool.j.count(recordEvInsert) != 0 || pool.j.count(recordEvDelete) != 0 {
		t.Fatal("транзакция обязана остановиться на охране UPDATE")
	}
	// Удалённому результат не пишется — суточный счётчик тоже не растёт
	// (T-179: дельта только при RowsAffected > 0 UPDATE, ТЗИ §6.1.3);
	// числитель temp_error — тем более (T-182).
	if pool.j.count(recordEvChecks) != 0 || pool.checksValue() != 0 ||
		pool.j.count(recordEvTempErr) != 0 || pool.tempErrValue() != 0 {
		t.Fatal("суточные счётчики тронуты при охране deleted")
	}
	if got := len(q.publishesSnapshot()); got != 0 {
		t.Fatalf("публикаций %d — без записи публиковать нельзя", got)
	}

	t.Run("отсутствующий домен — тот же путь", func(t *testing.T) {
		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), &DomainResult{
			ZoneID: 7, DomainID: 999, Name: "ghost.example",
			Subdomains: []SubdomainResult{{
				SubdomainID: 0, Addrs: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
				Records: []Record{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}},
			}},
		})
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v, ожидалось false/nil", ok, err)
		}
	})
}

// --- перезапись + SwapRef (T-044/T-056) ---------------------------------------

func TestRecordRewriteSwapsRefs(t *testing.T) {
	pool := newRecordFakePool()
	res, _ := recordRichResult()
	key := recordDomainKey{zone: 7, id: 91}
	pool.recordSeedDomain(7, 91, model.StatusAlive, 0)

	// Старая проверка: апекс с контентом H1, www с контентом H2
	// (у старой версии записи алиас мог хранить свой блок).
	h1, h2 := recordTestHash(1), recordTestHash(2)
	pool.refc[h1], pool.refc[h2] = 1, 1
	oldFinal := "https://old.rich.example/"
	pool.rows[key] = []recordFakeRow{
		{subdomainID: 0, variant: model.VariantMain, contentHash: h1[:], finalURL: &oldFinal},
		{subdomainID: 1, variant: model.VariantMain, contentHash: h2[:]},
		{subdomainID: 1, variant: model.VariantHTTP, scheme: model.SchemeHTTP}, // ключ, которого нет в новом итоге
	}
	// Новый контент апекса уже записан Put-ом (T-056): +1 от Put.
	h3 := recordTestHash(3)
	pool.refc[h3] = 1

	q := &recordFakeQueue{j: pool.j}
	ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
	if err != nil || !ok {
		t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
	}

	if pool.refc[h1] != 0 || pool.refc[h2] != 0 {
		t.Fatalf("старые ссылки: H1=%d H2=%d, ожидался 0/0 (строка мёртвая — T-042)", pool.refc[h1], pool.refc[h2])
	}
	if pool.refc[h3] != 1 {
		t.Fatalf("refcount нового контента %d, ожидался 1 (+1 Put уже был, −новых не нужно)", pool.refc[h3])
	}
	rows := pool.rows[key]
	if len(rows) != 2 {
		t.Fatalf("строк %d — перезапись удаляет и исчезнувшие ключи (§3.5)", len(rows))
	}
	for _, r := range rows {
		if r.finalURL != nil && *r.finalURL == oldFinal {
			t.Fatal("старая строка пережила перезапись")
		}
	}
	// Порядок операторов: −1 старых — до INSERT, всё после UPDATE домена.
	for _, pair := range [][2]recordEventKind{
		{recordEvDomains, recordEvSwap}, {recordEvSwap, recordEvInsert}, {recordEvInsert, recordEvCommit},
	} {
		if pool.j.indexOf(pair[0]) > pool.j.indexOf(pair[1]) {
			t.Fatalf("порядок нарушен: %v после %v", pair[0], pair[1])
		}
	}
}

func TestRecordIdempotentRepeat(t *testing.T) {
	pool := newRecordFakePool()
	res, hashes := recordRichResult()
	hApex := hashes["apex"]
	key := recordDomainKey{zone: 7, id: 91}
	pool.recordSeedDomain(7, 91, model.StatusNew, 0)
	// Повторная доставка переигрывает и сохранение (T-056): повторный Put
	// того же контента даёт лишний +1.
	pool.refc[hApex] = 1
	q := &recordFakeQueue{j: pool.j}
	r := recordRecorder(t, pool, q)

	if ok, err := r.RecordAndPublish(context.Background(), res); err != nil || !ok {
		t.Fatalf("первая запись: ok=%v err=%v", ok, err)
	}
	if pool.refc[hApex] != 1 {
		t.Fatalf("после первой записи refcount=%d, ожидался 1", pool.refc[hApex])
	}

	// Recheck-сценарий: Put добавил ссылку заново (refcount 2), перезапись
	// снимает ссылку старой строки — итог сбалансирован.
	pool.refc[hApex] = 2
	if ok, err := r.RecordAndPublish(context.Background(), res); err != nil || !ok {
		t.Fatalf("повторная запись: ok=%v err=%v (идемпотентность, ТЗ 3.4)", ok, err)
	}
	if pool.refc[hApex] != 1 {
		t.Fatalf("refcount после повтора=%d, двойное снятие ушло бы в минус (T-044)", pool.refc[hApex])
	}
	if len(pool.rows[key]) != 2 {
		t.Fatalf("строк %d — повтор обязан перезаписать, не дублировать", len(pool.rows[key]))
	}
	// Повтор после успешной записи публикует q:analyze заново: окно
	// «записано, не опубликовано» закрывается повторной доставкой.
	if got := len(q.publishesSnapshot()); got != 4 {
		t.Fatalf("публикаций суммарно %d, ожидалось 4 (2×2)", got)
	}
	// Суточный счётчик считает события записи (T-179, TZ_slow_done_today
	// §5.1): повторная доставка — ещё одно событие, +1 (не ×2 на
	// доставку); сходят к живому окну реконсиляцией (интеграционный
	// сценарий — TestCounters_RecordDoubleDelivery).
	if got := pool.checksValue(); got != 2 {
		t.Fatalf("суточный счётчик = %d, want 2 (по одному событию на запись)", got)
	}
}

// --- транзакционность и сбои шагов -------------------------------------------

func TestRecordTransactionFailures(t *testing.T) {
	res, _ := recordRichResult()
	seed := func(t *testing.T) (*recordFakePool, *recordFakeQueue) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		return pool, &recordFakeQueue{j: pool.j}
	}
	// assertRolledBack проверяет откат: домен в исходном состоянии, строк
	// oldRows (сброс к снапшоту), публикаций нет.
	assertRolledBack := func(t *testing.T, pool *recordFakePool, q *recordFakeQueue, err error, oldRows int) {
		t.Helper()
		if err == nil {
			t.Fatal("ожидалась ошибка шага")
		}
		key := recordDomainKey{zone: 7, id: 91}
		if dom := pool.domains[key]; dom.checkState != model.CheckStateQueued || dom.status != model.StatusNew {
			t.Fatalf("домен изменён при сбое: %v/%v — транзакция обязана откатиться", dom.status, dom.checkState)
		}
		if len(pool.rows[key]) != oldRows {
			t.Fatalf("строк %d после сбоя, ожидалось %d — откат к снапшоту не сработал", len(pool.rows[key]), oldRows)
		}
		if got := len(q.publishesSnapshot()); got != 0 {
			t.Fatalf("публикаций %d при незаписанном результате", got)
		}
	}

	t.Run("Begin", func(t *testing.T) {
		pool, q := seed(t)
		pool.beginErr = errors.New("соединение недоступно")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("UPDATE domains", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExec = map[string]error{recordDomainSQL: errors.New("блокировка")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("DELETE старых строк", func(t *testing.T) {
		pool, q := seed(t)
		pool.deleteErr = errors.New("сбой чтения")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("INSERT", func(t *testing.T) {
		pool, q := seed(t)
		pool.failExec = map[string]error{recordInsertSQL: errors.New("нарушение PK")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("суточный счётчик проверок", func(t *testing.T) {
		// T-179: сбой инкремента откатывает транзакцию целиком —
		// результат и счётчик меняются только вместе.
		pool, q := seed(t)
		pool.failExec = map[string]error{recordChecksBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if got := pool.checksValue(); got != 0 {
			t.Fatalf("суточный счётчик = %d после отката, want 0", got)
		}
	})
	t.Run("суточный числитель temp_error", func(t *testing.T) {
		// T-182: сбой инкремента числителя откатывает и результат, и
		// знаменатель — пара счётчиков меняется только вместе с записью.
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.failExec = map[string]error{recordTempErrBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		dom := pool.domains[recordDomainKey{zone: temp.ZoneID, id: temp.DomainID}]
		if dom.status != model.StatusNew || dom.checkState != model.CheckStateQueued {
			t.Fatalf("temp_error-домен изменён при сбое числителя: %v/%v — транзакция обязана откатиться",
				dom.status, dom.checkState)
		}
		if got := pool.checksValue(); got != 0 {
			t.Fatalf("суточный счётчик = %d после отката на числителе, want 0", got)
		}
		if got := pool.tempErrValue(); got != 0 {
			t.Fatalf("суточный числитель temp_error = %d после отката, want 0", got)
		}
	})
	t.Run("корзина зрелости ретрая", func(t *testing.T) {
		// T-202: сбой инкремента корзины откатывает транзакцию целиком —
		// эпизод без корзины открыл бы guard ретрая на сутки меньше.
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.failExec = map[string]error{recordRetryMatureBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		dom := pool.domains[recordDomainKey{zone: temp.ZoneID, id: temp.DomainID}]
		if dom.status != model.StatusNew || dom.checkState != model.CheckStateQueued {
			t.Fatalf("temp_error-домен изменён при сбое корзины: %v/%v — транзакция обязана откатиться",
				dom.status, dom.checkState)
		}
		if got := pool.retryMatureValue(); got != 0 {
			t.Fatalf("корзина зрелости = %d после отката, want 0", got)
		}
	})
	t.Run("корзина зрелости: RowsAffected ≠ 1", func(t *testing.T) {
		// T-202, ревью P3-2: страховочная ветка upsert'а — «молчаливая
		// потеря дельты держала бы guard ретрая закрытым», ответ ≠ 1
		// обязан ошибкой и откатом (молчание — баг счётчика).
		temp := recordSilentResult(true)
		pool, q := seed(t)
		pool.recordSeedDomain(temp.ZoneID, temp.DomainID, model.StatusNew, 1)
		pool.matureTag = 2 // INSERT 0 2 — чужой SQL/распад протокола
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), temp)
		assertRolledBack(t, pool, q, err, 0)
		if err == nil || !strings.Contains(err.Error(), "затронуто 2 строк (upsert корзины суток обязан 1)") {
			t.Fatalf("ошибка = %v, want RowsAffected-гард корзины зрелости", err)
		}
		if got := pool.retryMatureValue(); got != 0 {
			t.Fatalf("корзина зрелости = %d после отката, want 0", got)
		}
	})
	t.Run("часовая корзина темпа", func(t *testing.T) {
		// T-203: сбой инкремента корзины откатывает транзакцию целиком —
		// результат и корзина меняются только вместе (темп занижался бы
		// молча).
		pool, q := seed(t)
		pool.failExec = map[string]error{recordResultsBumpSQL: errors.New("счётчики недоступны")}
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if got := pool.resultsValue(); got != 0 {
			t.Fatalf("часовая корзина темпа = %d после отката, want 0", got)
		}
	})
	t.Run("часовая корзина темпа: RowsAffected ≠ 1", func(t *testing.T) {
		// T-203: страховочная ветка upsert'а (стиль корзины зрелости,
		// ревью T-202 P3-2) — молчаливая потеря дельты занижала бы темп.
		pool, q := seed(t)
		pool.resultsTag = 2 // INSERT 0 2 — чужой SQL/распад протокола
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
		if err == nil || !strings.Contains(err.Error(), "затронуто 2 строк (upsert корзины часа обязан 1)") {
			t.Fatalf("ошибка = %v, want RowsAffected-гард корзины темпа", err)
		}
		if got := pool.resultsValue(); got != 0 {
			t.Fatalf("часовая корзина темпа = %d после отката, want 0", got)
		}
	})
	t.Run("Commit", func(t *testing.T) {
		pool, q := seed(t)
		pool.commitErr = errors.New("сервер упал на фиксации")
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		assertRolledBack(t, pool, q, err, 0)
	})
	t.Run("ErrRefIntegrity откатывает всё", func(t *testing.T) {
		pool, q := seed(t)
		h := recordTestHash(5)
		pool.rows[recordDomainKey{zone: 7, id: 91}] = []recordFakeRow{
			{subdomainID: 0, variant: model.VariantMain, contentHash: h[:]},
		}
		// refcount хэша нулевой (мёртвая строка) — учёт разошёлся.
		_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if !errors.Is(err, content.ErrRefIntegrity) {
			t.Fatalf("ожидался ErrRefIntegrity: %v", err)
		}
		assertRolledBack(t, pool, q, err, 1)
		// Старая строка цела: удаление откатилось вместе с транзакцией.
		if rows := pool.rows[recordDomainKey{zone: 7, id: 91}]; len(rows) != 1 || string(rows[0].contentHash) != string(h[:]) {
			t.Fatalf("старая строка обязана пережить откат: %+v", rows)
		}
	})
	t.Run("отмена ctx", func(t *testing.T) {
		pool, q := seed(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := recordRecorder(t, pool, q).RecordAndPublish(ctx, res)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ожидалась отмена ctx: %v", err)
		}
		assertRolledBack(t, pool, q, err, 0)
	})
}

// --- публикация: после записи, ошибки не молчат -------------------------------

func TestRecordPublishFailures(t *testing.T) {
	t.Run("сбой публикации — результат записан, доставка вернётся", func(t *testing.T) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		q := &recordFakeQueue{j: pool.j, publishErrs: []error{nil, errors.New("Redis отвалился")}}
		res, _ := recordRichResult()

		ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "публикация q:analyze") {
			t.Fatalf("ожидалась ошибка публикации: %v", err)
		}
		if !ok {
			t.Fatal("запись состоялась до публикации — recorded=true")
		}
		if len(pool.rows[recordDomainKey{zone: 7, id: 91}]) != 2 {
			t.Fatal("строки обязаны остаться: публикация повторной доставкой допишет хвост")
		}
	})
	t.Run("первое же сообщение не уходит", func(t *testing.T) {
		pool := newRecordFakePool()
		pool.recordSeedDomain(7, 91, model.StatusNew, 0)
		q := &recordFakeQueue{j: pool.j, publishErrs: []error{errors.New("нет")}}
		res, _ := recordRichResult()
		if _, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), res); err == nil {
			t.Fatal("ожидалась ошибка публикации")
		}
	})
}

// --- валидация до транзакции ---------------------------------------------------

func TestRecordValidation(t *testing.T) {
	newRes := func(mutate func(*Record)) *DomainResult {
		res, _ := recordRichResult()
		if mutate != nil {
			mutate(&res.Subdomains[0].Records[0])
		}
		return res
	}
	cases := []struct {
		name string
		res  *DomainResult
		frag string
	}{
		{"нулевая зона", &DomainResult{ZoneID: 0, DomainID: 1, Name: "x.test", Subdomains: []SubdomainResult{{Records: []Record{{}}}}}, "идентификаторы"},
		{"невалидный вариант", newRes(func(r *Record) { r.Variant = model.Variant(7) }), "вариант"},
		{"невалидная схема", newRes(func(r *Record) { r.Scheme = model.Scheme(9) }), "схема"},
		{"алиас без ссылки", newRes(func(r *Record) { r.AliasKind = model.AliasIdentical }), "T-055"},
		{"дубликат ключа", func() *DomainResult {
			res, _ := recordRichResult()
			rec := res.Subdomains[0].Records[0]
			res.Subdomains[1].Records = append(res.Subdomains[1].Records, rec)
			res.Subdomains[1].Records[1].SubdomainID = 0
			return res
		}(), "дубликат"},
		{"ContentNotStored с хэшем", newRes(func(r *Record) { r.ContentNotStored = true }), "T-056"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRecordFakePool()
			pool.recordSeedDomain(7, 91, model.StatusNew, 0)
			q := &recordFakeQueue{j: pool.j}
			_, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), tc.res)
			if err == nil || !strings.Contains(err.Error(), tc.frag) {
				t.Fatalf("ожидалась ошибка валидации с %q: %v", tc.frag, err)
			}
			if pool.begins != 0 {
				t.Fatal("валидация обязана отсекать до открытия транзакции")
			}
		})
	}

	t.Run("nil-результат и зависимости", func(t *testing.T) {
		pool := newRecordFakePool()
		q := &recordFakeQueue{j: pool.j}
		if ok, err := recordRecorder(t, pool, q).RecordAndPublish(context.Background(), nil); err != nil || ok {
			t.Fatalf("nil: ok=%v err=%v", ok, err)
		}
		if _, err := NewRecorder(nil, q, nil); err == nil {
			t.Fatal("NewRecorder без пула прошёл")
		}
		if _, err := NewRecorder(pool, nil, nil); err == nil {
			t.Fatal("NewRecorder без очереди прошёл")
		}
	})
}

// --- smoke: цикл Run с цепочкой OnResult (сборка cmd/http-worker) --------------

// TestRecordOnResultSmoke — доставка q:http → ProcessDomain → ContentSaver
// (T-056) → запись результата + публикация q:analyze (T-058) → XACK:
// порядок «публикация до ack», сообщения по записям, домен done.
func TestRecordOnResultSmoke(t *testing.T) {
	page := storePageBody("smoke")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"https://ack.test/": {status: 200, body: page},
		"ack.test":          {status: 200, body: page, final: "https://ack.test/"},
	})
	pool := newRecordFakePool()
	pool.recordSeedDomain(5, 42, model.StatusNew, 1)
	q := &recordFakeQueue{j: pool.j, deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "ack.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}
	q.j.ackNotify = make(chan struct{}, 1)

	st := newStoreFake()
	saver, err := NewContentSaver(st, nil)
	if err != nil {
		t.Fatalf("NewContentSaver: %v", err)
	}
	recorder := recordRecorder(t, pool, q)

	// Цепочка сборки сервиса (cmd/http-worker): ContentSaver → запись →
	// публикация; OnResult читается из опций Run (worker.go).
	opts := Options{OnResult: func(c context.Context, res *DomainResult) error {
		if err := saver.Save(c, res); err != nil {
			return err
		}
		_, err := recorder.RecordAndPublish(c, res)
		return err
	}}
	opts.Store = st // ранние Put — в тот же фейк, что и сверка Save (T-147)
	w, err := New(fetch, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()
	select {
	case <-q.j.ackNotify:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("доставка не подтверждена за 5 с")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	dom := pool.domains[recordDomainKey{zone: 5, id: 42}]
	if dom.status != model.StatusAlive || dom.checkState != model.CheckStateDone {
		t.Fatalf("домен: %v/%v, ожидался alive/done", dom.status, dom.checkState)
	}
	rows := pool.rows[recordDomainKey{zone: 5, id: 42}]
	if len(rows) != 1 || rows[0].contentHash == nil {
		t.Fatalf("строки: %+v — контент оригинала записан ссылкой", rows)
	}
	pubs := q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].msg.SubdomainID != 0 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации q:analyze: %+v", pubs)
	}
	if q.j.indexOf(recordEvPublish) > q.j.indexOf(recordEvAck) {
		t.Fatal("XACK раньше публикации q:analyze — нарушен порядок ТЗ 4.2/5.5 п. 9")
	}
	if _, err := st.Get(context.Background(), content.HashOf([]byte(page))); err != nil {
		t.Fatalf("контент не сохранён: %v", err)
	}
}

// TestRecordOnResultPublishErrorNoAck — сбой публикации оставляет доставку
// без XACK: повторная доставка перезапишет результат и опубликует заново.
func TestRecordOnResultPublishErrorNoAck(t *testing.T) {
	page := storePageBody("pub-err")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"https://pub.test/": {status: 200, body: page},
		"pub.test":          {status: 200, body: page, final: "https://pub.test/"},
	})
	pool := newRecordFakePool()
	pool.recordSeedDomain(5, 43, model.StatusNew, 0)
	q := &recordFakeQueue{j: pool.j, deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 43, Name: "pub.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}, publishErrs: []error{errors.New("Redis недоступен")}}

	recorder := recordRecorder(t, pool, q)
	opts := Options{OnResult: func(c context.Context, res *DomainResult) error {
		_, err := recorder.RecordAndPublish(c, res)
		return err
	}}
	opts.Store = newStoreFake() // ранние Put T-147 — свежий фейк (Save в цепочке нет)
	w, err := New(fetch, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	deadline := time.After(5 * time.Second)
	for q.j.count(recordEvPublish) < 1 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("публикация не попыталась выполниться за 5 с")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Доставка обязана остаться неподтверждённой.
	time.Sleep(50 * time.Millisecond)
	if got := q.j.count(recordEvAck); got != 0 {
		t.Fatalf("XACK %d при сбое публикации — доставка обязана вернуться (ТЗ 3.4)", got)
	}
	if len(q.ackedSnapshot()) != 0 {
		t.Fatal("ack зафиксирован при ошибке OnResult")
	}
	// Запись при этом состоялась: повтор только допубликует.
	if len(pool.rows[recordDomainKey{zone: 5, id: 43}]) != 1 {
		t.Fatal("результат не записан перед публикацией")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
