// Юнит-тесты записи результата DNS-эпизода и цикла dns-worker (T-039;
// ТЗ 5.4 пп. 4–5, §7, 3.4) на фейковых пуле, очереди и резолвере — без
// БД, Redis и сети. Решение статусов (DecideEpisode), точные SQL-тексты
// и параметры записи, порядок «публикация q:http → запись done», ветка
// wildcard, ошибки/отмена, идемпотентный повтор пачки (фильтр
// актуальности) и smoke цикла Run (чтение → эпизоды → XACK).
// Поведение против живых Redis/PostgreSQL/unbound — интеграционные
// тесты (T-032, T-040).
package dnsworker

import (
	"context"
	"encoding/json"
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
	"github.com/miekg/dns"

	"crawler/internal/db"
	"crawler/internal/dnsclient"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- общий протокол событий пула и очереди ------------------------------

// resultEventKind — тип события общего протокола (порядок операций
// «публикация → запись» проверяется по индексам событий).
type resultEventKind string

const (
	resultEvQuerySubs resultEventKind = "query:subdomains"
	resultEvQueryPend resultEventKind = "query:pending"
	resultEvExec      resultEventKind = "exec"
	resultEvPublish   resultEventKind = "publish"
	resultEvAck       resultEventKind = "ack"
	resultEvConsume   resultEventKind = "consume"
)

// resultEvent — запись протокола: операция, деталь (SQL/стрим) и аргументы.
type resultEvent struct {
	kind   resultEventKind
	detail string
	args   []any
}

// resultJournal — потокобезопасный протокол событий (цикл Run работает в
// горутине, эпизоды — параллельно).
type resultJournal struct {
	mu     sync.Mutex
	events []resultEvent
	// ackNotify — неблокирующий сигнал на каждое Ack (синхронизация smoke).
	ackNotify chan struct{}
}

func (j *resultJournal) record(k resultEventKind, detail string, args ...any) {
	j.mu.Lock()
	j.events = append(j.events, resultEvent{kind: k, detail: detail, args: args})
	j.mu.Unlock()
}

func (j *resultJournal) snapshot() []resultEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]resultEvent, len(j.events))
	copy(out, j.events)
	return out
}

// indexOf возвращает индекс первого события вида (0-based) или −1.
func (j *resultJournal) indexOf(k resultEventKind, detail string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	for i, e := range j.events {
		if e.kind == k && (detail == "" || e.detail == detail) {
			return i
		}
	}
	return -1
}

func (j *resultJournal) count(k resultEventKind) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, e := range j.events {
		if e.kind == k {
			n++
		}
	}
	return n
}

func (j *resultJournal) signalAck() {
	if j.ackNotify == nil {
		return
	}
	select {
	case j.ackNotify <- struct{}{}:
	default:
	}
}

// --- фейковый pgx-пул ----------------------------------------------------

// resultFakeRows — pgx.Rows с заготовленными строками; позиционный Scan
// (стиль enqueueFakeRows).
type resultFakeRows struct {
	rows    [][]any
	i       int
	closed  bool
	err     error
	scanErr error
}

func resultFakeRowsOf(rows ...[]any) *resultFakeRows {
	return &resultFakeRows{rows: rows}
}

func (r *resultFakeRows) Close() { r.closed = true }

func (r *resultFakeRows) Err() error { return r.err }

func (r *resultFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *resultFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *resultFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *resultFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("resultFakeRows: Scan без Next")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("resultFakeRows: приёмников %d, колонок %d", len(dest), len(row))
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

func (r *resultFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *resultFakeRows) RawValues() [][]byte    { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая позиционным Scan.
func (r *resultFakeRows) Conn() *pgx.Conn      { return nil }
func (r *resultFakeRows) TypeMap() *pgtype.Map { return nil }

// resultFakePool — db.DB без БД: Query различает справочник субдоменов и
// фильтр пачки по тексту SQL, Exec возвращает заготовленные результаты по
// очереди (дефолт — UPDATE 1). Всё пишется в общий протокол.
type resultFakePool struct {
	j *resultJournal

	subdomains []model.Subdomain // ответ справочника
	subsErr    error             // ошибка чтения справочника

	// pending — заготовленные строки фильтра (пары zone_id, id); nil —
	// «зеркало»: актуальны все ключи пачки.
	pending     [][2]any
	pendingErr  error
	pendScanErr error

	execTags []pgconn.CommandTag // результат каждого Exec по очереди
	execErrs []error             // ошибки Exec по очереди
	execErr  error               // липовая ошибка всех Exec (если задана)

	mu      sync.Mutex
	execs   []resultExec
	queries int
}

// resultExec — записанный вызов Exec.
type resultExec struct {
	sql  string
	args []any
}

var _ db.DB = (*resultFakePool)(nil)

func (p *resultFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("resultFakePool: CopyFrom не должен вызываться")
}

func (p *resultFakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	p.mu.Lock()
	p.execs = append(p.execs, resultExec{sql: sql, args: args})
	i := len(p.execs) - 1
	p.mu.Unlock()
	p.j.record(resultEvExec, sql, args...)

	if p.execErr != nil {
		return pgconn.CommandTag{}, p.execErr
	}
	if i < len(p.execErrs) && p.execErrs[i] != nil {
		return pgconn.CommandTag{}, p.execErrs[i]
	}
	if i < len(p.execTags) {
		return p.execTags[i], nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (p *resultFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch sql {
	case subdomainsSelectSQL:
		p.j.record(resultEvQuerySubs, sql)
		if p.subsErr != nil {
			return nil, p.subsErr
		}
		rows := make([][]any, len(p.subdomains))
		for i, s := range p.subdomains {
			rows[i] = []any{s.ID, s.Label, s.Enabled}
		}
		return resultFakeRowsOf(rows...), nil
	case resultPendingSQL:
		p.j.record(resultEvQueryPend, sql, args...)
		p.mu.Lock()
		p.queries++
		p.mu.Unlock()
		if p.pendingErr != nil {
			return nil, p.pendingErr
		}
		var pairs [][2]any
		if p.pending != nil {
			pairs = p.pending
		} else {
			// Зеркало: актуальны все ключи пачки (args: []int2, []int8).
			zones := args[0].([]int16)
			ids := args[1].([]int64)
			for i := range zones {
				pairs = append(pairs, [2]any{zones[i], ids[i]})
			}
		}
		rows := make([][]any, len(pairs))
		for i, pr := range pairs {
			rows[i] = []any{pr[0], pr[1]}
		}
		out := resultFakeRowsOf(rows...)
		out.scanErr = p.pendScanErr
		return out, nil
	default:
		return nil, fmt.Errorf("resultFakePool: неожидаемый SQL %q", sql)
	}
}

// resultPoolExecs — снимок вызовов Exec.
func (p *resultFakePool) resultPoolExecs() []resultExec {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]resultExec, len(p.execs))
	copy(out, p.execs)
	return out
}

// --- фейковая очередь ----------------------------------------------------

// resultFakeQueue — queue.Queue: Publish проверяет значения через
// queue.HTTPTask.Validate-контракт не повторяет — просто типизирует и
// запоминает; Consume раздаёт заготовленные доставки по одной, исчерпав —
// ждёт отмены ctx (реальный XREADGROUP блокируется); Ack фиксируется.
type resultFakeQueue struct {
	j *resultJournal

	deliveries []queue.Delivery
	consumeErr error // липовая ошибка первого чтения

	publishErrs []error // ошибки Publish по очереди

	mu        sync.Mutex
	publishes []resultPublish
	acked     []resultAck
}

// resultPublish — записанная публикация.
type resultPublish struct {
	stream string
	task   queue.HTTPTask
	msg    queue.Message
}

// resultAck — записанное подтверждение.
type resultAck struct {
	stream string
	group  string
	ids    []string
}

var _ queue.Queue = (*resultFakeQueue)(nil)

func (q *resultFakeQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	q.mu.Lock()
	i := len(q.publishes)
	var task queue.HTTPTask
	if t, ok := msg.(queue.HTTPTask); ok {
		task = t
	}
	q.publishes = append(q.publishes, resultPublish{stream: stream, task: task, msg: msg})
	q.mu.Unlock()
	q.j.record(resultEvPublish, stream, msg)

	var err error
	if i < len(q.publishErrs) {
		err = q.publishErrs[i]
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("entry-publish-%d", i+1), nil
}

func (q *resultFakeQueue) Consume(ctx context.Context, stream, group, consumer string, n int) ([]queue.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.consumeErr != nil {
		err := q.consumeErr
		q.consumeErr = nil // однократная: цикл обязан пережить и продолжить
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
	q.j.record(resultEvConsume, stream, group, consumer, len(out))
	if len(out) > 0 {
		return out, nil
	}
	// Пустое чтение: реальная очередь блокируется в XREADGROUP — фейк
	// ждёт отмены (smoke-цикл паркуется до конца теста).
	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *resultFakeQueue) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked = append(q.acked, resultAck{stream: stream, group: group, ids: append([]string(nil), ids...)})
	q.mu.Unlock()
	for _, id := range ids {
		q.j.record(resultEvAck, stream, id)
	}
	q.j.signalAck()
	return nil
}

func (q *resultFakeQueue) resultPublishes() []resultPublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]resultPublish, len(q.publishes))
	copy(out, q.publishes)
	return out
}

func (q *resultFakeQueue) resultAcks() []resultAck {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]resultAck, len(q.acked))
	copy(out, q.acked)
	return out
}

// --- фейковый резолвер ---------------------------------------------------

// resultAnswer — заготовленный ответ на вопрос: Result с кодом и записями
// либо ошибка обмена (транспорт после ретраев dnsclient).
type resultAnswer struct {
	rcode dnsclient.RCode
	an    []dns.RR
	err   error
}

// resultReply — ответ с кодом и секцией answer.
func resultReply(rcode dnsclient.RCode, an ...dns.RR) resultAnswer {
	return resultAnswer{rcode: rcode, an: an}
}

// errResultNet — транспортная ошибка (не отмена ctx).
var errResultNet = errors.New("fake network error")

// resultQKey — ключ вопроса: каноническое имя (строчные, FQDN) и тип.
type resultQKey struct {
	name string
	typ  uint16
}

// resultFakeResolver — dnsclient.Resolver, отвечающий по имени и типу
// вопроса (в отличие от сценарного по порядку — эпизод комбинирует апекс,
// субдомены и wildcard-детект). Записи вида wildcard ищутся по суффиксу
// зоны: детект-вопрос T-038 несёт случайную метку.
type resultFakeResolver struct {
	mu      sync.Mutex
	answers map[resultQKey]resultAnswer
	wild    map[string]resultAnswer // суффикс зоны → ответ <random>.<зона>
	calls   []dnsclient.Question
	onCall  func(n int) // тест дергает отмену ctx между обменами
}

var _ dnsclient.Resolver = (*resultFakeResolver)(nil)

func resultNewResolver() *resultFakeResolver {
	return &resultFakeResolver{
		answers: make(map[resultQKey]resultAnswer),
		wild:    make(map[string]resultAnswer),
	}
}

// resultSet задаёт точный ответ на вопрос (имя канонизируется).
func (f *resultFakeResolver) setAnswer(name string, typ uint16, a resultAnswer) {
	key := resultQKey{name: strings.ToLower(dns.Fqdn(name)), typ: typ}
	f.answers[key] = a
}

// resultSetWild задаёт ответ wildcard-детекта для зоны (suffix — имя
// домена, например "example.com").
func (f *resultFakeResolver) setWild(zone string, a resultAnswer) {
	f.wild[strings.ToLower(dns.Fqdn(zone))] = a
}

func (f *resultFakeResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, q)
	n := len(f.calls)
	f.mu.Unlock()
	if f.onCall != nil {
		f.onCall(n)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := resultQKey{name: strings.ToLower(dns.Fqdn(q.Name)), typ: q.Type}
	f.mu.Lock()
	defer f.mu.Unlock()
	if a, ok := f.answers[key]; ok {
		return f.toResult(a)
	}
	for suffix, a := range f.wild {
		if key.name != suffix && strings.HasSuffix(key.name, "."+suffix) {
			return f.toResult(a)
		}
	}
	return nil, fmt.Errorf("resultFakeResolver: нет сценария для %s %s", dns.Type(q.Type), key.name)
}

func (f *resultFakeResolver) toResult(a resultAnswer) (*dnsclient.Result, error) {
	if a.err != nil {
		return nil, a.err
	}
	return &dnsclient.Result{RCode: a.rcode, Answer: a.an}, nil
}

func (f *resultFakeResolver) resultCalls() []dnsclient.Question {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]dnsclient.Question(nil), f.calls...)
}

// --- помощники сценариев --------------------------------------------------

// resultSubs — справочник: апекс (id 0), www (id 1), admin (id 2).
func resultSubs() []model.Subdomain {
	return []model.Subdomain{
		{ID: 0, Label: "", Enabled: true},
		{ID: 1, Label: "www", Enabled: true},
		{ID: 2, Label: "admin", Enabled: true},
	}
}

// resultSetDelegated — домен с адресами: апекс A, www CNAME→cdn с A,
// admin NXDOMAIN, wildcard-детект — NXDOMAIN (wildcard нет).
func resultSetDelegated(f *resultFakeResolver, domain string) {
	f.setAnswer(domain, dns.TypeA,
		resultReply(dnsclient.RCodeNOERROR, apexARR(domain+".", "192.0.2.10")))
	f.setAnswer("www."+domain, dns.TypeA, resultReply(dnsclient.RCodeNOERROR,
		subdomCNAME("www."+domain+".", "cdn."+domain+"."),
		apexARR("cdn."+domain+".", "203.0.113.5")))
	f.setAnswer("admin."+domain, dns.TypeA, resultReply(dnsclient.RCodeNXDOMAIN))
	f.setWild(domain, resultReply(dnsclient.RCodeNXDOMAIN))
}

// resultSetNXDOMAIN — домен без делегирования (апекс NXDOMAIN).
func resultSetNXDOMAIN(f *resultFakeResolver, domain string) {
	f.setAnswer(domain, dns.TypeA, resultReply(dnsclient.RCodeNXDOMAIN))
}

// resultSetWildcardZone — домен под wildcard: апекс A, www NODATA (NS
// подтверждает), admin — адреса wildcard-ответа.
func resultSetWildcardZone(f *resultFakeResolver, domain string) {
	f.setAnswer(domain, dns.TypeA,
		resultReply(dnsclient.RCodeNOERROR, apexARR(domain+".", "192.0.2.1")))
	f.setAnswer(domain, dns.TypeNS, resultReply(dnsclient.RCodeNOERROR,
		apexNSRR(domain+".", "ns1."+domain+".")))
	f.setAnswer("www."+domain, dns.TypeA, resultReply(dnsclient.RCodeNOERROR))
	f.setAnswer("admin."+domain, dns.TypeA,
		resultReply(dnsclient.RCodeNOERROR, apexARR("admin."+domain+".", "198.51.100.7")))
	f.setWild(domain, resultReply(dnsclient.RCodeNOERROR, apexARR("wild."+domain+".", "198.51.100.7")))
}

// resultDelivery — доставка DNSBatch с task_id и доменами.
func resultDelivery(id string, taskID string, domains ...queue.DNSDomain) queue.Delivery {
	payload, err := json.Marshal(queue.DNSBatch{TaskID: taskID, Domains: domains})
	if err != nil {
		panic(fmt.Sprintf("resultDelivery: маршалинг DNSBatch: %v", err))
	}
	return queue.Delivery{Stream: queue.StreamDNS, ID: id, Payload: payload}
}

// resultWaitAck — ждёт первого Ack в протоколе (smoke-синхронизация).
func resultWaitAck(j *resultJournal, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if j.count(resultEvAck) > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// --- SQL-контракты ---------------------------------------------------------

// TestResultSQLContract фиксирует точные тексты операторов записи (стиль
// dispatch_test: ожидание собирается независимо от прод-кода, литералы —
// коды схемы ТЗ 6.1/6.2: done = 2, temp_error = 5, deleted = 6).
func TestResultSQLContract(t *testing.T) {
	wantStatus := "UPDATE domains AS t " +
		"SET dns_mask = $3::int8, status = $4::int2, " +
		"temp_attempts = CASE WHEN $4::int2 = 5 THEN t.temp_attempts ELSE 0 END, " +
		"check_state = 2, queued_at = NULL, last_checked_at = now() " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> 6 AND t.check_state <> 2"
	if resultUpdateStatusSQL != wantStatus {
		t.Errorf("resultUpdateStatusSQL = %q,\nwant %q", resultUpdateStatusSQL, wantStatus)
	}

	wantKeep := "UPDATE domains AS t " +
		"SET dns_mask = $3::int8, temp_attempts = 0, " +
		"check_state = 2, queued_at = NULL, last_checked_at = now() " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> 6 AND t.check_state <> 2"
	if resultUpdateKeepStatusSQL != wantKeep {
		t.Errorf("resultUpdateKeepStatusSQL = %q,\nwant %q", resultUpdateKeepStatusSQL, wantKeep)
	}

	wantPending := "SELECT u.zone_id, u.id " +
		"FROM unnest($1::int2[], $2::int8[]) WITH ORDINALITY AS u(zone_id, id, ord) " +
		"JOIN domains AS d ON d.zone_id = u.zone_id AND d.id = u.id " +
		"WHERE d.status <> 6 AND d.check_state <> 2 " +
		"ORDER BY u.ord"
	if resultPendingSQL != wantPending {
		t.Errorf("resultPendingSQL = %q,\nwant %q", resultPendingSQL, wantPending)
	}

	if want := "SELECT id, label, enabled FROM subdomains ORDER BY id"; subdomainsSelectSQL != want {
		t.Errorf("subdomainsSelectSQL = %q, want %q", subdomainsSelectSQL, want)
	}
}

// --- DecideEpisode ---------------------------------------------------------

func TestResultDecideEpisode(t *testing.T) {
	delegated := &ApexResult{Status: ApexDelegated, Addresses: apexAddrs("192.0.2.10")}
	noAddress := &ApexResult{Status: ApexDelegatedNoAddress, NSQueried: true}

	cases := []struct {
		name     string
		apex     *ApexResult
		rep      *SubdomReport
		wild     WildcardResult
		wantSet  bool
		wantSt   model.Status
		wantMask model.DNSMask
		wantSubs []queue.HTTPSubdomain
		wantTemp bool
		wantErr  string
	}{
		{
			name:    "nxdomain апекса — not_delegated, субдомены не проверялись",
			apex:    &ApexResult{Status: ApexNotDelegated},
			wantSet: true,
			wantSt:  model.StatusNotDelegated,
		},
		{
			name:     "servfail апекса — temp_error",
			apex:     &ApexResult{Status: ApexTempError},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			name: "есть адреса — задача q:http, статус не пишется",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0, 1), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", A: apexAddrs("203.0.113.5"), Chain: []string{"cdn.example.net."}},
				{ID: 2, Label: "admin", RCode: dnsclient.RCodeNXDOMAIN},
			}},
			wantMask: mustMask(t, 0, 1),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
				{SubdomainID: 1, Label: "www", Addrs: []string{"203.0.113.5"}, CNAME: "cdn.example.net"},
			},
		},
		{
			name: "wildcard исключает из q:http, но не из dns_mask",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0, 1, 2), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", A: apexAddrs("203.0.113.5")},
				{ID: 2, Label: "admin", A: apexAddrs("198.51.100.7")},
			}},
			wild:     WildcardResult{Mask: mustMask(t, 2)},
			wantMask: mustMask(t, 0, 1, 2),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
				{SubdomainID: 1, Label: "www", Addrs: []string{"203.0.113.5"}},
			},
		},
		{
			name: "резолвятся только wildcard-имена — delegated_no_address, маска честная",
			apex: noAddress,
			rep: &SubdomReport{Mask: mustMask(t, 2), Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeNOERROR},
				{ID: 2, Label: "admin", A: apexAddrs("198.51.100.7")},
			}},
			wild:     WildcardResult{Mask: mustMask(t, 2)},
			wantSet:  true,
			wantSt:   model.StatusDelegatedNoAddress,
			wantMask: mustMask(t, 2),
		},
		{
			name: "адресов нет без сбоев — delegated_no_address",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeNXDOMAIN},
			}},
			wantSet: true,
			wantSt:  model.StatusDelegatedNoAddress,
		},
		{
			name: "адресов нет, субдомен servfail — temp_error",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeSERVFAIL},
			}},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			name: "адресов нет, транспорт субдомена — temp_error",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www", Err: errResultNet},
			}},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			// DNS-02: дефект зоны — не сбой резолвера: цикл CNAME на www
			// постоянен, повторы T-031 ничего не изменят — «адресов нет».
			name: "адресов нет, цикл CNAME субдомена — delegated_no_address",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www",
					Err: fmt.Errorf("%w: www.example.net. → cdn.example.net.", ErrSubdomCNAMELoop)},
			}},
			wantSet: true,
			wantSt:  model.StatusDelegatedNoAddress,
		},
		{
			name: "адресов нет, глубина CNAME субдомена — delegated_no_address",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "", RCode: dnsclient.RCodeNOERROR},
				{ID: 1, Label: "www",
					Err: fmt.Errorf("%w: 10 переходов от www.example.net.", ErrSubdomCNAMEDepth)},
			}},
			wantSet: true,
			wantSt:  model.StatusDelegatedNoAddress,
		},
		{
			// Транспортный сбой рядом с дефектом зоны всё же temp:
			// «адресов нет» по молчащему резолверу не выводится.
			name: "адресов нет, цикл CNAME и транспорт — temp_error",
			apex: noAddress,
			rep: &SubdomReport{Results: []SubdomResult{
				{ID: 0, Label: "",
					Err: fmt.Errorf("%w: a.example.net. → b.example.net.", ErrSubdomCNAMELoop)},
				{ID: 1, Label: "www", Err: errResultNet},
			}},
			wantSet:  true,
			wantSt:   model.StatusTempError,
			wantTemp: true,
		},
		{
			name: "адреса есть несмотря на сбой одного субдомена — частичный итог",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", Err: errResultNet},
			}},
			wantMask: mustMask(t, 0),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
			},
			wantTemp: true,
		},
		{
			// DNS-02: дефект зоны у субдомена без адресов не портит итог
			// нашедшим адреса и не помечает эпизод temp.
			name: "адреса есть, у другого субдомена цикл CNAME — без temp",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www",
					Err: fmt.Errorf("%w: www.example.net. → cdn.example.net.", ErrSubdomCNAMELoop)},
			}},
			wantMask: mustMask(t, 0),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
			},
		},
		{
			name: "nxdomain субдомена — не сбой (RFC 8020)",
			apex: delegated,
			rep: &SubdomReport{Mask: mustMask(t, 0), Results: []SubdomResult{
				{ID: 0, Label: "", A: apexAddrs("192.0.2.10")},
				{ID: 1, Label: "www", RCode: dnsclient.RCodeNXDOMAIN},
			}},
			wantMask: mustMask(t, 0),
			wantSubs: []queue.HTTPSubdomain{
				{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}},
			},
		},
		{
			name:    "нет результата апекса — ошибка",
			apex:    nil,
			wantErr: "нет результата апекса",
		},
		{
			name:    "апекс делегирован, отчёта субдоменов нет — ошибка",
			apex:    delegated,
			rep:     nil,
			wantErr: "нет отчёта субдоменов",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := DecideEpisode(tc.apex, tc.rep, tc.wild)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ошибка = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecideEpisode: %v", err)
			}
			if res.SetStatus != tc.wantSet {
				t.Errorf("SetStatus = %v, want %v", res.SetStatus, tc.wantSet)
			}
			if res.SetStatus && res.Status != tc.wantSt {
				t.Errorf("Status = %v, want %v", res.Status, tc.wantSt)
			}
			if res.Mask != tc.wantMask {
				t.Errorf("Mask = %d (%v), want %d (%v)", int64(res.Mask), res.Mask.IDs(), int64(tc.wantMask), tc.wantMask.IDs())
			}
			if res.TempFailed != tc.wantTemp {
				t.Errorf("TempFailed = %v, want %v", res.TempFailed, tc.wantTemp)
			}
			if res.HasHTTPTask() != (len(tc.wantSubs) > 0) {
				t.Fatalf("HasHTTPTask = %v, want %v", res.HasHTTPTask(), len(tc.wantSubs) > 0)
			}
			if !reflect.DeepEqual(res.HTTPSubdomains, tc.wantSubs) {
				t.Errorf("HTTPSubdomains = %+v, want %+v", res.HTTPSubdomains, tc.wantSubs)
			}
		})
	}
}

// mustMask — маска с битами (падение — ошибка теста).
func mustMask(t *testing.T, ids ...int) model.DNSMask {
	t.Helper()
	m, err := model.NewDNSMask(ids...)
	if err != nil {
		t.Fatalf("NewDNSMask(%v): %v", ids, err)
	}
	return m
}

// --- RecordEpisode ---------------------------------------------------------

func TestResultRecordEpisode(t *testing.T) {
	t.Run("терминальный статус — оператор с status и CASE temp_attempts", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		res := EpisodeResult{SetStatus: true, Status: model.StatusNotDelegated, Mask: mustMask(t, 1)}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42, res)
		if err != nil || !recorded {
			t.Fatalf("RecordEpisode = (%v, %v), want (true, nil)", recorded, err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 1 {
			t.Fatalf("Exec вызовов %d, want 1", len(execs))
		}
		if execs[0].sql != resultUpdateStatusSQL {
			t.Errorf("SQL = %q, want resultUpdateStatusSQL", execs[0].sql)
		}
		wantArgs := []any{int16(7), int64(42), int64(mustMask(t, 1)), int16(model.StatusNotDelegated)}
		if !reflect.DeepEqual(execs[0].args, wantArgs) {
			t.Errorf("args = %v, want %v", execs[0].args, wantArgs)
		}
	})

	t.Run("есть адреса — оператор без status (итог за HTTP-этапом)", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		res := EpisodeResult{Mask: mustMask(t, 0, 1),
			HTTPSubdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}}}}
		if _, err := RecordEpisode(context.Background(), pool, 7, 42, res); err != nil {
			t.Fatalf("RecordEpisode: %v", err)
		}
		execs := pool.resultPoolExecs()
		if len(execs) != 1 || execs[0].sql != resultUpdateKeepStatusSQL {
			t.Fatalf("SQL = %v, want resultUpdateKeepStatusSQL", execs[0].sql)
		}
		wantArgs := []any{int16(7), int64(42), int64(mustMask(t, 0, 1))}
		if !reflect.DeepEqual(execs[0].args, wantArgs) {
			t.Errorf("args = %v, want %v", execs[0].args, wantArgs)
		}
	})

	t.Run("охраны отсекли строку — успех без записи", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
		recorded, err := RecordEpisode(context.Background(), pool, 7, 42,
			EpisodeResult{SetStatus: true, Status: model.StatusTempError})
		if err != nil {
			t.Fatalf("RecordEpisode: %v", err)
		}
		if recorded {
			t.Error("recorded = true, want false (уже done или deleted)")
		}
	})

	t.Run("ошибка БД — ошибка записи", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, execErr: errors.New("бд недоступна")}
		_, err := RecordEpisode(context.Background(), pool, 7, 42, EpisodeResult{})
		if err == nil || !strings.Contains(err.Error(), "запись результата домена 7/42") ||
			!errors.Is(err, pool.execErr) {
			t.Fatalf("ошибка = %v, want «запись результата домена 7/42: бд недоступна»", err)
		}
	})

	t.Run("недопустимый статус — ошибка до БД", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		_, err := RecordEpisode(context.Background(), pool, 7, 42,
			EpisodeResult{SetStatus: true, Status: model.Status(99)})
		if err == nil || !strings.Contains(err.Error(), "недопустимый статус") {
			t.Fatalf("ошибка = %v, want «недопустимый статус»", err)
		}
		if len(pool.resultPoolExecs()) != 0 {
			t.Error("Exec не должен вызываться при невалидном статусе")
		}
	})

	t.Run("отмена ctx — без обращения к БД", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := RecordEpisode(ctx, pool, 7, 42, EpisodeResult{}); err == nil {
			t.Fatal("RecordEpisode с отменённым ctx = nil, want ошибка контекста")
		}
	})

	t.Run("пул не задан", func(t *testing.T) {
		if _, err := RecordEpisode(context.Background(), nil, 1, 1, EpisodeResult{}); err == nil ||
			!strings.Contains(err.Error(), "пул не задан") {
			t.Fatalf("ошибка = %v, want «пул не задан»", err)
		}
	})
}

// --- ProcessDomain ---------------------------------------------------------

// TestResultProcessDomainWithAddrs — сквозной эпизод домена с адресами:
// порядок «публикация q:http → запись done» (§3.4), содержимое задачи
// (адреса A/AAAA, CNAME апекса и субдоменов), оператор записи без status.
func TestResultProcessDomainWithAddrs(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")
	// askAAAA: апекс и www отвечают AAAA (терминальные имена цепочки).
	resolve.setAnswer("example.com", dns.TypeAAAA,
		resultReply(dnsclient.RCodeNOERROR, apexAAAARR("example.com.", "2001:db8::1")))
	resolve.setAnswer("cdn.example.com", dns.TypeAAAA,
		resultReply(dnsclient.RCodeNOERROR, apexAAAARR("cdn.example.com.", "2001:db8::2")))

	dom := queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}
	if err := ProcessDomain(context.Background(), pool, q, resolve, dom, resultSubs(), true, nil, nil); err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	// Порядок: публикация q:http строго раньше записи (комментарий файла).
	if pi, ei := j.indexOf(resultEvPublish, queue.StreamHTTP), j.indexOf(resultEvExec, ""); pi < 0 || ei < 0 || pi > ei {
		t.Fatalf("порядок нарушен: publish=%d exec=%d (нужна публикация до записи)", pi, ei)
	}
	if n := j.count(resultEvExec); n != 1 {
		t.Fatalf("Exec вызовов %d, want 1", n)
	}
	execs := pool.resultPoolExecs()
	if execs[0].sql != resultUpdateKeepStatusSQL {
		t.Errorf("SQL записи = %q, want resultUpdateKeepStatusSQL", execs[0].sql)
	}
	if !reflect.DeepEqual(execs[0].args, []any{int16(3), int64(11), int64(mustMask(t, 0, 1))}) {
		t.Errorf("args записи = %v", execs[0].args)
	}

	publishes := q.resultPublishes()
	if len(publishes) != 1 {
		t.Fatalf("публикаций %d, want 1", len(publishes))
	}
	p := publishes[0]
	if p.stream != queue.StreamHTTP {
		t.Errorf("стрим = %q, want %q", p.stream, queue.StreamHTTP)
	}
	want := queue.HTTPTask{
		ZoneID: 3, DomainID: 11, Name: "example.com",
		Subdomains: []queue.HTTPSubdomain{
			{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10", "2001:db8::1"}},
			// CNAME — presentation-форма без корневой точки (валидируется
			// очередью по лимиту 253 октета, T-024).
			{SubdomainID: 1, Label: "www", Addrs: []string{"203.0.113.5", "2001:db8::2"}, CNAME: "cdn.example.com"},
		},
	}
	if p.task.DomainID != want.DomainID || p.task.ZoneID != want.ZoneID || p.task.Name != want.Name {
		t.Errorf("задача = %+v, want %+v", p.task, want)
	}
	if !reflect.DeepEqual(p.task.Subdomains, want.Subdomains) {
		t.Errorf("субдомены задачи = %+v, want %+v", p.task.Subdomains, want.Subdomains)
	}
}

// TestResultProcessDomainNXDOMAIN — терминальная ветка апекса: субдомены
// и wildcard не опрашиваются (RFC 8020), записи — status=not_delegated.
func TestResultProcessDomainNXDOMAIN(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetNXDOMAIN(resolve, "gone.example.com")

	dom := queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "gone.example.com"}
	if err := ProcessDomain(context.Background(), pool, q, resolve, dom, resultSubs(), true, nil, nil); err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	if got := len(resolve.resultCalls()); got != 1 {
		t.Fatalf("обменов %d, want 1 (только апекс, RFC 8020)", got)
	}
	if n := j.count(resultEvPublish); n != 0 {
		t.Errorf("публикаций %d, want 0 (адресов нет)", n)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 1 || execs[0].sql != resultUpdateStatusSQL {
		t.Fatalf("SQL записи = %v, want resultUpdateStatusSQL", execs)
	}
	if !reflect.DeepEqual(execs[0].args,
		[]any{int16(3), int64(12), int64(0), int16(model.StatusNotDelegated)}) {
		t.Errorf("args записи = %v", execs[0].args)
	}
}

// TestResultProcessDomainWildcard — wildcard-зона: admin исключён из
// задачи (адреса ⊆ wildcard-ответу), апекс идёт всегда; wildcard-детект
// сбой — продолжение без фильтрации (политика T-038).
func TestResultProcessDomainWildcard(t *testing.T) {
	t.Run("детект сработал — исключённые не публикуются, маска полная", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetWildcardZone(resolve, "parked.example.com")

		if err := ProcessDomain(context.Background(), pool, q, resolve,
			queue.DNSDomain{ZoneID: 5, DomainID: 21, Name: "parked.example.com"},
			resultSubs(), false, nil, nil); err != nil {
			t.Fatalf("ProcessDomain: %v", err)
		}

		publishes := q.resultPublishes()
		if len(publishes) != 1 {
			t.Fatalf("публикаций %d, want 1", len(publishes))
		}
		subs := publishes[0].task.Subdomains
		if len(subs) != 1 || subs[0].SubdomainID != 0 || subs[0].Label != "" {
			t.Fatalf("субдомены задачи = %+v, want только апекс (admin — wildcard, www без адресов)", subs)
		}
		execs := pool.resultPoolExecs()
		if !reflect.DeepEqual(execs[0].args,
			[]any{int16(5), int64(21), int64(mustMask(t, 0, 2))}) {
			t.Errorf("args записи = %v, want маска с битами апекса и admin", execs[0].args)
		}
	})

	t.Run("детект не удался — без фильтрации, эпизод не роняется", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "flaky.example.com")
		// Детект-вопрос (случайная метка) — транспортный сбой.
		resolve.setWild("flaky.example.com", resultAnswer{err: errResultNet})

		if err := ProcessDomain(context.Background(), pool, q, resolve,
			queue.DNSDomain{ZoneID: 5, DomainID: 22, Name: "flaky.example.com"},
			resultSubs(), false, nil, nil); err != nil {
			t.Fatalf("ProcessDomain: %v (политика T-038 — продолжить)", err)
		}
		publishes := q.resultPublishes()
		if len(publishes) != 1 || len(publishes[0].task.Subdomains) != 2 {
			t.Fatalf("субдомены задачи = %+v, want апекс и www без фильтрации", publishes)
		}
		if n := j.count(resultEvExec); n != 1 {
			t.Errorf("Exec вызовов %d, want 1", n)
		}
	})
}

// TestResultProcessDomainErrors — ошибки шагов: сбой публикации — без
// записи (домен вернётся повторной доставкой); сбой записи — ошибка после
// публикации (ограниченный дубль задачи, §3.4); отмена ctx — эпизод не
// завершён, ничего не публикуется и не записывается.
func TestResultProcessDomainErrors(t *testing.T) {
	t.Run("сбой публикации — без записи", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j, publishErrs: []error{errors.New("redis unavailable")}}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")

		err := ProcessDomain(context.Background(), pool, q, resolve,
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}, resultSubs(), false, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "публикация q:http") {
			t.Fatalf("ошибка = %v, want «публикация q:http»", err)
		}
		if n := j.count(resultEvExec); n != 0 {
			t.Errorf("Exec вызовов %d, want 0 (запись только после публикации)", n)
		}
	})

	t.Run("сбой записи — ошибка эпизода, публикация уже была", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j, execErr: errors.New("бд недоступна")}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")

		err := ProcessDomain(context.Background(), pool, q, resolve,
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}, resultSubs(), false, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "запись результата") {
			t.Fatalf("ошибка = %v, want «запись результата»", err)
		}
		if n := j.count(resultEvPublish); n != 1 {
			t.Errorf("публикаций %d, want 1 (дубль ограничен попытками доставки, T-025)", n)
		}
	})

	t.Run("отмена ctx до начала — без обмена", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := ProcessDomain(ctx, pool, q, resolve,
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}, resultSubs(), false, nil, nil)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("ошибка = %v, want context.Canceled", err)
		}
		if got := len(resolve.resultCalls()); got != 0 {
			t.Errorf("обменов %d, want 0", got)
		}
		if n := j.count(resultEvExec) + j.count(resultEvPublish); n != 0 {
			t.Errorf("публикаций+записей = %d, want 0", n)
		}
	})

	t.Run("отмена ctx посреди субдоменов — эпизод не завершён", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		q := &resultFakeQueue{j: j}
		resolve := resultNewResolver()
		resultSetDelegated(resolve, "example.com")
		ctx, cancel := context.WithCancel(context.Background())
		resolve.onCall = func(n int) {
			if n >= 2 { // после апекса, в субдоменах
				cancel()
			}
		}

		err := ProcessDomain(ctx, pool, q, resolve,
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}, resultSubs(), false, nil, nil)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("ошибка = %v, want context.Canceled", err)
		}
		if n := j.count(resultEvExec) + j.count(resultEvPublish); n != 0 {
			t.Errorf("публикаций+записей = %d, want 0 (эпизод не завершён)", n)
		}
	})

	t.Run("очередь не задана", func(t *testing.T) {
		err := ProcessDomain(context.Background(), &resultFakePool{j: &resultJournal{}}, nil,
			resultNewResolver(), queue.DNSDomain{ZoneID: 1, DomainID: 1, Name: "example.com"},
			resultSubs(), false, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "очередь не задана") {
			t.Fatalf("ошибка = %v, want «очередь не задана»", err)
		}
	})
}

// --- PendingDomains / LoadSubdomains ---------------------------------------

func TestResultPendingDomains(t *testing.T) {
	t.Run("зеркало — все ключи пачки в её порядке", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		batch := []queue.DNSDomain{
			{ZoneID: 3, DomainID: 11, Name: "a.example.com"},
			{ZoneID: 3, DomainID: 12, Name: "b.example.com"},
			{ZoneID: 4, DomainID: 13, Name: "c.example.org"},
		}
		out, err := PendingDomains(context.Background(), pool, batch)
		if err != nil {
			t.Fatalf("PendingDomains: %v", err)
		}
		if !reflect.DeepEqual(out, batch) {
			t.Errorf("итог = %+v, want вся пачка в порядке", out)
		}
		ev := j.snapshot()
		if len(ev) != 1 || ev[0].kind != resultEvQueryPend {
			t.Fatalf("события = %+v, want один query:pending", ev)
		}
		wantArgs := []any{
			[]int16{3, 3, 4},
			[]int64{11, 12, 13},
		}
		if !reflect.DeepEqual(ev[0].args, wantArgs) {
			t.Errorf("args = %v, want %v", ev[0].args, wantArgs)
		}
	})

	t.Run("заготовленный фильтр — только недоработанные", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, pending: [][2]any{{int16(3), int64(12)}}}
		out, err := PendingDomains(context.Background(), pool, []queue.DNSDomain{
			{ZoneID: 3, DomainID: 11, Name: "a.example.com"},
			{ZoneID: 3, DomainID: 12, Name: "b.example.com"},
		})
		if err != nil {
			t.Fatalf("PendingDomains: %v", err)
		}
		want := []queue.DNSDomain{{ZoneID: 3, DomainID: 12, Name: "b.example.com"}}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("итог = %+v, want %+v (имя восстанавливается из пачки)", out, want)
		}
	})

	t.Run("пустая пачка — без обращения к БД", func(t *testing.T) {
		j := &resultJournal{}
		pool := &resultFakePool{j: j}
		out, err := PendingDomains(context.Background(), pool, nil)
		if err != nil || out != nil {
			t.Fatalf("PendingDomains = (%v, %v), want (nil, nil)", out, err)
		}
		if len(j.snapshot()) != 0 {
			t.Error("пустая пачка не должна обращаться к БД")
		}
	})

	t.Run("ошибка БД", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, pendingErr: errors.New("бд недоступна")}
		_, err := PendingDomains(context.Background(), pool,
			[]queue.DNSDomain{{ZoneID: 3, DomainID: 11, Name: "a.example.com"}})
		if err == nil || !strings.Contains(err.Error(), "фильтр пачки") {
			t.Fatalf("ошибка = %v, want «фильтр пачки»", err)
		}
	})

	t.Run("отмена ctx", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := PendingDomains(ctx, pool,
			[]queue.DNSDomain{{ZoneID: 3, DomainID: 11, Name: "a.example.com"}}); err == nil {
			t.Fatal("PendingDomains с отменённым ctx = nil, want ошибка")
		}
	})
}

func TestResultLoadSubdomains(t *testing.T) {
	t.Run("читает справочник с отключёнными строками", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, subdomains: []model.Subdomain{
			{ID: 0, Label: "", Enabled: true},
			{ID: 1, Label: "www", Enabled: true},
			{ID: 2, Label: "admin", Enabled: false},
		}}
		subs, err := LoadSubdomains(context.Background(), pool)
		if err != nil {
			t.Fatalf("LoadSubdomains: %v", err)
		}
		if len(subs) != 3 || subs[2].Label != "admin" || subs[2].Enabled {
			t.Errorf("справочник = %+v", subs)
		}
	})

	t.Run("пустой справочник — ошибка", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}}
		_, err := LoadSubdomains(context.Background(), pool)
		if err == nil || !errors.Is(err, ErrSubdomainsEmpty) {
			t.Fatalf("ошибка = %v, want ErrSubdomainsEmpty", err)
		}
	})

	t.Run("ошибка БД", func(t *testing.T) {
		pool := &resultFakePool{j: &resultJournal{}, subsErr: errors.New("бд недоступна")}
		if _, err := LoadSubdomains(context.Background(), pool); err == nil ||
			!strings.Contains(err.Error(), "справочник субдоменов") {
			t.Fatalf("ошибка = %v, want «справочник субдоменов»", err)
		}
	})
}

// --- Options ---------------------------------------------------------------

func TestResultOptionsDefaults(t *testing.T) {
	o := Options{}.withDefaults()
	if o.Concurrency != DefaultConcurrency || o.Group != DefaultGroup ||
		o.Consume != DefaultConsume || !strings.HasPrefix(o.Consumer, "dns-worker-") {
		t.Errorf("defaults = %+v", o)
	}
	custom := Options{Concurrency: 8, Group: "g", Consumer: "c1", Consume: 4, AskAAAA: true}.
		withDefaults()
	if custom.Concurrency != 8 || custom.Group != "g" || custom.Consumer != "c1" ||
		custom.Consume != 4 || !custom.AskAAAA {
		t.Errorf("кастомные значения затёрты: %+v", custom)
	}
}

func TestResultRunValidation(t *testing.T) {
	opts := Options{Concurrency: 1}
	if err := Run(context.Background(), nil, &resultFakeQueue{j: &resultJournal{}}, resultNewResolver(), opts); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Fatalf("ошибка = %v, want «пул не задан»", err)
	}
	if err := Run(context.Background(), &resultFakePool{j: &resultJournal{}}, nil, resultNewResolver(), opts); err == nil ||
		!strings.Contains(err.Error(), "очередь не задана") {
		t.Fatalf("ошибка = %v, want «очередь не задана»", err)
	}
	if err := Run(context.Background(), &resultFakePool{j: &resultJournal{}}, &resultFakeQueue{j: &resultJournal{}}, nil, opts); err == nil ||
		!strings.Contains(err.Error(), "резолвер не задан") {
		t.Fatalf("ошибка = %v, want «резолвер не задан»", err)
	}
}

// --- детерминированные состояния пачки (DNS-03) ----------------------------

// TestResultProcessBatchDictStates — пустой, целиком отключённый или
// кривой справочник subdomains — конфигурация, а не яд: пачка
// подтверждается без эпизодов, повторная доставка её не крутит.
func TestResultProcessBatchDictStates(t *testing.T) {
	dom := queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}
	cases := []struct {
		name string
		subs []model.Subdomain
	}{
		{"пустой справочник", nil},
		{"все субдомены отключены", []model.Subdomain{
			{ID: 0, Label: "", Enabled: false},
			{ID: 1, Label: "www", Enabled: false},
		}},
		{"кривая строка справочника", []model.Subdomain{
			{ID: 0, Label: "", Enabled: true},
			{ID: 99, Label: "bad", Enabled: true}, // id вне 0..62
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := &resultJournal{}
			pool := &resultFakePool{j: j, subdomains: tc.subs}
			q := &resultFakeQueue{j: j}
			resolve := resultNewResolver()
			err := processBatch(context.Background(), pool, q, resolve,
				resultDelivery("9-1", "task-9", dom), Options{Concurrency: 1},
				nil, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatalf("processBatch = %v, want nil (пачка подтверждается с Warn)", err)
			}
			if got := len(resolve.resultCalls()); got != 0 {
				t.Errorf("обменов %d, want 0 (эпизоды не стартуют)", got)
			}
			if n := j.count(resultEvExec) + j.count(resultEvPublish); n != 0 {
				t.Errorf("записей+публикаций = %d, want 0", n)
			}
		})
	}
}

// TestResultProcessBatchPoisonDomain — домен с пустым именем
// (детерминированная ошибка эпизода) не роняет пачку: Warn и пропуск —
// строка остаётся queued, остальные домены обрабатываются, доставка
// подтверждается.
func TestResultProcessBatchPoisonDomain(t *testing.T) {
	j := &resultJournal{}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	q := &resultFakeQueue{j: j}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")

	err := processBatch(context.Background(), pool, q, resolve,
		resultDelivery("9-2", "task-10",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"},
			queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "   "}),
		Options{Concurrency: 2}, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("processBatch = %v, want nil (ядовитый домен не роняет пачку)", err)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 1 || execs[0].args[1].(int64) != 11 {
		t.Fatalf("записи = %+v, want одна — только здоровый домен", execs)
	}
	if got := len(resolve.resultCalls()); got == 0 {
		t.Fatal("здоровый домен не обработан")
	}
}

// --- smoke цикла Run ---------------------------------------------------------

// TestResultRunSmoke — сквозной цикл: чтение q:dns → справочник → фильтр →
// эпизоды → публикация q:http → запись → XACK; чистый выход по отмене ctx.
func TestResultRunSmoke(t *testing.T) {
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	q := &resultFakeQueue{j: j, deliveries: []queue.Delivery{
		resultDelivery("1-1", "task-1",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"},
			queue.DNSDomain{ZoneID: 3, DomainID: 12, Name: "gone.example.com"}),
	}}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "example.com")
	resultSetNXDOMAIN(resolve, "gone.example.com")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{Concurrency: 2, Group: "g", Consumer: "c"})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("XACK не дождались: доставка не обработана")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v, want nil (чистый выход по отмене)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}

	// Публикация — только домену с адресами; запись — обоим.
	publishes := q.resultPublishes()
	if len(publishes) != 1 || publishes[0].task.DomainID != 11 ||
		publishes[0].task.Name != "example.com" {
		t.Fatalf("публикации = %+v, want одна q:http для example.com", publishes)
	}
	execs := pool.resultPoolExecs()
	if len(execs) != 2 {
		t.Fatalf("записей %d, want 2", len(execs))
	}
	byDomain := map[int64]resultExec{}
	for _, e := range execs {
		byDomain[e.args[1].(int64)] = e
	}
	if sql := byDomain[11].sql; sql != resultUpdateKeepStatusSQL {
		t.Errorf("example.com: SQL = %q, want без status (адреса есть)", sql)
	}
	if sql := byDomain[12].sql; sql != resultUpdateStatusSQL {
		t.Errorf("gone.example.com: SQL = %q, want со status (not_delegated)", sql)
	}
	acks := q.resultAcks()
	if len(acks) != 1 || len(acks[0].ids) != 1 || acks[0].ids[0] != "1-1" ||
		acks[0].group != "g" || acks[0].stream != queue.StreamDNS {
		t.Fatalf("XACK = %+v, want запись 1-1 группы g", acks)
	}
}

// TestResultRunIdempotentRedelivery — повторная доставка уже записанной
// пачки: фильтр отсекает done-строки — без DNS-обменов и дубликатов
// q:http, доставка подтверждается.
func TestResultRunIdempotentRedelivery(t *testing.T) {
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	pool := &resultFakePool{j: j, subdomains: resultSubs(), pending: [][2]any{}} // всё done/deleted
	q := &resultFakeQueue{j: j, deliveries: []queue.Delivery{
		resultDelivery("2-1", "task-2",
			queue.DNSDomain{ZoneID: 3, DomainID: 11, Name: "example.com"}),
	}}
	resolve := resultNewResolver() // сценарии не заданы: обмен = ошибка сценария

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{Concurrency: 1, Group: "g", Consumer: "c"})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("XACK не дождались")
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

	if got := len(resolve.resultCalls()); got != 0 {
		t.Errorf("обменов %d, want 0 (done-домены не проверяются повторно)", got)
	}
	if n := j.count(resultEvPublish); n != 0 {
		t.Errorf("публикаций %d, want 0 (дубликат q:http исключён)", n)
	}
	if n := j.count(resultEvExec); n != 0 {
		t.Errorf("записей %d, want 0", n)
	}
	if len(q.resultAcks()) != 1 {
		t.Errorf("XACK = %+v, want один", q.resultAcks())
	}
}

// TestResultRunSurvivesErrors — ошибка обработки (сбой публикации) не
// останавливает цикл и не подтверждает доставку; следующая обрабатывается;
// ошибка чтения переживается.
func TestResultRunSurvivesErrors(t *testing.T) {
	j := &resultJournal{ackNotify: make(chan struct{}, 4)}
	pool := &resultFakePool{j: j, subdomains: resultSubs()}
	q := &resultFakeQueue{j: j,
		deliveries: []queue.Delivery{
			resultDelivery("3-1", "task-3",
				queue.DNSDomain{ZoneID: 3, DomainID: 31, Name: "fail.example.com"}),
			resultDelivery("3-2", "task-4",
				queue.DNSDomain{ZoneID: 3, DomainID: 32, Name: "gone.example.com"}),
		},
		// Первая публикация q:http падает — первая доставка не подтверждается.
		publishErrs: []error{errors.New("redis unavailable")},
	}
	resolve := resultNewResolver()
	resultSetDelegated(resolve, "fail.example.com")
	resultSetNXDOMAIN(resolve, "gone.example.com")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, pool, q, resolve, Options{Concurrency: 1, Group: "g", Consumer: "c"})
	}()

	if !resultWaitAck(j, 5*time.Second) {
		cancel()
		t.Fatal("XACK второй доставки не дождались")
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

	acks := q.resultAcks()
	if len(acks) != 1 || acks[0].ids[0] != "3-2" {
		t.Fatalf("XACK = %+v, want только 3-2 (3-1 без XACK — вернётся доставкой)", acks)
	}
	// Записан только домен второй доставки; публикация пыталась — один раз
	// (безуспешно, у gone.example.com адресов нет — публиковать нечего).
	if n := j.count(resultEvExec); n != 1 {
		t.Errorf("записей %d, want 1", n)
	}
	if n := j.count(resultEvPublish); n != 1 {
		t.Errorf("попыток публикации %d, want 1 (безуспешная)", n)
	}
}
