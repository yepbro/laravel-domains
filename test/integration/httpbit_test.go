// Интеграционные тесты автомата бита http_published (T-257, ТЗ
// tasks/TZ-fsm.md §3 ФТ-3) против живого PostgreSQL: сценарная цепочка
// всех переходов спецификации (internal/dnsworker/result.go, таблица у
// resultMarkPublishedSQL — единая точка правды) реальными операторами
// писателей на одном домене: Б1 постановка после XADD q:http, Б4 пропуск
// публикации повторной доставкой, Б2 снятие записью результата, Б3 снятие
// постановкой планировщика — включая окно «XADD без бита» (повторная
// доставка публикует ДОПУСТИМЫЙ дубль) и переживание бита на pending
// (lease вернул строку незаписанной — вне эпизода бит значения не имеет);
// переживание «прочих» битов (is_idn) проверяется на каждом переходе.
// Отдельный сценарий — non-fatal отказ постановки Б1 (эпизод доводится
// до записи, дубль исключён done-строкой). Эпизоды исполняет ProcessDomain
// с детерминированным резолвером (сеть и классификация — dns_test.go на
// стенде); сбой записи эмулирует обёртка пула (стиль
// countersMatureFailPool). Дельты счётчиков зон после каждого эпизода
// сверяет реконсиляция monitor (T-178). Уровень вызовов — библиотечные,
// без запуска сервисов; изоляция — withTestSchema.
package integration

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
)

// httpBitQueue — queue.Queue сценария: публикация всегда успешна
// (транспорт — не предмет теста), q:http считается (единица наблюдения
// дублей/пропусков), q:dns постановок планировщика — нет.
type httpBitQueue struct{ http int }

func (q *httpBitQueue) Publish(_ context.Context, stream string, _ queue.Message) (string, error) {
	if stream == queue.StreamHTTP {
		q.http++
	}
	return "id-1", nil
}

func (q *httpBitQueue) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	panic("httpBitQueue: Consume не используется")
}

func (q *httpBitQueue) Ack(context.Context, string, string, ...string) error {
	panic("httpBitQueue: Ack не используется")
}

// httpBitResolver — детерминированный dnsclient.Resolver: апекс и www
// домена отвечают A-записью (эпизод всегда «есть адреса» → задача
// q:http, запись — ветка keep-status), остальные имена (случайная метка
// wildcard-детекта T-038) — NXDOMAIN.
type httpBitResolver struct{ domain string }

var _ dnsclient.Resolver = httpBitResolver{}

func (r httpBitResolver) Exchange(_ context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	fqdn := strings.ToLower(dns.Fqdn(r.domain))
	name := strings.ToLower(dns.Fqdn(q.Name))
	if q.Type == dns.TypeA && (name == fqdn || name == "www."+fqdn) {
		return &dnsclient.Result{RCode: dnsclient.RCodeNOERROR, Answer: []dns.RR{&dns.A{
			Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
			A:   net.ParseIP("192.0.2.10"),
		}}}, nil
	}
	return &dnsclient.Result{RCode: dnsclient.RCodeNXDOMAIN}, nil
}

// httpBitSubs — справочник сценария: апекс и www (оба резолвятся —
// маска 0b11, эпизод «есть адреса», запись — ветка keep-status).
func httpBitSubs() []model.Subdomain {
	return []model.Subdomain{
		{ID: 0, Label: "", Enabled: true}, // апекс
		{ID: 1, Label: "www", Enabled: true},
	}
}

// httpBitFlags — маска flags строки домена: проверка переживания
// «прочих» битов (is_idn) операторами постановки/снятия (Б1–Б3 —
// «прочие сохранены»).
func httpBitFlags(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, id int64) int16 {
	t.Helper()
	var flags int16
	if err := pool.QueryRow(ctx,
		"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&flags); err != nil {
		t.Fatalf("чтение flags: %v", err)
	}
	return flags
}

// httpBitRecordFailMark — опознаватель записи результата «есть адреса»
// (resultUpdateKeepStatusSQL dnsworker, Р3) в тексте SQL; дублирование
// литерала — стиль countersMatureIncMark (константа не экспортирована).
const httpBitRecordFailMark = "SET dns_mask = $3::int8, temp_attempts = 0"

// httpBitRecordFailPool — db.DB-обёртка пула: Query записи результата
// DNS падает (сценарий «воркер умер после XADD и пометки Б1, до записи
// Б2» — доставка не подтверждается, повтор доиграет эпизод), всё
// остальное (фильтр пачки, пометка) делегируется пулу.
type httpBitRecordFailPool struct{ *pgxpool.Pool }

func (p *httpBitRecordFailPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *httpBitRecordFailPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.Pool.Exec(ctx, sql, args...)
}

func (p *httpBitRecordFailPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, httpBitRecordFailMark) {
		return nil, errors.New("сбой записи результата (сценарий)")
	}
	return p.Pool.Query(ctx, sql, args...)
}

// httpBitMarkFailMark — опознаватель постановки бита
// (resultMarkPublishedSQL dnsworker) в тексте SQL; дублирование литерала
// — стиль countersMatureIncMark (константа не экспортирована).
const httpBitMarkFailMark = "SET flags = (t.flags | 4)::int2"

// httpBitMarkFailPool — db.DB-обёртка пула: Exec постановки бита падает
// (сценарий «отказ пометки после XADD — non-fatal Б1»: эпизод обязан
// дойти до записи), остальное делегируется пулу.
type httpBitMarkFailPool struct{ *pgxpool.Pool }

func (p *httpBitMarkFailPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *httpBitMarkFailPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, httpBitMarkFailMark) {
		return pgconn.CommandTag{}, errors.New("сбой постановки бита (сценарий)")
	}
	return p.Pool.Exec(ctx, sql, args...)
}

func (p *httpBitMarkFailPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.Pool.Query(ctx, sql, args...)
}

// httpBitFilter — фильтр пачки q:dns (resultPendingSQL) ровно на один
// домен сценария: возвращает его с состоянием бита (Б4).
func httpBitFilter(ctx context.Context, t *testing.T, pool *pgxpool.Pool, batch []queue.DNSDomain) dnsworker.PendingDomain {
	t.Helper()
	pend, err := dnsworker.PendingDomains(ctx, pool, batch)
	if err != nil {
		t.Fatalf("фильтр пачки: %v", err)
	}
	if len(pend) != 1 {
		t.Fatalf("фильтр пачки: доменов %d, хочу 1", len(pend))
	}
	return pend[0]
}

// TestHTTPPublishedBit_SpecTransitions — цепочка переходов спецификации
// на одном домене: Р1 → (окно «XADD без бита») → Б1 (+дубль окна) →
// Б4+Б2 → Р7+Р1 → Б1 → Р5 (бит пережил pending) → Б3 → публикация
// свежего эпизода без подавления. Число публикаций q:http растёт
// 1 → 2 (дубль окна) → 2 (Б4: без дубля) → 3 (Б1 эпизода 2) → 4
// (свежий эпизод после Б3).
func TestHTTPPublishedBit_SpecTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm-bit.test")
	// countersSeedDomains нумерует метки: prefix "spec" → label "spec0".
	ids := countersSeedDomains(t, pool, zone, "spec", 1, model.CheckStatePending, model.StatusNew)
	id := ids[0]
	const name = "spec0.fsm-bit.test"
	batch := []queue.DNSDomain{{ZoneID: zone, DomainID: id, Name: name}}
	subs := httpBitSubs()
	resolve := httpBitResolver{domain: name}
	q := &httpBitQueue{}
	fail := &httpBitRecordFailPool{Pool: pool}
	enqueue := func() {
		t.Helper()
		eres, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
			{ZoneID: zone, DomainID: id, Name: name},
		})
		if err != nil || eres.Queued != 1 || eres.Published != 1 {
			t.Fatalf("EnqueueBatch = %+v err=%v, хочу Queued=Published=1 (Р1)", eres, err)
		}
	}

	// «Прочий бит» is_idn (без связей со счётчиками, в отличие от
	// render_pending): спецификация Б1–Б3 требует его сохранения —
	// OR/AND ~бита не трогают чужие разряды.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
		int16(model.FlagIDN), zone, id); err != nil {
		t.Fatalf("сеем is_idn: %v", err)
	}

	// Ручная вставка домена мимо инкрементальных дельт — реконсиляция
	// авторитет (точка отсчёта счётчиков зон, стиль TestCheckState_LeaseNullRepair).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	// Р1: постановка планировщика — queued, бит чист (Б1 ещё не было),
	// is_idn пережил постановку.
	enqueue()
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || r.httpPublished {
		t.Fatalf("после постановки = %+v, хочу queued/бит чист (Р1)", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после постановки = %b, хочу только is_idn (прочие биты сохранены)", f)
	}
	countersVerifyEmpty(t, pool, "после постановки (Р1)")

	// Доставка 1 — окно «XADD без бита»: публикация q:http прошла
	// (XADD успешен), воркер умер ДО пометки и записи. Прошлая попытка
	// эмулируется руками (стиль TestDNSWorker_RedeliveryPublishedMarker):
	// ни Exec пометки, ни записи не было.
	if _, err := q.Publish(ctx, queue.StreamHTTP, queue.HTTPTask{
		ZoneID: zone, DomainID: id, Name: name,
		Subdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Label: "", Addrs: []string{"192.0.2.10"}}},
	}); err != nil {
		t.Fatalf("публикация q:http доставки 1: %v", err)
	}
	if q.http != 1 {
		t.Fatalf("публикаций q:http = %d, хочу 1", q.http)
	}

	// Доставка 2 (повторная): бит ЧИСТ — окно не закрыто, фильтр не
	// видит пометки, эпизод публикует ДУБЛЬ q:http (допустим — цена
	// окна, не потеря) и ставит бит (Б1); запись падает обёрткой —
	// доставка не подтверждена.
	dom := httpBitFilter(ctx, t, pool, batch)
	if dom.HTTPPublished {
		t.Fatal("бит стоит в окне «XADD без бита» — дубль был бы подавлен (нарушение спецификации)")
	}
	if err := dnsworker.ProcessDomain(ctx, fail, q, resolve, dom, subs, false, nil, nil); err == nil {
		t.Fatal("ProcessDomain доставки 2 = nil, хочу ошибку записи (сценарий сбоя до Б2)")
	}
	if q.http != 2 {
		t.Fatalf("публикаций q:http = %d, хочу 2 (дубль окна допустим)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || !r.httpPublished {
		t.Fatalf("после Б1 = %+v, хочу queued/бит установлен (постановка после XADD)", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN|model.FlagHTTPPublished) {
		t.Fatalf("flags после Б1 = %b, хочу is_idn|http_published (OR сохраняет прочие)", f)
	}
	countersVerifyEmpty(t, pool, "после Б1 (запись не состоялась — дельт нет)")

	// Доставка 3 (повторная): бит стоит («опубликовано, не записано») —
	// публикация ПРОПУЩЕНА (Б4), эпизод доходит до записи: done, бит
	// снят (Б2), статус не тронут (ветка «есть адреса» — итог за
	// HTTP-этапом), маска эпизода записана.
	dom = httpBitFilter(ctx, t, pool, batch)
	if !dom.HTTPPublished {
		t.Fatal("бит http_published не прочитан фильтром пачки (Б4)")
	}
	if err := dnsworker.ProcessDomain(ctx, pool, q, resolve, dom, subs, false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain доставки 3: %v", err)
	}
	if q.http != 2 {
		t.Fatalf("публикаций q:http = %d, хочу 2 (Б4: дубль исключён)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		r.httpPublished || !r.lastChecked || r.status != int16(model.StatusNew) {
		t.Fatalf("после Б2 = %+v, хочу done/бит снят/отметка есть/статус не тронут", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после Б2 = %b, хочу только is_idn (маска ~бита сохраняет прочие)", f)
	}
	var mask int64
	if err := pool.QueryRow(ctx,
		"SELECT dns_mask FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
	).Scan(&mask); err != nil {
		t.Fatalf("чтение dns_mask: %v", err)
	}
	if mask != 0b11 {
		t.Fatalf("dns_mask = %b, хочу 0b11 (апекс и www резолвятся)", mask)
	}
	countersVerifyEmpty(t, pool, "после записи результата (Б2)")

	// Эпизод 2: суточная перепроверка (Р7) возвращает домен, постановка
	// (Р1) даёт свежий queued с чистым битом.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 400 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст проверки: %v", err)
	}
	rk, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Interval: 300 * 24 * time.Hour})
	if err != nil || rk.Domains != 1 {
		t.Fatalf("RecheckOnce = %+v err=%v, хочу 1 (Р7)", rk, err)
	}
	enqueue()
	countersVerifyEmpty(t, pool, "после Р7+Р1 эпизода 2")

	// Доставка 4: эпизод публикует q:http и ставит бит (Б1), запись
	// падает (обёртка), аренда истекает — lease-возврат (Р5) оставляет
	// строку pending с ПЕРЕЖИВШИМ битом: вне эпизода бит значения не
	// имеет, читает только фильтр пачки.
	dom = httpBitFilter(ctx, t, pool, batch)
	if dom.HTTPPublished {
		t.Fatal("бит стоит до эпизода 2 — постановка не сняла (нарушение Б3)")
	}
	if err := dnsworker.ProcessDomain(ctx, fail, q, resolve, dom, subs, false, nil, nil); err == nil {
		t.Fatal("ProcessDomain доставки 4 = nil, хочу ошибку записи (сценарий сбоя до Б2)")
	}
	if q.http != 3 {
		t.Fatalf("публикаций q:http = %d, хочу 3 (Б1 эпизода 2)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || !r.httpPublished {
		t.Fatalf("после Б1 эпизода 2 = %+v, хочу queued/бит установлен", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN|model.FlagHTTPPublished) {
		t.Fatalf("flags после Б1 эпизода 2 = %b, хочу is_idn|http_published", f)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET queued_at = now() - interval '2 hours' WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст аренды: %v", err)
	}
	lres, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil || lres.Domains != 1 {
		t.Fatalf("LeaseOnce = %+v err=%v, хочу 1 (Р5)", lres, err)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStatePending) || !r.httpPublished {
		t.Fatalf("после lease-возврата = %+v, хочу pending/бит пережил (вне эпизода)", r)
	}
	countersVerifyEmpty(t, pool, "после lease-возврата (Р5)")

	// Б3: постановка нового эпизода снимает бит — иначе пометка прошлого
	// эпизода подавила бы публикацию свежего (потеря q:http).
	enqueue()
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateQueued) || r.httpPublished {
		t.Fatalf("после Б3 = %+v, хочу queued/бит снят постановкой планировщика", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после Б3 = %b, хочу только is_idn (маска ~бита сохраняет прочие)", f)
	}
	countersVerifyEmpty(t, pool, "после Б3")

	// Доставка 5: свежий эпизод с чистым битом публикует q:http БЕЗ
	// подавления (смысл Б3) и завершается записью (Б2).
	dom = httpBitFilter(ctx, t, pool, batch)
	if dom.HTTPPublished {
		t.Fatal("бит стоит после постановки — публикация эпизода была бы подавлена (нарушение Б3)")
	}
	if err := dnsworker.ProcessDomain(ctx, pool, q, resolve, dom, subs, false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain доставки 5: %v", err)
	}
	if q.http != 4 {
		t.Fatalf("публикаций q:http = %d, хочу 4 (свежий эпизод публикует после Б3)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) || r.httpPublished {
		t.Fatalf("после завершения эпизода 2 = %+v, хочу done/бит снят", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags после эпизода 2 = %b, хочу только is_idn", f)
	}
	countersVerifyEmpty(t, pool, "после завершения эпизода 2")
}

// TestHTTPPublishedBit_MarkFailureNonFatal — отказ постановки бита (Б1)
// не роняет эпизод: публикация уже состоялась (XADD прошёл), запись
// результата доводит домен до done (спецификация Б1 — non-fatal, Warn
// вместо ошибки); дубль q:http невозможен — done отсечён фильтром пачки,
// окно «XADD без бита» закрывается записью; прочие биты сохранены.
func TestHTTPPublishedBit_MarkFailureNonFatal(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // общий хелпер пакета
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "fsm-bit-nf.test")
	// countersSeedDomains нумерует метки: prefix "spec" → label "spec0".
	ids := countersSeedDomains(t, pool, zone, "spec", 1, model.CheckStateQueued, model.StatusNew)
	id := ids[0]
	const name = "spec0.fsm-bit-nf.test"
	// Строка как после Р1 (queued, аренда начата) с «прочим битом» is_idn.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1, queued_at = now() WHERE zone_id = $2 AND id = $3",
		int16(model.FlagIDN), zone, id); err != nil {
		t.Fatalf("сеем is_idn/queued_at: %v", err)
	}
	// Ручная вставка мимо дельт — реконсиляция авторитет (точка отсчёта).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	resolve := httpBitResolver{domain: name}
	q := &httpBitQueue{}
	fail := &httpBitMarkFailPool{Pool: pool}
	dom := httpBitFilter(ctx, t, pool, []queue.DNSDomain{{ZoneID: zone, DomainID: id, Name: name}})
	if err := dnsworker.ProcessDomain(ctx, fail, q, resolve, dom, httpBitSubs(), false, nil, nil); err != nil {
		t.Fatalf("ProcessDomain = %v, хочу nil (пометка non-fatal — эпизод доведён до записи)", err)
	}
	if q.http != 1 {
		t.Fatalf("публикаций q:http = %d, хочу 1 (XADD прошёл до отказа пометки)", q.http)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) ||
		r.httpPublished || !r.lastChecked {
		t.Fatalf("после эпизода = %+v, хочу done/бит чист/отметка есть", r)
	}
	if f := httpBitFlags(ctx, t, pool, zone, id); f != int16(model.FlagIDN) {
		t.Fatalf("flags = %b, хочу только is_idn (прочие биты сохранены)", f)
	}
	countersVerifyEmpty(t, pool, "после эпизода с отказом пометки")
}
