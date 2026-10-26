// Интеграционные тесты инкрементальных счётчиков конвейера (T-178, ТЗИ
// §12.2) против живого PostgreSQL (изолированная схема с миграциями,
// стиль e2e_test.go): переходы реальными функциями пакетов
// (scheduler/httpworker/ingest) ведут дельты; готовая verify-логика
// monitor сверяет счётчики с живыми агрегатами после каждого эпизода;
// сценарий дубль-доставки record (повторная запись одного домена) —
// дельты не двоятся; удаление домена в queued декрементирует правильную
// колонку; искусственный дрейф (ручной UPDATE zones) ловится verify и
// чинится recount. Прогон — make test-integration.
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
	"crawler/internal/dnsworker"
	"crawler/internal/httpworker"
	"crawler/internal/ingest"
	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
	"crawler/internal/scheduler"
	"crawler/internal/status"
)

// countersSchema — изолированная схема с миграциями (без метки
// pg_stat_activity: обрывы соединений — территория e2e).
func countersSchema(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// countersStubQueue — queue.Queue для постановки: публикация всегда
// успешна (проверяется БД-сторона переходов, не Redis).
type countersStubQueue struct{ published int }

func (q *countersStubQueue) Publish(context.Context, string, queue.Message) (string, error) {
	q.published++
	return "id-1", nil
}

func (q *countersStubQueue) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	panic("countersStubQueue: Consume не используется")
}

func (q *countersStubQueue) Ack(context.Context, string, string, ...string) error {
	panic("countersStubQueue: Ack не используется")
}

// countersSeedZone — зона справочника; возвращает id.
func countersSeedZone(t *testing.T, pool *pgxpool.Pool, name string) int16 {
	t.Helper()
	var id int16
	if err := pool.QueryRow(context.Background(),
		"INSERT INTO zones (name, is_active) VALUES ($1, true) RETURNING id", name,
	).Scan(&id); err != nil {
		t.Fatalf("зона %s: %v", name, err)
	}
	return id
}

// countersSeedDomains — вставка доменов с состоянием/статусом; prefix
// различает пачки одной зоны (UNIQUE (zone_id, label), ревью R2 P2-1);
// возвращает id по порядку.
func countersSeedDomains(t *testing.T, pool *pgxpool.Pool, zone int16, prefix string, n int, state model.CheckState, status model.Status) []int64 {
	t.Helper()
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		var id int64
		if err := pool.QueryRow(context.Background(),
			"INSERT INTO domains (zone_id, label, check_state, status) VALUES ($1, $2, $3, $4) RETURNING id",
			zone, fmt.Sprintf("%s%d", prefix, i), int16(state), int16(status),
		).Scan(&id); err != nil {
			t.Fatalf("домен %s%d: %v", prefix, i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// countersVerifyEmpty — сверка счётчиков с живыми агрегатами обязана
// сходиться (допуск 0): любая позиция — ошибка теста с именем эпизода.
func countersVerifyEmpty(t *testing.T, pool *pgxpool.Pool, what string) {
	t.Helper()
	mismatch, err := monitor.VerifyCounters(context.Background(), pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("%s: VerifyCounters: %v", what, err)
	}
	if len(mismatch) > 0 {
		t.Fatalf("%s: дрейф счётчиков после эпизода: %+v", what, mismatch)
	}
}

// TestCounters_PipelineTransitions — переходы реальными операторами
// пакетов (enqueue pending→queued, lease queued→pending, recheck
// done→pending) ведут дельты зон; после каждого эпизода счётчики
// сходятся с живыми агрегатами. Первый проход реконсиляции заполняет
// нули, повторный — нулевой дрейф (идемпотентность), глобальные
// счётчики — полная легенда model.Statuses.
func TestCounters_PipelineTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()
	q := &countersStubQueue{}

	zone := countersSeedZone(t, pool, "counters.test")
	pending := countersSeedDomains(t, pool, zone, "p", 5, model.CheckStatePending, model.StatusNew)
	countersSeedDomains(t, pool, zone, "d", 3, model.CheckStateDone, model.StatusAlive)

	// Первый проход реконсиляции — заполняет нули; повторный — нулевой
	// дрейф (идемпотентность).
	first, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	if first.Zones != 1 {
		t.Fatalf("первый проход: зон %d, хочу 1", first.Zones)
	}
	second, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("повторный проход: %v", err)
	}
	for _, p := range second.Positions {
		if p.Drift() != 0 {
			t.Fatalf("повторный проход: дрейф %s = %d→%d (проход идемпотентен)", p.Counter, p.Old, p.New)
		}
	}
	// Легенда глобальных счётчиков — в строках crawler_counters
	// (позиции дрейфа статусные строки не содержат: пишутся одной
	// реконсиляцией, R1).
	{
		rows, err := pool.Query(ctx, "SELECT name FROM crawler_counters")
		if err != nil {
			t.Fatalf("чтение счётчиков: %v", err)
		}
		names := map[string]bool{}
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatalf("чтение счётчиков: строка: %v", err)
			}
			names[n] = true
		}
		rows.Close()
		for _, want := range monitor.CounterNames() {
			if !names[want] {
				t.Fatalf("легенда глобальных счётчиков без %s", want)
			}
		}
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")

	// enqueue: pending → queued (дельты pending −5 / queued +5).
	batch := make([]scheduler.DispatchRow, 0, len(pending))
	for i, id := range pending {
		batch = append(batch, scheduler.DispatchRow{ZoneID: zone, DomainID: id, Name: fmt.Sprintf("p%d.counters.test", i)})
	}
	res, err := scheduler.EnqueueBatch(ctx, pool, q, batch)
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if res.Queued != len(pending) {
		t.Fatalf("EnqueueBatch: переведено %d, хочу %d", res.Queued, len(pending))
	}
	countersVerifyEmpty(t, pool, "после enqueue")

	// lease: истёкшая аренда возвращает переведённые строки в pending
	// (Timeout по умолчанию 30 минут — сдвигаем queued_at в прошлое).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET queued_at = now() - interval '2 hours' WHERE zone_id = $1 AND check_state = $2",
		zone, int16(model.CheckStateQueued)); err != nil {
		t.Fatalf("возраст аренды: %v", err)
	}
	leaseRes, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if leaseRes.Domains != int64(len(pending)) {
		t.Fatalf("LeaseOnce: возвращено %d, хочу %d", leaseRes.Domains, len(pending))
	}
	countersVerifyEmpty(t, pool, "после lease")

	// recheck (возрастная перепроверка): done-строки старые — перевод
	// checked −n / pending +n.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 400 WHERE zone_id = $1 AND check_state = $2",
		zone, int16(model.CheckStateDone)); err != nil {
		t.Fatalf("возраст done-строк: %v", err)
	}
	rk, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Interval: 300 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if rk.Domains != 3 {
		t.Fatalf("RecheckOnce: переведено %d, хочу 3", rk.Domains)
	}
	countersVerifyEmpty(t, pool, "после recheck")
}

// TestCounters_RecordDoubleDelivery — запись результата HTTP-эпизода
// поверх done (DNS-этап уже перевёл домен, T-178: DNS — главный переход
// queued → done) и её повторная доставка: дельт зон нет ни у одной
// записи (старое состояние строки — done, ТЗИ §6.4); суточный счётчик
// проверок (T-179) считает события записи — по +1 на доставку, —
// реконсиляция сводит строку к живому окну (один домен), дрейф
// daily:checks наблюдаем в позициях прохода; сверка сходится (суточная
// строка в verify не входит — принятое расхождение семантик,
// TZ_slow_done_today §5.1).
func TestCounters_RecordDoubleDelivery(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "record.test")
	ids := countersSeedDomains(t, pool, zone, "q", 1, model.CheckStateQueued, model.StatusNew)
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = 1, domains_queued = 1 WHERE id = $1", zone); err != nil {
		t.Fatalf("счётчики зоны: %v", err)
	}

	// DNS-эпизод переводит queued → done (терминальный статус не нужен:
	// ветка «есть адреса» — итог за HTTP-этапом).
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, ids[0], dnsworker.EpisodeResult{Mask: 1})
	if err != nil || !ok {
		t.Fatalf("RecordEpisode: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после DNS-перехода")

	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	res := &httpworker.DomainResult{ZoneID: zone, DomainID: ids[0], Name: "q0.record.test"}
	for i := 0; i < 2; i++ { // дубль-доставка поверх done — без дельт зон
		ok, err := rec.RecordAndPublish(ctx, res)
		if err != nil || !ok {
			t.Fatalf("запись %d: ok=%v err=%v", i+1, ok, err)
		}
	}
	var queued, checked int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_queued, domains_checked FROM zones WHERE id = $1", zone,
	).Scan(&queued, &checked); err != nil {
		t.Fatalf("чтение зоны: %v", err)
	}
	if queued != 0 || checked != 1 {
		t.Fatalf("после record×2 queued=%d checked=%d, хочу 0/1 (дельта один раз — на DNS-переходе)", queued, checked)
	}

	// Суточный счётчик (T-179): две записи результата — два события
	// (дельта не двоится на доставку: одна доставка — один инкремент);
	// живое окно — один домен с last_checked_at = current_date.
	if got := countersChecksToday(t, pool); got != 2 {
		t.Fatalf("после record×2 checks:<день> = %d, хочу 2 (по одному событию на запись)", got)
	}
	countersVerifyEmpty(t, pool, "после record×2 поверх done (суточная строка вне verify)")

	// Реконсиляция — авторитет: строка дня := живое окно (1), дрейф
	// |2 − 1| виден позицией daily:checks прохода.
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersChecksToday(t, pool); got != 1 {
		t.Fatalf("после реконсиляции checks:<день> = %d, хочу 1 (живое окно: один домен сегодня)", got)
	}
	drifted := false
	for _, p := range pass.Positions {
		if p.Counter != "daily:checks" {
			continue
		}
		drifted = true
		if p.Old != 2 || p.New != 1 || p.Drift() != 1 {
			t.Fatalf("daily:checks = %+v, хочу old=2 new=1 drift=1", p)
		}
	}
	if !drifted {
		t.Fatal("позиция daily:checks отсутствует в итогах прохода")
	}
	countersVerifyEmpty(t, pool, "после реконсиляции")
}

// countersChecksToday — значение строки суточного счётчика проверок
// текущего дня (имя вычисляет БД — тот же current_date, что у инкремента
// и окна реконсиляции); отсутствующая строка — ошибка теста.
func countersChecksToday(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return countersDailyToday(t, pool, "checks")
}

// countersTempToday — значение строки суточного числителя temp_error
// текущего дня (T-182); отсутствующая строка — ошибка теста.
func countersTempToday(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return countersDailyToday(t, pool, "temp_error")
}

// countersRetryMatureToday — значение сегодняшней корзины зрелости
// ретрая (T-202); отсутствующая строка — ошибка теста.
func countersRetryMatureToday(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	return countersDailyToday(t, pool, "retry_mature")
}

// countersDailyToday — значение суточной строки <prefix>:YYYY-MM-DD.
func countersDailyToday(t *testing.T, pool *pgxpool.Pool, prefix string) int64 {
	t.Helper()
	var v int64
	err := pool.QueryRow(context.Background(),
		"SELECT value FROM crawler_counters WHERE name = $1 || to_char(current_date, 'YYYY-MM-DD')",
		prefix+":",
	).Scan(&v)
	if err != nil {
		t.Fatalf("чтение %s:<день>: %v", prefix, err)
	}
	return v
}

// TestCounters_DNSTempErrorShare — суточный числитель temp_error (T-182,
// TZ_slow_dns_share §8.2): temp_error-эпизод растит числитель и знаменатель
// вместе; повторный эпизод без temp_error — только знаменатель; дубль
// temp_error-эпизода — снова оба (доля устойчива к дублям); реконсиляция
// сводит обе строки к живому окну (доля считается по строкам — скан окна
// суток из алерта исчез), позиции daily:checks/daily:temp_error наблюдают
// разницу семантик, verify суточные строки не сверяет.
func TestCounters_DNSTempErrorShare(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "share.test")
	ids := countersSeedDomains(t, pool, zone, "s", 2, model.CheckStateQueued, model.StatusNew)
	// Стартовый проход — колонки зон по живому агрегату (2 queued), суточные
	// строки дня нулями (домены сегодня не проверялись).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("стартовый проход: %v", err)
	}
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	// temp_error-эпизод: записи нет, все проверки — таймауты (вердикт
	// temp_error, §5.5 п. 9).
	tempRes := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "s0.share.test",
		Subdomains: []httpworker.SubdomainResult{{
			SubdomainID: 0,
			HTTPS:       httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
			HTTP:        httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
		}},
	}
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("temp_error-эпизод: ok=%v err=%v", ok, err)
	}
	if got := countersChecksToday(t, pool); got != 1 {
		t.Fatalf("после temp_error-эпизода checks:<день> = %d, хочу 1", got)
	}
	if got := countersTempToday(t, pool); got != 1 {
		t.Fatalf("после temp_error-эпизода temp_error:<день> = %d, хочу 1", got)
	}

	// Эпизод без temp_error (молчит постоянно — no_http): знаменатель
	// растёт, числитель не тронут.
	silentRes := &httpworker.DomainResult{ZoneID: zone, DomainID: ids[1], Name: "s1.share.test"}
	if ok, err := rec.RecordAndPublish(ctx, silentRes); err != nil || !ok {
		t.Fatalf("no_http-эпизод: ok=%v err=%v", ok, err)
	}
	if got := countersChecksToday(t, pool); got != 2 {
		t.Fatalf("после no_http-эпизода checks:<день> = %d, хочу 2", got)
	}
	if got := countersTempToday(t, pool); got != 1 {
		t.Fatalf("после no_http-эпизода temp_error:<день> = %d, хочу 1 (не temp_error-итог не инкрементирует)", got)
	}

	// Дубль-доставка temp_error-эпизода: +1 в числитель и знаменатель
	// одновременно — доля не менялась бы, события записи честны.
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("дубль temp_error-эпизода: ok=%v err=%v", ok, err)
	}
	if got := countersChecksToday(t, pool); got != 3 {
		t.Fatalf("после дубля checks:<день> = %d, хочу 3", got)
	}
	if got := countersTempToday(t, pool); got != 2 {
		t.Fatalf("после дубля temp_error:<день> = %d, хочу 2", got)
	}
	countersVerifyEmpty(t, pool, "после эпизодов доли (суточные строки вне verify)")

	// Реконсиляция — авторитет: живое окно — 2 домена дня (у одного
	// статус temp_error), доля 1/2; инкременты дали 3/2 (дубль) —
	// позиции daily:* наблюдают разницу.
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersChecksToday(t, pool); got != 2 {
		t.Fatalf("после реконсиляции checks:<день> = %d, хочу 2 (живое окно: два домена сегодня)", got)
	}
	if got := countersTempToday(t, pool); got != 1 {
		t.Fatalf("после реконсиляции temp_error:<день> = %d, хочу 1 (один temp_error-домен дня)", got)
	}
	byName := map[string]monitor.CounterPosition{}
	for _, p := range pass.Positions {
		byName[p.Counter] = p
	}
	if p := byName["daily:checks"]; p.Old != 3 || p.New != 2 || p.Drift() != 1 {
		t.Fatalf("daily:checks = %+v, хочу old=3 new=2 drift=1", p)
	}
	if p := byName["daily:temp_error"]; p.Old != 2 || p.New != 1 || p.Drift() != 1 {
		t.Fatalf("daily:temp_error = %+v, хочу old=2 new=1 drift=1 (дубль события)", p)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции доли")
}

// TestCounters_RetryMatureBaskets — корзины зрелости ретрая (T-202):
// temp_error-эпизоды ОБЕИХ веток конвейера (HTTP-итог и терминальная
// temp_error-ветка DNS-этапа — массовый DNS-инцидент приходит через
// неё) инкрементируют сегодняшнюю корзину одним и тем же ключом; итогы
// без temp_error корзину не трогают; дубль-доставка — ещё одно событие
// (потоковая семантика, как у суточных строк); реконсиляция пишет
// корзину живым окном (temp_error × done × дата), позиция
// retry_mature:today наблюдает разницу семантик. Guard читает только
// корзины прошедших суток — сегодняшняя тяжёлую выборку не открывает.
func TestCounters_RetryMatureBaskets(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "mature.test")
	ids := countersSeedDomains(t, pool, zone, "m", 3, model.CheckStateQueued, model.StatusNew)
	// Стартовый проход — создаёт корзины нулями (сегодняшних temp_error
	// эпизодов ещё нет).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("стартовый проход: %v", err)
	}
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	// HTTP temp_error-эпизод (все проверки — таймауты): корзина суток +1.
	tempRes := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "m0.mature.test",
		Subdomains: []httpworker.SubdomainResult{{
			SubdomainID: 0,
			HTTPS:       httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
			HTTP:        httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
		}},
	}
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("temp_error-эпизод HTTP: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 1 {
		t.Fatalf("после temp_error-эпизода HTTP retry_mature:<день> = %d, хочу 1", got)
	}

	// no_http-эпизод: корзину не трогает — зреют только temp_error-итоги.
	silentRes := &httpworker.DomainResult{ZoneID: zone, DomainID: ids[1], Name: "m1.mature.test"}
	if ok, err := rec.RecordAndPublish(ctx, silentRes); err != nil || !ok {
		t.Fatalf("no_http-эпизод: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 1 {
		t.Fatalf("после no_http-эпизода retry_mature:<день> = %d, хочу 1", got)
	}

	// Терминальная temp_error-ветка DNS-этапа (SERVFAIL апекса): тот же
	// ключ корзины — CTE того же оператора записи (T-202).
	if ok, err := dnsworker.RecordEpisode(ctx, pool, zone, ids[2], dnsworker.EpisodeResult{
		SetStatus: true,
		Status:    model.StatusTempError,
	}); err != nil || !ok {
		t.Fatalf("temp_error-эпизод DNS: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 2 {
		t.Fatalf("после temp_error-эпизода DNS retry_mature:<день> = %d, хочу 2", got)
	}

	// Дубль-доставка HTTP temp_error-эпизода: +1 событие (живое окно
	// насчитает 2 домена — разницу семантик сводит реконсиляция).
	if ok, err := rec.RecordAndPublish(ctx, tempRes); err != nil || !ok {
		t.Fatalf("дубль temp_error-эпизода: ok=%v err=%v", ok, err)
	}
	if got := countersRetryMatureToday(t, pool); got != 3 {
		t.Fatalf("после дубля retry_mature:<день> = %d, хочу 3", got)
	}
	countersVerifyEmpty(t, pool, "после эпизодов (корзины вне verify)")

	// Guard тяжёлой выборки ретрая: сегодняшняя корзина в сумму НЕ входит
	// (эпизоды зреют к полуночи) — зрелых нет, сумма 0.
	var mature int64
	if err := pool.QueryRow(ctx,
		"SELECT coalesce(sum(value), 0) FROM crawler_counters "+
			"WHERE name >= 'retry_mature:' AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
	).Scan(&mature); err != nil {
		t.Fatalf("guard-сумма корзин: %v", err)
	}
	if mature != 0 {
		t.Fatalf("сумма зрелых корзин = %d, хочу 0 (сегодняшние эпизоды незрелые)", mature)
	}

	// Реконсиляция — авторитет: корзина суток := живое окно
	// (temp_error × done × текущая дата — два домена), позиция
	// retry_mature:today наблюдает разницу (дубль события).
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersRetryMatureToday(t, pool); got != 2 {
		t.Fatalf("после реконсиляции retry_mature:<день> = %d, хочу 2 (живое окно: два temp_error-домена сегодня)", got)
	}
	byName := map[string]monitor.CounterPosition{}
	for _, p := range pass.Positions {
		byName[p.Counter] = p
	}
	if p := byName["retry_mature:today"]; p.Old != 3 || p.New != 2 || p.Drift() != 1 {
		t.Fatalf("retry_mature:today = %+v, хочу old=3 new=2 drift=1 (дубль события)", p)
	}
	if p := byName["retry_mature:past"]; p.Old != 0 || p.New != 0 || p.Drift() != 0 {
		t.Fatalf("retry_mature:past = %+v, хочу нули (зрелой когорты нет)", p)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции корзин")
}

// TestCounters_DailyChecksDayChange — ротация суток (T-179, ТЗ §9.2):
// эмуляция смены даты фиксированной таймзоной соединения (current_date
// выводится из TimeZone сессии): записи «в разные дни» ведут разные
// строки checks:, вчерашняя остаётся историей темпа, живое окно каждого
// дня считает только его записи. Пул с MaxConns=1 — SET TimeZone
// действует на единственное соединение, транзакции записи видят его.
func TestCounters_DailyChecksDayChange(t *testing.T) {
	skipUnlessIntegration(t)
	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("миграции: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("разбор DSN: %v", err)
	}
	cfg.MaxConns = 1 // TimeZone сессионный: все операторы — одно соединение
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("пул: %v", err)
	}
	t.Cleanup(pool.Close)
	ctx := context.Background()

	// Pacific/Midway (UTC−11) и Pacific/Kiritimati (UTC+14) —
	// разносят current_date минимум на сутки в любой момент прогона
	// (25 часов между зонами; DST в обеих нет). set_config —
	// параметризуемый аналог SET TIME ZONE (сам SET параметров не
	// принимает), is_local=false — уровень сессии.
	const (
		tzDay1 = "Pacific/Midway"
		tzDay2 = "Pacific/Kiritimati"
	)
	setTZ := func(tz string) {
		t.Helper()
		if _, err := pool.Exec(ctx, "SELECT set_config('TimeZone', $1, false)", tz); err != nil {
			t.Fatalf("таймзона %s: %v", tz, err)
		}
	}
	var day1, day2 string
	readDate := func(tz string) string {
		setTZ(tz)
		var d string
		if err := pool.QueryRow(ctx, "SELECT to_char(current_date, 'YYYY-MM-DD')").Scan(&d); err != nil {
			t.Fatalf("current_date (%s): %v", tz, err)
		}
		return d
	}
	day1 = readDate(tzDay1)
	day2 = readDate(tzDay2)
	if day1 == day2 {
		t.Fatalf("таймзоны не развели сутки: %s == %s (прогон в окне ±14ч вокруг полуночи — повторить)", day1, day2)
	}

	zone := countersSeedZone(t, pool, "days.test")
	ids := countersSeedDomains(t, pool, zone, "d", 2, model.CheckStateDone, model.StatusNew)
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	checksRow := func(day string) (int64, bool) {
		var v int64
		err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = $1", "checks:"+day).Scan(&v)
		if err != nil {
			return 0, false
		}
		return v, true
	}

	// День 1: первый домен записан — строка дня создана инкрементом.
	setTZ(tzDay1)
	if ok, err := rec.RecordAndPublish(ctx, &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "d0.days.test",
	}); err != nil || !ok {
		t.Fatalf("запись дня 1: ok=%v err=%v", ok, err)
	}
	if v, ok := checksRow(day1); !ok || v != 1 {
		t.Fatalf("checks:%s = (%d, %v), хочу (1, true) — строку создаёт первый инкремент суток", day1, v, ok)
	}

	// День 2 (для соединения): второй домен — новая строка, вчерашняя
	// не тронута.
	setTZ(tzDay2)
	if ok, err := rec.RecordAndPublish(ctx, &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[1], Name: "d1.days.test",
	}); err != nil || !ok {
		t.Fatalf("запись дня 2: ok=%v err=%v", ok, err)
	}
	if v, ok := checksRow(day2); !ok || v != 1 {
		t.Fatalf("checks:%s = (%d, %v), хочу (1, true) — новая строка нового дня", day2, v, ok)
	}
	if v, ok := checksRow(day1); !ok || v != 1 {
		t.Fatalf("checks:%s = (%d, %v) после записи дня 2, хочу (1, true) — история не тронута", day1, v, ok)
	}

	// Реконсиляция «дня 2»: окно current_date видит только домен дня 2
	// (last_checked_at дня 1 вне окна), строка дня 2 сходится, день 1
	// проход не перезаписывает.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if v, _ := checksRow(day2); v != 1 {
		t.Fatalf("после реконсиляции checks:%s = %d, хочу 1", day2, v)
	}
	if v, _ := checksRow(day1); v != 1 {
		t.Fatalf("реконсиляция тронула вчерашнюю строку checks:%s = %d, хочу 1 (история)", day1, v)
	}
}

// TestCounters_DNSStageTransitions — главный переход конвейера
// queued/pending → done ведёт дельты во всех ветках DNS-этапа
// (терминальный статус, «есть адреса», диагноз invalid), включая
// домен с истёкшей посреди эпизода арендой (pending) и повторную
// доставку (done — идемпотентный no-op, ревью P1-1).
func TestCounters_DNSStageTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "dns.test")
	ids := countersSeedDomains(t, pool, zone, "p", 4, model.CheckStatePending, model.StatusNew)
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	// Постановка: 4 × pending → queued.
	q := &countersStubQueue{}
	batch := make([]scheduler.DispatchRow, len(ids))
	for i, id := range ids {
		batch[i] = scheduler.DispatchRow{ZoneID: zone, DomainID: id, Name: fmt.Sprintf("p%d.dns.test", i)}
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	countersVerifyEmpty(t, pool, "после enqueue")

	// d0 «вернула аренда» посреди эпизода: pending → done — дельта
	// pending −1/checked +1 (предчтение старого состояния, ТЗИ §6.1.2).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET check_state = 0, queued_at = NULL WHERE zone_id = $1 AND id = $2",
		zone, ids[0]); err != nil {
		t.Fatalf("возврат аренды: %v", err)
	}
	if err := countersShiftForManual(ctx, pool, zone, 0, 1, -1, 0); err != nil {
		t.Fatalf("дельта возврата: %v", err)
	}

	// Терминальный статус (not_delegated): queued → done.
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, ids[1], dnsworker.EpisodeResult{
		SetStatus: true, Status: model.StatusNotDelegated, Mask: 1,
	})
	if err != nil || !ok {
		t.Fatalf("RecordEpisode terminal: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после терминального статуса")

	// Ветка «есть адреса»: pending → done.
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, ids[0], dnsworker.EpisodeResult{Mask: 3})
	if err != nil || !ok {
		t.Fatalf("RecordEpisode keep-status: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после ветки «есть адреса»")

	// Повторная доставка уже зафиксированного эпизода — no-op.
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, ids[1], dnsworker.EpisodeResult{
		SetStatus: true, Status: model.StatusNotDelegated, Mask: 1,
	})
	if err != nil || ok {
		t.Fatalf("RecordEpisode дубль: ok=%v err=%v, хочу (false, nil)", ok, err)
	}
	countersVerifyEmpty(t, pool, "после дубля доставки")

	// Диагноз invalid: queued → done, строка покидает цикл.
	ok, err = dnsworker.RecordInvalid(ctx, pool, zone, ids[2])
	if err != nil || !ok {
		t.Fatalf("RecordInvalid: ok=%v err=%v", ok, err)
	}
	countersVerifyEmpty(t, pool, "после диагноза invalid")

	var queued, checked int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_queued, domains_checked FROM zones WHERE id = $1", zone,
	).Scan(&queued, &checked); err != nil {
		t.Fatalf("чтение зоны: %v", err)
	}
	if queued != 1 || checked != 3 {
		t.Fatalf("после DNS-переходов queued=%d checked=%d, хочу 1/3", queued, checked)
	}

	// Суточный счётчик проверок (T-179): инкремент — событие записи
	// результата httpworker, DNS-переходы его не дают (эпизода HTTP у
	// терминальных веток нет) — строка дня осталась нулём стартового
	// прохода; живое окно выросло до 3 (last_checked_at = current_date
	// ставит и DNS-этап). Проход — авторитет: перезаписывает строку
	// окном, дрейф-позиция daily:checks наблюдает разницу потоков
	// (3 DNS-only завершения), не «нули-аномалию».
	if got := countersChecksToday(t, pool); got != 0 {
		t.Fatalf("после DNS-переходов checks:<день> = %d, хочу 0 (инкремент — только запись результата HTTP)", got)
	}
	pass, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if got := countersChecksToday(t, pool); got != 3 {
		t.Fatalf("после реконсиляции checks:<день> = %d, хочу 3 (живое окно всех веток конвейера)", got)
	}
	dailySeen := false
	for _, p := range pass.Positions {
		if p.Counter != "daily:checks" {
			continue
		}
		dailySeen = true
		if p.Old != 0 || p.New != 3 || p.Drift() != 3 {
			t.Fatalf("daily:checks = %+v, хочу old=0 new=3 drift=3 (DNS-only завершения окна)", p)
		}
	}
	if !dailySeen {
		t.Fatal("позиция daily:checks отсутствует в итогах прохода")
	}
}

// countersShiftForManual — дельта счётчиков зоны для ручного перевода
// строки мимо кода (вспомогательный перевод сценария «аренда истекла
// посреди эпизода»: total, pending, queued, checked).
func countersShiftForManual(ctx context.Context, pool *pgxpool.Pool, zone int16, total, pending, queued, checked int64) error {
	_, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = domains_total + $2, domains_pending = domains_pending + $3, "+
			"domains_queued = domains_queued + $4, domains_checked = domains_checked + $5 WHERE id = $1",
		zone, total, pending, queued, checked)
	return err
}

// TestCounters_DeleteWhileQueued — удаление доменов в queued
// (delete-файл через staging): декремент queued-колонки (check_state
// строк возвращён оператором), total тоже; сверка сходится (ТЗИ §12.2).
func TestCounters_DeleteWhileQueued(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "delete.test")
	countersSeedDomains(t, pool, zone, "q", 2, model.CheckStateQueued, model.StatusNew)
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = 2, domains_queued = 2 WHERE id = $1", zone); err != nil {
		t.Fatalf("счётчики зоны: %v", err)
	}

	// Партия delete в staging (StageTable — прямой INSERT: колонки
	// партии, line_no заполняет DEFAULT).
	batch, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	for i, label := range []string{"q0", "q1"} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
			batch[:], zone, label, int16(model.ActionDelete), int16(i+1)); err != nil {
			t.Fatalf("staging %s: %v", label, err)
		}
	}
	dres, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionDelete})
	if err != nil {
		t.Fatalf("ApplyDelete: %v", err)
	}
	if dres.Deleted != 2 || dres.NotFound != 0 {
		t.Fatalf("итог delete = %+v, хочу {2 0}", dres)
	}
	var total, queued int64
	if err := pool.QueryRow(ctx,
		"SELECT domains_total, domains_queued FROM zones WHERE id = $1", zone,
	).Scan(&total, &queued); err != nil {
		t.Fatalf("чтение зоны: %v", err)
	}
	if total != 0 || queued != 0 {
		t.Fatalf("после delete total=%d queued=%d, хочу 0/0", total, queued)
	}
	countersVerifyEmpty(t, pool, "после delete в queued")
}

// TestCounters_DeleteWithRenderPending — удаление домена с битом
// render_pending (T-186): применение delete снимает бит той же
// транзакцией и декрементирует счётчик рендер-долга (инвариант T-175
// «счётчик = число строк с битом»); прочие биты сохраняются; повторное
// применение партии (at-least-once, ТЗ 3.4) не декрементирует дважды;
// сверка сходится.
func TestCounters_DeleteWithRenderPending(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "render-del.test")
	ids := countersSeedDomains(t, pool, zone, "r", 2, model.CheckStateDone, model.StatusAlive)
	// Бит render_pending (2) поверх is_idn (1) — как постановка флага в
	// записи результата (renderq.go); счётчик долга — те же переходы
	// (бэкфилл 000015 считал бы 2).
	for _, id := range ids {
		if _, err := pool.Exec(ctx,
			"UPDATE domains SET flags = 3 WHERE zone_id = $1 AND id = $2", zone, id); err != nil {
			t.Fatalf("бит render_pending домена %d: %v", id, err)
		}
	}
	// Строка счётчика существует после миграции 000015 (бэкфилл пустой
	// схемы — 0); сеем значение, как вели бы переходы постановки бита.
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) VALUES ('render_pending', 2) "+
			"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value"); err != nil {
		t.Fatalf("счётчик долга: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_total = 2, domains_checked = 2 WHERE id = $1", zone); err != nil {
		t.Fatalf("счётчики зоны: %v", err)
	}

	apply := func(batch ingest.BatchID) ingest.DeleteResult {
		t.Helper()
		for i, label := range []string{"r0", "r1"} {
			if _, err := pool.Exec(ctx,
				"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
				batch[:], zone, label, int16(model.ActionDelete), int16(i+1)); err != nil {
				t.Fatalf("staging %s: %v", label, err)
			}
		}
		res, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionDelete})
		if err != nil {
			t.Fatalf("ApplyDelete: %v", err)
		}
		return res
	}

	if res := apply(mustBatchID(t)); res.Deleted != 2 || res.NotFound != 0 {
		t.Fatalf("итог delete = %+v, хочу {2 0}", res)
	}

	// Бит снят, is_idn сохранён; счётчик долга декрементирован на 2.
	for i, id := range ids {
		var flags int16
		if err := pool.QueryRow(ctx,
			"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
		).Scan(&flags); err != nil {
			t.Fatalf("домен r%d: %v", i, err)
		}
		if flags != 1 {
			t.Fatalf("домен r%d: flags = %d, хочу 1 (бит render_pending снят, is_idn сохранён)", i, flags)
		}
	}
	var debt int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&debt); err != nil {
		t.Fatalf("счётчик долга: %v", err)
	}
	if debt != 0 {
		t.Fatalf("счётчик рендер-долга = %d, хочу 0 (декремент на 2)", debt)
	}
	countersVerifyEmpty(t, pool, "после delete с битом")

	// Повторная доставка того же delete-содержимого (новая партия,
	// at-least-once, ТЗ 3.4): домены уже deleted — охрана статуса не
	// декрементирует счётчик дважды и не трогает биты, отчёт считает
	// строки «удалёнными» (семантика T-019).
	if res := apply(mustBatchID(t)); res.Deleted != 2 || res.NotFound != 0 {
		t.Fatalf("повтор = %+v, хочу {2 0} (уже удалён — тоже «удалён»)", res)
	}
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&debt); err != nil {
		t.Fatalf("счётчик долга (повтор): %v", err)
	}
	if debt != 0 {
		t.Fatalf("счётчик рендер-долга после повтора = %d, хочу 0 (идемпотентность)", debt)
	}
	countersVerifyEmpty(t, pool, "после повтора delete")
}

// mustBatchID — новая партия для повторного применения сценария T-186.
func mustBatchID(t *testing.T) ingest.BatchID {
	t.Helper()
	id, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestCounters_DriftCaughtAndFixed — искусственный дрейф (ручной UPDATE
// zones мимо кода): verify ловит позицию (для CLI это код 1), recount
// чинит, повторная verify чиста (критерий ТЗИ §12.3).
func TestCounters_DriftCaughtAndFixed(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "drift.test")
	countersSeedDomains(t, pool, zone, "p", 4, model.CheckStatePending, model.StatusNew)
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}

	// Дрейф: ручной SQL мимо кода переходов.
	if _, err := pool.Exec(ctx,
		"UPDATE zones SET domains_pending = domains_pending + 5 WHERE id = $1", zone); err != nil {
		t.Fatalf("дрейф: %v", err)
	}
	mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(mismatch) != 1 || mismatch[0].Counter != fmt.Sprintf("zone:%d:pending", zone) {
		t.Fatalf("verify = %+v, хочу единственную зону:%d:pending", mismatch, zone)
	}

	// recount чинит; повторная verify чиста.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("recount: %v", err)
	}
	countersVerifyEmpty(t, pool, "после recount")
}

// countersHourRenderer — Renderer для часовых корзин (T-203): живой
// результат рендера без Chromium (проверяется БД-сторона — флаг rendered
// и корзина, не браузер).
type countersHourRenderer struct{}

func (countersHourRenderer) Run(_ context.Context, _ render.RenderTask) (render.RenderOutput, error) {
	return render.RenderOutput{
		FinalURL: "https://h0.hour.test/rendered",
		DOM:      "<html><body>dom</body></html>",
	}, nil
}

// countersHourlyBucket — значение корзины <prefix>:YYYY-MM-DD"T"HH24
// (boundary — SQL-выражение часа, вычисляет БД); отсутствующая строка —
// (0, false).
func countersHourlyBucket(t *testing.T, pool *pgxpool.Pool, prefix, boundary string) (int64, bool) {
	t.Helper()
	var v int64
	err := pool.QueryRow(context.Background(),
		"SELECT value FROM crawler_counters WHERE name = $1 || to_char("+boundary+", 'YYYY-MM-DD\"T\"HH24')",
		prefix+":",
	).Scan(&v)
	if err != nil {
		return 0, false
	}
	return v, true
}

// TestCounters_HourlyThroughputBaskets — часовые корзины темпа (T-203):
// эпизод записи результата ведёт results-корзину текущего часа дельтой
// вставленных строк (молчаливый эпизод корзину не трогает); рендер ведёт
// rendered-корзину часа checked_at строки (флаг и корзина — одной
// транзакцией); реконсиляция сводит корзины с живым count по календарному
// часу (prev-корзину создаёт по строке, сидированной мимо инкрементов) и
// удаляет устаревшие корзины; читатель темпа (status.FetchThroughput)
// собирает окно из двух живых корзин.
func TestCounters_HourlyThroughputBaskets(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx := context.Background()

	zone := countersSeedZone(t, pool, "hour.test")
	ids := countersSeedDomains(t, pool, zone, "h", 3, model.CheckStateDone, model.StatusNew)
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("стартовый проход: %v", err)
	}
	rec, err := httpworker.NewRecorder(pool, &countersStubQueue{}, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	// Строка прошлого часа, сидированная мимо инкрементов (перепроверка
	// «до деплоя»): prev-корзину обязан создать проход живым окном.
	// Момент — первая минута предыдущего календарного часа (граница от
	// БД: строка гарантированно внутри prev-часа в любой момент прогона;
	// гонка с границей часа посреди теста — окно в миллисекунды).
	// Домен — ids[2], БЕЗ эпизодов записи: каждый RecordAndPublish
	// перезаписывает результат домена целиком (recordDeleteSQL, §3.5
	// «история проверок не хранится») — строка под h0/h1 была бы стёрта
	// эпизодами ниже, а не дожила бы до реконсиляции.
	if _, err := pool.Exec(ctx,
		"INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, final_url, checked_at) "+
			"VALUES ($1, $2, 1, 2, 'https://old.hour.test/', "+
			"date_trunc('hour', now() - interval '1 hour') + interval '1 minute')",
		zone, ids[2]); err != nil {
		t.Fatalf("строка прошлого часа: %v", err)
	}

	// Эпизод с двумя записями (два субдомена × по записи): корзина
	// текущего часа +2 — дельта равна вставленным строкам.
	rich := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[0], Name: "h0.hour.test",
		Subdomains: []httpworker.SubdomainResult{
			{
				SubdomainID: 0,
				Records: []httpworker.Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					FinalURL: "https://h0.hour.test/", HTTPStatus: 200,
				}},
			},
			{
				SubdomainID: 2,
				Records: []httpworker.Record{{
					Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
					FinalURL: "https://www.hour.test/", HTTPStatus: 200,
				}},
			},
		},
	}
	if ok, err := rec.RecordAndPublish(ctx, rich); err != nil || !ok {
		t.Fatalf("эпизод с записями: ok=%v err=%v", ok, err)
	}
	if v, ok := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now())"); !ok || v != 2 {
		t.Fatalf("после эпизода results:<час> = (%d, %v), хочу (2, true) — дельта вставленных строк", v, ok)
	}

	// Молчаливый temp_error-эпизод: записей нет — корзину не трогает.
	silent := &httpworker.DomainResult{
		ZoneID: zone, DomainID: ids[1], Name: "h1.hour.test",
		Subdomains: []httpworker.SubdomainResult{{
			SubdomainID: 0,
			HTTPS:       httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
			HTTP:        httpworker.SchemeCheck{ErrClass: httpworker.ErrTimeout},
		}},
	}
	if ok, err := rec.RecordAndPublish(ctx, silent); err != nil || !ok {
		t.Fatalf("молчаливый эпизод: ok=%v err=%v", ok, err)
	}
	if v, _ := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now())"); v != 2 {
		t.Fatalf("после молчаливого эпизода results:<час> = %d, хочу 2 (эпизод без строк корзину не трогает)", v)
	}

	// Рендер строки эпизода: rendered-корзина часа checked_at строки +1
	// (флаг и корзина — одна транзакция замены контента).
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rw, err := renderworker.New(pool, &countersStubQueue{}, store, countersHourRenderer{}, renderworker.Options{})
	if err != nil {
		t.Fatalf("renderworker.New: %v", err)
	}
	rendered, err := rw.ProcessResult(ctx, queue.Result{
		ZoneID: zone, DomainID: ids[0], SubdomainID: 0, Variant: model.VariantMain, Name: "h0.hour.test",
	})
	if err != nil || !rendered {
		t.Fatalf("ProcessResult: ok=%v err=%v", rendered, err)
	}
	if v, ok := countersHourlyBucket(t, pool, "rendered", "date_trunc('hour', now())"); !ok || v != 1 {
		t.Fatalf("после рендера rendered:<час> = (%d, %v), хочу (1, true)", v, ok)
	}

	// Устаревшие корзины (старше предыдущего часа): сид прямым INSERT —
	// проход обязан удалить (живое окно темпа, не история).
	for _, name := range []string{"results:2000-01-01T00", "rendered:2000-01-01T00"} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value, updated_at) VALUES ($1, 999, now())", name,
		); err != nil {
			t.Fatalf("устаревшая корзина %s: %v", name, err)
		}
	}

	// Реконсиляция — авторитет: prev-корзины создаются живым окном
	// (results 1 — сид-строка, rendered 0), cur — сходятся с
	// инкрементами (2/1, расхождений семантик нет — дрейф нулевой);
	// устаревшие удаляются.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if v, ok := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now() - interval '1 hour')"); !ok || v != 1 {
		t.Fatalf("после реконсиляции results:<prev час> = (%d, %v), хочу (1, true) — живое окно прошлого часа", v, ok)
	}
	if v, ok := countersHourlyBucket(t, pool, "rendered", "date_trunc('hour', now() - interval '1 hour')"); !ok || v != 0 {
		t.Fatalf("после реконсиляции rendered:<prev час> = (%d, %v), хочу (0, true) — пустой prev-час создан нулём", v, ok)
	}
	if v, _ := countersHourlyBucket(t, pool, "results", "date_trunc('hour', now())"); v != 2 {
		t.Fatalf("после реконсиляции results:<час> = %d, хочу 2 (корзина сходится с живым count)", v)
	}
	if v, _ := countersHourlyBucket(t, pool, "rendered", "date_trunc('hour', now())"); v != 1 {
		t.Fatalf("после реконсиляции rendered:<час> = %d, хочу 1", v)
	}
	for _, name := range []string{"results:2000-01-01T00", "rendered:2000-01-01T00"} {
		var n int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM crawler_counters WHERE name = $1", name,
		).Scan(&n); err != nil || n != 0 {
			t.Fatalf("устаревшая корзина %s пережила проход (count=%d err=%v)", name, n, err)
		}
	}

	// Читатель темпа: окно из двух живых корзин (prev 1 + cur 2), доля
	// rendered 1, окно покрытия 1–2 ч — делитель честной скорости.
	thr, err := status.FetchThroughput(ctx, pool)
	if err != nil {
		t.Fatalf("FetchThroughput: %v", err)
	}
	if thr.ResultsHour != 3 || thr.RenderedHour != 1 {
		t.Fatalf("throughput = %+v, хочу results_hour=3 rendered_hour=1 (prev+cur корзины)", thr)
	}
	if thr.HourWindowSec < 3600 || thr.HourWindowSec >= 7200 {
		t.Fatalf("hour_window_sec = %v, хочу [3600, 7200) — окно prev+cur корзин", thr.HourWindowSec)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции корзин темпа (корзины вне verify)")
}
