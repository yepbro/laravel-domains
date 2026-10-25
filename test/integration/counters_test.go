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

	"crawler/internal/dnsworker"
	"crawler/internal/httpworker"
	"crawler/internal/ingest"
	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
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
	leaseRes, err := scheduler.LeaseOnce(ctx, pool, scheduler.LeaseOptions{})
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
	var v int64
	err := pool.QueryRow(context.Background(),
		"SELECT value FROM crawler_counters WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')",
	).Scan(&v)
	if err != nil {
		t.Fatalf("чтение checks:<день>: %v", err)
	}
	return v
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
