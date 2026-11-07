// Интеграционные тесты T-259 (ТЗ-fsm §3 ФТ-5): поздний переиздатель
// рендера (internal/httpworker/renderlate.go, ФТ-5.3) против живых
// PostgreSQL/Redis — предикат выборки, интерлок непотреблёнными
// сообщениями q:render/q:render-fallback, SPA-вердикт по сохранённому
// контенту и наследование алиаса, курсорное замыкание; и CHECK-инвариант
// рендер-состояния subdomain_results_rendered_check (миграция 000018,
// ФТ-5.2): нарушение отвергается, легальные комбинации и существующие
// данные не задеты (NOT VALID), convalidated = false до регламентного
// VALIDATE. Уровень вызовов — библиотечные, без запуска сервисов
// (стиль render_requeue_test.go); изоляция — withTestSchema + Redis БД 15.
package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/content"
	"crawler/internal/httpworker"
	"crawler/internal/metrics"
	"crawler/internal/migrate"
	"crawler/internal/queue"
)

// lateBody — SPA-оболочка (T-057, та же, что rqBody соседнего файла).
var lateBody = []byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`)

// latePendingRedis — адаптер источника непотреблённых ключей для
// интерлоков теста: семантика cmd/http-worker retentionPending
// (недоставленный группе хвост от last-delivered-id; групп нет — весь
// стрим), минимальная реализация без пагинации (хвосты теста короткие).
type latePendingRedis struct {
	rdb *redis.Client
}

func (p latePendingRedis) PendingKeys(ctx context.Context, stream string) (map[content.RetentionPendingKey]struct{}, error) {
	keys := make(map[content.RetentionPendingKey]struct{})
	msgs, err := p.rdb.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		if err == redis.Nil || strings.Contains(err.Error(), "no such key") {
			return keys, nil
		}
		return nil, err
	}
	for _, m := range msgs {
		raw, ok := m.Values["data"].(string)
		if !ok {
			continue
		}
		var r queue.Result
		if json.Unmarshal([]byte(raw), &r) != nil || r.ZoneID <= 0 || r.DomainID <= 0 {
			continue
		}
		keys[content.RetentionPendingKey{
			Zone: r.ZoneID, Domain: r.DomainID, Sub: r.SubdomainID, Variant: int16(r.Variant),
		}] = struct{}{}
	}
	return keys, nil
}

// TestRenderLateRepublisher — критерии приёмки ФТ-5.3: потерянная запись
// (старше 6 ч, без бита, без сообщений) переиздаётся в q:render; запись
// в стриме (q:render или q:render-fallback) или под битом render_pending
// НЕ трогается; свежая, отмаркированная, rendered и не-SPA — тоже нет.
func TestRenderLateRepublisher(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(pool.Close)

	rdb := resilienceRedisEnv(t)
	t.Cleanup(func() {
		if err := rdb.Del(ctx, queue.StreamRender, queue.StreamRenderFallback).Err(); err != nil {
			t.Fatalf("очистка стримов: %v", err)
		}
	})
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// watermark 0 — ёмкость всегда есть: проверяются предикаты и
	// интерлок, не ворота (стиль render_requeue_test.go).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg := metrics.New("http-worker-test")
	met := httpworker.NewMetrics(reg)

	// Живые сообщения для интерлока: публикуются ДО прохода, никто не
	// потребляет — недоставленный хвост (групп у стримов нет).
	liveMsg := func(stream string, res queue.Result) {
		t.Helper()
		if _, err := q.Publish(ctx, stream, res); err != nil {
			t.Fatalf("публикация живой задачи %s: %v", stream, err)
		}
	}

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('late.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}

	// seed — домен + записи; age управляет checked_at (окно ФТ-5.3 — 6 ч).
	spaHash := func() []byte {
		put, err := store.Put(ctx, lateBody)
		if err != nil {
			t.Fatalf("сохранение SPA-контента: %v", err)
		}
		return put.Hash[:]
	}()
	rawPut, err := store.Put(ctx, []byte(`<html><body>plain static page with plenty of text</body></html>`))
	if err != nil {
		t.Fatalf("сохранение статического контента: %v", err)
	}

	var domainID int64
	seedDomain := func(label string, flags int16) int64 {
		t.Helper()
		if err := pool.QueryRow(ctx,
			"INSERT INTO domains (zone_id, label, flags) VALUES ($1, $2, $3) RETURNING id",
			zone, label, flags,
		).Scan(&domainID); err != nil {
			t.Fatalf("insert domains %s: %v", label, err)
		}
		return domainID
	}
	seedRow := func(domain int64, sub int16, hash []byte, age time.Duration, alias *int16, marked, rendered bool) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, alias_subdomain_id,
			  render_failed_at, rendered, checked_at)
			 VALUES ($1, $2, $3, 2, 'https://late.test/', $4, $5, $6, $7, now() - $8::interval)`,
			zone, domain, sub, hash, alias,
			markerAge(marked), rendered, age.String()); err != nil {
			t.Fatalf("insert subdomain_results (домен %d): %v", domain, err)
		}
	}

	old, fresh := 7*time.Hour, time.Hour
	// Потерянная SPA-запись: старше окна, без бита, без сообщений.
	lost := seedDomain("lost", 0)
	// Оригинал + алиас: оба старше окна, алиас без контента — вердикт
	// наследуется от оригинала (T-055).
	aliasDom := seedDomain("aliasdom", 0)
	origSub := int16(3)
	// Свежая запись — моложе окна: издание плодило бы дубли живых задач.
	freshDom := seedDomain("fresh", 0)
	// Долг: бит render_pending — владение прохода доработки, не переиздателя.
	debtDom := seedDomain("debt", 2)
	// Живые сообщения стримов: интерлок обязан исключить.
	liveDom := seedDomain("live", 0)
	fbDom := seedDomain("fb", 0)
	// Отмаркированная (Э3), rendered (Э2) и не-SPA — вне переиздания.
	markedDom := seedDomain("marked", 0)
	renderedDom := seedDomain("rendered", 0)
	staticDom := seedDomain("static", 0)
	// Отрендеренный оригинал + потерянный алиас: факт рендера оригинала
	// доказывает SPA-вердикт класса — алиас переиздаётся (ревью P2-1).
	doneOrigDom := seedDomain("doneorig", 0)
	doneOrigSub := int16(4)

	seedRow(lost, 0, spaHash, old, nil, false, false)
	seedRow(aliasDom, origSub, spaHash, old, nil, false, false)
	seedRow(aliasDom, 7, nil, old, &origSub, false, false)
	seedRow(freshDom, 0, spaHash, fresh, nil, false, false)
	seedRow(debtDom, 0, spaHash, old, nil, false, false)
	seedRow(liveDom, 0, spaHash, old, nil, false, false)
	seedRow(fbDom, 0, spaHash, old, nil, false, false)
	seedRow(markedDom, 0, spaHash, old, nil, true, false)
	seedRow(renderedDom, 0, spaHash, old, nil, false, true)
	seedRow(staticDom, 0, rawPut.Hash[:], old, nil, false, false)
	seedRow(doneOrigDom, doneOrigSub, spaHash, old, nil, false, true)
	seedRow(doneOrigDom, 9, nil, old, &doneOrigSub, false, false)

	liveMsg(queue.StreamRender, queue.Result{ZoneID: zone, DomainID: liveDom, SubdomainID: 0})
	liveMsg(queue.StreamRenderFallback, queue.Result{ZoneID: zone, DomainID: fbDom, SubdomainID: 0})

	// Выравнивание счётчика долга за бит seed-домена (инвариант бэкфилла
	// 000015): проход переиздателя счётчик не трогает, но схема остаётся
	// консистентной для соседних проверок.
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("выравнивание счётчика долга: %v", err)
	}

	cur := &httpworker.RenderLateCursor{}
	res, err := httpworker.LateOnce(ctx, pool, store, renderQ, latePendingRedis{rdb: rdb}, met,
		httpworker.LateOptions{BatchSize: 100}, cur)
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	// Пачка: потерянная, оригинал+алиас, алиас отрендеренного оригинала,
	// две живые (интерлок), не-SPA — итого 7 выбрано, 4 издано, 3
	// пропущено; свежая/долг/отмаркированная/rendered отсечены SQL.
	if res.Selected != 7 || res.Published != 4 || res.Skipped != 3 || res.Full {
		t.Fatalf("итог %+v, хочу selected=7 published=4 skipped=3", res)
	}
	if !res.Wrapped {
		t.Fatalf("проход не замкнул курсор (пачка короче лимита): %+v", res)
	}

	// Состав публикаций: потерянная запись, пара оригинал+алиас и алиас
	// отрендеренного оригинала.
	msgs, err := rdb.XRange(ctx, queue.StreamRender, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", queue.StreamRender, err)
	}
	got := map[int64]int{}
	for _, m := range msgs {
		var r queue.Result
		if err := json.Unmarshal([]byte(m.Values["data"].(string)), &r); err != nil {
			t.Fatalf("разбор сообщения %s: %v", m.ID, err)
		}
		got[r.DomainID]++
	}
	if len(got) != 4 || got[lost] != 1 || got[aliasDom] != 2 || got[doneOrigDom] != 1 {
		t.Fatalf("публикации по доменам %v, хочу {потерянная %d:1, оригинал+алиас %d:2, алиас rendered-оригинала %d:1}",
			got, lost, aliasDom, doneOrigDom)
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != 5 {
		t.Fatalf("сообщений q:render = %d, хочу 5 (4 переиздания + 1 живая)", n)
	}
	if v := lateMetricValue(t, reg, "crawler_render_late_republish_total"); v != 4 {
		t.Fatalf("crawler_render_late_republish_total = %v, хочу 4", v)
	}

	// Повторный проход: собственные публикации теперь — непотреблённый
	// хвост q:render, интерлок исключает их же записи; дублей нет.
	res, err = httpworker.LateOnce(ctx, pool, store, renderQ, latePendingRedis{rdb: rdb}, met,
		httpworker.LateOptions{BatchSize: 100}, cur)
	if err != nil {
		t.Fatalf("повторный LateOnce: %v", err)
	}
	if res.Published != 0 {
		t.Fatalf("повторный проход: published=%d, хочу 0 (интерлок собственных сообщений)", res.Published)
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != 5 {
		t.Fatalf("сообщений q:render после повтора = %d, хочу 5 (без дублей)", n)
	}
}

// label — имя домена для final_url сида (не используется сообщениями).

// markerAge — штамп маркера для сида: NULL либо час назад.
func markerAge(marked bool) any {
	if !marked {
		return nil
	}
	return time.Now().Add(-time.Hour)
}

// lateMetricValue — значение счётчика реестра по имени (без меток);
// серия не найдена — −1 (различимо от честного нуля). Типы семейств
// выводятся (стиль httpMetricValue, internal/httpworker/metrics_test.go).
func lateMetricValue(t *testing.T, g *metrics.Registry, name string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestSchema_ResultsRenderedCheck — парный тест-инвариант CHECK
// subdomain_results_rendered_check (T-259, ФТ-5.2, миграция 000018):
// «rendered = true ⇒ render_failed_at IS NULL» — нарушение отвергается
// схемой (23514), легальные комбинации проходят, ограничение добавлено
// NOT VALID (convalidated = false у родителя и секций — VALIDATE по
// регламенту миграций отдельным шагом, docs/schema.md) и не задевает
// существующие данные.
func TestSchema_ResultsRenderedCheck(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('chk.test') RETURNING id").Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'chk') RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	insertRow := func(sub int16, rendered bool, marker any) error {
		_, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, rendered, render_failed_at, checked_at)
			 VALUES ($1, $2, $3, 2, 'https://chk.test/', $4, $5, now())`,
			zone, id, sub, rendered, marker)
		return err
	}

	// Легальные комбинации: rendered без маркера; не-рендер с маркером
	// (Э3); не-рендер без маркера (Э0/Э1) — уникальные PK (ревью P1-1).
	if err := insertRow(0, true, nil); err != nil {
		t.Fatalf("rendered без маркера: %v", err)
	}
	if err := insertRow(1, false, time.Now()); err != nil {
		t.Fatalf("не-рендер с маркером: %v", err)
	}
	if err := insertRow(2, false, nil); err != nil {
		t.Fatalf("не-рендер без маркера: %v", err)
	}

	// Нарушение инварианта: rendered = true + маркер — 23514 схемой,
	// коду не приходится держать пару колонок вручную (ФТ-5.2).
	if err := insertRow(3, true, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "subdomain_results_rendered_check") {
		t.Errorf("rendered с маркером: err = %v, want violation subdomain_results_rendered_check", err)
	}
	// UPDATE существующей строки в нарушение — тоже отвергается (NOT
	// VALID проверяет новые записи, включая изменения); цель — конкретная
	// rendered-строка (sub=0).
	if _, err := pool.Exec(ctx,
		"UPDATE subdomain_results SET render_failed_at = now() "+
			"WHERE zone_id = $1 AND domain_id = $2 AND subdomain_id = 0",
		zone, id); err == nil ||
		!strings.Contains(err.Error(), "subdomain_results_rendered_check") {
		t.Errorf("UPDATE маркера на rendered-строке: err = %v, want violation", err)
	}

	// NOT VALID: ограничение в СХЕМЕ ТЕСТА (фильтр по current_schema —
	// ревью P3-1: в БД живут схемы соседних прогонов) на родителе и
	// секциях ещё не валидировано — VALIDATE отдельным шагом по
	// регламенту (docs/schema.md); новые секции (create_zone_partitions,
	// T-005) наследуют его валидированным. total ≥ 2 — рекурсия на
	// секции состоялась (родитель + DEFAULT-партиция).
	rows, err := pool.Query(ctx,
		`SELECT count(*) FILTER (WHERE convalidated) AS validated, count(*) AS total
		 FROM pg_constraint
		 WHERE conname = 'subdomain_results_rendered_check'
		   AND connamespace = current_schema()::regnamespace`)
	if err != nil {
		t.Fatalf("pg_constraint: %v", err)
	}
	defer rows.Close()
	var validated, total int
	if !rows.Next() || rows.Scan(&validated, &total) != nil {
		t.Fatalf("чтение pg_constraint: %v", rows.Err())
	}
	if total < 2 {
		t.Fatalf("ограничение в схеме теста на %d объектах, хочу ≥2 (родитель + секция; миграция 000018)", total)
	}
	if validated != 0 {
		t.Errorf("convalidated = true у %d из %d строк ограничения, хочу 0 (NOT VALID, VALIDATE — по регламенту)",
			validated, total)
	}
}
