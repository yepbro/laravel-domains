// Интеграционные тесты прохода доработки рендера RequeueOnce (T-066;
// выборка пачки без PK-порядка, закрепление плана SET LOCAL и guard
// счётчиком долга — T-181; приоритет зон двухступенчатой выборкой — T-184)
// против живых PostgreSQL/Redis: юнит-тесты renderq_test.go проверяют
// SQL-контракты и последовательность на фейках, здесь — реальное
// поведение на мигрированной схеме: проход публикует q:render и снимает
// флаг с декрементом счётчика; стационар без долга (счётчик 0) не
// выполняет тяжёлую выборку вовсе (обёртка пула со счётчиком вызовов —
// предписание ТЗ T-181 §7.2); повторный проход не выбирает снятое;
// отсутствие строки счётчика — ошибка прохода. Форма закреплённого плана
// проверяется EXPLAIN в той же транзакции с SET LOCAL (seq scan по
// domains, индексные формы отключены). Уровень вызовов — как у соседей:
// библиотечные вызовы без запуска сервисов; изоляция — withTestSchema +
// Redis БД 15.
package integration

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
	"crawler/internal/httpworker"
	"crawler/internal/migrate"
	"crawler/internal/queue"
)

// rqSeqScanRe — seq scan по domains (алиас d) в EXPLAIN: PG16 печатает
// листовую партицию («Seq Scan on domains_default d»), PG17+ — родителя
// («Seq Scan on d»).
var rqSeqScanRe = regexp.MustCompile(`Seq Scan on (\w+ )?d\b`)

// rqBody — SPA-оболочка (T-057: мало видимого текста, корневой контейнер,
// noscript с требованием JS; то же тело, что в юнит-фикстурах
// renderq_test.go): повторный вердикт прохода обязан увидеть SPA и
// опубликовать q:render.
var rqBody = []byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`)

// rqBatchPrefix — начало renderPendingDomainsSQL (литерал
// internal/httpworker/renderq.go, дублирование — стиль schema_test.go):
// обёртка пула считает вызовы тяжёлой выборки пачки — guard T-181 обязан
// держать её за нулём счётчика долга. Выборка идёт ВНУТРИ транзакции
// закрепления плана, потому счётчик живёт в обёртке транзакции.
const rqBatchPrefix = "SELECT d.zone_id, d.id, d.label || '.' || z.name"

// rqBatchSQL — полный текст выборки для EXPLAIN (LIMIT — литералом:
// параметр в EXPLAIN требует подготовленного оператора, проверке формы
// плана литерал безразличен).
const rqBatchSQL = rqBatchPrefix + " FROM domains AS d JOIN zones AS z ON z.id = d.zone_id " +
	"WHERE (d.flags & 2) <> 0 AND d.status <> 6 LIMIT 100"

// rqTracePool — RecordDB-обёртка пула: считает вызовы тяжёлой выборки
// пачки (обёртка пула со счётчиком вызовов — предписание ТЗ T-181 §7.2).
type rqTracePool struct {
	*pgxpool.Pool

	mu    sync.Mutex
	batch int
}

func (p *rqTracePool) countBatch() {
	p.mu.Lock()
	p.batch++
	p.mu.Unlock()
}

func (p *rqTracePool) batchCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batch
}

// Begin оборачивает транзакцию счётчиком выборки.
func (p *rqTracePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &rqTraceTx{Tx: tx, pool: p}, nil
}

// rqTraceTx — транзакция-обёртка: тяжёлая выборка выполняется внутри
// транзакции закрепления плана (renderSelectPending, T-181).
type rqTraceTx struct {
	pgx.Tx
	pool *rqTracePool
}

func (t *rqTraceTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.HasPrefix(sql, rqBatchPrefix) {
		t.pool.countBatch()
	}
	return t.Tx.Query(ctx, sql, args...)
}

// TestRenderRequeuePass — проход доработки на живых PostgreSQL/Redis:
// долг есть — публикует и снимает; счётчик 0 — тяжёлая выборка не
// выполняется; снятое не выбирается повторно; отсутствие строки
// счётчика — ошибка; форма плана закреплена (seq scan).
func TestRenderRequeuePass(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(raw.Close)
	pool := &rqTracePool{Pool: raw}

	rdb := resilienceRedisEnv(t)
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
	// watermark 0 — ёмкость всегда есть (HasCapacity без Redis): проход
	// не упирается в ворота, проверяется выборка и guard (T-181).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Форма плана закреплена (§4.2 ТЗ T-181): внутри транзакции SET LOCAL
	// (все индексные формы отключены) EXPLAIN показывает seq scan по
	// domains, индексных узлов нет. PG16 печатает листовую партицию
	// («Seq Scan on domains_default d»), PG17+ — родителя («Seq Scan on
	// d»): регулярка покрывает обе формы записи.
	t.Run("план выборки закреплён SET LOCAL", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, pin := range []string{
			"SET LOCAL enable_indexscan = off",
			"SET LOCAL enable_bitmapscan = off",
			"SET LOCAL enable_indexonlyscan = off",
		} {
			if _, err := tx.Exec(ctx, pin); err != nil {
				t.Fatalf("%s: %v", pin, err)
			}
		}
		rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+rqBatchSQL)
		if err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("EXPLAIN: чтение: %v", err)
			}
			plan.WriteString(line)
			plan.WriteByte('\n')
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		if !rqSeqScanRe.MatchString(plan.String()) {
			t.Errorf("закреплённый план без seq scan по domains (алиас d):\n%s", plan.String())
		}
		if strings.Contains(plan.String(), "Index") || strings.Contains(plan.String(), "Bitmap") {
			t.Errorf("закреплённый план содержит индексную форму (SET LOCAL не подействовал):\n%s", plan.String())
		}
	})

	// Долг: домен с флагом и SPA-записью; счётчик выравнивается вручную
	// (сид — прямой INSERT, пара бит+счётчик ведёт код; инвариант
	// бэкфилла миграции 000015: счётчик = число строк с битом).
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('requeue.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'spa', 2) RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	put, err := store.Put(ctx, rqBody)
	if err != nil {
		t.Fatalf("сохранение контента: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO subdomain_results
		 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, checked_at)
		 VALUES ($1, $2, 0, 2, 'https://spa.requeue.test/', $3, now())`,
		zone, id, put.Hash[:]); err != nil {
		t.Fatalf("insert subdomain_results: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("выравнивание счётчика долга: %v", err)
	}

	t.Run("долг есть — проход публикует и снимает", func(t *testing.T) {
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
			t.Fatalf("итог %+v, ожидался selected=1 cleared=1 published=1", res)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("сообщений q:render = %d, хочу 1", n)
		}
		var flags int16
		if err := pool.QueryRow(ctx,
			"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
		).Scan(&flags); err != nil {
			t.Fatal(err)
		}
		if flags&2 != 0 {
			t.Errorf("бит render_pending не снят (flags=%d)", flags)
		}
		var counter int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'render_pending'",
		).Scan(&counter); err != nil || counter != 0 {
			t.Errorf("счётчик долга после снятия = %d (err=%v), хочу 0", counter, err)
		}
	})

	t.Run("стационар: счётчик 0 — тяжёлая выборка не выполняется", func(t *testing.T) {
		before := pool.batchCalls()
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 0 || res.Published != 0 || res.Full {
			t.Fatalf("итог %+v, ожидался пустой (guard: долга нет)", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("тяжёлая выборка выполнена %d раз при нулевом счётчике (было %d)", n-before, before)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("публикаций q:render стало %d, хочу 1 (новых нет)", n)
		}
	})

	t.Run("повторный проход не выбирает снятое", func(t *testing.T) {
		// Рассинхрон counter > 0 при снятых флагах (граница доверия
		// guard, ТЗ T-181 §4.3): один холостой seq scan за проход —
		// выборка пуста, снятое не переиздаётся.
		if _, err := pool.Exec(ctx,
			"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
			t.Fatal(err)
		}
		before := pool.batchCalls()
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 0 || res.Published != 0 {
			t.Fatalf("итог %+v, ожидался selected=0 published=0 (снятое не выбирается)", res)
		}
		if n := pool.batchCalls(); n != before+1 {
			t.Fatalf("тяжёлых выборок %d (было %d), хочу +1 — холостой проход по счётчику", n, before)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("публикаций q:render стало %d, хочу 1 (дубликатов нет)", n)
		}
	})

	t.Run("строки счётчика нет — ошибка прохода", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "DELETE FROM crawler_counters WHERE name = 'render_pending'"); err != nil {
			t.Fatal(err)
		}
		if _, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{}); err == nil {
			t.Fatal("отсутствие строки счётчика не возвращено ошибкой (guard обязан лгать громче)")
		}
	})
}

// TestRenderRequeueFailedMarker — маркер «рендер невозможен»
// subdomain_results.render_failed_at (T-210, миграция 000017) на живых
// PostgreSQL/Redis: штамп финализации (SQL — литерал
// internal/renderworker/worker.go, дублирование — стиль schema_test.go;
// охраны content_hash/rendered, идемпотентность) исключает запись из
// выборки прохода (публикаций нет, флаг снимается без публикации —
// «черновой»), перезапись эпизода (DELETE + INSERT записи результата,
// T-058) сбрасывает маркер — следующий проход переиздаёт запись.
func TestRenderRequeueFailedMarker(t *testing.T) {
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
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// renderFailMarkerSQL — литерал internal/renderworker/worker.go
	// (renderFailMarkerSQL, T-210): расхождение кода и теста видно по
	// поведению охран ниже.
	const markerSQL = "UPDATE subdomain_results AS r " +
		"SET render_failed_at = coalesce(r.render_failed_at, now()) " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.rendered = false " +
		"AND r.content_hash IS NOT DISTINCT FROM $5::bytea"

	// Сид: домен с флагом долга и SPA-записью.
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('failed.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'spa', 2) RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	put, err := store.Put(ctx, rqBody)
	if err != nil {
		t.Fatalf("сохранение контента: %v", err)
	}
	insertRow := func(url string, hash []byte) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, checked_at)
			 VALUES ($1, $2, 0, 2, $3, $4, now())`,
			zone, id, url, hash); err != nil {
			t.Fatalf("insert subdomain_results: %v", err)
		}
	}
	insertRow("https://spa.failed.test/", put.Hash[:])
	alignDebt := func() {
		t.Helper()
		if _, err := pool.Exec(ctx,
			"UPDATE domains SET flags = flags | 2 WHERE zone_id = $1 AND id = $2", zone, id); err != nil {
			t.Fatalf("постановка флага: %v", err)
		}
		if _, err := pool.Exec(ctx,
			"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
			t.Fatalf("выравнивание счётчика долга: %v", err)
		}
	}
	alignDebt()

	marker := func() (*time.Time, error) {
		var m *time.Time
		err := pool.QueryRow(ctx,
			"SELECT render_failed_at FROM subdomain_results WHERE zone_id = $1 AND domain_id = $2", zone, id,
		).Scan(&m)
		return m, err
	}

	t.Run("штамп финализации идемпотентен, охраны работают", func(t *testing.T) {
		tag, err := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:])
		if err != nil {
			t.Fatalf("маркер: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Fatalf("RowsAffected = %d, хочу 1 (строка того контента, rendered = false)", tag.RowsAffected())
		}
		m, err := marker()
		if err != nil || m == nil {
			t.Fatalf("render_failed_at после штампа: %v err=%v", m, err)
		}

		// Повторная финализация (сбой публикации вернул доставку) —
		// RowsAffected 1, штамп ПЕРВОЙ финализации сохраняется (coalesce).
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:]); tag.RowsAffected() != 1 {
			t.Fatalf("повторный штамп: RowsAffected = %d, хочу 1 (идемпотентность)", tag.RowsAffected())
		}
		if m2, err := marker(); err != nil || m2 == nil || !m2.Equal(*m) {
			t.Fatalf("штамп переставлен повтором: %v (err=%v), хочу первую финализацию %v", m2, err, m)
		}

		// Охрана контента: новый эпизод переписал content_hash — штамп
		// старой доставкой не ставится и не трогается.
		fresh := put.Hash
		fresh[0] ^= 0xff
		if _, err := pool.Exec(ctx,
			"UPDATE subdomain_results SET content_hash = $3 WHERE zone_id = $1 AND domain_id = $2",
			zone, id, fresh[:]); err != nil {
			t.Fatalf("смена контента: %v", err)
		}
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:]); tag.RowsAffected() != 0 {
			t.Fatalf("штамп чужому контенту: RowsAffected = %d, хочу 0 (охрана content_hash)", tag.RowsAffected())
		}
		if m, err := marker(); err != nil || m == nil {
			t.Fatalf("штамп сбит охраной: %v err=%v", m, err)
		}

		// Охрана rendered: успешный конкурентный рендер закрыл вопрос —
		// штамп не ставится и снимается переводом в rendered.
		if _, err := pool.Exec(ctx,
			"UPDATE subdomain_results SET rendered = true, render_failed_at = NULL "+
				"WHERE zone_id = $1 AND domain_id = $2", zone, id); err != nil {
			t.Fatalf("перевод в rendered: %v", err)
		}
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), fresh[:]); tag.RowsAffected() != 0 {
			t.Fatalf("штамп rendered-строке: RowsAffected = %d, хочу 0 (охрана rendered)", tag.RowsAffected())
		}
		if m, err := marker(); err != nil || m != nil {
			t.Fatalf("render_failed_at после успешного рендера: %v err=%v (снимается)", m, err)
		}
	})

	t.Run("отмаркированная запись проходом не переиздаётся", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "DELETE FROM subdomain_results WHERE zone_id = $1 AND domain_id = $2", zone, id); err != nil {
			t.Fatalf("очистка строк: %v", err)
		}
		insertRow("https://spa.failed.test/", put.Hash[:])
		alignDebt()
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:]); tag.RowsAffected() != 1 {
			t.Fatalf("штамп: RowsAffected = %d, хочу 1", tag.RowsAffected())
		}
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 1 || res.Cleared != 1 || res.Published != 0 {
			t.Fatalf("итог %+v, ожидался selected=1 cleared=1 published=0 (отмаркированная запись не переиздаётся)", res)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 0 {
			t.Fatalf("сообщений q:render = %d, хочу 0 (T-210)", n)
		}
	})

	t.Run("новый эпизод сбрасывает маркер — проход переиздаёт", func(t *testing.T) {
		// Перезапись эпизода (T-058): DELETE старых строк + INSERT новых;
		// render_failed_at в INSERT не входит — DEFAULT NULL.
		if _, err := pool.Exec(ctx, "DELETE FROM subdomain_results WHERE zone_id = $1 AND domain_id = $2", zone, id); err != nil {
			t.Fatalf("DELETE эпизода: %v", err)
		}
		insertRow("https://spa.failed.test/", put.Hash[:])
		if m, err := marker(); err != nil || m != nil {
			t.Fatalf("render_failed_at после перезаписи эпизода: %v err=%v (сброс в NULL)", m, err)
		}
		alignDebt()

		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
			t.Fatalf("итог %+v, ожидался selected=1 cleared=1 published=1 (сброшенный маркер вернул запись в выборку)", res)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("сообщений q:render = %d, хочу 1", n)
		}
	})
}

// TestRenderRequeueZonePriority — приоритет зон прохода доработки (T-184,
// решение (а) T-176): пачка первой ступени берёт домены зон НЕ из списка
// render.requeue_deprioritized_zones (даже лежащие дальше по «куче» —
// вставка .рф первой имитирует прод-раскладку «флаговый долг .рф в начале
// кучи», T-181), долг зон списка дорабатывается по остаточному принципу;
// форма плана ступени с фильтром зон — тот же закреплённый seq scan.
func TestRenderRequeueZonePriority(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(raw.Close)

	// Форма плана первой ступени (фильтр зоны в JOIN): seq scan по
	// domains, индексных форм нет — приоритет не тянет патологию T-181.
	t.Run("план ступени закреплён SET LOCAL", func(t *testing.T) {
		tx, err := raw.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, pin := range []string{
			"SET LOCAL enable_indexscan = off",
			"SET LOCAL enable_bitmapscan = off",
			"SET LOCAL enable_indexonlyscan = off",
		} {
			if _, err := tx.Exec(ctx, pin); err != nil {
				t.Fatalf("%s: %v", pin, err)
			}
		}
		rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+rqBatchPrefix+
			" FROM domains AS d JOIN zones AS z ON z.id = d.zone_id "+
			"WHERE (d.flags & 2) <> 0 AND d.status <> 6 "+
			"AND z.name <> ALL('{xn--p1ai}'::text[]) LIMIT 1")
		if err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("EXPLAIN: чтение: %v", err)
			}
			plan.WriteString(line)
			plan.WriteByte('\n')
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		if !rqSeqScanRe.MatchString(plan.String()) {
			t.Errorf("закреплённый план ступени без seq scan по domains (алиас d):\n%s", plan.String())
		}
		if strings.Contains(plan.String(), "Index") || strings.Contains(plan.String(), "Bitmap") {
			t.Errorf("закреплённый план ступени содержит индексную форму:\n%s", plan.String())
		}
	})

	pool := &rqTracePool{Pool: raw}
	rdb := resilienceRedisEnv(t)
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
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Долг: домен .рф вставлен ПЕРВЫМ (прод-раскладка кучи), приоритетная
	// зона — за ним; обе записи SPA (проход обязан опубликовать обе).
	var zoneRF, zonePrio int16
	if err := pool.QueryRow(ctx, "INSERT INTO zones (name) VALUES ('xn--p1ai') RETURNING id").Scan(&zoneRF); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO zones (name) VALUES ('prio.test') RETURNING id").Scan(&zonePrio); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var rfID, prioID int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'rf', 2) RETURNING id", zoneRF,
	).Scan(&rfID); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'prio', 2) RETURNING id", zonePrio,
	).Scan(&prioID); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	put, err := store.Put(ctx, rqBody)
	if err != nil {
		t.Fatalf("сохранение контента: %v", err)
	}
	for _, d := range []struct {
		zone int16
		id   int64
		url  string
	}{
		{zoneRF, rfID, "https://rf.xn--p1ai/"},
		{zonePrio, prioID, "https://prio.prio.test/"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, checked_at)
			 VALUES ($1, $2, 0, 2, $3, $4, now())`,
			d.zone, d.id, d.url, put.Hash[:]); err != nil {
			t.Fatalf("insert subdomain_results: %v", err)
		}
	}
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = 2 WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("выравнивание счётчика долга: %v", err)
	}

	opts := httpworker.RequeueOptions{BatchSize: 1, DeprioritizedZones: []string{"xn--p1ai"}}

	// Проход 1 (пачка 1): первая ступень берёт приоритетную зону, минуя
	// .рф в начале кучи.
	res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, opts)
	if err != nil {
		t.Fatalf("проход 1: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 0 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("проход 1: итог %+v, ожидался selected=1 deprioritized=0 cleared=1 published=1", res)
	}
	var rfFlags int16
	if err := pool.QueryRow(ctx,
		"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zoneRF, rfID,
	).Scan(&rfFlags); err != nil {
		t.Fatal(err)
	}
	if rfFlags&2 == 0 {
		t.Fatal("домен .рф доработан раньше приоритетного — приоритет зон не работает")
	}

	// Проход 2: первая ступень исчерпана — вторая дорабатывает .рф.
	res, err = httpworker.RequeueOnce(ctx, pool, store, renderQ, opts)
	if err != nil {
		t.Fatalf("проход 2: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 1 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("проход 2: итог %+v, ожидался selected=1 deprioritized=1 (долг зон списка)", res)
	}
	var counter int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'",
	).Scan(&counter); err != nil || counter != 0 {
		t.Fatalf("счётчик долга после дренажа = %d (err=%v), хочу 0", counter, err)
	}

	// Порядок публикаций q:render: приоритетная зона — первой, .рф — за
	// ней (даже наоборот порядку вставки в кучу).
	msgs, err := rdb.XRange(ctx, queue.StreamRender, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", queue.StreamRender, err)
	}
	if len(msgs) != 2 {
		t.Fatalf("сообщений q:render = %d, хочу 2", len(msgs))
	}
	got := make([]int64, 0, 2)
	for _, m := range msgs {
		var r queue.Result
		if err := json.Unmarshal([]byte(m.Values["data"].(string)), &r); err != nil {
			t.Fatalf("разбор сообщения %s: %v", m.ID, err)
		}
		got = append(got, r.DomainID)
	}
	if got[0] != prioID || got[1] != rfID {
		t.Fatalf("порядок публикаций %v, ожидался [приоритетная %d, .рф %d]", got, prioID, rfID)
	}
}
