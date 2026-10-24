// Интеграционные тесты прохода доработки рендера RequeueOnce (T-066;
// выборка пачки без PK-порядка, закрепление плана SET LOCAL и guard
// счётчиком долга — T-181) против живых PostgreSQL/Redis: юнит-тесты
// renderq_test.go проверяют SQL-контракты и последовательность на
// фейках, здесь — реальное поведение на мигрированной схеме: проход
// публикует q:render и снимает флаг с декрементом счётчика; стационар
// без долга (счётчик 0) не выполняет тяжёлую выборку вовсе (обёртка
// пула со счётчиком вызовов — предписание ТЗ T-181 §7.2); повторный
// проход не выбирает снятое; отсутствие строки счётчика — ошибка
// прохода. Форма закреплённого плана проверяется EXPLAIN в той же
// транзакции с SET LOCAL (seq scan по domains, индексные формы
// отключены). Уровень вызовов — как у соседей: библиотечные вызовы
// без запуска сервисов; изоляция — withTestSchema + Redis БД 15.
package integration

import (
	"context"
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
