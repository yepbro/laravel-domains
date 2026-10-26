// Интеграционные тесты тикового прохода переанализа на время-курсоре
// (T-193; ТЗ TZ_slow_reanalyze_tick.md §7.2) против живого PostgreSQL:
// часовой тик обязан читать свежий срез композитным курсором
// (checked_at, PK) с опорой на subdomain_results_checked_idx (T-171),
// якорь старта — now() − 24 ч (остаточный класс старше — территория
// полного дренажа), курсор переживает проходы; полный дренаж при смене
// правил остаётся PK-формой с нуля и находит устаревшее вне окна.
// Guard корзин хвостов (T-204): тик без зрелых хвостов не вычитывает
// окно вовсе — корзины reanalyze: ведут инкременты записи/рендера,
// декремент штампа и реконсиляция monitor (FILTER-позиции живого
// окна), сценарий корзин — test/integration/counters_test.go.
// Юнит-уровень (SQL-контракты, якорь, продвижение курсора, цикл на
// фейках) — internal/analyzer/reanalyze_test.go. Уровень вызовов —
// библиотечные, без запуска сервисов (стиль render_requeue_test.go);
// изоляция — withTestSchema (search_path в DSN; ВСЕ подключения теста —
// один пул этой схемы), публикация — захватывающий стаб очереди (стиль
// counters_test.go): проверяется отбор и публикация, не Redis.
// Прогон — make test-integration (после вливания, из основного дерева).
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
)

// reanQ — queue.Queue: фиксирует публикации Result в q:analyze (только
// счёт и содержимое; Redis не нужен — стрим проверяют e2e/acceptance).
type reanQ struct {
	publishes []queue.Result
}

var _ queue.Queue = (*reanQ)(nil)

func (q *reanQ) Publish(_ context.Context, stream string, msg queue.Message) (string, error) {
	if stream != queue.StreamAnalyze {
		return "", fmt.Errorf("reanQ: публикация в %s, нужен %s", stream, queue.StreamAnalyze)
	}
	res, ok := msg.(queue.Result)
	if !ok {
		return "", fmt.Errorf("reanQ: публикация %T, нужен queue.Result", msg)
	}
	q.publishes = append(q.publishes, res)
	return fmt.Sprintf("entry-%d", len(q.publishes)), nil
}

func (q *reanQ) Consume(context.Context, string, string, string, int) ([]queue.Delivery, error) {
	panic("reanQ: Consume не используется")
}

func (q *reanQ) Ack(context.Context, string, string, ...string) error {
	panic("reanQ: Ack не используется")
}

// reanSeedRow — строка subdomain_results одного домена: PK различается
// субдоменом; version nil — NULL (никогда не анализировалась, T-058),
// иначе штамп набора; age — возраст отметки checked_at (целые часы,
// make_interval — детерминированный ввод без парсинга строк интервалов).
func reanSeedRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool,
	zone int16, domain int64, sub int16, version *int32, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO subdomain_results
		 (zone_id, domain_id, subdomain_id, scheme, checked_at, rules_version)
		 VALUES ($1, $2, $3, 2, now() - make_interval(hours => $4), $5)`,
		zone, domain, sub, int(age/time.Hour), version); err != nil {
		t.Fatalf("insert subdomain_results (sub %d): %v", sub, err)
	}
}

// reanBasketValue — значение корзины хвостов переанализа заданного
// возраста-часа (T-204; имя часа вычисляет БД тем же выражением, что
// операторы прохода).
func reanBasketValue(ctx context.Context, t *testing.T, pool *pgxpool.Pool, age time.Duration) int64 {
	t.Helper()
	var v int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'reanalyze:' || to_char(date_trunc('hour', now() - make_interval(hours => $1)), 'YYYY-MM-DD\"T\"HH24')",
		int(age/time.Hour)).Scan(&v); err != nil {
		t.Fatalf("корзина хвостов (%v назад): %v", age, err)
	}
	return v
}

// reanSkipHourBoundary — сценарии корзин хвостов привязаны к
// «предыдущему часу» от now() (сид и окно реконсиляции): смена
// календарного часа посреди теста сдвигает окно — сид становится
// двухчасовым, реконсиляция его не видит, ассерты падают. У самой
// границы часа тест пропускается: ~1 % прогонов дешевле редкого
// флака (ревью T-204, P3-4).
func reanSkipHourBoundary(t *testing.T) {
	t.Helper()
	if left := time.Until(time.Now().Truncate(time.Hour).Add(time.Hour)); left < 45*time.Second {
		t.Skipf("граница календарного часа через %v — окно «предыдущего часа» нестабильно", left)
	}
}

// TestReanalyze_TickCursor — сценарий §7.2 с guard'ом корзин хвостов
// (T-204): проход monitor материализует корзины живым окном (свежая
// строка без анализа — в корзине предыдущего часа), guard открывается —
// тик ходит время-формой и публикует хвост; проанализированная не
// отбирается; повторный тик не переизбирает (курсор за строкой, а не
// версия); смена правил — полный дренаж с нуля находит и старое вне
// lookback. Плюс EXPLAIN-контракт: предикат композитного курсора
// обслуживается индексом subdomain_results_checked_idx (на игрушечном
// объёме seq scan дешевле — поэтому enable_seqscan = off: проверяется
// индексируемость предиката, а не выбор по стоимости; стоимость —
// журнал T-177 на бою).
func TestReanalyze_TickCursor(t *testing.T) {
	skipUnlessIntegration(t)
	reanSkipHourBoundary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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

	// Справочник: зона + домен; три строки одного домена (PK — субдомен):
	// fresh — новый эпизод без анализа (NULL, 1 ч назад — час корзин
	//   живого окна прохода monitor: guard обязан его увидеть);
	// ancient — устаревшая версия 7 трое суток назад: вне lookback-якоря,
	//   территория только полного дренажа (§4.2);
	// done — проанализирована текущей версией (1 ч назад): не отбирается.
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('reanalyze.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var domain int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'example') RETURNING id", zone,
	).Scan(&domain); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	const (
		subFresh   int16 = 1
		subAncient int16 = 2
		subDone    int16 = 3
		vCurrent         = int32(42)
		vOld             = int32(7)
	)
	reanSeedRow(ctx, t, pool, zone, domain, subFresh, nil, time.Hour)
	old := vOld
	reanSeedRow(ctx, t, pool, zone, domain, subAncient, &old, 72*time.Hour)
	current := vCurrent
	reanSeedRow(ctx, t, pool, zone, domain, subDone, &current, time.Hour)

	// Проход реконсиляции (T-204): корзины хвостов материализуются
	// агрегатом живого окна — fresh (NULL, час назад) попадает в корзину
	// предыдущего часа, guard тика открывается; done (текущая версия) и
	// ancient (вне окна) корзин не числят.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if got := reanBasketValue(ctx, t, pool, time.Hour); got != 1 {
		t.Fatalf("корзина хвостов предыдущего часа = %d, want 1 (fresh NULL строка)", got)
	}

	q := &reanQ{}
	var cursor analyzer.ReanalyzeCursor

	t.Run("тик с якоря подбирает свежую без анализа", func(t *testing.T) {
		res, err := analyzer.ReanalyzeTick(ctx, pool, q, vCurrent, &cursor, analyzer.ReanalyzeOptions{})
		if err != nil {
			t.Fatalf("ReanalyzeTick: %v", err)
		}
		// fresh — да; done (текущая версия) и ancient (вне окна) — нет.
		if res.Selected != 1 || res.Published != 1 {
			t.Fatalf("итог = %+v, want 1/1 (только свежая NULL)", res)
		}
		got := q.publishes[0]
		if got.ZoneID != zone || got.DomainID != domain || got.SubdomainID != subFresh ||
			got.Variant != model.VariantMain || got.Name != "example.reanalyze.test" {
			t.Errorf("публикация = %+v, want ключ строки fresh", got)
		}
	})

	t.Run("повторный тик не переизбирает", func(t *testing.T) {
		// Строка всё ещё не проштампована (rules_version NULL): её
		// исключает курсор, а не версия — окно исчерпано (guard по-прежнему
		// открыт корзиной: выборка идёт, публикаций нет).
		res, err := analyzer.ReanalyzeTick(ctx, pool, q, vCurrent, &cursor, analyzer.ReanalyzeOptions{})
		if err != nil {
			t.Fatalf("повторный ReanalyzeTick: %v", err)
		}
		if res.Selected != 0 || res.Published != 0 || len(q.publishes) != 1 {
			t.Fatalf("повторный итог = %+v (публикаций %d), want 0/0/1 — без переизбрания",
				res, len(q.publishes))
		}
	})

	t.Run("смена правил — дренаж с нуля находит старое вне lookback", func(t *testing.T) {
		// PK-форма с нулевого курсора: fresh (NULL) и ancient (vOld) —
		// устарели против той же текущей версии, done — нет.
		res, err := analyzer.ReanalyzeDrain(ctx, pool, q, vCurrent, nil, analyzer.ReanalyzeOptions{})
		if err != nil {
			t.Fatalf("ReanalyzeDrain: %v", err)
		}
		if res.Selected != 2 || res.Published != 2 {
			t.Fatalf("дренаж = %+v, want 2/2 (fresh + ancient вне lookback)", res)
		}
		subs := map[int16]bool{}
		for _, m := range q.publishes[1:] {
			subs[m.SubdomainID] = true
		}
		if !subs[subAncient] {
			t.Errorf("дренаж не нашёл ancient (субдомен %d) — вне lookback", subAncient)
		}
		if subs[subDone] {
			t.Errorf("дренаж отобрал done (текущая версия) — не должна отбираться")
		}
	})

	t.Run("предикат курсора обслуживает subdomain_results_checked_idx", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		// На трёх строках seq scan дешевле индекса — отключаем его и
		// проверяем индексируемость предиката (форма §4.1), не выбор
		// планировщика по стоимости: на боевом объёме окно индекса —
		// доли секунды против 19,9 с полного скана (ТЗ §2).
		if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			t.Fatalf("SET LOCAL: %v", err)
		}
		// Время-форма с литералами (параметры в EXPLAIN требуют
		// подготовленного оператора — стиль rqBatchSQL): якорь —
		// now() − 24 ч, нулевая PK-часть.
		explain := "EXPLAIN (COSTS OFF) " + fmt.Sprintf(
			"SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.checked_at "+
				"FROM subdomain_results AS r "+
				"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id "+
				"WHERE r.rules_version IS DISTINCT FROM %d::int4 "+
				"AND d.status <> %d::int2 "+
				"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) "+
				"> (now() - interval '%d hours', 0::int2, 0::int8, 0::int2, 0::int2) "+
				"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant "+
				"LIMIT 5000",
			vCurrent, int16(model.StatusDeleted),
			int(analyzer.DefaultReanalyzeLookback/time.Hour))
		rows, err := tx.Query(ctx, explain)
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
		// Родительский индекс subdomain_results_checked_idx (миграция 0014)
		// партиционирован: на листе DEFAULT экземпляр материализуется как
		// subdomain_results_default_checked_at_idx (и <лист>_checked_at_idx
		// на зональных) — assert по суффиксу имени, инвариантному к разбиению.
		if !strings.Contains(plan.String(), "_checked_at_idx") {
			t.Errorf("предикат композитного курсора не обслуживается листовым экземпляром subdomain_results_checked_idx:\n%s",
				plan.String())
		}
	})
}
