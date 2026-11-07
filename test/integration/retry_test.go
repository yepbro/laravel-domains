// Интеграционные тесты прохода повторных проверок temp_error RetryOnce
// (T-031; выборка кандидатов seq scan'ом с закреплением плана SET LOCAL —
// T-191, guard корзинами зрелости retry_mature — T-202, вынос декремента
// корзин из оператора зон повтора — T-267) против живого
// PostgreSQL: юнит-тесты retry_test.go проверяют SQL-контракты и
// последовательность на фейках, здесь — реальное поведение на
// мигрированной схеме: зрелая когорта temp_error дренируется пачками
// (назначение повтора по temp_attempts < N, финализация в
// delegated_no_address по исчерпании), переведённые не выбираются
// повторно; декременты корзин зануляют guard — тяжёлая выборка не
// выполняется вовсе (обёртка пула со счётчиком вызовов — предписание ТЗ
// T-191 §7.2); сегодняшние эпизоды guard не открывают; отсутствие строк
// корзин — честный ноль; реконсиляция корзин сходится с живым окном.
// Форма закреплённого плана проверяется EXPLAIN в той же транзакции с
// SET LOCAL (seq scan по domains, индексные формы отключены). Уровень
// вызовов — как у соседей: библиотечные вызовы без запуска сервисов;
// изоляция — withTestSchema.
package integration

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/scheduler"
)

// retrySeqScanRe — seq scan по domains (алиас d) в EXPLAIN: PG16 печатает
// листовую партицию («Seq Scan on domains_default d»), PG17+ — родителя
// («Seq Scan on d»).
var retrySeqScanRe = regexp.MustCompile(`Seq Scan on (\w+ )?d\b`)

// retryBatchPrefix — начало retrySelectSQL (литерал
// internal/scheduler/retry.go, дублирование — стиль schema_test.go):
// обёртка пула считает вызовы тяжёлой выборки кандидатов — guard T-202
// обязан держать её за нулевой суммой корзин зрелости. Выборка идёт
// ВНУТРИ транзакции закрепления плана, потому счётчик живёт в обёртке
// транзакции.
const retryBatchPrefix = "SELECT d.zone_id, d.id, d.temp_attempts FROM domains d"

// retryBatchSQL — полный текст выборки для EXPLAIN (LIMIT — литералом:
// параметр в EXPLAIN требует подготовленного оператора, проверке формы
// плана литерал безразличен; коды — status temp_error 5, check_state
// done 2).
const retryBatchSQL = retryBatchPrefix +
	" WHERE d.status = 5 AND d.check_state = 2 AND d.last_checked_at < DATE '2020-01-01' LIMIT 100"

// retryTracePool — RetryDB-обёртка пула: считает вызовы тяжёлой выборки
// кандидатов (обёртка пула со счётчиком вызовов — предписание ТЗ T-191
// §7.2).
type retryTracePool struct {
	*pgxpool.Pool

	mu    sync.Mutex
	batch int
}

func (p *retryTracePool) countBatch() {
	p.mu.Lock()
	p.batch++
	p.mu.Unlock()
}

func (p *retryTracePool) batchCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batch
}

// Begin оборачивает транзакцию счётчиком выборки.
func (p *retryTracePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &retryTraceTx{Tx: tx, pool: p}, nil
}

// retryTraceTx — транзакция-обёртка: тяжёлая выборка выполняется внутри
// транзакции закрепления плана (retryCandidates, T-191).
type retryTraceTx struct {
	pgx.Tx

	pool *retryTracePool
}

func (t *retryTraceTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.HasPrefix(sql, retryBatchPrefix) {
		t.pool.countBatch()
	}
	return t.Tx.Query(ctx, sql, args...)
}

// retryMatureFailPool — RetryDB-обёртка пула: транзакция, чей Exec
// хвостового декремента корзин зрелости (retryRequeueMatureSQL, второй
// оператор назначения повтора, T-267) падает — проверка атомарности
// половины повтора: откатываются и перевод, и декремент.
type retryMatureFailPool struct {
	*pgxpool.Pool
}

// retryMatureDecMark — опознаватель декремента корзин в тексте SQL
// (дублирование литерал — стиль schema_test.go: константа не экспортирована).
const retryMatureDecMark = "FROM unnest($1::date[]) AS d"

func (p *retryMatureFailPool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &retryMatureFailTx{Tx: tx}, nil
}

// retryMatureFailTx — транзакция-обёртка: Exec декремента корзин
// зрелости падает, остальное делегируется.
type retryMatureFailTx struct {
	pgx.Tx
}

func (t *retryMatureFailTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "retry_mature") && strings.Contains(sql, retryMatureDecMark) {
		return pgconn.CommandTag{}, errors.New("сбой декремента корзин (сценарий)")
	}
	return t.Tx.Exec(ctx, sql, args...)
}

// TestRetryTempErrorPass — проход повторных проверок на живом PostgreSQL:
// зрелая когорта temp_error дренируется пачками (итерации сходятся,
// переведённые не выбираются повторно), декременты корзин зрелости
// (T-202) зануляют guard после дренажа — тяжёлая выборка не выполняется
// вовсе; сегодняшние эпизоды (незрелые) guard не открывает; отсутствие
// строк корзин — честный ноль, а не ошибка; реконсиляция корзин
// сходится с живым окном и вычищает устаревшие.
func TestRetryTempErrorPass(t *testing.T) {
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
	pool := &retryTracePool{Pool: raw}

	// Форма плана закреплена (§4.2 ТЗ T-191): внутри транзакции SET LOCAL
	// (все индексные формы отключены) EXPLAIN показывает seq scan по
	// domains, индексных узлов нет — планировщику запрещён вырожденный
	// диапазонный скан по частичному domains_recheck_idx.
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
		rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+retryBatchSQL)
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
		if !retrySeqScanRe.MatchString(plan.String()) {
			t.Errorf("закреплённый план без seq scan по domains (алиас d):\n%s", plan.String())
		}
		if strings.Contains(plan.String(), "Index") || strings.Contains(plan.String(), "Bitmap") {
			t.Errorf("закреплённый план содержит индексную форму (SET LOCAL не подействовал):\n%s", plan.String())
		}
	})

	// Когорта temp_error: зрелые кандидаты повтора (done + старше окна) —
	// домены requeue (попыток 1 < N) и final (попыток 3 = N); молодые и
	// незавершённые кандидатами не являются (окно RetryAfter и
	// check_state). last_checked_at — date-литералом в прошлом (окно
	// по умолчанию 24 ч, ТЗ 5.3). Корзину зрелости прошедших суток кладёт
	// руками (значение — как у реконсиляции T-202: два зрелых кандидата;
	// сид — прямой INSERT, инкрементов записи прямой INSERT не ведёт).
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('retry.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	seed := func(label string, status model.Status, state model.CheckState, checked string, attempts int16) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`INSERT INTO domains (zone_id, label, status, check_state, last_checked_at, temp_attempts)
			 VALUES ($1, $2, $3, $4, $5::date, $6)`,
			zone, label, int16(status), int16(state), checked, attempts,
		); err != nil {
			t.Fatalf("insert domains %s: %v", label, err)
		}
	}
	seed("requeue", model.StatusTempError, model.CheckStateDone, "2020-01-01", 1)
	seed("final", model.StatusTempError, model.CheckStateDone, "2020-01-01", int16(scheduler.DefaultRetryMaxRetries))
	seed("young", model.StatusTempError, model.CheckStateDone, "2999-01-01", 0)      // моложе окна — не кандидат
	seed("pending", model.StatusTempError, model.CheckStatePending, "2020-01-01", 0) // ждёт диспетчера — не кандидат
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) "+
			"VALUES ('retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD'), 2)",
	); err != nil {
		t.Fatalf("insert crawler_counters: %v", err)
	}

	// retryMatureSum — guard-сумма корзин прошедших суток (тот же
	// предикат, что retryMatureSQL планировщика).
	retryMatureSum := func(t *testing.T) int64 {
		t.Helper()
		var sum int64
		if err := pool.QueryRow(ctx,
			"SELECT coalesce(sum(value), 0) FROM crawler_counters "+
				"WHERE name >= 'retry_mature:' AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
		).Scan(&sum); err != nil {
			t.Fatalf("guard-сумма корзин: %v", err)
		}
		return sum
	}

	// Дренаж пачками по 1 (T-031): BatchSize = 1 — каждая пачка полная,
	// цикл продолжает итерации до исчерпания; переведённые (pending) и
	// незрелые не выбираются повторно. Итераций с тяжёлой выборкой — две
	// (по числу кандидатов): каждый перевод декрементирует корзину
	// (2 → 1 → 0), нулевой guard останавливает дренаж ДО следующей
	// выборки (T-202) — пустой тяжёлой выборки здесь не бывает; она —
	// запасной останов дренажа при дрейфе корзин вверх.
	t.Run("итерации дренажа сходятся: назначение и финализация", func(t *testing.T) {
		var logBuf strings.Builder
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))

		drainCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- scheduler.RetryTempErrors(drainCtx, pool, scheduler.RetryOptions{
				BatchSize: 1,
				Interval:  time.Hour, // тик не наступит: всё должен разобрать дренаж
				Log:       logger,
			})
		}()

		// Две полные пачки по 1; ждём двух тяжёлых выборок, третьей быть
		// не должно (guard закрыт декрементами; Interval = час).
		deadline := time.Now().Add(10 * time.Second)
		for pool.batchCalls() < 2 {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("дренаж не дошёл до исчерпания: тяжёлых выборок %d", pool.batchCalls())
			}
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // дренаж обязан остановиться без третьей выборки
		if n := pool.batchCalls(); n != 2 {
			t.Fatalf("тяжёлых выборок %d, хочу 2: нулевой guard закрывает дренаж до выборки", n)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("RetryTempErrors: %v", err)
		}

		// Состояние когорты: повтор — pending и +1 попытка; итог —
		// delegated_no_address и сброс счётчика; молодые и незавершённые
		// не тронуты.
		type domRow struct {
			status    model.Status
			state     model.CheckState
			attempts  int16
			lastCheck string
		}
		got := make(map[string]domRow)
		rows, err := pool.Query(ctx,
			`SELECT label, status, check_state, temp_attempts, last_checked_at::text FROM domains`,
		)
		if err != nil {
			t.Fatalf("select domains: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var label string
			var r domRow
			var status, state int16
			if err := rows.Scan(&label, &status, &state, &r.attempts, &r.lastCheck); err != nil {
				t.Fatalf("select domains: чтение: %v", err)
			}
			r.status, r.state = model.Status(status), model.CheckState(state)
			got[label] = r
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("select domains: %v", err)
		}
		if r := got["requeue"]; r.status != model.StatusTempError || r.state != model.CheckStatePending || r.attempts != 2 {
			t.Errorf("requeue = статус %d состояние %d попыток %d, want temp_error/pending/2 (назначен повтор)", r.status, r.state, r.attempts)
		}
		if r := got["final"]; r.status != model.StatusDelegatedNoAddress || r.state != model.CheckStateDone || r.attempts != 0 {
			t.Errorf("final = статус %d состояние %d попыток %d, want delegated_no_address/done/0 (итог по temp_attempts)", r.status, r.state, r.attempts)
		}
		if r := got["young"]; r.status != model.StatusTempError || r.state != model.CheckStateDone || r.attempts != 0 {
			t.Errorf("young тронут: %+v, want temp_error/done/0 (моложе окна)", r)
		}
		if r := got["pending"]; r.status != model.StatusTempError || r.state != model.CheckStatePending || r.attempts != 0 {
			t.Errorf("pending тронут: %+v, want temp_error/pending/0 (не кандидат)", r)
		}

		// Дельты счётчиков зоны (CTE zc retryRequeueSQL, T-178): единственный
		// переход done → pending — по requeue.
		var checked, pend int64
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checked, &pend); err != nil {
			t.Fatalf("select zones: %v", err)
		}
		if checked != -1 || pend != 1 {
			t.Errorf("счётчики зоны = checked %d pending %d, want -1 и 1 (переход done → pending)", checked, pend)
		}

		// Корзины зрелости (T-202/T-267): декременты хвостового оператора
		// назначения повтора ушли в корзину дат эпизодов (2020-01-01,
		// по −1 на назначение и финализацию — ключ выводится из дат
		// переведённых строк, переданных пачкой из RETURNING первого
		// оператора) — guard-сумма сходится с живым числом зрелых
		// кандидатов (0).
		var stale int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:2020-01-01'",
		).Scan(&stale); err != nil {
			t.Fatalf("корзина дат эпизодов: %v", err)
		}
		if stale != -2 {
			t.Errorf("корзина 2020-01-01 = %d, want -2 (декременты назначения и финализации)", stale)
		}
		if got := retryMatureSum(t); got != 0 {
			t.Errorf("guard-сумма корзин = %d, want 0 (зрелые исчерпаны, дренаж не откроется)", got)
		}

		// Итог дренажа — сумма пачек: по одному каждого вида.
		for _, want := range []string{"requeued=1", "finalized=1"} {
			if !strings.Contains(logBuf.String(), want) {
				t.Errorf("лог дренажа не содержит %q:\n%s", want, logBuf.String())
			}
		}
	})

	// Регрессия дедлока 40P01 счётчиков: решения пачки (назначение
	// повтора и финализация) выполняются РАЗДЕЛЬНЫМИ транзакциями.
	// Сессия-держатель держит SELECT FOR UPDATE domains-строку кандидата
	// финализации — как транзакция записи результата http-worker держит
	// свои domains-строки, пока не дойдёт до хвостовых счётчиков
	// (checks:/temp_error:/retry_mature:). Половина повтора обязана
	// зафиксироваться и отпустить свои блокировки (domains/зоны/корзина
	// retry_mature — перевод и хвостовой декремент ДВУМЯ операторами
	// одной транзакции, T-267), НЕ дожидаясь финализации: в общей
	// транзакции пачки она висела бы на чужой domains-строке, держа
	// корзину счётчика, — ребро «счётчики → domains» цикла дедлока с
	// записью результата.
	t.Run("половины пачки — независимые транзакции (регрессия 40P01)", func(t *testing.T) {
		seed("mixreq", model.StatusTempError, model.CheckStateDone, "2020-01-01", 0) // повтор
		seed("mixfin", model.StatusTempError, model.CheckStateDone, "2020-01-01", int16(scheduler.DefaultRetryMaxRetries))
		// Guard-фикстура самодостаточна: хвосты сабтеста дренажа выше —
		// корзина вчерашнего дня = 2 (сид guard'а) и декременты
		// 2020-01-01 = −2 (их сумма нулевая, но новый проход обязан
		// опираться только на свою корзину) — обнуляются; корзина дат
		// эпизодов кандидатов := 2. Строки не удаляются: корзину
		// вчерашнего дня читает сабтест реконсиляции ниже.
		if _, err := pool.Exec(ctx,
			"UPDATE crawler_counters SET value = 0 WHERE name LIKE 'retry_mature:%'",
		); err != nil {
			t.Fatalf("обнуление корзин зрелости: %v", err)
		}
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value, updated_at) "+
				"VALUES ('retry_mature:2020-01-01', 2, now()) "+
				"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value",
		); err != nil {
			t.Fatalf("reset корзины зрелости: %v", err)
		}

		// Держатель: domains-строка кандидата финализации захвачена до
		// прохода (образ транзакции записи результата).
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin держателя: %v", err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx,
			"SELECT id FROM domains WHERE zone_id = $1 AND label = $2 FOR UPDATE", zone, "mixfin",
		); err != nil {
			t.Fatalf("FOR UPDATE держателя: %v", err)
		}

		type onceResult struct {
			res scheduler.RetryResult
			err error
		}
		done := make(chan onceResult, 1)
		go func() {
			res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
			done <- onceResult{res, err}
		}()

		// Пока держатель не отпустил строку финализации, половина
		// повтора уже зафиксирована и видна из другой сессии: retryRequeue
		// закоммичен ДО ожидания retryFinalize. До фикса (общая
		// транзакция пачки) перевод повтора висел бы вместе с
		// финализацией — poll упирался бы в дедлайн.
		deadline := time.Now().Add(10 * time.Second)
		var state int16
		for time.Now().Before(deadline) {
			if err := pool.QueryRow(ctx,
				"SELECT check_state FROM domains WHERE zone_id = $1 AND label = $2", zone, "mixreq",
			).Scan(&state); err != nil {
				t.Fatalf("чтение mixreq: %v", err)
			}
			if state == int16(model.CheckStatePending) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if state != int16(model.CheckStatePending) {
			t.Fatalf("половина повтора не зафиксирована до финализации: check_state mixreq = %d", state)
		}

		// Держатель отпускает строку — финализация проходит, проход
		// завершается парой {1, 1}.
		if err := holder.Rollback(ctx); err != nil {
			t.Fatalf("откат держателя: %v", err)
		}
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("RetryOnce: %v", r.err)
			}
			if want := (scheduler.RetryResult{Requeued: 1, Finalized: 1}); r.res != want {
				t.Errorf("итог = %+v, want %+v", r.res, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("RetryOnce не завершился после освобождения строки")
		}
		var finStatus int16
		if err := pool.QueryRow(ctx,
			"SELECT status FROM domains WHERE zone_id = $1 AND label = $2", zone, "mixfin",
		).Scan(&finStatus); err != nil {
			t.Fatalf("чтение mixfin: %v", err)
		}
		if finStatus != int16(model.StatusDelegatedNoAddress) {
			t.Errorf("статус mixfin = %d, want delegated_no_address (финализация после освобождения)", finStatus)
		}
	})

	// Атомарность половины повтора (T-267): перевод и хвостовой
	// декремент корзин — ДВА оператора ОДНОЙ транзакции; сбой декремента
	// откатывает половину целиком — домен остаётся кандидатом (done),
	// дельты зон и корзины не тронуты. Контраст с записью результата
	// DNS-этапа, где операторы разнесены autocommit'ами и корзину
	// догоняет реконсиляция (counters_test.go, окно ±1).
	t.Run("сбой декремента корзин откатывает половину повтора", func(t *testing.T) {
		seed("mixfail", model.StatusTempError, model.CheckStateDone, "2020-01-01", 0)
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value, updated_at) "+
				"VALUES ('retry_mature:2020-01-01', 1, now()) "+
				"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value",
		); err != nil {
			t.Fatalf("reset корзины зрелости: %v", err)
		}
		fail := &retryMatureFailPool{Pool: raw}

		var checkedBefore, pendBefore int64
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checkedBefore, &pendBefore); err != nil {
			t.Fatalf("select zones до: %v", err)
		}
		if _, err := scheduler.RetryOnce(ctx, fail, scheduler.RetryOptions{BatchSize: 1}); err == nil ||
			!strings.Contains(err.Error(), "декремент корзин зрелости") {
			t.Fatalf("RetryOnce = %v, want ошибка декремента корзин зрелости", err)
		}
		var state, attempts int16
		if err := pool.QueryRow(ctx,
			"SELECT check_state, temp_attempts FROM domains WHERE zone_id = $1 AND label = $2", zone, "mixfail",
		).Scan(&state, &attempts); err != nil {
			t.Fatalf("чтение mixfail: %v", err)
		}
		if model.CheckState(state) != model.CheckStateDone || attempts != 0 {
			t.Fatalf("mixfail = состояние %d попыток %d, want done/0 (перевод откатился вместе с декрементом)",
				state, attempts)
		}
		var checked, pend int64
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checked, &pend); err != nil {
			t.Fatalf("select zones: %v", err)
		}
		if checked != checkedBefore || pend != pendBefore {
			t.Errorf("дельты зоны изменились при откате: checked %d pending %d, want без изменений (%d/%d)",
				checked, pend, checkedBefore, pendBefore)
		}
		var basket int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:2020-01-01'",
		).Scan(&basket); err != nil {
			t.Fatalf("корзина дат эпизодов: %v", err)
		}
		if basket != 1 {
			t.Errorf("корзина 2020-01-01 = %d, want 1 (декремент откатился вместе с переводом)", basket)
		}
		// Кандидат цел — следующий проход (живой пул) его разбирает:
		// перевод и декремент проходят вместе, дельты зоны уходят.
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{BatchSize: 1})
		if err != nil {
			t.Fatalf("повторный проход: %v", err)
		}
		if res.Requeued != 1 {
			t.Errorf("повторный проход = %+v, want requeued=1 (кандидат уцелел в done)", res)
		}
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checked, &pend); err != nil {
			t.Fatalf("select zones после прохода: %v", err)
		}
		if checked != checkedBefore-1 || pend != pendBefore+1 {
			t.Errorf("дельты зоны после прохода: checked %d pending %d, want %d и %d (done → pending)",
				checked, pend, checkedBefore-1, pendBefore+1)
		}
	})

	// Стационар (граница доверия guard, TZ T-202): зрелых нет — тяжёлая
	// выборка не выполняется вовсе, тик стоит одну сумму корзин.
	t.Run("зрелых нет — тяжёлая выборка не выполняется", func(t *testing.T) {
		before := pool.batchCalls()
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
		if err != nil {
			t.Fatalf("RetryOnce: %v", err)
		}
		if res != (scheduler.RetryResult{}) {
			t.Fatalf("итог %+v, хочу пустой (guard: зрелых нет)", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("тяжёлая выборка выполнена %d раз при нулевой сумме корзин (было %d)", n-before, before)
		}
	})

	// Незрелые (сегодняшние эпизоды, T-202): корзина текущих суток в
	// guard не входит — эпизоды зреют к полуночи, тяжёлая выборка не
	// открывается, хотя домен формально ответил критерию статуса.
	t.Run("сегодняшние эпизоды — guard не пускает", func(t *testing.T) {
		// Дата эпизода — current_date САМОЙ БД (bind-параметр $5::date
		// принимает только литерал, «current_date» строкой не парсится;
		// значение из БД — таймзона согласована с FILTER реконсиляции).
		var today string
		if err := pool.QueryRow(ctx,
			"SELECT to_char(current_date, 'YYYY-MM-DD')",
		).Scan(&today); err != nil {
			t.Fatalf("current_date: %v", err)
		}
		seed("fresh", model.StatusTempError, model.CheckStateDone, today, 0)
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value) "+
				"VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), 1)",
		); err != nil {
			t.Fatalf("insert корзина суток: %v", err)
		}
		before := pool.batchCalls()
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
		if err != nil {
			t.Fatalf("RetryOnce: %v", err)
		}
		if res != (scheduler.RetryResult{}) {
			t.Fatalf("итог %+v, хочу пустой (сегодняшний эпизод незрелый)", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("тяжёлая выборка выполнена при незрелых корзинах: %d вызовов (было %d)", n-before, before)
		}
	})

	// Реконсиляция (T-202) — авторитет корзин: сегодняшняя корзина :=
	// живое окно (fresh — один temp_error × done сегодня), зрелая — 0,
	// устаревшая корзина дат старых эпизодов (хвост декрементов
	// 2020-01-01) удаляется; guard-сумма остаётся нулём (сегодняшняя
	// корзина в сумму не входит).
	t.Run("реконсиляция корзин сходится с живым окном", func(t *testing.T) {
		if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
			t.Fatalf("ZoneStatsOnce: %v", err)
		}
		var today, past int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
		).Scan(&today); err != nil {
			t.Fatalf("корзина суток: %v", err)
		}
		if today != 1 {
			t.Errorf("корзина суток после прохода = %d, want 1 (fresh — один эпизод сегодня)", today)
		}
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD')",
		).Scan(&past); err != nil {
			t.Fatalf("зрелая корзина: %v", err)
		}
		if past != 0 {
			t.Errorf("зрелая корзина после прохода = %d, want 0 (живых зрелых нет)", past)
		}
		var stale int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM crawler_counters WHERE name = 'retry_mature:2020-01-01'",
		).Scan(&stale); err != nil {
			t.Fatalf("устаревшая корзина: %v", err)
		}
		if stale != 0 {
			t.Error("устаревшая корзина 2020-01-01 жива после прохода (очистка T-202 не сработала)")
		}
		if got := retryMatureSum(t); got != 0 {
			t.Errorf("guard-сумма корзин = %d, want 0 (сегодняшняя корзина в сумму не входит)", got)
		}
	})

	// Отсутствие строк корзин — честный ноль, а не ошибка: строки корзин
	// создаёт первый инкремент суток или проход реконсиляции, их никто
	// не гарантирует (в отличие от status_* T-178).
	t.Run("корзин нет — честный ноль", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "DELETE FROM crawler_counters WHERE name LIKE 'retry_mature:%'"); err != nil {
			t.Fatal(err)
		}
		before := pool.batchCalls()
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
		if err != nil {
			t.Fatalf("RetryOnce: %v (отсутствие корзин — не ошибка: coalesce даёт ноль)", err)
		}
		if res != (scheduler.RetryResult{}) {
			t.Fatalf("итог %+v, хочу пустой", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("тяжёлая выборка выполнена без корзин: %d вызовов (было %d)", n-before, before)
		}
	})
}
