// Юнит-тесты применения delete без БД (стиль stage_test.go/zones_test.go):
// текст SQL и параметры, счётчики, порядок «применить → очистить», пустая
// партия, идемпотентность повтора (ТЗ 3.4), пара «снятие бита
// render_pending × декремент счётчика долга» двумя операторами одной
// транзакции (T-186/T-269), перенос ключей битовых строк между
// операторами, ошибки шагов и очистки, отмена ctx. Реальное поведение
// против живой БД — интеграционные тесты ingest (T-023) и renderbit
// (гонка инцидента 08.11.2026, T-269).
package ingest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/model"
)

// applyDeletePool — ApplyDB для тестов применения delete: транзакция
// Begin → applyDeleteSQL → applyClearRenderSQL → Commit, затем очистка
// партии через пул. Помнит порядок обращений с SQL и аргументами, ответы
// операторов — заготовленными строками (позиционный Scan, стиль
// cleanupFakeRows). Ошибки шагов — по именованным полям.
type applyDeletePool struct {
	ops  []string // порядок: "begin", "query:<sql>", "commit", "exec:<sql>"
	args [][]any  // аргументы операций в порядке вызовов

	applyRows *cleanupFakeRows // ответ applyDeleteSQL: deleted, not_found, render_zones, render_labels
	clearRows *cleanupFakeRows // ответ applyClearRenderSQL: value

	beginErr     error
	applyErr     error
	clearRendErr error
	commitErr    error
	clearErr     error // очистка партии (пул)

	begins    int
	commits   int
	rollbacks int
}

var _ ApplyDB = (*applyDeletePool)(nil)

func (p *applyDeletePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

// Query пула применению не нужен (операторы — в транзакции): отказ.
func (p *applyDeletePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("applyDeletePool: Query вне транзакции не должен вызываться")
}

func (p *applyDeletePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.ops = append(p.ops, "exec:"+sql)
	p.args = append(p.args, args)
	if sql != clearStageSQL {
		return pgconn.CommandTag{}, errors.New("applyDeletePool: неожиданный Exec: " + sql)
	}
	if p.clearErr != nil {
		return pgconn.CommandTag{}, p.clearErr
	}
	return pgconn.NewCommandTag("DELETE 1"), nil
}

func (p *applyDeletePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	p.begins++
	p.ops = append(p.ops, "begin")
	return &applyDeleteTx{pool: p}, nil
}

// applyDeleteTx — транзакция фейкового пула: операторы применения и
// снятия бита, фиксация/откат.
type applyDeleteTx struct {
	pgx.Tx // остальные методы pgx.Tx применение не использует

	pool      *applyDeletePool
	committed bool
}

func (t *applyDeleteTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := t.pool
	p.ops = append(p.ops, "query:"+sql)
	p.args = append(p.args, args)
	switch sql {
	case applyDeleteSQL:
		if p.applyErr != nil {
			return nil, p.applyErr
		}
		if p.applyRows == nil {
			return nil, errors.New("applyDeletePool: не задан ответ applyDeleteSQL")
		}
		return p.applyRows, nil
	case applyClearRenderSQL:
		if p.clearRendErr != nil {
			return nil, p.clearRendErr
		}
		if p.clearRows == nil {
			return nil, errors.New("applyDeletePool: не задан ответ applyClearRenderSQL")
		}
		return p.clearRows, nil
	}
	return nil, errors.New("applyDeletePool: неожиданный Query: " + sql)
}

func (t *applyDeleteTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("applyDeletePool: Exec в транзакции не должен вызываться")
}

func (t *applyDeleteTx) Commit(context.Context) error {
	t.pool.ops = append(t.pool.ops, "commit")
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.pool.commits++
	t.committed = true
	return nil
}

func (t *applyDeleteTx) Rollback(context.Context) error {
	if t.committed {
		return nil // pgx.ErrTxClosed в реальности; здесь достаточно no-op
	}
	t.pool.rollbacks++
	return nil
}

// validDeleteBatch — валидная delete-партия для тестов применения.
func validDeleteBatch(t *testing.T) Batch {
	t.Helper()
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	return Batch{ID: id, ZoneID: 7, Action: model.ActionDelete}
}

// wantApplyArgs — аргументы оператора применения: идентификатор партии,
// код статуса deleted и код операции delete (в параметрах, а не в тексте
// SQL — точные значения model закрепляются тестом).
func wantApplyArgs(b Batch) []any {
	return []any{b.ID[:], int16(model.StatusDeleted), int16(model.ActionDelete)}
}

// TestApplyDelete — полный протокол применения с битовыми строками:
// транзакция (перевод статуса → снятие бита со счётчиком по ключам
// applyDeleteSQL → фиксация), затем очистка партии. Ключи битовых строк
// (render_zones/render_labels) передаются второй парой операторов
// дословно.
func TestApplyDelete(t *testing.T) {
	b := validDeleteBatch(t)
	zones := []int16{7, 9}
	labels := []string{"a.example", "b.example"}
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(5), int64(2), zones, labels}),
		clearRows: cleanupRows([]any{int64(3)}),
	}

	res, err := ApplyDelete(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{Deleted: 5, NotFound: 2}) {
		t.Errorf("счётчики = %+v, want {5 2}", res)
	}

	// Порядок: begin → применение → снятие бита → commit → очистка.
	want := []string{"begin", "query:" + applyDeleteSQL, "query:" + applyClearRenderSQL, "commit", "exec:" + clearStageSQL}
	if !reflect.DeepEqual(pool.ops, want) {
		t.Fatalf("порядок обращений = %v, want %v", pool.ops, want)
	}
	if !reflect.DeepEqual(pool.args[0], wantApplyArgs(b)) {
		t.Errorf("аргументы применения = %#v, want %#v", pool.args[0], wantApplyArgs(b))
	}
	if !reflect.DeepEqual(pool.args[1], []any{zones, labels}) {
		t.Errorf("аргументы снятия бита = %#v, want [%v %v]", pool.args[1], zones, labels)
	}
	if !reflect.DeepEqual(pool.args[2], []any{b.ID[:]}) {
		t.Errorf("аргументы очистки = %#v, want идентификатор партии", pool.args[2])
	}
	if pool.commits != 1 || pool.rollbacks != 0 {
		t.Errorf("commit=%d rollback=%d, want 1/0", pool.commits, pool.rollbacks)
	}
}

// TestApplyDeleteNoRenderRows — файл без флаговых доменов: вторая пара
// (снятие бита × счётчик) не выполняется вовсе — строку счётчика файл
// не трогает (единая точка сериализации флажащих транзакций, record.go;
// прецедент HAVING T-186).
func TestApplyDeleteNoRenderRows(t *testing.T) {
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(4), int64(1), []int16{}, []string{}}),
	}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{Deleted: 4, NotFound: 1}) {
		t.Errorf("счётчики = %+v, want {4 1}", res)
	}
	want := []string{"begin", "query:" + applyDeleteSQL, "commit", "exec:" + clearStageSQL}
	if !reflect.DeepEqual(pool.ops, want) {
		t.Fatalf("порядок обращений = %v, want %v (без applyClearRenderSQL)", pool.ops, want)
	}
}

// TestApplyDeleteSQLContract — точный текст применения: перенос статуса
// по ключу (zone_id, label) из строк своей партии с охраной «ещё не
// deleted» (T-178: дельты счётчиков зон декрементируют только
// действительно переводимые строки — повторное применение партии
// идемпотентно и по счётчикам, ТЗ 3.4; отчёт при этом сохраняет прежнюю
// семантику: уже удалённый домен считается строкой «удалён»); оператор
// перевода бит render_pending НЕ трогает (T-269: дельта снимка had_render
// двойно декрементировала счётчик при конкурентном снятии проходом
// доработки — инцидент 08.11.2026), факт «бит стоял на момент перевода»
// выносят RETURNING-колонка had_render (версия-основание обновления) и
// массивы render_zones/render_labels; дельты счётчиков зон — по
// check_state переведённых; домен остаётся в БД (нет DELETE); счётчики —
// по строкам staging.
func TestApplyDeleteSQLContract(t *testing.T) {
	want := `WITH pre AS MATERIALIZED (
	SELECT d.zone_id, d.label
	FROM ` + StageTable.Sanitize() + ` AS s
	JOIN domains AS d ON d.zone_id = s.zone_id AND d.label = s.label
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.status <> $2
), moved AS (
	UPDATE domains AS d
	SET status = $2
	FROM pre AS p
	WHERE d.zone_id = p.zone_id
	  AND d.label = p.label
	  AND d.status <> $2
	RETURNING d.zone_id, d.label, d.check_state, (d.flags & 2) <> 0 AS had_render
), zc AS (
	UPDATE zones AS z
	SET domains_total = z.domains_total - c.n_total,
	    domains_pending = z.domains_pending - c.n_pending,
	    domains_queued = z.domains_queued - c.n_queued,
	    domains_checked = z.domains_checked - c.n_checked
	FROM (SELECT zone_id,
	             count(*) AS n_total,
	             count(*) FILTER (WHERE check_state = 0) AS n_pending,
	             count(*) FILTER (WHERE check_state = 1) AS n_queued,
	             count(*) FILTER (WHERE check_state = 2) AS n_checked
	      FROM moved GROUP BY zone_id) AS c
	WHERE z.id = c.zone_id
)
SELECT
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND (EXISTS (SELECT 1 FROM moved AS m
	               WHERE m.zone_id = s.zone_id AND m.label = s.label)
	       OR EXISTS (SELECT 1 FROM domains AS d2
	                  WHERE d2.zone_id = s.zone_id AND d2.label = s.label
	                    AND d2.status = $2))) AS deleted,
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND NOT EXISTS (SELECT 1 FROM domains AS d
	                  WHERE d.zone_id = s.zone_id AND d.label = s.label)) AS not_found,
	(SELECT coalesce(array_agg(m.zone_id ORDER BY m.zone_id, m.label), '{}'::int2[]) FROM moved AS m WHERE m.had_render) AS render_zones,
	(SELECT coalesce(array_agg(m.label ORDER BY m.zone_id, m.label), '{}'::text[]) FROM moved AS m WHERE m.had_render) AS render_labels`
	if applyDeleteSQL != want {
		t.Errorf("applyDeleteSQL = %q, want точное совпадение", applyDeleteSQL)
	}

	// Таблица staging — из StageTable, как COPY и очистка партии.
	if !strings.Contains(applyDeleteSQL, `FROM `+StageTable.Sanitize()+` AS s`) {
		t.Errorf("SQL = %q, want обращение к staging через StageTable", applyDeleteSQL)
	}
	// Только перевод статуса: домен остаётся в БД (ТЗ 13.1), результаты и
	// контент чистит T-020 — здесь их нет. Бит render_pending оператор
	// перевода не снимает (T-269) — в SET только status.
	if !strings.Contains(applyDeleteSQL, "UPDATE domains") || strings.Contains(applyDeleteSQL, "DELETE FROM domains") {
		t.Errorf("SQL = %q, want только UPDATE domains", applyDeleteSQL)
	}
	if strings.Contains(applyDeleteSQL, "SET flags") || strings.Contains(applyDeleteSQL, "CASE WHEN") {
		t.Errorf("SQL = %q, want перевод статуса БЕЗ касания flags (снятие бита — applyClearRenderSQL, T-269)", applyDeleteSQL)
	}
	// Охрана статуса пинирует перевод для дельт (T-178): оператор не
	// меняет check_state — RETURNING отдаёт старое значение (ТЗИ §6.1.2).
	if !strings.Contains(applyDeleteSQL, "AND d.status <> $2") {
		t.Errorf("SQL = %q, want охрана «ещё не deleted» для дельт счётчиков", applyDeleteSQL)
	}
	// Отчёт: уже удалённый до оператора домен считается «удалён»
	// (EXISTS по domains со старым статусом — снимок оператора).
	if !strings.Contains(applyDeleteSQL, "OR EXISTS") {
		t.Errorf("SQL = %q, want альтернативу «уже был deleted» в счётчике deleted", applyDeleteSQL)
	}
	// Факт битовых строк — RETURNING-колонкой (версия-основание, не
	// снимок pre) и параллельными массивами для второй пары; ОБЯАТЕЛЬНЫЙ
	// ORDER BY в обоих array_agg — контракт парности ключей (len-проверка
	// в Go перестановку не ловит, порядок агрегатов без него не задан —
	// ревью T-269 P2-3).
	if !strings.Contains(applyDeleteSQL, "(d.flags & 2) <> 0 AS had_render") ||
		!strings.Contains(applyDeleteSQL, "array_agg(m.zone_id ORDER BY m.zone_id, m.label)") ||
		!strings.Contains(applyDeleteSQL, "array_agg(m.label ORDER BY m.zone_id, m.label)") {
		t.Errorf("SQL = %q, want had_render по версии-основанию и согласованно упорядоченные массивы ключей", applyDeleteSQL)
	}
}

// TestApplyClearRenderSQLContract — точный текст второй пары: снятие
// бита по ОХРАНЕ текущей версии (окно «снимок → блокировка» инцидента
// 08.11.2026 закрыто: конкурентное снятие даёт 0 строк, счётчик не
// трогается — зеркально Д7), декремент — по факту снятых (count из
// RETURNING, runbooks §6), строка счётчика не трогается файлами без
// флаговых доменов (guard count > 0), RETURNING — контроль наличия
// строки (миграция 000015).
func TestApplyClearRenderSQLContract(t *testing.T) {
	want := `WITH cleared AS (
	UPDATE domains AS d
	SET flags = (d.flags & -3)::int2
	FROM unnest($1::int2[], $2::text[]) AS k(zone_id, label)
	WHERE d.zone_id = k.zone_id
	  AND d.label = k.label
	  AND (d.flags & 2) <> 0
	RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending' AND (SELECT count(*) FROM cleared) > 0
RETURNING value`
	if applyClearRenderSQL != want {
		t.Errorf("applyClearRenderSQL = %q, want точное совпадение", applyClearRenderSQL)
	}
	// Охрана бита — по текущей версии строки (стиль renderClearPendingSQL
	// httpworker: RowsAffected 0 — перехода не было, счётчик не трогается).
	if !strings.Contains(applyClearRenderSQL, "AND (d.flags & 2) <> 0") {
		t.Errorf("SQL = %q, want охрану бита текущей версии", applyClearRenderSQL)
	}
	// Дельта — только по фактически снятым (count из RETURNING), не по
	// заготовленному числу.
	if !strings.Contains(applyClearRenderSQL, "value - (SELECT count(*) FROM cleared)") {
		t.Errorf("SQL = %q, want декремент по факту снятых", applyClearRenderSQL)
	}
}

// TestApplyDeleteEmptyBatch — партия без строк (файл из одних пустых
// строк/комментариев): no-op с нулевыми счётчиками, очистка выполняется.
func TestApplyDeleteEmptyBatch(t *testing.T) {
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(0), int64(0), []int16{}, []string{}}),
	}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{}) {
		t.Errorf("счётчики = %+v, want нулевые", res)
	}
	if len(pool.ops) != 4 {
		t.Errorf("обращений = %d (%v), want 4 — без второй пары", len(pool.ops), pool.ops)
	}
	if pool.commits != 1 {
		t.Errorf("commit = %d, want 1 (транзакция выполняется и пустой)", pool.commits)
	}
}

// TestApplyDeleteIdempotentRepeat — повторное применение после очистки —
// no-op: staging партии пуст, счётчики нулевые, ошибок нет (ТЗ 3.4).
func TestApplyDeleteIdempotentRepeat(t *testing.T) {
	b := validDeleteBatch(t)
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(3), int64(0), []int16{}, []string{}}),
	}
	// Второе применение — тот же ответ (партия в staging фейка не
	// моделируется: важен протокол повтора, не состояние).
	pool2 := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(0), int64(0), []int16{}, []string{}}),
	}

	first, err := ApplyDelete(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if first != (DeleteResult{Deleted: 3, NotFound: 0}) {
		t.Fatalf("первое применение = %+v, want {3 0}", first)
	}

	second, err := ApplyDelete(context.Background(), pool2, b)
	if err != nil {
		t.Fatalf("повторное применение: %v", err)
	}
	if second != (DeleteResult{}) {
		t.Errorf("повтор = %+v, want нулевые счётчики (no-op)", second)
	}
	if len(pool2.ops) != 4 || strings.Join(pool2.ops, ";") != strings.Join([]string{"begin", "query:" + applyDeleteSQL, "commit", "exec:" + clearStageSQL}, ";") {
		t.Errorf("повтор: обращения = %v, want без второй пары", pool2.ops)
	}
}

// TestApplyDeleteValidation — программные ошибки партии отбиваются до БД:
// партия без идентификатора и партия не-delete (иначе очистка в конце
// ApplyDelete молча уничтожила бы неприменённую add-партию).
func TestApplyDeleteValidation(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		b    Batch
	}{
		{"нулевой идентификатор", Batch{ZoneID: 1, Action: model.ActionDelete}},
		{"партия add", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}},
		{"нулевой код операции", Batch{ID: id, ZoneID: 1, Action: 0}},
		{"неизвестный код операции", Batch{ID: id, ZoneID: 1, Action: model.Action(9)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{}
			if _, err := ApplyDelete(context.Background(), pool, tc.b); err == nil {
				t.Fatal("ожидалась ошибка валидации")
			}
			if len(pool.ops) != 0 {
				t.Errorf("невалидная партия дошла до БД: %v", pool.ops)
			}
		})
	}
}

// TestApplyDeleteApplyError — ошибка применения (в том числе отмена ctx)
// прерывает транзакцию до пары и очистки: партия остаётся в staging для
// повтора (ТЗ 3.4).
func TestApplyDeleteApplyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"отмена контекста", context.Canceled},
		{"ошибка запроса", errors.New("connection refused")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{
				applyRows: cleanupRows([]any{int64(3), int64(0), []int16{}, []string{}}),
				applyErr:  tc.err,
			}

			_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
			if err == nil || !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want обёрнутая %v", err, tc.err)
			}
			if !strings.Contains(err.Error(), "применение delete") {
				t.Errorf("err = %v, want контекст «применение delete»", err)
			}
			if pool.commits != 0 || pool.rollbacks != 1 {
				t.Errorf("commit=%d rollback=%d, want 0/1 (откат)", pool.commits, pool.rollbacks)
			}
			if len(pool.ops) != 2 { // begin + query:применение
				t.Errorf("обращения = %v, want применение без пары и очистки", pool.ops)
			}
		})
	}
}

// TestApplyDeleteClearRenderError — ошибки второй пары: сбой оператора и
// отсутствие строки счётчика откатывают применение целиком (молчаливый
// рассинхрон «сняли бит — счётчик не сдвинут» недопустим, философия
// renderBumpDebtCounter).
func TestApplyDeleteClearRenderError(t *testing.T) {
	t.Run("сбой оператора", func(t *testing.T) {
		pool := &applyDeletePool{
			applyRows:    cleanupRows([]any{int64(2), int64(0), []int16{7}, []string{"a"}}),
			clearRendErr: errors.New("deadlock detected"),
		}
		_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
		if err == nil || !strings.Contains(err.Error(), "снятие бита render_pending") {
			t.Fatalf("err = %v, want ошибку второй пары", err)
		}
		if pool.commits != 0 || pool.rollbacks != 1 {
			t.Errorf("commit=%d rollback=%d, want 0/1 (откат)", pool.commits, pool.rollbacks)
		}
		if len(pool.ops) > 3 {
			t.Errorf("обращения = %v, want без commit/очистки", pool.ops)
		}
	})
	t.Run("строки счётчика нет", func(t *testing.T) {
		pool := &applyDeletePool{
			applyRows: cleanupRows([]any{int64(2), int64(0), []int16{7}, []string{"a"}}),
			clearRows: cleanupRows(), // RETURNING пуст — строки нет
		}
		_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
		if err == nil || !strings.Contains(err.Error(), "'render_pending' отсутствует") {
			t.Fatalf("err = %v, want ошибку отсутствия строки счётчика", err)
		}
		if pool.commits != 0 || pool.rollbacks != 1 {
			t.Errorf("commit=%d rollback=%d, want 0/1 (откат)", pool.commits, pool.rollbacks)
		}
	})
}

// TestApplyDeleteBadCounters — защита от развала контракта запроса:
// отсутствие строки счётчиков, лишние колонки, расхождение битовых
// массивов — ошибка, откат, очистка не выполняется.
func TestApplyDeleteBadCounters(t *testing.T) {
	cases := []struct {
		name     string
		result   *cleanupFakeRows
		want     string
		rollback bool
	}{
		{"нет строки", cleanupRows(), "счётчики не возвращены", true},
		{"лишняя колонка", cleanupRows([]any{int64(1), int64(2), []int16{}, []string{}, int64(9)}), "применение delete", true},
		{"массивы разошлись", cleanupRows([]any{int64(1), int64(0), []int16{7, 8}, []string{"a"}}), "битовые массивы разошлись", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{applyRows: tc.result}

			_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ошибку с %q", err, tc.want)
			}
			if tc.rollback && pool.rollbacks != 1 {
				t.Errorf("rollback = %d, want 1", pool.rollbacks)
			}
			if pool.commits != 0 {
				t.Errorf("commit = %d, want 0", pool.commits)
			}
		})
	}
}

// TestApplyDeleteTxErrors — ошибки транзакции (begin/commit) прерывают
// применение; очистка партии не выполняется.
func TestApplyDeleteTxErrors(t *testing.T) {
	t.Run("begin", func(t *testing.T) {
		pool := &applyDeletePool{beginErr: errors.New("pool closed")}
		if _, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t)); err == nil || !strings.Contains(err.Error(), "транзакция") {
			t.Fatalf("err = %v, want ошибку транзакции", err)
		}
		if len(pool.ops) != 0 {
			t.Errorf("обращения = %v, want пусто", pool.ops)
		}
	})
	t.Run("commit", func(t *testing.T) {
		pool := &applyDeletePool{
			applyRows: cleanupRows([]any{int64(1), int64(0), []int16{}, []string{}}),
			commitErr: errors.New("connection reset"),
		}
		if _, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t)); err == nil || !strings.Contains(err.Error(), "фиксация") {
			t.Fatalf("err = %v, want ошибку фиксации", err)
		}
		if len(pool.ops) != 3 { // begin, применение, commit (упавший)
			t.Errorf("обращения = %v, want без очистки партии", pool.ops)
		}
	})
}

// TestApplyDeleteClearError — ошибка очистки не отменяет применённый
// delete: счётчики возвращаются вместе с ошибкой, повтор доведёт очистку.
func TestApplyDeleteClearError(t *testing.T) {
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(4), int64(1), []int16{}, []string{}}),
		clearErr:  errors.New("deadlock detected"),
	}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err == nil || !strings.Contains(err.Error(), "deadlock detected") {
		t.Fatalf("err = %v, want ошибка очистки", err)
	}
	if !strings.Contains(err.Error(), "ingest: очистка партии") {
		t.Errorf("err = %v, want контекст очистки партии", err)
	}
	if res != (DeleteResult{Deleted: 4, NotFound: 1}) {
		t.Errorf("счётчики = %+v, want {4 1}: применение состоялось", res)
	}
	if pool.commits != 1 {
		t.Errorf("commit = %d, want 1 (применение зафиксировано до очистки)", pool.commits)
	}
}
