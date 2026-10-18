// Интеграционные тесты идемпотентности приёма на уровне доменов (T-097,
// вариант D; ТЗ 3.4, 5.1): повторная доставка и повторная обработка того
// же файла не меняют состояние доменов — файловый дедуп по sha256 лишь
// оптимизация, корректность гарантируют операции над доменами
// (ON CONFLICT по (zone_id, label), охрана revive по status = deleted,
// UPDATE delete без охраны, очистка refcount строго по фактически
// удалённым ссылкам).
//
// Сценарий «тот же файл обработан дважды» моделируется сбоем в окне
// между применением и записью отчёта (at-least-once, ТЗ 3.4): строка
// журнала откатывается, файл возвращается в incoming — конвейер применяет
// его снова, и состояние БД (домены, staging, журнал) сходится к тому же
// итогу. Реальная сходимость против живой БД — предмет этих проверок;
// юнит-уровень (порядок операций, счётчики на фейках) — internal/ingest/
// idempotency_test.go.
package integration

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"crawler/internal/ingest"
	"crawler/internal/model"
)

// domainSnapshot — полное наблюдаемое состояние доменов схемы в
// детерминированном порядке: сравнение снимков «до/после» повторной
// обработки и есть проверка неизменности состояния БД (T-097).
func (e *ingestEnv) domainSnapshot(t *testing.T) []string {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `
		SELECT z.name, d.label, d.status, d.check_state, d.flags,
		       coalesce(d.queued_at::text, '-'),
		       coalesce(d.last_checked_at::text, '-')
		FROM domains AS d JOIN zones AS z ON z.id = d.zone_id
		ORDER BY z.name, d.label`)
	if err != nil {
		t.Fatalf("снимок доменов: %v", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name, label, queued, checked string
		var status, checkState, flags int16
		if err := rows.Scan(&name, &label, &status, &checkState, &flags, &queued, &checked); err != nil {
			t.Fatalf("снимок доменов: чтение: %v", err)
		}
		out = append(out, fmt.Sprintf("%s/%s status=%d check=%d flags=%d queued=%s checked=%s",
			name, label, status, checkState, flags, queued, checked))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("снимок доменов: %v", err)
	}
	return out
}

// dropJournal имитирует сбой в окне между применением и записью отчёта:
// строка ingest_files не зафиксирована (откат), файл вернётся в incoming
// повторной доставкой.
func (e *ingestEnv) dropJournal(t *testing.T) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(), "DELETE FROM ingest_files"); err != nil {
		t.Fatalf("откат журнала ingest_files: %v", err)
	}
}

// assertStateUnchanged — состояние БД после повторной обработки совпадает
// с ожидаемым: домены неизменны, staging пуст, в журнале ровно wantFiles
// строк.
func (e *ingestEnv) assertStateUnchanged(t *testing.T, want []string, wantFiles int) {
	t.Helper()
	if got := e.domainSnapshot(t); !reflect.DeepEqual(got, want) {
		t.Errorf("состояние доменов изменилось:\nполучено:\n%s\nwant:\n%s",
			joinLines(got), joinLines(want))
	}
	if n := e.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage = %d, want 0 (партии покидают staging)", n)
	}
	if n := e.countOf(t, "ingest_files"); n != wantFiles {
		t.Errorf("записей в ingest_files = %d, want %d", n, wantFiles)
	}
}

// joinLines — снимок в многострочном виде для читаемых сообщений.
func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out
}

// TestIngest_ReapplySameFileConverges — главный сценарий T-097: «тот же
// файл обработан дважды — состояние БД неизменно». Сбой после применения
// до записи отчёта → повторная доставка того же содержимого → повторное
// применение. Add сходится через ON CONFLICT (всё «уже было»), delete —
// повторным переводом тех же строк в deleted (счётчики стабильны),
// отчёт в итоге ровно один.
func TestIngest_ReapplySameFileConverges(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	// --- add: первая обработка.
	add := fileContent("example.crawl.test", "shop.crawl.test")
	if out := env.process(t, "crawl.test/add/list.txt", add); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}
	afterAdd := env.domainSnapshot(t)
	if len(afterAdd) != 2 {
		t.Fatalf("доменов после add = %d, want 2", len(afterAdd))
	}

	// Сбой в окне применения/отчёта: журнал пуст, файл доставлен снова.
	env.dropJournal(t)
	out := env.process(t, "crawl.test/add/list.txt", add)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторный add: итог = %q, want applied (дубль по sha256 не находится — журнал пуст)", out.Kind)
	}
	// Повторное применение — no-op для доменов: счётчики отражают
	// текущее состояние (всё «уже было»), не первую попытку.
	if out.Report.Added != 0 || out.Report.Revived != 0 || out.Report.Existed != 2 {
		t.Errorf("повторный add: added %d, revived %d, existed %d; want 0/0/2",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}
	env.assertStateUnchanged(t, afterAdd, 1)

	// --- delete: первая обработка (один домен есть, одного нет).
	del := fileContent("example.crawl.test", "ghost.crawl.test")
	if out := env.process(t, "crawl.test/delete/x.txt", del); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}
	afterDelete := env.domainSnapshot(t)

	// Тот же сбой и повторная доставка: уже удалённый домен остаётся
	// удалённым (UPDATE без охраны по статусу — счётчик «удалено»
	// стабилен), отсутствующий — «не найдено».
	env.dropJournal(t)
	out = env.process(t, "crawl.test/delete/x.txt", del)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторный delete: итог = %q, want applied", out.Kind)
	}
	if out.Report.Deleted != 1 || out.Report.NotFound != 1 {
		t.Errorf("повторный delete: deleted %d, not_found %d; want 1/1 (счётчики стабильны)",
			out.Report.Deleted, out.Report.NotFound)
	}
	env.assertStateUnchanged(t, afterDelete, 1)
}

// TestIngest_ReviveCycleIdempotent — цикл delete → add (возврат) того же
// домена сходится: revive возвращает deleted-домен в new/pending;
// повторный add уже возвращённого — no-op (охрана status = deleted);
// повторное delete возвращает в deleted без изменения остальных колонок.
func TestIngest_ReviveCycleIdempotent(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}

	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("example.crawl.test", "# снятие")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}
	afterDelete := env.domainSnapshot(t)
	d, ok := env.domain(t, "crawl.test", "example")
	if !ok || d.status != int16(model.StatusDeleted) {
		t.Fatalf("после delete: домен = %+v, want status deleted", d)
	}

	// Возврат: revived, new/pending, без lease.
	out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test", "# возврат"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("возврат: итог = %q, want applied", out.Kind)
	}
	if out.Report.Added != 1 || out.Report.Revived != 1 || out.Report.Existed != 0 {
		t.Errorf("возврат: added %d, revived %d, existed %d; want 1/1/0",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}
	d, ok = env.domain(t, "crawl.test", "example")
	if !ok || d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
		t.Fatalf("после возврата: домен = %+v, want new/pending", d)
	}
	if d.queuedAt != nil {
		t.Error("после возврата queued_at не сброшен")
	}
	afterRevive := env.domainSnapshot(t)

	// Повторный add уже возвращённого домена — no-op: состояние то же,
	// счётчики «уже было».
	out = env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test", "# снова"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторный add: итог = %q, want applied", out.Kind)
	}
	if out.Report.Added != 0 || out.Report.Revived != 0 || out.Report.Existed != 1 {
		t.Errorf("повторный add: added %d, revived %d, existed %d; want 0/0/1",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}
	env.assertStateUnchanged(t, afterRevive, 4)

	// Повторное delete — то же состояние, что после первого.
	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("example.crawl.test", "# повторное снятие")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторное delete: итог = %q, want applied", out.Kind)
	}
	env.assertStateUnchanged(t, afterDelete, 5)
}

// TestIngest_SameContentCrossActionApplied — undo тем же списком (T-113;
// переопределяет пиннинг T-097 «граница контракта дедупа»): дедуп
// ослаблен до точной повторной доставки (zone_id, action, sha256) —
// тот же список из delete/ после add/ ТЕПЕРЬ обрабатывается: домены
// удаляются, журнал пополняется строкой delete. Последующая точная
// повторная доставка add-файла тем же содержимым — дубль (skip).
func TestIngest_SameContentCrossActionApplied(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	content := fileContent("example.crawl.test", "shop.crawl.test")
	if out := env.process(t, "crawl.test/add/list.txt", content); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}

	// undo тем же списком: действие другое — точной повторной доставки
	// нет, файл применяется, домены удаляются.
	out := env.process(t, "crawl.test/delete/list.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("тот же список из delete/: итог = %q, want applied (T-113: другое действие — не дубль)", out.Kind)
	}
	if out.Report.Deleted != 2 {
		t.Errorf("undo: deleted = %d, want 2 (оба домена списка)", out.Report.Deleted)
	}
	for _, label := range []string{"example", "shop"} {
		if d, ok := env.domain(t, "crawl.test", label); !ok || d.status != int16(model.StatusDeleted) {
			t.Errorf("после undo: домен %s = %+v, want status deleted", label, d)
		}
	}
	if n := env.countOf(t, "ingest_files"); n != 2 {
		t.Errorf("записей в ingest_files = %d, want 2 (add и undo-delete)", n)
	}

	// Точная повторная доставка add-файла тем же содержимым — дубль:
	// применение не выполняется, журнал не растёт.
	if out := env.process(t, "crawl.test/add/list.txt", content); out.Kind != ingest.OutcomeDuplicate {
		t.Fatalf("повторный add тем же содержимым: итог = %q, want duplicate", out.Kind)
	}
	if n := env.countOf(t, "ingest_files"); n != 2 {
		t.Errorf("записей в ingest_files = %d, want 2 (дубль не отчётится)", n)
	}
}

// TestIngest_CleanupIdempotent — фоновая очистка удалённых доменов
// идемпотентна (T-020, ТЗ 13.1/8): повторный проход не выбирает вычищенных
// и не трогает refcount; уменьшение строго по фактически удалённым
// ссылкам; полный цикл delete → очистка → revive → новые результаты →
// delete → очистка сходится к согласованным refcount.
func TestIngest_CleanupIdempotent(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)
	ctx := context.Background()

	if out := env.process(t, "crawl.test/add/list.txt",
		fileContent("a.crawl.test", "b.crawl.test", "c.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}

	// Идентификаторы доменов для посева результатов.
	domainKey := func(label string) (int16, int64) {
		var zoneID int16
		var id int64
		if err := env.pool.QueryRow(ctx, `
			SELECT d.zone_id, d.id FROM domains AS d
			JOIN zones AS z ON z.id = d.zone_id
			WHERE z.name = $1 AND d.label = $2`, "crawl.test", label,
		).Scan(&zoneID, &id); err != nil {
			t.Fatalf("домен %s: %v", label, err)
		}
		return zoneID, id
	}
	za, a := domainKey("a")
	zb, b := domainKey("b")
	zc, c := domainKey("c")
	if za != zb || zb != zc {
		t.Fatalf("зоны доменов разошлись: %d/%d/%d", za, zb, zc)
	}

	// Контент: h1 держат a, b, c (refcount 3), h2 — только b (refcount 1).
	h1 := make([]byte, 16)
	h1[15] = 0x01
	h2 := make([]byte, 16)
	h2[15] = 0x02
	if _, err := env.pool.Exec(ctx, `
		INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount)
		VALUES ($1, 1, 0, 10, 20, 3), ($2, 1, 16, 5, 5, 1)`, h1, h2); err != nil {
		t.Fatalf("посев contents: %v", err)
	}

	// Результаты: a → h1; b → h1 и h2; c → h1 (живой домен — не удаляем).
	addResult := func(domainID int64, subID int16, hash []byte) {
		if _, err := env.pool.Exec(ctx, `
			INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, checked_at, content_hash)
			VALUES ($1, $2, $3, 2, now(), $4)`, za, domainID, subID, hash); err != nil {
			t.Fatalf("посев результата %d/%d: %v", domainID, subID, err)
		}
	}
	addResult(a, 0, h1)
	addResult(b, 0, h1)
	addResult(b, 1, h2)
	addResult(c, 0, h1)

	// refcounts — наблюдаемое состояние contents.
	refcounts := func() map[string]int {
		rows, err := env.pool.Query(ctx, "SELECT encode(hash, 'hex'), refcount FROM contents ORDER BY hash")
		if err != nil {
			t.Fatalf("чтение refcount: %v", err)
		}
		defer rows.Close()
		out := map[string]int{}
		for rows.Next() {
			var h string
			var n int
			if err := rows.Scan(&h, &n); err != nil {
				t.Fatalf("чтение refcount: %v", err)
			}
			out[h] = n
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("чтение refcount: %v", err)
		}
		return out
	}

	// Удаляем a и b: их результаты (3 строки) обязаны уйти, ссылки
	// списаться; ссылка живого c сохраняется.
	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("a.crawl.test", "b.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}

	first, err := ingest.CleanupOnce(ctx, env.pool, ingest.CleanupOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("первый проход очистки: %v", err)
	}
	if first.Domains != 2 || first.Results != 3 || first.Contents != 2 {
		t.Errorf("первый проход = %+v, want 2 домена, 3 результата, 2 контента", first)
	}
	want := map[string]int{
		fmt.Sprintf("%x", h1): 1, // ссылки a и b списаны, c жива
		fmt.Sprintf("%x", h2): 0, // единственная ссылка (b) списана
	}
	if got := refcounts(); !reflect.DeepEqual(got, want) {
		t.Errorf("refcount после очистки = %v, want %v", got, want)
	}

	// Повторный проход — no-op: вычищенные не выбираются, refcount
	// повторно не уменьшается (идемпотентность, ТЗ 3.4).
	second, err := ingest.CleanupOnce(ctx, env.pool, ingest.CleanupOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("второй проход очистки: %v", err)
	}
	if second != (ingest.CleanupResult{}) {
		t.Errorf("второй проход = %+v, want no-op", second)
	}
	if got := refcounts(); !reflect.DeepEqual(got, want) {
		t.Errorf("refcount после повтора = %v, want прежние %v", got, want)
	}

	// Полный цикл: возврат a, воркер пишет новый результат со ссылкой
	// на h2 (refcount +1), повторное delete и очистка списывают её
	// обратно — счётчики сходятся.
	if out := env.process(t, "crawl.test/add/one.txt",
		fileContent("a.crawl.test", "# возврат")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("возврат: итог = %q, want applied", out.Kind)
	}
	addResult(a, 0, h2)
	if _, err := env.pool.Exec(ctx,
		"UPDATE contents SET refcount = refcount + 1 WHERE hash = $1", h2); err != nil {
		t.Fatalf("инкремент refcount: %v", err)
	}
	if got := refcounts(); got[fmt.Sprintf("%x", h2)] != 1 {
		t.Errorf("refcount h2 после нового результата = %v, want 1", got)
	}

	if out := env.process(t, "crawl.test/delete/y.txt",
		fileContent("a.crawl.test", "# повторное снятие")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("повторное delete: итог = %q, want applied", out.Kind)
	}
	third, err := ingest.CleanupOnce(ctx, env.pool, ingest.CleanupOptions{BatchSize: 100})
	if err != nil {
		t.Fatalf("третий проход очистки: %v", err)
	}
	if third.Domains != 1 || third.Results != 1 {
		t.Errorf("третий проход = %+v, want 1 домен, 1 результат", third)
	}
	want[fmt.Sprintf("%x", h2)] = 0
	if got := refcounts(); !reflect.DeepEqual(got, want) {
		t.Errorf("refcount после цикла = %v, want %v", got, want)
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage = %d, want 0", n)
	}
}
