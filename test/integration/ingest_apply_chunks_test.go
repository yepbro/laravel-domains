// Интеграционные тесты применения add пачками (T-205, ТЗ 5.1/3.5):
// курсор по PK staging режет партию на пачки по ingest.batch_size, всё
// применение — одна транзакция. Проверяется против живого PostgreSQL:
// файл больше нескольких пачек применяется полностью, счётчики зон
// сходятся с прямыми подсчётами по domains, повторное применение той же
// партии (та же — с тем же batch_id и строками) — no-op с нулевыми
// дельтами. Изоляция — как у ingest_test.go: миграции в собственной
// схеме (migratedSchema).
package integration

import (
	"context"
	"fmt"
	"io"
	"testing"

	"crawler/internal/ingest"
	"crawler/internal/model"
)

// TestIngest_ApplyAddChunked — зона-less add-файл (стиль NRD-фида,
// T-205) больше трёх пачек (BatchSize 4) применяется полностью:
// все домены в БД, счётчики каждой зоны сходятся с прямыми подсчётами
// по domains (дельты пачек накоплены и применены одним оператором).
func TestIngest_ApplyAddChunked(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t) // BatchSize 4: COPY и применение — пачками по 4

	// 11 доменов в двух зонах: 3 пачки применения (4 + 4 + 3). Зоны —
	// реальные TLD (com/org): у zone-less файла зона строки — суффикс
	// PSL (idn.Result.Zone), «crawl.test»/«walk.test» дали бы одну зону
	// test с составными метками (проверено Normalize) — как и NRD-фид,
	// файл смешивает зоны в одном списке.
	lines := make([]string, 0, 11)
	for i := 1; i <= 6; i++ {
		lines = append(lines, fmt.Sprintf("chunk%02d.com", i))
	}
	for i := 1; i <= 5; i++ {
		lines = append(lines, fmt.Sprintf("chunk%02d.org", i))
	}

	out := env.process(t, "add/chunks.txt", fileContent(lines...))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("итог = %q, want applied", out.Kind)
	}
	if out.Report.Added != 11 || out.Report.Existed != 0 || out.Report.Revived != 0 {
		t.Errorf("счётчики = added %d, existed %d, revived %d; want 11/0/0 (партия применена полностью)",
			out.Report.Added, out.Report.Existed, out.Report.Revived)
	}
	if n := env.countOf(t, "domains"); n != 11 {
		t.Errorf("доменов = %d, want 11 (ни одна пачка не потеряна)", n)
	}

	// Счётчики зон против прямых подсчётов: дельты пачек (total и pending)
	// обязаны сойтись с реальностью — по одной зоне на строку каждой пачки.
	rows, err := env.pool.Query(context.Background(), `
		SELECT z.name, z.domains_total, z.domains_pending,
		       (SELECT count(*) FROM domains AS d WHERE d.zone_id = z.id),
		       (SELECT count(*) FROM domains AS d
		        WHERE d.zone_id = z.id AND d.check_state = $1)
		FROM zones AS z ORDER BY z.name`,
		int16(model.CheckStatePending))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var seen int
	for rows.Next() {
		var (
			name                              string
			total, pending, totalDir, pendDir int64
		)
		if err := rows.Scan(&name, &total, &pending, &totalDir, &pendDir); err != nil {
			t.Fatal(err)
		}
		seen++
		if total != totalDir || pending != pendDir {
			t.Errorf("зона %s: счётчики total/pending = %d/%d, прямые подсчёты = %d/%d — дельты пачек разошлись",
				name, total, pending, totalDir, pendDir)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Errorf("зон = %d, want 2 (com, org)", seen)
	}

	// Партия покинула staging (применить → очистить, T-017).
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage = %d, want 0", n)
	}
}

// zoneCounters — наблюдаемые счётчики всех зон (сравнение до/после).
func (e *ingestEnv) zoneCounters(t *testing.T) map[int16][2]int64 {
	t.Helper()
	rows, err := e.pool.Query(context.Background(),
		"SELECT id, domains_total, domains_pending FROM zones ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	m := make(map[int16][2]int64)
	for rows.Next() {
		var id int16
		var c [2]int64
		if err := rows.Scan(&id, &c[0], &c[1]); err != nil {
			t.Fatal(err)
		}
		m[id] = c
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return m
}

// TestIngest_ApplyAddReapplyNoop — повторное применение той же партии
// (T-205, ТЗ 3.4): Stage с тем же batch_id затирает и копирует партию
// заново, повторный ApplyAdd упирается в сплошные конфликты — Inserted 0,
// дельты зон нулевые (счётчики не изменились), staging очищен.
func TestIngest_ApplyAddReapplyNoop(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)
	ctx := context.Background()

	// Зоны — как EnsureZone конвейера (T-014).
	store := ingest.NewZoneStore(env.pool)
	zc, err := ingest.EnsureZone(ctx, store, "crawl.test")
	if err != nil {
		t.Fatal(err)
	}
	zw, err := ingest.EnsureZone(ctx, store, "walk.test")
	if err != nil {
		t.Fatal(err)
	}

	// Партия 10 строк в двух зонах пачками по 4 (3 пачки применения).
	id, err := ingest.NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	batch := ingest.Batch{ID: id, Action: model.ActionAdd}
	rows := make([]ingest.Row, 10)
	for i := range rows {
		zone := zc.ID
		if i >= 6 {
			zone = zw.ID
		}
		rows[i] = ingest.Row{Label: fmt.Sprintf("row%02d", i+1), ZoneID: zone, LineNo: int64(i + 1)}
	}
	stage := func() (int64, error) {
		i := 0
		return ingest.Stage(ctx, env.pool, batch, 4, func() (ingest.Row, error) {
			if i == len(rows) {
				return ingest.Row{}, io.EOF
			}
			r := rows[i]
			i++
			return r, nil
		})
	}

	staged, err := stage()
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if staged != 10 {
		t.Fatalf("загружено строк = %d, want 10", staged)
	}
	res, err := ingest.ApplyAdd(ctx, env.pool, batch, 4)
	if err != nil {
		t.Fatalf("ApplyAdd: %v", err)
	}
	if res.Inserted != 10 || res.Revived != 0 || res.Existing != 0 {
		t.Fatalf("первое применение = %+v, want Inserted 10 (3 пачки)", res)
	}
	before := env.zoneCounters(t)

	// Повторная загрузка той же партии тем же batch_id (перезапись
	// staging) и повторное применение: всё уже в БД.
	if _, err := stage(); err != nil {
		t.Fatalf("Stage повторно: %v", err)
	}
	res2, err := ingest.ApplyAdd(ctx, env.pool, batch, 4)
	if err != nil {
		t.Fatalf("ApplyAdd повторно: %v", err)
	}
	if res2.Inserted != 0 || res2.Revived != 0 || res2.Existing != 10 {
		t.Errorf("повторное применение = %+v, want no-op: Inserted/Revived 0, Existing 10", res2)
	}
	after := env.zoneCounters(t)
	if len(after) != len(before) {
		t.Fatalf("зон = %d после повтора, было %d", len(after), len(before))
	}
	for id, c := range before {
		if after[id] != c {
			t.Errorf("зона %d: счётчики изменились %v → %v — повтор обязан быть no-op (нулевые дельты)", id, c, after[id])
		}
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage = %d, want 0 (партия очищена)", n)
	}
	if n := env.countOf(t, "domains"); n != 10 {
		t.Errorf("доменов = %d, want 10 (повтор не дублирует)", n)
	}
}
