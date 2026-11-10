// Второй раунд триажа мутантов (догонка прогона monitor 0.901): прямые
// тесты reportDrift и машин на фактических границах, VerifyCounters на
// фейковом пуле (юнит-уровень: ошибки каждого шага сверки, флаги Close
// строк, допуски), пины нулей на каждой ошибочной ветке читателей
// реконсиляции, ошибки источников с исходным текстом (psi/redis/
// renderdebt/unitevents), readMemInfo с посторонними строками, границы
// withDefaults в единицах (1нс — рабочее значение), точные значения
// парсеров на base/bitSize-инъекциях и полный диапазон согласования
// числительных.
package monitor

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// --- reportDrift: прямые вызовы (без гонок цикла) ----------------------------

// TestReportDriftDirect — первый проход молчит; стационар без дрейфа
// молчит; N позиций дрейфа — Warn с точной суммой и ровно пятью строками
// топа (шестая и дальше — счётчиком позиций, не строками).
func TestReportDriftDirect(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := zsLogger(slogBuf)
	reg := metrics.New("monitor")
	m := newZoneStatsLoopMetrics(reg)
	zero := []CounterPosition{{Counter: "zone:7:total", Old: 5, New: 5}}

	// Первый проход (fillFirst) — полная тишина даже с дрейфом.
	reportDrift(logger, m, ZoneStatsResult{Positions: []CounterPosition{
		{Counter: "x", Old: 1, New: 2}}}, true)
	if log := slogBuf.String(); log != "" {
		t.Fatalf("fillFirst пишет лог:\n%s", log)
	}
	// Стационар (дрейфа нет) — тишина.
	reportDrift(logger, m, ZoneStatsResult{Positions: zero}, false)
	if log := slogBuf.String(); log != "" {
		t.Fatalf("стационар без дрейфа пишет Warn:\n%s", log)
	}
	// Потоковые позиции (daily/retry/hourly) не поднимают Warn — но
	// пишутся в gauge.
	reportDrift(logger, m, ZoneStatsResult{Positions: []CounterPosition{
		{Counter: dailyChecksCounter, Old: 7, New: 9}}}, false)
	if log := slogBuf.String(); strings.Contains(log, "level=WARN") {
		t.Fatalf("потоковая позиция в Warn:\n%s", log)
	}
	if v := driftGaugeValue(t, reg, dailyChecksCounter); v != 2 {
		t.Errorf("gauge потоковой позиции = %v, want 2", v)
	}

	// Семь дрейфующих позиций по 2: Warn с sum_abs=14 и ровно пятью
	// строками топа.
	slogBuf = &zonestatsSyncBuf{}
	logger = zsLogger(slogBuf)
	positions := make([]CounterPosition, 7)
	for i := range positions {
		positions[i] = CounterPosition{Counter: "zone:" + strconv.Itoa(i) + ":total", Old: 0, New: 2}
	}
	reportDrift(logger, m, ZoneStatsResult{Positions: positions}, false)
	log := slogBuf.String()
	if !strings.Contains(log, "реконсиляция: дрейф") {
		t.Fatalf("Warn дрейфа не поднялся:\n%s", log)
	}
	if !strings.Contains(log, "sum_abs=14") {
		t.Errorf("Warn без точной суммы (want sum_abs=14):\n%s", log)
	}
	if !strings.Contains(log, "positions=7") {
		t.Errorf("Warn без счётчика позиций (want positions=7):\n%s", log)
	}
	if n := strings.Count(log, " → "); n != 5 {
		t.Errorf("строк топа = %d, want ровно 5:\n%s", n, log)
	}
	if strings.Contains(log, "zone:6:total") || strings.Contains(log, "zone:5:total") {
		t.Errorf("топ длиннее пяти строк:\n%s", log)
	}
}

// --- VerifyCounters: юнит-уровень на фейковом пуле -----------------------------

// verifyRow — строка verifyZonesSQL: id + 5 пар (сохранено, живой агрегат).
func verifyRow(id int16, stored, live [5]int64) []any {
	row := []any{id}
	for i := range stored {
		row = append(row, stored[i], live[i])
	}
	return row
}

// verifySteps — шаги одного прохода сверки: зоны (verifyZonesSQL),
// история (historyAggSQL), агрегат (recountAggQuerySQL), сохранённые
// счётчики (storedCountersSQL).
func verifySteps(zones, agg, history, stored *zonestatsFakeRows) []zonestatsStep {
	// Порядок запросов сверки: зоны → per-zone агрегат → история →
	// сохранённые счётчики.
	return []zonestatsStep{{rows: zones}, {rows: agg}, {rows: history}, {rows: stored}}
}

func TestVerifyCountersCleanAndDiff(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 5
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 2)
	// Совпадают — пустой результат.
	pool := &zonestatsFakePool{steps: verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{10, 4, 2, 3, 1}, [5]int64{10, 4, 2, 3, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 2), counterRowOf("status_alive", 5)),
	)}
	out, err := VerifyCounters(context.Background(), pool, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("расхождения при совпадении: %+v", out)
	}
	// Все наборы строк закрыты (Close обязан вызываться).
	for i, step := range pool.steps {
		if !step.rows.closed {
			t.Errorf("строки шага %d не закрыты Close", i)
		}
	}

	// Расхождение зоны и глобального счётчика — позиции с Old/New.
	pool2 := &zonestatsFakePool{steps: verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{10, 4, 0, 3, 1}, [5]int64{10, 6, 2, 3, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(1), int64(2)}),
		zonestatsRows(counterRowOf("render_pending", 5)),
	)}
	out, err = VerifyCounters(context.Background(), pool2, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	byName := map[string]CounterPosition{}
	for _, p := range out {
		byName[p.Counter] = p
	}
	if p := byName["zone:7:pending"]; p.Old != 4 || p.New != 6 {
		t.Errorf("zone:7:pending = %+v, want old=4 new=6", p)
	}
	if p := byName["zone:7:queued"]; p.Old != 0 || p.New != 2 {
		t.Errorf("zone:7:queued = %+v, want old=0 new=2", p)
	}
	if p := byName["global:laravel_appeared_total"]; p.Old != 0 || p.New != 1 {
		t.Errorf("кумулятив appeared = %+v, want old=0 (строки нет) new=1", p)
	}
	if p := byName["global:laravel_lost_total"]; p.Old != 0 || p.New != 2 {
		t.Errorf("кумулятив lost = %+v, want old=0 new=2", p)
	}
	if p := byName["global:render_pending"]; p.Old != 5 || p.New != 2 {
		t.Errorf("render_pending = %+v, want old=5 new=2", p)
	}

	// Допуск: расхождение в пределах Tolerance не наблюдается.
	pool3 := &zonestatsFakePool{steps: verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{10, 4, 2, 3, 1}, [5]int64{10, 4, 2, 3, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 3)),
	)}
	out, _ = VerifyCounters(context.Background(), pool3, VerifyOptions{Tolerance: 1})
	if len(out) != 0 {
		t.Errorf("расхождение в допуске наблюдается: %+v", out)
	}
}

func TestVerifyCountersErrors(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 0)
	hist := zonestatsRows([]any{int64(0), int64(0)})
	stored := zonestatsRows()
	// Ошибка каждого шага — с контекстом шага.
	for _, tc := range []struct {
		name string
		step int
		err  error
		want string
	}{
		{"зоны", 0, errors.New("бд недоступна"), "сверка счётчиков: зоны"},
		{"агрегат", 1, errors.New("бд недоступна"), "агрегат"},
		{"история", 2, errors.New("бд недоступна"), "история"},
		{"сохранённые", 3, errors.New("бд недоступна"), "сохранённые"},
	} {
		steps := verifySteps(
			zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
			zonestatsRows(aggRow), hist, stored)
		steps[tc.step].err = tc.err
		if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil ||
			!strings.Contains(err.Error(), tc.want) ||
			!strings.Contains(err.Error(), "бд недоступна") {
			t.Errorf("%s: %v, want контекст %q с исходной", tc.name, err, tc.want)
		}
	}
	// Пустой ответ истории — обрыв протокола.
	steps := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow), zonestatsRows(), stored)
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пустой ответ") {
		t.Errorf("пустая история = %v, want обрыв протокола", err)
	}
	// Кривые строки (Scan-ошибки) каждого набора — с контекстом «чтение»
	// (свежие наборы на итерацию: итератор строк одноразовый).
	narrow := [][]any{{int16(7)}, {int64(1)}, {int64(9)}, {"имя"}}
	for i, row := range narrow {
		steps := verifySteps(
			zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
			zonestatsRows(aggRow),
			zonestatsRows([]any{int64(0), int64(0)}),
			zonestatsRows(counterRowOf("render_pending", 0)))
		steps[i].rows = zonestatsRows(row)
		if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil ||
			!strings.Contains(err.Error(), "чтение") {
			t.Errorf("шаг %d: %v, want контекст чтения", i, err)
		}
	}
	// rows.Err после строк зон/агрегата/истории/сохранённых — с исходной
	// (свежие наборы на итерацию).
	for i := 0; i < 4; i++ {
		steps := verifySteps(
			zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
			zonestatsRows(aggRow),
			zonestatsRows([]any{int64(0), int64(0)}),
			zonestatsRows(counterRowOf("render_pending", 0)))
		steps[i].rows = &zonestatsFakeRows{rows: steps[i].rows.rows, err: errors.New("обрыв")}
		_, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{})
		if err == nil || !strings.Contains(err.Error(), "обрыв") {
			t.Errorf("rows.Err шага %d = %v, want исходная", i, err)
		}
		// Строки закрыты даже на ошибке итерации.
		if !steps[i].rows.closed && i != 0 {
			t.Errorf("шаг %d: строки не закрыты", i)
		}
	}
}

// --- читатели реконсиляции: пины нулей каждой ошибочной ветки -------------------

// TestZoneStatsReaderErrorPins — каждый читатель на каждой ошибке
// возвращает нулевые значения (пин), ошибки — с исходной причиной.
func TestZoneStatsReaderErrorPins(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("бум")

	// readHistoryAgg: пустой ответ, Scan, rows.Err — нули.
	p := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	tx, _ := p.Begin(ctx)
	if a, l, err := readHistoryAgg(ctx, tx); err == nil || a != 0 || l != 0 ||
		!strings.Contains(err.Error(), "пустой ответ") {
		t.Errorf("история/пусто: (%v/%v, %v)", a, l, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: &zonestatsFakeRows{
		rows: [][]any{{int64(1)}}, scanErr: boom}}}}
	tx, _ = p.Begin(ctx)
	if a, l, err := readHistoryAgg(ctx, tx); err == nil || a != 0 || l != 0 ||
		!strings.Contains(err.Error(), "чтение") {
		t.Errorf("история/Scan: (%v/%v, %v)", a, l, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: &zonestatsFakeRows{
		rows: [][]any{{int64(1), int64(2)}}, err: boom}}}}
	tx, _ = p.Begin(ctx)
	if a, l, err := readHistoryAgg(ctx, tx); err == nil || a != 0 || l != 0 {
		t.Errorf("история/rows.Err: (%v/%v, %v)", a, l, err)
	}

	// readDailyValue: rowsErr до строки, пусто-ок (ноль/false), Scan,
	// rowsErr после строки.
	for _, tc := range []struct {
		rows  *zonestatsFakeRows
		wantV int64
		wantE bool
	}{
		{&zonestatsFakeRows{err: boom}, 0, true},
		{zonestatsRows(), 0, false},
		{&zonestatsFakeRows{rows: [][]any{{int64(7)}}, scanErr: boom}, 0, true},
		{&zonestatsFakeRows{rows: [][]any{{int64(7)}}, err: boom}, 0, true},
	} {
		p := &zonestatsFakePool{steps: []zonestatsStep{{rows: tc.rows}}}
		tx, _ := p.Begin(ctx)
		v, exists, err := readDailyValue(ctx, tx, readDailyChecksSQL, "суточная")
		if tc.wantE && (err == nil || v != 0 || exists) {
			t.Errorf("readDailyValue %+v: (%v/%v, %v), want 0/false и ошибка", tc.rows, v, exists, err)
		}
		if !tc.wantE && (err != nil || v != 0 || exists) {
			t.Errorf("readDailyValue пусто: (%v/%v, %v), want 0/false без ошибки", v, exists, err)
		}
	}

	// writeDailyCounter: пустой ответ upsert, Scan, rows.Err — ноль.
	dailyRows := []*zonestatsFakeRows{
		zonestatsRows(),
		{rows: [][]any{{int64(1), int64(2)}}, scanErr: boom},
		{rows: [][]any{{int64(1)}}, err: boom},
	}
	for _, rows := range dailyRows {
		p := &zonestatsFakePool{steps: []zonestatsStep{{rows: rows}}}
		tx, _ := p.Begin(ctx)
		if v, err := writeDailyCounter(ctx, tx, recountDailyChecksSQL, 5, time.Unix(0, 0), "суточная"); err == nil || v != 0 {
			t.Errorf("writeDailyCounter %+v: (%v, %v), want 0 и ошибка", rows, v, err)
		}
	}

	// cleanupBaskets: Scan и rows.Err — ошибки с контекстом.
	cleanupRows := []*zonestatsFakeRows{
		{rows: [][]any{{int64(1), int64(2)}}, scanErr: boom},
		{rows: [][]any{{int64(1)}}, err: boom},
	}
	for _, rows := range cleanupRows {
		p := &zonestatsFakePool{steps: []zonestatsStep{{rows: rows}}}
		tx, _ := p.Begin(ctx)
		if err := cleanupBaskets(ctx, tx, retryMatureCleanupSQL, "очистка корзин ретрая"); err == nil ||
			!strings.Contains(err.Error(), "очистка корзин ретрая") {
			t.Errorf("cleanupBaskets %+v: %v, want контекст", rows, err)
		}
	}

	// readZonePositions/readCounterValues/readZoneAgg/readLaravelAgg:
	// Query-ошибка и rows.Err — nil-значения с исходной причиной.
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZonePositions(ctx, tx, nil); err == nil || m != nil {
		t.Errorf("чтение зон/Query: (%v, %v), want nil и ошибка", m, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readCounterValues(ctx, tx, CounterNames()); err == nil || m != nil {
		t.Errorf("чтение счётчиков/Query: (%v, %v), want nil и ошибка", m, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZoneAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("агрегат/Query: (%v, %v), want nil и ошибка", m, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readLaravelAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("laravel-агрегат/Query: (%v, %v), want nil и ошибка", m, err)
	}
	// rows.Err каждого — ошибка.
	rowErr := func(rows [][]any) *zonestatsFakeRows {
		return &zonestatsFakeRows{rows: rows, err: boom}
	}
	zRow := rowErr([][]any{zoneValsOf(7, 1, 1, 1, 1, 1)})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: zRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZonePositions(ctx, tx, nil); err == nil || m != nil {
		t.Errorf("чтение зон/rows.Err: (%v, %v)", m, err)
	}
	cRow := rowErr([][]any{counterRowOf("status_alive", 1)})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: cRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readCounterValues(ctx, tx, CounterNames()); err == nil || m != nil {
		t.Errorf("чтение счётчиков/rows.Err: (%v, %v)", m, err)
	}
	statuses := make([]int64, len(statusAggCols))
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 0)
	aRow := rowErr([][]any{aggRow})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: aRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZoneAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("агрегат/rows.Err: (%v, %v)", m, err)
	}
	lRow := rowErr([][]any{{int16(7), int64(1)}})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: lRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readLaravelAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("laravel-агрегат/rows.Err: (%v, %v)", m, err)
	}
}

// --- load: точные значения и пропущенные границы --------------------------------

// TestReadMemInfoForeignLines — посторонние строки meminfo (без двоеточия,
// пустое значение) пропускаются, не обрывают разбор; значения различимы
// по основанию (19kB ≠ base-9).
func TestReadMemInfoForeignLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "meminfo")
	content := "посторонняя строка без двоеточия\n" +
		"MemTotal: 19 kB\n" +
		"ПустоеЗначение:\n" +
		"MemAvailable: 7 kB\n"
	if err := writeFileForTest(p, content); err != nil {
		t.Fatal(err)
	}
	tt, a, st, sf, err := readMemInfo(p)
	if err != nil || tt != 19*1024 || a != 7*1024 || st != 0 || sf != 0 {
		t.Errorf("посторонние строки: (%v/%v/%v/%v, %v), want 19456/7168/0/0",
			tt, a, st, sf, err)
	}
	// Значение 2^63 КиБ в постороннем поле — валидно для uint64
	// (мутант bitSize 63 обязан принять).
	if err := writeFileForTest(p, "MemTotal: 19 kB\nMemAvailable: 7 kB\nHuge: 9223372036854775808 kB\n"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := readMemInfo(p); err != nil {
		t.Errorf("постороннее поле 2^63: %v, want валидно (uint64)", err)
	}
	// Нет файла — ошибка с нулями (пин).
	tt, a, st, sf, err = readMemInfo(filepath.Join(dir, "нет"))
	if err == nil || tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("нет файла: (%v/%v/%v/%v, %v), want нули и ошибка", tt, a, st, sf, err)
	}
}

// writeFileForTest — файл фикстуры (os.WriteFile обёртка).
func writeFileForTest(p, content string) error {
	return os.WriteFile(p, []byte(content), 0o644)
}

// TestReadLoadAvgMissingFilePins — отсутствующий файл: нули (пин значений
// на ошибке чтения).
func TestReadLoadAvgMissingFilePins(t *testing.T) {
	l1, l5, l15, err := readLoadAvg(filepath.Join(t.TempDir(), "нет"))
	if err == nil || l1 != 0 || l5 != 0 || l15 != 0 {
		t.Errorf("нет файла: (%v/%v/%v, %v), want нули и ошибка", l1, l5, l15, err)
	}
}

// TestReadProcsMissingRoot — отсутствующий корень: ошибка с контекстом
// «процессы» (не пустой снимок).
func TestReadProcsMissingRoot(t *testing.T) {
	if m, err := readProcs(filepath.Join(t.TempDir(), "нет")); err == nil ||
		!strings.Contains(err.Error(), "процессы") {
		t.Errorf("нет корня: (%v, %v), want ошибка «процессы»", m, err)
	}
}

// TestReadCPUStatMissingFileContext — отсутствующий файл: ошибка с
// контекстом «/proc/stat» (не ошибка «без строки cpu»).
func TestReadCPUStatMissingFileContext(t *testing.T) {
	if _, err := readCPUStat(filepath.Join(t.TempDir(), "нет")); err == nil ||
		!strings.Contains(err.Error(), "/proc/stat") {
		t.Errorf("нет файла: %v, want контекст /proc/stat", err)
	}
}

// TestParseProcStatLineLeadingParen — имя с самого начала строки
// (open == 0) — валидная строка, не пустой вклад.
func TestParseProcStatLineLeadingParen(t *testing.T) {
	line := "(weird) R 1 2 3 4 5 6 7 8 9 10 3 4"
	if comm, ticks := parseProcStatLine([]byte(line)); comm != "weird" || ticks != 7 {
		t.Errorf("open==0: (%q, %d), want weird и 7", comm, ticks)
	}
	// open < 0 → false: строки без открывающей скобки — пустой вклад.
	if comm, _ := parseProcStatLine([]byte("7 no paren R 1 2 3 4 5 6 7 8 9 10 3 4")); comm != "" {
		t.Errorf("без скобки = %q, want пустой вклад", comm)
	}
}

// --- slowqueries: границы единиц и полный диапазон числительных ------------------

// TestSlowQueriesPluralFullRange — согласование числительных по диапазону
// 0..3000 (мутанты n%99/n%101 различимы на n > 1300).
func TestSlowQueriesPluralFullRange(t *testing.T) {
	plural := func(n int) string {
		switch {
		case n%10 == 1 && n%100 != 11:
			return "медленный запрос"
		case n%10 >= 2 && n%10 <= 4 && (n%100 < 10 || n%100 >= 20):
			return "медленных запроса"
		}
		return "медленных запросов"
	}
	for n := 0; n <= 3000; n++ {
		if got := slowQueriesPhrase(n); got != plural(n) {
			t.Fatalf("slowQueriesPhrase(%d) = %q, want %q", n, got, plural(n))
		}
	}
}

// TestSlowQueriesUnitBoundaries — единичные значения рабочих параметров
// не заменяются дефолтами (границы «≤ 0»).
func TestSlowQueriesUnitBoundaries(t *testing.T) {
	if got := (SlowQueryOptions{MinDuration: 1}).withDefaults().MinDuration; got != 1 {
		t.Errorf("MinDuration=1: %v, want как задан", got)
	}
	pool := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	if _, err := (PGSlowQueries{Pool: pool, Limit: 1}).SlowQueries(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if got := pool.calls()[0].args[2]; got != 1 {
		t.Errorf("Limit=1: %v, want 1", got)
	}
}

// TestSlowQueryLoopStartupAndMetrics — стартовый проход выполняется сразу
// (до первого тика) и пишет метрики; интервал 1нс — рабочее значение.
func TestSlowQueryLoopStartupAndMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	q := slowQueryAt(11, "scheduler", 42.3, t0, "SELECT 1")
	src := &fakeSlowSource{passes: [][]SlowQuery{{q}}}
	var reports []string
	opts := SlowQueryOptions{
		Interval: time.Hour, // тик не наступит: только стартовый проход
		Metrics:  reg,
		Notify:   func(text string) { reports = append(reports, text) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, 1, cancel, done)
	if len(reports) != 1 {
		t.Fatalf("отчётов = %d, want 1 (стартовый проход до тика)", len(reports))
	}
	// Шапка завершается переводом строки (строки запросов с новой строки).
	if !strings.Contains(reports[0], "(порог 3с)\n") {
		t.Errorf("шапка без перевода строки:\n%q", reports[0])
	}
	// Метрики стартового прохода: gauge и счётчик.
	if v, ok := gaugeLabels(t, reg, "crawler_pg_slow_queries", nil); !ok || v != 1 {
		t.Errorf("crawler_pg_slow_queries = (%v, %v), want 1", v, ok)
	}
	if v, ok := slowCounterValue(t, reg, "crawler_pg_slow_queries_total", map[string]string{"app": "scheduler"}); !ok || v != 1 {
		t.Errorf("total{app=scheduler} = (%v, %v), want 1", v, ok)
	}

	// Интервал 1нс — рабочий (не «выключен»): цикл живёт и повторяет
	// проходы, отчёт стартового прохода приходит и при Interval=1нс.
	src2 := &fakeSlowSource{passes: [][]SlowQuery{{q}}}
	var reports2 []string
	opts2 := SlowQueryOptions{
		Interval: 1,
		Notify:   func(text string) { reports2 = append(reports2, text) },
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- SlowQueryLoop(ctx2, src2, opts2) }()
	waitSlowPasses(t, src2, 1, cancel2, done2)
	if len(reports2) != 1 {
		t.Errorf("Interval=1нс: отчётов = %d, want 1 (не выключен)", len(reports2))
	}
}

// TestPGSlowQueriesRows — строки снимка читаются в результат (не глотаются).
func TestPGSlowQueriesRows(t *testing.T) {
	pool := &zonestatsFakePool{steps: []zonestatsStep{
		{rows: zonestatsRows(slowScanRow(11, "scheduler", 42.3, "SELECT 1"),
			slowScanRow(12, "", 7.8, ""))}}}
	out, err := (PGSlowQueries{Pool: pool}).SlowQueries(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].PID != 11 || out[1].App != "" || out[1].Query != "" {
		t.Errorf("снимок = %+v, want обе строки как есть", out)
	}
}

// --- psi/redis/renderdebt/unitevents: исходный текст ошибок ----------------------

// TestOSPSISourceReadErrorText — каталог вместо файла: ошибка несёт
// исходный текст чтения (не превращается в ошибку формата PSI).
func TestOSPSISourceReadErrorText(t *testing.T) {
	dir := t.TempDir()
	_, err := (OSPSI{Path: dir}).PSI(context.Background())
	if err == nil || !strings.Contains(err.Error(), "PSI: ") ||
		strings.Contains(err.Error(), "строка") {
		t.Errorf("каталог: %v, want исходная ошибка чтения (не формат PSI)", err)
	}
}

// TestParsePSIMissingFieldKeepsError — отсутствующее поле — ошибка
// формата (не тихий нулевой снимок).
func TestParsePSIMissingFieldKeepsError(t *testing.T) {
	if st, err := parsePSI("some avg60=1 avg300=1 total=0\n"); err == nil || st != (PSIStat{}) {
		t.Errorf("нет avg10: (%+v, %v), want нули и ошибка", st, err)
	}
	if st, err := parsePSI("some avg10=1 total=0\n"); err == nil || st != (PSIStat{}) {
		t.Errorf("нет avg60: (%+v, %v), want нули и ошибка", st, err)
	}
}

// TestInfoSecondsMissingField — отсутствующее поле INFO — ошибка (не
// тихий ноль).
func TestInfoSecondsMissingField(t *testing.T) {
	if v, err := infoSeconds(map[string]string{"другое": "1"}, "uptime_in_seconds"); err == nil || v != 0 {
		t.Errorf("нет поля: (%v, %v), want 0 и ошибка", v, err)
	}
}

// TestInfoBytesMissingFieldAndBitSize — отсутствующее поле и граница
// bitSize (2^62 валидна для int64/64, отвергается 63).
func TestInfoBytesMissingFieldAndBitSize(t *testing.T) {
	if v, err := infoBytes(map[string]string{}, "maxmemory"); err == nil || v != 0 {
		t.Errorf("нет поля: (%v, %v), want 0 и ошибка", v, err)
	}
	if v, err := infoBytes(map[string]string{"k": "4611686018427387904"}, "k"); err != nil || v != 4611686018427387904 {
		t.Errorf("2^62: (%v, %v), want как есть", v, err)
	}
	if v, err := infoSeconds(map[string]string{"k": "4611686018427387904"}, "k"); err != nil || v != 4611686018427387904 {
		t.Errorf("2^62 сек: (%v, %v), want как есть", v, err)
	}
}

// TestRedisUptimeSourceError — ошибка чтения INFO — с исходным текстом.
func TestRedisUptimeSourceError(t *testing.T) {
	if _, err := redisUptimeOf(context.Background(), errInfoSource{}); err == nil ||
		!strings.Contains(err.Error(), "connection refused") {
		t.Errorf("ошибка INFO: %v, want исходная", err)
	}
}

// errInfoSource — источник INFO с заготовленной ошибкой.
type errInfoSource struct{}

func (errInfoSource) Info(context.Context) (string, error) {
	return "", errors.New("connection refused")
}

// redisUptimeOf — обёртка чтения аптайма (сборка из INFO).
func redisUptimeOf(ctx context.Context, src interface {
	Info(context.Context) (string, error)
}) (RedisUptime, error) {
	raw, err := src.Info(ctx)
	if err != nil {
		return RedisUptime{}, err
	}
	fields := parseRedisInfo(raw)
	secs, err := infoSeconds(fields, "uptime_in_seconds")
	if err != nil {
		return RedisUptime{}, err
	}
	return RedisUptime{Seconds: secs}, nil
}

// parseRedisInfo — разбор INFO в карту полей (локальная копия для теста).
func parseRedisInfo(raw string) map[string]string {
	fields := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return fields
}

// TestRenderDebtOfErrors — ошибки источника с исходным текстом: пул не
// задан, Redis не задан, ошибка счётчика.
func TestRenderDebtOfErrors(t *testing.T) {
	if _, err := renderDebtOf(context.Background(), nil, nil); err == nil ||
		!strings.Contains(err.Error(), "пул БД не задан") {
		t.Errorf("пул: %v, want «пул БД не задан»", err)
	}
	pool := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
	if _, err := renderDebtOf(context.Background(), pool, &renderLensSource{}); err == nil ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("счётчик: %v, want исходную", err)
	}
}

// TestRenderDebtLoopUnitIntervalAndGauge — интервал 1нс — рабочее
// значение (стартовый проход выполняется и пишет gauge долга; цикл
// живёт до отмены).
func TestRenderDebtLoopUnitIntervalAndGauge(t *testing.T) {
	src := &fakeDebtSource{debt: RenderDebt{Flags: 3, Lag: 1}}
	reg := metrics.New("monitor")
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- RenderDebtLoop(ctx, src, RenderDebtOptions{Interval: 1, Metrics: reg})
	}()
	// Стартовый проход обязан выполниться до отмены: ждём gauge.
	deadline := time.Now().Add(2 * time.Second)
	var v float64
	var ok bool
	for time.Now().Before(deadline) {
		if v, ok = gaugeLabels(t, reg, "crawler_render_pending_domains", nil); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	if !ok || v != 3 {
		t.Errorf("gauge рендер-долга = (%v, %v), want 3 (стартовый проход при Interval=1нс)", v, ok)
	}
}

// --- unitevents: границы ------------------------------------------------------------

// TestParseSystemctlShowNRestartsBoundaries — NRestarts до первого Id
// пропускается (не обрывает разбор), значения в десятой системе.
func TestParseSystemctlShowNRestartsBoundaries(t *testing.T) {
	out := "NRestarts=99\n" +
		"Id=a.service\nNRestarts=123\n" +
		"NRestarts=77\n" +
		"Id=b.service\nNRestarts=9223372036854775808\n"
	units := parseSystemctlShow(out)
	if len(units) != 2 {
		t.Fatalf("юнитов = %d, want 2 (строка до Id не обрывает разбор)", len(units))
	}
	if units[0].NRestarts != 77 {
		t.Errorf("NRestarts = %d, want 77 (последняя строка блока)", units[0].NRestarts)
	}
	if units[1].NRestarts != 9223372036854775808 {
		t.Errorf("NRestarts 2^63 = %d, want как есть (uint64)", units[1].NRestarts)
	}
}

// TestParseMemoryEventsForeignLine — строка без значения не обрывает
// разбор (continue), ключи после неё читаются.
func TestParseMemoryEventsForeignLine(t *testing.T) {
	ev, err := parseMemoryEvents("low\noom 4\n")
	if err != nil || ev.OOM != 4 {
		t.Errorf("строка без значения: (%+v, %v), want oom=4", ev, err)
	}
}

// TestOSUnitEventsCancelledCtx — отменённый ctx: ошибка контекста с
// исходной (не пустой снимок).
func TestOSUnitEventsCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if units, err := (OSUnitEvents{}).UnitEvents(ctx); err == nil || units != nil {
		t.Errorf("отмена ctx: (%v, %v), want nil и ошибка", units, err)
	}
}

// --- streamgroups: несколько стримов ------------------------------------------------

// TestStreamGroupsSkipEmptyThenRead — пустой стрим пропускается, Straße
// за ним читается (continue, не break).
func TestStreamGroupsSkipEmptyThenRead(t *testing.T) {
	group := redis.XInfoGroup{Name: "g", Lag: 3, Pending: 1, EntriesRead: 10}
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			alertStreams[0]: {},      // пустой — пропускается
			alertStreams[1]: {group}, // читается после пустого
			queue.StreamDNS: {},
		},
		xlen: map[string]int64{alertStreams[1]: 5},
	}
	st, err := (RedisStreamGroups{RDB: rdb}).StreamGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 1 || st[0].Stream != alertStreams[1] {
		t.Errorf("снимок = %+v, want только группы второго стрима", st)
	}
}

// --- alertstate: недостающие границы --------------------------------------------------

// TestNewDoneStaleInvalidSnapshots — невалидные метки снапшотов: первый
// проход инкрементирует серию (нет базы), повтор той же невалидной
// метки — новый снапшот (недействительная метка не совпадает с прошлой
// валидной).
func TestNewDoneStaleInvalidSnapshots(t *testing.T) {
	invalid := staleRow{value: 5} // snap невалиден
	// Первый проход — серия растёт даже без валидной метки.
	tr := &newDoneStaleTrack{minPasses: 2}
	if s := tr.decide("", time.Time{}, invalid); !strings.Contains(s.detail, "1/2") {
		t.Errorf("первый невалидный снапшот: %s, want серию 1/2", s.detail)
	}
	// Повтор невалидного — тоже рост: невалидная метка не считается
	// совпадением с прошлой (!Valid).
	if s := tr.decide("", time.Time{}, invalid); !strings.Contains(s.detail, "2/2") {
		t.Errorf("повтор невалидного: %s, want серию 2/2 (невалидная метка ≠ любой)", s.detail)
	}
	// Валидная метка, равная времени ноль: первый проход — рост
	// (нет базы — серия инкрементируется).
	zero := staleRow{value: 5, snap: sql.NullTime{Time: time.Time{}, Valid: true}}
	tr2 := &newDoneStaleTrack{minPasses: 2}
	if s := tr2.decide("", time.Time{}, zero); !strings.Contains(s.detail, "1/2") {
		t.Errorf("валидный zero-time снапшот: %s, want серию 1/2", s.detail)
	}
}

// TestCounterGrowthFirstPassBase — первый проход счётчика — база
// (серия 0), не рост.
func TestCounterGrowthFirstPassBase(t *testing.T) {
	tr := &counterGrowthTrack{minPasses: 2}
	s := tr.decide("", time.Time{}, 100)
	if !strings.Contains(s.detail, "0/2") {
		t.Errorf("база счётчика: %s, want серию 0/2", s.detail)
	}
	s = tr.decide("", time.Time{}, 150)
	if !strings.Contains(s.detail, "1/2") {
		t.Errorf("рост счётчика: %s, want серию 1/2", s.detail)
	}
}

// TestRenderDebtTrackThresholdExact — долг ровно у порога с серией
// роста — НЕ срабатывание (строгое >); долг ниже порога с серией — ок.
func TestRenderDebtTrackThresholdExact(t *testing.T) {
	d := func(total int64) RenderDebt { return RenderDebt{Flags: total} }
	// Ровно порог: 5 → 10 (серия 2), total == domains → ok.
	tr := &renderDebtTrack{domains: 10, passes: 2}
	_ = tr.decide("", time.Time{}, d(5))
	_ = tr.decide("", time.Time{}, d(8))
	if s := tr.decide("", time.Time{}, d(10)); s.status != AlertStatusOK {
		t.Errorf("долг ровно у порога: %s, want ok (строгое >)", s.status)
	}
	// Ниже порога с полной серией — ok (мутант «true» срабатывал бы).
	tr2 := &renderDebtTrack{domains: 10, passes: 2}
	_ = tr2.decide("", time.Time{}, d(6))
	_ = tr2.decide("", time.Time{}, d(7))
	if s := tr2.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("долг ниже порога с серией: %s, want ok", s.status)
	}
	// Граница ceil порога снятия: порог 9 → снятие < 9 (ceil(8.1));
	// уровень 8 снимает, 9 — удерживает.
	tr3 := &renderDebtTrack{domains: 9, passes: 1}
	_ = tr3.decide("", time.Time{}, d(2))  // база
	_ = tr3.decide("", time.Time{}, d(15)) // рост → firing
	if s := tr3.decide("", time.Time{}, d(9)); s.status != AlertStatusFiring {
		t.Errorf("удержание на 9 (порог 9): %s, want firing", s.status)
	}
	if s := tr3.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("снятие на 8 (порог снятия <9): %s, want ok (ceil)", s.status)
	}
}

// TestRenderFbDepthBelowWatermarkAfterDrop — глубина ниже порога с
// полной серией роста — ок (мутант «true» срабатывал бы).
func TestRenderFbDepthBelowWatermarkAfterDrop(t *testing.T) {
	tr := &renderFbDepthTrack{wm: 100, passes: 2}
	_ = tr.decide("", time.Time{}, 105) // база
	_ = tr.decide("", time.Time{}, 90)  // сброс (убыток)
	_ = tr.decide("", time.Time{}, 91)  // рост 1
	if s := tr.decide("", time.Time{}, 92); s.status != AlertStatusOK {
		t.Errorf("ниже порога с серией: %s (%s), want ok", s.status, s.detail)
	}
}

// TestQueueFreezeMovingCounter — деталь ok называет счётчик движимых
// групп: группа с движущимся ID считается.
func TestQueueFreezeMovingCounter(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newQueueFreezeMachine()
	moving := []StreamGroupState{{Stream: "q:dns", Group: "g", Depth: 5, LastDeliveredID: "1-0"}}
	s := m.transition(env.Alerts, context.Background(), obs(moving))
	if !strings.Contains(s.Detail, "1 из 1") {
		t.Errorf("движущаяся группа: %q, want «1 из 1»", s.Detail)
	}
	// Несколько групп: пустая до замороженной не обрывает разбор —
	// замороженная за пустой видна.
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.QueueFreeze = 15 * time.Minute
		o.Now = func() time.Time { return now }
	})
	m2 := env2.newQueueFreezeMachine()
	frozen := []StreamGroupState{
		{Stream: "q:dns", Group: "empty", Depth: 0, LastDeliveredID: "0-0"},
		{Stream: "q:http", Group: "worker", Depth: 40, LastDeliveredID: "5-1"},
	}
	_ = m2.transition(env2.Alerts, context.Background(), obs(frozen))
	now = now.Add(16 * time.Minute)
	s2 := m2.transition(env2.Alerts, context.Background(), obs(frozen))
	if s2.Status != AlertStatusFiring || !strings.Contains(s2.Detail, "q:http/worker") {
		t.Errorf("замороженная за пустой: %s (%s), want firing с q:http/worker", s2.Status, s2.Detail)
	}
}

// --- load: точный отчёт с чувствительными значениями -------------------------------

// TestLoadReportLoopMetricsAndDebt — второй проход: cpu-приборы пишутся
// (дельта есть), строка рендер-долга из рабочего источника в отчёте.
func TestLoadReportLoopMetricsAndDebt(t *testing.T) {
	reg := metrics.New("monitor")
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{User: 200_000, Total: 1_000_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 5000}},
	}
	st2 := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{User: 300_000, Total: 1_040_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 14_000}},
	}
	two := &fakeLoadSource{states: []LoadStat{st, st2}}
	fq := &switchQueuesSource{passes: []map[string]QueueStat{
		{queue.StreamDNS: {Len: 1, Read: 5000}},
		{queue.StreamDNS: {Len: 1, Read: 5100}},
	}}
	debt := &fakeDebtSource{debt: RenderDebt{Flags: 7, Lag: 3}}
	reports := make(chan string, 8)
	opts := LoadReportOptions{
		Interval:   20 * time.Millisecond,
		Metrics:    reg,
		Queues:     fq,
		RenderDebt: debt,
		Notify:     func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, two, opts) }()
	<-reports // база
	var r string
	select {
	case r = <-reports:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("второй отчёт не пришёл")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	// CPU-приборы второго прохода: дельта 40000 (user 100000, idle 0)
	// — busy 100%, user 250%.
	if v, ok := gaugeLabels(t, reg, "crawler_os_cpu_percent", map[string]string{"kind": "busy"}); !ok || v != 100 {
		t.Errorf("cpu{kind=busy} = (%v, %v), want 100", v, ok)
	}
	if v, ok := gaugeLabels(t, reg, "crawler_os_cpu_percent", map[string]string{"kind": "user"}); !ok || v != 250 {
		t.Errorf("cpu{kind=user} = (%v, %v), want 250", v, ok)
	}
	if !strings.Contains(r, "📬 Рендер-долг: флаги 7 + лаг 3 = 10") {
		t.Errorf("отчёт без строки долга:\n%s", r)
	}
}

// TestReadMemInfoZeroTotal — MemTotal: 0 — ошибка (пустое значение
// обязательного поля, мутант ослабления принимал бы).
func TestReadMemInfoZeroTotal(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "meminfo")
	if err := os.WriteFile(p, []byte("MemTotal: 0 kB\nMemAvailable: 1 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := readMemInfo(p); err == nil {
		t.Error("MemTotal 0: хочу ошибку (значение обязательного поля)")
	}
}

// TestRedisQueuesLenNoSuchKeyComplete — текстовый «no such key»: стрим
// в снимке присутствует ({0 0}), карта полная.
func TestRedisQueuesLenNoSuchKeyComplete(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups:   map[string][]redis.XInfoGroup{},
		xlen:     map[string]int64{},
		lensErrs: map[string]error{queue.StreamDNS: redis.Nil},
	}
	q, err := (RedisQueues{RDB: rdb}).Queues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != len(reportStreams) {
		t.Errorf("стримов = %d, want %d (отсутствующий — нулевым вкладом)", len(q), len(reportStreams))
	}
}

// TestLoadReportLoopNanosecondInterval — интервал 1нс — рабочее
// значение (не «выключен»): стартовый проход выполняется.
func TestLoadReportLoopNanosecondInterval(t *testing.T) {
	st := LoadStat{Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1}
	src := &fakeLoadSource{states: []LoadStat{st}}
	var reports int
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- LoadReportLoop(ctx, src, LoadReportOptions{
			Interval: 1,
			Notify:   func(string) { reports++ },
		})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for reports < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	if reports < 1 {
		t.Error("Interval=1нс: стартовый отчёт не построен (цикл принят выключенным)")
	}
}

// TestZoneStatsLoopMetricsNilReceiver — nil-приёмники метрик цикла —
// no-op без паники.
func TestZoneStatsLoopMetricsNilReceiver(t *testing.T) {
	var m *zoneStatsLoopMetrics
	m.observe(0)
	m.skip()
	m.setNewDoneStale(0)
}
