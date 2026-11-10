// Третья добивка триажа (прогон monitor 0.970): точные тексты ошибок
// чтения (отсутствующий файл — «no such file», отсутствующее поле —
// «отсутствует», пустой ответ — «пустой ответ»), вырезания return в
// OSLoad (битые loadavg/meminfo роняют проход, а не снимок с нулями),
// кривой PSI some при валидном full, негативные проверки топа (пятая
// строка не печатается), границы cpu-строки (ровно 5 полей), MemTotal 0
// с пином нулей, диск Total 1 байт, отрицательный busy в gauge, нулевой
// src цикла, newDoneStale с value=1, notify без getenv, счётчик
// вызовов источника рендер-долга, закрытие строк VerifyCounters на
// каждой ветке, UnitEvents с отменой ctx (errors.Is) и timeout 1нс,
// parseMemoryEvents беззначная строка ключа счётчика.
package monitor

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeCgroup/fakeSystemctl — фикстуры юнит-событий (unitevents_test.go).

// TestOSLoadSourceErrorTexts — битые обязательные файлы: ошибка несёт
// исходный текст чтения (мутанты вырезания return доводили бы до
// нулевого снимка или повторной ошибки с чужим текстом).
func TestOSLoadSourceErrorTexts(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "loadavg", "x 1 2\n") // loadavg не число
	_, err := (OSLoad{Root: root}).Load(context.Background())
	if err == nil || !strings.Contains(err.Error(), "loadavg") {
		t.Errorf("битый loadavg: %v, want ошибку loadavg", err)
	}

	root2 := fullCPUFixtures(t)
	writeFix(t, root2, "meminfo", "MemTotal: x kB\nMemAvailable: 1 kB\n")
	_, err = (OSLoad{Root: root2}).Load(context.Background())
	if err == nil || !strings.Contains(err.Error(), "meminfo") {
		t.Errorf("битый meminfo: %v, want ошибку meminfo", err)
	}

	// Отсутствующий файл — текст «no such file or directory» (ошибка
	// чтения, не производная форма).
	for _, miss := range []string{"loadavg", "stat", "meminfo"} {
		root3 := fullCPUFixtures(t)
		if err := removeFix(root3, miss); err != nil {
			t.Fatal(err)
		}
		_, err := (OSLoad{Root: root3}).Load(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no such file") {
			t.Errorf("нет %s: %v, want исходную ошибку чтения файла", miss, err)
		}
	}
}

// TestOSLoadBrokenSomeValidFull — кривая строка some при валидной full
// (и наоборот): ошибка соответствующего вида — проход не молчит и не
// подменяет виды.
func TestOSLoadBrokenSomeValidFull(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "pressure/memory",
		"some avg10=x avg60=1 avg300=1\nfull avg10=1 avg60=1 avg300=1\n")
	// Кривая строка some роняет проход даже при валидной full (мутант
	// вырезания return проглатывал ошибку и возвращал снимок).
	if _, err := (OSLoad{Root: root}).Load(context.Background()); err == nil {
		t.Error("кривой some при валидной full: хочу ошибку прохода")
	}
	root2 := fullCPUFixtures(t)
	writeFix(t, root2, "pressure/memory",
		"some avg10=1 avg60=1 avg300=1\nfull avg10=x avg60=1 avg300=1\n")
	if _, err := (OSLoad{Root: root2}).Load(context.Background()); err == nil {
		t.Error("кривой full: хочу ошибку")
	}
}

// TestReadMemInfoNoFileText — отсутствующий meminfo: текст «no such
// file», значения нули.
func TestReadMemInfoNoFileText(t *testing.T) {
	tt, a, st, sf, err := readMemInfo(filepath.Join(t.TempDir(), "нет"))
	if err == nil || !strings.Contains(err.Error(), "no such file") ||
		tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("нет файла: (%v/%v/%v/%v, %v), want нули и текст чтения", tt, a, st, sf, err)
	}
}

// TestReadMemInfoMissingFieldPins — отсутствие MemTotal/MemAvailable:
// нулевые значения (пин) и ошибка обязательности.
func TestReadMemInfoMissingFieldPins(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "meminfo")
	if err := writeFileForTest(p, "Cached: 1 kB\n"); err != nil {
		t.Fatal(err)
	}
	tt, a, st, sf, err := readMemInfo(p)
	if err == nil || tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("нет обязательных полей: (%v/%v/%v/%v, %v), want нули и ошибку", tt, a, st, sf, err)
	}
	// MemTotal 0 — та же ошибка, нули.
	if err := writeFileForTest(p, "MemTotal: 0 kB\nMemAvailable: 1 kB\n"); err != nil {
		t.Fatal(err)
	}
	tt, a, st, sf, err = readMemInfo(p)
	if err == nil || tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("MemTotal 0: (%v/%v/%v/%v, %v), want нули и ошибку", tt, a, st, sf, err)
	}
	// Значение без единицы (одно поле): ошибка разбора, а не паника
	// индекса соседнего поля.
	if err := writeFileForTest(p, "MemTotal: y\n"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := readMemInfo(p); err == nil {
		t.Error("значение без единицы: want ошибку разбора")
	}
}

// TestReadCPUStatFieldsBoundary — ровно пять полей cpu-строки — валидно,
// четыре — ошибка, шесть — валидно (границы < 5).
func TestReadCPUStatFieldsBoundary(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "stat")
	if err := writeFileForTest(p, "cpu  1 2 3\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := readCPUStat(p); err == nil {
		t.Error("три числа: хочу ошибку (строка короче пяти полей)")
	}
	// Четыре числа (пять полей с «cpu») — валидны: отсутствующие
	// поля читаются нулями.
	if err := writeFileForTest(p, "cpu  1 2 3 4\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	st4, err := readCPUStat(p)
	if err != nil || st4.Idle != 4 || st4.Iowait != 0 {
		t.Errorf("четыре числа: (%+v, %v), want idle 4 iowait 0", st4, err)
	}
	if err := writeFileForTest(p, "cpu  1 2 3 4 5\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	st, err := readCPUStat(p)
	if err != nil || st.Idle != 4 || st.Iowait != 5 {
		t.Errorf("пять полей: (%+v, %v), want idle 4 iowait 5", st, err)
	}
	if err := writeFileForTest(p, "cpu  1 2 3 4 5 6\ncpu0 1 1 1 1\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := readCPUStat(p); err != nil {
		t.Errorf("шесть полей: %v, want валидно", err)
	}
}

// TestReadProcsNoEmptyComm — вклад без имени не попадает в агрегат
// (PID без stat / кривая строка): ключ "" отсутствует.
func TestReadProcsNoEmptyComm(t *testing.T) {
	root := fullCPUFixtures(t)
	mkdirFixRaw(t, root, "400") // PID без stat — пустой вклад
	mkdirFix(t, root, "401", "(broken) no parens here", "1 1\n")
	procs, err := readProcs(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := procs[""]; ok {
		t.Errorf("пустое имя в агрегате: %+v", procs[""])
	}
}

// TestLoadReportLoopNilSource — источник не задан: ошибка, не паника.
func TestLoadReportLoopNilSource(t *testing.T) {
	if err := LoadReportLoop(context.Background(), nil, LoadReportOptions{Interval: time.Minute}); err == nil ||
		!strings.Contains(err.Error(), "источник не задан") {
		t.Errorf("nil источник: %v, want ошибку", err)
	}
}

// TestFormatLoadReportTopNotExtended — пятый кандидат топа CPU не
// печатается (срез ровно topProcs).
func TestFormatLoadReportTopNotExtended(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1,
	}
	procCPU := map[string]float64{"a": 2, "b": 1.7, "c": 1.3, "d": 0.9, "e": 0.7}
	r := formatLoadReport(st, nil, nil, procCPU, nil, nil, false, nil, nil, nil)
	if strings.Contains(r, "e 0.7я") {
		t.Errorf("пятый кандидат топа напечатан:\n%s", r)
	}
	if want := "🧮 Топ CPU: a 2.0я, b 1.7я, c 1.3я, d 0.9я"; topCPULine(r) != want {
		t.Errorf("топ CPU = %q, want %q", topCPULine(r), want)
	}
}

// TestCPUDeltaClampFraction — busy в (−1, 0) тоже прижимается к нулю
// (дробный перелив idle, мутант ослабления границы клампа).
func TestCPUDeltaClampFraction(t *testing.T) {
	prev := CPUStat{Total: 100, Cores: 1}
	cur := CPUStat{Idle: 300, Total: 300, Cores: 1} // d=200, idle 1.5
	busy, _, _, _, ok := cpuDelta(prev, cur)
	if !ok {
		t.Fatal("дельта не посчитана")
	}
	if busy != 0 {
		t.Errorf("busy = %v, want 0 (кламп дробного перелива)", busy)
	}
}

// TestFormatLoadReportTinyDisk — диск Total 1 байт: процент считается
// (не гасится), 100% при полном avail.
func TestFormatLoadReportTinyDisk(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1,
	}
	disks := []DiskLine{{Dir: "/tiny", Avail: 1, Total: 1}}
	r := formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, disks, nil)
	if !strings.Contains(r, "📀 Диск /tiny: свободно 0 из 0 ГиБ (100%)") {
		t.Errorf("диск Total 1: %q, want 100%%", r)
	}
}

// TestLoadReportLoopSpaceErrorSecondDir — сбой statfs первого каталога
// не обрывает секцию: второй каталог печатается.
func TestLoadReportLoopSpaceErrorSecondDir(t *testing.T) {
	st := LoadStat{Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1}
	src := &fakeLoadSource{states: []LoadStat{st}}
	space := &switchSpaceSource{
		errs:  []error{errors.New("statfs: отказано"), nil},
		stats: []SpaceStat{{}, {Avail: 1 << 30, Total: 2 << 30}},
	}
	reports := make(chan string, 4)
	opts := LoadReportOptions{
		Interval: time.Hour,
		Space:    space,
		Dirs:     []string{"/bad", "/good"},
		Notify:   func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не вышел")
	}
	select {
	case r := <-reports:
		if !strings.Contains(r, "📀 Диск /good:") {
			t.Errorf("второй каталог пропал после сбоя первого:\n%s", r)
		}
		if strings.Contains(r, "/bad") {
			t.Errorf("сбойный каталог в отчёте:\n%s", r)
		}
	default:
		t.Fatal("отчёт не пришёл")
	}
}

// switchSpaceSource — SpaceChecker с результатами по каталогам по
// порядку (первый — ошибка).
type switchSpaceSource struct {
	stats []SpaceStat
	errs  []error
	i     int
}

func (s *switchSpaceSource) Space(string) (SpaceStat, error) {
	if s.i < len(s.errs) && s.errs[s.i] != nil {
		s.i++
		return SpaceStat{}, s.errs[s.i-1]
	}
	if s.i >= len(s.stats) {
		return SpaceStat{}, nil
	}
	st := s.stats[s.i]
	s.i++
	return st, nil
}

// TestNewDoneStaleValueOne — значение 1 — полноценный снапшот (серия
// растёт, граница n > 0).
func TestNewDoneStaleValueOne(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tr := &newDoneStaleTrack{minPasses: 2}
	row := func(v int64, at time.Time) staleRow {
		return staleRow{value: v, snap: sql.NullTime{Time: at, Valid: true}}
	}
	if s := tr.decide("", time.Time{}, row(1, t0)); !strings.Contains(s.detail, "1/2") {
		t.Errorf("value=1: %s, want серию 1/2", s.detail)
	}
	if s := tr.decide("", time.Time{}, row(1, t0.Add(time.Minute))); s.status != AlertStatusFiring {
		t.Errorf("value=1 второй снапшот: %s, want firing", s.status)
	}
}

// TestTelegramNotifierNoGetenv — конструктор без Getenv использует
// os.Getenv (без паники на nil-функции).
func TestTelegramNotifierNoGetenv(t *testing.T) {
	// Секретов в окружении теста нет: конструктор честно выключает
	// доставку, но getenv вызван (не nil-функция).
	n := NewTelegramNotifier(TelegramOptions{Enabled: true, Script: "/bin/true"})
	if n == nil {
		t.Fatal("конструктор без Getenv вернул nil")
	}
}

// TestRedisUptimeMissingFieldText — отсутствующее поле INFO server —
// ошибка «поле отсутствует» (не молчаливый ноль).
func TestRedisUptimeMissingFieldText(t *testing.T) {
	if _, err := (RedisInfo{RDB: &fakeInfoRedis{info: "# Server\nrun_id:x\n"}}).RedisUptime(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "uptime_in_seconds отсутствует") {
		t.Errorf("нет поля: %v, want «поле отсутствует»", err)
	}
}

// TestRedisMemoryMissingFieldText — отсутствующее поле INFO memory —
// ошибка «поле отсутствует».
func TestRedisMemoryMissingFieldText(t *testing.T) {
	if _, err := (RedisInfo{RDB: &fakeInfoRedis{info: "# Memory\nused_memory_human:1K\n"}}).RedisMemory(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "отсутствует") {
		t.Errorf("нет поля: %v, want «поле отсутствует»", err)
	}
}

// TestParsePSIMissingFieldText — отсутствующее поле PSI — ошибка
// «поле отсутствует» (мутант глотал и возвращал частичный снимок).
func TestParsePSIMissingFieldText(t *testing.T) {
	if _, err := parsePSI("some avg60=1 avg300=1\n"); err == nil ||
		!strings.Contains(err.Error(), "avg10 отсутствует") {
		t.Errorf("нет avg10: %v, want «поле avg10 отсутствует»", err)
	}
}

// TestStreamGroupsClosedFlags — фейк групп: пустые группы не запрашивают
// XLEN (счётчик вызовов).
func TestStreamGroupsClosedFlags(t *testing.T) {
	inner := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{alertStreams[0]: {}},
		xlen:   map[string]int64{},
	}
	rdb := &countingGroupsRedis{fakeGroupsRedis: *inner}
	_, err := (RedisStreamGroups{RDB: rdb}).StreamGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rdb.xlenCalls != 0 {
		t.Errorf("XLEN вызван %d раз при пустых группах, want 0", rdb.xlenCalls)
	}
}

// countingGroupsRedis — fakeGroupsRedis со счётчиком XLEN.
type countingGroupsRedis struct {
	fakeGroupsRedis
	xlenCalls int
}

func (r *countingGroupsRedis) XLen(ctx context.Context, stream string) *redis.IntCmd {
	r.xlenCalls++
	return r.fakeGroupsRedis.XLen(ctx, stream)
}

// TestRenderDebtLoopSourceCalls — Interval 1нс: тикер молотит, источник
// зовётся много раз (мутант «выключен» звал бы только стартовый).
func TestRenderDebtLoopSourceCalls(t *testing.T) {
	src := &countingDebtSource{debt: RenderDebt{Flags: 1, Lag: 1}}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- RenderDebtLoop(ctx, src, RenderDebtOptions{Interval: 1}) }()
	deadline := time.Now().Add(1 * time.Second)
	for src.calls() < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	if src.calls() < 5 {
		t.Errorf("вызовов источника = %d, want ≥5 (тикер 1нс живой)", src.calls())
	}
}

// countingDebtSource — источник рендер-долга со счётчиком вызовов.
type countingDebtSource struct {
	debt RenderDebt
	n    int
}

func (s *countingDebtSource) RenderDebt(context.Context) (RenderDebt, error) {
	s.n++
	return s.debt, nil
}

func (s *countingDebtSource) calls() int { return s.n }

// TestVerifyCountersClosedEveryBranch — строки сверки закрываются Close
// на каждой ошибочной ветке (Scan-ошибка и пустой ответ истории).
func TestVerifyCountersClosedEveryBranch(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 0)
	// Scan-ошибка истории: hrows.Close вызван.
	steps := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		&zonestatsFakeRows{rows: [][]any{{int64(9)}}, scanErr: errors.New("бум")},
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil {
		t.Fatal("Scan-ошибка истории: хочу ошибку")
	}
	if !steps[2].rows.closed {
		t.Error("hrows не закрыты при Scan-ошибке")
	}
	// Пустой ответ истории: hrows.Close вызван.
	steps2 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		zonestatsRows(),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps2}, VerifyOptions{}); err == nil {
		t.Fatal("пустая история: хочу ошибку")
	}
	if !steps2[2].rows.closed {
		t.Error("hrows не закрыты при пустом ответе")
	}
	// rows.Err истории: закрыты.
	steps3 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		&zonestatsFakeRows{rows: [][]any{{int64(0), int64(0)}}, err: errors.New("обрыв")},
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps3}, VerifyOptions{}); err == nil {
		t.Fatal("rows.Err истории: хочу ошибку")
	}
	if !steps3[2].rows.closed {
		t.Error("hrows не закрыты при rows.Err")
	}
	// rows.Err зон: закрыты.
	steps4 := verifySteps(
		&zonestatsFakeRows{rows: [][]any{verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})}, err: errors.New("обрыв")},
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps4}, VerifyOptions{}); err == nil {
		t.Fatal("rows.Err зон: хочу ошибку")
	}
	if !steps4[0].rows.closed {
		t.Error("rows зон не закрыты при rows.Err")
	}
	// Scan-ошибка зон: закрыты.
	steps5 := verifySteps(
		&zonestatsFakeRows{rows: [][]any{{int16(7)}}, scanErr: errors.New("бум")},
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps5}, VerifyOptions{}); err == nil {
		t.Fatal("Scan-ошибка зон: хочу ошибку")
	}
	if !steps5[0].rows.closed {
		t.Error("rows зон не закрыты при Scan-ошибке")
	}
	// rows.Err сохранённых: закрыты.
	steps6 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		&zonestatsFakeRows{rows: [][]any{counterRowOf("render_pending", 0)}, err: errors.New("обрыв")})
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps6}, VerifyOptions{}); err == nil {
		t.Fatal("rows.Err сохранённых: хочу ошибку")
	}
	if !steps6[3].rows.closed {
		t.Error("rows сохранённых не закрыты при rows.Err")
	}
	// rows.Err агрегата (читается readZoneAgg с defer Close) — закрыты.
	steps7 := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		&zonestatsFakeRows{rows: [][]any{aggRow}, err: errors.New("обрыв")},
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 0)))
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps7}, VerifyOptions{}); err == nil {
		t.Fatal("rows.Err агрегата: хочу ошибку")
	}
	if !steps7[1].rows.closed {
		t.Error("rows агрегата не закрыты при rows.Err")
	}
}

// TestZoneStatsWriteDailyEmptyText — пустой ответ upsert — ошибка
// «пустой ответ upsert» (не производная Scan).
func TestZoneStatsWriteDailyEmptyText(t *testing.T) {
	p := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	tx, _ := p.Begin(context.Background())
	_, err := writeDailyCounter(context.Background(), tx, recountDailyChecksSQL, 5, time.Unix(0, 0), "суточная")
	if err == nil || !strings.Contains(err.Error(), "пустой ответ upsert") {
		t.Errorf("пустой upsert: %v, want «пустой ответ upsert»", err)
	}
}

// TestOSUnitEventsCtxErrors — отмена ctx до и после таймаута-обёртки:
// ошибки — контекстные (errors.Is), не пустой снимок.
func TestOSUnitEventsCtxErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (OSUnitEvents{}).UnitEvents(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("отмена до старта: %v, want context.Canceled", err)
	}
	// Timeout 1нс — истёкшая обёртка: ошибка контекста (не дефолт окна).
	if _, err := (OSUnitEvents{Timeout: 1}).UnitEvents(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("Timeout 1нс: %v, want deadline exceeded", err)
	}
}

// TestParseMemoryEventsBareKey — строка-ключ без значения не роняет
// разбор и не превращается в ошибку числа (посторонние ключи мимо).
func TestParseMemoryEventsBareKey(t *testing.T) {
	if _, err := parseMemoryEvents("oom\n"); err != nil {
		t.Errorf("ключ без значения: %v, want пропуск", err)
	}
	if ev, err := parseMemoryEvents("oom\noom 4\n"); err != nil || ev.OOM != 4 {
		t.Errorf("ключ без значения + валидная строка: (%+v, %v)", ev, err)
	}
}

// TestAlertDescKnownAndUnknown — известному алерту — описание из
// легенды (не имя), неизвестному — имя.
func TestAlertDescKnownAndUnknown(t *testing.T) {
	for name := range alertDescs {
		if got := alertDesc(name); got == name {
			t.Errorf("alertDesc(%s) = имя, want описание легенды", name)
		}
		break // достаточно одного известного
	}
	if got := alertDesc("нет-такого-алерта"); got != "нет-такого-алерта" {
		t.Errorf("alertDesc(неизвестный) = %q, want имя как есть", got)
	}
}

// TestOSUnitEventsSingleBrokenEvents — один юнит с кривым memory.events
// (читается, но не число): снимок не отдаётся (непрочитанный счётчик не
// подменяется нулями), ошибка называет юнит.
func TestOSUnitEventsSingleBrokenEvents(t *testing.T) {
	root := t.TempDir()
	cg := filepath.Join(root, "cg")
	show := "Id=crawler-http-worker.service\nNRestarts=2\n"
	fakeCgroup(t, cg, "crawler-http-worker.service", "oom x\n")
	_, err := (OSUnitEvents{
		CgroupFS:  cg,
		Systemctl: fakeSystemctl(t, root, show, ""),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), "crawler-http-worker.service") {
		t.Errorf("кривой memory.events: %v, want ошибку с именем юнита", err)
	}
}
