// Тесты триажа мутантов CI-прогона 09.11.2026 (половина A, score 0.758):
// добивки покрытия load.go — точные значения парсеров /proc (все поля
// CPU ненулевые — арифметика Total наблюдаема), границы условий
// (len(fields), n < 0, индексы), деградации PSI по одному файлу, ошибки
// источников с нулевыми значениями, метрики observe (gauge ×100),
// точный текст отчёта, границы fmtMem/round/топов, источник очередей
// на текстовой ошибке «no such key», вычислитель темпов и выключенный
// интервал цикла. Живые /proc и Redis — вне юнит-тестов (герметичность).
package monitor

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// fullCPUFixtures — каталог фикстур, где ВСЕ восемь полей агрегированной
// строки cpu ненулевые и различимые: арифметика st.Total и переносы
// индексов c[i] наблюдаемы (нулевые поля фикстуры loadFixtures прятали
// мутантов знака и индексов).
func fullCPUFixtures(t *testing.T) string {
	t.Helper()
	root := loadFixtures(t)
	writeFix(t, root, "loadavg", "1.5 2.5 3.5 1/100 500\n")
	writeFix(t, root, "stat",
		"cpu  11 22 33 44 55 66 77 88 99 100\n"+ // 10 полей: guest-хвост обязан игнорироваться
			"cpu0 11 22 33 44 55 66 77 88\n"+
			"cpu1 11 22 33 44 55 66 77 88\n")
	writeFix(t, root, "meminfo",
		"MemTotal:        16777216 kB\n"+
			"MemAvailable:     8388608 kB\n"+
			"SwapTotal:        2097152 kB\n"+
			"SwapFree:         1048576 kB\n")
	return root
}

// TestReadCPUStat_AllFieldsNonZero — Total — сумма всех восьми полей
// (guest-хвост вне суммы), каждое поле читается своей позицией.
func TestReadCPUStat_AllFieldsNonZero(t *testing.T) {
	st, err := readCPUStat(fullCPUFixtures(t) + "/stat")
	if err != nil {
		t.Fatalf("readCPUStat: %v", err)
	}
	if st.User != 11 || st.Nice != 22 || st.System != 33 || st.Idle != 44 ||
		st.Iowait != 55 || st.Irq != 66 || st.Softirq != 77 || st.Steal != 88 {
		t.Errorf("поля cpu = %+v, want 11/22/33/44/55/66/77/88", st)
	}
	if want := uint64(11 + 22 + 33 + 44 + 55 + 66 + 77 + 88); st.Total != want {
		t.Errorf("Total = %d, want %d (сумма всех полей)", st.Total, want)
	}
	if st.Cores != 2 {
		t.Errorf("Cores = %d, want 2", st.Cores)
	}
}

// TestReadCPUStat_MissingAggLine — строки cpuN без агрегированной «cpu »:
// Total 0 — ошибка (мутант ослабления первого дизъюнкта молчал бы).
func TestReadCPUStat_MissingAggLine(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "stat", "cpu0 11 22 33 44 55 66 77 88\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("нет строки cpu: хочу ошибку (Total 0)")
	}
	// Агрегированная строка без строк cpuN — тоже ошибка (Cores 0).
	writeFix(t, root, "stat", "cpu  11 22 33 44 55 66 77 88\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("нет строк cpuN: хочу ошибку (Cores 0)")
	}
}

// TestReadCPUStat_BrokenLines — короткая строка cpu и нечисловой тик.
func TestReadCPUStat_BrokenLines(t *testing.T) {
	root := fullCPUFixtures(t)
	writeFix(t, root, "stat", "cpu  11 22 33\n"+"cpu0 1 1 1 1 1 1 1 1\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("строка cpu короче пяти полей: хочу ошибку")
	}
	writeFix(t, root, "stat", "cpu  11 22 x 44 55 66 77 88\n"+"cpu0 1 1 1 1 1 1 1 1\n")
	if _, err := readCPUStat(root + "/stat"); err == nil {
		t.Error("нечисловой тик: хочу ошибку")
	}
	// Тик 2^63 — законен для uint64 (мутант bitSize 63 обязан его принять).
	writeFix(t, root, "stat", "cpu  9223372036854775808 22 33 44 55 66 77 88\n"+"cpu0 1 1 1 1 1 1 1 1\n")
	if _, err := readCPUStat(root + "/stat"); err != nil {
		t.Errorf("тик 2^63: %v (uint64 принимает)", err)
	}
	// Файл недоступен — ошибка с нулевой структурой (пин нулей).
	st, err := readCPUStat(root + "/нет")
	if err == nil || st != (CPUStat{}) {
		t.Errorf("нет файла: (%+v, %v), want нули и ошибка", st, err)
	}
}

// TestReadLoadAvg_Boundaries — ровно три поля валидны (граница < 3),
// два — ошибка; нулевая загрузка валидна, отрицательная — ошибка;
// значения на ошибке — нули (пин).
func TestReadLoadAvg_Boundaries(t *testing.T) {
	dir := t.TempDir()
	file := dir + "/loadavg"
	writeFix(t, dir, "loadavg", "0 0.5 1\n")
	if l1, l5, l15, err := readLoadAvg(file); err != nil || l1 != 0 || l5 != 0.5 || l15 != 1 {
		t.Errorf("ровно три поля/нули: (%v/%v/%v, %v)", l1, l5, l15, err)
	}
	writeFix(t, dir, "loadavg", "1 2\n")
	if l1, l5, l15, err := readLoadAvg(file); err == nil || l1 != 0 || l5 != 0 || l15 != 0 {
		t.Errorf("два поля: (%v/%v/%v, %v), want ошибка и нули", l1, l5, l15, err)
	}
	writeFix(t, dir, "loadavg", "1 2 3 4\n")
	if _, _, _, err := readLoadAvg(file); err != nil {
		t.Errorf("четыре поля: %v (лишние не мешают)", err)
	}
	writeFix(t, dir, "loadavg", "-1 2 3\n")
	if l1, _, _, err := readLoadAvg(file); err == nil || l1 != 0 {
		t.Errorf("отрицательная загрузка: (%v, %v), want ошибка и нули", l1, err)
	}
	writeFix(t, dir, "loadavg", "x 2 3\n")
	if l1, l5, l15, err := readLoadAvg(file); err == nil || l1 != 0 || l5 != 0 || l15 != 0 {
		t.Errorf("нечисловая загрузка: (%v/%v/%v, %v), want ошибка и нули", l1, l5, l15, err)
	}
	if _, _, _, err := readLoadAvg(dir + "/нет"); err == nil {
		t.Error("нет файла: хочу ошибку")
	}
}

// TestReadMemInfo_Boundaries — кривое значение и пустое поле значения —
// ошибки; swap отсутствует — нули без ошибки; на ошибке — нули.
func TestReadMemInfo_Boundaries(t *testing.T) {
	dir := t.TempDir()
	writeFix(t, dir, "meminfo", "MemTotal: 100 kB\nMemAvailable: x kB\n")
	if tt, a, st, sf, err := readMemInfo(dir + "/meminfo"); err == nil ||
		tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("нечисловое значение: (%v/%v/%v/%v, %v), want ошибка и нули", tt, a, st, sf, err)
	}
	writeFix(t, dir, "meminfo", "MemTotal: 100 kB\nMemAvailable:\n")
	if _, _, _, _, err := readMemInfo(dir + "/meminfo"); err == nil {
		t.Error("пустое значение: хочу ошибку (поле без числа)")
	}
	// kB → байты точно; без свопа — нули.
	writeFix(t, dir, "meminfo", "MemTotal: 1 kB\nMemAvailable: 1 kB\n")
	if tt, a, st, sf, err := readMemInfo(dir + "/meminfo"); err != nil ||
		tt != 1024 || a != 1024 || st != 0 || sf != 0 {
		t.Errorf("без свопа: (%v/%v/%v/%v, %v), want 1024/1024/0/0", tt, a, st, sf, err)
	}
}

// TestOSLoad_PSIDegradations — PSI присутствует, когда доступен хотя бы
// один файл давления (каждое присваивание PSIPresent наблюдаемо).
func TestOSLoad_PSIDegradations(t *testing.T) {
	root := fullCPUFixtures(t)
	// Только cpu-давление: файл памяти удалён.
	if err := removeFix(root, "pressure/memory"); err != nil {
		t.Fatal(err)
	}
	st, err := OSLoad{Root: root}.Load(context.Background())
	if err != nil {
		t.Fatalf("только pressure/cpu: %v", err)
	}
	if !st.PSIPresent || st.CPUPSI.Avg10 != 91.59 {
		t.Errorf("PSI cpu = present=%v %+v, want present и 91.59", st.PSIPresent, st.CPUPSI)
	}
	// Только память: cpu-файл удалён.
	root2 := fullCPUFixtures(t)
	if err := removeFix(root2, "pressure/cpu"); err != nil {
		t.Fatal(err)
	}
	st, err = OSLoad{Root: root2}.Load(context.Background())
	if err != nil {
		t.Fatalf("только pressure/memory: %v", err)
	}
	if !st.PSIPresent || st.MemPSISome.Avg10 != 7.85 {
		t.Errorf("PSI mem = present=%v some=%+v, want present и 7.85", st.PSIPresent, st.MemPSISome)
	}
	// Кривой cpu-файл — ошибка прохода (значения не молчат).
	root3 := fullCPUFixtures(t)
	writeFix(t, root3, "pressure/cpu", "some avg10=999 avg60=1 avg300=1 total=1\n")
	if _, err := (OSLoad{Root: root3}).Load(context.Background()); err == nil {
		t.Error("кривой pressure/cpu: хочу ошибку")
	}
	root4 := fullCPUFixtures(t)
	writeFix(t, root4, "pressure/memory", "some avg10=x\n")
	if _, err := (OSLoad{Root: root4}).Load(context.Background()); err == nil {
		t.Error("кривой pressure/memory: хочу ошибку")
	}
}

// removeFix — удаление файла фикстуры (деградации PSI).
func removeFix(root, name string) error {
	return os.Remove(root + "/" + name)
}

// TestOSLoad_MissingSources — обязательные источники: отсутствие
// loadavg/stat/meminfo и провал процессов роняет проход (мутанты
// вырезания return глотали бы ошибку и доходили до успеха).
func TestOSLoad_MissingSources(t *testing.T) {
	for _, miss := range []string{"loadavg", "stat", "meminfo"} {
		root := fullCPUFixtures(t)
		if err := removeFix(root, miss); err != nil {
			t.Fatal(err)
		}
		if _, err := (OSLoad{Root: root}).Load(context.Background()); err == nil {
			t.Errorf("нет %s: хочу ошибку прохода", miss)
		}
	}
}

// TestReadProcs_ContinueBoundaries — не-PID каталог и кривой stat между
// валидными PID: continue пропускает только сбойный вклад, break не
// должен обрывать обход (последующие PID агрегируются). Не-PID каталоги
// с собственным stat-файлом («.cfg», «zzz») в агрегат не попадают.
func TestReadProcs_ContinueBoundaries(t *testing.T) {
	root := fullCPUFixtures(t)
	// Не-PID каталог в середине + PID без stat-файла + PID с кривым stat,
	// затем два валидных: все валидные обязаны попасть в агрегат.
	mkdirFixRaw(t, root, "zcfg")
	mkdirFixRaw(t, root, "300") // пустой PID-каталог (stat нет)
	mkdirFix(t, root, "301", "(broken) no parens here", "1 1\n")
	mkdirFix(t, root, "302", "(alpha) S 1 0 0 0 -1 0 0 0 0 0 7 3 0 0 20 0 1", "1000 500 200 10 0 400 0\n")
	mkdirFix(t, root, "033", "(beta) S 1 0 0 0 -1 0 0 0 0 0 5 9 0 0 20 0 2", "2000 800 900 5 0 700 0\n")
	// Не-PID каталоги с СОБСТВЕННЫМ stat (dir[0] вне '0'..'9' с обеих
	// сторон границы): читаться не обязаны вовсе.
	mkdirFix(t, root, ".cfg", "(ghost) S 1 0 0 0 -1 0 0 0 0 0 1 1 0 0 20 0 3", "1 1\n")
	mkdirFix(t, root, "zzz", "(spook) S 1 0 0 0 -1 0 0 0 0 0 1 1 0 0 20 0 4", "1 1\n")
	// PID-каталоги с границами цифры: '9xx' и '0xx' — читаются.
	mkdirFix(t, root, "9xx", "(nine) S 1 0 0 0 -1 0 0 0 0 0 2 2 0 0 20 0 5", "1 1\n")
	procs, err := readProcs(root)
	if err != nil {
		t.Fatalf("readProcs: %v", err)
	}
	if got := procs["alpha"]; got.Ticks != 10 || got.Count != 1 {
		t.Errorf("alpha = %+v, want тики 10 (7+3)", got)
	}
	if got := procs["beta"]; got.Ticks != 14 || got.Count != 1 {
		t.Errorf("beta = %+v, want тики 14 (5+9) — PID с ведущим нулём читается", got)
	}
	if got := procs["nine"]; got.Ticks != 4 {
		t.Errorf("nine = %+v, want тики 4 (PID-каталог на границе '9')", got)
	}
	for _, ghost := range []string{"broken", "zcfg", "ghost", "spook"} {
		if _, ok := procs[ghost]; ok {
			t.Errorf("не-PID/кривой вклад %q попал в агрегат", ghost)
		}
	}
}

// mkdirFixRaw — пустой каталог фикстуры (не-PID / PID без stat).
func mkdirFixRaw(t *testing.T, root, name string) {
	t.Helper()
	if err := os.Mkdir(root+"/"+name, 0o755); err != nil {
		t.Fatal(err)
	}
}

// TestParseProcStatLine_Boundaries — ровно 13 полей после имени,
// имя в начале строки, close == open, пустое имя — пустой вклад.
func TestParseProcStatLine_Boundaries(t *testing.T) {
	// Ровно 13 полей: state + 12 (utime — 12-е, stime — 13-е) — валидно.
	line := "7 (ok) R 1 2 3 4 5 6 7 8 9 10 3 4"
	if comm, ticks := parseProcStatLine([]byte(line)); comm != "ok" || ticks != 7 {
		t.Errorf("13 полей = %q %d, want ok и 7 (3+4)", comm, ticks)
	}
	// 12 полей — коротко.
	if comm, ticks := parseProcStatLine([]byte("7 (short) R 1 2 3 4 5 6 7 8 9 10 3")); comm != "" || ticks != 0 {
		t.Errorf("12 полей = %q %d, want пустой вклад", comm, ticks)
	}
	// Скобки встречной ориентации («)(») — пустой вклад.
	if comm, _ := parseProcStatLine([]byte("1 )( R 1 2 3 4 5 6 7 8 9 10 3 4")); comm != "" {
		t.Errorf("встречные скобки = %q, want пустой вклад", comm)
	}
	// Пустое имя «()» — пустой вклад (и нулевые тики).
	if comm, ticks := parseProcStatLine([]byte("1 () R 1 2 3 4 5 6 7 8 9 10 3 4")); comm != "" || ticks != 0 {
		t.Errorf("пустое имя = (%q, %d), want пустой вклад с нулём", comm, ticks)
	}
	// Короткая/кривая строка — нулевые тики (пин значения на отказе).
	for _, bad := range []string{
		"7 (short) R 1 2 3 4 5 6 7 8 9 10 3",
		"1 )( R 1 2 3 4 5 6 7 8 9 10 3 4",
		"1 (x) R 1 2 3 4 5 6 7 8 9 10 p q",
	} {
		if _, ticks := parseProcStatLine([]byte(bad)); ticks != 0 {
			t.Errorf("%q: тики = %d, want 0", bad, ticks)
		}
	}
	// utime 2^63 — валиден для uint64 (мутант bitSize 63 обязан принять).
	if comm, ticks := parseProcStatLine([]byte(
		"9 (huge) R 1 2 3 4 5 6 7 8 9 10 9223372036854775808 5")); comm != "huge" || ticks != 9223372036854775813 {
		t.Errorf("utime 2^63 = (%q, %d), want huge и 2^63+5", comm, ticks)
	}
	// Нечисловые utime/stime — пустой вклад.
	if comm, _ := parseProcStatLine([]byte("1 (x) R 1 2 3 4 5 6 7 8 9 10 p q")); comm != "" {
		t.Errorf("нечисловые тики = %q, want пустой вклад", comm)
	}
}

// TestParseStatmRSS_Boundaries — резидент — ВТОРОЕ поле (не первое),
// одно поле — ноль; значение 2^63 читается.
func TestParseStatmRSS_Boundaries(t *testing.T) {
	if got := parseStatmRSS([]byte("100 900 1 1 1 1 1\n")); got != 900 {
		t.Errorf("resident = %d, want 900 (второе поле)", got)
	}
	if got := parseStatmRSS([]byte("100\n")); got != 0 {
		t.Errorf("одно поле = %d, want 0", got)
	}
	if got := parseStatmRSS([]byte("x y\n")); got != 0 {
		t.Errorf("нечисловое = %d, want 0", got)
	}
	if got := parseStatmRSS([]byte("5 9223372036854775808\n")); got != 9223372036854775808 {
		t.Errorf("2^63 = %d, want как есть (uint64)", got)
	}
}

// TestCPUDelta_IowaitAndClamp — ненулевая дельта iowait (формула busy),
// нулевая prev.Total+единичная дельта и отрицательный busy (кламп 0).
func TestCPUDelta_IowaitAndClamp(t *testing.T) {
	prev := CPUStat{User: 1000, System: 500, Idle: 500, Iowait: 100, Total: 2100, Cores: 4}
	cur := CPUStat{User: 1200, System: 700, Idle: 600, Iowait: 300, Total: 2800, Cores: 4}
	busy, user, sys, io, ok := cpuDelta(prev, cur)
	if !ok {
		t.Fatal("дельта не посчитана")
	}
	// dTotal=700: user 200/700, sys 200/700, io 200/700, idle 100/700 →
	// busy = 1 − 100/700 − 200/700 = 400/700 (сравнение с допуском —
	//浮 порядок операций).
	const eps = 1e-12
	if diff(busy, 400.0/700) > eps || diff(user, 200.0/700) > eps ||
		diff(sys, 200.0/700) > eps || diff(io, 200.0/700) > eps {
		t.Errorf("delta = busy %v user %v sys %v io %v", busy, user, sys, io)
	}
	// prev.Total = 1, d = 1 — дельта возможна (границы нулей).
	_, _, _, _, ok = cpuDelta(CPUStat{Total: 1}, CPUStat{Total: 2})
	if !ok {
		t.Error("prev.Total=1, d=1: хочу ok (нулевые границы — только точные нули)")
	}
	// Отрицательный busy (idle-дельта больше тотала) — кламп нулём.
	busy, _, _, _, ok = cpuDelta(CPUStat{Total: 100}, CPUStat{Total: 200, Idle: 300})
	if !ok {
		t.Fatal("кламп: дельта посчитана")
	}
	if busy != 0 {
		t.Errorf("отрицательный busy не прижат: %v, want 0", busy)
	}
	// Нет дельты — нулевые значения (пин нулей на «не измеряется»).
	if b, u2, s2, i2, ok := cpuDelta(cur, cur); ok || b != 0 || u2 != 0 || s2 != 0 || i2 != 0 {
		t.Errorf("нулевая дельта = (%v/%v/%v/%v, %v), want нули", b, u2, s2, i2, ok)
	}
	if b, u2, s2, i2, ok := cpuDelta(CPUStat{}, cur); ok || b != 0 || u2 != 0 || s2 != 0 || i2 != 0 {
		t.Errorf("первый проход = (%v/%v/%v/%v, %v), want нули", b, u2, s2, i2, ok)
	}
}

// diff — |a − b| для допусков сравнения дробей.
func diff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}

// TestLoadMetricsObserve — все приборы получают точные значения:
// loadavg ×3, cpu ×4 (доли ×100), PSI ×4, память ×3 (used = total −
// available), swap ×2 (used = total − free).
func TestLoadMetricsObserve(t *testing.T) {
	reg := metrics.New("monitor")
	m := NewLoadMetrics(reg)
	st := LoadStat{
		Load1: 1.5, Load5: 2.5, Load15: 3.5,
		PSIPresent: true,
		CPUPSI:     PSIStat{Avg10: 10, Avg60: 20, Avg300: 30},
		MemPSISome: PSIStat{Avg10: 4, Avg60: 5, Avg300: 6},
		MemPSIFull: PSIStat{Avg10: 7, Avg60: 8, Avg300: 9},
		MemTotal:   8 << 30, MemAvailable: 6 << 30,
		SwapTotal: 2 << 30, SwapFree: 1 << 30,
	}
	busy, user, sys, io := 0.37, 0.25, 0.11, 0.01
	m.observe(st, &busy, &user, &sys, &io)

	for kind, want := range map[string]float64{
		"1m": 1.5, "5m": 2.5, "15m": 3.5,
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_loadavg", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("loadavg{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"busy": 37, "user": 25, "system": 11, "iowait": 1,
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_cpu_percent", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("cpu{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"some_avg10": 4, "some_avg300": 6, "full_avg10": 7, "full_avg300": 9,
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_mem_psi_percent", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("mem_psi{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"total": float64(8 << 30), "available": float64(6 << 30), "used": float64(2 << 30),
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_mem_bytes", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("mem{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
	for kind, want := range map[string]float64{
		"total": float64(2 << 30), "used": float64(1 << 30),
	} {
		if v, ok := gaugeLabels(t, reg, "crawler_os_swap_bytes", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("swap{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}
}

// TestLoadConsts — пины констант отчёта (период, размеры топа, порог
// шума) и дефолт интервала.
func TestLoadConsts(t *testing.T) {
	if DefaultLoadReportInterval != 10*time.Minute {
		t.Errorf("DefaultLoadReportInterval = %v, want 10m", DefaultLoadReportInterval)
	}
	if topProcs != 4 {
		t.Errorf("topProcs = %d, want 4", topProcs)
	}
	if minProcCPUShare != 0.05 {
		t.Errorf("minProcCPUShare = %v, want 0.05", minProcCPUShare)
	}
}

// TestFormatLoadReport_Exact — полный текст отчёта на компактном снимке:
// любая правка формата (префиксы, разделители, проценты диска, дельты
// очередей ±N и темп) наблюдаема сравнением целиком.
func TestFormatLoadReport_Exact(t *testing.T) {
	st := LoadStat{
		Load1: 24.82, Load5: 21.96, Load15: 20.67,
		CPU:        CPUStat{Cores: 4},
		PSIPresent: true,
		CPUPSI:     PSIStat{Avg10: 91.59, Avg60: 90.37},
		MemPSISome: PSIStat{Avg10: 7.85},
		MemPSIFull: PSIStat{Avg10: 2},
		MemTotal:   7973728 << 10, MemAvailable: 2359296 << 10,
		SwapTotal: 3932160 << 10, SwapFree: 3729408 << 10,
		Procs: map[string]ProcAgg{
			"postgres":       {RSS: 2 << 30, Count: 3},
			"headless-shell": {RSS: 1 << 30, Count: 2},
		},
	}
	// busy/io подобраны чувствительными к мутациям множителя (×99/×101
	// меняют округление процента).
	busy, io := 0.997, 0.996
	procCPU := map[string]float64{"postgres": 1.4, "headless-shell": 0.05}
	queues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 2100},
		queue.StreamRender: {Len: 116319, Read: 1200},
	}
	rates := map[string]float64{queue.StreamDNS: 3.5, queue.StreamRender: 0.5}
	prevQueues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 120, Read: 0},
		queue.StreamRender: {Len: 116320, Read: 1100},
	}
	disks := []DiskLine{{Dir: "/var/lib/crawler/content", Avail: 44 << 30, Total: 80 << 30}}
	got := formatLoadReport(st, &busy, &io, procCPU, queues, rates, true, prevQueues, disks, nil)
	want := "📊 Нагрузка: CPU 100% (io 100%), PSI 92% [5м 90%]; load 24.8/22.0/20.7 из 4 ядер\n" +
		"🧮 Топ CPU: postgres 1.4я, headless-shell 0.1я\n" +
		"🧮 Топ память: postgres 2.0ГиБ (3 пр.), headless-shell 1.0ГиБ (2 пр.)\n" +
		"💾 RAM: занято 5.4 из 7.6 ГиБ (доступно 2.2), swap 198 МиБ; PSI mem 8% / full 2%\n" +
		"📀 Диск /var/lib/crawler/content: свободно 44 из 80 ГиБ (55%)\n" +
		"📬 Очереди: q:dns 121 (+1, 3.5/с) · q:render 116319 (−1, 0.5/с)\n" +
		"📬 Рендер-долг: н/д"
	if got != want {
		t.Errorf("отчёт =\n%s\nwant:\n%s", got, want)
	}
}

// TestFormatLoadReport_EdgeSections — границы секций: темп 0 и < 1 не
// печатаются, дельта ±0, диск с Total 0 (процент без деления на ноль),
// топ памяти только из нулевых RSS — «н/д», prevQueues без
// prevStarted — без дельт, ровно topProcs строк и порог шума по
// значению (0.05 проходит, ниже — нет).
func TestFormatLoadReport_EdgeSections(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1,
		CPU:      CPUStat{Cores: 2},
		MemTotal: 4 << 30, MemAvailable: 2 << 30,
		Procs: map[string]ProcAgg{"ghost": {Count: 2}}, // RSS 0 — «н/д»
	}
	queues := map[string]QueueStat{queue.StreamDNS: {Len: 5, Read: 10}}
	// Темпы: нулевой (не печатается), дробный < 1 печатается — с дельтой
	// ±0 (prevStarted=true, глубина не менялась).
	rates := map[string]float64{queue.StreamDNS: 0}
	prevQueues := map[string]QueueStat{queue.StreamDNS: {Len: 5}}
	r := formatLoadReport(st, nil, nil, nil, queues, rates, true, prevQueues, nil, nil)
	if !strings.Contains(r, "q:dns 5 (±0)") {
		t.Errorf("дельта ±0 не напечатана:\n%s", r)
	}
	if strings.Contains(r, "/с") {
		t.Errorf("нулевой темп напечатан:\n%s", r)
	}
	// Дробный темп 0.5 печатается; prevQueues без prevStarted — дельт нет
	// (прямой вызов формулятора: в цикле пары всегда вместе, здесь
	// разделяем).
	rates[queue.StreamDNS] = 0.5
	r = formatLoadReport(st, nil, nil, nil, queues, rates, true, prevQueues, nil, nil)
	if !strings.Contains(r, "q:dns 5 (±0, 0.5/с)") {
		t.Errorf("дробный темп не напечатан:\n%s", r)
	}
	r = formatLoadReport(st, nil, nil, nil, queues, rates, false, prevQueues, nil, nil)
	if strings.Contains(r, "(±0") || strings.Contains(r, "/с") {
		t.Errorf("дельта/темп без prevStarted напечатаны:\n%s", r)
	}

	// Диск с нулевым Total — процент 0 без паники.
	disks := []DiskLine{{Dir: "/x", Avail: 100, Total: 0}}
	r = formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, disks, nil)
	if !strings.Contains(r, "📀 Диск /x: свободно 0 из 0 ГиБ (0%)") {
		t.Errorf("диск с Total 0 = %q", r)
	}

	// Топ памяти из нулевых RSS — «н/д».
	if !strings.Contains(r, "🧮 Топ память: н/д") {
		t.Errorf("топ памяти без RSS не «н/д»:\n%s", r)
	}

	// Топ CPU: пять имён выше порога — печатаются ровно четыре
	// (topProcs); имя ровно в пороге 0.05 проходит, ниже — нет.
	procCPU := map[string]float64{
		"a": 2, "b": 1.7, "c": 1.3, "d": 0.9, "e": 0.7, "f": 0.05, "g": 0.04,
	}
	r = formatLoadReport(st, nil, nil, procCPU, nil, nil, false, nil, nil, nil)
	if want := "🧮 Топ CPU: a 2.0я, b 1.7я, c 1.3я, d 0.9я"; !strings.Contains(r, want) {
		t.Errorf("топ CPU = %q, want %q (ровно 4 строки)", topCPULine(r), want)
	}
	if strings.Contains(r, "g 0.0я") {
		t.Errorf("шум ниже порога в топе:\n%s", r)
	}
	// Граница порога: имя ровно в 0.05 — проходит (мутант «≤» терял бы
	// его), ниже порога — нет (два имени, топ не режет).
	r = formatLoadReport(st, nil, nil,
		map[string]float64{"f": 0.05, "g": 0.04}, nil, nil, false, nil, nil, nil)
	if !strings.Contains(r, "🧮 Топ CPU: f 0.1я") {
		t.Errorf("имя ровно в пороге 0.05 исчезло:\n%s", r)
	}
	if strings.Contains(topCPULine(r), "g") {
		t.Errorf("шум 0.04 в топе: %q", topCPULine(r))
	}
}

// TestSortTopOrder — равные значения: порядок по имени возрастанию
// (детерминизм), различные — по убыванию значения.
func TestSortTopOrder(t *testing.T) {
	items := topByCPU(map[string]float64{"b": 1, "a": 1, "c": 2})
	if len(items) != 3 || items[0].name != "c" || items[1].name != "a" || items[2].name != "b" {
		t.Errorf("порядок = %+v, want c,a,b (значение ↓, имя ↑)", items)
	}
	// RSS = 1 байт — имя попадает в топ (граница нуля).
	items = topByRSS(map[string]ProcAgg{"tiny": {RSS: 1, Count: 1}})
	if len(items) != 1 || items[0].name != "tiny" {
		t.Errorf("RSS 1 байт исчез из топа: %+v", items)
	}
	// Count = 1 — без суффикса процессов; Count = 2 — с ним (граница
	// «> 1» наблюдается текстом топа памяти).
	st := LoadStat{Procs: map[string]ProcAgg{
		"one": {RSS: 1 << 20, Count: 1},
		"two": {RSS: 2 << 20, Count: 2},
	}, MemTotal: 1, MemAvailable: 1}
	r := formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, nil, nil)
	if !strings.Contains(r, "two 2МиБ (2 пр.)") || strings.Contains(r, "one 1МиБ (1") {
		t.Errorf("аннотация числа процессов: %q", r)
	}
}

// TestFmtMemAndRound — границы человекочитаемых размеров и округления.
func TestFmtMemAndRound(t *testing.T) {
	cases := map[uint64]string{
		0:             "0МиБ",
		1 << 20:       "1МиБ",
		(1 << 30) - 1: "1024МиБ",
		1 << 30:       "1.0ГиБ",
		5<<30 + 1<<29: "5.5ГиБ",
	}
	for b, want := range cases {
		if got := fmtMem(b); got != want {
			t.Errorf("fmtMem(%d) = %q, want %q", b, got, want)
		}
	}
	for v, want := range map[float64]int{0.4: 0, 0.5: 1, 1.6: 2, -0.2: 0, -1.7: 0, 0: 0} {
		if got := round(v); got != want {
			t.Errorf("round(%v) = %d, want %d", v, got, want)
		}
	}
	if gib(2<<30) != 2 || mib(2<<20) != 2 {
		t.Errorf("gib/mib = %v/%v, want 2/2", gib(2<<30), mib(2<<20))
	}
}

// TestRedisQueues_TextNoSuchKey — текстовая ошибка «no such key» (не
// redis.Nil — прокси/кластер) у XLEN и XINFO GROUPS читается отсутствием
// стрима, секция жива.
func TestRedisQueues_TextNoSuchKey(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups:    map[string][]redis.XInfoGroup{},
		xlen:      map[string]int64{queue.StreamDNS: 5},
		lensErrs:  map[string]error{queue.StreamDNS: errors.New("MOVED 1234: no such key")},
		groupErrs: map[string]error{queue.StreamDNS: errors.New("MOVED 1234: no such key")},
	}
	q, err := RedisQueues{RDB: rdb}.Queues(context.Background())
	if err != nil {
		t.Fatalf("текстовый no such key: %v (секция обязана жить)", err)
	}
	if q[queue.StreamDNS] != (QueueStat{}) {
		t.Errorf("q:dns = %+v, want {0 0}", q[queue.StreamDNS])
	}
	// Полное вырезание ветки XLEN: оригинал глотает «no such key» и
	// живёт ({0 0}); мутант дошёл бы до XINFO GROUPS с её полноценной
	// ошибкой и ронял бы секцию — здесь обязана быть тишина.
	rdb2 := &fakeGroupsRedis{
		groups:    map[string][]redis.XInfoGroup{},
		xlen:      map[string]int64{queue.StreamDNS: 5},
		lensErrs:  map[string]error{queue.StreamDNS: errors.New("no such key")},
		groupErrs: map[string]error{queue.StreamDNS: errors.New("connection refused")},
	}
	q2, err := (RedisQueues{RDB: rdb2}).Queues(context.Background())
	if err != nil {
		t.Fatalf("ветка «нет ключа» вырезана молча: %v", err)
	}
	if q2[queue.StreamDNS] != (QueueStat{}) {
		t.Errorf("q:dns = %+v, want {0 0}", q2[queue.StreamDNS])
	}
}

// TestRedisQueues_ReadEqualGroups — группы с равным entries-read:
// максимум корректен и при равенстве (граница >).
func TestRedisQueues_ReadEqualGroups(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			queue.StreamHTTP: {
				{Name: "a", Lag: 0, Pending: 0, EntriesRead: 500},
				{Name: "b", Lag: 0, Pending: 0, EntriesRead: 500},
			},
		},
		xlen: map[string]int64{queue.StreamHTTP: 0},
	}
	q, err := RedisQueues{RDB: rdb}.Queues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := q[queue.StreamHTTP].Read; got != 500 {
		t.Errorf("Read = %d, want 500", got)
	}
}

// TestLoadReportLoop_DisabledAndRates — интервал 0 выключает цикл без
// снимков; темп доставок между проходами — дельта Read за окно (не
// произведение и не сумма с прошлым).
func TestLoadReportLoop_DisabledAndRates(t *testing.T) {
	src := &countingLoadSource{}
	if err := LoadReportLoop(context.Background(), src, LoadReportOptions{Interval: 0}); err != nil {
		t.Fatalf("выключенный цикл: %v", err)
	}
	if src.count != 0 {
		t.Errorf("выключенный цикл: снимков = %d, want 0", src.count)
	}

	// Темп: базовый снимок + окно с дельтой доставок 100 при прошлом
	// Read 5000 — печатается «N/с» порядка сотен (деление на окно ~20 мс;
	// мутанты умножения/сления дают единицы либо сотни тысяч).
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 1_000_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 5000}},
	}
	st2 := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 1_040_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 14_000}},
	}
	two := &fakeLoadSource{states: []LoadStat{st, st2}}
	queues := []map[string]QueueStat{
		{queue.StreamDNS: {Len: 1, Read: 5000}},
		{queue.StreamDNS: {Len: 2, Read: 5100}},
	}
	fq := &switchQueuesSource{passes: queues}
	reports := make(chan string, 8)
	opts := LoadReportOptions{
		Interval: 20 * time.Millisecond,
		Queues:   fq,
		Notify:   func(text string) { reports <- text },
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
	line := ""
	for _, l := range strings.Split(r, "\n") {
		if strings.HasPrefix(l, "📬 Очереди: ") {
			line = l
		}
	}
	if !strings.Contains(line, "/с") {
		t.Fatalf("темп не напечатан: %q", line)
	}
	// Вытащить темп «N.N/с» из хвоста «q:dns 2 (+1, N.N/с)».
	rate := 0.0
	if i := strings.Index(line, "/с"); i >= 0 {
		j := i
		for j > 0 && (line[j-1] >= '0' && line[j-1] <= '9' || line[j-1] == '.') {
			j--
		}
		if v, err := strconv.ParseFloat(line[j:i], 64); err == nil {
			rate = v
		}
	}
	// Окно — тикер 20 мс плюс джиттер: корректный темп 100/0.02..0.3 =
	// 333..5000; мутанты: *window ≈ 100*0.02..0.3 = 2..30; +p.Read
	// ≈ 10100/окно ≥ 33666. Коридор [150, 20000] разделяет их с запасом.
	if rate < 150 || rate > 20000 {
		t.Errorf("темп доставок = %v (окно ~20мс, дельта 100), want 150..20000", rate)
	}
	if !strings.Contains(line, "(+1") {
		t.Errorf("дельта бэклога не напечатана: %q", line)
	}
}

// TestLoadReportLoop_TinyWindowTopCPU — окно тиков CPU в одну единицу:
// доля ядра процесса всё равно считается (perCore 0.25 при 4 ядрах →
// дельта 1 тик = 4 ядра — масштаб окна не гасит топ).
func TestLoadReportLoop_TinyWindowTopCPU(t *testing.T) {
	base := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 100, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 100}},
	}
	cur := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 101, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 101}},
	}
	src := &fakeLoadSource{states: []LoadStat{base, cur}}
	reports := make(chan string, 8)
	opts := LoadReportOptions{
		Interval: 20 * time.Millisecond,
		Notify:   func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()
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
	// Окно d=1, perCore=0.25: доля a = 1/0.25 = 4.0я.
	if want := "🧮 Топ CPU: a 4.0я"; !strings.Contains(r, want) {
		t.Errorf("топ CPU окна в 1 тик = %q, want %q", topCPULine(r), want)
	}
}

// TestLoadReportLoop_SourceErrorNoReport — сбой снимка системы роняет
// проход ЦЕЛИКОМ: ни отчёта, ни базы дельт (мутант вырезания return
// строил бы отчёт из нулевого снимка).
func TestLoadReportLoop_SourceErrorNoReport(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Total: 1000, Cores: 1},
		MemTotal: 1, MemAvailable: 1,
		Procs: map[string]ProcAgg{"a": {Ticks: 10}},
	}
	// Первый проход — снимок, далее — стойкая ошибка источника.
	src := &flakyLoadSource{first: st, err: errors.New("источник недоступен")}
	// reports пишется из горутины цикла (Notify) и опрашивается здесь
	// до остановки цикла — доступ только под мьютексом (race-detector).
	var mu sync.Mutex
	var reports []string
	opts := LoadReportOptions{
		Interval: 10 * time.Millisecond,
		Notify:   func(text string) { mu.Lock(); reports = append(reports, text); mu.Unlock() },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, src, opts) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(reports)
		mu.Unlock()
		if n >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	n := len(reports)
	mu.Unlock()
	if n != 1 {
		cancel()
		t.Fatalf("отчётов = %d, want 1 (успешный старт)", n)
	}
	// Ждём пару ошибочных проходов и отменяем: новых отчётов нет.
	time.Sleep(60 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("цикл: %v", err)
	}
	mu.Lock()
	n = len(reports)
	mu.Unlock()
	if n != 1 {
		t.Errorf("отчётов после сбоев источника = %d, want 1 (сбой не строит отчёт)", n)
	}
}

// flakyLoadSource — успешный первый снимок, затем стойкая ошибка.
type flakyLoadSource struct {
	first LoadStat
	err   error
	done  bool
}

func (s *flakyLoadSource) Load(context.Context) (LoadStat, error) {
	if s.done {
		return LoadStat{}, s.err
	}
	s.done = true
	return s.first, nil
}

// TestLoadReportLoop_SpaceErrorNoDiskLine — сбой statfs каталога не
// порождает строку диска с нулями (continue, не «0 из 0»).
func TestLoadReportLoop_SpaceErrorNoDiskLine(t *testing.T) {
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1,
	}
	src := &fakeLoadSource{states: []LoadStat{st}}
	var reports []string
	opts := LoadReportOptions{
		Interval: time.Hour, // единственный стартовый проход
		Space:    fakeLoadSpace{err: errors.New("statfs: отказано")},
		Dirs:     []string{"/var/lib/crawler/content"},
		Notify:   func(text string) { reports = append(reports, text) },
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
	if len(reports) != 1 {
		t.Fatalf("отчётов = %d, want 1", len(reports))
	}
	if strings.Contains(reports[0], "Диск /var/lib/crawler/content: свободно 0 из 0") {
		t.Errorf("сбой statfs породил нулевую строку диска:\n%s", reports[0])
	}
	if !strings.Contains(reports[0], "📀 Диск: н/д") {
		t.Errorf("секция диска не «н/д» при единственном сбойном каталоге:\n%s", reports[0])
	}
}

// switchQueuesSource — QueueSource с очередью снимков по проходам.
type switchQueuesSource struct {
	passes []map[string]QueueStat
	i      int
}

func (s *switchQueuesSource) Queues(context.Context) (map[string]QueueStat, error) {
	if s.i >= len(s.passes) {
		return nil, nil
	}
	q := s.passes[s.i]
	s.i++
	return q, nil
}

// countingLoadSource — LoadSource со счётчиком вызовов (выключенный
// цикл не делает ни одного снимка).
type countingLoadSource struct {
	count int
}

func (s *countingLoadSource) Load(context.Context) (LoadStat, error) {
	s.count++
	return LoadStat{}, nil
}
