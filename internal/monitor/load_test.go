// Юнит-тесты отчёта системной нагрузки (T-173): разбор /proc-файлов
// (loadavg, stat, meminfo, строки процессов, PSI some/full), дельты
// утилизации CPU и тиков процессов, топы, формат отчёта (секции,
// «н/д»-деградация), источник OSLoad на каталоге фикстур, цикл одним
// проходом на фейках. Живые /proc и Redis — вне юнит-тестов (герметичность,
// стиль psi_test.go).
package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crawler/internal/queue"
)

// writeFix — файл фикстуры во временном корне (родительский каталог
// создаётся — фикстуры лежат в pressure/ и PID-каталогах).
func writeFix(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("фикстура %s: каталог: %v", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("фикстура %s: %v", name, err)
	}
}

// loadFixtures — каталог /proc-фикстур: 4 ядра, load 24/22/20, 2 ГиБ
// занятой памяти, своп 200 МиБ, PSI, три процесса (два headless-shell).
func loadFixtures(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFix(t, root, "loadavg", "24.82 21.96 20.67 3/943 1588621\n")
	// 4 ядра × 100 тиков: busy 97/100 по сумме ядер (user 70, sys 24,
	// idle 3, iowait 0 … на каждое ядро) — строка cpu агрегированная.
	writeFix(t, root, "stat",
		"cpu  28000 0 9600 1200 0 200 0 0\n"+
			"cpu0 7000 0 2400 300 0 50 0 0\n"+
			"cpu1 7000 0 2400 300 0 50 0 0\n"+
			"cpu2 7000 0 2400 300 0 50 0 0\n"+
			"cpu3 7000 0 2400 300 0 50 0 0\n")
	writeFix(t, root, "meminfo",
		"MemTotal:        7973728 kB\n"+
			"MemFree:          430080 kB\n"+
			"MemAvailable:    2359296 kB\n"+
			"Cached:          3801088 kB\n"+
			"SwapTotal:       3932160 kB\n"+
			"SwapFree:        3729408 kB\n")
	writeFix(t, root, "pressure/cpu",
		"some avg10=91.59 avg60=90.37 avg300=88.90 total=115180632759\n")
	writeFix(t, root, "pressure/memory",
		"some avg10=7.85 avg60=7.57 avg300=5.97 total=19881877618\n"+
			"full avg10=2.00 avg60=1.87 avg300=1.76 total=8010557278\n")

	// Процессы: utime+stime — поля 12/13 после имени; statm resident —
	// поле 2 (страницы по 4 КиБ в тесте не важны — множитель страницы хоста).
	// 1588621 (monitor) 20 0 ... utime=300 stime=100 → 400 тиков.
	mkdirFix(t, root, "1588621", "(monitor) R 1 0 0 0 -1 4194560 0 0 0 0 "+
		"300 100 0 0 20 0 1 0", "3000 500 200 10 0 400 0\n")
	// Два headless-shell по 1000 тиков и 262144 страниц (1 ГиБ).
	mkdirFix(t, root, "201", "(headless-shell) S 1 0 0 0 -1 4194560 0 0 0 0 "+
		"700 300 0 0 20 0 1 0", "262144 262144 100 10 0 262144 0\n")
	mkdirFix(t, root, "202", "(headless-shell) S 1 0 0 0 -1 4194560 0 0 0 0 "+
		"600 400 0 0 20 0 2 0", "262144 262144 100 10 0 262144 0\n")
	// postgres с именем, содержащим пробелы и скобки.
	mkdirFix(t, root, "203", "(postgres: 1[2] writer) S 1 0 0 0 -1 0 0 0 0 0 "+
		"500 500 0 0 20 0 3 0", "1000 900 50 5 0 900 0\n")
	// Не-PID каталог и файл — игнорируются.
	if err := os.Mkdir(filepath.Join(root, "acpi"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFix(t, root, "uptime", "12345.67 89012.34\n")
	return root
}

// mkdirFix — PID-каталог фикстуры со stat и statm.
func mkdirFix(t *testing.T, root, pid, stat, statm string) {
	t.Helper()
	dir := filepath.Join(root, pid)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFix(t, dir, "stat", pid+" "+stat+"\n")
	writeFix(t, dir, "statm", statm)
}

func TestOSLoad_Fixtures(t *testing.T) {
	st, err := OSLoad{Root: loadFixtures(t)}.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if st.Load1 != 24.82 || st.Load5 != 21.96 || st.Load15 != 20.67 {
		t.Errorf("loadavg = %v/%v/%v", st.Load1, st.Load5, st.Load15)
	}
	if st.CPU.Cores != 4 {
		t.Errorf("ядер = %d, want 4", st.CPU.Cores)
	}
	if want := uint64(28000 + 9600 + 1200 + 200); st.CPU.Total != want {
		t.Errorf("Total тиков = %d, want %d", st.CPU.Total, want)
	}
	if !st.PSIPresent || st.CPUPSI.Avg10 != 91.59 {
		t.Errorf("CPU PSI = %+v present=%v", st.CPUPSI, st.PSIPresent)
	}
	if st.MemPSISome.Avg10 != 7.85 || st.MemPSIFull.Avg10 != 2.00 {
		t.Errorf("mem PSI some/full = %+v / %+v", st.MemPSISome, st.MemPSIFull)
	}
	if st.MemTotal != 7973728*1024 || st.MemAvailable != 2359296*1024 {
		t.Errorf("meminfo total/available = %d/%d", st.MemTotal, st.MemAvailable)
	}
	if st.SwapTotal != 3932160*1024 || st.SwapFree != 3729408*1024 {
		t.Errorf("swap total/free = %d/%d", st.SwapTotal, st.SwapFree)
	}

	// Процессы: агрегаты по имени — headless-shell слит (2 процесса),
	// кривое имя postgres с пробелами/скобками разобрано целиком.
	if got := st.Procs["monitor"].Ticks; got != 400 {
		t.Errorf("monitor тики = %d, want 400", got)
	}
	if got := st.Procs["headless-shell"]; got.Ticks != 2000 || got.Count != 2 {
		t.Errorf("headless-shell = %+v, want тики 2000, процессов 2", got)
	}
	if got := st.Procs["postgres: 1[2] writer"].Ticks; got != 1000 {
		t.Errorf("postgres тики = %d, want 1000", got)
	}
	if _, ok := st.Procs["acpi"]; ok {
		t.Error("не-PID каталог попал в процессы")
	}
}

func TestOSLoad_NoPressureFiles(t *testing.T) {
	root := loadFixtures(t)
	// Ядро без PSI: файлы давления отсутствуют — снимок жив, PSI «н/д».
	if err := os.RemoveAll(filepath.Join(root, "pressure")); err != nil {
		t.Fatal(err)
	}
	st, err := OSLoad{Root: root}.Load(context.Background())
	if err != nil {
		t.Fatalf("Load без PSI: %v", err)
	}
	if st.PSIPresent {
		t.Error("PSIPresent = true без файлов давления")
	}
}

func TestOSLoad_BrokenMandatory(t *testing.T) {
	root := loadFixtures(t)
	if err := os.Remove(filepath.Join(root, "meminfo")); err != nil {
		t.Fatal(err)
	}
	if _, err := (OSLoad{Root: root}).Load(context.Background()); err == nil {
		t.Error("Load без meminfo: хочу ошибку прохода")
	}
}

func TestCPUDelta(t *testing.T) {
	prev := CPUStat{User: 1000, System: 500, Idle: 500, Total: 2000, Cores: 4}
	cur := CPUStat{User: 1200, System: 700, Idle: 600, Total: 2500, Cores: 4}
	busy, user, sys, io, ok := cpuDelta(prev, cur)
	if !ok {
		t.Fatal("delta не посчитана на растущих счётчиках")
	}
	// dTotal=500: user 200/500=0.4, sys 200/500=0.4, idle 100/500=0.2,
	// iowait 0 → busy 0.8.
	if busy != 0.8 || user != 0.4 || sys != 0.4 || io != 0 {
		t.Errorf("delta = busy %v user %v sys %v io %v, want 0.8/0.4/0.4/0", busy, user, sys, io)
	}
	if _, _, _, _, ok := cpuDelta(CPUStat{}, cur); ok {
		t.Error("первый проход (prev.Total=0) не «н/д»")
	}
	if _, _, _, _, ok := cpuDelta(cur, cur); ok {
		t.Error("нулевая дельта не «н/д»")
	}
}

// fakeLoadSource — LoadSource с очередью снимков (кончилась — последний).
type fakeLoadSource struct {
	states []LoadStat
}

func (s *fakeLoadSource) Load(context.Context) (LoadStat, error) {
	if len(s.states) == 0 {
		return LoadStat{}, errors.New("источник исчерпан")
	}
	st := s.states[0]
	if len(s.states) > 1 {
		s.states = s.states[1:]
	}
	return st, nil
}

// fakeLoadQueues — QueueSource с заготовленным снимком.
type fakeLoadQueues struct {
	queues map[string]QueueStat
	err    error
}

func (s fakeLoadQueues) Queues(context.Context) (map[string]QueueStat, error) {
	return s.queues, s.err
}

// fakeLoadSpace — SpaceChecker с единственным ответом.
type fakeLoadSpace struct {
	stat SpaceStat
	err  error
}

func (s fakeLoadSpace) Space(string) (SpaceStat, error) { return s.stat, s.err }

func TestLoadReportLoop_OnePass(t *testing.T) {
	st, err := OSLoad{Root: loadFixtures(t)}.Load(context.Background())
	if err != nil {
		t.Fatalf("снимок: %v", err)
	}
	src := &fakeLoadSource{states: []LoadStat{st}}
	queues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 500},
		queue.StreamRender: {Len: 116320, Read: 1200},
	}
	var reports []string
	opts := LoadReportOptions{
		Interval: time.Minute,
		Queues:   fakeLoadQueues{queues: queues},
		Space:    fakeLoadSpace{stat: SpaceStat{Avail: 44 << 30, Total: 79 << 30}},
		Dirs:     []string{"/var/lib/crawler/content"},
		Notify:   func(text string) { reports = append(reports, text) },
	}

	// Отмена ДО старта: стартовый pass выполняется (снимок после рестарта
	// — сразу), цикл выходит по уже закрытому ctx — ровно один отчёт.
	// Дельты (утилизация, темпы) этим проходом не проверяются — их
	// считает formatLoadReport-тест ниже.
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
		t.Fatal("цикл не вышел по отмене")
	}
	if len(reports) != 1 {
		t.Fatalf("отчётов = %d, want 1 (стартовый проход)", len(reports))
	}
	r := reports[0]
	for _, want := range []string{
		"📊 Нагрузка: CPU н/д",           // дельты нет — база
		"🧮 Топ CPU: н/д",                // темпы процессов — тоже дельта
		"headless-shell 2.0ГиБ (2 пр.)", // топ памяти полноценен сразу
		"💾 RAM: занято 5.4 из 7.6 ГиБ",  // 7973728−2359296 КиБ
		"swap 198 МиБ",
		"📀 Диск /var/lib/crawler/content: свободно 44 из 79 ГиБ (56%)",
		"📬 Очереди: q:dns 121 · q:render 116320", // размеры без дельт
	} {
		if !strings.Contains(r, want) {
			t.Errorf("отчёт не содержит %q:\n%s", want, r)
		}
	}
}

func TestFormatLoadReport_Deltas(t *testing.T) {
	st, err := OSLoad{Root: loadFixtures(t)}.Load(context.Background())
	if err != nil {
		t.Fatalf("снимок: %v", err)
	}
	busy, io := 0.975, 0.0 // user/sys доли отчётом не печатаются — только busy/io
	procCPU := map[string]float64{
		"postgres":       1.9,
		"headless-shell": 1.15,
		"http-worker":    0.5,
		"analyzer":       0.31,
		"unbound":        0.01, // шум: ниже порога
	}
	queues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 2100},
		queue.StreamRender: {Len: 115120, Read: 1200},
	}
	rates := map[string]float64{
		queue.StreamDNS:    3.5,
		queue.StreamRender: 2.0,
	}
	prevQueues := map[string]QueueStat{
		queue.StreamDNS:    {Len: 121, Read: 0},
		queue.StreamRender: {Len: 116320, Read: 0},
	}

	debt := RenderDebt{Flags: 443000, Lag: 99120}
	r := formatLoadReport(st, &busy, &io, procCPU, queues, rates, true, prevQueues, nil, &debt)
	for _, want := range []string{
		"CPU 98% (io 0%)",
		"PSI 92% [5м 90%]",
		"load 24.8/22.0/20.7 из 4 ядер",
		"Топ CPU: postgres 1.9я, headless-shell 1.1я, http-worker 0.5я, analyzer 0.3я",
		"Топ память: headless-shell 2.0ГиБ (2 пр.)",
		"PSI mem 8% / full 2%",
		"q:dns 121 (±0, 3.5/с)",
		"q:render 115120 (−1200, 2.0/с)",
		"📬 Рендер-долг: флаги 443000 + лаг 99120 = 542120",
	} {
		if !strings.Contains(r, want) {
			t.Errorf("отчёт не содержит %q:\n%s", want, r)
		}
	}
	if strings.Contains(r, "unbound") {
		t.Errorf("шум топа CPU (unbound 0.01я) попал в отчёт:\n%s", r)
	}
}

func TestFormatLoadReport_Degraded(t *testing.T) {
	// Нет очередей и диска — «н/д» секции; PSI отсутствует — строки
	// давления просто короче.
	st := LoadStat{
		Load1: 1.0, Load5: 1.0, Load15: 1.0,
		CPU:      CPUStat{Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {RSS: 1 << 30}},
	}
	r := formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, nil, nil)
	for _, want := range []string{"CPU н/д", "Топ CPU: н/д", "📬 Очереди: н/д", "📀 Диск: н/д", "📬 Рендер-долг: н/д"} {
		if !strings.Contains(r, want) {
			t.Errorf("деградированный отчёт не содержит %q:\n%s", want, r)
		}
	}
	if strings.Contains(r, "PSI") {
		t.Errorf("PSI попал в отчёт без данных:\n%s", r)
	}

	// Источник долга независим от очередей (ревью T-175, P3-1): при
	// недоступных очередях строка долга всё равно печатается.
	debt := RenderDebt{Flags: 443000, Lag: 99120}
	r = formatLoadReport(st, nil, nil, nil, nil, nil, false, nil, nil, &debt)
	if !strings.Contains(r, "📬 Рендер-долг: флаги 443000 + лаг 99120 = 542120") {
		t.Errorf("долг не напечатан при н/д очередях:\n%s", r)
	}
}

func TestParseProcStatLine(t *testing.T) {
	// Имя со скобками и пробелами; utime/stime — 12/13 поля после имени.
	line := "203 (postgres: 1[2] writer) S 1 0 0 0 -1 0 0 0 0 0 500 500 0 0 20 0"
	if comm, ticks := parseProcStatLine([]byte(line)); comm != "postgres: 1[2] writer" || ticks != 1000 {
		t.Errorf("parseProcStatLine = %q %d", comm, ticks)
	}
	if comm, _ := parseProcStatLine([]byte("1 (short) R 1 2 3")); comm != "" {
		t.Errorf("короткая строка = %q, want пустой вклад", comm)
	}
}

func TestRedisQueues_NoClient(t *testing.T) {
	if _, err := (RedisQueues{}).Queues(context.Background()); err == nil {
		t.Error("клиент nil: хочу ошибку источника")
	}
}
