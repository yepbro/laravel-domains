// Периодический отчёт системной нагрузки оператору (T-173, ТЗ 5.9): цикл
// monitor.load_report_interval (по умолчанию 10 минут) собирает один
// снимок из четырёх источников и доставляет его в Telegram (Notify-хук —
// TelegramNotifier.Send, тот же бот и группа, что у алертов T-162) плюс
// gauge Prometheus crawler_os_* (на проде скрейпера нет — приборы к
// моменту его появления) и строку Info в журнал monitor.
//
// Состав снимка (волна оптимизации CPU/памяти T-158…T-172 — дневник
// «до/после» каждого изменения без ручных замеров):
//
//   - система: load average, утилизация CPU за окно цикла (дельта тиков
//     /proc/stat), PSI CPU/memory, /proc/meminfo (+swap);
//   - топ процессов «htop»-вида: тики utime+stime и RSS каждого процесса
//     /proc агрегируются по имени (comm) — 12 рендереров Chromium видны
//     одной строкой «headless-shell», бэкенды PostgreSQL — «postgres»;
//     CPU за окно — дельтой между проходами, в долях ядра (1.0 = ядро);
//   - диск: statfs каталогов отчёта (контент-каталог — главный потребитель
//     тома; SpaceChecker алертов disk_free_*, alerts.go);
//   - очереди: XLEN рабочих стримов и темп потребления — дельта
//     entries-read групп XINFO GROUPS (Redis 7+; ядро < 7 — темп «н/д»,
//     размеры честны).
//
// Первый проход после старта — база: утилизация и темпы «н/д» (дельт
// нет), размеры (RAM, диск, очереди, топ RSS) полноценны. Деградация
// окружения — как у PSI (psi.go): нет /proc/pressure — теряется только
// доля PSI; сломанные /proc/loadavg|meminfo — Warn прохода, цикл живёт;
// недоступный Redis/диск — секция «н/д», остальные честны. Источники —
// интерфейсы с подменяемым корнем /proc и фейками: юнит-тесты герметичны
// (стиль OSPSI.Path, StreamGroupsSource).
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// DefaultLoadReportInterval — период снимков нагрузки: 10 минут (T-173):
// окно ловит эпизоды голода CPU/памяти (десятки минут — инцидент
// 23–24.10), 144 сообщения/сутки не тонут алерты в группе оператора.
const DefaultLoadReportInterval = 10 * time.Minute

// topProcs — сколько имён процессов в строках топа отчёта (CPU и RSS).
const topProcs = 4

// minProcCPUShare — порог шума топа CPU: имена с долей ядра ниже не
// поднимаются в строку отчёта (сотни спящих процессов ядра).
const minProcCPUShare = 0.05

// LoadStat — снимок системы и процессов одного прохода. Поля памяти —
// байты; тики — единицы USER_HZ ядра (счётчики растут монотонно).
type LoadStat struct {
	// Load1/Load5/Load15 — load average (работа + uninterruptible).
	Load1  float64
	Load5  float64
	Load15 float64
	// CPU — счётчики тиков агрегированной строки cpu /proc/stat и число
	// логических ядер (строки cpuN).
	CPU CPUStat
	// CPUPSI — давление CPU some (psi.go: avg10/60/300).
	CPUPSI PSIStat
	// MemPSISome/MemPSIFull — давление памяти /proc/pressure/memory.
	MemPSISome PSIStat
	MemPSIFull PSIStat
	// PSIPresent — файлы /proc/pressure/* доступны: false — доли PSI в
	// отчёте «н/д» (деградация как у алерта psi_cpu, T-162).
	PSIPresent bool
	// MemTotal, MemAvailable — /proc/meminfo, байты.
	MemTotal     uint64
	MemAvailable uint64
	// SwapTotal, SwapFree — /proc/meminfo, байты (0/0 — без свопа).
	SwapTotal uint64
	SwapFree  uint64
	// Procs — процессы, агрегированные по comm (имя исполняемого):
	// тики CPU (utime+stime) и суммарный RSS, байты. «htop-вид» отчёта.
	Procs map[string]ProcAgg
}

// ProcAgg — агрегат процессов одного имени (comm).
type ProcAgg struct {
	// Ticks — сумма тиков CPU (utime+stime) процессов имени.
	Ticks uint64
	// RSS — суммарный резидентный размер, байты.
	RSS uint64
}

// CPUStat — счётчики тиков CPU (/proc/stat): busy-доля прохода считается
// дельтой между снимками (Idle+Iowait — непроизводительные тики).
type CPUStat struct {
	User    uint64 // user (guest уже исключён ядром из user)
	Nice    uint64
	System  uint64
	Idle    uint64
	Iowait  uint64
	Irq     uint64
	Softirq uint64
	Steal   uint64
	Total   uint64 // сумма всех полей выше
	Cores   int    // строки cpuN (логические ядра)
}

// QueueStat — состояние одного рабочего стрима: размер и потребление.
type QueueStat struct {
	// Len — глубина стрима (XLEN).
	Len int64
	// Read — доставки группым (entries-read XINFO GROUPS, максимум по
	// группам): монотонный счётчик потребления для темпа отчёта;
	// 0 — групп нет или Redis < 7 (поле отсутствует) — темп «н/д».
	Read int64
}

// reportStreams — стримы очередей отчёта: все пять, включая карантин
// q:dead (его глубина — инвентарь оператора, ТЗ 4.2). Порядок — конвейер:
// q:dns → q:http → q:render → q:analyze → q:dead.
var reportStreams = []string{
	queue.StreamDNS, queue.StreamHTTP, queue.StreamRender, queue.StreamAnalyze, queue.StreamDead,
}

// LoadSource — источник снимка системы и процессов; интерфейс для
// подмены в юнит-тестах (реальное чтение — /proc хоста). Стиль PSISource.
type LoadSource interface {
	// Load возвращает снимок. Ошибка — «не измеряется» с причиной.
	Load(ctx context.Context) (LoadStat, error)
}

// OSLoad — LoadSource поверх /proc: Root — корень псевдофса ("" —
// "/proc"); тесты подменяют временным каталогом с фикстурами (loadavg,
// stat, meminfo, pressure/cpu, pressure/memory, PID-каталоги) — чтение
// настоящего /proc в тестах негерметично (нагрузка хоста CI меняется
// между запусками). Стиль OSPSI.Path.
type OSLoad struct {
	Root string
}

// compile-time: OSLoad — LoadSource.
var _ LoadSource = OSLoad{}

// Load снимает loadavg, счётчики CPU, PSI, память и процессы.
func (s OSLoad) Load(ctx context.Context) (LoadStat, error) {
	if err := ctx.Err(); err != nil {
		return LoadStat{}, err
	}
	root := s.Root
	// mutator-disable-next-line branch/if
	if root == "" {
		// mutator-disable-next-line statement/remove
		root = "/proc"
	}

	var st LoadStat
	l1, l5, l15, err := readLoadAvg(root + "/loadavg")
	if err != nil {
		return LoadStat{}, err
	}
	st.Load1, st.Load5, st.Load15 = l1, l5, l15

	if st.CPU, err = readCPUStat(root + "/stat"); err != nil {
		return LoadStat{}, err
	}

	// PSI — опционально: ядро без CONFIG_PSI (errNoPSI у psi.go) теряет
	// только долю давления, проход жив.
	if b, err := os.ReadFile(root + "/pressure/cpu"); err == nil {
		if st.CPUPSI, err = parsePSI(string(b)); err != nil {
			return LoadStat{}, fmt.Errorf("monitor: нагрузка: %w", err)
		}
		st.PSIPresent = true
	}
	if b, err := os.ReadFile(root + "/pressure/memory"); err == nil {
		if st.MemPSISome, err = parsePSIKind(string(b), "some"); err != nil {
			return LoadStat{}, fmt.Errorf("monitor: нагрузка: память PSI: %w", err)
		}
		if st.MemPSIFull, err = parsePSIKind(string(b), "full"); err != nil {
			return LoadStat{}, fmt.Errorf("monitor: нагрузка: память PSI: %w", err)
		}
		st.PSIPresent = true
	}

	st.MemTotal, st.MemAvailable, st.SwapTotal, st.SwapFree, err = readMemInfo(root + "/meminfo")
	if err != nil {
		return LoadStat{}, err
	}

	st.Procs, err = readProcs(root)
	if err != nil {
		return LoadStat{}, err
	}
	return st, nil
}

// readLoadAvg разбирает /proc/loadavg: «24.82 21.96 20.67 3/943 1588621».
func readLoadAvg(path string) (float64, float64, float64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("monitor: нагрузка: loadavg: %w", err)
	}
	fields := strings.Fields(string(b))
	if len(fields) < 3 {
		return 0, 0, 0, fmt.Errorf("monitor: нагрузка: loadavg: строка короче трёх чисел: %q", strings.TrimSpace(string(b)))
	}
	var v [3]float64
	for i := range v {
		n, err := strconv.ParseFloat(fields[i], 64)
		if err != nil || n < 0 {
			return 0, 0, 0, fmt.Errorf("monitor: нагрузка: loadavg: %q не число", fields[i])
		}
		v[i] = n
	}
	return v[0], v[1], v[2], nil
}

// readCPUStat разбирает агрегированную строку cpu и считает строки cpuN
// /proc/stat: «cpu  u n s idle iowait irq softirq steal …» (хвостовые
// guest/guest_nice в user не входят — ядро их уже вычло; длина строки
// между версиями ядра гуляет — отсутствующие поля читаются нулями).
func readCPUStat(path string) (CPUStat, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return CPUStat{}, fmt.Errorf("monitor: нагрузка: /proc/stat: %w", err)
	}
	var st CPUStat
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case strings.HasPrefix(line, "cpu "):
			fields := strings.Fields(line)
			if len(fields) < 5 {
				return CPUStat{}, fmt.Errorf("monitor: нагрузка: строка cpu короче счётчиков: %q", line)
			}
			var c [8]uint64
			for i, f := range fields[1:] {
				if i >= len(c) {
					break // guest-тики в busy-долю не входят
				}
				n, err := strconv.ParseUint(f, 10, 64)
				if err != nil {
					return CPUStat{}, fmt.Errorf("monitor: нагрузка: тик CPU %q не число", f)
				}
				c[i] = n
			}
			st = CPUStat{
				User: c[0], Nice: c[1], System: c[2], Idle: c[3],
				Iowait: c[4], Irq: c[5], Softirq: c[6], Steal: c[7],
			}
			st.Total = st.User + st.Nice + st.System + st.Idle +
				st.Iowait + st.Irq + st.Softirq + st.Steal
		case strings.HasPrefix(line, "cpu"):
			st.Cores++
		}
	}
	if st.Total == 0 || st.Cores == 0 {
		return CPUStat{}, errors.New("monitor: нагрузка: /proc/stat без строки cpu/cpuN")
	}
	return st, nil
}

// readMemInfo разбирает /proc/meminfo: килобайты ядра (kB = 1024 Б) →
// байты. MemTotal/MemAvailable обязательны; отсутствующий своп — не
// ошибка: 0/0 (без свопа).
func readMemInfo(path string) (total, available, swapTotal, swapFree uint64, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("monitor: нагрузка: meminfo: %w", err)
	}
	fields := make(map[string]uint64)
	for _, line := range strings.Split(string(b), "\n") {
		name, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			continue
		}
		n, perr := strconv.ParseUint(f[0], 10, 64)
		if perr != nil {
			return 0, 0, 0, 0, fmt.Errorf("monitor: нагрузка: meminfo: %q не число", f[0])
		}
		// Единица — всегда kB, множитель 1024; значения meminfo умещаются
		// в uint64 без переполнения.
		fields[strings.TrimSpace(name)] = n * 1024
	}
	total, okT := fields["MemTotal"]
	available, okA := fields["MemAvailable"]
	if !okT || !okA || total == 0 {
		return 0, 0, 0, 0, errors.New("monitor: нагрузка: meminfo без MemTotal/MemAvailable")
	}
	swapTotal, swapFree = fields["SwapTotal"], fields["SwapFree"]
	return total, available, swapTotal, swapFree, nil
}

// readProcs агрегирует процессы по comm: тики CPU (utime+stime из
// /proc/PID/stat) и RSS (resident из /proc/PID/statm × размер страницы).
// Исчезнувший между листингом и чтением процесс — пустой вклад (гонка
// жизней процессов нормальна: соседние проходы дают цельные дельты).
func readProcs(root string) (map[string]ProcAgg, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("monitor: нагрузка: процессы: %w", err)
	}
	page := uint64(os.Getpagesize())
	procs := make(map[string]ProcAgg, 64)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name[0] < '0' || name[0] > '9' {
			continue // не PID-каталог
		}
		b, err := os.ReadFile(root + "/" + name + "/stat")
		if err != nil {
			continue
		}
		comm, ticks := parseProcStatLine(b)
		if comm == "" {
			continue
		}
		var rss uint64
		if m, err := os.ReadFile(root + "/" + name + "/statm"); err == nil {
			rss = parseStatmRSS(m) * page
		}
		agg := procs[comm]
		agg.Ticks += ticks
		agg.RSS += rss
		procs[comm] = agg
	}
	return procs, nil
}

// parseProcStatLine — comm и тики CPU (utime+stime) из строки
// /proc/PID/stat. Имя заключено в скобки и может содержать пробелы и
// скобки: comm — от ПЕРВОЙ «(» до ПОСЛЕДНЕЙ «)» (стандартный разбор
// procfs), поля — после «)». Позиции полей после имени: 0 state …
// 11 utime, 12 stime (man proc_pid_stat); короткая/кривая строка —
// пустой вклад.
func parseProcStatLine(b []byte) (string, uint64) {
	s := string(b)
	open := strings.IndexByte(s, '(')
	close_ := strings.LastIndexByte(s, ')')
	if open < 0 || close_ <= open {
		return "", 0
	}
	comm := s[open+1 : close_]
	fields := strings.Fields(s[close_+1:])
	if comm == "" || len(fields) < 13 {
		return "", 0
	}
	utime, err1 := strconv.ParseUint(fields[11], 10, 64)
	stime, err2 := strconv.ParseUint(fields[12], 10, 64)
	if err1 != nil || err2 != nil {
		return "", 0
	}
	return comm, utime + stime
}

// parseStatmRSS — резидентные страницы (второе поле statm, man
// proc_pid_statm); кривая строка — 0 (RSS процесса неизвестен, тики
// остаются честными).
func parseStatmRSS(b []byte) uint64 {
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0
	}
	n, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// ---------------------------------------------------------------- очереди

// QueueSource — источник состояния стримов очередей для отчёта;
// интерфейс для подмены в юнит-тестах (реальное чтение — XLEN/XINFO
// живого Redis). Стиль StreamGroupsSource.
type QueueSource interface {
	// Queues возвращает снимок стримов отчёта (reportStreams): размер и
	// счётчик доставок. Ошибка — «не измеряется» с причиной.
	Queues(ctx context.Context) (map[string]QueueStat, error)
}

// RedisQueues — QueueSource поверх XLEN + XINFO GROUPS того же узкого
// окна go-redis, что у групп алертов (redisGroupsCmd, streamgroups.go).
type RedisQueues struct {
	// RDB — клиент Redis; nil — ошибка источника.
	RDB redisGroupsCmd
}

// compile-time: RedisQueues — QueueSource.
var _ QueueSource = RedisQueues{}

// Queues обходит стримы отчёта. Стрим без ключа (очередь пуста отродясь)
// — вклад {0, 0}: пустая очередь — честный нуль, а не отсутствие данных.
// Групп несколько быть может (консьюмеры раздельно) — счётчик доставок
// берётся максимумом по группам: одна группа делит общий entries-read,
// независимые группы читают стрим каждая со своей скоростью.
func (s RedisQueues) Queues(ctx context.Context) (map[string]QueueStat, error) {
	if s.RDB == nil {
		return nil, errors.New("monitor: нагрузка: очереди: клиент Redis не задан")
	}
	out := make(map[string]QueueStat, len(reportStreams))
	for _, stream := range reportStreams {
		length, err := s.RDB.XLen(ctx, stream).Result()
		if err != nil {
			// Стрима нет — Redis отвечает «no such key» (та же ветка, что
			// у BackpressureGate.Len и RedisStreamGroups).
			if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
				out[stream] = QueueStat{}
				continue
			}
			return nil, fmt.Errorf("monitor: нагрузка: очереди: %s: %w", stream, err)
		}
		st := QueueStat{Len: length}
		// Группы — источник темпа; стрим есть, групп нет (никто не
		// потребляет) — Read 0, размер честен; ошибка XINFO GROUPS здесь
		// не роняет секцию: глубина стрима важнее темпа (алерт T-162
		// следит за группами отдельно и со своими семантиками).
		if groups, err := s.RDB.XInfoGroups(ctx, stream).Result(); err == nil {
			for _, g := range groups {
				if g.EntriesRead > st.Read {
					st.Read = g.EntriesRead
				}
			}
		}
		out[stream] = st
	}
	return out, nil
}

// ---------------------------------------------------------------- метрики

// Значения метки kind метрик нагрузки.
const (
	osKindLoad1  = "1m"
	osKindLoad5  = "5m"
	osKindLoad15 = "15m"

	osKindCPUBusy   = "busy"
	osKindCPUUser   = "user"
	osKindCPUSys    = "system"
	osKindCPUIowait = "iowait"

	osKindMemTotal     = "total"
	osKindMemAvailable = "available"
	osKindMemUsed      = "used"

	osKindSwapTotal = "total"
	osKindSwapUsed  = "used"
)

// LoadMetrics — приборы снимка нагрузки (crawler_os_*): пишет успешный
// проход цикла (hold-last не нужен — стиль psimetrics.go). Топ
// процессов, диск и очереди gauge'ами не дублируются: их публикуют
// свои контуры (crawler_queue_depth у воркеров, алерты disk_free_*),
// либо они принадлежат только отчёту (процессы).
type LoadMetrics struct {
	loadavg *prometheus.GaugeVec
	cpu     *prometheus.GaugeVec
	memPSI  *prometheus.GaugeVec
	mem     *prometheus.GaugeVec
	swap    *prometheus.GaugeVec
}

// NewLoadMetrics — приборы в реестре reg (nil — nil, цикл без метрик:
// шов тестов, прецедент newCollectorMetrics).
func NewLoadMetrics(reg *metrics.Registry) *LoadMetrics {
	if reg == nil {
		return nil
	}
	return &LoadMetrics{
		loadavg: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_os_loadavg",
			Help: "Load average хоста из /proc/loadavg (T-173): kind=1m/5m/15m; сравнивается с числом ядер.",
		}, metrics.LabelKind),
		cpu: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_os_cpu_percent",
			Help: "Утилизация CPU хоста, % за окно цикла отчёта нагрузки (дельта /proc/stat; T-173): kind=busy/user/system/iowait. Первый проход после старта — приборы не пишутся (нет дельты).",
		}, metrics.LabelKind),
		memPSI: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_os_mem_psi_percent",
			Help: "Давление памяти по PSI, % из /proc/pressure/memory (T-173): kind=some_avg10/some_avg300/full_avg10/full_avg300. Ядро без PSI — приборы не пишутся.",
		}, metrics.LabelKind),
		mem: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_os_mem_bytes",
			Help: "Память хоста из /proc/meminfo, байты (T-173): kind=total/available/used (used = total − available).",
		}, metrics.LabelKind),
		swap: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_os_swap_bytes",
			Help: "Своп хоста из /proc/meminfo, байты (T-173): kind=total/used.",
		}, metrics.LabelKind),
	}
}

// observe публикует снимок (утилизация — доли 0..1 или nil, когда дельты
// ещё нет; nil-приёмник — no-op).
func (m *LoadMetrics) observe(st LoadStat, busy, user, sys, iowait *float64) {
	if m == nil {
		return
	}
	m.loadavg.WithLabelValues(osKindLoad1).Set(st.Load1)
	m.loadavg.WithLabelValues(osKindLoad5).Set(st.Load5)
	m.loadavg.WithLabelValues(osKindLoad15).Set(st.Load15)
	if busy != nil {
		m.cpu.WithLabelValues(osKindCPUBusy).Set(*busy * 100)
		m.cpu.WithLabelValues(osKindCPUUser).Set(*user * 100)
		m.cpu.WithLabelValues(osKindCPUSys).Set(*sys * 100)
		m.cpu.WithLabelValues(osKindCPUIowait).Set(*iowait * 100)
	}
	if st.PSIPresent {
		m.memPSI.WithLabelValues("some_avg10").Set(st.MemPSISome.Avg10)
		m.memPSI.WithLabelValues("some_avg300").Set(st.MemPSISome.Avg300)
		m.memPSI.WithLabelValues("full_avg10").Set(st.MemPSIFull.Avg10)
		m.memPSI.WithLabelValues("full_avg300").Set(st.MemPSIFull.Avg300)
	}
	m.mem.WithLabelValues(osKindMemTotal).Set(float64(st.MemTotal))
	m.mem.WithLabelValues(osKindMemAvailable).Set(float64(st.MemAvailable))
	m.mem.WithLabelValues(osKindMemUsed).Set(float64(st.MemTotal - st.MemAvailable))
	m.swap.WithLabelValues(osKindSwapTotal).Set(float64(st.SwapTotal))
	m.swap.WithLabelValues(osKindSwapUsed).Set(float64(st.SwapTotal - st.SwapFree))
}

// cpuDelta — доли тиков между двумя снимками (0..1); ok=false — дельта
// невозможна (первый проход или счётчики сброшены рестартом хоста).
func cpuDelta(prev, cur CPUStat) (busy, user, sys, iowait float64, ok bool) {
	d := cur.Total - prev.Total
	if prev.Total == 0 || d == 0 {
		return 0, 0, 0, 0, false
	}
	user = float64(cur.User-prev.User) / float64(d)
	sys = float64(cur.System-prev.System) / float64(d)
	iowait = float64(cur.Iowait-prev.Iowait) / float64(d)
	idle := float64(cur.Idle-prev.Idle) / float64(d)
	busy = 1 - idle - iowait
	if busy < 0 {
		busy = 0
	}
	return busy, user, sys, iowait, true
}

// ---------------------------------------------------------------- цикл

// LoadReportOptions — параметры цикла; Interval <= 0 выключает цикл
// (в отличие от withDefaults-циклов мониторинга: у отчёта нет «верного»
// периода — выбор оператора, конфиг monitor.load_report_interval).
type LoadReportOptions struct {
	// Interval — период снимков (DefaultLoadReportInterval).
	Interval time.Duration
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — реестр сервиса; nil — без метрик.
	Metrics *metrics.Registry
	// Notify — получатель отчёта оператором (Telegram-доставка T-162);
	// nil — отчёт только в журнал. Вызывается в горутине прохода —
	// обязан быть безопасен для конкурентного вызова (TelegramNotifier.Send).
	Notify func(text string)
	// Queues — источник состояния стримов; nil — секция очередей «н/д».
	Queues QueueSource
	// Space — источник места каталогов (SpaceChecker алертов disk_free_*,
	// alerts.go); nil — секция диска «н/д».
	Space SpaceChecker
	// Dirs — каталоги секции диска (обычно один — контент: главный
	// потребитель тома; пусто при Space ≠ nil — секция пустая).
	Dirs []string
}

// loadPassState — дельта-база прохода: счётчики предыдущего снимка
// (проходы последовательны в одной горутине — мьютекс не нужен).
type loadPassState struct {
	cpu     CPUStat
	ticks   map[string]uint64 // comm → тики прошлого прохода
	queues  map[string]QueueStat
	at      time.Time // момент прошлого прохода (окно темпов)
	started bool      // база снята (первый проход — темпы «н/д»)
}

// DiskLine — строка секции диска отчёта.
type DiskLine struct {
	Dir   string
	Avail uint64
	Total uint64
}

// LoadReportLoop — цикл отчётов нагрузки (T-173): проход сразу при
// старте (свежий снимок после рестарта; дельты — «н/д») и далее по
// ticker. Ошибка источника — Warn, цикл живёт (контракт OnRun, ТЗ 3.4):
// очередной проход повторит; частичные сбои (Redis недоступен) роняют
// только свою секцию. Возвращает nil по отмене ctx (graceful shutdown).
func LoadReportLoop(ctx context.Context, src LoadSource, opts LoadReportOptions) error {
	if src == nil {
		return errors.New("monitor: отчёт нагрузки: источник не задан")
	}
	if opts.Interval <= 0 {
		return nil // выключен конфигурацией (monitor.load_report_interval)
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	gauges := NewLoadMetrics(opts.Metrics)
	log.Info("цикл отчётов системной нагрузки",
		"interval", opts.Interval.String(), "queues", opts.Queues != nil, "dirs", len(opts.Dirs))

	var prev loadPassState
	pass := func() {
		st, err := src.Load(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("снимок системы", "err", err)
			}
			return
		}
		var busyP, userP, sysP, ioP *float64
		if busy, user, sys, io, ok := cpuDelta(prev.cpu, st.CPU); ok {
			busyP, userP, sysP, ioP = &busy, &user, &sys, &io
		}

		// Топ процессов: доли ядра — дельта тиков имени к дельте одного
		// ядра (delta_total/cores); сумма по именам ≈ busy × ядра.
		procCPU := make(map[string]float64, len(st.Procs))
		if d := st.CPU.Total - prev.cpu.Total; prev.started && d > 0 {
			if perCore := float64(d) / float64(st.CPU.Cores); perCore > 0 {
				for name, agg := range st.Procs {
					if dt := agg.Ticks - prev.ticks[name]; dt > 0 {
						procCPU[name] = float64(dt) / perCore
					}
				}
			}
		}

		// Очереди: размеры сейчас, темпы — дельты deliveries за окно.
		var queues map[string]QueueStat
		var rates map[string]float64
		if opts.Queues != nil {
			if q, err := opts.Queues.Queues(ctx); err != nil {
				if ctx.Err() == nil {
					log.Warn("снимок очередей", "err", err)
				}
			} else {
				queues = q
				window := time.Since(prev.at).Seconds()
				if prev.started && window > 0 {
					rates = make(map[string]float64, len(q))
					for stream, cur := range q {
						if p, ok := prev.queues[stream]; ok && cur.Read >= p.Read {
							rates[stream] = float64(cur.Read-p.Read) / window
						}
					}
				}
			}
		}

		// Диск: statfs каталогов (SpaceChecker алертов).
		var disks []DiskLine
		if opts.Space != nil {
			for _, dir := range opts.Dirs {
				space, err := opts.Space.Space(dir)
				if err != nil {
					if ctx.Err() == nil {
						log.Warn("снимок места", "dir", dir, "err", err)
					}
					continue
				}
				disks = append(disks, DiskLine{Dir: dir, Avail: space.Avail, Total: space.Total})
			}
		}

		gauges.observe(st, busyP, userP, sysP, ioP)

		report := formatLoadReport(st, busyP, ioP, procCPU, queues, rates, prev.started, prev.queues, disks)
		log.Info("отчёт нагрузки", "report", strings.ReplaceAll(report, "\n", " | "))
		if opts.Notify != nil {
			opts.Notify(report)
		}

		// База следующего прохода — после успешного построения отчёта:
		// сбойный проход дельты не сдвигает (окно темпов честно шире).
		ticks := make(map[string]uint64, len(st.Procs))
		for name, agg := range st.Procs {
			ticks[name] = agg.Ticks
		}
		prev = loadPassState{cpu: st.CPU, ticks: ticks, queues: queues, at: time.Now(), started: true}
	}

	pass()
	ticker := time.NewTicker(opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// formatLoadReport — компактный отчёт оператору (Telegram/журнал), до
// шести строк: система, топ CPU, топ RSS, память, диск, очереди.
// nil-доля утилизации и пустые карты — «н/д» секций. Гибибайты: meminfo
// в кибибайтах, деление на 2³⁰ без «десятых гигабайта». Доля ядра
// процессов: 1.0 = одно ядро целиком (масштаб htop).
func formatLoadReport(st LoadStat, busy, iowait *float64, procCPU map[string]float64,
	queues map[string]QueueStat, rates map[string]float64, prevStarted bool,
	prevQueues map[string]QueueStat, disks []DiskLine) string {
	var b strings.Builder
	b.WriteString("📊 Нагрузка: CPU ")
	if busy != nil {
		fmt.Fprintf(&b, "%d%% (io %d%%)", round(*busy*100), round(*iowait*100))
	} else {
		b.WriteString("н/д")
	}
	if st.PSIPresent {
		fmt.Fprintf(&b, ", PSI %d%% [5м %d%%]", round(st.CPUPSI.Avg10), round(st.CPUPSI.Avg60))
	}
	fmt.Fprintf(&b, "; load %.1f/%.1f/%.1f из %d ядер\n",
		st.Load1, st.Load5, st.Load15, st.CPU.Cores)

	b.WriteString("🧮 Топ CPU: ")
	if top := topByCPU(procCPU); len(top) == 0 {
		b.WriteString("н/д")
	} else {
		for i, it := range top {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%s %.1fя", it.name, it.value)
		}
	}

	b.WriteString("\n🧮 Топ RSS: ")
	if top := topByRSS(st.Procs); len(top) == 0 {
		b.WriteString("н/д")
	} else {
		for i, it := range top {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%s %.1fГиБ", it.name, gib(uint64(it.value)))
		}
	}

	fmt.Fprintf(&b, "\n💾 RAM: занято %.1f из %.1f ГиБ (доступно %.1f), swap %.0f МиБ",
		gib(st.MemTotal-st.MemAvailable), gib(st.MemTotal), gib(st.MemAvailable),
		mib(st.SwapTotal-st.SwapFree))
	if st.PSIPresent {
		fmt.Fprintf(&b, "; PSI mem %d%% / full %d%%",
			round(st.MemPSISome.Avg10), round(st.MemPSIFull.Avg10))
	}

	if len(disks) == 0 {
		b.WriteString("\n📀 Диск: н/д")
	} else {
		for _, d := range disks {
			pct := 0
			if d.Total > 0 {
				pct = round(float64(d.Avail) / float64(d.Total) * 100)
			}
			fmt.Fprintf(&b, "\n📀 Диск %s: свободно %.0f из %.0f ГиБ (%d%%)",
				d.Dir, gib(d.Avail), gib(d.Total), pct)
		}
	}

	if queues == nil {
		b.WriteString("\n📬 Очереди: н/д")
	} else {
		b.WriteString("\n📬 Очереди: ")
		first := true
		for _, stream := range reportStreams {
			q, ok := queues[stream]
			if !ok {
				continue
			}
			if !first {
				b.WriteString(" · ")
			}
			first = false
			fmt.Fprintf(&b, "%s %d", stream, q.Len)
			// Динамика окна: ±глубина и темп доставок; первый проход —
			// только размер (дельты нет).
			if p, ok := prevQueues[stream]; ok && prevStarted {
				switch d := q.Len - p.Len; {
				case d > 0:
					fmt.Fprintf(&b, " (+%d", d)
				case d < 0:
					fmt.Fprintf(&b, " (−%d", -d)
				default:
					b.WriteString(" (±0")
				}
				if rate := rates[stream]; rate > 0 {
					fmt.Fprintf(&b, ", %.1f/с", rate)
				}
				b.WriteString(")")
			}
		}
	}
	return b.String()
}

// topItem — строка топа: value — доля ядра (CPU) или байты (RSS).
type topItem struct {
	name  string
	value float64
}

// topByCPU — topProcs имён по доле ядра, шум ниже minProcCPUShare не
// поднимается.
func topByCPU(m map[string]float64) []topItem {
	items := make([]topItem, 0, len(m))
	for name, v := range m {
		if v < minProcCPUShare {
			continue
		}
		items = append(items, topItem{name, v})
	}
	return sortTop(items)
}

// topByRSS — topProcs имён по суммарному RSS.
func topByRSS(m map[string]ProcAgg) []topItem {
	items := make([]topItem, 0, len(m))
	for name, agg := range m {
		if agg.RSS == 0 {
			continue
		}
		items = append(items, topItem{name, float64(agg.RSS)})
	}
	return sortTop(items)
}

// sortTop — значение по убыванию, имя — по возрастанию при равенстве
// (детерминированный отчёт), срез до topProcs.
func sortTop(items []topItem) []topItem {
	slices.SortFunc(items, func(a, b topItem) int {
		if a.value != b.value {
			if a.value > b.value {
				return -1
			}
			return 1
		}
		return strings.Compare(a.name, b.name)
	})
	if len(items) > topProcs {
		items = items[:topProcs]
	}
	return items
}

// gib/mib — байты в гиби-/мебибайты для отчёта.
func gib(b uint64) float64 { return float64(b) / (1 << 30) }
func mib(b uint64) float64 { return float64(b) / (1 << 20) }

// round — округление к целому % (отчёт оператору, не метрика).
func round(v float64) int {
	if v < 0 {
		return 0
	}
	return int(v + 0.5)
}
