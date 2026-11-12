// The periodic system load report to the operator (T-173, spec 5.9): the
// monitor.load_report_interval loop (10 minutes by default) collects one
// snapshot from four sources and delivers it to Telegram (a Notify hook —
// TelegramNotifier.Send, the same bot and group as the T-162 alerts) plus
// the crawler_os_* Prometheus gauges (there is no scraper in production — the gauges are for
// when it appears) and an Info line into the monitor journal.
//
// The snapshot composition (the CPU/memory optimization wave T-158…T-172 — a diary
// of "before/after" for every change without manual measurements):
//
//   - system: load average, the CPU utilization over the loop window (a delta of
//     /proc/stat ticks), PSI CPU/memory, /proc/meminfo (+swap);
//   - the "htop-style" process top: the utime+stime ticks and the RSS of each
//     /proc process are aggregated by name (comm) — 12 Chromium renderers are visible
// as one "headless-shell" line, the PostgreSQL backends — "postgres";
//     the CPU over the window — by the delta between passes, in core fractions (1.0 = a core);
//     a name whose processes restarted over the window (the tick sum fell)
//     is skipped until the next window — the name's ticks are non-monotonic (T-189);
//   - disk: the statfs of the report directories (the content directory — the main consumer
//     of the volume; the SpaceChecker of the disk_free_* alerts, alerts.go);
//   - queues: the depth of the working streams — the backlog of the consumer groups
//     (min(lag, XLEN) + pending, a shared calculation with the gate — queue.GroupDepth;
//     the same figure as `crawlerctl status` and `crawler_queue_depth`, T-188;
//     a stream without groups and the q:dead quarantine — XLEN) and the consumption rate —
//     the delta of the entries-read of the XINFO GROUPS groups (Redis 7+; a core < 7 — the rate
//     is "n/a", the depths are honest) and the render debt on one line (T-175: the
//     render_pending flags + the q:render lag, renderdebt.go).
//
// The first pass after the start — the base: the utilization and rates are "n/a" (no
// deltas); the sizes (RAM, disk, queues, the RSS top) are full. An environment
// degradation — like PSI (psi.go): no /proc/pressure — only the PSI share is lost;
// broken /proc/loadavg|meminfo — a Warn of the pass, the loop keeps living;
// an unavailable Redis/disk — the section is "n/a", the others are honest. The sources are
// interfaces with a substitutable /proc root and fakes: the unit tests are hermetic
// (the OSPSI.Path, StreamGroupsSource style).
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

// DefaultLoadReportInterval — the load snapshot period: 10 minutes (T-173):
// the window catches CPU/memory starvation episodes (tens of minutes — the
// incident of Oct 23–24); 144 messages/day do not drown the alerts in the operator's group.
const DefaultLoadReportInterval = 10 * time.Minute

// topProcs — how many process names in the report top lines (CPU and RSS).
const topProcs = 4

// minProcCPUShare — the noise threshold of the CPU top: names with a core share below do
// not rise into a report line (hundreds of sleeping kernel processes).
const minProcCPUShare = 0.05

// LoadStat — the snapshot of the system and processes of one pass. The memory fields are
// bytes; the ticks — the kernel USER_HZ units (the counters grow monotonically).
type LoadStat struct {
	// Load1/Load5/Load15 — the load average (running + uninterruptible).
	Load1  float64
	Load5  float64
	Load15 float64
	// CPU — the tick counters of the aggregated cpu line of /proc/stat and the number
	// of logical cores (the cpuN lines).
	CPU CPUStat
	// CPUPSI — the CPU some pressure (psi.go: avg10/60/300).
	CPUPSI PSIStat
	// MemPSISome/MemPSIFull — the memory pressure of /proc/pressure/memory.
	MemPSISome PSIStat
	MemPSIFull PSIStat
	// PSIPresent — the /proc/pressure/* files are available: false — the PSI shares in
	// the report are "n/a" (a degradation like the psi_cpu alert, T-162).
	PSIPresent bool
	// MemTotal, MemAvailable — /proc/meminfo, bytes.
	MemTotal     uint64
	MemAvailable uint64
	// SwapTotal, SwapFree — /proc/meminfo, bytes (0/0 — without swap).
	SwapTotal uint64
	SwapFree  uint64
	// Procs — the processes aggregated by comm (the executable name):
	// the CPU ticks (utime+stime) and the total RSS, bytes. The "htop view" of the report.
	Procs map[string]ProcAgg
}

// ProcAgg — the aggregate of the processes of one name (comm).
type ProcAgg struct {
	// Ticks — the sum of the CPU ticks (utime+stime) of the name's processes.
	Ticks uint64
	// RSS — the total resident size, bytes. Shared memory is counted
	// in every process (30 postgres backends count the shared buffers
	// thirty times): the report prints Count next to it; the "sum" is read
	// with a caveat; PSS is unavailable — monitor (non-root) does not read
	// the smaps_rollup of foreign uids.
	RSS uint64
	// Count — the processes of the name (for the report).
	Count int
}

// CPUStat — the CPU tick counters (/proc/stat): the busy share of a pass is
// counted as the delta between snapshots (Idle+Iowait — unproductive ticks).
type CPUStat struct {
	User    uint64 // user (guest is already excluded from user by the kernel)
	Nice    uint64
	System  uint64
	Idle    uint64
	Iowait  uint64
	Irq     uint64
	Softirq uint64
	Steal   uint64
	Total   uint64 // the sum of all the fields above
	Cores   int    // the cpuN lines (logical cores)
}

// QueueStat — the state of one working stream: the depth and consumption.
type QueueStat struct {
	// Len — the stream depth: the backlog of the consumer groups — Σ over the groups
	// (min(lag, XLEN) + pending), the shared queue.GroupDepth calculation — the
	// same figure as `crawlerctl status` and `crawler_queue_depth` (T-188);
	// a stream without groups and the q:dead quarantine — XLEN. The raw XLEN for
	// working streams is not printed: the acknowledged history before the XTRIM —
	// is not a queue (the incident of Oct 25: q:render overstated ~5×).
	Len int64
	// Read — the group deliveries (entries-read of XINFO GROUPS, the maximum over
	// the groups): a monotonic consumption counter for the report rate;
	// 0 — no groups or Redis < 7 (the field is missing) — the rate is "n/a".
	Read int64
}

// reportStreams — the report queue streams: all six, including the render
// fallback queue q:render-fallback (T-237, FR-6) and the q:dead quarantine
// (its depth is the operator's inventory, spec 4.2). A single source —
// queue.AllStreams (S-P3-1); the order — the pipeline: q:dns → q:http →
// q:render → q:render-fallback → q:analyze → q:dead.
var reportStreams = queue.AllStreams()

// LoadSource — the source of the system and process snapshot; an interface for
// substitution in unit tests (the real read — the host /proc). The PSISource style.
type LoadSource interface {
	// Load returns the snapshot. An error — "not measured" with a reason.
	Load(ctx context.Context) (LoadStat, error)
}

// OSLoad — a LoadSource on top of /proc: Root — the pseudo-fs root ("" —
// "/proc"); the tests substitute a temporary directory with fixtures (loadavg,
// stat, meminfo, pressure/cpu, pressure/memory, PID directories) — reading
// the real /proc in tests is not hermetic (the CI host load changes
// between runs). The OSPSI.Path style.
type OSLoad struct {
	Root string
}

// compile-time: OSLoad is a LoadSource.
var _ LoadSource = OSLoad{}

// Load takes the loadavg, the CPU counters, PSI, memory and processes.
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

	// PSI — optional: a kernel without CONFIG_PSI (errNoPSI at psi.go) loses
	// only the pressure share; the pass lives.
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

// readLoadAvg parses /proc/loadavg: "24.82 21.96 20.67 3/943 1588621".
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

// readCPUStat parses the aggregated cpu line and counts the cpuN lines
// of /proc/stat: "cpu  u n s idle iowait irq softirq steal …" (the trailing
// guest/guest_nice are not included in user — the kernel has already subtracted them; the line
// length wanders between kernel versions — missing fields are read as zeros).
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
					break // guest ticks are not included in the busy share
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

// readMemInfo parses /proc/meminfo: kernel kilobytes (kB = 1024 B) →
// bytes. MemTotal/MemAvailable are required; a missing swap is not
// an error: 0/0 (no swap).
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
		// The unit is always kB, the multiplier 1024; the meminfo values fit
		// into uint64 without overflow.
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

// readProcs aggregates the processes by comm: the CPU ticks (utime+stime from
// /proc/PID/stat) and the RSS (resident from /proc/PID/statm × the page size).
// A process that vanished between the listing and the read — an empty contribution
// (the race of process lives is normal: adjacent passes give whole deltas).
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
			continue // not a PID directory
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
		agg.Count++
		procs[comm] = agg
	}
	return procs, nil
}

// parseProcStatLine — the comm and the CPU ticks (utime+stime) from the
// /proc/PID/stat line. The name is enclosed in parentheses and may contain spaces and
// parentheses: comm — from the FIRST "(" to the LAST ")" (the standard
// procfs parse); the fields — after ")". The field positions after the name: 0 state …
// 11 utime, 12 stime (man proc_pid_stat); a short/corrupt line —
// an empty contribution.
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

// parseStatmRSS — the resident pages (the second statm field, man
// proc_pid_statm); a corrupt line — 0 (the process RSS unknown; the
// ticks stay honest).
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

// ---------------------------------------------------------------- queues

// QueueSource — the source of the queue stream states for the report;
// an interface for substitution in unit tests (the real read — XLEN/XINFO
// of the live Redis). The StreamGroupsSource style.
type QueueSource interface {
	// Queues returns the snapshot of the report streams (reportStreams): the depth
	// (the group backlog, T-188) and the delivery counter. An error — "not measured"
	// with a reason.
	Queues(ctx context.Context) (map[string]QueueStat, error)
}

// RedisQueues — a QueueSource on top of XLEN + XINFO GROUPS of the same narrow
// go-redis window as the alert groups (redisGroupsCmd, streamgroups.go);
// the depth — the backlog of the consumer groups (queue.GroupDepth, T-188).
type RedisQueues struct {
	// RDB — the Redis client; nil — a source error.
	RDB redisGroupsCmd
}

// compile-time: RedisQueues is a QueueSource.
var _ QueueSource = RedisQueues{}

// Queues walks the report streams. A stream without a key (a queue empty from birth)
// — a contribution of {0, 0}: an empty queue is an honest zero, not missing data.
// The depth of a working stream with groups — the sum of its groups' backlogs
// (queue.GroupDepth — a shared calculation with the gate, `crawlerctl status` and
// `crawler_queue_depth`: the report has no right to diverge on whether there is
// work); a stream without groups and the q:dead quarantine (the depth is the
// operator's inventory; the quarantine has no groups by construction) — XLEN. There
// may be several groups (consumers separately) — the delivery counter
// is taken as the maximum over the groups: one group divides the shared entries-read,
// independent groups read the stream each at its own rate.
func (s RedisQueues) Queues(ctx context.Context) (map[string]QueueStat, error) {
	if s.RDB == nil {
		return nil, errors.New("monitor: нагрузка: очереди: клиент Redis не задан")
	}
	out := make(map[string]QueueStat, len(reportStreams))
	for _, stream := range reportStreams {
		xlen, err := s.RDB.XLen(ctx, stream).Result()
		if err != nil {
			// No stream — Redis answers "no such key" (the same branch as
			// BackpressureGate.Len and RedisStreamGroups).
			if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
				out[stream] = QueueStat{}
				continue
			}
			return nil, fmt.Errorf("monitor: нагрузка: очереди: %s: %w", stream, err)
		}
		groups, err := s.RDB.XInfoGroups(ctx, stream).Result()
		if err != nil {
			// The stream vanished between XLEN and XINFO (DEL/a trim to 0) — "no such
			// key", like the gate: no groups. A different error drops the section:
			// a backlog without group states is not counted, and a raw XLEN
			// instead of it would overstate the depth by the whole untrimmed
			// stream history (the Oct 25 divergence, spec §2).
			if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
				groups = nil
			} else {
				return nil, fmt.Errorf("monitor: нагрузка: очереди: группы %s: %w", stream, err)
			}
		}
		st := QueueStat{Len: xlen}
		if len(groups) > 0 && stream != queue.StreamDead {
			st.Len = 0
			for _, g := range groups {
				st.Len += queue.GroupDepth(g, xlen)
			}
		}
		for _, g := range groups {
			if g.EntriesRead > st.Read {
				st.Read = g.EntriesRead
			}
		}
		out[stream] = st
	}
	return out, nil
}

// ---------------------------------------------------------------- metrics

// Values of the kind label of the load metrics.
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

// LoadMetrics — the load snapshot gauges (crawler_os_*): a successful pass writes
// them (no hold-last needed — the psimetrics.go style). The process
// top, disk and queues are not duplicated by gauges: their publishers are
// their own circuits (crawler_queue_depth at the workers, the disk_free_* alerts),
// or they belong only to the report (the processes).
type LoadMetrics struct {
	loadavg *prometheus.GaugeVec
	cpu     *prometheus.GaugeVec
	memPSI  *prometheus.GaugeVec
	mem     *prometheus.GaugeVec
	swap    *prometheus.GaugeVec
}

// NewLoadMetrics — the gauges in the reg registry (nil — nil, a loop without metrics:
// the tests seam, the newCollectorMetrics precedent).
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

// observe publishes the snapshot (the utilization — shares of 0..1 or nil while there is
// no delta yet; a nil receiver — no-op).
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

// cpuDelta — the tick shares between two snapshots (0..1); ok=false — the delta
// is impossible (the first pass or the counters reset by a host restart).
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

// ---------------------------------------------------------------- loop

// LoadReportOptions — the loop parameters; Interval <= 0 turns the loop off
// (unlike the withDefaults monitor loops: the report has no "right"
// period — the operator's choice, the monitor.load_report_interval config).
type LoadReportOptions struct {
	// Interval — the snapshot period (DefaultLoadReportInterval).
	Interval time.Duration
	// Log — the loop logger; nil — entries suppressed (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — the service registry; nil — without metrics.
	Metrics *metrics.Registry
	// Notify — the report recipient for the operator (the Telegram delivery of T-162);
	// nil — the report only into the journal. Called in the pass goroutine —
	// it must be safe for a concurrent call (TelegramNotifier.Send).
	Notify func(text string)
	// Queues — the stream state source; nil — the queue section is "n/a".
	Queues QueueSource
	// RenderDebt — the render debt source (renderdebt.go, T-175); the debt
	// line in the queue section; nil — an "n/a" line.
	RenderDebt RenderDebtSource
	// Space — the directory space source (the SpaceChecker of the disk_free_*
	// alerts, alerts.go); nil — the disk section is "n/a".
	Space SpaceChecker
	// Dirs — the disk section directories (usually one — content: the main
	// volume consumer; empty at Space ≠ nil — an empty section).
	Dirs []string
}

// loadPassState — the delta base of a pass: the counters of the previous snapshot
// (the passes are sequential in one goroutine — no mutex needed).
type loadPassState struct {
	cpu     CPUStat
	ticks   map[string]uint64 // comm → the ticks of the past pass
	queues  map[string]QueueStat
	at      time.Time // the moment of the past pass (the rate window)
	started bool      // the base taken (the first pass — "n/a" rates)
}

// DiskLine — a line of the report disk section.
type DiskLine struct {
	Dir   string
	Avail uint64
	Total uint64
}

// LoadReportLoop — the load report loop (T-173): a pass right at
// start (a fresh snapshot after a restart; the deltas — "n/a") and then by
// ticker. A source error — a Warn, the loop keeps living (the OnRun contract, spec 3.4):
// the next pass will repeat; partial failures (Redis unavailable) drop
// only their section. Returns nil on a ctx cancellation (graceful shutdown).
func LoadReportLoop(ctx context.Context, src LoadSource, opts LoadReportOptions) error {
	if src == nil {
		return errors.New("monitor: отчёт нагрузки: источник не задан")
	}
	if opts.Interval <= 0 {
		return nil // disabled by configuration (monitor.load_report_interval)
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

		// The process top: the core shares — the delta of the name's ticks to the delta of one
		// core (delta_total/cores); the sum over the names ≈ busy × cores.
		// A restart/disappearance of a name's processes over the window (production of Oct 25: postgres
		// 18 → 16 processes) drops the name's tick sum — the uint64 subtraction
		// underflows to ~2⁶⁴ and gives junk "10¹⁴ cores":
		// the non-monotonic name is honestly skipped for the window (neither zero nor
		// junk; new names come with the full sum — their ticks started
		// inside the window).
		procCPU := make(map[string]float64, len(st.Procs))
		if d := st.CPU.Total - prev.cpu.Total; prev.started && d > 0 {
			if perCore := float64(d) / float64(st.CPU.Cores); perCore > 0 {
				for name, agg := range st.Procs {
					if agg.Ticks <= prev.ticks[name] {
						continue // a restart/disappearance of the name's processes: the window skipped
					}
					procCPU[name] = float64(agg.Ticks-prev.ticks[name]) / perCore
				}
			}
		}

		// The queues: the backlog now (RedisQueues); the rates — the delivery deltas
		// over the window; a backlog delta of ±N — the growth/drain of the real queue
		// (the untrimmed stream history does not affect it).
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

		// The render debt (T-175): the render_pending flags + the q:render lag —
		// the same line as the loop summary and the alert detail
		// (renderDebtOf). A source failure drops only the line ("n/a").
		var debt *RenderDebt
		if opts.RenderDebt != nil {
			if d, err := opts.RenderDebt.RenderDebt(ctx); err != nil {
				if ctx.Err() == nil {
					log.Warn("снимок рендер-долга", "err", err)
				}
			} else {
				debt = &d
			}
		}

		// Disk: the statfs of the directories (the alerts' SpaceChecker).
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

		report := formatLoadReport(st, busyP, ioP, procCPU, queues, rates, prev.started, prev.queues, disks, debt)
		log.Info("отчёт нагрузки", "report", strings.ReplaceAll(report, "\n", " | "))
		if opts.Notify != nil {
			opts.Notify(report)
		}

		// The base of the next pass — after the successful report build:
		// a failing pass does not shift the delta (the rate window is honestly wider).
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

// formatLoadReport — a compact report for the operator (Telegram/journal), up
// to six lines: the system, the CPU top, the RSS top, memory, disk, queues.
// A nil utilization share and empty maps — "n/a" sections. Gibibytes: meminfo
// is in kibibytes; division by 2³⁰ without "tenths of a gigabyte". The core
// share of the processes: 1.0 = one core entirely (the htop scale).
func formatLoadReport(st LoadStat, busy, iowait *float64, procCPU map[string]float64,
	queues map[string]QueueStat, rates map[string]float64, prevStarted bool,
	prevQueues map[string]QueueStat, disks []DiskLine, debt *RenderDebt) string {
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

	b.WriteString("\n🧮 Топ память: ")
	if top := topByRSS(st.Procs); len(top) == 0 {
		b.WriteString("н/д")
	} else {
		for i, it := range top {
			if i > 0 {
				b.WriteString(", ")
			}
			// The RSS sum with a caveat: shared pages are counted in every
			// process — the number of processes is printed next to it (postgres
			// and chromium have dozens); the per-process peak is looked up with htop.
			fmt.Fprintf(&b, "%s %s", it.name, fmtMem(uint64(it.value)))
			if it.count > 1 {
				fmt.Fprintf(&b, " (%d пр.)", it.count)
			}
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
			// The window dynamics: the ±depth and the delivery rate; the first pass —
			// only the size (no delta).
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
	// The render debt (T-175): the work waiting for a render — the render_pending
	// flags + the unconsumed remainder of q:render. The line is printed
	// even with unavailable queues (the T-175 review, P3-1: the debt source
	// is independent — the queue could be down while the debt is measured); "n/a" — when
	// the debt source itself is unavailable/not set.
	if debt == nil {
		b.WriteString("\n📬 Рендер-долг: н/д")
	} else {
		fmt.Fprintf(&b, "\n📬 Рендер-долг: %s", debt.String())
	}
	return b.String()
}

// topItem — a top line: value — a core share (CPU) or bytes (memory);
// count — the processes of the name (the memory top annotation).
type topItem struct {
	name  string
	value float64
	count int
}

// topByCPU — topProcs names by core share; noise below minProcCPUShare does
// not rise.
func topByCPU(m map[string]float64) []topItem {
	items := make([]topItem, 0, len(m))
	for name, v := range m {
		if v < minProcCPUShare {
			continue
		}
		items = append(items, topItem{name: name, value: v})
	}
	return sortTop(items)
}

// topByRSS — topProcs names by the total RSS (with the process count).
func topByRSS(m map[string]ProcAgg) []topItem {
	items := make([]topItem, 0, len(m))
	for name, agg := range m {
		if agg.RSS == 0 {
			continue
		}
		items = append(items, topItem{name, float64(agg.RSS), agg.Count})
	}
	return sortTop(items)
}

// sortTop — the value descending, the name ascending at equality
// (a deterministic report), sliced to topProcs.
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

// gib/mib — bytes to gibi-/mebibytes for the report.
func gib(b uint64) float64 { return float64(b) / (1 << 30) }
func mib(b uint64) float64 { return float64(b) / (1 << 20) }

// fmtMem — a human-readable size for the memory top: from 1 GiB — fractional
// GiB; below — whole MiB (processes smaller than a gigabyte are not smeared
// with frequent "0.0GiB").
func fmtMem(b uint64) string {
	if b >= 1<<30 {
		return fmt.Sprintf("%.1fГиБ", gib(b))
	}
	return fmt.Sprintf("%.0fМиБ", mib(b))
}

// round — rounding to a whole % (a report to the operator, not a metric).
func round(v float64) int {
	if v < 0 {
		return 0
	}
	return int(v + 0.5)
}
