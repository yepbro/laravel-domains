// Юнит-тесты контроля OOM/рестартов юнитов crawler (T-145, ТЗ 5.9) на
// фейковых источниках: пороги алерта (oom/oom_kill/NRestarts по
// отдельности и вместе), эпизод (один Warn на серию — фронтовая
// механика переходов), тихие деградации (нет источника, ошибка
// systemd-окружения, пустой список юнитов — без Warn, как SMART),
// разбор вывода systemctl show и memory.events (таблицы, включая
// «[not set]» и мусорные строки), OSUnitEvents против заглушек
// (скрипт-systemctl в TempDir + дерево cgroupfs; юнит без cgroup —
// нулями), метрики-обёртка (gauge по юнитам, hold-last на ошибке).
// Реальное чтение хостового cgroupfs/exec systemctl — за границами
// тестов (hermeticity, docs/mutation-testing.md §4).
package monitor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// --- фейковый источник --------------------------------------------------------

// fakeUnitEvents — снимки юнитов по заготовке, очередь ошибок, протокол.
type fakeUnitEvents struct {
	mu    sync.Mutex
	units []UnitEvents
	errs  []error
	calls int
}

func (f *fakeUnitEvents) UnitEvents(context.Context) ([]UnitEvents, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return nil, err
	}
	return f.units, nil
}

// newUnitEnv — тестовый оценщик с живым источником юнитов: дефолт —
// два здоровых юнита (все счётчики нули).
func newUnitEnv(t *testing.T, units []UnitEvents) (*alertsTestEnv, *fakeUnitEvents) {
	t.Helper()
	env := newAlertsTestEnv(t, nil)
	src := &fakeUnitEvents{units: units}
	env.deps.UnitEvents = src
	return env, src
}

// --- алерт unit_oom -------------------------------------------------------------

// TestAlertsUnitOOMFire — каждый вид события отдельно включает алерт:
// oom, oom_kill, NRestarts; вместе — один detail с каждым юнитом и его
// счётчиками; все нули — ok с числом юнитов.
func TestAlertsUnitOOMFire(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		units []UnitEvents
	}{
		{"oom", []UnitEvents{{Unit: "crawler-http-worker.service", OOM: 3}}},
		{"oom_kill", []UnitEvents{{Unit: "crawler-render-worker.service", OOMKill: 1}}},
		{"рестарты", []UnitEvents{{Unit: "crawler-chromium.service", NRestarts: 2}}},
	}
	for _, c := range cases {
		env, _ := newUnitEnv(t, c.units)
		s := snapshotOf(t, env.Pass(ctx), AlertUnitOOM)
		if s.Status != AlertStatusFiring {
			t.Errorf("%s: статус = %s (%s), want firing", c.name, s.Status, s.Detail)
		}
		if !strings.Contains(s.Detail, c.units[0].Unit) {
			t.Errorf("%s: detail %q не называет юнит %s", c.name, s.Detail, c.units[0].Unit)
		}
	}

	// Два горячих юнита — оба в detail, здоровый третий не упомянут.
	env, _ := newUnitEnv(t, []UnitEvents{
		{Unit: "crawler-http-worker.service", OOMKill: 1},
		{Unit: "crawler-monitor.service"},
		{Unit: "crawler-chromium.service", NRestarts: 4},
	})
	s := snapshotOf(t, env.Pass(ctx), AlertUnitOOM)
	if s.Status != AlertStatusFiring {
		t.Fatalf("два горячих: статус = %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"crawler-http-worker.service", "oom_kill=1", "crawler-chromium.service", "рестартов=4"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail %q не содержит %q", s.Detail, want)
		}
	}
	if strings.Contains(s.Detail, "crawler-monitor.service") {
		t.Errorf("detail %q называет здоровый юнит", s.Detail)
	}

	// Все нули — ok; деталь называет число юнитов.
	env, _ = newUnitEnv(t, []UnitEvents{
		{Unit: "crawler-http-worker.service"},
		{Unit: "crawler-monitor.service"},
	})
	s = snapshotOf(t, env.Pass(ctx), AlertUnitOOM)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "2 юнитов") {
		t.Errorf("здоровые юниты: %s (%s), want ok с числом юнитов", s.Status, s.Detail)
	}
}

// TestAlertsUnitOOMEpisode — один Warn на эпизод: срабатывание — Warn
// «АЛЕРТ», устойчивое срабатывание — тихо, сброс счётчиков (рестарт/
// reset-failed юнита) — Info «алерт снят».
func TestAlertsUnitOOMEpisode(t *testing.T) {
	env, src := newUnitEnv(t, []UnitEvents{{Unit: "crawler-http-worker.service", OOMKill: 1}})
	ctx := context.Background()

	env.Pass(ctx)
	if !strings.Contains(env.buf.String(), "level=WARN") ||
		!strings.Contains(env.buf.String(), "АЛЕРТ: OOM-убийства и крэш-рестарты юнитов crawler") {
		t.Fatalf("срабатывание без Warn:\n%s", env.buf.String())
	}

	// Продолжение эпизода (счётчики не сброшены) — тихо.
	before := env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(before):]; strings.Contains(delta, "level=WARN") || strings.Contains(delta, "level=INFO") {
		t.Errorf("устойчивое срабатывание повторило сигнал:\n%s", delta)
	}

	// Новое событие того же юнита внутри эпизода — без второго Warn.
	src.units = []UnitEvents{{Unit: "crawler-http-worker.service", OOMKill: 2, NRestarts: 1}}
	env.Pass(ctx)
	delta := env.buf.String()[len(before):]
	if strings.Contains(delta, "level=WARN") || strings.Contains(delta, "level=INFO") {
		t.Errorf("новое событие внутри эпизода дало второй сигнал:\n%s", delta)
	}

	// Сброс состояния юнита (cgroup пересоздана, NRestarts обнулён) — Info.
	src.units = []UnitEvents{{Unit: "crawler-http-worker.service"}}
	before = env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(before):]; !strings.Contains(delta, "алерт снят") || !strings.Contains(delta, "level=INFO") {
		t.Errorf("сброс эпизода без Info о снятии:\n%s", delta)
	}
}

// TestAlertsUnitOOMDegradation — деградации источника двух классов:
// окружение без systemd (errNoSystemd — тихое «не измеряется», без
// Warn, контракт SMART) и боевой отказ источника (Warn каждого прохода,
// стиль зонда ingest); плюс источник не задан и пустой список юнитов.
func TestAlertsUnitOOMDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil) // UnitEvents не задан
	env.deps.UnitEvents = nil       // явное отсутствие источника (T-162 дал env здоровый по умолчанию)
	s := snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "источник не задан") {
		t.Errorf("без источника: %s (%s), want not_measured «источник не задан»", s.Status, s.Detail)
	}

	// Dev-контейнер без systemd — тихо: Debug-переход, без Warn.
	env, src := newUnitEnv(t, nil)
	src.errs = []error{fmt.Errorf("monitor: юниты: %w: systemctl show: %w",
		errNoSystemd, errors.New("exec: systemctl: executable file not found"))}
	s = snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "systemd отсутствует") {
		t.Errorf("без systemd: %s (%s), want not_measured с классом errNoSystemd", s.Status, s.Detail)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("отсутствие systemd дало Warn (должно быть тихим, как SMART):\n%s", env.buf.String())
	}

	// Боевой отказ источника — Warn каждого прохода, «не измеряется»
	// с причиной; следующий проход по живому источнику работает.
	env, src = newUnitEnv(t, []UnitEvents{{Unit: "crawler-http-worker.service"}})
	src.errs = []error{errors.New("monitor: юниты: memory.events: EIO")}
	s = snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "EIO") {
		t.Errorf("боевой отказ: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "EIO") {
		t.Errorf("боевой отказ без Warn:\n%s", env.buf.String())
	}
	if s := snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM); s.Status != AlertStatusOK {
		t.Errorf("после отказа: %s (%s), want ok (цикл живёт)", s.Status, s.Detail)
	}

	// systemd жив, юниты не загружены — пустой список без ошибки.
	src.units = nil
	s = snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "не найдены") {
		t.Errorf("без юнитов: %s (%s), want not_measured «не найдены»", s.Status, s.Detail)
	}
}

// --- разбор вывода systemctl show ----------------------------------------------

// TestParseSystemctlShow — блоки Id/NRestarts по юниту, «[not set]» и
// мусорные значения — нули (как t092-sample.sh), NRestarts до Id —
// отбрасывается, пустой вывод — пустой список.
func TestParseSystemctlShow(t *testing.T) {
	units := parseSystemctlShow("Id=crawler-http-worker.service\nNRestarts=2\n\n" +
		"Id=crawler-monitor.service\nNRestarts=0\n\n")
	if len(units) != 2 {
		t.Fatalf("юнитов = %d, want 2: %+v", len(units), units)
	}
	if units[0].Unit != "crawler-http-worker.service" || units[0].NRestarts != 2 {
		t.Errorf("первый юнит = %+v, want crawler-http-worker.service NRestarts=2", units[0])
	}
	if units[1].Unit != "crawler-monitor.service" || units[1].NRestarts != 0 {
		t.Errorf("второй юнит = %+v, want crawler-monitor.service NRestarts=0", units[1])
	}

	// «[not set]» (юнит не запускался) и нечисло — нули, не ошибка.
	for _, val := range []string{"[not set]", "мусор"} {
		units = parseSystemctlShow("Id=crawler-web.service\nNRestarts=" + val + "\n")
		if len(units) != 1 || units[0].NRestarts != 0 {
			t.Errorf("NRestarts=%q: %+v, want 0", val, units)
		}
	}

	// NRestarts до первого Id отбрасывается; посторонние строки — мимо.
	units = parseSystemctlShow("NRestarts=9\nId=crawler-dns-worker.service\nLoadState=loaded\nNRestarts=1\n")
	if len(units) != 1 || units[0].Unit != "crawler-dns-worker.service" || units[0].NRestarts != 1 {
		t.Errorf("странный блок: %+v, want dns-worker NRestarts=1 (ранний NRestarts=9 отброшен)", units)
	}

	if got := parseSystemctlShow(""); got != nil {
		t.Errorf("пустой вывод = %+v, want nil", got)
	}
	if got := parseSystemctlShow("\n\n"); got != nil {
		t.Errorf("только пустые строки = %+v, want nil", got)
	}
}

// --- разбор memory.events --------------------------------------------------------

// TestParseMemoryEvents — oom/oom_kill/high из полного файла, чужие ключи
// и отсутствующие строки — мимо/нули, нечисловое значение — ошибка.
func TestParseMemoryEvents(t *testing.T) {
	ev, err := parseMemoryEvents("low 0\nhigh 42\nmax 7\noom 3\noom_kill 1\noom_group 0\n")
	if err != nil || ev.OOM != 3 || ev.OOMKill != 1 || ev.High != 42 {
		t.Errorf("полный файл = (%d, %d, %d, %v), want (3, 1, 42, nil)", ev.OOM, ev.OOMKill, ev.High, err)
	}

	ev, err = parseMemoryEvents("low 5\nmax 1\n")
	if err != nil || ev.OOM != 0 || ev.OOMKill != 0 || ev.High != 0 {
		t.Errorf("без oom/high-строк = (%d, %d, %d, %v), want нули без ошибки", ev.OOM, ev.OOMKill, ev.High, err)
	}

	ev, err = parseMemoryEvents("")
	if err != nil || ev.OOM != 0 || ev.OOMKill != 0 || ev.High != 0 {
		t.Errorf("пустой файл = (%d, %d, %d, %v), want нули без ошибки", ev.OOM, ev.OOMKill, ev.High, err)
	}

	// Строка без пробела — не ключ-значение, мимо.
	if _, err := parseMemoryEvents("oomkill\n"); err != nil {
		t.Errorf("строка без значения: ошибка = %v, want пропуск", err)
	}

	if ev, err := parseMemoryEvents("oom -1\n"); err == nil || ev.OOM != 0 {
		t.Errorf("нечисловое oom: (%d, %v), want ошибка и нуль", ev.OOM, err)
	}
	if ev, err := parseMemoryEvents("oom_kill x\n"); err == nil || ev.OOMKill != 0 {
		t.Errorf("нечисловое oom_kill: (%d, %v), want ошибка и нуль", ev.OOMKill, err)
	}
	if ev, err := parseMemoryEvents("high y\n"); err == nil || ev.High != 0 {
		t.Errorf("нечисловое high: (%d, %v), want ошибка и нуль (алерт memory_high, T-162)", ev.High, err)
	}
}

// --- OSUnitEvents против заглушек ---------------------------------------------

// fakeSystemctl — исполняемый скрипт-заглушка systemctl в каталоге dir:
// печатает out в stdout; при fail — stderr и код 1.
func fakeSystemctl(t *testing.T, dir, out, failMsg string) string {
	t.Helper()
	script := "#!/bin/sh\n"
	if failMsg != "" {
		script += "echo '" + failMsg + "' >&2\nexit 1\n"
	} else {
		// heredoc с кавычкой — без подстановок, вывод дословный.
		script += "cat <<'SYSTEMCTL_SHOW_EOF'\n" + out + "\nSYSTEMCTL_SHOW_EOF\n"
	}
	path := filepath.Join(dir, "systemctl-fake")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeCgroup — дерево cgroupfs юнита с memory.events в каталоге root;
// dir == "" — юнит без cgroup (остановлен).
func fakeCgroup(t *testing.T, root, unit, events string) {
	t.Helper()
	if events == "" {
		return // каталог юнита не создаётся
	}
	dir := filepath.Join(root, "system.slice", unit)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.events"), []byte(events), 0o444); err != nil {
		t.Fatal(err)
	}
}

// TestOSUnitEvents — systemctl show даёт юниты и NRestarts, memory.events
// — oom/oom_kill/high (high — троттлинг MemoryHigh, алерт memory_high
// T-162); юнит без cgroup — нули событий памяти (остановлен, рестарты
// учтены NRestarts).
func TestOSUnitEvents(t *testing.T) {
	root := t.TempDir()
	cg := filepath.Join(root, "cg")
	show := "Id=crawler-http-worker.service\nNRestarts=2\n\n" +
		"Id=crawler-analyzer.service\nNRestarts=0\n"
	fakeCgroup(t, cg, "crawler-http-worker.service", "low 0\nhigh 12\noom 3\noom_kill 1\n")
	fakeCgroup(t, cg, "crawler-analyzer.service", "low 0\noom 0\noom_kill 0\n")
	// У monitor.service каталога нет — как у остановленного юнита.
	show += "\nId=crawler-monitor.service\nNRestarts=1\n"

	units, err := (OSUnitEvents{
		CgroupFS:  cg,
		Systemctl: fakeSystemctl(t, root, show, ""),
	}).UnitEvents(context.Background())
	if err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	if len(units) != 3 {
		t.Fatalf("юнитов = %d, want 3: %+v", len(units), units)
	}
	want := []UnitEvents{
		{Unit: "crawler-http-worker.service", OOM: 3, OOMKill: 1, High: 12, NRestarts: 2},
		{Unit: "crawler-analyzer.service"},
		{Unit: "crawler-monitor.service", NRestarts: 1},
	}
	for i, w := range want {
		if units[i] != w {
			t.Errorf("юнит[%d] = %+v, want %+v", i, units[i], w)
		}
	}
}

// TestOSUnitEventsEmptyShow — systemd жив, шаблон ничего не нашёл: пустой
// список без ошибки (алерт сам переведёт в «не измеряется»).
func TestOSUnitEventsEmptyShow(t *testing.T) {
	root := t.TempDir()
	units, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", ""),
	}).UnitEvents(context.Background())
	if err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	if len(units) != 0 {
		t.Errorf("юнитов = %+v, want пусто", units)
	}
}

// TestOSUnitEventsErrors — классы отказов: «не systemd» (нет бинаря,
// PID 1 не systemd — errNoSystemd, тихий класс алерта) и боевые ошибки
// (systemctl упал по другой причине, испорченный memory.events — имена
// всех отказавших юнитов, обход без fail-fast), зависший systemctl —
// бюджет таймаута.
func TestOSUnitEventsErrors(t *testing.T) {
	root := t.TempDir()

	// PID 1 не systemd — ожидаемое отсутствие окружения.
	errNoBoot := "System has not been booted with systemd (PID 1)"
	_, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", errNoBoot),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), errNoBoot) {
		t.Errorf("PID 1 не systemd: ошибка = %v, want причина systemctl", err)
	}
	if !errors.Is(err, errNoSystemd) {
		t.Errorf("PID 1 не systemd: ошибка = %v, want класс errNoSystemd", err)
	}

	// Бинаря нет в PATH (dev-контейнер, Systemctl="" → «systemctl») —
	// тихий класс errNoSystemd.
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: "systemctl-точно-отсутствует-t145",
	}).UnitEvents(context.Background())
	if !errors.Is(err, errNoSystemd) {
		t.Errorf("systemctl отсутствует в PATH: ошибка = %v, want класс errNoSystemd", err)
	}

	// Явно заданный путь исчез (боевая поломка пакета, не контейнер) —
	// НЕ тихий класс: молчать о сломанном systemctl нельзя.
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: filepath.Join(root, "исчезнувший-systemctl"),
	}).UnitEvents(context.Background())
	if err == nil || errors.Is(err, errNoSystemd) {
		t.Errorf("заданный путь отсутствует: ошибка = %v, want боевая ошибка (не errNoSystemd)", err)
	}

	// Боевая ошибка systemctl (не «нет systemd») — не тихий класс.
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", "Failed to connect to bus"),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Failed to connect to bus") {
		t.Errorf("падение systemctl: ошибка = %v, want причина со stderr", err)
	}
	if errors.Is(err, errNoSystemd) {
		t.Errorf("падение шины: ошибка = %v, не должен быть классом errNoSystemd", err)
	}

	// Испорченный memory.events у двух юнитов: обход продолжает список,
	// причина называет каждого отказавшего (без fail-fast на первом).
	cg := filepath.Join(root, "cg")
	show := "Id=crawler-http-worker.service\nNRestarts=0\n\nId=crawler-dns-worker.service\nNRestarts=0\n"
	fakeCgroup(t, cg, "crawler-http-worker.service", "oom много\n")
	fakeCgroup(t, cg, "crawler-dns-worker.service", "oom_kill тоже\n")
	_, err = (OSUnitEvents{CgroupFS: cg, Systemctl: fakeSystemctl(t, root, show, "")}).
		UnitEvents(context.Background())
	for _, unit := range []string{"crawler-http-worker.service", "crawler-dns-worker.service"} {
		if err == nil || !strings.Contains(err.Error(), unit) {
			t.Errorf("два испорченных memory.events: ошибка = %v, want имя %s", err, unit)
		}
	}

	// Отмена ctx — до обращения к systemctl.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (OSUnitEvents{Systemctl: fakeSystemctl(t, root, "", "")}).UnitEvents(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("отмена ctx: ошибка = %v, want context.Canceled", err)
	}
}

// TestOSUnitEventsTimeout — зависший systemctl срезается бюджетом
// источника (проходы алертов последовательны — зависание не должно
// останавливать цикл): короткий Timeout + спящая заглушка. exec —
// чтобы kill пришёл в сам процесс, а не ждал осиротевшего потомка
// на дескрипторах пайпа (модель прямого потомка, как реальный
// systemctl). Маркер-файл доказывает, что процесс стартовал: ошибка
// тогда — именно срыв по бюджету, а не отказ запуска.
func TestOSUnitEventsTimeout(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "started")
	script := "#!/bin/sh\n touch " + marker + "\n exec sleep 30\n"
	path := filepath.Join(root, "systemctl-hang")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: path,
		Timeout:   50 * time.Millisecond,
	}).UnitEvents(context.Background())
	if err == nil {
		t.Fatal("зависший systemctl: ошибки нет, want бюджет таймаута")
	}
	if _, merr := os.Stat(marker); merr != nil {
		t.Fatalf("заглушка не стартовала (маркер не создан) — таймаут не проверен: %v", merr)
	}
	// Тождество ошибки не фиксируем (kill по ctx приходит как
	// «signal: killed», не обёртка DeadlineExceeded) — существенен
	// сам срыв по бюджету и не-тихий класс (боевой отказ, не «нет
	// systemd»).
	if errors.Is(err, errNoSystemd) {
		t.Errorf("зависший systemctl: ошибка = %v, не класс errNoSystemd", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("бюджет не срезал зависание: %s", elapsed)
	}
}

// TestOSUnitEventsNoCgroupRoot — systemd отвечает, юниты есть, но
// корня system.slice нет (cgroup v1/hybrid, чужой CgroupFS) — ошибка
// источника, а не ложное «ok» по нулям ENOENT (раунд 2 ревью, P3).
func TestOSUnitEventsNoCgroupRoot(t *testing.T) {
	root := t.TempDir()
	show := "Id=crawler-http-worker.service\nNRestarts=0\n"
	// Каталог cg не создаётся: корня system.slice нет.
	_, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, show, ""),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), "system.slice") {
		t.Errorf("корень cgroup v2 отсутствует: ошибка = %v, want ошибка про system.slice", err)
	}
	if errors.Is(err, errNoSystemd) {
		t.Errorf("корень cgroup v2 отсутствует: ошибка = %v, не класс errNoSystemd (systemctl-то ответил)", err)
	}

	// Пустой список юнитов при том же отсутствующем корне — без ошибки:
	// читать нечего, алерт сам скажет «не найдены».
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", ""),
	}).UnitEvents(context.Background())
	if err != nil {
		t.Errorf("пустой список без корня cgroup: ошибка = %v, want nil", err)
	}
}

// --- метрики-обёртка -------------------------------------------------------------

// gaugeLabels — значение gauge по имени и парам метка=значение.
// Служебную метку service (подставляет фабрика реестра) критерий не
// считает: серия матчится, когда все метки want совпадают, а прочие
// сводятся к service.
func gaugeLabels(t *testing.T, g prometheus.Gatherer, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := make(map[string]string, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			equal := true
			for k, v := range want {
				if got[k] != v {
					equal = false
				}
			}
			for k := range got {
				if _, ok := want[k]; !ok && k != metrics.LabelService {
					equal = false
				}
			}
			if equal {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestUnitEventsMetrics — обёртка зеркалит снимок в gauge обоих приборов
// (метки unit/kind), ошибка источника держит последние значения
// (hold-last), nil-реестр — чистый транзит.
func TestUnitEventsMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	src := &fakeUnitEvents{units: []UnitEvents{
		{Unit: "crawler-http-worker.service", OOM: 3, OOMKill: 1, NRestarts: 2},
		{Unit: "crawler-monitor.service"},
	}}
	m := NewUnitEventsMetrics(reg, src)

	if _, err := m.UnitEvents(context.Background()); err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	cases := []struct {
		metric string
		labels map[string]string
		want   float64
	}{
		{"crawler_unit_oom_events", map[string]string{"unit": "crawler-http-worker.service", "kind": "oom"}, 3},
		{"crawler_unit_oom_events", map[string]string{"unit": "crawler-http-worker.service", "kind": "oom_kill"}, 1},
		{"crawler_unit_oom_events", map[string]string{"unit": "crawler-monitor.service", "kind": "oom_kill"}, 0},
		{"crawler_unit_restarts", map[string]string{"unit": "crawler-http-worker.service"}, 2},
		{"crawler_unit_restarts", map[string]string{"unit": "crawler-monitor.service"}, 0},
	}
	for _, c := range cases {
		if v, ok := gaugeLabels(t, reg, c.metric, c.labels); !ok || v != c.want {
			t.Errorf("%s%v = (%v, %v), want %v", c.metric, c.labels, v, ok, c.want)
		}
	}

	// Ошибка источника — транзит ошибки, метрики не тронуты (hold-last).
	src.errs = []error{errors.New("контейнер без systemd")}
	if _, err := m.UnitEvents(context.Background()); err == nil {
		t.Error("ошибка источника не прокинута")
	}
	if v, ok := gaugeLabels(t, reg, "crawler_unit_restarts", map[string]string{"unit": "crawler-http-worker.service"}); !ok || v != 2 {
		t.Errorf("после ошибки restarts = (%v, %v), want прежние 2", v, ok)
	}

	// Новые значения перезаписывают gauge (не копятся рядами).
	src.errs = nil
	src.units = []UnitEvents{{Unit: "crawler-http-worker.service", OOM: 4, NRestarts: 3}}
	if _, err := m.UnitEvents(context.Background()); err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_unit_oom_events", map[string]string{"unit": "crawler-http-worker.service", "kind": "oom"}); v != 4 {
		t.Errorf("перезапись gauge: oom = %v, want 4", v)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_unit_restarts", map[string]string{"unit": "crawler-http-worker.service"}); v != 3 {
		t.Errorf("перезапись gauge: restarts = %v, want 3", v)
	}

	// nil-реестр — транзит без приборов (изолированные сборки).
	plain := NewUnitEventsMetrics(nil, &fakeUnitEvents{units: []UnitEvents{{Unit: "u.service"}}})
	units, err := plain.UnitEvents(context.Background())
	if err != nil || len(units) != 1 || units[0].Unit != "u.service" {
		t.Errorf("транзит без реестра = (%+v, %v), want источник как есть", units, err)
	}
}
