// Источник контроля OOM и рестартов юнитов crawler (T-145, ТЗ 5.9):
// счётчики memory.events cgroup v2 (oom/oom_kill/high) и NRestarts systemd.
// oom/oom_kill/NRestarts питают алерт unit_oom, рост счётчика high между
// проходами — алерт троттлинга MemoryHigh (memory_high, T-162: предвестник
// фаталов http-worker T-157 — оба читают один снимок источника).
// Потолки памяти юнитов заданы MemoryMax (T-085/T-143; soft MemoryHigh
// http-worker — тем же T-145), но потолок — страховка, а не гарантия:
// OOM-kill или крэш-рестарт — незапланированный простой конвейера
// (ТЗ 3.4) и первый симптом невыполнения бюджета (docs/deploy.md §2);
// алерт unit_oom (alerts.go) бьёт Warn в journald monitor, один Warn
// на эпизод (механика переходов T-080).
//
// Источники — без новых зависимостей (DBus-клиент не заводится):
//
//   - список юнитов и NRestarts — `systemctl show 'crawler-*.service'
//     -p Id -p NRestarts`: свойства менеджера читаются непривилегированно
//     (как `systemctl status` от любого пользователя — polkit на чтение
//     свойств не спрашивает), шаблон покрывает и остановленные/failed
//     юниты (краш-лупа по StartLimitBurst видно NRestarts, хотя cgroup
//     уже удалена);
//   - oom/oom_kill — прямое чтение
//     /sys/fs/cgroup/system.slice/<юнит>/memory.events: файлы cgroup
//     world-readable (0444 root), ProtectControlGroups юнита монитора
//     монтирует cgroupfs read-only — на чтение не влияет. Прямое чтение
//     — не выбор из равных: systemd не экспортирует счётчики memory.events
//     как свойства (systemd 259 не знает -p MemoryEvents), файл —
//     единственный путь; сэмплер t092 читал то же от root.
//
// В dev-контейнере нет ни systemd (runtime-образ distroless без
// systemctl), ни system.slice — источник возвращает ошибку класса
// errNoSystemd, алерт по ней тихо «не измеряется» (Debug, без Warn и
// без ложного срабатывания): тот же контракт, что у SMART NVMe
// (alerts.go) — окружение без systemd для dev — норма, а не авария.
// Прочие ошибки (боевой хост, но systemd/cgroupfs недоступен) — алерт
// «не измеряется» с Warn каждого прохода, цикл живёт: отказ уже
// работавшего контроля не должен быть невидимым. На боевом хосте
// systemctl и cgroupfs доступны юниту монитора без дополнительных прав.
package monitor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// unitEventsPattern — шаблон юнитов контроля: все crawler-сервисы
// системного slice. crawler.target не матчится суффиксом .service, а
// .service-суффикс отсекает возможные не-сервисные юниты набора.
const unitEventsPattern = "crawler-*.service"

// errNoSystemd — окружение без systemd (контейнер): systemctl не
// установлен или PID 1 — не systemd. Ожидаемое состояние dev-окружения,
// а не авария: алерт по этому классу деградирует тихо (как SMART),
// остальные ошибки источника (боевой хост, но systemd/cgroupfs сломан)
// — Warn каждого прохода, стиль остальных источников (зонд ingest).
var errNoSystemd = errors.New("systemd отсутствует (контейнер/dev)")

// Значения метки kind метрики crawler_unit_oom_events (uniteventsmetrics).
const (
	// unitEventOOM — memory.events oom: OOM-killer работал в cgroup.
	unitEventOOM = "oom"
	// unitEventOOMKill — memory.events oom_kill: процессы убиты.
	unitEventOOMKill = "oom_kill"
)

// UnitEvents — снимок событий одного юнита crawler за жизнь его cgroup
// (memory.events) и юнита (NRestarts).
type UnitEvents struct {
	// Unit — имя юнита (crawler-http-worker.service).
	Unit string
	// OOM — memory.events oom: число срабатываний OOM-killer в cgroup
	// (попытки, включая отбитые reclaim'ом).
	OOM uint64
	// OOMKill — memory.events oom_kill: убитые OOM-killer'ом процессы.
	OOMKill uint64
	// High — memory.events high: число попаданий за мягкий потолок
	// MemoryHigh — каждый — принудительный reclaim/задержки (троттлинг).
	// Абсолютное значение накапливается за жизнь cgroup (http-worker с
	// MemoryHigh=850M набирает >100 тыс. за сутки — deploy/systemd/
	// crawler-http-worker.service): событие алерта memory_high — рост
	// счётчика между проходами, не само значение (T-162).
	High uint64
	// NRestarts — systemctl NRestarts: крэш-рестарты с последнего
	// старта/сброса состояния юнита (reset-failed/stop).
	NRestarts uint64
}

// hot — у юнита есть события (алерт смотрит только на это).
func (e UnitEvents) hot() bool {
	return e.OOM > 0 || e.OOMKill > 0 || e.NRestarts > 0
}

// UnitEventsSource — источник событий юнитов; интерфейс для подмены в
// юнит-тестах (реальное чтение — systemctl + cgroupfs, в тестовом
// контейнере их нет). Стиль SpaceChecker/WearSource/BadHashesSource.
type UnitEventsSource interface {
	// UnitEvents возвращает снимки всех юнитов crawler-*.service.
	// Ошибка = «не измеряется» с причиной (контейнер без systemd);
	// пустой список без ошибки — systemd жив, юниты не загружены.
	UnitEvents(ctx context.Context) ([]UnitEvents, error)
}

// OSUnitEvents — UnitEventsSource поверх systemctl show + cgroup v2:
// один exec на проход (шаблон раскрывается менеджером сразу по всем
// юнитам) и по одному чтению memory.events на юнит.
type OSUnitEvents struct {
	// CgroupFS — корень cgroup v2 ("" — "/sys/fs/cgroup"); тесты
	// подменяют каталогом (стиль SysFS/DevFS у OSWear).
	CgroupFS string
	// Systemctl — путь к systemctl ("" — "systemctl" из PATH); тесты
	// подменяют заглушкой-скриптом.
	Systemctl string
	// Timeout — бюджет одного прохода источника, включая exec
	// (0 — unitEventsProbeTimeout); тесты подменяют малым, чтобы не
	// ждать реальных секунд на зависшей заглушке.
	Timeout time.Duration
}

// unitEventsProbeTimeout — бюджет прохода источника: проходы алертов
// последовательны (Pass), зависший systemctl/D-Bus не должен останавливать
// оценку остальных алертов дольше пары периодов цикла. 10 с — с запасом
// против холодного старта менеджера и одновременно ≪ периода цикла 60 с.
//
// Точное значение различимо только паузой в секунды против зависшего
// процесса — противоречит герметичности юнит-тестов
// (docs/mutation-testing.md §4): значение — настроечный бюджет, а не
// логика (стиль ingestProbeTimeout).
// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
const unitEventsProbeTimeout = 10 * time.Second

// compile-time: OSUnitEvents — UnitEventsSource.
var _ UnitEventsSource = OSUnitEvents{}

// UnitEvents собирает события всех юнитов crawler: systemctl show даёт
// список и NRestarts, memory.events каждого юнита — oom/oom_kill.
// Юнит без cgroup (остановлен: failed после исчерпания StartLimit или
// перед первым стартом) читается нулями событий памяти — NRestarts
// такого юнита остаётся в снимке и алертится. Чтение всех юнитов
// обходит целиком: ошибки каждого собираются в одну причину с именами
// (диагностика видит все отказавшие), но снимок не отдаётся — непрочитанный
// счётчик не подменяется нулём, иначе сбой чтения ложно снимал бы алерт.
func (s OSUnitEvents) UnitEvents(ctx context.Context) ([]UnitEvents, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	systemctl := s.Systemctl
	// Дефолт из PATH живёт только на боевом хосте: юнит-тесты всегда
	// задают Systemctl скриптом, реальный exec в тестах недопустим.
	// mutator-disable-next-line branch/if
	if systemctl == "" {
		// mutator-disable-next-line statement/remove
		systemctl = "systemctl"
	}
	cgroupFS := s.CgroupFS
	// Дефолт «/sys/fs/cgroup» — как systemctl выше.
	// mutator-disable-next-line branch/if
	if cgroupFS == "" {
		// mutator-disable-next-line statement/remove
		cgroupFS = "/sys/fs/cgroup"
	}

	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(ctx, systemctl,
		"show", unitEventsPattern, "--property=Id", "--property=NRestarts")
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		reason := fmt.Errorf("systemctl show: %w", err)
		if stderr := strings.TrimSpace(errOut.String()); stderr != "" {
			reason = fmt.Errorf("systemctl show: %w: %s", err, stderr)
		}
		// Окружение без systemd — ожидаемая деградация: бинаря нет в PATH
		// (dev-контейнер distroless) или PID 1 не systemd — тихий класс
		// errNoSystemd; прочее (боевой хост, systemd сломан — в т.ч.
		// заданный путь исчез, зависший менеджер срезан таймаутом) —
		// ошибка как есть, алерт ответит Warn.
		if errors.Is(err, exec.ErrNotFound) ||
			strings.Contains(errOut.String(), "System has not been booted with systemd") {
			return nil, fmt.Errorf("monitor: юниты: %w: %w", errNoSystemd, reason)
		}
		return nil, fmt.Errorf("monitor: юниты: %w", reason)
	}
	units := parseSystemctlShow(out.String())
	if len(units) > 0 {
		// Корень системного slice обязан существовать на cgroup v2-хосте
		// (systemctl уже ответил — systemd есть). Отсутствие — не «юниты
		// без событий», а неработающий источник (cgroup v1/hybrid, чужой
		// корень): без проверки все ENOENT ниже сложились бы в ложное
		// «ok» по нулям.
		sliceDir := filepath.Join(cgroupFS, "system.slice")
		if _, err := os.Stat(sliceDir); err != nil {
			return nil, fmt.Errorf("monitor: юниты: cgroup v2 %s недоступен: %w", sliceDir, err)
		}
	}
	var errs []string
	for i := range units {
		ev, err := readMemoryEvents(filepath.Join(
			cgroupFS, "system.slice", units[i].Unit, "memory.events"))
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", units[i].Unit, err))
			continue
		}
		units[i].OOM, units[i].OOMKill, units[i].High = ev.OOM, ev.OOMKill, ev.High
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("monitor: юниты: memory.events: %s", strings.Join(errs, "; "))
	}
	return units, nil
}

// timeout — бюджет прохода с дефолтом.
func (s OSUnitEvents) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return unitEventsProbeTimeout
}

// parseSystemctlShow разбирает вывод `systemctl show -p Id -p NRestarts
// <шаблон>`: блоки «Id=…\nNRestarts=…» по юниту (порядок строк в блоке
// фиксирован менеджером, разделитель — пустая строка, но парсер не
// зависит от порядка). NRestarts вне числа («[not set]» — юнит ещё не
// запускался) читается нулём, как в сэмплере t092-sample.sh; NRestarts
// до первого Id отбрасывается. Выделен чистой функцией для табличного
// теста без процесса.
func parseSystemctlShow(out string) []UnitEvents {
	var units []UnitEvents
	// cur — индекс текущего юнита; −1 — Id ещё не встречался.
	cur := -1
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(line, "Id="):
			units = append(units, UnitEvents{Unit: strings.TrimPrefix(line, "Id=")})
			cur = len(units) - 1
		case strings.HasPrefix(line, "NRestarts="):
			if cur < 0 {
				continue
			}
			n, err := strconv.ParseUint(strings.TrimPrefix(line, "NRestarts="), 10, 64)
			if err != nil {
				n = 0
			}
			units[cur].NRestarts = n
		}
	}
	return units
}

// memoryEvents — счётчики файла memory.events, нужные алертам:
// oom/oom_kill (unit_oom, T-145) и high (memory_high, T-162).
type memoryEvents struct {
	OOM     uint64
	OOMKill uint64
	High    uint64
}

// readMemoryEvents читает oom/oom_kill/high одного юнита из memory.events.
// Файла нет (юнит без cgroup — остановлен) — нули: счётчики живут в
// cgroup и пересозданием обнуляются, рестарт такой юнит уже учёл в
// NRestarts.
func readMemoryEvents(path string) (memoryEvents, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return memoryEvents{}, nil
	}
	if err != nil {
		return memoryEvents{}, err
	}
	return parseMemoryEvents(string(b))
}

// parseMemoryEvents разбирает файл memory.events: строки «ключ значение»
// (low/high/max/oom/oom_kill/oom_group); для алертов нужны oom/oom_kill/
// high, остальные ключи пропускаются, отсутствующие — нули. Значение не
// число — ошибка (формат фиксирован ядром, кривой файл — не молчать).
// Выделен чистой функцией для табличного теста.
func parseMemoryEvents(data string) (memoryEvents, error) {
	var ev memoryEvents
	for _, line := range strings.Split(data, "\n") {
		key, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		var dst *uint64
		switch key {
		case "oom":
			dst = &ev.OOM
		case "oom_kill":
			dst = &ev.OOMKill
		case "high":
			dst = &ev.High
		default:
			continue
		}
		n, perr := strconv.ParseUint(val, 10, 64)
		if perr != nil {
			return memoryEvents{}, fmt.Errorf("monitor: memory.events: %q: значение %q не число", key, val)
		}
		*dst = n
	}
	return ev, nil
}
