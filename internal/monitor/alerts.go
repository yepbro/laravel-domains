// Алерты мониторинга — минимальный набор ТЗ 5.9 (T-080; ТЗ 8, ТЗ 10.3),
// цикл оценки в том же сервисе monitor, что и пересчёт зон (T-078).
//
// Набор и источники:
//
//   - рост доли ошибок DNS — доля temp_error среди проверок текущих суток
//     (SQL по domains.last_checked_at/статусам; temp_error — исход DNS-этапа
//     после исчерпания ретраев, ТЗ 5.4; NXDOMAIN → not_delegated — валидный
//     результат, не ошибка);
//   - переполнение очередей — глубина рабочих стримов q:dns/q:http/
//     q:render/q:analyze (backlog групп потребления) против
//     scheduler.queue_high_watermark (порог backpressure, ТЗ 4.2/5.3;
//     q:dead гейтом не ограничивается — см. шапку
//     internal/queue/backpressure.go — и не проверяется);
//   - остановка прогресса — zones.updated_at (пишет ZoneStatsLoop, T-078)
//     старше N периодов пересчёта: живой monitor метит свою работу сам;
//   - нехватка места на диске — statfs каталога контента против
//     content.disk_free_percent (тот же порог, что ставит Put контента
//     в паузу, ТЗ 8; формула — как в internal/content/disk.go, подход
//     переиспользован собственной копией, пакет хранилища не тянется);
//   - износ NVMe (SMART) — percentage_used из health-лога NVMe (ТЗ 10.3),
//     чтение ioctl-ом admin-команды, без nvme-cli.
//
// Сигнал фронтовой (на переходах, стиль DiskWatchLoop в internal/content):
// Warn при срабатывании, Info при снятии, Debug когда алерт неприменим
// («не измеряется»). Ошибки источников (БД, Redis, statfs) — Warn каждого
// прохода, цикл живёт (контракт OnRun, T-012). Исключение — SMART NVMe:
// в Docker-контейнере /dev/nvme* недоступен, это ожидаемое состояние, а не
// авария — честная деградация «не измеряется» без Warn и без ложного
// срабатывания; на боевом systemd-хосте слой включается доступом юнита к
// /dev/nvme* (группа nvme-smart, T-089 — docs/disks.md) рядом с хостовым
// smartd.
//
// Гистерезис — у алертов с дребезгом у порога: доля ошибок DNS (включение
// 30 % / снятие 20 %) и очереди (включение на пороге гейта / снятие ниже
// 90 % порога). Диск зеркалирует порог паузы Put без зазора: алерт виден
// ровно тогда, когда запись контента остановлена; остановка прогресса и
// износ меняются только событиями, не порогом — гистерезис не нужен.
//
// Экспорт состояния — снапшот прохода AlertsResult в хук
// AlertsOptions.OnPass (решение: лог + поле). Собственный gauge-реестр
// Prometheus в monitor не заводится — не дублировать internal/metrics
// (T-076): метрики алертов (crawler_alert_firing по меткам) заведёт слой
// T-077 поверх общего реестра, хук даёт точку подключения без правки
// цикла.
package monitor

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// Статусы алертов (AlertSnapshot.Status).
const (
	// AlertStatusOK — условие алерта не выполнено.
	AlertStatusOK = "ok"
	// AlertStatusFiring — алерт сработал (Warn на переходе).
	AlertStatusFiring = "firing"
	// AlertStatusNotMeasured — алерт неприменим в текущем окружении
	// (нет данных/источник недоступен): не срабатывает ложно.
	AlertStatusNotMeasured = "not_measured"
)

// Имена алертов — метка в логах и снапшоте прохода.
const (
	AlertDNSErrors     = "dns_temp_error_share" // доля temp_error за сутки
	AlertQueueOverflow = "queue_overflow"       // глубина очереди у порога гейта
	AlertProgressStall = "progress_stall"       // zones.updated_at устарел
	AlertDiskFree      = "disk_free"            // место каталога контента
	AlertNVMeWear      = "nvme_wear"            // SMART percentage_used
)

// alertDescs — человекочитаемые описания алертов для записей лога.
var alertDescs = map[string]string{
	AlertDNSErrors:     "рост доли ошибок DNS",
	AlertQueueOverflow: "переполнение очередей",
	AlertProgressStall: "остановка прогресса",
	AlertDiskFree:      "нехватка места на диске",
	AlertNVMeWear:      "износ NVMe (SMART)",
}

// Пороги оценки. Числа ТЗ не задаёт; переиспользуются существующие пороги
// конфигурации (queue_high_watermark, disk_free_percent), остальное —
// константы с обоснованием (меньше поверхности конфигурации, значения
// связаны механикой этапов, а не тюнингом оператора).
const (
	// DefaultAlertsInterval — период оценки алертов: 60 с, как пересчёт
	// зон (верх коридора ТЗ 5.9 «30–60 с»); проход по domains — то же
	// последовательное чтение, что у T-078. cmd/monitor передаёт
	// monitor.zone_stats_interval: одна пара циклов мониторинга — один
	// период, отдельного поля конфигурации не заводим.
	DefaultAlertsInterval = 60 * time.Second

	// AlertStallFactor — порог остановки прогресса в периодах пересчёта
	// зон: updated_at старше 5 × zone_stats_interval — остановка. Проход
	// T-078 по 350 млн строк — минуты: единичный пропуск периода — не
	// авария, 5 периодов (≈5 мин при 60 с) — уверенная остановка.
	AlertStallFactor = 5

	// AlertDNSFireShare — доля temp_error, включающая алерт: 30 %.
	// Адаптивный лимитёр DNS (T-035) держит долю таймаутов ≤10 %,
	// снижая QPS: 30 % — устойчивый отказ резолверов (недоступен
	// Unbound/апстрим), а не адаптивное проседание.
	AlertDNSFireShare = 0.30

	// AlertDNSRecoverShare — снятие алерта ниже 20 %: гистерезис, доля
	// в коридоре 20–30 % не дребезжит Warn.
	AlertDNSRecoverShare = 0.20

	// AlertDNSMinSamples — минимум проверок в окне для оценки доли:
	// 2 из 2 в temp_error — шум пустого окна (сразу после полуночи или
	// простоя), не рост ошибок.
	AlertDNSMinSamples = 100

	// AlertQueueRecoverFactor — снятие алерта очередей ниже 90 % порога.
	// Гейт мягкий (между проверкой и публикацией другие продюсеры
	// добавляют записи, T-026): глубина в коридоре 90–100 % порога
	// дребезжала бы Warn. Срабатывание — ровно на пороге гейта
	// (depth ≥ watermark): алерт виден одновременно с приостановкой
	// публикации.
	AlertQueueRecoverFactor = 0.9

	// AlertNVMeWearPercent — percentage_used, включающая алерт износа:
	// 90 % — осталось ≤10 % расчётного ресурса, порог планирования
	// замены. Ресурс монотонен (не восстанавливается) — гистерезис не
	// нужен; значения >100 % валидны (ресурс исчерпан) и тоже алертятся.
	AlertNVMeWearPercent = 90
)

// dnsShareSQL — числитель и знаменатель доли temp_error одним проходом по
// domains. Окно — текущие сутки: last_checked_at имеет точность дня
// (date, ТЗ 6.1), минутных окон на колонке нет — тот же выбор, что у темпа
// crawlerctl status (T-079). current_date — таймзона сервера БД, как и
// now() записи last_checked_at (T-058). Частичный domains_recheck_idx
// (T-104) проходу не подходит: фильтр по статусу без check_state в
// предикате запроса индекс не доказывает, окно покрывает почти все
// done-строки — проход остаётся последовательным чтением партиций, та же
// цена, что у пересчёта зон (T-078), и та же частота.
const dnsShareSQL = "SELECT count(*) FILTER (WHERE status = $1), count(*) " +
	"FROM domains WHERE last_checked_at >= current_date"

// zoneFreshSQL — свежесть прогресса: последний момент пересчёта зон
// (zones.updated_at пишет ZoneStatsLoop, T-078). Справочник зон маленький;
// max по пустому набору — NULL.
const zoneFreshSQL = "SELECT max(updated_at) FROM zones"

// --------------------------------------------------------------- место на диске

// SpaceStat — снимок места файловой системы каталога, байты.
type SpaceStat struct {
	// Avail — доступно процессам без root-резерва (statfs.Bavail):
	// сервисы краулера работают непривилегированными (ТЗ 10.1).
	Avail uint64
	// Total — ёмкость тома (statfs.Blocks).
	Total uint64
}

// SpaceChecker — источник сведений о месте файловой системы; выделен
// интерфейсом, чтобы юнит-тесты подменяли statfs без реального тома.
// Подход internal/content/disk.go (T-046) собственной копией: monitor не
// тянет пакет хранилища (PackStore, zstd) ради одного сисколла, а
// проверка места — независимый взгляд мониторинга (ТЗ 5.9), параллельный
// DiskWatchLoop писателя контента.
type SpaceChecker interface {
	// Space возвращает снимок места каталога dir.
	Space(dir string) (SpaceStat, error)
}

// OSStatfs — SpaceChecker поверх syscall.Statfs.
type OSStatfs struct{}

// compile-time: OSStatfs — SpaceChecker.
var _ SpaceChecker = OSStatfs{}

// Space снимает statfs каталога dir.
func (OSStatfs) Space(dir string) (SpaceStat, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return SpaceStat{}, err
	}
	bs := uint64(st.Bsize)
	return SpaceStat{
		Avail: st.Bavail * bs,
		Total: st.Blocks * bs,
	}, nil
}

// ------------------------------------------------------------------ износ NVMe

// NVMeWear — износ одного NVMe-устройства по health-логу (ТЗ 10.3).
type NVMeWear struct {
	// Device — имя контроллера (nvme0), базовое имя в sysfs.
	Device string
	// PercentUsed — percentage_used health-лога: 0..100, значения >100
	// валидны — расчётный ресурс исчерпан (NVMe 1.x).
	PercentUsed int
	// DataUnitsWritten — записано единиц записи (единица NVMe = 512 000
	// байт): ТБ ≈ units × 5,12e-7; ТЗ 10.3 упоминает TBW — вычисляется
	// из той же страницы.
	DataUnitsWritten uint64
}

// WearSource — источник данных износа NVMe; интерфейс для подмены в
// юнит-тестах (реальное чтение — ioctl, оборудования в тестах нет).
type WearSource interface {
	// Wear возвращает износ всех NVMe-устройств. Ошибка = «не
	// измеряется» с причиной (контейнер без /dev/nvme*, пустой sysfs):
	// алерт деградирует, а не срабатывает ложно.
	Wear(ctx context.Context) ([]NVMeWear, error)
}

// OSWear — WearSource поверх sysfs + ioctl NVMe admin-команды: устройства
// обнаруживаются в sysfs (доступен и в контейнере — но сам /dev/nvme*
// туда не проброшен), износ читается health-логом (log page 0x02) через
// NVME_IOCTL_ADMIN_CMD — без nvme-cli и без новых зависимостей.
type OSWear struct {
	// SysFS — корень sysfs ("" — "/sys"); тесты подменяют каталогом.
	SysFS string
	// DevFS — корень devfs ("" — "/dev"); тесты подменяют каталогом.
	DevFS string
}

// compile-time: OSWear — WearSource.
var _ WearSource = OSWear{}

// Wear обходит NVMe-контроллеры sysfs и читает health-лог каждого.
// Ни одного устройства или ни одного доступного /dev-узла — ошибка
// деградации (в Docker-контейнере это норма: sysfs видит устройства
// хоста, /dev/nvme0 не проброшен — открыт не будет).
func (w OSWear) Wear(ctx context.Context) ([]NVMeWear, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sysfs := w.SysFS
	if sysfs == "" {
		sysfs = "/sys"
	}
	devfs := w.DevFS
	if devfs == "" {
		devfs = "/dev"
	}
	names, err := filepath.Glob(filepath.Join(sysfs, "class", "nvme", "nvme*"))
	if err != nil {
		return nil, fmt.Errorf("NVMe: обзор %s: %w", sysfs, err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("NVMe-устройства не обнаружены (%s)",
			filepath.Join(sysfs, "class", "nvme"))
	}
	var wears []NVMeWear
	var errs []string
	for _, name := range names {
		dev := filepath.Base(name)
		percent, units, err := wearOf(filepath.Join(devfs, dev))
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", dev, err))
			continue
		}
		wears = append(wears, NVMeWear{
			Device:           dev,
			PercentUsed:      percent,
			DataUnitsWritten: units,
		})
	}
	if len(wears) == 0 {
		return nil, fmt.Errorf("SMART недоступен: %s", strings.Join(errs, "; "))
	}
	return wears, nil
}

// Константы NVMe (uapi/linux/nvme.h, uapi/linux/nvme_ioctl.h).
const (
	// nvmeIoctlAdminCmd — _IOWR('N', 0x41, struct nvme_admin_cmd):
	// dir=3<<30 | size=72<<16 | type='N'<<8 | nr=0x41.
	nvmeIoctlAdminCmd = 0xc0484e41
	// nvmeAdminCmdSize — sizeof(struct nvme_admin_cmd) на 64-бит.
	nvmeAdminCmdSize = 72
	// nvmeAdminGetLogPage — opcode admin «получить страницу журнала».
	nvmeAdminGetLogPage = 0x02
	// nvmeLogHealth — идентификатор страницы health information.
	nvmeLogHealth = 0x02
	// nvmeHealthLogSize — размер страницы health information, байт.
	nvmeHealthLogSize = 512
	// nvmeNsidNone — скоуп контроллера (host/any namespace).
	nvmeNsidNone = 0xFFFFFFFF
)

// nvmeAdminCmd — struct nvme_admin_cmd (uapi/linux/nvme_ioctl.h): layout
// фиксирован ядром, порядок полей и выравнивание Go-структуры обязаны
// совпадать (проверка размера — compile-time ниже). 72 байта на 64-бит.
type nvmeAdminCmd struct {
	opcode      uint8
	flags       uint8
	rsvd1       uint16
	nsid        uint32
	cdw2, cdw3  uint32
	metadata    uint64
	addr        uint64
	metadataLen uint32
	dataLen     uint32
	cdw10       uint32
	cdw11       uint32
	cdw12       uint32
	cdw13       uint32
	cdw14       uint32
	cdw15       uint32
	timeoutMs   uint32
	result      uint32
}

// compile-time: размер структуры договаривается с константой ioctl-номера
// (номер кодирует size=72).
var _ = [1]struct{}{}[unsafe.Sizeof(nvmeAdminCmd{})-nvmeAdminCmdSize]

// wearOf читает износ одного устройства: открывает char-узел контроллера
// /dev/nvmeX (O_RDWR — ioctl admin требует записи) и забирает страницу
// health одним NVME_IOCTL_ADMIN_CMD. Декодирование — parseHealthLog.
func wearOf(devPath string) (percent int, unitsWritten uint64, err error) {
	// O_RDWR: ioctl admin-команды требуют доступа на запись к узлу —
	// контейнер без проброса устройства получит EPERM/ENOENT (деградация).
	f, err := os.OpenFile(devPath, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()

	// Буфер страницы — в обёртке с нулевым полем uint64: массив байт сам по
	// себе не выровнен, а тип структуры наследует выравнивание поля (8) —
	// адрес данных пойдёт в драйвер выровненным.
	var pageBuf struct {
		_    [0]uint64
		data [nvmeHealthLogSize]byte
	}
	cmd := nvmeAdminCmd{
		opcode:  nvmeAdminGetLogPage,
		nsid:    nvmeNsidNone,
		addr:    uint64(uintptr(unsafe.Pointer(&pageBuf.data[0]))),
		dataLen: nvmeHealthLogSize,
		// cdw10: NUMD (число двойных слов − 1) в старших битах, LID —
		// в младших.
		cdw10: uint32(nvmeHealthLogSize/4-1)<<16 | nvmeLogHealth,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(),
		nvmeIoctlAdminCmd, uintptr(unsafe.Pointer(&cmd)))
	// Адрес буфера ушёл в ioctl целым числом — GC его не видит; KeepAlive
	// держит буфер живым до возврата сисколла (правила unsafe.Pointer).
	runtime.KeepAlive(&pageBuf)
	if errno != 0 {
		return 0, 0, fmt.Errorf("ioctl NVME_ADMIN_GET_LOG_PAGE: %w", errno)
	}
	return parseHealthLog(pageBuf.data[:])
}

// parseHealthLog декодирует страницу health information (layout —
// struct nvme_smart_log из uapi/linux/nvme.h): critical_warning — байт 0,
// composite temperature — 1–2, available spare — 3, spare threshold — 4,
// percentage_used — байт 5 (значения >100 валидны — ресурс исчерпан);
// data units written — байты 48–63, 128-бит little-endian, старшая
// половина на практике 0. Выделен чистой функцией для табличного теста
// смещений без оборудования.
func parseHealthLog(page []byte) (percent int, unitsWritten uint64, err error) {
	if len(page) < nvmeHealthLogSize {
		return 0, 0, fmt.Errorf("NVMe: страница health короче %d байт: %d", nvmeHealthLogSize, len(page))
	}
	return int(page[5]), binary.LittleEndian.Uint64(page[48:56]), nil
}

// ------------------------------------------------------------------- алерты

// StreamLener — глубина стримов; *queue.BackpressureGate (T-026)
// удовлетворяет интерфейсу — тот же узкий API над go-redis, что у
// планировщика и crawlerctl status (T-079). Отдельный интерфейс, а не
// go-redis: тесты подменяют фейком, транспорт заменяем (ТЗ 4.2).
type StreamLener interface {
	// Len возвращает глубину стрима: непрочитанный группой потребителей
	// остаток плюс неподтверждённые доставки (backlog, см.
	// BackpressureGate.Len).
	Len(ctx context.Context, stream string) (int64, error)
}

// alertStreams — проверяемые стримы: рабочие, ограниченные гейтом
// backpressure (q:dead не ограничивается — см. шапку backpressure.go).
var alertStreams = []string{
	queue.StreamDNS, queue.StreamHTTP, queue.StreamRender, queue.StreamAnalyze,
}

// AlertsDeps — источники данных алертов. Каждый источник может деградировать
// («не измеряется» с причиной), кроме пула БД: монитор без БД бессмысленен
// (цикл заканчивается ошибкой).
type AlertsDeps struct {
	// Pool — БД: доля temp_error за сутки, zones.updated_at.
	Pool db.DB
	// Redis — длины стримов; nil — алерт очередей «не измеряется».
	Redis StreamLener
	// Space — место каталога контента; nil — OSStatfs.
	Space SpaceChecker
	// Wear — износ NVMe; nil — OSWear (sysfs/ioctl).
	Wear WearSource
}

// AlertsOptions — параметры оценки; нулевые значения заменяются
// дефолтами (withDefaults, стиль ZoneStatsOptions).
type AlertsOptions struct {
	// Interval — период цикла AlertsLoop (DefaultAlertsInterval);
	// рабочее значение — monitor.zone_stats_interval.
	Interval time.Duration
	// ZoneStatsInterval — период ZoneStatsLoop: единица порога остановки
	// прогресса (AlertStallFactor × интервал); 0 —
	// DefaultZoneStatsInterval.
	ZoneStatsInterval time.Duration
	// QueueWatermark — порог глубины стримов
	// (scheduler.queue_high_watermark); ≤ 0 — алерт очередей «не
	// измеряется» (валидация конфига требует ≥ 1). Эффективный порог
	// стрима ограничивается половиной его предела MAXLEN — как у гейта
	// backpressure (queue.StreamWatermark, T-026).
	QueueWatermark int64
	// QueueMaxLen — предел длины стримов MAXLEN (зеркалит redis.maxlen;
	// 0 — пределы по стримам политики T-025): нужен для эффективного
	// порога стрима (см. QueueWatermark).
	QueueMaxLen int64
	// ContentDir — каталог контента (content.dir); "" — алерт места «не
	// измеряется».
	ContentDir string
	// DiskFreePercent — порог свободного места
	// (content.disk_free_percent); ≤ 0 — контроль выключен, алерт «не
	// измеряется».
	DiskFreePercent float64
	// Now — источник времени (возраст zones.updated_at); nil — time.Now.
	Now func() time.Time
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// OnPass — хук экспорта состояния: снапшот каждого прохода (лог +
	// поле; см. шапку файла). Вызывается в цикле прохода — обязан быть
	// быстрым и не паниковать.
	OnPass func(AlertsResult)
}

// withDefaults заменяет неположительные периоды дефолтами (Now/Log
// проверяются отдельно: функций/указателей — nil).
func (o AlertsOptions) withDefaults() AlertsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultAlertsInterval
	}
	if o.ZoneStatsInterval <= 0 {
		o.ZoneStatsInterval = DefaultZoneStatsInterval
	}
	return o
}

// AlertSnapshot — состояние одного алерта после прохода.
type AlertSnapshot struct {
	// Name — имя алерта (константы Alert*).
	Name string
	// Status — AlertStatusOK | AlertStatusFiring | AlertStatusNotMeasured.
	Status string
	// Detail — измерение или причина «не измеряется».
	Detail string
}

// AlertsResult — снапшот прохода оценки: по одному AlertSnapshot на алерт,
// порядок фиксирован (evaluators).
type AlertsResult struct {
	Alerts []AlertSnapshot
}

// Firing — сработавшие алерты прохода.
func (r AlertsResult) Firing() []AlertSnapshot {
	var firing []AlertSnapshot
	for _, a := range r.Alerts {
		if a.Status == AlertStatusFiring {
			firing = append(firing, a)
		}
	}
	return firing
}

// verdict — итог проверки одного алерта до наложения гистерезиса
// (гистерезис решает внутри evaluators по prev).
type verdict struct {
	status string
	detail string
	// err — ошибка источника: алерт переводится в «не измеряется», ошибка
	// логируется Warn каждого прохода (цикл живёт). Пустые-guard'ы
	// («мало данных», «выключено») задают detail без err — тише.
	err error
}

// alertEval — одна проверка: prev — прошлое состояние алерта (для
// гистерезиса), verdict — новое.
type alertEval struct {
	name string
	eval func(ctx context.Context, prev string) verdict
}

// Alerts — оценка алертов с памятью состояний между проходами
// (гистерезис, фронтовые записи лога только на переходах).
type Alerts struct {
	deps  AlertsDeps
	opts  AlertsOptions
	log   *slog.Logger
	now   func() time.Time
	state map[string]string
}

// NewAlerts строит оценщик: нормализует опции (дефолты периодов), источники
// Space/Wear (nil → реализации ОС) и готовит память состояний.
func NewAlerts(deps AlertsDeps, opts AlertsOptions) *Alerts {
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	if deps.Space == nil {
		deps.Space = OSStatfs{}
	}
	if deps.Wear == nil {
		deps.Wear = OSWear{}
	}
	return &Alerts{deps: deps, opts: o, log: log, now: now, state: make(map[string]string)}
}

// evaluators — проверки прохода в фиксированном порядке (порядок
// снапшота AlertsResult).
func (a *Alerts) evaluators() []alertEval {
	return []alertEval{
		{AlertDNSErrors, a.evalDNS},
		{AlertQueueOverflow, a.evalQueues},
		{AlertProgressStall, a.evalStall},
		{AlertDiskFree, a.evalDisk},
		{AlertNVMeWear, a.evalWear},
	}
}

// Pass выполняет один проход: оценивает все алерты (ошибка источника
// одного не останавливает остальные), логирует переходы, отдаёт снапшот
// в OnPass и возвращает его.
func (a *Alerts) Pass(ctx context.Context) AlertsResult {
	evals := a.evaluators()
	snaps := make([]AlertSnapshot, 0, len(evals))
	for _, e := range evals {
		snaps = append(snaps, a.evaluate(ctx, e))
	}
	res := AlertsResult{Alerts: snaps}
	if a.opts.OnPass != nil {
		a.opts.OnPass(res)
	}
	a.log.Debug("проход алертов", "алертов", len(snaps), "сработало", len(res.Firing()))
	return res
}

// evaluate прогоняет одну проверку: ошибка источника — Warn (кроме
// отмены ctx — часть остановки, стиль ZoneStatsLoop) и «не измеряется»;
// переход состояния логируется фронтовой записью.
func (a *Alerts) evaluate(ctx context.Context, e alertEval) AlertSnapshot {
	prev := a.state[e.name]
	v := e.eval(ctx, prev)
	if v.err != nil {
		if ctx.Err() == nil {
			a.log.Warn("оценка алерта: "+alertDescs[e.name], "alert", e.name, "err", v.err)
		}
		v.status = AlertStatusNotMeasured
		if v.detail == "" {
			v.detail = v.err.Error()
		}
	}
	a.transition(e.name, prev, v)
	a.state[e.name] = v.status
	return AlertSnapshot{Name: e.name, Status: v.status, Detail: v.detail}
}

// transition пишет лог на смене состояния: срабатывание — Warn, снятие —
// Info, неприменимость — Debug. Исключение — уход сработавшего алерта
// в «не измеряется» (сбой источника): Warn об ошибке пишет evaluate,
// потерю самого алерта фиксируем Info — иначе авария исчезла бы из лога
// без единой записи о прекращении.
func (a *Alerts) transition(name, prev string, v verdict) {
	if prev == v.status {
		return
	}
	desc := name
	if d, ok := alertDescs[name]; ok {
		desc = d
	}
	switch v.status {
	case AlertStatusFiring:
		a.log.Warn("АЛЕРТ: "+desc, "alert", name, "detail", v.detail)
	case AlertStatusOK:
		if prev == AlertStatusFiring {
			a.log.Info("алерт снят: "+desc, "alert", name, "detail", v.detail)
		}
	case AlertStatusNotMeasured:
		if prev == AlertStatusFiring {
			a.log.Info("сработавший алерт больше не измеряется: "+desc, "alert", name, "причина", v.detail)
		} else {
			a.log.Debug("алерт не измеряется: "+desc, "alert", name, "причина", v.detail)
		}
	}
}

// evalDNS — доля temp_error среди проверок текущих суток. Гистерезис:
// включение ≥ AlertDNSFireShare, при сработавшем алерте удержание до
// падения ниже AlertDNSRecoverShare.
func (a *Alerts) evalDNS(ctx context.Context, prev string) verdict {
	rows, err := a.deps.Pool.Query(ctx, dnsShareSQL, int16(model.StatusTempError))
	if err != nil {
		return verdict{err: fmt.Errorf("monitor: доля ошибок DNS: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		return verdict{err: errors.New("monitor: доля ошибок DNS: пустой ответ")}
	}
	var temp, total int64
	if err := rows.Scan(&temp, &total); err != nil {
		return verdict{err: fmt.Errorf("monitor: доля ошибок DNS: чтение: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return verdict{err: fmt.Errorf("monitor: доля ошибок DNS: %w", err)}
	}
	if total < AlertDNSMinSamples {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: fmt.Sprintf("проверок за текущие сутки %d < %d — доля не показательна", total, AlertDNSMinSamples),
		}
	}
	share := float64(temp) / float64(total)
	detail := fmt.Sprintf("доля temp_error %.1f%% (%d из %d проверок за текущие сутки)",
		100*share, temp, total)
	firing := share >= AlertDNSFireShare ||
		(prev == AlertStatusFiring && share >= AlertDNSRecoverShare)
	if firing {
		return verdict{status: AlertStatusFiring, detail: detail}
	}
	return verdict{status: AlertStatusOK, detail: detail}
}

// evalQueues — глубина рабочих стримов против порога гейта. Срабатывание —
// depth ≥ эффективного порога стрима (в тот же момент гейт закрывает
// публикацию); снятие — все стримы ниже 90 % порога.
func (a *Alerts) evalQueues(ctx context.Context, prev string) verdict {
	if a.deps.Redis == nil {
		return verdict{status: AlertStatusNotMeasured, detail: "Redis не задан"}
	}
	if a.opts.QueueWatermark <= 0 {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: "порог scheduler.queue_high_watermark не задан",
		}
	}
	lens := make(map[string]int64, len(alertStreams))
	for _, s := range alertStreams {
		n, err := a.deps.Redis.Len(ctx, s)
		if err != nil {
			return verdict{err: fmt.Errorf("monitor: глубина %s: %w", s, err)}
		}
		lens[s] = n
	}
	// Порог — по стриму, как у гейта (T-026): для q:dns настроенные
	// 100 000 записей ограничиваются половиной его MAXLEN 10 000.
	wm := make(map[string]int64, len(alertStreams))
	for _, s := range alertStreams {
		wm[s] = queue.StreamWatermark(a.opts.QueueWatermark, a.opts.QueueMaxLen, s)
	}
	var hot, hold []string
	for _, s := range alertStreams {
		n := lens[s]
		recoverAt := int64(float64(wm[s]) * AlertQueueRecoverFactor)
		switch {
		case n >= wm[s]:
			hot = append(hot, fmt.Sprintf("%s=%d (порог %d)", s, n, wm[s]))
		case prev == AlertStatusFiring && n >= recoverAt:
			hold = append(hold, fmt.Sprintf("%s=%d (порог %d)", s, n, wm[s]))
		}
	}
	if len(hot) > 0 || len(hold) > 0 {
		at := hot
		if len(at) == 0 {
			at = hold // коридор гистерезиса: ниже порога гейта, но близко
		}
		return verdict{
			status: AlertStatusFiring,
			detail: strings.Join(at, ", ") + " — глубина у порога",
		}
	}
	// Деталь «ok» — максимальная глубина среди рабочих стримов; стартовое
	// значение — первый стрим: при всех нулях (простаивающие очереди)
	// деталь называет стрим, а не пустую строку.
	maxStream, maxLen := alertStreams[0], lens[alertStreams[0]]
	for _, s := range alertStreams[1:] {
		if lens[s] > maxLen {
			maxStream, maxLen = s, lens[s]
		}
	}
	return verdict{
		status: AlertStatusOK,
		detail: fmt.Sprintf("макс %s=%d", maxStream, maxLen),
	}
}

// evalStall — возраст последнего пересчёта зон (zones.updated_at пишет
// ZoneStatsLoop, T-078): старше AlertStallFactor периодов — прогресс
// остановлен. Гистерезис не нужен: возраст уменьшается только новым
// проходом, дребезга у порога нет. Оговорка самоконтроля: updated_at
// пишет сам monitor — при полной смерти процесса алерт некому сработать,
// пропажу сервиса видит метрика crawler_service_up каркаса (T-012).
func (a *Alerts) evalStall(ctx context.Context, _ string) verdict {
	rows, err := a.deps.Pool.Query(ctx, zoneFreshSQL)
	if err != nil {
		return verdict{err: fmt.Errorf("monitor: остановка прогресса: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		return verdict{err: errors.New("monitor: остановка прогресса: пустой ответ")}
	}
	var last sql.NullTime
	if err := rows.Scan(&last); err != nil {
		return verdict{err: fmt.Errorf("monitor: остановка прогресса: чтение: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return verdict{err: fmt.Errorf("monitor: остановка прогресса: %w", err)}
	}
	if !last.Valid {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: "справочник зон пуст — прогресс не отслеживается",
		}
	}
	age := a.now().Sub(last.Time)
	stallAfter := time.Duration(AlertStallFactor) * a.opts.ZoneStatsInterval
	if age > stallAfter {
		return verdict{
			status: AlertStatusFiring,
			detail: fmt.Sprintf("zones.updated_at старше порога: %s (порог %d × %s)",
				age.Round(time.Second), AlertStallFactor, a.opts.ZoneStatsInterval),
		}
	}
	return verdict{
		status: AlertStatusOK,
		detail: fmt.Sprintf("последний пересчёт %s назад (порог %s)",
			age.Round(time.Second), stallAfter),
	}
}

// evalDisk — доля свободного места каталога контента: та же формула, что
// пауза Put в internal/content (T-046), — алерт срабатывает ровно когда
// запись контента останавливается; отдельный порог рассинхронировал бы
// сигнал с реальной паузой, поэтому без зазора гистерезиса.
func (a *Alerts) evalDisk(_ context.Context, _ string) verdict {
	if a.opts.ContentDir == "" {
		return verdict{status: AlertStatusNotMeasured, detail: "каталог контента не задан"}
	}
	if a.opts.DiskFreePercent <= 0 {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: "контроль места выключен (content.disk_free_percent ≤ 0)",
		}
	}
	sp, err := a.deps.Space.Space(a.opts.ContentDir)
	if err != nil {
		return verdict{err: fmt.Errorf("monitor: свободное место %s: %w", a.opts.ContentDir, err)}
	}
	if sp.Total == 0 {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: "файловая система не сообщает ёмкость (total=0)",
		}
	}
	free := 100 * float64(sp.Avail) / float64(sp.Total)
	detail := fmt.Sprintf("свободно %.1f%% (%s из %s), порог %.1f%%",
		free, alertBytes(sp.Avail), alertBytes(sp.Total), a.opts.DiskFreePercent)
	if free < a.opts.DiskFreePercent {
		return verdict{status: AlertStatusFiring, detail: detail}
	}
	return verdict{status: AlertStatusOK, detail: detail}
}

// evalWear — износ NVMe по health-логу: алерт при percentage_used ≥
// AlertNVMeWearPercent любого устройства. Недоступность SMART — ожидаемое
// состояние контейнера: verdict без err (Debug, не Warn) — деградация
// честная и тихая (см. шапку файла).
func (a *Alerts) evalWear(ctx context.Context, _ string) verdict {
	wears, err := a.deps.Wear.Wear(ctx)
	if err != nil {
		return verdict{status: AlertStatusNotMeasured, detail: err.Error()}
	}
	if len(wears) == 0 {
		return verdict{status: AlertStatusNotMeasured, detail: "NVMe-устройства не найдены"}
	}
	// Худший по износу; старт — первое устройство, а не нулевое значение:
	// новый накопитель (0 %) оставил бы пустое имя в детали.
	worst := wears[0]
	for _, w := range wears[1:] {
		if w.PercentUsed > worst.PercentUsed {
			worst = w
		}
	}
	detail := fmt.Sprintf("%s: использовано %d%% ресурса, записано %.1f ТБ (TBW)",
		worst.Device, worst.PercentUsed, nvmeTBW(worst))
	if worst.PercentUsed >= AlertNVMeWearPercent {
		var hot []string
		for _, w := range wears {
			if w.PercentUsed >= AlertNVMeWearPercent {
				hot = append(hot, fmt.Sprintf("%s=%d%%", w.Device, w.PercentUsed))
			}
		}
		return verdict{
			status: AlertStatusFiring,
			detail: fmt.Sprintf("износ %s ≥ порога %d%% (%s)",
				strings.Join(hot, ", "), AlertNVMeWearPercent, detail),
		}
	}
	return verdict{status: AlertStatusOK, detail: detail}
}

// nvmeTBW — записано терабайт (единица NVMe = 512 000 байт, десятичные
// ТБ — как в спецификации TBW).
func nvmeTBW(w NVMeWear) float64 {
	return float64(w.DataUnitsWritten) * 512000 / 1e12
}

// AlertsLoop — периодический цикл оценки алертов (стиль ZoneStatsLoop):
// первый проход сразу при старте, далее по ticker. Ошибки источников —
// Warn внутри прохода, цикл живёт (контракт OnRun, T-012): очередной
// проход переоценит по живым данным. Отмена ctx — чистый nil (graceful
// shutdown, ТЗ 3.4).
func AlertsLoop(ctx context.Context, deps AlertsDeps, opts AlertsOptions) error {
	if deps.Pool == nil {
		return errors.New("monitor: алерты: пул БД не задан")
	}
	a := NewAlerts(deps, opts)
	a.log.Info("цикл оценки алертов",
		"interval", a.opts.Interval.String(), "алертов", len(a.evaluators()))

	pass := func() { a.Pass(ctx) }
	pass()
	ticker := time.NewTicker(a.opts.Interval)
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

// alertBytes — человекочитаемый размер для деталей алертов (локальный
// аналог content.humanBytes: пакет хранилища не тянется).
func alertBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
