// Алерты мониторинга — минимальный набор ТЗ 5.9 (T-080; ТЗ 8, ТЗ 10.3),
// цикл оценки в том же сервисе monitor, что и пересчёт зон (T-078).
//
// Набор и источники:
//
//   - рост доли ошибок DNS — доля temp_error среди проверок текущих
//     суток (суточные строки crawler_counters temp_error:/checks: —
//     точечные чтения, T-182; temp_error — исход DNS-этапа после
//     исчерпания ретраев, ТЗ 5.4; NXDOMAIN → not_delegated — валидный
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
//     чтение ioctl-ом admin-команды, без nvme-cli;
//   - мусорные content_hash очистки (T-099) — всплеск счётчика
//     crawler_ingest_bad_hashes_total в экспозиции /metrics сервиса
//     ingest (BadHashesSource, реализация IngestBadHashes — badhashes.go):
//     рост счётчика — ошибка писателя результатов (T-056), данные не
//     теряются (refcount по мусорной ссылке не менялся, ING-P2-4);
//   - OOM и рестарты юнитов crawler (T-145) — счётчики memory.events
//     (oom/oom_kill) cgroup v2 и NRestarts systemd (UnitEventsSource,
//     реализация OSUnitEvents — unitevents.go): событие — незапланированный
//     простой конвейера и симптом невыполнения бюджета памяти (ТЗ 3.4,
//     docs/deploy.md §2).
//   - неразобранный q:dead (T-154) — глубина карантина (XLEN через тот
//     же StreamLener: групп потребителей у q:dead нет) устойчиво
//     ненулевая; сверяется со счётчиком смертей
//     crawler_queue_dead_buried_total (metrics/dead.go) — глубина
//     (сток) держит сигнал, который темп (rate) растягивает медленным
//     клеймом на часы (пост-мортем docs/load-measurements.md §11.1);
//   - память Redis (T-154) — used_memory/maxmemory из INFO memory
//     (RedisMemorySource, реализация RedisInfo — redismem.go): при
//     noeviction (ТЗ 3.2) заполнение maxmemory — прокси-метрика
//     предстоящих OOM-отказов записи, прямого счётчика отказов INFO
//     не отдаёт;
//   - абсолютный минимум места на диске (T-162) — свободные байты
//     каталога контента против monitor.alerts.disk_free_min_bytes
//     (Warn) и disk_free_crit_bytes (эскалация отдельным алертом):
//     процентный порог на большом томе поздний — 10 % от 79 ГБ = 8 ГБ
//     ≈ 5 ч до заполнения при темпе ≈36 GiB/сут (замер §12);
//   - троттлинг MemoryHigh (T-162) — рост счётчика memory.events high
//     юнита между проходами (источник — тот же UnitEventsSource, что у
//     unit_oom): предвестник фаталов http-worker (T-157) до OOM-kill;
//   - замороженная очередь (T-162) — last-delivered-id группы рабочего
//     стрима не движется ≥ monitor.alerts.queue_freeze_window при
//     глубине > 0 (StreamGroupsSource, реализация RedisStreamGroups —
//     streamgroups.go): сигнатура отравленной задачи T-159 — запись
//     валит потребителя, XREADGROUP не читает, очередь стоит;
//   - PSI CPU (T-162) — some avg300 /proc/pressure/cpu выше
//     monitor.alerts.psi_cpu_percent устойчиво monitor.alerts.
//     psi_cpu_window (PSISource, реализация OSPSI — psi.go): голод
//     планировщика, на котором тонут все этапы разом (замер §12:
//     some 91–95 % в пиках; T-158 дал monitor'у вес CPU 50 против
//     1000 — собственные проходы голод не приближают);
//   - накопление рендер-долга (T-175) — флаги render_pending
//     (счётчик crawler_counters, миграция 000015) + лаг q:render
//     (renderdebt.go) выше monitor.alerts.render_debt_domains И растёт
//     monitor.alerts.render_debt_passes проходов подряд: очередь
//     переполнена и приток обгоняет дренаж — алерт T-162
//     «замороженная очередь» такой случай не ловит (доставки идут),
//     долг 443 тыс. (морозка 24.10) нашли ручным SQL.
//
// Доставка (T-162): переходы алертов (срабатывание/снятие) уходят
// оператору в Telegram — хук AlertsOptions.OnPass (TelegramNotifier,
// notify.go) зовёт exec deploy/telegram-notify.sh --message с дебаунсом
// «только изменение состояния»; без секретов/скрипта доставка выключена,
// сигнал остаётся Warn в журнале (ТЗ 10.4 — тот же канал, что у CI/CD).
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
// 30 % / снятие 20 %), очереди (включение на пороге гейта / снятие ниже
// 90 % порога) и память Redis (80 % / 75 %, T-154); q:dead гасится
// устойчивостью (3 прохода) и снимается пустым карантином (T-154). Диск
// зеркалирует порог паузы Put без зазора: алерт виден
// ровно тогда, когда запись контента остановлена; остановка прогресса и
// износ меняются только событиями, не порогом — гистерезис не нужен.
// Новые алерты T-162 гасятся устойчивостью вместо гистерезиса:
// memory_high — серия проходов с ростом счётчика, queue_frozen — окно
// неподвижности last-delivered-id при ненулевой глубине, psi_cpu — окно
// удержания avg300 выше порога (сам avg300 сглаживает 5 минут);
// абсолютные минимумы диска — без зазора, как процентный.
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
	AlertBadHashes     = "ingest_bad_hashes"    // всплеск мусорных content_hash
	AlertUnitOOM       = "unit_oom"             // OOM/рестарты юнитов crawler (T-145)
	AlertQueueDead     = "queue_dead"           // неразобранный q:dead (T-154)
	AlertRedisMemory   = "redis_memory"         // память Redis у maxmemory (T-154)
	AlertDiskMin       = "disk_free_min"        // абсолютный минимум места, Warn (T-162)
	AlertDiskCrit      = "disk_free_crit"       // эскалация абсолютного минимума (T-162)
	AlertMemHigh       = "memory_high"          // троттлинг MemoryHigh (T-162)
	AlertQueueFreeze   = "queue_frozen"         // замороженная очередь (T-162)
	AlertPSICPU        = "psi_cpu"              // PSI CPU some (T-162)
	AlertRenderDebt    = "render_debt"          // накопление рендер-долга (T-175)
)

// alertDescs — человекочитаемые описания алертов для записей лога.
var alertDescs = map[string]string{
	AlertDNSErrors:     "рост доли ошибок DNS",
	AlertQueueOverflow: "переполнение очередей",
	AlertProgressStall: "остановка прогресса",
	AlertDiskFree:      "нехватка места на диске",
	AlertNVMeWear:      "износ NVMe (SMART)",
	AlertBadHashes:     "мусорные content_hash при очистке удалённых доменов",
	AlertUnitOOM:       "OOM-убийства и крэш-рестарты юнитов crawler",
	AlertQueueDead:     "мертвые задачи в q:dead не разбираются",
	AlertRedisMemory:   "память Redis у порога maxmemory",
	AlertDiskMin:       "свободное место ниже абсолютного минимума",
	AlertDiskCrit:      "свободное место ниже порога эскалации",
	AlertMemHigh:       "троттлинг MemoryHigh юнита (устойчивый рост reclaim-событий)",
	AlertQueueFreeze:   "замороженная очередь: группа не читает стрим",
	AlertPSICPU:        "голод CPU (PSI some) устойчиво выше порога",
	AlertRenderDebt:    "накопление рендер-долга: приток SPA обгоняет дренаж q:render",
}

// Пороги оценки. Числа ТЗ не задаёт; переиспользуются существующие пороги
// конфигурации (queue_high_watermark, disk_free_percent), остальное —
// константы с обоснованием (меньше поверхности конфигурации, значения
// связаны механикой этапов, а не тюнингом оператора).
const (
	// DefaultAlertsInterval — период оценки алертов: 5 минут —
	// fallback нулевых AlertsOptions.Interval; рабочее значение —
	// monitor.alerts_interval (T-206 — отвязка от реконсиляции,
	// решение оператора 26.10). История частоты: исходные 60 с — верх
	// коридора ТЗ 5.9 «30–60 с», рассчитанный на малый корпус; T-161
	// держал «одну пару циклов» на zone_stats_interval (5 мин);
	// T-199 разредил реконсиляцию до 30 мин — окна детекции в проходах
	// растянулись в 6 раз; T-206 вернул циклу алертов собственные
	// 5 мин, эффективные окна прежние: queue_dead срабатывает за ≈15 мин
	// (3 прохода), mem_high/render_debt — за 10 мин (2 прохода),
	// bad_hashes держит окно 75 мин (15 проходов), погрешность
	// PSI-выборки ~5 мин.
	DefaultAlertsInterval = 5 * time.Minute

	// AlertStallFactor — порог остановки прогресса в периодах
	// РЕКОНСИЛЯЦИИ: updated_at старше 5 × zone_stats_interval —
	// остановка. Единица порога — период ZoneStatsLoop (метку пишет
	// именно этот цикл), а не интервал алертов: при 30-мин реконсиляции
	// порог 5 × 5 мин стрелял бы вечно; более частые проходы алертов
	// (T-206) только опрашивают свежесть чаще, порог остался
	// «5 пропущенных проходов реконсиляции» (≈2,5 ч на дефолтных
	// 30 мин). Проход T-078 по десяткам млн строк — минуты: единичный
	// пропуск периода — не авария, 5 периодов — уверенная остановка.
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

	// AlertBadHashesThreshold — порог всплеска мусорных content_hash
	// (T-099): новых строк за окно AlertBadHashesWindow. В норме инвариант
	// писателя результатов (ТЗ 6.1: 16 октет) держит ноль всегда, но
	// единичные реликты — строки, посаженные до фикса ING-P2-4 и
	// добираемые очисткой по одному — не должны будить оператора:
	// 10 за окно — уверенная ошибка писателя (T-056), не эхо прошлого.
	AlertBadHashesThreshold = 10

	// AlertBadHashesWindow — окно всплеска в проходах алертов:
	// 15 × период алертов (5 мин на дефолте, T-206) = 75 мин
	// (исходные «15 × 60 с = 15 мин» — до T-161; на 30-мин цикле
	// T-199 окно растягивалось до 7,5 ч). Задержка снятия — то же
	// окно: разовый всплеск держит алерт сработавшим до вытеснения из
	// кольца. Проходы очистки идут раз в минуту (DefaultCleanupInterval):
	// окно охватывает десятки проходов очистки — всплеск не проскакивает
	// между оценками, а короткий (одно-два мусорных строки) не набирает
	// порог; против дребезга гистерезис не нужен — сумма окна убывает
	// только старением дельт, у порога она держится лишь при продолжении
	// потока.
	AlertBadHashesWindow = 15

	// AlertDeadMinPasses — проходов алертов с ненулевой глубиной q:dead
	// до срабатывания queue_dead (T-154): 3 × период алертов (5 мин на
	// дефолте, T-206) ≈ 15 мин (исходные «3 × 60 с ≈ 3 мин» — до T-161;
	// на 30-мин цикле T-199 окно растягивалось до ≈1,5 ч). «Устойчиво
	// ненулевой», а не «любая смерть»: карантин не опустошается сам
	// (только разбор оператора, ТЗ 4.2), и единичная смерть при живом
	// разборе — штатная работа q:dead, не авария; в то же время окно
	// несопоставимо с инцидентным масштабом (5507 смертей за вечер,
	// пост-мортем §11.1) — сигнал опаздывает максимум на один проход.
	AlertDeadMinPasses = 3

	// AlertRedisMemFireRatio — доля used_memory/maxmemory, включающая
	// redis_memory (T-154): 80 %. При noeviction (ТЗ 3.2) заполнение
	// maxmemory — отказ записи XADD; 20 % запаса — межобрезочные остатки
	// TrimLoop (подтверждённый префикс до обрезки), фрагментация и буферы
	// клиентов, растущие быстрее обрезки (инцидент 22.10: 620 МиБ данных
	// при потолке 900 МиБ уже отказывал публикации).
	AlertRedisMemFireRatio = 0.80

	// AlertRedisMemRecoverRatio — снятие redis_memory ниже 75 %:
	// гистерезис 5 п.п. — память ползёт (остатки/фрагментация) и падает
	// резко (XTRIM/Drain), доля в коридоре 75–80 % не дребезжала бы Warn
	// у порога.
	AlertRedisMemRecoverRatio = 0.75

	// DefaultDiskFreeMinBytes — абсолютный минимум свободного места
	// каталога контента (T-162): 15 GiB Warn. Процентный порог
	// content.disk_free_percent на томе 79 ГБ поздний: 10 % = 8 ГБ ≈ 5 ч
	// до заполнения при темпе записи ≈36 GiB/сут (замер §12) — минимум
	// в байтах даёт оператору запас на разбор (освобождение
	// компактификацией — часы, docs/disks.md).
	DefaultDiskFreeMinBytes = int64(15 << 30)

	// DefaultDiskFreeCritBytes — эскалация абсолютного минимума (T-162):
	// 10 GiB — отдельный алерт disk_free_crit; на темпе 36 GiB/сут это
	// ≈6,5 ч до заполнения — время ещё есть, но действовать надо сейчас.
	DefaultDiskFreeCritBytes = int64(10 << 30)

	// DefaultQueueFreezeWindow — окно неподвижности last-delivered-id
	// группы стрима при глубине > 0 (T-162): 15 минут. Яд T-159 валит
	// потребителя на первой же записи — активный разбор (живая группа)
	// двигает last-delivered-id каждые секунды, 15 мин без движения при
	// ненулевой глубине — потребителя нет; короче — ложные срабатывания
	// на паузах между пачками, длиннее — теряется половина шага
	// «перезапусти потребителя и посмотри логи».
	DefaultQueueFreezeWindow = 15 * time.Minute

	// DefaultPSICPUPercent — порог PSI CPU some avg300 (T-162): 90 %.
	// Замер §12: рабочий голод планировщика — some 91–95 %; ниже 90 %
	// — утилизация без очереди (норма для CPU-bound конвейера).
	DefaultPSICPUPercent = 90.0

	// DefaultPSIWindow — окно устойчивости PSI (T-162): 30 мин. avg300
	// само сглаживает 5 минут, окно требует устойчивый голод — разовые
	// шпильки (деплой, разбор ретро-пачек) алертом не считаются.
	// Дискретизация оценки — период цикла алертов (monitor.
	// alerts_interval, 5 мин на дефолте — T-206 вернул частоту,
	// погрешность ~5 мин; на 30-мин цикле T-199 окно ≤ периода
	// вырождалось в «avg300 выше порога на двух соседних проходах» —
	// гарантированно ловился только голод ≳60 мин).
	DefaultPSIWindow = 30 * time.Minute

	// DefaultMemHighMinPasses — проходов алертов подряд с ростом счётчика
	// memory.events high (T-162): 2 × период алертов (5 мин на дефолте,
	// T-206) = 10 мин устойчивого троттлинга (на 30-мин цикле T-199
	// растягивалось до 1 ч). Единичный всплеск reclaim — штатная работа
	// MemoryHigh (850M у http-worker, T-145): мягкий потолок и должен
	// придавливать пики; устойчивый рост — предвестник фаталов T-157.
	DefaultMemHighMinPasses = 2

	// DefaultRenderDebtDomains — порог рендер-долга, включающий алерт
	// при росте (T-175): 50 тыс. доменов. Ориентир — фактический долг
	// морозки 24.10 (443 тыс. флагов + лаг q:render, найден ручным SQL)
	// на порядок выше порога, а здоровый фон — тысячи (локальное
	// переполнение при волне SPA гасится проходом доработки за минуты):
	// 50 тыс. — это сутки стагнации дренажа при темпе притока волны
	// (~0,6 дом/с), уверенное накопление, а не шум фона. Выше порога без
	// роста алерт не включается — стагнация видна строкой долга отчёта
	// нагрузки (T-173), алерт — про накопление.
	DefaultRenderDebtDomains = 50000

	// DefaultRenderDebtPasses — проходов алертов подряд с ростом долга
	// до срабатывания render_debt (T-175): 2 × период алертов (5 мин на
	// дефолте, T-206) = 10 мин непрерывного роста (по образцу
	// mem_high_passes; на 30-мин цикле T-199 растягивалось до 1 ч).
	// Разовый скачок (волна SPA против замороженного дренажа на минуту) —
	// штатное переполнение с флагами, проход доработки разбирает его сам;
	// устойчивый рост — переполнение стало стационарным, оператору
	// пора решать (контексты рендера, приток планировщика).
	DefaultRenderDebtPasses = 2
)

// dnsDailyCountersSQL — числитель и знаменатель доли temp_error текущих
// суток ДВУМЯ точечными чтениями суточных строк crawler_counters (T-182,
// TZ_slow_dns_share §7): temp_error:YYYY-MM-DD — события записей
// результатов с вердиктом temp_error (инкремент httpworker/record.go,
// T-182), checks:YYYY-MM-DD — события записей результатов (T-179);
// авторитет обеих строк — проход реконсиляции monitor (FILTER-колонки
// last_checked_at = current_date, занижены между проходами на DNS-only
// завершения — доля устойчива, абсолюты чинит проход). Имя дня
// вычисляет БД (current_date — та же таймзона, что у now() записи
// last_checked_at и окна реконсиляции); coalesce СНАРУЖИ подзапросов
// (стиль throughputSQL T-179): до первой записи суток подзапрос пуст —
// честный ноль. Прежний живой агрегат по окну суток (dnsShareSQL,
// count(*) FILTER … WHERE last_checked_at >= current_date) убран:
// Index Scan окна обязан был посещать heap каждой строки за статусом
// (status в индексе быть не может — HOT-инвариант, docs/schema.md),
// запрос №6 журнала медленных боя 24.10 (33 срабатывания >3 с, медиана
// 27,7 с, max 110,7 с — ≈1,3 ГБ чтений за оценку).
const dnsDailyCountersSQL = "SELECT " +
	"coalesce((SELECT value FROM crawler_counters " +
	"WHERE name = 'temp_error:' || to_char(current_date, 'YYYY-MM-DD')), 0), " +
	"coalesce((SELECT value FROM crawler_counters " +
	"WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')), 0)"

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
	// Дефолт «/sys» живёт только на боевом хосте: юнит-тесты всегда задают
	// SysFS каталогом, а чтение настоящего /sys негерметично (набор
	// устройств зависит от железа).
	// mutator-disable-next-line branch/if
	if sysfs == "" {
		// mutator-disable-next-line statement/remove
		sysfs = "/sys"
	}
	devfs := w.DevFS
	// Дефолт «/dev» — как у sysfs выше.
	// mutator-disable-next-line branch/if
	if devfs == "" {
		// mutator-disable-next-line statement/remove
		devfs = "/dev"
	}
	names, err := filepath.Glob(filepath.Join(sysfs, "class", "nvme", "nvme*"))
	// Glob с фиксированным валидным шаблоном ошибок не возвращает (только
	// malformed pattern) — ветка недостижима, тестом не убить.
	// mutator-disable-next-line branch/if
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
// (номер кодирует size=72). Мутант [2]struct{}{} тоже компилируется и
// поведением не наблюдаем — compile-time проверка.
// mutator-disable-next-line numbers/incrementer
var _ = [1]struct{}{}[unsafe.Sizeof(nvmeAdminCmd{})-nvmeAdminCmdSize]

// wearOf читает износ одного устройства: открывает char-узел контроллера
// /dev/nvmeX (O_RDWR — ioctl admin требует записи) и забирает страницу
// health одним NVME_IOCTL_ADMIN_CMD. Декодирование — parseHealthLog.
//
// mutator-disable-func — аппаратный passthrough: ioctl к /dev/nvme*
// исполняется только на боевом хосте; в CI-контейнере устройство не
// проброшено (юнит-тесты доходят лишь до отказа открытия, а значения
// возврата при ошибке игнорирует единственный вызывающий).
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
	// Эквивалентен только мутант конца среза page[48:57]: Uint64 читает
	// первые 8 байт, девятый не влияет. Аннотация действует на строку
	// целиком и заодно подавляет killable-мутанты page[5]→[6] и
	// page[49:56] — оба остаются закрытыми утверждениями
	// TestParseHealthLog; точечный инструмент (блэклист чексумм) отвергнут
	// как хрупкий (docs/mutation-testing.md §4).
	// mutator-disable-next-line numbers/incrementer
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

// BadHashesSource — счётчик мусорных content_hash очистки ingest
// (crawler_ingest_bad_hashes_total, T-099); реализация IngestBadHashes
// читает его из экспозиции /metrics сервиса ingest, юнит-тесты подменяют
// фейком. Отдельный интерфейс в стиле StreamLener/SpaceChecker: источник
// заменяем, алерт не зависит от HTTP.
type BadHashesSource interface {
	// BadHashes возвращает текущее значение счётчика (монотонно растёт
	// итогами проходов CleanupDeleted; рестарт ingest обнуляет).
	BadHashes(ctx context.Context) (int64, error)
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
	// BadHashes — счётчик мусорных content_hash очистки ingest (T-099);
	// nil — алерт «не измеряется».
	BadHashes BadHashesSource
	// UnitEvents — OOM/рестарты юнитов crawler (T-145); nil — алерт
	// «не измеряется». Дефолтной реализации ОС нет (в отличие от
	// Space/Wear): OSUnitEvents делает exec systemctl и читает хостовый
	// cgroupfs — в юнит-тестах пакета это недопустимо, источником
	// командует cmd/monitor (OSUnitEvents, обёрнутая в метрики
	// NewUnitEventsMetrics).
	UnitEvents UnitEventsSource
	// RedisMemory — used_memory/maxmemory из INFO memory (T-154);
	// nil — алерт redis_memory «не измеряется». Источником командует
	// cmd/monitor (RedisInfo того же клиента, что у гейта глубин,
	// обёрнутая в метрики NewRedisMemoryMetrics).
	RedisMemory RedisMemorySource
	// StreamGroups — группы рабочих стримов для алерта замороженной
	// очереди (T-162); nil — алерт «не измеряется». Источником командует
	// cmd/monitor (RedisStreamGroups того же клиента, что у гейта).
	StreamGroups StreamGroupsSource
	// PSI — давление CPU /proc/pressure/cpu (T-162); nil — OSPSI
	// (чистое чтение файла, как OSStatfs/OSWear).
	PSI PSISource
}

// AlertsOptions — параметры оценки; нулевые значения заменяются
// дефолтами (withDefaults, стиль ZoneStatsOptions).
type AlertsOptions struct {
	// Interval — период цикла AlertsLoop (DefaultAlertsInterval);
	// рабочее значение — monitor.alerts_interval (T-206): окна детекции
	// в проходах (AlertDeadMinPasses, MemHighMinPasses,
	// RenderDebtPasses, AlertBadHashesWindow) считаются от него.
	Interval time.Duration
	// ZoneStatsInterval — период ZoneStatsLoop: единица порога остановки
	// прогресса (AlertStallFactor × интервал; updated_at пишет цикл
	// реконсиляции — порог обязан считать его пропусками, а не проходы
	// алертов); 0 — DefaultZoneStatsInterval. Рабочее значение —
	// monitor.zone_stats_interval.
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
	// DiskFreeMinBytes — абсолютный минимум свободного места каталога
	// контента, Warn (monitor.alerts.disk_free_min_bytes, T-162);
	// ≤ 0 — DefaultDiskFreeMinBytes (выключения нет: абсолютный минимум
	// — страховка процентного порога, отключать её незачем).
	DiskFreeMinBytes int64
	// DiskFreeCritBytes — эскалация абсолютного минимума, отдельный
	// алерт (monitor.alerts.disk_free_crit_bytes, T-162); ≤ 0 —
	// DefaultDiskFreeCritBytes.
	DiskFreeCritBytes int64
	// QueueFreeze — окно неподвижности last-delivered-id группы при
	// глубине > 0 (monitor.alerts.queue_freeze_window, T-162);
	// ≤ 0 — DefaultQueueFreezeWindow.
	QueueFreeze time.Duration
	// PSICPUPercent — порог PSI CPU some avg300, %
	// (monitor.alerts.psi_cpu_percent, T-162); ≤ 0 — DefaultPSICPUPercent.
	PSICPUPercent float64
	// PSIWindow — окно устойчивости PSI выше порога
	// (monitor.alerts.psi_cpu_window, T-162); ≤ 0 — DefaultPSIWindow.
	PSIWindow time.Duration
	// MemHighMinPasses — проходов подряд с ростом счётчика
	// memory.events high до срабатывания memory_high
	// (monitor.alerts.mem_high_passes, T-162); ≤ 0 — DefaultMemHighMinPasses.
	MemHighMinPasses int
	// RenderDebtDomains — порог рендер-долга (флаги + лаг q:render),
	// включающий алерт при росте (monitor.alerts.render_debt_domains,
	// T-175); ≤ 0 — DefaultRenderDebtDomains.
	RenderDebtDomains int64
	// RenderDebtPasses — проходов подряд с ростом долга до срабатывания
	// render_debt (monitor.alerts.render_debt_passes, T-175); ≤ 0 —
	// DefaultRenderDebtPasses.
	RenderDebtPasses int
	// Now — источник времени (возраст zones.updated_at); nil — time.Now.
	Now func() time.Time
	// Log — логгер цикла; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// OnPass — хук экспорта состояния: снапшот каждого прохода (лог +
	// поле; см. шапку файла). Вызывается в цикле прохода — обязан быть
	// быстрым и не паниковать.
	OnPass func(AlertsResult)
}

// withDefaults заменяет неположительные периоды и пороги дефолтами (Now/Log
// проверяются отдельно: функций/указателей — nil).
func (o AlertsOptions) withDefaults() AlertsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultAlertsInterval
	}
	if o.ZoneStatsInterval <= 0 {
		o.ZoneStatsInterval = DefaultZoneStatsInterval
	}
	if o.DiskFreeMinBytes <= 0 {
		o.DiskFreeMinBytes = DefaultDiskFreeMinBytes
	}
	if o.DiskFreeCritBytes <= 0 {
		o.DiskFreeCritBytes = DefaultDiskFreeCritBytes
	}
	if o.QueueFreeze <= 0 {
		o.QueueFreeze = DefaultQueueFreezeWindow
	}
	if o.PSICPUPercent <= 0 {
		o.PSICPUPercent = DefaultPSICPUPercent
	}
	if o.PSIWindow <= 0 {
		o.PSIWindow = DefaultPSIWindow
	}
	if o.MemHighMinPasses <= 0 {
		o.MemHighMinPasses = DefaultMemHighMinPasses
	}
	if o.RenderDebtDomains <= 0 {
		o.RenderDebtDomains = DefaultRenderDebtDomains
	}
	if o.RenderDebtPasses <= 0 {
		o.RenderDebtPasses = DefaultRenderDebtPasses
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

// badHashesWindow — память окна всплеска мусорных content_hash (T-099):
// дельты счётчика между проходами алертов в кольце последних
// AlertBadHashesWindow значений. Первое наблюдение только базирует prev —
// старт мониторинга позже накопленного счётчика не ретроспективен (окно
// считает новые события, а не всю историю); убывание счётчика (рестарт
// ingest) даёт нулевую дельту — счётчик начинается заново, ложного
// всплеска нет.
type badHashesWindow struct {
	prev   int64
	seen   bool
	deltas []int64
	i      int
}

// push принимает текущее значение счётчика и возвращает сумму окна.
func (w *badHashesWindow) push(cur int64) int64 {
	var d int64
	if !w.seen {
		w.seen = true // база: накопленное до старта мониторинга не считается
		// Эквивалентный мутант: при cur == prev обе ветви дают дельту 0.
		// mutator-disable-next-line expression/comparison
	} else if cur > w.prev {
		d = cur - w.prev
	}
	w.prev = cur
	if w.deltas == nil {
		w.deltas = make([]int64, AlertBadHashesWindow)
	}
	w.deltas[w.i] = d
	// Эквивалентный мутант шага +2: 2 взаимно просто с длиной окна 15 —
	// кольцо в обоих случаях содержит последние 15 дельт, сумма окна
	// неотличима тестом.
	// mutator-disable-next-line numbers/incrementer
	w.i = (w.i + 1) % len(w.deltas)
	var sum int64
	for _, d := range w.deltas {
		sum += d
	}
	return sum
}

// memHighTrack — память трекинга троттлинга MemoryHigh одного юнита
// (T-162): прошлый счётчик memory.events high и серия проходов подряд с
// его ростом. Ростом считается только увеличение: рестарт юнита
// пересоздаёт cgroup и обнуляет счётчик — убыль начинает новую серию без
// ложного всплеска (стиль badHashesWindow).
type memHighTrack struct {
	prev   uint64
	streak int
	seen   bool
}

// freezeTrack — память трекинга замороженной группы стрима (T-162):
// последний замеченный last-delivered-id и момент, с которого он стоит.
// since живёт только при глубине > 0 — простой пустой очереди не
// накапливает «время без доставок» (см. evalQueueFreeze).
type freezeTrack struct {
	lastID string
	since  time.Time
}

// renderDebtTrack — память трекинга рендер-долга (T-175): прошлый
// снимок, серия проходов подряд с ростом (стиль memHighTrack) и защёлка
// сработавшего эпизода. Рост — строго увеличение Total: дренаж и приток
// плещутся вокруг константы — стагнация, а не накопление, серию не
// копит. Защёлка firing переживает «не измеряется» (сбой источника
// состояние алерта перезаписывает, ревью T-175 раунд 2, P2-1):
// удержание обязано опираться на неё, а не на прошлый статус — иначе
// короткий сбой БД/Redis посреди эпизода давал бы ложное ✅
// «Восстановлено» при долге выше порога снятия; сбрасывает защёлку
// только фактический дренаж ниже порога.
type renderDebtTrack struct {
	prev   int64
	streak int
	seen   bool
	firing bool
}

// Alerts — оценка алертов с памятью состояний между проходами
// (гистерезис, фронтовые записи лога только на переходах).
type Alerts struct {
	deps    AlertsDeps
	opts    AlertsOptions
	log     *slog.Logger
	now     func() time.Time
	state   map[string]string
	badHash badHashesWindow // окно всплеска мусорных content_hash (T-099)
	// deadStreak — проходов подряд с ненулевой глубиной q:dead (T-154):
	// «устойчиво ненулевой» вместо мгновенного срабатывания на первую
	// смерть; обнуляется пустым q:dead, ошибка источника не сбрасывает
	// (неизвестное наблюдение — не нулевое).
	deadStreak int
	// memHigh — трекинг memory.events high по юнитам (T-162), ключ — имя
	// юнита. Размер ограничен числом юнитов crawler (шаблон
	// unitEventsPattern); исчезнувшие юниты чистит evalMemHigh.
	memHigh map[string]memHighTrack
	// frozen — трекинг last-delivered-id по группам стримов (T-162),
	// ключ «стрим/группа». Записи живы только при глубине > 0.
	frozen map[string]freezeTrack
	// psiHot — момент первого наблюдения PSI выше порога (T-162);
	// нулевое время — давления нет, окно устойчивости не тикает.
	psiHot time.Time
	// renderDebt — трекинг роста рендер-долга (T-175): прошлый снимок и
	// серия растущих проходов.
	renderDebt renderDebtTrack
}

// NewAlerts строит оценщик: нормализует опции (дефолты периодов), источники
// Space/Wear/PSI (nil → реализации ОС) и готовит память состояний.
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
	if deps.PSI == nil {
		deps.PSI = OSPSI{}
	}
	return &Alerts{
		deps: deps, opts: o, log: log, now: now,
		state:   make(map[string]string),
		memHigh: make(map[string]memHighTrack),
		frozen:  make(map[string]freezeTrack),
	}
}

// evaluators — проверки прохода в фиксированном порядке (порядок
// снапшота AlertsResult).
func (a *Alerts) evaluators() []alertEval {
	return []alertEval{
		{AlertDNSErrors, a.evalDNS},
		{AlertQueueOverflow, a.evalQueues},
		{AlertProgressStall, a.evalStall},
		{AlertDiskFree, a.evalDisk},
		{AlertDiskMin, a.evalDiskMin},
		{AlertDiskCrit, a.evalDiskCrit},
		{AlertNVMeWear, a.evalWear},
		{AlertBadHashes, a.evalBadHashes},
		{AlertUnitOOM, a.evalUnitOOM},
		{AlertMemHigh, a.evalMemHigh},
		{AlertQueueDead, a.evalDead},
		{AlertQueueFreeze, a.evalQueueFreeze},
		{AlertRedisMemory, a.evalRedisMemory},
		{AlertPSICPU, a.evalPSICPU},
		{AlertRenderDebt, a.evalRenderDebt},
	}
}

// mutator-disable-regexp a\.log\.Debug\("проход statement/remove
// mutator-disable-regexp a\.log\.Info\("цикл statement/remove
// Обоснование: служебные записи цикла (итог прохода Debug, старт Info) —
// поведение алертов не меняют, гасятся по docs/mutation-testing.md §4.

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

// evalDNS — доля temp_error среди проверок текущих суток: числитель и
// знаменатель — суточные строки crawler_counters (T-182, точечные
// чтения dnsDailyCountersSQL; события записей результатов — ретраи и
// дубли доставки остаются в истории событий, доля устойчива, абсолюты
// чинит реконсиляция). До 100 событий знаменателя (AlertDNSMinSamples)
// доля — шум пустого окна, «не измеряется». Гистерезис прежний (пороги
// T-080/T-162 не меняются — T-182 меняет только источник чисел):
// включение ≥ AlertDNSFireShare, при сработавшем алерте удержание до
// падения ниже AlertDNSRecoverShare.
func (a *Alerts) evalDNS(ctx context.Context, prev string) verdict {
	rows, err := a.deps.Pool.Query(ctx, dnsDailyCountersSQL)
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
	// 100 000 записей ограничиваются половиной его MAXLEN по политике
	// (DefaultStreamMaxLen, T-141: 125 пачек).
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
	// Эквивалентный мутант alertStreams[0:]: первый элемент не превосходит
	// сам себя (старт максимума), итог не меняется.
	// mutator-disable-next-line numbers/decrementer
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

// evalDiskMin / evalDiskCrit — абсолютный минимум свободного места
// каталога контента (T-162): Warn при Avail < disk_free_min_bytes (15 GiB)
// и эскалация отдельным алертом при < disk_free_crit_bytes (10 GiB).
// Процентный порог (content.disk_free_percent) на большом томе поздний:
// 10 % от 79 ГБ — это 8 ГБ ≈ 5 ч до заполнения при темпе ≈36 GiB/сут
// (замер §12); абсолютный минимум даёт запас на разбор независимо от
// размера тома. Гистерезиса нет, как у процентного: место при инциденте
// убывает монотонно (~1,5 ГБ/ч) — дребезг у порога нечему двигать;
// срабатывание строгое (<), восстановление при возврате к порогу.
// Эскалация — второй алерт, а не «уровень» первого: каждое состояние
// видно своей парой сообщений срабатывание/снятие (дебаунс notify.go)
// и своей строкой в журнале, иерархия Warn→усиление читается по имени.
func (a *Alerts) evalDiskMin(_ context.Context, _ string) verdict {
	return a.evalDiskBytes(a.opts.DiskFreeMinBytes)
}

func (a *Alerts) evalDiskCrit(_ context.Context, _ string) verdict {
	return a.evalDiskBytes(a.opts.DiskFreeCritBytes)
}

// evalDiskBytes — общая проверка абсолютного порога: тот же снимок
// statfs, что у процентного алерта (две копии сисколла на проход дешевле
// связывания алертов общим кэшем). Порог ≥ ёмкости тома неприменим:
// свободное место никогда не достигнет минимума (dev-том 10 ГБ против
// 15 GiB) — вечное срабатывание было бы ложным, малые тома остаются
// под крылом процентного порога.
func (a *Alerts) evalDiskBytes(threshold int64) verdict {
	if a.opts.ContentDir == "" {
		return verdict{status: AlertStatusNotMeasured, detail: "каталог контента не задан"}
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
	if threshold >= int64(sp.Total) {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: fmt.Sprintf("минимум %s ≥ ёмкости тома %s — порог неприменим, малый том под крылом процентного",
				alertBytes(uint64(threshold)), alertBytes(sp.Total)),
		}
	}
	detail := fmt.Sprintf("свободно %s из %s, порог %s",
		alertBytes(sp.Avail), alertBytes(sp.Total), alertBytes(uint64(threshold)))
	if int64(sp.Avail) < threshold {
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
	// Эквивалентный мутант wears[0:]: первый элемент не превосходит сам
	// себя (старт максимума), итог не меняется.
	// mutator-disable-next-line numbers/decrementer
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

// evalBadHashes — всплеск мусорных content_hash очистки ingest (T-099):
// сумма дельт счётчика за окно AlertBadHashesWindow проходов ≥
// AlertBadHashesThreshold — срабатывание. Снятие — старением: окно
// держит сработавшим только при продолжении потока новых мусорных
// строк; гистерезис не нужен (см. AlertBadHashesWindow). Ошибка
// источника (ingest недоступен, метрики нет) — «не измеряется» с Warn:
// смерть самого ingest видна по crawler_service_up, а не этим алертом.
func (a *Alerts) evalBadHashes(ctx context.Context, _ string) verdict {
	if a.deps.BadHashes == nil {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: "источник не задан (monitor.ingest_metrics_url)",
		}
	}
	cur, err := a.deps.BadHashes.BadHashes(ctx)
	if err != nil {
		return verdict{err: fmt.Errorf("monitor: мусорные content_hash: %w", err)}
	}
	sum := a.badHash.push(cur)
	detail := fmt.Sprintf("счётчик %d, новых за окно %s — %d (порог %d)",
		cur, (time.Duration(AlertBadHashesWindow) * a.opts.Interval).Round(time.Second),
		sum, AlertBadHashesThreshold)
	if sum >= AlertBadHashesThreshold {
		return verdict{
			status: AlertStatusFiring,
			detail: detail + " — ошибка писателя результатов (T-056), refcount не менялся",
		}
	}
	return verdict{status: AlertStatusOK, detail: detail}
}

// evalUnitOOM — события юнитов crawler (T-145): срабатывание, пока у
// любого юнита есть oom/oom_kill (memory.events) или NRestarts > 0.
// Счётчики монотонны внутри жизни cgroup/юнита — гистерезис не нужен
// (стиль износа NVMe): эпизод тянется от первого события до сброса
// состояния юнита — перезапуска (деплой сбрасывает сам: stop →
// reset-failed → start в deploy-install.sh; reset-failed/старт обнуляет
// NRestarts, пересозданная cgroup забирает oom/oom_kill с собой). Один
// Warn на эпизод даёт фронтовая механика transition. Ошибка источника
// класса errNoSystemd (dev-контейнер) — тихое «не измеряется», как у
// SMART: окружение без systemd — норма dev, а не авария; прочие ошибки
// (боевой хост, systemd/cgroupfs сломан) — Warn каждого прохода, стиль
// остальных источников (зонд ingest).
func (a *Alerts) evalUnitOOM(ctx context.Context, _ string) verdict {
	if a.deps.UnitEvents == nil {
		return verdict{status: AlertStatusNotMeasured, detail: "источник не задан"}
	}
	units, err := a.deps.UnitEvents.UnitEvents(ctx)
	if err != nil {
		if errors.Is(err, errNoSystemd) {
			return verdict{status: AlertStatusNotMeasured, detail: err.Error()}
		}
		// Без повторного «monitor: юниты:» — источник уже ставит префикс.
		return verdict{err: err}
	}
	if len(units) == 0 {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: "юниты " + unitEventsPattern + " не найдены",
		}
	}
	var hot []string
	for _, u := range units {
		if !u.hot() {
			continue
		}
		hot = append(hot, fmt.Sprintf("%s: oom=%d oom_kill=%d рестартов=%d",
			u.Unit, u.OOM, u.OOMKill, u.NRestarts))
	}
	if len(hot) > 0 {
		return verdict{
			status: AlertStatusFiring,
			detail: strings.Join(hot, ", ") + " — эпизод до перезапуска юнита (NRestarts — reset-failed, oom/oom_kill — новая cgroup)",
		}
	}
	return verdict{
		status: AlertStatusOK,
		detail: fmt.Sprintf("OOM и рестартов нет (%d юнитов)", len(units)),
	}
}

// evalMemHigh — троттлинг MemoryHigh (T-162): счётчик memory.events high
// юнита растёт от прохода к проходу (каждое попадание за мягкий потолок —
// принудительный reclaim/задержка). Порог — не значение, а скорость:
// счётчик накапливается за жизнь cgroup (http-worker с MemoryHigh=850M
// набирает >100 тыс. за сутки), осмыслен только рост. Устойчивость — рост
// MemHighMinPasses проходов подряд (2 × период алертов = 10 мин на
// дефолтных 5 мин, T-206): одиночный
// всплеск — штатная работа мягкого потолка, устойчивый троттлинг —
// предвестник фаталов http-worker (T-157: рестарты по памяти до
// OOM-kill) и запрос потолка выше (docs/deploy.md §2). Снятие — первый
// проход без роста: гистерезис не нужен, событие снятия и есть «рост
// прекратился», дребезг у нуля серии невозможен (серия целая).
// Источник и деградации — те же, что у unit_oom: errNoSystemd (dev)
// тихо «не измеряется», прочие ошибки — Warn.
func (a *Alerts) evalMemHigh(ctx context.Context, _ string) verdict {
	if a.deps.UnitEvents == nil {
		return verdict{status: AlertStatusNotMeasured, detail: "источник не задан"}
	}
	units, err := a.deps.UnitEvents.UnitEvents(ctx)
	if err != nil {
		if errors.Is(err, errNoSystemd) {
			return verdict{status: AlertStatusNotMeasured, detail: err.Error()}
		}
		return verdict{err: err}
	}
	seen := make(map[string]bool, len(units))
	var hot []string
	for _, u := range units {
		seen[u.Unit] = true
		tr := a.memHigh[u.Unit]
		grew := int64(0)
		if tr.seen && u.High > tr.prev {
			tr.streak++
			grew = int64(u.High - tr.prev)
		} else {
			tr.streak = 0
		}
		tr.seen, tr.prev = true, u.High
		a.memHigh[u.Unit] = tr
		if tr.streak >= a.opts.MemHighMinPasses {
			hot = append(hot, fmt.Sprintf("%s: +%d за проход, серия %d", u.Unit, grew, tr.streak))
		}
	}
	for name := range a.memHigh { // юнит исчез (переименован/выгружен) — не копить
		if !seen[name] {
			delete(a.memHigh, name)
		}
	}
	if len(hot) > 0 {
		return verdict{
			status: AlertStatusFiring,
			detail: strings.Join(hot, ", ") + " — устойчивый троттлинг MemoryHigh (reclaim), предвестник фаталов (T-157)",
		}
	}
	return verdict{
		status: AlertStatusOK,
		detail: fmt.Sprintf("роста троттлинга MemoryHigh нет (%d юнитов, порог — рост %d проходов подряд)",
			len(units), a.opts.MemHighMinPasses),
	}
}

// evalDead — устойчиво ненулевая глубина q:dead (T-154): карантин без
// потребителей (ручной разбор, ТЗ 4.2), Len читает XLEN, покидают записи
// карантин только разбором оператора (XTRIM/runbooks §2) — глубина и есть
// «неразобранный остаток». Срабатывание — глубина > 0 AlertDeadMinPasses
// проходов подряд; сработавший алерт держит любая ненулевая глубина
// (гистерезис: эпизод от третьего прохода до разбора), снятие — пустой
// q:dead. Отдельный контроль роста не нужен: рост — частный случай
// устойчивой ненулевости, а дифференциация «растёт/стоит» дребезжала бы
// на подвижном XTRIM разбора. Источник — тот же Redis, что у алерта
// очередей (StreamLener), сверка со счётчиком смертей — темп
// crawler_queue_dead_buried_total (T-154) объясняет, откуда сток.
func (a *Alerts) evalDead(ctx context.Context, prev string) verdict {
	if a.deps.Redis == nil {
		return verdict{status: AlertStatusNotMeasured, detail: "Redis не задан"}
	}
	n, err := a.deps.Redis.Len(ctx, queue.StreamDead)
	if err != nil {
		return verdict{err: fmt.Errorf("monitor: глубина q:dead: %w", err)}
	}
	if n > 0 {
		a.deadStreak++
	} else {
		a.deadStreak = 0
	}
	detail := fmt.Sprintf("глубина q:dead=%d, ненулевых проходов подряд %d/%d; темп смертей — crawler_queue_dead_buried_total",
		n, a.deadStreak, AlertDeadMinPasses)
	if a.deadStreak >= AlertDeadMinPasses || (prev == AlertStatusFiring && n > 0) {
		return verdict{status: AlertStatusFiring, detail: detail + " — разбор: runbooks §2"}
	}
	return verdict{status: AlertStatusOK, detail: detail}
}

// evalQueueFreeze — замороженная очередь (T-162): last-delivered-id
// группы рабочего стрима не движется ≥ queue_freeze_window при глубине
// > 0. Живой потребитель двигает last-delivered-id каждым XREADGROUP
// (пачки в секунды — ТЗ 3.3); стоящий ID при непустом backlog — записи
// не доставляются вовсе: потребитель мёртв или валится на первой записи
// (яд T-159), lease-восстановление не поднимает. Согласована с
// BackpressureGate.Len о глубине (кламп T-168 в источнике):
// застрявший фантомный лаг инцидента 22–23.10 сам по себе не «глубина»,
// но замороженные доставки там были — алерт ловит и этот класс простоев.
//
// Трекинг: запись группы живёт только при Depth > 0 (пустая очередь не
// должна копить «время без доставок» — иначе час простоя + первая
// нечитаемая запись дали бы мгновенное ложное срабатывание); движение ID
// обновляет момент; первое наблюдение (нет записи) — база с текущего
// момента: после рестарта monitor алерт сработает не раньше окна, а не
// ретроспективно. Ошибка источника память не трогает — восстановление
// чтения доедет по фактическому ID. Снятие — первый проход, где группы
// ни одна не заморожена (глубина ушла в ноль или доставки пошли):
// гистерезис не нужен, окно 15 мин — естественный фильтр дребезга.
func (a *Alerts) evalQueueFreeze(ctx context.Context, _ string) verdict {
	if a.deps.StreamGroups == nil {
		return verdict{status: AlertStatusNotMeasured, detail: "источник не задан"}
	}
	groups, err := a.deps.StreamGroups.StreamGroups(ctx)
	if err != nil {
		return verdict{err: err}
	}
	now := a.now()
	seen := make(map[string]bool, len(groups))
	var frozen []string
	var moving int
	for _, g := range groups {
		key := g.Stream + "/" + g.Group
		seen[key] = true
		if g.Depth <= 0 {
			delete(a.frozen, key) // работа кончилась — копить нечего
			moving++
			continue
		}
		tr, ok := a.frozen[key]
		if !ok || g.LastDeliveredID != tr.lastID {
			a.frozen[key] = freezeTrack{lastID: g.LastDeliveredID, since: now}
			moving++
			continue
		}
		idle := now.Sub(tr.since)
		if idle >= a.opts.QueueFreeze {
			frozen = append(frozen, fmt.Sprintf("%s/%s: доставки нет %s при глубине %d (последний ID %s)",
				g.Stream, g.Group, idle.Round(time.Second), g.Depth, g.LastDeliveredID))
		}
	}
	for key := range a.frozen { // группа исчезла (стрим удалён) — не копить
		if !seen[key] {
			delete(a.frozen, key)
		}
	}
	if len(frozen) > 0 {
		return verdict{
			status: AlertStatusFiring,
			detail: strings.Join(frozen, "; ") + " — потребитель не читает: лог юнита-воркера и q:dead (яд T-159), перезапуск воркера",
		}
	}
	return verdict{
		status: AlertStatusOK,
		detail: fmt.Sprintf("групп с движением/без backlog — %d из %d (окно %s)",
			moving, len(groups), a.opts.QueueFreeze),
	}
}

// evalRedisMemory — доля памяти Redis used_memory/maxmemory из INFO
// memory (T-154): включение ≥ AlertRedisMemFireRatio (80 %), удержание
// сработавшего до AlertRedisMemRecoverRatio (75 %) — гистерезис против
// дребезга у порога (память ползёт межобрезочными остатками TrimLoop
// и падает резко после XTRIM). При noeviction (политика бюджета ТЗ 3.2)
// заполнение maxmemory — отказ записи XADD: прямого счётчика отказов
// INFO не отдаёт, доля — прокси-метрика с запасом на буферы/фрагментацию
// (инцидент 22.10: OOM-отказы публикаций при 620 из 900 МиБ).
// maxmemory 0 (без потолка) — «не измеряется»: отношение не определено,
// отказов записи по maxmemory не бывает.
func (a *Alerts) evalRedisMemory(ctx context.Context, prev string) verdict {
	if a.deps.RedisMemory == nil {
		return verdict{status: AlertStatusNotMeasured, detail: "источник не задан"}
	}
	m, err := a.deps.RedisMemory.RedisMemory(ctx)
	if err != nil {
		return verdict{err: fmt.Errorf("monitor: память Redis: %w", err)}
	}
	if m.MaxBytes <= 0 {
		return verdict{
			status: AlertStatusNotMeasured,
			detail: fmt.Sprintf("maxmemory=%d — потолка нет, доля не определена", m.MaxBytes),
		}
	}
	share := float64(m.UsedBytes) / float64(m.MaxBytes)
	detail := fmt.Sprintf("used_memory %s из %s maxmemory (%.1f%%), политика %s",
		alertBytes(uint64(m.UsedBytes)), alertBytes(uint64(m.MaxBytes)), 100*share, m.Policy)
	if share >= AlertRedisMemFireRatio || (prev == AlertStatusFiring && share >= AlertRedisMemRecoverRatio) {
		return verdict{
			status: AlertStatusFiring,
			detail: detail + " — при noeviction у порога отказы записи (OOM command not allowed)",
		}
	}
	return verdict{status: AlertStatusOK, detail: detail}
}

// evalPSICPU — голод CPU по PSI some avg300 (T-162): выше порога
// psi_cpu_percent (90 %) устойчиво psi_cpu_window (30 мин) — срабатывание.
// Метрика — some (задержка хотя бы одной задачи), не full и не load
// average: утилизация 100 % без очереди — норма CPU-bound конвейера, а
// вот some ≈90+ % — этапы ждут CPU втроём-вчетвером (замер §12), тонут
// все разом (T-158 поэтому понизил вес monitor'у). Устойчивость —
// первый проход выше порога стартует окно psiHot, срабатывание при
// его закрытии; avg300 сам сглаживает 5 минут, окно отбрасывает разовые
// шпильки (деплой, ретро-пачки). Гистерезис не нужен: чтобы «снять»,
// avg300 должен опуститься к порогу после 30+ минут выше — это
// фактическое восстановление, а не дребезг; сброс окна при кратком
// провале — консервативнее удержания (шпилька не продлевает алерт).
// Ядро без PSI — errNoPSI, тихая деградация (как SMART в контейнере).
func (a *Alerts) evalPSICPU(ctx context.Context, _ string) verdict {
	st, err := a.deps.PSI.PSI(ctx)
	if err != nil {
		if errors.Is(err, errNoPSI) {
			return verdict{status: AlertStatusNotMeasured, detail: err.Error()}
		}
		return verdict{err: fmt.Errorf("monitor: PSI CPU: %w", err)}
	}
	now := a.now()
	over := st.Avg300 > a.opts.PSICPUPercent
	switch {
	case !over:
		a.psiHot = time.Time{}
	case a.psiHot.IsZero():
		a.psiHot = now
	}
	detail := fmt.Sprintf("some avg10=%.2f%% avg60=%.2f%% avg300=%.2f%%, порог %.0f%% (окно %s)",
		st.Avg10, st.Avg60, st.Avg300, a.opts.PSICPUPercent, a.opts.PSIWindow)
	if over {
		held := now.Sub(a.psiHot)
		if held >= a.opts.PSIWindow {
			return verdict{
				status: AlertStatusFiring,
				detail: detail + fmt.Sprintf(" — выше порога уже %s", held.Round(time.Second)),
			}
		}
		return verdict{
			status: AlertStatusOK,
			detail: detail + fmt.Sprintf(" — выше порога %s из окна, накопление", held.Round(time.Second)),
		}
	}
	return verdict{status: AlertStatusOK, detail: detail}
}

// evalRenderDebt — накопление рендер-долга (T-175): срабатывание —
// долг (флаги render_pending по счётчику crawler_counters + лаг q:render,
// renderdebt.go) выше monitor.alerts.render_debt_domains И растёт
// render_debt_passes проходов подряд (серия положительных дельт,
// стиль evalMemHigh). Снятие — дренаж долга ниже 90 % порога: долг —
// УРОВЕНЬ (флаги + лаг), а не монотонный счётчик, лаг q:render
// колеблется на сотни-тысячи за проход при дренаже ~1–2 стр/с — снятие
// «по не-росту» качало бы эпизод 🚨/✅ каждые несколько проходов на
// стационарном высоком долге (ревью T-175, P2-1). Поэтому рост — только
// триггер (стагнация без серии роста не будит оператора — она видна
// строкой «Рендер-долг» отчёта нагрузки T-173 и gauge), уровень —
// удержание и снятие (гистерезис 90 % против дребезга у порога, стиль
// очередей AlertQueueRecoverFactor; ceil, чтобы малые пороги не теряли
// зазор — при пороге 1 снятие < 1 достигается нулём, ревью раунд 2
// P3-3). Удержание опирается на защёлку трекинга, а не прошлый статус:
// «не измеряется» (сбой источника) состояние алерта перезаписывает, а
// эпизод не рвёт (ревью раунд 2, P2-1). Первый проход — база (рост
// неизвестен). Порог, а не только рост: здоровый фон — тысячи флагов
// локальных переполнений, алертом считается именно накопление в
// масштабе суток дренажа. Источники — те же пул БД и гейт глубин, что у
// остальных алертов (счётчик — точечный Lookup, лаг — XLEN той же
// семантики, что queue_overflow): ошибка любого — «не измеряется» с
// Warn, память трекинга не трогается (восстановление доедет по
// фактическому долгу). Отсутствие строки счётчика (ручная правка
// таблицы, runbooks §6) — ошибка с Warn, а не тихий ноль: gauge долга
// не должен лгать.
func (a *Alerts) evalRenderDebt(ctx context.Context, _ string) verdict {
	if a.deps.Redis == nil {
		return verdict{status: AlertStatusNotMeasured, detail: "Redis не задан"}
	}
	d, err := renderDebtOf(ctx, a.deps.Pool, a.deps.Redis)
	if err != nil {
		return verdict{err: err}
	}
	tr := a.renderDebt
	var delta int64
	if tr.seen && d.Total() > tr.prev {
		tr.streak++
		delta = d.Total() - tr.prev
	} else {
		tr.streak = 0
	}
	tr.seen, tr.prev = true, d.Total()

	// Порог снятия — 90 % порога срабатывания, целочисленный ceil:
	// гистерезис против дребезга у порога (ревью T-175, P2-1), коридор
	// ≥ 1 при любом допустимом пороге (минимум 1) — сработавший алерт
	// всегда достижим для снятия (ревью раунд 2, P3-3); при 50 тыс.
	// снятие — ниже 45 тыс.
	recoverAt := (a.opts.RenderDebtDomains*9 + 9) / 10
	detail := fmt.Sprintf("рендер-долг: %s, порог %d (снятие < %d), серия роста %d/%d проходов (+%d за проход)",
		d.String(), a.opts.RenderDebtDomains, recoverAt, tr.streak, a.opts.RenderDebtPasses, delta)
	var v verdict
	switch {
	case d.Total() > a.opts.RenderDebtDomains && tr.streak >= a.opts.RenderDebtPasses:
		tr.firing = true
		v = verdict{
			status: AlertStatusFiring,
			detail: detail + " — накопление: приток SPA обгоняет дренаж (render.contexts / приток планировщика, разбор — docs/runbooks.md)",
		}
	case tr.firing && d.Total() >= recoverAt:
		// Удержание сработавшего: колебания лага при стационарном долге
		// (серия роста сброшена, уровень выше порога снятия) — эпизод
		// живёт до фактического дренажа. Защёлка, а не прошлый статус:
		// сбой источника («не измеряется») эпизод не закрывает.
		v = verdict{
			status: AlertStatusFiring,
			detail: detail + " — долг держится выше порога снятия (уровень — строка «Рендер-долг» отчёта нагрузки)",
		}
	default:
		tr.firing = false
		v = verdict{status: AlertStatusOK, detail: detail}
	}
	a.renderDebt = tr
	return v
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
