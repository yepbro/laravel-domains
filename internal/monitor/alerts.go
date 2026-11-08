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
//     долг 443 тыс. (морозка 24.10) нашли ручным SQL;
//   - рост глубины очереди фолбэка рендера (T-246, ТЗ рендера §4.6
//     п.3) — глубина q:render-fallback (той же семантикой, что алерт
//     очередей: backlog группы потребления воркера B) у эффективного
//     порога стрима И растёт 2 прохода подряд, ИЛИ держится над
//     порогом 2 прохода подряд без роста (S-P2-1: стационарная
//     глубина — остановленный B, упор в MAXLEN или эпизод после
//     рестарта монитора) — chromium-воркер B не
//     успевает дорабатывать (темп воркера ниже притока фолбэка, вплоть
//     до полной остановки B — чтение Redis живо при мёртвом воркере,
//     метрика crawler_queue_depth{queue="q:render-fallback"} экспонирует
//     сам B и при его смерти ряда нет);
//   - превышение бюджета доли фолбэка (T-246, ТЗ рендера §4.6 п.3) —
//     рост счётчика событий приостановки
//     crawler_render_fallback_suspended_total{reason="budget"} (T-239,
//     зонд /metrics рендер-воркера, suspends.go): доля публикаций за
//     окно бюджета ушла за потолок render.fallback.share_percent —
//     деградация основного движка (движко-зависимые финализации
//     множатся), задачи при этом не теряются (финализация штатная, ФТ-9).
//     Падение юнита движка отдельным алертом не вводится: юниты движков
//     рендера — crawler-*.service (crawler-chromium сегодня, moli/
//     lightpanda по ТЗ рендера §4.7 п.3) — покрывает существующий
//     unit_oom (T-145, шаблон unitEventsPattern);
//   - устойчивый рост browser-ошибок этапа рендера (S-P2-6, ТЗ рендера
//     §4.6 п.3, решение оператора — rate-детектор) — счётчик
//     crawler_stage_errors_total{stage="render",type="browser"} из
//     того же зонда /metrics рендер-воркера растёт 2 проходов подряд:
//     движок-кандидат (moli/lightpanda) завис или недоступен (гейт
//     T-239 приостанавливает фолбэк-публикации; событие гейта —
//     счётчик suspended{reason="gate"} и Warn-лог воркера, детектор —
//     здесь);
//   - домены «статус повис» (T-256, ТЗ-fsm §3 ФТ-2.2) — строка
//     crawler_counters new_done_stale (FILTER-колонка прохода
//     реконсиляции zonestats.go: status = new ∧ check_state = done ∧
//     last_checked_at старше суток — DNS записал «есть адреса», HTTP
//     итог не пришёл: потеря задачи q:http, компенсатора нет)
//     ненулевая в 2 НОВЫХ снапшотах реконсиляции подряд (серия — по
//     смене updated_at строки: проходы алертов чаще источника,
//     повторные чтения одного снапшота наблюдением не считаются).
//     Точечное PK-чтение, без скана domains; стационарно ноль в норме.
//     Автолечения нет — лечит регулярная перепроверка T-030, алерт
//     только сигнал оператору (gauge crawler_domains_new_done_stale
//     экспонирует уровень).
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
//
// Автомат алертов (T-264, ТЗ tasks/TZ-fsm.md §3 ФТ-10): каждый алерт —
// значение единой структуры «{имя, статус, track-память}»
// (alertMachine) с единственной точкой записи состояния transition;
// eval-функции этого файла возвращают наблюдение (observation), решение
// принимают типизированные track-памяти. Спецификация состояний и
// переходов — internal/monitor/alertstate.go.
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
	AlertDNSErrors        = "dns_temp_error_share"   // доля temp_error за сутки
	AlertQueueOverflow    = "queue_overflow"         // глубина очереди у порога гейта
	AlertProgressStall    = "progress_stall"         // zones.updated_at устарел
	AlertDiskFree         = "disk_free"              // место каталога контента
	AlertNVMeWear         = "nvme_wear"              // SMART percentage_used
	AlertBadHashes        = "ingest_bad_hashes"      // всплеск мусорных content_hash
	AlertUnitOOM          = "unit_oom"               // OOM/рестарты юнитов crawler (T-145)
	AlertQueueDead        = "queue_dead"             // неразобранный q:dead (T-154)
	AlertRedisMemory      = "redis_memory"           // память Redis у maxmemory (T-154)
	AlertDiskMin          = "disk_free_min"          // абсолютный минимум места, Warn (T-162)
	AlertDiskCrit         = "disk_free_crit"         // эскалация абсолютного минимума (T-162)
	AlertMemHigh          = "memory_high"            // троттлинг MemoryHigh (T-162)
	AlertQueueFreeze      = "queue_frozen"           // замороженная очередь (T-162)
	AlertPSICPU           = "psi_cpu"                // PSI CPU some (T-162)
	AlertRenderDebt       = "render_debt"            // накопление рендер-долга (T-175)
	AlertRenderFbDepth    = "render_fallback_depth"  // рост глубины q:render-fallback (T-246)
	AlertRenderFbBudget   = "render_fallback_budget" // превышение бюджета доли фолбэка (T-246)
	AlertRenderBrowserErr = "render_browser_errors"  // рост browser-ошибок этапа: движок недоступен (S-P2-6)
	AlertDomainsStale     = "domains_new_done_stale" // домены «статус повис» new∧done старше суток (T-256, ФТ-2.2)
)

// alertDescs — человекочитаемые описания алертов для записей лога.
var alertDescs = map[string]string{
	AlertDNSErrors:        "рост доли ошибок DNS",
	AlertQueueOverflow:    "переполнение очередей",
	AlertProgressStall:    "остановка прогресса",
	AlertDiskFree:         "нехватка места на диске",
	AlertNVMeWear:         "износ NVMe (SMART)",
	AlertBadHashes:        "мусорные content_hash при очистке удалённых доменов",
	AlertUnitOOM:          "OOM-убийства и крэш-рестарты юнитов crawler",
	AlertQueueDead:        "мертвые задачи в q:dead не разбираются",
	AlertRedisMemory:      "память Redis у порога maxmemory",
	AlertDiskMin:          "свободное место ниже абсолютного минимума",
	AlertDiskCrit:         "свободное место ниже порога эскалации",
	AlertMemHigh:          "троттлинг MemoryHigh юнита (устойчивый рост reclaim-событий)",
	AlertQueueFreeze:      "замороженная очередь: группа не читает стрим",
	AlertPSICPU:           "голод CPU (PSI some) устойчиво выше порога",
	AlertRenderDebt:       "накопление рендер-долга: приток SPA обгоняет дренаж q:render",
	AlertRenderFbDepth:    "накопление очереди фолбэка q:render-fallback: воркер B не успевает",
	AlertRenderFbBudget:   "превышение бюджета доли фолбэка: деградация основного движка",
	AlertRenderBrowserErr: "рост browser-ошибок этапа рендера: движок завис или недоступен",
	AlertDomainsStale:     "домены «статус повис»: new с завершённой проверкой старше 24 ч",
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

	// AlertRenderFallbackPasses — проходов алертов подряд до срабатывания
	// render_fallback_depth (T-246): 2 × период алертов (5 мин на
	// дефолте, T-206) = 10 мин (по образцу render_debt_passes/
	// mem_high_passes). Порог сам по себе не будит оператора: разовый
	// всплеск публикаций (эпизод деградации движка, разобранный
	// health-гейтом) — штатная работа фолбэка «собрать и доработать»
	// (ФТ-13), воркер B сгребает его темпом; серия из проходов — либо
	// непрерывный рост (темпа воркера не хватает или B стоит), либо
	// стационарная глубина над порогом (второй триггер, S-P2-1:
	// остановленный B, упор в предел MAXLEN стрима — DefaultStreamMaxLen:
	// q:render-fallback — 15 000 — или эпизод, переживший рестарт
	// монитора) — накопление approaching предела MAXLEN, оператору
	// пора поднимать потолок темпа/движок B. Константа, а не поле
	// конфигурации: окно связано механикой с серией render_debt, а порог
	// уровня — уже конфигурируемый scheduler.queue_high_watermark.
	AlertRenderFallbackPasses = 2

	// AlertFallbackBudgetWindow — окно событий приостановки бюджета доли
	// фолбэка в проходах алертов (T-246): 6 × период алертов (5 мин на
	// дефолте, T-206) = 30 мин. Бюджетное окно T-239 — 1000
	// подтверждённых доставок этапа (≈17 мин при ёмкости рендера ~60/мин,
	// замер §12): окно алерта вмещает эпизод и один рецидив следующего
	// окна; снятие — старением (события — защёлкивания, не состояние:
	// пока приостановка держится, новых точек нет — держит алерт только
	// окно, оператору виден fallback-рейт панели движков). Событие —
	// всегда аномалия (в норме публикации укладываются в потолок доли),
	// порог — одно событие, стиль износа NVMe.
	AlertFallbackBudgetWindow = 6

	// AlertRenderBrowserErrorsPasses — проходов алертов подряд с ростом
	// счётчика browser-ошибок этапа рендера до срабатывания
	// render_browser_errors (S-P2-6, решение оператора — rate-детектор):
	// 2 × период алертов (5 мин на дефолте, T-206) = 10 мин (по образцу
	// mem_high/render_debt). Разовая browser-ошибка — штатная работа ядра
	// рендера (перезапуск сессии/контекста, T-063) и эпизод, разбираемый
	// health-гейтом; серия роста — движок (moli/lightpanda) завис или
	// недоступен: гейт T-239 уже приостановил фолбэк-публикации,
	// браузер-класс ждёт в redelivery q:render — оператору нужен сигнал
	// про сам движок. Событие гейта остаётся счётчиком
	// suspended{reason="gate"} и Warn-логом воркера: гейт — порог стрика
	// в воркере, алерт — наблюдаемость средствами монитора.
	AlertRenderBrowserErrorsPasses = 2

	// AlertNewDoneStalePasses — НОВЫХ снапшотов реконсиляции подряд с
	// ненулевой строкой new_done_stale до срабатывания domains_new_done_stale
	// (T-256, ФТ-2.2): 2 × период реконсиляции (30 мин на дефолте,
	// T-199) ≈ 1 ч устойчивого ненулевого значения. Проходы алертов
	// (5 мин, T-206) чаще проходов источника — серия растёт только при
	// смене updated_at снапшота, повторные чтения одной строки наблюдением
	// не считаются: одиночный пограничный снапшот границы суток (полночь
	// состарила вчерашние in-flight эпизоды «новой» проверки — DNS-итог
	// вчера, HTTP в полёте на скане агрегата) следующим проходом
	// перезаписывается нулём и алерта не даёт (ревью T-256, P2-1).
	// Стационарно ноль: норма не будит вовсе.
	AlertNewDoneStalePasses = 2
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

// newDoneStaleSQL — точечное PK-чтение строки new_done_stale (T-256,
// ФТ-2.2): домены «статус повис» (status = new ∧ check_state = done ∧
// last_checked_at старше суток) — значение пишет FILTER-колонка
// f_new_done_stale прохода реконсиляции (zonestats.go), алерт ценой
// одного лукапа наблюдает её между проходами (глубина окна —
// zone_stats_interval), seq scan по domains не добавляет. updated_at —
// метка снапшота: проходы алертов чаще проходов источника (5 мин против
// 30), серия алерта растёт только на НОВОМ снапшоте — два чтения одной
// строки не «два наблюдения» (ревью T-256, P2-1). Пустой результат —
// строки ещё нет: её создаёт первый проход реконсиляции (обе петли
// monitor стартуют вместе; в отличие от render_pending, создающегося
// миграцией, корректного «нет строки» у new_done_stale не бывает — либо
// гонка старта, либо ручная правка таблицы) — тихое «не измеряется»
// без Warn, а не ложный ноль и не тревога.
const newDoneStaleSQL = "SELECT value, updated_at FROM crawler_counters WHERE name = 'new_done_stale'"

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

// FallbackBudgetSource — счётчик событий приостановки фолбэка по бюджету
// доли (crawler_render_fallback_suspended_total{reason="budget"}, T-239);
// реализация RenderSuspends (suspends.go) читает его из экспозиции
// /metrics рендер-воркера, юнит-тесты подменяют фейком. Стиль
// BadHashesSource: источник заменяем, алерт не зависит от HTTP.
type FallbackBudgetSource interface {
	// FallbackBudget возвращает текущее значение счётчика (монотонно
	// растёт защёлкиваниями приостановки; рестарт воркера обнуляет).
	FallbackBudget(ctx context.Context) (int64, error)
}

// RenderBrowserErrorsSource — счётчик ошибок класса браузера этапа
// рендера (crawler_stage_errors_total{stage="render",type="browser"},
// S-P2-6); реализация RenderSuspends (suspends.go) читает его из той же
// экспозиции /metrics рендер-воркера (адрес monitor.render_metrics_url),
// юнит-тесты подменяют фейком. Стиль FallbackBudgetSource: источник
// заменяем, алерт не зависит от HTTP.
type RenderBrowserErrorsSource interface {
	// RenderBrowserErrors возвращает текущее значение счётчика (монотонно
	// растёт инфра-ошибками движка; рестарт воркера обнуляет). Ошибка
	// errNotRenderExposition — «адрес настроен не на рендер-воркера».
	RenderBrowserErrors(ctx context.Context) (int64, error)
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
	// FallbackBudget — счётчик событий приостановки бюджета доли фолбэка
	// рендера (T-246); nil — алерт «не измеряется». Источником командует
	// cmd/monitor (RenderSuspends по monitor.render_metrics_url).
	FallbackBudget FallbackBudgetSource
	// RenderBrowserErrors — счётчик ошибок класса браузера этапа рендера
	// (S-P2-6); nil — алерт «не измеряется». Источник — тот же зонд
	// RenderSuspends по monitor.render_metrics_url (cmd/monitor).
	RenderBrowserErrors RenderBrowserErrorsSource
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
// порядок фиксирован (NewAlerts).
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

// Alerts — оценка алертов с памятью состояний между проходами
// (гистерезис, фронтовые записи лога только на переходах). Каждый алерт
// — машина alertMachine (T-264, ТЗ-fsm §3 ФТ-10): «{имя, статус,
// track-память}» с единственной точкой записи состояния transition —
// спецификация автомата и типизированные track-памяти в alertstate.go,
// этот файл — источники наблюдений (observe*) и сборка машин.
type Alerts struct {
	deps AlertsDeps
	opts AlertsOptions
	log  *slog.Logger
	now  func() time.Time
	// machines — машины алертов в фиксированном порядке (инвариант И4
	// alertstate.go): снапшот AlertsResult детерминирован, очередь
	// чтений Redis источников стабильна.
	machines []alertRunner
}

// NewAlerts строит оценщик: нормализует опции (дефолты периодов),
// источники Space/Wear/PSI (nil → реализации ОС) и собирает машины
// алертов (конструкторы newXMachine — рядом с observe-функциями
// источников; источники и пороги — шапка файла). Порядок машин —
// прежний порядок evaluators (T-080…T-256): от него зависит очередь
// чтений общих источников (фейки тестов позиционируют ошибки по номеру
// чтения Len).
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
	a := &Alerts{deps: deps, opts: o, log: log, now: now}
	a.machines = []alertRunner{
		a.newDNSMachine(),
		a.newQueuesMachine(),
		a.newStallMachine(),
		a.newDiskMachine(),
		a.newDiskMinMachine(),
		a.newDiskCritMachine(),
		a.newWearMachine(),
		a.newBadHashesMachine(),
		a.newUnitOOMMachine(),
		a.newMemHighMachine(),
		a.newDeadMachine(),
		a.newQueueFreezeMachine(),
		a.newRedisMemoryMachine(),
		a.newPSIMachine(),
		a.newRenderDebtMachine(),
		a.newRenderFbDepthMachine(),
		a.newRenderFbBudgetMachine(),
		a.newRenderBrowserErrMachine(),
		a.newDomainsStaleMachine(),
	}
	return a
}

// mutator-disable-regexp a\.log\.Debug\("проход statement/remove
// mutator-disable-regexp a\.log\.Info\("цикл statement/remove
// Обоснование: служебные записи цикла (итог прохода Debug, старт Info) —
// поведение алертов не меняют, гасятся по docs/mutation-testing.md §4.

// Pass выполняет один проход: прогоняет все машины (деградация
// источника одной не останавливает остальные), логирует переходы,
// отдаёт снапшот в OnPass и возвращает его.
func (a *Alerts) Pass(ctx context.Context) AlertsResult {
	snaps := make([]AlertSnapshot, 0, len(a.machines))
	for _, m := range a.machines {
		snaps = append(snaps, m.run(ctx, a))
	}
	res := AlertsResult{Alerts: snaps}
	if a.opts.OnPass != nil {
		a.opts.OnPass(res)
	}
	a.log.Debug("проход алертов", "алертов", len(snaps), "сработало", len(res.Firing()))
	return res
}

// logFront пишет фронтовую запись на смене состояния (инвариант И3
// alertstate.go): срабатывание — Warn, снятие — Info, неприменимость —
// Debug. Исключение — уход сработавшего алерта в «не измеряется» (сбой
// источника): Warn об ошибке пишет transition, потерю самого алерта
// фиксируем Info — иначе авария исчезла бы из лога без единой записи
// о прекращении.
func (a *Alerts) logFront(name, prev, status, detail string) {
	if prev == status {
		return
	}
	desc := name
	if d, ok := alertDescs[name]; ok {
		desc = d
	}
	switch status {
	case AlertStatusFiring:
		a.log.Warn("АЛЕРТ: "+desc, "alert", name, "detail", detail)
	case AlertStatusOK:
		if prev == AlertStatusFiring {
			a.log.Info("алерт снят: "+desc, "alert", name, "detail", detail)
		}
	case AlertStatusNotMeasured:
		if prev == AlertStatusFiring {
			a.log.Info("сработавший алерт больше не измеряется: "+desc, "alert", name, "причина", detail)
		} else {
			a.log.Debug("алерт не измеряется: "+desc, "alert", name, "причина", detail)
		}
	}
}

// observeDNS — доля temp_error среди проверок текущих суток: числитель
// и знаменатель — суточные строки crawler_counters (T-182, точечные
// чтения dnsDailyCountersSQL; события записей результатов — ретраи и
// дубли доставки остаются в истории событий, доля устойчива, абсолюты
// чинит реконсиляция). До 100 событий знаменателя (AlertDNSMinSamples)
// доля — шум пустого окна, тихое «не измеряется»; гистерезис доли —
// track машины (пороги T-080/T-162 не менялись с T-182).
func (a *Alerts) observeDNS(ctx context.Context) observation[dnsShare] {
	rows, err := a.deps.Pool.Query(ctx, dnsDailyCountersSQL)
	if err != nil {
		return observation[dnsShare]{err: fmt.Errorf("monitor: доля ошибок DNS: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		return observation[dnsShare]{err: errors.New("monitor: доля ошибок DNS: пустой ответ")}
	}
	var temp, total int64
	if err := rows.Scan(&temp, &total); err != nil {
		return observation[dnsShare]{err: fmt.Errorf("monitor: доля ошибок DNS: чтение: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return observation[dnsShare]{err: fmt.Errorf("monitor: доля ошибок DNS: %w", err)}
	}
	if total < AlertDNSMinSamples {
		return observation[dnsShare]{
			reason: fmt.Sprintf("проверок за текущие сутки %d < %d — доля не показательна", total, AlertDNSMinSamples),
		}
	}
	return observation[dnsShare]{value: dnsShare{temp: temp, total: total}}
}

// newDNSMachine — машина dns_temp_error_share: гистерезис порогов доли
// (включение ≥ AlertDNSFireShare 30 %, удержание сработавшего до
// AlertDNSRecoverShare 20 % — коридор не дребезжит Warn).
func (a *Alerts) newDNSMachine() *alertMachine[dnsShare] {
	return &alertMachine[dnsShare]{
		name: AlertDNSErrors,
		track: hysteresisTrack[dnsShare]{
			fire: func(v dnsShare) bool { return v.share() >= AlertDNSFireShare },
			hold: func(v dnsShare) bool { return v.share() >= AlertDNSRecoverShare },
			okDetail: func(v dnsShare) string {
				return fmt.Sprintf("доля temp_error %.1f%% (%d из %d проверок за текущие сутки)",
					100*v.share(), v.temp, v.total)
			},
		},
		eval: a.observeDNS,
	}
}

// observeQueues — глубина рабочих стримов (источник наблюдения): Len по
// каждому рабочему стриму alertStreams; гистерезис «порог гейта /
// 90 % порога» — track машины. q:dead алертом очередей не опрашивается
// (глубину карантина читает queue_dead).
func (a *Alerts) observeQueues(ctx context.Context) observation[map[string]int64] {
	if a.deps.Redis == nil {
		return observation[map[string]int64]{reason: "Redis не задан"}
	}
	if a.opts.QueueWatermark <= 0 {
		return observation[map[string]int64]{
			reason: "порог scheduler.queue_high_watermark не задан",
		}
	}
	lens := make(map[string]int64, len(alertStreams))
	for _, s := range alertStreams {
		n, err := a.deps.Redis.Len(ctx, s)
		if err != nil {
			return observation[map[string]int64]{err: fmt.Errorf("monitor: глубина %s: %w", s, err)}
		}
		lens[s] = n
	}
	return observation[map[string]int64]{value: lens}
}

// newQueuesMachine — машина queue_overflow: гистерезис глубин рабочих
// стримов — срабатывание на эффективном пороге стрима (в тот же момент
// гейт закрывает публикацию), снятие ниже 90 % порога
// (AlertQueueRecoverFactor — мягкий гейт: между проверкой и публикацией
// другие продюсеры добавляют записи, T-026). Порог — по стриму, как у
// гейта (T-026): для q:dns настроенные 100 000 записей ограничиваются
// половиной его MAXLEN по политике (DefaultStreamMaxLen, T-141:
// 125 пачек).
func (a *Alerts) newQueuesMachine() *alertMachine[map[string]int64] {
	wm := make(map[string]int64, len(alertStreams))
	for _, s := range alertStreams {
		wm[s] = queue.StreamWatermark(a.opts.QueueWatermark, a.opts.QueueMaxLen, s)
	}
	holdAt := func(s string) int64 {
		return int64(float64(wm[s]) * AlertQueueRecoverFactor)
	}
	return &alertMachine[map[string]int64]{
		name: AlertQueueOverflow,
		track: hysteresisTrack[map[string]int64]{
			fire: func(lens map[string]int64) bool {
				for _, s := range alertStreams {
					if lens[s] >= wm[s] {
						return true
					}
				}
				return false
			},
			hold: func(lens map[string]int64) bool {
				for _, s := range alertStreams {
					if lens[s] >= holdAt(s) {
						return true
					}
				}
				return false
			},
			okDetail: func(lens map[string]int64) string {
				// Деталь «ok» — максимальная глубина среди рабочих
				// стримов; стартовое значение — первый стрим: при всех
				// нулях (простаивающие очереди) деталь называет стрим,
				// а не пустую строку.
				maxStream, maxLen := alertStreams[0], lens[alertStreams[0]]
				// Эквивалентный мутант alertStreams[0:]: первый элемент
				// не превосходит сам себя (старт максимума), итог не
				// меняется.
				// mutator-disable-next-line numbers/decrementer
				for _, s := range alertStreams[1:] {
					if lens[s] > maxLen {
						maxStream, maxLen = s, lens[s]
					}
				}
				return fmt.Sprintf("макс %s=%d", maxStream, maxLen)
			},
			hotDetail: func(lens map[string]int64) string {
				// Сработавшая деталь называет горячие стримы; при их
				// отсутствии — удерживающие коридор гистерезиса.
				var hot, hold []string
				for _, s := range alertStreams {
					n := lens[s]
					switch {
					case n >= wm[s]:
						hot = append(hot, fmt.Sprintf("%s=%d (порог %d)", s, n, wm[s]))
					case n >= holdAt(s):
						hold = append(hold, fmt.Sprintf("%s=%d (порог %d)", s, n, wm[s]))
					}
				}
				at := hot
				if len(at) == 0 {
					at = hold // коридор гистерезиса: ниже порога гейта, но близко
				}
				return strings.Join(at, ", ") + " — глубина у порога"
			},
		},
		eval: a.observeQueues,
	}
}

// observeStall — свежесть последнего пересчёта зон: zones.updated_at
// (пишет ZoneStatsLoop, T-078); NULL — зон нет, прогресс не
// отслеживается (тихое «не измеряется»). Порог возраста — track машины;
// оговорка самоконтроля: updated_at пишет сам monitor — при полной
// смерти процесса алерт некому сработать, пропажу сервиса видит метрика
// crawler_service_up каркаса (T-012).
func (a *Alerts) observeStall(ctx context.Context) observation[time.Time] {
	rows, err := a.deps.Pool.Query(ctx, zoneFreshSQL)
	if err != nil {
		return observation[time.Time]{err: fmt.Errorf("monitor: остановка прогресса: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		return observation[time.Time]{err: errors.New("monitor: остановка прогресса: пустой ответ")}
	}
	var last sql.NullTime
	if err := rows.Scan(&last); err != nil {
		return observation[time.Time]{err: fmt.Errorf("monitor: остановка прогресса: чтение: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return observation[time.Time]{err: fmt.Errorf("monitor: остановка прогресса: %w", err)}
	}
	if !last.Valid {
		return observation[time.Time]{reason: "справочник зон пуст — прогресс не отслеживается"}
	}
	return observation[time.Time]{value: last.Time}
}

// newStallMachine — машина progress_stall: порог возраста метки в
// периодах РЕКОНСИЛЯЦИИ (AlertStallFactor × ZoneStatsInterval — метку
// пишет именно этот цикл, а не проходы алертов: при 30-мин
// реконсиляции порог 5 × 5 мин стрелял бы вечно; проход T-078 по
// десяткам млн строк — минуты, единичный пропуск периода не авария,
// 5 периодов — уверенная остановка). Гистерезис не нужен: возраст
// уменьшается только новым проходом, дребезга у порога нет.
func (a *Alerts) newStallMachine() *alertMachine[time.Time] {
	stallAfter := time.Duration(AlertStallFactor) * a.opts.ZoneStatsInterval
	return &alertMachine[time.Time]{
		name: AlertProgressStall,
		track: thresholdTrack[time.Time]{
			fire: func(now, last time.Time) bool {
				return now.Sub(last) > stallAfter
			},
			okDetail: func(now, last time.Time) string {
				return fmt.Sprintf("последний пересчёт %s назад (порог %s)",
					now.Sub(last).Round(time.Second), stallAfter)
			},
			hotDetail: func(now, last time.Time) string {
				return fmt.Sprintf("zones.updated_at старше порога: %s (порог %d × %s)",
					now.Sub(last).Round(time.Second), AlertStallFactor, a.opts.ZoneStatsInterval)
			},
		},
		eval: a.observeStall,
	}
}

// observeDisk — наблюдение процентного алерта места: каталог контента
// не задан / контроль выключен (content.disk_free_percent ≤ 0) — тихое
// «не измеряется», не ложное срабатывание; далее — общий снимок места
// observeDiskSpace.
func (a *Alerts) observeDisk(_ context.Context) observation[SpaceStat] {
	if a.opts.ContentDir == "" {
		return observation[SpaceStat]{reason: "каталог контента не задан"}
	}
	if a.opts.DiskFreePercent <= 0 {
		return observation[SpaceStat]{
			reason: "контроль места выключен (content.disk_free_percent ≤ 0)",
		}
	}
	return a.observeDiskSpace()
}

// observeDiskSpace — общий снимок statfs каталога контента: доля
// свободного места (та же формула, что пауза Put в internal/content,
// T-046) и абсолютные минимумы читают один и тот же срез (две копии
// сисколла на проход дешевле связывания алертов общим кэшем). Ошибка
// statfs — Warn; total=0 — файловая система не сообщает ёмкость.
func (a *Alerts) observeDiskSpace() observation[SpaceStat] {
	sp, err := a.deps.Space.Space(a.opts.ContentDir)
	if err != nil {
		return observation[SpaceStat]{err: fmt.Errorf("monitor: свободное место %s: %w", a.opts.ContentDir, err)}
	}
	if sp.Total == 0 {
		return observation[SpaceStat]{reason: "файловая система не сообщает ёмкость (total=0)"}
	}
	return observation[SpaceStat]{value: sp}
}

// observeDiskBytes — наблюдение абсолютных минимумов места
// (disk_free_min/disk_free_crit, T-162): каталог не задан — тихое «не
// измеряется»; далее — общий снимок observeDiskSpace.
func (a *Alerts) observeDiskBytes(_ context.Context) observation[SpaceStat] {
	if a.opts.ContentDir == "" {
		return observation[SpaceStat]{reason: "каталог контента не задан"}
	}
	return a.observeDiskSpace()
}

// newDiskMachine — машина disk_free: срабатывание при доле свободного
// строго ниже порога — ровно когда Pause контента останавливает запись;
// отдельный порог рассинхронировал бы сигнал с реальной паузой,
// поэтому без зазора гистерезиса.
func (a *Alerts) newDiskMachine() *alertMachine[SpaceStat] {
	pct := a.opts.DiskFreePercent
	return &alertMachine[SpaceStat]{
		name: AlertDiskFree,
		track: thresholdTrack[SpaceStat]{
			fire: func(_ time.Time, sp SpaceStat) bool {
				return 100*float64(sp.Avail)/float64(sp.Total) < pct
			},
			okDetail: func(_ time.Time, sp SpaceStat) string {
				free := 100 * float64(sp.Avail) / float64(sp.Total)
				return fmt.Sprintf("свободно %.1f%% (%s из %s), порог %.1f%%",
					free, alertBytes(sp.Avail), alertBytes(sp.Total), pct)
			},
		},
		eval: a.observeDisk,
	}
}

// newDiskBytesMachine — машина абсолютного минимума места (T-162):
// Warn при Avail < порога, эскалация — отдельным алертом, а не
// «уровнем» первого: каждое состояние видно своей парой сообщений
// срабатывание/снятие (дебаунс notify.go) и своей строкой в журнале,
// иерархия Warn→усиление читается по имени. Срабатывание строгое (<),
// восстановление возвратом к порогу; гистерезиса нет, как у
// процентного: место при инциденте убывает монотонно (~1,5 ГБ/ч) —
// дребезг у порога нечему двигать. Порог ≥ ёмкости тома неприменим:
// свободное место никогда не достигнет минимума (dev-том 10 ГБ против
// 15 GiB) — вечное срабатывание было бы ложным, малые тома остаются
// под крылом процентного порога.
func (a *Alerts) newDiskBytesMachine(name string, threshold int64) *alertMachine[SpaceStat] {
	return &alertMachine[SpaceStat]{
		name: name,
		track: thresholdTrack[SpaceStat]{
			fire: func(_ time.Time, sp SpaceStat) bool {
				return int64(sp.Avail) < threshold
			},
			na: func(sp SpaceStat) string {
				if threshold >= int64(sp.Total) {
					return fmt.Sprintf("минимум %s ≥ ёмкости тома %s — порог неприменим, малый том под крылом процентного",
						alertBytes(uint64(threshold)), alertBytes(sp.Total))
				}
				return ""
			},
			okDetail: func(_ time.Time, sp SpaceStat) string {
				return fmt.Sprintf("свободно %s из %s, порог %s",
					alertBytes(sp.Avail), alertBytes(sp.Total), alertBytes(uint64(threshold)))
			},
		},
		eval: a.observeDiskBytes,
	}
}

// newDiskMinMachine / newDiskCritMachine — Warn-минимум (15 GiB,
// DefaultDiskFreeMinBytes) и эскалация (10 GiB, DefaultDiskFreeCritBytes)
// абсолютного запаса места: процентный порог на большом томе поздний
// (10 % от 79 ГБ = 8 ГБ ≈ 5 ч до заполнения при темпе ≈36 GiB/сут,
// замер §12), минимум в байтах даёт запас на разбор независимо от
// размера тома.
func (a *Alerts) newDiskMinMachine() *alertMachine[SpaceStat] {
	return a.newDiskBytesMachine(AlertDiskMin, a.opts.DiskFreeMinBytes)
}

func (a *Alerts) newDiskCritMachine() *alertMachine[SpaceStat] {
	return a.newDiskBytesMachine(AlertDiskCrit, a.opts.DiskFreeCritBytes)
}

// observeWear — износ NVMe по health-логу. Недоступность SMART —
// ожидаемое состояние контейнера: тихое «не измеряется» без Warn —
// деградация честная (см. шапку файла); на боевом systemd-хосте слой
// включается доступом юнита к /dev/nvme* (группа nvme-smart, T-089 —
// docs/disks.md). Пустой список — источник жив, устройств нет.
func (a *Alerts) observeWear(ctx context.Context) observation[[]NVMeWear] {
	wears, err := a.deps.Wear.Wear(ctx)
	if err != nil {
		return observation[[]NVMeWear]{reason: err.Error()}
	}
	if len(wears) == 0 {
		return observation[[]NVMeWear]{reason: "NVMe-устройства не найдены"}
	}
	return observation[[]NVMeWear]{value: wears}
}

// newWearMachine — машина nvme_wear: алерт при percentage_used ≥
// AlertNVMeWearPercent (90 % — осталось ≤10 % расчётного ресурса, порог
// планирования замены) любого устройства; ресурс монотонен (не
// восстанавливается) — гистерезис не нужен, значения >100 % валидны
// (ресурс исчерпан) и тоже алертятся.
func (a *Alerts) newWearMachine() *alertMachine[[]NVMeWear] {
	// Худший по износу; старт — первое устройство, а не нулевое значение:
	// новый накопитель (0 %) оставил бы пустое имя в детали.
	worstOf := func(wears []NVMeWear) NVMeWear {
		worst := wears[0]
		// Эквивалентный мутант wears[0:]: первый элемент не превосходит
		// сам себя (старт максимума), итог не меняется.
		// mutator-disable-next-line numbers/decrementer
		for _, w := range wears[1:] {
			if w.PercentUsed > worst.PercentUsed {
				worst = w
			}
		}
		return worst
	}
	return &alertMachine[[]NVMeWear]{
		name: AlertNVMeWear,
		track: thresholdTrack[[]NVMeWear]{
			fire: func(_ time.Time, wears []NVMeWear) bool {
				return worstOf(wears).PercentUsed >= AlertNVMeWearPercent
			},
			okDetail: func(_ time.Time, wears []NVMeWear) string {
				worst := worstOf(wears)
				return fmt.Sprintf("%s: использовано %d%% ресурса, записано %.1f ТБ (TBW)",
					worst.Device, worst.PercentUsed, nvmeTBW(worst))
			},
			hotDetail: func(_ time.Time, wears []NVMeWear) string {
				worst := worstOf(wears)
				var hot []string
				for _, w := range wears {
					if w.PercentUsed >= AlertNVMeWearPercent {
						hot = append(hot, fmt.Sprintf("%s=%d%%", w.Device, w.PercentUsed))
					}
				}
				return fmt.Sprintf("износ %s ≥ порога %d%% (%s: использовано %d%% ресурса, записано %.1f ТБ (TBW))",
					strings.Join(hot, ", "), AlertNVMeWearPercent,
					worst.Device, worst.PercentUsed, nvmeTBW(worst))
			},
		},
		eval: a.observeWear,
	}
}

// nvmeTBW — записано терабайт (единица NVMe = 512 000 байт, десятичные
// ТБ — как в спецификации TBW).
func nvmeTBW(w NVMeWear) float64 {
	return float64(w.DataUnitsWritten) * 512000 / 1e12
}

// observeBadHashes — счётчик мусорных content_hash очистки ingest
// (T-099): источник — экспозиция /metrics сервиса ingest (BadHashes).
// Ошибка источника (ingest недоступен, метрики нет) — «не измеряется»
// с Warn: смерть самого ingest видна по crawler_service_up, а не этим
// алертом; окно всплеска (кольцо дельт, снятие старением — гистерезис
// не нужен, сумма окна убывает только старением) — track машины.
func (a *Alerts) observeBadHashes(ctx context.Context) observation[int64] {
	if a.deps.BadHashes == nil {
		return observation[int64]{
			reason: "источник не задан (monitor.ingest_metrics_url)",
		}
	}
	cur, err := a.deps.BadHashes.BadHashes(ctx)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: мусорные content_hash: %w", err)}
	}
	return observation[int64]{value: cur}
}

// newBadHashesMachine — машина ingest_bad_hashes: кольцо дельт за окно
// AlertBadHashesWindow проходов, порог AlertBadHashesThreshold (в норме
// инвариант писателя результатов держит ноль всегда; порог отсекает
// единичные реликты до фикса ING-P2-4 — обоснование у константы).
func (a *Alerts) newBadHashesMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name: AlertBadHashes,
		track: ringTrack{
			ring:      &deltaRing{},
			threshold: AlertBadHashesThreshold,
			passes:    AlertBadHashesWindow,
			interval:  a.opts.Interval,
			label:     "счётчик",
			fireNote:  " — ошибка писателя результатов (T-056), refcount не менялся",
		},
		eval: a.observeBadHashes,
	}
}

// observeUnitsSource — чтение источника юнитов без трактовки пустого
// списка: деградации общие у машин unit_oom и memory_high — источник
// не задан (тихо), errNoSystemd (dev-контейнер — тихо, как у SMART:
// окружение без systemd — норма dev, а не авария) и прочие ошибки
// (боевой хост, systemd/cgroupfs сломан — Warn каждого прохода, стиль
// остальных источников; без повторного «monitor: юниты:» — источник
// уже ставит префикс).
func (a *Alerts) observeUnitsSource(ctx context.Context) observation[[]UnitEvents] {
	if a.deps.UnitEvents == nil {
		return observation[[]UnitEvents]{reason: "источник не задан"}
	}
	units, err := a.deps.UnitEvents.UnitEvents(ctx)
	if err != nil {
		if errors.Is(err, errNoSystemd) {
			return observation[[]UnitEvents]{reason: err.Error()}
		}
		return observation[[]UnitEvents]{err: err}
	}
	return observation[[]UnitEvents]{value: units}
}

// observeUnitEvents — наблюдение unit_oom: пустой список — юниты не
// загружены, «не измеряется» (события юнитов вне наблюдения).
func (a *Alerts) observeUnitEvents(ctx context.Context) observation[[]UnitEvents] {
	o := a.observeUnitsSource(ctx)
	if o.err != nil || o.reason != "" {
		return o
	}
	if len(o.value) == 0 {
		return observation[[]UnitEvents]{reason: "юниты " + unitEventsPattern + " не найдены"}
	}
	return o
}

// observeMemHighUnits — наблюдение memory_high: пустой список —
// ВАЛИДНОЕ измерение (0 юнитов), а не «не найдены» — асимметрия
// осознанная и прежняя (evalMemHigh в master не имел ветки пустого
// списка): троттлинг — про скорость счётчиков живых юнитов, пустой
// ответ чистит память исчезнувших и даёт ok «0 юнитов» (эпизод без
// горячих юнитов закрывается честно), а unit_oom — про события, их
// отсутствие вне загруженных юнитов не наблюдаемо.
func (a *Alerts) observeMemHighUnits(ctx context.Context) observation[[]UnitEvents] {
	return a.observeUnitsSource(ctx)
}

// newUnitOOMMachine — машина unit_oom: срабатывание, пока у любого
// юнита есть oom/oom_kill (memory.events) или NRestarts > 0. Счётчики
// монотонны внутри жизни cgroup/юнита — гистерезис не нужен (стиль
// износа NVMe): эпизод тянется от первого события до сброса состояния
// юнита — перезапуска (деплой сбрасывает сам: stop → reset-failed →
// start в deploy-install.sh; reset-failed/старт обнуляет NRestarts,
// пересозданная cgroup забирает oom/oom_kill с собой). Один Warn на
// эпизод даёт фронтовая механика transition.
func (a *Alerts) newUnitOOMMachine() *alertMachine[[]UnitEvents] {
	return &alertMachine[[]UnitEvents]{
		name: AlertUnitOOM,
		track: thresholdTrack[[]UnitEvents]{
			fire: func(_ time.Time, units []UnitEvents) bool {
				for _, u := range units {
					if u.hot() {
						return true
					}
				}
				return false
			},
			okDetail: func(_ time.Time, units []UnitEvents) string {
				return fmt.Sprintf("OOM и рестартов нет (%d юнитов)", len(units))
			},
			hotDetail: func(_ time.Time, units []UnitEvents) string {
				var hot []string
				for _, u := range units {
					if !u.hot() {
						continue
					}
					hot = append(hot, fmt.Sprintf("%s: oom=%d oom_kill=%d рестартов=%d",
						u.Unit, u.OOM, u.OOMKill, u.NRestarts))
				}
				return strings.Join(hot, ", ") + " — эпизод до перезапуска юнита (NRestarts — reset-failed, oom/oom_kill — новая cgroup)"
			},
		},
		eval: a.observeUnitEvents,
	}
}

// newMemHighMachine — машина memory_high (T-162): серия роста счётчика
// memory.events high по юнитам — порог не значение, а скорость (счётчик
// накапливается за жизнь cgroup: http-worker с MemoryHigh=850M набирает
// >100 тыс. за сутки). Устойчивость — рост MemHighMinPasses проходов
// подряд (2 × период алертов = 10 мин на дефолтных 5 мин, T-206):
// одиночный всплеск — штатная работа мягкого потолка, устойчивый
// троттлинг — предвестник фаталов http-worker (T-157) и запроса потолка
// выше (docs/deploy.md §2). Снятие — первый проход без роста:
// гистерезис не нужен, событие снятия и есть «рост прекратился»,
// дребезг у нуля серии невозможен (серия целая). Источник и деградации
// — те же, что у unit_oom, КРОМЕ пустого списка: для memory_high это
// валидное измерение «0 юнитов» (observeMemHighUnits), не «не
// найдены» — прежняя асимметрия master.
func (a *Alerts) newMemHighMachine() *alertMachine[[]UnitEvents] {
	return &alertMachine[[]UnitEvents]{
		name:  AlertMemHigh,
		track: &memHighStreakTrack{minPasses: a.opts.MemHighMinPasses, units: make(map[string]memHighTrack)},
		eval:  a.observeMemHighUnits,
	}
}

// observeDead — глубина q:dead (T-154): карантин без потребителей
// (ручной разбор, ТЗ 4.2), Len читает XLEN, покидают записи карантин
// только разбором оператора (XTRIM/runbooks §2) — глубина и есть
// «неразобранный остаток». Источник — тот же Redis, что у алерта
// очередей (StreamLener); сверка темпа смертей — счётчик
// crawler_queue_dead_buried_total (T-154) объясняет, откуда сток.
// Устойчивость (серия проходов, удержание любой ненулевой глубиной,
// снятие пустым карантином) — track машины; отдельный контроль роста
// не нужен: рост — частный случай устойчивой ненулёвости, а
// дифференциация «растёт/стоит» дребезжала бы на подвижном XTRIM
// разбора.
func (a *Alerts) observeDead(ctx context.Context) observation[int64] {
	if a.deps.Redis == nil {
		return observation[int64]{reason: "Redis не задан"}
	}
	n, err := a.deps.Redis.Len(ctx, queue.StreamDead)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: глубина q:dead: %w", err)}
	}
	return observation[int64]{value: n}
}

// newDeadMachine — машина queue_dead: срабатывание на AlertDeadMinPasses
// проходе с ненулевой глубиной («устойчиво ненулевой», а не «любая
// смерть»: карантин не опустошается сам, но окно несопоставимо с
// инцидентным масштабом пост-мортема §11.1 — сигнал опаздывает максимум
// на один проход), удержание любой ненулевой глубиной, снятие пустым
// q:dead.
func (a *Alerts) newDeadMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name:  AlertQueueDead,
		track: &deadStreakTrack{minPasses: AlertDeadMinPasses},
		eval:  a.observeDead,
	}
}

// observeQueueFreeze — группы рабочих стримов (T-162):
// last-delivered-id и глубина групп потребления (источник
// StreamGroupsSource). Согласовано с BackpressureGate.Len о глубине
// (кламп T-168 в источнике): застрявший фантомный лаг инцидента
// 22–23.10 сам по себе не «глубина», но замороженные доставки там были
// — алерт ловит и этот класс простоев. Окно неподвижности — track
// машины.
func (a *Alerts) observeQueueFreeze(ctx context.Context) observation[[]StreamGroupState] {
	if a.deps.StreamGroups == nil {
		return observation[[]StreamGroupState]{reason: "источник не задан"}
	}
	groups, err := a.deps.StreamGroups.StreamGroups(ctx)
	if err != nil {
		return observation[[]StreamGroupState]{err: err}
	}
	return observation[[]StreamGroupState]{value: groups}
}

// newQueueFreezeMachine — машина queue_frozen: окно неподвижности
// last-delivered-id группы при глубине > 0 (QueueFreeze — 15 мин:
// живой потребитель двигает ID каждым XREADGROUP, пачки в секунды —
// ТЗ 3.3; короче — ложные срабатывания на паузах между пачками,
// длиннее — теряется половина шага «перезапусти потребителя и посмотри
// логи»). Снятие — первый проход, где ни одна группа не заморожена
// (глубина ушла в ноль или доставки пошли): гистерезис не нужен, окно
// — естественный фильтр дребезга.
func (a *Alerts) newQueueFreezeMachine() *alertMachine[[]StreamGroupState] {
	return &alertMachine[[]StreamGroupState]{
		name:  AlertQueueFreeze,
		track: &queueFreezeTrack{window: a.opts.QueueFreeze, groups: make(map[string]freezeEntry)},
		eval:  a.observeQueueFreeze,
	}
}

// observeRedisMemory — доля памяти Redis used_memory/maxmemory из INFO
// memory (T-154): при noeviction (политика бюджета ТЗ 3.2) заполнение
// maxmemory — отказ записи XADD: прямого счётчика отказов INFO не
// отдаёт, доля — прокси-метрика с запасом на буферы/фрагментацию
// (инцидент 22.10: OOM-отказы публикаций при 620 из 900 МиБ).
// maxmemory 0 (без потолка) — «не измеряется»: отношение не определено,
// отказов записи по maxmemory не бывает. Гистерезис 80 %/75 % — track
// машины.
func (a *Alerts) observeRedisMemory(ctx context.Context) observation[RedisMemory] {
	if a.deps.RedisMemory == nil {
		return observation[RedisMemory]{reason: "источник не задан"}
	}
	m, err := a.deps.RedisMemory.RedisMemory(ctx)
	if err != nil {
		return observation[RedisMemory]{err: fmt.Errorf("monitor: память Redis: %w", err)}
	}
	if m.MaxBytes <= 0 {
		return observation[RedisMemory]{
			reason: fmt.Sprintf("maxmemory=%d — потолка нет, доля не определена", m.MaxBytes),
		}
	}
	return observation[RedisMemory]{value: m}
}

// newRedisMemoryMachine — машина redis_memory: гистерезис доли
// used/max (включение AlertRedisMemFireRatio 80 % — 20 % запаса на
// межобрезочные остатки TrimLoop, фрагментацию и буферы клиентов;
// удержание сработавшего до AlertRedisMemRecoverRatio 75 % — память
// ползёт и падает резко после XTRIM, доля в коридоре не дребезжала бы
// Warn у порога).
func (a *Alerts) newRedisMemoryMachine() *alertMachine[RedisMemory] {
	share := func(m RedisMemory) float64 { return float64(m.UsedBytes) / float64(m.MaxBytes) }
	detail := func(m RedisMemory) string {
		return fmt.Sprintf("used_memory %s из %s maxmemory (%.1f%%), политика %s",
			alertBytes(uint64(m.UsedBytes)), alertBytes(uint64(m.MaxBytes)), 100*share(m), m.Policy)
	}
	return &alertMachine[RedisMemory]{
		name: AlertRedisMemory,
		track: hysteresisTrack[RedisMemory]{
			fire:     func(m RedisMemory) bool { return share(m) >= AlertRedisMemFireRatio },
			hold:     func(m RedisMemory) bool { return share(m) >= AlertRedisMemRecoverRatio },
			okDetail: func(m RedisMemory) string { return detail(m) },
			hotDetail: func(m RedisMemory) string {
				return detail(m) + " — при noeviction у порога отказы записи (OOM command not allowed)"
			},
		},
		eval: a.observeRedisMemory,
	}
}

// observePSI — давление CPU /proc/pressure/cpu (some avg10/60/300 —
// метрика some: задержка хотя бы одной задачи, не full и не load
// average: утилизация 100 % без очереди — норма CPU-bound конвейера).
// Ядро без PSI — errNoPSI, тихая деградация (как SMART в контейнере);
// окно устойчивости — track машины.
func (a *Alerts) observePSI(ctx context.Context) observation[PSIStat] {
	st, err := a.deps.PSI.PSI(ctx)
	if err != nil {
		if errors.Is(err, errNoPSI) {
			return observation[PSIStat]{reason: err.Error()}
		}
		return observation[PSIStat]{err: fmt.Errorf("monitor: PSI CPU: %w", err)}
	}
	return observation[PSIStat]{value: st}
}

// newPSIMachine — машина psi_cpu: окно удержания avg300 выше порога
// (PSICPUPercent 90 % — рабочий голод планировщика по замеру §12
// 91–95 %, ниже — утилизация без очереди; PSIWindow 30 мин — avg300
// сам сглаживает 5 минут, окно требует устойчивый голод, разовые
// шпильки деплоя/ретро-пачек алертом не считаются; T-158 понизил вес
// monitor'у — собственные проходы голод не приближают).
func (a *Alerts) newPSIMachine() *alertMachine[PSIStat] {
	return &alertMachine[PSIStat]{
		name:  AlertPSICPU,
		track: &psiWindowTrack{percent: a.opts.PSICPUPercent, window: a.opts.PSIWindow},
		eval:  a.observePSI,
	}
}

// observeRenderDebt — снимок рендер-долга (T-175): флаги render_pending
// (точечный Lookup счётчика crawler_counters) + лаг q:render (XLEN той
// же семантики, что queue_overflow) — renderDebtOf: один снимок с
// циклом gauge и строкой отчёта нагрузки (семантика не расходится).
// Источники — те же пул БД и гейт глубин: ошибка любого — «не
// измеряется» с Warn, память трекинга не трогается (восстановление
// доедет по фактическому долгу). Отсутствие строки счётчика (миграция
// не applied / ручная правка таблицы, runbooks §6) — ошибка с Warn,
// а не тихий ноль: gauge долга не должен лгать. Порог/серия/защёлка —
// track машины.
func (a *Alerts) observeRenderDebt(ctx context.Context) observation[RenderDebt] {
	if a.deps.Redis == nil {
		return observation[RenderDebt]{reason: "Redis не задан"}
	}
	d, err := renderDebtOf(ctx, a.deps.Pool, a.deps.Redis)
	if err != nil {
		return observation[RenderDebt]{err: err}
	}
	return observation[RenderDebt]{value: d}
}

// newRenderDebtMachine — машина render_debt: защёлка уровня + серия
// роста (RenderDebtDomains 50 тыс. — здоровый фон тысячи флагов,
// алерт про накопление в масштабе суток дренажа; RenderDebtPasses 2 —
// 10 мин непрерывного роста, разовый скачок разбирает проход
// доработки). Спецификация переходов — alertstate.go (renderDebtTrack).
func (a *Alerts) newRenderDebtMachine() *alertMachine[RenderDebt] {
	return &alertMachine[RenderDebt]{
		name:  AlertRenderDebt,
		track: &renderDebtTrack{domains: a.opts.RenderDebtDomains, passes: a.opts.RenderDebtPasses},
		eval:  a.observeRenderDebt,
	}
}

// observeRenderFallbackDepth — глубина q:render-fallback (T-246):
// backlog группы потребления воркера B через тот же гейт, что у
// алертов очередей. Метрика-зеркало сборки B
// crawler_queue_depth{queue="q:render-fallback"} экспонируется самим
// воркером и при его смерти ряда нет — инцидент «B стоит» виден только
// прямое чтение Redis, поэтому источник здесь Redis, а не зонд метрик.
// Порог — единый расчёт queue.StreamWatermark (для q:render-fallback
// это не порог гейта — стрим воротами не ограничивается, ТЗ рендера
// §4.3 п.3, — а порог наблюдения «глубина приближается к пределу
// MAXLEN стрима»: 15 000, половина — 7 500 на дефолтных watermark
// 100 тыс.); триггеры (серия роста ИЛИ стационарная глубина) и защёлка
// — track машины.
func (a *Alerts) observeRenderFallbackDepth(ctx context.Context) observation[int64] {
	if a.deps.Redis == nil {
		return observation[int64]{reason: "Redis не задан"}
	}
	if a.opts.QueueWatermark <= 0 {
		return observation[int64]{
			reason: "порог scheduler.queue_high_watermark не задан",
		}
	}
	n, err := a.deps.Redis.Len(ctx, queue.StreamRenderFallback)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: глубина %s: %w", queue.StreamRenderFallback, err)}
	}
	return observation[int64]{value: n}
}

// newRenderFbDepthMachine — машина render_fallback_depth: защёлка
// уровня с двумя триггерами — рост AlertRenderFallbackPasses проходов
// подряд (механика render_debt) ИЛИ стационарная глубина над порогом
// столько же проходов (S-P2-1: остановленный B, упор в MAXLEN или
// эпизод, переживший рестарт монитора). Разовый всплеск публикаций —
// штатная работа фолбэка «собрать и доработать» (ФТ-13), оператора не
// будит. Спецификация переходов — alertstate.go (renderFbDepthTrack).
func (a *Alerts) newRenderFbDepthMachine() *alertMachine[int64] {
	wm := queue.StreamWatermark(a.opts.QueueWatermark, a.opts.QueueMaxLen, queue.StreamRenderFallback)
	return &alertMachine[int64]{
		name:  AlertRenderFbDepth,
		track: &renderFbDepthTrack{wm: wm, passes: AlertRenderFallbackPasses},
		eval:  a.observeRenderFallbackDepth,
	}
}

// observeRenderFallbackBudget — счётчик событий приостановки бюджета
// доли фолбэка crawler_render_fallback_suspended_total{reason="budget"}
// (T-239, зонд /metrics рендер-воркера): событие, а не состояние — одно
// защёлкивание приостановки одна точка счётчика, пока приостановка
// держится, кандидаты фолбэка финализируются штатно (задачи не теряются,
// ФТ-9). Пустой monitor.render_metrics_url — тихое «не измеряется»
// (источник не подключён, стиль ingest-зонда); ошибка зонда (воркер
// остановлен, метрики нет) — «не измеряется» с Warn: смерть самого
// воркера видна по crawler_service_up. Окно событий — track машины.
func (a *Alerts) observeRenderFallbackBudget(ctx context.Context) observation[int64] {
	if a.deps.FallbackBudget == nil {
		return observation[int64]{
			reason: "источник не задан (monitor.render_metrics_url)",
		}
	}
	cur, err := a.deps.FallbackBudget.FallbackBudget(ctx)
	if err != nil {
		return observation[int64]{err: fmt.Errorf("monitor: бюджет доли фолбэка: %w", err)}
	}
	return observation[int64]{value: cur}
}

// newRenderFbBudgetMachine — машина render_fallback_budget: кольцо
// дельт событий за окно AlertFallbackBudgetWindow проходов (30 мин —
// окно вмещает эпизод бюджетного окна T-239 и один рецидив), порог 1 —
// каждое событие аномалия (в норме публикации укладываются в потолок
// доли); снятие старением: пока приостановка держится, новых точек нет
// — держит только окно, оператору виден fallback-рейт панели движков.
func (a *Alerts) newRenderFbBudgetMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name: AlertRenderFbBudget,
		track: ringTrack{
			// Окно 6 проходов (не 15 окна мусорных хешей): ёмкость кольца
			// задаётся здесь, push дефолта не знает.
			ring:      &deltaRing{size: AlertFallbackBudgetWindow},
			threshold: 1,
			passes:    AlertFallbackBudgetWindow,
			interval:  a.opts.Interval,
			label:     "события бюджета",
			fireNote:  " — доля фолбэка ушла за потолок render.fallback.share_percent: деградация основного движка (fallback-рейт — панель движков дашборда, причины — crawler_render_task_finalized_total)",
		},
		eval: a.observeRenderFallbackBudget,
	}
}

// observeRenderBrowserErrors — счётчик ошибок класса браузера этапа
// рендера crawler_stage_errors_total{stage="render",type="browser"}
// (S-P2-6) из зонда /metrics рендер-воркера (тот же адрес, что у зонда
// бюджета). Семейство stage_errors ленивое: «серии нет» у рендер-воркера
// — 0, не «не измеряется» (маркер crawler_stage_tasks_total{stage=
// "render"}; как suspended после предсоздания T-250). Чужая экспозиция
// (маркера нет) — тихое «не измеряется» без Warn: адрес
// monitor.render_metrics_url настроен не на рендер-воркера —
// конфигурационная проблема, зонд жив; сетевая ошибка зонда — «не
// измеряется» с Warn, как у остальных зондов. Серия роста — track
// машины.
func (a *Alerts) observeRenderBrowserErrors(ctx context.Context) observation[int64] {
	if a.deps.RenderBrowserErrors == nil {
		return observation[int64]{
			reason: "источник не задан (monitor.render_metrics_url)",
		}
	}
	cur, err := a.deps.RenderBrowserErrors.RenderBrowserErrors(ctx)
	if err != nil {
		if errors.Is(err, errNotRenderExposition) {
			return observation[int64]{
				reason: "экспозиция не рендер-воркера (нет crawler_stage_tasks_total{stage=\"render\"}) — проверьте monitor.render_metrics_url",
			}
		}
		return observation[int64]{err: fmt.Errorf("monitor: browser-ошибки рендера: %w", err)}
	}
	return observation[int64]{value: cur}
}

// newRenderBrowserErrMachine — машина render_browser_errors: серия
// роста счётчика AlertRenderBrowserErrorsPasses проходов (10 мин):
// разовая browser-ошибка — штатный перезапуск сессии/контекста (T-063)
// и эпизод, разбираемый health-гейтом, серию не набирает; серия роста
// — движок-кандидат (moli/lightpanda) завис или недоступен: гейт T-239
// уже приостановил фолбэк-публикации, браузер-класс ждёт в redelivery
// q:render — оператору нужен сигнал про сам движок. Событие гейта
// остаётся счётчиком suspended{reason="gate"} и Warn-логом воркера:
// детектор не дублирует защёлкивание, а видит первопричину.
func (a *Alerts) newRenderBrowserErrMachine() *alertMachine[int64] {
	return &alertMachine[int64]{
		name:  AlertRenderBrowserErr,
		track: &counterGrowthTrack{minPasses: AlertRenderBrowserErrorsPasses},
		eval:  a.observeRenderBrowserErrors,
	}
}

// observeDomainsNewDoneStale — строка crawler_counters new_done_stale
// (T-256, ФТ-2.2): домены «статус повис» (status = new ∧ check_state =
// done ∧ last_checked_at старше суток; DNS-этап записал ветку «есть
// адреса» — О6 общей таблицы статусов internal/model/enums.go, HTTP-итог
// О7–О9 не пришёл: задача q:http потеряна, компенсатора нет). Значение
// пишет FILTER-колонка f_new_done_stale прохода реконсиляции
// (zonestats.go), алерт читает его точечным лукапом — seq scan по
// domains не добавляет; updated_at — метка снапшота (серия растёт
// только на НОВОМ снапшоте — track машины). Пустой результат (строки
// нет до первого прохода реконсиляции — обе петли monitor стартуют
// вместе, корректного «нет строки» не бывает) — тихое «не измеряется»;
// ошибка чтения — Warn. Автолечения нет — лечит регулярная перепроверка
// T-030: алерт только сигнал, уровень наблюдаем gauge'ом
// crawler_domains_new_done_stale.
func (a *Alerts) observeDomainsNewDoneStale(ctx context.Context) observation[staleRow] {
	rows, err := a.deps.Pool.Query(ctx, newDoneStaleSQL)
	if err != nil {
		return observation[staleRow]{err: fmt.Errorf("monitor: домены «статус повис»: %w", err)}
	}
	defer rows.Close()
	if !rows.Next() {
		// Обрыв соединения — не «строки нет»: сначала сбой чтения
		// (стиль retryMatureCount).
		if err := rows.Err(); err != nil {
			return observation[staleRow]{err: fmt.Errorf("monitor: домены «статус повис»: %w", err)}
		}
		return observation[staleRow]{
			reason: "строки new_done_stale нет — первый проход реконсиляции ещё не завершился",
		}
	}
	var n int64
	var snap sql.NullTime
	if err := rows.Scan(&n, &snap); err != nil {
		return observation[staleRow]{err: fmt.Errorf("monitor: домены «статус повис»: чтение: %w", err)}
	}
	if err := rows.Err(); err != nil {
		return observation[staleRow]{err: fmt.Errorf("monitor: домены «статус повис»: %w", err)}
	}
	return observation[staleRow]{value: staleRow{value: n, snap: snap}}
}

// newDomainsStaleMachine — машина domains_new_done_stale: серия НОВЫХ
// ненулевых снапшотов реконсиляции (AlertNewDoneStalePasses — 2 × период
// реконсиляции ≈ 1 ч устойчивого ненулевого значения; стационарно ноль
// в норме — норма не будит вовсе).
func (a *Alerts) newDomainsStaleMachine() *alertMachine[staleRow] {
	return &alertMachine[staleRow]{
		name:  AlertDomainsStale,
		track: &newDoneStaleTrack{minPasses: AlertNewDoneStalePasses},
		eval:  a.observeDomainsNewDoneStale,
	}
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
		"interval", a.opts.Interval.String(), "алертов", len(a.machines))

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
