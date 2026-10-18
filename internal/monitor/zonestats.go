// Package monitor — служебный сервис мониторинга (ТЗ 5.9): периодический
// пакетный пересчёт кэширующих счётчиков прогресса зон zones.domains_total
// / zones.domains_checked (T-078) и оценка алертов минимального набора
// (T-080, alerts.go). Счётчики читают crawlerctl status
// (T-079) и партиционирование (docs/schema.md: текущий размер зоны), а
// пишутся только здесь — пересчёт «на лету» по каждой зоне был бы
// последовательностью count(*) на пользовательский запрос.
//
// Семантика счётчиков (ТЗ 5.9 «прогресс по зонам», 11.4 п. 6 «всего /
// проверено»):
//
//   - domains_total — общее число живых доменов зоны: статус результата
//     <> deleted (ТЗ 13.1: удалённые из реестра не проверяются — включая
//     их в знаменатель, прогресс никогда не дошёл бы до 100 %). «Живых»
//     здесь — присутствующих в реестре, а не status = alive: alive
//     узнаётся только в результате проверки и не может быть знаменателем
//     прогресса;
//   - domains_checked — проверенные в текущем цикле: check_state = done
//     (ТЗ 6.1). Возрастная перепроверка (ТЗ 5.3) переводит done → pending,
//     поэтому счётчик естественно отслеживает прогресс нового цикла, а не
//     застывает на 100 % после первого; last_checked_at IS NOT NULL дал бы
//     «когда-либо проверенные» — монотонную величину без спада на новом
//     цикле. Оба счётчика считаются по одному базовому множеству
//     (status <> deleted), иначе checked мог бы превысить total;
//   - domains_laravel (T-120) — Laravel-домены зоны: строки laravel_domains
//     уже прошли порог analyzer (§13.1), count(*) без порога; кэш читает
//     лендинг этапа 13 (карточки, облако/таблица зон) и crawlerctl.
//
// Проход — один оператор UPDATE … FROM (агрегат domains GROUP BY zone_id +
// агрегат laravel_domains GROUP BY zone_id, T-120):
// пересчёт всех зон пакетно, без row-per-zone (ТЗ 5.9) и без счётчика на
// каждую строку domains. GROUP BY по ключу партиционирования позволяет
// PostgreSQL агрегировать по партициям (partitionwise aggregate);
// последовательный проход по domains — заявленная ТЗ цена кэширующих
// счётчиков раз в 30–60 с. LEFT JOIN от zones: зона без живых доменов
// (например, последняя удалена) отсутствует в агрегате и получает нули —
// INNER JOIN оставил бы её счётчики застывшими. Снимок одного оператора
// даёт согласованные total/checked на один момент.
//
// TODO(T-077): метрики прогресса (доля проверенного по зонам, длительность
// прохода) — после пакета internal/metrics; каркас уже отдаёт сервису
// реестр Prometheus через /metrics.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
)

// DefaultZoneStatsInterval — период цикла пересчёта: 60 с, верхняя граница
// коридора ТЗ 5.9 «раз в 30–60 секунд» (меньше нагрузка на БД при тех же
// гарантиях свежести). Зеркалит дефолт monitor.zone_stats_interval
// (internal/config, T-006); рабочее значение цикл получает из конфига —
// стиль DefaultRecheckInterval (internal/scheduler).
const DefaultZoneStatsInterval = 60 * time.Second

// zoneStatsSQL — один проход: обе колонки зон пересчитываются агрегатом
// по domains, Laravel-счётчик — агрегатом по laravel_domains (T-120).
// FILTER-вариант count — один проход по строкам считает оба счётчика
// (вместо двух подзапросов/сканов). updated_at — момент пересчёта
// (для наблюдения за живостью monitor, ТЗ 5.9 «остановка прогресса»):
// таблица зон — маленький справочник, версии строк раз в период не
// создают заметного bloat.
//
// domains_laravel (T-120): строки laravel_domains уже прошли порог
// analyzer (§13.1 — таблицу пишут только уверенные срабатывания),
// отдельный порог не нужен; LEFT JOIN обнуляет зоны без находок.
const zoneStatsSQL = "UPDATE zones AS z " +
	"SET domains_total = s.total, " +
	"domains_checked = s.checked, " +
	"domains_laravel = s.laravel, " +
	"updated_at = $3 " +
	"FROM (SELECT z2.id AS zone_id, " +
	"COALESCE(c.total, 0) AS total, " +
	"COALESCE(c.checked, 0) AS checked, " +
	"COALESCE(l.laravel, 0) AS laravel " +
	"FROM zones AS z2 " +
	"LEFT JOIN (" +
	"SELECT zone_id, count(*) AS total, " +
	"count(*) FILTER (WHERE check_state = $1) AS checked " +
	"FROM domains WHERE status <> $2 GROUP BY zone_id" +
	") AS c ON c.zone_id = z2.id " +
	"LEFT JOIN (" +
	"SELECT zone_id, count(*) AS laravel " +
	"FROM laravel_domains GROUP BY zone_id" +
	") AS l ON l.zone_id = z2.id) AS s " +
	"WHERE z.id = s.zone_id"

// ZoneStatsOptions — параметры пересчёта; нулевые значения заменяются
// дефолтами (withDefaults, стиль scheduler.RecheckOptions).
type ZoneStatsOptions struct {
	// Interval — период цикла ZoneStatsLoop (DefaultZoneStatsInterval);
	// рабочее значение — monitor.zone_stats_interval из конфига. На разовый
	// ZoneStatsOnce не влияет.
	Interval time.Duration
	// Now — источник времени для zones.updated_at; nil — time.Now.
	// Тесты подставляют фиксированные часы (стиль scheduler, T-030).
	Now func() time.Time
	// Log — логгер цикла ZoneStatsLoop; nil — записи подавлены
	// (slog.DiscardHandler). ZoneStatsOnce не логирует — итог возвращается
	// ZoneStatsResult.
	Log *slog.Logger
}

// withDefaults заменяет неположительные значения дефолтами (Now
// проверяется отдельно: функций — nil).
func (o ZoneStatsOptions) withDefaults() ZoneStatsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultZoneStatsInterval
	}
	return o
}

// ZoneStatsResult — итог одного прохода ZoneStatsOnce.
type ZoneStatsResult struct {
	// Zones — зоны, чьи счётчики пересчитаны (RowsAffected: каждая строка
	// зон обновляется один раз — s.zone_id уникален по PK zones.id).
	Zones int64
}

// ZoneStatsOnce выполняет один проход (ТЗ 5.9): одним оператором
// пересчитывает domains_total/domains_checked всех зон по текущему
// состоянию domains. Проход идемпотентен (ТЗ 3.4): повтор даёт те же
// значения; сбой не оставляет половинчатых состояний — один оператор,
// один автокоммит.
func ZoneStatsOnce(ctx context.Context, pool db.DB, opts ZoneStatsOptions) (ZoneStatsResult, error) {
	if pool == nil {
		return ZoneStatsResult{}, errors.New("monitor: пересчёт счётчиков зон: пул не задан")
	}
	o := opts.withDefaults()
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}

	tag, err := pool.Exec(ctx, zoneStatsSQL,
		int16(model.CheckStateDone),
		int16(model.StatusDeleted),
		now(),
	)
	if err != nil {
		return ZoneStatsResult{}, fmt.Errorf("monitor: пересчёт счётчиков зон: %w", err)
	}
	return ZoneStatsResult{Zones: tag.RowsAffected()}, nil
}

// ZoneStatsLoop — периодический цикл пересчёта (ТЗ 5.9: раз в 30–60 с):
// ZoneStatsOnce по ticker, первый проход — сразу при старте (свежие
// счётчики после простоя/рестарта, стиль scheduler.RecheckDaily, T-030),
// а не ждёт тика. Ошибка прохода прерывает только его: логируется Warn,
// цикл живёт (контракт OnRun, T-012) — очередной проход повторит по
// актуальному состоянию. Отмена ctx — чистый выход с nil (graceful
// shutdown, ТЗ 3.4). Итог прохода — Debug, а не Info: проход успешен
// всегда, когда жива БД, Info каждые 30–60 с был бы шумом.
func ZoneStatsLoop(ctx context.Context, pool db.DB, opts ZoneStatsOptions) error {
	if pool == nil {
		return errors.New("monitor: пересчёт счётчиков зон: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл пересчёта счётчиков зон",
		"interval", o.Interval.String())

	pass := func() {
		res, err := ZoneStatsOnce(ctx, pool, o)
		switch {
		case err != nil && ctx.Err() != nil:
			// Отмена во время прохода — часть остановки, не тревога.
		case err != nil:
			log.Warn("проход пересчёта счётчиков зон", "err", err)
		default:
			log.Debug("проход пересчёта счётчиков зон", "zones", res.Zones)
		}
	}

	pass()
	ticker := time.NewTicker(o.Interval)
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
