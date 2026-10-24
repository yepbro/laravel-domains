// Package scheduler — планировщик проверок доменов (ТЗ 5.3). Файл
// recheck — суточный перевод готовых к перепроверке доменов обратно в
// очередь (T-030; выборку pending и lease — T-027/T-028/T-029).
//
// Семантика (ТЗ 5.3: «переводит done → pending»). done/pending —
// состояния проверки, колонка domains.check_state (0 pending, 1 queued,
// 2 done; ТЗ 6.1), а НЕ статус результата domains.status (ТЗ 6.2).
// Статус результата (alive, no_http, …) не меняется: он хранит итог
// последней проверки и перезапишется, когда новая завершится; сам факт
// готовности выражается состоянием — status=alive + check_state=pending
// означает «проверен ранее, ждёт перепроверки». Обратно в цикл домен
// возвращает только check_state=pending: его выбирает диспетчер T-027
// (частичный индекс domains_pending_idx). queued_at сбрасывается в NULL
// — как у воскрешённого домена в T-018: у pending нет lease (lease
// заведёт постановка в очередь, T-028).
//
// Условия готовности (ТЗ 5.3, 3.3, 11.4 п. 8): check_state = done И
// last_checked_at старше scheduler.recheck_interval (по умолчанию
// 4380 ч ≈ 6 месяцев; повторная проверка — не чаще этого интервала).
// NULL last_checked_at сравнением отсеивается (никогда не проверялся —
// возраста нет). Статус deleted исключается (ТЗ 13.1): удалённые из
// реестра домены повторной проверке не подлежат; temp_error остаётся
// кандидатом — его частые повторы ведёт T-031 (temp_error_retry_after),
// а возрастная перепроверка через интервал возвращает даже доменам,
// застрявшим в temp_error, шанс на итоговый статус.
//
// Проход — один оператор UPDATE … FROM (SELECT … LIMIT): пачка
// ограничена (стиль T-020: короткие блокировки, автокоммит одного
// оператора — не транзакция на миллионы строк). Переведённые строки
// перестают удовлетворять подзапросу, поэтому повторные проходы и
// рестарты идемпотентны и не делают лишней работы (ТЗ 3.4). Внешний
// WHERE перепроверяет условия отбора на актуальной версии строки —
// гонка с delete-файлом (T-019) или вторым проходом не переведёт
// строку, изменившую состояние между выборкой и блокировкой (как
// охрана статуса в cleanupDeleteSQL, T-020). Подзапрос идёт по
// частичному индексу domains_recheck_idx с самых старых done-строк
// (миграция 000008, T-104): пачки дренажа линейны, а проход не хранит
// позиции — внешнего состояния нет, сбой и рестарт ничего не теряют.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
)

const (
	// DefaultRecheckInterval — возраст последней проверки, после которого
	// домен готов к перепроверке: 4380 часов ≈ 6 месяцев (ТЗ 3.3). Зеркалит
	// дефолт scheduler.recheck_interval (internal/config); рабочий цикл
	// получает значение из конфига (T-012/T-013: конфиг — источник).
	DefaultRecheckInterval = 4380 * time.Hour

	// DefaultRecheckBatch — доменов в одной пачке перевода: в диапазоне
	// пачек планировщика (ТЗ 5.3: 1000–10 000), совпадает с дефолтом
	// scheduler.batch_size. Пачка ограничивает один UPDATE — блокировки
	// удерживаются минимум времени (стиль T-020).
	DefaultRecheckBatch = 5000

	// DefaultRecheckSchedule — период цикла RecheckDaily: раз в сутки
	// (ТЗ 5.3). Отдельной опции конфига нет: ТЗ 5.10 расписание не
	// параметризует, суточность фиксирована.
	DefaultRecheckSchedule = 24 * time.Hour
)

// recheckUpdateSQL — один проход: пачка созревших доменов (check_state
// = done, last_checked_at старше порога, не deleted) переводится в
// pending без смены статуса результата. invalid (диагноз ядовитой
// строки, T-109) в отборе исключён: единственный штатный путь возврата
// строки в конвейер после терминального диагноза — этот проход, без
// фильтра ядовитые строки циклились бы вечно (DNS-03). dispatch/lease
// фильтр не расширяют: lease видит только queued (запись диагноза
// ставит done), а invalid+pending (окно гонки recheck↔записи диагноза)
// без фильтра dispatch переиздаётся один раз и диагноз фиксируется
// заново — самозаживление; с фильтром dispatch такая строка зависла бы
// в pending навсегда, вне всех фоновых проходов.
// LIMIT в подзапросе — пачка.
// Сравнение date < timestamptz — кросс-типовой оператор btree-opfamily
// datetime: индекс по last_checked_at применим и к параметру-порогу
// timestamptz, гранулярность колонки — день (ТЗ 6.1), «старше интервала»
// выполняется с точностью до дня. Внешний WHERE перепроверяет ВСЕ условия
// отбора на актуальной версии строки — включая возраст last_checked_at
// (симметрия с lease.go: гонка со свежей записью результата между
// выборкой и блокировкой не переведёт заново проверенный домен).
//
// Коды check_state/status вшиты в текст литералами через model.* — коды
// схемы, а не данные (стиль dispatch.go): параметр в предикате частичного
// индекса планировщик не доказывает, generic-план подготовленного
// выражения (pgx кеширует их) терял бы пригодность domains_recheck_idx
// (миграция 000008, T-104); дополнительный предикат по status сужает
// отбор, не расширяя требование к частичному индексу — пригодность
// сохраняется (status в индекс не входит, HOT-апдейты не затронуты).
//
// ORDER BY (last_checked_at, zone_id, id) совпадает с ключом
// domains_recheck_idx: скан читает с самых старых done-строк ровно
// созревший префикс и останавливается по LIMIT. Пачки дренажа
// детерминированы (уникальный доменный ключ в хвосте сортировки),
// перевод идёт от самых давно проверенных; переведённые строки покидают
// индекс, следующая пачка продолжается с места остановки — дренаж линеен
// и не перечитывает несозревшее множество (внешнего состояния нет,
// сбой и рестарт ничего не теряют, ТЗ 3.4).
//
// Дельты счётчиков зон (checked −n / pending +n) — CTE zc того же
// оператора по RETURNING zone_id переведённых строк (T-178, ТЗИ §6.2:
// предикат пинирует старое состояние done); внешний SELECT отдаёт
// zone_id переведённых — RecheckOnce считает по нему итог.
var recheckUpdateSQL = fmt.Sprintf(
	"WITH b AS (SELECT zone_id, id FROM domains "+
		"WHERE check_state = %[1]d AND status <> %[2]d AND status <> %[3]d AND last_checked_at < $2 "+
		"ORDER BY last_checked_at, zone_id, id LIMIT $3), "+
		"upd AS (UPDATE domains AS d "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM b WHERE d.zone_id = b.zone_id AND d.id = b.id "+
		"AND d.check_state = %[1]d AND d.status <> %[2]d AND d.status <> %[3]d AND d.last_checked_at < $2 "+
		"RETURNING d.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_checked = z.domains_checked - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateDone), int16(model.StatusDeleted), int16(model.StatusInvalid))

// RecheckOptions — параметры перепроверки; нулевые значения заменяются
// дефолтами (withDefaults, стиль ingest.CleanupOptions).
type RecheckOptions struct {
	// Interval — порог возраста последней проверки (DefaultRecheckInterval);
	// рабочее значение — scheduler.recheck_interval из конфига.
	Interval time.Duration
	// BatchSize — максимум доменов в одной пачке (DefaultRecheckBatch).
	BatchSize int
	// Schedule — период цикла RecheckDaily (DefaultRecheckSchedule);
	// на разовый RecheckOnce не влияет.
	Schedule time.Duration
	// Now — источник времени для порога now() − Interval; nil — time.Now.
	// Тесты подставляют фиксированные часы (стиль watch, T-013).
	Now func() time.Time
	// Log — логгер цикла RecheckDaily; nil — записи подавлены
	// (slog.DiscardHandler). RecheckOnce не логирует — итог возвращается
	// RecheckResult.
	Log *slog.Logger
}

// withDefaults заменяет неположительные значения дефолтами (Now
// проверяется отдельно: функций — nil).
func (o RecheckOptions) withDefaults() RecheckOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRecheckInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRecheckBatch
	}
	if o.Schedule <= 0 {
		o.Schedule = DefaultRecheckSchedule
	}
	return o
}

// RecheckResult — итог одного прохода RecheckOnce.
type RecheckResult struct {
	// Domains — домены, переведённые в pending (не больше размера пачки).
	Domains int64
}

// RecheckOnce выполняет один проход (ТЗ 5.3): до opts.BatchSize доменов
// со check_state = done и last_checked_at старше opts.Interval (не
// deleted) переводятся в check_state = pending одним оператором —
// статус результата и остальные колонки не меняются, queued_at
// сбрасывается; CTE того же оператора ведёт дельты счётчиков зон
// (T-178). Повторный проход безопасен (ТЗ 3.4): переведённые
// строки подзапрос больше не выбирает.
func RecheckOnce(ctx context.Context, pool db.DB, opts RecheckOptions) (RecheckResult, error) {
	if pool == nil {
		return RecheckResult{}, errors.New("scheduler: перепроверка доменов: пул не задан")
	}
	o := opts.withDefaults()
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	cutoff := now().Add(-o.Interval)

	rows, err := pool.Query(ctx, recheckUpdateSQL,
		int16(model.CheckStatePending),
		cutoff,
		o.BatchSize,
	)
	if err != nil {
		return RecheckResult{}, fmt.Errorf("scheduler: перепроверка доменов: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return RecheckResult{}, fmt.Errorf("scheduler: перепроверка доменов: чтение: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return RecheckResult{}, fmt.Errorf("scheduler: перепроверка доменов: %w", err)
	}
	return RecheckResult{Domains: n}, nil
}

// RecheckDaily — суточный цикл перепроверки (ТЗ 5.3: «раз в сутки»):
// дренаж всех созревших доменов пачками RecheckOnce, затем ожидание до
// следующего суток-тика. Первый дренаж — сразу при старте: проход
// идемпотентен и подбирает созревшее за время простоя сервиса (стиль
// ingest.CleanupDeleted, T-020), а не ждёт тика — момент старта
// ничем не выделен. Ошибка пачки прерывает только текущий дренаж:
// логируется Warn, цикл живёт (контракт OnRun, T-012), очередной
// дренаж продолжит с того же места — переведённое не переводится
// дважды. Отмена ctx — чистый выход с nil (graceful shutdown, ТЗ 3.4).
func RecheckDaily(ctx context.Context, pool db.DB, opts RecheckOptions) error {
	if pool == nil {
		return errors.New("scheduler: перепроверка доменов: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("суточная перепроверка доменов",
		"schedule", o.Schedule.String(),
		"interval", o.Interval.String(),
		"batch", o.BatchSize)

	// drain переводит все созревшие домены: пачки повторяются, пока
	// проходят целиком; неполная пачка — множество исчерпано.
	drain := func() {
		var total int64
		for {
			res, err := RecheckOnce(ctx, pool, o)
			if err != nil {
				// Отмена во время прохода — часть остановки, не тревога;
				// прочая ошибка — Warn, очередной дренаж повторит.
				if ctx.Err() == nil {
					log.Warn("суточный дренаж перепроверки доменов",
						"err", err, "translated", total)
				}
				return
			}
			total += res.Domains
			if res.Domains < int64(o.BatchSize) {
				if total > 0 {
					log.Info("суточный дренаж перепроверки доменов", "domains", total)
				}
				return
			}
		}
	}

	drain()
	ticker := time.NewTicker(o.Schedule)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			drain()
		}
	}
}
