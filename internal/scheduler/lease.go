// Scheduler: возврат доменов с истёкшим lease (T-029; ТЗ 5.3, 3.4).
//
// Постановка в очередь (T-028) переводит домен в check_state = queued и
// отмечает queued_at — с этого момента действует аренда (lease): воркер
// обязан записать результат за срок scheduler.lease (по умолчанию
// 30 минут, ТЗ 5.3). Если результата нет — воркер погиб, потеря Redis
// (источник истины — PostgreSQL, ТЗ 3.4), сбой публикации — домен
// «зависает» в queued и вне цикла проверок. Проход LeaseOnce возвращает
// такие домены в pending: их перевыберет диспетчер (T-027) и поставит в
// очередь заново (T-028) — доставка at-least-once, повтор безопасен.
//
// «Без результата» встроено в условие отбора: воркер, записавший результат,
// переводит домен в done (контракты записи результатов, ТЗ 5.4/5.5) — строка
// перестаёт удовлетворять check_state = queued и не выбирается, сколько бы
// ни было ей лет. Честному воркеру, не успевшему за срок аренды, возврат не
// мешает: таймауты этапов — секунды против минут lease, а повторная
// доставка задачи идемпотентна (ТЗ 3.4).
//
// Условия возврата: check_state = queued И queued_at старше now() − lease.
// NULL queued_at сравнением отсеивается: queued без отметки времени —
// нарушенный инвариант T-028, переводить его по возрасту бессмысленно.
// Статус deleted исключается (ТЗ 13.1, как в перепроверке T-030):
// удалённый домен выпадает из цикла проверок целиком — диспетчер его не
// выбирает (охрана T-027), возвращать его в pending значило бы накапливать
// мёртвые строки в частичном индексе domains_pending_idx. Воскрешение
// add-файлом (T-018) само ставит new/pending и сбрасывает queued_at,
// из какого бы состояния домен ни возвращался.
//
// Проход — один оператор UPDATE … FROM (SELECT … LIMIT), стиль
// перепроверки (T-030) и фоновой очистки (T-020): пачка ограничена,
// автокоммит одного оператора — короткие блокировки, не транзакция на
// миллионы строк. Внешний WHERE перепроверяет отбор на актуальной версии
// строки — гонка с воркером, записавшим результат между выборкой и
// блокировкой, не возвращает уже готовый домен. Переведённые строки
// перестают удовлетворять подзапросу: повторные проходы идемпотентны
// (ТЗ 3.4). Фильтр deleted обязан жить в самом подзапросе (а не только во
// внешней охране): иначе мёртвые строки могли бы заполнять лимит пачки и
// закрывать живым возврат (голодание).
//
// Wiring в cmd/scheduler — сборка сервиса (T-028/поздние задачи).

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
	// DefaultLeaseTimeout — срок аренды queued-домена: старше — результат
	// не дождаться, домен возвращается в pending (ТЗ 5.3: «по умолчанию
	// 30 мин»). Зеркалит дефолт scheduler.lease (internal/config, T-006);
	// рабочий цикл получает значение из конфига.
	DefaultLeaseTimeout = 30 * time.Minute

	// DefaultLeaseBatch — доменов в одной пачке возврата: в диапазоне пачек
	// планировщика (ТЗ 5.3: 1000–10 000), совпадает с дефолтами выборки
	// (T-027) и перепроверки (T-030). Пачка ограничивает один UPDATE —
	// блокировки удерживаются минимум времени (стиль T-020).
	DefaultLeaseBatch = 5000
)

// leaseUpdateSQL — один проход: пачка доменов с истёкшей арендой
// (check_state = queued, queued_at старше порога, не deleted) переводится
// в pending, отметка аренды сбрасывается — у pending нет lease (инвариант
// T-018/T-030). LIMIT в подзапросе — пачка; ORDER BY по ключу домена
// (совпадает с PK, ТЗ 6.1) даёт детерминированные пачки. Внешний WHERE
// перепроверяет отбор на момент блокировки строки (гонки с воркерами и
// вторым проходом).
const leaseUpdateSQL = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM (SELECT zone_id, id FROM domains " +
	"WHERE check_state = $2 AND status <> $3 AND queued_at < $4 " +
	"ORDER BY zone_id, id LIMIT $5) AS b " +
	"WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = $2 AND d.status <> $3 AND d.queued_at < $4"

// LeaseOptions — параметры возврата; нулевые значения заменяются
// дефолтами (withDefaults, стиль RecheckOptions/ingest.CleanupOptions).
type LeaseOptions struct {
	// Timeout — срок аренды (DefaultLeaseTimeout, ТЗ 5.3): queued_at
	// старше now() − Timeout → возврат в pending; рабочее значение —
	// scheduler.lease из конфига.
	Timeout time.Duration
	// BatchSize — максимум доменов в одной пачке (DefaultLeaseBatch).
	BatchSize int
	// Interval — период цикла LeaseWatch; на разовый LeaseOnce не влияет.
	// Ноль/отрицательное — Timeout/2 (см. DefaultLeaseInterval-комментарий
	// в LeaseWatch).
	Interval time.Duration
	// Now — источник времени для порога now() − Timeout; nil — time.Now.
	// Тесты подставляют фиксированные часы (стиль T-013/T-030).
	Now func() time.Time
	// Log — логгер цикла LeaseWatch; nil — записи подавлены
	// (slog.DiscardHandler). LeaseOnce не логирует — итог возвращается
	// LeaseResult.
	Log *slog.Logger
}

// withDefaults заменяет неположительные значения дефолтами (Now
// проверяется отдельно: функций — nil).
func (o LeaseOptions) withDefaults() LeaseOptions {
	if o.Timeout <= 0 {
		o.Timeout = DefaultLeaseTimeout
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultLeaseBatch
	}
	if o.Interval <= 0 {
		// Половина срока аренды: см. обоснование в LeaseWatch. Вырожденный
		// Timeout ≤ 1 нс делением даёт 0 — тикеру нужен положительный
		// интервал, поэтому минимум 1 нс.
		o.Interval = o.Timeout / 2
		if o.Interval <= 0 {
			o.Interval = time.Nanosecond
		}
	}
	return o
}

// LeaseResult — итог одного прохода LeaseOnce.
type LeaseResult struct {
	// Domains — домены, возвращённые в pending (не больше размера пачки).
	Domains int64
}

// LeaseOnce выполняет один проход (ТЗ 5.3): до opts.BatchSize доменов
// со check_state = queued и queued_at старше opts.Timeout (не deleted,
// без результата) переводятся в check_state = pending одним оператором —
// отметка аренды сбрасывается, остальные колонки не меняются. Повторный
// проход безопасен (ТЗ 3.4): переведённые строки подзапрос больше не
// выбирает.
func LeaseOnce(ctx context.Context, pool db.DB, opts LeaseOptions) (LeaseResult, error) {
	if pool == nil {
		return LeaseResult{}, errors.New("scheduler: возврат истёкших lease: пул не задан")
	}
	o := opts.withDefaults()
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	cutoff := now().Add(-o.Timeout)

	tag, err := pool.Exec(ctx, leaseUpdateSQL,
		int16(model.CheckStatePending),
		int16(model.CheckStateQueued),
		int16(model.StatusDeleted),
		cutoff,
		o.BatchSize,
	)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("scheduler: возврат истёкших lease: %w", err)
	}
	return LeaseResult{Domains: tag.RowsAffected()}, nil
}

// LeaseWatch — фоновый цикл возврата доменов с истёкшей арендой: дренаж
// всех созревших пачками LeaseOnce, затем ожидание тика. Первый дренаж —
// сразу при старте: проход идемпотентен и подбирает зависшее за простой
// сервиса (остановка/рестарт не оставляют «застрявших» доменов — ТЗ 3.4,
// 11.4 п. 9; стиль стартового дренажа T-020/T-030). Ошибка пачки
// прерывает только текущий дренаж: логируется Warn, цикл живёт (контракт
// OnRun, T-012), очередной дренаж продолжит — переведённое не
// возвращается дважды. Отмена ctx — чистый выход с nil (graceful
// shutdown, ТЗ 3.4).
//
// Интервал проверок — lease/2 (а не фикс): баланс «скорость возврата vs
// нагрузка». Проход ищет queued-строки, индекса по check_state = 1 нет
// (частичный индекс — только pending, миграция 000001; индексация — вне
// задачи), поэтому частота обязана масштабироваться со сроком аренды:
// фикс при lease 30 мин давал бы десятки проходов за срок ради выигрыша
// в минуты возврата, а при коротком настраиваемом lease и вовсе
// «переспал» бы сам срок. Половина срока — две проверки за аренду:
// истёкший lease обнаруживается не позже чем через полсрока после
// истечения, всего домен зависнет не дольше чем на полтора срока.
func LeaseWatch(ctx context.Context, pool db.DB, opts LeaseOptions) error {
	if pool == nil {
		return errors.New("scheduler: возврат истёкших lease: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("возврат доменов с истёкшим lease",
		"interval", o.Interval.String(),
		"timeout", o.Timeout.String(),
		"batch", o.BatchSize)

	// drain возвращает все созревшие домены: пачки повторяются, пока
	// проходят целиком; неполная пачка — множество исчерпано.
	drain := func() {
		var total int64
		for {
			res, err := LeaseOnce(ctx, pool, o)
			if err != nil {
				// Отмена во время прохода — часть остановки, не тревога;
				// прочая ошибка — Warn, очередной дренаж повторит.
				if ctx.Err() == nil {
					log.Warn("дренаж возврата истёкших lease",
						"err", err, "returned", total)
				}
				return
			}
			total += res.Domains
			if res.Domains < int64(o.BatchSize) {
				if total > 0 {
					log.Info("дренаж возврата истёкших lease", "domains", total)
				}
				return
			}
		}
	}

	drain()
	ticker := time.NewTicker(o.Interval)
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
