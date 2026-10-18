// Scheduler: повторная проверка temp_error (T-031; ТЗ 5.3, 6.2).
//
// Домен, чья проверка завершилась временной ошибкой (status = temp_error,
// ТЗ 6.2: SERVFAIL/таймауты, «требуется повтор»), перепроверяется не сразу,
// а через temp_error_retry_after (по умолчанию 24 ч, ТЗ 5.3) и не более
// N раз подряд; после исчерпания попыток домен получает итоговый статус
// по результату последней проверки.
//
// Проход RetryOnce пачкой до opts.BatchSize выбирает кандидатов — домены
// в статусе temp_error, чья проверка завершена (check_state = done, ТЗ 7)
// и last_checked_at старше temp_error_retry_after, — и в одной транзакции
// делит их по счётчику попыток domains.temp_attempts (миграция 000005):
//
//   - попыток меньше N — назначается повтор: check_state → pending
//     (домена перевыберет диспетчер T-027 и поставит в q:dns, T-028),
//     счётчик увеличивается на единицу;
//   - попыток N или больше — фиксируется итоговый статус
//     delegated_no_address, счётчик сбрасывается в ноль.
//
// Итоговый статус — delegated_no_address (код 2, ТЗ 6.2): последняя
// проверка закончилась SERVFAIL/таймаутом, то есть адресов для HTTP-этапа
// не получено — это терминальная ветка «нет адресов» алгоритма ТЗ 7
// («нет адресов → status=delegated_no_address, done»). not_delegated
// отвергнут: он кодирует именно NXDOMAIN апекса (ТЗ 5.4 п. 1, RFC 8020),
// а ТЗ явно различает NXDOMAIN и SERVFAIL/таймаут; новых статусов набор
// не расширяет — семь статусов подтверждены заказчиком (ТЗ 13.1).
// Ошибка классификации самокорректируется регулярным циклом: домен
// остаётся в done и перепроверяется по recheck_interval (T-030, 6 мес).
//
// Счётчик temp_attempts без смены схемы не выразить: check_state —
// перечисление жизненного цикла, flags — битовые булевы признаки,
// dns_mask — маска субдоменов, метки времени не считают попытки, истории
// результатов нет (ТЗ 3.5, перезапись). Миграция 000005 — минимальная:
// одна smallint-колонка. Сброс счётчика при успешной (не temp_error)
// проверке — контракт DNS-этапа (T-036+): новый эпизод temp_error
// начинается с нуля; до его реализации счётчик между эпизодами может
// остаться ненулевым — эффект консервативен (меньше повторов).
//
// Wiring в cmd/scheduler — поздняя задача (OnRun каркаса T-012).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
)

const (
	// DefaultRetryAfter — окно повтора temp_error (ТЗ 5.3:
	// temp_error_retry_after, 24 часа).
	DefaultRetryAfter = 24 * time.Hour

	// DefaultRetryMaxRetries — максимум повторов подряд (ТЗ 5.3: «не более
	// N раз подряд»). Число ТЗ не задаёт; 3 — как счётчик попыток доставки
	// очереди (план T-025): единая политика повторов системы.
	DefaultRetryMaxRetries = 3

	// DefaultRetryBatch — лимит пачки: доменов на один проход, в стиле
	// пачек планировщика (ТЗ 5.3: 1000–10 000) и фоновой очистки T-020.
	DefaultRetryBatch = 1000

	// DefaultRetryInterval — интервал проходов цикла RetryTempErrors при
	// настройках по умолчанию. ТЗ частоту проходов не задаёт: с окном
	// повтора 24 часа часовой проход назначает повтор не позже чем через
	// час после готовности домена; если retry_after настроен меньше часа,
	// интервал ужимается до него (withDefaults), чтобы цикл не «переспал»
	// собственное окно. Проход — seq scan по таблице (индекс невозможен без
	// status в предикате — см. retrySelectSQL), часовой темп ограничивает
	// цену; после дренажа множество кандидатов пусто до созревания новых
	// temp_error (окно RetryAfter).
	DefaultRetryInterval = time.Hour
)

// SQL прохода. unnest-пачки — стиль пакетных UPDATE internal/db (ТЗ 6:
// «запись пачками … UPDATE … FROM unnest(...)»); охраны в WHERE —
// перечитывание состояния на момент апдейта (гонки с T-027/T-030/T-018
// не ломают инвариант «один инкремент на эпизод»), RETURNING отдаёт
// только фактически затронутые строки — счётчики прохода честные.
const (
	// retrySelectSQL — кандидаты повтора: temp_error после завершённой
	// проверки (check_state = done — точка инкремента: одно назначение
	// повтора на один эпизод проверки; pending уже ждёт выборки диспетчера,
	// queued в работе, ТЗ 5.3), окно — по last_checked_at. NULL
	// last_checked_at не выбирается: «ещё не проверялся» — не повод для
	// повтора. ORDER BY по ключу домена — детерминированные пачки без
	// сортировки (совпадает с PK, ТЗ 6.1). Точность окна — сутки:
	// last_checked_at имеет тип date (ТЗ 6.1).
	//
	// Индекса у прохода нет сознательно (T-104): предикат сводится к
	// status = temp_error, а индекс с status в ключе или предикате ломал
	// бы HOT массовых апдейтов статуса без смены check_state — применение
	// delete-файлов (apply_delete) и фиксация итогового статуса
	// (retryFinalizeSQL). Частичный domains_recheck_idx не выручает:
	// окно 24 ч покрывает почти все done-строки, диапазонный скан по
	// возрасту читал бы их целиком ради редких temp_error. Проход —
	// seq scan с часовым интервалом (DefaultRetryInterval).
	retrySelectSQL = "SELECT d.zone_id, d.id, d.temp_attempts FROM domains d " +
		"WHERE d.status = $1 " +
		"AND d.check_state = $2 " +
		"AND d.last_checked_at < $3 " +
		"ORDER BY d.zone_id, d.id LIMIT $4"

	// retryRequeueSQL — назначение повтора: check_state → pending (домена
	// перевыберет диспетчер T-027), счётчик попыток + 1. Охрана
	// status = temp_error AND check_state = done: домен, успевший уйти
	// из done мимо прохода (recheck T-030, возврат lease T-029, воскрешение
	// add-файлом T-018), не инкрементируется повторно.
	retryRequeueSQL = "UPDATE domains d " +
		"SET check_state = $3, temp_attempts = d.temp_attempts + 1 " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"AND d.status = $4 AND d.check_state = $5 " +
		"RETURNING d.zone_id, d.id"

	// retryFinalizeSQL — исчерпание попыток: итоговый статус по последней
	// проверке — delegated_no_address (см. комментарий в начале файла),
	// счётчик в ноль: следующий эпизод temp_error начнётся с чистого
	// счётчика. Охрана та же, что у назначения повтора.
	retryFinalizeSQL = "UPDATE domains d " +
		"SET status = $3, temp_attempts = 0 " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"AND d.status = $4 AND d.check_state = $5 " +
		"RETURNING d.zone_id, d.id"
)

// RetryDB — пул для прохода: db.DB, из которого можно начать транзакцию.
// Оба решения пачки (назначение повтора и итоговый статус) атомарны в
// одной транзакции: сбой между ними не оставляет половину пачки
// обработанной (повтор прохода безопасен, ТЗ 3.4); *pgxpool.Pool
// удовлетворяет интерфейсу.
type RetryDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ RetryDB = (*pgxpool.Pool)(nil)

// RetryOptions — параметры прохода; нулевые значения заменяются
// дефолтами (withDefaults, стиль ingest.CleanupOptions).
type RetryOptions struct {
	// RetryAfter — окно повтора temp_error: кандидаты — домены с
	// last_checked_at старше (DefaultRetryAfter; конфиг
	// scheduler.temp_error_retry_after, ТЗ 5.3).
	RetryAfter time.Duration
	// MaxRetries — максимум повторов подряд (DefaultRetryMaxRetries;
	// конфиг scheduler.temp_error_max_retries, ТЗ 5.3: «не более N раз
	// подряд»).
	MaxRetries int
	// BatchSize — максимум доменов в одном проходе (DefaultRetryBatch);
	// лимит пачки задаёт SELECT кандидатов.
	BatchSize int
	// Interval — период проходов цикла RetryTempErrors; на разовый
	// RetryOnce не влияет. Ноль/отрицательное — min(RetryAfter,
	// DefaultRetryInterval), см. DefaultRetryInterval.
	Interval time.Duration
	// Log — логгер цикла RetryTempErrors; nil — записи подавлены
	// (slog.DiscardHandler). RetryOnce не логирует — итог возвращается
	// RetryResult.
	Log *slog.Logger
}

// withDefaults заменяет неположительные значения дефолтами.
func (o RetryOptions) withDefaults() RetryOptions {
	if o.RetryAfter <= 0 {
		o.RetryAfter = DefaultRetryAfter
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = DefaultRetryMaxRetries
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRetryBatch
	}
	if o.Interval <= 0 {
		// Не дольше собственного окна повтора: при коротком retry_after
		// (тесты, эксперименты) цикл обязан просыпаться чаще окна.
		o.Interval = min(o.RetryAfter, DefaultRetryInterval)
	}
	return o
}

// RetryResult — итог одного прохода RetryOnce.
type RetryResult struct {
	// Requeued — домены, которым назначен повтор (check_state → pending,
	// temp_attempts + 1); после охраны — только фактически переведённые.
	Requeued int
	// Finalized — домены с зафиксированным итоговым статусом
	// (delegated_no_address, temp_attempts → 0).
	Finalized int
}

// retryCandidate — кандидат прохода: ключ домена и счётчик попыток.
type retryCandidate struct {
	zoneID   int16
	id       int64
	attempts int16
}

// RetryOnce выполняет один проход повторных проверок (ТЗ 5.3, 6.2):
// до opts.BatchSize доменов в статусе temp_error с завершённой
// проверкой и last_checked_at старше RetryAfter в одной транзакции
// получают либо назначенный повтор (check_state → pending,
// temp_attempts + 1 — попыток меньше MaxRetries), либо итоговый статус
// delegated_no_address (попытки исчерпаны, temp_attempts → 0). Повторный
// проход безопасен (ТЗ 3.4): домены в pending больше не кандидаты,
// охраны в UPDATE перепроверяют состояние на момент записи.
func RetryOnce(ctx context.Context, pool RetryDB, opts RetryOptions) (RetryResult, error) {
	if pool == nil {
		return RetryResult{}, errors.New("scheduler: повторные проверки temp_error: пул не задан")
	}
	o := opts.withDefaults()

	candidates, err := retryCandidates(ctx, pool, o.BatchSize, o.RetryAfter)
	if err != nil {
		return RetryResult{}, err
	}
	if len(candidates) == 0 {
		// Пустой набор — назначать нечего; транзакция не открывается.
		return RetryResult{}, nil
	}

	var (
		requeueZones []int16
		requeueIDs   []int64
		finalZones   []int16
		finalIDs     []int64
	)
	for _, c := range candidates {
		if int(c.attempts) < o.MaxRetries {
			requeueZones = append(requeueZones, c.zoneID)
			requeueIDs = append(requeueIDs, c.id)
		} else {
			finalZones = append(finalZones, c.zoneID)
			finalIDs = append(finalIDs, c.id)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return RetryResult{}, fmt.Errorf("scheduler: повторные проверки temp_error: транзакция: %w", err)
	}
	// Откат безусловно: после Commit — no-op. Прерванный проход не
	// оставляет половинчатых изменений (ТЗ 3.4).
	defer func() { _ = tx.Rollback(ctx) }()

	var res RetryResult
	if len(requeueZones) > 0 {
		n, err := retryUpdate(ctx, tx, retryRequeueSQL, "назначение повтора", requeueZones, requeueIDs,
			int16(model.CheckStatePending), int16(model.StatusTempError), int16(model.CheckStateDone))
		if err != nil {
			return RetryResult{}, err
		}
		res.Requeued = n
	}
	if len(finalZones) > 0 {
		n, err := retryUpdate(ctx, tx, retryFinalizeSQL, "итоговый статус", finalZones, finalIDs,
			int16(model.StatusDelegatedNoAddress), int16(model.StatusTempError), int16(model.CheckStateDone))
		if err != nil {
			return RetryResult{}, err
		}
		res.Finalized = n
	}
	if err := tx.Commit(ctx); err != nil {
		return RetryResult{}, fmt.Errorf("scheduler: повторные проверки temp_error: фиксация: %w", err)
	}
	return res, nil
}

// retryCandidates выбирает до limit кандидатов повтора (retrySelectSQL):
// ключи доменов и их счётчики попыток для деления пачки в проходе.
func retryCandidates(ctx context.Context, pool RetryDB, limit int, retryAfter time.Duration) ([]retryCandidate, error) {
	cutoff := time.Now().Add(-retryAfter)
	rows, err := pool.Query(ctx, retrySelectSQL,
		int16(model.StatusTempError), int16(model.CheckStateDone), cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: выборка кандидатов: %w", err)
	}
	defer rows.Close()

	candidates := make([]retryCandidate, 0, limit)
	for rows.Next() {
		var c retryCandidate
		if err := rows.Scan(&c.zoneID, &c.id, &c.attempts); err != nil {
			return nil, fmt.Errorf("scheduler: повторные проверки temp_error: чтение кандидатов: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: выборка кандидатов: %w", err)
	}
	return candidates, nil
}

// retryUpdate выполняет пачечный UPDATE … RETURNING и возвращает число
// фактически затронутых доменов (после охран — как RETURNING cleanup,
// T-020: вторая параллельная транзакция, успевшая первой, не считается).
// set — значение SET-колонки (check_state повтора или итоговый status),
// guardStatus/guardState — охрана WHERE.
func retryUpdate(ctx context.Context, tx pgx.Tx, sql, kind string,
	zoneIDs []int16, domainIDs []int64, set, guardStatus, guardState int16) (int, error) {
	rows, err := tx.Query(ctx, sql, zoneIDs, domainIDs, set, guardStatus, guardState)
	if err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: %s: %w", kind, err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var zoneID int16
		var id int64
		if err := rows.Scan(&zoneID, &id); err != nil {
			return 0, fmt.Errorf("scheduler: повторные проверки temp_error: %s: чтение ключей: %w", kind, err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: %s: %w", kind, err)
	}
	return n, nil
}

// RetryTempErrors — фоновая задача повторных проверок (ТЗ 5.3): по
// ticker выполняет дренаж всех созревших кандидатов пачками RetryOnce
// (первый дренаж — сразу при старте: накопленное за простой разбирается
// целиком, а не по пачке за тик), до отмены ctx и возвращает nil —
// контракт OnRun каркаса сервисов (T-012: чистое завершение по
// SIGTERM). Ошибка пачки прерывает только текущий дренаж: логируется
// (Warn), цикл живёт — очередной дренаж продолжит, проход идемпотентен
// (ТЗ 3.4).
func RetryTempErrors(ctx context.Context, pool RetryDB, opts RetryOptions) error {
	if pool == nil {
		return errors.New("scheduler: повторные проверки temp_error: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("фоновые повторные проверки temp_error",
		"interval", o.Interval.String(), "batch", o.BatchSize,
		"retry_after", o.RetryAfter.String(), "max_retries", o.MaxRetries)

	// Дренаж до исчерпания кандидатов (стиль LeaseWatch/RecheckDaily):
	// один тик обрабатывает все созревшие пачки, а не одну — иначе
	// очередь повторов рассасывалась темпом BatchSize за интервал
	// (1000/час по дефолту: массовый DNS-сбой оставался бы в temp_error
	// месяцами). Пачки повторяются, пока проходят целиком; переведённые
	// в pending не выбираются повторно — дренаж конечен, переводы
	// подбирает главный цикл постановки (T-028).
	drain := func() {
		var total RetryResult
		for {
			res, err := RetryOnce(ctx, pool, o)
			switch {
			case err != nil && ctx.Err() != nil:
				// Отмена во время прохода — часть остановки, не тревога.
				return
			case err != nil:
				log.Warn("дренаж повторных проверок temp_error", "err", err,
					"requeued", total.Requeued, "finalized", total.Finalized)
				return
			}
			total.Requeued += res.Requeued
			total.Finalized += res.Finalized
			if res.Requeued+res.Finalized < o.BatchSize {
				if total.Requeued > 0 || total.Finalized > 0 {
					log.Info("дренаж повторных проверок temp_error",
						"requeued", total.Requeued, "finalized", total.Finalized)
				}
				return // неполная пачка — множество исчерпано
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
