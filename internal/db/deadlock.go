// Прикладной ретрай тупика PostgreSQL (T-268). Дедлок 40P01 — штатный
// исход конкурентных транзакций: сервер обнаруживает цикл блокировок сам
// (deadlock_timeout, 1 с), жертва откатывается — данные не повреждаются,
// операцию можно повторить. Структурный инвариант порядка блокировок
// счётчиков (docs/schema.md, раздел crawler_counters; T-267) убирает
// известные циклы, но не гарантирует отсутствия новых: обёртка поглощает
// остаточные и будущие дедлоки в горячих точках конвейера, не отправляя
// задачу в ределивери (каждый повтор доставки q:http — полный
// HTTP-эпизод; инцидент 07.11 — runbooks.md §2.8).
//
// Повторяются только идемпотентные операции (ТЗ 3.4): транзакция записи
// результата http-worker (охраны/перезапись), autocommit-операторы записи
// DNS-эпизода (охраны done/deleted), транзакция решения ретрая
// планировщика (охраны обоих UPDATE). Диапазон паузы — джиттером, чтобы
// пачки, столкнувшиеся одновременно, не повторялись синхронно.
package db

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE 40P01 — deadlock_detected (коды — литералами, стиль
// resultUpdateStatusSQL T-039; pgerrcode не тянется ради одной константы).
const sqlstateDeadlock = "40P01"

// Параметры повтора по умолчанию (T-268).
const (
	// DefaultDeadlockRetries — повторные выполнения после первой неудачи:
	// жертва дедлока уже прождала deadlock_timeout (1 с), повтор почти
	// всегда успевает — двух хватает с запасом.
	DefaultDeadlockRetries = 2

	// DefaultDeadlockBase — база паузы повтора: миллисекунды против
	// секунд обнаружения дедлока — конкурирующая транзакция успевает
	// зафиксироваться и освободить строки.
	DefaultDeadlockBase = 50 * time.Millisecond

	// DefaultDeadlockMax — потолок паузы одного повтора: даже при
	// длинном приступе дедлоков добавка к Latency транзакции — доли
	// секунды, конвейер не замирает.
	DefaultDeadlockMax = 250 * time.Millisecond
)

// MaxDeadlockRetries — жёсткий потолок повторов обёртки: ретрай —
// страховка остаточных случаев, а не лечение постоянного цикла
// блокировок (устойчивый 40P01 — инверсия порядка где-то в коде,
// runbooks.md §2.8: чинить причину, а не глушить повторами).
const MaxDeadlockRetries = 2

// DeadlockRetry — параметры обёртки повтора; нулевые значения заменяются
// дефолтами, Retries сверх MaxDeadlockRetries ужимается до потолка
// (withDefaults). Тесты ускоряют паузу явным малым Base.
type DeadlockRetry struct {
	// Retries — повторные выполнения после первой неудачи (0 — дефолт;
	// > MaxDeadlockRetries — потолок).
	Retries int
	// Base — база паузы перед повтором; фактическая пауза — джиттер
	// [Base/2, Base].
	Base time.Duration
	// Max — потолок паузы одного повтора.
	Max time.Duration
}

// DefaultDeadlockRetry — параметры горячих точек конвейера (T-268):
// до двух повторов с паузой десятки миллисекунд.
var DefaultDeadlockRetry = DeadlockRetry{
	Retries: DefaultDeadlockRetries,
	Base:    DefaultDeadlockBase,
	Max:     DefaultDeadlockMax,
}

// withDefaults нормализует параметры.
func (r DeadlockRetry) withDefaults() DeadlockRetry {
	if r.Retries <= 0 {
		r.Retries = DefaultDeadlockRetries
	}
	if r.Retries > MaxDeadlockRetries {
		r.Retries = MaxDeadlockRetries
	}
	if r.Base <= 0 {
		r.Base = DefaultDeadlockBase
	}
	if r.Max <= 0 {
		r.Max = DefaultDeadlockMax
	}
	return r
}

// delay — пауза перед очередным повтором: джиттер [Base/2, Base],
// потолок Max.
func (r DeadlockRetry) delay() time.Duration {
	half := r.Base / 2
	d := half + time.Duration(rand.Int63n(int64(half)+1))
	if d > r.Max {
		return r.Max
	}
	return d
}

// IsDeadlock сообщает, является ли ошибка дедлоком PostgreSQL
// (SQLSTATE 40P01): ошибки проводятся через errors.As — распознаётся и
// завёрнутая fmt.Errorf("%w") вызывающими слоями.
func IsDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == sqlstateDeadlock
}

// RetryDeadlock выполняет fn и повторяет её при дедлоке 40P01: до
// r.Retries повторов (умолчание — DefaultDeadlockRetry), перед каждым —
// пауза с джиттером и потолком, прерываемая отменой ctx. onRetry (может
// быть nil) вызывается перед каждым фактическим повтором — точка метрики
// crawler_db_deadlock_retried_total (T-268). Иные ошибки возвращаются
// сразу; исчерпание повторов возвращает последнюю ошибку наружу —
// вызывающий решает судьбу доставки (at-least-once, ТЗ 3.4). Отмена ctx
// до повтора возвращает исходную ошибку дедлока: попытка прервана
// остановкой, а не исчерпана.
func RetryDeadlock(ctx context.Context, r DeadlockRetry, onRetry func(attempt int), fn func() error) error {
	if fn == nil {
		return errors.New("db: ретрай 40P01: повторяемая функция не задана")
	}
	o := r.withDefaults()
	err := fn()
	for attempt := 1; IsDeadlock(err) && attempt <= o.Retries; attempt++ {
		if ctx.Err() != nil {
			return err
		}
		if d := o.delay(); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-ctx.Done():
				timer.Stop()
				return err
			case <-timer.C:
			}
		}
		if onRetry != nil {
			onRetry(attempt)
		}
		err = fn()
	}
	return err
}
