package content

// Перезапись ссылки на контент при перезаписи результата (T-044; ТЗ 5.7, 8).
//
// ТЗ 5.7: «при перезаписи результата счётчик ссылок старого контента
// уменьшается». Дедупликация Put и снятие ссылки Delete — уже в хранилище
// (T-042); здесь недостающая операция учёта для слоя результатов:
// субдомен/вариант получил новый контент → новая ссылка добавляется,
// старая снимается. Запись самих строк subdomain_results — территория
// http-worker (T-058): ниже только чистые функции свопа для вызова оттуда;
// рекомендуемое место — одна транзакция с записью результата (SwapRefTx).

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
)

// refNoHash — нулевой Hash: «ссылки нет», NULL content_hash в
// subdomain_results (ТЗ 6.1). Вероятность хэша настоящего контента,
// равного нулю (первые 16 байт SHA-256), — 2^-128, пренебрежима;
// семантика зафиксирована в одном месте, чтобы слои не расходились
// в трактовке NULL.
var refNoHash Hash

// ErrRefIntegrity — учёт ссылок contents расходится с ожиданием свопа:
// контент не записан Put-ом (или мёртв — refcount = 0) там, где нужна
// живая ссылка. Это ошибка целостности, а не ErrNotFound: идемпотентная
// запись результата (ТЗ 3.4) в такую ветку попадать не должна; молча
// уйти в минус или пропустить шаг хуже, чем ошибиться — транзакция
// откатывается целиком, повторная доставка разбирается вызывающим.
var ErrRefIntegrity = errors.New("content: нарушен учёт ссылок на контент")

// RefDB — пул для свопа ссылок: db.DB, из которого можно начать
// транзакцию (стиль ingest.CleanupDB, scheduler.RetryDB). Оба оператора
// свопа атомарны в одной транзакции (ТЗ 3.4); *pgxpool.Pool
// удовлетворяет интерфейсу.
type RefDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: реальный пул подходит под интерфейс.
var _ RefDB = (*pgxpool.Pool)(nil)

// SwapRef перезаписывает ссылку на контент у одного результата (ТЗ 5.7,
// 8): newHash получает ссылку (refcount + 1), oldHash теряет её
// (refcount − 1) — атомарно в одной транзакции БД, поверх той же таблицы
// contents и тех же операторов, что у Put/Delete хранилища (T-042).
// Вызывается слоем записи результатов http-worker (T-058) при перезаписи
// subdomain_results со сменой content_hash; сами строки результатов
// функция не трогает.
//
// Сценарии (нулевой Hash — «нет ссылки», NULL content_hash, ТЗ 6.1):
//   - old == new — контент не сменился: no-op без транзакции
//     (идемпотентность повторной доставки, ТЗ 3.4);
//   - старый нулевой — первый контент результата: только +1 новому;
//   - новый нулевой — результат без контента: только −1 старому;
//   - оба нулевых — no-op;
//   - refcount старого после −1 достиг нуля — строка остаётся «мёртвой»
//     (контракт T-042): Get/Delete её не видят, место освобождает
//     компактификация паков (T-045).
//
// Порядок операторов — сначала +1 новому, потом −1 старому. Внутри одной
// транзакции промежуточного состояния нет; порядок зафиксирован для
// встраивания свопа в транзакцию записи результата (SwapRefTx, T-058) и
// на случай выполнения шагов раздельно: крах «после +1, до −1» оставляет
// лишнюю ссылку у старого контента — безопасно, блок лишь остаётся живым
// (утечка места, данные целы); крах в обратном порядке теряет живую
// ссылку: refcount контента, на который всё ещё ссылается результат,
// мог достичь нуля — компактификация T-045 освободит блок, Get вернёт
// ErrNotFound для живого результата, повторное применение правил анализа
// (ТЗ 5.7: контент хранится, пока нужно переприменять правила) станет
// невозможно. По ТЗ 3.4 выбираем безопасную сторону: утечка места лучше
// потери данных.
//
// Охрана refcount > 0 в обоих операторах — не только запрет ухода
// в минус: RowsAffected = 0 значит, что новый контент не был записан
// Put-ом (или мёртв), либо ссылка старого уже снята — расхождение учёта
// результата и contents. Такой случай — ErrRefIntegrity с откатом
// транзакции: лучше ошибиться, чем молча испортить счётчик.
func SwapRef(ctx context.Context, pool RefDB, oldHash, newHash Hash) error {
	if pool == nil {
		return errors.New("content: своп ссылок: пул не задан")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if oldHash == newHash {
		// Контент не сменился (в т.ч. обе ссылки нулевые): счётчики
		// не трогаются, транзакция не открывается.
		return nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("content: своп ссылок %s → %s: транзакция: %w", oldHash, newHash, err)
	}
	// Откат безусловно: после Commit — no-op. Прерванный своп не
	// оставляет половинчатых изменений (ТЗ 3.4).
	defer func() { _ = tx.Rollback(ctx) }()
	if err := SwapRefTx(ctx, tx, oldHash, newHash); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("content: своп ссылок %s → %s: фиксация: %w", oldHash, newHash, err)
	}
	return nil
}

// SwapRefTx — примитив SwapRef для транзакции вызывающего: T-058 пишет
// subdomain_results и меняет ссылки в ОДНОЙ транзакции — раздельные
// транзакции опасны в обе стороны (ссылка без строки или строка без
// ссылки, ТЗ 3.4). Семантика, сценарии и порядок операторов — как у
// SwapRef; Begin/Commit за вызывающим.
func SwapRefTx(ctx context.Context, tx pgx.Tx, oldHash, newHash Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil {
		return errors.New("content: своп ссылок: транзакция не задана")
	}
	if oldHash == newHash {
		return nil // контент не сменился (в т.ч. обе ссылки нулевые)
	}
	if newHash != refNoHash {
		tag, err := tx.Exec(ctx, sqlMetaAddRef, newHash[:])
		if err != nil {
			return fmt.Errorf("content: своп ссылок %s → %s: ссылка новому контенту: %w", oldHash, newHash, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s не записан в хранилище (Put) или мёртв — сначала Put, затем результат",
				ErrRefIntegrity, newHash)
		}
	}
	if oldHash != refNoHash {
		tag, err := tx.Exec(ctx, sqlMetaDelRef, oldHash[:])
		if err != nil {
			return fmt.Errorf("content: своп ссылок %s → %s: снятие ссылки старого контента: %w", oldHash, newHash, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: у %s нет живой ссылки — учёт результата расходится с contents",
				ErrRefIntegrity, oldHash)
		}
	}
	return nil
}

// SwapRef перезаписывает ссылку на контент через пул этого хранилища:
// та же таблица contents и те же операторы, что у Put/Delete (T-042).
// Удобен воркеру, уже держащему *PackStore (T-058). OpenPackStore
// принимает db.DB, поэтому способность пула открывать транзакции
// (RefDB, например *pgxpool.Pool) проверяется во время вызова.
// Семантика и порядок операторов — SwapRef.
func (s *PackStore) SwapRef(ctx context.Context, oldHash, newHash Hash) error {
	pool, ok := s.pool.(RefDB)
	if !ok {
		return fmt.Errorf("content: своп ссылок: пул %T не открывает транзакций (нужен RefDB, например *pgxpool.Pool)", s.pool)
	}
	return SwapRef(ctx, pool, oldHash, newHash)
}
