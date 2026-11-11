// Персистентный курсор дренажа retention (T-274; миграция 000020 —
// таблица retention_drain_cursor): позиция дренажа «один раз за жизнь»
// (T-185) переживает не только проходы цикла, но и РЕСТАРТ процесса
// http-worker — деплой, внешний power-cycle виртуалы (прецедент
// 11.11 11:03), потенциальный OOM-килл. До T-274 рестарт сбрасывал
// курсор в ноль: первый проход заново обходил «чистый префикс» истории
// subdomain_results (предикат «есть что чистить» вне индекса —
// heap-визит каждой строки; замеры 10–11.11.2026: ~60 мин на idle,
// >60 мин под полным фронтом), три прохода под фронтом не влезли в
// потолок SelectTimeout 3600 с (инцидент T-273) — дренаж вставал до
// спокойного окна.
//
// Контракт атомарности: upsert курсора исполняется в транзакции чистки
// пачки (retentionPurge) — сохранённая позиция не может опередить
// зафиксированную пачку; сбой пачки оставляет прежний курсор, повтор
// прохода идемпотентно перечитает уже чистые строки (ТЗ 3.4). Пачка с
// отложенными защитой очередей строками позицию в БД НЕ двигает (ревью
// T-274, P1): потеря списка ретраев при рестарте восполняется
// перечитыванием защитного окна — ключи отложатся заново, чистые строки
// выпадут из отбора сами; in-memory курсор двигается как раньше (зонд
// ретраев возвращает ключи в пределах жизни процесса). Принудительные
// catch-up-круги (retentiondrain.go planCircle) удаляют строку DELETE'ом
// в момент плана: рестарт в окне между планом и применением попадает в
// полный catch-up с нуля, а не продолжение со старой позиции — записи
// позади неё вычищает именно круг.
//
// Отсутствие/некорректность состояния — старт с нуля (поведение до
// T-274: один полный catch-up-проход). Таблицы нет (БД до миграции
// 000020) — сентинел errRetentionNoCursorTable: один Info на процесс и
// работа без персистентности (стиль errRetentionNoCounterTable, T-185);
// владелец состояния — только цикл RetentionLoop http-worker, разовый
// RetentionOnce (runbooks T-167, изолированные сборки) его не читает и
// не пишет.
package content

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SQL персистентного курсора. Строка одна (id = 1, CHECK миграции 000020),
// писатель один (цикл http-worker) — конфликтов нет; тексты закреплены
// юнит-тестами (retentioncursor_test.go).
var (
	// retentionCursorLoadSQL — чтение курсора при старте цикла: точечный
	// Lookup по PK единственной строки.
	retentionCursorLoadSQL = "SELECT checked_at, zone_id, domain_id, subdomain_id, variant " +
		"FROM retention_drain_cursor WHERE id = 1"

	// retentionCursorSaveSQL — upsert курсора (в транзакции пачки):
	// INSERT … ON CONFLICT по PK единственной строки; updated_at = now() —
	// диагностика последнего движения. Пачки с отложенными защитой
	// строками позицию не пишут (курсор не двигается за непочищенное —
	// ревью T-274, P1).
	retentionCursorSaveSQL = "INSERT INTO retention_drain_cursor " +
		"(id, checked_at, zone_id, domain_id, subdomain_id, variant, updated_at) " +
		"VALUES (1, $1, $2, $3, $4, $5, now()) " +
		"ON CONFLICT (id) DO UPDATE SET " +
		"checked_at = EXCLUDED.checked_at, zone_id = EXCLUDED.zone_id, " +
		"domain_id = EXCLUDED.domain_id, subdomain_id = EXCLUDED.subdomain_id, " +
		"variant = EXCLUDED.variant, updated_at = EXCLUDED.updated_at"

	// retentionCursorDeleteSQL — сброс персистентного состояния при
	// принудительном catch-up-круге (catchUp): круг обязан пережить
	// рестарт — обходит всё позади прежнего курсора, сохранённая позиция
	// ему мешает.
	retentionCursorDeleteSQL = "DELETE FROM retention_drain_cursor WHERE id = 1"
)

// Сентинелы состояния курсора (обрабатываются вызывающими, не ошибками
// прохода — стиль errRetentionNoCounterTable).
var (
	// errRetentionNoCursorTable — таблицы retention_drain_cursor нет
	// (SQLSTATE 42P01, БД до миграции 000020): штатное переходное
	// состояние старого стенда — персистентность выключается на процесс.
	errRetentionNoCursorTable = errors.New("content: retention: таблицы персистентного курсора нет (миграция 000020 не применена)")

	// errRetentionCursorInvalid — сохранённое состояние некорректно
	// (отметка в будущем сверх допуска часов — рассинхрон ручного SQL):
	// старт с нулевого курсора, Warn в журнал.
	errRetentionCursorInvalid = errors.New("content: retention: сохранённый курсор некорректен (отметка в будущем)")
)

// retentionCursorSkew — допуск расхождения часов приложения и БД при
// валидации загруженного курсора: checked_at пишет now() сервера БД
// (T-058), сверяется с часами процесса.
const retentionCursorSkew = time.Minute

// retentionNoTable — SQLSTATE 42P01 (undefined_table) в ошибке пула:
// литералом кода схемы, как в retentionDebt (стиль record.go).
func retentionNoTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// retentionLoadCursor — чтение персистентного курсора при старте цикла.
// ok=false: строки нет (первый запуск, круг сбросил состояние) —
// err = nil; состояние некорректно — err = errRetentionCursorInvalid.
// Таблицы нет — сентинел errRetentionNoCursorTable (caller: Info и работа
// без персистентности). Нулевая отметка строки (все-нулевой ручной SQL)
// трактуется как «состояния нет».
func retentionLoadCursor(ctx context.Context, pool RetentionDB) (cur retentionCursor, ok bool, err error) {
	rows, err := pool.Query(ctx, retentionCursorLoadSQL)
	if err != nil {
		if retentionNoTable(err) {
			return retentionCursor{}, false, errRetentionNoCursorTable
		}
		return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// Сначала сбой чтения: обрыв соединения — не «строки нет»
		// (стиль retentionDebt).
		if err := rows.Err(); err != nil {
			return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: %w", err)
		}
		return retentionCursor{}, false, nil
	}
	var variant int16
	if err := rows.Scan(&cur.checkedAt, &cur.key.zone, &cur.key.domain, &cur.key.sub, &variant); err != nil {
		return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: строка: %w", err)
	}
	cur.key.variant = variant
	if err := rows.Err(); err != nil {
		return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: %w", err)
	}
	if cur.checkedAt.IsZero() {
		return retentionCursor{}, false, nil
	}
	if cur.checkedAt.After(time.Now().Add(retentionCursorSkew)) {
		return retentionCursor{}, false, errRetentionCursorInvalid
	}
	return cur, true, nil
}

// retentionSaveCursorTx — upsert курсора в ОТКРЫТОЙ транзакции пачки
// (retentionPurge): атомарность «пачка закоммичена ⟺ курсор сохранён».
func retentionSaveCursorTx(ctx context.Context, tx pgx.Tx, cur retentionCursor) error {
	if _, err := tx.Exec(ctx, retentionCursorSaveSQL,
		cur.checkedAt, cur.key.zone, cur.key.domain, cur.key.sub, cur.key.variant,
	); err != nil {
		if retentionNoTable(err) {
			return errRetentionNoCursorTable
		}
		return fmt.Errorf("content: retention: сохранение курсора дренажа: %w", err)
	}
	return nil
}

// retentionResetCursor — удаление персистентного состояния при
// принудительном catch-up-круге (planCircle): круг обязан пережить
// рестарт — обойти всё позади прежнего курсора; сохранённая позиция ему
// мешает. Отдельная короткая транзакция, выполняется в момент плана
// (до применения круга).
func retentionResetCursor(ctx context.Context, pool RetentionDB) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("content: retention: транзакция сброса курсора: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, retentionCursorDeleteSQL); err != nil {
		if retentionNoTable(err) {
			return errRetentionNoCursorTable
		}
		return fmt.Errorf("content: retention: сброс персистентного курсора: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("content: retention: фиксация сброса курсора: %w", err)
	}
	return nil
}
