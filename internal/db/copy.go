package db

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
)

// RowIter — источник строк для потокового COPY: каждый вызов возвращает
// следующую строку (значения колонок по порядку columns) либо io.EOF в
// конце. Другие ошибки прерывают копирование. Функция-источник позволяет
// читать данные лениво (построчно из файла, из канала), не материализуя
// их целиком — память определяется только размером пачки (ТЗ 3.5).
type RowIter func() ([]any, error)

// CopyFrom копирует готовую пачку строк одним вызовом COPY. Проверяет
// соответствие строк списку колонок и оборачивает ошибку именем таблицы.
// Пустая пачка — no-op без обращения к БД.
func CopyFrom(ctx context.Context, pool DB, table pgx.Identifier, columns []string, rows [][]any) (int64, error) {
	if len(columns) == 0 {
		return 0, errors.New("db: COPY: список колонок пуст")
	}
	for i, row := range rows {
		if len(row) != len(columns) {
			return 0, fmt.Errorf("db: COPY %s: строка %d содержит %d значений, колонок %d", table, i, len(row), len(columns))
		}
	}
	if len(rows) == 0 {
		return 0, nil
	}
	n, err := pool.CopyFrom(ctx, table, columns, pgx.CopyFromRows(rows))
	if err != nil {
		return n, fmt.Errorf("db: COPY %s (%d строк): %w", table, len(rows), err)
	}
	return n, nil
}

// CopyFromStream копирует строки итератора в таблицу пачками по batchSize
// строк: каждая пачка — отдельный вызов COPY, то есть отдельная транзакция.
// Поэтому память ограничена batchSize (ТЗ 3.5), прогресс фиксируется в БД
// пачками, а отмена контекста прерывает копирование до начала следующей
// пачки — законченные пачки остаются в БД, и повтор безопасен при
// идемпотентной записи (ТЗ 3.4). Возвращает суммарное число скопированных
// строк (без строк пачки, на которой произошла ошибка).
func CopyFromStream(ctx context.Context, pool DB, table pgx.Identifier, columns []string, batchSize int, next RowIter) (int64, error) {
	if len(columns) == 0 {
		return 0, errors.New("db: COPY: список колонок пуст")
	}
	if batchSize < 1 {
		return 0, fmt.Errorf("db: COPY %s: размер пачки %d, минимум 1", table, batchSize)
	}

	var total int64
	batch := make([][]any, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := CopyFrom(ctx, pool, table, columns, batch)
		total += n
		batch = batch[:0]
		return err
	}
	interrupted := func(rowIdx int, cause error) error {
		return fmt.Errorf("db: COPY %s прерван на строке %d (скопировано %d): %w", table, rowIdx, total, cause)
	}

	rowIdx := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, interrupted(rowIdx, err)
		}
		row, err := next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return total, fmt.Errorf("db: COPY %s: источник строк на строке %d (скопировано %d): %w", table, rowIdx, total, err)
		}
		if len(row) != len(columns) {
			return total, fmt.Errorf("db: COPY %s: строка %d содержит %d значений, колонок %d", table, rowIdx, len(row), len(columns))
		}
		batch = append(batch, row)
		rowIdx++
		if len(batch) == batchSize {
			if err := ctx.Err(); err != nil {
				return total, interrupted(rowIdx, err)
			}
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}
