package db

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
)

// RowIter — a row source for streaming COPY: each call returns the next
// row (column values in columns order) or io.EOF at the end. Other errors
// interrupt the copy. The source function allows reading data lazily (line
// by line from a file, from a channel) without materializing it entirely —
// memory is bounded only by the chunk size (spec 3.5).
type RowIter func() ([]any, error)

// CopyFrom copies a ready chunk of rows with a single COPY call. Checks
// rows against the column list and wraps the error with the table name.
// An empty chunk is a no-op without touching the DB.
func CopyFrom(ctx context.Context, pool DB, table pgx.Identifier, columns []string, rows [][]any) (int64, error) {
	if len(columns) == 0 {
		return 0, errors.New("db: COPY: empty column list")
	}
	for i, row := range rows {
		if len(row) != len(columns) {
			return 0, fmt.Errorf("db: COPY %s: row %d has %d values, %d columns", table, i, len(row), len(columns))
		}
	}
	if len(rows) == 0 {
		return 0, nil
	}
	n, err := pool.CopyFrom(ctx, table, columns, pgx.CopyFromRows(rows))
	if err != nil {
		return n, fmt.Errorf("db: COPY %s (%d rows): %w", table, len(rows), err)
	}
	return n, nil
}

// CopyFromStream copies the iterator's rows into the table in chunks of
// batchSize rows: each chunk is a separate COPY call, i.e. a separate
// transaction. Memory is therefore bounded by batchSize (spec 3.5),
// progress is committed to the DB in chunks, and context cancellation
// interrupts the copy before the next chunk starts — completed chunks stay
// in the DB, and a retry is safe with idempotent writes (spec 3.4).
// Returns the total number of copied rows (excluding the rows of the chunk
// that failed).
func CopyFromStream(ctx context.Context, pool DB, table pgx.Identifier, columns []string, batchSize int, next RowIter) (int64, error) {
	if len(columns) == 0 {
		return 0, errors.New("db: COPY: empty column list")
	}
	if batchSize < 1 {
		return 0, fmt.Errorf("db: COPY %s: batch size %d, minimum 1", table, batchSize)
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
		return fmt.Errorf("db: COPY %s aborted at row %d (copied %d): %w", table, rowIdx, total, cause)
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
			return total, fmt.Errorf("db: COPY %s: row source failed at row %d (copied %d): %w", table, rowIdx, total, err)
		}
		if len(row) != len(columns) {
			return total, fmt.Errorf("db: COPY %s: row %d has %d values, %d columns", table, rowIdx, len(row), len(columns))
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
