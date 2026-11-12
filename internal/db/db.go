// Package db — the PostgreSQL access layer: a pgx connection pool and
// bulk-write helpers (spec 6.3, 3.5 of the main spec).
//
// Pool. Open creates a pgxpool from the parameters of the postgres config
// section (internal/config). The pool_size limit is set PER SERVICE, while
// the spec 6.3 budget (≤ 20–50 connections) is shared across all services:
// with eight services set 4–6 per service; connections open lazily, and
// idle time holds no connections.
//
// Bulk write (principles of spec 6: "write in COPY chunks, UPDATE … FROM
// unnest(...)", streaming per spec 3.5):
//
//   - CopyFrom / CopyFromStream — COPY into a table; the streaming variant
//     reads rows from the iterator in chunks of limited size, memory does
//     not depend on the data volume;
//   - Update — a typed constructor of a batch UPDATE … FROM
//     unnest($n::type[], …): each column's value array is a single query
//     parameter, so the chunk row count does not hit the protocol limit
//     (65535 parameters).
//
// All helpers take a context: cancellation interrupts the current
// operation, and a streaming COPY — between chunks (completed chunks stay
// in the DB; a retry is safe with idempotent writes, spec 3.4).
package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB — the part of the pgx pool API needed by the package helpers;
// *pgxpool.Pool satisfies the interface. Extracted so unit tests can check
// query and chunk assembly without a DB (real behavior lives in
// test/integration).
type DB interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}
