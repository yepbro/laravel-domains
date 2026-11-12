package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Column — a column of a batch update: a name and the PostgreSQL SQL type
// the query's array argument is cast to ($n::type[]). Types are as in the
// schema DDL: int2, int4, int8, text, bool, timestamptz, bytea, ...
type Column struct {
	Name string // the column name in the unnest alias u and in SET
	Type string // the SQL type of the array elements: int8, text, ...
}

// Update starts building a batch UPDATE … FROM unnest(...) for the table
// table (column and table names come from code, not from user input;
// they are quoted as identifiers).
//
// Query form:
//
//	UPDATE "domains" AS t
//	SET "check_state" = u."check_state", queued_at = now()
//	FROM unnest($1::int2[], $2::int8[], $3::int2[]) AS u("zone_id", "id", "check_state")
//	WHERE t."zone_id" = u."zone_id" AND t."id" = u."id" AND t."check_state" = 0
//
// Each column array is a single query parameter, so the chunk row count is
// limited only by a sane size, not by the protocol limit (65535
// parameters). Values are passed to Exec/Query column-wise: keys first
// (Key), then SET columns (Set); SetExpr expressions take no values.
//
// The Where predicate is a race guard: a mass state transition changes
// only rows in the expected state (scheduler, spec 5.3).
func Update(table string) *UpdateBuilder {
	return &UpdateBuilder{table: table}
}

// UpdateBuilder — the builder of a single batch UPDATE … FROM unnest(...).
// Immutability after assembly is not required: one builder — one query.
type UpdateBuilder struct {
	table     string
	keys      []Column
	assigns   []Column
	exprs     []string
	wheres    []string
	returning []string
}

// Key adds a key column: it goes into unnest and into the WHERE join
// (t.<name> = u.<name>). Keys identify the updated rows — for example,
// zone_id and id for tables partitioned by zone_id.
func (b *UpdateBuilder) Key(cols ...Column) *UpdateBuilder {
	b.keys = append(b.keys, cols...)
	return b
}

// Set adds a column with values from unnest: each chunk row gets its own
// value (SET t.<name> = u.<name>).
func (b *UpdateBuilder) Set(cols ...Column) *UpdateBuilder {
	b.assigns = append(b.assigns, cols...)
	return b
}

// SetExpr adds a fixed SET expression for the whole chunk — arbitrary SQL,
// e.g. "queued_at = now()" or the literal "status = 5".
func (b *UpdateBuilder) SetExpr(exprs ...string) *UpdateBuilder {
	b.exprs = append(b.exprs, exprs...)
	return b
}

// Where adds a guard predicate (an SQL expression over the t and u
// aliases), ANDed with the key join and previous predicates (repeated
// calls accumulate instead of overwriting: losing a guard in a batch
// UPDATE is data corruption, not an inconvenience).
func (b *UpdateBuilder) Where(pred string) *UpdateBuilder {
	b.wheres = append(b.wheres, pred)
	return b
}

// Returning sets the RETURNING column list (names or expressions): the
// rows actually updated under the guard are read via Query — for example,
// enqueueing exactly the domains transitioned pending → queued.
func (b *UpdateBuilder) Returning(cols ...string) *UpdateBuilder {
	b.returning = append(b.returning, cols...)
	return b
}

// columns returns the order of value columns: keys first, then SET columns.
func (b *UpdateBuilder) columns() []Column {
	cols := make([]Column, 0, len(b.keys)+len(b.assigns))
	cols = append(cols, b.keys...)
	cols = append(cols, b.assigns...)
	return cols
}

// build validates the description and returns the query text and the
// order of value columns.
func (b *UpdateBuilder) build() (string, []Column, error) {
	cols := b.columns()

	var problems []string
	if strings.TrimSpace(b.table) == "" {
		problems = append(problems, "не задана таблица")
	}
	if len(b.keys) == 0 {
		problems = append(problems, "не заданы ключи (Update(...).Key(...))")
	}
	if len(b.assigns) == 0 && len(b.exprs) == 0 {
		problems = append(problems, "не задано ни одного присваивания (Set/SetExpr)")
	}
	for _, c := range cols {
		if strings.TrimSpace(c.Name) == "" {
			problems = append(problems, "колонка с пустым именем")
		}
		if strings.TrimSpace(c.Type) == "" {
			problems = append(problems, fmt.Sprintf("колонка %s: не задан SQL-тип", c.Name))
		}
	}
	if len(problems) > 0 {
		return "", nil, fmt.Errorf("db: UPDATE %s: %s", b.table, strings.Join(problems, "; "))
	}

	unnest := make([]string, len(cols))
	alias := make([]string, len(cols))
	for i, c := range cols {
		unnest[i] = fmt.Sprintf("$%d::%s[]", i+1, c.Type)
		alias[i] = quoteIdent(c.Name)
	}

	set := make([]string, 0, len(b.assigns)+len(b.exprs))
	for _, c := range b.assigns {
		// In SET the target column is not qualified with the table alias:
		// PostgreSQL requires `SET col = ...`, not `SET t.col = ...`.
		set = append(set, fmt.Sprintf("%s = u.%s", quoteIdent(c.Name), quoteIdent(c.Name)))
	}
	set = append(set, b.exprs...)

	where := make([]string, 0, len(b.keys)+len(b.wheres))
	for _, c := range b.keys {
		where = append(where, fmt.Sprintf("t.%s = u.%s", quoteIdent(c.Name), quoteIdent(c.Name)))
	}
	where = append(where, b.wheres...)

	sql := fmt.Sprintf("UPDATE %s AS t SET %s FROM unnest(%s) AS u(%s) WHERE %s",
		quoteIdent(b.table), strings.Join(set, ", "),
		strings.Join(unnest, ", "), strings.Join(alias, ", "),
		strings.Join(where, " AND "))
	if len(b.returning) > 0 {
		sql += " RETURNING " + strings.Join(b.returning, ", ")
	}
	return sql, cols, nil
}

// args validates the value columns (parallel arrays of equal length,
// ordered keys first, then SET columns) and assembles the query arguments.
// A chunk of 0 rows is a no-op: returns nil, nil.
func (b *UpdateBuilder) args(columns [][]any) ([]any, error) {
	cols := b.columns()
	if len(columns) != len(cols) {
		names := make([]string, len(cols))
		for i, c := range cols {
			names[i] = c.Name
		}
		return nil, fmt.Errorf("db: UPDATE %s: передано %d массивов значений, нужно %d (колонки: %s)",
			b.table, len(columns), len(cols), strings.Join(names, ", "))
	}

	n := -1
	for i, col := range columns {
		if n < 0 {
			n = len(col)
			continue
		}
		if len(col) != n {
			return nil, fmt.Errorf("db: UPDATE %s: колонка %s содержит %d значений, а остальные %d (массивы обязаны быть параллельными)",
				b.table, cols[i].Name, len(col), n)
		}
	}
	if n == 0 {
		return nil, nil
	}

	args := make([]any, len(columns))
	for i := range columns {
		args[i] = columns[i]
	}
	return args, nil
}

// SQL returns the assembled query text (Query/Exec build it internally).
// The consumer — CTE-based transition wrappers: zone counter deltas run in
// the same statement as the transition (T-178, incremental-counters spec
// §6.1: "a second statement/CTE") — the transition text is reused as a CTE
// without duplicating the builder.
func (b *UpdateBuilder) SQL() (string, error) {
	sql, _, err := b.build()
	return sql, err
}

// Exec runs the batch UPDATE and returns the number of updated rows
// (under the guard predicate — less than or equal to the chunk row count).
// A chunk of 0 rows is a no-op without touching the DB. For queries with
// Returning use Query.
func (b *UpdateBuilder) Exec(ctx context.Context, pool DB, columns ...[]any) (int64, error) {
	if len(b.returning) > 0 {
		return 0, fmt.Errorf("db: UPDATE %s: задан Returning — используйте Query", b.table)
	}
	args, err := b.args(columns)
	if err != nil {
		return 0, err
	}
	if args == nil {
		return 0, nil
	}
	sql, _, err := b.build()
	if err != nil {
		return 0, err
	}
	tag, err := pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("db: UPDATE %s (%d строк): %w", b.table, len(columns[0]), err)
	}
	return tag.RowsAffected(), nil
}

// Query runs the batch UPDATE with Returning and returns the result rows
// (the table alias is t). The caller must close pgx.Rows.
func (b *UpdateBuilder) Query(ctx context.Context, pool DB, columns ...[]any) (pgx.Rows, error) {
	if len(b.returning) == 0 {
		return nil, fmt.Errorf("db: UPDATE %s: Returning не задан — используйте Exec", b.table)
	}
	args, err := b.args(columns)
	if err != nil {
		return nil, err
	}
	if args == nil {
		return nil, errors.New("db: UPDATE " + b.table + ": Query с пустой пачкой не возвращает строк")
	}
	sql, _, err := b.build()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("db: UPDATE %s (%d строк): %w", b.table, len(columns[0]), err)
	}
	return rows, nil
}

// quoteIdent quotes a name as a PostgreSQL identifier; supports a
// dot-separated schema ("public.domains" → "public"."domains").
func quoteIdent(name string) string {
	parts := strings.Split(name, ".")
	return pgx.Identifier(parts).Sanitize()
}
