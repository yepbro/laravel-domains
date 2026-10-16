package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Column — колонка пакетного обновления: имя и SQL-тип PostgreSQL, которым
// приводится массив-аргумент запроса ($n::type[]). Типы — как в DDL схемы:
// int2, int4, int8, text, bool, timestamptz, bytea, ...
type Column struct {
	Name string // имя колонки в unnest-псевдониме u и в SET
	Type string // SQL-тип элементов массива: int8, text, ...
}

// Update начинает построение пакетного UPDATE … FROM unnest(...) для
// таблицы table (имя колонок и таблицы — из кода, не из пользовательского
// ввода; экранируются как идентификаторы).
//
// Форма запроса:
//
//	UPDATE "domains" AS t
//	SET "check_state" = u."check_state", queued_at = now()
//	FROM unnest($1::int2[], $2::int8[], $3::int2[]) AS u("zone_id", "id", "check_state")
//	WHERE t."zone_id" = u."zone_id" AND t."id" = u."id" AND t."check_state" = 0
//
// Каждый массив-колонка — один параметр запроса, поэтому число строк пачки
// ограничено только разумным размером, а не лимитом протокола (65535
// параметров). Значения в Exec/Query передаются колонками: сначала ключи
// (Key), затем SET-колонки (Set); выражения SetExpr значений не принимают.
//
// Предикат Where — охрана от гонок: массовый перевод состояний меняет
// только строки в ожидаемом состоянии (scheduler, ТЗ 5.3).
func Update(table string) *UpdateBuilder {
	return &UpdateBuilder{table: table}
}

// UpdateBuilder — конструктор одного пакетного UPDATE … FROM unnest(...).
// Неизменяемость после сборки не требуется: один builder — один запрос.
type UpdateBuilder struct {
	table     string
	keys      []Column
	assigns   []Column
	exprs     []string
	where     string
	returning []string
}

// Key добавляет колонку-ключ: попадает в unnest и в соединение WHERE
// (t.<имя> = u.<имя>). Ключи идентифицируют обновляемые строки — например,
// zone_id и id для таблиц, партиционированных по zone_id.
func (b *UpdateBuilder) Key(cols ...Column) *UpdateBuilder {
	b.keys = append(b.keys, cols...)
	return b
}

// Set добавляет колонку со значениями из unnest: каждой строке пачки —
// своё значение (SET t.<имя> = u.<имя>).
func (b *UpdateBuilder) Set(cols ...Column) *UpdateBuilder {
	b.assigns = append(b.assigns, cols...)
	return b
}

// SetExpr добавляет фиксированное SET-выражение на всю пачку — произвольный
// SQL, например "queued_at = now()" или литерал "status = 5".
func (b *UpdateBuilder) SetExpr(exprs ...string) *UpdateBuilder {
	b.exprs = append(b.exprs, exprs...)
	return b
}

// Where добавляет предикат охраны (SQL-выражение над алиасами t и u),
// объединяемый с соединением ключей по AND.
func (b *UpdateBuilder) Where(pred string) *UpdateBuilder {
	b.where = pred
	return b
}

// Returning задаёт список колонок RETURNING (имена или выражения): строки,
// реально обновлённые с учётом охраны, читаются через Query — например,
// публикация в очередь ровно переведённых pending → queued доменов.
func (b *UpdateBuilder) Returning(cols ...string) *UpdateBuilder {
	b.returning = append(b.returning, cols...)
	return b
}

// columns возвращает порядок колонок со значениями: ключи, затем SET-колонки.
func (b *UpdateBuilder) columns() []Column {
	cols := make([]Column, 0, len(b.keys)+len(b.assigns))
	cols = append(cols, b.keys...)
	cols = append(cols, b.assigns...)
	return cols
}

// build проверяет описание и возвращает текст запроса и порядок колонок
// со значениями.
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
		// В SET целевая колонка не квалифицируется алиасом таблицы:
		// PostgreSQL требует `SET col = ...`, а не `SET t.col = ...`.
		set = append(set, fmt.Sprintf("%s = u.%s", quoteIdent(c.Name), quoteIdent(c.Name)))
	}
	set = append(set, b.exprs...)

	where := make([]string, 0, len(b.keys)+1)
	for _, c := range b.keys {
		where = append(where, fmt.Sprintf("t.%s = u.%s", quoteIdent(c.Name), quoteIdent(c.Name)))
	}
	if b.where != "" {
		where = append(where, b.where)
	}

	sql := fmt.Sprintf("UPDATE %s AS t SET %s FROM unnest(%s) AS u(%s) WHERE %s",
		quoteIdent(b.table), strings.Join(set, ", "),
		strings.Join(unnest, ", "), strings.Join(alias, ", "),
		strings.Join(where, " AND "))
	if len(b.returning) > 0 {
		sql += " RETURNING " + strings.Join(b.returning, ", ")
	}
	return sql, cols, nil
}

// args проверяет колонки значений (параллельные массивы одинаковой длины,
// порядок — ключи, затем SET-колонки) и собирает аргументы запроса.
// Пачка из 0 строк — no-op: возвращает nil, nil.
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

// Exec выполняет пакетный UPDATE и возвращает число обновлённых строк
// (с учётом предиката охраны — меньше или равно числу строк пачки).
// Пачка из 0 строк — no-op без обращения к БД. Для запросов с Returning
// используйте Query.
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

// Query выполняет пакетный UPDATE с Returning и возвращает строки
// результата (псевдоним таблицы — t). Вызовивший обязан закрыть pgx.Rows.
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

// quoteIdent экранирует имя как идентификатор PostgreSQL; поддерживает
// схему через точку ("public.domains" → "public"."domains").
func quoteIdent(name string) string {
	parts := strings.Split(name, ".")
	return pgx.Identifier(parts).Sanitize()
}
