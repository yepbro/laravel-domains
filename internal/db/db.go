// Package db — слой доступа к PostgreSQL: пул подключений pgx и хелперы
// массовой записи (ТЗ 6.3, 3.5 основного ТЗ).
//
// Пул. Open создаёт pgxpool по параметрам секции postgres конфигурации
// (internal/config). Лимит pool_size задаётся НА СЕРВИС, а бюджет ТЗ 6.3
// (≤ 20–50 соединений) — суммарный на все сервисы: при восьми сервисах
// задавайте 4–6 на сервис; соединения открываются лениво, простой не
// держит соединений.
//
// Массовая запись (принципы ТЗ 6: «запись пачками COPY, UPDATE … FROM
// unnest(...)», потоковость ТЗ 3.5):
//
//   - CopyFrom / CopyFromStream — COPY в таблицу; потоковый вариант читает
//     строки из итератора пачками ограниченного размера, память не зависит
//     от объёма данных;
//   - Update — типизированный конструктор пакетного UPDATE … FROM
//     unnest($n::type[], …): массив значений каждой колонки — один
//     параметр запроса, поэтому число строк пачки не упирается в лимит
//     протокола (65535 параметров).
//
// Все хелперы принимают контекст: отмена прерывает текущую операцию,
// а потоковый COPY — между пачками (законченные пачки остаются в БД;
// повтор безопасен при идемпотентной записи, ТЗ 3.4).
package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB — часть API pgx-пула, необходимая хелперам пакета; *pgxpool.Pool
// удовлетворяет интерфейсу. Выделен, чтобы юнит-тесты проверяли сборку
// запросов и пачек без БД (реальное поведение — в test/integration).
type DB interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}
