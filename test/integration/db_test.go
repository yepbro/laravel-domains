// Интеграционные тесты слоя БД (T-007): пул internal/db и хелперы массовой
// записи — потокочный COPY пачками и пакетный UPDATE … FROM unnest(...) —
// на живом PostgreSQL dev-окружения.
//
// Изоляция: тесты выполняются в собственной схеме db_test_* с таблицей
// items (ключи zone_id+id, как у таблиц схемы ТЗ 6.1), рабочая схема
// crawler не затрагивается (как в миграционных тестах, но без наката
// миграций — слою БД схема проекта не нужна).
package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
)

// dbTestEnv создаёт схему с таблицей items и возвращает пул с search_path
// на неё; схема удаляется при завершении теста.
func dbTestEnv(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	base, err := pgxpool.New(ctx, pgDSN(t))
	if err != nil {
		t.Fatalf("подключение к PostgreSQL: %v", err)
	}

	schema := fmt.Sprintf("db_test_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	if _, err := base.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	const table = `CREATE TABLE %s.items (
		zone_id   smallint NOT NULL,
		id        bigint   NOT NULL,
		status    smallint NOT NULL DEFAULT 0,
		data      text,
		queued_at timestamptz,
		PRIMARY KEY (zone_id, id))`
	if _, err := base.Exec(ctx, fmt.Sprintf(table, schema)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Пул открывается самим пакетом из секции postgres — как в сервисах.
	pool, err := db.Open(ctx, db.NewOptions(config.Postgres{
		DSN:      pgDSN(t) + "&search_path=" + schema,
		PoolSize: 4,
	}))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		if _, err := base.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("очистка схемы %s: %v", schema, err)
		}
		base.Close()
	})
	return pool
}

// itemData — данные строки: спецсимволы COPY (таб, перенос, кавычки)
// и юникод, чтобы проверить экранирование без искажений.
func itemData(i int64) string {
	return fmt.Sprintf("row-%06d \tданные\nюникод «сайт-проверка» \"в кавычках\"", i)
}

// itemsIter — итератор n строк (zone_id=1, id=0..n-1); onCancel вызывается
// перед выдачей строки rowIdx (отмена контекста и т. п.).
func itemsIter(n int64, onCancel func(rowIdx int64)) db.RowIter {
	i := int64(0)
	return func() ([]any, error) {
		if i >= n {
			return nil, io.EOF
		}
		if onCancel != nil {
			onCancel(i)
		}
		row := []any{int16(1), i, itemData(i)}
		i++
		return row, nil
	}
}

var itemsCopyColumns = []string{"zone_id", "id", "data"}

// itemsCopy копирует n строк в items пачками по batchSize (тестовые данные).
func itemsCopy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int64, batchSize int) {
	t.Helper()
	total, err := db.CopyFromStream(ctx, pool, pgx.Identifier{"items"}, itemsCopyColumns, batchSize, itemsIter(n, nil))
	if err != nil {
		t.Fatalf("COPY %d строк: %v", n, err)
	}
	if total != n {
		t.Fatalf("COPY вернул %d, want %d", total, n)
	}
}

func countItems(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	query := "SELECT count(*) FROM items"
	if where != "" {
		query += " WHERE " + where
	}
	if err := pool.QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// TestDB_PoolOpen — пул из секции postgres конфигурации открывается
// (fail-fast Ping уже прошёл), настройки применены, запросы работают.
func TestDB_PoolOpen(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)

	if max := pool.Config().MaxConns; max != 4 {
		t.Errorf("MaxConns = %d, want 4 (pool_size из NewOptions)", max)
	}
	var one int
	if err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Errorf("SELECT 1: %d, err=%v", one, err)
	}
	if total := pool.Stat().TotalConns(); total < 1 {
		t.Errorf("TotalConns = %d, want >= 1 после запроса", total)
	}
}

// TestDB_CopyFromStream — реальный COPY: пачки по 100 строк (1005 строк =
// 11 вызовов COPY), данные со спецсимволами не искажаются, DEFAULT
// колонок вне списка COPY применяется.
func TestDB_CopyFromStream(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 1005
	itemsCopy(t, ctx, pool, n, 100)

	if got := countItems(t, pool, ""); got != n {
		t.Errorf("строк в items = %d, want %d", got, n)
	}

	var data string
	if err := pool.QueryRow(ctx, "SELECT data FROM items WHERE id = $1", int64(500)).Scan(&data); err != nil {
		t.Fatal(err)
	}
	if want := itemData(500); data != want {
		t.Errorf("data строки 500 = %q, want %q (COPY исказил спецсимволы?)", data, want)
	}

	var status int
	if err := pool.QueryRow(ctx, "SELECT status FROM items WHERE id = 0").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != 0 {
		t.Errorf("status = %d, want 0 (DEFAULT колонки вне списка COPY)", status)
	}
}

// TestDB_CopyFromStream_Cancel — отмена контекста посреди потока:
// законченные пачки остаются в БД, незаконченная не записывается,
// возвращается ошибка с контекстом и числом закоммиченных строк.
func TestDB_CopyFromStream_Cancel(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Отмена перед строкой 30 (31-я): строки 0–29 = 3 полные пачки по 10.
	it := itemsIter(100, func(rowIdx int64) {
		if rowIdx == 30 {
			cancel()
		}
	})
	total, err := db.CopyFromStream(ctx, pool, pgx.Identifier{"items"}, itemsCopyColumns, 10, it)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if total != 30 {
		t.Errorf("скопировано %d, want 30 (пачки до отмены)", total)
	}
	if got := countItems(t, pool, ""); got != 30 {
		t.Errorf("строк в items = %d, want 30 (незаконченная пачка не записана)", got)
	}
}

// TestDB_UpdateUnnest — пакетный UPDATE через unnest: массовый перевод
// статуса, охрана Where (повтор — 0 строк), разные значения по строкам.
func TestDB_UpdateUnnest(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 60
	itemsCopy(t, ctx, pool, n, 50)

	zones := make([]any, n)
	ids := make([]any, n)
	for i := range n {
		zones[i] = int16(1)
		ids[i] = int64(i)
	}
	five := make([]any, n)
	for i := range five {
		five[i] = int16(5)
	}

	// Перевод всех в статус 5 с отметкой времени; только из статуса 0.
	got, err := db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		Set(db.Column{Name: "status", Type: "int2"}).
		SetExpr("queued_at = now()").
		Where("t.status = 0").
		Exec(ctx, pool, zones, ids, five)
	if err != nil {
		t.Fatal(err)
	}
	if got != n {
		t.Errorf("обновлено %d, want %d", got, n)
	}
	if c := countItems(t, pool, "status = 5 AND queued_at IS NOT NULL"); c != n {
		t.Errorf("status=5 с отметкой: %d, want %d", c, n)
	}

	// Повтор с той же охраной — ни одной строки (гонка защиты Where).
	if got, err = db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		SetExpr("status = 6").
		Where("t.status = 0").
		Exec(ctx, pool, zones, ids); err != nil || got != 0 {
		t.Errorf("повторный перевод: (%d, %v), want (0, nil)", got, err)
	}

	// Разные значения по строкам пачки: чётные → 7, нечётные → 9.
	mixed := make([]any, n)
	for i := range mixed {
		mixed[i] = int16(7)
		if i%2 == 1 {
			mixed[i] = int16(9)
		}
	}
	got, err = db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		Set(db.Column{Name: "status", Type: "int2"}).
		Where("t.status = 5").
		Exec(ctx, pool, zones, ids, mixed)
	if err != nil || got != n {
		t.Fatalf("перевод из 5: (%d, %v), want (%d, nil)", got, err, n)
	}
	if c := countItems(t, pool, "status = 7"); c != n/2 {
		t.Errorf("status=7: %d, want %d", c, n/2)
	}
	if c := countItems(t, pool, "status = 9"); c != n/2 {
		t.Errorf("status=9: %d, want %d", c, n/2)
	}
}

// TestDB_UpdateUnnestReturning — Query с RETURNING отдаёт ключи строк,
// реально переведённых с учётом охраны (паттерн scheduler ТЗ 5.3:
// публикация в очередь ровно переведённых доменов).
func TestDB_UpdateUnnestReturning(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 20
	itemsCopy(t, ctx, pool, n, 100)

	zones := make([]any, n)
	evenIDs := make([]any, 0, n/2)
	for i := range n {
		zones[i] = int16(1)
		if i%2 == 0 {
			evenIDs = append(evenIDs, int64(i))
		}
	}
	evenZones := make([]any, len(evenIDs))
	for i := range evenZones {
		evenZones[i] = int16(1)
	}

	rows, err := db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		SetExpr("status = 3").
		Where("t.status = 0 AND t.id % 2 = 0").
		Returning("t.id").
		Query(ctx, pool, evenZones, evenIDs)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация RETURNING: %v", err)
	}
	rows.Close()

	if len(got) != n/2 {
		t.Fatalf("RETURNING вернул %d строк, want %d", len(got), n/2)
	}
	for i := 0; i < n; i += 2 {
		if !got[int64(i)] {
			t.Errorf("id %d отсутствует в RETURNING", i)
		}
	}
	if c := countItems(t, pool, "status = 3"); c != n/2 {
		t.Errorf("status=3: %d, want %d", c, n/2)
	}
}

// TestDB_UpdateUnnest_BeyondParamLimit — пачка больше лимита параметров
// протокола (65535): массив каждой колонки — один аргумент unnest, поэтому
// 70 000 строк обновляются одним запросом. Регрессионный контракт:
// конструктор не должен «распадаться» на параметр на строку.
func TestDB_UpdateUnnest_BeyondParamLimit(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 70_000
	itemsCopy(t, ctx, pool, n, 10_000)

	zones := make([]any, n)
	for i := range zones {
		zones[i] = int16(1)
	}

	got, err := db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		SetExpr("status = 1").
		Exec(ctx, pool, zones, idRange(n))
	if err != nil {
		t.Fatalf("обновление %d строк одним запросом: %v", n, err)
	}
	if got != n {
		t.Errorf("обновлено %d, want %d", got, n)
	}
	if c := countItems(t, pool, "status = 1"); c != n {
		t.Errorf("status=1: %d, want %d", c, n)
	}
}

// idRange — массив id 0..n-1 (ключи пачки).
func idRange(n int64) []any {
	ids := make([]any, n)
	for i := range n {
		ids[i] = i
	}
	return ids
}
