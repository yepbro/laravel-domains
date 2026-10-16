// Package migrate — применение миграций PostgreSQL при старте сервисов.
//
// Схема БД хранится в репозитории (migrations/) и встраивается в бинарник
// через go:embed (migrations.FS), поэтому сервис применяет миграции сам,
// без внешнего каталога и отдельного шага развёртывания (ТЗ 6.3).
//
// Свойства раннера:
//   - гонка одновременного старта нескольких сервисов исключена сессионной
//     advisory-блокировкой pg_advisory_lock на выделенном соединении:
//     она закрывает и инициализацию драйвера (создание schema_migrations),
//     и применение; внутри golang-migrate есть своя advisory-блокировка
//     на время Up (ключ из имени БД + schema_migrations), так что CLI
//     migrate и раннер тоже не конфликтуют;
//   - повторный запуск на актуальной схеме не ошибка (migrate.ErrNoChange
//     считается успехом) — hot reload (air) и рестарты безопасны;
//   - dirty-состояние (прерванная миграция) — ошибка с понятным текстом;
//     восстановление: make psql → TRUNCATE schema_migrations (см. docs).
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"crawler/migrations"

	// Регистрация database/sql-драйвера "pgx/v5" (stdlib pgx).
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Параметры раннера. Значения с запасом: применение схемы на пустой БД
// занимает секунды, а lockTimeout должен покрывать самую медленную миграцию,
// чтобы параллельно стартующие сервисы ждали её, а не отваливались.
// Переменные (не константы) — для переопределения в тестах.
var (
	// lockTimeout — ожидание advisory-блокировки, пока миграции применяет
	// другой экземпляр (параллельный старт сервисов).
	lockTimeout = 5 * time.Minute
	// connectTimeout — ожидание доступности PostgreSQL при старте
	// (за пределами compose сервис может подняться раньше БД).
	connectTimeout = 30 * time.Second
)

// connectRetryInterval — пауза между попытками подключиться.
const connectRetryInterval = time.Second

// advisoryLockKey — ключ pg_advisory_lock раннера. Сессионная блокировка
// на выделенном соединении сериализует не только применение миграций
// (это умеет и сам golang-migrate), но и инициализацию драйвера —
// создание schema_migrations — при параллельном старте сервисов.
// Константа произвольная, но фиксированная для всех экземпляров.
const advisoryLockKey = 0x63726177 // "craw"

// ErrDirty возвращается, когда схема осталась в dirty-состоянии после
// прерванной миграции: golang-migrate отказывается применять следующие,
// пока версия не подтверждена вручную.
var ErrDirty = errors.New("миграции: схема в dirty-состоянии (прерванная миграция); выполните TRUNCATE schema_migrations или `migrate force <версия>` после разбора причины")

// Up применяет все встроенные миграции (up) до актуальной версии.
// Блокирует вызывающего до завершения (включая ожидание БД и блокировки);
// миграции применяются быстрее, чем истекает таймаут контекста, поэтому
// ctx используется только на этапах подключения и ожидания блокировки.
func Up(ctx context.Context, dsn string) error {
	if dsn == "" {
		return errors.New("миграции: пустой DSN PostgreSQL")
	}

	db, err := openAndWait(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck // закрытие после применения, БД больше не нужна

	// Выделенное соединение: сессионная advisory-блокировка живёт до конца
	// сессии, поэтому держать её на соединении из пула нельзя.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("миграции: соединение для блокировки: %w", err)
	}
	defer conn.Close() //nolint:errcheck // также снимает advisory-блокировку

	if err := acquireAdvisoryLock(ctx, conn); err != nil {
		return err
	}
	defer releaseAdvisoryLock(conn)

	src, err := newSource()
	if err != nil {
		return fmt.Errorf("миграции: встроенные файлы: %w", err)
	}

	// Одна миграция = один многострочный SQL-файл: pgx выполняет его
	// целиком через простой протокол (без аргументов), как и CLI migrate.
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return fmt.Errorf("миграции: драйвер БД: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return fmt.Errorf("миграции: инициализация: %w", err)
	}
	m.LockTimeout = lockTimeout

	err = m.Up()
	switch {
	case err == nil:
		version, _, verr := m.Version()
		if verr != nil {
			return fmt.Errorf("миграции: версия после применения: %w", verr)
		}
		slog.Info("миграции: схема применена", "version", version)
		return nil
	case errors.Is(err, migrate.ErrNoChange):
		slog.Info("миграции: схема уже актуальна, применение не требуется")
		return nil
	case errors.As(err, new(migrate.ErrDirty)):
		return ErrDirty
	default:
		return fmt.Errorf("миграции: применение: %w", err)
	}
}

// acquireAdvisoryLock ждёт pg_advisory_lock не дольше lockTimeout:
// если миграции у другого экземпляра зависли, лучше упасть (сервис
// перезапустится по restart-политике), чем ждать бессрочно.
func acquireAdvisoryLock(ctx context.Context, conn *sql.Conn) error {
	lockCtx, cancel := context.WithTimeout(ctx, lockTimeout)
	defer cancel()
	const query = `SELECT pg_advisory_lock($1)`
	if _, err := conn.ExecContext(lockCtx, query, advisoryLockKey); err != nil {
		return fmt.Errorf("миграции: ожидание advisory-блокировки (её держит другой экземпляр?): %w", err)
	}
	return nil
}

// releaseAdvisoryLock снимает блокировку; ошибка не критична —
// закрытие соединения снимает её само.
func releaseAdvisoryLock(conn *sql.Conn) {
	const query = `SELECT pg_advisory_unlock($1)`
	if _, err := conn.ExecContext(context.Background(), query, advisoryLockKey); err != nil {
		slog.Warn("миграции: снятие advisory-блокировки не удалось (снимется при закрытии соединения)", "err", err)
	}
}

// newSource возвращает источник миграций поверх встроенного набора.
// Вынесен отдельно, чтобы юнит-тесты могли проверить набор так же,
// как его читает golang-migrate.
func newSource() (source.Driver, error) {
	return iofs.New(migrations.FS, ".")
}

// openAndWait открывает пул к PostgreSQL и ждёт его доступности не дольше
// connectTimeout: на машине разработчика БД обычно уже healthy
// (compose depends_on), вне compose старт может скрещиваться во времени.
func openAndWait(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx/v5", dsn)
	if err != nil {
		return nil, fmt.Errorf("миграции: разбор DSN: %w", err)
	}

	deadline := time.Now().Add(connectTimeout)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, connectRetryInterval)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			redacted := redactDSN(dsn)
			return nil, fmt.Errorf("миграции: PostgreSQL недоступен (%s): %w", redacted, err)
		}
		slog.Info("миграции: PostgreSQL ещё недоступен, повторю попытку", "err", err.Error(), "retry_in", connectRetryInterval.String())
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("миграции: ожидание PostgreSQL прервано: %w", ctx.Err())
		case <-time.After(connectRetryInterval):
		}
	}
}

// redactDSN скрывает пароль в сообщениях об ошибках.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return "см. postgres.dsn (конфигурация) / APP_PG_DSN"
	}
	// url.UserPassword кодирует символы маски, поэтому пароль просто
	// отбрасывается: важно, чтобы он не попал в лог, а не как выглядит замена.
	u.User = url.User(u.User.Username())
	return u.String()
}
