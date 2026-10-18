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
//   - сигнал завершения (отмена контекста) прерывает и применение:
//     golang-migrate Up не принимает контекст, поэтому раннер сам снимает
//     выполняющийся SQL запросом pg_cancel_backend (соединения раннера
//     помечены уникальным application_name), недоведённый файл миграции
//     откатывается PostgreSQL целиком, а версия схемы возвращается к
//     последней применённой — повторный старт применяет её заново (T-112);
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
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"crawler/internal/db"
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
	// cancelTimeout — бюджет на снятие применяющейся миграции по сигналу:
	// pg_cancel_backend по собственным соединениям раннера через
	// pg_stat_activity. Не уложились — отмена не состоялась, миграция
	// дорабатывает до конца (поведение C-04).
	cancelTimeout = 5 * time.Second
)

// connectRetryInterval — пауза между попытками подключиться.
const connectRetryInterval = time.Second

// sqlDriver — имя database/sql-драйвера для подключений раннера
// (переменная для подмены в тестах путей ошибки).
var sqlDriver = "pgx/v5"

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

// ErrInterrupted возвращается, когда применение прервано отменой контекста
// (сигналом завершения): выполнявшийся SQL снят на стороне PostgreSQL,
// недоведённый файл миграции откатился целиком, версия схемы возвращена к
// последней применённой. Повторный старт применит прерванную миграцию
// заново — это штатная быстрая остановка, а не сбой.
var ErrInterrupted = errors.New("миграции: применение прервано сигналом завершения")

// runnerAppPrefix — префикс application_name соединений раннера;
// tagSeq делает метку уникальной на вызов Up: отмена миграции по сигналу
// адресует строго собственные соединения и не трогает применяющую
// миграцию другого экземпляра при параллельном старте сервисов.
var (
	runnerAppPrefix = "crawler-migrations"
	tagSeq          atomic.Uint64
)

// Up применяет все встроенные миграции (up) до актуальной версии.
// Блокирует вызывающего до завершения (включая ожидание БД и блокировки).
// Отмена ctx прерывает не только фазы ожидания, но и применение:
// golang-migrate Up контекст не принимает, поэтому раннер снимает
// выполняющийся SQL на стороне PostgreSQL (см. cancelApplying), после
// чего возвращает ErrInterrupted — повторный старт доводит применение.
func Up(ctx context.Context, dsn string) error {
	if dsn == "" {
		return errors.New("миграции: пустой DSN PostgreSQL")
	}

	// Метка соединений: по ней pg_cancel_backend адресует только этот
	// вызов Up. Не-URL DSN метку не получает — отмена применения
	// недоступна, остаётся поведение C-04 (миграция дорабатывает).
	tag := fmt.Sprintf("%s-%d-%d", runnerAppPrefix, os.Getpid(), tagSeq.Add(1))

	db, err := openAndWait(ctx, withRunnerTag(dsn, tag))
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
		// Драйвер уже держит выделенное соединение — закрываем, чтобы
		// не протекать и на этом пути.
		if cerr := driver.Close(); cerr != nil {
			slog.Warn("миграции: закрытие драйвера", "err", cerr)
		}
		return fmt.Errorf("миграции: инициализация: %w", err)
	}
	// m.Close закрывает источник и драйвер: выделенное драйвером
	// соединение возвращается пулу немедленно, а не живёт без владельца
	// до сборщика мусора (db.Close закрывает только простаивающие
	// соединения, in-use остаётся висеть).
	defer func() {
		if _, cerr := m.Close(); cerr != nil {
			slog.Warn("миграции: закрытие раннера", "err", cerr)
		}
	}()
	m.LockTimeout = lockTimeout

	// Фазы подключения и advisory-блокировки выше прерываемы самим ctx;
	// инициализация драйвера и m.Up — нет (golang-migrate не принимает
	// контекст). Наблюдатель по отмене ctx снимает выполняющийся SQL
	// запросом pg_cancel_backend — это прерывает и ожидание внутренней
	// блокировки golang-migrate, и применяющуюся миграцию.
	upDone := make(chan struct{})
	watchDone := make(chan struct{})
	var cancelled atomic.Bool
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			cancelled.Store(cancelApplying(db, tag))
		case <-upDone:
		}
	}()

	err = m.Up()
	close(upDone)
	<-watchDone // отмена завершилась до разбора результата — без гонки с флагом

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
	case cancelled.Load():
		return interrupted(driver, m, err)
	case errors.As(err, new(migrate.ErrDirty)):
		return ErrDirty
	default:
		return fmt.Errorf("миграции: применение: %w", err)
	}
}

// interrupted оформляет результат прерванного отменой применения.
//
// Если отменено само тело миграции (драйвер помечает такую ошибку
// «migration failed»), dirty-строка версии гарантированно принадлежит
// этому прерванному применению: PostgreSQL выполняет файл миграции одним
// запросом простого протокола — все инструкции в одной неявной транзакции,
// отмена откатывает файл целиком. Раннер возвращает версию к предыдущей
// применённой (семантика `migrate force <v−1>`), и повторный старт
// применяет миграцию заново, а не упирается в dirty.
//
// Отмена в прочих фазах (ожидание внутренней блокировки golang-migrate,
// инициализация драйвера) состояние схемы не меняла — сброс не нужен.
func interrupted(driver database.Driver, m *migrate.Migrate, err error) error {
	if !interruptedInRun(err) {
		return fmt.Errorf("%w: применение остановлено до тела миграции, состояние схемы не менялось: %w", ErrInterrupted, err)
	}

	version, dirty, verr := m.Version()
	if verr != nil || !dirty || version == 0 {
		return fmt.Errorf("%w: состояние версии не удалось подтвердить: %w", ErrInterrupted, err)
	}

	prev := int(version) - 1
	if prev == 0 {
		prev = database.NilVersion // миграция была первой: версия — пустая таблица
	}
	if serr := driver.SetVersion(prev, false); serr != nil {
		// Схема откатана (неявная транзакция), но строка версии осталась
		// dirty: следующий старт упрётся в ErrDirty — направление
		// восстановления то же, что и для любой прерванной миграции.
		return fmt.Errorf("%w: миграция %d отменена и откатана, но версия не восстановлена (осталась dirty): %w; откат версии: %v",
			ErrInterrupted, version, err, serr)
	}
	slog.Warn("миграции: применение прервано сигналом, миграция откатана, схема на предыдущей версии",
		"interrupted_version", version, "rolled_back_to", prev)
	return fmt.Errorf("%w: миграция %d отменена и откатана целиком, схема на версии %d, повторный старт применит её заново: %w",
		ErrInterrupted, version, prev, err)
}

// interruptedInRun — ошибка Up из тела миграции: драйвер pgx помечает
// сбой выполнения SQL меткой «migration failed» (для PgError — с текстом
// сервера после метки, например «migration failed: canceling statement…»).
// Только такая ошибка после нашей отмены означает, что dirty-строка
// версии выставлена этим применением (SetVersion(v, true) — до выполнения
// файла, снимается после).
func interruptedInRun(err error) bool {
	var dbErr database.Error
	return errors.As(err, &dbErr) && strings.HasPrefix(dbErr.Err, "migration failed")
}

// cancelApplying снимает выполняющиеся SQL-запросы соединений этого
// вызова Up (метка tag в pg_stat_activity): так прерывается применяющаяся
// миграция — golang-migrate контекст не принимает, но PostgreSQL отменяет
// запрос pg_cancel_backend, и файл миграции откатывается целиком.
// Фильтр state = 'active' щадит простаивающие соединения раннера
// (advisory-блокировка), а уникальная метка — применяющую миграцию
// другого экземпляра и соединения сервисов. Возвращает true, только если
// хотя бы один запрос реально отменён.
func cancelApplying(db *sql.DB, tag string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
	defer cancel()
	const query = `
		SELECT count(*) FILTER (WHERE pg_cancel_backend(pid))
		FROM pg_stat_activity
		WHERE application_name = $1
		  AND state = 'active'
		  AND pid <> pg_backend_pid()`
	var n int
	if err := db.QueryRowContext(ctx, query, tag).Scan(&n); err != nil {
		slog.Warn("миграции: снять применяющуюся миграцию не удалось — доработает до конца", "err", err)
		return false
	}
	return n > 0
}

// withRunnerTag добавляет в URL-DSN метку application_name: соединения
// раннера видны в pg_stat_activity и адресуемы для отмены при сигнале
// завершения. Не-URL DSN возвращается без изменений — метки нет, отмена
// применения недоступна (миграция дорабатывает, поведение C-04).
func withRunnerTag(dsn, tag string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" || u.Opaque != "" {
		return dsn
	}
	q := u.Query()
	q.Set("application_name", tag)
	u.RawQuery = q.Encode()
	return u.String()
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
	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("миграции: разбор DSN: %w", err)
	}
	// На путях ошибки (таймаут, отмена) *sql.DB закрывается: иначе
	// остаются фоновая opener-горутина и успевшие открыться соединения.
	connected := false
	defer func() {
		if !connected {
			_ = db.Close()
		}
	}()

	deadline := time.Now().Add(connectTimeout)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, connectRetryInterval)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			connected = true
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

// redactDSN скрывает секреты DSN в сообщениях об ошибках (db.RedactDSN —
// единая реализация с internal/db; fallback упоминает и env-источник DSN
// раннера — APP_PG_DSN).
func redactDSN(dsn string) string {
	return db.RedactDSN(dsn, "см. postgres.dsn (конфигурация) / APP_PG_DSN")
}
