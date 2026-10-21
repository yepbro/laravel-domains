// Юнит-тесты миграционного раннера без БД: целостность встроенного
// набора миграций (go:embed), разбор набора источником iofs, поведение
// при недоступной БД и скрытие пароля в ошибках.
package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/database"

	"crawler/migrations"
)

// migrationName — формат golang-migrate: <версия>_<имя>.<up|down>.sql.
var migrationName = regexp.MustCompile(`^(\d+)_(.+)\.(up|down)\.sql$`)

// TestEmbeddedFiles_ValidPairs проверяет встроенный набор migrations/:
// имена соответствуют формату golang-migrate, у каждой версии есть пара
// up/down, версии идут с единицы без пропусков, файлы не пустые.
func TestEmbeddedFiles_ValidPairs(t *testing.T) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir встроенного набора: %v", err)
	}

	type pair struct{ up, down bool }
	versions := map[uint]*pair{}
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			t.Errorf("%s: имя не соответствует формату NNN_name.up|down.sql", e.Name())
			continue
		}
		v := parseVersion(t, m[1])
		p := versions[v]
		if p == nil {
			p = &pair{}
			versions[v] = p
		}
		if m[3] == "up" {
			p.up = true
		} else {
			p.down = true
		}

		raw, err := migrations.FS.ReadFile(e.Name())
		if err != nil {
			t.Fatalf("ReadFile %s: %v", e.Name(), err)
		}
		if len(strings.TrimSpace(string(raw))) == 0 {
			t.Errorf("%s: пустой файл миграции", e.Name())
		}
	}

	if len(versions) == 0 {
		t.Fatal("встроенный набор миграций пуст")
	}
	for v := uint(1); v <= uint(len(versions)); v++ {
		p, ok := versions[v]
		if !ok {
			t.Fatalf("версия %d отсутствует: версии должны идти с 1 без пропусков", v)
		}
		if !p.up || !p.down {
			t.Errorf("версия %d: неполная пара (up=%v down=%v)", v, p.up, p.down)
		}
	}
}

// TestSource_IOFS проверяет, что источник iofs читает встроенный набор
// именно так, как его увидит golang-migrate при применении: версии по
// возрастанию, у каждой читаются оба направления.
func TestSource_IOFS(t *testing.T) {
	src, err := newSource()
	if err != nil {
		t.Fatalf("iofs.New: %v", err)
	}

	next, err := src.First()
	if err != nil {
		t.Fatalf("First: %v (набор пуст?)", err)
	}
	seen := map[uint]bool{}
	for err == nil {
		if next == 0 {
			t.Fatal("некорректная версия 0")
		}
		if seen[next] {
			t.Fatalf("версия %d встречается дважды", next)
		}
		seen[next] = true
		next, err = src.Next(next)
	}
	// iofs сообщает об окончании набора ошибкой fs.ErrNotExist, а не io.EOF.
	if err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, io.EOF) {
		t.Fatalf("Next: неожиданная ошибка: %v", err)
	}
	if len(seen) == 0 {
		t.Fatal("источник не отдал ни одной версии")
	}

	for v := range seen {
		for name, read := range map[string]func(uint) (io.ReadCloser, string, error){
			"up":   src.ReadUp,
			"down": src.ReadDown,
		} {
			r, identifier, err := read(v)
			if err != nil {
				t.Fatalf("чтение %s версии %d: %v", name, v, err)
			}
			body, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				t.Fatalf("содержимое %s версии %d: %v", name, v, err)
			}
			if identifier == "" {
				t.Errorf("версия %d: пустой идентификатор %s-миграции", v, name)
			}
			if len(strings.TrimSpace(string(body))) == 0 {
				t.Errorf("версия %d: пустая %s-миграция", v, name)
			}
		}
	}
}

// TestUp_EmptyDSN — раннер отказывается работать без DSN, не трогая сеть.
func TestUp_EmptyDSN(t *testing.T) {
	if err := Up(t.Context(), ""); err == nil {
		t.Fatal("Up с пустым DSN должен вернуть ошибку")
	} else if !strings.Contains(err.Error(), "DSN") {
		t.Errorf("ошибка должна упоминать DSN: %v", err)
	}
}

// TestUp_ConnectTimeout — недоступная БД не подвешивает вызывающего
// дольше настроенного таймаута подключения.
func TestUp_ConnectTimeout(t *testing.T) {
	restore := withConnectTimeout(300 * time.Millisecond)
	defer restore()

	// 127.0.0.1:1 — гарантированно закрытый порт, соединение падает сразу.
	const dsn = "postgres://nobody:nopass@127.0.0.1:1/db?sslmode=disable"
	start := time.Now()
	err := Up(t.Context(), dsn)
	if err == nil {
		t.Fatal("Up к недоступной БД должен вернуть ошибку")
	}
	if !strings.Contains(err.Error(), "недоступен") {
		t.Errorf("ошибка должна упоминать недоступность БД: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Up ждал %s — заметно дольше таймаута подключения", elapsed)
	}
}

// TestOpenAndWait_ClosesDBOnError — на путях ошибки (недоступная БД,
// отмена контекста) *sql.DB закрывается: фейковый драйвер фиксирует
// Close коннектора (database/sql вызывает его при db.Close()).
func TestOpenAndWait_ClosesDBOnError(t *testing.T) {
	restore := withConnectTimeout(300 * time.Millisecond)
	defer restore()
	registerFailDriver()

	prevDriver := sqlDriver
	sqlDriver = "failmigrate"
	defer func() { sqlDriver = prevDriver }()

	_, err := openAndWait(t.Context(), "not-a-real-dsn")
	if err == nil {
		t.Fatal("openAndWait с всегда падающим драйвером должен вернуть ошибку")
	}
	if !failConnectorOf.closed.Load() {
		t.Error("*sql.DB не закрыт на пути ошибки (утечка opener-горутины и соединений)")
	}
}

// failConnector — коннектор, чей Connect всегда падает; Close помечает
// закрытие пула вызывающим кодом, connects считает попытки соединения
// (каждая попытка cancelApplying обращается к БД минимум раз).
type failConnector struct {
	closed   atomic.Bool
	connects atomic.Int64
}

func (c *failConnector) Connect(context.Context) (driver.Conn, error) {
	c.connects.Add(1)
	return nil, errors.New("нет соединения")
}

func (c *failConnector) Driver() driver.Driver { return failDriver{} }

func (c *failConnector) Close() error { c.closed.Store(true); return nil }

type failDriver struct{}

func (failDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("нет соединения")
}

func (failDriver) OpenConnector(string) (driver.Connector, error) { return failConnectorOf, nil }

var (
	failDriverOnce  sync.Once
	failConnectorOf = &failConnector{}
)

func registerFailDriver() {
	failDriverOnce.Do(func() { sql.Register("failmigrate", failDriver{}) })
}

// TestRedactDSN — пароль не должен попадать в сообщения об ошибках.
func TestRedactDSN(t *testing.T) {
	cases := []struct {
		name, dsn, want string
	}{
		{
			"пароль в userinfo",
			"postgres://crawler:secret@172.28.100.2:5432/crawler?sslmode=disable",
			"postgres://crawler@172.28.100.2:5432/crawler?sslmode=disable",
		},
		{
			"без пароля",
			"postgres://crawler@localhost:5432/crawler",
			"postgres://crawler@localhost:5432/crawler",
		},
		{
			"пароль в query-строке (libpq-форма pgx)",
			"postgres://crawler@172.28.100.2:5432/crawler?password=dev-only-crawler&sslmode=disable",
			"postgres://crawler@172.28.100.2:5432/crawler?password=redacted&sslmode=disable",
		},
		{
			"sslpassword в query-строке",
			"postgres://crawler@localhost:5432/crawler?sslpassword=key&sslmode=verify-full",
			"postgres://crawler@localhost:5432/crawler?sslmode=verify-full&sslpassword=redacted",
		},
		{
			"пароль и в userinfo, и в query",
			"postgres://crawler:secret@localhost:5432/crawler?password=dev-only-crawler",
			"postgres://crawler@localhost:5432/crawler?password=redacted",
		},
		{
			"мусор — не раскрываем",
			"not a url at all",
			"см. postgres.dsn (конфигурация) / APP_PG_DSN",
		},
	}
	for _, c := range cases {
		if got := redactDSN(c.dsn); got != c.want {
			t.Errorf("%s: redactDSN = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestWithRunnerTag — метка application_name адресует соединения раннера
// для отмены применяющейся миграции (T-112): URL-DSN получает метку с
// сохранением остальных параметров, не-URL DSN проходит без изменений
// (отмена недоступна — работает поведение C-04 «дорабатывает»).
func TestWithRunnerTag(t *testing.T) {
	const tag = "crawler-migrations-123-1"
	cases := []struct {
		name, dsn, want string
	}{
		{
			"URL с параметрами",
			"postgres://crawler:secret@127.0.0.1:5432/crawler?sslmode=disable",
			"postgres://crawler:secret@127.0.0.1:5432/crawler?application_name=" + tag + "&sslmode=disable",
		},
		{
			"application_name перезаписывается (раннер обязан быть отличим)",
			"postgres://crawler@localhost:5432/crawler?application_name=custom",
			"postgres://crawler@localhost:5432/crawler?application_name=" + tag,
		},
		{
			"URL без параметров",
			"postgres://crawler@localhost/crawler",
			"postgres://crawler@localhost/crawler?application_name=" + tag,
		},
		{
			"keyword-DSN — без метки",
			"host=localhost port=5432 user=crawler",
			"host=localhost port=5432 user=crawler",
		},
		{
			"мусор — без метки",
			"not a url at all",
			"not a url at all",
		},
	}
	for _, c := range cases {
		if got := withRunnerTag(c.dsn, tag); got != c.want {
			t.Errorf("%s: withRunnerTag = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestInterruptedInRun — якорь решения об откате версии (T-112): сброс
// dirty-строки после отмены допустим только для ошибки из тела миграции
// (драйвер pgx помечает её «migration failed») — тогда dirty-строка
// гарантированно выставлена нашим прерванным применением.
func TestInterruptedInRun(t *testing.T) {
	inRun := database.Error{OrigErr: errors.New("57014"), Err: "migration failed"}
	// Форма PgError: драйвер дописывает текст сервера после метки.
	inRunMsg := database.Error{OrigErr: errors.New("57014"), Err: "migration failed: canceling statement due to user request"}
	lockWait := database.Error{OrigErr: errors.New("57014"), Err: "try lock failed"}
	setVersion := database.Error{OrigErr: errors.New("57014")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"тело миграции", inRun, true},
		{"тело миграции с текстом сервера", inRunMsg, true},
		{"тело миграции, обёрнутая", fmt.Errorf("миграции: применение: %w", inRun), true},
		{"ожидание внутренней блокировки", lockWait, false},
		{"SetVersion без метки", setVersion, false},
		{"посторонняя ошибка", errors.New("network"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := interruptedInRun(c.err); got != c.want {
			t.Errorf("%s: interruptedInRun = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestCancelApplying_RetriesUntilBudget — отмена применения не сдаётся
// после первой неудачной попытки: одиночный запрос pg_cancel_backend
// может попасть в зазор между пометкой dirty и началом файла миграции
// (соединение раннера ещё idle — фильтру state='active' не соответствует)
// или не выполниться. Фейковый драйвер считает попытки: после исчерпания
// бюджета cancelApplying возвращает false, попыток больше одной.
func TestCancelApplying_RetriesUntilBudget(t *testing.T) {
	restore := withCancelTimeout(300 * time.Millisecond)
	defer restore()
	registerFailDriver()

	prevDriver := sqlDriver
	sqlDriver = "failmigrate"
	defer func() { sqlDriver = prevDriver }()

	db, err := sql.Open(sqlDriver, "not-a-real-dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	done := make(chan struct{}) // не закрывается: бюджет — единственный выход
	before := failConnectorOf.connects.Load()
	start := time.Now()
	if cancelApplying(db, "crawler-migrations-test", done) {
		t.Fatal("cancelApplying без БД не может отменить запрос")
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("cancelApplying завершился за %s — раньше бюджета отмены", elapsed)
	}
	if attempts := failConnectorOf.connects.Load() - before; attempts < 2 {
		t.Errorf("cancelApplying сдался после первой попытки (попыток соединения: %d), а должен повторять", attempts)
	}
}

// TestCancelApplying_StopsWhenUpDone — отмена прекращается, как только
// завершилось само применение (upDone закрыт): дальше отменять нечего,
// даже если бюджет не исчерпан и попытки не удаются.
func TestCancelApplying_StopsWhenUpDone(t *testing.T) {
	restore := withCancelTimeout(10 * time.Second) // бюджет большой: выход только по done
	defer restore()
	registerFailDriver()

	prevDriver := sqlDriver
	sqlDriver = "failmigrate"
	defer func() { sqlDriver = prevDriver }()

	db, err := sql.Open(sqlDriver, "not-a-real-dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	done := make(chan struct{})
	close(done)
	before := failConnectorOf.connects.Load()
	start := time.Now()
	if cancelApplying(db, "crawler-migrations-test", done) {
		t.Fatal("cancelApplying без БД не может отменить запрос")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("cancelApplying вышел по done за %s — заметно дольше одной попытки", elapsed)
	}
	if attempts := failConnectorOf.connects.Load() - before; attempts != 0 {
		t.Errorf("cancelApplying сделал %d попыток после завершения применения — должен выйти сразу", attempts)
	}
}

func parseVersion(t *testing.T, s string) uint {
	t.Helper()
	var v uint
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("версия %q не число", s)
		}
		v = v*10 + uint(c-'0')
	}
	return v
}

// withConnectTimeout подменяет таймаут подключения для теста
// и возвращает функцию восстановления.
func withConnectTimeout(d time.Duration) func() {
	old := connectTimeout
	connectTimeout = d
	return func() { connectTimeout = old }
}

// withCancelTimeout подменяет бюджет отмены применения для теста
// и возвращает функцию восстановления.
func withCancelTimeout(d time.Duration) func() {
	old := cancelTimeout
	cancelTimeout = d
	return func() { cancelTimeout = old }
}
