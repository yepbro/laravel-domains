// Unit tests of the migration runner without a DB: the integrity of the
// embedded migration set (go:embed), the iofs source reading of the set,
// the behavior with an unavailable DB and the password hiding in the
// errors.
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

// migrationName — the golang-migrate format: <version>_<name>.<up|down>.sql.
var migrationName = regexp.MustCompile(`^(\d+)_(.+)\.(up|down)\.sql$`)

// TestEmbeddedFiles_ValidPairs checks the embedded migrations/ set: the
// names follow the golang-migrate format, every version has an up/down
// pair, the versions start at 1 without gaps, the files are not empty.
func TestEmbeddedFiles_ValidPairs(t *testing.T) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir of the embedded set: %v", err)
	}

	type pair struct{ up, down bool }
	versions := map[uint]*pair{}
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			t.Errorf("%s: the name does not match the NNN_name.up|down.sql format", e.Name())
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
			t.Errorf("%s: an empty migration file", e.Name())
		}
	}

	if len(versions) == 0 {
		t.Fatal("the embedded migration set is empty")
	}
	for v := uint(1); v <= uint(len(versions)); v++ {
		p, ok := versions[v]
		if !ok {
			t.Fatalf("version %d is missing: versions must run from 1 without gaps", v)
		}
		if !p.up || !p.down {
			t.Errorf("version %d: an incomplete pair (up=%v down=%v)", v, p.up, p.down)
		}
	}
}

// TestSource_IOFS checks that the iofs source reads the embedded set
// exactly as golang-migrate will see it when applying: the versions in
// ascending order, both directions readable for each.
func TestSource_IOFS(t *testing.T) {
	src, err := newSource()
	if err != nil {
		t.Fatalf("iofs.New: %v", err)
	}

	next, err := src.First()
	if err != nil {
		t.Fatalf("First: %v (is the set empty?)", err)
	}
	seen := map[uint]bool{}
	for err == nil {
		if next == 0 {
			t.Fatal("an invalid version 0")
		}
		if seen[next] {
			t.Fatalf("version %d occurs twice", next)
		}
		seen[next] = true
		next, err = src.Next(next)
	}
	// iofs reports the end of the set with an fs.ErrNotExist error, not
	// io.EOF.
	if err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, io.EOF) {
		t.Fatalf("Next: an unexpected error: %v", err)
	}
	if len(seen) == 0 {
		t.Fatal("the source returned no versions")
	}

	for v := range seen {
		for name, read := range map[string]func(uint) (io.ReadCloser, string, error){
			"up":   src.ReadUp,
			"down": src.ReadDown,
		} {
			r, identifier, err := read(v)
			if err != nil {
				t.Fatalf("reading %s of version %d: %v", name, v, err)
			}
			body, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				t.Fatalf("the body of %s of version %d: %v", name, v, err)
			}
			if identifier == "" {
				t.Errorf("version %d: an empty %s migration identifier", v, name)
			}
			if len(strings.TrimSpace(string(body))) == 0 {
				t.Errorf("version %d: an empty %s migration", v, name)
			}
		}
	}
}

// TestUp_EmptyDSN — the runner refuses to work without a DSN, without
// touching the network.
func TestUp_EmptyDSN(t *testing.T) {
	if err := Up(t.Context(), ""); err == nil {
		t.Fatal("Up with an empty DSN must return an error")
	} else if !strings.Contains(err.Error(), "DSN") {
		t.Errorf("the error must mention DSN: %v", err)
	}
}

// TestUp_ConnectTimeout — an unavailable DB does not hang the caller
// longer than the configured connection timeout.
func TestUp_ConnectTimeout(t *testing.T) {
	restore := withConnectTimeout(300 * time.Millisecond)
	defer restore()

	// 127.0.0.1:1 — a guaranteed closed port; the connection fails right
	// away.
	const dsn = "postgres://nobody:nopass@127.0.0.1:1/db?sslmode=disable"
	start := time.Now()
	err := Up(t.Context(), dsn)
	if err == nil {
		t.Fatal("Up against an unavailable database must return an error")
	}
	if !strings.Contains(err.Error(), "unavailable") {
		t.Errorf("the error must mention the database unavailability: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Up waited %s — noticeably longer than the connect timeout", elapsed)
	}
}

// TestOpenAndWait_ClosesDBOnError — on the error paths (an unavailable
// DB, a context cancellation) the *sql.DB is closed: the fake driver
// records the connector Close (database/sql calls it at db.Close()).
func TestOpenAndWait_ClosesDBOnError(t *testing.T) {
	restore := withConnectTimeout(300 * time.Millisecond)
	defer restore()
	registerFailDriver()

	prevDriver := sqlDriver
	sqlDriver = "failmigrate"
	defer func() { sqlDriver = prevDriver }()

	_, err := openAndWait(t.Context(), "not-a-real-dsn")
	if err == nil {
		t.Fatal("openAndWait with an always-failing driver must return an error")
	}
	if !failConnectorOf.closed.Load() {
		t.Error("*sql.DB is not closed on the error path (an opener goroutine and connections leak)")
	}
}

// failConnector — a connector whose Connect always fails; Close marks the
// pool closing by the calling code, connects counts the connection
// attempts (every cancelApplying attempt touches the DB at least once).
type failConnector struct {
	closed   atomic.Bool
	connects atomic.Int64
}

func (c *failConnector) Connect(context.Context) (driver.Conn, error) {
	c.connects.Add(1)
	return nil, errors.New("no connection")
}

func (c *failConnector) Driver() driver.Driver { return failDriver{} }

func (c *failConnector) Close() error { c.closed.Store(true); return nil }

type failDriver struct{}

func (failDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("no connection")
}

func (failDriver) OpenConnector(string) (driver.Connector, error) { return failConnectorOf, nil }

var (
	failDriverOnce  sync.Once
	failConnectorOf = &failConnector{}
)

func registerFailDriver() {
	failDriverOnce.Do(func() { sql.Register("failmigrate", failDriver{}) })
}

// TestRedactDSN — the password must not get into the error messages.
func TestRedactDSN(t *testing.T) {
	cases := []struct {
		name, dsn, want string
	}{
		{
			"a password in userinfo",
			"postgres://crawler:secret@172.28.100.2:5432/crawler?sslmode=disable",
			"postgres://crawler@172.28.100.2:5432/crawler?sslmode=disable",
		},
		{
			"no password",
			"postgres://crawler@localhost:5432/crawler",
			"postgres://crawler@localhost:5432/crawler",
		},
		{
			"a password in the query string (the pgx libpq form)",
			"postgres://crawler@172.28.100.2:5432/crawler?password=dev-only-crawler&sslmode=disable",
			"postgres://crawler@172.28.100.2:5432/crawler?password=redacted&sslmode=disable",
		},
		{
			"sslpassword in the query string",
			"postgres://crawler@localhost:5432/crawler?sslpassword=key&sslmode=verify-full",
			"postgres://crawler@localhost:5432/crawler?sslmode=verify-full&sslpassword=redacted",
		},
		{
			"a password in both userinfo and query",
			"postgres://crawler:secret@localhost:5432/crawler?password=dev-only-crawler",
			"postgres://crawler@localhost:5432/crawler?password=redacted",
		},
		{
			"junk — not revealed",
			"not a url at all",
			"see postgres.dsn (configuration) / APP_PG_DSN",
		},
	}
	for _, c := range cases {
		if got := redactDSN(c.dsn); got != c.want {
			t.Errorf("%s: redactDSN = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestWithRunnerTag — the application_name label addresses the runner's
// connections for cancelling an in-flight migration (T-112): a URL DSN
// gets the label with the other parameters preserved; a non-URL DSN
// passes unchanged (no cancellation available — the C-04 "runs to the
// end" behavior works).
func TestWithRunnerTag(t *testing.T) {
	const tag = "crawler-migrations-123-1"
	cases := []struct {
		name, dsn, want string
	}{
		{
			"a URL with parameters",
			"postgres://crawler:secret@127.0.0.1:5432/crawler?sslmode=disable",
			"postgres://crawler:secret@127.0.0.1:5432/crawler?application_name=" + tag + "&sslmode=disable",
		},
		{
			"application_name is overwritten (the runner must be distinguishable)",
			"postgres://crawler@localhost:5432/crawler?application_name=custom",
			"postgres://crawler@localhost:5432/crawler?application_name=" + tag,
		},
		{
			"a URL without parameters",
			"postgres://crawler@localhost/crawler",
			"postgres://crawler@localhost/crawler?application_name=" + tag,
		},
		{
			"a keyword DSN — no tag",
			"host=localhost port=5432 user=crawler",
			"host=localhost port=5432 user=crawler",
		},
		{
			"junk — no tag",
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

// TestInterruptedInRun — the anchor of the version rollback decision
// (T-112): resetting the dirty row after the cancellation is allowed only
// for an error from the migration body (the pgx driver marks it "migration
// failed") — then the dirty row is guaranteed to have been set by our
// interrupted application.
func TestInterruptedInRun(t *testing.T) {
	inRun := database.Error{OrigErr: errors.New("57014"), Err: "migration failed"}
	// The PgError form: the driver appends the server text after the label.
	inRunMsg := database.Error{OrigErr: errors.New("57014"), Err: "migration failed: canceling statement due to user request"}
	lockWait := database.Error{OrigErr: errors.New("57014"), Err: "try lock failed"}
	setVersion := database.Error{OrigErr: errors.New("57014")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"migration body", inRun, true},
		{"migration body with the server text", inRunMsg, true},
		{"migration body, wrapped", fmt.Errorf("migrate: apply: %w", inRun), true},
		{"an internal lock wait", lockWait, false},
		{"SetVersion without the marker", setVersion, false},
		{"a foreign error", errors.New("network"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := interruptedInRun(c.err); got != c.want {
			t.Errorf("%s: interruptedInRun = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestCancelApplying_RetriesUntilBudget — the application cancellation
// does not give up after the first failed attempt: a single
// pg_cancel_backend query can land in the gap between marking dirty and
// the start of the migration file (the runner's connection is still idle
// — it does not match the state='active' filter) or fail to execute. The
// fake driver counts the attempts: after the budget is exhausted
// cancelApplying returns false; there is more than one attempt.
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

	done := make(chan struct{}) // never closed: the budget is the only exit
	before := failConnectorOf.connects.Load()
	start := time.Now()
	if cancelApplying(db, "crawler-migrations-test", done) {
		t.Fatal("cancelApplying without a database cannot cancel a query")
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("cancelApplying finished in %s — earlier than the cancel budget", elapsed)
	}
	if attempts := failConnectorOf.connects.Load() - before; attempts < 2 {
		t.Errorf("cancelApplying gave up after the first attempt (%d connection attempts), it must retry", attempts)
	}
}

// TestCancelApplying_StopsWhenUpDone — the cancellation stops as soon as
// the application itself finished (upDone closed): there is nothing left
// to cancel, even if the budget is not exhausted and the attempts fail.
func TestCancelApplying_StopsWhenUpDone(t *testing.T) {
	restore := withCancelTimeout(10 * time.Second) // a large budget: the exit only by done
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
		t.Fatal("cancelApplying without a database cannot cancel a query")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("cancelApplying exited on done in %s — noticeably longer than one attempt", elapsed)
	}
	if attempts := failConnectorOf.connects.Load() - before; attempts != 0 {
		t.Errorf("cancelApplying made %d attempts after the apply finished — it must exit at once", attempts)
	}
}

func parseVersion(t *testing.T, s string) uint {
	t.Helper()
	var v uint
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("version %q is not a number", s)
		}
		v = v*10 + uint(c-'0')
	}
	return v
}

// withConnectTimeout substitutes the connection timeout for the test
// and returns the restore function.
func withConnectTimeout(d time.Duration) func() {
	old := connectTimeout
	connectTimeout = d
	return func() { connectTimeout = old }
}

// withCancelTimeout substitutes the application cancellation budget for
// the test and returns the restore function.
func withCancelTimeout(d time.Duration) func() {
	old := cancelTimeout
	cancelTimeout = d
	return func() { cancelTimeout = old }
}
