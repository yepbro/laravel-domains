// Интеграционные тесты конвейера приёма файлов ingest (T-023; ТЗ 11.2,
// 5.1): путь «файл на диске в incoming → конвейер internal/ingest →
// записи в zones/domains/ingest_files + перемещение файла в
// processed/failed» против живого PostgreSQL с настоящей схемой миграций.
//
// Уровень: сквозной конвейер Pipeline (T-021) с реальными SQL-хранилищами
// (SQLZoneStore, SQLFileStore, COPY в staging, применение add/delete) —
// их поведение против живой БД здесь и проверяется. Наблюдение каталога
// (fsnotify, стабильность файла) — юнит-территория T-013: конвейер
// получает готовый путь, как его выдаёт наблюдатель (cmd/ingest).
//
// Изоляция: каждая проверка накатывает миграции в собственную схему
// (migratedSchema, как тесты схемы) — повторный прогон идемпотентен,
// рабочая схема crawler не затрагивается. Каталог incoming — t.TempDir.
package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/idn"
	"crawler/internal/ingest"
	"crawler/internal/model"
)

// ingestEnv — окружение проверки приёма: схема с миграциями, каталог
// incoming (t.TempDir) и конвейер на реальных SQL-хранилищах — та же
// сборка, что делает cmd/ingest.
type ingestEnv struct {
	pool *pgxpool.Pool
	dir  string // incoming
	pipe *ingest.Pipeline
}

func newIngestEnv(t *testing.T) *ingestEnv {
	t.Helper()

	pool := migratedSchema(t)
	dir := t.TempDir()
	pipe, err := ingest.NewPipeline(
		ingest.NewZoneStore(pool),
		ingest.NewFileStore(pool),
		pool,
		ingest.PipelineOptions{
			Dir: dir,
			// Маленькая пачка COPY: файл из десятков строк грузится
			// несколькими пачками, как большие файлы в проде.
			BatchSize: 4,
		},
	)
	if err != nil {
		t.Fatalf("ingest.NewPipeline: %v", err)
	}
	return &ingestEnv{pool: pool, dir: dir, pipe: pipe}
}

// putFile пишет файл rel (относительно incoming) с содержимым content и
// возвращает абсолютный путь.
func (e *ingestEnv) putFile(t *testing.T, rel, content string) string {
	t.Helper()
	abs := filepath.Join(e.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return abs
}

// process пишет файл и прогоняет его через конвейер. Ошибка возврата —
// только инфраструктурный сбой (файл остался в incoming); плановые итоги
// (applied/duplicate/failed) проверяются по Outcome.
func (e *ingestEnv) process(t *testing.T, rel, content string) ingest.Outcome {
	t.Helper()
	out, err := e.pipe.Process(context.Background(), e.putFile(t, rel, content))
	if err != nil {
		t.Fatalf("Process(%s): %v", rel, err)
	}
	return out
}

// fileContent собирает содержимое файла с завершающим переводом строки.
func fileContent(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

// domainState — наблюдаемые колонки domains.
type domainState struct {
	label      string
	status     int16
	checkState int16
	flags      int16
	queuedAt   *time.Time
}

// domain читает домен по punycode-имени зоны и метке.
func (e *ingestEnv) domain(t *testing.T, zone, label string) (domainState, bool) {
	t.Helper()
	var d domainState
	err := e.pool.QueryRow(context.Background(), `
		SELECT d.label, d.status, d.check_state, d.flags, d.queued_at
		FROM domains AS d JOIN zones AS z ON z.id = d.zone_id
		WHERE z.name = $1 AND d.label = $2`, zone, label,
	).Scan(&d.label, &d.status, &d.checkState, &d.flags, &d.queuedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false
	}
	if err != nil {
		t.Fatalf("чтение домена %s.%s: %v", label, zone, err)
	}
	return d, true
}

// zoneState — наблюдаемые колонки zones.
type zoneState struct {
	id       int16
	name     string
	unicode  string // '' при NULL
	isActive bool
}

// zone читает строку zones по punycode-имени.
func (e *ingestEnv) zone(t *testing.T, name string) (zoneState, bool) {
	t.Helper()
	var z zoneState
	err := e.pool.QueryRow(context.Background(),
		"SELECT id, name, COALESCE(name_unicode, ''), is_active FROM zones WHERE name = $1", name,
	).Scan(&z.id, &z.name, &z.unicode, &z.isActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return z, false
	}
	if err != nil {
		t.Fatalf("чтение зоны %s: %v", name, err)
	}
	return z, true
}

// fileRecordState — колонки отчёта ingest_files. Конвейер пишет их
// всегда (nullable в схеме ТЗ 6.1 — для ручных вставок), NULL в живой
// записи — ошибка проверки.
type fileRecordState struct {
	zoneID     int16
	action     int16
	path       string
	linesTotal int64
	added      int64
	existed    int64
	deleted    int64
	rejected   int64
}

// fileBySHA читает строку журнала ingest_files по хэшу содержимого.
func (e *ingestEnv) fileBySHA(t *testing.T, sha []byte) (fileRecordState, bool) {
	t.Helper()
	var f fileRecordState
	err := e.pool.QueryRow(context.Background(), `
		SELECT zone_id, action, path, lines_total, added, existed, deleted, rejected
		FROM ingest_files WHERE sha256 = $1`, sha,
	).Scan(&f.zoneID, &f.action, &f.path, &f.linesTotal, &f.added, &f.existed, &f.deleted, &f.rejected)
	if errors.Is(err, pgx.ErrNoRows) {
		return f, false
	}
	if err != nil {
		t.Fatalf("чтение ingest_files: %v", err)
	}
	return f, true
}

// countOf — строк в таблице тестовой схемы.
func (e *ingestEnv) countOf(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count(%s): %v", table, err)
	}
	return n
}

// existsOnDisk — путь существует.
func existsOnDisk(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

// TestIngest_AddCreatesZonesAndDomains — сценарий «файл в incoming →
// записи в БД» (ТЗ 11.2, 5.1): add-файл создаёт зону автоматически,
// строки становятся доменами (new/pending, is_idn у IDN-меток), отчёт
// пишется в ingest_files (sha256, счётчики), файл уходит в processed
// с сохранением структуры.
func TestIngest_AddCreatesZonesAndDomains(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	content := fileContent(
		"example.crawl.test", // валидная строка
		"# комментарий",      // пропущена парсером
		"",                   // пустая — тоже
		"сайт.crawl.test",    // IDN: метка в punycode, бит is_idn (ТЗ 5.2 п. 9)
		"shop.crawl.test",
		"example.crawl.test", // повтор строки: валидна, дедуп — на стороне БД
	)
	sha := sha256.Sum256([]byte(content))
	out := env.process(t, "crawl.test/add/list.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("итог = %q, want applied", out.Kind)
	}

	// Зона создана автоматически при первом файле (ТЗ 5.1).
	z, ok := env.zone(t, "crawl.test")
	if !ok {
		t.Fatal("зона crawl.test не создана")
	}
	if !z.isActive {
		t.Error("зона создана неактивной, want is_active = true")
	}
	if z.unicode != "" {
		t.Errorf("name_unicode = %q, want NULL (ASCII-зона, ТЗ 5.2 п. 7)", z.unicode)
	}
	if n := env.countOf(t, "zones"); n != 1 {
		t.Errorf("зон = %d, want 1", n)
	}

	// Три уникальных домена (повтор строки не создаёт дубль), все новые
	// и в ожидании проверки (ТЗ 6.2), без lease.
	if n := env.countOf(t, "domains"); n != 3 {
		t.Errorf("доменов = %d, want 3", n)
	}
	for _, label := range []string{"example", "shop"} {
		d, ok := env.domain(t, "crawl.test", label)
		if !ok {
			t.Fatalf("домен %s.crawl.test не создан", label)
		}
		if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
			t.Errorf("%s: status/check_state = %d/%d, want new/pending", label, d.status, d.checkState)
		}
		if d.queuedAt != nil {
			t.Errorf("%s: queued_at не NULL у нового домена", label)
		}
		if d.flags&int16(model.FlagIDN) != 0 {
			t.Errorf("%s: бит is_idn выставлен у ASCII-метки", label)
		}
	}

	// IDN-домен: метка в punycode (ТЗ 5.2), бит is_idn (ТЗ 5.2 п. 9).
	// Зона пути crawl.test не входит в PSL, поэтому Label результата
	// нормализации составной — метка без зоны пути отрезается вручную
	// (как это делает валидатор T-016).
	res, err := idn.Normalize("сайт.crawl.test")
	if err != nil {
		t.Fatal(err)
	}
	label := strings.TrimSuffix(res.ASCII, ".crawl.test")
	if !strings.HasPrefix(label, "xn--") || strings.Contains(label, ".") {
		t.Fatalf("тестовые данные: метка сайт = %q, ожидается punycode без точек", label)
	}
	d, ok := env.domain(t, "crawl.test", label)
	if !ok {
		t.Fatalf("IDN-домен сайт.crawl.test (%s) не создан", label)
	}
	if d.flags&int16(model.FlagIDN) == 0 {
		t.Errorf("у метки %s нет бита is_idn", label)
	}

	// Отчёт в журнале (ТЗ 6.1): sha256 содержимого и счётчики.
	f, ok := env.fileBySHA(t, sha[:])
	if !ok {
		t.Fatal("отчёт файла не записан в ingest_files")
	}
	want := fileRecordState{
		zoneID:     z.id,
		action:     int16(model.ActionAdd),
		path:       "crawl.test/add/list.txt",
		linesTotal: 6,
		added:      3,
	}
	if f != want {
		t.Errorf("запись ingest_files = %+v, want %+v", f, want)
	}

	// Файл покинул incoming и лёг в processed с сохранением структуры
	// (ТЗ 5.1); партия staging очищена.
	if !existsOnDisk(t, filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/add/list.txt")) {
		t.Error("файл не перемещён в processed/crawl.test/add/list.txt")
	}
	if existsOnDisk(t, filepath.Join(env.dir, "crawl.test/add/list.txt")) {
		t.Error("файл остался в incoming")
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage после обработки = %d, want 0", n)
	}
}

// TestIngest_UnicodeZoneDirectory — каталог зоны в unicode и в punycode —
// одна зона (ТЗ 5.1, 11.4 п. 1): zones.name — punycode, name_unicode —
// unicode-форма для отображения; файлы из обоих каталогов сходятся в
// одну строку zones без дублей.
func TestIngest_UnicodeZoneDirectory(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	out := env.process(t, "москва/add/список.txt", fileContent("сайт.москва", "торг.москва"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("итог = %q, want applied", out.Kind)
	}

	// Зона создана в punycode-форме; xn--80adxhks — устоявшаяся
	// punycode-форма «москва» (независимый якорь против idn.Normalize).
	z, ok := env.zone(t, "xn--80adxhks")
	if !ok {
		t.Fatal("зона москва (xn--80adxhks) не создана")
	}
	if z.name != "xn--80adxhks" || z.unicode != "москва" {
		t.Errorf("зона = %q/%q, want xn--80adxhks/москва", z.name, z.unicode)
	}

	// IDN-домен: метка punycode + бит is_idn; unicode-имя файла роли
	// не играет.
	res, err := idn.Normalize("сайт.москва")
	if err != nil {
		t.Fatal(err)
	}
	d, ok := env.domain(t, "xn--80adxhks", res.Label)
	if !ok {
		t.Fatalf("домен сайт.москва (%s) не создан", res.Label)
	}
	if d.flags&int16(model.FlagIDN) == 0 {
		t.Error("у IDN-метки нет бита is_idn")
	}

	// Файл из punycode-каталога той же зоны — та же строка zones,
	// дублей зон не возникает (ТЗ 11.4 п. 1).
	out = env.process(t, "xn--80adxhks/add/second.txt", fileContent("third.москва"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("итог = %q, want applied", out.Kind)
	}
	if n := env.countOf(t, "zones"); n != 1 {
		t.Errorf("зон = %d, want 1 (unicode/punycode-каталоги — одна зона)", n)
	}
	if n := env.countOf(t, "domains"); n != 3 {
		t.Errorf("доменов = %d, want 3", n)
	}
	if _, ok := env.domain(t, "xn--80adxhks", "third"); !ok {
		t.Error("домен third.москва не создан")
	}
}

// TestIngest_DuplicateFileSkipped — повтор того же файла (тот же sha256)
// уже в журнале: повторная обработка безопасна (ТЗ 5.1) — применение не
// выполняется, новых строк нет, файл уходит в processed без второй
// записи ingest_files.
func TestIngest_DuplicateFileSkipped(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	content := fileContent("example.crawl.test", "shop.crawl.test")
	out := env.process(t, "crawl.test/add/list.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("первая обработка: итог = %q, want applied", out.Kind)
	}

	// Та же копия файла появляется в incoming снова.
	out = env.process(t, "crawl.test/add/list.txt", content)
	if out.Kind != ingest.OutcomeDuplicate {
		t.Fatalf("повторная обработка: итог = %q, want duplicate", out.Kind)
	}
	if n := env.countOf(t, "ingest_files"); n != 1 {
		t.Errorf("записей в ingest_files = %d, want 1 (дубль не отчётится)", n)
	}
	if n := env.countOf(t, "domains"); n != 2 {
		t.Errorf("доменов = %d, want 2 (дубль не применяется)", n)
	}

	// Оба экземпляра файла в архиве: коллизия имён — суффикс -2 (T-021).
	proc := filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/add")
	for _, name := range []string{"list.txt", "list-2.txt"} {
		if !existsOnDisk(t, filepath.Join(proc, name)) {
			t.Errorf("в архиве processed нет %s", name)
		}
	}
}

// TestIngest_ModifiedFileSamePath — изменённый файл по тому же пути
// (другой sha256) с пересечением и новыми доменами: существующие не
// меняются, новые добавляются (ТЗ 5.1: «существующий домен не меняется,
// кроме случая статуса deleted»).
func TestIngest_ModifiedFileSamePath(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/list.txt",
		fileContent("example.crawl.test", "shop.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("первая обработка: итог = %q, want applied", out.Kind)
	}

	// example «прошёл» проверку: статус/состояние сменил не ingest —
	// повторный add не должен их затирать.
	if _, err := env.pool.Exec(context.Background(), `
		UPDATE domains AS d SET status = $1, check_state = $2, queued_at = now()
		FROM zones AS z WHERE z.id = d.zone_id AND z.name = $3 AND d.label = $4`,
		int16(model.StatusAlive), int16(model.CheckStateDone), "crawl.test", "example"); err != nil {
		t.Fatal(err)
	}

	out := env.process(t, "crawl.test/add/list.txt",
		fileContent("example.crawl.test", "blog.crawl.test", "forum.crawl.test"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("вторая обработка: итог = %q, want applied", out.Kind)
	}
	if out.Report.Added != 2 || out.Report.Existed != 1 || out.Report.Revived != 0 {
		t.Errorf("счётчики = added %d, existed %d, revived %d; want 2/1/0",
			out.Report.Added, out.Report.Existed, out.Report.Revived)
	}

	// Существующий домен не тронут: alive/done и lease на месте.
	d, ok := env.domain(t, "crawl.test", "example")
	if !ok {
		t.Fatal("домен example исчез")
	}
	if d.status != int16(model.StatusAlive) || d.checkState != int16(model.CheckStateDone) {
		t.Errorf("example: status/check_state = %d/%d, want alive/done (повторный add не меняет)",
			d.status, d.checkState)
	}
	if d.queuedAt == nil {
		t.Error("queued_at сброшен у существующего домена")
	}

	// Новые — как из первого файла: new/pending.
	for _, label := range []string{"blog", "forum"} {
		d, ok := env.domain(t, "crawl.test", label)
		if !ok {
			t.Fatalf("домен %s не добавлен", label)
		}
		if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
			t.Errorf("%s: status/check_state = %d/%d, want new/pending", label, d.status, d.checkState)
		}
	}
	if n := env.countOf(t, "domains"); n != 4 {
		t.Errorf("доменов = %d, want 4", n)
	}
	if n := env.countOf(t, "ingest_files"); n != 2 {
		t.Errorf("записей в ingest_files = %d, want 2 (по одной на файл)", n)
	}
}

// TestIngest_DeleteMarksDeleted — delete-файл переводит перечисленные
// домены в deleted, домен остаётся в БД (ТЗ 13.1); отсутствующие —
// not_found в отчёте конвейера (колонки в ingest_files нет, ТЗ 6.1).
func TestIngest_DeleteMarksDeleted(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/list.txt",
		fileContent("example.crawl.test", "shop.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}

	content := fileContent("example.crawl.test", "ghost.crawl.test")
	sha := sha256.Sum256([]byte(content))
	out := env.process(t, "crawl.test/delete/x.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}
	if out.Report.Deleted != 1 || out.Report.NotFound != 1 {
		t.Errorf("счётчики delete = deleted %d, not_found %d; want 1/1",
			out.Report.Deleted, out.Report.NotFound)
	}

	// Домен остаётся в БД со статусом deleted (ТЗ 13.1).
	d, ok := env.domain(t, "crawl.test", "example")
	if !ok {
		t.Fatal("удалённый домен исчез из БД (ТЗ 13.1: остаётся)")
	}
	if d.status != int16(model.StatusDeleted) {
		t.Errorf("example: status = %d, want deleted", d.status)
	}

	// Не перечисленные в файле домены не тронуты.
	if d, _ := env.domain(t, "crawl.test", "shop"); d.status != int16(model.StatusNew) {
		t.Errorf("shop: status = %d, want new (в delete-файле не упомянут)", d.status)
	}
	if n := env.countOf(t, "domains"); n != 2 {
		t.Errorf("доменов = %d, want 2 (строки из БД не удаляются)", n)
	}

	// Отчёт delete-файла в журнале: действие delete, счётчик удалённых.
	f, ok := env.fileBySHA(t, sha[:])
	if !ok {
		t.Fatal("отчёт delete-файла не записан")
	}
	if f.action != int16(model.ActionDelete) || f.path != "crawl.test/delete/x.txt" || f.deleted != 1 {
		t.Errorf("запись ingest_files = %+v, want action delete, deleted 1", f)
	}
	if !existsOnDisk(t, filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/delete/x.txt")) {
		t.Error("delete-файл не перемещён в processed")
	}
}

// TestIngest_ReviveDeletedDomain — возврат удалённого домена add-файлом
// (ТЗ 5.1: «кроме случая статуса deleted — он возвращается в
// new/pending»): status → new, check_state → pending, lease сброшен.
func TestIngest_ReviveDeletedDomain(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	if out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("add: итог = %q, want applied", out.Kind)
	}
	// Домен успел уйти в работу до удаления: lease остался у
	// deleted-домена — возврат обязан его сбросить (pending без lease).
	if _, err := env.pool.Exec(context.Background(), `
		UPDATE domains AS d SET check_state = $1, queued_at = now()
		FROM zones AS z WHERE z.id = d.zone_id AND z.name = $2 AND d.label = $3`,
		int16(model.CheckStateQueued), "crawl.test", "example"); err != nil {
		t.Fatal(err)
	}
	if out := env.process(t, "crawl.test/delete/x.txt",
		fileContent("# снятие с проверки", "example.crawl.test")); out.Kind != ingest.OutcomeApplied {
		t.Fatalf("delete: итог = %q, want applied", out.Kind)
	}

	// Другое содержимое — другой sha256 (иначе это был бы дубль файла),
	// домен тот же.
	out := env.process(t, "crawl.test/add/one.txt",
		fileContent("example.crawl.test", "# повторная загрузка"))
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("возврат: итог = %q, want applied", out.Kind)
	}
	if out.Report.Added != 1 || out.Report.Revived != 1 || out.Report.Existed != 0 {
		t.Errorf("счётчики = added %d, revived %d, existed %d; want 1/1/0",
			out.Report.Added, out.Report.Revived, out.Report.Existed)
	}

	d, ok := env.domain(t, "crawl.test", "example")
	if !ok {
		t.Fatal("домен example исчез")
	}
	if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
		t.Errorf("example: status/check_state = %d/%d, want new/pending после возврата",
			d.status, d.checkState)
	}
	if d.queuedAt != nil {
		t.Error("queued_at не сброшен у возвращённого домена")
	}
}

// TestIngest_RejectedLinesWithinThreshold — единичные плохие строки не
// роняют файл (ТЗ 5.1): чужая зона и битый punycode отклонены и видны в
// счётчиках отчёта (rejected-причины), валидные строки применены, файл
// обработан в пределах порога ingest.max_error_ratio.
func TestIngest_RejectedLinesWithinThreshold(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	lines := make([]string, 0, 22)
	for i := 1; i <= 20; i++ {
		lines = append(lines, fmt.Sprintf("ok%02d.crawl.test", i))
	}
	lines = append(lines,
		"bad.example.com",  // чужая зона (wrong_zone, ТЗ 5.2 п. 6)
		"xn--a.crawl.test", // битый punycode (invalid, ТЗ 5.2 п. 3)
	)
	content := fileContent(lines...)
	sha := sha256.Sum256([]byte(content))

	out := env.process(t, "crawl.test/add/dump.txt", content)
	if out.Kind != ingest.OutcomeApplied {
		t.Fatalf("итог = %q, want applied (2 отклонения из 22 ≈ 9%% ниже порога 10%%)", out.Kind)
	}
	if out.Report.Rejected != 2 || out.Report.Accepted != 20 {
		t.Errorf("строки = accepted %d, rejected %d; want 20/2", out.Report.Accepted, out.Report.Rejected)
	}
	if out.Report.Reasons[ingest.ReasonWrongZone] != 1 || out.Report.Reasons[ingest.ReasonInvalid] != 1 {
		t.Errorf("причины отклонений = %v, want wrong_zone 1, invalid 1", out.Report.Reasons)
	}

	// Только валидные строки стали доменами.
	if n := env.countOf(t, "domains"); n != 20 {
		t.Errorf("доменов = %d, want 20", n)
	}
	if _, ok := env.domain(t, "crawl.test", "bad"); ok {
		t.Error("строка чужой зоны попала в domains")
	}

	f, ok := env.fileBySHA(t, sha[:])
	if !ok {
		t.Fatal("отчёт не записан")
	}
	if f.rejected != 2 || f.added != 20 || f.linesTotal != 22 {
		t.Errorf("запись ingest_files = %+v, want rejected 2, added 20, lines 22", f)
	}
	if !existsOnDisk(t, filepath.Join(env.dir, ingest.DirProcessed, "crawl.test/add/dump.txt")) {
		t.Error("файл с единичными отказами ушёл не в processed")
	}
}

// TestIngest_ErrorRatioFailsFile — доля отказов выше
// ingest.max_error_ratio (ТЗ 5.1, T-016): файл ошибочен целиком — ни одна
// строка не применяется (включая валидные), записи в ingest_files нет,
// файл в failed/ с лог-отчётом рядом.
func TestIngest_ErrorRatioFailsFile(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	// 4 отклонения из 5 содержательных строк = 80% > 10%.
	content := fileContent(
		"ok.crawl.test", // валидная — но файл ошибочен целиком
		"a.example.com", // чужая зона
		"b.example.com",
		"c.example.com",
		"xn--a.crawl.test", // битый punycode
	)
	out := env.process(t, "crawl.test/add/dump.txt", content)
	if out.Kind != ingest.OutcomeFailed {
		t.Fatalf("итог = %q, want failed (доля отказов выше порога)", out.Kind)
	}

	// Ни доменов, ни отчёта: партия отброшена, строка журнала не пишется
	// (T-021: тот же хэш не занимает журнал, исправленный файл можно
	// загрузить снова).
	if n := env.countOf(t, "domains"); n != 0 {
		t.Errorf("доменов = %d, want 0 (файл ошибочен целиком)", n)
	}
	if n := env.countOf(t, "ingest_files"); n != 0 {
		t.Errorf("записей в ingest_files = %d, want 0", n)
	}
	if n := env.countOf(t, "ingest_stage"); n != 0 {
		t.Errorf("строк в ingest_stage = %d, want 0 (партия отброшена)", n)
	}

	// Файл в failed/ с лог-отчётом рядом (ТЗ 5.1).
	failed := filepath.Join(env.dir, ingest.DirFailed, "crawl.test/add/dump.txt")
	if !existsOnDisk(t, failed) {
		t.Fatal("файл не перемещён в failed/crawl.test/add/dump.txt")
	}
	report, err := os.ReadFile(failed + ".report")
	if err != nil {
		t.Fatalf("лог-отчёт рядом с файлом: %v", err)
	}
	if !strings.Contains(string(report), "crawl.test/add/dump.txt") {
		t.Errorf("лог-отчёт не называет файл:\n%s", report)
	}
	for _, reason := range []string{"wrong_zone: 3", "invalid: 1"} {
		if !strings.Contains(string(report), reason) {
			t.Errorf("лог-отчёт без счётчика %q:\n%s", reason, report)
		}
	}
	if existsOnDisk(t, filepath.Join(env.dir, "crawl.test/add/dump.txt")) {
		t.Error("файл остался в incoming")
	}
}
