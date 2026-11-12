// Accounting of processed files in ingest_files (T-021; spec 5.1, 6.1):
// writing the report (sha256, action, line and apply counters) and
// protection against reprocessing the same file via the unique triple
// (zone_id, action, sha256) — exact redelivery (T-113). DB access goes
// through the small FileStore interface (unit tests run against a fake,
// the ZoneStore style from T-014); the SQL implementation SQLFileStore
// sits on top of internal/db.
//
// Also here — the text log report on file processing (renderReport):
// next to the file in failed/ (spec 5.1: "files with an error (with a
// log report nearby)") and the basis of the service log records. The
// format is fixed by the TestRenderReport test; the rejected log of
// per-line rejections (T-016) remains a separate stream of records.
package ingest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/model"
)

// FileStore — the ingest_files log of processed files (spec 5.1, 6.1)
// required by the ingest pipeline: protection against reprocessing the
// same file and report writing. The interface is deliberately small
// (like ZoneStore, T-014): pipeline unit tests get by with a fake, the
// SQL implementation is replaceable.
type FileStore interface {
	// FilesBySHA256 returns all log rows with the given sha256 (any zone
	// and action, from earliest to latest): the pipeline searches among
	// them for an exact redelivery — a match of (zone_id, action, sha256),
	// the unique key of T-113 — and flags the same content that was
	// delivered differently (another zone, another action). An empty
	// slice — the content was not processed; anything else — a store
	// error.
	FilesBySHA256(ctx context.Context, sha []byte) ([]model.IngestFile, error)

	// RecordFile writes the report of a processed file (the apply
	// counters). The triple (zone_id, action, sha256) is unique
	// (migration 000009): a concurrent report of the same file is a
	// store error.
	RecordFile(ctx context.Context, rec FileRecord) error
}

// FileRecord — an ingest_files row for a processed file (spec 6.1):
// the values of the report columns. The mapping of pipeline counters —
// FileReport.record.
type FileRecord struct {
	ZoneID      int16        // the file zone (zones.id)
	Action      model.Action // 1 add, 2 delete (spec 6.1)
	Path        string       // the path relative to incoming (ParsePath)
	SHA256      []byte       // content hash, 32 octets
	LinesTotal  int64        // total physical lines of the file
	Added       int64        // add: added (including those revived from deleted)
	Existed     int64        // add: already existed
	Deleted     int64        // delete: moved to deleted
	Rejected    int64        // lines rejected (parser + validator)
	ProcessedAt time.Time    // the moment processing finished
}

// ingestFileColumns — the ingest_files columns in model.IngestFile field
// order (the model's db tags, T-008); pgx.RowToStructByName.
const ingestFileColumns = "id, zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected, processed_at"

const (
	// From earliest to latest (ORDER BY id): the classification of
	// deliveries in the pipeline is deterministic, the "first delivery"
	// references are the earliest. Served by ingest_files_sha256_idx
	// (migration 000009).
	sqlFilesBySHA = "SELECT " + ingestFileColumns + " FROM ingest_files WHERE sha256 = $1 ORDER BY id"

	// A plain INSERT, no ON CONFLICT: a concurrent report of the same
	// file (a second service instance) is rejected by the unique triple
	// (zone_id, action, sha256) — the schema knows better whether the
	// file was processed; silently suppressing the conflict would lose
	// the counters of one of the attempts.
	sqlFileRecord = "INSERT INTO ingest_files " +
		"(zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected, processed_at) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
)

// SQLFileStore — FileStore on top of a pgx pool (internal/db). Real
// behavior against a live DB — the ingest integration tests (T-023);
// unit tests verify the SQL text and parameters against a fake pool.
type SQLFileStore struct {
	pool db.DB
}

// NewFileStore — a FileStore on a pgx pool (*pgxpool.Pool and any
// wrappers satisfying db.DB).
func NewFileStore(pool db.DB) *SQLFileStore {
	return &SQLFileStore{pool: pool}
}

// FilesBySHA256 — all log rows with the given content, from earliest to
// latest; an empty slice — the content was not processed.
func (s *SQLFileStore) FilesBySHA256(ctx context.Context, sha []byte) ([]model.IngestFile, error) {
	rows, err := s.pool.Query(ctx, sqlFilesBySHA, sha)
	if err != nil {
		return nil, fmt.Errorf("db: ingest_files: поиск по sha256: %w", err)
	}
	fs, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.IngestFile])
	if err != nil {
		return nil, fmt.Errorf("db: ingest_files: поиск по sha256: %w", err)
	}
	return fs, nil
}

// RecordFile writes the report of a processed file. A zone-less file
// writes zone_id = NULL (a file has no zones — lines do); NULL in the
// unique triple (zone_id, action, sha256) does not conflict
// (migration 000009), the exact-redelivery protection is done by the
// pipeline.
func (s *SQLFileStore) RecordFile(ctx context.Context, rec FileRecord) error {
	if len(rec.SHA256) != sha256.Size {
		return fmt.Errorf("ingest: отчёт файла %s: sha256 — %d октетов, ожидается %d",
			rec.Path, len(rec.SHA256), sha256.Size)
	}
	var zoneID any // nil → NULL: a zone-less file (add|delete/<file>)
	if rec.ZoneID > 0 {
		zoneID = rec.ZoneID
	}
	if _, err := s.pool.Exec(ctx, sqlFileRecord,
		zoneID, int16(rec.Action), rec.Path, rec.SHA256,
		rec.LinesTotal, rec.Added, rec.Existed, rec.Deleted, rec.Rejected,
		rec.ProcessedAt,
	); err != nil {
		return fmt.Errorf("db: ingest_files: отчёт %s: %w", rec.Path, err)
	}
	return nil
}

// FileReport — the outcome of processing one file: line counters
// (parser T-015 + validator T-016) and apply counters (T-018/T-019).
// The values are written to ingest_files (record) and to the log report
// (renderReport); fields without schema columns (Skipped, Accepted,
// Revived, NotFound, Reasons) live only in the log report and the
// service log records.
type FileReport struct {
	// Zone — the punycode zone of the file path (in the DB — ZoneID; for
	// the log report). Empty for a zone-less file (add|delete/<file>: the
	// line zones come from the names) and if the path was not parsed
	// (invalid files also get a log report).
	Zone string
	// ZoneID — the zones.id identifier; 0 — a zone-less file or the path
	// was not parsed (NULL in the log).
	ZoneID int16
	// Action — the file action (1 add, 2 delete); 0 if the path was not parsed.
	Action model.Action
	// Path — the path relative to incoming (ParsePath) — written to
	// ingest_files.path (T-014).
	Path string
	// SHA256 — the content hash of the file; nil if reading did not complete.
	SHA256 []byte

	// Line counters.
	LinesTotal int64 // physical lines of the file (spec 5.1: "the number of lines")
	Skipped    int64 // empty lines and comments — not stored in the DB
	Rejected   int64 // rejected: parser (UTF-8/length) + validator (name/zone)
	Accepted   int64 // valid lines (made it to staging)

	// Reasons — rejections broken down by reason (Reason), for the log report.
	Reasons map[Reason]int

	// Members — archive members processed (T-133); 0 for txt files.
	// There is no schema column — only the log report and the service
	// log records.
	Members int

	// Apply (after ApplyAdd/ApplyDelete; zeros if it never got there).
	Added    int64 // add: Inserted + Revived
	Revived  int64 // add: revived from deleted — part of Added
	Existed  int64 // add: already existed
	Deleted  int64 // delete: moved to deleted
	NotFound int64 // delete: rows without the domain in the DB
}

// record — the ingest_files column values for this outcome (spec 6.1).
// Counter mapping: Revived is part of Added (a domain revived from
// deleted is also the file's "added" result; the schema does not
// distinguish separate entities); NotFound has no column (spec 5.1
// lists added/already existed/deleted/rejected) — it is visible in the
// log report: NotFound = Accepted − Deleted over the lines of a delete
// file.
func (r FileReport) record(at time.Time) FileRecord {
	return FileRecord{
		ZoneID:      r.ZoneID,
		Action:      r.Action,
		Path:        r.Path,
		SHA256:      r.SHA256,
		LinesTotal:  r.LinesTotal,
		Added:       r.Added,
		Existed:     r.Existed,
		Deleted:     r.Deleted,
		Rejected:    r.Rejected,
		ProcessedAt: at,
	}
}

// renderReport — the text log report on file processing (spec 5.1:
// failed/ — "files with an error (with a log report nearby)"; the same
// text is the basis of the service log records). verdict — the outcome
// in one line: applied, a sha256 duplicate — already processed,
// invalid — <reason>.
func renderReport(rep FileReport, verdict string) string {
	var b strings.Builder
	b.WriteString("# лог-отчёт ingest (ТЗ 5.1)\n")
	fmt.Fprintf(&b, "файл: %s\n", rep.Path)
	if rep.Zone != "" {
		fmt.Fprintf(&b, "зона: %s (id %d)\n", rep.Zone, rep.ZoneID)
	} else if rep.Action.Valid() {
		b.WriteString("зона: по именам строк (файл без каталога зоны)\n")
	} else {
		b.WriteString("зона: — (путь не разобран)\n")
	}
	fmt.Fprintf(&b, "действие: %s\n", actionName(rep.Action))
	if len(rep.SHA256) > 0 {
		fmt.Fprintf(&b, "sha256: %x\n", rep.SHA256)
	}
	fmt.Fprintf(&b, "итог: %s\n", verdict)

	fmt.Fprintf(&b, "строк: %d\n", rep.LinesTotal)
	if rep.Members > 0 {
		fmt.Fprintf(&b, "членов архива: %d\n", rep.Members)
	}
	fmt.Fprintf(&b, "  пропущено (пустые и комментарии): %d\n", rep.Skipped)
	fmt.Fprintf(&b, "  отклонено: %d\n", rep.Rejected)
	for _, r := range sortedReasons(rep.Reasons) {
		// string(r), not %s: the report carries the stable reason name
		// (as in the rejected log, T-016), not its text.
		fmt.Fprintf(&b, "    %s: %d\n", string(r), rep.Reasons[r])
	}
	fmt.Fprintf(&b, "  валидных: %d\n", rep.Accepted)

	switch rep.Action {
	case model.ActionAdd:
		fmt.Fprintf(&b, "добавлено: %d (в т.ч. возвращено из deleted: %d)\n", rep.Added, rep.Revived)
		fmt.Fprintf(&b, "уже было: %d\n", rep.Existed)
	case model.ActionDelete:
		fmt.Fprintf(&b, "удалено: %d\n", rep.Deleted)
		fmt.Fprintf(&b, "не найдено в БД: %d\n", rep.NotFound)
	}
	return b.String()
}

// actionName — the file action name; zero value (path not parsed) — a dash.
func actionName(a model.Action) string {
	if a.Valid() {
		return a.String()
	}
	return "—"
}

// sortedReasons — rejection reasons in a deterministic order
// (alphabetical): the log report is reproducible line by line.
func sortedReasons(m map[Reason]int) []Reason {
	out := make([]Reason, 0, len(m))
	for r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// mergeReasons — parser and validator rejections in one map
// (FileReport.Reasons): the parser reasons (invalid_utf8, line_len) and
// the validator ones (wrong_zone, the idn.ErrorKind classes) do not
// overlap by name.
func mergeReasons(parser, validator map[Reason]int) map[Reason]int {
	out := make(map[Reason]int, len(parser)+len(validator))
	for k, n := range parser {
		out[k] += n
	}
	for k, n := range validator {
		out[k] += n
	}
	return out
}
