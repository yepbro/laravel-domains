// Package ingest — loading validated lines of input files into the DB
// (stage 1, spec 5.1/3.5).
//
// The staging table ingest_stage (migration 000004) takes the stream of
// lines of one file in chunks of ingest.batch_size (50,000 by default,
// spec 5.1) via COPY: each chunk is a separate transaction, memory is
// bounded by the chunk size, and cancelling the context between chunks
// does not lose completed chunks (spec 3.4/3.5). Applying add/delete to
// domains — tasks T-018/T-019; after applying, the batch is cleared
// (ClearBatch).
//
// Idempotency (spec 3.4, 5.1): a batch is identified by batch_id — the
// creation time plus randomness (T-098). Reloading the same batch
// (restarting an interrupted one) first deletes its previous rows and
// copies the stream anew — data is not corrupted or duplicated, junk
// does not accumulate. Parallel loads (different batch_id) do not
// interfere with each other: clearing targets only its own batch.
package ingest

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/model"
)

// BatchIDLen — the size of the load batch identifier: 16 bytes (128 bits).
const BatchIDLen = 16

// batchTimeLen — the size of the time prefix of the identifier: the high
// 8 bytes, Unix microseconds (T-098).
const batchTimeLen = 8

// compile-time: the time prefix fits into the identifier — otherwise the
// id[:batchTimeLen] slices below would go beyond the array bounds.
var _ [BatchIDLen - batchTimeLen]struct{}

// BatchID — the staging load batch identifier: BatchIDLen bytes — the
// high batchTimeLen are the Unix microseconds of the batch creation time
// (big-endian), the rest are random bytes (crypto/rand, NewBatchID).
// Issued one per file load, it ties together all COPY chunks of this
// load and addresses the batch clearing. The time in the prefix makes
// the byte order (memcmp bytea in PostgreSQL) chronological: the
// background cleanup of orphaned batches (CleanupStaleBatches, T-098)
// cuts the batch_id range by age without a time column — and still by a
// scan over the primary key prefix.
type BatchID [BatchIDLen]byte

// NewBatchID generates a batch identifier with the current creation time.
func NewBatchID() (BatchID, error) {
	return newBatchIDAt(time.Now())
}

// newBatchIDAt — generating a batch identifier with a given creation
// time (time stubbing in tests): the time prefix plus a random tail.
func newBatchIDAt(t time.Time) (BatchID, error) {
	var id BatchID
	binary.BigEndian.PutUint64(id[:batchTimeLen], uint64(t.UnixMicro()))
	if _, err := rand.Read(id[batchTimeLen:]); err != nil {
		return BatchID{}, fmt.Errorf("ingest: генерация идентификатора партии: %w", err)
	}
	return id, nil
}

// batchIDBefore — the "created strictly before moment t" boundary in
// batch_id terms: identifiers of batches created before t are byte-wise
// smaller than the returned one. A zero random tail is the minimum
// value of its microsecond, so batches of microsecond t and later are
// not touched.
func batchIDBefore(t time.Time) BatchID {
	var id BatchID
	binary.BigEndian.PutUint64(id[:batchTimeLen], uint64(t.UnixMicro()))
	return id
}

// Batch — a batch: the load of lines of one input file into staging.
// The lines of a zone directory belong to one zone (spec 5.1: the path
// <zone>/add|delete/*.txt), and the zone is set on the batch; the lines
// of a zone-less file (add|delete/<file>) each carry their zone
// (Row.ZoneID — derived from the name, T-014/validate), the batch
// ZoneID stays zero. The file has a single operation, so the action is
// set on the batch and written into every line: applying (T-018/T-019)
// reads everything it needs from the staging line itself, without side
// reference tables.
type Batch struct {
	ID     BatchID      // the batch identifier (NewBatchID)
	ZoneID int16        // the file zone (zones.id); 0 — a zone-less file
	Action model.Action // 1 add, 2 delete (spec 5.1)
}

// Row — a validated line of an input file (name checking — T-016):
// a domain label without a zone in punycode (spec 5.2; it may be
// composite — "магазин.рф.co.uk" in the co.uk zone gives
// "xn--80aairftm.xn--p1ai", the internal/idn contract) and its
// position in the file. For a zone-less file the line also carries its
// zone (ZoneID — zones.id, the pipeline EnsureZone); for a
// zone-directory file ZoneID is zero — the zone is taken from the
// batch.
type Row struct {
	Label  string
	ZoneID int16 // the row's zones.id; 0 — from Batch.ZoneID
	LineNo int64 // the line number in the source file, 1-based
}

// RowIter — the source of the line stream: the next line or io.EOF at
// the end. Like db.RowIter, it allows reading data lazily (line by
// line from the file) without materializing it entirely — memory is
// determined by the chunk size (spec 3.5).
type RowIter func() (Row, error)

// StageTable / StageColumns — the staging table and the COPY columns in
// the declaration order of migration 000004; the SQL correspondence is
// checked by the TestStageTableContract contract test (the integration
// schema check — T-023).
var (
	StageTable   = pgx.Identifier{"ingest_stage"}
	StageColumns = []string{"batch_id", "zone_id", "label", "action", "line_no"}
)

// MaxLabelLen — the domain label length limit: 63 octets in ASCII form
// (spec 5.2 item 4). Here it is a cheap line invariant: the stream
// validator (T-016) must reject a too long label.
const MaxLabelLen = 63

// clearStageSQL — batch clearing: the leading batch_id of the primary
// key serves a prefix scan. The table name comes from StageTable
// (identifier quoting, as in internal/db), a single source for SQL and
// COPY.
var clearStageSQL = "DELETE FROM " + StageTable.Sanitize() + " WHERE batch_id = $1"

// Stage loads the stream of validated lines into the staging table in
// chunks of batchSize rows (ingest.batch_size, 50,000 by default —
// spec 5.1) via db.CopyFromStream: a chunk is a separate COPY
// transaction, memory is bounded by batchSize (spec 3.5); cancelling
// ctx between chunks interrupts the load without touching completed
// chunks (spec 3.4). Returns the number of lines that made it into
// staging (not counting the lines of a failed or cancelled chunk).
//
// Before copying, the previous rows of this batch are deleted: a repeat
// call with the same Batch.ID rewrites the batch from scratch — data is
// not duplicated or corrupted (the idempotency of a retry, spec 3.4).
// Foreign batches (other batch_id) are not touched.
func Stage(ctx context.Context, pool db.DB, b Batch, batchSize int, next RowIter) (int64, error) {
	if b.ID == (BatchID{}) {
		return 0, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	if b.ZoneID < 0 {
		return 0, fmt.Errorf("ingest: зона партии %d: ожидается идентификатор zones.id (>= 1) либо 0 (зона-less файл)", b.ZoneID)
	}
	if !b.Action.Valid() {
		return 0, fmt.Errorf("ingest: операция партии %d: ожидается 1 add / 2 delete (ТЗ 5.1)", int16(b.Action))
	}
	if batchSize < 1 {
		return 0, fmt.Errorf("ingest: размер пачки %d, минимум 1 (ingest.batch_size)", batchSize)
	}

	// Retry idempotency (spec 3.4): the source of truth is the input
	// file, so an interrupted load is simply reloaded in full, not
	// appended to: we wipe our own previous batch (under-loaded or
	// already applied) and copy the stream anew.
	if _, err := ClearBatch(ctx, pool, b.ID); err != nil {
		return 0, err
	}

	staged, err := db.CopyFromStream(ctx, pool, StageTable, StageColumns, batchSize, b.rowIter(next))
	if err != nil {
		return staged, fmt.Errorf("ingest: загрузка партии в %s: %w", StageTable, err)
	}
	return staged, nil
}

// ClearBatch deletes from staging all rows of the batch and returns
// their number. Called by Stage before loading (rewriting its own
// batch) and by the future apply after completion (T-018/T-019) —
// staging does not grow from file to file.
func ClearBatch(ctx context.Context, pool db.DB, id BatchID) (int64, error) {
	if id == (BatchID{}) {
		return 0, errors.New("ingest: партия без идентификатора (ingest.NewBatchID)")
	}
	tag, err := pool.Exec(ctx, clearStageSQL, id[:])
	if err != nil {
		return 0, fmt.Errorf("ingest: очистка партии в %s: %w", StageTable, err)
	}
	return tag.RowsAffected(), nil
}

// rowIter adapts the typed line stream to db.RowIter: it collects the
// COPY column values (in StageColumns order) and checks cheap
// per-line invariants — full name validation happens before loading
// (T-016). The checks protect staging from programming errors of the
// source: the label is non-empty and non-ASCII (it is a label without
// a zone in punycode, spec 5.2); it may be composite (the internal/idn
// contract — dots are allowed), but each of its parts is non-empty and
// no longer than MaxLabelLen (spec 5.2 item 4); the line number is
// 1-based and strictly grows — the (batch_id, line_no) uniqueness
// within a batch is enforced before the DB. The line zone is
// Row.ZoneID (a zone-less file), falling back to the batch ZoneID (a
// zone-directory file); it must be a zones.id >= 1.
func (b Batch) rowIter(next RowIter) db.RowIter {
	var lastLine int64
	return func() ([]any, error) {
		row, err := next()
		if err != nil {
			return nil, err
		}
		switch {
		case row.Label == "":
			return nil, fmt.Errorf("ingest: строка %d: пустая метка домена", row.LineNo)
		case !isASCII(row.Label):
			return nil, fmt.Errorf("ingest: строка %d: %q — не-ASCII в метке: ожидается punycode (ТЗ 5.2)", row.LineNo, row.Label)
		case !validLabelParts(row.Label):
			return nil, fmt.Errorf("ingest: строка %d: %q — каждая метка без зоны непустая и не длиннее %d октетов (ТЗ 5.2 п. 4)", row.LineNo, row.Label, MaxLabelLen)
		case row.LineNo < 1:
			return nil, fmt.Errorf("ingest: номер строки %d: нумерация с единицы", row.LineNo)
		case row.LineNo <= lastLine:
			return nil, fmt.Errorf("ingest: номер строки %d не растёт (предыдущий %d): строки партии уникальны по line_no", row.LineNo, lastLine)
		}
		zone := row.ZoneID
		if zone == 0 {
			zone = b.ZoneID
		}
		if zone < 1 {
			return nil, fmt.Errorf("ingest: строка %d: зона строки не определена: ожидается zones.id (>= 1) в строке или партии", row.LineNo)
		}
		lastLine = row.LineNo
		return []any{b.ID[:], zone, row.Label, int16(b.Action), row.LineNo}, nil
	}
}

// validLabelParts — a composite label is allowed (the internal/idn
// contract, spec 5.2): each part between dots is a non-empty DNS label
// no longer than MaxLabelLen octets.
func validLabelParts(label string) bool {
	start := 0
	for i := 0; i <= len(label); i++ {
		if i == len(label) || label[i] == '.' {
			if n := i - start; n < 1 || n > MaxLabelLen {
				return false
			}
			start = i + 1
		}
	}
	return true
}

// isASCII — all octets of the label are below 0x80 (the punycode form, spec 5.2).
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7f {
			return false
		}
	}
	return true
}
