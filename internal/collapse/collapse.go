// Package collapse — moving the data of 2LD zones into TLD zones
// (the tld-only-zones spec §7, T-196): the zone of the system is the last
// label of the name (internal/idn, T-194), the reference table of zones is
// collapsed to the TLD by moving rows: `zone_id` → TLD, `label` →
// `label || '.' || prefix`. The move is a console maintenance command for
// the downtime window (crawlerctl zones collapse, a precedent of zones
// sync: a CLI, not a migration — the heavy row move goes in batches with
// a report, preconditions and dry-run, the spec §12.4); the logic is moved
// into a package so integration tests can run it against a live schema.
//
// Properties (spec §7.1):
//   - idempotency and resumability: only the rows still sitting in the
//     2LD zone are moved (batches address zone_id = <2LD>); a rerun
//     continues from the stop point, no 2LD zones — a no-op;
//   - batches §7.3: windows of DefaultBatch ids from the temp table
//     moving_ids (a single-connection session), each batch is a separate
//     transaction (domains and their children move atomically per batch);
//     a UNIQUE (zone_id, label) violation within a batch is impossible
//     (§12.5); an anomalous conflict rolls the batch back and stops the
//     command with the zone name;
//   - the allowlist ingest.allowed_zone_paths — excluded zones (testbed
//     test zones live as zones, §7.1);
//   - precondition: no domains with check_state = 1 in the affected zones;
//   - the move is irreversible (§12.5): rollback is a backup restore.
package collapse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
	"crawler/internal/monitor"
)

// DefaultBatch — the batch window of the move (§7.3): 50000 ids, a
// constant, not a config; Options.Batch lets the integration test shrink
// the window (a resumption check after a failure in the middle).
const DefaultBatch = 50000

// ---------------------------------------------------------------- SQL
//
// The constants are contracts: unit tests compare the texts and the
// parameters (the style of cmd/crawlerctl zonessync.go); table/column
// names belong to the schema migrations.

// zonePartsFrom — the shared part of selecting the zone partitions from
// the catalog: the direct LIST children of the root tables by the
// "FOR VALUES IN (<zone_id>)" boundary (the technique of migration 000003
// create_zone_partitions; two spelling variants — pg_get_expr prints the
// smallint boundary quoted, %L gives exactly that, %s is a safety net
// against a format change). For subdomain_results/laravel_* the selected
// child is the intermediate parent of the hash leaves: a DROP TABLE of
// the parent takes the leaves with it (§7.2 step 6).
const zonePartsFrom = "FROM pg_inherits i " +
	"JOIN pg_class c ON c.oid = i.inhrelid " +
	"JOIN pg_class p ON p.oid = i.inhparent " +
	"JOIN pg_namespace n ON n.oid = p.relnamespace " +
	"WHERE n.nspname = current_schema() " +
	"AND p.relname IN ('domains','subdomain_results','laravel_domains','laravel_findings') "

// InventorySQL — an inventory of 2LD zones outside the allowlist (§7.2):
// LIKE '%.%' is a compound name; name <> ALL($1) is the allowlist
// subtraction (NOT IN without NULL traps). The scalar subqueries are the
// report columns of §7.4 (domains_now prescribed by the spec; the counts
// of the child zones and the partition — for the plan), all of them are
// point reads by the zone_id PK prefix. domains_total is the monitor
// cache (T-178) before the move, cross-checked against the live
// domains_now.
const InventorySQL = "SELECT z.id, z.name, z.domains_total, " +
	"(SELECT count(*) FROM domains d WHERE d.zone_id = z.id) AS domains_now, " +
	"(SELECT count(*) FROM subdomain_results r WHERE r.zone_id = z.id), " +
	"(SELECT count(*) FROM laravel_findings f WHERE f.zone_id = z.id), " +
	"(SELECT count(*) FROM laravel_history h WHERE h.zone_id = z.id), " +
	"(SELECT count(*) FROM ingest_files i WHERE i.zone_id = z.id), " +
	"EXISTS (SELECT 1 " + zonePartsFrom +
	"AND pg_get_expr(c.relpartbound, c.oid) IN (" +
	"format('FOR VALUES IN (%s)', z.id), format('FOR VALUES IN (%L)', z.id::text))) " +
	"FROM zones z WHERE z.name LIKE '%.%' AND z.name <> ALL($1) ORDER BY z.name"

// QueuedSQL — the §7.1 precondition: domains in the queue (check_state = 1)
// over the affected zones; the state code is a parameter (internal/model).
const QueuedSQL = "SELECT zone_id, count(*) FROM domains " +
	"WHERE zone_id = ANY($1::int2[]) AND check_state = $2 GROUP BY zone_id"

// ZoneIDByNameSQL / ZoneInsertSQL — step 1: the TLD zone is found by name,
// a missing one is created active (the style of zones sync: the unicode
// form only for XN zones, ASCII — NULL via nullif).
const ZoneIDByNameSQL = "SELECT id FROM zones WHERE name = $1"

const ZoneInsertSQL = "INSERT INTO zones (name, name_unicode, is_active) " +
	"VALUES ($1, nullif($2, ''), true) RETURNING id"

// CallPartitionsSQL — step 2: dedicated partitions of the TLD zone before
// the move, so the rows land right in them; idempotent (migration 000003
// skips the existing ones with a NOTICE).
const CallPartitionsSQL = "CALL create_zone_partitions($1)"

// Step 3 — the moving_ids temp table for the session of the connection:
// the frozen set of ids being moved (the batch windows of §7.3 address its
// bounds). Before creating it — DROP IF EXISTS: a rerun of the zone
// (resumption) rebuilds the table from the remaining rows in the 2LD zone.
const (
	movingDropSQL   = "DROP TABLE IF EXISTS moving_ids"
	movingCreateSQL = "CREATE TEMP TABLE moving_ids AS SELECT id FROM domains WHERE zone_id = $1"
	movingIndexSQL  = "CREATE INDEX ON moving_ids (id)"
	// movingChunkSQL — the next chunk of live move ids (a keyset by id):
	// a batch of §7.3 is a chunk of moving_ids rows, not a window of the
	// global id space (the zone domains are sparse over the whole
	// domain_id_seq — a 50k id window is almost empty; production zones
	// gave hundreds of empty batches). The batch bounds are [min, max] of
	// the chunk, the range is closed.
	movingChunkSQL = "SELECT id FROM moving_ids WHERE id > $1 ORDER BY id LIMIT $2"
	// spotLabelSQL — the label of the first domain of the zone before the
	// move: material for the spot check of §7.5 (domain_info after it).
	spotLabelSQL = "SELECT label FROM domains WHERE zone_id = $1 ORDER BY id LIMIT 1"
)

// Step 4 — an UPDATE with row movement (PG16 moves rows between LIST
// partitions when the key changes): domains change the zone and append
// the prefix to the label (§7.2), the children are addressed by the
// domain_id range of the same window. The zone_id = $zid guard in WHERE is
// the idempotency: already moved rows do not fall under the predicate.
const (
	moveDomainsSQL = "UPDATE domains " +
		"SET zone_id = $1, label = label || '.' || $2 " +
		"WHERE zone_id = $3 AND id >= $4 AND id <= $5"
	moveResultsSQL  = "UPDATE subdomain_results SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id <= $4"
	moveLaravelSQL  = "UPDATE laravel_domains SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id <= $4"
	moveFindingsSQL = "UPDATE laravel_findings SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id <= $4"
	// laravel_history — without partitions, an order of magnitude smaller
	// in volume (migration 000011): a single statement per zone
	// (§7.2 step 4).
	moveHistorySQL = "UPDATE laravel_history SET zone_id = $1 WHERE zone_id = $2"
)

// Step 5 — the ingest_files dedup journal: rebinding to the TLD keeps the
// dedup exact for redelivery (T-113); the same content that was already
// delivered to T is a duplicate and is deleted (the owner row remains).
// The NULL semantics of the key (manual rows) — IS NOT DISTINCT FROM.
const (
	filesDedupeSQL = "DELETE FROM ingest_files a USING ingest_files b " +
		"WHERE a.zone_id = $1 AND b.zone_id = $2 " +
		"AND a.action IS NOT DISTINCT FROM b.action AND a.sha256 = b.sha256"
	filesMoveSQL = "UPDATE ingest_files SET zone_id = $1 WHERE zone_id = $2"
)

// ZonePartsSQL — step 6: selecting the partitions of the 2LD zone
// (parent — the root table, part — the zone partition). The DROP is
// executed separately per row: the name from the catalog is escaped with
// pgx.Identifier.
const ZonePartsSQL = "SELECT p.relname AS parent, c.relname AS part " + zonePartsFrom +
	"AND pg_get_expr(c.relpartbound, c.oid) IN (" +
	"format('FOR VALUES IN (%s)', $1::smallint), format('FOR VALUES IN (%L)', $1::text))"

// ZoneLeftSQL — the safety net before the DROP: the move must have emptied
// the zone in all tables with a zone_id (the batches cover [min, max] of
// moving_ids — all the children of the zone); a remainder is a data
// anomaly (an orphan outside the range), a DROP would silently delete the
// rows — the command stops with an error.
const ZoneLeftSQL = "SELECT " +
	"(SELECT count(*) FROM domains WHERE zone_id = $1), " +
	"(SELECT count(*) FROM subdomain_results WHERE zone_id = $1), " +
	"(SELECT count(*) FROM laravel_domains WHERE zone_id = $1), " +
	"(SELECT count(*) FROM laravel_findings WHERE zone_id = $1), " +
	"(SELECT count(*) FROM laravel_history WHERE zone_id = $1), " +
	"(SELECT count(*) FROM ingest_files WHERE zone_id = $1)"

// ZoneDeleteSQL — step 7: the 2LD zone row leaves the reference table.
const ZoneDeleteSQL = "DELETE FROM zones WHERE id = $1"

// Post-checks §7.5: no 2LD zones outside the allowlist remain; no orphan
// zone_id in the tables with a zone.
const (
	CheckZonesLeftSQL = "SELECT count(*) FROM zones WHERE name LIKE '%.%' AND name <> ALL($1)"

	CheckOrphansSQL = "SELECT " +
		"(SELECT count(*) FROM domains t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM subdomain_results t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM laravel_domains t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM laravel_findings t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
		"(SELECT count(*) FROM laravel_history t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id))"
)

// SpotCheckSQL — the spot check of §7.5 via the SQL function of a domain
// (migration 000006): the zone is the longest suffix from the reference
// table, after the collapse it is T. A scalar function in FROM gives
// exactly one row even with NULL (the domain/zone not found) — WHERE
// di IS NOT NULL squeezes out an empty answer.
const SpotCheckSQL = "SELECT di->'zone'->>'name', di->'domain'->>'label' " +
	"FROM domain_info($1) di WHERE di IS NOT NULL"

// ---------------------------------------------------------------- types

// Conn — a connection of the move: the moving_ids temp table lives in the
// session of one connection (step 3), the batches are transactions of the
// same connection (§7.3); *pgxpool.Conn and *pgx.Conn satisfy the
// interface.
type Conn interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Options — the parameters of Run; zero values mean defaults. The
// Allowlist field is filled by the CLI from ingest.allowed_zone_paths
// (validation and normalization — internal/config, T-195).
type Options struct {
	// Allowlist — multi-level zones that are not collapsed (spec §7.1).
	Allowlist []string
	// Apply — execute the move; false — dry-run: an inventory and a plan,
	// changes nothing.
	Apply bool
	// Force — ignore the check_state = 1 precondition (after an operator
	// triage, the runbook §8.2).
	Force bool
	// SkipReconcile — skip the counter reconciliation after the move
	// (the first monitor pass will do it, §7.2 step 8).
	SkipReconcile bool
	// Batch — the size of the id window (§7.3); 0 — DefaultBatch. Not a
	// config: a reduction for the resumption integration test.
	Batch int
	// Log — the command logger; nil — records suppressed
	// (slog.DiscardHandler, the style of RecoverOptions). There is one
	// record: a Warn about a failed final moving_ids cleanup
	// (dropMoving, T-229).
	Log *slog.Logger
}

// ZoneRow — an inventory row: the zone and the move volumes (the report §7.4).
type ZoneRow struct {
	ID       int16
	Name     string // punycode, a compound name (LIKE '%.%')
	Total    int64  // zones.domains_total — the monitor cache before the move
	Domains  int64  // the live count (domains_now per the spec)
	Results  int64  // subdomain_results
	Findings int64  // laravel_findings
	History  int64  // laravel_history
	Files    int64  // ingest_files
	Parts    bool   // the zone has dedicated partitions
}

// Spec — the zone of the move, computed from the name (§7.2): `5g.in` →
// the TLD `in`, the prefix `5g`; `city.kawasaki.jp` → the TLD `jp`, the
// prefix `city.kawasaki`.
type Spec struct {
	ID     int16  // the id of the 2LD zone
	Name   string // the name of the 2LD zone
	TLD    string // the target zone — the last label of the name
	Prefix string // the name without .TLD — appended to the label
}

// SpecOf — a Spec from an inventory row: the zone = the last label, the
// prefix = everything before it (the formula §4.1 of the spec).
func SpecOf(z ZoneRow) (Spec, error) {
	i := strings.LastIndexByte(z.Name, '.')
	if i <= 0 || i == len(z.Name)-1 {
		return Spec{}, fmt.Errorf("zone name %q is not compound (an empty zone or prefix label)", z.Name)
	}
	return Spec{ID: z.ID, Name: z.Name, TLD: z.Name[i+1:], Prefix: z.Name[:i]}, nil
}

// Result — the outcome of moving one zone (the apply report; the counters
// are the rows moved by this run: a resumed run counts the remainder).
type Result struct {
	Zone         string // the name of the 2LD zone
	TLD          string // the target zone
	TLDZoneID    int16
	TLDZoneNew   bool // the TLD zone was created by the move
	Domains      int64
	Results      int64
	Laravel      int64 // laravel_domains
	Findings     int64
	History      int64
	Files        int64
	FilesDupes   int64 // ingest_files duplicates deleted (step 5)
	DroppedParts []string
	Batches      int
	// The spot check §7.5: the full name of the first domain of the zone
	// and the expected label after the move; an empty name — there were
	// no domains in the zone.
	SpotName  string
	SpotLabel string
}

// ---------------------------------------------------------------- steps

// Inventory — an inventory of the 2LD zones outside the allowlist
// (InventorySQL): the list for the dry-run, the move loop and the
// precondition.
func Inventory(ctx context.Context, pool db.DB, allowlist []string) ([]ZoneRow, error) {
	if pool == nil {
		return nil, errors.New("collapse: inventory: pool not set")
	}
	rows, err := pool.Query(ctx, InventorySQL, allowlistArg(allowlist))
	if err != nil {
		return nil, fmt.Errorf("collapse: 2LD zone inventory: %w", err)
	}
	defer rows.Close()
	var zones []ZoneRow
	for rows.Next() {
		var z ZoneRow
		if err := rows.Scan(&z.ID, &z.Name, &z.Total, &z.Domains, &z.Results,
			&z.Findings, &z.History, &z.Files, &z.Parts); err != nil {
			return nil, fmt.Errorf("collapse: 2LD zone inventory: scan: %w", err)
		}
		zones = append(zones, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("collapse: 2LD zone inventory: %w", err)
	}
	return zones, nil
}

// allowlistArg — the argument of ALL($1): nil becomes an empty array
// (ALL(NULL) is NULL, which kills the whole predicate).
func allowlistArg(allowlist []string) []string {
	if allowlist == nil {
		return []string{}
	}
	return allowlist
}

// QueuedCounts — the domains in the queue (check_state = 1) by zones: the
// §7.1 precondition. An empty map — the precondition holds.
func QueuedCounts(ctx context.Context, pool db.DB, ids []int16) (map[int16]int64, error) {
	if len(ids) == 0 {
		return map[int16]int64{}, nil
	}
	rows, err := pool.Query(ctx, QueuedSQL, ids, int16(model.CheckStateQueued))
	if err != nil {
		return nil, fmt.Errorf("collapse: queued domains: %w", err)
	}
	defer rows.Close()
	queued := make(map[int16]int64)
	for rows.Next() {
		var id int16
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("collapse: queued domains: scan: %w", err)
		}
		queued[id] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("collapse: queued domains: %w", err)
	}
	return queued, nil
}

// Zone — moves one 2LD zone into the TLD (steps 1–7 §7.2) over a single
// session connection (the temp table of step 3). Idempotent: every
// statement addresses the remaining rows in the 2LD zone; a failure at any
// step leaves the base in a state from which a rerun of the command
// continues.
func Zone(ctx context.Context, conn Conn, spec Spec, batch int) (Result, error) {
	if conn == nil {
		return Result{}, fmt.Errorf("zone %s: connection not set", spec.Name)
	}
	if batch <= 0 {
		batch = DefaultBatch
	}
	res := Result{Zone: spec.Name, TLD: spec.TLD}

	// Step 1 — the TLD zone.
	tid, created, err := ensureZone(ctx, conn, spec.TLD)
	if err != nil {
		return res, err
	}
	res.TLDZoneID, res.TLDZoneNew = tid, created

	// Step 2 — the TLD partitions before the move (the rows land right in them).
	if _, err := conn.Exec(ctx, CallPartitionsSQL, tid); err != nil {
		return res, fmt.Errorf("zone %s: partitions of zone %s (id %d): %w", spec.Name, spec.TLD, tid, err)
	}

	// Step 3 — moving_ids: the frozen set of ids being moved.
	if _, err := conn.Exec(ctx, movingDropSQL); err != nil {
		return res, fmt.Errorf("zone %s: recreating moving_ids: %w", spec.Name, err)
	}
	if _, err := conn.Exec(ctx, movingCreateSQL, spec.ID); err != nil {
		return res, fmt.Errorf("zone %s: moving_ids: %w", spec.Name, err)
	}
	if _, err := conn.Exec(ctx, movingIndexSQL); err != nil {
		return res, fmt.Errorf("zone %s: moving_ids index: %w", spec.Name, err)
	}

	// The spot check §7.5: the first domain of the zone before the move
	// (after it — empty, the check is not executed).
	if label, ok, err := firstZoneLabel(ctx, conn, spec.ID); err != nil {
		return res, err
	} else if ok {
		res.SpotName = label + "." + spec.Name
		res.SpotLabel = label + "." + spec.Prefix
	}

	// Step 4 — the move in batches (§7.3): chunks of batch live ids from
	// moving_ids in ascending order (a keyset), the batch bounds are
	// [min, max] of the chunk; the domains and their children of one
	// range move atomically.
	last := int64(0)
	for {
		ids, err := movingChunk(ctx, conn, last, batch)
		if err != nil {
			return res, err
		}
		if len(ids) == 0 {
			break
		}
		m, err := moveBatch(ctx, conn, spec, tid, ids[0], ids[len(ids)-1])
		if err != nil {
			return res, err
		}
		res.Batches++
		res.Domains += m.domains
		res.Results += m.results
		res.Laravel += m.laravel
		res.Findings += m.findings
		last = ids[len(ids)-1]
		if len(ids) < batch {
			break
		}
	}

	// laravel_history — one statement per zone (no partitions).
	if tag, err := conn.Exec(ctx, moveHistorySQL, tid, spec.ID); err != nil {
		return res, fmt.Errorf("zone %s: laravel_history: %w", spec.Name, err)
	} else {
		res.History = tag.RowsAffected()
	}

	// Step 5 — the ingest_files dedup journal: a duplicate of the content
	// in T is deleted, the remainder is rebound.
	if tag, err := conn.Exec(ctx, filesDedupeSQL, spec.ID, tid); err != nil {
		return res, fmt.Errorf("zone %s: ingest_files dedup: %w", spec.Name, err)
	} else {
		res.FilesDupes = tag.RowsAffected()
	}
	if tag, err := conn.Exec(ctx, filesMoveSQL, tid, spec.ID); err != nil {
		return res, fmt.Errorf("zone %s: ingest_files: %w", spec.Name, err)
	} else {
		res.Files = tag.RowsAffected()
	}

	// Step 6 — drop the partitions of the 2LD zone (with the ZoneLeftSQL guard).
	parts, err := dropZonePartitions(ctx, conn, spec)
	if err != nil {
		return res, err
	}
	res.DroppedParts = parts

	// Step 7 — the zone row.
	if tag, err := conn.Exec(ctx, ZoneDeleteSQL, spec.ID); err != nil {
		return res, fmt.Errorf("zone %s: row deletion: %w", spec.Name, err)
	} else if tag.RowsAffected() != 1 {
		return res, fmt.Errorf("zone %s: row deletion: %d rows affected, want 1", spec.Name, tag.RowsAffected())
	}
	return res, nil
}

// ensureZone — step 1: the zone id by name; a missing one is created
// active (is_active = true — like the auto-creation by ingest and zones
// sync, the unicode form only for XN zones).
func ensureZone(ctx context.Context, conn db.DB, name string) (id int16, created bool, err error) {
	rows, err := conn.Query(ctx, ZoneIDByNameSQL, name)
	if err != nil {
		return 0, false, fmt.Errorf("zone %s: reference table read: %w", name, err)
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, false, fmt.Errorf("zone %s: reference table read: %w", name, err)
		}
		return id, false, rows.Err()
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("zone %s: reference table read: %w", name, err)
	}
	// The unicode form (the style of zones sync): only when it differs
	// from the punycode.
	unicode := ""
	if u := idn.ToUnicode(name); u != name {
		unicode = u
	}
	irows, err := conn.Query(ctx, ZoneInsertSQL, name, unicode)
	if err != nil {
		return 0, false, fmt.Errorf("zone %s: inserting zone %s: %w", name, name, err)
	}
	defer irows.Close()
	if !irows.Next() {
		return 0, false, fmt.Errorf("zone %s: inserting zone %s: no row inserted", name, name)
	}
	if err := irows.Scan(&id); err != nil {
		return 0, false, fmt.Errorf("zone %s: inserting zone %s: scan: %w", name, name, err)
	}
	return id, true, irows.Err()
}

// firstZoneLabel — the label of the first (min id) domain of the zone;
// ok = false — no domains.
func firstZoneLabel(ctx context.Context, conn db.DB, zoneID int16) (string, bool, error) {
	rows, err := conn.Query(ctx, spotLabelSQL, zoneID)
	if err != nil {
		return "", false, fmt.Errorf("zone %d: first domain: %w", zoneID, err)
	}
	defer rows.Close()
	if rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return "", false, fmt.Errorf("zone %d: first domain: scan: %w", zoneID, err)
		}
		return label, true, rows.Err()
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("zone %d: first domain: %w", zoneID, err)
	}
	return "", false, nil
}

// movingChunk — the next chunk of move ids (a keyset: strictly after
// after, ascending, at most limit rows); an empty chunk — the move is
// exhausted. It is read to the end (the rows are exhausted — the
// connection is free for the batch transaction).
func movingChunk(ctx context.Context, conn db.DB, after int64, limit int) ([]int64, error) {
	rows, err := conn.Query(ctx, movingChunkSQL, after, limit)
	if err != nil {
		return nil, fmt.Errorf("moving_ids: chunk: %w", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("moving_ids: chunk: scan: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// batchMoved — the rows of one batch by tables.
type batchMoved struct {
	domains, results, laravel, findings int64
}

// moveBatch — one batch of §7.3: a transaction with four UPDATEs of one
// range (the domains and their children move atomically); a rollback on
// error is the batch as a whole. A UNIQUE (zone_id, label) violation is
// impossible (§12.5); an anomalous conflict stops the command with the
// zone name and the range — a natural guard, a separate pre-check is not
// needed.
func moveBatch(ctx context.Context, conn Conn, spec Spec, tid int16, lo, hi int64) (batchMoved, error) {
	var m batchMoved
	tx, err := conn.Begin(ctx)
	if err != nil {
		return m, fmt.Errorf("zone %s: batch [%d, %d]: transaction: %w", spec.Name, lo, hi, err)
	}
	// The rollback unconditionally: after Commit — a no-op (the style of
	// monitor, T-044).
	defer func() { _ = tx.Rollback(ctx) }()

	steps := []struct {
		sql  string
		args []any
		dst  *int64
	}{
		{moveDomainsSQL, []any{tid, spec.Prefix, spec.ID, lo, hi}, &m.domains},
		{moveResultsSQL, []any{tid, spec.ID, lo, hi}, &m.results},
		{moveLaravelSQL, []any{tid, spec.ID, lo, hi}, &m.laravel},
		{moveFindingsSQL, []any{tid, spec.ID, lo, hi}, &m.findings},
	}
	for _, st := range steps {
		tag, err := tx.Exec(ctx, st.sql, st.args...)
		if err != nil {
			return m, fmt.Errorf("zone %s: batch [%d, %d]: %w", spec.Name, lo, hi, err)
		}
		*st.dst = tag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return m, fmt.Errorf("zone %s: batch [%d, %d]: commit: %w", spec.Name, lo, hi, err)
	}
	return m, nil
}

// dropZonePartitions — step 6: the emptiness guard of the zone
// (ZoneLeftSQL), the partition selection (ZonePartsSQL) and a DROP per
// each. The names come from the catalog, identifier escaping —
// pgx.Identifier (external input is unacceptable).
func dropZonePartitions(ctx context.Context, conn db.DB, spec Spec) ([]string, error) {
	what := fmt.Sprintf("zone %s: partitions: ", spec.Name)

	left, err := zoneLeft(ctx, conn, spec.ID)
	if err != nil {
		return nil, fmt.Errorf("%sleftover: %w", what, err)
	}
	tables := []string{"domains", "subdomain_results", "laravel_domains", "laravel_findings", "laravel_history", "ingest_files"}
	for i, n := range left {
		if n > 0 {
			return nil, fmt.Errorf("%sthe move is incomplete: %s rows remain in the zone — %d (a data anomaly; DROP would delete them)",
				what, tables[i], n)
		}
	}

	prows, err := conn.Query(ctx, ZonePartsSQL, spec.ID)
	if err != nil {
		return nil, fmt.Errorf("%sselection: %w", what, err)
	}
	defer prows.Close()
	var parts []string
	for prows.Next() {
		var parent, part string
		if err := prows.Scan(&parent, &part); err != nil {
			return nil, fmt.Errorf("%sselection: scan: %w", what, err)
		}
		parts = append(parts, part)
	}
	if err := prows.Err(); err != nil {
		return nil, fmt.Errorf("%sselection: %w", what, err)
	}
	for _, part := range parts {
		if _, err := conn.Exec(ctx, "DROP TABLE "+pgx.Identifier{part}.Sanitize()); err != nil {
			return nil, fmt.Errorf("%sDROP %s: %w", what, part, err)
		}
	}
	return parts, nil
}

// zoneLeft — one row of the ZoneLeftSQL remainder. The read is extracted
// into a helper: a single-row aggregate must be consumed and closed
// (defer at the exit) before the next query — there is one move connection
// (the temp table §7.3), an open portal keeps it busy (conn busy).
func zoneLeft(ctx context.Context, conn db.DB, zoneID int16) (left [6]int64, err error) {
	rows, err := conn.Query(ctx, ZoneLeftSQL, zoneID)
	if err != nil {
		return left, err
	}
	defer rows.Close()
	if !rows.Next() {
		return left, errors.New("empty aggregate answer")
	}
	if err := rows.Scan(&left[0], &left[1], &left[2], &left[3], &left[4], &left[5]); err != nil {
		return left, fmt.Errorf("scan: %w", err)
	}
	return left, rows.Err()
}

// Orphans — the counts of orphan zone_id of the post-check §7.5 (links to
// missing zones in the tables with a zone_id).
type Orphans struct {
	Domains, Results, Laravel, Findings, History int64
}

// Any — are there orphans in at least one table?
func (o Orphans) Any() bool {
	return o.Domains != 0 || o.Results != 0 || o.Laravel != 0 || o.Findings != 0 || o.History != 0
}

// PostChecks — the post-checks §7.5: no 2LD zones outside the allowlist
// remain (zonesLeft = 0) and there are no orphan zone_id.
//
// Every aggregate is read by its own helper: a single-row set is closed
// BEFORE the next pool query (the style of zoneLeft and
// status.FetchLaravelStats) — an open set holds the connection until
// Close, and the move connection is already taken by Run for the whole
// pass (the temp table §7.3): with pool_size of the CLI = 2 the second
// post-check query would wait for a connection forever (the T-217
// deadlock).
func PostChecks(ctx context.Context, pool db.DB, allowlist []string) (zonesLeft int64, orphans Orphans, err error) {
	zonesLeft, err = zonesLeftCount(ctx, pool, allowlist)
	if err != nil {
		return 0, Orphans{}, err
	}
	orphans, err = orphansCounts(ctx, pool)
	if err != nil {
		return 0, Orphans{}, err
	}
	return zonesLeft, orphans, nil
}

// zonesLeftCount — the post-check §7.5: 2LD zones outside the allowlist.
// The read sits in a helper: rows are closed (defer) before returning —
// the next pool query does not compete for a connection with an open set
// (T-217).
func zonesLeftCount(ctx context.Context, pool db.DB, allowlist []string) (int64, error) {
	zrows, err := pool.Query(ctx, CheckZonesLeftSQL, allowlistArg(allowlist))
	if err != nil {
		return 0, fmt.Errorf("collapse: 2LD zone post-check: %w", err)
	}
	defer zrows.Close()
	if !zrows.Next() {
		// First a read failure: a dropped connection is not "empty" (the
		// style of monitor.readDailyValue, the T-217 review P3-4).
		if err := zrows.Err(); err != nil {
			return 0, fmt.Errorf("collapse: 2LD zone post-check: %w", err)
		}
		return 0, fmt.Errorf("collapse: 2LD zone post-check: empty aggregate answer")
	}
	var zonesLeft int64
	if err := zrows.Scan(&zonesLeft); err != nil {
		return 0, fmt.Errorf("collapse: 2LD zone post-check: scan: %w", err)
	}
	if err := zrows.Err(); err != nil {
		return 0, fmt.Errorf("collapse: 2LD zone post-check: %w", err)
	}
	return zonesLeft, nil
}

// orphansCounts — the post-check §7.5: orphan zone_id in the tables with
// a zone. A pair of zonesLeftCount: the set is closed before returning
// (T-217).
func orphansCounts(ctx context.Context, pool db.DB) (Orphans, error) {
	orows, err := pool.Query(ctx, CheckOrphansSQL)
	if err != nil {
		return Orphans{}, fmt.Errorf("collapse: orphan post-check: %w", err)
	}
	defer orows.Close()
	if !orows.Next() {
		// First a read failure — not "empty" (a pair of zonesLeftCount,
		// the T-217 review P3-4).
		if err := orows.Err(); err != nil {
			return Orphans{}, fmt.Errorf("collapse: orphan post-check: %w", err)
		}
		return Orphans{}, fmt.Errorf("collapse: orphan post-check: empty aggregate answer")
	}
	var orphans Orphans
	if err := orows.Scan(&orphans.Domains, &orphans.Results, &orphans.Laravel, &orphans.Findings, &orphans.History); err != nil {
		return Orphans{}, fmt.Errorf("collapse: orphan post-check: scan: %w", err)
	}
	if err := orows.Err(); err != nil {
		return Orphans{}, fmt.Errorf("collapse: orphan post-check: %w", err)
	}
	return orphans, nil
}

// SpotCheck — the spot check §7.5: domain_info(name) answers the zone
// wantZone and the label wantLabel (the appended prefix).
func SpotCheck(ctx context.Context, pool db.DB, name, wantZone, wantLabel string) error {
	rows, err := pool.Query(ctx, SpotCheckSQL, name)
	if err != nil {
		return fmt.Errorf("collapse: spot check %s: %w", name, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("collapse: spot check %s: domain_info found no domain", name)
	}
	var zone, label string
	if err := rows.Scan(&zone, &label); err != nil {
		return fmt.Errorf("collapse: spot check %s: scan: %w", name, err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("collapse: spot check %s: %w", name, err)
	}
	if zone != wantZone || label != wantLabel {
		return fmt.Errorf("collapse: spot check %s: zone %q, label %q; want zone %q, label %q",
			name, zone, label, wantZone, wantLabel)
	}
	return nil
}

// ---------------------------------------------------------------- pass

// movingDropTimeout — the budget of the final moving_ids cleanup (T-229):
// a separate context following the pattern of discardTimeout (ingest,
// pipeline.discard) and refundTimeout (queue, recover) — the cleanup must
// survive the ctx cancellation of the command (Ctrl-C at the finish: the
// context is already cancelled, while the temp table would survive the
// connection returning to the pool) but must not hang indefinitely: a
// DROP without a deadline on a stuck PostgreSQL would hold the connection
// and the CLI exit for an arbitrary time.
const movingDropTimeout = 5 * time.Second

// dropMoving — the final cleanup of the moving_ids temp table: a
// best-effort DROP with its own time budget, an error does not shadow the
// command result — a Warn to the log (T-229). A table that survived the
// cleanup does not break the next runs: step 3 of Zone recreates it
// (DROP IF EXISTS) in the same session of the pool connection.
func dropMoving(conn Conn, timeout time.Duration, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := conn.Exec(ctx, movingDropSQL); err != nil {
		log.Warn("collapse: the final moving_ids cleanup not done — the temp table will stay in the connection's session, the next zone run will recreate it",
			"err", err)
	}
}

// Run — the full pass of the command (§7.1–§7.5): the inventory and the
// plan report, the check_state = 1 precondition, with --apply — the zone
// move by the steps §7.2, the counter reconciliation (shared with
// monitor/crawlerctl counters, T-178 — it also authoritatively recomputes
// the columns of the TLD zones and the global cumulatives), the
// post-checks and the spot checks. The report goes to stdout (the style of
// zones sync).
//
// The move goes over one pool connection (the moving_ids temp table lives
// in its session), reads — through the pool.
func Run(ctx context.Context, pool *pgxpool.Pool, opts Options, stdout io.Writer) error {
	if pool == nil {
		return errors.New("collapse: pool not set")
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	allow := allowlistArg(opts.Allowlist)
	batch := opts.Batch
	if batch <= 0 {
		batch = DefaultBatch
	}

	zones, err := Inventory(ctx, pool, allow)
	if err != nil {
		return err
	}
	if len(zones) == 0 {
		out(stdout, "no 2LD zones outside the allowlist — nothing to collapse (no-op)\n")
		return nil
	}

	// The plan report §7.4 (dry-run and apply).
	specs := make([]Spec, len(zones))
	for i, z := range zones {
		spec, err := SpecOf(z)
		if err != nil {
			return err
		}
		specs[i] = spec
	}
	printPlan(stdout, zones)

	// The precondition §7.1: domains in the queue of the affected zones.
	ids := make([]int16, len(zones))
	for i, z := range zones {
		ids[i] = z.ID
	}
	queued, err := QueuedCounts(ctx, pool, ids)
	if err != nil {
		return err
	}
	if len(queued) > 0 {
		parts := make([]string, 0, len(queued))
		for _, z := range zones { // the inventory order (by name)
			if n, ok := queued[z.ID]; ok {
				parts = append(parts, fmt.Sprintf("%s — %d", z.Name, n))
			}
		}
		out(stdout, "queued domains (check_state = 1) in the affected zones: %s\n", strings.Join(parts, ", "))
		if !opts.Apply {
			out(stdout, "drain the queues before the move (the TLD-only-zones runbook §8.2) or repeat with --force\n")
		} else if !opts.Force {
			return fmt.Errorf("the affected zones have queued domains (check_state = 1): %s; drain the queues (the TLD-only-zones runbook §8.2) or repeat with --force",
				strings.Join(parts, ", "))
		} else {
			out(stdout, "continuing with --force (after the operator's queue triage, the TLD-only-zones runbook §8.2)\n")
		}
	}

	if !opts.Apply {
		out(stdout, "dry-run: no data changed; the move will be done by crawlerctl zones collapse --apply\n")
		return nil
	}

	// The move: one connection (the temp table §7.3), the zones in the
	// inventory order; a failure stops the command with the zone name.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("collapse: move connection: %w", err)
	}
	defer func() {
		// The final moving_ids cleanup (§7.2): the temp table would survive
		// the connection returning to the pool. Best-effort with its own
		// time budget (dropMoving, T-229): a ctx cancellation does not
		// hinder the cleanup, and a stuck PostgreSQL does not hold the
		// connection and the CLI exit; Release — on all paths, regardless
		// of the DROP outcome.
		dropMoving(conn, movingDropTimeout, log)
		conn.Release()
	}()

	results := make([]Result, len(zones))
	for i, spec := range specs {
		res, err := Zone(ctx, conn, spec, batch)
		if err != nil {
			return err
		}
		results[i] = res
		printMoved(stdout, res)
	}

	// Step 8 — the counters: the zone columns are not merged by hand, the
	// shared reconciliation (T-178) authoritatively recomputes the TLD
	// zones and the global cumulatives; --skip-reconcile leaves that to
	// monitor.
	if opts.SkipReconcile {
		out(stdout, "counter reconciliation skipped (--skip-reconcile): the first monitor pass will recompute the zone columns\n")
	} else {
		rec, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
		if err != nil {
			return fmt.Errorf("collapse: counter reconciliation: %w", err)
		}
		drifted := 0
		for _, p := range rec.Positions {
			if p.Drift() == 0 {
				continue
			}
			drifted++
			out(stdout, "  %-32s %12d → %d (Δ%d)\n", p.Counter, p.Old, p.New, p.Drift())
		}
		out(stdout, "counter reconciliation: %d zones rewritten, %d positions with drift\n", rec.Zones, drifted)
	}

	// The post-checks §7.5.
	zonesLeft, orphans, err := PostChecks(ctx, pool, allow)
	if err != nil {
		return err
	}
	out(stdout, "post-checks: 2LD zones outside the allowlist: %d; orphan zone_id: domains %d, subdomain_results %d, laravel_domains %d, laravel_findings %d, laravel_history %d\n",
		zonesLeft, orphans.Domains, orphans.Results, orphans.Laravel, orphans.Findings, orphans.History)
	if zonesLeft != 0 || orphans.Any() {
		return fmt.Errorf("post-checks do not add up: %d 2LD zones, orphans %+v", zonesLeft, orphans)
	}
	if !opts.SkipReconcile {
		mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
		if err != nil {
			return fmt.Errorf("collapse: counter cross-check: %w", err)
		}
		if len(mismatch) > 0 {
			return fmt.Errorf("collapse: zone counters ≠ the aggregate: %d positions with drift (crawlerctl counters recount fixes it)", len(mismatch))
		}
		out(stdout, "zone counters = the aggregate: no mismatches\n")
	}

	// The spot check §7.5: one moved name per zone with domains.
	for i := range results {
		res := results[i]
		if res.SpotName == "" {
			continue
		}
		if err := SpotCheck(ctx, pool, res.SpotName, specs[i].TLD, res.SpotLabel); err != nil {
			return err
		}
		out(stdout, "spot check %s: zone %s, label %s — ok\n", res.SpotName, specs[i].TLD, res.SpotLabel)
	}
	out(stdout, "the move is finished: %d 2LD zones\n", len(zones))
	return nil
}

// out — printing the report (the style of cmd/crawlerctl: a write error is
// ignored, the stream is the stdout of the process).
func out(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}

// printPlan — the plan table §7.4 in the style of zones list: numeric
// columns right-aligned, text ones left-aligned; the total — per each
// target TLD.
func printPlan(w io.Writer, zones []ZoneRow) {
	head := []string{"zone", "TLD", "domains", "results", "findings", "history", "files", "partitions"}
	rows := [][]string{head}
	for _, z := range zones {
		tld := zoneTLD(z.Name)
		parts := "default"
		if z.Parts {
			parts = "yes"
		}
		rows = append(rows, []string{
			z.Name, tld,
			fmt.Sprintf("%d", z.Domains), fmt.Sprintf("%d", z.Results),
			fmt.Sprintf("%d", z.Findings), fmt.Sprintf("%d", z.History),
			fmt.Sprintf("%d", z.Files), parts,
		})
	}
	wmax := make([]int, len(head))
	for _, r := range rows {
		for i, c := range r {
			if l := len([]rune(c)); l > wmax[i] {
				wmax[i] = l
			}
		}
	}
	for _, r := range rows {
		var b strings.Builder
		for i, c := range r {
			if i > 0 {
				b.WriteByte(' ')
			}
			if i >= 2 && i <= 6 { // numeric columns — right-aligned
				fmt.Fprintf(&b, "%*s", wmax[i], c)
			} else {
				fmt.Fprintf(&b, "%-*s", wmax[i], c)
			}
		}
		out(w, "%s\n", b.String())
	}
	// Totals per the target TLDs (§7.4): several 2LDs of one TLD are summed.
	byTLD := make(map[string]int64)
	for _, z := range zones {
		byTLD[zoneTLD(z.Name)] += z.Domains
	}
	tlds := make([]string, 0, len(byTLD))
	for tld := range byTLD {
		tlds = append(tlds, tld)
	}
	sort.Strings(tlds)
	for _, tld := range tlds {
		n := 0
		for _, z := range zones {
			if zoneTLD(z.Name) == tld {
				n++
			}
		}
		word := "zone"
		if n > 1 {
			word = "zones"
		}
		out(w, "total: %d 2LD zones, %d domains → %s %s\n", n, byTLD[tld], word, tld)
	}
}

// zoneTLD — the target zone of a name: the last label (the formula §4.1
// of the spec); called only for the compound names of the inventory.
func zoneTLD(name string) string {
	return name[strings.LastIndexByte(name, '.')+1:]
}

// printMoved — a line of the apply report: the actual move of the zone.
func printMoved(w io.Writer, r Result) {
	newMark := ""
	if r.TLDZoneNew {
		newMark = " (zone created)"
	}
	parts := "no partitions"
	if len(r.DroppedParts) > 0 {
		parts = fmt.Sprintf("%d partitions dropped: %s", len(r.DroppedParts), strings.Join(r.DroppedParts, ", "))
	}
	out(w, "%s → %s (id %d%s): batches %d, domains %d, results %d, laravel domains %d, findings %d, history %d, files %d (%d dupes removed), %s\n",
		r.Zone, r.TLD, r.TLDZoneID, newMark, r.Batches, r.Domains, r.Results,
		r.Laravel, r.Findings, r.History, r.Files, r.FilesDupes, parts)
}

// compile-time: the pool connection fits the move interface.
var _ Conn = (*pgxpool.Conn)(nil)
