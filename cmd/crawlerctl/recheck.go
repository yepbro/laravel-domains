// ---------------------------------------------------------------- recheck
//
// crawlerctl recheck (T-084, spec 4.1): forced enqueueing of domains for
// checking — the "manual tasks" from spec 4.1. Names are accepted in any
// form (unicode/punycode, case, trailing dot; internal/idn.Normalize — the
// same pipeline as domain, T-081); domains are looked up by the key
// (zone, label) (UNIQUE, migration 000001).
//
// The mechanism (pinned by the T-028/T-030 semantics, consistent with
// subdomains recheck T-083): check_state → pending, queued_at = NULL —
// the publication into q:dns is done by the scheduler, which owns the
// backpressure (watermark) and the lease (T-028); a CLI publishing by
// itself would duplicate the pipeline and bypass the queue threshold.
// A pending domain is picked up by DispatchBatch (T-027) in a round-robin
// fashion between zones.
//
// Semantics by the original states (check_state):
//
//   - done → pending: the main case (an operator checked the site by
//     hand — we do not wait for recheck_interval, spec 5.3);
//   - pending → pending: an idempotent no-op — the domain is already
//     waiting, the transition changes nothing;
//   - queued → pending: equivalent to a manual lease refund (T-029):
//     the domain has been taken into work, a re-check is guaranteed; the
//     task may already have been published into q:dns — the domain will
//     be checked twice; at-least-once delivery and idempotent result
//     records allow this (spec 3.4), the duplicated work is limited to
//     one cycle.
//
// deleted — a rejection with a message (spec 13.1: deleted domains are
// not checked); not found (no zone / no label in the zone) — a rejection
// with a list; exit code 1 is returned if at least one domain was not
// enqueued (the enqueued ones are translated anyway — the partial
// success is visible in the output).

package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
)

// recheckHelp — the crawlerctl recheck --help text.
const recheckHelp = `crawlerctl recheck — принудительная постановка домена на проверку

Использование:
  crawlerctl recheck <домен> [домен …]

Домены (одно имя или несколько, в любой форме: unicode/punycode,
регистр, точка на конце) ставятся на проверку, не дожидаясь интервала
перепроверки (ТЗ 5.3): найденные по имени домены переводятся в
check_state = pending (queued_at сбрасывается), публикацию в q:dns
делает планировщик — с учётом backpressure, темп задаёт очередь.
Это «ручные задачи» crawlerctl из ТЗ 4.1.

Домен в работе (queued) переводится как возврат lease: он проверится
заново; уже ожидающий (pending) остаётся в очереди. Удалённый
(deleted) домен не проверяется (ТЗ 13.1) — отказ с сообщением.
Не найденные имена (нет зоны в справочнике или нет метки в зоне)
перечисляются в отказе, код 1; некорректное имя — код 2.

Коды возврата: 0 — все домены поставлены, 1 — часть не поставлена
или ошибка, 2 — использование.
`

// ---------------------------------------------------------------- SQL

// recheckLookupSQL — resolves all requested names with a single query:
// full punycode names arrive as an array (unnest — the internal/db batch
// operations style), WITH ORDINALITY preserves the argument order for
// output. The zone is the longest suffix of the name present in the zones
// reference table (the same way domain_info splits the zone, T-082: PSL
// does not know test zones like crawl.test, the last label would be
// wrong; co.uk is preferred over uk), LATERAL LIMIT 1 — by descending
// length. The label is the name prefix without the dot and the zone:
// substr(name, 1, length(name) − length(zone) − 1). The LEFT JOINs lose
// no name: a missing zone/label — coalesce zeros (real rows have id ≥ 1,
// migration 000001). The zone is_active — the warning "the domain waits
// for the zone to be enabled" (zones disable, T-084). A zone never
// contains "%" / "_" (idna.Lookup, spec 5.2) — the LIKE pattern is safe.
// Verified by a live run in an isolated schema (the T-081 style).
const recheckLookupSQL = "SELECT n.ord, " +
	"coalesce(z.id, 0) AS zone_id, coalesce(z.name, '') AS zone_name, coalesce(z.is_active, false) AS zone_on, " +
	"coalesce(d.id, 0) AS domain_id, coalesce(d.label, '') AS label, " +
	"coalesce(d.check_state, 0) AS check_state, coalesce(d.status, 0) AS status " +
	"FROM unnest($1::text[]) WITH ORDINALITY AS n(name, ord) " +
	"LEFT JOIN LATERAL (" +
	"SELECT id, name, is_active FROM zones AS z " +
	"WHERE n.name LIKE '%.' || z.name " +
	"ORDER BY length(z.name) DESC, z.name LIMIT 1" +
	") AS z ON true " +
	"LEFT JOIN domains AS d ON d.zone_id = z.id AND d.label = substr(n.name, 1, length(n.name) - length(z.name) - 1) " +
	"ORDER BY n.ord"

// recheckUpdateSQL — moves the found domains to pending with a single
// statement over PK keys (unnest — the enqueueUpdate T-028 style):
// check_state = pending and queued_at = NULL — as in the daily recheck
// (T-030): pending has no lease, the enqueueing will set one. The
// check_state transition is row R11 of the FSM spec (the single source of
// truth: internal/scheduler/checkstate.go, T-255).
// The guard d.status <> deleted re-checks the status at the row lock
// moment — a race with the delete file (T-019) will not move a domain
// being deleted; RETURNING — the keys of the actually moved rows (the
// ones raced into deleted are skipped and end up in the rejection).
const recheckUpdateSQL = "UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM unnest($2::int2[], $3::int8[]) AS k(zone_id, domain_id) " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id AND d.status <> $4 " +
	"RETURNING d.zone_id, d.id"

// recheckStatesSQL — the old check_states of the rows being moved, a
// point pre-read with FOR UPDATE (T-178, incremental-counters spec
// §6.1.2: the move operator does not pin the old state — pending/queued/
// done are moved alike, and PG 16 has no RETURNING OLD): the row lock
// serializes the pre-read with the move in one transaction (recheckDB) —
// the old state is actual at the UPDATE moment, the zone counter deltas
// are exact (done → checked −n/pending +n, queued → queued −n/pending +n,
// pending → no deltas: the state did not change).
const recheckStatesSQL = "SELECT d.zone_id, d.id, d.check_state " +
	"FROM domains AS d " +
	"JOIN unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"ON d.zone_id = k.zone_id AND d.id = k.domain_id " +
	"WHERE d.status <> $3 " +
	"FOR UPDATE OF d"

// recheckDB — a move pool with transactions: the state pre-read, the move
// and the zone counter deltas are atomic (T-178, incremental-counters spec
// §6.1.3); *pgxpool.Pool (openManagePool) satisfies the interface.
type recheckDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ---------------------------------------------------------------- data

// recheckItem — one requested domain after name resolution
// (a recheckLookupSQL row). Zero ids — "no row" (coalesce).
type recheckItem struct {
	ord      int64
	zoneID   int16 // 0 — the zone is not in the reference table
	zoneName string
	zoneOn   bool  // the zone is_active
	domainID int64 // 0 — the domain is not in the zone
	label    string
	state    model.CheckState
	status   model.Status
}

// fetchRecheckItems resolves full punycode names (already normalized)
// into domain rows; the result — one item per name, in the argument
// order (ord).
func fetchRecheckItems(ctx context.Context, pool db.DB, names []string) ([]recheckItem, error) {
	rows, err := pool.Query(ctx, recheckLookupSQL, names)
	if err != nil {
		return nil, fmt.Errorf("разрешение имён доменов: %w", err)
	}
	defer rows.Close()
	items := make([]recheckItem, 0, len(names))
	for rows.Next() {
		var it recheckItem
		var state, status int16 // schema codes (smallint), model types below
		if err := rows.Scan(&it.ord, &it.zoneID, &it.zoneName, &it.zoneOn,
			&it.domainID, &it.label, &state, &status); err != nil {
			return nil, fmt.Errorf("разрешение имён доменов: чтение: %w", err)
		}
		it.state, it.status = model.CheckState(state), model.Status(status)
		if it.ord < 1 || it.ord > int64(len(names)) {
			return nil, fmt.Errorf("разрешение имён доменов: ord %d вне диапазона 1..%d (контракт WITH ORDINALITY)", it.ord, len(names))
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("разрешение имён доменов: %w", err)
	}
	return items, nil
}

// recheckKey — a domains row key (PK, spec 6.1): a move is addressed by
// the (zone_id, id) pair; duplicate keys in unnest are harmless — the
// UPDATE applies once per row.
type recheckKey struct {
	zone int16
	id   int64
}

// recheckTranslate moves the found domains to pending in one transaction
// (T-178): the FOR UPDATE pre-read of the old states → the move
// (recheckUpdateSQL) → the zone counter deltas by the old states of the
// moved rows (db.ZoneShift); returns the set of the actually moved keys
// (per RETURNING — the status guard could skip a row).
func recheckTranslate(ctx context.Context, pool recheckDB, keys []recheckKey) (map[recheckKey]bool, error) {
	zones := make([]int16, len(keys))
	ids := make([]int64, len(keys))
	for i, k := range keys {
		zones[i], ids[i] = k.zone, k.id
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: транзакция: %w", err)
	}
	// Rollback unconditionally: after Commit it is a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	// Old states: the row locks keep the pre-read and the move consistent
	// (duplicate record/lease delivery changes the state only up to the
	// lock — after it there is no way around the transaction).
	oldStates := make(map[recheckKey]model.CheckState, len(keys))
	rows, err := tx.Query(ctx, recheckStatesSQL, zones, ids, int16(model.StatusDeleted))
	if err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: чтение состояний: %w", err)
	}
	for rows.Next() {
		var k recheckKey
		var state int16
		if err := rows.Scan(&k.zone, &k.id, &state); err != nil {
			rows.Close()
			return nil, fmt.Errorf("перевод доменов в pending: чтение состояний: строка: %w", err)
		}
		oldStates[k] = model.CheckState(state)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("перевод доменов в pending: чтение состояний: %w", err)
	}
	rows.Close()

	rows, err = tx.Query(ctx, recheckUpdateSQL,
		int16(model.CheckStatePending), zones, ids, int16(model.StatusDeleted))
	if err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: %w", err)
	}
	shift := db.NewZoneShift()
	done := make(map[recheckKey]bool, len(keys))
	for rows.Next() {
		var k recheckKey
		if err := rows.Scan(&k.zone, &k.id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("перевод доменов в pending: чтение: %w", err)
		}
		done[k] = true
		switch oldStates[k] {
		case model.CheckStateDone:
			shift.Add(k.zone, 0, 1, 0, -1) // checked −1, pending +1
		case model.CheckStateQueued:
			shift.Add(k.zone, 0, 1, -1, 0) // queued −1, pending +1
		}
		// pending → pending: the state did not change — no deltas.
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("перевод доменов в pending: %w", err)
	}
	rows.Close()

	if err := shift.Exec(ctx, tx); err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("перевод доменов в pending: фиксация: %w", err)
	}
	return done, nil
}

// ---------------------------------------------------------------- action

// recheckRun — enqueues domains for checking: name resolution, moving the
// found ones, a per-line summary. Returns an error if at least one domain
// was not enqueued (the moved ones stay in pending — the move is not
// rolled back, a repeated command run is idempotent). names — full
// punycode names (idn.Normalize), the order is preserved in the output.
func recheckRun(ctx context.Context, pool recheckDB, names []string, stdout io.Writer) error {
	items, err := fetchRecheckItems(ctx, pool, names)
	if err != nil {
		return err
	}

	// Candidates for the move: found and not deleted. Rejections are
	// collected per line (what exactly was not enqueued and why).
	var keys []recheckKey
	var failed []string
	for _, it := range items {
		name := names[it.ord-1] // ord is 1-based, WITH ORDINALITY
		switch {
		case it.zoneID == 0:
			// A candidate zone for the message — the longest name suffix
			// (the domainNotFound style, T-081: what exactly is not found).
			zone := zoneSuffixCandidates(name)[0]
			out(stdout, "%s: не найден: зоны %s нет в справочнике (зоны создаёт ingest при первом файле)\n", name, zone)
			failed = append(failed, name)
		case it.domainID == 0:
			out(stdout, "%s: не найден: в зоне %s нет домена с меткой %s\n",
				name, it.zoneName, strings.TrimSuffix(name, "."+it.zoneName))
			failed = append(failed, name)
		case it.status == model.StatusDeleted:
			out(stdout, "%s: удалён (deleted) — повторная проверка не выполняется (ТЗ 13.1)\n", name)
			failed = append(failed, name)
		default:
			keys = append(keys, recheckKey{zone: it.zoneID, id: it.domainID})
		}
	}

	translated := map[recheckKey]bool{}
	if len(keys) > 0 {
		translated, err = recheckTranslate(ctx, pool, keys)
		if err != nil {
			return err
		}
	}

	// Per-line summary of the move — a second pass over items (the
	// argument order): the move could skip a row raced into deleted.
	for _, it := range items {
		if it.domainID == 0 {
			continue // the rejection is already printed
		}
		name := names[it.ord-1]
		k := recheckKey{zone: it.zoneID, id: it.domainID}
		if !translated[k] {
			if it.status != model.StatusDeleted {
				out(stdout, "%s: не поставлен: статус изменился на deleted во время команды (ТЗ 13.1)\n", name)
				failed = append(failed, name)
			}
			continue
		}
		note := ""
		if !it.zoneOn {
			note = " — зона выключена, домен ждёт включения (crawlerctl zones enable " + it.zoneName + ")"
		}
		out(stdout, "%s: поставлен на проверку (было check_state: %s)%s\n", name, it.state, note)
	}

	out(stdout, "переведено в проверку (pending): %d из %d\n", len(translated), len(items))
	if len(failed) > 0 {
		return fmt.Errorf("не поставлены: %s", joinNames(failed))
	}
	out(stdout, "домены проверит планировщик: выборка pending → q:dns (backpressure задаёт темп, ТЗ 5.3)\n")
	return nil
}

// joinNames lists the rejection names: the first three in full, the rest
// as a counter (+N) — the one-line message does not stretch.
func joinNames(names []string) string {
	const show = 3
	if len(names) <= show {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s (+%d)", strings.Join(names[:show], ", "), len(names)-show)
}

// ---------------------------------------------------------------- command

// cmdRecheck — crawlerctl recheck: name normalization (before the
// configuration — a wrong name is a usage error, exit code 2), then a
// one-shot enqueue. Ctrl-C/SIGTERM interrupt the command; the moved ones
// stay (the move is idempotent, a repeat continues).
func cmdRecheck(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("recheck")
	if handled, err := parseFlags(fs, recheckHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() == 0 {
		return usagef("укажите домен: crawlerctl recheck example.crawl.test")
	}
	names := make([]string, 0, fs.NArg())
	for _, a := range fs.Args() {
		res, err := idn.Normalize(a)
		if err != nil {
			return usagef("recheck: некорректное имя домена %q: %v", a, err)
		}
		names = append(names, res.ASCII)
	}

	ctx, stop := signalCtx()
	defer stop()

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("recheck %s: %w", joinNames(names), err)
	}
	defer pool.Close()

	if err := recheckRun(ctx, pool, names, stdout); err != nil {
		return fmt.Errorf("recheck %s: %w", joinNames(names), err)
	}
	return nil
}
