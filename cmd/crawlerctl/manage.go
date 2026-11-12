// ---------------------------------------------------------------- management
//
// Management commands from spec 4.1: reanalyze (T-074), subdomains
// (T-083); status — in status.go (T-079), domain — in domain.go (T-081),
// zones and recheck — in zones.go/recheck.go (T-084). The command parses
// the arguments and validates the input (names — via internal/idn); the
// DB selections are made by the commands themselves.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// ---------------------------------------------------------------- reanalyze

// reanalyzeHelp — the crawlerctl reanalyze --help text.
const reanalyzeHelp = `crawlerctl reanalyze — re-analysis of the stored results (spec 5.8)

Usage:
  crawlerctl reanalyze [--zone NAME]

Finds subdomain_results with a stale rules_version (including the never
analyzed ones) and enqueues them into q:analyze in batches — no re-download
happens, the analysis goes over the stored content. Without --zone — all
zones. The rule set version is read from the analyzer.rules_dir catalog at
the moment of the run (the live analyzer service reloads the rules on SIGHUP
and re-analyzes on its own); the analyzer worker records the new results.
Exit codes: 0 — success, 1 — error, 2 — usage.
`

func cmdReanalyze(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("reanalyze")
	zone := fs.String("zone", "", "re-analysis by zone (empty — all)")
	if handled, err := parseFlags(fs, reanalyzeHelp, args, stdout); handled {
		return err
	}
	zoneName := ""
	if *zone != "" {
		// Input validation — through the same pipeline as the incoming
		// directories (internal/idn.NormalizeZone): the zone is stored in punycode.
		normalized, err := idn.NormalizeZone(*zone)
		if err != nil {
			return usagef("reanalyze: invalid zone name %q: %v", *zone, err)
		}
		zoneName = normalized
	}
	if fs.NArg() > 0 {
		return usagef("reanalyze: extra arguments %v (the command takes only --zone)", fs.Args())
	}

	// The configuration — a single YAML + environment (internal/config):
	// the PostgreSQL DSN, the Redis address, the rules directory and the
	// backpressure threshold.
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}

	// The rule set version — the staleness criterion; the directory is
	// read once.
	set, err := rules.LoadDir(cfg.Analyzer.RulesDir)
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}

	// A one-off CLI command: Ctrl-C/SIGTERM interrupt the drain (what has
	// been published stays, at-least-once).
	ctx, stop := signalCtx()
	defer stop()

	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // connection budget of spec 6.3 — as the services
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}
	defer pool.Close()

	zoneDesc := "all"
	var zoneID *int16
	if zoneName != "" {
		id, found, err := analyzer.ZoneIDByName(ctx, pool, zoneName)
		if err != nil {
			return fmt.Errorf("reanalyze: %w", err)
		}
		if !found {
			return fmt.Errorf("reanalyze: zone %s is not in the catalog (ingest creates zones on the first file)", zoneName)
		}
		zoneID, zoneDesc = &id, zoneName
	}

	// The queue — as in the services (RedisQueue + the backpressure gate
	// T-026); the q:analyze publication waits for capacity (the pause warn
	// goes to stderr), a ctx cancellation aborts it.
	log := slog.New(slog.NewTextHandler(stderr, nil))
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // trim limits — for the gate threshold (T-026/T-025)
		Log:       log,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ on each stream (T-025)
	}), gate)

	out(stdout, "reanalyze: ruleset v%d, zone: %s\n", set.Version, zoneDesc)
	res, err := analyzer.ReanalyzeDrain(ctx, pool, q, set.Version, zoneID, analyzer.ReanalyzeOptions{})
	if err != nil {
		return fmt.Errorf("reanalyze: enqueued into q:analyze %d, interrupted: %w", res.Published, err)
	}
	if res.Published == 0 {
		out(stdout, "reanalyze: no stale results — re-analysis not required\n")
		return nil
	}
	out(stdout, "reanalyze: enqueued into q:analyze: %d\n", res.Published)
	return nil
}

// ---------------------------------------------------------------- subdomains
//
// Management of the checked subdomains reference table (spec 5.10, T-083).
// The reference table is the subdomains table (migration 000001):
// id smallint PK — the bit number of dns_mask (0..62, a maximum of 63
// subdomains — bit 63 is the sign bit), label text UNIQUE ('' — the apex,
// id 0), enabled boolean. There is no id sequence: the free bit is
// assigned by crawlerctl (max(id)+1), the integrity is guarded by PK and
// UNIQUE — a race of two add runs ends with an insert error.
//
// Adding/disabling requires no data migration (spec 13.1): the bit is
// bound to the row forever, the dns-worker re-reads the reference table on
// every q:dns batch (T-039) — changes are picked up without a service
// restart. Checking a new subdomain over the already checked domains —
// subdomains recheck (spec 13.1): done domains are moved to pending, then
// the domain goes through the regular pipeline (the T-027 selection →
// q:dns T-028), and the DNS stage checks it already with the new
// subdomain from the reference table.

// subdomainsHelp — the crawlerctl subdomains [--help] text; the same text is
// printed for subdomains <action> --help.
const subdomainsHelp = `crawlerctl subdomains — managing the checked subdomains (spec 5.10)

Usage:
  crawlerctl subdomains list
  crawlerctl subdomains add <label>
  crawlerctl subdomains disable <label|bit>
  crawlerctl subdomains recheck <label> [--zone NAME]

The subdomains catalog — the subdomains table: every subdomain occupies a
dns_mask bit (id = the bit number, at most 63 — bit 63 is the sign bit),
'' — the apex (bit 0). Adding a subdomain — a new row with a free bit, no
data migration needed (spec 13.1); the dns-worker re-reads the catalog on
every batch — the changes are picked up without a service restart.

  list                the catalog table: bit, label, state
  add <label>         add a subdomain (enabled = true); the label is a short
                      name without dots (www, admin, …), unicode is converted
                      to punycode
  disable <label|bit> disable the check (enabled = false): the dns_mask bit
                      and the accumulated results are not changed — the masks
                      are stable (spec 13.1); the apex (bit 0) is the basis
                      of a domain check, it cannot be disabled. The argument
                      is first looked up as a label, then as a bit number
  recheck <label> [--zone NAME]
                      checking a subdomain over the already checked domains
                      (spec 13.1): the zone's done domains (--zone) or those
                      of all zones (deleted excluded) are moved in batches
                      to pending and pass the DNS/HTTP stage again already
                      with the new subdomain; the domains in pending/queued
                      will be checked with it anyway. The pace is set by the
                      scheduler (the q:dns backpressure)

Exit codes: 0 — success, 1 — error, 2 — usage.
`

func cmdSubdomains(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("specify an action: crawlerctl subdomains list|add|disable|recheck")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "list":
		return subdomainsList(rest, stdout, stderr)
	case "add":
		return subdomainsAdd(rest, stdout, stderr)
	case "disable":
		return subdomainsDisable(rest, stdout, stderr)
	case "recheck":
		return subdomainsRecheckCmd(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", subdomainsHelp)
		return flag.ErrHelp
	default:
		return usagef("unknown action %q (valid: list, add, disable, recheck)", action)
	}
}

// ---------------------------------------------------------------- SQL

// subdomainsSelectSQL — the whole reference table by ascending bit; the
// same query is read by the dns-worker (T-039), here — the list output.
const subdomainsSelectSQL = "SELECT id, label, enabled FROM subdomains ORDER BY id"

// subdomainByLabelSQL — a row by label (UNIQUE, migration 000001): the
// "label already exists" check in add, the dereference of disable and the
// subdomain of the recheck command.
const subdomainByLabelSQL = "SELECT id, label, enabled FROM subdomains WHERE label = $1"

// subdomainByIDSQL — a row by bit number: disable <bit> after a failed
// search by label.
const subdomainByIDSQL = "SELECT id, label, enabled FROM subdomains WHERE id = $1"

// subdomainNextIDSQL — the next free bit max(id)+1 (an empty reference
// table would give 0 — the apex). The 0..62 range is checked by Go
// (model.ValidateSubdomainID) before the insert.
const subdomainNextIDSQL = "SELECT coalesce(max(id), -1) + 1 FROM subdomains"

// subdomainInsertSQL — the insert of a new subdomain with the assigned
// bit, enabled = true right away (spec 5.10: add = enable). The race of
// two add runs is caught by PK/UNIQUE on this statement.
const subdomainInsertSQL = "INSERT INTO subdomains (id, label, enabled) VALUES ($1, $2, true) RETURNING id"

// subdomainDisableSQL — disabling: only enabled changes; the id (bit) and
// the results are not touched — the masks are stable (spec 13.1).
// Addressing by bit after dereferencing the argument (label → row → id).
const subdomainDisableSQL = "UPDATE subdomains SET enabled = false WHERE id = $1 RETURNING id, label, enabled"

// subdomainRecheckAllSQL / subdomainRecheckZoneSQL — a single pass moving
// ready domains to pending (the scheduler.recheckUpdateSQL style, T-030):
// LIMIT — a batch (short locks, the autocommit of one statement),
// ORDER BY over the PK key — deterministic batches, the check_state/status
// guard in both the subquery and the outer WHERE (a race with the delete
// file T-019 and a second pass will not move a changed row).
// The check_state transition is row R12 of the FSM spec (the single source
// of truth: internal/scheduler/checkstate.go, T-255).
// Differences from the daily recheck: no last_checked_at age threshold
// (all ready domains of the zone must be rechecked, not the ripe ones)
// and there is a zone filter. The check_state/status codes — parameters,
// as in T-030. The zone counter deltas (checked −n / pending +n) — the
// zc CTE over the RETURNING zone_id of the moved ones: the predicate pins
// the old done state (T-178, review P2-2); the outer SELECT — the count
// of the moved ones (the CommandTag of a compound statement is not
// applicable).
const subdomainRecheckAllSQL = "WITH b AS (SELECT zone_id, id FROM domains " +
	"WHERE check_state = $2 AND status <> $3 " +
	"ORDER BY zone_id, id LIMIT $4), " +
	"upd AS (UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM b WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = $2 AND d.status <> $3 " +
	"RETURNING d.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_checked = z.domains_checked - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT count(*) FROM upd"

const subdomainRecheckZoneSQL = "WITH b AS (SELECT zone_id, id FROM domains " +
	"WHERE check_state = $2 AND status <> $3 AND zone_id = $4 " +
	"ORDER BY zone_id, id LIMIT $5), " +
	"upd AS (UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM b WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = $2 AND d.status <> $3 AND d.zone_id = $4 " +
	"RETURNING d.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_checked = z.domains_checked - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT count(*) FROM upd"

// subdomainRecheckBatch — domains in one move batch: the scheduler batch
// range (spec 5.3: 1000–10,000), matches the default of the daily recheck
// (scheduler.DefaultRecheckBatch).
const subdomainRecheckBatch = 5000

// ---------------------------------------------------------------- data

// subdomainRow — a reference table row for the CLI (the migration 000001 columns).
type subdomainRow struct {
	id      int16  // the dns_mask bit
	label   string // "" — the apex
	enabled bool
}

// fetchSubdomains — the whole reference table by ascending bit (list).
func fetchSubdomains(ctx context.Context, pool db.DB) ([]subdomainRow, error) {
	rows, err := pool.Query(ctx, subdomainsSelectSQL)
	if err != nil {
		return nil, fmt.Errorf("subdomains catalog: %w", err)
	}
	defer rows.Close()
	var subs []subdomainRow
	for rows.Next() {
		var s subdomainRow
		if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
			return nil, fmt.Errorf("subdomains catalog: reading: %w", err)
		}
		subs = append(subs, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("subdomains catalog: %w", err)
	}
	return subs, nil
}

// lookupSubdomainLabel — a row by label; found = false — no such label.
func lookupSubdomainLabel(ctx context.Context, pool db.DB, label string) (subdomainRow, bool, error) {
	rows, err := pool.Query(ctx, subdomainByLabelSQL, label)
	if err != nil {
		return subdomainRow{}, false, fmt.Errorf("subdomain by label %q: %w", label, err)
	}
	defer rows.Close()
	if rows.Next() {
		var s subdomainRow
		if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
			return subdomainRow{}, false, fmt.Errorf("subdomain by label %q: reading: %w", label, err)
		}
		return s, true, nil
	}
	if err := rows.Err(); err != nil {
		return subdomainRow{}, false, fmt.Errorf("subdomain by label %q: %w", label, err)
	}
	return subdomainRow{}, false, nil
}

// lookupSubdomainID — a row by bit number; found = false — no such bit.
func lookupSubdomainID(ctx context.Context, pool db.DB, id int16) (subdomainRow, bool, error) {
	rows, err := pool.Query(ctx, subdomainByIDSQL, id)
	if err != nil {
		return subdomainRow{}, false, fmt.Errorf("subdomain by bit %d: %w", id, err)
	}
	defer rows.Close()
	if rows.Next() {
		var s subdomainRow
		if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
			return subdomainRow{}, false, fmt.Errorf("subdomain by bit %d: reading: %w", id, err)
		}
		return s, true, nil
	}
	if err := rows.Err(); err != nil {
		return subdomainRow{}, false, fmt.Errorf("subdomain by bit %d: %w", id, err)
	}
	return subdomainRow{}, false, nil
}

// nextSubdomainBit — the free bit max(id)+1.
func nextSubdomainBit(ctx context.Context, pool db.DB) (int, error) {
	rows, err := pool.Query(ctx, subdomainNextIDSQL)
	if err != nil {
		return 0, fmt.Errorf("next subdomain bit: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("next subdomain bit: empty response")
	}
	var next int16
	if err := rows.Scan(&next); err != nil {
		return 0, fmt.Errorf("next subdomain bit: reading: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("next subdomain bit: %w", err)
	}
	return int(next), nil
}

// insertSubdomain — inserts a subdomain with the assigned bit.
func insertSubdomain(ctx context.Context, pool db.DB, id int, label string) error {
	rows, err := pool.Query(ctx, subdomainInsertSQL, int16(id), label)
	if err != nil {
		return fmt.Errorf("subdomain insert (bit %d): %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("subdomain insert (bit %d): row not inserted", id)
	}
	var got int16
	if err := rows.Scan(&got); err != nil {
		return fmt.Errorf("subdomain insert (bit %d): reading: %w", id, err)
	}
	return rows.Err()
}

// disableSubdomainByID — disabling by bit; returns the row after the
// update (none — a race with row deletion between the lookup and the
// UPDATE).
func disableSubdomainByID(ctx context.Context, pool db.DB, id int16) (subdomainRow, error) {
	rows, err := pool.Query(ctx, subdomainDisableSQL, id)
	if err != nil {
		return subdomainRow{}, fmt.Errorf("subdomain disable (bit %d): %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return subdomainRow{}, fmt.Errorf("subdomain disable (bit %d): row not updated", id)
	}
	var s subdomainRow
	if err := rows.Scan(&s.id, &s.label, &s.enabled); err != nil {
		return subdomainRow{}, fmt.Errorf("subdomain disable (bit %d): reading: %w", id, err)
	}
	return s, rows.Err()
}

// resolveSubdomainArg — dereferences the disable argument: first the label
// (normalization; the priority — digits can also be labels), if not found
// — the bit number (an argument of digits). No label and not a number —
// found = false.
func resolveSubdomainArg(ctx context.Context, pool db.DB, arg string) (subdomainRow, bool, error) {
	if label, lerr := normalizeSubdomainLabel(arg); lerr == nil {
		s, found, err := lookupSubdomainLabel(ctx, pool, label)
		if err != nil || found {
			return s, found, err
		}
	}
	// The 0..62 range is checked before the search (ValidateSubdomainID,
	// as in add): without the check the int16(id) truncation would
	// silently map 65537 to bit 1 — disable would disable a foreign
	// subdomain instead of "not found".
	if id, perr := strconv.Atoi(strings.TrimSpace(arg)); perr == nil {
		if err := model.ValidateSubdomainID(id); err != nil {
			return subdomainRow{}, false, nil
		}
		return lookupSubdomainID(ctx, pool, int16(id))
	}
	return subdomainRow{}, false, nil
}

// ---------------------------------------------------------------- actions

// subdomainsListRun — prints the reference table: bit, label, state.
func subdomainsListRun(ctx context.Context, pool db.DB, stdout io.Writer) error {
	subs, err := fetchSubdomains(ctx, pool)
	if err != nil {
		return err
	}
	enabled := 0
	out(stdout, "bit  label              act\n")
	for _, s := range subs {
		mark := "-"
		if s.enabled {
			mark = "+"
			enabled++
		}
		label := s.label
		if label == "" {
			label = "(apex)"
		}
		out(stdout, "%3d  %-18s  %s\n", s.id, label, mark)
	}
	out(stdout, "total %d of %d, enabled %d\n", len(subs), model.MaxSubdomains, enabled)
	return nil
}

// subdomainsAddRun — adds a subdomain with the free bit.
func subdomainsAddRun(ctx context.Context, pool db.DB, label string, stdout io.Writer) error {
	// The label is unique (migration 000001): a duplicate — an error with
	// the bit (enabled or disabled — there is nothing to insert anyway).
	if s, found, err := lookupSubdomainLabel(ctx, pool, label); err != nil {
		return err
	} else if found {
		state := "enabled"
		if !s.enabled {
			state = "disabled"
		}
		return fmt.Errorf("the label is already in the catalog: bit %d, %s", s.id, state)
	}
	// The free bit: max(id)+1, the 0..62 range — bit 63 is the sign bit
	// (model.ValidateSubdomainID, spec 5.10).
	id, err := nextSubdomainBit(ctx, pool)
	if err != nil {
		return err
	}
	if err := model.ValidateSubdomainID(id); err != nil {
		return err
	}
	if err := insertSubdomain(ctx, pool, id, label); err != nil {
		return err
	}
	out(stdout, "subdomain %s added: bit %d\n", label, id)
	out(stdout, "checking the already checked domains: crawlerctl subdomains recheck %s [--zone NAME]\n", label)
	return nil
}

// subdomainApexGuard — forbids disabling the apex: without an A/NS query
// of the apex the NXDOMAIN branch and the wildcard detect are impossible
// (spec 5.4 items 1, 3) — the apex is the basis of a domain check.
func subdomainApexGuard() error {
	return fmt.Errorf("the apex (bit 0) is the basis of a domain check (spec 5.4 item 1), disabling is not provided")
}

// subdomainsDisableRun — disables a subdomain (enabled = false).
func subdomainsDisableRun(ctx context.Context, pool db.DB, arg string, stdout io.Writer) error {
	if strings.TrimSpace(arg) == "" {
		return subdomainApexGuard()
	}
	s, found, err := resolveSubdomainArg(ctx, pool, arg)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("subdomain %q not found in the catalog (see subdomains list)", arg)
	}
	if s.id == 0 {
		return subdomainApexGuard()
	}
	// The idempotent branch — before any write: "already disabled" makes
	// no UPDATE (an extra write is pointless, the message matches the
	// lookup state).
	if !s.enabled {
		out(stdout, "subdomain %s (bit %d) already disabled\n", s.label, s.id)
		return nil
	}
	after, err := disableSubdomainByID(ctx, pool, s.id)
	if err != nil {
		return err
	}
	out(stdout, "subdomain %s (bit %d) disabled: the bit and the accumulated masks/results are not changed (spec 13.1)\n",
		after.label, after.id)
	return nil
}

// subdomainsRecheckRun — starts a subdomain check over ready domains: the
// done → pending drain (the mechanism — a state move: the publication into
// q:dns is done by the scheduler, which owns the backpressure and the
// lease, and the DNS worker takes the subdomain list from the reference
// table itself, T-039, — publishing special tasks with a subdomain list
// would duplicate the pipeline).
func subdomainsRecheckRun(ctx context.Context, pool db.DB, label string, zoneID *int16, zoneDesc string, stdout io.Writer) error {
	s, found, err := lookupSubdomainLabel(ctx, pool, label)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("subdomain %s is not in the catalog — run subdomains add %s first (spec 5.10)", label, label)
	}
	if !s.enabled {
		return fmt.Errorf("subdomain %s (bit %d) is disabled — the DNS worker does not check it (spec 5.4 item 2)", label, s.id)
	}

	out(stdout, "subdomain %s (bit %d), zone: %s\n", label, s.id, zoneDesc)
	total, err := drainSubdomainRecheck(ctx, pool, zoneID)
	if err != nil {
		if total > 0 {
			return fmt.Errorf("moved to pending %d domains, interrupted: %w", total, err)
		}
		return err
	}
	if total == 0 {
		out(stdout, "no done domains to recheck (done domains, except deleted)\n")
		return nil
	}
	out(stdout, "moved to pending: %d domains\n", total)
	out(stdout, "the domains will pass the DNS/HTTP stage again with the current catalog; the scheduler sets the pace (the q:dns backpressure)\n")
	return nil
}

// drainSubdomainRecheck — drains in batches until the done domains are
// exhausted: the moved rows no longer satisfy the subquery, a partial
// batch — the set is exhausted (the RecheckOnce/RecheckDaily style,
// T-030). An interrupted drain is partial but not destructive: the move is
// idempotent, a repeated command run continues from the same place.
func drainSubdomainRecheck(ctx context.Context, pool db.DB, zoneID *int16) (int64, error) {
	var total int64
	for {
		n, err := subdomainRecheckOnce(ctx, pool, zoneID)
		if err != nil {
			return total, err
		}
		total += n
		if n < subdomainRecheckBatch {
			return total, nil
		}
	}
}

// subdomainRecheckOnce — a single pass moving a batch done → pending
// (the SQL — subdomainRecheckAllSQL/subdomainRecheckZoneSQL).
func subdomainRecheckOnce(ctx context.Context, pool db.DB, zoneID *int16) (int64, error) {
	what := "moving done → pending"
	var sql string
	var args []any
	if zoneID == nil {
		sql = subdomainRecheckAllSQL
		args = []any{int16(model.CheckStatePending), int16(model.CheckStateDone),
			int16(model.StatusDeleted), subdomainRecheckBatch}
	} else {
		z := *zoneID
		what = fmt.Sprintf("moving done → pending (zone %d)", z)
		sql = subdomainRecheckZoneSQL
		args = []any{int16(model.CheckStatePending), int16(model.CheckStateDone),
			int16(model.StatusDeleted), z, subdomainRecheckBatch}
	}
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	n, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	return n, nil
}

// ---------------------------------------------------------------- commands

// openManagePool — the DB pool of the management commands (subdomains,
// zones, recheck): a single YAML + environment (internal/config),
// pool_size 2 — the connection budget of spec 6.3 (the reanalyze style).
// The caller closes it (defer pool.Close()).
func openManagePool(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	return db.Open(ctx, poolOpts)
}

func subdomainsList(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains list")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("subdomains list: extra arguments %v (the command takes no arguments)", fs.Args())
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains list: %w", err)
	}
	defer pool.Close()
	if err := subdomainsListRun(ctx, pool, stdout); err != nil {
		return fmt.Errorf("subdomains list: %w", err)
	}
	return nil
}

func subdomainsAdd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains add")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("specify a subdomain label: crawlerctl subdomains add www")
	}
	label, err := normalizeSubdomainLabel(fs.Arg(0))
	if err != nil {
		return usagef("subdomains add: %v", err)
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains add %s: %w", label, err)
	}
	defer pool.Close()
	if err := subdomainsAddRun(ctx, pool, label, stdout); err != nil {
		return fmt.Errorf("subdomains add %s: %w", label, err)
	}
	return nil
}

func subdomainsDisable(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains disable")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("specify a subdomain: crawlerctl subdomains disable www|bit")
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains disable %s: %w", fs.Arg(0), err)
	}
	defer pool.Close()
	if err := subdomainsDisableRun(ctx, pool, fs.Arg(0), stdout); err != nil {
		return fmt.Errorf("subdomains disable %s: %w", fs.Arg(0), err)
	}
	return nil
}

// subdomainsRecheckCmd — crawlerctl subdomains recheck: starts a subdomain
// check over a zone / all domains (spec 13.1).
func subdomainsRecheckCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains recheck")
	zone := fs.String("zone", "", "check zone (empty — all zones)")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("specify a subdomain label: crawlerctl subdomains recheck www [--zone NAME]")
	}
	label, err := normalizeSubdomainLabel(fs.Arg(0))
	if err != nil {
		return usagef("subdomains recheck: %v", err)
	}
	zoneName := ""
	if *zone != "" {
		// Input validation — through the same pipeline as reanalyze
		// (internal/idn.NormalizeZone): the zone is stored in punycode.
		normalized, zerr := idn.NormalizeZone(*zone)
		if zerr != nil {
			return usagef("subdomains recheck: invalid zone name %q: %v", *zone, zerr)
		}
		zoneName = normalized
	}

	// A one-off CLI command: Ctrl-C/SIGTERM interrupt the drain — the
	// moved ones stay (the move is idempotent), a repeated command run
	// continues.
	ctx, stop := signalCtx()
	defer stop()

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("subdomains recheck %s: %w", label, err)
	}
	defer pool.Close()

	zoneDesc := "all"
	var zoneID *int16
	if zoneName != "" {
		id, found, zerr := analyzer.ZoneIDByName(ctx, pool, zoneName)
		if zerr != nil {
			return fmt.Errorf("subdomains recheck %s: %w", label, zerr)
		}
		if !found {
			return fmt.Errorf("subdomains recheck %s: zone %s is not in the catalog (ingest creates zones on the first file)", label, zoneName)
		}
		zoneID, zoneDesc = &id, zoneName
	}

	if err := subdomainsRecheckRun(ctx, pool, label, zoneID, zoneDesc, stdout); err != nil {
		return fmt.Errorf("subdomains recheck %s: %w", label, err)
	}
	return nil
}

// normalizeSubdomainLabel brings a subdomain label to the canonical form:
// unicode → punycode (spec 5.2, HostToASCII — a short label is a valid
// "host"), then the DNS label check: latin letters/digits/hyphen inside,
// 1..63 octets. An empty label — the apex (id 0) — is already in the
// reference table and cannot be added.
func normalizeSubdomainLabel(label string) (string, error) {
	if strings.TrimSpace(label) == "" {
		return "", fmt.Errorf("the subdomain label is empty: the apex '' is already in the catalog (id 0)")
	}
	ascii, err := idn.HostToASCII(label)
	if err != nil {
		return "", fmt.Errorf("invalid subdomain label %q: %v", label, err)
	}
	if !validSubdomainLabel(ascii) {
		return "", fmt.Errorf("invalid subdomain label %q: allowed are a-z, 0-9 and a hyphen inside, length 1..63", label)
	}
	return ascii, nil
}

// validSubdomainLabel checks an ASCII label: [a-z0-9-], a hyphen only
// inside, the length 1..63 (the DNS label length, spec 5.2 item 4).
func validSubdomainLabel(ascii string) bool {
	if len(ascii) == 0 || len(ascii) > 63 {
		return false
	}
	for i := 0; i < len(ascii); i++ {
		c := ascii[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
			if i == 0 || i == len(ascii)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
