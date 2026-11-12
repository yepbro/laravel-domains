// ---------------------------------------------------------------- import
//
// crawlerctl fixtures import (T-243, the render engine selection task
// spec tasks/TZ_render_engines.md §6.1 items 3–4): an idempotent import
// of a format v1 dump (fixtures export, T-242) into the bench's test DB
// and publishing of the fixture tasks straight into q:render — the render
// stage is tested in isolation, without DNS/HTTP. The test DB is
// isolated from production (§6.1 item 3): the dev-stack migrations are
// applied separately, only dump rows come here.
//
// Import idempotency — by primary keys from the dump keys (T-242 rows
// keep their original identifiers): INSERT … ON CONFLICT (PK) DO UPDATE —
// a repeated import does not duplicate rows but restores the dump state
// (a worker run changes rows — rendered, content references,
// finalization markers; a repeated import returns the fixture to its
// original look, so two runs are compared from the same start). The only
// deliberate difference from the dump: content_hash is reset to NULL —
// the content store was not exported (T-242), the production reference
// would dangle on the bench and break the render's write (SwapRefTx,
// ErrRefIntegrity); the DOM on the bench is written by the render
// itself. Sequences — per the T-242 contract notes (the fixtures.go doc
// comment): zones.id — GENERATED ALWAYS AS IDENTITY, original ids enter
// via OVERRIDING SYSTEM VALUE; domains.id — nextval('domain_id_seq');
// after the import both sequences are set by setval above the maximum
// (GREATEST of the table's and the dump's maximum) — the bench's new
// zones/domains do not start at 1 and do not collide with the fixture.
//
// Natural keys (zones.name, domains (zone_id, label)) are not in the
// conflict set: a PK-based import assumes a clean test DB (migrations
// without data, §6.1 item 3); a zone/domain with the same name but a
// different id is a UNIQUE conflict that is checked with a clear error
// before writing (not with pgcode 23505 internals from the INSERT).
//
// Publishing (--publish): one queue.Result message per dump row (the
// same contract that the HTTP stage and the rework pass publish into
// q:render, renderq.go; Fresh is not set — the worker itself sets it
// when publishing to q:analyze, worker.go) into the bench
// configuration's q:render stream. Mode all — all dump rows, smoke —
// only the smoke subsample domains (§6.1 item 6). Duplicate keys within
// the selection are cut off with a count (the T-242 dump is a set, a
// guard against hand-assembled ones); rerunning import with --publish
// adds the tasks into the stream again — the stream is cleared between
// runs (the instructions are in docs/dev-environment.md).
//
// Writing — only with an explicit --force (S-P3-5, an extra review round
// of stage 35; the model is zones collapse --apply): the import
// overwrites zones/domains/result rows by PK and sweeps contents; a
// wrong DSN would irreversibly change someone else's rows. Without the
// flag — a dry-run: the target from the DSN (no password) and the full
// import plan without a single write; with the flag the target and the
// plan are printed before the first write. --publish complements
// --force — the q:render publishing silently does not happen.

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// fixturesImportBatch — dump rows in one INSERT (unnest arrays): all
// table columns — one array parameter per column (the recordInsertSQL
// style, T-058); 500 rows × ≤17 columns does not hit the protocol limit
// (an unsigned short of parameters), a batch-pass doubling — like the
// drain cycles (subdomainRecheckBatch).
const fixturesImportBatch = 500

// The --publish publishing modes (a two-valued flag: empty — do not publish).
const (
	fixturesPublishAll   = "all"   // all dump rows (the full §5.1 run)
	fixturesPublishSmoke = "smoke" // the smoke subsample domains (§6.1 item 6)
)

// ---------------------------------------------------------------- SQL

// fixturesZoneByNameSQL — the natural zone keys for the import
// pre-check: a zone with the same name but a different id (a UNIQUE
// conflict) is caught before the INSERT with a clear error. ANY over the
// dump's name array.
const fixturesZoneByNameSQL = "SELECT id, name FROM zones WHERE name = ANY($1::text[])"

// fixturesDomainByLabelSQL — the same for domains: (zone_id, label) from
// the dump's unnest keys (the batch operation style of internal/db).
const fixturesDomainByLabelSQL = "SELECT d.zone_id, d.label, d.id FROM domains AS d " +
	"JOIN unnest($1::int2[], $2::text[]) AS k(zone_id, label) " +
	"ON d.zone_id = k.zone_id AND d.label = k.label"

// fixturesImportZonesSQL — zone import by PK: original ids enter past
// the identity (OVERRIDING SYSTEM VALUE, the T-242 contract); a PK
// conflict — an update to the dump state (re-import idempotency).
const fixturesImportZonesSQL = "INSERT INTO zones " +
	"(id, name, name_unicode, is_active, domains_total, domains_checked, updated_at) " +
	"OVERRIDING SYSTEM VALUE " +
	"SELECT z.id, z.name, z.name_unicode, z.is_active, z.domains_total, " +
	"z.domains_checked, z.updated_at FROM unnest(" +
	"$1::int2[], $2::text[], $3::text[], $4::bool[], $5::int8[], $6::int8[], $7::timestamptz[]) " +
	"AS z(id, name, name_unicode, is_active, domains_total, domains_checked, updated_at) " +
	"ON CONFLICT (id) DO UPDATE SET " +
	"name = EXCLUDED.name, name_unicode = EXCLUDED.name_unicode, " +
	"is_active = EXCLUDED.is_active, domains_total = EXCLUDED.domains_total, " +
	"domains_checked = EXCLUDED.domains_checked, updated_at = EXCLUDED.updated_at"

// fixturesImportDomainsSQL — domain import by PK (zone_id, id); the
// label and state come from the dump (UNIQUE (zone_id, label) is covered
// by the pre-check). last_checked_at is cast to date from timestamptz
// with a UTC shift (AT TIME ZONE 'UTC'): a direct cast would take the
// date in the session's TimeZone — with a negative offset (a bench
// outside UTC) the date would slide a day back.
const fixturesImportDomainsSQL = "INSERT INTO domains " +
	"(zone_id, id, label, status, check_state, flags, dns_mask, queued_at, last_checked_at, temp_attempts) " +
	"SELECT d.zone_id, d.id, d.label, d.status, d.check_state, d.flags, d.dns_mask, " +
	"d.queued_at, (d.last_checked_at AT TIME ZONE 'UTC')::date, d.temp_attempts FROM unnest(" +
	"$1::int2[], $2::int8[], $3::text[], $4::int2[], $5::int2[], $6::int2[], " +
	"$7::int8[], $8::timestamptz[], $9::timestamptz[], $10::int2[]) " +
	"AS d(zone_id, id, label, status, check_state, flags, dns_mask, " +
	"queued_at, last_checked_at, temp_attempts) " +
	"ON CONFLICT (zone_id, id) DO UPDATE SET " +
	"label = EXCLUDED.label, status = EXCLUDED.status, " +
	"check_state = EXCLUDED.check_state, flags = EXCLUDED.flags, " +
	"dns_mask = EXCLUDED.dns_mask, queued_at = EXCLUDED.queued_at, " +
	"last_checked_at = EXCLUDED.last_checked_at, temp_attempts = EXCLUDED.temp_attempts"

// fixturesImportResultsSQL — subdomain_results row import by PK
// (zone_id, domain_id, subdomain_id, variant): the columns are the
// format v1 contract (fixturesResultCols, T-242) + render_failed_at (the
// failed stratum); DB columns outside the dump (addrs/cname/tls/
// redirects) are untouched by the import — they were not exported
// (de-identification, §6.1 item 2) and the render task does not need
// them. content_hash is deliberately NOT imported (and is reset to NULL
// by a repeated import): the content store was not exported (T-242, "the
// bench's render writes the DOM itself"), the production reference
// dangles on the bench and breaks the render result write — SwapRefTx
// removes the old content's reference and fails with ErrRefIntegrity on
// the missing contents row (internal/content/refcount.go); NULL is a
// valid "no content" (hasOld=false, the worker writes its own DOM
// without a swap).
const fixturesImportResultsSQL = "INSERT INTO subdomain_results " +
	"(zone_id, domain_id, subdomain_id, variant, scheme, " +
	"http_status, final_url, alias_kind, alias_subdomain_id, " +
	"headers, cookies, rendered, error, response_ms, " +
	"checked_at, rules_version, render_failed_at) " +
	"SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.scheme, " +
	"r.http_status, r.final_url, r.alias_kind, r.alias_subdomain_id, " +
	"r.headers, r.cookies, r.rendered, r.error, r.response_ms, " +
	"r.checked_at, r.rules_version, r.render_failed_at FROM unnest(" +
	"$1::int2[], $2::int8[], $3::int2[], $4::int2[], $5::int2[], " +
	"$6::int2[], $7::text[], $8::int2[], $9::int2[], " +
	"$10::jsonb[], $11::jsonb[], $12::bool[], $13::int2[], " +
	"$14::int4[], $15::timestamptz[], $16::int4[], $17::timestamptz[]) " +
	"AS r(zone_id, domain_id, subdomain_id, variant, scheme, " +
	"http_status, final_url, alias_kind, alias_subdomain_id, " +
	"headers, cookies, rendered, error, response_ms, " +
	"checked_at, rules_version, render_failed_at) " +
	"ON CONFLICT (zone_id, domain_id, subdomain_id, variant) DO UPDATE SET " +
	"scheme = EXCLUDED.scheme, http_status = EXCLUDED.http_status, " +
	"final_url = EXCLUDED.final_url, alias_kind = EXCLUDED.alias_kind, " +
	"alias_subdomain_id = EXCLUDED.alias_subdomain_id, " +
	"headers = EXCLUDED.headers, cookies = EXCLUDED.cookies, " +
	"rendered = EXCLUDED.rendered, content_hash = NULL, " +
	"error = EXCLUDED.error, response_ms = EXCLUDED.response_ms, " +
	"checked_at = EXCLUDED.checked_at, rules_version = EXCLUDED.rules_version, " +
	"render_failed_at = EXCLUDED.render_failed_at"

// Import sequences (the T-242 contract notes): the zones identity — via
// pg_get_serial_sequence (GENERATED ALWAYS AS IDENTITY carries the
// linkage info), domain_id_seq — by literal: a DEFAULT nextval without
// OWNED BY is not linked to the column, pg_get_serial_sequence('domains',
// 'id') would return NULL (migration 000001). GREATEST with the table's
// maximum covers DBs that already have rows with larger ids before the
// fixture; setval(v) sets last_value=v (is_called) — nextval will
// return v+1.
const (
	fixturesZonesSeqSQL = "SELECT setval(pg_get_serial_sequence('zones', 'id'), " +
		"GREATEST((SELECT coalesce(max(id), 0) FROM zones), $1::int))"

	fixturesDomainsSeqSQL = "SELECT setval('domain_id_seq', " +
		"GREATEST((SELECT coalesce(max(id), 0) FROM domains), $1::int8))"
)

// fixturesSweepContentsSQL — cleanup of orphaned content after a run
// (review T-243 P2-1): a repeated import resets the rows' content_hash
// but does not touch contents — the DOM references of the previous run
// were left with refcount > 0 and zero holders: compaction removes only
// refcount = 0, retention picks only rows with live references — the
// content was never freed and grew with every run. On the bench the only
// reference holder is subdomain_results: we sweep contents rows without
// references (compaction of the store will shrink the packs on disk —
// the accounting files do not hold them). Runs with the bench's workers
// stopped (Put before the result write — a transient reference).
const fixturesSweepContentsSQL = "DELETE FROM contents WHERE NOT EXISTS " +
	"(SELECT 1 FROM subdomain_results AS r WHERE r.content_hash = contents.hash)"

// The import plan (S-P3-5) — read-only counters for the dry-run and the
// --force intro output: dump rows already existing by PK (an insert /
// update breakdown) and the content cleanup volume — the same predicate
// as fixturesSweepContentsSQL but counting, without a DELETE.
const (
	fixturesPlanZonesExistSQL = "SELECT count(*) FROM zones AS z " +
		"JOIN unnest($1::int2[]) AS k(id) ON z.id = k.id"

	fixturesPlanDomainsExistSQL = "SELECT count(*) FROM domains AS d " +
		"JOIN unnest($1::int2[], $2::int8[]) AS k(zone_id, id) " +
		"ON d.zone_id = k.zone_id AND d.id = k.id"

	fixturesPlanResultsExistSQL = "SELECT count(*) FROM subdomain_results AS r " +
		"JOIN unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[]) " +
		"AS k(zone_id, domain_id, subdomain_id, variant) " +
		"ON r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant"

	fixturesPlanSweepCountSQL = "SELECT count(*) FROM contents WHERE NOT EXISTS " +
		"(SELECT 1 FROM subdomain_results AS r WHERE r.content_hash = contents.hash)"
)

// ---------------------------------------------------------------- dump

// fixturesDump — a read format v1 dump: the manifest and the file rows
// (the structs are shared with export, T-242). Duplicate keys within the
// files (the T-242 dump is a set, a guard against hand-assembled ones)
// are folded during reading: the last entry wins — the same semantics as
// importing over existing rows.
type fixturesDump struct {
	Manifest fixturesManifest
	Zones    []fixturesZone
	Domains  []fixturesDomain
	Results  []fixturesResult
	Smoke    []fixturesSmokeDomain

	DupZones   int
	DupDomains int
	DupResults int
}

// fixturesReadNDJSON reads a dump file line by line (one JSON object per
// line): a streaming json.Decoder — dump rows (headers/cookies) are not
// size-limited, bufio.Reader keeps memory in check.
func fixturesReadNDJSON[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(bufio.NewReader(f))
	var out []T
	for n := 1; ; n++ {
		var row T
		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: запись %d: %w", path, n, err)
		}
		out = append(out, row)
	}
}

// fixturesDedup folds duplicate keys: the last entry wins (the dump
// state), the counter goes to the report.
func fixturesDedup[T any, K comparable](rows []T, dups *int, key func(T) K) []T {
	if len(rows) < 2 {
		return rows
	}
	pos := make(map[K]int, len(rows))
	out := make([]T, 0, len(rows))
	for _, r := range rows {
		k := key(r)
		if j, ok := pos[k]; ok {
			out[j] = r
			*dups++
		} else {
			pos[k] = len(out)
			out = append(out, r)
		}
	}
	return out
}

// fixturesReadDump reads and validates a format v1 dump. manifest.json
// appears last in the export — its absence means an incomplete dump (the
// T-242 contract) and no import happens. The manifest's file row
// counters are cross-checked against what was actually read (a dump
// truncated while copying is an error, not a silent partial import);
// referential integrity (the domains' zones, the rows' domains) and the
// uniqueness of natural keys (zones.name, the domains' (zone_id, label)
// — the PK dedup does not see them, the INSERT would fail with pgcode
// 23505) are checked before the DB: a truncated or hand-reassembled
// dump fails with a clear error.
func fixturesReadDump(dir string) (*fixturesDump, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("дамп %s: манифест: %w (без manifest.json дамп не завершён — контракт экспорта)", dir, err)
	}
	var manifest fixturesManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("дамп %s: manifest.json: %w", dir, err)
	}
	if manifest.FormatVersion != fixturesFormatVersion {
		return nil, fmt.Errorf("дамп %s: формат v%d не поддерживается (инструмент понимает v%d)",
			dir, manifest.FormatVersion, fixturesFormatVersion)
	}

	dump := &fixturesDump{Manifest: manifest}
	// Reading before the dedup: the manifest's counter is about the file
	// rows, duplicates are folded after the cross-check.
	files := []struct {
		name string
		read func() error
	}{
		{"zones.ndjson", func() (err error) {
			dump.Zones, err = fixturesReadNDJSON[fixturesZone](filepath.Join(dir, "zones.ndjson"))
			return
		}},
		{"domains.ndjson", func() (err error) {
			dump.Domains, err = fixturesReadNDJSON[fixturesDomain](filepath.Join(dir, "domains.ndjson"))
			return
		}},
		{"subdomain_results.ndjson", func() (err error) {
			dump.Results, err = fixturesReadNDJSON[fixturesResult](filepath.Join(dir, "subdomain_results.ndjson"))
			return
		}},
		{"smoke.ndjson", func() (err error) {
			dump.Smoke, err = fixturesReadNDJSON[fixturesSmokeDomain](filepath.Join(dir, "smoke.ndjson"))
			return
		}},
	}
	for _, f := range files {
		if _, err := os.Stat(filepath.Join(dir, f.name)); err != nil {
			return nil, fmt.Errorf("дамп %s: %w", dir, err)
		}
		if err := f.read(); err != nil {
			return nil, fmt.Errorf("дамп %s: %w", dir, err)
		}
	}
	for _, f := range files {
		got := 0
		switch f.name {
		case "zones.ndjson":
			got = len(dump.Zones)
		case "domains.ndjson":
			got = len(dump.Domains)
		case "subdomain_results.ndjson":
			got = len(dump.Results)
		case "smoke.ndjson":
			got = len(dump.Smoke)
		}
		want, ok := manifest.Files[f.name]
		if !ok {
			return nil, fmt.Errorf("дамп %s: манифест без счётчика строк %s (формат v1 его требует)", dir, f.name)
		}
		if want != got {
			return nil, fmt.Errorf("дамп %s: %s: строк %d, манифест насчитал %d — дамп обрезан или пересобран",
				dir, f.name, got, want)
		}
	}

	// Duplicate keys within the files — sets with the last entry winning.
	dump.Zones = fixturesDedup(dump.Zones, &dump.DupZones, func(z fixturesZone) int16 { return z.ID })
	dump.Domains = fixturesDedup(dump.Domains, &dump.DupDomains,
		func(d fixturesDomain) fixturesDomainKey { return fixturesDomainKey{d.ZoneID, d.ID} })
	dump.Results = fixturesDedup(dump.Results, &dump.DupResults,
		func(r fixturesResult) fixturesKey { return fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant} })

	// Identifiers are positive: zero/negative ones (broken hand-made
	// dumps) would bring down both the PK and the queue
	// (queue.Result.validate) — it is more honest to fail before writing,
	// naming the file.
	seenNames := make(map[string]bool, len(dump.Zones))
	for _, z := range dump.Zones {
		if z.ID <= 0 {
			return nil, fmt.Errorf("дамп %s: zones.ndjson: id %d ≤ 0", dir, z.ID)
		}
		if seenNames[z.Name] {
			return nil, fmt.Errorf("дамп %s: zones.ndjson: имя %s дважды с разными id (UNIQUE zones.name — предпроверка наката здесь бессильна)", dir, z.Name)
		}
		seenNames[z.Name] = true
	}
	zoneIDs := make(map[int16]bool, len(dump.Zones))
	for _, z := range dump.Zones {
		zoneIDs[z.ID] = true
	}
	seenLabels := make(map[string]bool, len(dump.Domains))
	for _, d := range dump.Domains {
		if d.ID <= 0 {
			return nil, fmt.Errorf("дамп %s: domains.ndjson: домен %d/%d: id ≤ 0", dir, d.ZoneID, d.ID)
		}
		if !zoneIDs[d.ZoneID] {
			return nil, fmt.Errorf("дамп %s: domains.ndjson: домен %d/%d ссылается на зону, которой нет в zones.ndjson", dir, d.ZoneID, d.ID)
		}
		label := strconv.Itoa(int(d.ZoneID)) + ":" + d.Label
		if seenLabels[label] {
			return nil, fmt.Errorf("дамп %s: domains.ndjson: домен с меткой %s в зоне %d дважды с разными id (UNIQUE (zone_id, label))", dir, d.Label, d.ZoneID)
		}
		seenLabels[label] = true
	}
	domainKeys := make(map[fixturesDomainKey]bool, len(dump.Domains))
	for _, d := range dump.Domains {
		domainKeys[fixturesDomainKey{d.ZoneID, d.ID}] = true
	}
	for _, r := range dump.Results {
		if !domainKeys[fixturesDomainKey{r.ZoneID, r.DomainID}] {
			return nil, fmt.Errorf("дамп %s: subdomain_results.ndjson: строка %d/%d/%d/%v ссылается на домен, которого нет в domains.ndjson",
				dir, r.ZoneID, r.DomainID, r.SubdomainID, r.Variant)
		}
	}
	return dump, nil
}

// ---------------------------------------------------------------- import

// fixturesImportStats — the import result: the applied rows (after the
// dump dedup) and the set sequences (0 — not executed: an empty dump
// file, the sequence is untouched).
type fixturesImportStats struct {
	Zones     int
	Domains   int
	Results   int
	ZoneSeq   int64
	DomainSeq int64
}

// fixturesImportRun — the dump import into the test DB: natural-key
// pre-checks → zones (OVERRIDING SYSTEM VALUE) → domains → result rows →
// sequences. Every statement autocommits (a one-off tool): an
// interrupted import is repeated in full, the PK idempotency finishes
// the same state.
func fixturesImportRun(ctx context.Context, pool db.DB, dump *fixturesDump, stdout io.Writer) (*fixturesImportStats, error) {
	if err := fixturesCheckNaturalKeys(ctx, pool, dump); err != nil {
		return nil, err
	}

	stats := &fixturesImportStats{}
	var err error
	if stats.Zones, err = fixturesImportZones(ctx, pool, dump.Zones); err != nil {
		return nil, err
	}
	if stats.Domains, err = fixturesImportDomains(ctx, pool, dump.Domains); err != nil {
		return nil, err
	}
	if stats.Results, err = fixturesImportResults(ctx, pool, dump.Results); err != nil {
		return nil, err
	}
	if len(dump.Zones) > 0 {
		if stats.ZoneSeq, err = fixturesSetval(ctx, pool, fixturesZonesSeqSQL, maxDumpZoneID(dump.Zones)); err != nil {
			return nil, err
		}
	}
	if len(dump.Domains) > 0 {
		if stats.DomainSeq, err = fixturesSetval(ctx, pool, fixturesDomainsSeqSQL, maxDumpDomainID(dump.Domains)); err != nil {
			return nil, err
		}
	}
	// Cleanup of the previous run's orphaned content (fixturesSweepContentsSQL):
	// resetting the rows' content_hash would leave contents references
	// without holders. The bench's workers are stopped at this moment
	// (the run instructions).
	var swept int64
	if len(dump.Results) > 0 {
		tag, err := pool.Exec(ctx, fixturesSweepContentsSQL)
		if err != nil {
			return nil, fmt.Errorf("накат: уборка осиротевшего контента: %w", err)
		}
		swept = tag.RowsAffected()
		if swept > 0 {
			out(stdout, "осиротевший контент прогона убран: %d строк contents\n", swept)
		}
	}

	out(stdout, "накат: зоны %d, домены %d, строки результатов %d\n", stats.Zones, stats.Domains, stats.Results)
	if dump.DupZones+dump.DupDomains+dump.DupResults > 0 {
		out(stdout, "дубли ключей в дампе (схлопнуты): зоны %d, домены %d, результаты %d\n",
			dump.DupZones, dump.DupDomains, dump.DupResults)
	}
	out(stdout, "последовательности: zones.id → %d, domain_id_seq → %d\n", stats.ZoneSeq, stats.DomainSeq)
	return stats, nil
}

// fixturesCheckNaturalKeys — zones with the same name and domains with
// the same label but a different id: a UNIQUE conflict invisible to ON
// CONFLICT by PK. The import assumes a clean test DB (§6.1 item 3); a
// conflict is an error with a list, not a pgcode 23505 from the depths
// of the INSERT.
func fixturesCheckNaturalKeys(ctx context.Context, pool db.DB, dump *fixturesDump) error {
	if len(dump.Zones) > 0 {
		names := make([]string, 0, len(dump.Zones))
		dumpID := make(map[string]int16, len(dump.Zones))
		for _, z := range dump.Zones {
			names = append(names, z.Name)
			dumpID[z.Name] = z.ID
		}
		rows, err := pool.Query(ctx, fixturesZoneByNameSQL, names)
		if err != nil {
			return fmt.Errorf("накат: проверка имён зон: %w", err)
		}
		defer rows.Close()
		var conflicts []string
		for rows.Next() {
			var id int16
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				return fmt.Errorf("накат: проверка имён зон: чтение: %w", err)
			}
			if want, ok := dumpID[name]; ok && want != id {
				conflicts = append(conflicts, fmt.Sprintf("%s (id дампа %d, id БД %d)", name, want, id))
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("накат: проверка имён зон: %w", err)
		}
		if len(conflicts) > 0 {
			return fmt.Errorf("накат: зоны с тем же именем уже в БД с другими id: %s — тестовая БД должна быть чистой (миграции без данных, ТЗ §6.1 п. 3)",
				strings.Join(conflicts, ", "))
		}
	}

	if len(dump.Domains) > 0 {
		zones := make([]int16, 0, len(dump.Domains))
		labels := make([]string, 0, len(dump.Domains))
		// The domain's natural key is the (zone_id, label) pair: a map by
		// the string key zone:label (number:text is unambiguous, ':' is
		// invalid in a DNS label) → the dump id.
		byLabel := make(map[string]int64, len(dump.Domains))
		for _, d := range dump.Domains {
			zones = append(zones, d.ZoneID)
			labels = append(labels, d.Label)
			byLabel[strconv.Itoa(int(d.ZoneID))+":"+d.Label] = d.ID
		}
		rows, err := pool.Query(ctx, fixturesDomainByLabelSQL, zones, labels)
		if err != nil {
			return fmt.Errorf("накат: проверка меток доменов: %w", err)
		}
		defer rows.Close()
		var conflicts []string
		for rows.Next() {
			var zone int16
			var label string
			var id int64
			if err := rows.Scan(&zone, &label, &id); err != nil {
				return fmt.Errorf("накат: проверка меток доменов: чтение: %w", err)
			}
			if want, ok := byLabel[strconv.Itoa(int(zone))+":"+label]; ok && want != id {
				conflicts = append(conflicts, fmt.Sprintf("зона %d/%s (id дампа %d, id БД %d)", zone, label, want, id))
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("накат: проверка меток доменов: %w", err)
		}
		if len(conflicts) > 0 {
			return fmt.Errorf("накат: домены с той же меткой уже в БД с другими id: %s — тестовая БД должна быть чистой (миграции без данных, ТЗ §6.1 п. 3)",
				strings.Join(conflicts, ", "))
		}
	}
	return nil
}

// fixturesImportZones — zone import in batches (a handful of zones in a
// dump, the batch is the same mechanism). Returns the number of applied
// rows.
func fixturesImportZones(ctx context.Context, pool db.DB, zones []fixturesZone) (int, error) {
	applied := 0
	for start := 0; start < len(zones); start += fixturesImportBatch {
		end := min(start+fixturesImportBatch, len(zones))
		part := zones[start:end]
		ids := make([]int16, len(part))
		names := make([]string, len(part))
		unicode := make([]*string, len(part))
		active := make([]bool, len(part))
		total := make([]int64, len(part))
		checked := make([]int64, len(part))
		updated := make([]*time.Time, len(part))
		for i, z := range part {
			ids[i], names[i], active[i] = z.ID, z.Name, z.IsActive
			total[i], checked[i] = z.DomainsTotal, z.DomainsChecked
			if z.NameUnicode != nil {
				s := *z.NameUnicode
				unicode[i] = &s
			}
			if z.UpdatedAt != nil {
				t := *z.UpdatedAt
				updated[i] = &t
			}
		}
		tag, err := pool.Exec(ctx, fixturesImportZonesSQL,
			ids, names, unicode, active, total, checked, updated)
		if err != nil {
			return applied, fmt.Errorf("накат зон (пачка %d…%d): %w", start, end, err)
		}
		if int(tag.RowsAffected()) != len(part) {
			return applied, fmt.Errorf("накат зон (пачка %d…%d): применено %d строк из %d", start, end, tag.RowsAffected(), len(part))
		}
		applied += len(part)
	}
	return applied, nil
}

// fixturesImportDomains — domain import in fixturesImportBatch batches.
func fixturesImportDomains(ctx context.Context, pool db.DB, domains []fixturesDomain) (int, error) {
	applied := 0
	for start := 0; start < len(domains); start += fixturesImportBatch {
		end := min(start+fixturesImportBatch, len(domains))
		part := domains[start:end]
		zones := make([]int16, len(part))
		ids := make([]int64, len(part))
		labels := make([]string, len(part))
		status := make([]int16, len(part))
		checkState := make([]int16, len(part))
		flags := make([]int16, len(part))
		dnsMask := make([]int64, len(part))
		queued := make([]*time.Time, len(part))
		lastChecked := make([]*time.Time, len(part))
		tempAttempts := make([]int16, len(part))
		for i, d := range part {
			zones[i], ids[i], labels[i] = d.ZoneID, d.ID, d.Label
			status[i], checkState[i], flags[i] = d.Status, d.CheckState, d.Flags
			dnsMask[i], tempAttempts[i] = d.DNSMask, d.TempAttempts
			if d.QueuedAt != nil {
				t := *d.QueuedAt
				queued[i] = &t
			}
			if d.LastCheckedAt != nil {
				t := *d.LastCheckedAt
				lastChecked[i] = &t
			}
		}
		tag, err := pool.Exec(ctx, fixturesImportDomainsSQL,
			zones, ids, labels, status, checkState, flags, dnsMask, queued, lastChecked, tempAttempts)
		if err != nil {
			return applied, fmt.Errorf("накат доменов (пачка %d…%d): %w", start, end, err)
		}
		if int(tag.RowsAffected()) != len(part) {
			return applied, fmt.Errorf("накат доменов (пачка %d…%d): применено %d строк из %d", start, end, tag.RowsAffected(), len(part))
		}
		applied += len(part)
	}
	return applied, nil
}

// fixturesImportResults — subdomain_results row import in batches. jsonb
// (headers/cookies) goes as text into jsonb[] (the recordInsertSQL
// style, T-058); NULL columns are nil array elements. content_hash is
// not passed at all (see fixturesImportResultsSQL): the column gets
// DEFAULT NULL.
func fixturesImportResults(ctx context.Context, pool db.DB, rowsIn []fixturesResult) (int, error) {
	applied := 0
	for start := 0; start < len(rowsIn); start += fixturesImportBatch {
		end := min(start+fixturesImportBatch, len(rowsIn))
		part := rowsIn[start:end]
		n := len(part)
		zones := make([]int16, n)
		domains := make([]int64, n)
		subs := make([]int16, n)
		variants := make([]int16, n)
		scheme := make([]int16, n)
		httpStatus := make([]*int16, n)
		finalURL := make([]*string, n)
		aliasKind := make([]int16, n)
		aliasSub := make([]*int16, n)
		headers := make([]*string, n)
		cookies := make([]*string, n)
		rendered := make([]bool, n)
		errCode := make([]*int16, n)
		responseMS := make([]*int32, n)
		checkedAt := make([]time.Time, n)
		rulesVersion := make([]*int32, n)
		renderFailedAt := make([]*time.Time, n)
		for i, r := range part {
			zones[i], domains[i], subs[i], variants[i] = r.ZoneID, r.DomainID, r.SubdomainID, r.Variant
			scheme[i], aliasKind[i], rendered[i] = r.Scheme, r.AliasKind, r.Rendered
			checkedAt[i] = r.CheckedAt
			if r.HTTPStatus != nil {
				v := *r.HTTPStatus
				httpStatus[i] = &v
			}
			if r.FinalURL != nil {
				v := *r.FinalURL
				finalURL[i] = &v
			}
			if r.AliasSubdomainID != nil {
				v := *r.AliasSubdomainID
				aliasSub[i] = &v
			}
			headers[i] = fixturesJSONText(r.Headers)
			cookies[i] = fixturesJSONText(r.Cookies)
			if r.Error != nil {
				v := *r.Error
				errCode[i] = &v
			}
			if r.ResponseMs != nil {
				v := *r.ResponseMs
				responseMS[i] = &v
			}
			if r.RulesVersion != nil {
				v := *r.RulesVersion
				rulesVersion[i] = &v
			}
			if r.RenderFailedAt != nil {
				v := *r.RenderFailedAt
				renderFailedAt[i] = &v
			}
		}
		tag, err := pool.Exec(ctx, fixturesImportResultsSQL,
			zones, domains, subs, variants, scheme,
			httpStatus, finalURL, aliasKind, aliasSub,
			headers, cookies, rendered, errCode, responseMS,
			checkedAt, rulesVersion, renderFailedAt)
		if err != nil {
			return applied, fmt.Errorf("накат результатов (пачка %d…%d): %w", start, end, err)
		}
		if int(tag.RowsAffected()) != n {
			return applied, fmt.Errorf("накат результатов (пачка %d…%d): применено %d строк из %d", start, end, tag.RowsAffected(), n)
		}
		applied += n
	}
	return applied, nil
}

// fixturesJSONText — the dump's jsonb (json.RawMessage) into a text[]
// element for the jsonb[] cast: nil (NULL in the dump) stays nil.
func fixturesJSONText(raw json.RawMessage) *string {
	if len(raw) == 0 {
		return nil
	}
	s := string(raw)
	return &s
}

// fixturesSetval sets a sequence and returns the new value (for the
// report).
func fixturesSetval(ctx context.Context, pool db.DB, sql string, maxID int64) (int64, error) {
	rows, err := pool.Query(ctx, sql, maxID)
	if err != nil {
		return 0, fmt.Errorf("накат: последовательность: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("накат: последовательность: пустой ответ setval")
	}
	var v int64
	if err := rows.Scan(&v); err != nil {
		return 0, fmt.Errorf("накат: последовательность: чтение: %w", err)
	}
	return v, rows.Err()
}

// maxDumpZoneID — the maximum id of the dump's zones (for setval).
func maxDumpZoneID(zones []fixturesZone) int64 {
	var max int64
	for _, z := range zones {
		if int64(z.ID) > max {
			max = int64(z.ID)
		}
	}
	return max
}

// maxDumpDomainID — the maximum id of the dump's domains.
func maxDumpDomainID(domains []fixturesDomain) int64 {
	var max int64
	for _, d := range domains {
		if d.ID > max {
			max = d.ID
		}
	}
	return max
}

// ---------------------------------------------------------------- plan (S-P3-5)

// fixturesImportPlan — the import plan: the target from the DSN without
// the secret and the per-table counters — everything the operator must
// see before the first write. Built with SELECTs only: the dry-run
// (without --force) and the intro output before a real import are the
// same plan.
type fixturesImportPlan struct {
	Target        string // user@host:port/dbname — no password (fixturesImportTarget)
	Zones         int    // dump rows to import (after the dedup)
	ZonesExist    int    // of them already in the DB by PK → ON CONFLICT DO UPDATE
	Domains       int
	DomainsExist  int
	Results       int
	ResultsExist  int
	SweepContents int64 // contents rows the import will sweep (orphans without live references)
	MaxZoneID     int64 // the sequences will be raised to at least the dump's maximum
	MaxDomainID   int64
	DupZones      int
	DupDomains    int
	DupResults    int
}

// fixturesImportTarget — the import target from the DSN for the operator
// output: user@host:port/dbname without the password and query secrets
// (S-P3-5: the import overwrites rows, the target must be on screen
// before writing — but not the whole DSN). A DSN not in URL form
// (keyword/value) and unparsable strings — db.RedactDSN: the password is
// cut out, the structure is not revealed further.
func fixturesImportTarget(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" || u.Path == "" {
		return db.RedactDSN(dsn, "DSN postgres без host/dbname — см. postgres.dsn (конфигурация)")
	}
	user := ""
	if u.User != nil {
		user = u.User.Username() + "@"
	}
	return user + u.Host + u.Path // host[:port] + /dbname
}

// fixturesImportPlanBuild — the plan over the dump and the current DB
// state: natural-key pre-checks (the same as the import's — failing is
// more honest to see in the dry-run, before --force), the dump rows
// broken down into inserts and updates by existing PKs, the content
// sweep volume. Read-only.
func fixturesImportPlanBuild(ctx context.Context, pool db.DB, dump *fixturesDump, target string) (*fixturesImportPlan, error) {
	if err := fixturesCheckNaturalKeys(ctx, pool, dump); err != nil {
		return nil, err
	}
	plan := &fixturesImportPlan{
		Target:      target,
		Zones:       len(dump.Zones),
		Domains:     len(dump.Domains),
		Results:     len(dump.Results),
		MaxZoneID:   maxDumpZoneID(dump.Zones),
		MaxDomainID: maxDumpDomainID(dump.Domains),
		DupZones:    dump.DupZones,
		DupDomains:  dump.DupDomains,
		DupResults:  dump.DupResults,
	}
	if len(dump.Zones) > 0 {
		ids := make([]int16, len(dump.Zones))
		for i, z := range dump.Zones {
			ids[i] = z.ID
		}
		n, err := fixturesCountRows(ctx, pool, fixturesPlanZonesExistSQL, ids)
		if err != nil {
			return nil, fmt.Errorf("план наката: существующие зоны: %w", err)
		}
		plan.ZonesExist = int(n)
	}
	if len(dump.Domains) > 0 {
		zones := make([]int16, len(dump.Domains))
		ids := make([]int64, len(dump.Domains))
		for i, d := range dump.Domains {
			zones[i], ids[i] = d.ZoneID, d.ID
		}
		n, err := fixturesCountRows(ctx, pool, fixturesPlanDomainsExistSQL, zones, ids)
		if err != nil {
			return nil, fmt.Errorf("план наката: существующие домены: %w", err)
		}
		plan.DomainsExist = int(n)
	}
	if len(dump.Results) > 0 {
		n, err := fixturesCountRows(ctx, pool, fixturesPlanSweepCountSQL)
		if err != nil {
			return nil, fmt.Errorf("план наката: уборка контента: %w", err)
		}
		plan.SweepContents = n
		zones := make([]int16, len(dump.Results))
		domains := make([]int64, len(dump.Results))
		subs := make([]int16, len(dump.Results))
		variants := make([]int16, len(dump.Results))
		for i, r := range dump.Results {
			zones[i], domains[i], subs[i], variants[i] = r.ZoneID, r.DomainID, r.SubdomainID, r.Variant
		}
		if n, err = fixturesCountRows(ctx, pool, fixturesPlanResultsExistSQL, zones, domains, subs, variants); err != nil {
			return nil, fmt.Errorf("план наката: существующие строки результатов: %w", err)
		}
		plan.ResultsExist = int(n)
	}
	return plan, nil
}

// fixturesCountRows — the plan's scalar count query.
func fixturesCountRows(ctx context.Context, pool db.DB, sql string, args ...any) (int64, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, errors.New("пустой ответ count")
	}
	var n int64
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("чтение count: %w", err)
	}
	return n, rows.Err()
}

// fixturesPrintPlan — the import plan: the target (verify this is the
// bench, not production), rows per table with an insert/update
// breakdown, the sequences, the dump's duplicate keys, the swept
// content.
func fixturesPrintPlan(w io.Writer, plan *fixturesImportPlan, mode string) {
	out(w, "цель наката: %s (режим: %s)\n", plan.Target, mode)
	out(w, "план: зоны %d (вставка %d, обновление %d), домены %d (вставка %d, обновление %d), "+
		"строки результатов %d (вставка %d, обновление %d)\n",
		plan.Zones, plan.Zones-plan.ZonesExist, plan.ZonesExist,
		plan.Domains, plan.Domains-plan.DomainsExist, plan.DomainsExist,
		plan.Results, plan.Results-plan.ResultsExist, plan.ResultsExist)
	out(w, "последовательности: zones.id → не ниже %d, domain_id_seq → не ниже %d\n",
		plan.MaxZoneID, plan.MaxDomainID)
	out(w, "дубли ключей дампа (схлопнуты): зоны %d, домены %d, результаты %d\n",
		plan.DupZones, plan.DupDomains, plan.DupResults)
	out(w, "выметается из contents: %d строк без живых ссылок\n", plan.SweepContents)
}

// fixturesPublishCount — the publishing tasks per mode (a plan line):
// all — all dump rows, smoke — the subsample domain rows (the same
// filter as fixturesPublishRun).
func fixturesPublishCount(dump *fixturesDump, mode string) int {
	if mode != fixturesPublishSmoke {
		return len(dump.Results)
	}
	smokeKeys := make(map[fixturesDomainKey]bool, len(dump.Smoke))
	for _, s := range dump.Smoke {
		smokeKeys[fixturesDomainKey{s.ZoneID, s.DomainID}] = true
	}
	n := 0
	for _, r := range dump.Results {
		if smokeKeys[fixturesDomainKey{r.ZoneID, r.DomainID}] {
			n++
		}
	}
	return n
}

// fixturesImportOpts — the import execution parameters.
type fixturesImportOpts struct {
	Force     bool   // write; without the flag — a dry-run
	DSN       string // the target from the DSN (no secret) for the plan
	Publish   string // the publishing mode: "", all, smoke (only with Force)
	RedisAddr string // the publishing target for the plan
}

// fixturesImportApply — the plan and the import: before the first write
// — the target from the DSN and the full plan; without Force — a dry-run,
// returning without writing (the plan's SELECTs are not writes); with
// Force — the plan is printed and the import runs. Extracted from
// fixturesImportCmd for flag tests without configuration and a DB.
func fixturesImportApply(ctx context.Context, pool db.DB, dump *fixturesDump, opts fixturesImportOpts, stdout io.Writer) error {
	plan, err := fixturesImportPlanBuild(ctx, pool, dump, fixturesImportTarget(opts.DSN))
	if err != nil {
		return err
	}
	mode := "dry-run"
	if opts.Force {
		mode = "force"
	}
	fixturesPrintPlan(stdout, plan, mode)
	if opts.Publish != "" {
		out(stdout, "план публикации q:render (%s): задач %d → Redis %s\n",
			opts.Publish, fixturesPublishCount(dump, opts.Publish), opts.RedisAddr)
	}
	if !opts.Force {
		out(stdout, "dry-run: БД не изменена — накат выполнит та же команда с --force\n")
		return nil
	}
	_, err = fixturesImportRun(ctx, pool, dump, stdout)
	return err
}

// ---------------------------------------------------------------- publishing

// fixturesPublishStats — the publishing result: published tasks, skipped
// duplicate keys, the breakdown by the dump's strata and (in smoke mode)
// the subsample domains.
type fixturesPublishStats struct {
	Published    int
	SkippedDups  int
	Strata       map[string]int
	SmokeDomains int
}

// fixturesPublishRun — publishing the fixture tasks into q:render
// (§6.1 item 4): one queue.Result per dump row — the same contract as
// the q:render producers (renderq.go: the HTTP stage and the rework
// pass); Fresh is not set — the render worker itself marks Fresh its
// q:analyze publishing (worker.go). The stream is the bench
// configuration's constant; the T-174 gates do not participate: the bench
// publishes once into an empty queue (debt/depth is the live pipeline's
// concern, not an isolated run's).
func fixturesPublishRun(ctx context.Context, q queue.Queue, dump *fixturesDump, mode string, stdout io.Writer) (*fixturesPublishStats, error) {
	if mode != fixturesPublishAll && mode != fixturesPublishSmoke {
		return nil, fmt.Errorf("публикация: неизвестный режим %q (допустимы: %s, %s)", mode, fixturesPublishAll, fixturesPublishSmoke)
	}

	// The domain name — from the dump rows (label.zone, punycode): the
	// q:render producers write it for logs and manual analysis (spec
	// 4.2). An absence (a dump desync is excluded by reading) — an empty
	// name; the queue.Result contract makes the name optional.
	labels := make(map[fixturesDomainKey]string, len(dump.Domains))
	for _, d := range dump.Domains {
		labels[fixturesDomainKey{d.ZoneID, d.ID}] = d.Label
	}
	zoneNames := make(map[int16]string, len(dump.Zones))
	for _, z := range dump.Zones {
		zoneNames[z.ID] = z.Name
	}
	nameOf := func(r fixturesResult) string {
		label, ok := labels[fixturesDomainKey{r.ZoneID, r.DomainID}]
		if !ok {
			return ""
		}
		zone := zoneNames[r.ZoneID]
		if zone == "" {
			return label
		}
		return label + "." + zone
	}

	// Smoke mode — only the rows of the smoke subsample domains (§6.1 item 6).
	selection := dump.Results
	stats := &fixturesPublishStats{Strata: map[string]int{}}
	if mode == fixturesPublishSmoke {
		smokeKeys := make(map[fixturesDomainKey]bool, len(dump.Smoke))
		for _, s := range dump.Smoke {
			smokeKeys[fixturesDomainKey{s.ZoneID, s.DomainID}] = true
		}
		stats.SmokeDomains = len(dump.Smoke)
		selection = nil
		for _, r := range dump.Results {
			if smokeKeys[fixturesDomainKey{r.ZoneID, r.DomainID}] {
				selection = append(selection, r)
			}
		}
		if len(selection) == 0 {
			// An empty smoke subsample with code 0 is indistinguishable to
			// the operator from a working run (a degraded dump, the wrong
			// directory) — an honest refusal: the calling script must not
			// count the run as having happened.
			return stats, fmt.Errorf("публикация: smoke-подвыборка пуста (доменов %d, задач 0) — публиковать нечего", stats.SmokeDomains)
		}
	}

	// Duplicate keys (a set of tasks — the same invariant as the T-242
	// dump) are skipped with a count: each row is exactly one task.
	seen := make(map[fixturesKey]bool, len(selection))
	for _, r := range selection {
		k := fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant}
		if seen[k] {
			stats.SkippedDups++
			continue
		}
		seen[k] = true
		msg := queue.Result{
			ZoneID:      r.ZoneID,
			DomainID:    r.DomainID,
			SubdomainID: r.SubdomainID,
			Variant:     model.Variant(r.Variant),
			Name:        nameOf(r),
		}
		if _, err := q.Publish(ctx, queue.StreamRender, msg); err != nil {
			return stats, fmt.Errorf("публикация q:render: опубликовано %d, прервано (зона %d, домен %d): %w",
				stats.Published, r.ZoneID, r.DomainID, err)
		}
		stats.Published++
		stats.Strata[r.Stratum]++
	}

	switch mode {
	case fixturesPublishSmoke:
		out(stdout, "публикация q:render (smoke, домены %d): опубликовано %d (ok %d, failed %d, heavy %d)\n",
			stats.SmokeDomains, stats.Published, stats.Strata["ok"], stats.Strata["failed"], stats.Strata["heavy"])
	default:
		out(stdout, "публикация q:render: опубликовано %d (ok %d, failed %d, heavy %d)\n",
			stats.Published, stats.Strata["ok"], stats.Strata["failed"], stats.Strata["heavy"])
	}
	if stats.SkippedDups > 0 {
		out(stdout, "дубли ключей задач пропущены: %d\n", stats.SkippedDups)
	}
	return stats, nil
}

// ---------------------------------------------------------------- command

// fixturesImportCmd — crawlerctl fixtures import: flag parsing, dump
// reading (local checks before infrastructure), the plan and the import
// behind --force (S-P3-5: without the flag — a dry-run, --publish
// complements --force), publishing. Connections follow the reanalyze
// pattern: configuration + environment (internal/config), pool 2 (the
// spec 6.3 budget), Redis — the redis section's address.
func fixturesImportCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures import")
	dir := fs.String("dir", "", "каталог дампа формата v1 (обязателен)")
	publish := fs.String("publish", "", "публикация задач фикстуры в q:render без DNS/HTTP: "+fixturesPublishAll+
		" — все записи дампа, "+fixturesPublishSmoke+" — домены smoke-подвыборки (только с --force)")
	force := fs.Bool("force", false,
		"выполнить накат (без флага — dry-run: цель из DSN и полный план без записи)")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*dir) == "" {
		return usagef("укажите каталог дампа: crawlerctl fixtures import --dir /path/to/fixtures")
	}
	switch *publish {
	case "", fixturesPublishAll, fixturesPublishSmoke:
	default:
		return usagef("fixtures import: --publish: неизвестный режим %q (допустимы: %s, %s)",
			*publish, fixturesPublishAll, fixturesPublishSmoke)
	}
	if *publish != "" && !*force {
		return usagef("fixtures import: --publish дополняет --force (dry-run ничего не накатывает и не публикует)")
	}
	if fs.NArg() > 0 {
		return usagef("fixtures import: лишние аргументы %v (команда принимает только флаги)", fs.Args())
	}

	ctx, stop := signalCtx()
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // connection budget per spec 6.3 — same as the services
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	defer pool.Close()

	dump, err := fixturesReadDump(*dir)
	if err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	if err := fixturesImportApply(ctx, pool, dump, fixturesImportOpts{
		Force:     *force,
		DSN:       cfg.Postgres.DSN,
		Publish:   *publish,
		RedisAddr: cfg.Redis.Addr,
	}, stdout); err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	if !*force || *publish == "" {
		return nil
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ per stream (T-025)
	})
	if _, err := fixturesPublishRun(ctx, q, dump, *publish, stdout); err != nil {
		return fmt.Errorf("fixtures import: %w", err)
	}
	return nil
}
