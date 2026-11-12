// ---------------------------------------------------------------- fixtures
//
// crawlerctl fixtures (T-242, the render engine selection task spec
// tasks/TZ_render_engines.md §6.1 items 1–2): export of a domain fixture
// from the production DB for the local render engine comparison bench
// (§5.1) and smoke runs of pinned-version changes (§6.1 item 6). A
// one-off stage tool: the export reads the DB; dump import and task
// publishing — fixtures import (T-243, fixtures_import.go); the content
// store is not exported — the DOM on the bench is written by the render
// itself, the baseline is captured there too.
//
// The selection (§6.1 item 1) — subdomain_results rows that were real
// q:render tasks: the base predicate final_url IS NOT NULL is the render
// target criterion of the rework pass selection (renderPendingRowsSQL,
// T-057: the SPA verdict passed, the HTTP episode record was published
// to q:render). Strata:
//
//	ok ~2000     rendered — a successful chromium render (the bench
//	             baseline);
//	failed ~500  render_failed_at IS NOT NULL within the last --weeks
//	             weeks — finalizations without a render: tarpits
//	             T-148/timeouts/task class T-172 (potential fallback
//	             clients) AND engine-independent cert T-159/DNS T-211
//	             (not clients, FR-7) — the class is not stored in the
//	             row and cannot be split (see the format contract
//	             below);
//	heavy ~500   rendered with the largest content (contents.raw_len —
//	             the DOM size reference) — SPA-heavy.
//
// ok and heavy do not intersect (heavy is selected first, ok excludes
// its keys with an anti-join), failed does not intersect them by the
// T-210 marker invariant ("render_failed_at set ⇒ rendered = false");
// duplicate keys are additionally cut off by the selection collector.
// Zone proportions — a zone's quota equals its share of the stratum
// (the largest remainder method); within a zone the rows are ordered by
// a deterministic pseudo-random md5(seed:key) hash — re-exporting with
// the same DB state and the same --seed reproduces the selection
// (export idempotency). Fewer available rows than the quota (a dev DB,
// a fresh production) — not an error: all are taken, and the degradation
// is honestly recorded in the output and the manifest.
//
// The dump format (fixtures format v1) — a directory of five files:
//
//	zones.ndjson              rows of the zones reference table: the
//	                          columns id/name/name_unicode/is_active/
//	                          domains_total/domains_checked/updated_at;
//	                          the live zone progress counters
//	                          (domains_laravel, domains_pending,
//	                          domains_queued) are not exported — the
//	                          bench's monitor recomputes them (an
//	                          explicit column list, not "all": an
//	                          "all columns" wording would disagree with
//	                          the schema on every migration)
//	domains.ndjson            rows of domains for the selected domains
//	                          (all columns — domain identity and state)
//	subdomain_results.ndjson  the selected rows: identifiers, render task
//	                          fields (final_url — the render target,
//	                          cookies/headers — the episode context,
//	                          alias_* — T-055 verdict inheritance) and
//	                          stratum diagnostics (rendered,
//	                          render_failed_at, error, response_ms) +
//	                          the service field stratum
//	smoke.ndjson              keys of the smoke subsample (zone_id,
//	                          domain_id, name) — domains of the reduced
//	                          §6.1 item 6 run
//	manifest.json             metadata: the format version, seed, quotas
//	                          and actual stratum/zone counters,
//	                          degradations
//
// NDJSON: one JSON object per line, UTF-8, without HTML escaping. Row
// identifiers are kept original — re-import is idempotent by primary
// keys (INSERT … ON CONFLICT — the T-243 import). Values: bytea → base64
// (content_hash — 24 chars); timestamptz and date → RFC 3339; jsonb
// (headers/cookies) → nested JSON as-is; NULL of any column → null. The
// addrs/cname/tls/redirects columns are not exported: the render task
// does not need them, de-identification per §6.1 item 2 (final_url is
// public by the task definition; the spec explicitly asks to export
// cookies/headers).
//
// The failed stratum — all finalizations without a render by the T-210
// marker: tarpit T-148, task class T-172, cert T-159, DNS T-211. The
// finalization class is not stored in the subdomain_results row (the
// error column is about the HTTP phase), so the engine-independent
// cert/DNS (not fallback clients, FR-7) cannot be separated without a
// schema change — their share is visible on the bench via the
// finalization reasons (the taskFinalizeReason dictionary, §4.6);
// recorded for interpreting the bench results.
//
// Each file is written to a temporary name and renamed after a
// successful close; manifest.json appears last — its presence means a
// completed dump (an interrupted export leaves no half-files under the
// final names). The previous manifest.json is removed before writing the
// first data file of a re-export — a run that fails mid-write does not
// leave an old manifest next to the new files.
//
// Re-export idempotency: the SELECTION COMPOSITION and the fields of the
// subdomain_results/smoke rows are deterministic (with unchanged data
// and the same --seed — byte for byte). The domains/zones rows are a
// snapshot of the state at export time: live fields (domains.status/
// check_state/flags/dns_mask/queued_at/last_checked_at, zone counters
// and updated_at, the manifest's created_at) change between runs
// together with the DB; this does not hinder the T-243 import (row keys
// are stable), the bench monitor recomputes the zone counters.
//
// The T-243 import (schema notes): zones.id — GENERATED ALWAYS AS
// IDENTITY (inserting original ids — OVERRIDING SYSTEM VALUE),
// domains.id — nextval('domain_id_seq') (after the import — setval above
// the maximum exported id, otherwise the bench's new domains would start
// at 1 and collide with the fixture).

package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
)

// fixturesHelp — help text for crawlerctl fixtures [--help]; it is also
// printed for fixtures <action> --help.
const fixturesHelp = `crawlerctl fixtures — the render engine comparison bench fixtures (T-242/T-243/T-244)

Usage:
  crawlerctl fixtures export --out DIR [--ok N] [--failed N]
                              [--heavy N] [--smoke N] [--weeks N]
                              [--seed N]
  crawlerctl fixtures import --dir DIR [--force] [--publish all|smoke]
  crawlerctl fixtures snapshot --dir DIR --label NAME --engine NAME
                               [--metrics URL] [--engine-rss-max BYTES]
                               [--engine-crashes N] [--out FILE] [--no-dom]
  crawlerctl fixtures report --baseline FILE --candidate FILE
                               [--out FILE] [--memory-budget BYTES]
                               [--fail-exit]

Exporting a domain fixture from the DB (the render spec §6.1 items 1–2):
a selection of subdomain_results rows — real q:render tasks (final_url
after the SPA verdict T-057) — by the strata ok (~2000 random successful
at chromium) / failed (~500 problematic of the recent weeks: finalizations
without a render by the render_failed_at marker T-210 — tarpits/task class
T-172, including the engine-independent cert/DNS: the class is not stored
in the row, their share is visible on the bench via the finalization
reasons) / heavy (~500 SPA-heavy — the largest content), proportional to
the zone shares. Plus a smoke subsample of domains (~300) for the shortened
runs of engine version changes. The content store is not exported (the DOM
is written by the bench render).

  --out DIR      the dump directory (required); created, the existing dump
                 files are overwritten
  --ok N         the ok quota (the default 2000)
  --failed N     the failed quota (the default 500)
  --heavy N      the SPA-heavy quota (the default 500)
  --smoke N      the number of smoke domains (the default 300)
  --weeks N      the failed freshness window, weeks (the default 4)
  --seed N       the deterministic selection seed (the default 242):
                 the same seed and the same data — the same dump rows

import — an idempotent import of a format v1 dump into a test DB (§6.1
item 3) and publishing of the fixture tasks straight into q:render (§6.1
item 4) — the render stage is tested in isolation, without DNS/HTTP. The
import is a write without a rollback (overwrites the zones/domains/result
rows by PK and sweeps the contents): without --force — a dry-run, the
target (user@host:port/dbname from the DSN, no password — verify it is
the bench, not production) and the full plan (rows per table
insert/update, key duplicates, the swept content) are printed without a
single write; --force — the import, the target and the plan are printed
before the first write. The import goes over the primary keys from the
dump keys (a repeated import does not duplicate but restores the dump
state — the runs are compared from the same start); the zones enter past
the identity (OVERRIDING SYSTEM VALUE), the zones.id and domain_id_seq
sequences are set above the maximum; the dump's content links are reset
(contents was not exported — the DOM is written by the bench itself). The
DB must be clean (the dev-stack migrations without data): a zone/domain
with the same name but a different id — a conflict, the import stops
before writing.

  --dir DIR       the dump directory (required; the format v1 — export)
  --force         perform the import (without the flag — dry-run: the target
                  and the plan without writes; verify the target — a test-DB
                  tool)
  --publish MODE  publishing the fixture tasks into q:render (only with
                  --force): all — all the dump rows,
                  smoke — the smoke subsample domains. The publishing goes
                  into the configuration's Redis (redis.addr) — the bench
                  worker must read the same instance. A repeated run with
                  --publish adds the rows
                  again: clear q:render between runs (the instructions —
                  docs/dev-environment.md)

The dump has fewer stratum rows than the quota (little data) — all the
available ones are taken, the degradation is noted in the output and
manifest.json. The dump format v1 — the export→import contract, in detail:
the doc comments of cmd/crawlerctl/fixtures.go and fixtures_import.go.

snapshot — capturing one run's results (§6.1 item 5) into a JSON snapshot,
the report's input: the fixture rows from the test DB (successful renders,
the DOM length/hash/nodes/scripts, the analyzer verdicts), the q:analyze
boundaries from Redis (the pace) and — with --metrics — the render-worker
stage counters (the finalization reasons, the page time). Taken AFTER the
run (an empty q:render, the fallback drained) and BEFORE the repeated
import (the import resets the rows and sweeps the content — the run cannot
be read back from the DB). Between runs restart the render-worker: the
/metrics counters then equal exactly the run (otherwise the report marks
them cumulative and degrades the tied verdicts).

  --dir DIR            the fixture dump directory (the run's keys; required)
  --label NAME         the run label (required; the file is
                       <dir>/<label>.snapshot.json)
  --engine NAME        the run engine: chromium|moli|lightpanda (required)
  --metrics URL        the bench render-worker's /metrics URL (empty — no
                       stage metrics; e.g. http://render-worker:9105/metrics)
  --engine-rss-max N   the engine container's max RSS over the run, bytes
                       (a periodic docker stats --no-stream during the run;
                       0 — not set: the dev stack has no engine memory
                       exporter, the report without it degrades the memory
                       threshold)
  --engine-crashes N   engine core crashes over the run (>= 0 — set)
  --no-dom             do not read the store DOM (no nodes/scripts)

report — a snapshot comparison over the spec §5.1 item 3 metrics (success,
the finalization reasons, the avg/p95 pace, memory, the Laravel detection
diff against baseline-chromium per the T-213 protocol, the structural DOM
metrics — a normalized comparison, no byte-level diff is applied) with
explicit verdict rows against the §5.1 item 4 thresholds (detection <1%,
success, pace, memory <⅓ of the chromium budget, 0 crashes). A missing
source — a "NO DATA" row, the report invents no values.

  --baseline FILE    the baseline-chromium snapshot (required)
  --candidate FILE   the candidate engine's snapshot (required)
  --out FILE         write the report to a file (for the T-247 protocol)
  --memory-budget N  the chromium memory budget for the ⅓ threshold (the
                     default 2 GiB — the render circuit's production budget,
                     spec 3.2)
  --fail-exit        exit code 1 on any FAILED verdict (for scripts)

The DB connection — as with the other commands: the configuration's
postgres.dsn (CRAWLER_POSTGRES_DSN / APP_PG_DSN). export reads the DB
without writes; import writes into a test DB and (with --publish) into
Redis; snapshot reads the DB, Redis (redis.addr) and the content store
(content.dir) without writes; report works over the snapshot files without
infrastructure.

Exit codes: 0 — success, 1 — error, 2 — usage.
`

// Quota defaults — the spec §6.1 item 1 plan (3000 rows: ~2000/500/500)
// and smoke ~300 domains (item 6); the failed freshness window — the
// spec's "recent weeks". The seed is an arbitrary constant: a fixed
// value makes the default export reproducible (the same dump for the
// same data).
const (
	fixturesFormatVersion = 1
	fixturesDefaultOK     = 2000
	fixturesDefaultFailed = 500
	fixturesDefaultHeavy  = 500
	fixturesDefaultSmoke  = 300
	fixturesDefaultWeeks  = 4
	fixturesDefaultSeed   = 242
)

// ---------------------------------------------------------------- SQL

// fixturesResultCols — the subdomain_results selection columns: row
// identifiers, render task fields and stratum diagnostics (the format v1
// contract — the fixtures block doc comment). r.* — the alias of the ok
// stratum's anti-join.
const fixturesResultCols = "r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
	"r.scheme, r.http_status, r.final_url, r.alias_kind, r.alias_subdomain_id, " +
	"r.headers, r.cookies, r.rendered, r.content_hash, r.error, r.response_ms, " +
	"r.checked_at, r.rules_version, r.render_failed_at"

// fixturesKeyHash — a deterministic pseudo-random row order within a
// zone: md5 of the seed and the row key (the seed parameter number is
// substituted in place). Unlike ORDER BY random() it depends on neither
// the session nor the server version: re-exporting with the same --seed
// reproduces the selection. Keys are cast to text explicitly —
// concatenating numbers with text is ambiguous (12||34 = 1234); the ':'
// separator and ::text make the hash stable.
const fixturesKeyHash = "md5($%d::text || ':' || r.zone_id::text || ':' || r.domain_id::text || ':' || r.subdomain_id::text || ':' || r.variant::text)"

// Stratum row counts per zone (quotas proportional to shares): aggregates
// over the partitioned table are cut down to partitions. The base of all
// three — a "real q:render task" (final_url IS NOT NULL).
const (
	// fixturesCountOkSQL — rendered (a successful chromium render); it
	// also includes heavy candidates: ok availability is computed after
	// subtracting the taken heavy (heavy ⊂ rendered).
	fixturesCountOkSQL = "SELECT zone_id, count(*) FROM subdomain_results " +
		"WHERE final_url IS NOT NULL AND rendered GROUP BY zone_id"

	// fixturesCountFailedSQL — the T-210 marker within the --weeks
	// freshness window. Freshness is by render_failed_at (the stamp of
	// the FIRST finalization, coalesce T-210), not by the episode's
	// checked_at: an old episode record flushed out of the backlog is a
	// fresh marker (review T-242 P2); the spec's "recent weeks" is about
	// finalizations. make_interval — a named argument, without casting
	// strings to an interval.
	fixturesCountFailedSQL = "SELECT zone_id, count(*) FROM subdomain_results " +
		"WHERE final_url IS NOT NULL AND render_failed_at IS NOT NULL " +
		"AND render_failed_at >= now() - make_interval(weeks => $1::int) GROUP BY zone_id"

	// fixturesCountHeavySQL — rendered with live content: the DOM size is
	// known only for rows referencing contents (retention T-166 cleans
	// the content_hash of non-finds — a row without content is not
	// heavy).
	fixturesCountHeavySQL = "SELECT r.zone_id, count(*) FROM subdomain_results AS r " +
		"JOIN contents AS c ON c.hash = r.content_hash " +
		"WHERE r.final_url IS NOT NULL AND r.rendered GROUP BY r.zone_id"
)

// Stratum selection within one zone: LIMIT — the zone's quota (missing
// rows are degradation, not an error). heavy goes first (it feeds the ok
// anti-join).
var (
	fixturesSelectHeavySQL = "SELECT " + fixturesResultCols + " FROM subdomain_results AS r " +
		"JOIN contents AS c ON c.hash = r.content_hash " +
		"WHERE r.zone_id = $1::int2 AND r.final_url IS NOT NULL AND r.rendered " +
		"ORDER BY c.raw_len DESC, " + fmt.Sprintf(fixturesKeyHash, 2) + " LIMIT $3::int8"

	// fixturesSelectOkSQL — random successful rows: an anti-join against
	// the heavy keys (unnest arrays — the batch operation style of
	// internal/db) keeps the strata from intersecting — a row never
	// enters the dump twice. Empty arrays (no heavy) — the anti-join
	// lets everything through (unnest of 0 rows).
	fixturesSelectOkSQL = "SELECT " + fixturesResultCols + " FROM subdomain_results AS r " +
		"LEFT JOIN unnest($2::int2[], $3::int8[], $4::int2[], $5::int2[]) " +
		"AS ex(zone_id, domain_id, subdomain_id, variant) " +
		"ON r.zone_id = ex.zone_id AND r.domain_id = ex.domain_id " +
		"AND r.subdomain_id = ex.subdomain_id AND r.variant = ex.variant " +
		"WHERE r.zone_id = $1::int2 AND r.final_url IS NOT NULL AND r.rendered " +
		"AND ex.zone_id IS NULL " +
		"ORDER BY " + fmt.Sprintf(fixturesKeyHash, 6) + " LIMIT $7::int8"

	fixturesSelectFailedSQL = "SELECT " + fixturesResultCols + " FROM subdomain_results AS r " +
		"WHERE r.zone_id = $1::int2 AND r.final_url IS NOT NULL " +
		"AND r.render_failed_at IS NOT NULL " +
		"AND r.render_failed_at >= now() - make_interval(weeks => $2::int) " +
		"ORDER BY " + fmt.Sprintf(fixturesKeyHash, 3) + " LIMIT $4::int8"

	// fixturesDomainsSQL — domains of the selected rows (DISTINCT over
	// the unnest keys): whole table rows — domain identity and state for
	// the bench.
	fixturesDomainsSQL = "SELECT d.zone_id, d.id, d.label, d.status, d.check_state, " +
		"d.flags, d.dns_mask, d.queued_at, d.last_checked_at, d.temp_attempts " +
		"FROM domains AS d JOIN (SELECT DISTINCT k.zone_id, k.domain_id " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id)) AS k " +
		"ON d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"ORDER BY d.zone_id, d.id"

	// fixturesZonesSQL — the export zones (id = ANY over an unnest-
	// compatible array): the reference table is small, a point select.
	fixturesZonesSQL = "SELECT id, name, name_unicode, is_active, domains_total, " +
		"domains_checked, updated_at FROM zones WHERE id = ANY($1::int2[]) ORDER BY id"
)

// ---------------------------------------------------------------- data

// fixturesZone — a zones row of the dump: the format v1 columns (see the
// contract — the live counters domains_laravel/pending/queued are not
// exported).
type fixturesZone struct {
	ID             int16      `json:"id"`
	Name           string     `json:"name"` // punycode
	NameUnicode    *string    `json:"name_unicode"`
	IsActive       bool       `json:"is_active"`
	DomainsTotal   int64      `json:"domains_total"`
	DomainsChecked int64      `json:"domains_checked"`
	UpdatedAt      *time.Time `json:"updated_at"`
}

// fixturesDomain — a domains row of the dump (all table columns): the
// row keys are original (zone_id, id), import idempotency is by PK.
type fixturesDomain struct {
	ZoneID        int16      `json:"zone_id"`
	ID            int64      `json:"id"`
	Label         string     `json:"label"` // without the zone, punycode
	Status        int16      `json:"status"`
	CheckState    int16      `json:"check_state"`
	Flags         int16      `json:"flags"`    // bit 0 is_idn, 1 render_pending, …
	DNSMask       int64      `json:"dns_mask"` // resolving subdomains
	QueuedAt      *time.Time `json:"queued_at"`
	LastCheckedAt *time.Time `json:"last_checked_at"`
	TempAttempts  int16      `json:"temp_attempts"`
}

// fixturesResult — a subdomain_results row of the dump: the columns the
// render task needs (the T-243 contract) plus stratum — the stratum
// label of the selection (ok|failed|heavy; a fixture service field that
// does not exist in the table — dropped on import). bytea content_hash →
// base64, NULL columns → null, jsonb headers/cookies → nested JSON.
type fixturesResult struct {
	ZoneID           int16           `json:"zone_id"`
	DomainID         int64           `json:"domain_id"`
	SubdomainID      int16           `json:"subdomain_id"`
	Variant          int16           `json:"variant"` // 0 main, 1 http
	Scheme           int16           `json:"scheme"`  // 1 http, 2 https
	HTTPStatus       *int16          `json:"http_status"`
	FinalURL         *string         `json:"final_url"` // the render target
	AliasKind        int16           `json:"alias_kind"`
	AliasSubdomainID *int16          `json:"alias_subdomain_id"`
	Headers          json.RawMessage `json:"headers"` // the episode's jsonb
	Cookies          json.RawMessage `json:"cookies"` // the episode's jsonb
	Rendered         bool            `json:"rendered"`
	ContentHash      []byte          `json:"content_hash"` // 16 bytes, base64
	Error            *int16          `json:"error"`
	ResponseMs       *int32          `json:"response_ms"`
	CheckedAt        time.Time       `json:"checked_at"`
	RulesVersion     *int32          `json:"rules_version"`
	RenderFailedAt   *time.Time      `json:"render_failed_at"`
	Stratum          string          `json:"stratum"`
}

// fixturesSmokeDomain — a smoke.ndjson row: the subsample domain key and
// the full name (label.zone) for task publishing and bench logs.
type fixturesSmokeDomain struct {
	ZoneID   int16  `json:"zone_id"`
	DomainID int64  `json:"domain_id"`
	Name     string `json:"name"`
}

// fixturesStratumStat — stratum counters in the manifest: the quota,
// available (the sum of the per-zone counts), actually taken, degradation
// (taken < quota) and the per-zone breakdown of what was taken.
type fixturesStratumStat struct {
	Quota     int            `json:"quota"`
	Available int            `json:"available"`
	Taken     int            `json:"taken"`
	Degraded  bool           `json:"degraded"`
	Zones     map[string]int `json:"zones"`
}

// fixturesManifest — the dump's manifest.json: the format version, the
// selection parameters and the counters. created_at — the run time (the
// only non-deterministic field); everything else is reproduced with the
// same --seed and data.
type fixturesManifest struct {
	FormatVersion int                            `json:"format_version"`
	CreatedAt     time.Time                      `json:"created_at"`
	Seed          int64                          `json:"seed"`
	Weeks         int                            `json:"failed_window_weeks"`
	Strata        map[string]fixturesStratumStat `json:"strata"`
	SmokeQuota    int                            `json:"smoke_quota"`
	SmokeDomains  int                            `json:"smoke_domains"`
	Files         map[string]int                 `json:"files"`
}

// fixturesKey — the subdomain_results primary key (migration 000002).
type fixturesKey struct {
	zone      int16
	domain    int64
	subdomain int16
	variant   int16
}

// fixturesCollect — the selection collector: duplicate keys (stratum
// intersections on bad data, outside the T-210 invariants) are dropped
// with a count — the dump stays a set of unique rows and re-import is
// idempotent.
type fixturesCollect struct {
	rows []fixturesResult
	seen map[fixturesKey]bool
	dups int
}

func newFixturesCollect() *fixturesCollect {
	return &fixturesCollect{seen: make(map[fixturesKey]bool)}
}

// add adds stratum rows (stratum — the label), cutting off duplicates.
func (c *fixturesCollect) add(stratum string, rows []fixturesResult) {
	for _, r := range rows {
		r.Stratum = stratum
		k := fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant}
		if c.seen[k] {
			c.dups++
			continue
		}
		c.seen[k] = true
		c.rows = append(c.rows, r)
	}
}

// ---------------------------------------------------------------- quotas

// fixturesAllocateQuotas distributes total rows between zones
// proportionally to their shares (counts) by the largest remainder
// method: the integer parts of the shares plus a remainder top-up by
// descending fractional part; on a tie — by ascending zone_id
// (determinism). A zone's quota is never above what is available
// (total ≤ the sum); total above the sum or zero counts — quotas over
// what is available (degradation is resolved at selection time: LIMIT
// returns less). The sum of quotas equals min(total, the sum of counts);
// zero zones do not enter the result.
func fixturesAllocateQuotas(counts map[int16]int64, total int) map[int16]int {
	var sum int64
	for _, n := range counts {
		sum += n
	}
	if total <= 0 || sum == 0 {
		return nil
	}
	if int64(total) > sum {
		total = int(sum)
	}

	zones := make([]int16, 0, len(counts))
	for z := range counts {
		if counts[z] > 0 {
			zones = append(zones, z)
		}
	}
	sort.Slice(zones, func(i, j int) bool { return zones[i] < zones[j] })

	quotas := make(map[int16]int, len(zones))
	rest := total
	for _, z := range zones {
		exact := float64(total) * float64(counts[z]) / float64(sum)
		base := int64(exact)
		if base > counts[z] {
			base = counts[z]
		}
		quotas[z] = int(base)
		rest -= int(base)
	}
	if rest > 0 {
		type share struct {
			zone int16
			frac float64
		}
		shares := make([]share, 0, len(zones))
		for _, z := range zones {
			exact := float64(total) * float64(counts[z]) / float64(sum)
			shares = append(shares, share{zone: z, frac: exact - float64(int64(exact))})
		}
		sort.SliceStable(shares, func(i, j int) bool {
			if shares[i].frac != shares[j].frac {
				return shares[i].frac > shares[j].frac
			}
			return shares[i].zone < shares[j].zone // determinism on equal shares
		})
		for i := 0; i < len(shares) && rest > 0; i++ {
			if int64(quotas[shares[i].zone]) < counts[shares[i].zone] {
				quotas[shares[i].zone]++
				rest--
			}
		}
	}
	return quotas
}

// ---------------------------------------------------------------- selection

// fixturesZoneCounts — stratum row counters per zone (the aggregates of
// fixturesCount*SQL). Zones without rows do not enter the result.
func fixturesZoneCounts(ctx context.Context, pool db.DB, sql string, args ...any) (map[int16]int64, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("stratum count by zones: %w", err)
	}
	defer rows.Close()
	counts := make(map[int16]int64)
	for rows.Next() {
		var zone int16
		var n int64
		if err := rows.Scan(&zone, &n); err != nil {
			return nil, fmt.Errorf("stratum count by zones: reading: %w", err)
		}
		counts[zone] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("stratum count by zones: %w", err)
	}
	return counts, nil
}

// fixturesScanResults reads stratum selection rows (the column order is
// fixturesResultCols).
func fixturesScanResults(rows pgx.Rows) ([]fixturesResult, error) {
	var out []fixturesResult
	for rows.Next() {
		var r fixturesResult
		if err := rows.Scan(&r.ZoneID, &r.DomainID, &r.SubdomainID, &r.Variant,
			&r.Scheme, &r.HTTPStatus, &r.FinalURL, &r.AliasKind, &r.AliasSubdomainID,
			&r.Headers, &r.Cookies, &r.Rendered, &r.ContentHash, &r.Error,
			&r.ResponseMs, &r.CheckedAt, &r.RulesVersion, &r.RenderFailedAt); err != nil {
			return nil, fmt.Errorf("stratum row reading: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("stratum row reading: %w", err)
	}
	return out, nil
}

// fixturesSortedZones — an ascending zone list (a deterministic traversal
// of the selection: partitions are addressed in turn; the order only
// affects the row order in the dump, but it is pinned too).
func fixturesSortedZones(quotas map[int16]int) []int16 {
	zones := make([]int16, 0, len(quotas))
	for z, q := range quotas {
		if q > 0 {
			zones = append(zones, z)
		}
	}
	sort.Slice(zones, func(i, j int) bool { return zones[i] < zones[j] })
	return zones
}

// fixturesPickHeavy — the heavy stratum per zone: the largest content
// (raw_len DESC), the zone's quota as LIMIT; the tie-break is a
// deterministic hash.
func fixturesPickHeavy(ctx context.Context, pool db.DB, quotas map[int16]int, seed string) ([]fixturesResult, error) {
	var out []fixturesResult
	for _, z := range fixturesSortedZones(quotas) {
		rows, err := pool.Query(ctx, fixturesSelectHeavySQL, z, seed, int64(quotas[z]))
		if err != nil {
			return nil, fmt.Errorf("heavy selection (zone %d): %w", z, err)
		}
		part, err := fixturesScanResults(rows)
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("heavy selection (zone %d): %w", z, err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// fixturesPickOk — the ok stratum per zone: random successful rows with
// the heavy keys excluded by an anti-join (unnest arrays from the
// already read rows).
func fixturesPickOk(ctx context.Context, pool db.DB, quotas map[int16]int, seed string, heavy []fixturesResult) ([]fixturesResult, error) {
	hz, hd, hs, hv := fixturesKeyArrays(heavy)
	var out []fixturesResult
	for _, z := range fixturesSortedZones(quotas) {
		rows, err := pool.Query(ctx, fixturesSelectOkSQL,
			z, hz, hd, hs, hv, seed, int64(quotas[z]))
		if err != nil {
			return nil, fmt.Errorf("ok selection (zone %d): %w", z, err)
		}
		part, err := fixturesScanResults(rows)
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("ok selection (zone %d): %w", z, err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// fixturesPickFailed — the failed stratum per zone: the T-210 marker in
// the weeks freshness window, a random order (a deterministic hash).
func fixturesPickFailed(ctx context.Context, pool db.DB, quotas map[int16]int, seed string, weeks int) ([]fixturesResult, error) {
	var out []fixturesResult
	for _, z := range fixturesSortedZones(quotas) {
		rows, err := pool.Query(ctx, fixturesSelectFailedSQL, z, weeks, seed, int64(quotas[z]))
		if err != nil {
			return nil, fmt.Errorf("failed selection (zone %d): %w", z, err)
		}
		part, err := fixturesScanResults(rows)
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("failed selection (zone %d): %w", z, err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// fixturesKeyArrays — the key columns of the rows (parallel arrays for
// the unnest of the ok anti-join against heavy).
func fixturesKeyArrays(rows []fixturesResult) (zones []int16, domains []int64, subs []int16, variants []int16) {
	zones = make([]int16, len(rows))
	domains = make([]int64, len(rows))
	subs = make([]int16, len(rows))
	variants = make([]int16, len(rows))
	for i, r := range rows {
		zones[i], domains[i], subs[i], variants[i] = r.ZoneID, r.DomainID, r.SubdomainID, r.Variant
	}
	return zones, domains, subs, variants
}

// fixturesFetchDomains — domains of the selected rows by keys (DISTINCT
// inside the SQL), ordered by zone_id, id.
func fixturesFetchDomains(ctx context.Context, pool db.DB, rows []fixturesResult) ([]fixturesDomain, error) {
	zones, domains, _, _ := fixturesKeyArrays(rows)
	out := []fixturesDomain{} // not nil: an empty selection = an empty file, not an absence
	rrows, err := pool.Query(ctx, fixturesDomainsSQL, zones, domains)
	if err != nil {
		return nil, fmt.Errorf("fixture domain selection: %w", err)
	}
	defer rrows.Close()
	for rrows.Next() {
		var d fixturesDomain
		if err := rrows.Scan(&d.ZoneID, &d.ID, &d.Label, &d.Status, &d.CheckState,
			&d.Flags, &d.DNSMask, &d.QueuedAt, &d.LastCheckedAt, &d.TempAttempts); err != nil {
			return nil, fmt.Errorf("fixture domain selection: reading: %w", err)
		}
		out = append(out, d)
	}
	if err := rrows.Err(); err != nil {
		return nil, fmt.Errorf("fixture domain selection: %w", err)
	}
	return out, nil
}

// fixturesFetchZones — the export zones by the domains' ids.
func fixturesFetchZones(ctx context.Context, pool db.DB, domains []fixturesDomain) ([]fixturesZone, error) {
	seen := make(map[int16]bool)
	ids := make([]int16, 0, len(domains))
	for _, d := range domains {
		if !seen[d.ZoneID] {
			seen[d.ZoneID] = true
			ids = append(ids, d.ZoneID)
		}
	}
	out := []fixturesZone{}
	rows, err := pool.Query(ctx, fixturesZonesSQL, ids)
	if err != nil {
		return nil, fmt.Errorf("fixture zone selection: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var z fixturesZone
		if err := rows.Scan(&z.ID, &z.Name, &z.NameUnicode, &z.IsActive,
			&z.DomainsTotal, &z.DomainsChecked, &z.UpdatedAt); err != nil {
			return nil, fmt.Errorf("fixture zone selection: reading: %w", err)
		}
		out = append(out, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fixture zone selection: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------- smoke

// fixturesSmokePick — the smoke domain subsample (§6.1 item 6): unique
// domains of the selection in a deterministic pseudo-random order — the
// same hash recipe as the SQL selection (md5(seed:key)), the first
// limit. name — the full name (label '.' zone, punycode) for publishing
// the bench's tasks.
func fixturesSmokePick(rows []fixturesResult, domains []fixturesDomain, zones []fixturesZone, seed string, limit int) []fixturesSmokeDomain {
	if limit <= 0 || len(rows) == 0 {
		return nil
	}
	labels := make(map[fixturesDomainKey]string, len(domains))
	for _, d := range domains {
		labels[fixturesDomainKey{d.ZoneID, d.ID}] = d.Label
	}
	zoneNames := make(map[int16]string, len(zones))
	for _, z := range zones {
		zoneNames[z.ID] = z.Name
	}

	// Unique domains of the selection (the appearance order does not
	// matter — the deterministic hash sort below).
	type hashed struct {
		key  fixturesDomainKey
		hash string
	}
	seen := make(map[fixturesDomainKey]bool)
	uniq := make([]hashed, 0, len(rows))
	for _, r := range rows {
		k := fixturesDomainKey{r.ZoneID, r.DomainID}
		if seen[k] {
			continue
		}
		seen[k] = true
		uniq = append(uniq, hashed{key: k, hash: fixturesDomainHash(seed, k)})
	}
	sort.Slice(uniq, func(i, j int) bool {
		if uniq[i].hash != uniq[j].hash {
			return uniq[i].hash < uniq[j].hash
		}
		if uniq[i].key.zone != uniq[j].key.zone {
			return uniq[i].key.zone < uniq[j].key.zone
		}
		return uniq[i].key.domain < uniq[j].key.domain
	})
	if limit > len(uniq) {
		limit = len(uniq)
	}
	out := make([]fixturesSmokeDomain, 0, limit)
	for _, h := range uniq[:limit] {
		name := labels[h.key] + "." + zoneNames[h.key.zone]
		out = append(out, fixturesSmokeDomain{ZoneID: h.key.zone, DomainID: h.key.domain, Name: name})
	}
	return out
}

// fixturesDomainKey — the domain key (the domains PK).
type fixturesDomainKey struct {
	zone   int16
	domain int64
}

// fixturesDomainHash — the SQL selection's hash recipe for a domain
// (md5 hex): a stable smoke order between runs with the same seed.
func fixturesDomainHash(seed string, k fixturesDomainKey) string {
	sum := md5.Sum([]byte(seed + ":" + strconv.Itoa(int(k.zone)) + ":" + strconv.FormatInt(k.domain, 10)))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------- writing

// fixturesEncodeAll encodes NDJSON rows: one object per line, without
// HTML escaping (the headers/cookies content is read as-is).
func fixturesEncodeAll[T any](w io.Writer, rows []T) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := range rows {
		if err := enc.Encode(&rows[i]); err != nil {
			return fmt.Errorf("line %d: %w", i+1, err)
		}
	}
	return nil
}

// fixturesWriteFile — an atomic dump file write: the content goes to
// name+".tmp" and is renamed (an interrupted export leaves no half-files
// under the final names; a re-export overwrites). Mode 0600 — the dump
// carries episode cookies/headers (the spec explicitly asks to export
// them); reading them by another host user is not expected.
func fixturesWriteFile(dir, name string, write func(*bufio.Writer) error) error {
	full := filepath.Join(dir, name)
	tmp := full + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("%s: %w", tmp, err)
	}
	w := bufio.NewWriter(f)
	if err := write(w); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: writing: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: closing: %w", tmp, err)
	}
	if err := os.Rename(tmp, full); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: renaming: %w", full, err)
	}
	return nil
}

// ---------------------------------------------------------------- export

// fixturesParams — the export parameters (the fixtures export flags; the
// defaults are the constants above). A zero stratum is excluded from the
// selection.
type fixturesParams struct {
	OutDir string
	OK     int
	Failed int
	Heavy  int
	Smoke  int
	Weeks  int
	Seed   int64
}

// fixturesExportRun — the fixture export: stratum counts per zone →
// quotas (proportional to shares) → the heavy selection → ok (the heavy
// anti-join) → failed → domains/zones → smoke → files (the manifest
// last).
func fixturesExportRun(ctx context.Context, pool db.DB, p fixturesParams, stdout io.Writer) (*fixturesManifest, error) {
	seedText := strconv.FormatInt(p.Seed, 10)

	// heavy: the content size is the priority, selected first.
	heavyCounts, err := fixturesZoneCounts(ctx, pool, fixturesCountHeavySQL)
	if err != nil {
		return nil, err
	}
	heavyRows, err := fixturesPickHeavy(ctx, pool, fixturesAllocateQuotas(heavyCounts, p.Heavy), seedText)
	if err != nil {
		return nil, err
	}

	// ok: rendered minus the taken heavy (heavy ⊂ rendered — the
	// availability is honestly reduced by their number in each zone).
	okCounts, err := fixturesZoneCounts(ctx, pool, fixturesCountOkSQL)
	if err != nil {
		return nil, err
	}
	heavyPerZone := make(map[int16]int64)
	for _, r := range heavyRows {
		heavyPerZone[r.ZoneID]++
	}
	for z, n := range heavyPerZone {
		okCounts[z] -= n
		if okCounts[z] < 0 {
			okCounts[z] = 0 // guard against count/selection desync by a write race
		}
	}
	okRows, err := fixturesPickOk(ctx, pool, fixturesAllocateQuotas(okCounts, p.OK), seedText, heavyRows)
	if err != nil {
		return nil, err
	}

	// failed: the T-210 marker in the freshness window.
	failedCounts, err := fixturesZoneCounts(ctx, pool, fixturesCountFailedSQL, p.Weeks)
	if err != nil {
		return nil, err
	}
	failedRows, err := fixturesPickFailed(ctx, pool, fixturesAllocateQuotas(failedCounts, p.Failed), seedText, p.Weeks)
	if err != nil {
		return nil, err
	}

	collect := newFixturesCollect()
	collect.add("heavy", heavyRows)
	collect.add("ok", okRows)
	collect.add("failed", failedRows)

	domains, err := fixturesFetchDomains(ctx, pool, collect.rows)
	if err != nil {
		return nil, err
	}
	zones, err := fixturesFetchZones(ctx, pool, domains)
	if err != nil {
		return nil, err
	}
	zoneNames := make(map[int16]string, len(zones))
	for _, z := range zones {
		zoneNames[z.ID] = z.Name
	}
	smoke := fixturesSmokePick(collect.rows, domains, zones, seedText, p.Smoke)

	// Manifest: stratum counters (quota/available/taken/degradation/zones).
	stat := func(quota, available int, rows []fixturesResult) fixturesStratumStat {
		perZone := make(map[string]int)
		for _, r := range rows {
			perZone[zoneNames[r.ZoneID]]++
		}
		return fixturesStratumStat{
			Quota:     quota,
			Available: available,
			Taken:     len(rows),
			Degraded:  len(rows) < quota,
			Zones:     perZone,
		}
	}
	sumCounts := func(counts map[int16]int64) int {
		total := 0
		for _, n := range counts {
			total += int(n)
		}
		return total
	}
	manifest := &fixturesManifest{
		FormatVersion: fixturesFormatVersion,
		CreatedAt:     time.Now().UTC(),
		Seed:          p.Seed,
		Weeks:         p.Weeks,
		Strata: map[string]fixturesStratumStat{
			"ok":     stat(p.OK, sumCounts(okCounts), okRows),
			"failed": stat(p.Failed, sumCounts(failedCounts), failedRows),
			"heavy":  stat(p.Heavy, sumCounts(heavyCounts), heavyRows),
		},
		SmokeQuota:   p.Smoke,
		SmokeDomains: len(smoke),
		Files:        make(map[string]int),
	}
	if collect.dups > 0 {
		// Duplicates outside the T-210 invariants: they did not enter the
		// dump, the operator sees the fact (the source data needs
		// analysis).
		out(stdout, "duplicate keys across the strata (skipped): %d\n", collect.dups)
	}

	// The dump directory is private (0700): the files carry episode
	// cookies/headers.
	if err := os.MkdirAll(p.OutDir, 0o700); err != nil {
		return nil, fmt.Errorf("the dump directory %s: %w", p.OutDir, err)
	}
	// The previous manifest.json goes before the first data file is
	// written: a re-export failing midway does NOT leave the old manifest
	// next to the new files — a dump without a manifest is honestly
	// "incomplete" (the "manifest present ⇒ dump complete" contract,
	// review T-242 P1). Until that point a failure (DB queries) does not
	// touch the old complete dump.
	if err := os.Remove(filepath.Join(p.OutDir, "manifest.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("removing the previous manifest: %w", err)
	}
	// Order: manifest.json last — its presence means a completed dump.
	manifest.Files["zones.ndjson"] = len(zones)
	if err := fixturesWriteFile(p.OutDir, "zones.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, zones)
	}); err != nil {
		return nil, err
	}
	manifest.Files["domains.ndjson"] = len(domains)
	if err := fixturesWriteFile(p.OutDir, "domains.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, domains)
	}); err != nil {
		return nil, err
	}
	manifest.Files["subdomain_results.ndjson"] = len(collect.rows)
	if err := fixturesWriteFile(p.OutDir, "subdomain_results.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, collect.rows)
	}); err != nil {
		return nil, err
	}
	manifest.Files["smoke.ndjson"] = len(smoke)
	if err := fixturesWriteFile(p.OutDir, "smoke.ndjson", func(w *bufio.Writer) error {
		return fixturesEncodeAll(w, smoke)
	}); err != nil {
		return nil, err
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	manifestJSON = append(manifestJSON, '\n')
	if err := fixturesWriteFile(p.OutDir, "manifest.json", func(w *bufio.Writer) error {
		if _, err := w.Write(manifestJSON); err != nil {
			return fmt.Errorf("manifest: writing: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Report: strata with honest degradation + the file summary.
	for _, name := range []string{"ok", "failed", "heavy"} {
		s := manifest.Strata[name]
		note := ""
		if s.Degraded {
			note = " — degradation: less data than the quota"
		}
		out(stdout, "stratum %s: quota %d, available %d, taken %d%s\n",
			name, s.Quota, s.Available, s.Taken, note)
	}
	if p.Smoke > 0 && len(smoke) < p.Smoke {
		out(stdout, "smoke: domains in the selection %d of %d — degradation: fewer unique domains\n", len(smoke), p.Smoke)
	} else {
		out(stdout, "smoke: domains %d\n", len(smoke))
	}
	for _, name := range []string{"zones.ndjson", "domains.ndjson", "subdomain_results.ndjson", "smoke.ndjson"} {
		out(stdout, "  %s: %d rows\n", name, manifest.Files[name])
	}
	out(stdout, "  manifest.json: ok\n")
	out(stdout, "dump written: %s (format v1, import — T-243)\n", p.OutDir)
	return manifest, nil
}

// ---------------------------------------------------------------- command

// cmdFixtures — crawlerctl fixtures: action routing (export — T-242;
// import — T-243, fixtures_import.go; snapshot/report — T-244,
// fixtures_snapshot.go / fixtures_report.go).
func cmdFixtures(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("specify an action: crawlerctl fixtures export|import|snapshot|report")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "export":
		return fixturesExportCmd(rest, stdout, stderr)
	case "import":
		return fixturesImportCmd(rest, stdout, stderr)
	case "snapshot":
		return fixturesSnapshotCmd(rest, stdout, stderr)
	case "report":
		return fixturesReportCmd(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", fixturesHelp)
		return flag.ErrHelp
	default:
		return usagef("unknown action %q (valid: export, import, snapshot, report)", action)
	}
}

// fixturesExportCmd — crawlerctl fixtures export: flag parsing and
// validation (before the DB connection — code 2), then the export
// following the pattern of the management commands (openManagePool:
// configuration + environment, pool 2). A one-off CLI: Ctrl-C/SIGTERM
// interrupt the export — the already written files are atomic
// (.tmp + rename), rerunning the command overwrites the dump.
func fixturesExportCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures export")
	outDir := fs.String("out", "", "the dump directory (required)")
	okN := fs.Int("ok", fixturesDefaultOK, "the ok quota (rendered)")
	failedN := fs.Int("failed", fixturesDefaultFailed, "the failed quota (render_failed_at)")
	heavyN := fs.Int("heavy", fixturesDefaultHeavy, "the SPA-heavy quota (large content)")
	smokeN := fs.Int("smoke", fixturesDefaultSmoke, "the number of smoke domains")
	weeks := fs.Int("weeks", fixturesDefaultWeeks, "the failed freshness window, weeks")
	seed := fs.Int64("seed", fixturesDefaultSeed, "the deterministic selection seed")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*outDir) == "" {
		return usagef("specify the dump directory: crawlerctl fixtures export --out /path/to/fixtures")
	}
	if fs.NArg() > 0 {
		return usagef("fixtures export: extra arguments %v (the command takes only flags)", fs.Args())
	}
	for name, v := range map[string]int{"--ok": *okN, "--failed": *failedN, "--heavy": *heavyN, "--smoke": *smokeN} {
		if v < 0 {
			return usagef("fixtures export: %s: negative value %d", name, v)
		}
	}
	if *okN+*failedN+*heavyN == 0 {
		return usagef("fixtures export: the stratum quota sum is zero (--ok/--failed/--heavy)")
	}
	if *weeks < 1 {
		return usagef("fixtures export: --weeks: the freshness window is at least 1 week (got %d)", *weeks)
	}

	ctx, stop := signalCtx()
	defer stop()

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("fixtures export: %w", err)
	}
	defer pool.Close()

	params := fixturesParams{
		OutDir: *outDir,
		OK:     *okN,
		Failed: *failedN,
		Heavy:  *heavyN,
		Smoke:  *smokeN,
		Weeks:  *weeks,
		Seed:   *seed,
	}
	if _, err := fixturesExportRun(ctx, pool, params, stdout); err != nil {
		return fmt.Errorf("fixtures export: %w", err)
	}
	return nil
}
