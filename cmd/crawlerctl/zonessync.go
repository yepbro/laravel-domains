// ---------------------------------------------------------------- zones sync
//
// crawlerctl zones sync (T-125, stage 14): the initial filling and the
// refresh of the zone reference table from the machine-readable slice of
// the IANA root zone registry (tlds-alpha-by-domain.txt, ~1,460 delegated
// TLDs, IDN zones already in punycode — matches the zones.name contract,
// spec 5.2). This is a reference table of TLD names, NOT a source of
// domains: domains still enter the system only through ingest
// (var/incoming).
//
// Write semantics: a single batch insert of new zones (is_active = true —
// as ingest auto-creation does, spec 5.1: an empty zone is indifferent to
// the scheduler, DispatchBatch goes from domains) and, by the explicit
// --deactivate-missing flag, disabling of DB zones missing from the slice
// (retired TLDs). sync does not update existing rows at all: is_active is
// the operator's ownership (zones enable/disable), the counters are
// monitor's (T-078). Idempotent: a repeated run changes nothing.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"crawler/internal/db"
	"crawler/internal/idn"
)

// zoneListURL — the canonical slice of the IANA root zone registry: the
// same content as the iana.org/domains/root/db page, without HTML parsing.
// Format: a first comment line with the file version, then one zone per
// line in upper case (IDNs — in punycode).
const zoneListURL = "https://data.iana.org/TLD/tlds-alpha-by-domain.txt"

// Slice download parameters. The real file is ~30 KiB; the cap guards
// against a wrong URL with a large response (a round value with a margin
// for registry growth is taken). The timeout covers the whole http.Client
// exchange (connection + body).
const (
	zoneSyncHTTPTimeout = 30 * time.Second
	zoneListMaxBytes    = 1 << 20 // 1 MiB
)

// ---------------------------------------------------------------- SQL

// zonesSyncInsertSQL — a single batch insert of the reference table from
// an unnest batch (the "write in batches" principle of spec 6): name —
// punycode, is_active = true —
// explicitly, as in ingest auto-creation (spec 5.1; the schema DEFAULT
// matches, but the contract is set here), name_unicode — nullif(u.unicode, ”):
// the unicode form only for XN-- zones (idna.ToUnicode on the fly, spec
// 5.2 item 7), an ASCII zone stores NULL — it carries no information (the
// displayUnicode style, internal/ingest/zones.go).
//
// NOT EXISTS filters out already existing zones before the insert: the
// GENERATED ALWAYS AS IDENTITY identity (migration 000001) executes the
// id default for every candidate row — conflicting rows also consume
// sequence values, and repeated syncs (~1,460 rows per run) would exhaust
// smallint (32767) in ~20 runs; with the filter the spend is only really
// new zones (~a few per year at IANA). ON CONFLICT (name) DO NOTHING
// remains a race guard: a zone inserted concurrently (ingest EnsureZone, a
// second sync) between the check and the insert is not an error,
// RowsAffected counts only the actually inserted rows.
const zonesSyncInsertSQL = "INSERT INTO zones (name, name_unicode, is_active) " +
	"SELECT u.name, nullif(u.unicode, ''), true " +
	"FROM unnest($1::text[], $2::text[]) AS u(name, unicode) " +
	"WHERE NOT EXISTS (SELECT 1 FROM zones AS z WHERE z.name = u.name) " +
	"ON CONFLICT (name) DO NOTHING"

// zonesDeactivateMissingSQL — disabling DB zones missing from the slice
// (retired TLDs): a separate statement, only by the explicit
// --deactivate-missing flag. The is_active guard — idempotency (a repeated
// run disables 0 rows) and an honest report: RETURNING returns only the
// actually disabled ones. Domains and counters are not touched — the
// zones disable semantics (spec 13.1); updated_at is monitor's ownership
// (T-078).
// name <> ALL($1) — set subtraction on the DB side (NOT IN without NULL
// traps: the names array is NOT NULL).
const zonesDeactivateMissingSQL = "UPDATE zones SET is_active = false " +
	"WHERE is_active AND name <> ALL($1::text[]) " +
	"RETURNING id, name, coalesce(name_unicode, '') AS name_unicode"

// ---------------------------------------------------------------- parser

// zoneListFile — a parsed registry slice: the version from the first
// comment line, the zones in punycode without duplicates in file order and
// the duplicate count (one zone in two spellings — unicode and punycode —
// converges into one after normalization).
type zoneListFile struct {
	Version    string
	Zones      []string
	Duplicates int
}

// parseZoneList parses the IANA slice: "#" comment lines are skipped, the
// first of them — the file header before the first zone — carries the file
// version (into the result; a comment after the zone lines does not count
// as the version), empty lines do not count as data (a trailing newline is
// not junk), each zone line is validated by idn.NormalizeZone — the same
// pipeline as the incoming directories and the zones enable/disable
// arguments: junk is an error with the line number, there are no silent
// skips (a broken slice must not reach the DB).
func parseZoneList(data []byte) (zoneListFile, error) {
	var res zoneListFile
	seen := make(map[string]bool)
	firstComment := true
	for i, line := range strings.Split(string(data), "\n") {
		num := i + 1
		s := strings.TrimSpace(line)
		if s == "" {
			continue
		}
		if strings.HasPrefix(s, "#") {
			if firstComment {
				res.Version = zoneListVersion(s)
				firstComment = false
			}
			continue
		}
		firstComment = false // data started — the file header is over
		zone, err := idn.NormalizeZone(s)
		if err != nil {
			return zoneListFile{}, fmt.Errorf("строка %d: %q: %v", num, s, err)
		}
		if seen[zone] {
			res.Duplicates++
			continue
		}
		seen[zone] = true
		res.Zones = append(res.Zones, zone)
	}
	// An empty list — a rejection before the DB: inserting an empty batch
	// is pointless, and --deactivate-missing with an empty list would
	// disable all zones at once.
	if len(res.Zones) == 0 {
		return zoneListFile{}, errors.New("зон в файле нет (только комментарии и пустые строки)")
	}
	return res, nil
}

// zoneListVersion extracts the file version from the first comment line
// of the slice: the canonical format — "# Version 2026101900, Last
// Updated …", the version is the first word after "Version" without the
// comma. A comment of another format (a custom file) is returned in full:
// that is all the file reports about itself.
func zoneListVersion(comment string) string {
	s := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "#"))
	if s == "" {
		return ""
	}
	fields := strings.Fields(s)
	if len(fields) >= 2 && strings.EqualFold(fields[0], "version") {
		return strings.TrimSuffix(fields[1], ",")
	}
	return s
}

// ---------------------------------------------------------------- download

// fetchZoneList downloads the slice by URL: stdlib net/http with a timeout
// for the whole exchange, a ctx cancellation aborts the download (Ctrl-C).
// Non-200 — an error (a rare-for-files status describes the source problem
// better than the response body), the body — with the zoneListMaxBytes
// size cap.
func fetchZoneList(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("загрузка %s: %w", url, err)
	}
	client := &http.Client{Timeout: zoneSyncHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("загрузка %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("загрузка %s: HTTP %s", url, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, zoneListMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("загрузка %s: чтение тела: %w", url, err)
	}
	if len(data) > zoneListMaxBytes {
		return nil, fmt.Errorf("загрузка %s: файл больше лимита %d байт", url, zoneListMaxBytes)
	}
	return data, nil
}

// readZoneListFile reads the slice file with the same zoneListMaxBytes cap
// as the URL download: --file and --url are the same input, the protection
// is the same (an erroneously specified giant file must not be read into
// memory in full).
func readZoneListFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, zoneListMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > zoneListMaxBytes {
		return nil, fmt.Errorf("файл больше лимита %d байт", zoneListMaxBytes)
	}
	return data, nil
}

// ---------------------------------------------------------------- write

// zonesSyncInsert performs the batch insert of new zones (zonesSyncInsertSQL)
// and returns the inserted count (the already existing ones are cut off by
// NOT EXISTS/ON CONFLICT).
func zonesSyncInsert(ctx context.Context, pool db.DB, list zoneListFile) (int64, error) {
	names := list.Zones
	unicodes := make([]string, len(names))
	for i, name := range names {
		// displayUnicode (the ingest style): the unicode form if it differs
		// from the punycode; an ASCII zone — an empty string → NULL
		// (nullif in SQL).
		if u := idn.ToUnicode(name); u != name {
			unicodes[i] = u
		}
	}
	tag, err := pool.Exec(ctx, zonesSyncInsertSQL, names, unicodes)
	if err != nil {
		return 0, fmt.Errorf("вставка зон: %w", err)
	}
	return tag.RowsAffected(), nil
}

// zonesDeactivateMissing disables DB zones missing from the slice
// (zonesDeactivateMissingSQL) and returns their rows for the report
// (sorted by name — the RETURNING order is not defined).
func zonesDeactivateMissing(ctx context.Context, pool db.DB, names []string) ([]zoneRow, error) {
	rows, err := pool.Query(ctx, zonesDeactivateMissingSQL, names)
	if err != nil {
		return nil, fmt.Errorf("выключение отсутствующих в срезе: %w", err)
	}
	defer rows.Close()
	var off []zoneRow
	for rows.Next() {
		// Three destinations — exactly the RETURNING columns (id, name,
		// name_unicode): pgx rejects a desync, is_active after the UPDATE
		// is always false.
		var z zoneRow
		if err := rows.Scan(&z.id, &z.name, &z.nameUnicode); err != nil {
			return nil, fmt.Errorf("выключение отсутствующих в срезе: чтение: %w", err)
		}
		off = append(off, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("выключение отсутствующих в срезе: %w", err)
	}
	sort.Slice(off, func(i, j int) bool { return off[i].name < off[j].name })
	return off, nil
}

// zonesSyncRun — writes the reference table: the insert of new zones, the
// read/added/already existed report; by the flag — disabling of the zones
// missing from the slice, with their list.
func zonesSyncRun(ctx context.Context, pool db.DB, list zoneListFile, deactivateMissing bool, stdout io.Writer) error {
	inserted, err := zonesSyncInsert(ctx, pool, list)
	if err != nil {
		return err
	}
	existed := int64(len(list.Zones)) - inserted
	out(stdout, "прочитано зон %d (дублей в файле %d), добавлено %d, уже было %d\n",
		len(list.Zones), list.Duplicates, inserted, existed)
	if !deactivateMissing {
		return nil
	}
	off, err := zonesDeactivateMissing(ctx, pool, list.Zones)
	if err != nil {
		return err
	}
	if len(off) == 0 {
		out(stdout, "отсутствующих в срезе активных зон нет\n")
		return nil
	}
	names := make([]string, len(off))
	for i, z := range off {
		names[i] = zoneDesc(z)
	}
	out(stdout, "выключено отсутствующих в срезе (retired TLD): %d: %s\n",
		len(off), strings.Join(names, ", "))
	out(stdout, "домены выключенных зон не выбираются планировщиком до crawlerctl zones enable <зона>\n")
	return nil
}

// ---------------------------------------------------------------- command

// zonesSync — crawlerctl zones sync: the source is --file (offline, a
// production without outbound internet — the file is downloaded in
// advance, e.g. with curl) or --url (the default — the canonical
// data.iana.org); the slice is read and parsed before the DB connection —
// a source failure / a junk line do not touch the configuration and the pool.
func zonesSync(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones sync")
	file := fs.String("file", "", "файл среза реестра IANA (офлайн; вместо --url)")
	listURL := fs.String("url", "", "URL среза реестра (по умолчанию "+zoneListURL+")")
	deactivate := fs.Bool("deactivate-missing", false,
		"выключить зоны БД, отсутствующие в срезе (retired TLD)")
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("zones sync: лишние аргументы %v (источник задаётся флагами --file/--url)", fs.Args())
	}
	if *file != "" && *listURL != "" {
		return usagef("zones sync: --file и --url несовместимы — укажите один источник")
	}

	// A one-off CLI command: Ctrl-C/SIGTERM interrupt the download/write.
	ctx, stop := signalCtx()
	defer stop()

	var data []byte
	src := *listURL
	if *file != "" {
		src = *file
		raw, err := readZoneListFile(*file)
		if err != nil {
			return fmt.Errorf("zones sync: чтение %s: %w", *file, err)
		}
		data = raw
	} else {
		if src == "" {
			src = zoneListURL
		}
		raw, err := fetchZoneList(ctx, src)
		if err != nil {
			return fmt.Errorf("zones sync: %w", err)
		}
		data = raw
	}
	list, err := parseZoneList(data)
	if err != nil {
		return fmt.Errorf("zones sync: %w", err)
	}
	version := list.Version
	if version == "" {
		version = "неизвестна (комментария версии нет)"
	}
	out(stdout, "источник: %s, версия файла: %s\n", src, version)

	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("zones sync: %w", err)
	}
	defer pool.Close()
	if err := zonesSyncRun(ctx, pool, list, *deactivate, stdout); err != nil {
		return fmt.Errorf("zones sync: %w", err)
	}
	return nil
}
