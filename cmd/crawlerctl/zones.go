// ---------------------------------------------------------------- zones
//
// crawlerctl zones (T-084, spec 4.1, 5.3): zone management — list/enable/
// disable of the zones.is_active flag and sync — filling the reference
// table from the IANA registry (T-125, zonessync.go). A zone is a
// reference row (created by ingest on the first file, spec 5.1, or by
// zones sync); is_active — the zone participation
// in the scheduler selection: DispatchBatch (T-027) joins zones with the
// z.is_active condition, so a disabled zone is never selected at all — its
// domains stay pending and wait for the zone to be enabled (no datafix
// needed: enable returns the zone into the selection automatically, the
// domain states did not change).
//
// disable does not touch domains (spec 13.1: deleted/disabled — the state
// is preserved): neither check_state, nor statuses, nor results; the
// pause is expressed by the single zone flag. Ingest of a disabled zone
// keeps working: file ingest does not depend on is_active (spec 5.1),
// domains accumulate and are checked after enabling. The progress
// counters domains_total/domains_checked are updated by monitor (T-078,
// spec 5.9) — list only displays them.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"crawler/internal/db"
	"crawler/internal/idn"
)

// zonesHelp — the crawlerctl zones [--help] text and per-action help.
const zonesHelp = `crawlerctl zones — managing the check zones (spec 4.1, 5.3)

Usage:
  crawlerctl zones list
  crawlerctl zones enable <zone>
  crawlerctl zones disable <zone>
  crawlerctl zones sync [--file PATH] [--url URL] [--deactivate-missing]
  crawlerctl zones collapse [--apply] [--skip-reconcile] [--force]

A zone — a name in any form (unicode/punycode, case, a trailing dot),
like the incoming/<zone> directory; punycode is stored in the DB (spec 5.2).

  list                the zone table: id, name (punycode and unicode), the
                      active flag, the total/checked counters (updated by
                      monitor, spec 5.9)
  enable <zone>       enable the zone's check (is_active = true): the zone's
                      domains return into the scheduler selection — the ones
                      that accumulated in pending during the pause are checked
                      in the general order
  disable <zone>      pause the zone's check (is_active = false):
                      the scheduler does not select the zone's domains; the
                      domains themselves, their states and results are not
                      changed (spec 13.1), the zone's file ingest keeps
                      working as before
  sync [--file PATH] [--url URL] [--deactivate-missing]
                      populate the zone catalog from the slice of the IANA
                      root zone registry (tlds-alpha-by-domain.txt, ~1460
                      TLDs, IDNs already in punycode). New zones are added
                      enabled (is_active = true — like the ingest
                      auto-creation, spec 5.1), existing rows are not changed
                      (is_active is the operator's ownership, the counters —
                      monitor), a repeated run changes nothing.
                      The source: --url (the default data.iana.org) or a
                      pre-downloaded file --file (offline mode, production
                      without outbound internet). --deactivate-missing
                      additionally disables the DB zones missing from the
                      slice (retired TLD), with their list in the report.
                      This is a catalog of TLD names, not a source of domains:
                      domains — only through ingest (var/incoming)
  collapse [--apply] [--skip-reconcile] [--force]
                      move the 2LD zone data into TLD zones (T-196, a
                      maintenance window): the zone is the name's last label
                      (example.5g.in → the zone in, the label example.5g).
                      Without the flags — dry-run: inventory and plan, changes
                      nothing. --apply performs the move in batches over all
                      the tables with zone_id (domains with the prefix glued
                      to the label, results, findings, history), rebinds
                      ingest_files with dedup handling, drops the 2LD
                      partitions and deletes their rows; idempotent and
                      resumable (a repeat continues from where it stopped).
                      After the move — the counter reconciliation (shared
                      with monitor); --skip-reconcile skips it, --force
                      ignores the domains in the queue (check_state = 1). The
                      ingest.allowed_zone_paths zones (test ones) are not
                      collapsed. The move is irreversible: the way back is a
                      backup restore (runbook — docs/runbooks.md)

Exit codes: 0 — success, 1 — error (an unknown zone, the DB, the slice
source, a junk line), 2 — usage.
`

// ---------------------------------------------------------------- SQL

// zonesSelectSQL — the whole zone reference table by ascending name (the
// zoneStatsSQL style from internal/status, T-079): name — punycode (the canonical form), coalesce
// gives an empty string instead of NULL for ASCII zones (name_unicode is
// stored only for display and for names differing from name, spec 5.2 item 7).
const zonesSelectSQL = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
	"is_active, domains_total, domains_checked FROM zones ORDER BY name"

// zoneByNameSQL — a zone row by punycode name (UNIQUE, migration 000001):
// the dereference of the enable/disable argument and the "zone already in
// the state" check.
const zoneByNameSQL = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
	"is_active, domains_total, domains_checked FROM zones WHERE name = $1"

// zoneSetActiveSQL — enabling/disabling a zone with a single UPDATE by id:
// only is_active changes, domains and counters are not touched (spec 13.1;
// updated_at is monitor's ownership, T-078). RETURNING — the row after
// the update (empty — a race with zone deletion between the lookup and
// the UPDATE).
const zoneSetActiveSQL = "UPDATE zones SET is_active = $1 WHERE id = $2 " +
	"RETURNING id, name, coalesce(name_unicode, '') AS name_unicode, is_active"

// ---------------------------------------------------------------- data

// zoneRow — a zone reference table row for the CLI (the migration 000001
// columns; the counters are updated by monitor, T-078).
type zoneRow struct {
	id          int16
	name        string // punycode — the canonical form (spec 5.2)
	nameUnicode string // "" — NULL; display only
	active      bool
	total       int64 // domains_total
	checked     int64 // domains_checked
}

// scanZoneRow reads a reference table row in the column order of the SQL constant.
func scanZoneRow(scan func(dest ...any) error) (zoneRow, error) {
	var z zoneRow
	if err := scan(&z.id, &z.name, &z.nameUnicode, &z.active, &z.total, &z.checked); err != nil {
		return zoneRow{}, err
	}
	return z, nil
}

// fetchZones — the whole reference table by ascending name (list).
func fetchZones(ctx context.Context, pool db.DB) ([]zoneRow, error) {
	rows, err := pool.Query(ctx, zonesSelectSQL)
	if err != nil {
		return nil, fmt.Errorf("zones catalog: %w", err)
	}
	defer rows.Close()
	var zones []zoneRow
	for rows.Next() {
		z, err := scanZoneRow(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("zones catalog: reading: %w", err)
		}
		zones = append(zones, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("zones catalog: %w", err)
	}
	return zones, nil
}

// lookupZoneByName — a zone row by punycode name; found = false — no such zone.
func lookupZoneByName(ctx context.Context, pool db.DB, name string) (zoneRow, bool, error) {
	rows, err := pool.Query(ctx, zoneByNameSQL, name)
	if err != nil {
		return zoneRow{}, false, fmt.Errorf("zone by name %s: %w", name, err)
	}
	defer rows.Close()
	if rows.Next() {
		z, err := scanZoneRow(rows.Scan)
		if err != nil {
			return zoneRow{}, false, fmt.Errorf("zone by name %s: reading: %w", name, err)
		}
		return z, true, nil
	}
	if err := rows.Err(); err != nil {
		return zoneRow{}, false, fmt.Errorf("zone by name %s: %w", name, err)
	}
	return zoneRow{}, false, nil
}

// setZoneActive — enabling/disabling by id; returns the row after the
// update. An empty answer — the row disappeared between the lookup and
// the UPDATE (nobody deletes zones, so the rejection is treated as a DB
// contract error).
func setZoneActive(ctx context.Context, pool db.DB, id int16, active bool) (zoneRow, error) {
	rows, err := pool.Query(ctx, zoneSetActiveSQL, active, id)
	if err != nil {
		return zoneRow{}, fmt.Errorf("zone toggle (id %d): %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return zoneRow{}, fmt.Errorf("zone toggle (id %d): row not updated", id)
	}
	var z zoneRow
	if err := rows.Scan(&z.id, &z.name, &z.nameUnicode, &z.active); err != nil {
		return zoneRow{}, fmt.Errorf("zone toggle (id %d): reading: %w", id, err)
	}
	if err := rows.Err(); err != nil {
		return zoneRow{}, fmt.Errorf("zone toggle (id %d): %w", id, err)
	}
	return z, nil
}

// zoneDesc — the zone name for messages: punycode + the unicode form if
// it differs (name_unicode is empty for ASCII zones, spec 5.2 item 7).
func zoneDesc(z zoneRow) string {
	if z.nameUnicode != "" && z.nameUnicode != z.name {
		return fmt.Sprintf("%s (%s)", z.name, z.nameUnicode)
	}
	return z.name
}

// ---------------------------------------------------------------- actions

// zonesListRun — prints the zone reference table: id, name, unicode, act, counters.
func zonesListRun(ctx context.Context, pool db.DB, stdout io.Writer) error {
	zones, err := fetchZones(ctx, pool)
	if err != nil {
		return err
	}
	active := 0
	var total, checked int64
	head := []string{"id", "zone", "unicode", "act", "total", "checked"}
	rows := [][]string{head}
	for _, z := range zones {
		mark := "-"
		if z.active {
			mark = "+"
			active++
		}
		total += z.total
		checked += z.checked
		uni := z.nameUnicode
		if uni == z.name {
			uni = "" // equals the punycode — no column duplication
		}
		if uni == "" {
			uni = "—"
		}
		rows = append(rows, []string{
			strconv.FormatInt(int64(z.id), 10),
			z.name, uni, mark,
			strconv.FormatInt(z.total, 10), strconv.FormatInt(z.checked, 10),
		})
	}
	widths := make([]int, len(head))
	for _, r := range rows {
		for i, c := range r {
			if w := cellWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}
	// Numeric columns (id, counters) — right-aligned, text — left-aligned
	// (the writeStatusTable style, T-079).
	for _, r := range rows {
		var b strings.Builder
		for i, c := range r {
			if i > 0 {
				b.WriteByte(' ')
			}
			if i == 0 || i >= 4 { // id and counters are numbers
				fmt.Fprintf(&b, "%*s", widths[i], c)
			} else {
				fmt.Fprintf(&b, "%-*s", widths[i], c)
			}
		}
		out(stdout, "%s\n", b.String())
	}
	out(stdout, "zones %d, enabled %d, domains total %d, checked %d\n",
		len(zones), active, total, checked)
	return nil
}

// zonesToggleRun — enabling/disabling a zone by name (is_active).
// Idempotent: a repeated enable/disable reports "already enabled/disabled"
// (the subdomains disable style, T-083).
func zonesToggleRun(ctx context.Context, pool db.DB, action string, name string, stdout io.Writer) error {
	z, found, err := lookupZoneByName(ctx, pool, name)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("zone %s is not in the catalog (ingest creates zones on the first file; see crawlerctl zones list)", name)
	}
	want := action == "enable"
	if z.active == want {
		state := "enabled"
		if !z.active {
			state = "disabled"
		}
		out(stdout, "zone %s (id %d) already %s\n", zoneDesc(z), z.id, state)
		return nil
	}
	after, err := setZoneActive(ctx, pool, z.id, want)
	if err != nil {
		return err
	}
	if want {
		out(stdout, "zone %s (id %d) enabled: the domains return into the scheduler selection (pending is checked in the general order)\n",
			zoneDesc(after), after.id)
		return nil
	}
	out(stdout, "zone %s (id %d) disabled: the domain check is paused; the domains, their states and results are not changed (spec 13.1)\n",
		zoneDesc(after), after.id)
	return nil
}

// ---------------------------------------------------------------- commands

// cmdZones — routing of the zone actions: list / enable / disable / sync
// (sync — T-125, zonessync.go) / collapse (T-196, zonescollapse.go).
func cmdZones(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("specify an action: crawlerctl zones list|enable|disable|sync|collapse")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "list":
		return zonesList(rest, stdout, stderr)
	case "enable", "disable":
		return zonesToggle(action, rest, stdout, stderr)
	case "sync":
		return zonesSync(rest, stdout, stderr)
	case "collapse":
		return zonesCollapse(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", zonesHelp)
		return flag.ErrHelp
	default:
		return usagef("unknown action %q (valid: list, enable, disable, sync, collapse)", action)
	}
}

func zonesList(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones list")
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("zones list: extra arguments %v (the command takes no arguments)", fs.Args())
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("zones list: %w", err)
	}
	defer pool.Close()
	if err := zonesListRun(ctx, pool, stdout); err != nil {
		return fmt.Errorf("zones list: %w", err)
	}
	return nil
}

func zonesToggle(action string, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones " + action)
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("specify a zone: crawlerctl zones %s crawl.test", action)
	}
	// Input validation — through the same pipeline as the incoming
	// directories (internal/idn.NormalizeZone): the zone is stored in
	// punycode (spec 5.2).
	zone, err := idn.NormalizeZone(fs.Arg(0))
	if err != nil {
		return usagef("zones %s: invalid zone name %q: %v", action, fs.Arg(0), err)
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("zones %s %s: %w", action, zone, err)
	}
	defer pool.Close()
	if err := zonesToggleRun(ctx, pool, action, zone, stdout); err != nil {
		return fmt.Errorf("zones %s %s: %w", action, zone, err)
	}
	return nil
}
