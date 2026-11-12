// Reference table of ingest zones (spec 5.1, 5.2 items 6-7): EnsureZone
// looks up a zone by punycode name or creates one automatically on the
// first file (is_active = true); DB access goes through the small
// ZoneStore interface (unit tests run against a fake); the SQL
// implementation SQLZoneStore sits on top of internal/db.
package ingest

import (
	"context"
	"errors"
	"fmt"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
)

// ErrZoneNotFound — no zone with the given punycode name exists in the
// zones table. Returned by ZoneStore.ZoneByName; for EnsureZone this is
// the signal to create the zone (spec 5.1: automatically on the first file).
var ErrZoneNotFound = errors.New("ingest: зоны нет в таблице zones")

// ZoneStore — access to the zones table required by file ingest: find a
// zone by name or create one. The interface is deliberately small:
// EnsureZone depends only on it, so unit tests get by with a fake and the
// store is replaceable (SQLZoneStore — pgx; any number of test doubles).
type ZoneStore interface {
	// ZoneByName returns the zones row by punycode name (zones.name,
	// spec 5.2: the ASCII form is what is stored inside the system). A
	// missing zone is ErrZoneNotFound; anything else is a store error.
	ZoneByName(ctx context.Context, name string) (model.Zone, error)

	// CreateZone creates a zone with is_active = true (spec 5.1) and returns
	// its row. An already taken name is not an error: the existing row is
	// returned (concurrent processing of two files of the same zone — one
	// zone, no duplicates, spec 11.4 item 1).
	CreateZone(ctx context.Context, name string, nameUnicode *string) (model.Zone, error)
}

// EnsureZone returns the zone named by the incoming directory (unicode or
// punycode — spec 5.1): the name is normalized to its ASCII form (spec 5.2
// item 6), the zone is looked up in the DB by that name, and a missing one
// is created automatically on the first file with is_active = true (spec
// 5.1, 11.4 item 1). This is how the unicode and punycode directories of
// one zone converge into a single zones row — both names normalize into
// one lookup key.
//
// name of a new zone — punycode (zones.name), name_unicode — the unicode
// form for display only (spec 5.2 item 7); for an ASCII zone it equals
// name and is not stored (NULL): it carries no information.
func EnsureZone(ctx context.Context, store ZoneStore, zone string) (model.Zone, error) {
	ascii, err := idn.NormalizeZone(zone)
	if err != nil {
		return model.Zone{}, fmt.Errorf("ingest: зона %q: %w", zone, err)
	}

	z, err := store.ZoneByName(ctx, ascii)
	if err == nil {
		return z, nil
	}
	if !errors.Is(err, ErrZoneNotFound) {
		return model.Zone{}, fmt.Errorf("ingest: поиск зоны %s: %w", ascii, err)
	}

	z, err = store.CreateZone(ctx, ascii, displayUnicode(ascii))
	if err != nil {
		return model.Zone{}, fmt.Errorf("ingest: создание зоны %s: %w", ascii, err)
	}
	return z, nil
}

// displayUnicode — the value of zones.name_unicode for a punycode zone
// name: the unicode form (idna.ToUnicode on the fly, spec 5.2 item 7) if
// it differs from the ASCII form; for an ASCII zone — nil (NULL).
func displayUnicode(ascii string) *string {
	if u := idn.ToUnicode(ascii); u != ascii {
		return &u
	}
	return nil
}

// zoneColumns — columns of a zones row in model.Zone field order; the
// model's db tags correspond to this list (pgx.RowToStructByName, T-008).
const zoneColumns = "id, name, name_unicode, is_active, domains_total, domains_checked, updated_at"

const (
	sqlZoneByName = "SELECT " + zoneColumns + " FROM zones WHERE name = $1"

	// is_active is set explicitly rather than by the schema DEFAULT: "a new
	// zone is created automatically on the first file (is_active = true)" —
	// spec 5.1. ON CONFLICT DO NOTHING: concurrent creation of the same
	// zone is not an error; without a RETURNING row the existing one is
	// read by a repeated lookup (below in CreateZone).
	sqlZoneCreate = "INSERT INTO zones (name, name_unicode, is_active) " +
		"VALUES ($1, $2, true) ON CONFLICT (name) DO NOTHING RETURNING " + zoneColumns
)

// SQLZoneStore — ZoneStore on top of a pgx pool (internal/db). Real
// behavior against a live DB is covered by integration tests (T-023);
// unit tests verify the SQL text and parameters against a fake pool.
type SQLZoneStore struct {
	pool db.DB
}

// NewZoneStore — a ZoneStore on a pgx pool (*pgxpool.Pool and any wrappers
// satisfying db.DB).
func NewZoneStore(pool db.DB) *SQLZoneStore {
	return &SQLZoneStore{pool: pool}
}

// ZoneByName looks up the zone by punycode name; a missing one — ErrZoneNotFound.
func (s *SQLZoneStore) ZoneByName(ctx context.Context, name string) (model.Zone, error) {
	z, err := s.queryZone(ctx, sqlZoneByName, "поиск зоны "+name, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Zone{}, ErrZoneNotFound
	}
	return z, err
}

// CreateZone inserts the zone (name — punycode, name_unicode — display,
// spec 5.2 item 7) and returns its row. If the INSERT returned no row
// (ON CONFLICT DO NOTHING — the name is taken by a concurrent creation),
// the zone is read by a repeated lookup: two files of one zone give one
// zones row.
func (s *SQLZoneStore) CreateZone(ctx context.Context, name string, nameUnicode *string) (model.Zone, error) {
	z, err := s.queryZone(ctx, sqlZoneCreate, "создание зоны "+name, name, nameUnicode)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.ZoneByName(ctx, name)
	}
	return z, err
}

// queryZone runs a query returning at most one zones row and parses it
// into model.Zone. pgx.ErrNoRows is not wrapped: the callers
// (ZoneByName/CreateZone) interpret it themselves.
func (s *SQLZoneStore) queryZone(ctx context.Context, sql, what string, args ...any) (model.Zone, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return model.Zone{}, fmt.Errorf("db: zones: %s: %w", what, err)
	}
	z, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[model.Zone])
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return model.Zone{}, fmt.Errorf("db: zones: %s: %w", what, err)
	}
	return z, err
}
