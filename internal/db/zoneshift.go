// Package db: batch zone state-counter shift (T-178, incremental-counters
// spec §6.1): deltas run in the transition transaction — as a second
// statement (here) or a CTE of the same statement (scheduler: batch
// transitions by RETURNING zone_id). Rolling the transaction back rolls
// back the "transition + delta" pair — no separate consistency is needed
// (incremental-counters spec §3.2.3). Negative values are not clamped: a
// negative counter is visible drift that reconciliation fixes, not a
// greatest(0, …) mask (incremental-counters spec §6.1.4).
package db

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgconn"
)

// Execer — an executor of a single statement: the transition
// transaction's pgx.Tx or a pool (db.DB); the incremental shift runs only
// inside a transition transaction — the interface unifies both.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// ZoneCounterShiftSQL — batch shift of zone columns by deltas: parallel
// arrays (zone_id + 4 deltas; 0 — the column is unchanged;
// domains_laravel is shifted by the analyzer with a separate point
// statement — the laravel pipeline state transitions do not change it).
// unnest — the package's batch UPDATE style (spec 6: "UPDATE … FROM
// unnest(...)").
const ZoneCounterShiftSQL = "UPDATE zones AS z " +
	"SET domains_total = z.domains_total + u.d_total, " +
	"domains_pending = z.domains_pending + u.d_pending, " +
	"domains_queued = z.domains_queued + u.d_queued, " +
	"domains_checked = z.domains_checked + u.d_checked " +
	"FROM unnest($1::int2[], $2::int8[], $3::int8[], $4::int8[], $5::int8[]) " +
	"AS u(id, d_total, d_pending, d_queued, d_checked) WHERE z.id = u.id"

// ZoneShift — an accumulator of transition zone counter deltas: collected
// from the transition statement's RETURNING zone_id (an in-memory GROUP BY
// — tens of zones per chunk), applied with a single ZoneCounterShiftSQL
// statement in the same transaction. Not thread-safe: one transition — one
// accumulator.
type ZoneShift struct {
	byZone map[int16][4]int64
}

// NewZoneShift — an empty delta accumulator.
func NewZoneShift() *ZoneShift {
	return &ZoneShift{byZone: make(map[int16][4]int64)}
}

// Add records the column deltas of one transition zone (values are ±n;
// 0 — the column is unchanged).
func (s *ZoneShift) Add(zone int16, total, pending, queued, checked int64) {
	d := s.byZone[zone]
	d[0], d[1], d[2], d[3] = d[0]+total, d[1]+pending, d[2]+queued, d[3]+checked
	s.byZone[zone] = d
}

// Len — the number of zones with deltas.
func (s *ZoneShift) Len() int { return len(s.byZone) }

// Exec applies the accumulated deltas in the transition transaction. An
// empty accumulator is a no-op without touching the DB. RowsAffected not
// equal to the zone count — the zone row is missing (possible only with a
// manual edit of the reference table): a silent skip is a source of drift,
// the error is louder (the renderBumpDebtCounter style, T-175).
func (s *ZoneShift) Exec(ctx context.Context, ex Execer) error {
	if len(s.byZone) == 0 {
		return nil
	}
	ids := make([]int16, 0, len(s.byZone))
	for id := range s.byZone {
		ids = append(ids, id)
	}
	// The order is deterministic by ascending zone_id — the call protocol
	// is reproducible in unit tests (a Go map iterates in random order).
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	cols := make([][4]int64, len(ids))
	for i, id := range ids {
		cols[i] = s.byZone[id]
	}
	total := make([]int64, len(cols))
	pending := make([]int64, len(cols))
	queued := make([]int64, len(cols))
	checked := make([]int64, len(cols))
	for i, d := range cols {
		total[i], pending[i], queued[i], checked[i] = d[0], d[1], d[2], d[3]
	}
	tag, err := ex.Exec(ctx, ZoneCounterShiftSQL, ids, total, pending, queued, checked)
	if err != nil {
		return fmt.Errorf("db: zone counter shift (%d zones): %w", len(ids), err)
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("db: zone counter shift: %d of %d rows affected (the zone row is required)",
			tag.RowsAffected(), len(ids))
	}
	return nil
}
