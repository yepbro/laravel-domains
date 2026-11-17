// Scheduler: the terminal diagnosis of pending rows unenqueueable by name
// (T-309; the hy4-flash-free bulk review, the scheduler stage P3-3) — the
// repair of the enqueue cycle's head-of-line blocking. enqueueValidate
// (enqueue.go) rejected the WHOLE batch before the move: a pending row whose
// full name (label || '.' || zone, dispatch.go T-027) is longer than 253
// octets (spec 5.2 item 4; the queue contract queue.DNSBatch.validate
// rejects such a name) is selected by the dispatcher from the head of the
// zone queue by id into EVERY next batch — the UPDATE does not happen, the
// row never leaves pending, and the cycle repeats the same refusal every
// IdleWait: the valid domains of the zone (and through the shared batch —
// of the other zones too) stop being enqueued FOREVER. The regular ingest
// rejects such names (internal/ingest/validate.go), the sources of the row
// are manual SQL, a corrupted row, a direct fixtures import
// (cmd/crawlerctl/fixtures_import.go writes the label as is) — the same
// trust boundary as the queued/NULL queued_at invariant I1 (checkstate.go):
// the schema does not defend the length, the writers do.
//
// The guard is TWO parts inside the same EnqueueBatch call:
//
//   - the filter (enqueueSplit, enqueue.go): data-level invalid rows (an
//     empty name or longer than enqueueMaxNameLen octets) are EXCLUDED from
//     the move and publication — the cycle continues with the valid rows;
//     a key-level defect (zone_id/domain_id ≤ 0) remains an error of the
//     whole batch — a caller bug, not DB data: such keys do not exist in
//     the database, there is nothing to filter into a diagnosis;
//
//   - the terminal diagnosis (this file): the filtered over-long rows are
//     moved to status = invalid, check_state = done — the R13-like
//     diagnosis of a poison row (dnsworker/result.go resultInvalidSQL,
//     T-109): the row leaves pending (the dispatcher no longer selects
//     it), the recheck R7 does not return invalid — the only way out is
//     manual triage. The empty-name branch of the filter has no DB
//     counterpart: the dispatcher cannot produce such a row (label + '.' +
//     zone always has the dot), its diagnosis is the DNS stage's territory
//     (R13). The counter crawler_scheduler_enqueue_invalid_total and the
//     loop's Warn make the incident observable (runbooks.md §2.11).
//
// Why NOT a CHECK constraint (octet_length(label) ≤ 250 — the review's
// variant (b) schema half): the real invariant is cross-table — the full
// length octet_length(label) + 1 + the zone name ≤ 253, and a CHECK on the
// label alone is a heuristic in BOTH directions: it misses violations (a
// short label with a long zone name) and rejects legal rows (a label of
// 251–252 octets in a one-two letter zone). The project precedent for
// manual SQL breaking a scheduler invariant is a repair pass + a counter
// (leasenull.go, T-255 — the queued_at NULL invariant likewise got no
// CHECK), and a NOT VALID constraint could not be validated while violators
// exist — the premise of this task is that they may.
//
// Why the diagnosis is driven by the filter's exact keys, not by a
// selection pass over pending (the leasenull.go form): no index supports
// "the name length" — the selection would be a full scan of pending
// (hundreds of millions of rows, spec 3.3). The filter hands the diagnosis
// the keys of the rows it has ACTUALLY seen — the cost is proportional to
// the number of broken rows, not to the corpus. The statement re-checks the
// length at the moment the row is LOCKED (the project's guard-predicate
// style, checkstate.go): a row fixed by manual SQL between the selection
// and the lock is not diagnosed. A failure of the diagnosis does not block
// the enqueue of the valid rows: the error is returned AFTER them (the
// publication-failure style of enqueue.go) — the loop Warns and repeats the
// batch; the filter is idempotent, so the retry is safe (spec 3.4).

package scheduler

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// enqueueInvalidSQL — the terminal diagnosis of a batch of unenqueueable rows
// (R16 of the check_state machine, checkstate.go): an unnest batch UPDATE by
// PK probes (the leaseNullUpdateSQL form, T-255) — the planner is led away
// from unnest and probes domains by PK (zone_id, id), there is no scan on
// the UPDATE side. The assignments repeat the R13 poison-row diagnosis
// (resultInvalidSQL, T-109): status = invalid (the check is impossible —
// the name cannot even be published to q:dns), temp_attempts = 0 (there
// will be no retries), clearing the http_published bit (a done row is clean
// of the staging mark, T-108), check_state = done with queued_at = NULL
// (the lease invariants I2/I3 — the row was pending, the mark is already
// NULL, the assignment keeps the statement self-sufficient), last_checked_at
// = now() — the diagnosis is the outcome of the check episode. The guards
// are re-checked at the moment the row is LOCKED: pending (the row is
// exactly a dispatcher selection candidate), not deleted (§13.1 — a delete
// file T-019 must not be overwritten), and the length itself —
// octet_length(label) + 1 + the zone name (a correlated subquery by the
// zones PK; dispatch.go assembles the full name the same way) >
// enqueueMaxNameLen: a row fixed between the selection and the lock is not
// diagnosed. The zone counter deltas (pending −n / checked +n) — the CTE zc
// over the RETURNING zone_id of the diagnosed rows (T-178,
// incremental-counters spec §6.2: the predicate pins the old pending
// state); the outer SELECT — EnqueueBatch counts the outcome by RETURNING
// (the guard could have filtered out part of the batch). Schema codes — as
// literals via model.* (the dispatch.go style).
var enqueueInvalidSQL = fmt.Sprintf(
	"WITH upd AS (UPDATE domains AS t "+
		"SET status = %d::int2, temp_attempts = 0, flags = (t.flags & %d)::int2, "+
		"check_state = %d, queued_at = NULL, last_checked_at = now() "+
		"FROM unnest($1::int2[], $2::int8[]) AS u(zone_id, id) "+
		"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
		"AND t.check_state = %d AND t.status <> %d "+
		"AND octet_length(t.label) + 1 + (SELECT octet_length(z.name) FROM zones AS z WHERE z.id = t.zone_id) > %d "+
		"RETURNING t.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_pending = z.domains_pending - c.n, "+
		"domains_checked = z.domains_checked + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.StatusInvalid), int16(^int16(model.FlagHTTPPublished)), int16(model.CheckStateDone),
	int16(model.CheckStatePending), int16(model.StatusDeleted), enqueueMaxNameLen)

// enqueueDiagnoseInvalid — the terminal diagnosis of the batch of data-level
// invalid rows (enqueueInvalidSQL): the keys come from the enqueue filter
// (enqueueSplit), the guards and the length are re-checked by the statement
// itself at the moment the row is locked. Returns the number of rows
// diagnosed by RETURNING (after the guards — a row fixed in the race window
// is not counted; a duplicate key in the batch is diagnosed once, the second
// probe no longer finds it pending).
func enqueueDiagnoseInvalid(ctx context.Context, pool db.DB, invalid []DispatchRow) (int, error) {
	zones := make([]int16, len(invalid))
	ids := make([]int64, len(invalid))
	for i, r := range invalid {
		zones[i], ids[i] = r.ZoneID, r.DomainID
	}
	rows, err := pool.Query(ctx, enqueueInvalidSQL, zones, ids)
	if err != nil {
		return 0, fmt.Errorf("scheduler: batch enqueue: diagnosing %d invalid rows: %w", len(invalid), err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return 0, fmt.Errorf("scheduler: batch enqueue: diagnosing invalid rows: read: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: batch enqueue: diagnosing invalid rows: %w", err)
	}
	return n, nil
}

// enqueueInvalidMetrics — the diagnosis counter (T-309); nil — a no-op
// (the leaseNullMetrics style).
type enqueueInvalidMetrics struct {
	diagnosed prometheus.Counter
}

// newEnqueueInvalidMetrics — the counter in the reg registry (nil — nil): the
// name/HELP — the single point of truth of the metric documentation
// (docs/services.md); registration is idempotent (T-076) — the loop builds
// it once at start.
func newEnqueueInvalidMetrics(reg *metrics.Registry) *enqueueInvalidMetrics {
	if reg == nil {
		return nil
	}
	return &enqueueInvalidMetrics{
		diagnosed: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_scheduler_enqueue_invalid_total",
			Help: "Terminal diagnosis (status=invalid) of pending rows unenqueueable by name (longer than 253 octets, spec 5.2 item 4) by the enqueue cycle's repair (T-309, R16): without the guard one such row at the head of a zone queue rejected the whole batch and blocked the enqueue forever. Each event is one diagnosed row (by RETURNING after the guards); zero in steady state — the sources are manual SQL, corrupted rows and direct fixtures imports (ingest rejects such names); triage in docs/runbooks.md §2.11.",
		}).WithLabelValues(),
	}
}

// add — n diagnosed rows; nil — a no-op.
func (m *enqueueInvalidMetrics) add(n int) {
	if m == nil {
		return
	}
	m.diagnosed.Add(float64(n))
}
