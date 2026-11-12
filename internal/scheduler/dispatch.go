// Package scheduler — the check scheduler (spec 5.3): selection of domains
// check_state = pending in batches with round-robin distribution between zones
// (this file, T-027), moving the selected ones to queued with publication to q:dns
// (T-028), returning domains with an expired lease (T-029), scheduled rechecks
// of done and temp_error domains (T-030/T-031).
//
// The batch selection (T-027) is two cheap queries instead of one window
// function over the whole pending set (which on the target corpus is hundreds
// of millions of rows, spec 3.3):
//
//   - active zones with work: zones — a small reference table, read entirely
//     with the domains_pending > 0 filter (T-183, the T-178 counter): the semantics
//     of the column is the predicate of the LATERAL selection (check_state = pending over
//     the base set status <> deleted), the filter cuts off exactly the zones
//     whose probe would return 0 rows — before T-183, circle 1 probed all active
//     zones of the IANA reference table (T-125), ~2000 of them are empty by pending;
//   - the queue prefix of each zone: a LATERAL subquery per zone with a LIMIT —
//     exactly the partial index domains_pending_idx (zone_id, id) WHERE
//     check_state = 0 (migration 000001): reading and sorting go over the
//     ready index, the number of rows read is proportional to the batch and
//     the number of zones, not to the pending volume.
//
// Round-robin zone interleaving (spec 5.3: "round-robin between zones so that
// a large zone does not hold up the others") is assembled in the application by the same
// rules the SQL used to set:
//
//   - each zone is a queue: within a zone domains are ordered by id (the
//     LATERAL selection order = the zone's queue prefix; previous batches consumed
//     its beginning — rows moved to queued leave the selection);
//   - the rn position is the number in the zone's queue (1-based); the merge takes
//     the zone's element with the minimal key (rn − 0.5)/weight, on
//     equality — the smaller zone_id: a zone's adjacent rows do not follow each other
//     in a batch, a large zone does not crowd out the batch;
//   - the zone weight (spec 5.3: "zone weights — optional") shifts the position of its
//     element toward (rn − 0.5)/weight — a zone with weight w occupies in the common
//     order a share proportional to w (step scheduling); a weight of 1 for
//     everyone degenerates into an equal round-robin order. There is no weight column in the
//     schema (spec 6.1 does not require it), the weights come as DispatchQuery parameters;
//     the source of the values is the scheduler.zone_weights configuration: zone names
//     are resolved to zone_id via the reference table (ResolveZoneWeights, T-106).
//
// The zone's share quota of a batch is the LATERAL subquery's LIMIT: ceil(L ×
// max_weight / Σ of the round's zone weights) + 1 — an upper estimate of the heaviest
// zone's share (with equal weights — ceil(L/number of zones) + 1: that many rows of one zone
// at most in the first L of the round-robin order; the round's zones are only
// the active ones with work, filtered-out zones give no rows). The +1 headroom and
// reading "extra" rows are harmless: the outer constraint is the batch LIMIT
// in the merge.
//
// Top-up: if the pool of read rows was not enough for a full batch
// (some zones are exhausted — their shares go to the neighbors), the selection repeats
// only for the "starved" zones (they returned exactly the round quota — possibly,
// truncated), with a keyset cursor over the zone's last id (d.id > after:
// OFFSET shifts are not needed and impossible — the move to queued changes
// the set between queries). The top-up round quota is the batch remainder
// (Limit − collected, T-183): any zone's contribution to the batch does not exceed
// the remainder, reading beyond it does not get into the merge (the outer constraint is
// the batch LIMIT); the worst case "a batch from one zone" — the remainder = Limit,
// the behavior is unchanged. No more than dispatchMaxRounds rounds: the tail will be finished by
// the next call of the loop (at-least-once delivery, spec 3.4; the selection
// does not change state — until T-028).
//
// The guard d.status <> deleted: applying a delete (T-019) changes the status
// without touching check_state — without the guard deleted domains would remain
// candidates forever, even though workers do not write their results (spec §8);
// a domain resurrected by an add file has status = new and returns to the
// selection on its own (T-018). The check_state/status codes are baked into the text as literals
// via model.* — these are schema codes, not data (the internal/ingest style);
// the literals are also needed for the partial index domains_pending_idx to be usable
// (the scheduler cannot prove a parameter in a partial index predicate).
// Inactive zones (is_active = false, crawlerctl zones disable) do not get into the
// selection: their checking is suspended, the domains remain pending and
// will return to the cycle when the zone is enabled.
//
// Memory (spec 3.5): the batch is bounded by Limit (scheduler.batch_size, spec 5.3:
// 1000–10000), the rows read — zones × quota.
package scheduler

import (
	"context"
	"fmt"
	"sort"

	"crawler/internal/db"
	"crawler/internal/model"
)

// dispatchMaxRounds — the limit of batch top-up rounds: each next
// round is a separate query, but only over the "starved" zones with keyset;
// more than two rounds never happen (equal shares are exhausted by the first round),
// the limit guards against pathological weights.
// Equivalent mutants (T-M05): 3 and 5 are indistinguishable from 4 — the loop body
// executes at most 2 times: round 2 requires a "starved" zone (n == quota)
// with an incomplete batch, and from round 1 on, quota = the batch remainder (Limit − collected)
// and such a zone added the remainder on top of the already read rows — in total
// ≥ Limit is available, the merge fills the batch up to Limit (break). The observable
// number of queries is the same.
// mutator-disable-next-line numbers/decrementer, numbers/incrementer
const dispatchMaxRounds = 4

// dispatchZonesSQL — active zones with work (a small reference table, without
// aggregates): domains_pending > 0 — the incremental counter of T-178 (incremental-counters
// spec §4.1), whose semantics is the LATERAL selection predicate itself (check_state =
// pending over the base set status <> deleted), so the filter
// cuts off exactly the zones whose circle-1 probe would return 0 rows (T-183:
// before the filter, circle 1 probed all ~2204 active zones of the IANA reference table,
// ~2000 are empty by pending — query #9 of the slow-query log, §2 of the T-183 spec).
// Freshness is maintained by the transition deltas in the same transactions (T-178): the dispatcher's
// enqueue decrements itself — the column is accurate by the next batch;
// apply_add opens a new zone immediately, without waiting for reconciliation.
// Trust boundary: drift (manual SQL bypassing the increments) can hide a zone
// with work until the nearest reconciliation (monitor.zone_stats_interval) —
// observed by the crawler_counters_drift metric; the reverse case (the counter
// > 0, the queue empty) — one "idle" probe, no degradation. A zone outside the
// selection — its pending domains wait for enabling (crawlerctl) or
// replenishment.
const dispatchZonesSQL = "SELECT id, name FROM zones WHERE is_active AND domains_pending > 0"

// resolveZoneWeightsSQL — the zone reference table in full for resolving weight
// names from the configuration (scheduler.zone_weights, T-106): not only
// the active ones — an inactive zone's weight resolves and takes effect when it is
// enabled (crawlerctl zones enable, T-084) without a scheduler restart.
// zones — a small reference table; name is unique (migration 000001).
const resolveZoneWeightsSQL = "SELECT id, name FROM zones"

// dispatchZoneSQL — zone queue prefixes: unnest of the batch of zones with keyset
// cursors (after = 0 — from the head), LATERAL reads the zone's queue over
// the partial index domains_pending_idx (ORDER BY id, LIMIT — the quota).
// The order of the response rows is not defined — the merge sorts itself.
var dispatchZoneSQL = fmt.Sprintf(
	"SELECT d.zone_id, d.id, d.label "+
		"FROM unnest($1::int2[], $2::int8[]) AS z(zone_id, after_id) "+
		"JOIN LATERAL ("+
		"SELECT dd.zone_id, dd.id, dd.label FROM domains AS dd "+
		"WHERE dd.zone_id = z.zone_id AND dd.id > z.after_id "+
		"AND dd.check_state = %d AND dd.status <> %d "+
		"ORDER BY dd.id LIMIT $3"+
		") AS d ON true",
	int16(model.CheckStatePending), int16(model.StatusDeleted))

// DispatchRow — a domain of the selected batch: the input of the move to queued and
// publication to q:dns (T-028). Name — the full domain name in punycode
// (label || '.' || zones.name, spec 5.2: inside the system the ASCII form).
type DispatchRow struct {
	DomainID int64  `db:"domain_id"`
	ZoneID   int16  `db:"zone_id"`
	Name     string `db:"name"`
}

// DispatchQuery — batch selection parameters.
type DispatchQuery struct {
	// Limit — the batch size: scheduler.batch_size (spec 5.3: 1000–10000).
	Limit int
	// ZoneWeights — zone weights: zone_id → weight ≥ 1. nil or an empty map —
	// all weights are 1 (an equal round-robin order). There is no weight column in the
	// schema (spec 6.1) — the weights are a selection argument, not DB data.
	ZoneWeights map[int16]int
}

// dispatchZone — a selection zone: the reference name and the queue of read
// rows (the zone's pending queue prefix, id ascending).
type dispatchZone struct {
	id   int16
	name string
	rows []dispatchPick
}

// dispatchPick — a row of a zone's queue: the domain key, the label (the name without the zone)
// and the zone (for the full name and the merge).
type dispatchPick struct {
	zoneID   int16
	domainID int64
	label    string
}

// DispatchBatch selects a batch of domains check_state = pending in round-robin
// order between zones (spec 5.3): active zones with work (domains_pending
// > 0, T-183) → their queue prefixes by index → a weighted merge up to
// Limit rows. An empty result is an empty slice without an error. The state
// of the domains is not changed: the move to queued and the
// publication are performed by the next step of the cycle (T-028); the next selection
// continues the zone queues from the point of consumption (moved rows
// leave the pending set).
func DispatchBatch(ctx context.Context, pool db.DB, q DispatchQuery) ([]DispatchRow, error) {
	if err := q.dispatchValidate(); err != nil {
		return nil, err
	}
	zones, err := dispatchActiveZones(ctx, pool)
	if err != nil {
		return nil, err
	}
	if len(zones) == 0 {
		// No zones with work — nothing to probe (active zones
		// without pending are filtered by domains_pending > 0, T-183; inactive
		// ones — by is_active, their pending waits for enabling, crawlerctl).
		return []DispatchRow{}, nil
	}

	// The zone's quota per round — an upper estimate of its share of the batch (see the header).
	weights := q.ZoneWeights
	// Equivalent mutant (T-M05): maxW := 0 ≡ 1 — zones are non-empty here
	// (the early return above), and every normalized weight is ≥ 1, the very first
	// pass of the loop will set maxW ≥ 1.
	// mutator-disable-next-line numbers/decrementer
	sumW, maxW := 0, 1
	for _, z := range zones {
		w := weights[z.id]
		// Equivalent mutants (T-M05): `w <= 1` and `w < 2` do not change
		// the behavior — map values are ≥ 1 (dispatchValidate), a missing
		// zone gives 0: both boundaries turn 0 → 1 and assign w = 1
		// the same value (no-op). `w < 0` (decrementer) is distinguishable — the sum
		// of zero weights would give a division by zero — and is not suppressed.
		// mutator-disable-next-line expression/comparison, numbers/incrementer
		if w < 1 {
			w = 1
		}
		sumW += w
		// Equivalent mutant (T-M05): `w >= maxW` at w == maxW
		// assigns maxW the same value — the result (the maximum of the weights) is the same.
		// mutator-disable-next-line expression/comparison
		if w > maxW {
			maxW = w
		}
	}
	quota := (q.Limit*maxW + sumW - 1) / sumW // ceil(L × maxW / ΣW)
	// Equivalent mutants (T-M05): quota ≥ 1 always — L ≥ 1 (validation),
	// maxW ≥ 1 and sumW ≥ maxW (the maximum is one of the weights), the numerator
	// L×maxW + sumW − 1 ≥ sumW, integer division gives ≥ 1; the branch is
	// defensive, unreachable (including cutting out the branch/if body — a no-op
	// assignment).
	// mutator-disable-next-line expression/comparison, numbers/decrementer, numbers/incrementer, branch/if
	if quota < 1 {
		// Equivalent mutants (T-M05): the body of the unreachable branch (see the if
		// above) — including cutting out the assignment (statement/remove:
		// a no-op `_ = quota`) and numeric shifts.
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer, statement/remove
		quota = 1
	}
	quota++

	// Rounds: first all zones, then only the "starved" ones (they returned exactly
	// the quota: possibly truncated by LIMIT), keyset from the zone's last id.
	var merged []dispatchPick
	pending := zones // the zones of the round
	// Equivalent mutants (T-M05): the round counter is not used in the body,
	// and the body executes at most 2 times (see dispatchMaxRounds), so
	// `round <= …`, starting from −1 or 1 give the same executions.
	// mutator-disable-next-line expression/comparison, numbers/decrementer, numbers/incrementer
	for round := 0; round < dispatchMaxRounds; round++ {
		ids := make([]any, len(pending))
		afters := make([]any, len(pending))
		for i, z := range pending {
			after := int64(0) // from the head of the zone's queue
			// Equivalent mutant (T-M05): `n > 1` ≡ `n > 0` — in round 0
			// z.rows is empty, from round 1 on, pending is only the "starved" zones,
			// each with the previous round's quota of rows (round 0: ≥ 2,
			// see quota++; in top-up rounds rows only
			// accumulate); a length of 1 never occurs.
			// mutator-disable-next-line numbers/incrementer
			if n := len(z.rows); n > 0 {
				after = z.rows[n-1].domainID
			}
			ids[i] = z.id
			afters[i] = after
		}
		picked, err := dispatchPickZones(ctx, pool, ids, afters, quota)
		if err != nil {
			return nil, err
		}
		starving := make([]*dispatchZone, 0, len(pending))
		for _, z := range pending {
			n := len(picked[z.id])
			// Equivalent mutants (T-M05): `n == -1` is unexecutable (len ≥ 0),
			// and skipping the continue for n == 0 is indifferent: appending an empty slice
			// is a no-op, n == quota is false (quota ≥ 1 > 0 = n); the same — cutting out
			// the if body (branch/if): a no-op instead of continue.
			// Replacing continue with break is not equivalent — covered by the test
			// RefillStopsAtExhaustedZone.
			// mutator-disable-next-line numbers/decrementer, branch/if
			if n == 0 {
				continue
			}
			z.rows = append(z.rows, picked[z.id]...)
			if n == quota {
				starving = append(starving, z) // possibly more to come
			}
		}
		merged = dispatchMerge(zones, weights, q.Limit)
		if len(merged) == q.Limit {
			break // the batch is full
		}
		if len(starving) == 0 {
			break // all zones gave out all their pending — nothing left to collect
		}
		// The quota cap by the batch remainder (T-183): any zone's contribution to the batch does not
		// exceed the remainder — the merge needs just remaining rows of the zone
		// to fill the batch completely; reading beyond (previously — Limit for
		// each starved zone) cannot get into the merge. Here
		// remaining ≥ 1: an incomplete batch (the break above).
		quota = q.Limit - len(merged)
		pending = starving
	}

	names := make(map[int16]string, len(zones))
	for _, z := range zones {
		names[z.id] = z.name
	}
	batch := make([]DispatchRow, 0, len(merged))
	for _, m := range merged {
		batch = append(batch, DispatchRow{
			DomainID: m.domainID,
			ZoneID:   m.zoneID,
			Name:     m.label + "." + names[m.zoneID],
		})
	}
	return batch, nil
}

// dispatchActiveZones — active zones with work (domains_pending > 0,
// T-183): id and name (the name is the suffix of the domain's full name, spec 5.2). Zones are a
// small table, reading it entirely is cheap; the snapshot is consistent within the
// call (disabling a zone between the selection queries would leave its domains in
// the round's queue — their move to queued follows the selection,
// at-least-once, spec 3.4; symmetrically, a zone being exhausted by a concurrent
// enqueue between the queries gives one "idle" probe — no degradation).
func dispatchActiveZones(ctx context.Context, pool db.DB) ([]*dispatchZone, error) {
	rows, err := pool.Query(ctx, dispatchZonesSQL)
	if err != nil {
		return nil, fmt.Errorf("scheduler: domain batch selection: active zones: %w", err)
	}
	defer rows.Close()

	zones := make([]*dispatchZone, 0, 16)
	for rows.Next() {
		var z dispatchZone
		if err := rows.Scan(&z.id, &z.name); err != nil {
			return nil, fmt.Errorf("scheduler: domain batch selection: active zones: read: %w", err)
		}
		zones = append(zones, &z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: domain batch selection: active zones: %w", err)
	}
	return zones, nil
}

// ResolveZoneWeights — the source of zone weights from the configuration (spec 5.3:
// "zone weights — optional in the config", T-106): the map "zone name
// (punycode, scheduler.zone_weights keys) → weight" is converted into the map
// "zone_id → weight" (DispatchQuery.ZoneWeights) by reading the zones reference table
// in one query — all zones, not only active ones (an inactive zone's weight takes
// effect when it is enabled, crawlerctl zones enable, without a restart).
// The caller's map is not modified. The values have already been validated by the configuration
// (weight 1..1000); revalidation — dispatchValidate on every selection.
//
// Names without a row in the reference table are not an error: ingest creates zones at
// the first file (spec 5.1), the scheduler may start earlier; such
// names are returned separately (unknown, ascending) for the caller's Warn
// — until the zone appears and a restart happens it is checked with weight 1.
// An empty map or nil — nil without touching the DB: equal weights,
// the default behavior.
func ResolveZoneWeights(ctx context.Context, pool db.DB, byName map[string]int) (weights map[int16]int, unknown []string, err error) {
	if len(byName) == 0 {
		return nil, nil, nil
	}
	rows, err := pool.Query(ctx, resolveZoneWeightsSQL)
	if err != nil {
		return nil, nil, fmt.Errorf("scheduler: zone weights: zone reference table: %w", err)
	}
	defer rows.Close()

	rest := make(map[string]int, len(byName)) // names awaiting a reference-table row
	for name, w := range byName {
		rest[name] = w
	}
	weights = make(map[int16]int, len(byName))
	for rows.Next() {
		var id int16
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, nil, fmt.Errorf("scheduler: zone weights: zone reference table: read: %w", err)
		}
		if w, ok := rest[name]; ok {
			weights[id] = w
			delete(rest, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("scheduler: zone weights: zone reference table: %w", err)
	}
	// Equivalent mutant (T-M05): unknown = nil instead of an empty slice —
	// when rest has no names; the caller (cmd/scheduler) reads only len and
	// strings.Join under a len > 0 guard; empty and nil are indistinguishable.
	// mutator-disable-next-line statement/remove
	unknown = make([]string, 0, len(rest))
	for name := range rest {
		unknown = append(unknown, name)
	}
	sort.Strings(unknown)
	return weights, unknown, nil
}

// dispatchPickZones — zone queue prefixes (dispatchZoneSQL): by after
// (the zone's last read id, 0 — from the head) up to limit rows of each.
// The response is grouped by zone; within a zone it is sorted by id — the
// order of the SQL rows is not defined, the queue must be ascending.
func dispatchPickZones(ctx context.Context, pool db.DB, zoneIDs, afters []any, limit int) (map[int16][]dispatchPick, error) {
	rows, err := pool.Query(ctx, dispatchZoneSQL, zoneIDs, afters, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: domain batch selection: %w", err)
	}
	defer rows.Close()

	picked := make(map[int16][]dispatchPick, len(zoneIDs))
	for rows.Next() {
		var p dispatchPick
		if err := rows.Scan(&p.zoneID, &p.domainID, &p.label); err != nil {
			return nil, fmt.Errorf("scheduler: domain batch selection: row read: %w", err)
		}
		picked[p.zoneID] = append(picked[p.zoneID], p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: domain batch selection: %w", err)
	}
	for z := range picked {
		ps := picked[z]
		// Equivalent mutant (T-M05): `<=` differs from `<` only on
		// equal domainIDs within a zone — there are none: id is part of the domains PK
		// (migration 000001), the LATERAL selection does not repeat rows.
		// mutator-disable-next-line expression/comparison
		sort.Slice(ps, func(i, j int) bool { return ps[i].domainID < ps[j].domainID })
	}
	return picked, nil
}

// dispatchMerge — merging the zone queues into the batch's round-robin order: until
// limit rows are collected, the head of the zone with the minimal key
// (rn − 0.5)/weight is taken (rn — the position in the zone's queue, 1-based), on equality —
// the smaller zone_id. Repeats the order of the T-027 window selection (the global
// ORDER BY (rn − 0.5)/weight, zone_id, rn) over the read prefixes.
func dispatchMerge(zones []*dispatchZone, weights map[int16]int, limit int) []dispatchPick {
	type head struct {
		zone *dispatchZone
		rn   int     // the position of the zone queue's next row
		key  float64 // (rn − 0.5)/weight
	}
	heads := make([]*head, 0, len(zones))
	for _, z := range zones {
		if len(z.rows) == 0 {
			continue
		}
		w := float64(weights[z.id])
		// Equivalent mutants (T-M05): `w <= 1`/`w < 2` do not change
		// the behavior — map values are ≥ 1 (validation), a missing zone
		// gives 0 → 1, and w = 1 is assigned again (no-op). `w < 0`
		// (decrementer) is distinguishable — the key 0.5/0 = +Inf breaks the order —
		// and is not suppressed.
		// mutator-disable-next-line expression/comparison, numbers/incrementer
		if w < 1 {
			w = 1
		}
		h := &head{zone: z, rn: 1, key: (1 - 0.5) / w}
		heads = append(heads, h)
	}

	out := make([]dispatchPick, 0, limit)
	for len(out) < limit && len(heads) > 0 {
		// The minimal key; equal ones — the smaller zone_id (determinism).
		best := 0
		// Equivalent mutant (T-M05): starting with i = 0 compares the head with
		// itself (the keys and zone.id are equal) — the choice of best does not change.
		// mutator-disable-next-line numbers/decrementer
		for i := 1; i < len(heads); i++ {
			h, b := heads[i], heads[best]
			// Equivalent mutant (T-M05): `<=` on zone.id differs from
			// `<` only with equal ids — the heads are different zones, ids
			// are distinct. The line annotation suppresses also the killable mutants of other
			// comparisons of the line (the §4 docs/mutation-testing.md trade-off:
			// splitting across lines is a change to production code).
			// mutator-disable-next-line expression/comparison
			if h.key < b.key || (h.key == b.key && h.zone.id < b.zone.id) {
				best = i
			}
		}
		h := heads[best]
		out = append(out, h.zone.rows[h.rn-1])
		h.rn++
		if h.rn > len(h.zone.rows) {
			heads = append(heads[:best], heads[best+1:]...) // the zone is exhausted
			continue
		}
		w := float64(weights[h.zone.id])
		// Equivalent mutants (T-M05): `w <= 1`/`w < 2` — as in the first
		// normalizing if above; `w < 0` (decrementer) is distinguishable (the key +Inf)
		// and is not suppressed.
		// mutator-disable-next-line expression/comparison, numbers/incrementer
		if w < 1 {
			w = 1
		}
		h.key = (float64(h.rn) - 0.5) / w
	}
	return out
}

// dispatchValidate checks the parameters before touching the DB: an invalid
// query does not spend a connection.
func (q DispatchQuery) dispatchValidate() error {
	if q.Limit < 1 {
		return fmt.Errorf("scheduler: batch selection: Limit = %d, expected ≥ 1 (scheduler.batch_size, spec 5.3)", q.Limit)
	}
	for zoneID, w := range q.ZoneWeights {
		if w < 1 {
			return fmt.Errorf("scheduler: batch selection: zone %d weight = %d, expected ≥ 1", zoneID, w)
		}
	}
	return nil
}
