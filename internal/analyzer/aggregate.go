// Package analyzer analyzes saved content against the Laravel rules
// (spec 5.8): passive, over already downloaded pages, cookies and headers.
// Consuming q:analyze and writing laravel_findings — T-071 (worker.go),
// re-analysis — T-074 (reanalyze.go).
//
// This file is the domain-level aggregation (T-072; spec 5.8: "at the
// domain level — an aggregate (the maximum score, the union range, the
// subdomain mask)", the laravel_domains table — §6.1). The recalculation
// runs in the worker transaction right after the domain findings are
// written: every committed state is consistent — findings and the
// aggregate do not diverge (spec 3.4).
//
// The aggregate is built from the domain's confident hits (the
// laravel_findings with is_laravel; §13.1: the laravel_domains entry
// threshold "score ≥ 0.6" is the definition of confidence; weak hits
// remain only in findings):
//
//   - score — the maximum: only the maximum could reach the threshold, so
//     the value equals the maximum over all the domain's findings;
//   - the version range — the union of the confident hits' ranges: the
//     lower bound is min(ver_min), the upper is max(ver_max) (spec 5.8
//     "union range" — not the intersection: the intersection is computed
//     by the findings themselves against the rules of one page). The
//     comparison is numeric by components (rules.CompareVersions): a
//     textual MIN/MAX in SQL would order "10" before "5.3". A NULL bound
//     on any hit (no bound or a T-070 conflict) propagates to the domain:
//     the union is unbounded on that side; a bound inversion is impossible
//     — every hit has ver_min ≤ ver_max (T-070);
//   - found_on_mask — the bits of subdomains with an is_laravel hit (any
//     variant — the variants of one subdomain collapse into a bit), the
//     model.DNSMask semantics: bit # = subdomains.id, bits 0..62;
//   - rules_version — the version of the rule set the recalculation ran
//     with: during the re-analysis transition period (T-074) the domain
//     findings are of mixed versions, the row is rewritten by every
//     delivery — after the last update of the domain findings the version
//     is fresh;
//   - updated_at — the DB server's now(): the transaction time (the record.go style).
//
// A domain without is_laravel rows does not enter laravel_domains (the
// §13.1 threshold): the recalculation deletes an existing row — the
// earlier signs were not confirmed in the current episode (§3.5: only
// the last result is kept). Row presence transitions are written to the
// append-only laravel_history journal (T-118, appeared/lost) — only for
// fresh HTTP episodes (queue.Result.Fresh): re-analysis (T-074)
// recalculates the same downloaded content against new rules and is not
// a fact about the site; the semantics details are in the
// aggregateDomain comment. A find removal by re-analysis (non-fresh) is
// the signal for the domain content point drain (T-187,
// content.DrainDomain): the return value of aggregateDomain.
//
// Reconciling parallel deliveries of a domain: the worker processes up
// to Concurrency deliveries at once, there can be several instances
// (spec 4.2), and the HTTP stage publishes q:analyze in a batch over a domain's rows.
// Two recalculations reading the findings before each other's commit
// would produce an aggregate without the neighbor delivery's contribution
// (the later write would erase the earlier one — until the domain's next
// delivery). Therefore the recalculation first takes a
// pg_advisory_xact_lock on domain_id (globally unique — the
// domain_id_seq sequence, T-003): a second recalculation of the domain
// waits for the first one's commit and reads already complete findings.
// An advisory lock, not a domains row lock: by that moment the worker
// holds the subdomain_results row lock (the T-071 stamp), and the
// "stamp → domains" chain would close into a cycle with the T-058
// recheck transaction (UPDATE domains → DELETE subdomain_results);
// only analyzer recalculations take an advisory key of this class — no wait cycles.
package analyzer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"crawler/internal/model"
	"crawler/internal/rules"
)

// The aggregation SQL. Keys are type literals (the worker.go style);
// now() is the DB server transaction time (record.go).
var (
	// aggregateLockSQL — serializes the recalculations of one domain until
	// the end of the transaction (see the package doc comment). It is
	// transaction-scoped — released by commit/rollback, there is no separate unlock.
	aggregateLockSQL = "SELECT pg_advisory_xact_lock($1::int8)"

	// aggregateSelectSQL — the recalculation input: the domain's confident hits.
	// Read in the worker transaction — it sees this delivery's own findings
	// write. The order is by PK (the leaf partition key sort), deterministic:
	// equal bounds written differently ("9.19" and "9.19.0") put the first
	// one into the aggregate.
	aggregateSelectSQL = "SELECT subdomain_id, score, ver_min, ver_max FROM laravel_findings " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 AND is_laravel"

	// aggregateUpsertSQL — writing the aggregate: an upsert by the
	// laravel_domains PK (§3.5 rewrite, §6.1): repeated deliveries and
	// re-analysis rewrite the row from scratch. NULL ver_min/ver_max —
	// the union is unbounded on that side (including a T-070 conflict).
	aggregateUpsertSQL = "INSERT INTO laravel_domains (" +
		"zone_id, domain_id, score, ver_min, ver_max, found_on_mask, rules_version, updated_at) " +
		"VALUES ($1::int2, $2::int8, $3::float4, $4::text, $5::text, $6::int8, $7::int4, now()) " +
		"ON CONFLICT (zone_id, domain_id) DO UPDATE SET " +
		"score = $3::float4, ver_min = $4::text, ver_max = $5::text, " +
		"found_on_mask = $6::int8, rules_version = $7::int4, updated_at = now()"

	// aggregateDeleteSQL — no confident hits: the domain row does not exist
	// (the §13.1 threshold), an existing one is deleted (§3.5). A missing
	// row is not an error (DELETE 0).
	aggregateDeleteSQL = "DELETE FROM laravel_domains " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8"

	// aggregateExistsSQL — whether the domain row existed before the
	// recalculation: the appeared/lost transition compares "before" and
	// "after" (T-118). By PK — an index lookup; it runs under the domain's
	// advisory lock, a parallel recalculation will not slip in between the check and the write.
	aggregateExistsSQL = "SELECT EXISTS (" +
		"SELECT 1 FROM laravel_domains WHERE zone_id = $1::int2 AND domain_id = $2::int8)"

	// aggregateHistorySQL — the appeared/lost transition journal (T-118,
	// the larasites laravel_histories analog): append-only, changed_at is
	// the transaction time (the DB server now()). Written only on fresh —
	// a fresh HTTP episode (queue.Result.Fresh): re-analysis of the same
	// content against new rules is not a fact about the site (see the
	// package doc comment).
	aggregateHistorySQL = "INSERT INTO laravel_history (zone_id, domain_id, appeared) " +
		"VALUES ($1::int2, $2::int8, $3)"

	// aggregateZoneLaravelSQL — the delta of the zone Laravel counter on a
	// laravel_domains row presence transition (T-178, the incremental-counters spec §6.5):
	// it runs in the recalculation transaction — a rollback rolls back the
	// "row + counter" pair. Transitions are counted by fact (existed vs
	// the result), not only by fresh: re-analysis also changes the
	// laravel_domains set, the reconciliation (the count(*) aggregate)
	// counts rows regardless of their origin. RowsAffected != 1 — the
	// zone row is gone (a manual reference-table edit): silent drift is unacceptable.
	aggregateZoneLaravelSQL = "UPDATE zones " +
		"SET domains_laravel = domains_laravel + $1::int8 WHERE id = $2::int2"

	// aggregateCounterDeltaSQL — the delta of the appeared/lost history
	// cumulative in crawler_counters (T-178, the incremental-counters spec §6.5):
	// an upsert with accumulation — the first reconciliation creates the
	// row (the incremental-counters spec §5), the increment before it
	// guarantees the row itself (unlike render_pending T-175, whose row is
	// backfilled by migration 000015). The same transaction as the
	// laravel_history row: a rollback rolls back the pair.
	aggregateCounterDeltaSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ($1, $2::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"
)

// The row names of the history cumulatives (crawler_counters, T-178).
const (
	counterLaravelAppeared = "laravel_appeared_total"
	counterLaravelLost     = "laravel_lost_total"
)

// aggregateDomain recalculates the domain's laravel_domains row from
// its findings. It is called from the worker transaction (T-071) after
// the findings are written: the SELECT sees this transaction's writes,
// a failure of any step rolls back the findings and the aggregate
// together. version is the delivery rule set version (rules.Set.Version):
// the recalculation is stamped with it.
//
// fresh is the delivery of a fresh HTTP episode (queue.Result.Fresh: a
// publication of the HTTP/render stage; re-analysis T-074 — false). When
// the laravel_domains row presence changes and fresh, a transition is
// written to laravel_history (T-118): appearance — appeared=true,
// disappearance — false. Without fresh no history is written: a
// recalculation under a new rule set and the flapping of borderline
// scores around the threshold (T-116) are not site events. The
// semantics are tied to the row, not the site: "left during re-analysis,
// came back with a fresh episode" leaves a single appeared (appearance)
// without a paired lost — the price of protection against rule noise; the trend over a set of domains is correct.
//
// The unfound return is the signal of a find removal by re-analysis
// (T-187): the laravel_domains row existed and was removed by a
// delivery WITHOUT fresh (RowsAffected > 0 ∧ ¬fresh ∧ existed). The
// domain's old subdomain_results rows are not rewritten by such a
// delivery and return to the retention candidate set behind the drain
// cursor (T-185) — the caller answers with the domain content point
// drain (content.DrainDomain, worker.drainUnfound). On fresh the signal
// is false: the fresh episode rewrote the rows with marks ahead of the cursor (T-058).
func aggregateDomain(ctx context.Context, tx pgx.Tx, zoneID int16, domainID int64, version int32, fresh bool) (unfound bool, err error) {
	// The lock first: the recalculations of one domain line up in a queue,
	// the second reads the findings after the first one's commit.
	if _, err := tx.Exec(ctx, aggregateLockSQL, domainID); err != nil {
		return false, fmt.Errorf("domain lock: %w", err)
	}

	rows, err := tx.Query(ctx, aggregateSelectSQL, zoneID, domainID)
	if err != nil {
		return false, fmt.Errorf("reading hits: %w", err)
	}
	agg, err := scanAggregate(rows)
	rows.Close()
	if err != nil {
		return false, fmt.Errorf("reading hits: %w", err)
	}

	// The row presence before the recalculation — for the appeared/lost
	// transition (T-118); under the advisory lock a concurrent recalculation
	// will not change it between the check and the write.
	var existed bool
	if err := tx.QueryRow(ctx, aggregateExistsSQL, zoneID, domainID).Scan(&existed); err != nil {
		return false, fmt.Errorf("reading domain row: %w", err)
	}

	if agg.findings == 0 {
		tag, err := tx.Exec(ctx, aggregateDeleteSQL, zoneID, domainID)
		if err != nil {
			return false, fmt.Errorf("deleting domain row: %w", err)
		}
		// Equivalent mutants (T-M10): RowsAffected of a DELETE is 0 or 1,
		// and existed is read under the advisory lock BEFORE the delete
		// (only recalculations write laravel_domains, they are serialized) —
		// existed=true ⟺ the DELETE will affect 1: weakening "> 0" to ">= 0" /
		// "> -1" and cutting out existed are distinguishable only at
		// RowsAffected == 0 ∧ existed — unreachable. Row granularity also kills
		// the killable fresh cut — it is covered by TestAggregate_History
		// (re-analysis writes no history); trade-off §4 docs/mutation-testing.md.
		// mutator-disable-next-line expression/comparison, expression/remove, numbers/decrementer
		if tag.RowsAffected() > 0 && existed {
			// The row existed and disappeared — the zone counter −1 (a presence
			// transition, T-178); on fresh — lost in the journal and the cumulative (T-118).
			if err := aggregateShiftZoneLaravel(ctx, tx, zoneID, -1); err != nil {
				return false, err
			}
			if fresh {
				if _, err := tx.Exec(ctx, aggregateHistorySQL, zoneID, domainID, false); err != nil {
					return false, fmt.Errorf("history write (lost): %w", err)
				}
				if err := aggregateBumpHistoryCounter(ctx, tx, counterLaravelLost, 1); err != nil {
					return false, err
				}
			}
			// The T-187 signal: a transition without fresh — re-analysis removed
			// the find without rewriting the domain's rows (see the function doc comment).
			return !fresh, nil
		}
		return false, nil
	}
	tag, err := tx.Exec(ctx, aggregateUpsertSQL,
		zoneID, domainID, agg.score, agg.verMin, agg.verMax,
		int64(agg.foundOnMask), version)
	if err != nil {
		return false, fmt.Errorf("writing domain row: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("writing domain row: %d rows affected", tag.RowsAffected())
	}
	if existed {
		return false, nil
	}
	// There was no row and one appeared — the zone counter +1 (a presence
	// transition, T-178; regardless of fresh — re-analysis also changes the
	// set); on fresh — appeared in the journal and the cumulative (T-118).
	if err := aggregateShiftZoneLaravel(ctx, tx, zoneID, 1); err != nil {
		return false, err
	}
	if fresh {
		if _, err := tx.Exec(ctx, aggregateHistorySQL, zoneID, domainID, true); err != nil {
			return false, fmt.Errorf("history write (appeared): %w", err)
		}
		if err := aggregateBumpHistoryCounter(ctx, tx, counterLaravelAppeared, 1); err != nil {
			return false, err
		}
	}
	return false, nil
}

// aggregateShiftZoneLaravel — the zones.domains_laravel delta in the
// recalculation transaction (aggregateZoneLaravelSQL, T-178).
func aggregateShiftZoneLaravel(ctx context.Context, tx pgx.Tx, zoneID int16, delta int64) error {
	tag, err := tx.Exec(ctx, aggregateZoneLaravelSQL, delta, zoneID)
	if err != nil {
		return fmt.Errorf("laravel zone counter: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("laravel zone counter: %d rows affected (zone row %d is required)", tag.RowsAffected(), zoneID)
	}
	return nil
}

// aggregateBumpHistoryCounter — the increment of the appeared/lost
// cumulative in the recalculation transaction (aggregateCounterDeltaSQL, T-178).
func aggregateBumpHistoryCounter(ctx context.Context, tx pgx.Tx, name string, delta int64) error {
	tag, err := tx.Exec(ctx, aggregateCounterDeltaSQL, name, delta)
	if err != nil {
		return fmt.Errorf("history counter (%s): %w", name, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("history counter (%s): %d rows affected", name, tag.RowsAffected())
	}
	return nil
}

// aggRow is a confident hit of the domain, the recalculation input.
type aggRow struct {
	subdomainID int16
	score       float32
	verMin      *string
	verMax      *string
}

// domainAggregate is the aggregate of the domain's Laravel signs (spec
// 5.8): the fields of the laravel_domains row; findings is the number
// of counted hits (0 — no domain row).
type domainAggregate struct {
	score       float32
	verMin      *string
	verMax      *string
	foundOnMask model.DNSMask
	findings    int
}

// scanAggregate folds the domain's confident hits into the aggregate:
// the maximum score, the union version range and the subdomain mask
// (the decisions are in the package doc comment). A bad subdomain
// number is an error: the mask bits are 0..62 (model.DNSMask), a silently
// lost subdomain would corrupt found_on_mask.
func scanAggregate(rows pgx.Rows) (domainAggregate, error) {
	var (
		agg                   domainAggregate
		minStr, maxStr        string
		unboundedMin, haveMin bool
		unboundedMax, haveMax bool
	)
	for rows.Next() {
		var r aggRow
		if err := rows.Scan(&r.subdomainID, &r.score, &r.verMin, &r.verMax); err != nil {
			return domainAggregate{}, err
		}
		// An equivalent mutant (T-M10): ">" → ">=" differs only by
		// reassigning an equal score (the value and type are the same); ±0.0
		// does not arise in the stage data — the hit score is computed by
		// ScoreRules as a noisy-OR of weights [0, 1]. Cutting the guard out:
		// "findings == 0" and "== -1" are covered by the test (a negative
		// junk score of the first row).
		// mutator-disable-next-line expression/comparison
		if agg.findings == 0 || r.score > agg.score {
			agg.score = r.score
		}
		agg.findings++
		if err := agg.foundOnMask.Set(int(r.subdomainID)); err != nil {
			return domainAggregate{}, fmt.Errorf("found_on_mask: %w", err)
		}
		// Bounds: a NULL (no bound/conflict) is remembered as a flag — the
		// union is unbounded on that side; from the known bounds the
		// extreme is taken (min for the lower, max for the upper); equal
		// bounds written differently keep the first (the PK order is deterministic).
		if r.verMin == nil {
			unboundedMin = true
		} else if !haveMin || rules.CompareVersions(*r.verMin, minStr) < 0 {
			minStr, haveMin = *r.verMin, true
		}
		if r.verMax == nil {
			unboundedMax = true
		} else if !haveMax || rules.CompareVersions(*r.verMax, maxStr) > 0 {
			maxStr, haveMax = *r.verMax, true
		}
	}
	if err := rows.Err(); err != nil {
		return domainAggregate{}, err
	}
	// Equivalent mutants (T-M10): on empty input the loop never ran —
	// haveMin/haveMax and unbounded* are false, the function tail does not
	// change the aggregate; the early return and falling through it give
	// the same result ("== -1" and cutting the return are one branch).
	// mutator-disable-next-line numbers/decrementer, branch/if
	if agg.findings == 0 {
		return agg, nil
	}
	// An equivalent mutant (T-M10): "haveMin → true" is distinguishable only
	// at haveMin=false ∧ unboundedMin=false — on non-empty input every row
	// sets at least one of the flags, and empty input is cut off by the return above.
	// mutator-disable-next-line expression/remove
	if haveMin && !unboundedMin {
		agg.verMin = &minStr
	}
	// An equivalent mutant (T-M10): symmetric to verMin — "haveMax → true"
	// is distinguishable only at haveMax=false ∧ unboundedMax=false, which
	// is unreachable on non-empty input.
	// mutator-disable-next-line expression/remove
	if haveMax && !unboundedMax {
		agg.verMax = &maxStr
	}
	return agg, nil
}
