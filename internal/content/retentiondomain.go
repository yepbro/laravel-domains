// Point drain of a single domain's content (T-187) — closing the residual
// class of loss behind the cursor (review of T-185, P1/P3-R3;
// docs/content.md).
//
// The class: removing a Laravel find by re-analysis — delivered without
// fresh (repeated publications of crawlerctl reanalyze campaigns and the
// T-074 cycle, redeliveries of the same messages) — deletes the
// laravel_domains row (aggregateDeleteSQL) without rewriting
// subdomain_results: the domain's old records return to the retention
// candidate set already behind the time-based drain cursor (T-185: a row is
// visited once in its lifetime) — the hourly pass no longer sees them, and
// content and jsonb are held until rare catch-up circles or a recheck
// (~6 months). Fresh find losses do not lose rows (the DELETE+INSERT
// rewrite writes a fresh stamp ahead of the cursor, T-058); deleted domains
// are purged by T-020.
//
// The event is known at the point of the transition — aggregateDomain
// (analyzer, RowsAffected > 0 ∧ ¬fresh ∧ existed; T-187 returns it as a
// signal) — and the drain is performed right there: analyzer/http-worker
// are different services; a signal through the DB (a counter) knows "how
// many" but not "which domains"; without migrations the keys cannot be
// passed; a circle over the counter would bring back the hourly full
// passes rejected by T-185; a Redis signal stream would be a new queue for
// the sake of the P3 tail. A direct call reuses the purge machinery of
// T-166 in full (the selection filter set + retentionPurge) and requires no
// changes to the cycle.
//
// Differences from the cycle pass (deliberate; the interlocks/snapshots of
// T-166):
//
//   - the q:analyze depth interlock and the unconsumed-keys snapshot
//     (retentionpending.go) are not consulted: they live in http-worker,
//     while the drain is performed by the q:analyze consumer itself — by
//     the time of the event, confident domain firings have already been
//     triaged (there are no more of them). The residual window is an
//     unconsumed WEAK row message of the same domain during a stream
//     standstill: late processing on an empty input (content trimmed) will
//     yield "no signs" — a loss is possible only if new rules simultaneously
//     removed the last confident row and raised the weak one's weight (a
//     mixed change of one set), which is healed by a recheck; the
//     analyzeMarkSQL guard (content_hash IS NOT DISTINCT FROM, NULL is
//     legal per T-056) converges on the trimmed row as well;
//   - render_pending/rendered, deleted domains, and a repeated find
//     (NOT EXISTS laravel_domains) are filtered by the same selection:
//     records under the render flag stay behind the drain — the catch-up
//     circle on debt-counter reset (T-185) will catch up with them,
//     deleted ones are purged by T-020, and a re-find via concurrent
//     delivery re-protects the content anew.
//
// It runs in a separate short transaction AFTER the delivery commit
// (drain errors do not bring the analysis down — poison pill); it is
// idempotent: a repeated call will find no candidates (the rows are already
// trimmed), and the purge guards close the rewrite races (T-166).
package content

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DrainDomain — point drain of one domain's non-find content (T-187):
// purge candidates by the PK prefix (zone_id, domain_id) — the
// retentionSelectSQL selection filters without a cursor — are purged by the
// regular retentionPurge batch (removing the reference and jsonb → decref
// → contents deletion, spec 3.4). retention is the same content.retention
// window as the cycle's: the aging boundary is one for all purgers. An
// empty outcome (the domain is a find / under the render flag / all clean)
// is not an error; no transaction is opened.
func DrainDomain(ctx context.Context, pool RetentionDB, retention time.Duration, zoneID int16, domainID int64) (RetentionStats, error) {
	if pool == nil {
		return RetentionStats{}, errors.New("content: retention: domain drain: the pool is not set")
	}
	if retention <= 0 {
		// retention = 0 — content is kept forever (RetentionLoop never
		// fires): point purging would contradict the mode.
		return RetentionStats{}, errors.New("content: retention: domain drain: the retention window is not set (retention disabled?)")
	}
	o := RetentionOptions{Retention: retention}.withDefaults()
	batch, err := retentionDomainSelect(ctx, pool, o, zoneID, domainID)
	if err != nil {
		return RetentionStats{}, err
	}
	if len(batch) == 0 {
		return RetentionStats{}, nil
	}
	// Drain in place, without a cursor: the domain's rows are addressed by
	// the PK prefix (T-274 — nil, as with the retry probe).
	st, err := retentionPurge(ctx, pool, o, batch, nil)
	if err != nil {
		return st, fmt.Errorf("content: retention: domain %d/%d drain: %w", zoneID, domainID, err)
	}
	return st, nil
}

// retentionDomainSelect — purge candidates of a single domain
// (retentionDomainSQL): the domain's rows aged beyond the window and not
// protected (find/render/deletion), in PK order.
func retentionDomainSelect(ctx context.Context, pool RetentionDB, o RetentionOptions, zoneID int16, domainID int64) ([]retentionCandidate, error) {
	rows, err := pool.Query(ctx, retentionDomainSQL, zoneID, domainID, o.Retention.String())
	if err != nil {
		return nil, fmt.Errorf("content: retention: domain drain: selection: %w", err)
	}
	defer rows.Close()

	var out []retentionCandidate
	for rows.Next() {
		var (
			c       retentionCandidate
			variant int16
		)
		if err := rows.Scan(&c.key.zone, &c.key.domain, &c.key.sub, &variant, &c.hash, &c.checkedAt); err != nil {
			return nil, fmt.Errorf("content: retention: domain drain: selection: read: %w", err)
		}
		c.key.variant = variant
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: retention: domain drain: selection: %w", err)
	}
	return out, nil
}
