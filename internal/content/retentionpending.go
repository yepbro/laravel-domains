// Exact retention protection against purging records that queues are
// waiting on (T-166/T-167, the fix of the q:render window). The
// render_pending flag (bit 1 of domains.flags, T-066) protects only records
// whose q:render publication has NOT happened — the queue was full, and the
// flag was set instead of the publication by the same transaction
// (renderq.go). Records whose Result messages are ALREADY published to
// q:render/q:render-fallback/q:analyze (the queue was free) carry no flag:
// with a stuck consumer (the Oct 23 incident: the q:render lag ≈99k,
// last-delivered-id frozen since 08:59, the T-159 render worker not
// deployed) the messages age in the stream together with the records, and
// the selection predicate "r.rendered OR (d.flags & render_pending) = 0"
// does not see them — the very next pass would trim the content_hash and
// headers/cookies/tls/redirects/addrs/cname of records that live messages
// point to. The render reads the content of SPA records (spec 5.6), and the
// analyzer reads the bodies and headers/cookies (spec 5.8): a loss means
// degraded verdicts (for the cert cohort, where the render will not happen
// at all, raw content is the only source of signals) — the T-166 requirement
// "do not delete before rendered" would be broken. q:analyze has a window
// of the same nature but weaker: the interlock over the total depth
// (RetentionDepth) does not see stale unconsumed messages when the depth is
// below the threshold — closed by the same check. A record waiting on
// q:render-fallback (T-238: publication from the finalization path instead
// of the render_failed_at marker) waits for content the same way — worker
// B's pace is limited (FR-13), and when the fallback is exhausted the
// finalization publishes q:analyze over raw bodies.
//
// The protection is exact, by the messages themselves: before a pass (and
// refreshed during a long drain) the keys of unconsumed messages of all
// protected streams are read — records delivered to the group but not
// acknowledged (pending) wait minutes for the consumer (the RecoverLoop
// claim → XACK or q:dead, T-025); the undelivered tail (the lag) waits
// hours when stuck. XRANGE covers the main window and does not touch the
// group at all (XREADGROUP ">" is forbidden here — it would move
// last-delivered-id and break the real consumer). The message key is the
// Result of T-024 (zone_id, domain_id, subdomain_id, variant; published by
// record.go and renderSPAResults) — the same PK as the candidates'
// (retentionSelectSQL); the exception — by a batch filter in Go after the
// selection: the selection SQL does not change (the key set cannot be
// passed to the DB side, and the rewrite races are closed by the UPDATE
// guards anyway).
//
// Races are safe in both directions:
//   - a publication after the snapshot references a fresh record
//     (checked_at has just been written, T-058) — not a candidate, no
//     protection needed; the exception is the re-publication of the T-066
//     rework pass (RequeueRenderPending publishes q:render over the
//     domain's aged records and only then clears the flag) — closed by the
//     snapshot refresh during a long drain;
//   - a message consumed after the snapshot — the record stays excluded
//     until the end of the pass (extra caution for the minutes of the
//     drain); the next pass purges it normally: the protection cannot be
//     late, only delay.
//
// The snapshot is taken once per pass, not per batch: reading the tail
// during a standstill means tens of thousands of records, and fresh
// publications are protected by the freshness of checked_at. The refresh
// happens after every DefaultRetentionPendingRefresh batch selections: a
// long drain (the T-167 backfill — ~114 batches) lasts minutes, in which a
// T-066 pass may re-publish q:render over aged records; re-reading the
// snapshot is cheaper than allowing a window. A refresh in the middle of a
// drain does not revisit what has been passed (forward cursor pagination,
// the T-074 style) — only future batches.
//
// A stream read error is a conservative skip of the pass, like the depth
// error of the interlock: purging "blindly" is costlier than deferring —
// queue records are untouched, committed batches stand (batches are atomic
// in themselves), and the cycle retries after Interval; the outcome goes to
// OnPass (the pass is not failed, it is postponed) — a Warn in the log and
// the crawler_content_retention_pending_errors_total metric
// (retentionmetrics.go).
package content

import (
	"context"
	"fmt"

	"crawler/internal/queue"
)

// DefaultRetentionPendingRefresh — how many drain batch selections share
// one snapshot of unconsumed keys. 25: a typical pass — ~13 inflow batches
// (measurement §12) — refreshes once per couple of passes; the backfill
// (~114 batches, minutes) — ~5 refreshes per pass; the staleness window
// (the race with the T-066 re-publication) narrows from a pass down to
// seconds-minutes of the drain. The refresh costs two XRANGEs over the
// streams.
const DefaultRetentionPendingRefresh = 25

// retentionPendingStreams — the streams whose unconsumed messages protect
// records from purging: the render (the content of SPA records, spec 5.6),
// the analysis (bodies and headers/cookies, spec 5.8), and the render
// fallback (q:render-fallback, T-238: worker B's pace is limited by FR-13 —
// a record may wait for the rework longer than the retention window; its
// raw content is the only source of signals when the fallback is exhausted,
// when worker B finalizes the record and publishes q:analyze). The names
// are constants of the queue package (the single source, as in httpworker).
var retentionPendingStreams = []string{queue.StreamRender, queue.StreamRenderFallback, queue.StreamAnalyze}

// RetentionPendingKey — the key of a subdomain_results row in a Result
// message (T-024): the same PK as the retention candidates' (spec 6.1).
// Exported for the source adapter (cmd/http-worker) that builds the set
// from stream records.
type RetentionPendingKey struct {
	Zone    int16
	Domain  int64
	Sub     int16
	Variant int16
}

// RetentionPending — the source of unconsumed stream keys: a narrow
// interface; the package does not depend on Redis specifics (the style of
// RetentionDepth for the interlock); the service build wires in an adapter
// over the gate client (cmd/http-worker/main.go). The implementation
// returns the set of keys of the stream stream's messages not yet delivered
// to its consumer group (XRANGE from the group's last-delivered-id,
// exclusive, up to '+'); the streams are the constants
// q:render/q:render-fallback/q:analyze (retentionPendingStreams).
type RetentionPending interface {
	PendingKeys(ctx context.Context, stream string) (map[RetentionPendingKey]struct{}, error)
}

// retentionPendingSnapshot — a snapshot of the sets of all protected
// streams: one pass works against one snapshot (see the file header). An
// error of any stream is an error of the whole snapshot: partial protection
// is worse than skipping the pass (an uncovered q:render is the subject of
// the fix).
func retentionPendingSnapshot(ctx context.Context, src RetentionPending) (*retentionPendingSets, error) {
	sets := make(map[string]map[RetentionPendingKey]struct{}, len(retentionPendingStreams))
	for _, s := range retentionPendingStreams {
		keys, err := src.PendingKeys(ctx, s)
		if err != nil {
			return nil, fmt.Errorf("content: retention: непотреблённые %s: %w", s, err)
		}
		sets[s] = keys
	}
	return &retentionPendingSets{sets: sets}, nil
}

// retentionPendingSets — the snapshot: a set of keys per stream.
// The zero value (nil-sets) means "no waiters".
type retentionPendingSets struct {
	sets map[string]map[RetentionPendingKey]struct{}
}

// key — the snapshot's external key for an internal candidate key.
func (p *retentionPendingSets) key(k retentionKey) RetentionPendingKey {
	return RetentionPendingKey{Zone: k.zone, Domain: k.domain, Sub: k.sub, Variant: k.variant}
}

// waiting — whether the key waits on the consumers of at least one stream
// of the snapshot: the single exclusion criterion — used both by deferBatch
// (the batch filter) and by the queue-guard retry list assembly (T-185,
// retentiondrain.go).
func (p *retentionPendingSets) waiting(k retentionKey) bool {
	if p == nil {
		return false // the protection is off — no waiters
	}
	key := p.key(k)
	for _, s := range retentionPendingStreams {
		if _, ok := p.sets[s][key]; ok {
			return true
		}
	}
	return false
}

// countDeferred — accounting of an excluded record by the holding streams
// (a record is published to q:render and q:analyze at the same time — there
// may be two holders; the observability of "who holds" matters more than a
// single count).
func (p *retentionPendingSets) countDeferred(k retentionKey, st *RetentionStats) {
	key := p.key(k)
	for _, s := range retentionPendingStreams {
		if _, ok := p.sets[s][key]; !ok {
			continue
		}
		if st.Deferred == nil {
			st.Deferred = make(map[string]int64, len(retentionPendingStreams))
		}
		st.Deferred[s]++
	}
}

// deferBatch excludes from the batch the candidate records whose keys are
// in the snapshot (the criterion — waiting): their messages wait for
// consumers — the content and jsonb are still needed ("do not delete
// before rendered", T-166); every exclusion is counted in stats.Deferred.
// The filtering is in place: the caller does not need the batch after the
// call, and a second batch allocation in the drain would be superfluous.
func (p *retentionPendingSets) deferBatch(batch []retentionCandidate, st *RetentionStats) []retentionCandidate {
	if p == nil {
		return batch // the protection is off — the batch unchanged
	}
	kept := batch[:0]
	for _, c := range batch {
		if p.waiting(c.key) {
			p.countDeferred(c.key, st)
			continue
		}
		kept = append(kept, c)
	}
	return kept
}
