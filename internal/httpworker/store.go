// Reconciling the domain result's content with the store (T-056/T-147; spec 5.5
// item 8, §5.7, §8, §11.4 item 3). The page bodies are saved by an EARLY Put —
// the worker puts every downloaded page into the ContentStore immediately on
// receipt (worker.go checkScheme, T-147) and does not hold the bytes in
// the slot's memory; the alias deduplication (T-055) goes by the hashes. ContentSaver
// is called in Options.OnResult (the cmd/http-worker build) before writing
// the result (Recorder, T-058) and the XACK (spec 4.2) and decides the fate of the
// early Puts' references:
//
//   - the class's original record (StoreContent, T-055) with an untrimmed
//     body and a passed Put inherits the reference: the record's content_hash —
//     is the reference from the Put, the subdomain_results row takes
//     ownership (the refcount is not changed; the swap on a rewrite will remove it, T-044/T-058);
//   - the references that did not become anyone's row are released by Delete:
//     the checks folded by the variant logic (both protocols — one site),
//     the losing alias classes (the content is stored once, spec 5.5
//     item 6), repeated downloads of the same content. Without the folding
//     the refcount would systematically grow — the early Put does not know who
//     its pages will become;
//   - the aliases receive none of its own reference: content_hash
//     is normalized to nil (the class's content is available through
//     the original's alias_subdomain_id, T-055);
//   - a truncated body (Truncated, T-049) never received an early Put —
//     the hash is removed, ContentNotStored is set;
//   - the pages whose Put did not pass (ErrStoreFull — the content
//     write pause, spec 8/T-046; ErrTooLarge — the max_store_bytes limit)
//     are marked by the worker with ContentStored=false: the record lives with
//     content_hash = nil and ContentNotStored — the domain result is NOT
//     dropped (§8: the content write pause, not the results'; a repeated
//     domain check recheck T-030 will download and save). The Warn is written
//     by the worker upon receiving the answer.
//
// The deduplication remains two-level (§8): the task level — the final URLs' cache
// and the DetectAliases classes (a comparison by hashes), the store
// level — Put/PutBatch by hash (T-041/T-042): identical pages
// of different domains lie in one block, the refcount grows, no new bytes
// are written (Created = false, the dedup share metric of spec 11.4 item 3 —
// Save takes the created statistics from the registry of early Puts).
//
// The hash — one and the same source: the worker computes content.HashOf over
// the UTF-8-normalized body (T-051), the early Put receives the same bytes,
// and the worker compares HashOf(data) with the Put's return (the T-041 contract) before
// discarding the body — a desynchronization drops the task immediately (T-147).
//
// The price of the early Put against the T-102 batch: one fsync and INSERT per
// downloaded page instead of a batch per delivery. Accepted deliberately
// (T-147): buffering the bodies until the end of the task for the sake of a group commit
// held ≈3.34 MiB/slot (the measurement of T-092 §11) — the slot memory is dearer
// than extra fsyncs (an SSD holds thousands per second; the post-deploy measurement —
// docs/load-measurements.md). PutBatch remains the BatchStore contract
// (T-102) for consumers storing several contents at once.
//
// At-least-once (spec 3.4): a repeated delivery of the task replays both
// the downloads and the early Puts (dedup: +1 without new bytes) and the reconciliation.
// The references of an attempt that did not reach Save (a process crash, a Put failure, a ctx
// cancellation) remain without a row — a space leak in the safe direction (the
// same choice as SwapRef T-044): the block stays live, no data is
// lost.

package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"crawler/internal/content"
)

// ContentSaver — reconciling the domain result records' content with the store
// (T-056/T-147). Wired by the service build into Options.OnResult (the hook
// is called in the Run loop before the XACK, T-053) as the chain's first link — before
// writing the result (Recorder, T-058): by the time of the row, the early
// Puts' references are already distributed (the ownership — with the rows, the extras — removed):
//
//	saver, err := httpworker.NewContentSaver(store, logger)
//	…
//	opts.OnResult = func(ctx context.Context, res *httpworker.DomainResult) error {
//		if err := saver.Save(ctx, res); err != nil {
//			return err
//		}
//		_, err := recorder.RecordAndPublish(ctx, res) // T-058
//		return err
//	}
//
// A Save error (except the skips above) — an OnResult error: the delivery is not
// acknowledged and will come back with a re-delivery (spec 3.4) — the same
// way the result write (Recorder, T-058) will behave on a DB failure.
//
// Concurrency-safe: the state is immutable after New; Save is called for
// one task's result, there are no concurrent Saves of the same records.
type ContentSaver struct {
	store content.ContentStore
	log   *slog.Logger
}

// NewContentSaver builds the content reconciliation over the store (production —
// *content.PackStore, T-042; nil — an error: a silent "saving to nowhere"
// would lose content unnoticed).
func NewContentSaver(store content.ContentStore, log *slog.Logger) (*ContentSaver, error) {
	if store == nil {
		return nil, errors.New("httpworker: сохранение контента: хранилище не задано")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &ContentSaver{store: store, log: log}, nil
}

// Save distributes the task's early Put references (T-147) over the
// domain result's records:
//
//   - the alias records store no content — they have no own reference in contents,
//     ContentHash is normalized to nil (the class's content is available
//     through the original record's alias_subdomain_id, T-055);
//   - a truncated body (Truncated) and a body without a passed Put — the content
//     is not stored, the hash is removed, ContentNotStored is set (see
//     the file comment); the Warn about the reason was written by the worker on receipt;
//   - an original with an untruncated stored body stays with the hash —
//     its early Put's reference becomes the row's reference;
//   - the early Put references no record ended up on are removed
//     by Delete (reconciling the DomainResult.puts registry);
//   - the records referencing a hash without an early Put — a desynchronization
//     of the worker and the store: a reference without a counter would break the
//     accounting (SwapRefTx T-044, ErrRefIntegrity) — a loud error, the delivery without
//     an XACK (spec 3.4);
//   - a ctx cancellation and other Delete errors — a Save error: the episode is
//     not completed, a repeated delivery will replay it entirely (spec 3.4).
//
// The call's idempotency is not required (one delivery — one call);
// a repeated delivery of the task will simply repeat the downloads, the Puts and the reconciliation.
func (s *ContentSaver) Save(ctx context.Context, res *DomainResult) error {
	if res == nil {
		return nil // does not happen (processDelivery); the guard of direct calls
	}
	var stats saveStats
	claims := make(map[content.Hash]int, len(res.puts))

	//	// Pass 1 — the records' normalization: which of them gets the early
	//	// Put's reference (accounting the requirements — claims by hashes).
	for i := range res.Subdomains {
		for j := range res.Subdomains[i].Records {
			r := &res.Subdomains[i].Records[j]
			if r.ContentHash == nil {
				continue // no content — nothing to store or remove
			}
			if !r.StoreContent {
				//			// The class's alias (T-055): the content is stored once at
				//			// the original; its own reference was never created —
				//			// the record's content_hash NULL.
				r.ContentHash = nil
				continue
			}
			if r.Truncated {
				//			// There was no early Put (T-049): the truncated bytes' hash
				//			// would lie during the deduplication — not stored.
				stats.truncated++
				r.ContentHash, r.ContentNotStored = nil, true
				continue
			}
			if !r.ContentStored {
				//			// The early Put did not pass: ErrStoreFull (the content
				//			// write pause, spec 8/T-046) or ErrTooLarge (the
				//			// max_store_bytes limit, spec 5.5) — the Warn is at the worker; no
				//			// reference for the record.
				stats.unstored++
				r.ContentHash, r.ContentNotStored = nil, true
				continue
			}
			claims[*r.ContentHash]++
		}
	}

	//	// Pass 2 — reconciling the registry of early Puts (in the download order):
	//	// the reference claimed by a record goes to it, the others are removed.
	for i := range res.puts {
		p := &res.puts[i]
		if claims[p.hash] > 0 {
			claims[p.hash]--
			stats.stored++
			if p.created {
				stats.created++
			}
			continue
		}
		stats.released++
		if err := s.store.Delete(ctx, p.hash); err != nil {
			if errors.Is(err, content.ErrNotFound) {
				//			// The reference was already removed by someone else (the race of a
				//			// re-delivery of the same task, T-025): the integrity
				//			// counter is not broken, a Warn for analysis — the block may still be live
				//			// by other references.
				s.log.Warn("снятие ссылки раннего Put: ссылки уже нет (гонка повторной доставки?)",
					"domain", res.Name, "hash", p.hash.String())
				continue
			}
			return fmt.Errorf("httpworker: домен %s: снятие ссылки несохранённого контента %s: %w",
				res.Name, p.hash, err)
		}
	}

	//	// Unmet claims remain — the records reference a hash without
	//	// an early Put: the result construction is desynchronized with
	//	// the store, a reference without a counter would drop the accounting at the very
	//	// first swap (T-044). A loud delivery failure is better than a silent corruption.
	for h, n := range claims {
		if n > 0 {
			return fmt.Errorf("httpworker: домен %s: записи требуют %s ×%d без раннего Put (рассинхронизация воркера и хранилища)",
				res.Name, h, n)
		}
	}

	if stats.unstored > 0 {
		s.log.Warn("контент части записей не сохранён (пауза записи контента/лимит; Warn о причине — при получении страниц)",
			"domain", res.Name, "records", stats.unstored)
	}
	if stats.stored+stats.released+stats.unstored+stats.truncated > 0 {
		s.log.Debug("контент результата домена",
			"domain", res.Name, "stored", stats.stored, "created", stats.created,
			"released", stats.released, "skipped_unstored", stats.unstored, "skipped_truncated", stats.truncated)
	}
	return nil
}

// saveStats — the counters of one delivery's reconciliation outcomes: stored — the references
// that became records; created — of them, those that wrote new bytes (the
// dedup share, spec 11.4 item 3); released — the removed extras; unstored and
// truncated — the records without content (a pause/a limit/a trimming).
type saveStats struct {
	stored, created, released, unstored, truncated int
}
