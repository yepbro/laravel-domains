# Content storage: pack files (spec 5.7)

External (outside PostgreSQL) storage of final pages: the raw HTML is
written by the http-worker, the final DOM — by the render-worker; the
analyzer and `crawlerctl domain --content` read it. Implementation
`internal/content` (`PackStore`), the `ContentStore` interface — task
T-041; compression and the trainable zstd dictionary — T-043.

## Directory and pack files

The directory is `content.dir`. Files:

| File | Purpose |
|---|---|
| `pack-NNNNNN.pack` | a pack: an append-only container of compressed content blocks |
| `.packs.lock` | the inter-process writer lock file (flock) |
| `dict.zst` | the active zstd dictionary (the path — `content.zstd_dictionary`, trained by `crawlerctl dict train`) |
| `dict-NNNNNNNN.zstd` | the previous-dictionary registry: an archive of the dictionary with ID `NNNNNNNN` (8 hexadecimal digits), the readability of old packs after retraining (T-103) |

`pack_id` — the ordinal number in the file name (1, 2, …), grows
monotonically. The pack registry is the set of files itself plus each
file's header (it duplicates the id): there is no separate `packs`
table in the schema (spec 6.1), no migration was introduced. The newest
pack is the maximum `pack_id` in the directory.

## Pack format (v1)

All integers are little-endian.

```
header (23 bytes):
  magic    [4]bytes   "CDPK"
  version  u8         1
  hdrLen   u16        the header length as a whole (23; the fields of
                      future versions are appended at the end — the v1
                      reader takes the first 23 bytes and ignores the
                      unknown tail: a header extension does not break
                      the v1 reader; hdrLen below 23 — corruption)
  packID   u32        the pack number = the number in the file name
  maxSize  u64        the rotation threshold at creation time
                      (content.pack_size)
  dictID   u32        the zstd dictionary ID at creation time
                      (0 — no dictionary)

records until the end of the file:
  len      u32        the length of the block's compressed data
  block    [len]bytes a zstd frame (CompressWithDict — the T-043
                      dictionary, or without one)
```

`contents.off` points at the **beginning of the compressed data** of
the block (after the length prefix), `contents.len` — at its length:
a read is a single `ReadAt`. `raw_len` — the size of the uncompressed
content; it is verified after decompression (corruption detection).
The length prefix is needed by the pack scan of compaction (T-045); an
interrupted write in the middle of a block leaves an unfinished record
without metadata — it does not affect content reads, and the T-045
scanner must be robust to it.

The `dictID` in the header records which dictionary compressed the
pack's blocks at its creation moment (the startup diagnostic —
`WarnDictMismatch`; blocks moved by compaction keep their original
compression — a frame carries its own dictID). Readability of old
frames after retraining is provided by the previous-dictionary
registry — see "Dictionary and the previous-dictionary registry".

## Dictionary and the previous-dictionary registry (T-043/T-103)

Compression of small pages (parkings, stubs) is noticeably more
efficient with a trained dictionary: the typical pattern is encoded
once. The active dictionary is one per process, a file at the
`content.zstd_dictionary` path (`SetDictFile` at startup); **writing**
new blocks always uses the active dictionary (`CompressWithDict`), the
registry takes no part in compression.

**The previous-dictionary registry** makes retraining safe for
readability. Before writing the new file, `crawlerctl dict train`
archives the previous dictionary as `dict-<id>.zstd` (id — the dictID,
8 hexadecimal digits; an atomic write) into the active file's
directory. **Reading** (`Get` → `DecompressWithDict`) works frame by
frame: while a frame decompresses with the active (or a frame-based,
dictionary-less) decoder, the registry is not needed; the "unknown
dictionary" error triggers a late registration — the dictID is
extracted from the frame header, the dictionary is **lazily** loaded
from `dict-<id>.zstd` and **memoized** for the process lifetime
(repeated reads do not hit the file system). A negative result is not
memoized: a file dropped into the registry on a live service is picked
up by the next read, with no restart. The registry also works without
an active dictionary file — the directory is determined by the
configuration path.

A dictionary missing from the registry (the file is lost) — the former
decompression error with a refined message: `zstd.ErrUnknownDictionary`
inside `ErrCorrupted`; the text names the dictID and the path of the
expected file (recovery — runbooks.md §1).

**Lifetime and space.** Registry files never expire and are never
deleted automatically: ~100 KB per dictionary (110 KiB with the
default training size), the space is not critical by hand. A file may
be deleted only when the storage no longer holds any packs compressed
with that dictionary (a fresh storage, a full replacement after
compaction): the registry keeps no automatic copies, and a deleted
file can be restored only by retraining on the same sample corpus
(determinism gives the same ID) — lost samples mean the readability of
old packs is lost forever. The retraining procedure — runbooks.md §1.

**Compaction** moves compressed blocks as they are, without
recompression by the active dictionary: frames carry their own dictID,
the registry is enough for reading. The header of the new pack names
the active dictionary at its creation moment — inside there may also
be moved blocks of previous dictionaries (the `WarnDictMismatch`
diagnostic looks at the pack header; frame reading does not depend on
it).

## Rotation

The threshold is `content.pack_size` (512 MB by default; the spec 5.7
recommendation: 512 MB–1 GB, the bounds are validated by the config).
When appending the next record would exceed the threshold and the pack
already contains blocks, a new pack `pack_id+1` is opened. The first
block of a pack is always written, even if it alone exceeds the
threshold: `max_store_bytes` ≤ 16 MiB and `pack_size` ≥ 1 MiB by
config validation — rotation must always make progress.

## Concurrent writing

Puts from many goroutines are serialized by a mutex on the write
position; writers from different processes (http-worker,
render-worker) — by `flock(LOCK_EX)` on `.packs.lock`. Appends
**always go into the directory's newest pack**: the active pack is
synchronized with the registry under the lock (the registry is
refreshed by the directory mtime, which changes when packs are
created/deleted; appending does not change it), so every pack except
the newest is closed for writing — the precondition of compaction
(T-045). Under the lock:

1. the directory's newest pack is determined via the mtime cache
   (creating/deleting packs invalidates the cache, appending does not);
2. the active pack's size is recomputed with `stat` (another process
   may have appended to it);
3. on rotation the newest pack is looked up in the directory — the new
   pack's `pack_id` is allocated atomically, with no name collisions;
4. the block is appended with a single `write` (the file is opened
   with `O_APPEND`) and synced to disk (`fsync`) — the block's bytes
   reach disk before the metadata row: after a power loss a durable
   row without bytes is impossible (PostgreSQL commits asynchronously,
   `synchronous_commit = off`); then **the metadata row is committed
   before the lock is released**: the block and its row appear
   atomically with respect to compaction, which deletes old packs
   under the same lock (T-045). Holding flock for the duration of a
   database call is the price of the inter-process protocol; content
   writing is not a hot path (pages arrive from the network slower
   than a commit). The header of the pack being created and the
   directory record about the new file are synchronized the same way,
   before the first block.

**Group commit (T-102).** A worker delivery saves several pages at
once, and paying an fsync-per-block for each is wasteful: `PutBatch`
(an optional extension of `BatchStore`) commits a batch of contents
with a single group commit — a dedup batch with one
`UPDATE … FROM unnest(hashes, copy counts) … RETURNING` (the increment
is the copy count of the hash within the batch: each element Put adds
a reference, T-228), then under **one** acquisition of the mutex and
flock: appending of all blocks, **one batch fsync**, **one batched
INSERT** of the metadata (unnest arrays; hash duplicates within the
batch are merged into one row with a reference count — `ON CONFLICT`
cannot touch one row twice, and `refcount = n` preserves the "every
Put adds a reference" semantics). Invariant C-1 is not weakened: the
batch fsync runs before committing any row of the batch ("bytes before
rows"; packs closed by rotation inside the batch are synced at
rotation — like the compaction batches of C-11), and the batched
INSERT runs under an flock taken before the first append — compaction
cannot delete the pack of an uncommitted batch row. A batch failure is
atomic (a single INSERT statement): either there are no rows or all of
them are in place; the failed blocks are junk for T-045; redelivery is
idempotent by hash. A synthetic benchmark (`BenchmarkPut*`,
`internal/content`): row-by-row writing ~7 ms/content versus ~1.2 ms
(batch of 8) and ~0.5 ms (batch of 32) on a local NVMe/ext4 — on tmpfs
the difference disappears, the gain is precisely in amortizing the
fsync.

Get does not block: packs are append-only, committed offsets are
stable, the file is opened for reading on demand. A vanished file
(compaction deleted the pack between reading the row and opening the
file — the row has already moved) is not an error: the metadata is
re-read once, the read follows the new location. A block that is
unreadable while its row is alive (lost/corrupted bytes) is
distinguishable from missing content: Get answers `ErrCorrupted` (the
sentinel in `store.go`), not `ErrNotFound` and not a database error.

## Metadata and transactionality

The `contents(hash PK, pack_id, off, len, raw_len, refcount)` row
(migration 000001, spec 6.1) is maintained through the `internal/db`
pool with four statements:

| Operation | SQL |
|---|---|
| Put, dedup | `UPDATE contents SET refcount = refcount + 1 WHERE hash = $1 AND refcount > 0` |
| Put, new block | `INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) VALUES (…) ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + 1, pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, … RETURNING (xmax = 0)` |
| PutBatch, dedup batch (T-102) | `UPDATE contents SET refcount = refcount + v.c FROM unnest($1::bytea[], $2::int4[]) AS v(h, c) WHERE hash = v.h AND refcount > 0 RETURNING hash` (the increment is the copy count, T-228) |
| PutBatch, new blocks (T-102) | `INSERT INTO contents (…) SELECT … FROM unnest($1::bytea[], …, $6::int4[]) AS v(…) ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + EXCLUDED.refcount, … RETURNING hash, (xmax = 0)` |
| Get | `SELECT pack_id, off, len, raw_len FROM contents WHERE hash = $1 AND refcount > 0` |
| Delete | `UPDATE contents SET refcount = refcount - 1 WHERE hash = $1 AND refcount > 0` |

The order of recording new content (spec 3.4, at-least-once): **spool
the block into the pack (with `fsync`) → commit the metadata**. A
crash between the steps leaves a junk block in the pack without a
metadata row — it is invisible (Get goes only through the metadata)
and is cleaned up by compaction (T-045). The reverse window — "the row
is durable, the bytes are not" — is closed by the fsync of the block
before the INSERT (and the fsync of the header/directory when creating
a pack): a row that survives a power loss always has its bytes on
disk. Deduplication is the other way around, the `UPDATE` first: live
content writes no bytes at all (`Created = false`, the T-041
contract). A batch (`PutBatch`, T-102) follows the same order but with
one fsync and one batched INSERT per batch (the protocol — "Concurrent
writing", group commit); `Created = false` for deduplications and for
hash repeats within the batch.

The reference counter:

- Put adds one reference (dedup — an `UPDATE`, new content — an
  `INSERT`, a concurrent race for one content —
  `ON CONFLICT … refcount + 1`, the loser's block is junk for T-045);
- Delete removes one; `refcount = 0` — the row stays "dead": Get does
  not see it, Delete refuses (`ErrNotFound`); the physical space
  reclamation is compaction (T-045), which must delete dead rows
  together with the blocks;
- resurrection: a Put of content with a dead row appends a fresh block
  and moves the placement onto it (the `EXCLUDED` fields of the
  upsert); `Created` is `false` here — the conflict goes through the
  `DO UPDATE` branch (`RETURNING (xmax = 0)` distinguishes insert from
  update; the CommandTag of `INSERT … ON CONFLICT` is always
  `INSERT 0 1` and does not distinguish the branches). Bytes were in
  fact written — the divergence from "deduplication" is rare and
  affects only the dedup share metric (spec 11.4 item 3);
- a result rewrite with a content change — the reference swap of
  `SwapRef` / `SwapRefTx` / `(*PackStore).SwapRef` (T-044, spec 5.7,
  8): atomically in one transaction `+1` to the new hash, then `−1` to
  the old one (the same statements as above). The "new first, then
  old" order — the choice per spec 3.4: a crash between separately
  executed steps leaves an extra reference at the old content (safe,
  the block stays alive), the opposite order could lose a live
  reference (refcount → 0 for the content the result points to —
  compaction T-045 would free the block). A zero `Hash` — "no
  reference" (NULL `content_hash`): an old zero — only `+1`, a new
  zero — only `−1`, `old == new` — a no-op without a transaction
  (redelivery idempotency). A violated `refcount > 0` guard
  (`RowsAffected` = 0) — the integrity error `ErrRefIntegrity` with a
  transaction rollback: it is better to fail than to silently corrupt
  the counter. The `subdomain_results` record is written by the
  http-worker (T-058), which performs the swap in the same transaction
  (`SwapRefTx`).
- the non-find retention (T-166) — the third reference removal:
  clearing the record (`content_hash` → NULL) decreases `refcount` in
  the same transaction (a `GREATEST` guard against going negative, as
  in the T-020 cleanup); `contents` rows left without references are
  deleted immediately; see "Content retention and the non-find trim
  (T-166)".

### Two representations of "dead" (T-265)

The specification of the refcount machine
(`absent → live(n) → dead → deleted`, the transitions and invariants)
— the doc comment of `internal/content/pack.go` (spec-fsm §3 FR-11).
The "0 = dead" invariant gives dead content two representations; for
the consumers (`Get`, the `Put` dedup, `Delete`) they are
indistinguishable — "there is no content" (`ErrNotFound` /
`UPDATE 0`); the difference is visible to space accounting and
compaction:

| | a tombstone row | the row is deleted |
|---|---|---|
| `contents` | the row exists, `refcount = 0` | there is no row |
| Created by | `Delete`/`SwapRef` of the last reference (T-042/T-044); the T-020 cleanup of deleted domains (the `GREATEST` clamp, the row stays); marking an unreadable block dead by compaction (T-045) | retention T-166 (a decref to 0 → a `DELETE` in the same transaction — bypassing the tombstone); the T-045 compaction purge (`DELETE … WHERE refcount = 0`) |
| Dead block bytes | yes — until the pack's compaction (T-045) | yes — a junk block/tail in the pack until the compaction pass |
| Resurrection by `Put` | the `ON CONFLICT DO UPDATE` branch: `refcount` 0→1, the placement `EXCLUDED` (a fresh block), `Created = false` | the `INSERT` branch: a new row, `Created = true` — indistinguishable from new content |
| Who purges the row | compaction T-045 (together with deleting the pack) | — (the row is already gone) |

Both representations keep the counter non-negative: point statements —
the `refcount > 0` guard predicate (a repeated `Delete` —
`ErrNotFound`, a swap without a live reference — `ErrRefIntegrity`);
bulk passes (retention, the T-020 cleanup) — the
`GREATEST(refcount − n, 0)` clamp on desync. Since migration 000019
(T-265) the invariant is duplicated by the schema CHECK
`contents_refcount_check` (`NOT VALID`, the VALIDATE — per the
docs/schema.md "CHECK invariants" procedure).

## Writing from the http-worker (T-056/T-147)

The raw HTML of final pages is written by the http-worker with an
**early Put** (T-147): every downloaded page goes into the
`ContentStore` immediately upon receipt — the worker
(`internal/httpworker/worker.go`, checkScheme) has read the body,
converted it to UTF-8, computed the hash and the SPA verdict — and
saves the bytes with a `Put` by hash, after which the body is not held
in memory (the slot memory: metadata + `http.task_per_domain` buffers
of readable bodies; before T-147 the bodies of original records were
held until the end of the domain task — ≈3.34 MiB/slot, the T-092 §11
measurement). The service assembly (`cmd/http-worker`) opens
`OpenPackStore` (the `content.dir` directory, the limits of the
`content` section), activates the zstd dictionary (`SetDictFile`,
T-043), and starts `DiskWatchLoop` (T-046) as a goroutine; the store
is passed both to the worker (`Options.Store`) and to the reference
reconciliation (`ContentSaver`, `internal/httpworker/store.go` —
called in the `Options.OnResult` hook before recording the result
T-058 and the XACK, spec 4.2).

Deduplication of parkings and stubs (spec §8) — two-level:

1. **the domain task** (T-053/T-055): the "final URL → result" cache
   does not download one page twice; `DetectAliases` groups records
   with the same final URL or the same content hash into an
   equivalence class — the comparison always goes by hashes, the
   records do not need the bodies; the class's content is stored
   **once** — `content_hash` remains only at the original record
   (`StoreContent = true`); alias records get no reference of their
   own: their `content_hash` is NULL, the content is accessible
   through the original's `alias_subdomain_id`;
2. **the store** (T-041/T-042): `Put` deduplicates by hash — the same
   parking page of different domains is stored as one block,
   `refcount` grows, no new bytes are written (`Created = false`).

The reconciliation of early Put references — `ContentSaver` in the
same hook: the task's Put registry is checked against the result
records — the original record **inherits** its early-Put reference
(the `subdomain_results` row takes ownership, the refcount does not
change, the swap at a rewrite will remove it — T-044/T-058), while
references that never became anyone's row — collapsed by the check's
variant folding (both protocols — one site), the losing alias classes,
repeated downloads of the same content — are released by `Delete`. A
record referencing a hash without an early Put is a desync of the
worker and the store: a loud delivery error (a reference without a
counter would break the `SwapRef` accounting, T-044).

One source for the hash: the worker computes `HashOf` over the
UTF-8-normalized body (T-051), the early `Put` receives the same bytes
and must return the same hash (the T-041 contract) — the worker
verifies the return before discarding the body, a mismatch fails the
task immediately.

An empty body (`Content-Length: 0` — parkings and empty
400/404/500) — valid content with the same contract (T-138): `ToUTF8`
returns an empty non-nil slice, the hash of empty bytes is the same
for all empty pages — the variant folding, aliases, and deduplication
gather them into one class and one block, the early Put saves the
empty bytes like any others, and the record's `content_hash` is not
extinguished (before T-138 the "record body unavailable" fuse read an
empty body as missing and cleared the hash for ~4% of tasks).

The cost of the early Put against the group commit: one fsync and one
metadata INSERT per downloaded page instead of a batch per delivery
(`PutBatch`, T-102; the http-worker no longer uses it — buffering
bodies until the end of the task for the sake of a single commit held
up the slot memory peak; `PutBatch` remains the `BatchStore` contract
for consumers that save several contents at once). The volume: ~2–4
Puts per domain against the former ~3 per stored records plus ~1–2
Deletes of the folding — an SSD holds thousands of fsync/s; the
throughput impact is visible in the stage metrics after the deploy
(docs/load-measurements.md).

Content may fail to be saved — the domain result is **not failed**
because of it (spec §8: a pause of content writing, not of results):
the record lives on with `content_hash = NULL` and the
`Record.ContentNotStored` flag; the worker logs a Warn when it
receives the page (the reconciliation adds a summary Warn about
records without content). The causes:

| Cause | Behavior |
|---|---|
| `ErrStoreFull` — the disk is below the `content.disk_free_percent` threshold (T-046) or ENOSPC | the content is not saved; dedup references to already-live content go through even at overflow; a recheck of the domain (recheck, T-030) will save the page |
| `ErrTooLarge` — a body over `max_store_bytes` (spec 5.5) | the content is rejected by the store entirely, the limit is checked by `Put` |
| the `max_body_bytes` cut-off (`Record.Truncated`, T-049) | not saved at all, and without a `Put`: the hash of truncated bytes would lie during deduplication; partiality is already flagged by `Truncated`/`ErrBody` |

The `content_hash` in `subdomain_results` — a reference into
`contents` (§6.1): the hash appears in a record only together with the
saved block — a reference without a counter would break the accounting
(`SwapRef` T-044, the background cleanup T-020).

Other kinds of early `Put` errors (a database failure, a ctx
cancellation), `Delete` errors of the folding, and registry desync — a
task/`OnResult` error: the delivery is not acknowledged, the
redelivery will replay the episode (spec 3.4). A repeated `Put` of the
same bytes adds an extra reference without a new record — a space leak
in the safe direction (the same choice as `SwapRef`): the block stays
alive, no data is lost. References of an attempt that never reached
the folding (a process crash, a Put failure) — the same safe leak; the
folding removes the extra references of a live attempt only.

Memory: page bodies are not held in the slot memory (T-147) — the
early Put frees the bytes right after reading; the task keeps hashes,
headers, chains, and verdicts, and readable bodies are bounded by
`http.task_per_domain` × `max_body_bytes` (the read buffer is
preallocated by Content-Length without Content-Encoding — no
`io.ReadAll` doublings; the original bytes of non-UTF-8 bodies are
freed right after decoding, before hash/SPA/Put — T-163). The body cap
is `max_body_bytes` (2 MiB), pages per task ≤ 2 × subdomains ≤ 126
(§5.10). The measurement of the ≤1.5 MiB/slot criterion (T-163): the
unit anon / `crawler_http_tasks_inflight`, the transit buffer peaks —
`crawler_http_body_bytes_inflight`, the mixture — the
`crawler_http_body_bytes` histogram (/metrics http-worker, 9104); the
soft heap cap `http.mem_limit` keeps the anon below the unit's
MemoryHigh.

## Content replacement by the render (T-067)

The final DOM of rendered pages is written by the render-worker
(`internal/renderworker`, spec 5.6 "the final DOM replaces the raw
HTML in the storage"): the q:render task points at a
`subdomain_results` row, the DOM (the same HTML-format bytes Chrome
serializes — T-064) is stored with a `Put` into the same storage, then
**in one transaction** the row switches to the new reference
(`rendered = true`, `content_hash` = the DOM hash, the render's final
URL/cookies/headers), and the old reference is removed by `SwapRefTx`
(T-044) — exactly the result-rewrite swap (T-058). Opening
`OpenPackStore`, the dictionary, and `DiskWatchLoop` — the same as the
http-worker's: the packs are shared by both writers (flock, see
"Concurrent writing").

Peculiarities versus the HTTP stage's writing:

- the DOM is byte-identical to the row's stored content — the `Put`
  and the swap are skipped (the reference already exists, an extra
  `+1` is not needed);
- the row is rewritten only while its `content_hash` is the same as
  the one read before the render (the optimistic `UPDATE` guard): a
  domain recheck (T-030/T-058) that rewrote the result cuts off a
  content substitution of a foreign episode — the `Put` reference is
  removed by `Delete`, the delivery is acknowledged;
- an alias row (the class's content is stored at the original, T-055)
  receives **its own** reference after the render: the profiles are
  isolated, every page's DOM is its own thing (the equivalence class
  is an optimization of the HTTP stage, not a ban on the record owning
  content);
- `ErrTooLarge` (a DOM inflated by JS beyond `max_store_bytes`) — a
  task error, the row is not changed: the delivery attempts will run
  out and the task will go to q:dead (T-025) — a signal to the
  operator to raise the limit; `ErrStoreFull` — temporary, a
  redelivery will wait for space.

A q:render redelivery is idempotent (spec 3.4): a repeated render
rewrites the content and the refcount (the T-058 balance),
`rules_version` is reset to NULL — the analyzer's previous verdict
does not apply to the new content, the q:analyze publication after the
commit re-analyzes.

## Compaction (T-045)

Delete and the background cleanup (T-020) only mark data dead
(`refcount = 0`), rewrites move placements onto fresh blocks, and the
write crash windows leave blocks without rows — the space in the packs
is occupied but not live. The background compaction
(`internal/content/compact.go`) rewrites the live blocks into new
packs and deletes the old files when a pack's share of live data falls
**strictly below the threshold** — 50% by default (spec 5.7;
`DefaultCompactThreshold`, `CompactOptions.Threshold`).

**The scan** (`CompactScan`): the pack's `contents` rows (`pack_id`,
`refcount`) plus a traversal of the file by length prefixes — the
live, dead, and junk bytes/blocks; the live share = live bytes /
record bytes. An unwritten tail (a write crash) — junk bytes, not a
block; a row pointing outside a whole record — an error (corruption),
such a pack is not compacted.

**The rewrite of one pack** (`CompactPack`; the order per spec 3.4):

1. the live blocks are rewritten into the newest pack through the
   regular writing path (the `appendBatch` batch: mutex + flock,
   `O_APPEND`, one fsync per batch — the source pack is opened once
   per pass) — the old file is not touched, the blocks are copied
   verbatim (compressed, `len`/`raw_len` in the metadata do not
   change); a block unreadable at copying (lost/corrupted bytes) does
   not stop the rewrite: its row is marked dead by the same
   transaction (`refcount = 0`; such content cannot be deleted any
   other way — the dedup Put sees a live row and writes no bytes), the
   pack stays compactable, and the integrity divergence is collected
   into the `CompactPack` error (a Warn of the pass). A row pointing
   outside a whole record of the file — metadata corruption: the scan
   refuses, such a pack is not compacted (manual intervention);
2. the transaction — in batches of 1000 rows, kept short — moves the
   placements of the live rows onto the new blocks (`UPDATE … WHERE
   pack_id = N AND refcount > 0`) and deletes the pack's dead rows
   (`DELETE … WHERE refcount = 0`, together with the blocks — as the
   T-041/T-042 contract requires);
3. the old pack's file is deleted after the checks (no live rows
   remain for the pack, the size did not change); the unlink — under
   the mutex and flock.

Crash safety at every step (spec 3.4):

- a crash before committing the transactions — the old pack is intact
  and remains the source of truth, the copied blocks are junk in the
  new pack, the next pass replays;
- a crash after the commit, before the file deletion — the file is all
  junk (no live rows remain for the pack): the next pass sees a live
  share of 0 and deletes it;
- a row that died between the read and the transaction (the
  `refcount > 0` guard) is neither moved nor deleted by the batch (it
  was read as live): before the pack file is deleted, such rows are
  purged by the final cleanup of the pack's dead rows (`DELETE … WHERE
  pack_id = N AND refcount = 0`) — orphaned pointers into the deleted
  pack do not accumulate; a row not deleted (a crash before the
  cleanup) is invisible (Get only by `refcount > 0`), a Put
  resurrection will append a fresh block and move the placement — no
  loss.

**Closedness for writing.** Only packs other than the newest are
compacted (checked under flock in the same acquisition in which the
new packs' ids are allocated). Writers append exclusively into the
newest pack and commit the metadata row without releasing the flock,
so the set of a non-newest pack's rows is frozen at the moment the
next pack appears — there are no late appends and no "dangling" rows,
and the check before deletion is a safety net. A deployment
requirement follows: the writers and the compaction must be the same
software version (mixing lock semantics during a rolling deploy is not
supported).

**The loop** (`CompactLoop`, in the style of RecheckDaily and T-020):
`CompactOnce` passes on a ticker (the first one immediately — a drain
of what accumulated during the downtime); a pack error is collected
into the pass error — a Warn, the loop continues; a ctx cancellation —
a clean `nil` (the OnRun contract, spec 3.4). The interval — an hour
(`DefaultCompactInterval`; spec 13.2 leaves the pace to development,
no configuration knobs yet — like T-020). The http-worker starts the
loop — an `OnRun` goroutine, next to `DiskWatchLoop` (one owner per
storage; the second writer does not start the compaction).

The pass accounts for the "old + new" space peak: a pack with live
blocks does not start being copied when the free space is already
below the `content.disk_free_percent` threshold — the copy would be
written over the not-yet-deleted pack and would hit ENOSPC halfway;
such a pack is returned in the pass error (a Warn of the loop) and
retried when the space returns. Packs without live blocks copy nothing
and are deleted at any space level — this is exactly the path to
freeing space on an overflowing volume.

**Occupied space.** There is no separate counter: the space occupied
by the storage is the sum of the directory's pack-file sizes, and
after an old pack is deleted it honestly decreases
(`CompactResult.FreedBytes`; the pass total —
`CompactStats.FreedBytes`). For the same reason the proactive space
monitoring reads the free space with a live `statfs` rather than
accumulating a counter: rotation and compaction are reflected
immediately in the next check (see "Limits and disk overflow").

## Content retention and the non-find trim (T-166)

A disk without new capacity (the operator's decision of 2026-10-23:
volume expansion is unavailable, S3 and archiving are postponed): the
growth of ≈36 GiB/day (content 20.3 + the DB 15.5,
docs/load-measurements.md §12) is stopped by a time window. The
pipeline needs content and the heavy jsonb for a limited time — the
analyzer reads the bodies and the headers/cookies (spec 5.8), the
render replaces the content of SPA records (spec 5.6), the re-analysis
of finds (T-074) re-applies the rules to the stored pages. A record
older than `content.retention` and no longer needed by these consumers
is freed; the row's passport (`http_status`, `final_url`, `error`,
`response_ms`, `checked_at`, `scheme`, `subdomain_id`, `variant`)
always remains — `domain_info` (spec 5.11) and the dashboard keep
answering, and "empty" content is legal for all consumers (T-056).

**The selection criteria** (all parts mandatory):

- `checked_at` older than `content.retention` (the default 6 h; the
  boundary — the database server's `now() − interval`; the HTTP
  episode writes `checked_at` with the same `now()`, T-058);
- the domain is **not a Laravel find**: there is no `laravel_domains`
  row (a find — confident threshold hits of §13.1). Finds always keep
  their content — the proof and the material of the T-074 re-analysis;
  weak hits (`laravel_findings` with `is_laravel = false`) do not
  count as a find;
- the record **is not waiting for a render**: either the domain has no
  `render_pending` flag, or the record is already `rendered` and its
  messages are consumed (the exact protection by unconsumed
  q:render/q:analyze messages — below). The flag is the funnel of the
  T-066 rework pass (`RequeueRenderPending` re-reads record content
  for a repeated SPA verdict); `rendered = true` — the render has
  already replaced the content (T-067), the old one is not needed;
- the domain is not deleted (§13.1: deleted — the territory of the
  background cleanup T-020).

**The cleanup in one transaction** (spec 3.4, the refcount invariants
of T-044): one UPDATE strips the row of both the reference and the
jsonb — `content_hash`, `headers`, `cookies`, `tls`, `redirects`,
`addrs`, `cname` → NULL; by the actually trimmed non-NULL hashes
(aggregated: one piece of content may be referenced from many records)
`contents.refcount` is decreased, and rows that lost their last
reference are deleted immediately — not merely marked dead, as in the
T-020 cleanup: retention is a mass source of dead blocks, and
otherwise the `contents` rows would accumulate until the compaction of
the affected packs (a pack with a live share ≥ the threshold does not
start it). The physical space of the blocks in the packs is returned
by the hourly T-045 compaction — the purge merely revokes the
content's claim on the space. A failure at any step of the transaction
rolls it back entirely: a reference trimmed without a decref would
break the accounting forever (the crash windows are pinned by tests,
in the style of T-044).

The trimmed columns are unindexed (the cleanup UPDATE touches neither
the PK — the row key — nor the T-171 `checked_at` index — the record's
passport remains) — the UPDATE stays HOT (fillfactor 85, migration
000002); the space of the pages is returned by the tuned autovacuum
(docs/db.md); the bloat control — the same as for the mass rechecks of
T-030.

**Races with rewriting** (a recheck T-030 rewrites rows DELETE+INSERT
with a fresh `checked_at` and possibly the same content) are cut off
by the UPDATE's double guard: a re-check of the age and of
`content_hash IS NOT DISTINCT FROM` against the read value (NULL is
correct — "no content" is legal, T-056; the comparison as in
renderUpdateSQL T-067). The protected scenarios are agreed with the
consumers: a q:analyze delivery of a trimmed record analyzes the empty
input (does not crash, the T-056 basis), a q:render task will render
and honestly record its DOM (the update guard simply will not match if
the content was trimmed after the read — lost render work, not lost
data); records of domains with the `render_pending` flag are provided
with content until the flag is cleared.

**The q:analyze interlock** — the timing must wait for the consumers:
the stream depth (the consumption group's backlog —
min(lag, XLEN) + pending, the same source as the T-026/T-168 gate) at
the threshold means "the analyzer cannot keep up", the queue entries
are not yet processed. The pass (and every drain batch) is skipped
while the depth is ≥ the `content.retention_analyze_watermark`
threshold (0 — the shared `scheduler.queue_high_watermark`); a depth
read error is also a skip (conservatively): trimming blindly is more
expensive than postponing the pass. The skip is visible in the
`crawler_content_retention_skips_total` metric and in a Warn of the
http-worker log.

**Exact protection by unconsumed messages** (the T-166/T-167 fix,
`internal/content/retentionpending.go`): the `render_pending` flag
protects only records whose q:render publication never happened — the
queue was full, the flag was set instead of the publication (T-066).
Records whose messages are ALREADY sitting in q:render/q:analyze have
no flag, and with a hung consumer (23.10: the q:render lag ≈99K, the
last-delivered-id frozen) the depth-based interlock does not cover
them — the depth is below the threshold while the messages age
together with the records. Before the pass, retention reads the keys
of the unconsumed messages of both streams — XRANGE from the
consumption group's last-delivered-id (exclusive) to `+`, an adapter
over the gate's Redis client in the http-worker (the narrow
`content.RetentionPending` interface, like `RetentionDepth` in the
interlock) — and excludes those records from the batches: the "do not
delete before rendered" requirement is enforced by both parts — the
flag for the unpublished and the exclusion for the published. A
message key — a Result (T-024: zone_id, domain_id, subdomain_id,
variant) — coincides with the candidates' PK, so the SQL selection
does not change. The races are safe in both directions: new
publications point at fresh records (non-candidates), and consumption
after the snapshot merely postpones the cleanup to the next pass; the
snapshot is taken once per pass and refreshed every 25 batch
selections of a long drain (the T-066 republication race). A stream
read error — a conservative skip of the pass: the queue records are
untouched; the metrics `crawler_content_retention_pending_errors_total`
and `crawler_content_retention_deferred_total{stream}` (the excluded
records per stream — who is holding them), a Warn in the http-worker
log.

**The loop** (`internal/content/retention.go` + `retentiondrain.go`,
in the style of CompactLoop/CleanupDeleted): the hourly
`content.retention_interval` ticker, the first pass immediately — a
drain of what accumulated during the downtime (with a persistent
cursor — a continuation from the saved position, otherwise a catch-up
circle from zero). The http-worker starts the loop (an `OnRun`
goroutine, next to the T-045 compaction): the service already holds
the DB pool, the stream-depth gate, and the metric registry, while the
purge merely "feeds" the compaction dead blocks; a second owner does
not need the loop (the guards make a parallel run safe but pointless —
like the compaction). `content.retention = 0` — retention is off, the
loop does not start.

**The time-based drain — "once in a lifetime"** (T-185; the diagnosis
of slow-log query #13 of 24.10 — the heaviest by total time: 50
threshold hits of 3 s in 2.4 h, a median of 76 s, ≈63 min of database
time — the hourly PK circles re-read the heap of the whole traversed
history, and the trail grew with the table ≈3M rows/day). The
selection — cursor pagination by `(checked_at, PK) > cursor` in
`content.retention_batch` batches (the working range of 10–50K rows)
until the aging window or the interlock is exhausted: both
`checked_at` bounds are served by `subdomain_results_checked_idx`
(T-171; the planner of PG16 derives the lower bound from the row-value
comparison by itself — verified with EXPLAIN), and the determinism of
batches within equal timestamps (`checked_at` is written with the
stable `now()` of the transaction, T-058) is kept by an Incremental
Sort over the index order (a narrow window of steady drainage) or a
Sort of the slice (a wide circle). The loop cursor survives passes: a
cleaned row stays behind the cursor, a rechecked one (DELETE+INSERT
with a fresh `checked_at`, T-058) enters the drain again when it ages
out; an "incomplete batch" — the drain runs to the end of the current
aging window, and the hourly pass continues from where it stopped —
the cost of a pass ∝ the inflow (the slice aged out in an hour ≈125K
rows), not the table size. Repeated passes are idempotent; the
composite index `(checked_at, zone_id, …)` is not needed by the
hourly drain (an Incremental Sort per the actual EXPLAIN) — a
candidate for a wide circle per a post-deploy measurement (runbooks
§5.2: when the starting circle systematically exceeds the 300 s
bucket).

**The persistent cursor — removing the restart penalty (T-274).** The
cursor survives http-worker RESTARTS too (a deploy, an external
power-cycle of the VM — the 11.11 11:03 precedent, a potential OOM
kill): the position is saved into the `retention_drain_cursor` table
(migration 000020, a single row `id = 1`) **by the cleanup transaction
of each batch** — the upsert is executed in the `retentionPurge` tx
before Commit, so the saved position cannot run ahead of the committed
batch; a failed batch leaves the previous cursor (the retry is
idempotent, spec 3.4). The frequency — hundreds of upserts per pass
(~155 batches over the 16 h drain of 10–11.11), a point PK update of
one row; the table is not on the hot front's path. The loop start
reads the cursor and continues from it. A missing row (the first
start, a circle), a zero timestamp, a timestamp in the future (beyond
a minute of clock skew), or a read failure — start from the zero
cursor, the pre-T-274 behavior (a full catch-up, a Warn on invalid
values); no table (a database before migration 000020) — Info and
operation without saving (in the style of the T-175 render-debt
counter). The one-off `RetentionOnce` (the runbooks T-167 path A,
isolated builds) neither reads nor writes the state — the owner is the
http-worker loop.

A batch holding queue-protected rows does **not** move the database
position (the T-274 review): the retry list is not persistent, and its
loss at a restart is compensated precisely by the cursor lagging
behind — the start re-reads the protective window, the deferred keys
are deferred again, and clean rows drop out of the selection by
themselves; the in-memory cursor moves as before (the probe returns
keys within the process lifetime). Forced catch-up circles
(render-debt clearing, retry-list overflow, the fallback day) delete
the row with a `DELETE` **at plan time** — a restart in the window
between the plan and its application (≤ the pass interval) lands in a
full catch-up from zero, not a continuation from the old position: the
records behind it are cleaned precisely by the circle; position saving
is suppressed until the circle is applied. A failure to reflect the
plan — a Warn without a pass error (a restart in the window degrades
to a continuation from the old position — more expensive, but without
data loss). Before T-274 every restart meant a new catch-up from
zero: ~60 min of traversing the clean prefix at idle, >60 min under
full front load (the measurements of 10–11.11.2026 — the T-273
incident: three passes under front load did not fit into the 3600 s
cap, and the drain continued only in the quiet window).

**Plan pinning, timeouts, and backoff** (T-271; the production
incident of 08–09.11.2026 — five PostgreSQL OOM kills overnight with
zero retention progress; revised by T-273 — the 09–10.11 regression).
The degradation of the `retentionSelectSQL` plan to "a seq scan of 9M
rows + a hash join of `domains` + a Sort" held ≈1 GiB of working
structures per selection on top of the 2 GiB `shared_buffers`; the
cursor moves only with a committed batch — there was not a single
successful batch, every pass ran as a catch-up circle from the zero
cursor, and a ticker tick that fired during a long pass (the runtime
holds one undelivered tick and hands it over immediately when the loop
returns to select) immediately restarted the pass into the PG that had
just been raised after a kill.
Since T-271 the selection runs in a plan-pinning tx wrapper modeled on
T-191: `SET LOCAL enable_seqscan/hashjoin/mergejoin/bitmapscan = off`
before the SELECT (the shape — an Index Scan over
`subdomain_results_checked_idx` + a Nested Loop of probes + Limit;
`enable_sort` is not disabled — an Incremental Sort over the index
order; the bitmap path is off for the same reason — a bitmap does not
preserve the index order, the ORDER BY would need an explicit Sort),
plus `SET LOCAL statement_timeout = '7200000ms'` and a client ctx
deadline `SelectTimeout` by the same wrapper (T-274: the value is
provided by the `content.retention_select_timeout` config, the default
2 h; it was 3600 s — T-273, 600 s — T-271).
The 600 s covered regular batches (the T-201 measurement: a median of
13.8 s, a maximum of 222.8 s ≈ a 2.7× margin) but not the first
catch-up pass from the zero cursor after a retention outage: the
"there is something to clean" predicate is outside the index, every
row of the clean skip prefix requires a heap visit, and at production
volume (the backlog of 08–09.11 ≈ 4–5M rows) a pass costs 30–60 min —
the deadline killed it BEFORE the commit, the cursor stayed at zero,
and the next pass repeated the identical doomed scan (an endless loop:
a slow drain turned into a permanently broken one). The 3600 s equaled
the upper bound of the "30–60 min" estimate — no margin, and the
measurements of 10–11.11 confirmed it: at idle a pass took ~60 min
exactly, under full front load — over an hour (three passes hit the
cap, `retention_errors_total` = 3, and the drain continued only in the
quiet window). The 7200 s (T-274) — a doubled margin over the upper
bound of the measurements under load; changing the cap without a
deploy is a knob. The persistent cursor (T-274) reduces the number of
catch-up passes to the first one after an outage but does not
eliminate them. After the first successful commit the cursor jumps
over the clean prefix and the regular passes return to seconds; the
retry is idempotent — the batch was not committed. An errored pass
puts the loop ticker into a 15-minute backoff
(`DefaultRetentionErrorBackoff`) **from the end of the pass**
(T-273: a reset after every error — earlier the anchor was the end of
the first errored pass, and repeated errors came with a pause of
"backoff − pass duration"; the Warn cadence masked the duration);
15 minutes is shorter than the hourly interval deliberately — a pass
error signals PG ill health, and the retry arrives earlier than the
next regular tick but an order of magnitude later than the PG raise.
The first successful pass restores the regular interval. Pass errors
are counted by the `crawler_content_retention_errors_total` metric
(T-273) — before it, errors were visible only in journalctl (the
:9104 metrics stayed at zero amid 70+ WARNs over the incident's day).
The heavy selection — under the shared scan gate of the http-worker
`db.ScanGate`: retention and the late render republisher do not
collide in PostgreSQL (the insurance against "1+1 GiB").

The catch-up contract (T-273, pinned): a pass from the zero cursor (a
process start without saved state, render-debt clearing, retry-list
overflow, the fallback day) must complete within `SelectTimeout` at
any depth of the clean skip prefix and must drain the set to the end —
cleaned rows drop out of the selection, and a repeated pass is empty.
The contract is pinned by an integration test on a
production-proportion fixture (partitions, a skip prefix of 200K clean
rows ahead of the candidates — `test/integration/content_test.go`,
TestContent_RetentionCatchUpFromZeroCursor). Restarts no longer spawn
such passes (T-274 — the persistent cursor, the integration
TestContent_RetentionCursorRestart); a structural cure for the
from-zero traversal cost (a partial index, variant C of the T-273
study) — a separate decision after a write-amplification measurement:
if the persistent cursor + 7200 s prove sufficient, it is not needed
at all.

The full circles are abolished — the explicit bounded mechanisms
(`internal/content/retentiondrain.go`) replaced their role in retries
(picking up protected rows on the next circle):

- **queue-protection retries** — a bounded list of keys (100K — the
  order of the stream depths at the watermarks), deferred by the
  q:render/q:analyze snapshot; the next pass offers them with a point
  probe by PK (`retentionProbeSQL`; the plan's point-read shape is
  pinned by `JOIN LATERAL LIMIT 1` — with long stagnation lists the
  planner could re-choose the shape into a hash join + a seq scan over
  the whole aged-out history, the T-185 review): still-protected keys
  stay in the list, keys that stopped being candidates (the protection
  is gone and the cleanup went through, the row was rewritten, a find)
  drop out; an overflow — a Warn and a conservative catch-up circle;
  the list loss (a restart) is covered by the persistent cursor
  lagging behind — the database position does not move for the
  deferred rows, the start re-reads the protective window and defers
  the keys again (T-274);
- **the render-debt catch-up** — records under the `render_pending`
  flag are silently filtered out by the selection, and the time cursor
  passes them by: a point read of `crawler_counters['render_pending']`
  (T-175) at the start of every pass catches the counter's transition
  to 0 — the moment all render protection is lifted at once — and
  schedules one full circle from the zero cursor (it catches up with
  everything that accumulated during the debt epoch; the frequency —
  by the actual debt drains, not by the clock). A counter unavailable
  — no row, no table (42P01, the normal state before migration
  000015) or another read failure — fallback: a circle once a day from
  the anchor, which is moved by successful reads and by the circles
  themselves (fallback means "unavailable for a day IN A ROW", a
  transient blip of a circle does not raise it). The hourly
  discretization can miss a debt epoch that fit entirely between reads
  (accumulated and drained to zero): the first observed zero picks it
  up, as do a retry-list overflow or a restart — with a persistent
  debt without zeros the class is held until one of those happens (a
  deliberate residual; the "circle on debt decrease" trigger was
  rejected: a long drain would decrease every hour — the circles would
  come back).

The residual class of loss behind the cursor (recorded by the T-185
review, P1) — the removal of a Laravel find **by re-analysis** — a
delivery without a fresh mark (repeated q:analyze deliveries,
`crawlerctl reanalyze` campaigns, rules-reload): the
`aggregateDeleteSQL` of the `laravel_domains` row is not accompanied
by a rewrite of `subdomain_results`, and the domain's old records
return to the candidate set already behind the cursor. Fresh find
losses (an HTTP episode) lose no rows: the DELETE+INSERT rewrite
writes a fresh mark ahead of the cursor; deleted domains are
physically cleaned by the T-020 pass within minutes.

**The point drain on find removal (T-187).** The event is known at the
transition point — `aggregateDomain` (analyzer) returns the signal
`RowsAffected > 0 ∧ ¬fresh ∧ existed`, and the worker calls
`content.DrainDomain` right after committing the delivery
transaction: the domain's cleanup candidates by the PK prefix
`(zone_id, domain_id)` (the same selection filters as the hourly
selection, without a cursor — the partition leaf cuts the selection
down to the domain's rows) are cleaned by the regular T-166 purge
mechanics in a separate short transaction. The choice of a direct call
instead of a signal through the DB/queue: the counter knows "how many"
but not "which domains" (the keys cannot be passed without
migrations), a circle by the counter would bring back the hourly full
passes rejected by T-185, and a Redis signal stream — a new queue for
the sake of a P3 tail. The differences from the loop pass
(deliberate): the q:analyze depth interlock and the unconsumed-keys
snapshot are not consulted — the q:analyze consumer itself performs
the drain upon the transition that already happened (the domain's
confident hits are already processed); records under the render flag,
deleted domains, and a renewed find are excluded by the same selection
(the render debt will be caught by the T-185 catch-up circle, deleted
ones are cleaned by T-020). A drain failure — a Warn and a metric, the
delivery is not failed (the findings and the aggregate are committed);
the residual window — an unconsumed message of a weak row of the same
domain with a stalled stream and a mixed weight change of one ruleset
(the last confident row removed, a weak one raised): its late
processing over the empty input will yield "no hits", and the find
will return at the T-030 recheck. Observability:
`crawler_analyzer_laravel_unfound_total`,
`crawler_analyzer_unfound_drained_rows_total`,
`crawler_analyzer_unfound_drain_errors_total` (/metrics analyzer). An
extremely rare crash between the delivery commit and the drain leaves
the domain's records in the pre-T-187 class (the rare catch-up circles
or a recheck will pick them up). A daily unconditional circle was
rejected: it would return dozens of slow records into the T-177
journal every day, against the task's acceptance criterion.

The q:analyze interlock, the unconsumed-key snapshots (the first —
after the interlock, refreshed every 25 selections of a long drain),
and the purge/decref/delete semantics with guard re-checks — unchanged
(T-166); the `OnPass` pass statistics and metrics are the same.

Recorded deviations (agreed with the customer, T-166):

- the re-analysis of non-finds under new rules is impossible: after
  the cleanup there is no content and no headers/cookies; re-applying
  the rules — at the recheck (≈6 months);
- weak hits do not protect content (a find — a `laravel_domains` row):
  after a rules change the T-074 re-analysis will rewrite them to "no
  hits" over the empty input — until then the hit history is preserved
  in `laravel_findings.signals`;
- the recheck loses the "did the site change" comparison: the
  DELETE+INSERT rewrite (T-058) writes the result from scratch, there
  is no previous content to compare against.

### The one-off cleanup campaign for the accumulated volume (T-167)

Data checked before the window was introduced does not fall under the
same criteria automatically, purely because of its date — the
history's age already exceeds the window, and the regular cycle drains
it on the very first pass. The one-off campaign over the historical
mass (20 GB of content / ≈3.7 GB of the journal at the time of
filing) — [`runbooks.md`](runbooks.md) §5: path A — the regular T-166
cycle with progress monitoring and slowdown under CPU starvation;
path B — the operational SQL `deploy/sql/t167-retention-backfill.sh`
(the same semantics and guards, reduced batches with pauses and
PSI/queue gates — XLEN and the consumption groups' lags); the final
steps — VACUUM (ANALYZE) of the affected partitions and pack-compaction
monitoring under a mass of orphan blocks; the acceptance criteria —
content ≤3–5 GB, `subdomain_results` ≤1.5 GB, the finds and `q:dead`
untouched. The `render_pending` cohort is excluded by the campaign
until the `q:render` drain (T-159) — the T-166 selection makes this
exclusion itself; records with unconsumed `q:render`/`q:analyze`
messages are excluded by the exact protection (above) — with a stalled
`q:render`, path B honestly stops at the lag gate until the stream
drains.

## Limits and disk overflow

Two configurable limits (spec 5.5, 5.7, 8):

- **`content.max_store_bytes`** — the limit of one content item ("the
  max size of stored HTML"). An excess — `ErrTooLarge`, the content is
  rejected entirely: the worker saves the result without the content
  (T-056). The bytes are compared before compression — the limit is
  about the stored page, not the block.
- **`content.disk_free_percent`** — the reaction threshold for disk
  overflow (spec 8: "the services pause content writing and signal it
  with an alert; the threshold is configurable"). The threshold — the
  share of free space of the `content.dir` directory's file system, %:
  a typical lower bound for data volumes, 5 by default, the range
  (0, 95] is validated by the config.

The disk threshold mechanics (`internal/content/disk.go`, T-046):

1. **Before writing** a new block (a Put after a dedup miss) a
   `statfs` of the content directory is taken: with the share of
   available space (`statfs.Bavail` — what an unprivileged service
   sees, spec 10.1) below the threshold, the Put answers
   `ErrStoreFull` ("the content-write pause"): the block and the
   metadata row do not appear, the data is not corrupted. On this
   branch the worker does not save the content but does record the
   result — that is the spec 8 contract; the retry — later
   (at-least-once, spec 3.4).
   The pause does not stop the rest: **dedup references** to live
   content keep being added (no bytes are spent), and **reading** (the
   analyzer, re-analysis, `crawlerctl domain --content`) works always.
2. **Periodically** `DiskWatchLoop` checks the same condition on a
   ticker (`DefaultDiskWatchInterval`, 30 s; the first pass
   immediately): a Warn in the log on entering the pause and an Info
   on recovery — the alert is visible to the operator even when there
   are no records. The signal is edge-triggered (on transitions), not
   on every tick.
3. **Recovery is automatic**: the space was freed (compaction deleted
   packs, a manual cleanup, the volume was expanded) — the next check
   goes through, and writing continues without restarting the service.
4. **The alert signal** — a typed `StoreFullEvent` event into the
   `PackOptions.OnStoreFull` hook (the counter for T-080: the alert
   metrics/rules live there, the alert itself is not implemented
   here). The event sources: `put` (every stopped Put), `enospc` (the
   physical write hit the end of the disk — the last line of defense
   after the threshold, T-042), `watch` (the periodic check's
   transitions, including recovery — `Recovered`). Threshold stubs for
   tests — the `SpaceChecker` interface (`OSStatfs` — the real file
   system).

An `ENOSPC` during the physical pack write remains the safety net for
the case when the space ran out between checks (or the threshold is
disabled by a zero in `PackOptions` — the config does not allow that):
the error is mapped into `ErrStoreFull` (T-042) and produces an
`enospc` event.

## Errors

- `ErrTooLarge` — the content is bigger than `content.max_store_bytes`
  and is rejected entirely (spec 5.5);
- `ErrStoreFull` — no space: the free share is below
  `content.disk_free_percent` (the proactive write pause, see "Limits
  and disk overflow") or the physical write hit `ENOSPC`;
- `ErrNotFound` — no live row (it was never written or all references
  were removed).

## Configuration

| Field | Env | Default | Meaning |
|---|---|---|---|
| `content.dir` | `CRAWLER_CONTENT_DIR` | `/data/content` | the packs directory |
| `content.pack_size` | `CRAWLER_CONTENT_PACK_SIZE` | `512MB` | the pack rotation threshold |
| `content.max_store_bytes` | `CRAWLER_CONTENT_MAX_STORE_BYTES` | `2097152` | the limit of one content item |
| `content.zstd_dictionary` | `CRAWLER_CONTENT_ZSTD_DICTIONARY` | `/data/content/dict.zst` | the zstd dictionary (empty — no dictionary) |
| `content.disk_free_percent` | `CRAWLER_CONTENT_DISK_FREE_PERCENT` | `5` | the free-space threshold, % (spec 8) |
| `content.retention` | `CRAWLER_CONTENT_RETENTION` | `6h` | the lifetime window of non-find content, 0 — off (T-166; the "Content retention and the non-find trim" section) |
| `content.retention_interval` | `CRAWLER_CONTENT_RETENTION_INTERVAL` | `1h` | the period of the retention cycle's passes |
| `content.retention_batch` | `CRAWLER_CONTENT_RETENTION_BATCH` | `10000` | rows in a batch transaction (the 10–50K range) |
| `content.retention_analyze_watermark` | `CRAWLER_CONTENT_RETENTION_ANALYZE_WATERMARK` | `0` | the q:analyze interlock threshold (0 — the shared `scheduler.queue_high_watermark`) |
| `content.retention_select_timeout` | `CRAWLER_CONTENT_RETENTION_SELECT_TIMEOUT` | `2h` | the cap of one batch selection: statement_timeout + the ctx deadline (T-274; 0 — the code default, the range 1m..24h) |

The complete reference — `docs/configuration.md`.
