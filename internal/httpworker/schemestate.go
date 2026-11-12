// The scheme check state FSM (T-266, spec tasks/TZ-fsm.md §3 FR-12):
// the single source of truth about the LEGAL combinations of the fields of one
// check's outcome (SchemeCheck, worker.go) and of the content part of a subdomain-result
// record (Record, worker.go). The state is encoded not by a separate column, but
// by the combination of the struct field values — the center of the formalization is
// the matrix of legal combinations and the invariants, not a transition table
// (TZ-fsm spec §1: the specification lives in the package doc comment next to the code).
// The state writers — checkScheme and workerSkipSubdomain (worker.go),
// the variant logic ProcessDomain + DetectAliases (httpfetch, T-054/T-055) and
// the ContentSaver.Save normalization (store.go, T-056); everything they write
// must fit the matrices below.
//
// The validator methods (FR-12) check the matrices' invariants at the stage
// boundaries:
//
//   - validateResult (SchemeCheck.validate + Record.validate over all
//     subdomains) — the end of ProcessDomain, before passing the result to
//     Options.OnResult (the first link of the chain — ContentSaver, T-056):
//     an illegal combination — a result construction bug, the q:http task
//     is returned without an XACK (spec 3.4), garbage does not reach the DB;
//   - Record.validate — newRecordBatch (record.go, T-058) before
//     the write transaction: the records after the ContentSaver normalization;
//     the ContentNotStored ⇒ hash = nil and AliasNone ⇔ alias_id invariants,
//     previously checked piecemeal (recordValidateRow), are centralized
//     here (FR-12).
//
// # The scheme check state (SchemeCheck, 7 fields)
//
// The state vector: Available × Cached × ErrClass × DeadDial ×
// ContentHash≠nil × ContentStored × Truncated. The matrix below — a LIST
// of the writers' states (all the combinations the code writes); the executable
// part of the specification — the I1–I8 invariants (SchemeCheck.validate):
// the combinations outside the matrix rows that do not violate the invariants (for example,
// partial writes of direct calls) are admitted by the validator.
//
//	| # | Available | Cache | ErrClass | DeadDial | Hash | Stored | Truncated | Written by |
//	|---|----------|-----|----------|----------|-----|-----------|----------|-------|
//	| S1 | yes | — | ErrNone | — | ≠nil | yes | — | checkScheme: the answer is received, the early Put passed (T-147) |
//	| S2 | yes | — | ErrNone | — | ≠nil | — | — | checkScheme: the answer is received, the Put is in the content write pause (ErrStoreFull/ErrTooLarge, spec 8); the storeErr branches' outcome of the same shape (a hash desync, a Put failure) — the episode fails in ProcessDomain before the validation |
//	| S3 | yes | — | ErrBody | — | ≠nil | — | yes | checkScheme: the body is truncated by the max_body_bytes cap (T-049), the hash — over the truncated bytes |
//	| S4 | yes | yes | ErrNone | — | ≠nil | yes | — | checkScheme: the cache hit of page S1 (spec 5.5 item 6), FinalURL — the starting URL (see below) |
//	| S5 | yes | yes | ErrNone | — | ≠nil | — | — | checkScheme: the cache hit of page S2 — the write pause is inherited |
//	| S6 | yes | yes | ErrBody | — | ≠nil | — | yes | checkScheme: the cache hit of page S3 — the class and the truncation are carried by the cache outcome |
//	| S7 | — | — | ErrNetwork | yes | nil | — | — | checkScheme: connection refused of the starting host, the chain is empty (T-164) |
//	| S8 | — | — | ErrTimeout | yes | nil | — | — | checkScheme: the connect timeout of the starting host, the chain is empty (T-164) |
//	| S9 | — | — | ErrTimeout | — | nil | — | — | checkScheme: the headers/overall/anti-tarpit/body-read timeout; a partial outcome (Status/Header/TLS/FinalURL/Redirects) may be filled (§8) |
//	| S10 | — | — | ErrTLS | — | nil | — | — | checkScheme: a TLS handshake failure (InsecureSkipVerify does not save from a refusal to establish the connection) |
//	| S11 | — | — | ErrNetwork | — | nil | — | — | checkScheme: the other transport ones — a disconnect, an error/resolution of the redirect host (the chain may be non-empty), a dial's unreachable/SSRF filter (the chain is empty); it differs from S7/S8 by DeadDial=false — not a connect-phase refusal |
//	| S12 | — | — | ErrRedirects | — | nil | — | — | checkScheme: the max_redirects limit or a loop (T-048), Redirects — the performed part |
//	| S13 | — | — | ErrBody | — | nil | — | — | checkScheme: a body read error after the headers (a broken Content-Encoding, a zip bomb — ErrDecompressTooLarge): the protocol answered, there is no page |
//	| S14 | — | — | ErrCutoff | — | nil | — | — | workerSkipSubdomain: the synthetics of a subdomain skipped by the dead-host cutoff (T-164); creates no records (§8) |
//
// The invariants (the numbering is logical; the check order in validate()
// is chosen so that every branch is reachable: codes → cache →
// unavailability → availability → storage → cutoff → dead dial):
//
//	I1  Scheme and ErrClass — the model enums' codes (§6.1, ErrorClass).
//	I2  DeadDial ⇒ unavailable, without cache and hash, with an EMPTY chain
//	    of redirects and a connect-phase class: ErrNetwork (connection
//	    refused) or ErrTimeout (a connect timeout) — the signature
//	    of the dead host T-164 (httpfetch.IsConnectFailure).
//	I3  Cached ⇒ available with a hash: the cache stores only read
//	    pages (workerURLCache.put — the end of a successful checkScheme).
//	I4  Available ⇒ the body is read: ContentHash ≠ nil, FinalURL ≠ nil,
//	    SPA ≠ nil; ErrBody ⟺ Truncated — truncation is the only ErrBody
//	    of an available check (a body read error extinguishes Available, S13).
//	I5  ¬Available ⇒ no content part: the hash nil, ¬Truncated,
//	    ¬ContentStored; partial Status/Header/TLS/FinalURL — allowed
//	    (the data for parsing, §8); Cached is excluded by I3.
//	I6  ContentStored ⇒ without truncation (the availability and the hash presence at
//	    ContentStored are required by I5/I4; a truncated body does not get the early
//	    Put, T-049).
//	I7  ErrCutoff ⇒ ¬Available: a cutoff is "the check was not performed".
//	I8  ErrClass = ErrNone ⟺ ErrText = "" — errors are always with text
//	    for parsing, a success — without.
//
// # The record's content part (Record)
//
// A record is created ONLY by an available check (DecideVariants, T-054:
// both protocols are silent — no records), therefore the record's content part —
// a projection of the S1–S3 states (the cache hits S4–S6 give the same values), and
// the alias marking — DetectAliases (T-055). The stage boundaries carry two
// record shapes (before/after the ContentSaver normalization, store.go); the matrix —
// a list of the writers' shapes, the executable part — the P1–P7 invariants
// (Record.validate, valid at BOTH boundaries — the intersection of the shapes,
// "—" = false):
//
//	| # | Boundary | AliasKind | StoreContent | Hash | ContentStored | ContentNotStored | Truncated | Written by |
//	|---|---------|-----------|--------------|-----|---------------|------------------|-----------|-------|
//	| R1 | both | None | yes | ≠nil | yes | — | — | ProcessDomain: the original, the early Put passed; ContentSaver does not change it |
//	| R2 | before Save | None | yes | ≠nil | — | — | — | ProcessDomain: the original, a content write pause → Save → R6 |
//	| R3 | before Save | None | yes | ≠nil | — | — | yes | ProcessDomain: the original, a truncation → Save → R7 |
//	| R4 | before Save | ≠None | — | ≠nil | yes/— | — | — | DetectAliases: an alias — the hash is its own copy of the source check, Save will remove it → R8 |
//	| R5 | before Save | ≠None | — | ≠nil | — | — | yes | DetectAliases: an alias of a truncated page → Save → R8 |
//	| R6 | after Save | None | yes | nil | — | yes | — | ContentSaver: the content is not stored (a write pause/the max_store_bytes limit), the hash is removed |
//	| R7 | after Save | None | yes | nil | — | yes | yes | ContentSaver: a truncated body is not stored, the hash is removed |
//	| R8 | after Save | ≠None | — | nil | yes/— | — | yes/— | ContentSaver: an alias — the content is at the original; Truncated/ContentStored — an untouched copy of src (Save does not change them) |
//
// The invariants:
//
//	P1  Variant/Scheme/AliasKind — the model enums' codes (§6.1).
//	P2  (AliasKind = AliasNone) ⟺ (AliasSubdomainID = nil) — the contract
//	    of T-055 (FR-12: formerly the recordValidateRow check).
//	P3  StoreContent ⇒ AliasKind = AliasNone: the class's content is stored
//	    once, at the original (T-055). The converse is not required:
//	    DetectAliases gives StoreContent=false to a lone record without a hash (in
//	    the real worker flow the hash is always there — I4; the assumption — for
//	    the partial writes of newRecordBatch's direct calls).
//	P4  ContentNotStored ⇒ the hash nil ∧ StoreContent ∧ ¬ContentStored —
//	    a sign of only an original whose content is not stored; it is not set
//	    for aliases (their content — in the original record, T-055).
//	P5  The hash is either nil or a full content.HashLen; an original
//	    (StoreContent) must carry a hash or the ContentNotStored sign.
//	P6  Truncated ⇒ ¬ContentStored (a truncated body is not stored, T-049).
//	P7  A record's ErrClass ∈ {ErrNone, ErrBody}, ErrBody ⟺ Truncated —
//	    a projection of I4 of the available source check.
//
// # The deliberate distortion of final_url on a cache hit
//
// A cache hit (S4–S6) fills FinalURL with the check's STARTING URL, not
// the primary source's final URL (worker.go checkScheme, the assignment
// check.FinalURL = start). The cache key — scheme+host+port+path WITHOUT the query and
// fragment (workerCacheKey: the counter parameters ?i=1 must not
// break the deduplication of one page, spec 5.5 item 6), therefore the
// primary source's final URL with parameters is written by its query-less
// form, and Redirects — a synthetic single step with the starting URL.
// Deliberate behavior, NOT changed (T-266, TZ-fsm spec §3 FR-12): the host and
// path — the criteria of the variants (T-054) and aliases (T-055) — match
// the primary source exactly (they enter the key), the divergence — only the query
// of the starting form; the cache does not store the chain of another's check.

package httpworker

import (
	"fmt"

	"crawler/internal/content"
	"crawler/internal/model"
)

// validate — the centralized validator of the scheme check state (FR-12,
// T-266): the combination of the 7 state fields must satisfy the invariants
// I1–I8 (the writers' state matrix S1–S14 — the file's doc comment).
// Called from validateResult (the "before ContentSaver" boundary).
func (c *SchemeCheck) validate() error {
	//	// I1: the enum codes — the model's scheme and the stage error class.
	if !c.Scheme.Valid() {
		return fmt.Errorf("check: scheme %d", int16(c.Scheme))
	}
	if c.ErrClass < ErrNone || c.ErrClass > ErrCutoff {
		return fmt.Errorf("check %s: error class %d is outside the ErrorClass enum", c.Scheme, int16(c.ErrClass))
	}
	//	// I8: the class and the error text agree — an error is always with text
	//	// (the parsing §8), a success without.
	if (c.ErrClass == ErrNone) != (c.ErrText == "") {
		return fmt.Errorf("check %s: class %s with an error text %q", c.Scheme, c.ErrClass, c.ErrText)
	}
	//	// I3 (checked before I5/I4 — the only owner of the Cached branch):
	//	// a cache hit — a live check with a hash, the cache stores only
	//	// read pages.
	if c.Cached && (!c.Available || c.ContentHash == nil) {
		return fmt.Errorf("check %s: a cache hit without a page body (available=%v hash=%v)",
			c.Scheme, c.Available, c.ContentHash != nil)
	}
	//	// I5: the content part — only of an available check (the load
	//	// and body read errors do not fill it; Cached is excluded by I3).
	if !c.Available && (c.ContentHash != nil || c.Truncated || c.ContentStored) {
		return fmt.Errorf("check %s: unavailable but carries the content part (hash=%v truncated=%v stored=%v)",
			c.Scheme, c.ContentHash != nil, c.Truncated, c.ContentStored)
	}
	//	// I4: an available check read the body — the hash, the final URL and
	//	// the SPA verdict are there; ErrBody exactly at a truncation.
	if c.Available {
		if c.ContentHash == nil || c.FinalURL == nil || c.SPA == nil {
			return fmt.Errorf("check %s: available without the content part (hash=%v final_url=%v spa=%v)",
				c.Scheme, c.ContentHash != nil, c.FinalURL != nil, c.SPA != nil)
		}
		if (c.ErrClass == ErrBody) != c.Truncated {
			return fmt.Errorf("check %s: class %s at Truncated=%v (the ErrBody of an available check is only a truncation)",
				c.Scheme, c.ErrClass, c.Truncated)
		}
	}
	//	// I6: a stored body — without truncation (the unavailability and the
	//	// hash absence at ContentStored are cut off by I5/I4; a truncated body does
	//	// not get the early Put, T-049).
	if c.ContentStored && c.Truncated {
		return fmt.Errorf("check %s: ContentStored with a truncated body (T-049)", c.Scheme)
	}
	//	// I7: a cutoff — the synthetics of a skipped subdomain (workerSkipSubdomain),
	//	// never an available check.
	if c.ErrClass == ErrCutoff && c.Available {
		return fmt.Errorf("check %s: ErrCutoff on an available check", c.Scheme)
	}
	//	// I2: a dead dial — an unavailable check with an empty chain and a
	//	// connect-phase refusal class (T-164).
	if c.DeadDial && (c.Available || c.Cached || c.ContentHash != nil || len(c.Redirects) > 0 ||
		(c.ErrClass != ErrNetwork && c.ErrClass != ErrTimeout)) {
		return fmt.Errorf("check %s: DeadDial without the dead-connect signature (class %s, %d chain hops)",
			c.Scheme, c.ErrClass, len(c.Redirects))
	}
	return nil
}

// validate — the centralized validator of the record's content part (FR-12,
// T-266): the P1–P7 invariants are valid at both boundaries (the writers'
// shape matrix R1–R8 — the file's doc comment). Called from validateResult
// (the records before ContentSaver) and newRecordBatch before the write transaction
// (after the normalization).
func (r *Record) validate() error {
	//	// P1: the model codes (§6.1).
	if !r.Variant.Valid() {
		return fmt.Errorf("subdomain %d: variant %d", r.SubdomainID, int16(r.Variant))
	}
	if !r.Scheme.Valid() {
		return fmt.Errorf("subdomain %d: scheme %d", r.SubdomainID, int16(r.Scheme))
	}
	if !r.AliasKind.Valid() {
		return fmt.Errorf("subdomain %d: alias kind %d", r.SubdomainID, int16(r.AliasKind))
	}
	//	// P2: the alias link is self-consistent (the T-055 contract).
	if (r.AliasKind == model.AliasNone) != (r.AliasSubdomainID == nil) {
		return fmt.Errorf("subdomain %d: alias_kind %d ⟺ alias_subdomain_id (the T-055 contract)",
			r.SubdomainID, int16(r.AliasKind))
	}
	//	// P3: the class's content is stored once at the original — StoreContent
	//	// only at an independent record.
	if r.StoreContent && r.AliasKind != model.AliasNone {
		return fmt.Errorf("subdomain %d: StoreContent on an alias (the class content belongs to the original, T-055)", r.SubdomainID)
	}
	//	// P4: the "not stored" sign — only of an original, only without a hash and
	//	// without a stored body (it is not set for aliases).
	if r.ContentNotStored && (r.ContentHash != nil || !r.StoreContent || r.ContentStored) {
		return fmt.Errorf("subdomain %d: ContentNotStored with a live content_hash/alias/stored body (the T-056 contract)",
			r.SubdomainID)
	}
	//	// P5: the hash — a full content.Hash; the original carries a hash or the sign of
	//	// its absence (typed as content.Hash — the length check is a guard
	//	// for direct calls).
	if r.ContentHash != nil && len(*r.ContentHash) != content.HashLen {
		return fmt.Errorf("subdomain %d: content_hash is %d bytes (want %d)",
			r.SubdomainID, len(*r.ContentHash), content.HashLen)
	}
	if r.StoreContent && r.ContentHash == nil && !r.ContentNotStored {
		return fmt.Errorf("subdomain %d: an original without content must carry a hash or ContentNotStored (the T-056 contract)",
			r.SubdomainID)
	}
	//	// P6: a truncated body is not stored (T-049).
	if r.Truncated && r.ContentStored {
		return fmt.Errorf("subdomain %d: Truncated with ContentStored (a truncated body is not stored, T-049)", r.SubdomainID)
	}
	//	// P7: a record is created only by an available check — the class ErrNone or
	//	// ErrBody (a truncation), the other classes live in the checks, not in the records.
	if r.ErrClass != ErrNone && r.ErrClass != ErrBody {
		return fmt.Errorf("subdomain %d: record error class %s (a record is created only by an available check)",
			r.SubdomainID, r.ErrClass)
	}
	if (r.ErrClass == ErrBody) != r.Truncated {
		return fmt.Errorf("subdomain %d: class %s at Truncated=%v (the ErrBody of a record is only a truncation)",
			r.SubdomainID, r.ErrClass, r.Truncated)
	}
	return nil
}

// validateResult — the stage boundary "before ContentSaver" (FR-12, T-266):
// all the checks and records of the domain's result pass the automaton's validators.
// Called from ProcessDomain (worker.go) before passing the result to
// Options.OnResult; a separate function — for the testability of the boundary
// (inside ProcessDomain there is nothing to build an illegal state with: all the fields
// are collected by checkScheme/DetectAliases legally).
func validateResult(res *DomainResult) error {
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for _, c := range []*SchemeCheck{&sub.HTTPS, &sub.HTTP} {
			if err := c.validate(); err != nil {
				return fmt.Errorf("httpworker: subdomain %s: %w", sub.Hostname, err)
			}
		}
		for j := range sub.Records {
			if err := sub.Records[j].validate(); err != nil {
				return fmt.Errorf("httpworker: subdomain %s: %w", sub.Hostname, err)
			}
		}
	}
	return nil
}
