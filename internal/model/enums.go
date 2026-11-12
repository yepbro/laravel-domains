package model

import (
	"fmt"
	"strings"
)

// Status — the domain status, the domains.status column (smallint,
// spec 6.2). Codes 0..6 are fixed by the schema and a test; code 7 is the
// diagnostic invalid (T-109). Spec 6.2 allows additional statuses (for
// example, parked): a new constant extends Statuses and the statusNames
// table with the next code in order.
//
// # The domain status FSM: the shared decision table (T-256, TZ-fsm spec §3 FR-2)
//
// The single source of truth about the status transitions: the table
// "stage × condition → status" over the writers of the regular pipeline
// (the administrative paths — the testbed fixture import T-242,
// cmd/crawlerctl, which carries the dump state as is, and manual SQL — are
// outside the table). The formalization does not rewrite the transitions
// onto the FSM framework — they remain SQL statements with guard
// predicates (TZ-fsm spec §1); the specification is below, the writers
// reference it and do not duplicate it. The stage branches are expanded in
// the executor packages: DNS — internal/dnsworker (result.go, at the
// record operators), HTTP — internal/httpworker (record.go). The adjacent
// FSM is check_state (internal/scheduler/checkstate.go, T-255): the status
// is the fact of the last episode, check_state is the state of the cycle;
// the transitions of that table that carry the status transitions of this
// one as a side effect are the result records and the diagnosis
// (R2/R3/R4/R13 = O2–O6, O10), the retry (R8 = O11), and the
// deletion/revival/insertion (R14/R9/R10 = O12/O13/O1).
//
//	| #   | Stage     | Condition                                        | Status                | Side effects                                      | Writer |
//	|-----|-----------|--------------------------------------------------|-----------------------|---------------------------------------------------|--------|
//	| O1  | ingest    | insertion of a new domain by an add-file         | new (the schema DEFAULT) | —                                               | ingest/apply_add.go insertAddSQL (T-018) |
//	| O2  | DNS       | apex NXDOMAIN (RFC 8020)                         | not_delegated         | dns_mask = 0; temp_attempts = 0                   | dnsworker/result.go resultUpdateStatusSQL |
//	| O3  | DNS       | apex SERVFAIL/timeout after retries              | temp_error            | dns_mask = 0; temp_attempts is kept               | dnsworker/result.go resultUpdateStatusSQL |
//	| O4  | DNS       | no addresses, no resolver failures (incl. wildcard-only and zone defects: CNAME loop/depth) | delegated_no_address | the episode dns_mask; temp_attempts = 0 | dnsworker/result.go resultUpdateStatusSQL |
//	| O5  | DNS       | no addresses, there was a resolver failure (transport / a code outside NOERROR and NXDOMAIN) | temp_error | the episode dns_mask; temp_attempts is kept | dnsworker/result.go resultUpdateStatusSQL |
//	| O6  | DNS       | addresses exist (the mask is non-empty after the wildcard filter) | NOT written: the HTTP stage decides the status (O7–O9); until its outcome the previous one is kept (the first check — new) | the episode dns_mask; temp_attempts = 0; q:http publication | dnsworker/result.go resultUpdateKeepStatusSQL |
//	| O7  | HTTP      | records exist (at least one subdomain×protocol responded) | alive       | temp_attempts = 0; subdomain_results rows         | httpworker/record.go recordDomainSQL |
//	| O8  | HTTP      | no records, among the checks there is the timeout-class ErrTimeout (incl. a connect timeout) | temp_error | temp_attempts is kept | httpworker/record.go recordDomainSQL |
//	| O9  | HTTP      | no records, no timeouts (incl. connect-refused — ErrNetwork) | no_http | temp_attempts = 0                                 | httpworker/record.go recordDomainSQL |
//	| O10 | DNS       | poison row: the check is impossible deterministically (an empty name) | invalid (T-109) | temp_attempts = 0; a re-check never returns such rows | dnsworker/result.go resultInvalidSQL |
//	| O11 | scheduler | temp_error, the temp_error_retry_after window has passed, temp_attempts ≥ max_retries | delegated_no_address (ASYMMETRY A1) | temp_attempts = 0; check_state stays done | scheduler/retry.go retryFinalizeSQL (T-031) |
//	| O12 | ingest    | delete file: the domain is removed from the registered | deleted       | result rows are cleaned by T-020; the render_pending flag is cleared by the same transaction | ingest/apply_delete.go (T-019) |
//	| O13 | ingest    | revival: a deleted domain in a new add-file      | new                   | dns_mask/flags/last_checked_at are kept (the fact of the past check is not zeroed) | ingest/apply_add.go reviveAddSQL (T-018) |
//
// temp_attempts (T-031): resets to 0 on every non-temp_error episode-record
// outcome (O2, O4, O6, O7, O9, O10) and on the retry finalization (O11);
// the temp_error outcomes (O3, O5, O8) keep the accumulated value — the
// scheduler increments it when assigning a retry (R8 of the check_state
// spec); the ingest transitions (O12, O13) do not touch the counter.
//
// ## Deliberate asymmetries (decisions, NOT defects — not to be "fixed")
//
//	A1  O11: temp_error on exhausted attempts is finalized into
//	    delegated_no_address, not into a special status: the last check
//	    ended with SERVFAIL/timeout — there are no addresses for the
//	    HTTP stage; this is the terminal "no addresses" branch of the
//	    spec 7 algorithm. not_delegated was rejected (it encodes exactly
//	    an apex NXDOMAIN, RFC 8020), the set does not grow new statuses
//	    (the seven statuses are confirmed by the customer, spec 13.1).
//	    The error self-corrects by the regular re-check T-030
//	    (see scheduler/retry.go).
//	A2  O8/O9: connect-refused → no_http versus a connect timeout →
//	    temp_error: an RST to a SYN (the port is closed) is a permanent
//	    property of the host, a retry in the near horizon is pointless
//	    (ErrNetwork, not temp); SYN silence (a connect timeout) is
//	    transient (ErrTimeout).
//	    httpworker/worker.go workerClassifyError classifies by
//	    httpfetch.IsTimeout (net.Error.Timeout).
//	A3  The HTTP stage (O7–O9) records the outcome WITHOUT the
//	    check_state <> done guard (unlike the DNS operators): redelivery
//	    must re-publish q:analyze — a done filter would drop the
//	    publication forever (httpworker/record.go, the file comment).
//	    The price is a theoretical race with a fresh re-check; the window
//	    is practically unreachable (delivery attempts are exhausted
//	    within minutes, a recheck takes months; the early recheck R15
//	    (T-270) shortens the "months" to 24 h — a duplicate task of a
//	    revived standing worker is absorbed by the same idempotency:
//	    the episode is rewritten with DELETE+INSERT, q:analyze is
//	    re-published).
//
// ## Observability of the "stuck status" anomaly (FR-2.2)
//
// O6 without O7–O9 (the q:http task lost after XADD) leaves the domain in
// status = new ∧ check_state = done: the state is NOT the norm
// (O6 must end with an HTTP outcome); it is observed by the gauge
// crawler_domains_new_done_stale and the domains_new_done_stale alert
// (a pass of monitor/zonestats.go over the reconciliation counters,
// without a seq scan over domains). The T-270 compensator treats it —
// the early recheck (R15 of the check_state spec,
// internal/scheduler/stalerequeue.go: the hourly pass returns new∧done
// older than 24 h to pending, a cheap probe by the new_done_stale row —
// domains are not scanned in the steady state); the domain does not wait
// for the age-based re-check T-030 (recheck_interval, months).
// The gauge remains a signal to the operator about NEW losses (the class
// is open: a Redis restart without persistence — docs/redis.md §5,
// OOM windows — docs/load-measurements.md §11.1).
type Status int16

// Domain statuses (spec 6.2). The comments are the values from the §6.2
// table.
const (
	// StatusNew — not checked yet.
	StatusNew Status = 0
	// StatusNotDelegated — the apex is NXDOMAIN; subdomains are not
	// checked (RFC 8020, spec 5.4 item 1).
	StatusNotDelegated Status = 1
	// StatusDelegatedNoAddress — delegated, but no subdomain has
	// A/AAAA (spec 5.4 item 5).
	StatusDelegatedNoAddress Status = 2
	// StatusNoHTTP — addresses exist; the web does not respond over
	// http or https.
	StatusNoHTTP Status = 3
	// StatusAlive — at least one subdomain responded over HTTP(S).
	StatusAlive Status = 4
	// StatusTempError — a temporary error (SERVFAIL, timeouts); a retry
	// is required (spec 5.3: temp_error_retry_after).
	StatusTempError Status = 5
	// StatusDeleted — removed from the registered by a delete file
	// (spec 5.1, 13.1).
	StatusDeleted Status = 6
	// StatusInvalid — the diagnostic terminal status of a poison row
	// (T-109, the remainder of DNS-03): the check is impossible — the
	// name is empty or whitespace (a junk/corrupted domains row),
	// repeating the episode would not change the outcome. Not a fact
	// about the domain: it is set by the DNS stage instead of the
	// eternal queued→lease→dispatch loop; a recheck never returns such
	// rows — the only way out is manual triage (editing the name or
	// deleting the row). Code 7 is an extension of the §6.2 table
	// allowed by the spec: the "seven statuses" precedent (spec 13.1,
	// retry.go T-031) is deliberately overridden: temp_error would
	// produce false retries finalized into delegated_no_address — a
	// deception about the domain, not diagnostics.
	StatusInvalid Status = 7
)

// Statuses — all statuses in ascending code order (iteration, CLI output).
var Statuses = []Status{
	StatusNew,
	StatusNotDelegated,
	StatusDelegatedNoAddress,
	StatusNoHTTP,
	StatusAlive,
	StatusTempError,
	StatusDeleted,
	StatusInvalid,
}

// statusNames — the status names: 0..6 — exactly from the spec 6.2 table,
// 7 — the diagnostic invalid (T-109, an extension of §6.2).
var statusNames = map[Status]string{
	StatusNew:                "new",
	StatusNotDelegated:       "not_delegated",
	StatusDelegatedNoAddress: "delegated_no_address",
	StatusNoHTTP:             "no_http",
	StatusAlive:              "alive",
	StatusTempError:          "temp_error",
	StatusDeleted:            "deleted",
	StatusInvalid:            "invalid",
}

// statusByName — the reverse mapping for ParseStatus.
var statusByName = func() map[string]Status {
	m := make(map[string]Status, len(statusNames))
	for s, name := range statusNames {
		m[name] = s
	}
	return m
}()

// String returns the status name of spec 6.2 ("new", "not_delegated", …);
// for a code outside the table — "Status(<code>)".
func (s Status) String() string {
	if name, ok := statusNames[s]; ok {
		return name
	}
	return fmt.Sprintf("Status(%d)", int16(s))
}

// Valid reports that the code is in the spec 6.2 status table.
func (s Status) Valid() bool {
	_, ok := statusNames[s]
	return ok
}

// ParseStatus parses a status name (spec 6.2) into a code: "new",
// "alive", … Case and surrounding whitespace do not matter; an unknown
// name is an error with the list of the valid ones.
func ParseStatus(name string) (Status, error) {
	s, ok := statusByName[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return 0, fmt.Errorf("model: unknown domain status %q (spec 6.2: %s)", name, strings.Join(statusNamesList(), ", "))
	}
	return s, nil
}

// statusNamesList — the names in code order (error messages).
func statusNamesList() []string {
	names := make([]string, len(Statuses))
	for i, s := range Statuses {
		names[i] = statusNames[s]
	}
	return names
}

// CheckState — the check state of a domain, the domains.check_state column
// (smallint, spec 6.1): 0 pending, 1 queued, 2 done. The lifecycle of the
// scheduler task (spec 5.3); the scheduler performs the state changes —
// here only the values.
type CheckState int16

const (
	// CheckStatePending — waiting to be selected by the scheduler (the
	// partial index domains_pending_idx over check_state = 0).
	CheckStatePending CheckState = 0
	// CheckStateQueued — taken for processing, the lease (queued_at) is
	// active.
	CheckStateQueued CheckState = 1
	// CheckStateDone — the DNS/HTTP stage is complete (spec 7); the
	// render and the analysis complement the result asynchronously.
	CheckStateDone CheckState = 2
)

// String returns the state name ("pending", "queued", "done").
func (cs CheckState) String() string {
	switch cs {
	case CheckStatePending:
		return "pending"
	case CheckStateQueued:
		return "queued"
	case CheckStateDone:
		return "done"
	default:
		return fmt.Sprintf("CheckState(%d)", int16(cs))
	}
}

// Valid reports that the code is 0, 1, or 2.
func (cs CheckState) Valid() bool {
	return cs >= CheckStatePending && cs <= CheckStateDone
}

// Variant — the record variant, the subdomain_results.variant column
// (smallint, spec 6.1): 0 main, 1 the http variant (if different).
// The selection logic is spec 5.5 item 5.
type Variant int16

const (
	// VariantMain — the main variant: both protocols lead to the same
	// site or only one protocol responds (which one — the scheme
	// column).
	VariantMain Variant = 0
	// VariantHTTP — the http variant: a second record when http and
	// https lead to different sites (variant 0 is the https site).
	VariantHTTP Variant = 1
)

// String returns the variant name ("main", "http").
func (v Variant) String() string {
	switch v {
	case VariantMain:
		return "main"
	case VariantHTTP:
		return "http"
	default:
		return fmt.Sprintf("Variant(%d)", int16(v))
	}
}

// Valid reports that the code is 0 or 1.
func (v Variant) Valid() bool {
	return v == VariantMain || v == VariantHTTP
}

// Scheme — the protocol of the result record, the
// subdomain_results.scheme column (smallint, spec 6.1): 1 http, 2 https.
type Scheme int16

const (
	// SchemeHTTP — the record was obtained over http.
	SchemeHTTP Scheme = 1
	// SchemeHTTPS — the record was obtained over https.
	SchemeHTTPS Scheme = 2
)

// String returns the protocol name ("http", "https").
func (s Scheme) String() string {
	switch s {
	case SchemeHTTP:
		return "http"
	case SchemeHTTPS:
		return "https"
	default:
		return fmt.Sprintf("Scheme(%d)", int16(s))
	}
}

// Valid reports that the code is 1 or 2.
func (s Scheme) Valid() bool {
	return s == SchemeHTTP || s == SchemeHTTPS
}

// ParseScheme parses a protocol name ("http", "https"). Case and
// surrounding whitespace do not matter.
func ParseScheme(name string) (Scheme, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "http":
		return SchemeHTTP, nil
	case "https":
		return SchemeHTTPS, nil
	default:
		return 0, fmt.Errorf("model: unknown scheme %q (allowed: http, https)", name)
	}
}

// AliasKind — the relation of a subdomain to another subdomain of the same
// domain, the subdomain_results.alias_kind column (smallint, spec 6.1):
// 0 none, 1 identical, 2 redirect. The detection rules are spec 5.5
// item 6.
type AliasKind int16

const (
	// AliasNone — an independent result; no alias.
	AliasNone AliasKind = 0
	// AliasIdentical — identical to another subdomain: the final URL or
	// the content hash matches; the content is stored once.
	AliasIdentical AliasKind = 1
	// AliasRedirect — a redirect to another subdomain of this domain.
	AliasRedirect AliasKind = 2
)

// String returns the relation name ("none", "identical", "redirect").
func (a AliasKind) String() string {
	switch a {
	case AliasNone:
		return "none"
	case AliasIdentical:
		return "identical"
	case AliasRedirect:
		return "redirect"
	default:
		return fmt.Sprintf("AliasKind(%d)", int16(a))
	}
}

// Valid reports that the code is 0, 1, or 2.
func (a AliasKind) Valid() bool {
	return a >= AliasNone && a <= AliasRedirect
}

// Action — the input file action, the ingest_files.action column
// (smallint, spec 6.1): 1 add, 2 delete (spec 5.1: the path
// <zone>/add|delete/).
type Action int16

const (
	// ActionAdd — the domain addition file.
	ActionAdd Action = 1
	// ActionDelete — the domain deletion file.
	ActionDelete Action = 2
)

// String returns the action name ("add", "delete").
func (a Action) String() string {
	switch a {
	case ActionAdd:
		return "add"
	case ActionDelete:
		return "delete"
	default:
		return fmt.Sprintf("Action(%d)", int16(a))
	}
}

// Valid reports that the code is 1 or 2.
func (a Action) Valid() bool {
	return a == ActionAdd || a == ActionDelete
}
