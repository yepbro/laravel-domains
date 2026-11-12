// Package model — the Go typing of a domain mirroring the PostgreSQL schema
// (migration 000001): row structs of the tables, enumerations (the smallint
// codes of spec 6.1/6.2), the Flags bit flags and the DNSMask subdomain mask.
//
// The package deliberately contains no checking logic: status and state
// transitions are performed by the scheduler and the workers (spec 7), and
// the queue message formats are introduced by stage 2 (T-024) — here there
// are only the values, their parsing, validation and bitwise operations
// over them.
//
// The Go types match the columns (spec 6: "field sizes are minimal"):
//
//	smallint   → int16 or a named type based on it (Status, …);
//	int        → int32;   bigint → int64;  real → float32;
//	text       → string;  bytea  → []byte; timestamptz/date → time.Time;
//	jsonb      → []byte — raw JSON; concrete structs are introduced by
//	             the HTTP stage (T-058) and the analyzer (T-069).
//
// NULL columns are pointers (bytea/jsonb are sufficiently a nil []byte).
// The db tags repeat the column names from the migration in the same order,
// so the structs suit pgx.RowToStructByName; the correspondence of the tags
// to the schema is pinned by the TestStructColumnsMatchSchema test.
package model

import "time"

// Zone — a zones row: the reference table of zones (TLD + test-zone
// allowlist) with progress counters (updated by monitor, spec 5.9).
type Zone struct {
	ID             int16      `db:"id"`
	Name           string     `db:"name"`         // punycode
	NameUnicode    *string    `db:"name_unicode"` // display only (spec 5.2 item 7)
	IsActive       bool       `db:"is_active"`
	DomainsTotal   int64      `db:"domains_total"`
	DomainsChecked int64      `db:"domains_checked"`
	UpdatedAt      *time.Time `db:"updated_at"`
}

// Subdomain — a subdomains row: the reference table of checked subdomains.
// ID is the bit number in DNSMask (0..62, spec 5.10); Label "" is the apex.
// Adding a subdomain needs no data migration (spec 5.10).
type Subdomain struct {
	ID      int16  `db:"id"`
	Label   string `db:"label"`
	Enabled bool   `db:"enabled"`
}

// Domain — a domains row: a zone domain and the state of its check.
// Label is the left part of the name without the zone, in punycode
// (spec 5.2): everywhere inside the system the ASCII form is used.
type Domain struct {
	ZoneID        int16      `db:"zone_id"`
	ID            int64      `db:"id"`
	Label         string     `db:"label"`
	Status        Status     `db:"status"`          // spec 6.2
	CheckState    CheckState `db:"check_state"`     // 0 pending, 1 queued, 2 done
	Flags         Flags      `db:"flags"`           // bit 0 is_idn, bit 1 render_pending
	DNSMask       DNSMask    `db:"dns_mask"`        // resolving subdomains
	QueuedAt      *time.Time `db:"queued_at"`       // enqueue lease
	LastCheckedAt *time.Time `db:"last_checked_at"` // date; partially indexed (domains_recheck_idx, T-104)
	// TempAttempts — the number of consecutive re-checks assigned as
	// temp_error (T-031, migration 000005): the scheduler increments it
	// when assigning a retry; it resets on the final status and on a
	// successful check (spec 5.3).
	TempAttempts int16 `db:"temp_attempts"`
}

// SubdomainResult — a subdomain_results row: the check result of one
// (domain, subdomain, variant). The primary key is the first four columns;
// a re-check of a domain overwrites the rows with the same key (spec 3.5).
type SubdomainResult struct {
	ZoneID           int16     `db:"zone_id"`
	DomainID         int64     `db:"domain_id"`
	SubdomainID      int16     `db:"subdomain_id"`
	Variant          Variant   `db:"variant"` // 0 main, 1 http (if different)
	Scheme           Scheme    `db:"scheme"`  // 1 http, 2 https
	HTTPStatus       *int16    `db:"http_status"`
	FinalURL         *string   `db:"final_url"` // the page at the end of the redirect chain
	AliasKind        AliasKind `db:"alias_kind"`
	AliasSubdomainID *int16    `db:"alias_subdomain_id"`
	Addrs            []byte    `db:"addrs"` // packed A/AAAA (format — the DNS stage)
	CNAME            *string   `db:"cname"`
	TLS              []byte    `db:"tls"`       // jsonb: certificate, issuer, expiry, SAN
	Redirects        []byte    `db:"redirects"` // jsonb: the redirect chain
	Headers          []byte    `db:"headers"`   // jsonb: response headers
	Cookies          []byte    `db:"cookies"`   // jsonb
	Rendered         bool      `db:"rendered"`
	ContentHash      []byte    `db:"content_hash"` // 16 bytes, a reference to contents
	Error            *int16    `db:"error"`
	ResponseMs       *int32    `db:"response_ms"`
	CheckedAt        time.Time `db:"checked_at"`
	RulesVersion     *int32    `db:"rules_version"`
}

// Content — a contents row: content metadata in the pack files of the
// storage (spec 5.7). Hash is the 128-bit deduplication; RefCount is the
// number of referencing results.
type Content struct {
	Hash     []byte `db:"hash"` // 16 bytes
	PackID   int32  `db:"pack_id"`
	Off      int64  `db:"off"`
	Len      int32  `db:"len"`
	RawLen   int32  `db:"raw_len"`
	RefCount int32  `db:"refcount"`
}

// LaravelDomain — a laravel_domains row: the aggregate of Laravel signs
// over a domain (spec 5.8). FoundOnMask is the same subdomain bits as
// DNSMask.
type LaravelDomain struct {
	ZoneID       int16     `db:"zone_id"`
	DomainID     int64     `db:"domain_id"`
	Score        float32   `db:"score"`
	VerMin       *string   `db:"ver_min"`
	VerMax       *string   `db:"ver_max"`
	FoundOnMask  DNSMask   `db:"found_on_mask"` // the subdomains where Laravel was found
	RulesVersion int32     `db:"rules_version"`
	UpdatedAt    time.Time `db:"updated_at"`
}

// LaravelFinding — a laravel_findings row: rule hits per one result
// (domain, subdomain, variant); they are written on every hit regardless
// of the threshold (spec 5.8).
type LaravelFinding struct {
	ZoneID       int16   `db:"zone_id"`
	DomainID     int64   `db:"domain_id"`
	SubdomainID  int16   `db:"subdomain_id"`
	Variant      Variant `db:"variant"`
	Score        float32 `db:"score"`
	VerMin       *string `db:"ver_min"`
	VerMax       *string `db:"ver_max"`
	Signals      []byte  `db:"signals"` // jsonb: [{rule, weight, evidence}, …]
	IsLaravel    bool    `db:"is_laravel"`
	RulesVersion int32   `db:"rules_version"` // the rule set hash (internal/rules)
}

// IngestFile — an ingest_files row: the log of processed input files,
// idempotency by sha256 (spec 5.1). The counters are the result of
// applying the file.
type IngestFile struct {
	ID          int64      `db:"id"`
	ZoneID      *int16     `db:"zone_id"`
	Action      *Action    `db:"action"` // 1 add, 2 delete
	Path        *string    `db:"path"`
	SHA256      []byte     `db:"sha256"`
	LinesTotal  *int64     `db:"lines_total"`
	Added       *int64     `db:"added"`
	Existed     *int64     `db:"existed"`
	Deleted     *int64     `db:"deleted"`
	Rejected    *int64     `db:"rejected"`
	ProcessedAt *time.Time `db:"processed_at"`
}
