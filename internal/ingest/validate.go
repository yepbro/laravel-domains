// Package ingest — file ingest (spec 5.1). T-016: validating the lines
// of a file against the zone given in the path (<zone>/add|delete/*.txt).
//
// A line must be a full domain name from the path zone: the name is
// normalized by the internal/idn pipeline (spec 5.2: lowercase,
// punycode, lengths), then the zone of the result is cross-checked
// against the path zone (spec 5.2 item 6). Invalid and too long names
// are rejected with a typed reason (the idn.Error class) and written
// to a separate rejected log (spec 5.1: the file, the line number, the
// truncated line, the reason); single bad lines do not make the file
// invalid — only an error share above ingest.max_error_ratio fails it.
//
// A zone-less file (add|delete/<file>): the zone of each line is
// derived from the name itself — the TLD of the normalized result
// (idn.Result.Zone — the last label of the name); there is no
// cross-check against the path zone (none is given), wrong_zone is
// impossible, zones mix within one file. Numeric suffixes (dump dates,
// IPv4 literals) do not become a zone — the numeric_zone rejection
// (idn.ErrNumericZone).
//
// Zone directories are TLDs (incoming/<tld>/add|delete/, the
// tld-only-zones spec §5.1): a full TLD dump is accepted as a whole
// (example.5g.in in incoming/in/ is a valid line with the composite
// label example.5g). A multi-level directory (a zone with a dot) is
// allowed only from the ingest.allowed_zone_paths allowlist (test
// zones like crawl.test): outside the list NewValidator answers with
// a file-level error — the pipeline moves the file to failed/, its
// lines are not parsed (T-021).
//
// T-132 (stage 16, an extension of spec 5.1): an extraction step
// (extract.go) precedes validation — a line with separators (space,
// comma, semicolon) or a token-URL is parsed into candidates; the
// first one that passes the full pipeline becomes the line record;
// if none passes — ReasonNoDomain. A single line without a scheme
// goes the old way unchanged.
//
// File parsing (empty lines, comments, CRLF/BOM) — T-015; path parsing
// and zone directory normalization — T-014; loading valid lines into
// the DB — T-017; moving to processed/failed and the ingest_files
// report — T-021.
package ingest

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"crawler/internal/idn"
)

// DefaultMaxErrorRatio — the default threshold for the share of
// rejected lines of a file (ingest.max_error_ratio). Duplicates
// config.Default so the package works without configuration too. 10%:
// real zone files contain single junk (typos, truncated records)
// which per spec 5.1 must not fail the file, while a file in the wrong
// zone directory or a broken dump give an error share close to one —
// ten percent cuts those off too.
const DefaultMaxErrorRatio = 0.1

// Line rejection reasons. The Reason type is shared by the package
// (declared in parser.go): the rejected log and the file report
// (spec 5.1). The normalization error classes match idn.ErrorKind
// (T-016 only surfaces them); wrong_zone is an ingest-specific reason:
// the domain belongs to another zone than the file directory
// (spec 5.2 item 6).
const (
	// ReasonEmpty — the name is empty after removing spaces and trailing dots.
	ReasonEmpty Reason = "empty"
	// ReasonInvalid — the name fails UTS-46/IDNA (including a broken xn--).
	ReasonInvalid Reason = "invalid"
	// ReasonLabelLen — a label longer than 63 octets in ASCII form.
	ReasonLabelLen Reason = "label_len"
	// ReasonNameLen — a name longer than 253 octets in ASCII form.
	ReasonNameLen Reason = "name_len"
	// ReasonNoLabel — there is no label before the zone: the name is itself a zone.
	ReasonNoLabel Reason = "no_label"
	// ReasonNumericZone — the zone of the name (the last label, the TLD)
	// consists only of digits (the idn.ErrNumericZone class): numeric
	// TLDs do not exist — dump dates ("Jun 18, 1999") and IPv4
	// literals ("1.2.3.4") do not pass zone derivation, no fictitious
	// numeric zones arise.
	ReasonNumericZone Reason = "numeric_zone"
	// ReasonWrongZone — the domain is not from the path zone (spec 5.2 item 6).
	ReasonWrongZone Reason = "wrong_zone"
	// ReasonNoDomain — no domain was found in a line with foreign data
	// (T-132): there are no candidates (no token contains a dot) or none
	// passed the validation pipeline.
	ReasonNoDomain Reason = "no_domain"
)

// reasonDescriptions — reason texts for String (the idn.ErrorKind style).
var reasonDescriptions = map[Reason]string{
	ReasonEmpty:       "пустое имя",
	ReasonInvalid:     "невалидное имя домена",
	ReasonLabelLen:    "метка длиннее 63 символов",
	ReasonNameLen:     "имя длиннее 253 символов",
	ReasonNoLabel:     "нет метки перед зоной",
	ReasonNumericZone: "числовая зона — таких TLD не существует",
	ReasonWrongZone:   "домен не из зоны пути",
	ReasonNoDomain:    "домен в строке не найден",
}

// String returns the reason text (the model.Status.String style).
func (r Reason) String() string {
	if d, ok := reasonDescriptions[r]; ok {
		return d
	}
	return string(r)
}

// Entry — a valid file line: a domain in the form for loading into the
// DB (T-017): zone_id is looked up by Zone, Label is the domains.label
// column, IsIDN is the is_idn bit in domains.flags (model.FlagIDN).
type Entry struct {
	// Zone — the line zone in ASCII form: the path zone (common to all
	// lines of a zone directory) or — for a zone-less file — the TLD
	// derived from the name itself (idn.Result.Zone — the last label,
	// the tld-only-zones spec §5.1).
	Zone string
	// Label — the domain label: the name without the zone; it may be
	// composite ("магазин.рф.co.uk" in the co.uk zone — the
	// internal/idn contract, the T-010 corpus).
	Label string
	// IsIDN — the label contains xn-- (spec 5.2 item 9; HasXNLabel on the label).
	IsIDN bool
}

// ValidatorOptions — the parameters for validating one file.
type ValidatorOptions struct {
	// File — the file path: an attribute of every rejected log record.
	File string
	// Zone — the zone from the file path (incoming/<zone>/add|delete,
	// T-014). Already normalized to ASCII (T-014); a unicode form or
	// uppercase is not an error — it is normalized again
	// (idempotently). The empty string means a zone-less file
	// (add|delete/<file>): line zones are derived from the names, there
	// is no cross-check against the path zone. A multi-level name (with
	// a dot) is allowed only from AllowedZonePaths — otherwise a
	// file-level error (the tld-only-zones spec §5.1).
	Zone string
	// AllowedZonePaths — multi-level zones allowed to be the file
	// directory (ingest.allowed_zone_paths, the tld-only-zones spec
	// §5.1; §6.1): testbed test zones like crawl.test. nil/empty —
	// working directories are TLD-only. Entries are compared against
	// the normalized path zone; they are normalized themselves in
	// checkZonePath idempotently (like the zone), a junk entry simply
	// does not match.
	AllowedZonePaths []string
	// MaxErrorRatio — the threshold for the share of rejected lines:
	// above it the file as a whole is considered invalid
	// (ingest.max_error_ratio). 0 — the DefaultMaxErrorRatio default;
	// (0, 1] is allowed, as in config.Validate.
	MaxErrorRatio float64
	// RejectLog — the rejected logger (NewRejectedLogger); nil — do not
	// write records (counters are kept anyway).
	RejectLog *slog.Logger
}

// Validator — validation of the lines of one zone file: it checks the
// lines, writes the rejected ones to the rejected log and keeps the
// counters for the "is the file as a whole an error" decision (the
// error share threshold, spec 5.1).
type Validator struct {
	file     string
	zone     string
	ratio    float64
	log      *slog.Logger
	total    int
	accepted int
	reasons  map[Reason]int
}

// errZoneNotAllowed — the directory zone is multi-level and not in the
// ingest.allowed_zone_paths allowlist (the tld-only-zones spec §5.1):
// a file-level error — the pipeline moves the file to failed/ with the
// reason, its lines are not parsed (T-021); identified via errors.Is
// (the errArchiveRejected style).
var errZoneNotAllowed = errors.New("многоуровневая зона каталога вне ingest.allowed_zone_paths")

// checkZonePath checks the file directory zone after normalization
// (the tld-only-zones spec §5.1): working directories are TLDs (a name
// without dots), a multi-level name — only from the allowlist.
// Allowlist entries are normalized here idempotently (a config value
// is already canonical — a repeated call changes nothing; a direct
// call with "Crawl.Test" works too), a non-zone name matches
// nothing and is left for config.Validate. Returns errZoneNotAllowed
// wrapped in the zone context for a forbidden zone; nil — for a TLD
// and allowlisted ones.
func checkZonePath(zone string, allowed []string) error {
	if !strings.Contains(zone, ".") {
		return nil // a TLD — the working layout (incoming/<tld>/add|delete/)
	}
	for _, a := range allowed {
		if a == zone {
			return nil
		}
		if na, err := idn.NormalizeZone(a); err == nil && na == zone {
			return nil
		}
	}
	return fmt.Errorf("зона %s: %w", zone, errZoneNotAllowed)
}

// NewValidator prepares file validation: the zone is normalized by the
// same pipeline as the lines (spec 5.2 item 6: "the zone is normalized
// the same way too"), so a zone directory in unicode and punycode
// gives one zone. An empty zone is a zone-less file: no zone
// cross-check is performed, the zone of each line is derived from the
// name (idn.Result.Zone). A multi-level zone outside the allowlist —
// a file-level error (errZoneNotAllowed, the tld-only-zones spec §5.1).
func NewValidator(opts ValidatorOptions) (*Validator, error) {
	zone := ""
	if opts.Zone != "" {
		var err error
		zone, err = idn.NormalizeZone(opts.Zone)
		if err != nil {
			return nil, fmt.Errorf("ingest: зона файла %s: %w", opts.File, err)
		}
		if err := checkZonePath(zone, opts.AllowedZonePaths); err != nil {
			return nil, fmt.Errorf("ingest: файл %s: %w", opts.File, err)
		}
	}
	ratio := opts.MaxErrorRatio
	if ratio == 0 {
		ratio = DefaultMaxErrorRatio
	}
	if ratio < 0 || ratio > 1 {
		return nil, fmt.Errorf("ingest: порог доли ошибок %v вне диапазона (0, 1]", ratio)
	}
	log := opts.RejectLog
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Validator{
		file:    opts.File,
		zone:    zone,
		ratio:   ratio,
		log:     log,
		reasons: make(map[Reason]int),
	}, nil
}

// Line validates the next meaningful line of the file: n is the line
// number in the file for the rejected log (empty lines and # comments
// are excluded by the T-015 parser, they never get here). A valid line
// is a domain of the path zone (ok = true, the reason is empty); an
// invalid one — the rejection reason: the line is written to the
// rejected log and counted in the counters.
//
// T-132: a line with separators (space, comma, semicolon) or a
// token-URL is parsed into candidates (extract.go): they are scanned
// left to right through the full pipeline, the first one that passes
// becomes the line record, none passed — ReasonNoDomain. A single
// line without a scheme goes the old way — the reasons did not change.
func (v *Validator) Line(n int, line string) (Entry, Reason, bool) {
	v.total++
	if cands, extract := extractCandidates(line); extract {
		for _, cand := range cands {
			if e, _, valid := checkName(cand, v.zone); valid {
				v.accepted++
				return e, "", true
			}
		}
		v.reject(n, line, ReasonNoDomain)
		return Entry{}, ReasonNoDomain, false
	}
	e, reason, ok := checkName(line, v.zone)
	if !ok {
		v.reject(n, line, reason)
		return Entry{}, reason, false
	}
	v.accepted++
	return e, "", true
}

// checkName passes the full domain name through the validation pipeline
// (idn.Normalize + domainLabel with the path zone cross-check, spec
// 5.2 items 1-6): the common path of the old single-line check and the
// extraction candidates (T-132). The counters and the rejected log are
// up to the caller (Line). A zone-less file (zone == ""): the zone
// and the label are taken from the normalized name directly (the zone
// is the TLD, the last label) — there is nothing to cross-check
// against.
func checkName(name, zone string) (Entry, Reason, bool) {
	res, err := idn.Normalize(name)
	if err != nil {
		reason := ReasonInvalid
		if kind, ok := idn.KindOf(err); ok {
			reason = Reason(kind)
		}
		return Entry{}, reason, false
	}
	if zone == "" {
		return Entry{Zone: res.Zone, Label: res.Label, IsIDN: idn.HasXNLabel(res.Label)}, "", true
	}
	label, reason := domainLabel(res, zone)
	if reason != "" {
		return Entry{}, reason, false
	}
	return Entry{Zone: zone, Label: label, IsIDN: idn.HasXNLabel(label)}, "", true
}

// domainLabel extracts the domain label of the path zone from the
// normalized name (spec 5.2 items 5-6; the tld-only-zones spec §5.2)
// and returns an empty reason if the name is a full domain name from
// the zone zone, or the rejection reason. Three branches:
//
//  1. res.Zone == zone — the path is the TLD of the name: the
//     res.Label is already split off by the last label (idn,
//     TLD-only); a composite label is the norm (example.5g.in in the
//     in directory → example.5g, formerly wrong_zone).
//  2. res.ASCII == zone — the name itself equals the path zone: there
//     is no label before the zone (ReasonNoLabel; for TLD paths
//     idn.Normalize catches this earlier — a guard branch).
//  3. the name ends with ".zone" and the path zone ends with the
//     name's "."+TLD: the path is deeper than the TLD — an
//     allowlisted test zone (example.crawl.test in the crawl.test
//     directory → label example). Otherwise — ReasonWrongZone (the
//     domain is registered in another zone, the path is not its own).
//
// The branch code did not change (the tld-only-zones spec §5.2): the
// gate for multi-level paths stands earlier — in NewValidator
// (checkZonePath, the allowlist).
func domainLabel(res idn.Result, zone string) (string, Reason) {
	if res.Zone == zone {
		// res.Label is split off by the last label (TLD-only); composite
		// labels are allowed — the internal/idn contract (the T-010 corpus).
		return res.Label, ""
	}
	// The zone name itself: there is no label before the path zone (for
	// TLD paths idn.Normalize catches this earlier as ErrNoLabel — here
	// allowlisted multi-level zones).
	if res.ASCII == zone {
		return "", ReasonNoLabel
	}
	// The name must end with the path zone, and the path zone must end
	// with the TLD of the name (the path is no smaller than the TLD —
	// otherwise the domain is of a foreign zone).
	suffix := "." + zone
	if !strings.HasSuffix(res.ASCII, suffix) || !strings.HasSuffix(zone, "."+res.Zone) {
		return "", ReasonWrongZone
	}
	return strings.TrimSuffix(res.ASCII, suffix), ""
}

// reject counts the rejected line and writes it to the rejected log
// (spec 5.1): the file, the line number, the truncated line and the
// reason.
func (v *Validator) reject(n int, line string, reason Reason) {
	v.reasons[reason]++
	v.log.Warn("строка отклонена",
		"file", v.file,
		"line", n,
		"text", truncate(line),
		"reason", string(reason),
	)
}

// ValidatorStats — file line counters (the ingest_files report, spec 5.1, T-021).
type ValidatorStats struct {
	// Total — meaningful lines of the file (without empty ones and
	// comments — the T-015 parser excludes them).
	Total int
	// Accepted — valid domain names of the zone.
	Accepted int
	// Rejected — rejected lines (Total − Accepted).
	Rejected int
	// Reasons — rejections broken down by reason.
	Reasons map[Reason]int
}

// FileResult — the outcome of file validation.
type FileResult struct {
	ValidatorStats
	// Failed — the share of rejected lines exceeded
	// ingest.max_error_ratio: the file as a whole is considered
	// invalid (spec 5.1), T-021 moves it to failed/ with the log
	// report; valid lines of such a file are not loaded.
	Failed bool
}

// Finish completes file validation and returns the outcome: the
// counters and the verdict by the error share threshold. The threshold
// is compared strictly: a share equal to the threshold does not fail
// the file ("single bad lines", spec 5.1); a file without meaningful
// lines is not considered invalid.
func (v *Validator) Finish() FileResult {
	rejected := v.total - v.accepted
	return FileResult{
		ValidatorStats: ValidatorStats{
			Total:    v.total,
			Accepted: v.accepted,
			Rejected: rejected,
			Reasons:  v.reasons,
		},
		Failed: v.total > 0 && float64(rejected)/float64(v.total) > v.ratio,
	}
}

// maxLoggedRunes — how many runes of the line to keep in rejected log
// records: the input is untrusted, a line may be arbitrarily long
// (the idn.Error truncation style).
const maxLoggedRunes = 255

// truncate cuts the line to maxLoggedRunes runes with an ellipsis.
func truncate(s string) string {
	if runes := []rune(s); len(runes) > maxLoggedRunes {
		return string(runes[:maxLoggedRunes]) + "…"
	}
	return s
}

// NewRejectedLogger — the rejected logger: a separate stream of
// records about rejected lines (spec 5.1: "rejected lines are written
// to the rejected log with the reason"). The format — the
// internal/service style (T-012): a slog text handler on w, the
// service attribute in every record (the value is the log name
// "rejected"). Where to write (the same stderr as the service log,
// or a separate file) and the level are up to the caller (cmd/ingest,
// T-017+); the records themselves are produced by Validator.reject.
func NewRejectedLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})).
		With("service", "rejected")
}
