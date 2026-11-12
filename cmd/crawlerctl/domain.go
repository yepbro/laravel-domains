// ---------------------------------------------------------------- domain
//
// crawlerctl domain (T-081, spec 5.11): information about a domain by
// name — for the operator (a sectioned summary) and for scripts (--json).
// The single source of data is the SQL function domain_info(text)
// (migration 000006, T-082): the CLI and psql share one source of truth.
// The command's role is to normalize the input (internal/idn, the same
// pipeline as for incoming rows), to dress up the output and, with the
// --content flag, to unpack content from the store (content_hash in the
// results is a reference; the bytes come from the ContentStore).
//
// Decisions (fixed per spec 5.11):
//
//   - "what exactly" is not found: domain_info answers NULL on the empty
//     case without distinguishing "no zone"/"no domain in the zone"; the
//     command clarifies with one light query against the zones reference
//     table (the longest name suffix present in zones — the function
//     splits the zone the same way);
//   - --json is the raw domain_info response (pretty-print); domain not
//     found — the {"found":false,…} object and exit code 1;
//   - --content — stdout receives only the bytes of the selected record
//     (the render's final DOM or the raw HTML); the summary and the note
//     about the selected record go to stderr, so the stream can be
//     redirected to a file. The record choice is the first "meaningful"
//     one: the rendered record has priority (DOM is the pipeline's main
//     result for SPA, spec 5.6), then the first record with content in
//     subdomain×variant order. --json and --content are incompatible:
//     machine output is never mixed with content bytes;
//   - cookies and headers (spec 5.11) are not part of the output: they
//     live in subdomain_results, not in the content store, and did not
//     make it into the domain_info response (the T-082 contract); the
//     summary is not byte-oriented anyway, and --content must remain a
//     clean stream.

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"
)

// domainHelp — help text for crawlerctl domain --help.
const domainHelp = `crawlerctl domain — the domain info by name (spec 5.11)

Usage:
  crawlerctl domain <name> [--json]
  crawlerctl domain <name> --content

The name — in any form: unicode or punycode, any case, a trailing dot
(normalization per spec 5.2 via internal/idn; punycode is stored in the DB).

The data — the SQL function domain_info (migration 000006): the zone, the
status, the check state, the last check, the DNS-alive subdomains (the
dns_mask decoding), the results by subdomain and variant (the scheme, the
HTTP status, the final URL, the alias, the DOM/HTML content, the error,
the check time) and the Laravel signs: the score, the version range, the
fired rules with evidence fragments; weak firings — a separate section
with the is_laravel mark (domains with only weak firings below the write
threshold into laravel_domains have no row and are not exposed by the
function).

--json     the domain_info response as-is, pretty-print (2 spaces);
           not found — {"found":false,…} and exit code 1.
--content  instead of the summary, print the stored content of the selected
           record to stdout as-is (bytes; the priority — the rendered DOM,
           otherwise the first record with content in the subdomain×variant
           order). The summary and the note about the selected record go to
           stderr, stdout can be redirected to a file. Incompatible with
           --json.

An unknown domain or zone — a clear message (what exactly was not found),
exit code 1; a malformed name — code 2. The command runs on the dev
environment stack (PostgreSQL; --content — also the content directory).
Exit codes: 0 — success, 1 — not found/error, 2 — usage.
`

// ---------------------------------------------------------------- SQL

// domainInfoSQL — the command's only query: the SQL function domain_info
// (migration 000006, T-082) returns the domain summary as one jsonb —
// zone/statuses/dns_alive/results/laravel aggregate with findings; a
// missing domain or zone is NULL, not an error. The function splits off
// the zone itself (the longest name suffix from the zones reference
// table) and looks the domain up by the unique key (zone_id, label)
// without a full scan.
const domainInfoSQL = "SELECT domain_info($1) AS info"

// zoneLongestSQL — the clarification for "not found" (spec 5.11: the
// message says what exactly was not found): the longest name suffix
// present in the zones reference table. The command passes the candidate
// suffixes (all but the full length — a domain has at least one label
// before the zone); a zone without domains does not exist (ingest
// creates zones together with domains), so the candidates are enough.
const zoneLongestSQL = "SELECT z.name FROM zones AS z " +
	"WHERE z.name = ANY($1::text[]) " +
	"ORDER BY length(z.name) DESC, z.name LIMIT 1"

// ---------------------------------------------------------------- data

// domainSubRef — {id, label} from the subdomains reference table, from
// the jsonb (dns_alive, laravel.found_on).
type domainSubRef struct {
	ID    int16  `json:"id"`
	Label string `json:"label"`
}

// domainResult — a results row: the result for a (subdomain, variant)
// pair. Nullable columns (http_status, final_url, alias, error,
// content_hash) arrive as json null → zero Go values; for the output
// that is the same "no value" (http_status 0 and error 0 never occur as
// data).
type domainResult struct {
	SubdomainID int16  `json:"subdomain_id"`
	Subdomain   string `json:"subdomain"`
	Variant     int16  `json:"variant"`
	SchemeName  string `json:"scheme_name"`
	HTTPStatus  int16  `json:"http_status"`
	FinalURL    string `json:"final_url"`
	AliasKind   int16  `json:"alias_kind"`
	Alias       string `json:"alias"`
	Rendered    bool   `json:"rendered"`
	Error       int16  `json:"error"`
	ContentHash string `json:"content_hash"` // hex; "" — no content
	CheckedAt   string `json:"checked_at"`
}

// domainSignal — evidence from laravel_findings.signals
// ([{rule, weight, evidence}, …] — the recording stage format, T-071).
type domainSignal struct {
	Rule     string  `json:"rule"`
	Weight   float64 `json:"weight"`
	Evidence string  `json:"evidence"`
}

// domainFinding — a laravel_findings row: rule hits for one result;
// weak ones (is_laravel = false, score below the threshold) are stored
// the same way as confirmed ones (spec 5.8, 13.1).
type domainFinding struct {
	SubdomainID int16          `json:"subdomain_id"`
	Subdomain   string         `json:"subdomain"`
	Variant     int16          `json:"variant"`
	Score       float64        `json:"score"`
	VerMin      string         `json:"ver_min"`
	VerMax      string         `json:"ver_max"`
	IsLaravel   bool           `json:"is_laravel"`
	Signals     []domainSignal `json:"signals"`
}

// domainLaravel — the laravel_domains aggregate with the found_on
// breakdown and all of the domain's findings by descending score
// (the T-082 contract).
type domainLaravel struct {
	Score        float64         `json:"score"`
	VerMin       string          `json:"ver_min"`
	VerMax       string          `json:"ver_max"`
	FoundOnMask  int64           `json:"found_on_mask"`
	FoundOn      []domainSubRef  `json:"found_on"`
	RulesVersion int32           `json:"rules_version"`
	UpdatedAt    string          `json:"updated_at"`
	Findings     []domainFinding `json:"findings"`
}

// domainInfo — the domain_info response. Timestamps are strings as-is
// (last_checked_at has day precision, timestamptz — ISO 8601; the
// formatting is formatDomainTS, parsing into time.Time adds nothing).
type domainInfo struct {
	Zone struct {
		ID          int16  `json:"id"`
		Name        string `json:"name"`
		NameUnicode string `json:"name_unicode"`
	} `json:"zone"`
	Domain struct {
		ID             int64  `json:"id"`
		Label          string `json:"label"`
		Name           string `json:"name"`
		StatusName     string `json:"status_name"`
		CheckStateName string `json:"check_state_name"`
		IsIDN          bool   `json:"is_idn"`
		RenderPending  bool   `json:"render_pending"`
		DNSMask        int64  `json:"dns_mask"`
		TempAttempts   int16  `json:"temp_attempts"`
		QueuedAt       string `json:"queued_at"`
		LastCheckedAt  string `json:"last_checked_at"`
	} `json:"domain"`
	DNSAlive []domainSubRef `json:"dns_alive"`
	Results  []domainResult `json:"results"`
	Laravel  *domainLaravel `json:"laravel"`
}

// fetchDomainInfo calls domain_info(punycode name): the raw jsonb (for
// --json) and a parsed copy (for the human-readable output). NULL —
// the domain or zone was not found: found=false without an error.
func fetchDomainInfo(ctx context.Context, pool db.DB, name string) (info domainInfo, raw []byte, found bool, err error) {
	if pool == nil {
		return domainInfo{}, nil, false, errors.New("domain: domain_info: pool not set")
	}
	rows, err := pool.Query(ctx, domainInfoSQL, name)
	if err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return domainInfo{}, nil, false, errors.New("domain: domain_info: empty response")
	}
	if err := rows.Scan(&raw); err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: reading: %w", err)
	}
	if err := rows.Err(); err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: %w", err)
	}
	if raw == nil { // NULL: not found — not an error (the T-082 contract)
		return domainInfo{}, nil, false, nil
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return domainInfo{}, nil, false, fmt.Errorf("domain: domain_info: json parsing: %w", err)
	}
	return info, raw, true, nil
}

// zoneSuffixCandidates — name suffixes that are zone candidates (longest
// first): example.crawl.test → [crawl.test, test]. The full name is not
// included: a domain has at least one label before the zone (as in
// domain_info).
func zoneSuffixCandidates(name string) []string {
	labels := strings.Split(name, ".")
	cands := make([]string, 0, len(labels)-1)
	for i := 1; i < len(labels); i++ {
		cands = append(cands, strings.Join(labels[i:], "."))
	}
	return cands
}

// fetchCatalogZone looks up the longest name suffix in the zones
// reference table (the command's second query — only when domain_info
// returned NULL). Returns best (the found zone, otherwise the longest
// candidate — for the "which zone is missing" message) and found.
func fetchCatalogZone(ctx context.Context, pool db.DB, name string) (best string, found bool, err error) {
	if pool == nil {
		return "", false, errors.New("domain: zones: pool not set")
	}
	cands := zoneSuffixCandidates(name)
	if len(cands) == 0 {
		return "", false, nil
	}
	rows, err := pool.Query(ctx, zoneLongestSQL, cands)
	if err != nil {
		return "", false, fmt.Errorf("domain: zones: %w", err)
	}
	defer rows.Close()
	var zone []byte
	if !rows.Next() {
		// Not found — not an error: best stays the longest candidate.
		return cands[0], false, rows.Err()
	}
	if err := rows.Scan(&zone); err != nil {
		return "", false, fmt.Errorf("domain: zones: reading: %w", err)
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("domain: zones: %w", err)
	}
	if zone == nil {
		return cands[0], false, nil
	}
	return string(zone), true, nil
}

// ---------------------------------------------------------------- output

// subLabel — subdomain label for output: ” — the domain's apex (id 0).
func subLabel(label string) string {
	if label == "" {
		return "(apex)"
	}
	return label
}

// orEmDash — an empty string → "—" (no value). A local name, not the
// orDash from test-rules ("?"): in the domain summary empty fields mean
// precisely "no value".
func orEmDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// parseDomainTS parses a timestamp from the jsonb: timestamptz arrives
// ISO 8601 (RFC 3339); date ("2006-01-02", last_checked_at) carries no
// time and never reaches the parser — it is printed as-is. A foreign
// format does not crash the output: the string is shown raw.
func parseDomainTS(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}

// formatDomainTS — a jsonb timestamp for output: timestamptz (ISO 8601)
// → "2006-01-02 15:04:05", date stays as-is.
func formatDomainTS(s string) string {
	if t, err := parseDomainTS(s); err == nil {
		return t.Format("2006-01-02 15:04:05")
	}
	return s
}

// formatDomainTSShort — the same without seconds (the "checked" column
// of the results table).
func formatDomainTSShort(s string) string {
	if t, err := parseDomainTS(s); err == nil {
		return t.Format("2006-01-02 15:04")
	}
	return s
}

// httpStatusDesc — the result's HTTP status: 0/null — "—" (a zero status
// does not exist — a response has a status, a request error does not).
func httpStatusDesc(r domainResult) string {
	if r.HTTPStatus == 0 {
		return "—"
	}
	return strconv.FormatInt(int64(r.HTTPStatus), 10)
}

// verRange — the Laravel version range: "9.0 – 11", "≥ 9.0", "≤ 11";
// both bounds empty — "—" (signs were found without a version).
func verRange(min, max string) string {
	switch {
	case min != "" && max != "":
		if min == max {
			return min
		}
		return min + " – " + max
	case min != "":
		return "≥ " + min
	case max != "":
		return "≤ " + max
	default:
		return "—"
	}
}

// httpErrorName — the subdomain error class name by the
// subdomain_results.error code. Codes — internal/httpworker.ErrorClass
// (introduced by T-058); the names are duplicated so the CLI does not
// pull in the worker package.
func httpErrorName(code int16) string {
	switch code {
	case 0:
		return "none"
	case 1:
		return "timeout"
	case 2:
		return "redirects"
	case 3:
		return "tls"
	case 4:
		return "body"
	case 5:
		return "network"
	default:
		return fmt.Sprintf("code %d", code)
	}
}

// contentKind — what is stored in the record: the render's final DOM or
// the raw HTML (spec 5.6); "—" — no content (the check never got to
// fetching bodies).
func contentKind(r domainResult) string {
	switch {
	case r.ContentHash == "":
		return "—"
	case r.Rendered:
		return "DOM"
	default:
		return "HTML"
	}
}

// aliasDesc — the subdomain's relation to another subdomain (spec 5.5
// item 6): "≡ label" — identical, "→ label" — redirect.
func aliasDesc(r domainResult) string {
	switch model.AliasKind(r.AliasKind) {
	case model.AliasIdentical:
		return "≡ " + subLabel(r.Alias)
	case model.AliasRedirect:
		return "→ " + subLabel(r.Alias)
	default:
		return "—"
	}
}

// resultFinalDesc — the final URL or the check error (code and class,
// spec 5.11 "error"), "—" — neither.
func resultFinalDesc(r domainResult) string {
	desc := r.FinalURL
	if r.Error != 0 {
		msg := fmt.Sprintf("error %d (%s)", r.Error, httpErrorName(r.Error))
		if desc != "" {
			desc += " — " + msg
		} else {
			desc = msg
		}
	}
	return orEmDash(desc)
}

// writeDomainText — the human-readable summary: a header
// (domain/zone/statuses), DNS-alive subdomains, the results table, the
// Laravel signs. minScore — the laravel_domains threshold from the
// configuration (also shown in §5.8: a score ≥ the threshold makes it
// into the aggregate).
func writeDomainText(w io.Writer, info domainInfo, minScore float64) {
	d, z := info.Domain, info.Zone

	out(w, "domain:        %s\n", d.Name)
	if u := idn.ToUnicode(d.Name); u != d.Name {
		out(w, "unicode:      %s\n", u)
	}
	if z.NameUnicode != "" && z.NameUnicode != z.Name {
		out(w, "zone:          %s (unicode: %s, id %d)\n", z.Name, z.NameUnicode, z.ID)
	} else {
		out(w, "zone:          %s (id %d)\n", z.Name, z.ID)
	}
	out(w, "status:        %s\ncheck:         %s\nchecked:       %s\n",
		orEmDash(d.StatusName), orEmDash(d.CheckStateName), orEmDash(d.LastCheckedAt))
	if d.QueuedAt != "" {
		out(w, "queued since:  %s\n", formatDomainTS(d.QueuedAt))
	}
	var flags []string
	if d.IsIDN {
		flags = append(flags, "is_idn")
	}
	if d.RenderPending {
		flags = append(flags, "render_pending")
	}
	if len(flags) > 0 {
		out(w, "flags:         %s\n", strings.Join(flags, ", "))
	}
	if d.TempAttempts > 0 {
		out(w, "temp attempts: %d\n", d.TempAttempts)
	}

	// The dns_mask breakdown — labels from the subdomains reference table (spec 5.11).
	if len(info.DNSAlive) == 0 {
		out(w, "\nDNS-alive subdomains (dns_mask = %d): none\n", d.DNSMask)
	} else {
		labels := make([]string, len(info.DNSAlive))
		for i, s := range info.DNSAlive {
			labels[i] = subLabel(s.Label)
		}
		out(w, "\nDNS-alive subdomains (dns_mask = %d): %s\n", d.DNSMask, strings.Join(labels, ", "))
	}

	// Results by subdomain × variant — a monospace table (the
	// writeStatusTable style: widths from content, the last column
	// without padding).
	out(w, "\nresults by subdomain and variant:\n")
	if len(info.Results) == 0 {
		out(w, "  — none (the domain was not checked or the DNS is not delegated)\n")
	} else {
		head := []string{"subdomain", "var.", "scheme", "http", "content", "alias", "checked", "final URL / error"}
		rows := [][]string{head}
		for _, r := range info.Results {
			rows = append(rows, []string{
				subLabel(r.Subdomain),
				model.Variant(r.Variant).String(),
				orEmDash(r.SchemeName),
				httpStatusDesc(r),
				contentKind(r),
				aliasDesc(r),
				formatDomainTSShort(r.CheckedAt),
				resultFinalDesc(r),
			})
		}
		widths := make([]int, len(head))
		for _, row := range rows {
			for i, c := range row {
				if w := cellWidth(c); w > widths[i] {
					widths[i] = w
				}
			}
		}
		for _, row := range rows {
			var b strings.Builder
			for i, c := range row {
				if i > 0 {
					b.WriteByte(' ')
				}
				if i == len(row)-1 {
					b.WriteString(c) // the last column — as-is
				} else {
					fmt.Fprintf(&b, "%-*s", widths[i], c)
				}
			}
			out(w, "%s\n", b.String())
		}
	}

	writeLaravelText(w, info.Laravel, minScore)
}

// writeLaravelText — the Laravel signs section: the aggregate (score,
// version range, subdomains, rules version), confirmed rule hits with
// evidence fragments, and a separate section for weak ones (is_laravel =
// false, below the threshold) — all rule hits are stored (spec 13.1).
func writeLaravelText(w io.Writer, l *domainLaravel, minScore float64) {
	out(w, "\nLaravel:\n")
	if l == nil {
		out(w, "  — no signs found (no row in laravel_domains;\n"+
			"    domains with only weak firings below the %s threshold are not exposed)\n",
			scoreFmt(minScore))
		return
	}

	labels := make([]string, len(l.FoundOn))
	for i, s := range l.FoundOn {
		labels[i] = subLabel(s.Label)
	}
	out(w, "  score:       %s (threshold %s)\n", scoreFmt(l.Score), scoreFmt(minScore))
	out(w, "  versions:    %s\n", verRange(l.VerMin, l.VerMax))
	out(w, "  subdomains:  %s\n", orEmDash(strings.Join(labels, ", ")))
	out(w, "  rules:       version %d, updated %s\n", l.RulesVersion, orEmDash(formatDomainTS(l.UpdatedAt)))

	var weak, strong []domainFinding
	for _, f := range l.Findings {
		if f.IsLaravel {
			strong = append(strong, f)
		} else {
			weak = append(weak, f)
		}
	}
	if len(strong) > 0 {
		out(w, "  firings (is_laravel = true):\n")
		for _, f := range strong {
			writeFindingText(w, f)
		}
	}
	if len(weak) > 0 {
		out(w, "  weak firings (is_laravel = false, score < %s):\n", scoreFmt(minScore))
		for _, f := range weak {
			writeFindingText(w, f)
		}
	}
}

// writeFindingText — one rule hit: the subdomain, variant, score, the
// version range (if narrowed) and the rules with evidence fragments.
func writeFindingText(w io.Writer, f domainFinding) {
	ver := verRange(f.VerMin, f.VerMax)
	if ver != "—" {
		out(w, "    %s %s  score %s, versions %s:\n", subLabel(f.Subdomain), model.Variant(f.Variant), scoreFmt(f.Score), ver)
	} else {
		out(w, "    %s %s  score %s:\n", subLabel(f.Subdomain), model.Variant(f.Variant), scoreFmt(f.Score))
	}
	for _, s := range f.Signals {
		out(w, "      %s (+%s): \"%s\"\n", s.Rule, scoreFmt(s.Weight), s.Evidence)
	}
}

// scoreFmt — score/weight with three decimals: the real precision in
// laravel_* (T-071) is shorter, the further tail carries no
// information.
func scoreFmt(v float64) string {
	return strconv.FormatFloat(v, 'f', 3, 64)
}

// writeDomainJSON — machine output: the raw domain_info response
// (pretty-print, 2 spaces — a normalized look for the eyes; the json is
// also valid for jq). The response comes from jsonb — always valid; a
// json.Indent failure (a foreign source) is guarded by printing as-is.
func writeDomainJSON(w io.Writer, raw []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		buf.Reset()
		buf.Write(raw)
	}
	buf.WriteByte('\n')
	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("domain: json output: %w", err)
	}
	return nil
}

// writeDomainNotFoundJSON — the machine answer to "not found":
// found=false and whatever could be learned about the zone (for
// scripts).
func writeDomainNotFoundJSON(w io.Writer, name, zone string, zoneFound bool) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(struct {
		Found     bool   `json:"found"`
		Name      string `json:"name"`
		Zone      string `json:"zone"`
		ZoneFound bool   `json:"zone_found"`
	}{false, name, zone, zoneFound}); err != nil {
		return fmt.Errorf("domain: json output: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- content

// domainContentStore — the part of the ContentStore that domain
// --content needs: only Get. *content.PackStore (T-042) satisfies it;
// extracted as an interface for tests without a store.
type domainContentStore interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// selectContentResult — the record whose content --content shows: the
// first "meaningful" one. Priority goes to the rendered record (the
// SPA's final DOM is the pipeline's main result, spec 5.6); on a tie —
// the first record with content in subdomain×variant order (domain_info
// sorts by subdomain_id, variant: the apex and the main variant come
// first).
func selectContentResult(results []domainResult) (domainResult, bool) {
	first := -1
	for i, r := range results {
		if r.ContentHash == "" {
			continue
		}
		if first < 0 {
			first = i
		}
		if r.Rendered {
			return r, true
		}
	}
	if first < 0 {
		return domainResult{}, false
	}
	return results[first], true
}

// writeDomainContent — the --content output: the domain summary and the
// note about the selected record go to stderr, the bytes go to stdout
// as-is (redirect to a file; spec 5.11 "unpacked from the store").
func writeDomainContent(ctx context.Context, store domainContentStore, info domainInfo, minScore float64, stdout, stderr io.Writer) error {
	sel, ok := selectContentResult(info.Results)
	if !ok {
		return errors.New("domain: the domain has no stored content (content_hash is empty in all the results — the check never got to body fetching)")
	}
	rawHash, err := hex.DecodeString(sel.ContentHash)
	if err != nil || len(rawHash) != content.HashLen {
		return fmt.Errorf("domain: invalid content_hash %q in a result (subdomain %s, variant %s)",
			sel.ContentHash, subLabel(sel.Subdomain), model.Variant(sel.Variant))
	}
	var hash content.Hash
	copy(hash[:], rawHash)
	data, err := store.Get(ctx, hash)
	if err != nil {
		return fmt.Errorf("domain: content %s: %w", sel.ContentHash, err)
	}

	kind := "HTML"
	if sel.Rendered {
		kind = "DOM (render)"
	}
	writeDomainText(stderr, info, minScore)
	out(stderr, "\ncontent (--content): subdomain %s, variant %s, %s, %d bytes, hash %s\n",
		subLabel(sel.Subdomain), model.Variant(sel.Variant), kind, len(data), sel.ContentHash)
	if _, err := stdout.Write(data); err != nil {
		return fmt.Errorf("domain: content output: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- command

// cmdDomain — crawlerctl domain: name normalization (spec 5.2), the
// domain_info query, output (--json / --content / human-readable).
func cmdDomain(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("domain")
	asJSON := fs.Bool("json", false, "machine output: the domain_info response as-is (pretty-print)")
	withContent := fs.Bool("content", false, "print the stored record content (DOM/HTML bytes) instead of the summary")
	if handled, err := parseFlags(fs, domainHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("specify a domain name: crawlerctl domain example.crawl.test")
	}
	if *asJSON && *withContent {
		return usagef("domain: the --json and --content flags are incompatible (machine output is not mixed with content bytes)")
	}
	// Operator input normalization — Normalize from internal/idn: the same
	// pipeline as for incoming rows (spec 5.2). Before the configuration:
	// a bad name is code 2 without touching the stack.
	res, err := idn.Normalize(fs.Arg(0))
	if err != nil {
		return usagef("invalid domain name %q: %v", fs.Arg(0), err)
	}

	// Configuration — a single YAML + environment (internal/config): the
	// PostgreSQL DSN, the content directory and the zstd dictionary for
	// --content, the laravel_min_score threshold for the Laravel section
	// (the status style, T-079).
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("domain: %w", err)
	}
	ctx, stop := signalCtx()
	defer stop()

	// CLI pool_size = 2 — the spec 6.3 connection budget as in the
	// services (the manage.go style, T-074).
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("domain: %w", err)
	}
	defer pool.Close()

	info, raw, found, err := fetchDomainInfo(ctx, pool, res.ASCII)
	if err != nil {
		return err
	}
	if !found {
		return domainNotFound(ctx, pool, res.ASCII, *asJSON, stdout)
	}

	if *asJSON {
		return writeDomainJSON(stdout, raw)
	}

	if *withContent {
		// A read-only content store reader — the analyzer style (T-071):
		// the same zstd dictionary and PackStore as the writing stages
		// (otherwise unpacking foreign blocks is impossible); Put limits
		// are not needed. ReadOnly: reading creates neither the directory
		// nor the lock file — it works even without write access to
		// content.dir, and a typo in the directory does not create
		// stray paths (runbook: content read check).
		if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, slog.New(slog.NewTextHandler(stderr, nil))); err != nil {
			return fmt.Errorf("domain: %w", err)
		}
		store, err := content.OpenPackStore(pool, content.PackOptions{Dir: cfg.Content.Dir, ReadOnly: true})
		if err != nil {
			return fmt.Errorf("domain: %w", err)
		}
		defer func() { _ = store.Close() }()
		content.WarnDictMismatch(store.Dir(), slog.New(slog.NewTextHandler(stderr, nil)))
		return writeDomainContent(ctx, store, info, cfg.Analyzer.LaravelMinScore, stdout, stderr)
	}

	writeDomainText(stdout, info, cfg.Analyzer.LaravelMinScore)
	return nil
}

// domainNotFound — a clear message about what exactly was not found
// (spec 5.11): domain_info answers NULL for both cases, the command
// clarifies against the zones reference table. --json prints found:false
// (scripts); a human gets the message on stderr via run (code 1).
func domainNotFound(ctx context.Context, pool db.DB, name string, asJSON bool, stdout io.Writer) error {
	zone, zoneFound, err := fetchCatalogZone(ctx, pool, name)
	if err != nil {
		return err
	}
	if asJSON {
		if err := writeDomainNotFoundJSON(stdout, name, zone, zoneFound); err != nil {
			return err
		}
	}
	if zoneFound {
		label := strings.TrimSuffix(name, "."+zone)
		return fmt.Errorf("domain: domain %s not found: the zone %s has no domain with the label %s (the domain was not ingested, or the name's zone is a different one: the zone is split by the name's last label (TLD) and the zones catalog, the tld-only-zones spec §4)", name, zone, label)
	}
	return fmt.Errorf("domain: domain %s not found: the zone %s is missing from the zones catalog (ingest creates zones on the first file)", name, zone)
}
