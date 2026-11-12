// The end-to-end corpus of the variant and alias logic — task T-059
// (spec 11.1: "the http/https variant logic and the www/bare-www aliases
// (including the redirects bare → www, www → bare, http → https, http
// leads to another site)"; the acceptance criterion §11.4 item 2: "the
// check of each subdomain runs independently; the results follow the
// variant and alias rules (section 5.5)").
//
// Unlike TestDecideVariants (T-054) and TestDetectAliases* (T-055), which
// check each function separately, a corpus case is a whole domain story:
// subdomains × http/https checks → DecideVariants per subdomain
// independently → the whole domain's candidate records → DetectAliases.
// The stages are wired like the T-053 worker's: the record's scheme is the
// check that is the source of its data (variant 0 in a record pair
// carries the https outcome), and AliasInput is assembled from the same
// VariantInput the record got.
//
// The expectations pin which subdomain_results records arise per §11.4
// item 2: variant/scheme (T-054) and alias_kind/alias_subdomain_id
// (T-055), plus the StoreContent flag "the class content is stored once".
// Every record is run through the model validation
// (model.Variant/Scheme/AliasKind) — the corpus holds the types'
// contract.
package httpfetch

import (
	"fmt"
	"net/url"
	"testing"

	"crawler/internal/content"
	"crawler/internal/model"
)

// corpusProbe is the outcome of checking a subdomain over one protocol:
// availability (the server answered with any status, spec 5.5 item 5),
// the final URL of the page at the end of the redirect chain, the final
// page's body for the content hash, and the noHash flag "content not
// stored" (the max_store_bytes limit of T-056 or an error before storing
// — a nil hash). An unavailable check has no URL.
type corpusProbe struct {
	available bool
	final     string
	body      string
	noHash    bool
}

// corpusSubdomain is a subdomain of the task's domain and the outcomes of
// its checks over both protocols: each subdomain is checked independently
// (§11.4 item 2).
type corpusSubdomain struct {
	id    int16
	label string // '' is the apex
	https corpusProbe
	http  corpusProbe
}

// corpusRecord is the expected subdomain-result record after the variant
// and alias logic: the subdomain, variant/scheme,
// alias_kind/alias_subdomain_id (aliasSub −1 is nil for an independent
// record) and the store-content flag.
type corpusRecord struct {
	subdomain int16
	variant   model.Variant
	scheme    model.Scheme
	aliasKind model.AliasKind
	aliasSub  int16
	store     bool
}

// corpusCase is a domain story: the subdomains with check outcomes and
// the expected records (in subdomain order; within a subdomain —
// [variant 0, variant 1], as DecideVariants returns them).
type corpusCase struct {
	name   string
	domain string
	subs   []corpusSubdomain
	want   []corpusRecord
}

// corpusProbeHash is the content hash of the final page (content.HashOf —
// the T-041 contract: Put must return HashOf(data)); noHash or an
// unavailable check give nil.
func corpusProbeHash(p corpusProbe) *content.Hash {
	if !p.available || p.noHash {
		return nil
	}
	h := content.HashOf([]byte(p.body))
	return &h
}

// corpusHashBytes is the VariantInput hash in the AliasInput.ContentHash
// form; nil stays nil (content not stored).
func corpusHashBytes(h *content.Hash) []byte {
	if h == nil {
		return nil
	}
	return h[:]
}

// corpusProbeIn is the folding input from a check outcome: the final URL
// goes through NewVariantInput (the port is stripped, the query and
// fragment are excluded), as the T-053 worker will do; an unavailable
// check is a nil URL.
func corpusProbeIn(t *testing.T, scheme model.Scheme, p corpusProbe) VariantInput {
	t.Helper()
	var final *url.URL
	if p.available {
		u, err := url.Parse(p.final)
		if err != nil {
			t.Fatalf("corpus: url.Parse(%q): %v", p.final, err)
		}
		final = u
	}
	return NewVariantInput(scheme, p.available, final, corpusProbeHash(p))
}

// corpusRun runs a domain story through the "variants → aliases"
// pipeline:
//
//  1. DecideVariants for each subdomain independently (§11.4 item 2);
//     the argument order alternates — check outcomes reach the T-053
//     worker in completion order, and the pair is distinguished by the
//     Scheme field;
//  2. DetectAliases over all the domain's candidate records;
//  3. the model contract: the Variant/Scheme/AliasKind codes are valid,
//     alias_subdomain_id is consistent with alias_kind, references
//     ANOTHER subdomain (spec 5.5 item 6), and StoreContent never
//     happens with a nil hash.
func corpusRun(t *testing.T, tc corpusCase) []corpusRecord {
	t.Helper()

	// A candidate record: the variant/scheme columns and the record's data
	// are the outcome of its scheme's check (the record's scheme is the
	// data source, T-054).
	type candidate struct {
		subdomain int16
		label     string
		variant   model.Variant
		scheme    model.Scheme
		data      VariantInput
	}
	var candidates []candidate
	for i, sub := range tc.subs {
		httpsIn := corpusProbeIn(t, model.SchemeHTTPS, sub.https)
		httpIn := corpusProbeIn(t, model.SchemeHTTP, sub.http)
		first, second := httpsIn, httpIn
		if i%2 == 1 {
			first, second = httpIn, httpsIn
		}
		records, err := DecideVariants(first, second)
		if err != nil {
			t.Fatalf("corpus %q: DecideVariants(subdomain %q): %v", tc.name, sub.label, err)
		}
		for _, r := range records {
			if !r.Variant.Valid() {
				t.Fatalf("corpus %q: variant %d fails model.Variant.Valid()", tc.name, r.Variant)
			}
			if !r.Scheme.Valid() {
				t.Fatalf("corpus %q: scheme %d fails model.Scheme.Valid()", tc.name, r.Scheme)
			}
			data := httpsIn
			if r.Scheme == model.SchemeHTTP {
				data = httpIn
			}
			candidates = append(candidates, candidate{
				subdomain: sub.id, label: sub.label,
				variant: r.Variant, scheme: r.Scheme, data: data,
			})
		}
	}

	in := make([]AliasInput, len(candidates))
	for i, c := range candidates {
		in[i] = AliasInput{
			SubdomainID: c.subdomain,
			Label:       c.label,
			FinalHost:   c.data.Host,
			FinalPath:   c.data.Path,
			Scheme:      c.scheme,
			Variant:     c.variant,
			ContentHash: corpusHashBytes(c.data.ContentHash),
		}
	}
	results := DetectAliases(tc.domain, in)
	if len(results) != len(in) {
		t.Fatalf("corpus %q: DetectAliases returned %d results, want %d", tc.name, len(results), len(in))
	}

	out := make([]corpusRecord, len(results))
	for i, res := range results {
		if !res.AliasKind.Valid() {
			t.Errorf("corpus %q: record %d: alias %d fails model.AliasKind.Valid()",
				tc.name, i, res.AliasKind)
		}
		if (res.AliasSubdomainID == nil) != (res.AliasKind == model.AliasNone) {
			t.Errorf("corpus %q: record %d: alias_subdomain_id = %v with alias_kind %v — the pair must be consistent",
				tc.name, i, res.AliasSubdomainID, res.AliasKind)
		}
		if res.AliasSubdomainID != nil && *res.AliasSubdomainID == in[i].SubdomainID {
			t.Errorf("corpus %q: record %d: alias_subdomain_id points to its own subdomain %d (spec 5.5 item 6 — another subdomain)",
				tc.name, i, in[i].SubdomainID)
		}
		if res.StoreContent && in[i].ContentHash == nil {
			t.Errorf("corpus %q: record %d: StoreContent with a nil hash — nothing to store", tc.name, i)
		}
		if res.SubdomainID != in[i].SubdomainID || res.Variant != in[i].Variant {
			t.Errorf("corpus %q: record %d: the identity (%d, %v) is not parallel to the input (%d, %v)",
				tc.name, i, res.SubdomainID, res.Variant, in[i].SubdomainID, in[i].Variant)
		}
		rec := corpusRecord{
			subdomain: candidates[i].subdomain,
			variant:   candidates[i].variant,
			scheme:    candidates[i].scheme,
			aliasKind: res.AliasKind,
			aliasSub:  -1,
			store:     res.StoreContent,
		}
		if res.AliasSubdomainID != nil {
			rec.aliasSub = *res.AliasSubdomainID
		}
		out[i] = rec
	}
	return out
}

// corpusRecordString is a human-readable record for error messages.
func corpusRecordString(r corpusRecord) string {
	orig := "—"
	if r.aliasSub >= 0 {
		orig = fmt.Sprintf("%d", r.aliasSub)
	}
	return fmt.Sprintf("{subdomain %d, variant %v, scheme %v, alias_kind %v, alias_subdomain_id %s, content %v}",
		r.subdomain, r.variant, r.scheme, r.aliasKind, orig, r.store)
}

// TestVariantsCorpus is the corpus of §11.4 item 2 acceptance stories
// (the §11.1 scenarios + parking pages, chains of three, nil hashes, and
// the independence of subdomains).
func TestVariantsCorpus(t *testing.T) {
	cases := []corpusCase{
		// Spec 11.1 "bare → www": the apex redirects to www — the final
		// URL of both apex checks = www's address, http/https converge →
		// one variant 0 record per subdomain; the apex's final host is
		// www's own address ⇒ a redirect alias (kind 2) to www; the
		// content is stored once at the www original.
		{
			name:   "bare → www: the apex final URL is the www address; the apex is a redirect alias to www",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 1, store: false},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// Spec 11.1 "www → bare", mirrored: www's final host is the
		// apex's own address ⇒ the class original is the apex (id 0, the
		// minimal subdomain_id), www is a redirect alias to it.
		{
			name:   "www → bare: mirrored — the apex is the original, www is a redirect alias",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 0, store: false},
			},
		},
		// Spec 11.1 "http → https": the http check converges by a redirect
		// onto the same page as https — one site ⇒ one variant 0 record
		// with the scheme https (the canonical protocol, the https
		// check's data). The final URL's counter query (?i=2) does not
		// split the site: the query is outside the criterion (spec 5.5
		// item 5 "host and path").
		{
			name:   "http → https: both checks converge on one page — one variant 0 scheme https record",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "https://example.com/?i=2", body: "главная"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// Spec 11.1 "http leads to another site": the http check's final
		// host is foreign (a parking page) ⇒ two records: variant 0
		// (https, the apex's site) and variant 1 (http, the foreign site).
		// Records of one subdomain are never aliases of each other; both
		// are independent and store their own content. An explicit :80 in
		// the final URL does not interfere — the port is stripped.
		{
			name:   "http leads to another site: a foreign final host — two records, variant 0 (https) and variant 1 (http)",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "http://parking.example.net:80/parked.html", body: "парковка"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 0, variant: model.VariantHTTP, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// A parking page (spec 5.7, §8): both subdomains are at home on
		// DIFFERENT URLs, the content is one (equal hash) ⇒ one class,
		// the original is the minimal subdomain_id (the apex), and www is
		// identical to it (kind 1 — its own address, foreign content);
		// the class content is stored once.
		{
			name:   "a parking: an equal hash at different URLs — www is identical to the apex, one content",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "заглушка хостинга"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "заглушка хостинга"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "заглушка хостинга"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "заглушка хостинга"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasIdentical, aliasSub: 0, store: false},
			},
		},
		// A chain of three, the redirect variant: admin and app converge
		// on api (the final host is api's own address), api is at home ⇒
		// a single original (api), both others are redirect aliases to it;
		// the transitivity of a=b=c gives one class.
		{
			name:   "a chain of three: admin and app redirect to api — one original (api)",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 2, label: "admin",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
				},
				{
					id: 3, label: "api",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
				},
				{
					id: 4, label: "app",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "api-сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 2, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 3, store: false},
				{subdomain: 3, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 4, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 3, store: false},
			},
		},
		// A chain of three, the hash variant: all are at home on their own
		// addresses, the content is one (a shared placeholder) ⇒ one class
		// by hash, the original is the minimal (subdomain_id, variant) —
		// admin (id 2), the rest are identical (kind 1, their own address
		// at an equal hash).
		{
			name:   "a chain of three by hash: all at home, one content — the minimal subdomain_id is the original",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 2, label: "admin",
					https: corpusProbe{available: true, final: "https://admin.example.com/", body: "одно и то же"},
					http:  corpusProbe{available: true, final: "https://admin.example.com/", body: "одно и то же"},
				},
				{
					id: 3, label: "api",
					https: corpusProbe{available: true, final: "https://api.example.com/", body: "одно и то же"},
					http:  corpusProbe{available: true, final: "https://api.example.com/", body: "одно и то же"},
				},
				{
					id: 4, label: "app",
					https: corpusProbe{available: true, final: "https://app.example.com/", body: "одно и то же"},
					http:  corpusProbe{available: true, final: "https://app.example.com/", body: "одно и то же"},
				},
			},
			want: []corpusRecord{
				{subdomain: 2, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 3, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasIdentical, aliasSub: 2, store: false},
				{subdomain: 4, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasIdentical, aliasSub: 2, store: false},
			},
		},
		// nil hashes: no record stored content (the T-056 limit) — the
		// hash criterion is unavailable, bare → www works by the final
		// URL; there is no class content to store (store false even for
		// the original).
		{
			name:   "nil hashes: content not stored — bare → www works by the final URL, no content is stored",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "огромная страница", noHash: true},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 1, store: false},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: false},
			},
		},
		// A nil hash on one side: the apex's http check ends on a different
		// path, its content was not stored — the hash branch is unprovable
		// (two nils are needed, here there is one) ⇒ "the protocols lead
		// to different sites": two apex records; the apex's https outcome
		// and www converge (www → bare) — a redirect alias to the apex's
		// variant 0, while variant 1 has its own content and no aliases.
		{
			name:   "one hash nil: the hash branch is unprovable — the apex has two variants; www → bare — a redirect to variant 0",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "http://example.com/en", body: "английская версия", noHash: true},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://example.com/", body: "главная"},
					http:  corpusProbe{available: true, final: "https://example.com/", body: "главная"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 0, variant: model.VariantHTTP, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: false},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 0, store: false},
			},
		},
		// The independence of checks (§11.4 item 2): the apex answered
		// over neither protocol — no records for it at all (the final
		// no_http status is decided by the T-058 aggregator per §7),
		// while www is alive and yields its record without any aliases.
		{
			name:   "subdomains are independent: the apex does not answer at all — no records; www lives with its own record",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{},
					http:  corpusProbe{},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// A mixed story: the apex's https check redirects to www (a
		// redirect alias to the www original), the apex's http check moves
		// to a parking page — "http leads to another site" ⇒ the apex has
		// two records; variant 1 (the parking) does not enter the alias
		// class and stores its own content.
		{
			name:   "mixed: the apex https converges on www (an alias), the apex http moves to a parking (variant 1)",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "http://parking.example.net/", body: "парковка"},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
					http:  corpusProbe{available: true, final: "https://www.example.com/", body: "сайт"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasRedirect, aliasSub: 1, store: false},
				{subdomain: 0, variant: model.VariantHTTP, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
		// Spec 5.5 item 5 "only one protocol works": the apex answers over
		// https only, www over http only (the outcome stays at its own
		// address); one variant 0 record each with ITS OWN check's scheme —
		// the record's scheme is the data source. The sites are different —
		// no aliases.
		{
			name:   "one protocol works: the apex https-only, www http-only — one record each with its own scheme",
			domain: "example.com",
			subs: []corpusSubdomain{
				{
					id: 0, label: "",
					https: corpusProbe{available: true, final: "https://example.com/", body: "сайт"},
					http:  corpusProbe{},
				},
				{
					id: 1, label: "www",
					https: corpusProbe{},
					http:  corpusProbe{available: true, final: "http://www.example.com/", body: "сайт www"},
				},
			},
			want: []corpusRecord{
				{subdomain: 0, variant: model.VariantMain, scheme: model.SchemeHTTPS,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
				{subdomain: 1, variant: model.VariantMain, scheme: model.SchemeHTTP,
					aliasKind: model.AliasNone, aliasSub: -1, store: true},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := corpusRun(t, tc)
			if len(got) != len(tc.want) {
				t.Errorf("records %d, want %d:\n  got: %s\n  expected: %s",
					len(got), len(tc.want), corpusRecordsString(got), corpusRecordsString(tc.want))
			}
			n := len(got)
			if len(tc.want) < n {
				n = len(tc.want)
			}
			for i := 0; i < n; i++ {
				if got[i] != tc.want[i] {
					t.Errorf("record[%d] = %s, want %s", i, corpusRecordString(got[i]), corpusRecordString(tc.want[i]))
				}
			}
		})
	}
}

// corpusRecordsString is the record list on a single line for diagnostics.
func corpusRecordsString(rs []corpusRecord) string {
	out := "["
	for i, r := range rs {
		if i > 0 {
			out += " "
		}
		out += corpusRecordString(r)
	}
	return out + "]"
}
