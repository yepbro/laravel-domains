// The http/https variant logic (T-054; spec 5.5 item 5, the §7 algorithm
// summary "variant 0/1 (only if different sites)"). For a subdomain the
// worker checks both protocols (T-053), and this file's pure functions
// decide how many subdomain_results records to create and with which
// variant/scheme. The input is generalized (VariantInput) and does not
// depend on FetchResult/ChainResult internals — the T-053 worker does the
// mapping; the same-site criterion (VariantSameSite) is reused by the
// subdomain aliases (T-055, spec 5.5 item 6 "the final URL … or the
// content hash matches") and by the T-059 test corpus.

package httpfetch

import (
	"fmt"
	"net/url"
	"strings"

	"crawler/internal/content"
	"crawler/internal/model"
)

// VariantInput is the outcome of checking a subdomain over one protocol
// (https or http). Scheme is the protocol of the check (the starting
// request), not necessarily of the final URL: an http check may lead by a
// redirect to an https page and vice versa.
type VariantInput struct {
	// Scheme is the protocol the check was made with (model.SchemeHTTP /
	// model.SchemeHTTPS). It determines the scheme of the record created
	// from this outcome (see VariantRecord).
	Scheme model.Scheme
	// Available — the check reached the final page: the server answered
	// with any status, including 4xx/5xx (the protocol "works",
	// spec 5.5 item 5); network errors, timeouts, exceeding the limit/a
	// redirect cycle — false. The consumer classifies the error code for
	// the result (T-058, §8); only availability matters here.
	Available bool
	// Host is the host of the FINAL URL (the page at the end of the
	// redirect chain, spec 5.5 item 4): punycode, lower case, no port.
	// The normalization is done by the T-048 chain (Location/meta refresh
	// hosts are punycode and lower case, spec 5.2 item 8), and the
	// starting URL is punycode by the task's construction; NewVariantInput
	// strips the port.
	Host string
	// Path is the path of the final URL (the decoded form of url.URL.Path).
	// The query and fragment do not enter the variant criterion (see
	// VariantSameSite) and are not represented in this field.
	Path string
	// ContentHash is the content hash of the final page (content.Hash,
	// 16 bytes, spec 5.7); nil — the content was not stored (the
	// max_store_bytes limit of T-056, an error before storing): the
	// same-site criterion degenerates to host+path. The hash of an empty
	// body is a valid value, not nil.
	ContentHash *content.Hash
}

// NewVariantInput assembles the input from the check's final URL. The
// representation of the final URL in the variant logic is fixed here in
// one place (the T-053 worker does not duplicate the rules): the host —
// without the port (url.Hostname; standard :80/:443 and non-standard
// ports do not split one site into variants), the path — u.Path, without
// the query and fragment. final == nil (no page) gives zero Host/Path. It
// does not repeat the punycode normalization: the T-048 chain has already
// brought the hosts to punycode and lower case.
func NewVariantInput(scheme model.Scheme, available bool, final *url.URL, hash *content.Hash) VariantInput {
	in := VariantInput{Scheme: scheme, Available: available, ContentHash: hash}
	if final != nil {
		in.Host = final.Hostname()
		in.Path = final.Path
	}
	return in
}

// VariantSameSite is the criterion of two outcomes being the same site
// (spec 5.5 item 5: "the same final host and path, or the same content
// hash", an OR criterion). true when:
//
//   - the final hosts and paths matched: the host — case-insensitively
//     (case is not significant in DNS; a punycode host is already lower
//     case, EqualFold is a guard against non-normalized input), the path
//     — byte-exact. The spec names only "host and path", so the query is
//     not in the criterion: counter parameters (?i=1) do not split one
//     site into two variants, and a real page difference at one path is
//     caught by the hash; the fragment is never sent to the server at
//     all. Percent-encoding is compared in the decoded form
//     (url.URL.Path);
//   - or the content hashes of both outcomes are non-nil and equal —
//     parking pages and hosting placeholders from different paths
//     (§8 "natural deduplication by hash"). Two nils are not enough:
//     "content not stored" proves nothing about sameness, and the
//     criterion degenerates to host+path.
//
// An unavailable outcome is "the same site" with nothing: both arguments
// must be Available (DecideVariants guarantees it; the T-055 aliases
// compare available outcomes).
func VariantSameSite(a, b VariantInput) bool {
	if !a.Available || !b.Available {
		return false
	}
	if strings.EqualFold(a.Host, b.Host) && a.Path == b.Path {
		return true
	}
	return a.ContentHash != nil && b.ContentHash != nil && *a.ContentHash == *b.ContentHash
}

// VariantRecord is the subdomain-result record the variant logic creates:
// the values of the variant/scheme columns (the subdomain_results model,
// spec 6.1). At the same time Scheme points to the check that is the
// record's data source (the final URL, hash, TLS, headers): variant 0,
// when it is the only record in the outcome pair and in the record pair,
// carries the https check's data, variant 1 — the http check's.
type VariantRecord struct {
	Variant model.Variant
	Scheme  model.Scheme
}

// DecideVariants decides which subdomain-result records to create from
// the outcomes of checking a subdomain over both protocols
// (spec 5.5 item 5):
//
//   - both unavailable — no records (nil): the domain's final status
//     ("nobody answered → no_http") is decided by the T-058 episode
//     aggregator per §7;
//   - one available — a single variant=0 record with that check's scheme
//     ("only one protocol works → one record with the corresponding
//     scheme"); the final URL's scheme may differ — the record carries
//     its own check's data;
//   - both available and leading to one site (VariantSameSite) — a single
//     variant=0 scheme=https record: https is the canonical protocol; the
//     spec item 5 explicitly assigns it to variant 0 ("two records:
//     variant = 0 (https) and variant = 1 (http)"), the data is taken
//     from the https check;
//   - the protocols lead to different sites — two records: variant=0
//     (https), variant=1 (http).
//
// The arguments are the https and http check outcomes in any order,
// distinguished by the Scheme field; the pair must be {http, https},
// otherwise an error (silently mixed-up outcomes would give wrong
// variant/scheme in the DB). The record order is deterministic:
// [variant 0, variant 1].
func DecideVariants(a, b VariantInput) ([]VariantRecord, error) {
	var httpsIn, httpIn VariantInput
	switch {
	case a.Scheme == model.SchemeHTTPS && b.Scheme == model.SchemeHTTP:
		httpsIn, httpIn = a, b
	case a.Scheme == model.SchemeHTTP && b.Scheme == model.SchemeHTTPS:
		httpsIn, httpIn = b, a
	default:
		return nil, fmt.Errorf("httpfetch: итоги проверок не пара http+https: схемы %v и %v",
			a.Scheme, b.Scheme)
	}

	switch {
	case !httpsIn.Available && !httpIn.Available:
		return nil, nil
	case httpsIn.Available && !httpIn.Available:
		return []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}}, nil
	case !httpsIn.Available && httpIn.Available:
		return []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTP}}, nil
	}

	if VariantSameSite(httpsIn, httpIn) {
		return []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}}, nil
	}
	return []VariantRecord{
		{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
		{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
	}, nil
}
