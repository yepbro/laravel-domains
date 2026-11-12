package httpfetch

import (
	"strings"

	"crawler/internal/model"
)

// AliasInput is a generalized result of checking a single subdomain after
// folding (T-054): one persisted record (subdomain × variant).
// The pure DetectAliases function does not depend on the folding types —
// the worker (T-053) assembles the input from chain outcomes and content
// hashes.
type AliasInput struct {
	// SubdomainID is subdomains.id ('' is the apex, id 0; migration 000001).
	SubdomainID int16
	// Label is the subdomain label in punycode, '' is the apex (for
	// reference; the subdomain's own address is built as label + "." + domain).
	Label string
	// FinalHost is the host of the record's final URL (punycode, lower case —
	// the redirect stage contract, T-048); empty means there is no page
	// (fetch error) and the record matches nothing.
	FinalHost string
	// FinalPath is the path of the final URL; empty means the root "/".
	FinalPath string
	// Scheme is the scheme of the persisted record (for reference, it does
	// not affect comparisons: the protocol has already been folded by
	// folding).
	Scheme model.Scheme
	// Variant is the record's variant (0 primary, 1 http): it participates
	// only in the deterministic choice of the class original when
	// subdomain_id values are equal.
	Variant model.Variant
	// ContentHash is 16 bytes of the content hash (content.HashOf) or nil
	// when there is no content (not fetched or not stored).
	ContentHash []byte
}

// AliasResult is DetectAliases' decision for an AliasInput record: the kind
// of relation to the original subdomain and the "store content" flag. The
// result order parallels the input order; SubdomainID/Variant repeat the
// record identity for the caller's convenience.
type AliasResult struct {
	SubdomainID int16
	Variant     model.Variant
	// AliasKind — 0 none, 1 identical, 2 redirect (spec 5.5 item 6,
	// model.AliasKind).
	AliasKind model.AliasKind
	// AliasSubdomainID is the class original subdomain (the one that stores
	// the content); nil when AliasKind = AliasNone.
	AliasSubdomainID *int16
	// StoreContent stores the content in the storage: true only for the
	// class original and only with a non-zero hash (the class content is
	// stored once, spec 5.5 item 6).
	StoreContent bool
}

// DetectAliases marks the aliases of the subdomains of a single domain
// (spec 5.5 item 6): a record whose final URL or content hash matched a
// record of ANOTHER subdomain of this domain is marked with alias_kind and
// alias_subdomain_id, and the content is stored once. The function is pure —
// it loads nothing and writes nothing to the DB; the input is records after
// folding, including the case of a subdomain's single persisted record.
//
// Matches (the same criteria as in folding, T-054, but without a
// dependency on its types):
//
//   - final URL: normalized host+path are equal (host in lower case,
//     trailing FQDN dot trimmed, empty path is the root "/");
//     query and fragment are not compared — counter parameters
//     (?i=1, ?i=2, …) must not break the bare ≡ www alias;
//   - content hash: both non-zero and equal (parking pages and hosting
//     placeholders, spec 5.7, §8).
//
// Records linked by any of the criteria — including transitively
// (a=b, b=c ⇒ one class a=b=c) — form an equivalence class. Variants of
// the same subdomain are not declared aliases of each other:
// alias_subdomain_id references a different subdomain (spec item 6
// "another subdomain of this domain"); a record that ends up in one class
// transitively through a third record (a violation of the folding
// contract — variants of the same subdomain must be different sites)
// stays independent.
//
// The class original (whose record stores the content) is chosen
// deterministically — independent of the completion order of parallel
// subdomain fetches inside the worker (T-053):
//
//  1. a record "at home": its final host equals the subdomain's own
//     address (label + "." + domain) — the page at the end of the chain
//     lives at its own address and the others converged onto it
//     (bare → www); with several "home" records (identical content with
//     no redirect) — the minimal (subdomain_id, variant);
//  2. if nobody is "at home" (all records moved to one external URL —
//     a shared parking page) — the minimal (subdomain_id, variant) of the
//     class.
//
// The minimal subdomain_id automatically gives priority to the apex
// (id 0), then www (id 1) — as in the spec 11.1 tests (bare → www).
//
// Relation kinds (model.AliasKind, spec 5.5 item 6): AliasNone —
// independent; AliasIdentical — identical to the original (the same final
// URL or the same hash at its own address); AliasRedirect — redirects to
// the original subdomain: the record's final host is the original's own
// address, the chain converged straight onto it (the classic bare → www:
// bare's final host is www's address, www's is its own). The kind
// describes the record's relation specifically to the original from
// alias_subdomain_id — the pair is self-consistent.
//
// domain is the punycode name of the task's domain (T-053): the
// subdomains' own addresses are built by joining label + "." + domain.
func DetectAliases(domain string, in []AliasInput) []AliasResult {
	out := make([]AliasResult, len(in))
	if len(in) == 0 {
		return out
	}

	// The subdomains' own addresses (for "at home" and the original's
	// address).
	own := make([]string, len(in))
	for i, r := range in {
		own[i] = aliasOwnHost(domain, r.Label)
	}

	// Equivalence classes: union-find over URL and hash matches.
	uf := newAliasUF(len(in))
	byURL := make(map[string]int)
	byHash := make(map[string]int)
	for i, r := range in {
		if h := aliasNormHost(r.FinalHost); h != "" {
			key := h + "\x00" + aliasNormPath(r.FinalPath)
			if j, ok := byURL[key]; ok {
				uf.union(i, j)
			} else {
				byURL[key] = i
			}
		}
		if len(r.ContentHash) > 0 {
			key := string(r.ContentHash)
			if j, ok := byHash[key]; ok {
				uf.union(i, j)
			} else {
				byHash[key] = i
			}
		}
	}
	classes := make(map[int][]int, len(in))
	for i := range in {
		root := uf.find(i)
		classes[root] = append(classes[root], i)
	}

	for _, members := range classes {
		origin := aliasPickOrigin(in, own, members)
		originID := in[origin].SubdomainID
		for _, m := range members {
			r := in[m]
			res := AliasResult{
				SubdomainID: r.SubdomainID,
				Variant:     r.Variant,
				AliasKind:   model.AliasNone,
				// With no content there is nothing to store — true for
				// singletons and for an original without a page
				// (nil hash).
				StoreContent: len(r.ContentHash) > 0,
			}
			if m != origin && originID != r.SubdomainID {
				res.StoreContent = false
				res.AliasSubdomainID = &originID
				if aliasNormHost(r.FinalHost) == own[origin] {
					// The final address is the original's own address:
					// the record converged onto it via a redirect chain.
					res.AliasKind = model.AliasRedirect
				} else {
					res.AliasKind = model.AliasIdentical
				}
			}
			out[m] = res
		}
	}
	return out
}

// aliasPickOrigin is the index of the class original: the first pass is the
// records "at home" (final host = the subdomain's own address), the second
// (nobody is home — a shared external URL) is all records; in both passes
// the minimal (subdomain_id, variant) wins.
func aliasPickOrigin(in []AliasInput, own []string, members []int) int {
	best := -1
	for pass := 0; pass < 2 && best < 0; pass++ {
		for _, m := range members {
			atHome := aliasNormHost(in[m].FinalHost) == own[m]
			if pass == 0 && !atHome {
				continue
			}
			if best < 0 || aliasLess(in[m], in[best]) {
				best = m
			}
		}
	}
	return best
}

// aliasLess is the original selection order when "at home" status is equal:
// the minimal subdomain_id, then the minimal variant (determinism with
// several records of one subdomain).
func aliasLess(a, b AliasInput) bool {
	if a.SubdomainID != b.SubdomainID {
		return a.SubdomainID < b.SubdomainID
	}
	return a.Variant < b.Variant
}

// aliasNormHost normalizes a host for comparison: lower case and the
// trailing dot trimmed (FQDN form). The hosts of final URLs have already
// been converted to punycode by the redirect stage (T-048), so re-encoding
// is unnecessary — it would also reject IP literals from Location.
func aliasNormHost(h string) string {
	return strings.ToLower(strings.TrimSuffix(h, "."))
}

// aliasNormPath is the path of the final URL for comparison: an empty path
// is the root "/" (as with URL serialization).
func aliasNormPath(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

// aliasOwnHost is the subdomain's own address: label + "." + domain;
// the apex (label "") is the domain name itself.
func aliasOwnHost(domain, label string) string {
	d := aliasNormHost(domain)
	if label == "" {
		return d
	}
	return strings.ToLower(label) + "." + d
}

// aliasUF is a union-find over record indices: the equivalence classes of
// URL/hash matches (transitivity a=b=c).
type aliasUF struct {
	parent []int
}

func newAliasUF(n int) *aliasUF {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return &aliasUF{parent: p}
}

func (u *aliasUF) find(x int) int {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]] // path compression
		x = u.parent[x]
	}
	return x
}

func (u *aliasUF) union(a, b int) {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return
	}
	u.parent[rb] = ra
}
