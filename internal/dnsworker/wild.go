// Wildcard detection of the DNS stage (T-038; spec 5.4 item 3, §8).

package dnsworker

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"strings"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/model"
)

// A wildcard zone: parking domains and hosting stubs answer with an A
// record for any name inside the domain ("*.example.com. A"). For the
// crawler this means that formally "resolving" subdomains admin/crm/api/
// … are synthesized by the wildcard and lead to the same stub: checking
// each over HTTP is a guaranteed identical result and extra requests
// (spec 9: the minimum number of requests per site). Detection (spec 5.4
// item 3): an A query for <random>.<domain> with a random label that is
// guaranteed not to exist; if the random name receives A records — the
// zone is under a wildcard, and subdomains whose addresses are covered
// by the wildcard answer are excluded from the HTTP check (except the
// apex and www).
//
// The API composition (two pure operations instead of one combined "do
// the detection yourself" — the latter degenerates into ambiguous nil
// addresses):
//
//	DetectWildcard(ctx, resolver, domain) → WildcardInfo  — the detection query;
//	MarkWildcardSubdomains(wildcard addresses, subdomains) — comparison/mask.
//
// The consumer (building the q:http task, T-039) calls them in a row;
// the subdomain input is generalized (WildSubdomain) and does not depend
// on the T-037 types.

const (
	// wildLabelLength — the length of the random label of the detection
	// query: 16 chars of a-z0-9 ≈ 82.7 bits of entropy (rationale —
	// wildRandomLabel).
	wildLabelLength = 16
	// wildLabelAlphabet — the label alphabet: lowercase letters and
	// digits. No hyphen: the rules of its positions in a label are not
	// needed, and the punycode prefix "xn--" (spec 5.2 item 3) is
	// impossible in principle. DNS name comparison is case-insensitive —
	// the label is lowercase right away.
	wildLabelAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// WildSubdomain — the generalized stage input: the result of the DNS
// queries for a single subdomain. Independent of the T-037 types: the
// worker assembles the input from its own structures — ID/Label from the
// subdomains reference table, Addrs — the A/AAAA addresses after
// unwinding the CNAME chain (T-037).
type WildSubdomain struct {
	// ID — subdomains.id, the bit number in dns_mask (0..62, spec 5.10).
	ID int
	// Label — the subdomain label; "" — the apex, "www" — the spec 5.4
	// item 3 exception. Labels are stored in the system in lowercase
	// punycode (spec 5.2).
	Label string
	// Addrs — the subdomain addresses (A and AAAA); an empty set — not
	// DNS-live, it does not reach the HTTP stage anyway.
	Addrs []net.IP
}

// WildcardInfo — the outcome of the detection query (DetectWildcard).
type WildcardInfo struct {
	// Detected — the zone is under a wildcard: the random name received
	// A records.
	Detected bool
	// Addrs — the addresses of the wildcard answer, terminal ones
	// (including the CNAME-wrapping case); empty when Detected=false.
	Addrs []net.IP
	// Label — the random label used (diagnostics, logs).
	Label string
}

// WildcardResult — the outcome of MarkWildcardSubdomains: the mask of
// the subdomains excluded from the HTTP check.
type WildcardResult struct {
	// Mask — the bits of the marked subdomains; the same semantics as
	// domains.dns_mask (spec 5.4 item 4): a bit = subdomains.id. The
	// consumer (T-039) excludes these subdomains from the q:http task.
	Mask model.DNSMask
}

// IsWildcard reports whether the subdomain id is marked as wildcard.
func (r WildcardResult) IsWildcard(id int) bool { return r.Mask.Test(id) }

// IDs returns the numbers of the marked subdomains in ascending order.
func (r WildcardResult) IDs() []int { return r.Mask.IDs() }

// DetectWildcard performs the wildcard detection query for a domain
// (spec 5.4 item 3): an A record for <random>.<domain> via the stage
// resolver (dnsclient.Resolver — the same interface as the apex and
// subdomain queries of T-036/T-037; the concurrency/QPS limits of T-035
// apply to the detection automatically, no separate configuration is
// needed).
//
// The outcome:
//
//   - Detected=true — the random name received A records, the zone is
//     under a wildcard; Addrs — the addresses of the wildcard answer,
//     Label — the label;
//   - Detected=false, err=nil — an authoritative "no wildcard": NXDOMAIN
//     for the random name (a normal zone, all subdomains genuine — the
//     detection negation) or NOERROR without A records (NODATA or a
//     wildcard without addresses: an empty set matches nothing — no
//     DNS-live subdomain falls under it);
//   - err≠nil — the detection failed: a network failure/cancellation
//     (a dnsclient error) or a non-authoritative answer (SERVFAIL and
//     other RCODEs). This does not drop the domain check: the consumer
//     treats the error as "the detection failed" and continues WITHOUT
//     filtering — all DNS-live subdomains go to HTTP. The direction was
//     chosen deliberately: a false negative costs a few extra HTTP
//     requests to the stub, a false positive — missed sites (the mask
//     excludes subdomains from the check entirely); temp_error for the
//     domain is not needed here — the domain itself is already
//     classified by the apex query of T-036.
//
// domain is the domain name (apex) in punycode, as in the T-036
// questions; normalization (spec 5.2) is the caller's responsibility.
func DetectWildcard(ctx context.Context, res dnsclient.Resolver, domain string) (WildcardInfo, error) {
	if strings.TrimSpace(domain) == "" {
		return WildcardInfo{}, fmt.Errorf("dnsworker: wildcard detect: empty domain name")
	}
	label, err := wildRandomLabel()
	if err != nil {
		return WildcardInfo{}, fmt.Errorf("dnsworker: wildcard detect %s: %w", domain, err)
	}
	name := label + "." + domain
	r, err := res.Exchange(ctx, dnsclient.Question{Name: name, Type: dns.TypeA})
	if err != nil {
		return WildcardInfo{Label: label}, fmt.Errorf("dnsworker: wildcard detect %s: %w", name, err)
	}
	switch r.RCode {
	case dnsclient.RCodeNXDOMAIN:
		// An authoritative "the name does not exist": the zone has no
		// wildcard, the resolving subdomains are genuine (the detection
		// negation).
		return WildcardInfo{Label: label}, nil
	case dnsclient.RCodeNOERROR:
		addrs := wildAnswerAddrs(r)
		if len(addrs) == 0 {
			// NOERROR without A records: an addressless wildcard
			// synthesizes no addresses for the subdomains either —
			// nothing to compare, the outcome is the same as without a
			// wildcard.
			return WildcardInfo{Label: label}, nil
		}
		return WildcardInfo{Detected: true, Addrs: addrs, Label: label}, nil
	default:
		// SERVFAIL/REFUSED/… — an answer authoritative for neither "yes"
		// nor "no": an honest "the detection failed" (the consumer
		// policy is in the function comment). The RCODE stays in the
		// message for diagnostics.
		return WildcardInfo{Label: label}, fmt.Errorf(
			"dnsworker: wildcard detect %s: a non-authoritative answer %s", name, r.RCode)
	}
}

// MarkWildcardSubdomains marks the subdomains whose addresses are
// explained by the wildcard answer (spec 5.4 item 3): it returns the
// mask of the bits of the subdomains excluded from the HTTP check. The
// function is pure — no network, the detection query has already been
// made (DetectWildcard); wildAddrs — the addresses of the wildcard
// answer (WildcardInfo.Addrs), possibly empty: then nobody is marked.
//
// The match criterion is a subset: a subdomain is wildcard ⇔ it has
// addresses and all of them are inside the set of the wildcard answer's
// addresses (set equality is a special case). Rationale (spec 5.4
// item 3 "whose addresses match the wildcard answer", §8): wildcard
// synthesis returns exactly the RRset of the wildcard record, so a
// subdomain answer entirely covered by this set is indistinguishable
// from a synthesized one — we treat it as wildcard; any address outside
// the set proves an explicit record — a real host, it goes to the HTTP
// check. A subset rather than strict equality is also needed for
// truncated answers (the resolver returned part of the wildcard's
// round-robin set — the subdomain's addresses still all come from the
// wildcard set). An empty set of subdomain addresses is not marked:
// "∅ ⊆ anything" would mark everyone indiscriminately, while a DNS-live
// subdomain must have an address.
//
// The apex (Label "") and www (Label "www") are never marked, whatever
// their addresses — the direct rule of spec 5.4 item 3: "…are not
// checked over HTTP (except the apex and www)": the main names of the
// domain are always checked, even if they formally answer like a
// wildcard (a parking at the apex is still the only meaningful content
// of the domain). Identification is by label, not by id: "" = apex and
// "www" — the subdomains reference table contract (migration 000001:
// bit 0 — apex, bit 1 — www); labels are stored lowercase (spec 5.2).
//
// An error — only an invalid subdomain ID (outside 0..62: the mask
// model, spec 5.10): the input must come from the subdomains reference
// table bound by this constraint, the error signals a caller bug; a
// partially filled mask is not returned in that case.
func MarkWildcardSubdomains(wildAddrs []net.IP, subs []WildSubdomain) (WildcardResult, error) {
	var mask model.DNSMask
	for _, sub := range subs {
		if !wildExplainedByWildcard(sub, wildAddrs) {
			continue
		}
		if err := mask.Set(sub.ID); err != nil {
			return WildcardResult{}, fmt.Errorf("dnsworker: wildcard marking of subdomain %d (%q): %w",
				sub.ID, sub.Label, err)
		}
	}
	return WildcardResult{Mask: mask}, nil
}

// wildExplainedByWildcard — the marking criterion for a single subdomain
// (see MarkWildcardSubdomains): not the apex and not www, addresses
// present and every one of them inside the set of the wildcard answer's
// addresses.
func wildExplainedByWildcard(sub WildSubdomain, wildAddrs []net.IP) bool {
	if sub.Label == "" || sub.Label == "www" {
		return false // spec 5.4 item 3: except the apex and www
	}
	if len(sub.Addrs) == 0 || len(wildAddrs) == 0 {
		return false
	}
	for _, addr := range sub.Addrs {
		if !wildContainsIP(wildAddrs, addr) {
			return false // an address outside the wildcard set — an explicit record
		}
	}
	return true
}

// wildContainsIP looks for an address in the set. net.IP.Equal, not a
// byte comparison: the same point arrives in a 4-octet form (an A
// record) or a 16-octet one (AAAA, IPv4-mapped) — Equal does not
// distinguish them.
func wildContainsIP(addrs []net.IP, ip net.IP) bool {
	for _, a := range addrs {
		if a.Equal(ip) {
			return true
		}
	}
	return false
}

// wildRandomLabel generates the random label of the detection query.
//
// crypto/rand, not a seeded math/rand: the label must be guaranteed not
// to exist, and the source must not require seed management (the shared
// math/rand before Go 1.20 is deterministic without a seed, a local one
// is extra state per worker, and the global one is under a mutex on top
// of that). The cost is one getrandom(2) per domain, negligible against
// a DNS exchange; a read error is an explicit detection error, the
// consumer will continue without filtering.
//
// 16 chars of a-z0-9 — log2(36^16) ≈ 82.7 bits of entropy: the
// probability of colliding with an existing record even in a zone with
// a million names is ≈ 10^6·36^-16 ≈ 10^-19. The length ≤ 63 octets —
// the label limit (spec 5.2 item 4) is met with margin. Mapping an
// octet to a symbol by remainder: the modular bias (256 mod 36 = 4)
// shifts the symbol probabilities by less than 2% — immaterial for the
// task of "not colliding with an existing name".
func wildRandomLabel() (string, error) {
	buf := make([]byte, wildLabelLength)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("random label: %w", err)
	}
	label := make([]byte, wildLabelLength)
	for i, b := range buf {
		label[i] = wildLabelAlphabet[int(b)%len(wildLabelAlphabet)]
	}
	return string(label), nil
}

// wildAnswerAddrs collects the terminal A addresses of the detection
// query answer.
//
// A wildcard is often a CNAME wrap (random → CNAME → A, for example to
// a hosting parking). Chasing the target with separate queries is not
// needed: the detection goes through the local Unbound — a recursing
// resolver (spec 5.4), which returns the already unwound chain in the
// answer section (the CNAME + the target's A records, RFC 1034 §4.3.2)
// — all A records of the answer are the wildcard answer's addresses;
// the terminal addresses, not the CNAME, are what must be compared. Own
// queries would add an exchange per wildcard domain (350M domains;
// spec 5.4 — the minimum number of requests) with no new information; and if
// the target has no addresses (a CNAME without an A), the wildcard
// gives no addresses — DetectWildcard returns Detected=false.
func wildAnswerAddrs(r *dnsclient.Result) []net.IP {
	var addrs []net.IP
	for _, rr := range r.Answer {
		if a, ok := rr.(*dns.A); ok {
			addrs = append(addrs, a.A)
		}
	}
	return addrs
}
