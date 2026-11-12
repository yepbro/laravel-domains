// Dns-worker: domain apex check (T-036; spec 5.4 item 1, §6.2, §7).
//
// CheckApex is the first step of the spec §7 DNS stage algorithm: an A
// query for the apex and classification of the response into §6.2
// branches. Only queries via dnsclient.Resolver and classification live
// here: writing the status/domains.dns_mask and publishing q:http is
// T-039; subdomains (A/AAAA/CNAME) and wildcard detection are
// T-037/T-038 — they extend the pipeline after the apex.
//
// Classification decisions (fixed per the spec):
//
//   - NXDOMAIN is the only source of not_delegated: §6.2 defines the
//     status explicitly as "apex NXDOMAIN"; RFC 8020 extends the
//     resolver's negative answer to the whole subtree of names —
//     subdomains and the wildcard are not queried (ApexStatus.Stop,
//     "do not proceed");
//   - SERVFAIL, REFUSED and other codes (FORMERR, NOTIMP, …), as well as
//     a network failure after dnsclient retries are exhausted →
//     temp_error: the resolver did not answer about the zone. REFUSED is
//     the resolver speaking about itself (ACL, query rate limit,
//     overload), not about the absence of delegation: a false terminal
//     not_delegated caused by the resolver is ruled out; temp_error
//     returns the domain to retry (T-031);
//   - ctx cancellation/deadline is an error for the caller, not the
//     domain's outcome: the episode did not finish, the task comes back
//     via delivery (lease/XAUTOCLAIM, spec 3.4);
//   - the apex AAAA is not queried: spec 5.4 item 2 assigns AAAA to
//     subdomains (there the apex is label "", T-037); the addresses of
//     this step are IPv4 from the apex A records.
//
// NS "when needed" (§5.4 item 1): while A answers with addresses, NS is
// not needed; NS is queried only on an empty A (NODATA) and confirms the
// zone exists — the "NS/response present, no A/AAAA" branch of §5.4
// item 5.

package dnsworker

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/model"
)

// ErrEmptyDomain — an empty/whitespace domain name in a q:dns batch (a
// junk or corrupted domains string). The error is deterministic:
// redelivery will not change the outcome, so the worker loop records it
// with the terminal invalid diagnosis (RecordInvalid, T-109) instead of
// dropping the batch (DNS-03): the row leaves the re-publication loop
// instead of coming back with every lease pass.
var ErrEmptyDomain = errors.New("dnsworker: empty domain name — the apex is required")

// ApexStatus — a branch of the apex response classification (spec 5.4
// item 1, §6.2, §7). Branch names that coincide with the domain statuses
// of spec 6.2 are the status names; ApexDelegated is an intermediate
// outcome (the final status is set by the HTTP stage).
type ApexStatus int8

const (
	// ApexDelegated — NOERROR with A records in the response: the apex is
	// delegated, addresses exist ("addresses → q:http", §7). The pipeline
	// continues: subdomains (T-037), wildcard (T-038), recording the
	// result (T-039); the addresses are the HTTP stage input (§5.4
	// item 4, §5.5 item 2).
	ApexDelegated ApexStatus = iota
	// ApexDelegatedNoAddress — NOERROR without A records (NODATA): the
	// name exists, the apex has no addresses; an NS query confirms the
	// zone ("NS/response present, no A/AAAA", §5.4 item 5). A separate
	// branch per §7 ("no addresses → delegated_no_address"), but the
	// final status is decided by T-039: addresses may be found at
	// subdomains (T-037), the pipeline continues.
	ApexDelegatedNoAddress
	// ApexNotDelegated — apex NXDOMAIN: status not_delegated (§6.2),
	// subdomains and the wildcard are not checked — RFC 8020 (NXDOMAIN
	// proves the absence of the whole subtree of names). See Stop.
	ApexNotDelegated
	// ApexTempError — SERVFAIL, REFUSED, another response code, or a
	// network failure after dnsclient retries: status temp_error (§6.2,
	// "a retry is required"), the episode repeats in full (T-031).
	// Subdomains are not checked.
	ApexTempError
)

// String returns the name of the classification branch ("delegated",
// "delegated_no_address", "not_delegated", "temp_error"); for a code
// outside the list — "ApexStatus(<code>)".
func (s ApexStatus) String() string {
	switch s {
	case ApexDelegated:
		return "delegated"
	case ApexDelegatedNoAddress:
		return "delegated_no_address"
	case ApexNotDelegated:
		return "not_delegated"
	case ApexTempError:
		return "temp_error"
	default:
		return fmt.Sprintf("ApexStatus(%d)", int8(s))
	}
}

// Stop — an explicit "do not proceed" sign (spec 5.4 item 1): NXDOMAIN
// ends the domain's DNS stage — subdomains (T-037) and the wildcard
// (T-038) are not checked (RFC 8020: the resolver's negative answer
// extends to the whole subtree); temp_error also stops the episode —
// with an unresponsive resolver, checking subdomains is pointless, and
// the retry is scheduled for the episode in full (T-031). The delegation
// branches continue the pipeline (false).
func (s ApexStatus) Stop() bool {
	return s == ApexNotDelegated || s == ApexTempError
}

// DomainStatus returns the domains.status code (spec 6.2) for the
// terminal apex branches: ApexNotDelegated → StatusNotDelegated
// (NXDOMAIN, RFC 8020), ApexTempError → StatusTempError. For the
// delegation branches the outcome is not decided at this step — false:
// the subdomain addresses (T-037) yield either a q:http task or the
// delegated_no_address status (§5.4 item 5, §7); T-039 performs the
// write.
func (s ApexStatus) DomainStatus() (model.Status, bool) {
	switch s {
	case ApexNotDelegated:
		return model.StatusNotDelegated, true
	case ApexTempError:
		return model.StatusTempError, true
	default:
		return model.StatusNew, false
	}
}

// ApexResult — the structured outcome of the apex check: the
// classification, the apex addresses, and the flag of the confirming NS
// query. The DB write and the q:http publication are T-039; here only
// the query results.
type ApexResult struct {
	// Status — the classification branch (see ApexStatus).
	Status ApexStatus
	// Addresses — the IPv4 addresses of the apex A records in response
	// order; the HTTP stage input (§5.4 item 4, §5.5 item 2). Non-empty
	// only for ApexDelegated (the apex AAAA is T-037; NS records are not
	// addresses).
	Addresses []netip.Addr
	// NSQueried — whether the confirming NS query ran (the NODATA branch
	// only, §5.4 item 1) — diagnostics and justification of
	// ApexDelegatedNoAddress for the T-039 logs.
	NSQueried bool
}

// CheckApex checks the domain apex (spec 5.4 item 1): an A query, a
// confirming NS query on an empty response, and classification of the
// outcome per §6.2/§7.
//
// domain is the full domain name in punycode, as in the q:dns message
// (spec 5.2); the trailing dot is optional (normalized to FQDN). The
// branching and the flags are described in the package comment and
// ApexStatus; the result is not written to the DB — the status write,
// dns_mask and the q:http publication are T-039.
//
// An error is returned only for deterministic failures: an empty domain
// name and ctx cancellation/deadline. Network errors after dnsclient
// retries (the T-034 contract: an error means attempts exhausted) and
// any response codes are classification branches (ApexTempError), not
// errors.
func CheckApex(ctx context.Context, r dnsclient.Resolver, domain string) (*ApexResult, error) {
	fqdn, err := apexFQDN(domain)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Step 1 — the apex A (spec 5.4 item 1).
	res, err := r.Exchange(ctx, dnsclient.Question{Name: fqdn, Type: dns.TypeA})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return &ApexResult{Status: ApexTempError}, nil
	}
	switch res.RCode {
	case dnsclient.RCodeNOERROR:
		if addrs := apexAAddresses(res); len(addrs) > 0 {
			return &ApexResult{Status: ApexDelegated, Addresses: addrs}, nil
		}
		// NODATA: the name exists (NOERROR — RFC 2308), no addresses;
		// the NS query (step 2) confirms the delegation.
	case dnsclient.RCodeNXDOMAIN:
		// The domain is not delegated; the subtree does not exist
		// (RFC 8020) — subdomains and the wildcard are not checked, NS
		// is not needed.
		return &ApexResult{Status: ApexNotDelegated}, nil
	default:
		// SERVFAIL, REFUSED, FORMERR, NOTIMP and others — the resolver
		// did not answer about the zone: repeat the episode (T-031).
		return &ApexResult{Status: ApexTempError}, nil
	}

	// Step 2 — NS "when needed" (spec 5.4 item 1): only when A is empty.
	ns, err := r.Exchange(ctx, dnsclient.Question{Name: fqdn, Type: dns.TypeNS})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return &ApexResult{Status: ApexTempError, NSQueried: true}, nil
	}
	out := &ApexResult{Status: ApexDelegatedNoAddress, NSQueried: true}
	switch ns.RCode {
	case dnsclient.RCodeNOERROR:
		// The zone (or the name) exists: "NS/response present, no
		// A/AAAA" (§5.4 item 5) — the apex has no addresses either
		// (step 1); NS records are not addresses by themselves. The
		// final status is T-039 based on the subdomain outcomes (§7):
		// even without apex addresses, subdomains (www, for example)
		// may yield addresses and a q:http task.
	case dnsclient.RCodeNXDOMAIN:
		// A fresher negative answer contradicts the NOERROR of step 1 —
		// the registration disappeared between the queries; the
		// resolver has the last word: the name does not exist
		// (RFC 8020), the subdomains are not checked.
		out.Status = ApexNotDelegated
	default:
		// SERVFAIL/REFUSED/… on NS too — the resolver did not answer:
		// temp_error.
		out.Status = ApexTempError
	}
	return out, nil
}

// apexFQDN normalizes the domain name to the miekg/dns question FQDN:
// a trailing dot (dns.Fqdn is idempotent — an already normalized name is
// unchanged). dnsclient.Exchange appends the dot itself; the contract is
// fixed explicitly here — the question always goes out in canonical
// form. The check is minimal: the name must be non-empty and not the
// root zone; deep validation is the territory of internal/idn at system
// input (spec 5.2); the name arrives from q:dns already normalized
// (T-024).
func apexFQDN(domain string) (string, error) {
	name := dns.Fqdn(strings.TrimSpace(domain))
	if name == "." {
		return "", ErrEmptyDomain
	}
	return name, nil
}

// apexAAddresses collects the IPv4 addresses of the response A records in
// answer order. RecordsOf filters by type; the CNAME chain is unwound by
// the recursing resolver (Unbound), which brings the target A records
// into the same answer section — unwinding manually is subdomain
// territory (T-037). Records without a valid IPv4 are skipped: a healthy
// resolver's response contains none, and this safety net against junk
// does not break the classification (the NODATA branch).
func apexAAddresses(res *dnsclient.Result) []netip.Addr {
	rrs := res.RecordsOf(dns.TypeA)
	if len(rrs) == 0 {
		return nil
	}
	addrs := make([]netip.Addr, 0, len(rrs))
	for _, rr := range rrs {
		a, ok := rr.(*dns.A)
		if !ok {
			continue
		}
		addr, ok := netip.AddrFromSlice(a.A.To4())
		if !ok {
			continue
		}
		addrs = append(addrs, addr)
	}
	return addrs
}
