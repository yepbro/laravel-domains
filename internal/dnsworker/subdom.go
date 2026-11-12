// Package dnsworker — the DNS stage of the domain check (spec 5.4): the
// apex (T-036), subdomains (T-037, this file), and wildcard detection
// (T-038) on top of the internal/dnsclient client (T-034).
//
// The subdom file — checking a domain's subdomains (T-037; spec 5.4
// item 2): for every enabled subdomain from the subdomains reference
// table — A/AAAA queries with CNAME chain unwinding; an outcome per
// subdomain and the DNSMask aggregate (a bit = subdomains.id for
// subdomains with addresses, spec 5.4 item 4). Recording the result, the
// domain status and the q:http publication are T-039: here only raw
// data, including the temp_error signs (RCode/Err).
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

// subdomMaxChain — the CNAME chain length limit: the number of hops over
// CNAME records from the original subdomain name (the 10th hop executes,
// the 11th is cut off with ErrSubdomCNAMEDepth).
//
// Rationale for the limit. Legal chains are 1–3 links long (CDNs,
// parkings, migrations); client resolvers keep the same order: Go net
// limits the CNAME chase to ten hops, the unbound recursing resolver has
// its own chase limit (it terminates malicious zones with SERVFAIL
// before we ever see them). The limit guarantees termination against
// zones answering with an endless ladder of unique names: every hop is
// an exchange with the resolver, and without a limit a single subdomain
// would consume time and QPS budget without bound. Loops (A→B→A) are
// caught even earlier than the limit — by the set of visited names
// (subdomCheckOne).
const subdomMaxChain = 10

// CNAME chain errors — sentinel values for errors.Is at the consumer
// (T-039): the subdomain outcome is "no addresses", the cause is
// recorded in SubdomResult.Err.
var (
	// ErrSubdomCNAMELoop — the CNAME chain looped (A→B→A): the zone is
	// broken, the terminal name does not exist.
	ErrSubdomCNAMELoop = errors.New("dnsworker: цикл в цепочке CNAME")
	// ErrSubdomCNAMEDepth — the CNAME chain is longer than subdomMaxChain
	// (a ladder of unique names); continuing is pointless and expensive.
	ErrSubdomCNAMEDepth = errors.New("dnsworker: цепочка CNAME длиннее предела")
)

// SubdomResult — the outcome of checking a single subdomain. "Addresses
// present/absent" is HasAddrs (setting the dns_mask bit, spec 5.4
// item 4); a subdomain's errors do not interrupt the others (spec 5.4:
// an outcome per subdomain separately).
type SubdomResult struct {
	// ID — subdomains.id, the bit number in DNSMask (0..62, spec 5.10).
	ID int
	// Label — the subdomain label, "" — the domain apex.
	Label string
	// A — the IPv4 addresses of the chain's terminal name (no
	// duplicates, in answer record order).
	A []netip.Addr
	// AAAA — the IPv6 addresses of the terminal name; filled only when
	// askAAAA (spec 5.4: AAAA is disabled by config when there is no
	// IPv6).
	AAAA []netip.Addr
	// Chain — the unwound CNAME chain: canonical (lowercase FQDNs with
	// a trailing dot) targets in hop order; Chain[0] is the target of
	// the CNAME of the original subdomain name, the last element is the
	// final target. Empty — the subdomain is not an alias.
	Chain []string
	// RCode — the code of the last completed exchange (dnsclient: an
	// RCODE is not an error). A raw sign for T-039: SERVFAIL at a
	// subdomain is a temp_error candidate for the domain, NXDOMAIN —
	// the name does not exist (RFC 8020). The zero NOERROR value with a
	// transport-level Err — there were no exchanges with an answer, see
	// Err.
	RCode dnsclient.RCode
	// Err — the subdomain error: transport (the network after dnsclient
	// retries), a loop, or a CNAME depth overrun. nil for legal
	// answers, including NXDOMAIN/NODATA/SERVFAIL (those are in RCode).
	Err error
}

// HasAddrs reports whether the subdomain has A or AAAA addresses — the
// criterion of the dns_mask bit "a resolving subdomain" (spec 5.4
// item 4). Addresses and Err are not mutually exclusive: the AAAA
// exchange could fail at the transport level after A records were found.
func (r SubdomResult) HasAddrs() bool { return len(r.A) > 0 || len(r.AAAA) > 0 }

// CNAME returns the target of the first CNAME record of the original
// subdomain name (the direct alias) or "" — the cname field of the
// q:http task (spec 5.4 item 4).
func (r SubdomResult) CNAME() string {
	if len(r.Chain) == 0 {
		return ""
	}
	return r.Chain[0]
}

// Target returns the final target of the CNAME chain (the last name for
// which addresses were looked up) or "" for a subdomain without an
// alias.
func (r SubdomResult) Target() string {
	if len(r.Chain) == 0 {
		return ""
	}
	return r.Chain[len(r.Chain)-1]
}

// SubdomReport — the outcome of checking the subdomains of one domain.
type SubdomReport struct {
	// Domain — the domain name as it arrived on input (punycode,
	// spec 5.2).
	Domain string
	// Mask — a DNSMask with the bits of the subdomains that have
	// addresses (spec 5.4 item 4: "a bit = subdomains.id for every
	// resolving subdomain"). An empty mask — not a single address, a
	// contribution to delegated_no_address (the status decision is
	// T-039, spec 5.4 item 5).
	Mask model.DNSMask
	// Results — outcomes for the enabled subdomains in input order.
	Results []SubdomResult
}

// CheckSubdomains checks the domain's subdomains (T-037; spec 5.4
// item 2): for every enabled subdomain — an A query with CNAME chain
// unwinding and, when askAAAA, an AAAA query for the terminal name. The
// answer is a report with an outcome per subdomain and the DNSMask
// aggregate.
//
// The source of the subdomain list is the subdomains table (T-039 reads
// and passes it; the input here is a slice of model.Subdomain rows).
// Rows with Enabled = false are skipped: the input may be the full
// reference table or a pre-filtered one — the result is the same.
//
// Subdomains are traversed sequentially, without concurrency or
// semaphores of its own — deliberately: the concurrency/QPS limits and
// the pace belong to the Resolver decorator of task T-035
// (dns.concurrency/max_qps, spec 5.4); an own semaphore would duplicate
// it and conflict with the configured one; an errgroup without a limit
// would produce a burst of questions that waits on someone else's
// limiter anyway and would strip the question order of determinism in
// tests. The dnsclient socket pool bounds concurrent exchanges from
// below.
//
// An individual subdomain's error does not interrupt the others (spec
// 5.4: subdomains are independent): a transport failure, SERVFAIL,
// NXDOMAIN are recorded in SubdomResult (Err/RCode) and the batch moves
// on; T-039 makes the final temp_error decision from the aggregate of
// the signs. Only a ctx cancellation/deadline interrupts the call as a
// whole — it returns (nil, ctx.Err()): no report of a partial check is
// produced, the task comes back via at-least-once delivery (spec 3.4).
func CheckSubdomains(
	ctx context.Context,
	resolve dnsclient.Resolver,
	domain string,
	subs []model.Subdomain,
	askAAAA bool,
) (*SubdomReport, error) {
	if strings.TrimSpace(domain) == "" {
		return nil, errors.New("dnsworker: пустое имя домена")
	}

	// Input validation before the first exchange: reference table errors
	// are an error of the call, not of individual subdomains (an id
	// outside 0..62 breaks DNSMask, a malformed label means a question
	// with a junk name for every subdomain).
	enabled, err := filterEnabledSubdomains(subs)
	if err != nil {
		return nil, err
	}
	if len(enabled) == 0 {
		return nil, errors.New("dnsworker: нет включённых субдоменов — проверять нечего")
	}

	report := &SubdomReport{Domain: domain, Results: make([]SubdomResult, 0, len(enabled))}
	withAddrs := make([]int, 0, len(enabled))
	for _, s := range enabled {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		res, err := subdomCheckOne(ctx, resolve, subdomFullName(domain, s.Label), askAAAA)
		if err != nil {
			return nil, err // only a ctx cancellation/deadline (see subdomCheckOne)
		}
		res.ID = int(s.ID)
		res.Label = s.Label
		if res.HasAddrs() {
			withAddrs = append(withAddrs, int(s.ID))
		}
		report.Results = append(report.Results, res)
	}
	// The ids were validated by ValidateSubdomainID above — the error is
	// unreachable, but it must not be ignored.
	if err := report.Mask.Set(withAddrs...); err != nil {
		return nil, err
	}
	return report, nil
}

// filterEnabledSubdomains validates the reference table rows and selects
// the enabled subdomains. A separate function (rather than the body of
// CheckSubdomains): a deterministic reference table failure — an id
// outside 0..62, malformed or duplicate labels, "no enabled rows" — is
// distinguished by the worker loop from transport failures: redelivery
// does not cure it, the batch is acknowledged with a Warn instead of
// spinning in retries (DNS-03).
func filterEnabledSubdomains(subs []model.Subdomain) ([]model.Subdomain, error) {
	enabled := make([]model.Subdomain, 0, len(subs))
	seenIDs := make(map[int]struct{}, len(subs))
	seenLabels := make(map[string]struct{}, len(subs))
	for _, s := range subs {
		if !s.Enabled {
			continue
		}
		if err := model.ValidateSubdomainID(int(s.ID)); err != nil {
			return nil, fmt.Errorf("dnsworker: субдомен id %d: %w", s.ID, err)
		}
		// A label is a single DNS label (as in the T-006 config and the
		// q:http task); punycode labels (xn--…) pass as ordinary octets.
		if len(s.Label) > 63 || strings.ContainsAny(s.Label, ". ") {
			return nil, fmt.Errorf("dnsworker: субдомен id %d: метка %q — одна DNS-метка до 63 октетов без точек и пробелов (пустая строка — апекс)", s.ID, s.Label)
		}
		if _, dup := seenIDs[int(s.ID)]; dup {
			return nil, fmt.Errorf("dnsworker: субдомен id %d повторяется во входе", s.ID)
		}
		if _, dup := seenLabels[s.Label]; dup {
			return nil, fmt.Errorf("dnsworker: метка субдомена %q повторяется во входе", s.Label)
		}
		seenIDs[int(s.ID)] = struct{}{}
		seenLabels[s.Label] = struct{}{}
		enabled = append(enabled, s)
	}
	return enabled, nil
}

// subdomCheckOne checks a single name (the full subdomain name): an A
// phase with CNAME chasing, then an AAAA phase for the terminal name.
// The returned error is only a ctx cancellation/deadline (to abort the
// whole call); any DNS failures stay in SubdomResult (Err/RCode).
//
// A separate CNAME-type question is never issued: per RFC 1035 §3.3.1
// the answer to a question for an alias name must contain the CNAME
// record itself — it arrives in the A/AAAA answers, and a separate
// exchange would duplicate it. This is exactly the "A, AAAA, CNAME
// queries" of spec 5.4 item 2: records of all three types are collected
// from the answers, and the CNAME chain is unwound by hopping.
func subdomCheckOne(ctx context.Context, resolve dnsclient.Resolver, name string, askAAAA bool) (SubdomResult, error) {
	var out SubdomResult
	cur := subdomCanon(name)
	visited := map[string]bool{cur: true}
	var res *dnsclient.Result

	for {
		// The answer for cur: a recursing resolver chases the chain by
		// itself and attaches it to the answer in full (RFC 1035 §3.3.1,
		// §6/2.3 of recursing practice) — first look for cur records in
		// the answer section already received, exchange only if the
		// answer does not cover cur (the chain is cut short by
		// NODATA/NXDOMAIN at the target).
		if res == nil || !subdomAnswerMentions(res.Answer, cur) {
			r, err := resolve.Exchange(ctx, dnsclient.Question{Name: cur, Type: dns.TypeA})
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return out, ctxErr
				}
				out.Err = fmt.Errorf("запрос A %s: %w", cur, err)
				return out, nil
			}
			res = r
			out.RCode = r.RCode
			if r.RCode != dnsclient.RCodeNOERROR {
				// NXDOMAIN/SERVFAIL/… — a legal answer without
				// addresses: the sign is in RCode, no addresses and no
				// Err. Rationale for CNAME→NXDOMAIN (the target does
				// not exist): the alias subdomain exists (the CNAME is
				// in place), there are no addresses — the dns_mask bit
				// is not set; for the domain status this is a
				// contribution to "no addresses" (spec 5.4 item 5), not
				// to a temporary error. The NXDOMAIN of the target does
				// not refute the existence of the subdomain itself
				// (RFC 8020 only cuts the subtree of the target).
				return out, nil
			}
		}

		target, addrs, _ := subdomScanAnswer(res.Answer, cur)
		if target == "" {
			// Terminal name: A records collected or NODATA (the name
			// exists, there are no records of this type).
			for _, a := range addrs {
				out.A = subdomAppendUnique(out.A, a)
			}
			break
		}
		if len(out.Chain) >= subdomMaxChain {
			out.Err = fmt.Errorf("%w: %d переходов от %s", ErrSubdomCNAMEDepth, subdomMaxChain, subdomCanon(name))
			return out, nil
		}
		if visited[target] {
			out.Err = fmt.Errorf("%w: %s → %s", ErrSubdomCNAMELoop, cur, target)
			return out, nil
		}
		out.Chain = append(out.Chain, target)
		visited[target] = true
		cur = target
	}

	// AAAA — a single question for the terminal name: a CNAME is a
	// property of the name, not of the query type (RFC 2181 §10.1: a
	// name with a CNAME carries no other records), and the chain has
	// already been unwound by the A phase — chasing it again would
	// duplicate the exchanges. Not asked after a failure (Err) or after
	// a negative answer (non-NOERROR): NXDOMAIN is unconditional for all
	// types (RFC 8020), and repeating the question with a different type
	// after a SERVFAIL/resolver refusal is pointless. A rare CNAME that
	// appeared at the terminal between the phases is not unwound — the
	// absence of AAAA at the name itself is recorded (the zone state has
	// changed; the next recheck episode will see the new chain).
	if askAAAA && out.Err == nil && out.RCode == dnsclient.RCodeNOERROR {
		r, err := resolve.Exchange(ctx, dnsclient.Question{Name: cur, Type: dns.TypeAAAA})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return out, ctxErr
			}
			out.Err = fmt.Errorf("запрос AAAA %s: %w", cur, err)
			return out, nil
		}
		out.RCode = r.RCode
		if r.RCode == dnsclient.RCodeNOERROR {
			_, _, addrs6 := subdomScanAnswer(r.Answer, cur)
			for _, a := range addrs6 {
				out.AAAA = subdomAppendUnique(out.AAAA, a)
			}
		}
	}
	return out, nil
}

// subdomFullName builds the full subdomain name: an empty label is the
// apex (the domain name itself), otherwise label.name.
func subdomFullName(domain, label string) string {
	if label == "" {
		return domain
	}
	return label + "." + domain
}

// subdomCanon normalizes a DNS name to the canonical comparison form:
// lowercase (DNS is case-insensitive, RFC 1035 §2.3.3) and an FQDN with
// a trailing dot.
func subdomCanon(name string) string {
	return strings.ToLower(dns.Fqdn(name))
}

// subdomAnswerMentions reports whether the answer section contains
// records belonging to the owner name: it decides whether the answer
// already received covers the next chain step or a new exchange is
// needed.
func subdomAnswerMentions(answer []dns.RR, owner string) bool {
	for _, rr := range answer {
		if subdomCanon(rr.Header().Name) == owner {
			return true
		}
	}
	return false
}

// subdomScanAnswer selects the owner name's records from the answer
// section: the CNAME target (the first one; a correct zone has exactly
// one — RFC 2181 §10.1 forbids a CNAME coexisting with other records of
// the name) and the A/AAAA addresses. A CNAME takes priority over the
// addresses of the same name: a resolver that returned an alias chases
// the chain further, and A octets next to a CNAME are junk of a broken
// zone.
func subdomScanAnswer(answer []dns.RR, owner string) (target string, a, aaaa []netip.Addr) {
	for _, rr := range answer {
		if subdomCanon(rr.Header().Name) != owner {
			continue
		}
		switch v := rr.(type) {
		case *dns.CNAME:
			if target == "" {
				target = subdomCanon(v.Target)
			}
		case *dns.A:
			// An A is 4 octets by definition (RFC 1035 §3.4.1); To4 cuts
			// off junk non-canonical RDATA.
			if addr, ok := netip.AddrFromSlice(v.A.To4()); ok {
				a = append(a, addr)
			}
		case *dns.AAAA:
			if addr, ok := netip.AddrFromSlice(v.AAAA); ok {
				aaaa = append(aaaa, addr)
			}
		}
	}
	return target, a, aaaa
}

// subdomAppendUnique adds an address without duplicates: DNS answers are
// small (tens of records), a linear check is cheaper than a map per
// answer, and the record order is preserved.
func subdomAppendUnique(dst []netip.Addr, addr netip.Addr) []netip.Addr {
	for _, x := range dst {
		if x == addr {
			return dst
		}
	}
	return append(dst, addr)
}
