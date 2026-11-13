// Integration tests of the DNS stage on the test bench (T-040; spec 11.2,
// 5.4, §7): stage 3 components against the live unbound → coredns stack
// (zones from deploy/docker/testbed, scenarios.yaml scenarios).
//
// The level is components, not the raw protocol: TestDNS_TestbedScenarios
// (env_test.go) has already pinned the resolution scenarios by dns.Exchange
// exchanges; here the same zones are run through dnsclient.Client (a UDP
// socket pool, TCP fallback, retries — T-034) and dnsworker
// (CheckApex/CheckSubdomains/DetectWildcard/MarkWildcardSubdomains —
// T-036…T-038, ProcessDomain/Run — T-039). No duplication of the env_test
// scenarios: the expectations here are on the classification branches and
// the stage structures, not on the RCODEs of individual questions.
//
// The end-to-end scenario (Run): publishing to q:dns → the worker loop →
// writing the result in an isolated DB schema (migratedSchema) → a q:http
// task with addresses; an NXDOMAIN domain is terminal not_delegated
// without q:http. Redis isolation — a dedicated DB 15 (resilienceRedisEnv):
// the production q:* stream names are transport constants (T-024), keys
// are cleaned up before/after.
package integration

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
)

// dnsBlackholeAddr — the bench's black hole address (network.blackhole_ip,
// scenarios.yaml): ports without a listener, packets vanish — a
// deterministic transport timeout for dnsclient without racing the
// resolver's SERVFAIL (a live unbound on a dead upstream answers SERVFAIL
// in ~0.5 s — the outcome depends on jitter; it is the temp_error branch
// in both cases).
const dnsBlackholeAddr = "172.28.100.250:53"

// tcFallbackRecords — the number of A records of the tc-fallback scenario
// (dns.a_many in scenarios.yaml): 100 records ≈ 1.6 KiB in compressed
// packing — the answer does not fit even in the 1232 B EDNS0 buffer
// (dnsclient advertises OPT RR), unbound sets TC=1 and delivers the full
// set over TCP.
const tcFallbackRecords = 100

// dnsEnv — a dnsclient client to the bench's live unbound (unboundAddr
// from env_test.go — the address from the dev config). The per-attempt
// timeout is squeezed to a second: all bench answers are single
// milliseconds, and the dead branches do not need the default 2 s retry
// cycles (the test runs in seconds).
func dnsEnv(t *testing.T) *dnsclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := dnsclient.NewClient(ctx, dnsclient.Options{
		Addr:        unboundAddr,
		Timeout:     time.Second,
		Retries:     dnsclient.DefaultRetries,
		RetryDelay:  50 * time.Millisecond,
		UDPPoolSize: 8,
	})
	if err != nil {
		t.Fatalf("dnsclient to %s: %v", unboundAddr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ---------------------------------------------------------------- dnsclient

// TestDNSClient_TestbedScenarios — the Exchange contract (T-034) on the
// bench zones: an RCODE is not an error, TCP fallback on TC=1, retries on
// a dead transport, IsTimeout.
func TestDNSClient_TestbedScenarios(t *testing.T) {
	skipUnlessIntegration(t)
	c := dnsEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("a regular A: the address in one attempt without TCP", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "same-site.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if r.RCode != dnsclient.RCodeNOERROR {
			t.Errorf("rcode = %s, want NOERROR", r.RCode)
		}
		if r.TCP || r.Attempts != 1 {
			t.Errorf("tcp=%v attempts=%d, want false/1 (a small answer fully in UDP)", r.TCP, r.Attempts)
		}
		as := r.RecordsOf(dns.TypeA)
		if len(as) != 1 {
			t.Fatalf("A records: %d, want 1", len(as))
		}
		if a, ok := as[0].(*dns.A); !ok || a.A.String() != testwebIP {
			t.Errorf("the address = %v, want %s", as[0], testwebIP)
		}
	})

	t.Run("NXDOMAIN — a response code, not an error; SOA in authority", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "nxdomain-case.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v (NXDOMAIN is not a network error, the T-034 contract)", err)
		}
		if r.RCode != dnsclient.RCodeNXDOMAIN {
			t.Errorf("rcode = %s, want NXDOMAIN", r.RCode)
		}
		if len(r.Answer) != 0 {
			t.Errorf("answer not empty: %d records", len(r.Answer))
		}
		if len(r.Ns) == 0 {
			t.Error("authority empty — a negative answer without SOA (RFC 2308), the T-036 consumer needs Ns")
		}
	})

	t.Run("NODATA: NOERROR without A records", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "delegated-no-address.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if r.RCode != dnsclient.RCodeNOERROR {
			t.Fatalf("rcode = %s, want NOERROR (the name exists, no records of the type)", r.RCode)
		}
		if n := len(r.RecordsOf(dns.TypeA)); n != 0 {
			t.Errorf("A records: %d, want 0 (NODATA)", n)
		}
	})

	t.Run("SERVFAIL — a response code, not an error", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "servfail.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v (SERVFAIL is not a network error, the T-034 contract)", err)
		}
		if r.RCode != dnsclient.RCodeSERVFAIL {
			t.Errorf("rcode = %s, want SERVFAIL", r.RCode)
		}
	})

	t.Run("TCP-fallback: TC=1 on a large answer, the full set over TCP", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "tc-fallback.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if !r.TCP {
			t.Error("not over TCP in the end: the truncated answer (TC=1) did not trigger the fallback (T-034)")
		}
		if r.Attempts != 2 {
			t.Errorf("attempts = %d, want 2 (UDP with TC=1 + a repeat over TCP)", r.Attempts)
		}
		if r.RCode != dnsclient.RCodeNOERROR {
			t.Fatalf("rcode = %s, want NOERROR", r.RCode)
		}
		uniq := make(map[string]bool, tcFallbackRecords)
		for _, rr := range r.RecordsOf(dns.TypeA) {
			a, ok := rr.(*dns.A)
			if !ok {
				t.Fatalf("not an A record: %v", rr)
			}
			if a.A.Mask(net.CIDRMask(24, 32)).String() != "192.0.2.0" {
				t.Errorf("the address %s outside the scenario block 192.0.2.0/24", a.A)
			}
			uniq[a.A.String()] = true
		}
		if len(uniq) != tcFallbackRecords {
			t.Errorf("unique A addresses: %d, want %d (the TCP answer not truncated)", len(uniq), tcFallbackRecords)
		}
	})

	t.Run("a dead resolver: the retries exhausted, IsTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// A separate client to the bench's black hole: the fail-fast
		// NewClient opens the pool, UDP connect(2) needs no listener.
		dead, err := dnsclient.NewClient(ctx, dnsclient.Options{
			Addr:        dnsBlackholeAddr,
			Timeout:     250 * time.Millisecond,
			Retries:     1,
			RetryDelay:  50 * time.Millisecond,
			UDPPoolSize: 2,
		})
		if err != nil {
			t.Fatalf("the client to the black hole %s: %v", dnsBlackholeAddr, err)
		}
		defer func() { _ = dead.Close() }()

		start := time.Now()
		_, err = dead.Exchange(ctx, dnsclient.Question{Name: "same-site.crawl.test", Type: dns.TypeA})
		if err == nil {
			t.Fatal("the dead resolver returned an answer")
		}
		if !dnsclient.IsTimeout(err) {
			t.Errorf("the error is not a timeout (the T-035 adaptive distinguishes the branches): %v", err)
		}
		if el := time.Since(start); el > 3*time.Second {
			t.Errorf("the retries lasted %v — longer than two attempts with a pause", el)
		}
	})
}

// ---------------------------------------------------------------- CheckApex

// TestDNSWorker_CheckApex — apex classification (T-036; spec 5.4 item 1,
// §7) on the bench zones through the live unbound: a delegated one gets
// addresses, NXDOMAIN → not_delegated + Stop, SERVFAIL/timeout →
// temp_error, NODATA with a confirming NS → delegated_no_address (the
// apex branch).
func TestDNSWorker_CheckApex(t *testing.T) {
	skipUnlessIntegration(t)
	resolve := dnsEnv(t)

	cases := []struct {
		name      string
		domain    string
		status    dnsworker.ApexStatus
		nsQueried bool
		addrs     []netip.Addr
		stop      bool
		domStatus model.Status
		domOK     bool
	}{
		{
			name: "a regular domain", domain: "same-site.crawl.test",
			status: dnsworker.ApexDelegated,
			addrs:  []netip.Addr{netip.MustParseAddr(testwebIP)},
		},
		{
			name: "a wildcard zone: the apex's explicit A", domain: "wildcard.crawl.test",
			status: dnsworker.ApexDelegated,
			addrs:  []netip.Addr{netip.MustParseAddr(testwebIP)},
		},
		{
			name: "NXDOMAIN → not_delegated", domain: "nxdomain-case.crawl.test",
			status: dnsworker.ApexNotDelegated, stop: true,
			domStatus: model.StatusNotDelegated, domOK: true,
		},
		{
			name: "SERVFAIL → temp_error", domain: "servfail.crawl.test",
			status: dnsworker.ApexTempError, stop: true,
			domStatus: model.StatusTempError, domOK: true,
		},
		{
			name: "the zone's dead upstream (timeout/SERVFAIL) → temp_error", domain: "dns-timeout.crawl.test",
			status: dnsworker.ApexTempError, stop: true,
			domStatus: model.StatusTempError, domOK: true,
		},
		{
			name: "NODATA + NS → the no-addresses branch", domain: "delegated-no-address.crawl.test",
			status: dnsworker.ApexDelegatedNoAddress, nsQueried: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := dnsworker.CheckApex(ctx, resolve, tc.domain)
			if err != nil {
				t.Fatalf("CheckApex: %v", err)
			}
			if res.Status != tc.status {
				t.Errorf("status = %s, want %s", res.Status, tc.status)
			}
			if res.Status.Stop() != tc.stop {
				t.Errorf("Stop = %v, want %v (the %s branch)", res.Status.Stop(), tc.stop, res.Status)
			}
			if st, ok := res.Status.DomainStatus(); st != tc.domStatus || ok != tc.domOK {
				t.Errorf("DomainStatus = (%s, %v), want (%s, %v)", st, ok, tc.domStatus, tc.domOK)
			}
			if res.NSQueried != tc.nsQueried {
				t.Errorf("NSQueried = %v, want %v", res.NSQueried, tc.nsQueried)
			}
			if len(res.Addresses) != len(tc.addrs) {
				t.Fatalf("addresses: %v, want %v", res.Addresses, tc.addrs)
			}
			for i, a := range res.Addresses {
				if a != tc.addrs[i] {
					t.Errorf("address[%d] = %s, want %s", i, a, tc.addrs[i])
				}
			}
		})
	}
}

// ---------------------------------------------------- CheckSubdomains/wildcard

// dnsTestSubs — the input of the subdomain reference table (migration
// 000001: bit 0 — apex, bit 1 — www); three labels are enough to cover
// direct records, NXDOMAIN, and wildcard synthesis.
func dnsTestSubs() []model.Subdomain {
	return []model.Subdomain{
		{ID: 0, Label: "", Enabled: true},
		{ID: 1, Label: "www", Enabled: true},
		{ID: 2, Label: "admin", Enabled: true},
	}
}

// TestDNSWorker_CheckSubdomains — subdomain checking (T-037; spec 5.4
// item 2) on the bench zones: direct A records, unwinding a CNAME chain
// to addresses, wildcard synthesis, SERVFAIL of a looped chain, and
// NXDOMAIN — a verdict per subdomain separately, the mask over the
// resolving ones.
func TestDNSWorker_CheckSubdomains(t *testing.T) {
	skipUnlessIntegration(t)
	resolve := dnsEnv(t)

	t.Run("a regular domain: the apex and www resolve, admin NXDOMAIN", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err := dnsworker.CheckSubdomains(ctx, resolve, "same-site.crawl.test", dnsTestSubs(), true)
		if err != nil {
			t.Fatalf("CheckSubdomains: %v", err)
		}
		byLabel := map[string]dnsworker.SubdomResult{}
		for _, r := range rep.Results {
			byLabel[r.Label] = r
		}
		for _, label := range []string{"", "www"} {
			r := byLabel[label]
			if !r.HasAddrs() || len(r.A) != 1 || r.A[0].String() != testwebIP {
				t.Errorf("label %q: A = %v, want [%s]", label, r.A, testwebIP)
			}
			if r.Err != nil || r.RCode != dnsclient.RCodeNOERROR {
				t.Errorf("label %q: err=%v rcode=%s — a legal answer is not an error", label, r.Err, r.RCode)
			}
			if len(r.Chain) != 0 || r.CNAME() != "" {
				t.Errorf("label %q: an unexpected CNAME chain %v", label, r.Chain)
			}
			// AAAA was asked (askAAAA=true): the bench zone has no AAAA —
			// NODATA does not fail a subdomain with found A records.
			if len(r.AAAA) != 0 {
				t.Errorf("label %q: AAAA = %v, want empty (NODATA)", label, r.AAAA)
			}
		}
		admin := byLabel["admin"]
		if admin.RCode != dnsclient.RCodeNXDOMAIN || admin.HasAddrs() || admin.Err != nil {
			t.Errorf("admin: rcode=%s addrs=%v err=%v, want NXDOMAIN without addresses and without an error",
				admin.RCode, admin.A, admin.Err)
		}
		if !rep.Mask.Test(0) || !rep.Mask.Test(1) || rep.Mask.Test(2) {
			t.Errorf("the mask = %b: bits 0,1 resolving, bit 2 not (spec 5.4 item 4)", int64(rep.Mask))
		}
	})

	t.Run("a CNAME chain unfolds to the final addresses", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err := dnsworker.CheckSubdomains(ctx, resolve, "cname-chain.crawl.test", dnsTestSubs(), false)
		if err != nil {
			t.Fatalf("CheckSubdomains: %v", err)
		}
		if len(rep.Results) != 3 {
			t.Fatalf("results: %d, want 3", len(rep.Results))
		}
		apex := rep.Results[0] // input order: "" is first
		if want := "alias-target.crawl.test."; apex.CNAME() != want || apex.Target() != want {
			t.Errorf("the apex chain: cname=%q target=%q, want %q", apex.CNAME(), apex.Target(), want)
		}
		if len(apex.A) != 1 || apex.A[0].String() != testwebIP {
			t.Errorf("the chain's addresses: %v, want [%s] (the recursing resolver brings the target A)", apex.A, testwebIP)
		}
		if !rep.Mask.Test(0) || rep.Mask.Test(1) || rep.Mask.Test(2) {
			t.Errorf("the mask = %b: only bit 0 — the www/admin chains do not resolve", int64(rep.Mask))
		}
	})

	t.Run("a wildcard zone: all labels synthesized to one address", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err := dnsworker.CheckSubdomains(ctx, resolve, "wildcard.crawl.test", dnsTestSubs(), false)
		if err != nil {
			t.Fatalf("CheckSubdomains: %v", err)
		}
		for _, r := range rep.Results {
			if len(r.A) != 1 || r.A[0].String() != testwebIP {
				t.Errorf("label %q: A = %v, want [%s] (*.wildcard)", r.Label, r.A, testwebIP)
			}
		}
		for _, id := range []int{0, 1, 2} {
			if !rep.Mask.Test(id) {
				t.Errorf("bit %d not set — the wildcard names resolve (in the mask, spec 5.4 item 4)", id)
			}
		}
	})

	t.Run("a looping CNAME: the resolver answers SERVFAIL, the subdomain without addresses", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err := dnsworker.CheckSubdomains(ctx, resolve, "cname-loop.crawl.test", dnsTestSubs()[:1], false)
		if err != nil {
			t.Fatalf("CheckSubdomains: %v", err)
		}
		r := rep.Results[0]
		// Unbound detects the loop itself and answers SERVFAIL (does not
		// hang): the sign is in RCode, no addresses, no transport error —
		// the contribution to temp_error is decided by T-039, not here.
		if r.RCode != dnsclient.RCodeSERVFAIL || r.HasAddrs() || r.Err != nil {
			t.Errorf("apex: rcode=%s addrs=%v err=%v, want SERVFAIL without addresses and an error",
				r.RCode, r.A, r.Err)
		}
		if rep.Mask.Test(0) {
			t.Error("the apex bit set without addresses")
		}
	})
}

// TestDNSWorker_DetectWildcard — wildcard detection (T-038; spec 5.4
// item 3) on the live stack: a random label of a wildcard zone gets
// addresses, of a regular zone — NXDOMAIN (the detection negative); the
// marking uses the subset criterion — the synthesized admin/crm/api/app
// are marked, the apex and www never.
func TestDNSWorker_DetectWildcard(t *testing.T) {
	skipUnlessIntegration(t)
	resolve := dnsEnv(t)

	t.Run("a wildcard zone: a random label gets addresses", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		info, err := dnsworker.DetectWildcard(ctx, resolve, "wildcard.crawl.test")
		if err != nil {
			t.Fatalf("DetectWildcard: %v", err)
		}
		if !info.Detected {
			t.Fatal("the wildcard not detected: *.wildcard synthesizes A for any label")
		}
		if len(info.Addrs) != 1 || !info.Addrs[0].Equal(net.ParseIP(testwebIP)) {
			t.Errorf("the wildcard addresses = %v, want [%s]", info.Addrs, testwebIP)
		}
		if len(info.Label) != 16 {
			t.Errorf("the detect label %q: length %d, want 16 (crypto/rand)", info.Label, len(info.Label))
		}
	})

	t.Run("a regular zone: the detect negation without an error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		info, err := dnsworker.DetectWildcard(ctx, resolve, "same-site.crawl.test")
		if err != nil {
			t.Fatalf("DetectWildcard: %v (a random label's NXDOMAIN is an authoritative \"no\")", err)
		}
		if info.Detected || len(info.Addrs) != 0 {
			t.Errorf("the outcome = %+v, want Detected=false without addresses", info)
		}
	})

	t.Run("the marking: synthesized subdomains marked, the apex and www not", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// The live T-039 pipeline: the subdomain results of the wildcard
		// zone against the detection query addresses (all six rows of the
		// 000001 reference table: bit 0 — apex, bit 1 — www, then
		// admin/crm/api/app).
		labels := map[int16]string{0: "", 1: "www", 2: "admin", 3: "crm", 4: "api", 5: "app"}
		subs := make([]model.Subdomain, 0, len(labels))
		for i := int16(0); i < 6; i++ {
			subs = append(subs, model.Subdomain{ID: i, Label: labels[i], Enabled: true})
		}
		rep, err := dnsworker.CheckSubdomains(ctx, resolve, "wildcard.crawl.test", subs, false)
		if err != nil {
			t.Fatalf("CheckSubdomains: %v", err)
		}
		info, err := dnsworker.DetectWildcard(ctx, resolve, "wildcard.crawl.test")
		if err != nil || !info.Detected {
			t.Fatalf("DetectWildcard: detected=%v err=%v", info.Detected, err)
		}

		wildSubs := make([]dnsworker.WildSubdomain, 0, len(rep.Results))
		for _, r := range rep.Results {
			addrs := make([]net.IP, 0, len(r.A))
			for _, a := range r.A {
				addrs = append(addrs, net.IP(a.AsSlice()))
			}
			wildSubs = append(wildSubs, dnsworker.WildSubdomain{
				ID: r.ID, Label: labels[int16(r.ID)], Addrs: addrs,
			})
		}
		marked, err := dnsworker.MarkWildcardSubdomains(info.Addrs, wildSubs)
		if err != nil {
			t.Fatalf("MarkWildcardSubdomains: %v", err)
		}
		got := marked.IDs()
		want := []int{2, 3, 4, 5} // admin/crm/api/app — the direct rule of spec 5.4 item 3
		if len(got) != len(want) {
			t.Fatalf("marked %v, want %v (the apex and www are never marked)", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("marked %v, want %v", got, want)
			}
		}
	})
}

// ---------------------------------------------------------------- end-to-end

// TestDNSWorker_RunTestbedBatch — an end-to-end DNS episode (T-039; spec
// 5.4 items 4–5, §7): a q:dns batch (a delegated domain + NXDOMAIN)
// through the live unbound/Redis/PostgreSQL: the Run loop consumes the
// batch, writes the results into the isolated schema, the delegated one
// gets a q:http task with the addresses of the DNS-alive subdomains
// (apex and www), NXDOMAIN is terminal not_delegated without a task.
// Full delivery durability is T-032; here is the scenario contract as a
// whole.
func TestDNSWorker_RunTestbedBatch(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	resolve := dnsEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The zone and pending domains: same-site (A at the apex and www) and
	// nxdomain-case (apex NXDOMAIN).
	labels := []string{"same-site", "nxdomain-case"}
	ids := insertPendingDomains(t, pool, "crawl.test", labels)
	if len(ids) != 2 {
		t.Fatalf("domains inserted: %d, want 2", len(ids))
	}
	var zoneID int16
	if err := pool.QueryRow(ctx, "SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("zone crawl.test: %v", err)
	}

	// Publishing the q:dns batch — as the T-028 scheduler does.
	if _, err := q.Publish(ctx, queue.StreamDNS, queue.DNSBatch{Domains: []queue.DNSDomain{
		{ZoneID: zoneID, DomainID: ids[0], Name: "same-site.crawl.test"},
		{ZoneID: zoneID, DomainID: ids[1], Name: "nxdomain-case.crawl.test"},
	}}); err != nil {
		t.Fatalf("publishing q:dns: %v", err)
	}

	// The worker loop (group/consumer are unique — the test Redis DB is
	// cleaned up, but the names do not clash with neighboring tests).
	const group = "it-dns-e2e"
	runDone := make(chan error, 1)
	go func() {
		runDone <- dnsworker.Run(ctx, pool, q, resolve, dnsworker.Options{
			Group: group, Consumer: "it-e2e-1",
			Concurrency: 2, Consume: 1, AskAAAA: true,
		})
	}()

	// Both episodes are finished (the polling deadline keeps the test
	// time in seconds: bench answers are milliseconds).
	deadline := time.Now().Add(15 * time.Second)
	for {
		var done int
		if err := pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM domains WHERE check_state = %d", int16(model.CheckStateDone)),
		).Scan(&done); err != nil {
			t.Fatalf("counting done: %v", err)
		}
		if done == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the domains not moved to done within 15 s — the worker's loop did not process the batch")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Domain records: the delegated one — the apex+www mask without a
	// terminal status (the verdict belongs to the HTTP stage, §7),
	// NXDOMAIN — not_delegated with a zero mask (RFC 8020: subdomains were
	// not checked).
	type row struct {
		status   model.Status
		mask     int64
		queuedAt *time.Time
		lastAt   *time.Time
	}
	rows := map[string]row{}
	for i, label := range labels {
		var r row
		if err := pool.QueryRow(ctx,
			"SELECT status, dns_mask, queued_at, last_checked_at FROM domains WHERE id = $1", ids[i],
		).Scan(&r.status, &r.mask, &r.queuedAt, &r.lastAt); err != nil {
			t.Fatalf("domain %s: %v", label, err)
		}
		rows[label] = r
	}
	if r := rows["same-site"]; r.status != model.StatusNew || r.mask != 0b11 {
		t.Errorf("same-site: status=%s mask=%b, want new and bits 0,1 (apex+www)", r.status, r.mask)
	}
	if r := rows["nxdomain-case"]; r.status != model.StatusNotDelegated || r.mask != 0 {
		t.Errorf("nxdomain-case: status=%s mask=%b, want not_delegated and an empty mask", r.status, r.mask)
	}
	for label, r := range rows {
		if r.queuedAt != nil {
			t.Errorf("%s: queued_at not cleared on the lease completion", label)
		}
		if r.lastAt == nil {
			t.Errorf("%s: last_checked_at not recorded", label)
		}
	}

	// The q:http task — exactly one, for the delegated domain: DNS-alive
	// subdomains with addresses (apex and www), the NXDOMAIN domain gets
	// no task.
	httpMsgs := consumeTasks(t, q, queue.StreamHTTP, "it-http-e2e", "op", 10)
	if len(httpMsgs) != 1 {
		t.Fatalf("tasks in q:http: %d, want 1 (the NXDOMAIN domain without a task)", len(httpMsgs))
	}
	task, err := queue.Decode[queue.HTTPTask](httpMsgs[0])
	if err != nil {
		t.Fatalf("decoding q:http: %v", err)
	}
	if task.Name != "same-site.crawl.test" || task.ZoneID != zoneID || task.DomainID != ids[0] {
		t.Errorf("the q:http task: name=%s zone=%d domain=%d", task.Name, task.ZoneID, task.DomainID)
	}
	subIDs := map[int16]bool{}
	for _, s := range task.Subdomains {
		subIDs[s.SubdomainID] = true
		if len(s.Addrs) != 1 || s.Addrs[0] != testwebIP {
			t.Errorf("subdomain %d (%q): addresses %v, want [%s]", s.SubdomainID, s.Label, s.Addrs, testwebIP)
		}
		if s.CNAME != "" {
			t.Errorf("subdomain %d: an unexpected CNAME %q", s.SubdomainID, s.CNAME)
		}
	}
	if len(subIDs) != 2 || !subIDs[0] || !subIDs[1] {
		t.Errorf("the task's subdomains: %v, want exactly the apex and www (only they have addresses)", subIDs)
	}

	// The q:dns delivery is acknowledged after writing (Processing, T-025):
	// the PEL is empty, q:dead is empty.
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("unacked q:dns: %d, want 0 (XACK after the DB write)", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("entries in q:dead: %d, want 0", n)
	}

	// Graceful shutdown: canceling ctx — a clean nil (the OnRun contract, T-012).
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run after the ctx cancel: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("the worker's loop did not stop after the ctx cancel")
	}
}

// TestDNSWorker_InvalidDomainStatus — the terminal diagnosis of a poison
// row (T-109, the DNS-03 residual): a domain with an empty name in a
// q:dns batch is recorded as invalid + done (instead of the eternal
// queued → lease-return → dispatch cycle), the delivery is acknowledged
// (not q:dead), and the recheck does not return the row (the status
// filter in the selection).
func TestDNSWorker_InvalidDomainStatus(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	resolve := dnsEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A junk row: the label is valid for insertion, but the name in the
	// batch message is whitespace — like the corrupted domains row (DNS-03).
	ids := insertPendingDomains(t, pool, "crawl.test", []string{"poison"})
	if len(ids) != 1 {
		t.Fatalf("domains inserted: %d, want 1", len(ids))
	}
	var zoneID int16
	if err := pool.QueryRow(ctx, "SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("zone crawl.test: %v", err)
	}

	if _, err := q.Publish(ctx, queue.StreamDNS, queue.DNSBatch{Domains: []queue.DNSDomain{
		{ZoneID: zoneID, DomainID: ids[0], Name: "   "},
	}}); err != nil {
		t.Fatalf("publishing q:dns: %v", err)
	}

	const group = "it-dns-invalid"
	runDone := make(chan error, 1)
	go func() {
		runDone <- dnsworker.Run(ctx, pool, q, resolve, dnsworker.Options{
			Group: group, Consumer: "it-invalid-1",
			Concurrency: 1, Consume: 1,
		})
	}()

	// The diagnosis is recorded: the row goes to done (the lease is
	// finished) instead of staying queued for infinite republishing.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var state int16
		if err := pool.QueryRow(ctx,
			"SELECT check_state FROM domains WHERE id = $1", ids[0],
		).Scan(&state); err != nil {
			t.Fatalf("reading the domain: %v", err)
		}
		if model.CheckState(state) == model.CheckStateDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the poison domain did not get a terminal diagnosis within 15 s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var (
		status   model.Status
		queuedAt *time.Time
	)
	if err := pool.QueryRow(ctx,
		"SELECT status, queued_at FROM domains WHERE id = $1", ids[0],
	).Scan(&status, &queuedAt); err != nil {
		t.Fatalf("reading the diagnosis: %v", err)
	}
	if status != model.StatusInvalid {
		t.Errorf("status = %s, want invalid (the terminal diagnosis T-109)", status)
	}
	if queuedAt != nil {
		t.Error("queued_at not cleared — the lease not finished by the diagnosis")
	}

	// The delivery is acknowledged after the write, not by quarantine.
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("unacked q:dns: %d, want 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("entries in q:dead: %d, want 0 (a diagnosis is not quarantine)", n)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run after the ctx cancel: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("the worker's loop did not stop after the ctx cancel")
	}

	// The recheck does not return invalid: both rows are aged to
	// 2000-01-01 — the temp_error control row moves to pending, the
	// poison one stays done (the status filter in the recheck selection).
	// The worker ctx is already canceled — the check operations run on
	// their own background.
	ageCtx := context.Background()
	if _, err := pool.Exec(ageCtx, "UPDATE domains SET last_checked_at = DATE '2000-01-01'"); err != nil {
		t.Fatalf("aging the rows: %v", err)
	}
	if _, err := pool.Exec(ageCtx,
		fmt.Sprintf("INSERT INTO domains (zone_id, label, status, check_state, last_checked_at) "+
			"VALUES ($1, 'control', %d, %d, DATE '2000-01-01')",
			int16(model.StatusTempError), int16(model.CheckStateDone)),
		zoneID); err != nil {
		t.Fatalf("inserting the control domain: %v", err)
	}
	res, err := scheduler.RecheckOnce(ageCtx, pool, scheduler.RecheckOptions{})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if res.Domains != 1 {
		t.Errorf("RecheckOnce moved %d domains, want 1 (only the control one)", res.Domains)
	}
	var poisonState, controlState int16
	if err := pool.QueryRow(ageCtx,
		"SELECT check_state FROM domains WHERE id = $1", ids[0]).Scan(&poisonState); err != nil {
		t.Fatalf("reading the poison one: %v", err)
	}
	if err := pool.QueryRow(ageCtx,
		"SELECT check_state FROM domains WHERE label = 'control' AND zone_id = $1", zoneID).Scan(&controlState); err != nil {
		t.Fatalf("reading the control one: %v", err)
	}
	if model.CheckState(poisonState) != model.CheckStateDone {
		t.Errorf("the poison one: check_state = %s, want done (the recheck does not return invalid)",
			model.CheckState(poisonState))
	}
	if model.CheckState(controlState) != model.CheckStatePending {
		t.Errorf("the control one: check_state = %s, want pending (regular done rows are returned)",
			model.CheckState(controlState))
	}
}

// TestDNSWorker_RedeliveryPublishedMarker — the semantics of redelivering
// a batch with the http_published staging marker (T-108, DNS-04): for
// domains whose q:http task was published by the previous attempt (the
// result write failed after publishing — the bit is set), the redelivery
// brings them to the write WITHOUT a q:http duplicate; done clears the
// bit. Failure emulation: the publishing and the marking are done by
// hand (the worker "died" between XADD and the write), the q:dns batch
// is delivered again.
func TestDNSWorker_RedeliveryPublishedMarker(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	resolve := dnsEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Two delegated bench domains (A at the apex and www — q:http tasks).
	labels := []string{"same-site", "alias-target"}
	ids := insertPendingDomains(t, pool, "crawl.test", labels)
	var zoneID int16
	if err := pool.QueryRow(ctx, "SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("zone crawl.test: %v", err)
	}

	// "The previous attempt": published q:http for both domains and set
	// the marker, but did not write the result (a crash between XADD and
	// the UPDATE-done).
	for i, label := range labels {
		name := label + ".crawl.test"
		if _, err := q.Publish(ctx, queue.StreamHTTP, queue.HTTPTask{
			ZoneID: zoneID, DomainID: ids[i], Name: name,
			Subdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Label: "", Addrs: []string{testwebIP}}},
		}); err != nil {
			t.Fatalf("publishing q:http (%s): %v", name, err)
		}
		if _, err := pool.Exec(ctx,
			fmt.Sprintf("UPDATE domains SET flags = flags | %d, check_state = %d WHERE id = $1",
				int16(model.FlagHTTPPublished), int16(model.CheckStateQueued)),
			ids[i]); err != nil {
			t.Fatalf("marking http_published (%s): %v", label, err)
		}
	}

	// Redelivering the same batch (XAUTOCLAIM would return it, T-025).
	if _, err := q.Publish(ctx, queue.StreamDNS, queue.DNSBatch{Domains: []queue.DNSDomain{
		{ZoneID: zoneID, DomainID: ids[0], Name: "same-site.crawl.test"},
		{ZoneID: zoneID, DomainID: ids[1], Name: "alias-target.crawl.test"},
	}}); err != nil {
		t.Fatalf("publishing q:dns: %v", err)
	}

	const group = "it-dns-marker"
	runDone := make(chan error, 1)
	go func() {
		runDone <- dnsworker.Run(ctx, pool, q, resolve, dnsworker.Options{
			Group: group, Consumer: "it-marker-1", Concurrency: 2, Consume: 1, AskAAAA: true,
		})
	}()

	// Both domains are written (done) — the redelivery brought the episodes to the write.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var done int
		if err := pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM domains WHERE check_state = %d", int16(model.CheckStateDone)),
		).Scan(&done); err != nil {
			t.Fatalf("counting done: %v", err)
		}
		if done == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the domains not recorded within 15 s — the batch repeat not finished")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run after the ctx cancel: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("the worker's loop did not stop after the ctx cancel")
	}

	// Checks after cancellation — on their own context: the test ctx is
	// already canceled, using it returned "context canceled".
	postCtx, postCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer postCancel()

	// Writing cleared the marker bit: flags no longer carries
	// http_published, the status/mask are written honestly (the verdict
	// belongs to the HTTP stage — new).
	for i, label := range labels {
		var (
			status model.Status
			flags  int16
			mask   int64
		)
		if err := pool.QueryRow(postCtx,
			"SELECT status, flags, dns_mask FROM domains WHERE id = $1", ids[i],
		).Scan(&status, &flags, &mask); err != nil {
			t.Fatalf("domain %s: %v", label, err)
		}
		if model.Flags(flags).IsHTTPPublished() {
			t.Errorf("%s: the http_published bit not cleared by the result write", label)
		}
		if status != model.StatusNew || mask != 0b11 {
			t.Errorf("%s: status=%s mask=%b, want new and bits 0,1 (apex+www)", label, status, mask)
		}
	}

	// No q:http duplicates: the stream has exactly the two entries of "the
	// previous attempt" — the redelivery did not publish the bit-set
	// domains. (The entries are read by a group without acknowledgment —
	// reading does not change the test's PEL.)
	httpMsgs := consumeTasks(t, q, queue.StreamHTTP, "it-http-marker", "op", 10)
	if len(httpMsgs) != 2 {
		t.Fatalf("entries in q:http: %d, want 2 (the repeat publishes no duplicates)", len(httpMsgs))
	}
	seen := map[string]int{}
	for _, m := range httpMsgs {
		task, err := queue.Decode[queue.HTTPTask](m)
		if err != nil {
			t.Fatalf("decoding q:http: %v", err)
		}
		seen[task.Name]++
	}
	for _, name := range []string{"same-site.crawl.test", "alias-target.crawl.test"} {
		if seen[name] != 1 {
			t.Errorf("q:http tasks for %s: %d, want exactly one", name, seen[name])
		}
	}

	// The q:dns delivery is acknowledged after the write (Processing, T-025).
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("unacked q:dns: %d, want 0", n)
	}
}

// TestDNSWorker_EnqueueClearsMarker — scheduler enqueueing (T-028)
// clears the http_published bit (T-108): returning a row to work (lease,
// revival, recheck) goes through pending → queued, and a fresh episode
// must start with a clean bit — otherwise the previous episode's marker
// would suppress publishing its task (a lost q:http).
func TestDNSWorker_EnqueueClearsMarker(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ids := insertPendingDomains(t, pool, "crawl.test", []string{"same-site"})
	var zoneID int16
	if err := pool.QueryRow(ctx, "SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("zone crawl.test: %v", err)
	}
	// The domain returned to pending with the bit still set (the extreme
	// failure path: the write never happened, the lease moved the row to
	// pending).
	if _, err := pool.Exec(ctx,
		fmt.Sprintf("UPDATE domains SET flags = flags | %d WHERE id = $1",
			int16(model.FlagHTTPPublished)),
		ids[0]); err != nil {
		t.Fatalf("marking http_published: %v", err)
	}

	res, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zoneID, DomainID: ids[0], Name: "same-site.crawl.test"},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if res.Queued != 1 || res.Published != 1 {
		t.Fatalf("EnqueueBatch = %+v, want queued=published=1", res)
	}

	var flags int16
	if err := pool.QueryRow(ctx, "SELECT flags FROM domains WHERE id = $1", ids[0]).Scan(&flags); err != nil {
		t.Fatalf("reading flags: %v", err)
	}
	if model.Flags(flags).IsHTTPPublished() {
		t.Error("the http_published bit survived the enqueue — a fresh episode would start with a suppressed publication")
	}
}
