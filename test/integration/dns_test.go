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
		t.Fatalf("dnsclient к %s: %v", unboundAddr, err)
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

	t.Run("обычный A: адрес одной попыткой без TCP", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "same-site.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if r.RCode != dnsclient.RCodeNOERROR {
			t.Errorf("rcode = %s, want NOERROR", r.RCode)
		}
		if r.TCP || r.Attempts != 1 {
			t.Errorf("tcp=%v attempts=%d, want false/1 (маленький ответ целиком в UDP)", r.TCP, r.Attempts)
		}
		as := r.RecordsOf(dns.TypeA)
		if len(as) != 1 {
			t.Fatalf("A-записей: %d, хочу 1", len(as))
		}
		if a, ok := as[0].(*dns.A); !ok || a.A.String() != testwebIP {
			t.Errorf("адрес = %v, хочу %s", as[0], testwebIP)
		}
	})

	t.Run("NXDOMAIN — код ответа, не ошибка; SOA в authority", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "nxdomain-case.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v (NXDOMAIN — не сетевая ошибка, контракт T-034)", err)
		}
		if r.RCode != dnsclient.RCodeNXDOMAIN {
			t.Errorf("rcode = %s, want NXDOMAIN", r.RCode)
		}
		if len(r.Answer) != 0 {
			t.Errorf("answer не пуст: %d записей", len(r.Answer))
		}
		if len(r.Ns) == 0 {
			t.Error("authority пуст — отрицательный ответ без SOA (RFC 2308), Ns нужен потребителю T-036")
		}
	})

	t.Run("NODATA: NOERROR без A-записей", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "delegated-no-address.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if r.RCode != dnsclient.RCodeNOERROR {
			t.Fatalf("rcode = %s, want NOERROR (имя есть, записей типа нет)", r.RCode)
		}
		if n := len(r.RecordsOf(dns.TypeA)); n != 0 {
			t.Errorf("A-записей: %d, хочу 0 (NODATA)", n)
		}
	})

	t.Run("SERVFAIL — код ответа, не ошибка", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "servfail.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v (SERVFAIL — не сетевая ошибка, контракт T-034)", err)
		}
		if r.RCode != dnsclient.RCodeSERVFAIL {
			t.Errorf("rcode = %s, want SERVFAIL", r.RCode)
		}
	})

	t.Run("TCP-fallback: TC=1 на большом ответе, полный набор по TCP", func(t *testing.T) {
		r, err := c.Exchange(ctx, dnsclient.Question{Name: "tc-fallback.crawl.test", Type: dns.TypeA})
		if err != nil {
			t.Fatalf("Exchange: %v", err)
		}
		if !r.TCP {
			t.Error("итог не по TCP: усечённый ответ (TC=1) не повлёк fallback (T-034)")
		}
		if r.Attempts != 2 {
			t.Errorf("attempts = %d, хочу 2 (UDP с TC=1 + повтор по TCP)", r.Attempts)
		}
		if r.RCode != dnsclient.RCodeNOERROR {
			t.Fatalf("rcode = %s, want NOERROR", r.RCode)
		}
		uniq := make(map[string]bool, tcFallbackRecords)
		for _, rr := range r.RecordsOf(dns.TypeA) {
			a, ok := rr.(*dns.A)
			if !ok {
				t.Fatalf("запись не A: %v", rr)
			}
			if a.A.Mask(net.CIDRMask(24, 32)).String() != "192.0.2.0" {
				t.Errorf("адрес %s вне блока сценария 192.0.2.0/24", a.A)
			}
			uniq[a.A.String()] = true
		}
		if len(uniq) != tcFallbackRecords {
			t.Errorf("уникальных A-адресов: %d, хочу %d (TCP-ответ не обрезан)", len(uniq), tcFallbackRecords)
		}
	})

	t.Run("мёртвый резолвер: ретраи исчерпаны, IsTimeout", func(t *testing.T) {
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
			t.Fatalf("клиент на чёрную дыру %s: %v", dnsBlackholeAddr, err)
		}
		defer func() { _ = dead.Close() }()

		start := time.Now()
		_, err = dead.Exchange(ctx, dnsclient.Question{Name: "same-site.crawl.test", Type: dns.TypeA})
		if err == nil {
			t.Fatal("мёртвый резолвер вернул ответ")
		}
		if !dnsclient.IsTimeout(err) {
			t.Errorf("ошибка не таймаут (адаптив T-035 различает ветки): %v", err)
		}
		if el := time.Since(start); el > 3*time.Second {
			t.Errorf("ретраи длились %v — дольше двух попыток с паузой", el)
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
			name: "обычный домен", domain: "same-site.crawl.test",
			status: dnsworker.ApexDelegated,
			addrs:  []netip.Addr{netip.MustParseAddr(testwebIP)},
		},
		{
			name: "wildcard-зона: явный A апекса", domain: "wildcard.crawl.test",
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
			name: "мёртвый апстрим зоны (таймаут/SERVFAIL) → temp_error", domain: "dns-timeout.crawl.test",
			status: dnsworker.ApexTempError, stop: true,
			domStatus: model.StatusTempError, domOK: true,
		},
		{
			name: "NODATA + NS → ветка без адресов", domain: "delegated-no-address.crawl.test",
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
				t.Errorf("статус = %s, хочу %s", res.Status, tc.status)
			}
			if res.Status.Stop() != tc.stop {
				t.Errorf("Stop = %v, хочу %v (ветка %s)", res.Status.Stop(), tc.stop, res.Status)
			}
			if st, ok := res.Status.DomainStatus(); st != tc.domStatus || ok != tc.domOK {
				t.Errorf("DomainStatus = (%s, %v), хочу (%s, %v)", st, ok, tc.domStatus, tc.domOK)
			}
			if res.NSQueried != tc.nsQueried {
				t.Errorf("NSQueried = %v, хочу %v", res.NSQueried, tc.nsQueried)
			}
			if len(res.Addresses) != len(tc.addrs) {
				t.Fatalf("адресов: %v, хочу %v", res.Addresses, tc.addrs)
			}
			for i, a := range res.Addresses {
				if a != tc.addrs[i] {
					t.Errorf("адрес[%d] = %s, хочу %s", i, a, tc.addrs[i])
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

	t.Run("обычный домен: апекс и www резолвятся, admin NXDOMAIN", func(t *testing.T) {
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
				t.Errorf("метка %q: A = %v, хочу [%s]", label, r.A, testwebIP)
			}
			if r.Err != nil || r.RCode != dnsclient.RCodeNOERROR {
				t.Errorf("метка %q: err=%v rcode=%s — легальный ответ не ошибка", label, r.Err, r.RCode)
			}
			if len(r.Chain) != 0 || r.CNAME() != "" {
				t.Errorf("метка %q: неожиданная CNAME-цепочка %v", label, r.Chain)
			}
			// AAAA was asked (askAAAA=true): the bench zone has no AAAA —
			// NODATA does not fail a subdomain with found A records.
			if len(r.AAAA) != 0 {
				t.Errorf("метка %q: AAAA = %v, хочу пусто (NODATA)", label, r.AAAA)
			}
		}
		admin := byLabel["admin"]
		if admin.RCode != dnsclient.RCodeNXDOMAIN || admin.HasAddrs() || admin.Err != nil {
			t.Errorf("admin: rcode=%s addrs=%v err=%v, хочу NXDOMAIN без адресов и без ошибки",
				admin.RCode, admin.A, admin.Err)
		}
		if !rep.Mask.Test(0) || !rep.Mask.Test(1) || rep.Mask.Test(2) {
			t.Errorf("маска = %b: биты 0,1 резолвящихся, бит 2 нет (ТЗ 5.4 п. 4)", int64(rep.Mask))
		}
	})

	t.Run("CNAME-цепочка разворачивается до конечных адресов", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err := dnsworker.CheckSubdomains(ctx, resolve, "cname-chain.crawl.test", dnsTestSubs(), false)
		if err != nil {
			t.Fatalf("CheckSubdomains: %v", err)
		}
		if len(rep.Results) != 3 {
			t.Fatalf("результатов: %d, хочу 3", len(rep.Results))
		}
		apex := rep.Results[0] // input order: "" is first
		if want := "alias-target.crawl.test."; apex.CNAME() != want || apex.Target() != want {
			t.Errorf("цепочка апекса: cname=%q target=%q, хочу %q", apex.CNAME(), apex.Target(), want)
		}
		if len(apex.A) != 1 || apex.A[0].String() != testwebIP {
			t.Errorf("адреса цепочки: %v, хочу [%s] (рекурсор приносит целевые A)", apex.A, testwebIP)
		}
		if !rep.Mask.Test(0) || rep.Mask.Test(1) || rep.Mask.Test(2) {
			t.Errorf("маска = %b: только бит 0 — www/admin цепочки не резолвятся", int64(rep.Mask))
		}
	})

	t.Run("wildcard-зона: все метки синтезируются одним адресом", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err := dnsworker.CheckSubdomains(ctx, resolve, "wildcard.crawl.test", dnsTestSubs(), false)
		if err != nil {
			t.Fatalf("CheckSubdomains: %v", err)
		}
		for _, r := range rep.Results {
			if len(r.A) != 1 || r.A[0].String() != testwebIP {
				t.Errorf("метка %q: A = %v, хочу [%s] (*.wildcard)", r.Label, r.A, testwebIP)
			}
		}
		for _, id := range []int{0, 1, 2} {
			if !rep.Mask.Test(id) {
				t.Errorf("бит %d не установлен — wildcard-имена резолвятся (в маске, ТЗ 5.4 п. 4)", id)
			}
		}
	})

	t.Run("зацикленная CNAME: резолвер отвечает SERVFAIL, субдомен без адресов", func(t *testing.T) {
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
			t.Errorf("апекс: rcode=%s addrs=%v err=%v, хочу SERVFAIL без адресов и ошибки",
				r.RCode, r.A, r.Err)
		}
		if rep.Mask.Test(0) {
			t.Error("бит апекса установлен без адресов")
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

	t.Run("wildcard-зона: случайная метка получает адреса", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		info, err := dnsworker.DetectWildcard(ctx, resolve, "wildcard.crawl.test")
		if err != nil {
			t.Fatalf("DetectWildcard: %v", err)
		}
		if !info.Detected {
			t.Fatal("wildcard не детектирован: *.wildcard синтезирует A для любой метки")
		}
		if len(info.Addrs) != 1 || !info.Addrs[0].Equal(net.ParseIP(testwebIP)) {
			t.Errorf("адреса wildcard = %v, хочу [%s]", info.Addrs, testwebIP)
		}
		if len(info.Label) != 16 {
			t.Errorf("метка детекта %q: длина %d, хочу 16 (crypto/rand)", info.Label, len(info.Label))
		}
	})

	t.Run("обычная зона: детект-отрицание без ошибки", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		info, err := dnsworker.DetectWildcard(ctx, resolve, "same-site.crawl.test")
		if err != nil {
			t.Fatalf("DetectWildcard: %v (NXDOMAIN случайной метки — авторитетное «нет»)", err)
		}
		if info.Detected || len(info.Addrs) != 0 {
			t.Errorf("итог = %+v, хочу Detected=false без адресов", info)
		}
	})

	t.Run("пометка: синтезированные субдомены помечены, апекс и www — нет", func(t *testing.T) {
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
			t.Fatalf("помечены %v, хочу %v (апекс и www не помечаются никогда)", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("помечены %v, хочу %v", got, want)
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
		t.Fatalf("вставлено доменов: %d, хочу 2", len(ids))
	}
	var zoneID int16
	if err := pool.QueryRow(ctx, "SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("зона crawl.test: %v", err)
	}

	// Publishing the q:dns batch — as the T-028 scheduler does.
	if _, err := q.Publish(ctx, queue.StreamDNS, queue.DNSBatch{Domains: []queue.DNSDomain{
		{ZoneID: zoneID, DomainID: ids[0], Name: "same-site.crawl.test"},
		{ZoneID: zoneID, DomainID: ids[1], Name: "nxdomain-case.crawl.test"},
	}}); err != nil {
		t.Fatalf("публикация q:dns: %v", err)
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
			t.Fatalf("подсчёт done: %v", err)
		}
		if done == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("домены не переведены в done за 15 с — цикл воркера не обработал пачку")
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
			t.Fatalf("домен %s: %v", label, err)
		}
		rows[label] = r
	}
	if r := rows["same-site"]; r.status != model.StatusNew || r.mask != 0b11 {
		t.Errorf("same-site: status=%s mask=%b, хочу new и биты 0,1 (апекс+www)", r.status, r.mask)
	}
	if r := rows["nxdomain-case"]; r.status != model.StatusNotDelegated || r.mask != 0 {
		t.Errorf("nxdomain-case: status=%s mask=%b, хочу not_delegated и пустую маску", r.status, r.mask)
	}
	for label, r := range rows {
		if r.queuedAt != nil {
			t.Errorf("%s: queued_at не сброшен по завершении lease", label)
		}
		if r.lastAt == nil {
			t.Errorf("%s: last_checked_at не записан", label)
		}
	}

	// The q:http task — exactly one, for the delegated domain: DNS-alive
	// subdomains with addresses (apex and www), the NXDOMAIN domain gets
	// no task.
	httpMsgs := consumeTasks(t, q, queue.StreamHTTP, "it-http-e2e", "op", 10)
	if len(httpMsgs) != 1 {
		t.Fatalf("задач в q:http: %d, хочу 1 (NXDOMAIN-домен без задачи)", len(httpMsgs))
	}
	task, err := queue.Decode[queue.HTTPTask](httpMsgs[0])
	if err != nil {
		t.Fatalf("декодирование q:http: %v", err)
	}
	if task.Name != "same-site.crawl.test" || task.ZoneID != zoneID || task.DomainID != ids[0] {
		t.Errorf("задача q:http: name=%s zone=%d domain=%d", task.Name, task.ZoneID, task.DomainID)
	}
	subIDs := map[int16]bool{}
	for _, s := range task.Subdomains {
		subIDs[s.SubdomainID] = true
		if len(s.Addrs) != 1 || s.Addrs[0] != testwebIP {
			t.Errorf("субдомен %d (%q): адреса %v, хочу [%s]", s.SubdomainID, s.Label, s.Addrs, testwebIP)
		}
		if s.CNAME != "" {
			t.Errorf("субдомен %d: неожиданный CNAME %q", s.SubdomainID, s.CNAME)
		}
	}
	if len(subIDs) != 2 || !subIDs[0] || !subIDs[1] {
		t.Errorf("субдомены задачи: %v, хочу ровно апекс и www (адреса есть только у них)", subIDs)
	}

	// The q:dns delivery is acknowledged after writing (Processing, T-025):
	// the PEL is empty, q:dead is empty.
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("неподтверждённых q:dns: %d, хочу 0 (XACK после записи в БД)", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("записей в q:dead: %d, хочу 0", n)
	}

	// Graceful shutdown: canceling ctx — a clean nil (the OnRun contract, T-012).
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run после отмены ctx: %v, хочу nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("цикл воркера не остановился после отмены ctx")
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
		t.Fatalf("вставлено доменов: %d, хочу 1", len(ids))
	}
	var zoneID int16
	if err := pool.QueryRow(ctx, "SELECT id FROM zones WHERE name = $1", "crawl.test").Scan(&zoneID); err != nil {
		t.Fatalf("зона crawl.test: %v", err)
	}

	if _, err := q.Publish(ctx, queue.StreamDNS, queue.DNSBatch{Domains: []queue.DNSDomain{
		{ZoneID: zoneID, DomainID: ids[0], Name: "   "},
	}}); err != nil {
		t.Fatalf("публикация q:dns: %v", err)
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
			t.Fatalf("чтение домена: %v", err)
		}
		if model.CheckState(state) == model.CheckStateDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ядовитый домен не получил терминальный диагноз за 15 с")
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
		t.Fatalf("чтение диагноза: %v", err)
	}
	if status != model.StatusInvalid {
		t.Errorf("статус = %s, хочу invalid (терминальный диагноз T-109)", status)
	}
	if queuedAt != nil {
		t.Error("queued_at не сброшен — lease не завершён диагнозом")
	}

	// The delivery is acknowledged after the write, not by quarantine.
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("неподтверждённых q:dns: %d, хочу 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("записей в q:dead: %d, хочу 0 (диагноз — не карантин)", n)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run после отмены ctx: %v, хочу nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("цикл воркера не остановился после отмены ctx")
	}

	// The recheck does not return invalid: both rows are aged to
	// 2000-01-01 — the temp_error control row moves to pending, the
	// poison one stays done (the status filter in the recheck selection).
	// The worker ctx is already canceled — the check operations run on
	// their own background.
	ageCtx := context.Background()
	if _, err := pool.Exec(ageCtx, "UPDATE domains SET last_checked_at = DATE '2000-01-01'"); err != nil {
		t.Fatalf("старение строк: %v", err)
	}
	if _, err := pool.Exec(ageCtx,
		fmt.Sprintf("INSERT INTO domains (zone_id, label, status, check_state, last_checked_at) "+
			"VALUES ($1, 'control', %d, %d, DATE '2000-01-01')",
			int16(model.StatusTempError), int16(model.CheckStateDone)),
		zoneID); err != nil {
		t.Fatalf("вставка контрольного домена: %v", err)
	}
	res, err := scheduler.RecheckOnce(ageCtx, pool, scheduler.RecheckOptions{})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if res.Domains != 1 {
		t.Errorf("RecheckOnce перевёл %d доменов, хочу 1 (только контрольный)", res.Domains)
	}
	var poisonState, controlState int16
	if err := pool.QueryRow(ageCtx,
		"SELECT check_state FROM domains WHERE id = $1", ids[0]).Scan(&poisonState); err != nil {
		t.Fatalf("чтение ядовитого: %v", err)
	}
	if err := pool.QueryRow(ageCtx,
		"SELECT check_state FROM domains WHERE label = 'control' AND zone_id = $1", zoneID).Scan(&controlState); err != nil {
		t.Fatalf("чтение контрольного: %v", err)
	}
	if model.CheckState(poisonState) != model.CheckStateDone {
		t.Errorf("ядовитый: check_state = %s, хочу done (recheck не возвращает invalid)",
			model.CheckState(poisonState))
	}
	if model.CheckState(controlState) != model.CheckStatePending {
		t.Errorf("контрольный: check_state = %s, хочу pending (обычные done-строки возвращаются)",
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
		t.Fatalf("зона crawl.test: %v", err)
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
			t.Fatalf("публикация q:http (%s): %v", name, err)
		}
		if _, err := pool.Exec(ctx,
			fmt.Sprintf("UPDATE domains SET flags = flags | %d, check_state = %d WHERE id = $1",
				int16(model.FlagHTTPPublished), int16(model.CheckStateQueued)),
			ids[i]); err != nil {
			t.Fatalf("пометка http_published (%s): %v", label, err)
		}
	}

	// Redelivering the same batch (XAUTOCLAIM would return it, T-025).
	if _, err := q.Publish(ctx, queue.StreamDNS, queue.DNSBatch{Domains: []queue.DNSDomain{
		{ZoneID: zoneID, DomainID: ids[0], Name: "same-site.crawl.test"},
		{ZoneID: zoneID, DomainID: ids[1], Name: "alias-target.crawl.test"},
	}}); err != nil {
		t.Fatalf("публикация q:dns: %v", err)
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
			t.Fatalf("подсчёт done: %v", err)
		}
		if done == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("домены не записаны за 15 с — повтор пачки не дообработан")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run после отмены ctx: %v, хочу nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("цикл воркера не остановился после отмены ctx")
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
			t.Fatalf("домен %s: %v", label, err)
		}
		if model.Flags(flags).IsHTTPPublished() {
			t.Errorf("%s: бит http_published не снят записью результата", label)
		}
		if status != model.StatusNew || mask != 0b11 {
			t.Errorf("%s: status=%s mask=%b, хочу new и биты 0,1 (апекс+www)", label, status, mask)
		}
	}

	// No q:http duplicates: the stream has exactly the two entries of "the
	// previous attempt" — the redelivery did not publish the bit-set
	// domains. (The entries are read by a group without acknowledgment —
	// reading does not change the test's PEL.)
	httpMsgs := consumeTasks(t, q, queue.StreamHTTP, "it-http-marker", "op", 10)
	if len(httpMsgs) != 2 {
		t.Fatalf("записей в q:http: %d, хочу 2 (повтор не публикует дублей)", len(httpMsgs))
	}
	seen := map[string]int{}
	for _, m := range httpMsgs {
		task, err := queue.Decode[queue.HTTPTask](m)
		if err != nil {
			t.Fatalf("декодирование q:http: %v", err)
		}
		seen[task.Name]++
	}
	for _, name := range []string{"same-site.crawl.test", "alias-target.crawl.test"} {
		if seen[name] != 1 {
			t.Errorf("задач q:http для %s: %d, хочу ровно одну", name, seen[name])
		}
	}

	// The q:dns delivery is acknowledged after the write (Processing, T-025).
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("неподтверждённых q:dns: %d, хочу 0", n)
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
		t.Fatalf("зона crawl.test: %v", err)
	}
	// The domain returned to pending with the bit still set (the extreme
	// failure path: the write never happened, the lease moved the row to
	// pending).
	if _, err := pool.Exec(ctx,
		fmt.Sprintf("UPDATE domains SET flags = flags | %d WHERE id = $1",
			int16(model.FlagHTTPPublished)),
		ids[0]); err != nil {
		t.Fatalf("пометка http_published: %v", err)
	}

	res, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zoneID, DomainID: ids[0], Name: "same-site.crawl.test"},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if res.Queued != 1 || res.Published != 1 {
		t.Fatalf("EnqueueBatch = %+v, хочу queued=published=1", res)
	}

	var flags int16
	if err := pool.QueryRow(ctx, "SELECT flags FROM domains WHERE id = $1", ids[0]).Scan(&flags); err != nil {
		t.Fatalf("чтение flags: %v", err)
	}
	if model.Flags(flags).IsHTTPPublished() {
		t.Error("бит http_published пережил постановку — свежий эпизод начнётся с подавленной публикацией")
	}
}
