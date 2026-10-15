// testbedgen — генератор тестового стенда (ТЗ 4.4).
//
// Читает deploy/docker/testbed/scenarios.yaml и testdata/laravel-samples/,
// генерирует:
//
//	deploy/docker/testbed/coredns/zones/db.<zone>  — файлы DNS-зон
//	deploy/docker/testbed/coredns/zones.conf        — серверные блоки CoreDNS
//	deploy/docker/testbed/web/Caddyfile             — виртуальные хосты Caddy
//	deploy/docker/testbed/web/sites/<сценарий>/     — статические фикстуры
//
// Запуск: make testbed-gen
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/net/idna"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------- yaml-схема

type Network struct {
	TestwebIP    string `yaml:"testweb_ip"`
	MockserverIP string `yaml:"mockserver_ip"`
	BlackholeIP  string `yaml:"blackhole_ip"`
	CoreDNSIP    string `yaml:"coredns_ip"`
	Serial       int    `yaml:"serial"`
}

type Config struct {
	Network   Network    `yaml:"network"`
	Zones     []string   `yaml:"zones"`
	Scenarios []Scenario `yaml:"scenarios"`
	Plain     []PlainDom `yaml:"plain"`
}

type DNSCfg struct {
	A         string `yaml:"a"`          // testweb | mockserver | blackhole | ""
	WWW       bool   `yaml:"www"`        // создать www.<name>
	NS        bool   `yaml:"ns"`         // NS без адреса (delegated_no_address)
	Wildcard  bool   `yaml:"wildcard"`   // *.<name>
	CNAME     string `yaml:"cname"`      // CNAME на <cname>.<zone>
	CNAMELoop bool   `yaml:"cname_loop"` // зацикленная CNAME-цепочка
	Special   string `yaml:"special"`    // servfail | timeout
}

type SiteCfg struct {
	Template     string `yaml:"template"` // generic|parked|redirect-chain|meta-refresh|js-redirect|spa|mock
	Scheme       string `yaml:"scheme"`   // both|http-only|https-only
	HTTP         string `yaml:"http"`     // redirect-to-https|different (при scheme=both)
	ApexRedirect bool   `yaml:"apex_redirect_to_www"`
	WWWRedirect  bool   `yaml:"www_redirect_to_apex"`
	WWWSame      bool   `yaml:"www_same_as_apex"`
}

type Scenario struct {
	Name string  `yaml:"name"`
	Zone string  `yaml:"zone"` // базовая зона, по умолчанию первая из zones
	DNS  DNSCfg  `yaml:"dns"`
	Site SiteCfg `yaml:"site"`
	// Expect — документация ожидаемого результата (для интеграционных тестов).
	Expect string `yaml:"expect"`
}

type PlainDom struct {
	Zone  string `yaml:"zone"`
	Names []struct {
		Label string `yaml:"label"`
		WWW   bool   `yaml:"www"`
	} `yaml:"names"`
}

// ---------------------------------------------------------------- генератор

type zoneData struct {
	name    string // punycode
	records []string
}

type generator struct {
	cfg     Config
	zones   map[string]*zoneData // ключ — punycode-зона
	blocks  []string             // дополнительные серверные блоки CoreDNS
	caddy   strings.Builder      // тело Caddyfile
	baseDir string
}

func main() {
	root := flag.String("root", ".", "корень репозитория")
	flag.Parse()

	cfgBytes, err := os.ReadFile(filepath.Join(*root, "deploy/docker/testbed/scenarios.yaml"))
	if err != nil {
		log.Fatalf("testbedgen: %v", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(cfgBytes, &cfg); err != nil {
		log.Fatalf("testbedgen: scenarios.yaml: %v", err)
	}
	if len(cfg.Zones) == 0 {
		log.Fatal("testbedgen: не задано ни одной зоны")
	}

	g := &generator{cfg: cfg, zones: map[string]*zoneData{}, baseDir: *root}
	g.run()
}

func (g *generator) run() {
	net := g.cfg.Network
	if net.TestwebIP == "" {
		net.TestwebIP = "172.28.100.12"
	}
	if net.MockserverIP == "" {
		net.MockserverIP = "172.28.100.13"
	}
	if net.BlackholeIP == "" {
		net.BlackholeIP = "172.28.100.250"
	}
	if net.CoreDNSIP == "" {
		net.CoreDNSIP = "172.28.100.11"
	}
	if net.Serial == 0 {
		net.Serial = 1
	}
	g.cfg.Network = net

	baseZone := puny(g.cfg.Zones[0])

	for _, z := range g.cfg.Zones {
		pz := puny(z)
		g.zones[pz] = &zoneData{name: pz}
	}

	sort.Slice(g.cfg.Scenarios, func(i, j int) bool { return g.cfg.Scenarios[i].Name < g.cfg.Scenarios[j].Name })
	for _, sc := range g.cfg.Scenarios {
		zone := sc.Zone
		if zone == "" {
			zone = baseZone
		}
		g.addScenario(puny(zone), sc)
	}

	for _, p := range g.cfg.Plain {
		g.addPlain(puny(p.Zone), p)
	}

	g.addSamples(baseZone)

	g.writeZones()
	g.writeZonesConf()
	g.writeCaddyfile()
	fmt.Printf("testbedgen: готово: зон=%d, сценариев=%d\n", len(g.zones), len(g.cfg.Scenarios))
}

// ---------------------------------------------------------------- сценарии

func (g *generator) addScenario(zone string, sc Scenario) {
	net := g.cfg.Network
	fqdn := sc.Name + "." + zone
	www := "www." + fqdn

	switch sc.DNS.Special {
	case "servfail":
		g.blocks = append(g.blocks, fmt.Sprintf(`
%s:53 {
    errors
    template IN A %s {
        rcode SERVFAIL
    }
    template IN AAAA %s {
        rcode SERVFAIL
    }
}
`, fqdn, fqdn, fqdn))
		return
	case "timeout":
		g.blocks = append(g.blocks, fmt.Sprintf(`
%s:53 {
    errors
    forward . %s
}
`, fqdn, net.BlackholeIP))
		return
	}

	z := g.zone(zone)
	if ip := ipFor(sc.DNS.A, net); ip != "" {
		z.add("%s IN A %s", sc.Name, ip)
		if sc.DNS.WWW {
			z.add("www.%s IN A %s", sc.Name, ip)
		}
	}

	if sc.DNS.NS {
		z.add("%s IN NS ns.%s.", sc.Name, zone)
	}
	if sc.DNS.Wildcard {
		z.add("*.%s IN A %s", sc.Name, net.TestwebIP)
	}
	if sc.DNS.CNAME != "" {
		z.add("%s IN CNAME %s.%s.", sc.Name, sc.DNS.CNAME, zone)
	}
	if sc.DNS.CNAMELoop {
		z.add("%s IN CNAME %s-loop-b.%s.", sc.Name, sc.Name, zone)
		z.add("%s-loop-b IN CNAME %s.%s.", sc.Name, sc.Name, zone)
	}

	// Сайт генерируем, только если трафик действительно попадает на testweb.
	if ipFor(sc.DNS.A, net) == net.TestwebIP || sc.DNS.CNAME != "" || sc.DNS.Wildcard {
		g.addSite(sc, zone, fqdn, www)
	}
}

func ipFor(target string, net Network) string {
	switch target {
	case "testweb":
		return net.TestwebIP
	case "mockserver":
		return net.MockserverIP
	case "blackhole":
		return net.BlackholeIP
	default:
		return ""
	}
}

// addPlain добавляет простые домены (без «злых» сценариев) в дополнительные зоны.
func (g *generator) addPlain(zone string, p PlainDom) {
	z := g.zone(zone)
	for _, n := range p.Names {
		// Метка может быть в unicode — приводим полное имя к punycode.
		full := puny(n.Label + "." + zone)
		label := strings.TrimSuffix(full, "."+zone)
		z.add("%s IN A %s", label, g.cfg.Network.TestwebIP)

		siteDir := filepath.Join(g.baseDir, "deploy/docker/testbed/web/sites", zone, label)
		g.writeFixture(siteDir, "index.html", genericPage(full))

		serveBody := fmt.Sprintf("    root * /srv/sites/%s/%s\n    file_server", zone, label)
		httpHosts := "http://" + full
		httpsHosts := "https://" + full
		if n.WWW {
			z.add("www.%s IN A %s", label, g.cfg.Network.TestwebIP)
			httpHosts += ", http://www." + full
			httpsHosts += ", https://www." + full
		}
		g.caddySiteBlock(httpHosts, "", serveBody)
		g.caddySiteBlock(httpsHosts, "    tls internal\n", serveBody)
	}
}

// addSamples подключает образцы Laravel из testdata/laravel-samples (ТЗ 4.4.2).
func (g *generator) addSamples(baseZone string) {
	samplesDir := filepath.Join(g.baseDir, "testdata/laravel-samples")
	entries, err := os.ReadDir(samplesDir)
	if err != nil {
		log.Printf("testbedgen: каталог образцов не найден (%v) — пропускаю", err)
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		metaPath := filepath.Join(samplesDir, id, "meta.yaml")
		if _, err := os.Stat(metaPath); err != nil {
			continue
		}
		fqdn := id + "." + baseZone
		g.zone(baseZone).add("%s IN A %s", id, g.cfg.Network.TestwebIP)

		// headers.json — объект имя -> массив значений (в т.ч. Set-Cookie).
		var hdrs map[string][]string
		if raw, err := os.ReadFile(filepath.Join(samplesDir, id, "headers.json")); err == nil {
			_ = yaml.Unmarshal(raw, &hdrs)
		}

		var body strings.Builder
		for _, name := range sortedHeaderNames(hdrs) {
			for _, v := range hdrs[name] {
				fmt.Fprintf(&body, "    header +%s \"%s\"\n", name, caddyEscape(v))
			}
		}
		fmt.Fprintf(&body, "    root * /srv/samples/%s\n    file_server", id)
		fmt.Fprintf(&g.caddy, "(sample_%s) {\n%s\n}\n\n", id, body.String())
		g.caddySiteBlock("http://"+fqdn, "", "    import sample_"+id)
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", "    import sample_"+id)
	}
}

// addSite генерирует Caddy-блоки и фикстуры по описанию сайта сценария.
func (g *generator) addSite(sc Scenario, zone, fqdn, www string) {
	_ = zone
	s := sc.Site
	sitesDir := filepath.Join(g.baseDir, "deploy/docker/testbed/web/sites", sc.Name)

	switch s.Template {
	case "parked":
		// Идентичное содержимое для parked-1/parked-2 — дедупликация по хэшу.
		g.writeFixture(sitesDir, "index.html", parkedPage())
		g.serveBlocks(sc, fqdn, www, fmt.Sprintf("root * /srv/sites/%s\nfile_server", sc.Name))

	case "redirect-chain":
		g.writeFixture(sitesDir, "final.html", genericPage(fqdn+" — финал цепочки"))
		body := "    redir / /r1 307\n    redir /r1 /r2 307\n    redir /r2 /r3 307\n    redir /r3 /final.html 307\n" +
			"    root * /srv/sites/" + sc.Name + "\n    file_server"
		fmt.Fprintf(&g.caddy, "(chain_%s) {\n%s\n}\n\n", sc.Name, body)
		g.caddySiteBlock("http://"+fqdn, "", "    import chain_"+sc.Name)
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", "    import chain_"+sc.Name)

	case "meta-refresh":
		g.writeFixture(sitesDir, "index.html", metaRefreshPage(fqdn, "final.html"))
		g.writeFixture(sitesDir, "final.html", genericPage(fqdn+" — финал meta refresh"))
		g.serveBlocks(sc, fqdn, www, fmt.Sprintf("root * /srv/sites/%s\nfile_server", sc.Name))

	case "js-redirect":
		g.writeFixture(sitesDir, "index.html", jsRedirectPage(fqdn, "final.html"))
		g.writeFixture(sitesDir, "final.html", genericPage(fqdn+" — финал js redirect"))
		g.serveBlocks(sc, fqdn, www, fmt.Sprintf("root * /srv/sites/%s\nfile_server", sc.Name))

	case "spa":
		g.writeFixture(sitesDir, "index.html", spaPage(fqdn))
		g.serveBlocks(sc, fqdn, www, fmt.Sprintf("root * /srv/sites/%s\nfile_server", sc.Name))

	default: // generic
		g.writeFixture(sitesDir, "index.html", genericPage(fqdn))
		g.serveBlocks(sc, fqdn, www, fmt.Sprintf("root * /srv/sites/%s\nfile_server", sc.Name))
	}
}

// serveBlocks — стандартные комбинации http/https-блоков сценария.
func (g *generator) serveBlocks(sc Scenario, fqdn, www, serveBody string) {
	s := sc.Site

	switch {
	case s.ApexRedirect: // bare -> www: alias_kind = 2, одно хранение контента
		g.caddySiteBlock("http://"+fqdn, "", "    redir https://"+www+"{uri} permanent")
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", "    redir https://"+www+"{uri} permanent")
		g.caddySiteBlock("http://"+www, "", serveBody)
		g.caddySiteBlock("https://"+www, "    tls internal\n", serveBody)

	case s.WWWRedirect: // www -> bare
		g.caddySiteBlock("http://"+www, "", "    redir https://"+fqdn+"{uri} permanent")
		g.caddySiteBlock("https://"+www, "    tls internal\n", "    redir https://"+fqdn+"{uri} permanent")
		g.caddySiteBlock("http://"+fqdn, "", serveBody)
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", serveBody)

	case s.WWWSame: // одинаковый контент без редиректа: alias_kind = 1
		g.caddySiteBlock("http://"+fqdn+", http://"+www, "", serveBody)
		g.caddySiteBlock("https://"+fqdn+", https://"+www, "    tls internal\n", serveBody)

	case s.Scheme == "http-only":
		g.caddySiteBlock("http://"+fqdn, "", serveBody)

	case s.Scheme == "https-only":
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", serveBody)

	case s.HTTP == "redirect-to-https": // один сайт: единственная запись, variant = 0
		g.caddySiteBlock("http://"+fqdn, "", "    redir https://"+fqdn+"{uri} permanent")
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", serveBody)

	case s.HTTP == "different": // http и https — разные сайты: variant 0 и 1
		sitesDir := filepath.Join(g.baseDir, "deploy/docker/testbed/web/sites", sc.Name)
		g.writeFixture(filepath.Join(sitesDir, "http"), "index.html", genericPage(fqdn+" — HTTP-версия"))
		g.writeFixture(filepath.Join(sitesDir, "https"), "index.html", genericPage(fqdn+" — HTTPS-версия"))
		g.caddySiteBlock("http://"+fqdn, "", fmt.Sprintf("root * /srv/sites/%s/http\nfile_server", sc.Name))
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", fmt.Sprintf("root * /srv/sites/%s/https\nfile_server", sc.Name))

	default: // оба протокола, одинаковый контент, без редиректа
		g.caddySiteBlock("http://"+fqdn, "", serveBody)
		g.caddySiteBlock("https://"+fqdn, "    tls internal\n", serveBody)
	}
}

func (g *generator) caddySiteBlock(addresses, prefix, body string) {
	fmt.Fprintf(&g.caddy, "%s {\n%s%s\n}\n\n", addresses, prefix, body)
}

// ---------------------------------------------------------------- зоны

func (g *generator) zone(name string) *zoneData {
	z, ok := g.zones[name]
	if !ok {
		z = &zoneData{name: name}
		g.zones[name] = z
	}
	return z
}

func (z *zoneData) add(format string, args ...any) {
	z.records = append(z.records, fmt.Sprintf(format, args...))
}

func (g *generator) writeZones() {
	net := g.cfg.Network
	zonesDir := filepath.Join(g.baseDir, "deploy/docker/testbed/coredns/zones")
	if err := os.MkdirAll(zonesDir, 0o755); err != nil {
		log.Fatal(err)
	}
	for _, z := range g.zones {
		sort.Strings(z.records)
		var b strings.Builder
		fmt.Fprintf(&b, "$ORIGIN %s.\n", z.name)
		fmt.Fprintf(&b, "@ 3600 IN SOA ns.%s. hostmaster.%s. ( %d 3600 600 86400 300 )\n", z.name, z.name, net.Serial)
		fmt.Fprintf(&b, "@ IN NS ns.%s.\n", z.name)
		fmt.Fprintf(&b, "ns IN A %s\n\n", net.CoreDNSIP)
		for _, r := range z.records {
			b.WriteString(r)
			b.WriteString("\n")
		}
		if err := os.WriteFile(filepath.Join(zonesDir, "db."+z.name), []byte(b.String()), 0o644); err != nil {
			log.Fatal(err)
		}
	}
}

func (g *generator) writeZonesConf() {
	var b strings.Builder
	b.WriteString("# Сгенерировано testbedgen (make testbed-gen). Не редактировать вручную.\n")
	for _, z := range sortedZoneNames(g.zones) {
		fmt.Fprintf(&b, "\n%s:53 {\n    errors\n    log\n    file /zones/db.%s %s\n}\n", z, z, z)
	}
	for _, extra := range g.blocks {
		b.WriteString(extra)
	}
	path := filepath.Join(g.baseDir, "deploy/docker/testbed/coredns/zones.conf")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		log.Fatal(err)
	}
}

func (g *generator) writeCaddyfile() {
	var head strings.Builder
	head.WriteString(`# Сгенерировано testbedgen (make testbed-gen). Не редактировать вручную.
{
	# TLS-сертификаты выпускает внутренний CA Caddy (self-signed);
	# автоматические HTTP->HTTPS-редиректы отключены — редиректы часть сценариев.
	auto_https disable_redirects
	admin localhost:2019
	log
}

`)
	path := filepath.Join(g.baseDir, "deploy/docker/testbed/web/Caddyfile")
	if err := os.WriteFile(path, []byte(head.String()+g.caddy.String()), 0o644); err != nil {
		log.Fatal(err)
	}
}

// ---------------------------------------------------------------- фикстуры

func (g *generator) writeFixture(dir, name, content string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		log.Fatal(err)
	}
}

func genericPage(title string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="ru">
<head><meta charset="utf-8"><title>%s</title></head>
<body><h1>%s</h1><p>Тестовая страница testbed.</p></body>
</html>
`, title, title)
}

func parkedPage() string {
	// Одинаковое содержимое для parked-1 и parked-2: дедупликация по хэшу.
	return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Welcome to your new domain</title></head>
<body><h1>Future home of something quite cool</h1>
<p>If you're the domain owner, log in to your hosting control panel to start managing this site.</p></body>
</html>
`
}

func metaRefreshPage(title, target string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="ru">
<head><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=%s"><title>%s</title></head>
<body><p>Перенаправляем…</p></body>
</html>
`, target, title)
}

func jsRedirectPage(title, target string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="ru">
<head><meta charset="utf-8"><title>%s</title>
<script>window.location.replace("%s");</script></head>
<body><noscript>Включите JavaScript</noscript></body>
</html>
`, title, target)
}

func spaPage(title string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="ru">
<head><meta charset="utf-8"><title>%s</title></head>
<body>
<div id="root"></div>
<noscript>Для работы сайта требуется включённый JavaScript.</noscript>
<script>
  // JS выставляет cookie и рисует контент — проверка рендера (ТЗ 5.6).
  document.cookie = "js_session=rendered-by-js; path=/";
  setTimeout(function () {
    document.getElementById("root").innerHTML =
      "<h1>SPA-контент после рендера</h1><p>Этот текст виден только после исполнения JS.</p>";
  }, 150);
</script>
</body>
</html>
`, title)
}

// ---------------------------------------------------------------- утилиты

func puny(name string) string {
	p, err := idna.Lookup.ToASCII(name)
	if err != nil {
		log.Fatalf("testbedgen: некорректное имя %q: %v", name, err)
	}
	return p
}

func caddyEscape(s string) string { return strings.ReplaceAll(s, `"`, `\"`) }

func sortedHeaderNames(m map[string][]string) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func sortedZoneNames(m map[string]*zoneData) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
