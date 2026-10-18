// ---------------------------------------------------------------- check
//
// Живая проверка одного домена вне цикла планировщика (T-119, аналог
// domain:check и веб-формы larasites): DNS всех субдоменов → HTTP обоих
// схем живых (+ рендер при SPA-вердикте, если настроен render.remote_url)
// → анализ правилами; вывод per-subdomain: статус, цепочка редиректов,
// сводка TLS, сработавшие правила с evidence, score, диапазон версий.
//
// Read-only к БД и очередям: ничего не пишет и не публикует — публикацией
// владеет scheduler (семантика crawlerctl recheck); из БД читается только
// справочник subdomains. Конвейер переиспользует этапы как библиотеки:
// dnsworker (CheckApex/CheckSubdomains/DetectWildcard/DecideEpisode),
// httpworker.ProcessDomain (без OnResult — без записи и публикации),
// render.Manager, rules.Set. UA/таймауты/лимиты — из конфигурации
// (httpfetch.NewOptions/render.NewOptions подхватывают секции http/render);
// никаких активных зондов — только GET / живых субдоменов (этика §5.8/§9).

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/idn"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/rules"
)

// checkHelp — справка crawlerctl check --help.
const checkHelp = `crawlerctl check — живая проверка домена конвейером (T-119)

Использование:
  crawlerctl check <домен>

Полный цикл для одного имени: DNS (справочник subdomains из БД) → HTTP
обоих схем DNS-живых субдоменов (рендер SPA-страниц при настроенном
render.remote_url) → пассивный анализ правилами Laravel. Вывод по
субдоменам: статус, цепочка редиректов, сводка TLS, сработавшие правила
с evidence, score, диапазон версий.

Read-only: ничего не пишет в БД и не публикует в очереди — постановка
домена на штатную проверку отдельной командой (crawlerctl recheck).
User-Agent, таймауты и лимиты — из конфигурации (секции dns/http/render);
анализ — актуальный набор правил analyzer.rules_dir и порог
analyzer.laravel_min_score. Никаких активных зондов: только GET /
живых субдоменов (пассивная этика ТЗ §5.8/§9).
Коды возврата: 0 — проверка выполнена, 1 — ошибка, 2 — использование.
`

func cmdCheck(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("check")
	if handled, err := parseFlags(fs, checkHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите ровно один домен: crawlerctl check example.com")
	}
	norm, err := idn.Normalize(fs.Arg(0))
	if err != nil {
		return usagef("check: некорректное имя домена %q: %v", fs.Arg(0), err)
	}
	name := norm.ASCII

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	ctx, stop := signalCtx()
	defer stop()

	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2 // бюджет соединений ТЗ 6.3 — у сервисов
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	defer pool.Close()

	// Справочник субдоменов — единственное чтение БД (read-only): тот же,
	// что у боевого DNS-этапа (перечитывается на каждую пачку).
	subs, err := dnsworker.LoadSubdomains(ctx, pool)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}

	// DNS-этап (T-036…T-038) как библиотека: резолвер — локальный клиент
	// T-034 под лимитами T-035 (цепь резервов воркера для одного имени
	// избыточна — адрес из dns.resolvers).
	client, err := dnsclient.NewClient(ctx, dnsclient.NewOptions(cfg.DNS))
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	defer func() { _ = client.Close() }()
	resolve, err := dnsclient.NewLimitResolver(client, dnsclient.NewLimitOptions(cfg.DNS))
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}

	out(stdout, "check: %s (зона %s", name, norm.Zone)
	if norm.Unicode != name {
		out(stdout, ", unicode %s", norm.Unicode)
	}
	out(stdout, ") — правила %s, порог %.2f\n\n", cfg.Analyzer.RulesDir, cfg.Analyzer.LaravelMinScore)

	// --- DNS ----------------------------------------------------------------
	apex, err := dnsworker.CheckApex(ctx, resolve, name)
	if err != nil {
		return fmt.Errorf("check: %s: апекс: %w", name, err)
	}
	out(stdout, "DNS апекс: %s", apex.Status)
	if apex.Addresses != nil {
		out(stdout, " —")
		for _, a := range apex.Addresses {
			out(stdout, " %s", a)
		}
	}
	out(stdout, "\n")

	if apex.Status.Stop() {
		// Терминальная ветка DNS: not_delegated/temp_error — HTTP не идёт
		// (RFC 8020/T-036).
		if st, ok := apex.Status.DomainStatus(); ok {
			out(stdout, "\nитог: терминальный статус DNS-этапа — %s\n", st)
		}
		return nil
	}

	rep, err := dnsworker.CheckSubdomains(ctx, resolve, name, subs, cfg.DNS.IPv6)
	if err != nil {
		return fmt.Errorf("check: %s: субдомены: %w", name, err)
	}
	var wild dnsworker.WildcardResult
	info, werr := dnsworker.DetectWildcard(ctx, resolve, name)
	switch {
	case werr != nil:
		// Политика T-038: продолжить без фильтрации.
		out(stderr, "check: wildcard-детект не удался — продолжаю без фильтрации: %v\n", werr)
	case info.Detected:
		wild, err = dnsworker.MarkWildcardSubdomains(info.Addrs, checkWildSubs(rep))
		if err != nil {
			return fmt.Errorf("check: %s: wildcard-пометка: %w", name, err)
		}
		out(stdout, "DNS wildcard: обнаружен (%s…) — синтетические имена отфильтрованы\n", info.Label)
	}

	episode, err := dnsworker.DecideEpisode(apex, rep, wild)
	if err != nil {
		return fmt.Errorf("check: %s: решение эпизода: %w", name, err)
	}
	out(stdout, "DNS-живые субдомены (%d):\n", len(episode.HTTPSubdomains))
	for _, s := range episode.HTTPSubdomains {
		label := s.Label
		if label == "" {
			label = "(апекс)"
		}
		out(stdout, "  %-12s", label)
		for _, a := range s.Addrs {
			out(stdout, " %s", a)
		}
		if s.CNAME != "" {
			out(stdout, "  CNAME %s", s.CNAME)
		}
		out(stdout, "\n")
	}
	if !episode.HasHTTPTask() {
		out(stdout, "\nитог: терминальный статус DNS-этапа — %s (адресов нет)\n", episode.Status)
		return nil
	}

	// --- HTTP ---------------------------------------------------------------
	// Транспорт как у боевого http-worker (T-049/T-050): UA, таймауты и
	// лимиты — из секции http; резолв хостов редиректов — тем же резолвером
	// с кэшем (T-047); TLS без проверки (контент нужен всегда — ТЗ 5.5
	// п. 3, сводку сохраняет TLSInfo).
	opts := httpfetch.NewOptions(cfg.HTTP)
	opts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	opts.Resolver = httpfetch.NewCachingResolver(
		httpworker.NewDNSLookup(resolve, cfg.DNS.IPv6), httpfetch.CacheOptions{})
	fetcher := httpfetch.NewFetcher(opts)
	defer fetcher.CloseIdleConnections()

	worker, err := httpworker.New(httpworker.ChainFetch(fetcher), httpworker.Options{
		MaxRedirects: cfg.HTTP.MaxRedirects,
		Log:          slog.New(slog.NewTextHandler(stderr, nil)),
	})
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	// Идентификаторы синтетические: ProcessDomain их не валидирует (ключи
	// нужны только записи результата — здесь записи нет), имя и субдомены
	// проходят полный боевой контроль workerPrepareTask.
	task := queue.HTTPTask{ZoneID: 1, DomainID: 1, Name: name, Subdomains: episode.HTTPSubdomains}
	dom, err := worker.ProcessDomain(ctx, task)
	if err != nil {
		return fmt.Errorf("check: %s: HTTP: %w", name, err)
	}

	// Рендер SPA (T-066): внешний CDP-браузер render.remote_url; не
	// настроен — SPA анализируется по исходному HTML (вывод честно
	// помечает отсутствие рендера).
	var renderer *render.Manager
	if cfg.Render.RemoteURL != "" {
		renderer = render.NewManager(ctx, render.NewOptions(*cfg))
		if err := renderer.Start(); err != nil {
			out(stderr, "check: рендер недоступен (%s) — SPA анализируется по исходному HTML: %v\n",
				cfg.Render.RemoteURL, err)
			renderer = nil
		} else {
			defer renderer.Close()
		}
	} else {
		out(stderr, "check: render.remote_url не настроен — SPA анализируется по исходному HTML\n")
	}

	// Анализ (§5.8): актуальный набор, порог — как у analyzer.
	set, err := rules.LoadDir(cfg.Analyzer.RulesDir)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}

	out(stdout, "\nHTTP-проверка (правила v%d):\n", set.Version)
	for i := range dom.Subdomains {
		sub := &dom.Subdomains[i]
		label := sub.Label
		if label == "" {
			label = "(апекс)"
		}
		out(stdout, "\n%s [%s]:\n", sub.Hostname, label)
		if len(sub.Records) == 0 {
			out(stdout, "  записей нет — обе схемы молчат (%s)\n", checkSchemeErrs(sub))
		}
		for j := range sub.Records {
			checkPrintRecord(stdout, ctx, &sub.Records[j], renderer, set, cfg.Analyzer.LaravelMinScore)
		}
	}
	out(stdout, "\ncheck: %s — проверка завершена (ничего не записано, очереди не тронуты)\n", name)
	return nil
}

// checkWildSubs — вход wildcard-пометки T-038 из отчёта субдоменов
// (копия resultWildSubs dnsworker — там она не экспортирована).
func checkWildSubs(rep *dnsworker.SubdomReport) []dnsworker.WildSubdomain {
	res := make([]dnsworker.WildSubdomain, 0, len(rep.Results))
	for _, r := range rep.Results {
		addrs := make([]net.IP, 0, len(r.A)+len(r.AAAA))
		for _, a := range r.A {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		for _, a := range r.AAAA {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		res = append(res, dnsworker.WildSubdomain{ID: r.ID, Label: r.Label, Addrs: addrs})
	}
	return res
}

// checkSchemeErrs — сводка ошибок схем для субдомена без записей.
func checkSchemeErrs(sub *httpworker.SubdomainResult) string {
	https := checkSchemeErrText(&sub.HTTPS)
	plain := checkSchemeErrText(&sub.HTTP)
	switch {
	case https != "" && plain != "":
		return "https: " + https + "; http: " + plain
	case https != "":
		return "https: " + https
	case plain != "":
		return "http: " + plain
	default:
		return "нет ответа"
	}
}

func checkSchemeErrText(c *httpworker.SchemeCheck) string {
	if c.ErrClass != httpworker.ErrNone {
		return c.ErrClass.String()
	}
	if !c.Available {
		return "нет ответа"
	}
	return ""
}

// checkPrintRecord — вывод одной записи (субдомен×вариант): итоговый URL,
// цепочка редиректов, TLS-сводка, анализ правил (score, версии, сигналы).
// SPA-запись при живом рендере анализируется по DOM (куки и заголовки
// браузера добавляются к пассивному набору — конвейер T-066/T-067).
func checkPrintRecord(w io.Writer, ctx context.Context, rec *httpworker.Record,
	renderer *render.Manager, set *rules.Set, minScore float64) {
	variant := "main"
	if rec.Variant == model.VariantHTTP {
		variant = "http"
	}
	scheme := "http"
	if rec.Scheme == model.SchemeHTTPS {
		scheme = "https"
	}
	status := fmt.Sprintf("%d", rec.HTTPStatus)
	if rec.ErrClass != httpworker.ErrNone {
		status = "ошибка " + rec.ErrClass.String()
	}
	out(w, "  запись %s/%s: %s %s\n", variant, scheme, status, rec.FinalURL)

	if len(rec.Redirects) > 1 {
		out(w, "    редиректы:")
		for i, r := range rec.Redirects {
			if i == len(rec.Redirects)-1 {
				break // последний шаг — итоговая страница, уже в заголовке
			}
			out(w, " %d→%s", r.Status, r.URL)
		}
		out(w, "\n")
	}
	if rec.TLS != nil && rec.Scheme == model.SchemeHTTPS {
		verdict := "валиден"
		if !rec.TLS.Valid {
			probs := make([]string, 0, len(rec.TLS.Problems))
			for _, p := range rec.TLS.Problems {
				probs = append(probs, string(p))
			}
			verdict = "ПРОБЛЕМЫ: " + strings.Join(probs, ", ")
		}
		out(w, "    TLS: %s, истекает %s, CN %q, цепочка %d\n",
			verdict, rec.TLS.NotAfter.Format("2006-01-02"),
			rec.TLS.SubjectCommonName, rec.TLS.ChainLength)
	}
	if rec.AliasKind != model.AliasNone {
		out(w, "    алиас: %s субдомена %s\n", rec.AliasKind, checkAliasID(rec.AliasSubdomainID))
	}

	// Контент для правил: тело записи (или DOM рендера SPA).
	if rec.SPA != nil && rec.SPA.SPA && renderer != nil {
		ro, rerr := renderer.Run(ctx, render.RenderTask{URL: rec.FinalURL})
		if rerr == nil {
			// Куки и заголовки браузера — в пассивный набор (T-067):
			// JS-куки и httpOnly видны только рендеру.
			cookies := make([]rules.Cookie, 0, len(rec.Cookies)+len(ro.Cookies))
			cookies = append(cookies, checkRuleCookies(rec.Cookies)...)
			for _, c := range ro.Cookies {
				cookies = append(cookies, rules.Cookie{Name: c.Name, Value: c.Value})
			}
			checkPrintAnalysis(w, ctx, set, minScore, rules.Page{
				HTML:    []byte(ro.DOM),
				Cookies: cookies,
				Headers: map[string][]string(ro.ResponseHeaders),
			}, true)
			return
		}
		out(w, "    рендер не удался — анализ по исходному HTML: %v\n", rerr)
	}
	checkPrintAnalysis(w, ctx, set, minScore, rules.Page{
		HTML:    rec.Body,
		Cookies: checkRuleCookies(rec.Cookies),
		Headers: map[string][]string(rec.Header),
	}, false)
}

// checkPrintAnalysis — прогон правил и вывод итога.
func checkPrintAnalysis(w io.Writer, ctx context.Context, set *rules.Set, minScore float64, page rules.Page, rendered bool) {
	res := set.EvaluateCtx(ctx, page, rules.Limits{}) // нулевые → DefaultLimits
	src := "исходный HTML"
	if rendered {
		src = "DOM после рендера"
	}
	ver := "-"
	if res.VerMin != "" || res.VerMax != "" {
		ver = fmt.Sprintf("[%s..%s]", orDash(res.VerMin), orDash(res.VerMax))
	}
	if res.Conflict {
		ver = "[конфликт]"
	}
	out(w, "    laravel: score=%.3f ver=%s правил=%d is_laravel=%v (%s)\n",
		res.Score, ver, len(res.Signals), res.IsLaravel(minScore), src)
	for _, ev := range res.Signals {
		out(w, "      - %-28s w=%.2f  «%s»\n", ev.Rule, ev.Weight, ev.Evidence)
	}
}

// checkRuleCookies — конвертация http.Cookie в правила анализа.
func checkRuleCookies(cs []*http.Cookie) []rules.Cookie {
	res := make([]rules.Cookie, 0, len(cs))
	for _, c := range cs {
		res = append(res, rules.Cookie{Name: c.Name, Value: c.Value})
	}
	return res
}

func checkAliasID(id *int16) string {
	if id == nil {
		return "?"
	}
	return fmt.Sprintf("%d", *id)
}
