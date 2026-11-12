// Package checker — библиотечный конвейер живой проверки одного домена
// (T-123, рефакторинг crawlerctl check из T-119): DNS всех субдоменов →
// HTTP обоих схем живых (+ рендер SPA при настроенном render.remote_url)
// → пассивный анализ правилами. Read-only к БД и очередям: ничего не
// пишет и не публикует (публикацией владеет scheduler); из БД читается
// только справочник subdomains. Потребители — CLI crawlerctl check
// (отладка правил) и публичная форма лендинга (internal/web, T-123);
// UA/таймауты/лимиты — из конфигурации, никаких активных зондов
// (пассивная этика ТЗ §5.8/§9).
package checker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/dnsworker"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/idn"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/rules"
)

// SubRecord — итог одной записи (субдомен×вариант): то, что показывает
// и CLI, и форма лендинга.
type SubRecord struct {
	SubdomainID int16
	Label       string // "" — апекс
	Host        string // label.domain
	Variant     string // main | http
	Scheme      string // http | https
	Status      string // HTTP-код или «ошибка <класс>»
	FinalURL    string
	Redirects   []httpfetch.RedirectStep
	TLSValid    bool
	TLSNotAfter time.Time
	HasTLS      bool
	Signals     []rules.Evidence
	Score       float64
	VerMin      string
	VerMax      string
	Rendered    bool // анализ по DOM рендера
}

// Result — итог проверки домена.
type Result struct {
	Domain   string // punycode
	Unicode  string
	Zone     string
	Apex     string // ветка апекса DNS (delegated/not_delegated/…)
	Terminal string // терминальный статус DNS-этапа ("" — HTTP шёл)
	Records  []SubRecord
	Found    bool // хоть одна запись с is_laravel
	Score    float64
	VerMin   string
	VerMax   string
	DNSMS    int64 // длительность DNS-фазы, мс
	TotalMS  int64 // длительность всей проверки, мс
}

// Logger — минимальный логгер конвейера (удовлетворяется *slog.Logger).
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

// LiveFunc — шов конвейера для потребителей (форма лендинга T-123
// подменяет в тестах без сети; продакшн-реализация — метод Run
// долгоживущего контура Runtime, T-220, и одноразовая функция Run для
// CLI).
type LiveFunc func(ctx context.Context, cfg *config.Config, pool Pool, domain string, log Logger) (*Result, error)

// Pool — зависимость конвейера: справочник subdomains (db.DB).
type Pool = db.DB

// Run выполняет полный конвейер одного имени на готовом контуре (T-220):
// метод совпадает с LiveFunc по сигнатуре — web передаёт его от контура,
// собранного на время жизни сервиса; cfg-параметр игнорируется (контур
// собран из своей конфигурации при NewRuntime, включая порог
// analyzer.laravel_min_score). pool используется только чтением
// справочника subdomains (LoadSubdomains). Ошибки сети/анализа не
// возвращаются как отказ проверки — они отражаются в записях
// (Status/Signals); возврат ошибки — только инфраструктурные сбои
// (БД справочника, отмена ctx). Одноразовый вызов без своего контура —
// свободная функция Run (CLI).
func (rt *Runtime) Run(ctx context.Context, _ *config.Config, pool Pool, domain string, log Logger) (*Result, error) {
	if log == nil {
		// NewRuntime допускает nil-логгер — конвейер не должен
		// паниковать на Warn (P3-1 ревью T-220).
		log = rt.log
	}
	start := time.Now()
	norm, err := idn.Normalize(domain)
	if err != nil {
		return nil, fmt.Errorf("checker: invalid domain name %q: %w", domain, err)
	}
	res := &Result{Domain: norm.ASCII, Unicode: norm.Unicode, Zone: norm.Zone}

	subs, err := dnsworker.LoadSubdomains(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("checker: %w", err)
	}

	// DNS-этап как библиотека: цепь ручной проверки (resolve.go) под
	// лимитами T-035 — цепь и лимитёр собраны на контуре один раз
	// (NewRuntime), здесь только сам проход.
	resolve := rt.resolve

	dnsStart := time.Now()
	apex, err := dnsworker.CheckApex(ctx, resolve, norm.ASCII)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: apex: %w", norm.ASCII, err)
	}
	res.Apex = apex.Status.String()
	if apex.Status.Stop() {
		if st, ok := apex.Status.DomainStatus(); ok {
			res.Terminal = st.String()
		}
		res.TotalMS = time.Since(start).Milliseconds()
		return res, nil
	}

	rep, err := dnsworker.CheckSubdomains(ctx, resolve, norm.ASCII, subs, rt.cfg.DNS.IPv6)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: subdomains: %w", norm.ASCII, err)
	}
	var wild dnsworker.WildcardResult
	if info, werr := dnsworker.DetectWildcard(ctx, resolve, norm.ASCII); werr != nil {
		log.Warn("checker: wildcard detection failed — continuing without filtering", "err", werr)
	} else if info.Detected {
		if wild, err = dnsworker.MarkWildcardSubdomains(info.Addrs, wildSubs(rep)); err != nil {
			return nil, fmt.Errorf("checker: %s: wildcard marking: %w", norm.ASCII, err)
		}
	}
	episode, err := dnsworker.DecideEpisode(apex, rep, wild)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: episode decision: %w", norm.ASCII, err)
	}
	if !episode.HasHTTPTask() {
		res.Terminal = episode.Status.String()
		res.DNSMS = time.Since(dnsStart).Milliseconds()
		res.TotalMS = time.Since(start).Milliseconds()
		return res, nil
	}
	res.DNSMS = time.Since(dnsStart).Milliseconds()

	// Транспорт как у боевого http-worker (T-049/T-050/T-047) — один на
	// контур (NewRuntime) с общими limitState; здесь только сам проход.
	fetcher := rt.fetcher

	// Живой проверке некуда (и незачем) писать боевой контент: воркер
	// с T-147 сохраняет тела ранним Put и требует ContentStore — одноразовое
	// хранилище в памяти принимает страницы, анализ читает их обратно по
	// хэшу записи (render для SPA подменит HTML своим DOM ниже).
	mem := newCheckerStore()
	worker, err := httpworker.New(httpworker.ChainFetch(fetcher), httpworker.Options{
		MaxRedirects: rt.cfg.HTTP.MaxRedirects,
		Store:        mem,
	})
	if err != nil {
		return nil, fmt.Errorf("checker: %w", err)
	}
	// Идентификаторы синтетические: ProcessDomain их не валидирует.
	task := queue.HTTPTask{ZoneID: 1, DomainID: 1, Name: norm.ASCII, Subdomains: episode.HTTPSubdomains}
	dom, err := worker.ProcessDomain(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("checker: %s: HTTP: %w", norm.ASCII, err)
	}

	// Рендер SPA — внешний CDP-браузер (render.remote_url); менеджер
	// собран на контуре, поколение браузера поднимается лениво первым
	// рендером (недоступный Chromium — анализ по исходному HTML, как и
	// при ошибке рендера записи). Первый отказ класса «браузер» гасит
	// рендер до конца этой проверки (аналог renderer=nil одноразового
	// Run до T-220): повторные попытки на общем менеджере стартуют
	// поколение под его мьютексом и платят Dial+SessionStart таймауты
	// на каждой SPA-записи (P2-3 ревью T-220), следующая проверка
	// попробует снова.
	renderer := rt.renderer
	renderDown := false

	// Снапшот правил — из Reloader контура: набор читается один раз на
	// сборке и актуализируется фоновой перезагрузкой, а не с диска на
	// каждый запрос (T-220).
	set := rt.rules.Set()

	for i := range dom.Subdomains {
		sub := &dom.Subdomains[i]
		for j := range sub.Records {
			rec := &sub.Records[j]
			sr := SubRecord{
				SubdomainID: rec.SubdomainID,
				Label:       sub.Label,
				Host:        sub.Hostname,
				FinalURL:    rec.FinalURL,
				Redirects:   rec.Redirects,
				HasTLS:      rec.TLS != nil && rec.Scheme == model.SchemeHTTPS,
			}
			if sr.HasTLS {
				sr.TLSValid = rec.TLS.Valid
				sr.TLSNotAfter = rec.TLS.NotAfter
			}
			switch rec.Variant {
			case model.VariantHTTP:
				sr.Variant = "http"
			default:
				sr.Variant = "main"
			}
			if rec.Scheme == model.SchemeHTTPS {
				sr.Scheme = "https"
			} else {
				sr.Scheme = "http"
			}
			if rec.ErrClass != httpworker.ErrNone {
				sr.Status = "error " + rec.ErrClass.String()
			} else {
				sr.Status = fmt.Sprintf("%d", rec.HTTPStatus)
			}

			page := rules.Page{
				HTML:    mem.body(rec.ContentHash),
				Cookies: ruleCookies(rec.Cookies),
				Headers: map[string][]string(rec.Header),
			}
			if rec.SPA != nil && rec.SPA.SPA && renderer != nil && !renderDown {
				if ro, rerr := renderer.Run(ctx, render.RenderTask{URL: rec.FinalURL}); rerr == nil {
					page.HTML = []byte(ro.DOM)
					for _, c := range ro.Cookies {
						page.Cookies = append(page.Cookies, rules.Cookie{Name: c.Name, Value: c.Value})
					}
					page.Headers = map[string][]string(ro.ResponseHeaders)
					sr.Rendered = true
				} else if render.ClassOf(rerr) == render.ClassBrowser {
					log.Warn("checker: render unavailable — SPA on the raw HTML for the rest of the check", "err", rerr)
					renderDown = true
				}
			}
			eval := set.EvaluateCtx(ctx, page, rules.Limits{})
			sr.Signals = eval.Signals
			sr.Score = eval.Score
			sr.VerMin, sr.VerMax = eval.VerMin, eval.VerMax
			if eval.IsLaravel(rt.cfg.Analyzer.LaravelMinScore) {
				sr.Score = eval.Score
				if eval.Score > res.Score {
					res.Score = eval.Score
					res.VerMin, res.VerMax = eval.VerMin, eval.VerMax
				}
				res.Found = true
			}
			res.Records = append(res.Records, sr)
		}
	}
	res.TotalMS = time.Since(start).Milliseconds()
	return res, nil
}

// wildSubs — вход wildcard-пометки T-038 (копия resultWildSubs dnsworker).
func wildSubs(rep *dnsworker.SubdomReport) []dnsworker.WildSubdomain {
	out := make([]dnsworker.WildSubdomain, 0, len(rep.Results))
	for _, r := range rep.Results {
		addrs := make([]net.IP, 0, len(r.A)+len(r.AAAA))
		for _, a := range r.A {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		for _, a := range r.AAAA {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		out = append(out, dnsworker.WildSubdomain{ID: r.ID, Label: r.Label, Addrs: addrs})
	}
	return out
}

func ruleCookies(cs []*http.Cookie) []rules.Cookie {
	out := make([]rules.Cookie, 0, len(cs))
	for _, c := range cs {
		out = append(out, rules.Cookie{Name: c.Name, Value: c.Value})
	}
	return out
}

// checkerStore — одноразовое хранилище контента живой проверки в памяти
// (T-147): http-worker сохраняет тела страниц ранним Put и работает без
// удержания байтов в памяти, живой проверке некуда (и незачем) писать
// боевые паки — страницы одной проверки оседают здесь и читаются назад
// по хэшу записи для пассивного анализа (рендер SPA подменит HTML своим
// DOM). Реализует content.ContentStore (Put/Get/Delete, контракт
// T-041: дедуп по хэшу, Created, refcount); память ограничена задачей
// одного домена (страниц ≤ 2 × субдомены, тело ≤ max_body_bytes).
type checkerStore struct {
	mu     sync.Mutex
	blocks map[content.Hash][]byte
	refs   map[content.Hash]int
}

func newCheckerStore() *checkerStore {
	return &checkerStore{
		blocks: make(map[content.Hash][]byte),
		refs:   make(map[content.Hash]int),
	}
}

func (s *checkerStore) Put(_ context.Context, data []byte) (content.PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := content.HashOf(data)
	if s.refs[h] > 0 {
		s.refs[h]++
		return content.PutResult{Hash: h}, nil
	}
	s.blocks[h] = append([]byte(nil), data...)
	s.refs[h] = 1
	return content.PutResult{Hash: h, Created: true}, nil
}

func (s *checkerStore) Get(_ context.Context, h content.Hash) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs[h] <= 0 {
		return nil, content.ErrNotFound
	}
	return append([]byte(nil), s.blocks[h]...), nil
}

func (s *checkerStore) Delete(_ context.Context, h content.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs[h] <= 0 {
		return content.ErrNotFound
	}
	s.refs[h]--
	if s.refs[h] == 0 {
		delete(s.blocks, h)
	}
	return nil
}

// body возвращает тело по хэшу записи (nil — контента нет: обрезка,
// несохранённая страница, алиас без ссылки). Для живой проверки контент
// алиаса доступен как собственный (класс T-055 един) — правила видят
// реальные байты страницы записи.
func (s *checkerStore) body(h *content.Hash) []byte {
	if h == nil {
		return nil
	}
	b, err := s.Get(context.Background(), *h)
	if err != nil {
		return nil
	}
	return b
}
