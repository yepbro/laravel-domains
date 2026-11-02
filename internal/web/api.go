// Форма ручной проверки лендинга (T-123): POST /api/check-domain —
// гибрид сохранённых данных конвейера (domain_info) и живой проверки
// (internal/checker, общий конвейер с crawlerctl check). Рейт-лимит
// 4 запроса/мин на IP — in-memory token bucket (один инстанс сервиса,
// stdlib; фиксированное решение этапа 13 — как троттлинг larasites).
// Результат пишется в web_checks (T-120) — кто/что/когда проверял.
// Адрес посетителя за прокси и исходы/сбои/длительность формы — T-124
// (TCP-loopback пир) и T-221 (пир unix-сокета — вариант деплоя из
// deploy/nginx/README.md; исходы — метрики metrics.go на :9107).
//
// Ресурсы живой проверки (T-220): конвейер работает на долгоживущем
// контуре checker.Runtime (одна цепь резолверов с малым пулом UDP,
// общий Fetcher с общими лимитами, кэш правил, один render.Manager —
// собирается лениво первой проверкой, web.New/liveFunc), а потолок
// одновременных конвейеров держит семафор web.live_concurrency:
// перегруз — быстрый 503 с Retry-After, не очередь до web.timeout.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/checker"
	"crawler/internal/idn"
)

// checkerDB — адаптер web.DB → db.DB пакета checker (CopyFrom/Exec форме
// не нужны, checker их не вызывает — read-only конвейер; Exec —
// предохранитель с ошибкой, см. реализацию).
type checkerDB struct{ db DB }

func (c checkerDB) Ping(ctx context.Context) error { return c.db.Ping(ctx) }

func (c checkerDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("web: CopyFrom не используется формой")
}

func (c checkerDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	// Предохранитель read-only (P3-1 ревью T-221): живой конвейер Exec
	// не вызывает, журнал формы пишет собственным путём (recordCheck)
	// мимо адаптера. Если checker когда-нибудь начнёт писать — ошибка
	// вскроет это сразу, а не пройдёт молча.
	return pgconn.CommandTag{}, fmt.Errorf("web: неожидаемый Exec живой проверки: %s", sql)
}

func (c checkerDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.db.Query(ctx, sql, args...)
}

// checkResponse — единый JSON-ответ формы: found/score/версии +
// per-subdomain записи (контракт перенесённого landing.js).
type checkResponse struct {
	Found      bool             `json:"found"`
	Score      float64          `json:"score"`
	VerMin     string           `json:"ver_min,omitempty"`
	VerMax     string           `json:"ver_max,omitempty"`
	Stored     *storedSummary   `json:"stored,omitempty"`
	Subdomains []checkSubRecord `json:"subdomains"`
	DurationMS int64            `json:"duration_ms"`
}

// storedSummary — последний эпизод краулера по домену (domain_info,
// сокращение до полей формы).
type storedSummary struct {
	Status    string   `json:"status"`
	CheckedAt string   `json:"checked_at,omitempty"`
	Score     *float64 `json:"score,omitempty"`
	VerMin    string   `json:"ver_min,omitempty"`
	VerMax    string   `json:"ver_max,omitempty"`
}

// checkSubRecord — запись субдомена в ответе.
type checkSubRecord struct {
	Subdomain string             `json:"subdomain"`
	Variant   string             `json:"variant"`
	Status    string             `json:"status"`
	FinalURL  string             `json:"final_url,omitempty"`
	Redirects []redirectStepJSON `json:"redirects,omitempty"`
	TLSValid  *bool              `json:"tls_valid,omitempty"`
	Score     float64            `json:"score"`
	VerMin    string             `json:"ver_min,omitempty"`
	VerMax    string             `json:"ver_max,omitempty"`
	Signals   []signalJSON       `json:"signals"`
	Rendered  bool               `json:"rendered"`
}

type redirectStepJSON struct {
	URL    string `json:"url"`
	Status int    `json:"status"`
}

type signalJSON struct {
	Rule     string  `json:"rule"`
	Weight   float64 `json:"weight"`
	Evidence string  `json:"evidence"`
}

// errLiveOverloaded — семафор одновременных живых проверок полон (T-220):
// быстрый 503 с Retry-After вместо ожидания слота до web.timeout.
var errLiveOverloaded = errors.New("живая проверка перегружена")

// handleCheckDomainLive — T-123: валидация → stored → live (timeout
// web.timeout, семафор web.live_concurrency) → запись web_checks → JSON.
// Ошибки: 422 (валидация, формат по образцу landing.js), 429 (рейт-лимит,
// +Retry-After), 503 (перегрузка живых проверок, +Retry-After — всем
// клиентам: 200 с found=false выглядел бы «Laravel не найден» для домена
// со stored-данными, а stored-часть landing.js не показывает), 502
// (живая проверка не удалась инфраструктурно и домен конвейеру неизвестен).
func (s *Server) handleCheckDomainLive(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ip, isIP := clientIP(r)
	if !s.limiter.allow(ip, time.Now()) {
		s.met.Outcome(checkResultRateLimited)
		w.Header().Set("Retry-After", "60")
		http.Error(w, `{"error":"rate_limited"}`, http.StatusTooManyRequests)
		return
	}

	var req struct {
		Domain string `json:"domain"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		s.met.Outcome(checkResultInvalid)
		checkWriteError(w, "invalid", http.StatusUnprocessableEntity)
		return
	}
	normalized, err := validateCheckInput(req.Domain)
	if err != nil {
		s.met.Outcome(checkResultInvalid)
		checkWriteError(w, "invalid", http.StatusUnprocessableEntity)
		return
	}

	resp := checkResponse{Subdomains: []checkSubRecord{}}

	// (1) stored: последний эпизод краулера, если домен известен.
	if st := s.fetchStored(r.Context(), normalized); st != nil {
		resp.Stored = st
	}

	// (2) live: полный конвейер с потолком web.timeout под семафором
	// одновременных проверок (T-220): конвейер держит сеть/память,
	// перегруз отвечает быстро (503 с Retry-After), а не копит очередь
	// до таймаута. Сам конвейер работает на общем долгоживущем контуре
	// checker.Runtime — одна цепь резолверов, общий Fetcher, кэш
	// правил, один render.Manager на сервис.
	var (
		live    *checker.Result
		liveErr error
	)
	select {
	case s.liveSem <- struct{}{}:
		func() {
			defer func() { <-s.liveSem }()
			fn, err := s.liveFunc()
			if err != nil {
				liveErr = err
				return
			}
			liveCtx, cancel := context.WithTimeout(r.Context(), s.cfg.Web.Timeout.D())
			defer cancel()
			live, liveErr = fn(liveCtx, s.cfg, checkerDB{s.db}, normalized, s.opt.Log)
		}()
	default:
		liveErr = errLiveOverloaded
		s.opt.Log.Warn("форма: живая проверка перегружена", "domain", normalized,
			"limit", cap(s.liveSem))
	}

	if liveErr == nil {
		resp.Found = live.Found
		resp.Score = live.Score
		resp.VerMin, resp.VerMax = live.VerMin, live.VerMax
		resp.DurationMS = live.TotalMS
		for i := range live.Records {
			rec := &live.Records[i]
			sub := checkSubRecord{
				Subdomain: rec.Label,
				Variant:   rec.Variant,
				Status:    rec.Status,
				FinalURL:  rec.FinalURL,
				Score:     rec.Score,
				VerMin:    rec.VerMin,
				VerMax:    rec.VerMax,
				Signals:   make([]signalJSON, 0, len(rec.Signals)),
				Rendered:  rec.Rendered,
			}
			if rec.HasTLS {
				v := rec.TLSValid
				sub.TLSValid = &v
			}
			for _, step := range rec.Redirects {
				sub.Redirects = append(sub.Redirects, redirectStepJSON{URL: step.URL, Status: step.Status})
			}
			for _, ev := range rec.Signals {
				sub.Signals = append(sub.Signals, signalJSON{Rule: ev.Rule, Weight: ev.Weight, Evidence: ev.Evidence})
			}
			resp.Subdomains = append(resp.Subdomains, sub)
		}
	}
	// Ошибка живой проверки не рушит ответ: отдаём stored-часть с
	// пометкой; полностью пустой итог без stored — 502.

	// (3) журнал web_checks (T-120): best-effort — отказ записи не
	// ломает ответ посетителю. Перегрузка не журналируется: проверки
	// не было, строка found=false/score=0 портила бы аудит и добавляла
	// INSERT в БД на пути «быстрого» 503. Адрес без IP (пир unix-сокета
	// без валидного заголовка прокси, T-221) не пишется вовсе: колонка
	// ip_address inet NOT NULL произвольную строку не примет — пропуск
	// явный (Warn + метрика non_ip), не молчаливый.
	if !errors.Is(liveErr, errLiveOverloaded) {
		if isIP {
			s.recordCheck(r.Context(), normalized, resp.Found, resp.Score, resp.DurationMS, ip)
		} else {
			s.opt.Log.Warn("форма: журнал web_checks пропущен — адрес клиента не IP (пир без валидного заголовка прокси)",
				"domain", normalized)
			s.met.Failure(checkErrNonIP)
		}
		s.met.Duration(time.Since(start))
	}

	if liveErr != nil {
		if errors.Is(liveErr, errLiveOverloaded) {
			// Перегруз — быстрый отказ всем клиентам с подсказкой
			// повтора: независимо от stored-части (landing.js её не
			// показывает, 200 с found=false читался бы как «Laravel
			// не найден»).
			s.met.Outcome(checkResultBusy)
			w.Header().Set("Retry-After", "5")
			http.Error(w, `{"error":"busy"}`, http.StatusServiceUnavailable)
			return
		}
		s.met.Failure(checkErrLive)
		if resp.Stored == nil {
			s.met.Outcome(checkResultFailed)
			s.opt.Log.Warn("форма: живая проверка не удалась", "domain", normalized, "err", liveErr)
			http.Error(w, `{"error":"check_failed"}`, http.StatusBadGateway)
			return
		}
		s.met.Outcome(checkResultStoredOnly)
	} else {
		s.met.Outcome(checkResultOK)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

// validateCheckInput — валидация по образцу CheckDomainRequest larasites:
// trim, срез хвостовой точки, http(s)-префикс → хост, ASCII-домен или
// IDN (≥2 непустые метки), потолок длины. Возвращает punycode-имя.
func validateCheckInput(input string) (string, error) {
	s := strings.TrimSpace(input)
	s = strings.TrimSuffix(s, ".")
	if s == "" || len(s) > 2048 {
		return "", fmt.Errorf("пустой или сверхдлинный ввод")
	}
	s = strings.ToLower(s)
	// http(s)://host/… → host (порт срезается).
	if i := strings.Index(s, "://"); i > 0 {
		rest := s[i+3:]
		if host, _, ok := strings.Cut(rest, "/"); ok || host != "" {
			s = host
		}
		if h, _, err := net.SplitHostPort(s); err == nil {
			s = h
		}
	}
	if strings.ContainsAny(s, " /\\@:") {
		return "", fmt.Errorf("мусор в имени")
	}
	res, err := idn.Normalize(s)
	if err != nil {
		return "", fmt.Errorf("не домен: %w", err)
	}
	return res.ASCII, nil
}

// fetchStored — чтение domain_info (T-082): сокращение до полей формы.
func (s *Server) fetchStored(ctx context.Context, domain string) *storedSummary {
	rows, err := s.db.Query(ctx,
		"SELECT domain_info($1::text)", domain)
	if err != nil {
		return nil // best-effort: stored-часть опциональна
	}
	defer rows.Close()
	if !rows.Next() {
		return nil
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil || len(raw) == 0 {
		return nil
	}
	var full struct {
		Domain struct {
			StatusName string `json:"status_name"`
			LastCheck  string `json:"last_checked_at"`
		} `json:"domain"`
		Laravel *struct {
			Score  float64 `json:"score"`
			VerMin string  `json:"ver_min"`
			VerMax string  `json:"ver_max"`
		} `json:"laravel"`
	}
	if err := json.Unmarshal(raw, &full); err != nil {
		return nil
	}
	st := &storedSummary{Status: full.Domain.StatusName, CheckedAt: full.Domain.LastCheck}
	if full.Laravel != nil {
		sc := full.Laravel.Score
		st.Score = &sc
		st.VerMin, st.VerMax = full.Laravel.VerMin, full.Laravel.VerMax
	}
	return st
}

// recordCheck — INSERT web_checks (T-120): best-effort — отказ записи не
// ломает ответ посетителю, но логируется и считается метрикой (T-221:
// до неё INSERT шёл через Query без чтения строк — ошибка проглатывалась
// и журнал молча пустел). Exec, а не Query: INSERT строк не возвращает.
// Контекст запроса отменяется уходом посетителя (живая проверка до
// web.timeout), а журнал — аудит «кто/что/когда проверял»: INSERT живёт
// собственным коротким окном, не зависящим от отключения клиента
// (P3-5 ревью T-221).
func (s *Server) recordCheck(ctx context.Context, punycode string, found bool, score float64, durMS int64, ip string) {
	jctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if _, err := s.db.Exec(jctx,
		"INSERT INTO web_checks (domain, punycode_domain, ip_address, found, score, duration_ms) "+
			"VALUES ($1::text, $2::text, $3::inet, $4, $5::real, $6::int4)",
		punycode, punycode, ip, found, score, int32(durMS)); err != nil {
		// Домен и причина — без адреса посетителя (персональные данные
		// в лог не попадают, адрес остаётся только в web_checks).
		s.opt.Log.Warn("форма: журнал web_checks не записан", "domain", punycode, "err", err)
		s.met.Failure(checkErrJournal)
	}
}

// unixLocalKey — ключ бакета рейт-лимита для пира unix-сокета без
// валидного X-Real-IP/X-Forwarded-For (T-221): строка, заведомо не IP —
// не пишется в inet, все такие посетители делят один бакет (прокси
// сконфигурирован без заголовков — общее окно честнее обхода лимита).
const unixLocalKey = "unix-local"

// clientIP — адрес клиента для рейт-лимита и журнала web_checks.
// Второе возвращаемое — является ли результат IP вовсе: ложь для пира
// unix-сокета без валидного заголовка прокси и мусорного RemoteAddr
// (в inet не пишется).
//
// Unix-сокет (T-221, deploy/nginx/README.md §«Вариант unix-сокета»)
// распознаётся положительно — по локальному адресу соединения, который
// http.Server кладёт в контекст: у пира unix-сокета это *net.UnixAddr
// (RemoteAddr при этом «@» автобинда или путь сокета — не IP, а путь с
// двоеточием даже разобрался бы SplitHostPort'ом как host:port).
// Пир unix-сокета — сам nginx на том же хосте: доверенный прокси, как и
// loopback TCP. Адрес посетителя — X-Real-IP, затем X-Forwarded-For
// (запас для конфигураций без X-Real-IP), только валидный net.ParseIP;
// невалидный или отсутствующий — ключ-метка unixLocalKey без записи в
// журнал (inet NOT NULL).
//
// TCP-ветка (T-124) без изменений: за reverse-proxy все соединения
// приходят с loopback nginx — адрес посетителя несёт X-Real-IP (образец
// nginx перезаписывает его значением $remote_addr — подделать снаружи
// нельзя, а сам сервис слушает loopback, прямых соединений с визитором
// нет). Заголовку доверяем только от loopback-пира и только валидному
// IP; иначе — RemoteAddr (подделка заголовка снаружи и с чужих адресов
// не расходует чужой бакет).
func clientIP(r *http.Request) (string, bool) {
	if _, ok := r.Context().Value(http.LocalAddrContextKey).(*net.UnixAddr); ok {
		// Пир unix-сокета: доверенный прокси.
		if pip := proxyHeaderIP(r.Header); pip != "" {
			return pip, true
		}
		return unixLocalKey, false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if pip := net.ParseIP(strings.TrimSpace(r.Header.Get("X-Real-IP"))); pip != nil {
			return pip.String(), true
		}
	}
	return host, net.ParseIP(host) != nil
}

// proxyHeaderIP — адрес посетителя из заголовков прокси (unix-пир):
// X-Real-IP, затем X-Forwarded-For. XFF берётся ПОСЛЕДНЯЯ позиция — её
// дописал наш nginx ($proxy_add_x_forwarded_for, P1-1 ревью T-221):
// начало списка пришло от клиента и не доверено — иначе посетитель
// слал бы каждый запрос с новым значением и обходил токен-бакет.
// Возвращается только валидный net.ParseIP; неверный или отсутствующий
// заголовок — "".
func proxyHeaderIP(h http.Header) string {
	if pip := net.ParseIP(strings.TrimSpace(h.Get("X-Real-IP"))); pip != nil {
		return pip.String()
	}
	if xff := strings.TrimSpace(h.Get("X-Forwarded-For")); xff != "" {
		last := xff[strings.LastIndex(xff, ",")+1:]
		if pip := net.ParseIP(strings.TrimSpace(last)); pip != nil {
			return pip.String()
		}
	}
	return ""
}

func checkWriteError(w http.ResponseWriter, code string, status int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":"` + code + `"}`))
}

// --- token bucket: 4 запроса/мин на IP ---------------------------------------

type tokenBucketLimiter struct {
	rate  int
	per   time.Duration
	mu    sync.Mutex
	state map[string]*bucketState
}

type bucketState struct {
	tokens float64
	last   time.Time
}

func newLimiter(rate int, per time.Duration) *tokenBucketLimiter {
	return &tokenBucketLimiter{rate: rate, per: per, state: make(map[string]*bucketState)}
}

func (l *tokenBucketLimiter) allow(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state[ip]
	if !ok {
		st = &bucketState{tokens: float64(l.rate) - 1, last: now}
		l.state[ip] = st
		// Ленивая уборка: карта растёт с числом уникальных IP; записи
		// старше окна удаляются при проходе (форма — не hot path).
		if len(l.state) > 4096 {
			for k, v := range l.state {
				if now.Sub(v.last) > l.per*4 {
					delete(l.state, k)
				}
			}
		}
		return true
	}
	refill := float64(l.rate) * now.Sub(st.last).Seconds() / l.per.Seconds()
	st.tokens = min(float64(l.rate), st.tokens+refill)
	st.last = now
	if st.tokens < 1 {
		return false
	}
	st.tokens--
	return true
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
