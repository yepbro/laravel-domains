// Проход цепочки редиректов HTTP-этапа (T-048; ТЗ 5.5 пп. 1, 4 и
// ТЗ 5.2 п. 8). Транспорт T-047 редиректов не следует (RoundTrip без
// http.Client, CheckRedirect неприменим) — цепочкой управляет этот файл:
// HTTP-переходы 301/302/303/307/308 и <meta http-equiv="refresh"> у
// 200-ответов считаются вместе против одного max_redirects и одного
// детекта циклов; итоговая страница — последний ответ цепочки.

package httpfetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"crawler/internal/idn"
)

// DefaultMaxRedirects — максимум переходов цепочки (таблица лимитов
// ТЗ 5.5: «макс. число редиректов 8»). Подставляется FetchChain для
// пустых опций; явное значение приходит из http.max_redirects (T-006).
const DefaultMaxRedirects = 8

// DefaultChainTimeout — потолок общего бюджета цепочки редиректов.
// ТЗ 5.5 числом его не задаёт: TotalTimeout (20 с) действует на один
// хоп, а цепочка без общего бюджета тянулась бы (max_redirects хопов ×
// хоп-таймаут, конфиг допускает до 100 переходов) десятки минут, занимая
// слот concurrency воркера и per-IP семафор. Дефолт бюджета — сумма
// бюджетов хопов, но не выше этого потолка: медленные/враждебные сайты
// не могут удерживать проверку домена дольше двух минут.
const DefaultChainTimeout = 2 * time.Minute

// Лимиты сканирования тела на meta refresh. В конфигурации их нет:
// ТЗ 5.5 числом не задаёт — деталь реализации.
const (
	// redirectMetaLimit — сколько байт начала HTML-тела просматривать:
	// <head> с мета-тегами укладывается в десятки килобайт даже с
	// inline-стилями, дальше читать бессмысленно (HTML 4.2.5: meta
	// refresh действует в head). Тз 9 — недоверенный контент
	// ограничивается по размеру и времени, здесь конечны оба.
	redirectMetaLimit = 256 << 10
	// redirectMetaChunk — одно чтение при сканировании.
	redirectMetaChunk = 8 << 10
	// redirectMetaOverlap — перекрытие между кусками: маркер ("<meta",
	// "</head"), разрезанный границей чтения, обязан найтись.
	redirectMetaOverlap = 16
)

// Патерны сканера (байтовые — для EqualFold без конвертаций).
var (
	redirectMetaOpen = []byte("<meta")
	redirectHeadEnd  = []byte("</head")
)

// ErrTooManyRedirects — цепочка прервана: превышен max_redirects или
// обнаружен цикл (ТЗ 5.5 п. 1). Одна классификация на обе причины:
// и лимит, и цикл — постоянное свойство сайта, а не временный сбой,
// повтор проверки исход не изменит, поэтому это НЕ temp_error
// (temp_error зарезервирован за SERVFAIL/таймаутами, ТЗ 6.2): по §8
// «бесконечные редиректы прерываются по лимитам из 5.5, фиксируется
// код ошибки» — потребитель фиксирует код ошибки в результате, а
// финальный статус домена (no_http, если ни один субдомен не ответил)
// решает агрегатор эпизода (T-058, алгоритм §7).
var ErrTooManyRedirects = errors.New("httpfetch: слишком много редиректов")

// RedirectStep — шаг цепочки: URL запроса и статус полученного ответа
// (формат записи subdomain_results.redirects, ТЗ 5.5 п. 7 «цепочка
// редиректов»; jsonb T-058). Последний элемент — итоговая страница.
type RedirectStep struct {
	URL    string `json:"url"`
	Status int    `json:"status"`
}

// ChainResult — итог прохода цепочки. Result — итоговый ответ (конец
// цепочки, ТЗ 5.5 п. 4), nil при ошибке (лимит/цикл/транспорт): Chain
// тогда содержит выполненную часть.
type ChainResult struct {
	Result *FetchResult
	Chain  []RedirectStep
}

// ChainOptions — параметры прохода цепочки.
type ChainOptions struct {
	// MaxRedirects — максимум переходов цепочки: HTTP-редиректы и meta
	// refresh считаются вместе (ТЗ 5.5 п. 1 «учёт»). Семантика
	// http.max_redirects (T-006): 0 — не следовать вовсе (итог — первый
	// ответ), отрицательные значения сводятся к 0. Удобное значение по
	// умолчанию — DefaultMaxRedirects, его подставляет FetchChain.
	MaxRedirects int
	// TotalTimeout — общий бюджет цепочки: родительский контекст для
	// таймаута каждого хопа и чтения итогового тела. 0 — по умолчанию:
	// сумма бюджетов хопов (1+MaxRedirects)×TotalTimeout, не выше
	// DefaultChainTimeout. Явное значение принимается как есть.
	TotalTimeout time.Duration
}

// FetchChain проходит цепочку редиректов от стартового запроса до
// итоговой страницы с лимитом DefaultMaxRedirects (8, таблица лимитов
// ТЗ 5.5). Явный лимит (http.max_redirects) — FetchChainOpts.
func FetchChain(ctx context.Context, f *Fetcher, req FetchRequest) (*ChainResult, error) {
	return FetchChainOpts(ctx, f, req, ChainOptions{MaxRedirects: DefaultMaxRedirects})
}

// FetchChainOpts выполняет стартовый запрос и ведёт цепочку до итоговой
// страницы — «в конце цепочки» (ТЗ 5.5 п. 4): за HTTP-редиректами
// (301/302/303/307/308 с Location; 303 семантически требует GET — все
// запросы цепочки и так GET) и meta refresh 200-ответов; Location и url
// мета-тега разрешаются относительно текущего ответа (net/url), хосты
// приводятся к punycode и нижнему регистру (ТЗ 5.2 п. 8). Схема http↔https
// — обычный шаг цепочки, нестандартные порты сохраняются.
//
// Итог — последний ответ: его статус и тело (Body — читатель; лимит
// чтения и защита от тарпитов — T-049) в ChainResult.Result, история —
// Chain ([]RedirectStep: URL запроса и статус каждого ответа).
//
// Ошибки: транспортная ошибка любого шага возвращается как есть
// (классификация IsTimeout → temp_error за потребителем, ТЗ 6.2);
// превышение лимита и цикл — обёртки ErrTooManyRedirects (Result nil,
// Chain — выполненная часть); резолв нового хоста — ошибка резолвера.
// Адреса DNS-этапа действуют только на стартовый хост (повторное его
// появление в цепочке переиспользует их, ТЗ 5.5 п. 2 «подключение к
// ранее резолвленному IP»); прочие хосты резолвит резолвер транспорта —
// «через тот же резолвер с кэшем» (обёртка CachingResolver T-047 в
// wiring воркера).
func FetchChainOpts(ctx context.Context, f *Fetcher, req FetchRequest, opts ChainOptions) (*ChainResult, error) {
	if req.URL == nil {
		return nil, errors.New("httpfetch: запрос без URL")
	}
	max := opts.MaxRedirects
	if max < 0 {
		max = 0
	}

	// Стартовый URL — тем же конвейером, что и переходы: punycode-хост,
	// явный путь. Контракт задачи q:http даёт punycode, нормализация
	// страхует от неожиданного unicode.
	start, ok := redirectResolve(req.URL, req.URL.String())
	if !ok {
		return nil, fmt.Errorf("httpfetch: стартовый URL %s: схема или хост не нормализуются", req.URL)
	}
	startHost := start.Hostname()
	visited := map[string]struct{}{redirectKey(start): {}}
	cur := FetchRequest{URL: start, Addrs: req.Addrs}

	// Общий бюджет цепочки: родительский контекст для всех хопов (свой
	// TotalTimeout у каждого Fetch остаётся внутри). Отмена бюджета в
	// полёте закрывает соединение хопа — ошибка тайм-класса
	// (temp_error у потребителя). Итоговое тело читается потребителем
	// уже после возврата: бюджет живёт до закрытия тела (chainBody),
	// в ошибочных путях снимается здесь.
	budget := opts.TotalTimeout
	if budget <= 0 {
		budget = time.Duration(1+max) * f.opt.TotalTimeout
		if budget > DefaultChainTimeout {
			budget = DefaultChainTimeout
		}
	}
	ctx, cancelBudget := context.WithTimeout(ctx, budget)

	var chain []RedirectStep
	redirects := 0
	for {
		res, err := f.Fetch(ctx, cur)
		if err != nil {
			cancelBudget()
			return &ChainResult{Chain: chain}, err
		}
		chain = append(chain, RedirectStep{URL: cur.URL.String(), Status: res.StatusCode})

		next, follow := redirectNext(res)
		if !follow || max == 0 {
			// Конец цепочки: итог — этот ответ. max_redirects = 0 —
			// режим «не следовать» (T-006): первый ответ итоговый.
			res.Body = &chainBody{rc: res.Body, cancel: cancelBudget}
			return &ChainResult{Result: res, Chain: chain}, nil
		}
		_ = res.Body.Close()
		if redirects >= max {
			cancelBudget()
			return &ChainResult{Chain: chain}, fmt.Errorf(
				"%w: лимит %d превышен на переходе к %s", ErrTooManyRedirects, max, next)
		}
		if _, dup := visited[redirectKey(next)]; dup {
			cancelBudget()
			return &ChainResult{Chain: chain}, fmt.Errorf("%w: цикл на %s", ErrTooManyRedirects, next)
		}
		visited[redirectKey(next)] = struct{}{}
		redirects++

		addrs := req.Addrs
		if host := next.Hostname(); host != startHost {
			// Новый хост: адреса DNS-этапа на него не действуют (они
			// выданы стартовому хосту) — резолв резолвером транспорта.
			addrs, err = f.lookup(ctx, host)
			if err != nil {
				cancelBudget()
				return &ChainResult{Chain: chain}, err
			}
		}
		cur = FetchRequest{URL: next, Addrs: addrs}
	}
}

// redirectIsHTTPStatus — коды, за которыми следуют (ТЗ 5.5 п. 1);
// прочие 3xx (300, 304, …) переходами не считаются.
func redirectIsHTTPStatus(code int) bool {
	switch code {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}

// redirectNext определяет следующий URL цепочки по ответу. Второе
// значение false — ответ итоговый. Ответ с битым или чужим Location
// (не URL, схема не http/https, невалидный хост) не следуется: итог —
// сам ответ, как браузер без перехода. У 200-ответа HTML сканируется
// тело (и оборачивается redirectBody — байты не теряются).
func redirectNext(res *FetchResult) (*url.URL, bool) {
	if redirectIsHTTPStatus(res.StatusCode) {
		raw := strings.TrimSpace(res.Header.Get("Location"))
		if raw == "" {
			return nil, false
		}
		return redirectResolve(res.FinalURL, raw)
	}
	if res.StatusCode == http.StatusOK {
		ct := strings.ToLower(res.Header.Get("Content-Type"))
		if !strings.Contains(ct, "html") {
			return nil, false // meta refresh — поведение HTML-документа
		}
		raw, ok := redirectScanMeta(res)
		if !ok {
			return nil, false
		}
		return redirectResolve(res.FinalURL, raw)
	}
	return nil, false
}

// redirectResolve строит URL следующего запроса из сырой строки
// Location/meta refresh: относительные ссылки разрешаются по базе
// текущего ответа (net/url), хост приводится к punycode (ТЗ 5.2 п. 8 —
// «хосты из Location и <meta refresh>, содержащие unicode, приводятся
// к punycode перед запросом») и нижнему регистру, пустой путь
// нормализуется в "/". IP-литералы (IPv4/IPv6) HostToASCII не нужны и
// IPv6 на нём падает (T-009) — проходят как есть: url.URL.Hostname()
// отдаёт литерал без скобок, готовый к диалу. false — по строке
// нельзя идти: не URL, чужая схема, нет хоста или невалидное имя
// (битый punycode, запрещённые UTS-46 символы).
func redirectResolve(base *url.URL, raw string) (*url.URL, bool) {
	ref, err := url.Parse(raw)
	if err != nil {
		return nil, false
	}
	u := base.ResolveReference(ref)
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, false
	}
	host := u.Hostname()
	if host == "" {
		return nil, false
	}
	if _, err := netip.ParseAddr(host); err != nil {
		// Имя (не IP-литерал): punycode и нижний регистр.
		ascii, err := idn.HostToASCII(host)
		if err != nil {
			return nil, false
		}
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort(ascii, port)
		} else {
			u.Host = ascii
		}
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u, true
}

// redirectKey — ключ детекта циклов (ТЗ 5.5 п. 1): нормализованный URL
// без фрагмента — фрагмент на сервер не отправляется и переходом не
// является. Query учитывается: редиректы со счётчиком (?i=1, ?i=2, …) —
// легитимные разные страницы, не цикл; точное повторение URL с query —
// цикл; «бесконечные» параметрические цепочки останавливает max_redirects.
func redirectKey(u *url.URL) string {
	k := *u
	k.Fragment, k.RawFragment = "", ""
	return k.String()
}

// redirectScanMeta сканирует начало HTML-тела 200-ответа на meta refresh
// (ТЗ 5.5 п. 4). Возвращает URL перехода, если найден (первый валидный
// тег с url; refresh самого себя без url игнорируется). Прочитанный при
// сканировании префикс тела возвращается потребителю обёрткой
// redirectBody — итог без перехода не теряет байтов. Скан конечен:
// redirectMetaLimit байт и закрывающий </head> (meta refresh после конца
// head недействителен, HTML 4.2.5). Парсер минимальный строковый —
// полный HTML-парсер для одного тега избыточен и тяжёл (§9: недоверенный
// контент — только ограниченные парсеры).
func redirectScanMeta(res *FetchResult) (string, bool) {
	var buf bytes.Buffer
	chunk := make([]byte, redirectMetaChunk)
	pos, total := 0, 0
	for total < redirectMetaLimit {
		n, rerr := res.Body.Read(chunk)
		if n > 0 {
			total += n
			buf.Write(chunk[:n])
			var raw string
			var stop bool
			pos, raw, stop = redirectScanBuffer(buf.Bytes(), pos)
			if raw != "" {
				// Переход найден, но может не состояться: url чужой
				// схемы (redirectResolve вернёт false) или режим
				// «не следовать» у вызывающего. Прочитанный сканом
				// префикс возвращается потребителю обёрткой — итог
				// без перехода не теряет байтов и в этом пути.
				if buf.Len() > 0 {
					res.Body = &redirectBody{prefix: bytes.NewReader(buf.Bytes()), rc: res.Body}
				}
				return raw, true // переход: тело закроет вызывающий
			}
			if stop {
				break
			}
		}
		if rerr != nil {
			break // EOF (тело короче лимита) или ошибка чтения — скан окончен
		}
	}
	if buf.Len() > 0 {
		res.Body = &redirectBody{prefix: bytes.NewReader(buf.Bytes()), rc: res.Body}
	}
	return "", false
}

// redirectScanBuffer — проход сканера по накопленному буферу с позиции
// pos: ищет теги <meta (регистр не важен) до закрывающего </head>.
// Возвращает новую позицию, URL перехода ("" — не найден) и признак
// «дальше не искать» (</head раньше очередного <meta>).
func redirectScanBuffer(b []byte, pos int) (int, string, bool) {
	for {
		m := redirectIndexFold(b, pos, redirectMetaOpen)
		h := redirectIndexFold(b, pos, redirectHeadEnd)
		if h >= 0 && (m < 0 || h < m) {
			return len(b), "", true
		}
		if m < 0 {
			// Маркер может быть разрезан границей чтения — перекрытие.
			if p := len(b) - redirectMetaOverlap; p > pos {
				return p, "", false
			}
			return pos, "", false
		}
		end := redirectTagEnd(b, m)
		if end < 0 {
			return pos, "", false // тег не дочитан — ждём следующих байтов
		}
		if raw, ok := redirectMetaURL(b, m, end); ok {
			return end + 1, raw, false
		}
		pos = end + 1
	}
}

// redirectIndexFold — индекс первого вхождения pat (без учёта регистра)
// в b[from:] или -1.
func redirectIndexFold(b []byte, from int, pat []byte) int {
	for i := from; i+len(pat) <= len(b); i++ {
		if bytes.EqualFold(b[i:i+len(pat)], pat) {
			return i
		}
	}
	return -1
}

// redirectTagEnd — индекс '>' тега, начинающегося в b[start] (учёт
// кавычек значений: '>' внутри кавычек тег не закрывает), или -1.
func redirectTagEnd(b []byte, start int) int {
	var quote byte
	for i := start + 1; i < len(b); i++ {
		c := b[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '>':
			return i
		}
	}
	return -1
}

// redirectMetaURL извлекает URL перехода из тега в диапазоне
// [tagStart, tagEnd) — имени с атрибутами, без угловых скобок.
// Регистр имён атрибутов и http-equiv не важен; значения — в кавычках
// ('…' или "…") или без. false — не refresh, нет контента или контент
// без url (refresh самого себя — игнор).
func redirectMetaURL(b []byte, tagStart, tagEnd int) (string, bool) {
	p := tagStart + len(redirectMetaOpen)
	if p >= tagEnd {
		return "", false // "<meta>" без атрибутов
	}
	if c := b[p]; !redirectIsSpace(c) && c != '/' {
		return "", false // "<metadata" и подобные — не meta
	}
	equiv, content := redirectTagAttrs(b, p, tagEnd)
	if !strings.EqualFold(equiv, "refresh") {
		return "", false
	}
	return redirectMetaContent(content)
}

// redirectTagAttrs разбирает атрибуты тега (диапазон [p, end)) простым
// сканом «имя[=значение]», разделённые пробелами; значение — в кавычках
// или до пробела. Возвращает значения http-equiv и content (пустые
// строки — атрибут отсутствует).
func redirectTagAttrs(b []byte, p, end int) (equiv, content string) {
	for p < end {
		for p < end && (redirectIsSpace(b[p]) || b[p] == '/') {
			p++
		}
		if p >= end {
			break
		}
		ns := p
		for p < end && b[p] != '=' && !redirectIsSpace(b[p]) {
			p++
		}
		name := strings.ToLower(string(b[ns:p]))
		vs, ve := p, p
		if p < end && b[p] == '=' {
			p++
			for p < end && redirectIsSpace(b[p]) {
				p++
			}
			if p < end && (b[p] == '\'' || b[p] == '"') {
				q := b[p]
				p++
				vs = p
				for p < end && b[p] != q {
					p++
				}
				ve = p
				if p < end {
					p++ // закрывающая кавычка
				}
			} else {
				vs = p
				for p < end && !redirectIsSpace(b[p]) {
					p++
				}
				ve = p
			}
		}
		switch name {
		case "http-equiv":
			equiv = string(b[vs:ve])
		case "content":
			content = string(b[vs:ve])
		}
	}
	return equiv, content
}

// redirectIsSpace — пробельные символы разметки.
func redirectIsSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	}
	return false
}

// redirectMetaContent разбирает значение content: "N; url=X" (N — целые
// секунды задержки). Задержку краулер не ждёт: и 0 (немедленный переход),
// и N > 0 ведут на один и тот же итоговый адрес в конце цепочки (ТЗ 5.5
// п. 4 — краулер не браузер, «дождаться» счётчик не может). Без части
// url — refresh самого себя: игнор (X нет). Формы без числа ("url=/x")
// и с битым/отрицательным числом невалидны по HTML и не обрабатываются.
func redirectMetaContent(content string) (string, bool) {
	left, rest, hasSemi := strings.Cut(content, ";")
	left = strings.TrimSpace(left)
	if left == "" || strings.HasPrefix(left, "-") {
		return "", false
	}
	if _, err := strconv.Atoi(left); err != nil {
		return "", false
	}
	if !hasSemi {
		return "", false // "5" — обновление самого себя
	}
	eq := strings.Index(rest, "=")
	if eq < 0 {
		return "", false
	}
	if name := strings.TrimSpace(rest[:eq]); !strings.EqualFold(name, "url") {
		return "", false
	}
	v := strings.TrimSpace(rest[eq+1:])
	if len(v) >= 2 && (v[0] == '\'' || v[0] == '"') && v[len(v)-1] == v[0] {
		v = strings.TrimSpace(v[1 : len(v)-1]) // парные кавычки вокруг URL
	}
	if v == "" {
		return "", false
	}
	return v, true
}

// redirectBody — тело итогового ответа после сканирования на meta
// refresh: сначала прочитанный префикс, затем исходный поток; Close
// закрывает исходный поток (с его таймером TotalTimeout — cancelBody
// T-047).
type redirectBody struct {
	prefix *bytes.Reader
	rc     io.ReadCloser
}

func (b *redirectBody) Read(p []byte) (int, error) {
	if b.prefix.Len() > 0 {
		return b.prefix.Read(p)
	}
	return b.rc.Read(p)
}

func (b *redirectBody) Close() error { return b.rc.Close() }

// chainBody — тело итогового ответа под бюджетом цепочки: Close тела
// останавливает бюджет (общий ctx цепочки), как cancelBody останавливает
// общий таймаут одного запроса (T-047). Чтение итогового тела идёт уже
// после возврата FetchChainOpts — бюджет живёт до закрытия тела.
type chainBody struct {
	rc     io.ReadCloser
	cancel context.CancelFunc
}

func (b *chainBody) Read(p []byte) (int, error) { return b.rc.Read(p) }

func (b *chainBody) Close() error {
	err := b.rc.Close()
	b.cancel()
	return err
}
