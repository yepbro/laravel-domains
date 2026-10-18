// Съём результата рендера (T-064; ТЗ 5.6 «Результат: итоговый DOM
// (document.documentElement.outerHTML) и все cookies (включая выставленные
// JS) через CDP», критерий приёмки §11.4 п. 3) и заголовки ответа основной
// навигации. Заголовки — требование ТЗ 5.8 (правила по заголовкам): у
// отрендеренной страницы итоговый документ не обязан совпадать с тем, что
// снял HTTP-этап, поэтому рендер обязан отдать свои.
//
// Выбор способов съёма:
//
//   - DOM — chromedp.Evaluate(`document.documentElement.outerHTML`):
//     сериализация живого дерева после дорисовки JS (внесённые скриптами
//     элементы, атрибуты и тексты входят в результат), формат — тот же
//     HTML-сериализатор Chrome, что отдаёт браузер при сохранении страницы,
//     то есть текстуальный HTML. Результат напрямую ложится в хранилище
//     контента (замена сырого HTML, T-067) и в матчер правил по HTML/meta
//     (T-068) без преобразований. Альтернатива DOM.getFlattenedDocument —
//     размеченное дерево узлов с nodeId, которое пришлось бы обратно
//     сериализовывать (DOM.getOuterHTML по узлам) — сложнее и не даёт
//     готового HTML. Ограничение outerHTML — shadow-деревья не
//     раскрываются, но правила Laravel работают по светлому DOM.
//
//   - cookies — Network.getCookies (CDP) с URL итоговой страницы: команда
//     читает хранилище cookies изолированного контекста, а не
//     document.cookie — видны httpOnly и выставленные страницей через JS
//     (ТЗ 5.6 «все cookies»; document.cookie httpOnly не отдаёт).
//     Упомянутый в декомпозиции Network.getAllCookies из актуальной версии
//     протокола удалён (в cdproto v0.157.8 команды нет); Network.getCookies
//     над сессией вкладки читает то же хранилище контекста, а явный URL
//     итоговой страницы даёт набор «cookies этой страницы» без subframe-шума.
//
//   - заголовки — ответ основной навигации из Network.responseReceived:
//     chromedp.NavigateResponse (вместо равного ему по ожиданию Navigate)
//     отслеживает цепочку loader/request через HTTP-редиректы и возвращает
//     ответ итогового документа. Отдельная подписка на события не нужна.
//     Речь об ответе самой навигации: JS-редирект после загрузки уже не
//     отслеживается (его итог виден в FinalURL, а заголовки — документа,
//     чей контент отрисован). Заголовки из responseReceivedExtraInfo
//     (report-only набор) не запрашиваются: для правил ТЗ 5.8 достаточно
//     основного набора.
package render

import (
	"context"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// extractDOMExpr — выражение съёма итогового DOM (ТЗ 5.6): outerHTML
// корневого элемента — полное дерево после дорисовки JS.
const extractDOMExpr = `document.documentElement.outerHTML`

// extractDOMTruncated — признак обрезки сверхлимитного DOM (R-02):
// HTML-комментарий в конце документа — анализатор (правила по HTML)
// и оператор видят, что дерево снято не целиком.
const extractDOMTruncated = "<!-- crawler: dom truncated -->"

// extractDOMExprLimited — выражение съёма с потолком длины: тяжёлая или
// враждебная страница (SPA-монстр, раздутый JS) не тянет через CDP строку
// сверх лимита — обрезка происходит в самом браузере. substring считает
// UTF-16 единицы, поэтому точный байтовый предел дожимается в Go
// (truncateDOM).
func extractDOMExprLimited(limit int) string {
	if limit <= 0 {
		return extractDOMExpr
	}
	return extractDOMExpr + fmt.Sprintf(".substring(0, %d)", limit)
}

// truncateDOM — жёсткий байтовый предел итогового DOM в памяти воркера:
// при MemoryMax=128M и render.contexts=3 неограниченный DOM (плюс
// transient-буфер chromedp и копии хэша/Put) выбивает cgroup-бюджет
// OOM-kill'ом. Сверх лимита DOM обрезается и получает признак обрезки;
// граница среза откатывается на начало многобайтного символа.
func truncateDOM(dom string, limit int) string {
	if limit <= 0 || len(dom) <= limit {
		return dom
	}
	cut := limit - len(extractDOMTruncated)
	for cut > 0 && !utf8.RuneStart(dom[cut]) {
		cut-- // не рвать многобайтный символ посередине
	}
	if cut <= 0 {
		return dom[:limit] // вырожденный лимит меньше признака обрезки
	}
	return dom[:cut] + extractDOMTruncated
}

// ExtractCookie — cookie итоговой страницы в формате колонки
// subdomain_results.cookies (jsonb). Форма повторяет запись HTTP-этапа
// (recordCookie, T-058) — единый контракт хранения: name/value — цели правил
// анализатора (ТЗ 5.8), атрибуты — те же имена JSON-полей. max_age рендером
// не заполняется: хранилище браузера (откуда читает CDP) держит только
// абсолютный срок, относительный Max-Age теряется при сохранении cookie;
// поле оставлено, чтобы форма записи совпадала с HTTP-этапом полностью.
type ExtractCookie struct {
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	Path     string `json:"path,omitempty"`
	Domain   string `json:"domain,omitempty"`
	Expires  string `json:"expires,omitempty"` // RFC 3339, UTC
	MaxAge   int    `json:"max_age,omitempty"`
	Secure   bool   `json:"secure,omitempty"`
	HTTPOnly bool   `json:"http_only,omitempty"`
	SameSite string `json:"same_site,omitempty"`
}

// extractSameSite — имя атрибута SameSite в кодировке HTTP-этапа
// (recordSameSite, T-058): не выставлен — пустая строка (omitempty).
func extractSameSite(s network.CookieSameSite) string {
	switch s {
	case network.CookieSameSiteLax:
		return "lax"
	case network.CookieSameSiteStrict:
		return "strict"
	case network.CookieSameSiteNone:
		return "none"
	default:
		return ""
	}
}

// extractCookieFields — cookie CDP в ExtractCookie. Срок у CDP абсолютный
// (unix-секунды): session-cookie (без срока) остаётся без expires,
// отрицательное значение — то же отсутствие срока.
func extractCookieFields(c *network.Cookie) ExtractCookie {
	ec := ExtractCookie{
		Name:     c.Name,
		Value:    c.Value,
		Domain:   c.Domain,
		Path:     c.Path,
		Secure:   c.Secure,
		HTTPOnly: c.HTTPOnly,
		SameSite: extractSameSite(c.SameSite),
	}
	if !c.Session && c.Expires > 0 {
		ec.Expires = time.Unix(int64(c.Expires), 0).UTC().Format(time.RFC3339)
	}
	return ec
}

// extractCookiesFromCDP — cookies CDP в запись рендера с детерминированным
// порядком (name, domain, path): jsonb-набор стабилен независимо от
// порядка cookie в ответе хранилища. Пустой набор — nil (NULL в jsonb).
func extractCookiesFromCDP(cs []*network.Cookie) []ExtractCookie {
	if len(cs) == 0 {
		return nil
	}
	out := make([]ExtractCookie, 0, len(cs))
	for _, c := range cs {
		out = append(out, extractCookieFields(c))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if out[i].Domain != out[j].Domain {
			return out[i].Domain < out[j].Domain
		}
		return out[i].Path < out[j].Path
	})
	return out
}

// extractResponseHeaders — заголовки ответа основной навигации в
// http.Header: единый контракт с HTTP-этапом (subdomain_results.headers,
// T-058 — канонические имена ключей, значения списком). CDP отдаёт
// заголовки объектом имя→значение (значения почти всегда строки, числа
// редки); повторявшиеся заголовки (в первую очередь Set-Cookie) приходят
// массивом — массив сохраняется поэлементно, как у HTTP-этапа: склейка
// через запятую необратима (значения cookie содержат запятые —
// Expires=Wed, 21 Oct 2025 07:28:00 GMT) и расходится с формой jsonb,
// которую читает анализатор (map[string][]string, T-058/T-069).
func extractResponseHeaders(h network.Headers) http.Header {
	if len(h) == 0 {
		return nil
	}
	out := make(http.Header, len(h))
	for k, v := range h {
		out[textproto.CanonicalMIMEHeaderKey(k)] = extractHeaderValues(v)
	}
	return out
}

// extractHeaderValues — значения заголовка из CDP списком: одиночное
// значение (string/число) — срез из одного элемента, массив — поэлементно
// (контракт «значения списком» T-058; рендер повторяет запись HTTP-этапа,
// где повторяющиеся заголовки — отдельные элементы).
func extractHeaderValues(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return []string{extractHeaderString(v)}
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		out = append(out, extractHeaderString(item))
	}
	return out
}

// extractHeaderString — значение заголовка из CDP в строку (json-декодер
// даёт string/float64; прочие формы CDP не отдаёт — запасной Sprintf).
func extractHeaderString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// extractCDP — команды CDP съёма результата; структура вместо пакета, чтобы
// юнит-тесты подменяли поля фейками (тот же приём, что probeFn у Session).
type extractCDP struct {
	// dom — вычисление выражения итогового DOM в контексте вкладки.
	dom func(ctx context.Context) (string, error)
	// cookies — cookies хранилища контекста, применимые к URL.
	cookies func(ctx context.Context, pageURL string) ([]*network.Cookie, error)
	// domLimit — потолок длины DOM в байтах, 0 — без лимита: выражение
	// съёма и truncateDOM (R-02).
	domLimit int
}

// newExtractCDP — реальные команды CDP (выполняются над контекстом вкладки
// сессии); domLimit — потолок итогового DOM (Options.MaxDOMBytes).
func newExtractCDP(domLimit int) extractCDP {
	return extractCDP{
		domLimit: domLimit,
		dom: func(ctx context.Context) (string, error) {
			return chromedp.Run(ctx, chromedp.Evaluate[string](extractDOMExprLimited(domLimit)))
		},
		cookies: func(ctx context.Context, pageURL string) ([]*network.Cookie, error) {
			var out []*network.Cookie
			err := chromedp.Do(ctx, chromedp.Func(func(c context.Context, t *chromedp.Target) error {
				res, err := cdp.Call(c, t, network.GetCookies, network.GetCookiesParams{
					URLs: []string{pageURL},
				})
				out = res.Cookies
				return err
			}))
			return out, err
		},
	}
}

// extractResult — финальный этап Session.Run (T-064): итоговый DOM и все
// cookies итоговой страницы в out. DOM обязателен (без него результат не
// нужен) и ограничен потолком Options.MaxDOMBytes (обрезка с признаком,
// R-02); cookies снимаются по FinalURL; для схем без cookie-хранилища
// (data:, about:) набора не существует — шаг пропускается без ошибки.
func (e extractCDP) extractResult(ctx context.Context, out *RenderOutput) error {
	dom, err := e.dom(ctx)
	if err != nil {
		return fmt.Errorf("итоговый DOM (%s): %w", extractDOMExpr, err)
	}
	out.DOM = truncateDOM(dom, e.domLimit)

	if u, perr := url.Parse(out.FinalURL); perr == nil &&
		(u.Scheme == "http" || u.Scheme == "https") {
		cs, err := e.cookies(ctx, out.FinalURL)
		if err != nil {
			return fmt.Errorf("cookies итоговой страницы %s: %w", out.FinalURL, err)
		}
		out.Cookies = extractCookiesFromCDP(cs)
	}
	return nil
}
