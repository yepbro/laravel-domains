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
	"strings"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// extractDOMExpr — выражение съёма итогового DOM (ТЗ 5.6): outerHTML
// корневого элемента — полное дерево после дорисовки JS.
const extractDOMExpr = `document.documentElement.outerHTML`

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
// редки); повторявшиеся заголовки браузер к этому моменту объединяет в одну
// строку — так и записывается.
func extractResponseHeaders(h network.Headers) http.Header {
	if len(h) == 0 {
		return nil
	}
	out := make(http.Header, len(h))
	for k, v := range h {
		out[textproto.CanonicalMIMEHeaderKey(k)] = []string{extractHeaderString(v)}
	}
	return out
}

// extractHeaderString — значение заголовка из CDP в строку (json-декодер
// даёт string/float64/[]any).
func extractHeaderString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	case []any:
		parts := make([]string, 0, len(x))
		for _, item := range x {
			parts = append(parts, extractHeaderString(item))
		}
		return strings.Join(parts, ", ")
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
}

// newExtractCDP — реальные команды CDP (выполняются над контекстом вкладки
// сессии).
func newExtractCDP() extractCDP {
	return extractCDP{
		dom: func(ctx context.Context) (string, error) {
			return chromedp.Run(ctx, chromedp.Evaluate[string](extractDOMExpr))
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
// нужен), cookies снимаются по FinalURL; для схем без cookie-хранилища
// (data:, about:) набора не существует — шаг пропускается без ошибки.
func (e extractCDP) extractResult(ctx context.Context, out *RenderOutput) error {
	dom, err := e.dom(ctx)
	if err != nil {
		return fmt.Errorf("итоговый DOM (%s): %w", extractDOMExpr, err)
	}
	out.DOM = dom

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
