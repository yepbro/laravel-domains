package render

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/network"
)

// Проверки T-064 без живого Chromium: преобразования чистые (cookie CDP →
// запись рендера, заголовки CDP → http.Header), оркестрация съёма — на
// фейках extractCDP (приём probeFn у Session). Живые проверки съёма на
// реальном Chromium — extract_live_test.go.

func TestExtractCookieFields(t *testing.T) {
	// Полный набор атрибутов: срок абсолютный → RFC 3339 UTC, флаги и
	// SameSite сохраняются.
	got := extractCookieFields(&network.Cookie{
		Name:     "XSRF-TOKEN",
		Value:    "eyJpdiI6",
		Domain:   ".example.com",
		Path:     "/",
		Expires:  1792567680, // 2026-10-21T07:28:00Z
		HTTPOnly: true,
		Secure:   true,
		SameSite: network.CookieSameSiteLax,
	})
	want := ExtractCookie{
		Name: "XSRF-TOKEN", Value: "eyJpdiI6", Domain: ".example.com",
		Path: "/", Expires: "2026-10-21T07:28:00Z",
		HTTPOnly: true, Secure: true, SameSite: "lax",
	}
	if got != want {
		t.Fatalf("cookie:\n got %+v\nwant %+v", got, want)
	}

	cases := []struct {
		name string
		in   network.Cookie
		want ExtractCookie
	}{
		{
			name: "session-cookie без срока",
			in:   network.Cookie{Name: "a", Value: "1", Session: true, Expires: -1},
			want: ExtractCookie{Name: "a", Value: "1"},
		},
		{
			name: "отрицательный срок — тоже без expires",
			in:   network.Cookie{Name: "a", Value: "1", Expires: -1},
			want: ExtractCookie{Name: "a", Value: "1"},
		},
		{
			name: "samesite strict",
			in:   network.Cookie{Name: "a", SameSite: network.CookieSameSiteStrict},
			want: ExtractCookie{Name: "a", SameSite: "strict"},
		},
		{
			name: "samesite none",
			in:   network.Cookie{Name: "a", SameSite: network.CookieSameSiteNone},
			want: ExtractCookie{Name: "a", SameSite: "none"},
		},
		{
			name: "samesite не выставлен",
			in:   network.Cookie{Name: "a"},
			want: ExtractCookie{Name: "a"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractCookieFields(&tc.in); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestExtractCookiesFromCDP(t *testing.T) {
	if got := extractCookiesFromCDP(nil); got != nil {
		t.Fatalf("пустой набор: %+v, хочу nil (NULL в jsonb)", got)
	}
	// Порядок детерминирован (name, domain, path) независимо от прихода.
	got := extractCookiesFromCDP([]*network.Cookie{
		{Name: "b", Domain: ".example.com", Path: "/"},
		{Name: "a", Domain: ".other.com", Path: "/"},
		{Name: "a", Domain: ".example.com", Path: "/deep"},
		{Name: "a", Domain: ".example.com", Path: "/"},
	})
	want := []ExtractCookie{
		{Name: "a", Domain: ".example.com", Path: "/"},
		{Name: "a", Domain: ".example.com", Path: "/deep"},
		{Name: "a", Domain: ".other.com", Path: "/"},
		{Name: "b", Domain: ".example.com", Path: "/"},
	}
	if len(got) != len(want) {
		t.Fatalf("размер %d, хочу %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("позиция %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestExtractCookieJSONContract — форма jsonb-записи совпадает с HTTP-этапом
// (recordCookie, T-058): имена полей snake_case, порядок полей записи,
// omitempty пустых атрибутов.
func TestExtractCookieJSONContract(t *testing.T) {
	full := []ExtractCookie{{
		Name: "XSRF-TOKEN", Value: "eyJpdiI6", Domain: ".example.com",
		Path: "/", Expires: "2026-10-21T07:28:00Z",
		HTTPOnly: true, Secure: true, SameSite: "lax",
	}}
	got, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `[{"name":"XSRF-TOKEN","value":"eyJpdiI6","path":"/",` +
		`"domain":".example.com","expires":"2026-10-21T07:28:00Z","secure":true,` +
		`"http_only":true,"same_site":"lax"}]`
	if string(got) != want {
		t.Fatalf("json:\n got  %s\nwant %s", got, want)
	}

	minimal := []ExtractCookie{{Name: "a", Value: "1"}}
	got, err = json.Marshal(minimal)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(got) != `[{"name":"a","value":"1"}]` {
		t.Fatalf("минимальная запись: %s", got)
	}
}

func TestExtractResponseHeaders(t *testing.T) {
	if got := extractResponseHeaders(nil); got != nil {
		t.Fatalf("nil-заголовки: %+v, хочу nil", got)
	}
	if got := extractResponseHeaders(network.Headers{}); got != nil {
		t.Fatalf("пустые заголовки: %+v, хочу nil", got)
	}

	got := extractResponseHeaders(network.Headers{
		"content-type":   "text/html; charset=utf-8",
		"X-Powered-By":   "PHP/8.3",
		"content-length": float64(1234),
		"set-cookie":     []any{"a=1", "b=2"},
	})
	want := http.Header{
		"Content-Type":   []string{"text/html; charset=utf-8"},
		"X-Powered-By":   []string{"PHP/8.3"},
		"Content-Length": []string{"1234"},
		"Set-Cookie":     []string{"a=1, b=2"},
	}
	if len(got) != len(want) {
		t.Fatalf("размер %d, хочу %d: %+v", len(got), len(want), got)
	}
	for k, vs := range want {
		if g := got.Get(k); g != vs[0] {
			t.Errorf("%s = %q, хочу %q", k, g, vs[0])
		}
	}
}

// fakeExtractCDP — extractCDP с фейками команд и учётом вызовов.
type fakeExtractCDP struct {
	dom     string
	domErr  error
	domSeen bool

	cookies    []*network.Cookie
	cookiesErr error
	cookieSeen bool
}

func (f *fakeExtractCDP) cdp() extractCDP {
	return extractCDP{
		dom: func(context.Context) (string, error) {
			f.domSeen = true
			return f.dom, f.domErr
		},
		cookies: func(_ context.Context, _ string) ([]*network.Cookie, error) {
			f.cookieSeen = true
			return f.cookies, f.cookiesErr
		},
	}
}

func TestExtractResult(t *testing.T) {
	t.Run("DOM и cookies итоговой страницы", func(t *testing.T) {
		f := &fakeExtractCDP{
			dom:     "<html><body>итог</body></html>",
			cookies: []*network.Cookie{{Name: "js", Value: "1", HTTPOnly: true}},
		}
		out := RenderOutput{FinalURL: "https://example.com/final"}
		if err := f.cdp().extractResult(context.Background(), &out); err != nil {
			t.Fatalf("extractResult: %v", err)
		}
		if out.DOM != f.dom {
			t.Errorf("DOM %q", out.DOM)
		}
		if len(out.Cookies) != 1 || out.Cookies[0] != (ExtractCookie{Name: "js", Value: "1", HTTPOnly: true}) {
			t.Errorf("cookies %+v", out.Cookies)
		}
	})

	t.Run("пустая страница — пустой DOM без ошибки", func(t *testing.T) {
		f := &fakeExtractCDP{}
		out := RenderOutput{FinalURL: "https://example.com/"}
		if err := f.cdp().extractResult(context.Background(), &out); err != nil {
			t.Fatalf("extractResult: %v", err)
		}
		if out.DOM != "" {
			t.Errorf("DOM %q, хочу пустой", out.DOM)
		}
	})

	t.Run("ошибка evaluate", func(t *testing.T) {
		f := &fakeExtractCDP{domErr: errors.New("сбой вкладки")}
		out := RenderOutput{FinalURL: "https://example.com/"}
		err := f.cdp().extractResult(context.Background(), &out)
		if err == nil || !strings.Contains(err.Error(), "итоговый DOM") {
			t.Fatalf("ошибка: %v", err)
		}
		if !errors.Is(err, f.domErr) {
			t.Errorf("исходная ошибка не пробита: %v", err)
		}
		if f.cookieSeen {
			t.Error("после сбоя DOM cookies не запрашиваются")
		}
	})

	t.Run("ошибка cookies", func(t *testing.T) {
		f := &fakeExtractCDP{cookiesErr: errors.New("cookie store недоступен")}
		out := RenderOutput{FinalURL: "https://example.com/"}
		err := f.cdp().extractResult(context.Background(), &out)
		if err == nil || !strings.Contains(err.Error(), "cookies") {
			t.Fatalf("ошибка: %v", err)
		}
	})

	t.Run("без http(s) cookies не запрашиваются", func(t *testing.T) {
		for _, final := range []string{"data:text/html,hello", "about:blank"} {
			f := &fakeExtractCDP{cookies: []*network.Cookie{{Name: "x"}}}
			out := RenderOutput{FinalURL: final}
			if err := f.cdp().extractResult(context.Background(), &out); err != nil {
				t.Fatalf("%s: extractResult: %v", final, err)
			}
			if f.cookieSeen {
				t.Errorf("%s: cookies запрошены для схемы без хранилища", final)
			}
			if out.Cookies != nil {
				t.Errorf("%s: cookies %+v, хочу nil", final, out.Cookies)
			}
		}
	})
}
