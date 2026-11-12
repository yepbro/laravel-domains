// SPA shell heuristic tests (T-057; spec 5.5 item 8, spec 13.2).
// A pure function: the inputs are typical framework shells, classic
// pages, and the decision table combinations; separate checks — the
// visible text threshold, attribute spelling variants, the negative
// forms of a JS redirect, non-HTML junk, and the scan's zero
// allocations.

package httpfetch

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

// --- helpers -----------------------------------------------------------------

// spaTablePage is a page with controlled signs: visible is the exact
// number of visible characters (the text of a single <p>; no other
// visible nodes), container/noscript/jsRedirect/metaRefresh switch the
// corresponding sign on.
func spaTablePage(visible int, container, noscript, jsRedirect, metaRefresh bool) []byte {
	var sb strings.Builder
	sb.WriteString("<!DOCTYPE html><html><head><title></title>")
	if metaRefresh {
		sb.WriteString(`<meta http-equiv="refresh" content="30">`)
	}
	sb.WriteString("</head><body>")
	if container {
		sb.WriteString(`<div id="root">`)
	}
	sb.WriteString("<p>")
	sb.WriteString(strings.Repeat("a", visible))
	sb.WriteString("</p>")
	if container {
		sb.WriteString("</div>")
	}
	if noscript {
		sb.WriteString("<noscript>Please enable JavaScript to run this app.</noscript>")
	}
	if jsRedirect {
		sb.WriteString(`<script>window.location = "/login";</script>`)
	}
	sb.WriteString("</body></html>")
	return []byte(sb.String())
}

// spaReasonsOf is the expected signs in DetectSPA's canonical order (the
// enumeration order of spec 5.5 item 8).
func spaCheckVerdict(t *testing.T, html []byte, wantSPA bool, wantReasons ...SPAReason) SPAVerdict {
	t.Helper()
	v := DetectSPA(html)
	if v.SPA != wantSPA {
		t.Errorf("SPA = %v, ожидалось %v (reasons=%v, visible=%d)", v.SPA, wantSPA, v.Reasons, v.VisibleText)
	}
	if len(wantReasons) == 0 && len(v.Reasons) != 0 {
		t.Errorf("Reasons = %v, ожидалось пусто", v.Reasons)
	} else if len(wantReasons) != 0 && !reflect.DeepEqual(v.Reasons, wantReasons) {
		t.Errorf("Reasons = %v, ожидалось %v", v.Reasons, wantReasons)
	}
	return v
}

// --- framework shells -------------------------------------------------------

func TestDetectSPAFrameworkShells(t *testing.T) {
	t.Run("React create-react-app", func(t *testing.T) {
		html := []byte(`<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>React App</title>
    <script defer src="/static/js/main.a1b2c3.js"></script>
  </head>
  <body>
    <noscript>You need to enable JavaScript to run this app.</noscript>
    <div id="root"></div>
  </body>
</html>`)
		v := spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonRootContainer, SPAReasonNoScriptJS)
		if v.VisibleText != 8 { // "ReactApp" without a space: 5 + 3
			t.Errorf("VisibleText = %d, ожидалось 8", v.VisibleText)
		}
	})

	t.Run("Next.js pages router", func(t *testing.T) {
		html := []byte(`<!DOCTYPE html><html><head><meta charSet="utf-8"/>
<meta name="next-head-count" content="2"/><link rel="preload" href="/_next/static/chunks/main.js"/></head>
<body><div id="__next"></div><script id="__NEXT_DATA__" type="application/json">
{"props":{"pageProps":{"items":[]}},"page":"/","query":{},"buildId":"Xy9_"}</script>
<script src="/_next/static/chunks/main.js" async=""></script></body></html>`)
		// The content is entirely in __NEXT_DATA__ (script — not visible
		// text).
		spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonRootContainer)
	})

	t.Run("Nuxt", func(t *testing.T) {
		html := []byte(`<!DOCTYPE html><html data-n-head=""><head><meta charset="utf-8"><title>site</title>
</head><body><div id="__nuxt"></div><script src="/_nuxt/1.entry.js" defer></script></body></html>`)
		spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonRootContainer)
	})

	t.Run("Vue с русским noscript", func(t *testing.T) {
		html := []byte(`<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8"><title>сайт</title>
</head><body><noscript>Пожалуйста, включите JavaScript.</noscript><div id="app"></div>
<script src="/js/chunk-vendors.js"></script></body></html>`)
		spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonRootContainer, SPAReasonNoScriptJS)
	})

	t.Run("фикстура стенда spa-shell (deploy/docker/testbed)", func(t *testing.T) {
		// The canonical form of the integration bench (T-060/T-090).
		html := []byte(`<!DOCTYPE html>
<html lang="ru">
<head><meta charset="utf-8"><title>spa-shell.crawl.test</title></head>
<body>
<div id="root"></div>
<noscript>Для работы сайта требуется включённый JavaScript.</noscript>
<script>
  document.cookie = "js_session=rendered-by-js; path=/";
  setTimeout(function () {
    document.getElementById("root").innerHTML =
      "<h1>SPA-контент после рендера</h1><p>Этот текст виден только после исполнения JS.</p>";
  }, 150);
</script>
</body>
</html>`)
		v := spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonRootContainer, SPAReasonNoScriptJS)
		if v.VisibleText != 20 { // "spa-shell.crawl.test"
			t.Errorf("VisibleText = %d, ожидалось 20", v.VisibleText)
		}
	})
}

// --- the decision table -------------------------------------------------------

func TestDetectSPADecisionTable(t *testing.T) {
	const rich = 5000 // deliberately a lot of text
	const poor = 50   // deliberately little
	t.Run("1 JS-редирект достаточен при богатом тексте", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(rich, false, false, true, false), true, SPAReasonJSRedirect)
	})
	t.Run("2 meta refresh достаточен при богатом тексте", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(rich, false, false, false, true), true, SPAReasonMetaRefresh)
	})
	t.Run("3 мало текста + контейнер", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(poor, true, false, false, false), true,
			SPAReasonLowText, SPAReasonRootContainer)
	})
	t.Run("4 мало текста + noscript (контейнер нестандартный)", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(poor, false, true, false, false), true,
			SPAReasonLowText, SPAReasonNoScriptJS)
	})
	t.Run("5 мало текста без контейнера и noscript — не SPA", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(poor, false, false, false, false), false, SPAReasonLowText)
	})
	t.Run("6 контейнер + noscript при богатом тексте (SSR) — не SPA", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(rich, true, true, false, false), false,
			SPAReasonRootContainer, SPAReasonNoScriptJS)
	})
	t.Run("7 noscript при богатом тексте — не SPA", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(rich, false, true, false, false), false, SPAReasonNoScriptJS)
	})
	t.Run("8 богатый текст без признаков — не SPA", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(rich, false, false, false, false), false)
	})
	t.Run("9 все признаки сразу", func(t *testing.T) {
		spaCheckVerdict(t, spaTablePage(poor, true, true, true, true), true,
			SPAReasonLowText, SPAReasonRootContainer, SPAReasonNoScriptJS,
			SPAReasonJSRedirect, SPAReasonMetaRefresh)
	})
}

// --- the visible text threshold ---------------------------------------------

func TestDetectSPAVisibleThreshold(t *testing.T) {
	for _, tc := range []struct {
		n       int
		wantSPA bool
	}{
		{299, true},  // below the threshold — "little"
		{300, false}, // at the threshold — no longer "less"
		{301, false},
	} {
		html := []byte(`<html><body><div id="app">` + strings.Repeat("a", tc.n) + `</div></body></html>`)
		v := DetectSPA(html)
		if v.VisibleText != tc.n {
			t.Errorf("n=%d: VisibleText = %d", tc.n, v.VisibleText)
		}
		if v.SPA != tc.wantSPA {
			t.Errorf("n=%d: SPA = %v, ожидалось %v", tc.n, v.SPA, tc.wantSPA)
		}
	}
}

// --- the root container: attribute spelling ----------------------------------

func TestDetectSPARootContainerAttrForms(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tag    string
		wantOK bool
	}{
		{"двойные кавычки", `<div id="root">`, true},
		{"одинарные кавычки", `<div id='app'>`, true},
		{"без кавычек", `<div id=__next>`, true},
		{"регистр атрибута и значения", `<DIV ID=ROOT>`, true},
		{"nuxt", `<div id="__nuxt">`, true},
		{"пробелы вокруг", `<div id = "root" >`, true},
		{"похожее значение", `<div id="rootx">`, false},
		{"другой атрибут", `<div data-id="root">`, false},
		{"id не корневой", `<div id="content">`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Rich text: isolates the attribute check from the pair
			// "little text + a container" (row 3 of the decision table).
			html := []byte(`<html><body>` + tc.tag + `<p>` +
				strings.Repeat("a", 500) + `</p></body></html>`)
			var want []SPAReason
			if tc.wantOK {
				want = []SPAReason{SPAReasonRootContainer}
			}
			spaCheckVerdict(t, html, false, want...)
		})
	}
}

// --- noscript ----------------------------------------------------------------

func TestDetectSPANoScript(t *testing.T) {
	t.Run("английский", func(t *testing.T) {
		html := []byte(`<html><body><p>ok</p><noscript>Please enable JavaScript.</noscript></body></html>`)
		spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonNoScriptJS)
	})
	t.Run("русский с заглавной", func(t *testing.T) {
		html := []byte(`<html><body><p>ок</p><noscript>Включите JavaScript.</noscript></body></html>`)
		spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonNoScriptJS)
	})
	t.Run("короткая форма капсом", func(t *testing.T) {
		html := []byte(`<html><body><p>ок</p><noscript>ВКЛЮЧИТЕ JS</noscript></body></html>`)
		spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonNoScriptJS)
	})
	t.Run("без требования JS — не признак", func(t *testing.T) {
		html := []byte(`<html><body><p>ок</p><noscript><img src="/pixel.gif" alt=""></noscript></body></html>`)
		spaCheckVerdict(t, html, false, SPAReasonLowText)
	})
	t.Run("текст требования за пределами лимита не читается", func(t *testing.T) {
		// The first spaNoScriptLimit bytes hold no keys (a long fallback);
		// the JS demand sits at the very end, outside the scan window.
		filler := strings.Repeat("ф", spaNoScriptLimit)
		html := []byte(`<html><body><p>ок</p><noscript>` + filler +
			` включите js</noscript></body></html>`)
		spaCheckVerdict(t, html, false, SPAReasonLowText)
	})
}

// --- JS redirect ---------------------------------------------------------------

func TestDetectSPAJSRedirect(t *testing.T) {
	tail := `</p>` + strings.Repeat("b", 5000) + `</body></html>`
	for _, tc := range []struct {
		name string
		js   string
		want bool
	}{
		{"window.location присваивание", `<script>window.location = "https://example.com/";</script>`, true},
		{"window.location без пробелов", `<script>window.location="https://example.com/"</script>`, true},
		{"document.location.href", `<script>document.location.href = '/login';</script>`, true},
		{"location.replace", `<script>location.replace('/login');</script>`, true},
		{"location.assign с пробелом", `<script>location.assign ('/login');</script>`, true},
		{"пробелы вокруг точки", `<script>location . href = '/x';</script>`, true},
		{"inline onclick", `<a href="/x" onclick="location.href='/x';return false">Войти</a>`, true},
		{"сравнение == не редирект", `<script>if (window.location.pathname == "/") go();</script>`, false},
		{"строгое сравнение ===", `<script>if (document.location.href === "/x") go();</script>`, false},
		{"член pathname не редирект", `<script>document.location.pathname = "/x";</script>`, false},
		{"слово в тексте не цель", `<script>var loc = window.location; use(loc);</script>`, false},
		{"URL query не редирект", `<a href="?location=msk">Город</a>`, false},
		{"путь /location/ не редирект", `<a href="/location/map">Карта</a>`, false},
		{"часть слова не редирект", `<a href="/mylocations">Склады</a>`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			html := []byte(`<html><body><p>` + tc.js + tail)
			v := DetectSPA(html)
			if v.SPA != tc.want {
				t.Errorf("SPA = %v, ожидалось %v (reasons=%v)", v.SPA, tc.want, v.Reasons)
			}
			if tc.want && !v.SPA {
				t.Errorf("признак js_redirect не сработал: %v", v.Reasons)
			}
			if !tc.want && len(v.Reasons) != 0 {
				t.Errorf("ложные признаки: %v", v.Reasons)
			}
		})
	}
}

// --- meta refresh --------------------------------------------------------------

func TestDetectSPAMetaRefresh(t *testing.T) {
	richBody := func(meta string) []byte {
		return []byte(`<html><head>` + meta + `</head><body><p>` +
			strings.Repeat("c", 5000) + `</p></body></html>`)
	}
	t.Run("обновление самого себя", func(t *testing.T) {
		spaCheckVerdict(t, richBody(`<meta http-equiv="refresh" content="30">`), true, SPAReasonMetaRefresh)
	})
	t.Run("циклический url (T-048 оборвал цепочку)", func(t *testing.T) {
		spaCheckVerdict(t, richBody(`<meta http-equiv="refresh" content="0; url=/">`), true, SPAReasonMetaRefresh)
	})
	t.Run("регистр и кавычки", func(t *testing.T) {
		spaCheckVerdict(t, richBody(`<META HTTP-EQUIV='Refresh' CONTENT='30'>`), true, SPAReasonMetaRefresh)
	})
	t.Run("мета-тег в теле документа", func(t *testing.T) {
		html := []byte(`<html><head></head><body><meta http-equiv="refresh" content="10"><p>` +
			strings.Repeat("c", 5000) + `</p></body></html>`)
		spaCheckVerdict(t, html, true, SPAReasonMetaRefresh)
	})
	t.Run("не refresh — не признак", func(t *testing.T) {
		spaCheckVerdict(t, richBody(`<meta http-equiv="content-type" content="text/html; charset=utf-8">`), false)
	})
	t.Run("name вместо http-equiv", func(t *testing.T) {
		spaCheckVerdict(t, richBody(`<meta name="refresh" content="30">`), false)
	})
}

// --- non-HTML and degenerate inputs --------------------------------------------

func TestDetectSPADegenerate(t *testing.T) {
	t.Run("пустое тело", func(t *testing.T) {
		v := spaCheckVerdict(t, nil, false, SPAReasonLowText)
		if v.VisibleText != 0 {
			t.Errorf("VisibleText = %d, ожидалось 0", v.VisibleText)
		}
	})
	t.Run("только пробелы", func(t *testing.T) {
		spaCheckVerdict(t, []byte(" \n\t\r\n "), false, SPAReasonLowText)
	})
	t.Run("бинарный мусор", func(t *testing.T) {
		// A deterministic pseudo-random buffer with markup bytes; UTF-8 of
		// the input is guaranteed by T-051, the scan must not panic.
		// "Not SPA" unconditionally: no container, no noscript, no
		// location word (the arithmetic step 7 ≠ 3 yields no "lo"), and no
		// meta; "little text" alone does not decide (row 5).
		garbage := make([]byte, 512)
		for i := range garbage {
			garbage[i] = byte((i*7 + 3) % 256)
		}
		garbage = append([]byte("\x89PNG\r\n\x1a\n"), garbage...)
		v := DetectSPA(garbage)
		if v.SPA {
			t.Errorf("мусор признан SPA: %v", v.Reasons)
		}
		for _, r := range v.Reasons {
			if r != SPAReasonLowText {
				t.Errorf("у мусора сработал лишний признак %v", r)
			}
		}
	})
	t.Run("незакрытый script до конца", func(t *testing.T) {
		html := []byte("<html><body><p>xy</p><script>var a = 1; хвост без закрытия")
		v := spaCheckVerdict(t, html, false, SPAReasonLowText)
		if v.VisibleText != 2 {
			t.Errorf("VisibleText = %d, ожидалось 2 (хвост — script)", v.VisibleText)
		}
	})
	t.Run("одиночный < в конце", func(t *testing.T) {
		spaCheckVerdict(t, []byte("<p>ok</p><"), false, SPAReasonLowText)
	})
}

// --- visible text counting ------------------------------------------------------

func TestDetectSPAVisibleCounting(t *testing.T) {
	t.Run("script, style, noscript, комментарии — не текст", func(t *testing.T) {
		html := []byte(`<html><head><style>body { color: red; }</style></head>` +
			`<body><!-- служебный комментарий --><script>var x = "текст в скрипте";</script>` +
			`<noscript>enable js</noscript><p>abc</p></body></html>`)
		// "little text + noscript" — row 4 of the decision table: SPA.
		v := spaCheckVerdict(t, html, true, SPAReasonLowText, SPAReasonNoScriptJS)
		if v.VisibleText != 3 {
			t.Errorf("VisibleText = %d, ожидалось 3", v.VisibleText)
		}
	})
	t.Run("кириллица считается байтами", func(t *testing.T) {
		// 150 Cyrillic characters = 300 bytes: a byte-based threshold is
		// more conservative than a rune-based one.
		html := []byte(`<html><body><p>` + strings.Repeat("ф", 150) + `</p></body></html>`)
		v := DetectSPA(html)
		if v.VisibleText != 300 {
			t.Errorf("VisibleText = %d, ожидалось 300", v.VisibleText)
		}
		if v.SPA {
			t.Errorf("300 байт = порог, не «меньше»: SPA = %v", v.SPA)
		}
	})
	t.Run("классическая страница — не SPA", func(t *testing.T) {
		html := []byte(`<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<title>Добро пожаловать</title></head><body><header><h1>ООО «Ромашка»</h1></header>
<nav><a href="/about">О компании</a> <a href="/services">Услуги</a></nav>
<main><p>` + strings.Repeat("Компания работает на рынке с 2001 года. ", 20) +
			`</p></main><footer>© 2026 Ромашка</footer></body></html>`)
		spaCheckVerdict(t, html, false)
	})
	t.Run("SSR SPA с большим SEO-текстом — рендер не нужен", func(t *testing.T) {
		html := []byte(`<!DOCTYPE html><html><head><title>Каталог</title></head><body><div id="__nuxt">` +
			strings.Repeat("Товар и цена. ", 400) + `</div></body></html>`)
		spaCheckVerdict(t, html, false, SPAReasonRootContainer)
	})
}

// --- performance: a single pass with no allocations -----------------------------

func TestDetectSPAScanAllocations(t *testing.T) {
	// A quiet page (no signs): no reason to append, no attributes found —
	// the scan allocates nothing at all. It pins the "single pass with no
	// body copies" behavior (spec 13.2 — an implementation detail).
	page := bytes.Repeat([]byte("<p>обычный текст без признаков</p>"), 40000) // ~1.2 MB
	if v := DetectSPA(page); v.SPA || len(v.Reasons) != 0 {
		t.Fatalf("тихая страница дала признаки: %v", v.Reasons)
	}
	allocs := testing.AllocsPerRun(20, func() { DetectSPA(page) })
	if allocs != 0 {
		t.Errorf("аллокаций на скан: %v, ожидалось 0", allocs)
	}
}
