// The SPA shell heuristic (T-057; spec 5.5 item 8 "the render decision",
// spec 13.2 "the SPA shell detection heuristic is at the developer's
// discretion"). A pure function over the page's final body (UTF-8 is
// guaranteed by T-051, the 2 MB cap by T-049): by the set of signs it
// decides whether a render in Chromium is needed (publishing to q:render
// is done by the T-053 consumer — here only the verdict and the fired
// signs).
//
// The signs:
//
//   - little visible text — the visible characters (bytes) after the
//     coarse removal of tags and the content of script/style/noscript is
//     less than SPAVisibleTextMax;
//   - a root container — the attribute id="root|app|__next|__nuxt" (in
//     any quotes and case) on any tag: the framework's mount point
//     (React/Vue/Next/Nuxt, the "and the like" of spec 5.5 item 8);
//   - a noscript demanding JS — a <noscript> whose text asks to enable
//     JavaScript/JS (the key list is spaNoScriptKeys);
//   - a JS redirect — a string scan for location.replace/location.assign/
//     location.href=…/location=… (window.location and document.location
//     are special cases of "bare" location; no execution is performed);
//   - meta refresh — <meta http-equiv="refresh"> in the final HTML.
//
// The decision table (a reading of spec 5.5 item 8: the ";"-separated list
// gives self-sufficient signs, "little visible text + a root container" is
// a paired condition, noscript is a reinforcing sign):
//
//	#  text  container  noscript  JS-redirect  meta refresh  SPA
//	1  *      *          *         yes          *             yes
//	2  *      *          *         *            yes           yes
//	3  low    yes        *         no           no            yes
//	4  low    no         yes       no           no            yes
//	5  low    no         no        no           no            no
//	6  rich   yes        yes       no           no            no
//	7  rich   no         yes       no           no            no
//	8  rich   no         no        no           no            no
//
// Rationale:
//
//   - a JS redirect alone is sufficient (row 1): the page's own code
//     takes the browser to another address — by design the raw HTML was
//     never meant to be the final content, it is a springboard.
//   - meta refresh alone is sufficient (row 2): T-048 has already
//     followed all meta refresh hops with a url, so the final HTML only
//     retains a refresh without a hop — "self-refresh" (including a cycle
//     cut off by the cycle detect) or one with a url that cannot be
//     followed. A page refreshing itself is designed for repeated
//     execution in the browser (JS logic, cookies); the spec lists meta
//     refresh as a separate sign. A false positive (a classic page with
//     content="300") costs one render, a false negative would leave a
//     placeholder as the final content until the domain is rechecked.
//   - the pair "little text + a container" (row 3) is the spec's
//     wording: a container alone is not indicative (id="app" occurs in
//     classic sites' markup), and so is little text alone (parking and
//     empty default pages: a render is pointless, there is nothing to
//     add); together — a framework shell without content.
//   - noscript is reinforcing (row 4): "enable JavaScript" on an empty
//     page means a shell with any container, including a non-standard
//     one (the "and the like" of the spec). Alone (row 7) it is not
//     indicative: classic sites show a polite warning while working
//     without JS; with rich text (rows 6–8) no render is needed — SSR/
//     classic pages have already delivered the content.
//
// The error cost is asymmetric (§5.6, T-066): a false positive is one
// bounded render in the queue, a miss is empty content in the storage
// until the next recheck. That is why the sign scanners are deliberately
// minimal and tolerant of false positives.
//
// The implementation is a single linear pass over the body with no copies
// or allocations on "quiet" pages (zero allocations are pinned by a
// test): counting visible text alternates with tag parsing, the content
// of script/style is skipped to the closing tag, noscript is checked for
// keywords in the first spaNoScriptLimit bytes. The JS redirect is an
// independent scan over the whole document (it also catches inline
// handlers like onclick="location.href=…"); no HTML parser is used: the
// signs are described by byte markers, and full parsing of untrusted
// markup for the sake of a heuristic is redundant (§9 — bounded parsers;
// the T-048 style).

package httpfetch

import (
	"bytes"
	"strings"
)

// SPAVisibleTextMax is the "little visible text" threshold: the visible
// characters (bytes, not runes: Cyrillic counts double weight — the
// conservative side) after the coarse removal of tags, script, style and
// noscript. The threshold comes from shell practice: React stubs
// (create-react-app), Next/Nuxt/Vue carry 0–~200 visible bytes — all the
// text is moved into noscript (not shown when JS is on) and into JS
// bundles; meaningful main pages start at hundreds of bytes. 300 separates
// the classes with margin in both directions; it is tuned by rebuilding
// the constant (spec 13.2 — the heuristic is at the developer's
// discretion; no separate config key until the load measurement data of
// §11.3 exists).
const SPAVisibleTextMax = 300

// spaNoScriptLimit — how many bytes of <noscript> text to scan for a JS
// demand: real warnings are one or two sentences; long noscripts (a
// fallback markup) do not affect the sign.
const spaNoScriptLimit = 4096

// SPAReason is one fired SPA shell sign (spec 5.5 item 8). The values are
// stable strings: the consumer (T-053) puts them into the q:render task
// and logs them.
type SPAReason string

// The heuristic's signs. The declaration order is the spec's list order.
const (
	// SPAReasonLowText — the visible text is less than SPAVisibleTextMax.
	SPAReasonLowText SPAReason = "low_text"
	// SPAReasonRootContainer — a framework root container was found
	// (id=root/app/__next/__nuxt).
	SPAReasonRootContainer SPAReason = "root_container"
	// SPAReasonNoScriptJS — a <noscript> demands enabling JavaScript.
	SPAReasonNoScriptJS SPAReason = "noscript_js"
	// SPAReasonJSRedirect — a string trace of a JS redirect
	// (location=…, location.href=…, location.replace(…, location.assign(…).
	SPAReasonJSRedirect SPAReason = "js_redirect"
	// SPAReasonMetaRefresh — a meta refresh in the final HTML (without a
	// hop: T-048 consumed the hops, only "self-refresh" remains).
	SPAReasonMetaRefresh SPAReason = "meta_refresh"
)

// SPAVerdict is the render decision for the final page (spec 5.5 item 8).
// SPA=true — the page looks like an SPA shell; the consumer publishes a
// task to q:render (T-053); SPA=false — the raw HTML is final.
// Reasons — all fired signs (not only the deciding ones); VisibleText —
// the counted visible characters (bytes), for metrics and threshold
// tuning.
type SPAVerdict struct {
	SPA         bool        `json:"spa"`
	Reasons     []SPAReason `json:"reasons,omitempty"`
	VisibleText int         `json:"visible_text"`
}

// DetectSPA decides whether the final HTML looks like an SPA shell
// (spec 5.5 item 8). The argument is the body of the chain's final
// response (T-048) in UTF-8 (T-051); the URL context is not needed: the
// signs do not depend on the address, and the Location and meta refresh
// hops have already been performed by the chain — only "is a render
// needed" is recorded here, the hop target does not matter. The function
// is pure: it neither modifies nor copies the input, and makes decisions
// by the decision table from the package comment.
func DetectSPA(html []byte) SPAVerdict {
	s := spaScan(html)
	v := SPAVerdict{VisibleText: s.visibleText}
	low := s.visibleText < SPAVisibleTextMax
	if low {
		v.Reasons = append(v.Reasons, SPAReasonLowText)
	}
	if s.container {
		v.Reasons = append(v.Reasons, SPAReasonRootContainer)
	}
	if s.noscript {
		v.Reasons = append(v.Reasons, SPAReasonNoScriptJS)
	}
	if s.jsRedirect {
		v.Reasons = append(v.Reasons, SPAReasonJSRedirect)
	}
	if s.metaRefresh {
		v.Reasons = append(v.Reasons, SPAReasonMetaRefresh)
	}
	// The decision table: the strong signs (a JS redirect, meta refresh)
	// suffice alone; the pair "little text + (a container or noscript)"
	// follows the spec's wording; a container/noscript with rich text
	// needs no render (rows 6–7).
	v.SPA = s.jsRedirect || s.metaRefresh || (low && (s.container || s.noscript))
	return v
}

// spaSigns is the signs counted by the scan.
type spaSigns struct {
	visibleText int  // the visible characters (bytes) outside tags/script/style/noscript
	container   bool // the root container root/app/__next/__nuxt
	noscript    bool // a <noscript> demanding JS
	jsRedirect  bool // a string trace of a JS redirect
	metaRefresh bool // a <meta http-equiv="refresh"> in the final HTML
}

// Scanner markers (byte-based — for EqualFold without conversions, the
// redirect.go style).
var (
	spaCommentOpen = []byte("<!--")
	spaCommentEnd  = []byte("-->")
	spaScriptEnd   = []byte("</script")
	spaStyleEnd    = []byte("</style")
	spaNoScriptEnd = []byte("</noscript")
	spaJSWord      = []byte("location")

	spaNameScript   = []byte("script")
	spaNameStyle    = []byte("style")
	spaNameNoScript = []byte("noscript")
	spaNameMeta     = []byte("meta")

	spaAttrID    = []byte("id")
	spaAttrEquiv = []byte("http-equiv")

	// spaJSReplace/spaJSAssign/spaJSHref — members of location meaning a
	// hop; string literals are not compared so as not to allocate.
	spaJSReplace = []byte("replace")
	spaJSAssign  = []byte("assign")
	spaJSHref    = []byte("href")

	spaRootIDs = []string{"root", "app", "__next", "__nuxt"}

	// spaNoScriptKeys is the keyword list for JS demands in <noscript>.
	// The word "JavaScript" is spelled the same in any language and covers
	// most wordings (EN/DE/FR/ES/RU: "Включите JavaScript", "JavaScript is
	// required"); the other keys are short "JS" forms of typical RU/EN
	// phrases. The search is case-insensitive (including Cyrillic —
	// bytes.EqualFold), so lower-case variants suffice. The list is
	// minimal: extending it with exotics does not pay off, noscript is a
	// reinforcing sign, and missing it is compensated by the pair
	// "text + container".
	spaNoScriptKeys = [][]byte{
		[]byte("javascript"),
		[]byte("enable js"),
		[]byte("js required"),
		[]byte("включите js"),
		[]byte("включить js"),
		[]byte("требуется js"),
		[]byte("нужен js"),
	}
)

// spaScan is a single pass over the HTML: the text between tags counts as
// visible (bytes, except whitespace), tags are parsed as encountered. The
// content of script/style is skipped to the closing tag (not visible
// text), noscript is checked for a JS demand (also not visible: a browser
// with JS on does not show it), comments are skipped. An unclosed
// script/style/comment consumes the rest of the document (the tail is not
// text). It reuses the T-048 primitives: redirectTagEnd (quote
// accounting), redirectIndexFold, redirectIsSpace.
func spaScan(b []byte) spaSigns {
	var s spaSigns
	s.jsRedirect = spaHasJSRedirect(b)
	i := 0
	for i < len(b) {
		lt := bytes.IndexByte(b[i:], '<')
		if lt < 0 {
			s.visibleText += spaVisibleCount(b[i:])
			return s
		}
		lt += i
		s.visibleText += spaVisibleCount(b[i:lt])
		if bytes.HasPrefix(b[lt:], spaCommentOpen) {
			e := bytes.Index(b[lt+len(spaCommentOpen):], spaCommentEnd)
			if e < 0 {
				return s // an unclosed comment — the rest is not text
			}
			i = lt + len(spaCommentOpen) + e + len(spaCommentEnd)
			continue
		}
		end := redirectTagEnd(b, lt)
		if end < 0 {
			return s // the tag is not closed — count the rest as markup
		}
		closing := false
		nameStart := lt + 1
		if b[nameStart] == '/' {
			closing = true // closing: its content has already been skipped
			nameStart++
		}
		nameEnd := nameStart
		for nameEnd < end && !redirectIsSpace(b[nameEnd]) && b[nameEnd] != '/' {
			nameEnd++
		}
		name := b[nameStart:nameEnd]
		switch {
		case !closing && bytes.EqualFold(name, spaNameScript):
			// Script content is not visible; unclosed — until the end.
			if ce := redirectIndexFold(b, end+1, spaScriptEnd); ce >= 0 {
				i = ce // parsing continues from the closing tag
			} else {
				return s
			}
		case !closing && bytes.EqualFold(name, spaNameStyle):
			if ce := redirectIndexFold(b, end+1, spaStyleEnd); ce >= 0 {
				i = ce
			} else {
				return s
			}
		case !closing && bytes.EqualFold(name, spaNameNoScript):
			cs := end + 1
			ce := redirectIndexFold(b, cs, spaNoScriptEnd)
			regionEnd := len(b)
			if ce >= 0 {
				regionEnd = ce
			}
			if limit := cs + spaNoScriptLimit; regionEnd > limit {
				regionEnd = limit
			}
			if regionEnd > cs && spaNoScriptDemand(b[cs:regionEnd]) {
				s.noscript = true
			}
			if ce < 0 {
				return s
			}
			i = ce
		default:
			if bytes.EqualFold(name, spaNameMeta) &&
				strings.EqualFold(spaTagAttr(b, nameEnd, end, spaAttrEquiv), "refresh") {
				// The sign is caught over the whole document, not only in
				// head: T-048 is bounded to head by the HTML spec of the
				// tag's action, while here the sign describes the page's
				// intent — broken markup does not cancel it.
				s.metaRefresh = true
			}
			if spaIsRootID(spaTagAttr(b, nameEnd, end, spaAttrID)) {
				s.container = true
			}
			i = end + 1
		}
	}
	return s
}

// spaVisibleCount is the visible characters (bytes) of a text fragment:
// everything except whitespace (the HTML spaces of redirectIsSpace).
// Entities are not expanded: "coarse tag removal" per spec 13.2; the
// threshold with margin survives &nbsp; inflation.
func spaVisibleCount(b []byte) int {
	n := 0
	for _, c := range b {
		if !redirectIsSpace(c) {
			n++
		}
	}
	return n
}

// spaNoScriptDemand is there a JS demand in the <noscript> text (the
// spaNoScriptKeys, case-insensitive).
func spaNoScriptDemand(region []byte) bool {
	for _, k := range spaNoScriptKeys {
		if redirectIndexFold(region, 0, k) >= 0 {
			return true
		}
	}
	return false
}

// spaIsRootID — the id attribute value from the root container list (a
// case-insensitive comparison: HTML ids are case-sensitive, but "ROOT"
// instead of "root" does not affect the sign, and false positives are
// practically nonexistent).
func spaIsRootID(id string) bool {
	for _, want := range spaRootIDs {
		if strings.EqualFold(id, want) {
			return true
		}
	}
	return false
}

// spaTagAttr is the value of the want attribute of the tag in the
// [p, end) range (the redirectTagAttrs style: case-insensitive names,
// values in '…'/"…"/unquoted, '/' between attributes skipped). An
// allocation happens only on a found value; "" — the attribute is absent.
func spaTagAttr(b []byte, p, end int, want []byte) string {
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
		name := b[ns:p]
		for p < end && redirectIsSpace(b[p]) {
			p++ // HTML5 allows spaces around '=': id = "root"
		}
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
					p++ // the closing quote
				}
			} else {
				vs = p
				for p < end && !redirectIsSpace(b[p]) {
					p++
				}
				ve = p
			}
		}
		if bytes.EqualFold(name, want) {
			return string(b[vs:ve])
		}
	}
	return ""
}

// spaHasJSRedirect is a string scan of the document for a JS redirect:
// the word "location" (case matters: JS identifiers are case-sensitive,
// window.Location does not exist), not preceded by a JS-name byte or a
// URL-context byte (?location=…, &location=…, /location=…), with a
// continuation:
//
//	= (not ==)     — location = "https://…" (and window.location = …)
//	.replace( …    — location.replace('/login')
//	.assign(  …    — location.assign('/login')
//	.href = (not ==) — location.href = '/login' (window/document are
//	                 special cases of "bare" location)
//
// The whole HTML is scanned, not only <script>: inline handlers
// (onclick="location.href='/x'") are the same springboard. No execution is
// performed (§9): this is a trace of intent, not proof of a hop. The
// minimalism is deliberate: a false positive (an article with a
// window.location example in the visible text) costs one render, a miss —
// empty content as the page's outcome. Members like
// location.pathname=…, ==/=== comparisons and quoted examples without "="
// do not count.
func spaHasJSRedirect(b []byte) bool {
	for from := 0; from < len(b); {
		j := bytes.Index(b[from:], spaJSWord)
		if j < 0 {
			return false
		}
		j += from
		from = j + 1
		if j > 0 {
			c := b[j-1]
			if spaJSNameByte(c) {
				continue // xlocation, location_ — not a hop target
			}
			switch c {
			case '=', '?', '&', '/', '#':
				continue // URL context: ?location=…, /location/…
			}
		}
		m := j + len(spaJSWord)
		for m < len(b) && spaJSBlank(b[m]) {
			m++
		}
		if m >= len(b) {
			continue
		}
		switch b[m] {
		case '.':
			m++
			for m < len(b) && spaJSBlank(b[m]) {
				m++ // location . href — spaces around the dot are allowed
			}
			ns := m
			ne := ns
			for ne < len(b) && spaJSNameByte(b[ne]) {
				ne++
			}
			p := ne
			for p < len(b) && spaJSBlank(b[p]) {
				p++
			}
			member := b[ns:ne]
			if (bytes.EqualFold(member, spaJSReplace) || bytes.EqualFold(member, spaJSAssign)) &&
				p < len(b) && b[p] == '(' {
				return true
			}
			if bytes.EqualFold(member, spaJSHref) &&
				p < len(b) && b[p] == '=' && (p+1 >= len(b) || b[p+1] != '=') {
				return true
			}
		case '=':
			if m+1 >= len(b) || b[m+1] != '=' {
				return true // location = "https://…"
			}
		}
	}
	return false
}

// spaJSNameByte is a byte belonging to a JS identifier (Latin letters,
// digits, _, $).
func spaJSNameByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
		c >= '0' && c <= '9' || c == '_' || c == '$'
}

// spaJSBlank is the whitespace allowed in JS between tokens.
func spaJSBlank(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	}
	return false
}
