// Charsets and response compression (spec 5.5):
//
//   - "Accept-Encoding: gzip, br is supported; decompression with a size
//     cap (protection against zip bombs)": the transport sends gzip and br
//     itself (an explicit header disables net/http's transparent
//     decompression, which has no cap) and wraps the body in a reader with
//     a cap on the decompressed size;
//   - "The response charset is determined (header, meta charset, detect)
//     and converted to UTF-8": ToUTF8 — BOM → Content-Type charset → meta
//     charset (first 1024 bytes) → detect (valid UTF-8, Cyrillic frequency
//     score) → the windows-1252 default (WHATWG).
//
// The body reading chain: network → Content-Encoding decompression (a
// single coding or a compound "gzip, br" — a chain of layers per RFC 9110,
// the MaxBodyBytes cap per layer, T-100) → [the readable body limit,
// T-049] → the consumer; the UTF-8 conversion runs on top of the read
// bytes via ToUTF8 (the consumer buffers the body anyway for the dedup
// hash and storage; a streaming reader swap gains nothing and adds a
// second read). Everything further down the pipeline (the T-049 limit, the
// hash, the storage raw_len of T-042) sees already uncompressed bytes —
// the decompressed stream is the response body.
package httpfetch

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/andybalholm/brotli"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/htmlindex"
	xunicode "golang.org/x/text/encoding/unicode"
)

const (
	// acceptEncoding is the Accept-Encoding header value of requests
	// (spec 5.5: "Accept-Encoding: gzip, br is supported"). It is set by
	// the transport (Fetch and RoundTrip), not net/http: a manual header
	// disables the Go transport's transparent decompression — compressed
	// bytes arrive as is and are decompressed here, with a cap.
	acceptEncoding = "gzip, br"

	// defaultDecompressLimit is the cap on the decompressed body when
	// Options.MaxBodyBytes is not set: 2 MB from the spec 5.5 limits
	// table ("max readable body size"). The decompression cap equals the
	// body limit rather than being a separate number: the decompressed
	// stream is the body, so "2 MB readable" must also mean
	// "2 MB decompressed" — otherwise reading to the end (the dedup
	// hash) would consume gigabytes of a zip bomb. A separate name, not
	// DefaultMaxBodyBytes, to avoid arguing with T-049 over the body
	// limit default constant.
	defaultDecompressLimit = 2 << 20

	// charsetPreview is the content-based charset detection window: the
	// BOM is always at the start, meta charset is looked up in the first
	// 1024 bytes (the WHATWG window; x/net/html/charset uses the same
	// number).
	charsetPreview = 1024

	// cyrillicMinLetters is the minimum number of Cyrillic letters to
	// trust the frequency detect: fewer means not enough signal, and the
	// windows-1252 default remains.
	cyrillicMinLetters = 32

	// cyrillicMinAvgFreq is the threshold on the average (geometric)
	// letter frequency of the decoded text: Russian text gives
	// 0.04–0.07 (frequent о/е/а), a random reading gives 0.01 and below.
	// It cuts off Latin text with diacritics misread as Cyrillic.
	cyrillicMinAvgFreq = 0.02
)

// ErrDecompressTooLarge — the decompressed body exceeded the cap
// (protection against zip bombs, spec 5.5). The response body remains
// readable up to the cap; the error is returned by reading beyond it.
var ErrDecompressTooLarge = errors.New("httpfetch: распакованное тело превышает потолок распаковки")

// --- compression: Accept-Encoding and capped decompression ------------------

// parseContentCodings parses the Content-Encoding value into a list of
// codings: RFC 9110 lists them comma-separated in the order the server
// applied them (the first is the innermost layer, the last the outermost);
// the case and spaces around names are not significant. An empty value is
// an empty list (no coding). An empty element ("gzip,,br", "gzip,") or a
// coding outside {gzip, x-gzip, br, deflate, identity} is an error: a
// broken server gets an honest rejection instead of a silent delivery of
// compressed bytes as the body.
func parseContentCodings(contentEncoding string) ([]string, error) {
	if strings.TrimSpace(contentEncoding) == "" {
		return nil, nil
	}
	parts := strings.Split(contentEncoding, ",")
	codings := make([]string, 0, len(parts))
	for _, part := range parts {
		c := strings.ToLower(strings.TrimSpace(part))
		switch c {
		case "gzip", "x-gzip", "br", "deflate", "identity":
			codings = append(codings, c)
		default:
			return nil, fmt.Errorf("httpfetch: неподдерживаемое Content-Encoding %q (ожидается gzip/br/deflate/identity)", contentEncoding)
		}
	}
	return codings, nil
}

// identityOnlyContentEncoding reports whether the Content-Encoding value
// reduces to identity/empty (a single "identity" or a compound
// "identity, identity"): the body is untransformed, the coding headers
// stay truthful — there is no need to strip them (decodeResponseBody).
func identityOnlyContentEncoding(ce string) bool {
	for _, part := range strings.Split(ce, ",") {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case "", "identity":
		default:
			return false
		}
	}
	return true
}

// decodeContentEncoding wraps a compressed body according to the
// Content-Encoding value (gzip and its x-gzip alias, br, deflate,
// identity/empty) with a reader capped at the decompressed size limit
// (<= 0 means defaultDecompressLimit). The crawler does not advertise
// deflate (Accept-Encoding: gzip, br), but some servers send it
// unrequested — the stream is accepted defensively: a zlib wrapper
// (RFC 1950) or raw deflate (RFC 1951), the form is told apart by the
// header. Compound values ("gzip, br", T-100) are decompressed as a chain
// of layers per RFC 9110: the codings are applied by the server in
// enumeration order, so the last one in the list forms the outermost
// layer, and the chain is built from it back to the first. The limit cap
// applies per layer, not to the product: the total decompression work for
// one response is bounded by the number of layers × (limit +
// decompressTailMargin) — each layer is limited as if it were the only
// one, and the bomb protection does not weaken. The tail classification
// (truncation vs ErrDecompressTooLarge) is performed by the final layer on
// the last decoded stream — single-coding semantics do not change.
// Accept-Encoding still does not advertise compound values — the value
// comes only from a broken server and is handled defensively, like a
// single deflate. Returns a reader that closes the original body (the
// Close chain cascades down to it).
func decodeContentEncoding(rc io.ReadCloser, contentEncoding string, limit int64) (io.ReadCloser, error) {
	if limit <= 0 {
		limit = defaultDecompressLimit
	}
	codings, err := parseContentCodings(contentEncoding)
	if err != nil {
		return nil, err
	}
	// Built from the outermost layer (the last coding) to the innermost
	// (the first): each reader strips one layer, decompressCap on top
	// applies the limit cap. identity builds no layer (a no-op); a chain
	// of only identities or an empty list return the body as is.
	var cur = rc // io.ReadCloser — the static type from the parameter
	for i := len(codings) - 1; i >= 0; i-- {
		switch codings[i] {
		case "identity":
			continue
		case "gzip", "x-gzip":
			// gzip.NewReader reads the header immediately: an empty
			// compressed body returns io.EOF — that is not an error, the
			// body is simply empty (the layer below has already given
			// everything — return it as is).
			gz, err := gzip.NewReader(cur)
			if err == io.EOF {
				return cur, nil
			}
			if err != nil {
				return nil, fmt.Errorf("httpfetch: битый gzip-заголовок тела: %w", err)
			}
			cur = &decompressCap{dec: gz, src: cur, left: limit}
		case "br":
			// brotli.Reader validates the header lazily, on read.
			cur = &decompressCap{dec: brotli.NewReader(cur), src: cur, left: limit}
		case "deflate":
			cur = &decompressCap{dec: newDeflateReader(cur), src: cur, left: limit}
		}
	}
	return cur, nil
}

// newDeflateReader is a deflate stream reader: a zlib wrapper (RFC 1950)
// or a raw stream (RFC 1951); both forms occur in real
// "Content-Encoding: deflate" (a historical ambiguity of HTTP servers;
// browsers and curl go through the same fork). The zlib header is told
// apart by two bytes: the deflate method (CM=8), a window of ≤ 32 KB
// (CINFO ≤ 7), no preset dictionary (FDICT=0), and the header checksum
// divisible by 31. Reading is lazy: no source data is lost (the reader
// keeps a peek buffer).
func newDeflateReader(rc io.ReadCloser) io.Reader {
	br := bufio.NewReader(rc)
	if hdr, err := br.Peek(2); err == nil && looksLikeZlib(hdr) {
		if zr, err := zlib.NewReader(br); err == nil {
			return zr
		}
	}
	return flate.NewReader(br)
}

// looksLikeZlib — a plausible zlib header (see newDeflateReader).
func looksLikeZlib(hdr []byte) bool {
	if len(hdr) < 2 {
		return false
	}
	cmf, flg := hdr[0], hdr[1]
	return cmf&0x0f == 8 && cmf>>4 <= 7 && flg&0x20 == 0 &&
		(int(cmf)*256+int(flg))%31 == 0
}

// decompressTailMargin — how many decompressed bytes beyond the cap are
// read to classify the tail: the end of the stream within the margin is
// an honest truncation (a live site slightly over the limit), continuation
// beyond the margin is a zip bomb (ErrDecompressTooLarge, spec 5.5/T-051).
const decompressTailMargin = 64 << 10

// decompressCap is a decompressed stream with a cap (zip-bomb protection).
// In compound packaging (T-100) an instance sits on every layer: src is
// then the decompressCap of the layer below, Close cascades down to the
// response body, closing each layer exactly once. The brotli reader has no
// separate Close, and gzip's does not touch the source — a single
// src.Close is enough.
type decompressCap struct {
	dec      io.Reader     // the decompressed stream (gzip.Reader / brotli.Reader)
	src      io.ReadCloser // the compressed response body
	left     int64         // the remaining cap in bytes
	tailDone bool          // the tail beyond the cap has been classified already
	tailErr  error         // the tail classification verdict (repeated reads repeat it)
}

func (c *decompressCap) Read(p []byte) (int, error) {
	if c.left > 0 {
		if int64(len(p)) > c.left {
			p = p[:c.left]
		}
		n, err := c.dec.Read(p)
		c.left -= int64(n)
		return n, err
	}
	if len(p) == 0 {
		return 0, nil // the io.Reader contract; classification on the next read
	}
	if !c.tailDone {
		c.tailDone = true
		n, err := c.classifyTail(p)
		if n > 0 {
			c.tailErr = io.EOF // the first tail byte is given out — the stream ends after it
		} else {
			c.tailErr = err
		}
		return n, c.tailErr
	}
	return 0, c.tailErr
}

// classifyTail classifies the tail beyond the cap (performed once):
// EOF right away — the body is exactly at the cap (not an error); data
// ending within decompressTailMargin — truncation: the first tail byte is
// copied into p as the "body is longer than the cap" marker (BodyLimiter
// sets Truncated based on it, H-2), the rest of the tail is read out and
// discarded; a stream continuing beyond the margin is a zip bomb:
// ErrDecompressTooLarge, reading stops without spending more of the bomb
// (T-051).
func (c *decompressCap) classifyTail(p []byte) (int, error) {
	var buf [4096]byte
	read := int64(0)
	for read < decompressTailMargin {
		n, err := c.dec.Read(buf[:])
		read += int64(n)
		if n > 0 {
			p[0] = buf[0] // the tail marker for the consumer
			for read < decompressTailMargin {
				n2, err2 := c.dec.Read(buf[:])
				read += int64(n2)
				if err2 == io.EOF {
					return 1, nil // the end is within the margin — truncation
				}
				if err2 != nil {
					return 1, err2 // the tail started but broke off with a network error
				}
			}
			return 0, ErrDecompressTooLarge // the stream continues — a bomb
		}
		if err == io.EOF {
			return 0, io.EOF // the body is exactly at the cap
		}
		if err != nil {
			return 0, err
		}
		// (0, nil) is an allowed rare case; keep going until the margin
	}
	return 0, ErrDecompressTooLarge
}

func (c *decompressCap) Close() error { return c.src.Close() }

// --- charsets: detection and conversion to UTF-8 ----------------------------

// CharsetSource is the step at which the body charset was decided.
// Observability for the worker (logs/metrics "how we learned it"); it does
// not affect decoding.
type CharsetSource int

const (
	// CharsetNone — the body is empty, there is no charset.
	CharsetNone CharsetSource = iota
	// CharsetBOM — the byte order mark (the strongest signal, WHATWG).
	CharsetBOM
	// CharsetHeader — the charset parameter of the Content-Type header.
	CharsetHeader
	// CharsetMeta — a meta charset in the first 1024 bytes (both kinds:
	// <meta charset> and <meta http-equiv="Content-Type">).
	CharsetMeta
	// CharsetDetect — detection: valid UTF-8 or a Cyrillic frequency
	// score (koi8-r / windows-1251 / iso-8859-5).
	CharsetDetect
	// CharsetDefault — the WHATWG windows-1252 default (a superset of
	// latin-1): nothing was determined.
	CharsetDefault
)

// String is the short name of the source (bom/header/meta/detect/default/none).
func (s CharsetSource) String() string {
	switch s {
	case CharsetBOM:
		return "bom"
	case CharsetHeader:
		return "header"
	case CharsetMeta:
		return "meta"
	case CharsetDetect:
		return "detect"
	case CharsetDefault:
		return "default"
	default:
		return "none"
	}
}

// CharsetResult is the outcome of converting the body to UTF-8.
type CharsetResult struct {
	// UTF8 is the body in UTF-8. Invalid sequences are replaced with
	// U+FFFD (RuneError), not an error. For an already-UTF-8 body the
	// bytes may share memory with ToUTF8's input (the fast no-copy path).
	// An empty body is an empty non-nil slice: an empty response is
	// valid, while nil further down the pipeline would mean "no body" and
	// would deprive the record of content_hash (T-138).
	UTF8 []byte
	// Charset is the canonical name of the source charset (WHATWG:
	// "utf-8", "windows-1251", "koi8-r", …); "" only for an empty body.
	Charset string
	// Source is the step at which the charset was determined.
	Source CharsetSource
}

// ToUTF8 converts the response body to UTF-8 by the spec 5.5 priority
// ("header, meta charset, detect"), with the BOM first:
//
//	BOM → Content-Type charset → meta charset (first 1024 bytes) →
//	detect (valid UTF-8; the Cyrillic frequency score) →
//	the windows-1252 default (WHATWG, a superset of latin-1).
//
// An unknown charset label in the header is skipped ("best effort": the
// page knows its own charset better than a broken header), and a
// windows-1252/latin-1 meta declaration is indistinguishable from the
// default — such bodies go through detect anyway. The function returns no
// error: unrecognized means windows-1252, invalid sequences mean U+FFFD.
func ToUTF8(h http.Header, body []byte) CharsetResult {
	if len(body) == 0 {
		// An empty body is a valid body (T-138): UTF8 is an empty but
		// non-nil slice. nil further down the pipeline reads as "no body
		// at all" (the ContentSaver fuse zeroes out content_hash), while
		// an empty response (200/400/404/500 with Content-Length: 0)
		// must reach Put with the hash of empty bytes: HashOf(nil) ==
		// HashOf([]byte{}), so dedup and the equivalence classes of empty
		// pages work like for any content.
		return CharsetResult{UTF8: []byte{}, Source: CharsetNone}
	}

	// The BOM overrides everything (WHATWG): the server had the last word
	// in the charset header, the actual bytes are the final authority.
	// UTF-32 BOMs are not detected (like WHATWG/browsers): a UTF-32LE
	// prefix FF FE 00 00 is taken for UTF-16LE. The unicode.UTF8 decoder
	// does not strip the BOM — we strip it ourselves; the UTF-16 decoder
	// consumes it with UseBOM.
	switch {
	case bytes.HasPrefix(body, []byte{0xEF, 0xBB, 0xBF}):
		return CharsetResult{
			UTF8:    sanitizeUTF8(body[3:]),
			Charset: "utf-8",
			Source:  CharsetBOM,
		}
	case bytes.HasPrefix(body, []byte{0xFF, 0xFE}):
		return decodeAs(xunicode.UTF16(xunicode.LittleEndian, xunicode.UseBOM), "utf-16le", CharsetBOM, body)
	case bytes.HasPrefix(body, []byte{0xFE, 0xFF}):
		return decodeAs(xunicode.UTF16(xunicode.BigEndian, xunicode.UseBOM), "utf-16be", CharsetBOM, body)
	}

	// Content-Type charset.
	if cs := headerCharset(h.Get("Content-Type")); cs != "" {
		if enc, err := htmlindex.Get(cs); err == nil {
			name, _ := htmlindex.Name(enc)
			return decodeAs(enc, name, CharsetHeader, body)
		}
		// An unknown label: skip to meta/detect.
	}

	// meta charset. The x/net/html/charset prescan (html.Tokenizer, both
	// meta kinds, utf-16→utf-8 normalization) is battle-tested parsing;
	// we would write our own worse. The header is empty (already handled),
	// there is no BOM — the BOM/header branches inside will not fire.
	enc, name, _ := charset.DetermineEncoding(previewBytes(body), "")
	metaMark := metaCharsetRe.Match(previewBytes(body))
	if enc == encoding.Nop {
		// utf-8: the meta declared it (including utf-16 normalization) or
		// the preview is valid UTF-8; only the mark tells the sources
		// apart.
		src := CharsetDetect
		if metaMark {
			src = CharsetMeta
		}
		return CharsetResult{UTF8: sanitizeUTF8(body), Charset: "utf-8", Source: src}
	}
	if name != "windows-1252" {
		// A concrete non-default charset means the prescan (meta).
		return decodeAs(enc, name, CharsetMeta, body)
	}

	// x/net came back with windows-1252 — either the meta declared it or
	// it is the default. Detect over the whole body: valid UTF-8 (x/net
	// only looked at the preview and never checked an ASCII body for
	// UTF-8)…
	if utf8.Valid(body) {
		return CharsetResult{UTF8: sanitizeUTF8(body), Charset: "utf-8", Source: CharsetDetect}
	}
	// …the Cyrillic frequency score: it rescues both unmarked legacy
	// content (koi8-r with no hints) and mangled
	// meta charset="windows-1252" pages.
	if enc, name := detectCyrillic(body); enc != nil {
		return decodeAs(enc, name, CharsetDetect, body)
	}
	src := CharsetDefault
	if metaMark {
		src = CharsetMeta
	}
	return decodeAs(charmap.Windows1252, "windows-1252", src, body)
}

// decodeAs decodes the body with a known charset: the result is UTF-8,
// invalid sequences become U+FFFD, not an error (spec 5.5 does not
// qualify broken bytes as a fetch failure).
func decodeAs(enc encoding.Encoding, name string, src CharsetSource, body []byte) CharsetResult {
	out, err := enc.NewDecoder().Bytes(body)
	if err != nil {
		// x/text decoders replace invalid bytes with U+FFFD without an
		// error; only truncated sequences lead here (an odd UTF-16 length
		// and the like) — keep the ready prefix and put U+FFFD at the
		// truncation point (the appended bytes are not nil).
		return CharsetResult{UTF8: append(out, "\uFFFD"...), Charset: name, Source: src}
	}
	return CharsetResult{UTF8: emptyIfNil(out), Charset: name, Source: src}
}

// emptyIfNil normalizes an empty decoded output into a non-nil slice: a
// body of a single UTF-16 BOM decodes to zero bytes, and a future x/text
// version may return such output as nil (the current one returns a
// make-allocated buffer). An empty body is a valid body; nil further down
// the pipeline means "no body" (T-138) — the ToUTF8 contract does not rely
// on library implementation details.
func emptyIfNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// sanitizeUTF8 replaces invalid sequences with U+FFFD (maximal
// subpartitions, the WHATWG/Go semantics): a valid body is returned as is,
// without a copy.
func sanitizeUTF8(b []byte) []byte {
	if utf8.Valid(b) {
		return b
	}
	out := make([]byte, 0, len(b)+2*utf8.UTFMax)
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && size == 1 {
			out = append(out, "\uFFFD"...)
			i++
			continue
		}
		out = append(out, b[i:i+size]...)
		i += size
	}
	return out
}

// headerCharset extracts the charset parameter from Content-Type; empty
// when the header is missing or unparseable (a broken header means no
// information, not a reason to give up the body).
func headerCharset(contentType string) string {
	if contentType == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return params["charset"]
}

// previewBytes is the first charsetPreview bytes of the body (the
// meta/BOM window).
func previewBytes(b []byte) []byte {
	if len(b) > charsetPreview {
		return b[:charsetPreview]
	}
	return b
}

// metaCharsetRe is a rough mark of "there is a meta charset in the
// window". Needed only for the Source field (the meta declared
// utf-8/1252, or a detect with no hints): it does not affect the charset
// choice — the prescan would already have returned the concrete charset
// if the meta had declared it correctly.
var metaCharsetRe = regexp.MustCompile(`(?i)<meta[^>]*\bcharset`)

// --- Cyrillic detect ---------------------------------------------------------

// cyrCharsets is the candidate list for the frequency detect.
// x/net/html/charset has no content heuristic (meta prescan, the UTF-8
// check and the windows-1252 default), while spec 5.5 requires a
// "detect": typical unmarked non-UTF-8 content is Cyrillic legacy
// (koi8-r, windows-1251, iso-8859-5); other scripts are left to the
// WHATWG default.
var cyrCharsets = []struct {
	enc  encoding.Encoding
	name string
	cm   *charmap.Charmap
}{
	{charmap.KOI8R, "koi8-r", charmap.KOI8R},
	{charmap.Windows1251, "windows-1251", charmap.Windows1251},
	{charmap.ISO8859_5, "iso-8859-5", charmap.ISO8859_5},
}

// ruLetterFreq is the frequencies of Russian lower-case letters (shares).
// The correct charset accumulates the frequent о/е/а/и — the sum of
// logarithms of frequencies tells the candidates apart: a wrong reading
// yields rare letters.
var ruLetterFreq = map[rune]float64{
	'о': 0.110, 'е': 0.085, 'а': 0.080, 'и': 0.074, 'н': 0.066,
	'т': 0.064, 'с': 0.051, 'р': 0.047, 'в': 0.045, 'л': 0.044,
	'к': 0.035, 'м': 0.032, 'д': 0.030, 'п': 0.028, 'у': 0.023,
	'я': 0.020, 'ы': 0.019, 'з': 0.018, 'б': 0.016, 'г': 0.015,
	'ч': 0.015, 'й': 0.012, 'х': 0.010, 'ж': 0.009, 'ю': 0.006,
	'ш': 0.005, 'ц': 0.004, 'щ': 0.004, 'э': 0.003, 'ф': 0.002,
	'ъ': 0.0002, 'ё': 0.0002, 'ь': 0.014,
}

// detectCyrillic looks for a single-byte Cyrillic charset by letter
// frequencies: it decodes bytes >= 0x80 with each candidate and compares
// the sum of letter frequency logarithms. It returns nil when the
// Cyrillic signal is not enough (too few letters, or the text is more
// likely Latin with diacritics) — the windows-1252 default remains.
func detectCyrillic(body []byte) (encoding.Encoding, string) {
	best := -1
	bestScore := 0.0
	letters := 0
	for i, c := range cyrCharsets {
		score := 0.0
		n := 0
		for _, b := range body {
			if b < 0x80 {
				continue
			}
			r := c.cm.DecodeByte(b)
			if !isCyrillic(r) {
				continue
			}
			n++
			score += math.Log(letterFreq(unicode.ToLower(r)))
		}
		if n >= cyrillicMinLetters && (best < 0 || score > bestScore) {
			best, bestScore, letters = i, score, n
		}
	}
	if best < 0 {
		return nil, ""
	}
	// The threshold on the average (geometric) letter frequency: Russian
	// text gives ~0.04–0.07 (frequent vowels), Latin with diacritics read
	// as Cyrillic is a set of rare letters and < 0.02.
	if avg := math.Exp(bestScore / float64(letters)); avg < cyrillicMinAvgFreq {
		return nil, ""
	}
	return cyrCharsets[best].enc, cyrCharsets[best].name
}

// letterFreq is the frequency of a Russian letter (unknown letters get a
// rare penalty so that odd readings do not score points).
func letterFreq(r rune) float64 {
	if f, ok := ruLetterFreq[r]; ok {
		return f
	}
	return 0.0005
}

// isCyrillic — the main Cyrillic block without extensions (А-я, Ё, ё).
func isCyrillic(r rune) bool {
	return (r >= 0x0410 && r <= 0x044F) || r == 0x0401 || r == 0x0451
}
