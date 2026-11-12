// Charset and compression tests (T-051, T-100, spec 5.5). Two blocks:
//
//   - ToUTF8: detection priorities (BOM → header → meta → detect →
//     default), invalid sequence replacement, an empty body;
//   - compression: decodeContentEncoding (gzip/br/deflate/identity, broken
//     values, compound values — an RFC 9110 layer chain with a per-layer
//     cap, the cap against zip bombs, closing the source) and the
//     end-to-end Fetch/RoundTrip path — Accept-Encoding reaches the
//     server, the body is decompressed, the coding headers are stripped,
//     the "compression + charset" chain works together.

package httpfetch

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/andybalholm/brotli"
	"golang.org/x/text/encoding/charmap"
	xunicode "golang.org/x/text/encoding/unicode"
)

// --- helpers -----------------------------------------------------------------

// ruSample is natural Russian text for the frequency detect: the length is
// enough for a confident signal (>= cyrillicMinLetters), and the frequent
// letters are present.
const ruSample = "Сервис массовой проверки доменов ищет сайты на фреймворке Laravel: транспорт скачивает главную страницу, определяет кодировку ответа и приводит содержимое к универсальному формату, анализатор применяет правила и сохраняет результат проверки в хранилище."

// encodeBytes encodes UTF-8 text into a single-byte charmap charset.
func encodeBytes(t *testing.T, cm *charmap.Charmap, s string) []byte {
	t.Helper()
	b, err := cm.NewEncoder().Bytes([]byte(s))
	if err != nil {
		t.Fatalf("кодирование %v: %v", cm, err)
	}
	return b
}

// gzipBytes compresses data (the common case and zip bombs in tests).
func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// brotliBytes compresses data with brotli (the lowest quality — the test
// cares about correctness, not the compression ratio).
func brotliBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := brotli.NewWriterOptions(&buf, brotli.WriterOptions{Quality: 1})
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("brotli write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("brotli close: %v", err)
	}
	return buf.Bytes()
}

// utf16leBytes encodes text into UTF-16LE and prepends the BOM (as servers
// serving UTF-16 do).
func utf16leBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := xunicode.UTF16(xunicode.LittleEndian, xunicode.UseBOM).NewEncoder().Bytes([]byte(s))
	if err != nil {
		t.Fatalf("utf-16le encode: %v", err)
	}
	if !bytes.HasPrefix(b, []byte{0xFF, 0xFE}) {
		b = append([]byte{0xFF, 0xFE}, b...)
	}
	return b
}

// utf16beBytes encodes text into UTF-16BE with a BOM — by swapping the
// bytes of the LE representation (the test does not need a separate BE
// encoder).
func utf16beBytes(t *testing.T, s string) []byte {
	t.Helper()
	le := utf16leBytes(t, s)[2:] // LE bytes without the BOM
	be := make([]byte, 2+len(le))
	be[0], be[1] = 0xFE, 0xFF
	for i := 0; i+1 < len(le); i += 2 {
		be[2+i], be[2+i+1] = le[i+1], le[i]
	}
	return be
}

// headerOf is an http.Header from a single Content-Type.
func headerOf(contentType string) http.Header {
	h := http.Header{}
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	return h
}

// --- ToUTF8: charset detection ----------------------------------------------

// TestToUTF8EmptyBody: an empty body is a valid body (T-138): an empty but
// non-nil UTF8 slice (nil further down the pipeline means "no body";
// ContentSaver zeroes out content_hash) with no charset, whatever the
// input — nil or an empty slice.
func TestToUTF8EmptyBody(t *testing.T) {
	for name, body := range map[string][]byte{"nil": nil, "пустой слайс": {}} {
		got := ToUTF8(headerOf("text/html; charset=windows-1251"), body)
		if got.UTF8 == nil || len(got.UTF8) != 0 {
			t.Fatalf("%s: UTF8=%#v, ожидался пустой не-nil слайс", name, got.UTF8)
		}
		if got.Charset != "" || got.Source != CharsetNone {
			t.Fatalf("%s: получили %+v, ожидались пустой charset и CharsetNone", name, got)
		}
	}
}

// TestToUTF8BOMOnlyBody: a body of a single BOM decodes to zero bytes —
// the result must remain a valid empty body (not nil, T-138): a UTF-8 BOM
// is stripped with a slice, UTF-16 with the UseBOM decoder, and an empty
// output is normalized by emptyIfNil.
func TestToUTF8BOMOnlyBody(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    []byte
		charset string
	}{
		{"utf-8 bom", []byte{0xEF, 0xBB, 0xBF}, "utf-8"},
		{"utf-16le bom", []byte{0xFF, 0xFE}, "utf-16le"},
		{"utf-16be bom", []byte{0xFE, 0xFF}, "utf-16be"},
	} {
		got := ToUTF8(headerOf("text/html"), tc.body)
		if got.UTF8 == nil || len(got.UTF8) != 0 {
			t.Fatalf("%s: UTF8=%#v, ожидался пустой не-nil слайс", tc.name, got.UTF8)
		}
		if got.Charset != tc.charset || got.Source != CharsetBOM {
			t.Fatalf("%s: charset=%q source=%v, ожидались %q/bom", tc.name, got.Charset, got.Source, tc.charset)
		}
	}
}

// TestToUTF8HeaderCharset: Content-Type charset=windows-1251 — Russian
// text decodes, the source is header.
func TestToUTF8HeaderCharset(t *testing.T) {
	got := ToUTF8(headerOf("text/html; charset=windows-1251"),
		encodeBytes(t, charmap.Windows1251, ruSample))
	if string(got.UTF8) != ruSample {
		t.Errorf("декод windows-1251: получили %q", got.UTF8)
	}
	if got.Charset != "windows-1251" || got.Source != CharsetHeader {
		t.Errorf("источник: charset=%q source=%v, ожидались windows-1251/header", got.Charset, got.Source)
	}
}

// TestToUTF8HeaderAliasAndCase: alias labels and case (CP1251) produce the
// canonical name.
func TestToUTF8HeaderAliasAndCase(t *testing.T) {
	got := ToUTF8(headerOf(`text/html; charset="CP1251"`),
		encodeBytes(t, charmap.Windows1251, ruSample))
	if got.Charset != "windows-1251" || got.Source != CharsetHeader || string(got.UTF8) != ruSample {
		t.Errorf("алиас CP1251: charset=%q source=%v совпадение=%v",
			got.Charset, got.Source, string(got.UTF8) == ruSample)
	}
}

// TestToUTF8BOMUTF8: a UTF-8 BOM overrides the header and is stripped.
func TestToUTF8BOMUTF8(t *testing.T) {
	body := append([]byte{0xEF, 0xBB, 0xBF}, ruSample...)
	got := ToUTF8(headerOf("text/html; charset=koi8-r"), body)
	if got.Source != CharsetBOM || got.Charset != "utf-8" {
		t.Fatalf("источник: %+v, ожидался BOM/utf-8", got)
	}
	if string(got.UTF8) != ruSample {
		t.Errorf("BOM не снят или тело искажено: %q", got.UTF8)
	}
}

// TestToUTF8BOMUTF16: UTF-16LE/BE with a BOM decode (the decoder consumes
// the BOM), the header contradicts — it does not matter, the BOM is
// stronger.
func TestToUTF8BOMUTF16(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
		want string
	}{
		{"utf-16le", utf16leBytes(t, "Привет, мир!"), "utf-16le"},
		{"utf-16be", utf16beBytes(t, "Привет, мир!"), "utf-16be"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ToUTF8(headerOf("text/html; charset=windows-1251"), tc.body)
			if got.Source != CharsetBOM || got.Charset != tc.want {
				t.Fatalf("источник: charset=%q source=%v, ожидались %q/bom", got.Charset, got.Source, tc.want)
			}
			if string(got.UTF8) != "Привет, мир!" {
				t.Errorf("декод: %q", got.UTF8)
			}
		})
	}
}

// TestToUTF8MetaCharset: a page without a charset in the header declares
// it via <meta charset> (the first 1024 bytes).
func TestToUTF8MetaCharset(t *testing.T) {
	body := encodeBytes(t, charmap.Windows1251,
		"<html><head><meta charset=\"windows-1251\"><title>Проверка</title></head><body>"+ruSample+"</body></html>")
	got := ToUTF8(headerOf("text/html"), body)
	if got.Charset != "windows-1251" || got.Source != CharsetMeta {
		t.Fatalf("источник: charset=%q source=%v, ожидались windows-1251/meta", got.Charset, got.Source)
	}
	if !strings.Contains(string(got.UTF8), ruSample) {
		t.Errorf("декод по meta: тело не содержит образец: %q", got.UTF8)
	}
}

// TestToUTF8MetaHTTPEquiv: <meta http-equiv="Content-Type"
// content="...; charset=..."> — the second kind of declaration.
func TestToUTF8MetaHTTPEquiv(t *testing.T) {
	body := encodeBytes(t, charmap.KOI8R,
		`<html><head><meta http-equiv="Content-Type" content="text/html; charset=koi8-r"></head><body>`+ruSample+`</body></html>`)
	got := ToUTF8(headerOf(""), body)
	if got.Charset != "koi8-r" || got.Source != CharsetMeta {
		t.Fatalf("источник: charset=%q source=%v, ожидались koi8-r/meta", got.Charset, got.Source)
	}
	if !strings.Contains(string(got.UTF8), ruSample) {
		t.Errorf("декод по http-equiv: %q", got.UTF8)
	}
}

// TestToUTF8HeaderBeatsMeta: the header takes priority over meta
// (spec 5.5).
func TestToUTF8HeaderBeatsMeta(t *testing.T) {
	body := encodeBytes(t, charmap.KOI8R,
		`<html><head><meta charset="windows-1251"></head><body>`+ruSample+`</body></html>`)
	got := ToUTF8(headerOf("text/html; charset=koi8-r"), body)
	if got.Charset != "koi8-r" || got.Source != CharsetHeader {
		t.Fatalf("приоритет: charset=%q source=%v, ожидались koi8-r/header", got.Charset, got.Source)
	}
}

// TestToUTF8UnknownHeaderLabel: an unknown header label — "best effort":
// skip it, the page's declaration decides.
func TestToUTF8UnknownHeaderLabel(t *testing.T) {
	body := encodeBytes(t, charmap.KOI8R,
		`<html><head><meta charset="koi8-r"></head><body>`+ruSample+`</body></html>`)
	got := ToUTF8(headerOf("text/html; charset=x-unknown-cp866"), body)
	if got.Charset != "koi8-r" || got.Source != CharsetMeta {
		t.Fatalf("неизвестная метка: charset=%q source=%v, ожидались koi8-r/meta", got.Charset, got.Source)
	}
}

// TestToUTF8DetectUTF8: with no hints, valid UTF-8 (and clean ASCII) is
// detected as utf-8, the bytes pass through undistorted.
func TestToUTF8DetectUTF8(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"ascii", "<html><body>plain ascii page</body></html>"},
		{"utf8", "<html><body>" + ruSample + "</body></html>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ToUTF8(headerOf("text/html"), []byte(tc.body))
			if got.Charset != "utf-8" || got.Source != CharsetDetect {
				t.Fatalf("источник: charset=%q source=%v, ожидались utf-8/detect", got.Charset, got.Source)
			}
			if string(got.UTF8) != tc.body {
				t.Errorf("тело изменилось: %q", got.UTF8)
			}
		})
	}
}

// TestToUTF8DetectKOI8R: Russian text in koi8-r with no hints at all — the
// frequency detect (x/net has no content heuristic; the default would
// give windows-1252 and mojibake).
func TestToUTF8DetectKOI8R(t *testing.T) {
	got := ToUTF8(headerOf(""), encodeBytes(t, charmap.KOI8R, ruSample))
	if got.Charset != "koi8-r" || got.Source != CharsetDetect {
		t.Fatalf("детект: charset=%q source=%v, ожидались koi8-r/detect", got.Charset, got.Source)
	}
	if string(got.UTF8) != ruSample {
		t.Errorf("декод: %q", got.UTF8)
	}
}

// TestToUTF8DetectWindows1251: likewise for windows-1251 (a frequent
// legacy charset of the RU segment) — the detect tells koi8-r and cp1251
// apart.
func TestToUTF8DetectWindows1251(t *testing.T) {
	got := ToUTF8(headerOf(""), encodeBytes(t, charmap.Windows1251, ruSample))
	if got.Charset != "windows-1251" || got.Source != CharsetDetect {
		t.Fatalf("детект: charset=%q source=%v, ожидались windows-1251/detect", got.Charset, got.Source)
	}
	if string(got.UTF8) != ruSample {
		t.Errorf("декод: %q", got.UTF8)
	}
}

// TestToUTF8DetectRescuesMislabeledMeta: the meta lies (windows-1252), the
// body is windows-1251: detect kicks in for invalid UTF-8 and rescues the
// content.
func TestToUTF8DetectRescuesMislabeledMeta(t *testing.T) {
	body := encodeBytes(t, charmap.Windows1251,
		`<html><head><meta charset="windows-1252"></head><body>`+ruSample+`</body></html>`)
	got := ToUTF8(headerOf("text/html"), body)
	if got.Charset != "windows-1251" || got.Source != CharsetDetect {
		t.Fatalf("спасение: charset=%q source=%v, ожидались windows-1251/detect", got.Charset, got.Source)
	}
	if !strings.Contains(string(got.UTF8), ruSample) {
		t.Errorf("декод: %q", got.UTF8)
	}
}

// TestToUTF8DefaultWindows1252: Latin with diacritics and no hints — the
// Cyrillic signal is not enough, the WHATWG windows-1252 default (a
// superset of latin-1).
func TestToUTF8DefaultWindows1252(t *testing.T) {
	got := ToUTF8(headerOf(""), encodeBytes(t, charmap.Windows1252, "École naïve café"))
	if got.Charset != "windows-1252" || got.Source != CharsetDefault {
		t.Fatalf("умолчание: charset=%q source=%v, ожидались windows-1252/default", got.Charset, got.Source)
	}
	if string(got.UTF8) != "École naïve café" {
		t.Errorf("latin-1 декод: %q", got.UTF8)
	}
}

// TestToUTF8InvalidUTF8Replaced: utf-8 is declared, the bytes are broken —
// U+FFFD replacement instead of an error, the valid part survives.
func TestToUTF8InvalidUTF8Replaced(t *testing.T) {
	body := []byte("текст\xff\xfe и \xc3 конец")
	got := ToUTF8(headerOf("text/html; charset=utf-8"), body)
	if got.Charset != "utf-8" || got.Source != CharsetHeader {
		t.Fatalf("источник: %+v", got)
	}
	if !strings.Contains(string(got.UTF8), "\uFFFD") {
		t.Errorf("ожидались замены U+FFFD: %q", got.UTF8)
	}
	if !strings.Contains(string(got.UTF8), "текст") || !strings.Contains(string(got.UTF8), "конец") {
		t.Errorf("валидная часть потеряна: %q", got.UTF8)
	}
}

// --- compression: decodeContentEncoding -------------------------------------

// TestDecodeContentEncoding: identity/empty — the body as is; gzip and br
// decompress; the default cap does not cut ordinary bodies.
func TestDecodeContentEncoding(t *testing.T) {
	plain := []byte(ruSample)

	t.Run("identity", func(t *testing.T) {
		rc := io.NopCloser(bytes.NewReader(plain))
		got, err := decodeContentEncoding(rc, "identity", 0)
		if err != nil {
			t.Fatalf("identity: %v", err)
		}
		b, err := io.ReadAll(got)
		if err != nil || !bytes.Equal(b, plain) {
			t.Errorf("identity: %q err=%v", b, err)
		}
		_ = got.Close()
	})

	t.Run("пустое-значение", func(t *testing.T) {
		rc := io.NopCloser(bytes.NewReader(plain))
		got, err := decodeContentEncoding(rc, "", 0)
		if err != nil {
			t.Fatalf("пусто: %v", err)
		}
		b, _ := io.ReadAll(got)
		if !bytes.Equal(b, plain) {
			t.Errorf("пусто: %q", b)
		}
		_ = got.Close()
	})

	for _, tc := range []struct {
		name       string
		enc        string
		compressed func() []byte
	}{
		{"gzip", "gzip", func() []byte { return gzipBytes(t, plain) }},
		{"gzip-регистр", " GZip ", func() []byte { return gzipBytes(t, plain) }},
		{"x-gzip-алиас", "x-gzip", func() []byte { return gzipBytes(t, plain) }},
		{"br", "br", func() []byte { return brotliBytes(t, plain) }},
		{"deflate-zlib", "deflate", func() []byte { return zlibBytes(t, plain) }},
		{"deflate-регистр", " Deflate ", func() []byte { return zlibBytes(t, plain) }},
		{"deflate-сырой", "deflate", func() []byte { return flateRawBytes(t, plain) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeContentEncoding(io.NopCloser(bytes.NewReader(tc.compressed())), tc.enc, 0)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			b, err := io.ReadAll(got)
			if err != nil || !bytes.Equal(b, plain) {
				t.Errorf("%s: длина=%d err=%v", tc.name, len(b), err)
			}
			_ = got.Close()
		})
	}
}

// TestDecodeContentEncodingUnsupported: exotic and broken compound values
// are an error (the crawler advertises only gzip and br; a single deflate
// and correct compound values are accepted defensively — a broken server
// sends them unrequested, and an empty or unknown element within the
// value cannot be parsed).
func TestDecodeContentEncodingUnsupported(t *testing.T) {
	for _, ce := range []string{
		"compress", "zstd", "*", "gzip, zstd", "zstd, gzip",
		"gzip,,br", "gzip,", ",br", "br,,",
	} {
		if _, err := decodeContentEncoding(io.NopCloser(bytes.NewReader(nil)), ce, 0); err == nil {
			t.Errorf("Content-Encoding %q: ожидалась ошибка", ce)
		}
	}
}

// TestDecodeContentEncodingLayered: compound Content-Encoding values
// (T-100, remainder H-10) — a decompression chain in RFC 9110 order: the
// codings are applied by the server in enumeration order, the last one in
// the list forms the outermost layer ("gzip, br" = the server compressed
// with gzip, then wrapped in br — the body is br(gzip(data))). identity in
// a compound value is a no-op and builds no layer; the per-layer cap does
// not cut ordinary bodies.
func TestDecodeContentEncodingLayered(t *testing.T) {
	plain := []byte(ruSample)
	for _, tc := range []struct {
		name string
		enc  string
		body []byte
	}{
		{"gzip-затем-br", "gzip, br", brotliBytes(t, gzipBytes(t, plain))},
		{"br-затем-gzip", "br, gzip", gzipBytes(t, brotliBytes(t, plain))},
		{"gzip-в-gzip", "gzip, gzip", gzipBytes(t, gzipBytes(t, plain))},
		{"deflate-внешний", "gzip, deflate", zlibBytes(t, gzipBytes(t, plain))},
		{"deflate-внешний-сырой", "br, deflate", flateRawBytes(t, brotliBytes(t, plain))},
		{"deflate-внутренний", "deflate, gzip", gzipBytes(t, zlibBytes(t, plain))},
		{"identity-в-середине", "gzip, identity, br", brotliBytes(t, gzipBytes(t, plain))},
		{"identity-внутри", "identity, gzip", gzipBytes(t, plain)},
		{"x-gzip-в-составе", "x-gzip, br", brotliBytes(t, gzipBytes(t, plain))},
		{"регистр-и-пробелы", " GZip , BR ", brotliBytes(t, gzipBytes(t, plain))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeContentEncoding(io.NopCloser(bytes.NewReader(tc.body)), tc.enc, 0)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			b, err := io.ReadAll(got)
			if err != nil || !bytes.Equal(b, plain) {
				t.Errorf("%s: длина=%d err=%v", tc.name, len(b), err)
			}
			_ = got.Close()
		})
	}
}

// TestDecodeContentEncodingLayeredBomb: compressed-in-compressed beyond
// the cap — ErrDecompressTooLarge, the bytes up to the cap are available
// (the T-051 contract holds in the chain: the final layer stops at
// limit + decompressTailMargin, intermediate layers are bounded by the
// same per-layer cap — the total bomb protection does not weaken).
func TestDecodeContentEncodingLayeredBomb(t *testing.T) {
	bomb := bytes.Repeat([]byte("A"), 1<<20)
	got, err := decodeContentEncoding(
		io.NopCloser(bytes.NewReader(brotliBytes(t, gzipBytes(t, bomb)))), "gzip, br", 4096)
	if err != nil {
		t.Fatalf("обёртка бомбы: %v", err)
	}
	defer func() { _ = got.Close() }()
	b, err := io.ReadAll(got)
	if !errors.Is(err, ErrDecompressTooLarge) {
		t.Fatalf("ожидался ErrDecompressTooLarge, получено err=%v (прочитано %d)", err, len(b))
	}
	if len(b) != 4096 {
		t.Errorf("до потолка должно читаться ровно 4096, прочитано %d", len(b))
	}
}

// zlibBytes is a zlib wrapper (RFC 1950) over deflate.
func zlibBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	return buf.Bytes()
}

// flateRawBytes is a raw deflate stream (RFC 1951, no zlib header): this
// is the form of "Content-Encoding: deflate" served by servers that never
// read RFC 1950.
func flateRawBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	fw, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	if _, err := fw.Write(b); err != nil {
		t.Fatalf("flate write: %v", err)
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("flate close: %v", err)
	}
	return buf.Bytes()
}

// TestDecodeContentEncodingBomb: a decompressed stream beyond the cap —
// ErrDecompressTooLarge, the bytes up to the cap are available.
func TestDecodeContentEncodingBomb(t *testing.T) {
	bomb := bytes.Repeat([]byte("A"), 1<<20) // decompresses to 1 MB
	got, err := decodeContentEncoding(io.NopCloser(bytes.NewReader(gzipBytes(t, bomb))), "gzip", 4096)
	if err != nil {
		t.Fatalf("обёртка бомбы: %v", err)
	}
	defer func() { _ = got.Close() }()
	b, err := io.ReadAll(got)
	if !errors.Is(err, ErrDecompressTooLarge) {
		t.Fatalf("ожидался ErrDecompressTooLarge, получено err=%v (прочитано %d)", err, len(b))
	}
	if len(b) != 4096 {
		t.Errorf("до потолка должно читаться ровно 4096, прочитано %d", len(b))
	}
}

// TestDecodeContentEncodingExactLimit: a body exactly at the cap is not an
// error: the probe byte beyond it gets EOF.
func TestDecodeContentEncodingExactLimit(t *testing.T) {
	plain := bytes.Repeat([]byte("B"), 1000)
	got, err := decodeContentEncoding(io.NopCloser(bytes.NewReader(gzipBytes(t, plain))), "gzip", 1000)
	if err != nil {
		t.Fatalf("обёртка: %v", err)
	}
	b, err := io.ReadAll(got)
	if err != nil || !bytes.Equal(b, plain) {
		t.Fatalf("ровно в потолок: len=%d err=%v", len(b), err)
	}
	_ = got.Close()
}

// TestDecodeContentEncodingClosesSource: the wrapper's Close closes the
// original body exactly once (also on a decompression error).
func TestDecodeContentEncodingClosesSource(t *testing.T) {
	var mu sync.Mutex
	closed := 0
	newRC := func(b []byte) io.ReadCloser {
		return &countingCloser{Reader: bytes.NewReader(b), onClose: func() {
			mu.Lock()
			closed++
			mu.Unlock()
		}}
	}

	got, err := decodeContentEncoding(newRC(gzipBytes(t, []byte("тело"))), "gzip", 1<<20)
	if err != nil {
		t.Fatalf("обёртка: %v", err)
	}
	_, _ = io.ReadAll(got)
	if err := got.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A decompression error (a broken stream) must close the source too —
	// the caller does it (decodeResponseBody); we check the wrapper's
	// contract: after a read with an error, Close still works.
	bad := append(gzipBytes(t, []byte("x")), 0xFF, 0xFF, 0xFF)
	got2, err := decodeContentEncoding(newRC(bad), "gzip", 1<<20)
	if err != nil {
		t.Fatalf("обёртка битого: %v", err)
	}
	_, _ = io.ReadAll(got2) // the stream error does not matter for the test
	_ = got2.Close()

	mu.Lock()
	defer mu.Unlock()
	if closed != 2 {
		t.Errorf("закрытий исходного тела: %d, ожидалось 2", closed)
	}
}

// countingCloser is a source that counts Close calls.
type countingCloser struct {
	io.Reader
	onClose func()
}

func (c *countingCloser) Close() error {
	c.onClose()
	return nil
}

// --- compression: the end-to-end Fetch path ---------------------------------

// encodingServer is an httptest server serving a body with the given
// Content-Encoding/Content-Type and remembering the request's
// Accept-Encoding.
func encodingServer(t *testing.T, body []byte, hdr map[string]string) (*httptest.Server, *syncValue) {
	t.Helper()
	ae := &syncValue{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ae.Set(r.Header.Get("Accept-Encoding"))
		for k, v := range hdr {
			w.Header().Set(k, v)
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, ae
}

// syncValue is a string written from the server goroutine.
type syncValue struct {
	mu sync.Mutex
	v  string
}

func (s *syncValue) Set(v string) {
	s.mu.Lock()
	s.v = v
	s.mu.Unlock()
}

func (s *syncValue) Get() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.v
}

// fetchFrom is a Fetch to the test server at the loopback address.
func fetchFrom(t *testing.T, f *Fetcher, rawurl string) *FetchResult {
	t.Helper()
	u, err := url.Parse(rawurl)
	if err != nil {
		t.Fatalf("url %q: %v", rawurl, err)
	}
	res, err := f.Fetch(t.Context(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("fetch %s: %v", rawurl, err)
	}
	return res
}

// TestFetchAcceptEncoding: the transport sends Accept-Encoding: gzip, br.
func TestFetchAcceptEncoding(t *testing.T) {
	srv, ae := encodingServer(t, []byte("ok"), nil)
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	if got := ae.Get(); got != "gzip, br" {
		t.Errorf("Accept-Encoding сервера: %q, ожидался %q", got, "gzip, br")
	}
}

// TestFetchGzipRoundtrip: the server serves gzip — the consumer reads the
// decompressed body, the coding headers are stripped (the server's
// truthful gzip-length Content-Length disappears together with
// Content-Encoding).
func TestFetchGzipRoundtrip(t *testing.T) {
	plain := "<html><body>" + ruSample + "</body></html>"
	gz := gzipBytes(t, []byte(plain))
	srv, _ := encodingServer(t, gz, map[string]string{
		"Content-Encoding": "gzip",
		"Content-Type":     "text/html",
		"Content-Length":   strconv.Itoa(len(gz)),
	})
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if string(b) != plain {
		t.Errorf("тело: %q", b)
	}
	if v := res.Header.Get("Content-Encoding"); v != "" {
		t.Errorf("Content-Encoding не снят: %q", v)
	}
	if v := res.Header.Get("Content-Length"); v != "" {
		t.Errorf("Content-Length не снят: %q", v)
	}
}

// TestFetchBrotliRoundtrip: br decompresses through the end-to-end path
// (the andybalholm/brotli dependency: stdlib has no brotli, while
// spec 5.5 requires "Accept-Encoding: gzip, br").
func TestFetchBrotliRoundtrip(t *testing.T) {
	plain := "<html><body>" + ruSample + "</body></html>"
	srv, _ := encodingServer(t, brotliBytes(t, []byte(plain)), map[string]string{
		"Content-Encoding": "br",
	})
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if string(b) != plain {
		t.Errorf("тело: %q", b)
	}
}

// TestFetchIdentityKeepsHeaders: without compression the headers are left
// alone.
func TestFetchIdentityKeepsHeaders(t *testing.T) {
	srv, _ := encodingServer(t, []byte("plain"), map[string]string{
		"Content-Type":   "text/plain",
		"Content-Length": "5",
	})
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, _ := io.ReadAll(res.Body)
	if string(b) != "plain" {
		t.Errorf("тело: %q", b)
	}
	if v := res.Header.Get("Content-Length"); v != "5" {
		t.Errorf("Content-Length искажён: %q", v)
	}
}

// TestFetchMultiLineContentEncoding: a list-form Content-Encoding spread
// over several lines (RFC 9110 §5.3 — equivalent to the comma-separated
// list) is assembled into a single value: "gzip" + "br" on two lines is
// read as compound packaging (Get would take only the first line and trip
// over the second layer's header; P2 review of T-100).
func TestFetchMultiLineContentEncoding(t *testing.T) {
	plain := "<html><body>" + ruSample + "</body></html>"
	body := brotliBytes(t, gzipBytes(t, []byte(plain)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Encoding", "gzip")
		w.Header().Add("Content-Encoding", "br")
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if string(b) != plain {
		t.Errorf("тело: %q", b)
	}
	if v := res.Header.Get("Content-Encoding"); v != "" {
		t.Errorf("Content-Encoding не снят: %q", v)
	}
}

// TestFetchIdentityCompositeKeepsHeaders: a compound value of only
// identities does not transform the body — the coding headers remain
// truthful and are not stripped (as with a single identity; P3 review of
// T-100).
func TestFetchIdentityCompositeKeepsHeaders(t *testing.T) {
	body := []byte("plain-identity")
	srv, _ := encodingServer(t, body, map[string]string{
		"Content-Encoding": "identity, identity",
		"Content-Type":     "text/plain",
		"Content-Length":   strconv.Itoa(len(body)),
	})
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, _ := io.ReadAll(res.Body)
	if !bytes.Equal(b, body) {
		t.Errorf("тело: %q", b)
	}
	if v := res.Header.Get("Content-Encoding"); v != "identity, identity" {
		t.Errorf("Content-Encoding искажён: %q", v)
	}
	if v := res.Header.Get("Content-Length"); v != strconv.Itoa(len(body)) {
		t.Errorf("Content-Length искажён: %q", v)
	}
}

// TestFetchGzipEmptyBody: Content-Encoding: gzip with an empty body is not
// an error (gzip.NewReader returns EOF); emptiness is read.
func TestFetchGzipEmptyBody(t *testing.T) {
	srv, _ := encodingServer(t, nil, map[string]string{"Content-Encoding": "gzip"})
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("пустое gzip-тело: %v", err)
	}
	if len(b) != 0 {
		t.Errorf("ожидалось пустое тело, получено %q", b)
	}
}

// TestFetchUnsupportedEncoding: the server sent an unrequested
// Content-Encoding — an honest error, not compressed bytes as the body.
func TestFetchUnsupportedEncoding(t *testing.T) {
	srv, _ := encodingServer(t, []byte("x"), map[string]string{
		"Content-Encoding": "compress",
	})
	f := NewFetcher(Options{})
	u, _ := url.Parse(srv.URL)
	_, err := f.Fetch(t.Context(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err == nil || !strings.Contains(err.Error(), "Content-Encoding") {
		t.Fatalf("ожидалась ошибка Content-Encoding, получено %v", err)
	}
}

// TestFetchDeflateRoundtrip: an unrequested deflate (a zlib wrapper and a
// raw stream — the historical ambiguity of servers) decompresses: the page
// does not fail with a fetch error, the coding headers are stripped;
// Accept-Encoding still advertises only gzip and br.
func TestFetchDeflateRoundtrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
		want string
	}{
		{"zlib", zlibBytes(t, []byte("deflate-zlib-body")), "deflate-zlib-body"},
		{"сырой-поток", flateRawBytes(t, []byte("deflate-raw-body")), "deflate-raw-body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, ae := encodingServer(t, tc.body, map[string]string{
				"Content-Encoding": "deflate",
			})
			f := NewFetcher(Options{})
			res := fetchFrom(t, f, srv.URL)
			defer func() { _ = res.Body.Close() }()
			b, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("чтение: %v", err)
			}
			if string(b) != tc.want {
				t.Errorf("тело: %q, ожидалось %q", b, tc.want)
			}
			if res.Header.Get("Content-Encoding") != "" {
				t.Errorf("Content-Encoding не снят: %q", res.Header.Get("Content-Encoding"))
			}
			if ae.Get() != acceptEncoding {
				t.Errorf("Accept-Encoding: %q, ожидался %q (deflate не рекламируем)", ae.Get(), acceptEncoding)
			}
		})
	}
}

// TestFetchGzipBomb: the decompressed stream exceeds MaxBodyBytes —
// reading the body returns ErrDecompressTooLarge (the decompression cap
// sits before the readable body limit of T-049 but is checked with the
// same number).
func TestFetchGzipBomb(t *testing.T) {
	bomb := bytes.Repeat([]byte("A"), 1<<20)
	srv, _ := encodingServer(t, gzipBytes(t, bomb), map[string]string{
		"Content-Encoding": "gzip",
	})
	f := NewFetcher(Options{MaxBodyBytes: 64 << 10})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if !errors.Is(err, ErrDecompressTooLarge) {
		t.Fatalf("ожидался ErrDecompressTooLarge, получено %v (прочитано %d)", err, len(b))
	}
	if len(b) != 64<<10 {
		t.Errorf("до потолка должно читаться 64 КиБ, прочитано %d", len(b))
	}
}

// runBodyLimitCase is a shared run of the "body vs limit" case
// (encodingServer + Fetch + truncation checks): used by single-layer gzip
// and compound packaging (T-100) — the truncation semantics are uniform,
// the tail classification runs on the final decoded stream.
func runBodyLimitCase(t *testing.T, ce string, wrap func([]byte) []byte, size, limit int, wantTrunc bool) {
	t.Helper()
	srv, _ := encodingServer(t, wrap(bytes.Repeat([]byte("z"), size)), map[string]string{
		"Content-Encoding": ce,
	})
	f := NewFetcher(Options{
		MaxBodyBytes: int64(limit),
		Limits:       &LimitOptions{MaxBodyBytes: int64(limit)},
	})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		t.Fatalf("обрезка — не ошибка чтения: %v", readErr)
	}
	if len(b) != min(size, limit) {
		t.Errorf("прочитано %d байт, ожидалось %d", len(b), min(size, limit))
	}
	if res.BodyLimiter == nil || !res.BodyLimiter.Truncated() == wantTrunc {
		t.Errorf("признак обрезки: %v, ожидался %v", res.BodyLimiter.Truncated(), wantTrunc)
	}
}

// TestFetchGzipJustOverLimit: a compressed body slightly over the limit
// behaves like an uncompressed one over the limit
// (TestFetchBodyLimitExactAndTruncated in limits_test.go): truncation at
// the limit with Truncated and a read without error
// (spec 8, "the data is partial, the status is known"), not
// ErrDecompressTooLarge, which the worker would read as a read error and
// zero out the Available of a live site. The decompression and read caps
// are equal (the production NewOptions sets both from
// http.max_body_bytes).
func TestFetchGzipJustOverLimit(t *testing.T) {
	run := func(t *testing.T, size, limit int, wantTrunc bool) {
		t.Helper()
		runBodyLimitCase(t, "gzip", func(b []byte) []byte { return gzipBytes(t, b) }, size, limit, wantTrunc)
	}
	t.Run("ровно в лимит", func(t *testing.T) { run(t, 1000, 1000, false) })
	t.Run("на байт больше", func(t *testing.T) { run(t, 1001, 1000, true) })
	t.Run("заметно больше", func(t *testing.T) { run(t, 64<<10, 1000, true) })
}

// TestFetchLayeredJustOverLimit: compound packaging "slightly over the
// limit" behaves like the single-layer one (TestFetchGzipJustOverLimit):
// truncation with Truncated and a read without error; the tail
// classification runs on the final (last decoded) stream — the H-2
// semantics do not depend on the number of layers.
func TestFetchLayeredJustOverLimit(t *testing.T) {
	run := func(t *testing.T, size, limit int, wantTrunc bool) {
		t.Helper()
		runBodyLimitCase(t, "gzip, br", func(b []byte) []byte {
			return brotliBytes(t, gzipBytes(t, b))
		}, size, limit, wantTrunc)
	}
	t.Run("ровно в лимит", func(t *testing.T) { run(t, 1000, 1000, false) })
	t.Run("на байт больше", func(t *testing.T) { run(t, 1001, 1000, true) })
	t.Run("заметно больше", func(t *testing.T) { run(t, 64<<10, 1000, true) })
}

// TestFetchGzipBombUnderLimiter: a bomb with the T-049 limits enabled —
// ErrDecompressTooLarge (the T-051 contract: a bomb page is a body error,
// the worker writes no records and zeroes Available); the tail
// classification spends no more of the bomb than limit +
// decompressTailMargin, the body up to the limit is available. Truncation
// without an error is only a tail within the margin
// (TestFetchGzipJustOverLimit), not a bomb.
func TestFetchGzipBombUnderLimiter(t *testing.T) {
	bomb := bytes.Repeat([]byte("A"), 1<<20)
	srv, _ := encodingServer(t, gzipBytes(t, bomb), map[string]string{
		"Content-Encoding": "gzip",
	})
	const limit = 64 << 10
	f := NewFetcher(Options{
		MaxBodyBytes: limit,
		Limits:       &LimitOptions{MaxBodyBytes: limit},
	})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, readErr := io.ReadAll(res.Body)
	if !errors.Is(readErr, ErrDecompressTooLarge) {
		t.Fatalf("бомба = %v, want ErrDecompressTooLarge (T-051)", readErr)
	}
	if len(b) != limit {
		t.Errorf("прочитано %d байт, ожидался лимит %d", len(b), limit)
	}
}

// TestFetchLayeredRoundtrip: compound Content-Encoding through the
// end-to-end path — the body is decompressed by a chain of layers
// (RFC 9110: the last coding in the list is the outermost layer), the
// coding headers are stripped; Accept-Encoding still advertises only gzip
// and br — compound values are not requested, they come from a broken
// server.
func TestFetchLayeredRoundtrip(t *testing.T) {
	plain := "<html><body>" + ruSample + "</body></html>"
	body := brotliBytes(t, gzipBytes(t, []byte(plain))) // "gzip, br"
	srv, ae := encodingServer(t, body, map[string]string{
		"Content-Encoding": "gzip, br",
		"Content-Type":     "text/html",
	})
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	if string(b) != plain {
		t.Errorf("тело: %q", b)
	}
	if v := res.Header.Get("Content-Encoding"); v != "" {
		t.Errorf("Content-Encoding не снят: %q", v)
	}
	if v := res.Header.Get("Content-Length"); v != "" {
		t.Errorf("Content-Length не снят: %q", v)
	}
	if ae.Get() != acceptEncoding {
		t.Errorf("Accept-Encoding: %q, ожидался %q (составные не рекламируем)", ae.Get(), acceptEncoding)
	}
}

// TestFetchLayeredBombUnderLimiter: a bomb in compound packaging with the
// T-049 limits enabled — ErrDecompressTooLarge, the body up to the limit
// is available (the T-051 contract does not depend on the number of
// layers: the per-layer cap is the same bodyCeiling).
func TestFetchLayeredBombUnderLimiter(t *testing.T) {
	bomb := bytes.Repeat([]byte("A"), 1<<20)
	srv, _ := encodingServer(t, brotliBytes(t, gzipBytes(t, bomb)), map[string]string{
		"Content-Encoding": "gzip, br",
	})
	const limit = 64 << 10
	f := NewFetcher(Options{
		MaxBodyBytes: limit,
		Limits:       &LimitOptions{MaxBodyBytes: limit},
	})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, readErr := io.ReadAll(res.Body)
	if !errors.Is(readErr, ErrDecompressTooLarge) {
		t.Fatalf("бомба = %v, want ErrDecompressTooLarge (T-051)", readErr)
	}
	if len(b) != limit {
		t.Errorf("прочитано %d байт, ожидался лимит %d", len(b), limit)
	}
}

// TestFetchDecompressCeilingFollowsLimiter: the decompression cap and the
// read limit are one number from one source: with the T-049 limits
// enabled it is Limits.MaxBodyBytes; Options.MaxBodyBytes does not affect
// decompression (two independent fields diverged in a manually assembled
// Options — decompression cut the body by its own number before the
// limiter).
func TestFetchDecompressCeilingFollowsLimiter(t *testing.T) {
	page := bytes.Repeat([]byte("q"), 1000) // below the limiter's limit
	srv, _ := encodingServer(t, gzipBytes(t, page), map[string]string{
		"Content-Encoding": "gzip",
	})
	f := NewFetcher(Options{
		MaxBodyBytes: 500, // a trap: not the source when Limits is set
		Limits:       &LimitOptions{MaxBodyBytes: 1500},
	})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	b, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		t.Fatalf("тело меньше лимита — чтение без ошибки: %v", readErr)
	}
	if len(b) != len(page) {
		t.Errorf("прочитано %d байт, ожидалось %d (потолок не из Limits.MaxBodyBytes)", len(b), len(page))
	}
	if res.BodyLimiter.Truncated() {
		t.Error("тело меньше лимита помечено обрезкой")
	}
}

// TestFetchGzipCharsetChain: compression + charset together: gzip over
// windows-1251, the header declares the charset — after Fetch and ToUTF8
// the consumer sees Russian UTF-8 (the dedup hash and the storage raw_len
// are computed over these bytes).
func TestFetchGzipCharsetChain(t *testing.T) {
	page := `<html><head><meta charset="windows-1251"></head><body>` + ruSample + `</body></html>`
	srv, _ := encodingServer(t, gzipBytes(t, encodeBytes(t, charmap.Windows1251, page)), map[string]string{
		"Content-Encoding": "gzip",
		"Content-Type":     "text/html",
	})
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("чтение: %v", err)
	}
	got := ToUTF8(res.Header, raw)
	if got.Charset != "windows-1251" || got.Source != CharsetMeta {
		t.Fatalf("кодировка: charset=%q source=%v", got.Charset, got.Source)
	}
	if !strings.Contains(string(got.UTF8), ruSample) {
		t.Errorf("русский текст не восстановлен: %q", got.UTF8)
	}
}

// TestRoundTripDecompresses: a direct RoundTrip (the
// http.Client/T-048 path) also sets Accept-Encoding and decompresses; the
// caller's own Accept-Encoding value is not overwritten.
func TestRoundTripDecompresses(t *testing.T) {
	ae := &syncValue{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ae.Set(r.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(gzipBytes(t, []byte("roundtrip")))
	}))
	defer srv.Close()

	f := NewFetcher(Options{})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("запрос: %v", err)
	}
	resp, err := f.RoundTrip(req)
	if err != nil {
		t.Fatalf("roundtrip: %v", err)
	}
	b, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(b) != "roundtrip" {
		t.Fatalf("тело: %q err=%v", b, err)
	}
	if ae.Get() != "gzip, br" {
		t.Errorf("Accept-Encoding: %q", ae.Get())
	}

	// The caller's own Accept-Encoding is left alone (the request is
	// cloned, not rewritten): identity stays identity.
	ae2 := &syncValue{}
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ae2.Set(r.Header.Get("Accept-Encoding"))
		_, _ = w.Write([]byte("как есть"))
	}))
	defer srv2.Close()
	req2, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv2.URL, nil)
	req2.Header.Set("Accept-Encoding", "identity")
	resp2, err := f.RoundTrip(req2)
	if err != nil {
		t.Fatalf("roundtrip 2: %v", err)
	}
	b2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	if ae2.Get() != "identity" {
		t.Errorf("чужой Accept-Encoding затёрт: %q", ae2.Get())
	}
	if string(b2) != "как есть" {
		t.Errorf("тело 2: %q", b2)
	}
}
