// Тесты кодировок и сжатия (T-051, T-100, ТЗ 5.5). Два блока:
//
//   - ToUTF8: приоритеты определения (BOM → заголовок → meta → детект →
//     умолчание), замена невалидных последовательностей, пустое тело;
//   - сжатие: decodeContentEncoding (gzip/br/deflate/identity, битые
//     значения, составные значения — цепочка слоёв RFC 9110 с потолком
//     на слой, потолок против zip-бомб, закрытие источника) и сквозной
//     путь Fetch/RoundTrip — Accept-Encoding доходит до сервера, тело
//     распаковывается, кодирующие заголовки снимаются, цепочка
//     «сжатие + кодировка» работает вместе.

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

// --- вспомогательные --------------------------------------------------------

// ruSample — естественный русский текст для частотного детекта: длины
// хватает на уверенный сигнал (>= cyrillicMinLetters), частые буквы
// присутствуют.
const ruSample = "Сервис массовой проверки доменов ищет сайты на фреймворке Laravel: транспорт скачивает главную страницу, определяет кодировку ответа и приводит содержимое к универсальному формату, анализатор применяет правила и сохраняет результат проверки в хранилище."

// encodeBytes кодирует UTF-8 текст в однобайтовую кодировку charmap.
func encodeBytes(t *testing.T, cm *charmap.Charmap, s string) []byte {
	t.Helper()
	b, err := cm.NewEncoder().Bytes([]byte(s))
	if err != nil {
		t.Fatalf("кодирование %v: %v", cm, err)
	}
	return b
}

// gzipBytes сжимает данные (обычный случай и zip-бомбы в тестах).
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

// brotliBytes сжимает данные brotli (минимальное качество — тесту
// важна корректность, не степень сжатия).
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

// utf16leBytes кодирует текст в UTF-16LE и prepends BOM (как это делают
// серверы, отдающие UTF-16).
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

// utf16beBytes кодирует текст в UTF-16BE с BOM — перестановкой байтов
// LE-представления (отдельного BE-энкодера тесту не нужно).
func utf16beBytes(t *testing.T, s string) []byte {
	t.Helper()
	le := utf16leBytes(t, s)[2:] // LE-байты без BOM
	be := make([]byte, 2+len(le))
	be[0], be[1] = 0xFE, 0xFF
	for i := 0; i+1 < len(le); i += 2 {
		be[2+i], be[2+i+1] = le[i+1], le[i]
	}
	return be
}

// headerOf — http.Header из одного Content-Type.
func headerOf(contentType string) http.Header {
	h := http.Header{}
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	return h
}

// --- ToUTF8: определение кодировки ------------------------------------------

// TestToUTF8EmptyBody: пустое тело — пустой результат без кодировки.
func TestToUTF8EmptyBody(t *testing.T) {
	got := ToUTF8(headerOf("text/html; charset=windows-1251"), nil)
	if len(got.UTF8) != 0 || got.Charset != "" || got.Source != CharsetNone {
		t.Fatalf("пустое тело: получили %+v, ожидались нули", got)
	}
}

// TestToUTF8HeaderCharset: Content-Type charset=windows-1251 — русский
// текст декодируется, источник header.
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

// TestToUTF8HeaderAliasAndCase: метки-алиасы и регистр (CP1251) дают
// каноническое имя.
func TestToUTF8HeaderAliasAndCase(t *testing.T) {
	got := ToUTF8(headerOf(`text/html; charset="CP1251"`),
		encodeBytes(t, charmap.Windows1251, ruSample))
	if got.Charset != "windows-1251" || got.Source != CharsetHeader || string(got.UTF8) != ruSample {
		t.Errorf("алиас CP1251: charset=%q source=%v совпадение=%v",
			got.Charset, got.Source, string(got.UTF8) == ruSample)
	}
}

// TestToUTF8BOMUTF8: BOM UTF-8 перекрывает заголовок и снимается.
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

// TestToUTF8BOMUTF16: UTF-16LE/BE с BOM декодируются (BOM потребляет
// декодер), заголовок противоречит — не важно, BOM сильнее.
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

// TestToUTF8MetaCharset: страница без charset в заголовке объявляет
// кодировку через <meta charset> (первые 1024 байта).
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
// content="...; charset=..."> — второй вид декларации.
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

// TestToUTF8HeaderBeatsMeta: заголовок приоритетнее meta (ТЗ 5.5).
func TestToUTF8HeaderBeatsMeta(t *testing.T) {
	body := encodeBytes(t, charmap.KOI8R,
		`<html><head><meta charset="windows-1251"></head><body>`+ruSample+`</body></html>`)
	got := ToUTF8(headerOf("text/html; charset=koi8-r"), body)
	if got.Charset != "koi8-r" || got.Source != CharsetHeader {
		t.Fatalf("приоритет: charset=%q source=%v, ожидались koi8-r/header", got.Charset, got.Source)
	}
}

// TestToUTF8UnknownHeaderLabel: неизвестная метка заголовка — «лучшая
// попытка»: пропускаем, декларация страницы решает.
func TestToUTF8UnknownHeaderLabel(t *testing.T) {
	body := encodeBytes(t, charmap.KOI8R,
		`<html><head><meta charset="koi8-r"></head><body>`+ruSample+`</body></html>`)
	got := ToUTF8(headerOf("text/html; charset=x-unknown-cp866"), body)
	if got.Charset != "koi8-r" || got.Source != CharsetMeta {
		t.Fatalf("неизвестная метка: charset=%q source=%v, ожидались koi8-r/meta", got.Charset, got.Source)
	}
}

// TestToUTF8DetectUTF8: без указаний валидный UTF-8 (и чистый ASCII)
// детектируется как utf-8, байты проходят без искажения.
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

// TestToUTF8DetectKOI8R: русский текст в koi8-r без каких-либо указаний
// — частотный детект (x/net контентной эвристики не имеет, умолчанием
// отдал бы windows-1252 и кракозябры).
func TestToUTF8DetectKOI8R(t *testing.T) {
	got := ToUTF8(headerOf(""), encodeBytes(t, charmap.KOI8R, ruSample))
	if got.Charset != "koi8-r" || got.Source != CharsetDetect {
		t.Fatalf("детект: charset=%q source=%v, ожидались koi8-r/detect", got.Charset, got.Source)
	}
	if string(got.UTF8) != ruSample {
		t.Errorf("декод: %q", got.UTF8)
	}
}

// TestToUTF8DetectWindows1251: аналогично для windows-1251 (частый
// легаси ру-сегмента) — детект различает koi8-r и cp1251.
func TestToUTF8DetectWindows1251(t *testing.T) {
	got := ToUTF8(headerOf(""), encodeBytes(t, charmap.Windows1251, ruSample))
	if got.Charset != "windows-1251" || got.Source != CharsetDetect {
		t.Fatalf("детект: charset=%q source=%v, ожидались windows-1251/detect", got.Charset, got.Source)
	}
	if string(got.UTF8) != ruSample {
		t.Errorf("декод: %q", got.UTF8)
	}
}

// TestToUTF8DetectRescuesMislabeledMeta: meta врёт (windows-1252), тело
// — windows-1251: детект включается для невалидного UTF-8 и спасает
// содержимое.
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

// TestToUTF8DefaultWindows1252: латиница с диакритикой без указаний —
// кириллического сигнала не хватает, умолчание WHATWG windows-1252
// (надмножество latin-1).
func TestToUTF8DefaultWindows1252(t *testing.T) {
	got := ToUTF8(headerOf(""), encodeBytes(t, charmap.Windows1252, "École naïve café"))
	if got.Charset != "windows-1252" || got.Source != CharsetDefault {
		t.Fatalf("умолчание: charset=%q source=%v, ожидались windows-1252/default", got.Charset, got.Source)
	}
	if string(got.UTF8) != "École naïve café" {
		t.Errorf("latin-1 декод: %q", got.UTF8)
	}
}

// TestToUTF8InvalidUTF8Replaced: заявлен utf-8, байты битые — замена
// U+FFFD вместо ошибки, валидная часть сохраняется.
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

// --- сжатие: decodeContentEncoding ------------------------------------------

// TestDecodeContentEncoding: identity/пусто — тело как есть; gzip и br
// распаковываются; потолок по умолчанию не режет обычные тела.
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

// TestDecodeContentEncodingUnsupported: экзотика и битые составные
// значения — ошибка (краулер рекламирует только gzip и br; одиночный
// deflate и корректные составные значения принимаются оборонительно —
// незапрошенно их присылает сломанный сервер, пустой или неизвестный
// элемент в составе разобрать нельзя).
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

// TestDecodeContentEncodingLayered: составные Content-Encoding (T-100,
// остаток H-10) — цепочка распаковки в порядке RFC 9110: кодировки
// применяются сервером в порядке перечисления, последняя в списке
// образует внешний слой («gzip, br» = сервер сжал gzip, затем поверх
// br — тело br(gzip(data))). identity в составе — пустая операция,
// слоя не строит; потолок на каждый слой обычные тела не режет.
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

// TestDecodeContentEncodingLayeredBomb: сжатое-в-сжатом сверх потолка —
// ErrDecompressTooLarge, байты до потолка доступны (контракт T-051
// сохраняется в цепочке: итоговый слой останавливается на
// limit + decompressTailMargin, промежуточные слои ограничены тем же
// потолком на слой — суммарная защита от бомб не слабеет).
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

// zlibBytes — zlib-обёртка (RFC 1950) поверх deflate.
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

// flateRawBytes — сырой deflate-поток (RFC 1951, без zlib-заголовка):
// такую форму «Content-Encoding: deflate» отдают серверы, не читавшие
// RFC 1950.
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

// TestDecodeContentEncodingBomb: распакованный поток сверх потолка —
// ErrDecompressTooLarge, байты до потолка доступны.
func TestDecodeContentEncodingBomb(t *testing.T) {
	bomb := bytes.Repeat([]byte("A"), 1<<20) // распакуется в 1 МБ
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

// TestDecodeContentEncodingExactLimit: тело ровно в потолок — не
// ошибка: пробный байт за пределом получает EOF.
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

// TestDecodeContentEncodingClosesSource: Close обёртки закрывает
// исходное тело ровно один раз (и на ошибке распаковки тоже).
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

	// Ошибка распаковки (битый поток) тоже обязана закрыть источник —
	// это делает вызывающий (decodeResponseBody), проверяем договор
	// обёртки: после чтения с ошибкой Close работает.
	bad := append(gzipBytes(t, []byte("x")), 0xFF, 0xFF, 0xFF)
	got2, err := decodeContentEncoding(newRC(bad), "gzip", 1<<20)
	if err != nil {
		t.Fatalf("обёртка битого: %v", err)
	}
	_, _ = io.ReadAll(got2) // ошибка потока — не важна для теста
	_ = got2.Close()

	mu.Lock()
	defer mu.Unlock()
	if closed != 2 {
		t.Errorf("закрытий исходного тела: %d, ожидалось 2", closed)
	}
}

// countingCloser — источник с подсчётом Close.
type countingCloser struct {
	io.Reader
	onClose func()
}

func (c *countingCloser) Close() error {
	c.onClose()
	return nil
}

// --- сжатие: сквозной путь Fetch --------------------------------------------

// encodingServer — httptest-сервер, отдающий тело с заданным
// Content-Encoding/Content-Type и запоминающий Accept-Encoding запроса.
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

// syncValue — строка, записываемая из горутины сервера.
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

// fetchFrom — Fetch к тестовому серверу по адресу петли.
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

// TestFetchAcceptEncoding: транспорт отправляет Accept-Encoding: gzip, br.
func TestFetchAcceptEncoding(t *testing.T) {
	srv, ae := encodingServer(t, []byte("ok"), nil)
	f := NewFetcher(Options{})
	res := fetchFrom(t, f, srv.URL)
	defer func() { _ = res.Body.Close() }()
	if got := ae.Get(); got != "gzip, br" {
		t.Errorf("Accept-Encoding сервера: %q, ожидался %q", got, "gzip, br")
	}
}

// TestFetchGzipRoundtrip: сервер отдаёт gzip — потребитель читает
// распакованное тело, кодирующие заголовки сняты (правдивый gzip-length
// Content-Length сервера исчезает вместе с Content-Encoding).
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

// TestFetchBrotliRoundtrip: br распаковывается сквозным путём
// (зависимость andybalholm/brotli: в stdlib brotli нет, а ТЗ 5.5
// требует «Accept-Encoding: gzip, br»).
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

// TestFetchIdentityKeepsHeaders: без сжатия заголовки не трогаются.
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

// TestFetchMultiLineContentEncoding: списочный Content-Encoding,
// разнесённый на несколько строк (RFC 9110 §5.3 — эквивалент списка
// через запятую), собирается в одно значение: «gzip» + «br» двумя
// строками читается как составная упаковка (Get взял бы только первую
// строку и споткнулся о заголовок второго слоя; P2 ревью T-100).
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

// TestFetchIdentityCompositeKeepsHeaders: составное значение из одних
// identity не преобразует тело — кодирующие заголовки остаются правдой
// и не снимаются (как у одиночного identity; P3 ревью T-100).
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

// TestFetchGzipEmptyBody: Content-Encoding: gzip с пустым телом — не
// ошибка (gzip.NewReader вернёт EOF), читается пустота.
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

// TestFetchUnsupportedEncoding: сервер прислал незапрашиваемое
// Content-Encoding — честная ошибка, не сжатые байты телом.
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

// TestFetchDeflateRoundtrip: незапрошенный deflate (zlib-обёртка и
// сырой поток — историческая двусмысленность серверов) распаковывается:
// страница не проваливается ошибкой запроса, кодирующие заголовки
// снимаются; Accept-Encoding по-прежнему рекламирует только gzip и br.
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

// TestFetchGzipBomb: распакованный поток превышает MaxBodyBytes —
// чтение тела возвращает ErrDecompressTooLarge (потолок распаковки
// стоит до лимита читаемого тела T-049, но проверяется тем же числом).
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

// runBodyLimitCase — общий прогон кейса «тело vs лимит» (encodingServer
// + Fetch + проверки обрезки): используется однослойным gzip и составной
// упаковкой (T-100) — семантика обрезки едина, классификация хвоста идёт
// по итоговому декодированному потоку.
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

// TestFetchGzipJustOverLimit: сжатое тело чуть больше лимита ведёт себя
// как несжатое сверх лимита (TestFetchBodyLimitExactAndTruncated в
// limits_test.go): обрезка по лимиту с Truncated и чтение без ошибки
// (ТЗ 8 «данные частичны, статус получен»), а не ErrDecompressTooLarge,
// который воркер прочёл бы как read-ошибку и погасил бы Available
// живого сайта. Потолки распаковки и чтения равны (боевая сборка
// NewOptions ставит оба из http.max_body_bytes).
func TestFetchGzipJustOverLimit(t *testing.T) {
	run := func(t *testing.T, size, limit int, wantTrunc bool) {
		t.Helper()
		runBodyLimitCase(t, "gzip", func(b []byte) []byte { return gzipBytes(t, b) }, size, limit, wantTrunc)
	}
	t.Run("ровно в лимит", func(t *testing.T) { run(t, 1000, 1000, false) })
	t.Run("на байт больше", func(t *testing.T) { run(t, 1001, 1000, true) })
	t.Run("заметно больше", func(t *testing.T) { run(t, 64<<10, 1000, true) })
}

// TestFetchLayeredJustOverLimit: составная упаковка «чуть больше
// лимита» — как однослойная (TestFetchGzipJustOverLimit): обрезка с
// Truncated и чтение без ошибки; классификация хвоста проходит по
// итоговому (последнему декодированному) потоку — семантика H-2 не
// меняется от числа слоёв.
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

// TestFetchGzipBombUnderLimiter: бомба при включённых лимитах T-049 —
// ErrDecompressTooLarge (контракт T-051: страница-бомба — ошибка тела,
// воркер не пишет записи и гасит Available); классификация хвоста не
// расходует бомбу дальше лимита+decompressTailMargin, тело до лимита
// доступно. Обрезка без ошибки — только хвост в пределах маржи
// (TestFetchGzipJustOverLimit), не бомба.
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

// TestFetchLayeredRoundtrip: составное Content-Encoding сквозным путём —
// тело распаковывается цепочкой слоёв (RFC 9110: последняя кодировка в
// списке — внешний слой), кодирующие заголовки сняты; Accept-Encoding
// по-прежнему рекламирует только gzip и br — составные значения не
// запрашиваем, их присылает сломанный сервер.
func TestFetchLayeredRoundtrip(t *testing.T) {
	plain := "<html><body>" + ruSample + "</body></html>"
	body := brotliBytes(t, gzipBytes(t, []byte(plain))) // «gzip, br»
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

// TestFetchLayeredBombUnderLimiter: бомба в составной упаковке при
// включённых лимитах T-049 — ErrDecompressTooLarge, тело до лимита
// доступно (контракт T-051 не зависит от числа слоёв: потолок на
// каждый слой один и тот же bodyCeiling).
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

// TestFetchDecompressCeilingFollowsLimiter: потолок распаковки и лимит
// чтения — одно число из одного источника: при включённых лимитах
// T-049 это Limits.MaxBodyBytes, Options.MaxBodyBytes на распаковку не
// влияет (два независимых поля в ручной сборке Options расходились —
// распаковка резала тело по своему числу до лимитёра).
func TestFetchDecompressCeilingFollowsLimiter(t *testing.T) {
	page := bytes.Repeat([]byte("q"), 1000) // меньше лимита лимитёра
	srv, _ := encodingServer(t, gzipBytes(t, page), map[string]string{
		"Content-Encoding": "gzip",
	})
	f := NewFetcher(Options{
		MaxBodyBytes: 500, // ловушка: не источник при включённых Limits
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

// TestFetchGzipCharsetChain: сжатие + кодировка вместе: gzip поверх
// windows-1251, заголовок объявляет charset — после Fetch и ToUTF8
// потребитель видит русский UTF-8 (хэш дедупа и raw_len хранилища
// считаются по этим байтам).
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

// TestRoundTripDecompresses: прямой RoundTrip (путь http.Client/T-048)
// тоже ставит Accept-Encoding и распаковывает; собственное значение
// Accept-Encoding запроса не затирается.
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

	// Свой Accept-Encoding вызывающего не трогаем (клонируем запрос,
	// не переписываем): identity остаётся identity.
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
