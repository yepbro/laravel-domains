// Кодировки и сжатие ответов (ТЗ 5.5):
//
//   - «Поддерживается Accept-Encoding: gzip, br; распаковка — с
//     ограничением размера (защита от zip-бомб)»: транспорт отправляет
//     gzip, br сам (явный заголовок отключает прозрачную распаковку
//     net/http, у которой нет потолка) и оборачивает тело читателем с
//     потолком распакованного размера;
//   - «Кодировка ответа определяется (заголовок, meta charset, детект) и
//     приводится к UTF-8»: ToUTF8 — BOM → Content-Type charset → meta
//     charset (первые 1024 байта) → детект (валидный UTF-8, частотная
//     оценка кириллицы) → умолчание windows-1252 (WHATWG).
//
// Цепочка чтения тела: сеть → распаковка Content-Encoding (одна
// кодировка или составная «gzip, br» — цепочка слоёв RFC 9110,
// потолок MaxBodyBytes на каждый слой, T-100) → [лимит читаемого тела
// T-049] → потребитель; приведение к UTF-8 — поверх прочитанных байтов
// через ToUTF8 (потребитель всё равно буферизует тело для хэша дедупа
// и хранения, потоковая замена ридера выигрыша не даёт, а двойного
// чтения — добавляет). Всё ниже по конвейеру (лимит T-049, хэш,
// raw_len хранилища T-042) видит уже несжатые байты — распакованный
// поток и есть тело ответа.
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
	// acceptEncoding — значение заголовка Accept-Encoding запросов
	// (ТЗ 5.5: «Поддерживается Accept-Encoding: gzip, br»). Ставит
	// транспорт (Fetch и RoundTrip), а не net/http: ручной заголовок
	// отключает прозрачную распаковку транспорта Go — сжатые байты
	// приходят как есть и распаковываются здесь, с потолком.
	acceptEncoding = "gzip, br"

	// defaultDecompressLimit — потолок распакованного тела, когда
	// Options.MaxBodyBytes не задан: 2 МБ из таблицы лимитов ТЗ 5.5
	// («макс. размер читаемого тела»). Потолок распаковки равен лимиту
	// тела, а не отдельному числу: распакованный поток и есть тело, и
	// «2 МБ читаемого» обязано означать и «2 МБ распакованного» — без
	// этого чтение до конца (хэш дедупа) потребляло бы гигабайты
	// zip-бомбы. Отдельное имя, а не DefaultMaxBodyBytes — не спорим с
	// T-049 за константу дефолта лимита тела.
	defaultDecompressLimit = 2 << 20

	// charsetPreview — окно определения кодировки по содержимому: BOM
	// всегда в начале, meta charset — по первым 1024 байтам (окно WHATWG,
	// то же число использует x/net/html/charset).
	charsetPreview = 1024

	// cyrillicMinLetters — минимальное число кириллических букв для
	// доверия частотному детекту: меньше — сигнала не хватает, остаётся
	// умолчание windows-1252.
	cyrillicMinLetters = 32

	// cyrillicMinAvgFreq — порог средней (геометрической) частоты букв
	// decoded-текста: русский текст даёт 0.04–0.07 (частые о/е/а),
	// случайное прочтение — 0.01 и ниже. Отсекает латиницу с
	// диакритикой, неверно прочитанную как кириллицу.
	cyrillicMinAvgFreq = 0.02
)

// ErrDecompressTooLarge — распакованное тело превысило потолок
// (защита от zip-бомб, ТЗ 5.5). Тело ответа при этом остаётся
// читаемым до потолка; ошибка возвращается чтением за пределом.
var ErrDecompressTooLarge = errors.New("httpfetch: распакованное тело превышает потолок распаковки")

// --- сжатие: Accept-Encoding и распаковка с потолком ------------------------

// parseContentCodings разбирает значение Content-Encoding в список
// кодировок: RFC 9110 перечисляет их через запятую в порядке применения
// сервером (первая — внутренний слой, последняя — внешний), регистр и
// пробелы вокруг имён не значимы. Пустое значение — пустой список (нет
// кодировки). Пустой элемент («gzip,,br», «gzip,») или кодировка вне
// {gzip, x-gzip, br, deflate, identity} — ошибка: сломанному серверу
// честный отказ, а не молчаливая выдача сжатых байтов телом.
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

// identityOnlyContentEncoding — сводится ли значение Content-Encoding
// к identity/пусто (одиночное «identity» или составное «identity,
// identity»): тело не преобразовано, кодирующие заголовки остаются
// правдивыми — снимать их (decodeResponseBody) не нужно.
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

// decodeContentEncoding оборачивает сжатое тело по значению
// Content-Encoding (gzip и алиас x-gzip, br, deflate, identity/пусто)
// читателем с потолком распакованного размера limit (<= 0 —
// defaultDecompressLimit). deflate краулер не рекламирует
// (Accept-Encoding: gzip, br), но часть серверов отдаёт его
// незапрошенным — поток принимается оборонительно: zlib-обёртка
// (RFC 1950) или сырой deflate (RFC 1951), форма различается по
// заголовку. Составные значения («gzip, br», T-100) распаковываются
// цепочкой слоёв по RFC 9110: кодировки применяются сервером в порядке
// перечисления, поэтому последняя в списке образует внешний слой, и
// цепочка строится от неё к первой. Потолок limit — на каждый слой, а
// не произведение: суммарная работа распаковки одного ответа ограничена
// числом слоёв × (limit + decompressTailMargin) — каждый слой ограничен
// как единственный, защита от бомб не слабеет. Классификация хвоста
// (обрезка против ErrDecompressTooLarge) выполняется итоговым слоем по
// последнему декодированному потоку — семантика однослойных кодировок
// не меняется. Accept-Encoding составные значения по-прежнему не
// рекламирует — значение приходит только от сломанного сервера и
// обрабатывается оборонительно, как одиночный deflate. Возвращает
// читатель, закрывающий исходное тело (цепочка Close каскадом до него).
func decodeContentEncoding(rc io.ReadCloser, contentEncoding string, limit int64) (io.ReadCloser, error) {
	if limit <= 0 {
		limit = defaultDecompressLimit
	}
	codings, err := parseContentCodings(contentEncoding)
	if err != nil {
		return nil, err
	}
	// Строим от внешнего слоя (последняя кодировка) к внутреннему
	// (первая): каждый читатель снимает один слой, decompressCap поверх
	// ставит потолок limit. identity слоя не строит (пустая операция);
	// цепочка из одних identity или пустой список возвращают тело как
	// есть.
	var cur = rc // io.ReadCloser — статический тип из параметра
	for i := len(codings) - 1; i >= 0; i-- {
		switch codings[i] {
		case "identity":
			continue
		case "gzip", "x-gzip":
			// gzip.NewReader читает заголовок немедленно: пустое сжатое
			// тело возвращает io.EOF — это не ошибка, тело просто пустое
			// (слой ниже уже отдал всё — возвращаем его как есть).
			gz, err := gzip.NewReader(cur)
			if err == io.EOF {
				return cur, nil
			}
			if err != nil {
				return nil, fmt.Errorf("httpfetch: битый gzip-заголовок тела: %w", err)
			}
			cur = &decompressCap{dec: gz, src: cur, left: limit}
		case "br":
			// brotli.NewReader заголовок валидирует лениво, при чтении.
			cur = &decompressCap{dec: brotli.NewReader(cur), src: cur, left: limit}
		case "deflate":
			cur = &decompressCap{dec: newDeflateReader(cur), src: cur, left: limit}
		}
	}
	return cur, nil
}

// newDeflateReader — читатель deflate-потока: zlib-обёртка (RFC 1950)
// или сырой поток (RFC 1951); в реальном «Content-Encoding: deflate»
// встречаются обе формы (историческая двусмысленность HTTP-серверов,
// ту же развилку проходят браузеры и curl). Заголовок zlib различается
// по двум байтам: метод deflate (CM=8), окно ≤ 32 КБ (CINFO ≤ 7), без
// предустановленного словаря (FDICT=0) и контрольная сумма заголовка
// кратна 31. Чтение лениво: данные источника не теряются (reader
// держит peek-буфер).
func newDeflateReader(rc io.ReadCloser) io.Reader {
	br := bufio.NewReader(rc)
	if hdr, err := br.Peek(2); err == nil && looksLikeZlib(hdr) {
		if zr, err := zlib.NewReader(br); err == nil {
			return zr
		}
	}
	return flate.NewReader(br)
}

// looksLikeZlib — правдоподобный zlib-заголовок (см. newDeflateReader).
func looksLikeZlib(hdr []byte) bool {
	if len(hdr) < 2 {
		return false
	}
	cmf, flg := hdr[0], hdr[1]
	return cmf&0x0f == 8 && cmf>>4 <= 7 && flg&0x20 == 0 &&
		(int(cmf)*256+int(flg))%31 == 0
}

// decompressTailMargin — сколько распакованных байтов за потолком
// читается для классификации хвоста: конец потока в пределах маржи —
// честная обрезка (живой сайт чуть больше лимита), продолжение за
// маржой — zip-бомба (ErrDecompressTooLarge, ТЗ 5.5/Т-051).
const decompressTailMargin = 64 << 10

// decompressCap — распакованный поток с потолком (защита от zip-бомб).
// В составной упаковке (T-100) экземпляры стоят на каждый слой: src
// тогда — decompressCap слоя ниже, Close каскадом доходит до тела
// ответа, закрывая каждый слой ровно один раз. Отдельного Close у
// brotli-ридера нет, а у gzip он не трогает источник — достаточно
// одного src.Close.
type decompressCap struct {
	dec      io.Reader     // распакованный поток (gzip.Reader / brotli.Reader)
	src      io.ReadCloser // сжатое тело ответа
	left     int64         // остаток потолка в байтах
	tailDone bool          // хвост за потолком уже классифицирован
	tailErr  error         // итог классификации хвоста (повторные чтения его повторяют)
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
		return 0, nil // контракт io.Reader; классификация — при следующем чтении
	}
	if !c.tailDone {
		c.tailDone = true
		n, err := c.classifyTail(p)
		if n > 0 {
			c.tailErr = io.EOF // первый байт хвоста отдан — дальше конец потока
		} else {
			c.tailErr = err
		}
		return n, c.tailErr
	}
	return 0, c.tailErr
}

// classifyTail — классификация хвоста за потолком (выполняется один раз):
// EOF сразу — тело ровно в потолке (не ошибка); данные, конец которых
// укладывается в decompressTailMargin, — обрезка: первый байт хвоста
// копируется в p как признак «тело длиннее потолка» (BodyLimiter по нему
// ставит Truncated, H-2), остальной хвост дочитывается и отбрасывается;
// поток, продолжающийся за маржу, — zip-бомба: ErrDecompressTooLarge,
// чтение останавливается, не расходуя бомбу дальше (Т-051).
func (c *decompressCap) classifyTail(p []byte) (int, error) {
	var buf [4096]byte
	read := int64(0)
	for read < decompressTailMargin {
		n, err := c.dec.Read(buf[:])
		read += int64(n)
		if n > 0 {
			p[0] = buf[0] // признак хвоста потребителю
			for read < decompressTailMargin {
				n2, err2 := c.dec.Read(buf[:])
				read += int64(n2)
				if err2 == io.EOF {
					return 1, nil // конец в пределах маржи — обрезка
				}
				if err2 != nil {
					return 1, err2 // хвост начался, но оборвался ошибкой сети
				}
			}
			return 0, ErrDecompressTooLarge // поток продолжается — бомба
		}
		if err == io.EOF {
			return 0, io.EOF // тело ровно в потолке
		}
		if err != nil {
			return 0, err
		}
		// (0, nil) — допустимый редкий случай; продолжаем до маржи
	}
	return 0, ErrDecompressTooLarge
}

func (c *decompressCap) Close() error { return c.src.Close() }

// --- кодировки: определение и приведение к UTF-8 -----------------------------

// CharsetSource — на каком шаге решилась кодировка тела. Наблюдаемость
// для воркера (лог/метрики «откуда узнали»), на декод не влияет.
type CharsetSource int

const (
	// CharsetNone — тело пустое, кодировки нет.
	CharsetNone CharsetSource = iota
	// CharsetBOM — метка порядка байтов (сильнейший сигнал, WHATWG).
	CharsetBOM
	// CharsetHeader — параметр charset заголовка Content-Type.
	CharsetHeader
	// CharsetMeta — meta charset в первых 1024 байтах (оба вида:
	// <meta charset> и <meta http-equiv="Content-Type">).
	CharsetMeta
	// CharsetDetect — детект: валидный UTF-8 или частотная оценка
	// кириллицы (koi8-r / windows-1251 / iso-8859-5).
	CharsetDetect
	// CharsetDefault — умолчание WHATWG windows-1252 (надмножество
	// latin-1): не определили никак.
	CharsetDefault
)

// String — короткое имя источника (bom/header/meta/detect/default/none).
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

// CharsetResult — итог приведения тела к UTF-8.
type CharsetResult struct {
	// UTF8 — тело в UTF-8. Некорректные последовательности заменяются
	// U+FFFD (RuneError), а не ошибкой. Для уже-UTF-8 тела байты могут
	// разделять память с входом ToUTF8 (быстрый путь без копии).
	UTF8 []byte
	// Charset — каноническое имя исходной кодировки (WHATWG: "utf-8",
	// "windows-1251", "koi8-r", …); "" только для пустого тела.
	Charset string
	// Source — шаг, на котором кодировка определена.
	Source CharsetSource
}

// ToUTF8 приводит тело ответа к UTF-8 по приоритету ТЗ 5.5
// («заголовок, meta charset, детект»), с BOM впереди:
//
//	BOM → Content-Type charset → meta charset (первые 1024 байта) →
//	детект (валидный UTF-8; кириллическая частотная оценка) →
//	умолчание windows-1252 (WHATWG, надмножество latin-1).
//
// Неизвестная метка charset в заголовке пропускается («лучшая попытка»:
// страница о своей кодировке знает больше сломанного заголовка), а
// meta-декларация windows-1252/latin-1 неотличима от умолчания — такие
// тела всё равно проходят детект. Функция не возвращает ошибку:
// нераспознанное — windows-1252, невалидные последовательности — U+FFFD.
func ToUTF8(h http.Header, body []byte) CharsetResult {
	if len(body) == 0 {
		return CharsetResult{Source: CharsetNone}
	}

	// BOM перекрывает всё (WHATWG): у сервера была последняя попытка
	// в charset заголовка, фактические байты — финальная инстанция.
	// UTF-32 BOM не детектируем (как WHATWG/браузеры): UTF-32LE-префикс
	// FF FE 00 00 примет за UTF-16LE. Декодер unicode.UTF8 BOM не
	// убирает — снимаем сами; UTF-16 декодер с UseBOM потребляет.
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
		// Неизвестная метка: пропускаем к meta/детекту.
	}

	// meta charset. Prescan x/net/html/charset (html.Tokenizer, оба
	// вида meta, нормализация utf-16→utf-8) — battle-tested разбор;
	// свой писали бы хуже. Заголовок пуст (уже прошли), BOM нет —
	// ветки BOM/заголовка внутри не сработают.
	enc, name, _ := charset.DetermineEncoding(previewBytes(body), "")
	metaMark := metaCharsetRe.Match(previewBytes(body))
	if enc == encoding.Nop {
		// utf-8: meta так объявила (включая нормализацию utf-16) или
		// превью — валидный UTF-8; источник различает только пометка.
		src := CharsetDetect
		if metaMark {
			src = CharsetMeta
		}
		return CharsetResult{UTF8: sanitizeUTF8(body), Charset: "utf-8", Source: src}
	}
	if name != "windows-1252" {
		// Конкретная не-умолчальная кодировка — это prescan (meta).
		return decodeAs(enc, name, CharsetMeta, body)
	}

	// x/net вернулся с windows-1252 — то ли meta его объявила, то ли
	// умолчание. Детект по всему телу: валидный UTF-8 (x/net смотрел
	// только превью и ASCII-тело не проверял на UTF-8 вовсе)…
	if utf8.Valid(body) {
		return CharsetResult{UTF8: sanitizeUTF8(body), Charset: "utf-8", Source: CharsetDetect}
	}
	// …частотная оценка кириллицы: спасает и немаркированный легаси
	// (koi8-r без указаний), и перепутанные meta charset="windows-1252".
	if enc, name := detectCyrillic(body); enc != nil {
		return decodeAs(enc, name, CharsetDetect, body)
	}
	src := CharsetDefault
	if metaMark {
		src = CharsetMeta
	}
	return decodeAs(charmap.Windows1252, "windows-1252", src, body)
}

// decodeAs декодирует тело известной кодировкой: результат UTF-8,
// некорректные последовательности — U+FFFD, не ошибка (ТЗ 5.5 не
// квалифицирует битые байты как отказ загрузки).
func decodeAs(enc encoding.Encoding, name string, src CharsetSource, body []byte) CharsetResult {
	out, err := enc.NewDecoder().Bytes(body)
	if err != nil {
		// Декодеры x/text заменяют невалидные байты на U+FFFD без
		// ошибки; сюда ведут только обрывы последовательностей
		// (нечётная длина UTF-16 и т.п.) — сохраняем готовый префикс
		// и ставим U+FFFD на месте обрыва.
		return CharsetResult{UTF8: append(out, "\uFFFD"...), Charset: name, Source: src}
	}
	return CharsetResult{UTF8: out, Charset: name, Source: src}
}

// sanitizeUTF8 — замена некорректных последовательностей на U+FFFD
// (максимальные подпоследовательности, семантика WHATWG/Go): валидное
// тело возвращается как есть, без копии.
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

// headerCharset извлекает параметр charset из Content-Type; пусто при
// отсутствии или неразборном заголовке (битый заголовок — нет
// информации, не повод отказываться от тела).
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

// previewBytes — первые charsetPreview байт тела (окно meta/BOM).
func previewBytes(b []byte) []byte {
	if len(b) > charsetPreview {
		return b[:charsetPreview]
	}
	return b
}

// metaCharsetRe — грубая пометка «в окне есть meta charset». Нужна
// только для поля Source (мета объявила utf-8/1252 или детект без
// указаний): на выбор кодировки не влияет — prescan уже вернул бы
// конкретную кодировку, если бы meta её корректно объявила.
var metaCharsetRe = regexp.MustCompile(`(?i)<meta[^>]*\bcharset`)

// --- детект кириллицы --------------------------------------------------------

// cyrCharsets — кандидаты частотного детекта. x/net/html/charset
// контентной эвристики не имеет (meta-prescan, проверка UTF-8 и
// умолчание windows-1252), а ТЗ 5.5 требует «детект»: типичный
// немаркированный не-UTF-8 контент — кириллический легаси
// (koi8-r, windows-1251, iso-8859-5), прочие письменности оставляем
// умолчанию WHATWG.
var cyrCharsets = []struct {
	enc  encoding.Encoding
	name string
	cm   *charmap.Charmap
}{
	{charmap.KOI8R, "koi8-r", charmap.KOI8R},
	{charmap.Windows1251, "windows-1251", charmap.Windows1251},
	{charmap.ISO8859_5, "iso-8859-5", charmap.ISO8859_5},
}

// ruLetterFreq — частоты строчных букв русского языка (доли). Правильная
// кодировка набирает частые о/е/а/и — сумма логарифмов частот различает
// кандидатов: неверное прочтение даёт редкие буквы.
var ruLetterFreq = map[rune]float64{
	'о': 0.110, 'е': 0.085, 'а': 0.080, 'и': 0.074, 'н': 0.066,
	'т': 0.064, 'с': 0.051, 'р': 0.047, 'в': 0.045, 'л': 0.044,
	'к': 0.035, 'м': 0.032, 'д': 0.030, 'п': 0.028, 'у': 0.023,
	'я': 0.020, 'ы': 0.019, 'з': 0.018, 'б': 0.016, 'г': 0.015,
	'ч': 0.015, 'й': 0.012, 'х': 0.010, 'ж': 0.009, 'ю': 0.006,
	'ш': 0.005, 'ц': 0.004, 'щ': 0.004, 'э': 0.003, 'ф': 0.002,
	'ъ': 0.0002, 'ё': 0.0002, 'ь': 0.014,
}

// detectCyrillic ищет однобайтовую кириллическую кодировку по частотам
// букв: декодирует байты >= 0x80 каждым кандидатом и сравнивает сумму
// логарифмов частот букв. Возвращает nil, если кириллического сигнала
// не хватает (мало букв или текст скорее латинский с диакритикой) —
// остаётся умолчание windows-1252.
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
	// Порог средней (геометрической) частоты буквы: русский текст даёт
	// ~0.04–0.07 (частые гласные), латиница с диакритикой, прочитанная
	// как кириллица, — набор редких букв и < 0.02.
	if avg := math.Exp(bestScore / float64(letters)); avg < cyrillicMinAvgFreq {
		return nil, ""
	}
	return cyrCharsets[best].enc, cyrCharsets[best].name
}

// letterFreq — частота буквы русского языка (неизвестные буквы — редкий
// штраф, чтобы странные прочтения не набирали очки).
func letterFreq(r rune) float64 {
	if f, ok := ruLetterFreq[r]; ok {
		return f
	}
	return 0.0005
}

// isCyrillic — основная кириллица без расширений (А-я, Ё, ё).
func isCyrillic(r rune) bool {
	return (r >= 0x0410 && r <= 0x044F) || r == 0x0401 || r == 0x0451
}
