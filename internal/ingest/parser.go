// Package ingest — обработка входных файлов доменов (ТЗ 5.1).
//
// Потоковый построчный парсер — этап между наблюдением каталога
// (internal/watch, T-013) и валидацией имён (T-016): читает io.Reader и
// отдаёт значимые строки с номерами; по номерам rejected-лог (T-016) и
// отчёт файла (T-021) указывают на реальную строку. Память не зависит от
// размера файла (ТЗ 3.5): фиксированное окно чтения и накопитель одной
// строки, файл целиком в память не собирается.
//
// Формат файла (ТЗ 5.1): UTF-8, одна запись в строке. Парсер отвечает
// только за формат и ничего не знает про домены: значимая строка
// отдаётся как есть, соответствие полному имени домена проверяет
// потребитель (T-016, internal/idn). Правила разбора:
//
//   - конец строки — \n, \r или пара \r\n (CRLF не делится на два конца);
//     нормализация — снятие конца строки;
//   - BOM (U+FEFF) снимается только в начале файла; в середине строки он
//     остаётся в данных и убирается дальше по конвейеру маппингом UTS-46
//     (idn.Normalize, ТЗ 5.2);
//   - пустые строки и строки-комментарии (первый значимый символ «#»)
//     пропускаются (ТЗ 5.1);
//   - крайние пробелы снимаются (unicode.IsSpace — тот же предикат, что
//     в idn.cleanup); внутренние пробелы остаются — их убирает
//     нормализация имён (ТЗ 5.2 п. 1);
//   - строка длиннее MaxLineLen байт или с невалидным UTF-8 отклоняется
//     с номером и причиной, разбор продолжается — файл не считается
//     ошибочным из-за единичных плохих строк (ТЗ 5.1); отклонения
//     отдаются потребителю для rejected-лога (T-016).
package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

// MaxLineLen — лимит сырой строки файла (без конца строки), байты.
// Валидная строка («полное имя домена», ТЗ 5.1) после нормализации не
// длиннее 253 октет в ASCII-форме (ТЗ 5.2 п. 4), а её исходная форма —
// не длиннее с запасом: каждый октет punycode-формы соответствует не
// более чем одной кодовой точке входа (≤4 байта UTF-8), то есть ≤1012
// байт плюс крайние пробелы и завершающая точка. Строки длиннее — мусор:
// они отклоняются построчно, не буферизуясь без лимита (ТЗ 3.5), и не
// мешают разбору остальных строк файла.
const MaxLineLen = 1024

// maxRejectText — сколько байт сырой строки переносить в Reject.Text:
// вход недоверенный, rejected-логу (T-016) длинный мусор не нужен.
const maxRejectText = 255

// Reason — устойчивая причина отклонения строки в пакете ingest: у парсера
// (невалидный UTF-8, длинная строка) и у валидатора (классы idn.ErrorKind,
// wrong_zone — см. validate.go). Попадает в rejected-лог и отчёт файла.
type Reason string

const (
	// ReasonUTF8 — строка не является валидным UTF-8: формат файла —
	// UTF-8 (ТЗ 5.1).
	ReasonUTF8 Reason = "invalid_utf8"
	// ReasonLong — сырая строка длиннее MaxLineLen байт: мусор, а не
	// имя (обоснование лимита — MaxLineLen).
	ReasonLong Reason = "line_len"
)

// Line — значимая строка файла (не пустая и не комментарий).
type Line struct {
	// Num — номер физической строки файла, 1-based. Пустые, комментарии
	// и отклонённые строки тоже нумеруются: rejected-лог (T-016) и отчёт
	// (T-021) ссылаются на реальную строку файла.
	Num int
	// Text — содержимое строки: без конца строки, без BOM первой строки
	// и без крайних пробелов. Внутренние пробелы остаются — их снимает
	// нормализация имён (ТЗ 5.2 п. 1).
	Text string
}

// Reject — строка, отклонённая парсером (ReasonUTF8, ReasonLong).
// Валидация доменного имени — не здесь: парсер отвечает за формат,
// семантику строки проверяет потребитель (T-016).
type Reject struct {
	// Num — номер физической строки файла, 1-based.
	Num int
	// Reason — причина отклонения.
	Reason Reason
	// Text — начало сырой строки без конца строки (до maxRejectText
	// байт). При ReasonUTF8 может содержать невалидные байты как есть —
	// это исходные данные строки.
	Text string
	// Bytes — полный размер сырой строки в байтах (без конца строки).
	Bytes int
}

// Stats — счётчики строк обработанного файла; основа отчёта ТЗ 5.1
// (кол-во строк, … отклонено) — задача T-021. Инвариант:
// Total = Skipped + Rejected + Data.
type Stats struct {
	Total    int // всего физических строк файла
	Skipped  int // пропущено: пустые строки и комментарии «#»
	Rejected int // отклонено парсером: не UTF-8, длиннее MaxLineLen
	Data     int // отдано значимых строк (OnLine)
}

// Options — параметры разбора. Оба колбэка необязательны: nil — строки
// соответствующего рода только считаются.
type Options struct {
	// OnLine вызывается для каждой значимой строки. Возвращённая ошибка
	// останавливает разбор и возвращается из Parse обёрнутой с номером
	// строки — способ останова по решению потребителя (например, ошибка
	// записи в БД, отмена).
	OnLine func(Line) error
	// OnReject вызывается для каждой отклонённой строки — сырьё
	// rejected-лога T-016. Ошибка останавливает разбор как у OnLine.
	OnReject func(Reject) error
}

// Parse читает r построчно: значимые строки — в opts.OnLine, отклонённые —
// в opts.OnReject, пустые и комментарии — молча пропускаются. Возвращает
// счётчики. Ошибка — либо сбой чтения r, либо ошибка колбэка; строки,
// отданные до ошибки, остаются у потребителя (разбор не «откатывается»),
// счётчики соответствуют отданному.
func Parse(r io.Reader, opts Options) (Stats, error) {
	var st Stats
	sc := newScanner(r)
	for {
		raw, size, ok, err := sc.readLine()
		if err != nil {
			return st, fmt.Errorf("ingest: чтение входа: %w", err)
		}
		if !ok {
			return st, nil
		}
		st.Total++
		switch {
		case size > MaxLineLen:
			st.Rejected++
			if err := opts.emitReject(Reject{
				Num:    st.Total,
				Reason: ReasonLong,
				Text:   truncateText(raw),
				Bytes:  size,
			}); err != nil {
				return st, err
			}
		case !utf8.Valid(raw):
			st.Rejected++
			if err := opts.emitReject(Reject{
				Num:    st.Total,
				Reason: ReasonUTF8,
				Text:   truncateText(raw),
				Bytes:  size,
			}); err != nil {
				return st, err
			}
		default:
			text := bytes.TrimFunc(raw, unicode.IsSpace)
			if len(text) == 0 || text[0] == '#' {
				st.Skipped++
				continue
			}
			st.Data++
			if opts.OnLine != nil {
				if err := opts.OnLine(Line{Num: st.Total, Text: string(text)}); err != nil {
					return st, fmt.Errorf("ingest: строка %d: %w", st.Total, err)
				}
			}
		}
	}
}

// emitReject передаёт отклонение потребителю с общей обработкой ошибок.
func (o Options) emitReject(rj Reject) error {
	if o.OnReject == nil {
		return nil
	}
	if err := o.OnReject(rj); err != nil {
		return fmt.Errorf("ingest: строка %d (отклонение): %w", rj.Num, err)
	}
	return nil
}

// truncateText обрезает сырую строку до maxRejectText байт для Reject.Text.
func truncateText(raw []byte) string {
	if len(raw) > maxRejectText {
		raw = raw[:maxRejectText]
	}
	return string(raw)
}

// scanWindow — окно чтения по умолчанию, 64 КиБ: строки в нём почти
// всегда помещаются целиком (лимит строки — MaxLineLen), а refill
// дозакрывает редкие пересечения края.
const scanWindow = 64 << 10

// scanner — построчный сканер байтового потока с фиксированной памятью
// (окно + накопитель одной строки, ТЗ 3.5). Концы строк \n, \r и \r\n;
// BOM снимается один раз, в начале файла.
type scanner struct {
	r       io.Reader
	buf     []byte // массив окна чтения
	pend    []byte // непотреблённые байты (префикс buf)
	acc     []byte // накопитель строки, пересекающей край окна (cap = MaxLineLen)
	started bool   // BOM проверяется один раз, перед первой строкой
	eof     bool
	err     error // ошибка чтения: выдаётся, когда окно иссякнет
}

func newScanner(r io.Reader) *scanner { return newScannerWindow(r, scanWindow) }

// newScannerWindow — сканер с окном заданного размера: тесты прижимают
// окно, чтобы проверить границы (CR на краю окна, CRLF через край,
// BOM). Минимум 3 байта — длина BOM.
func newScannerWindow(r io.Reader, window int) *scanner {
	if window < 3 {
		window = 3
	}
	return &scanner{
		r:   r,
		buf: make([]byte, window),
		acc: make([]byte, 0, MaxLineLen),
	}
}

// refill пополняет окно, перенося непотреблённые байты в его начало
// (окно исчерпано не полностью, когда строка пересекла край или CR
// оказался последним байтом). Read может отдавать мало — окно
// доливается целиком, чтобы BOM в начале файла не оказался разрезан
// между окнами. Возвращает true, если в окне есть данные; EOF и ошибка
// чтения запоминаются: ошибка выдаётся следующим readLine после
// исчерпания окна — уже прочитанные строки не теряются.
func (s *scanner) refill() bool {
	if s.eof || s.err != nil {
		return false
	}
	n := copy(s.buf, s.pend)
	for n < len(s.buf) {
		k, err := s.r.Read(s.buf[n:])
		n += k
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.eof = true
			} else {
				s.err = err
			}
			break
		}
		if k == 0 {
			// Read обязан возвращать данные или ошибку; иначе — защита
			// от зацикливания (io.ErrNoProgress).
			s.err = io.ErrNoProgress
			break
		}
	}
	s.pend = s.buf[:n]
	return n > 0
}

// stripBOM снимает UTF-8 BOM (U+FEFF) в начале файла — и только в начале.
func (s *scanner) stripBOM() {
	if len(s.pend) >= 3 && bytes.Equal(s.pend[:3], []byte{0xEF, 0xBB, 0xBF}) {
		s.pend = s.pend[3:]
	}
}

// readLine возвращает следующую физическую строку файла. line — байты
// без конца строки (для строки длиннее MaxLineLen — первые MaxLineLen
// байт), size — полный размер строки в байтах; ok = false — строк больше
// нет. Ошибка чтения возвращается, когда данные окна исчерпаны.
func (s *scanner) readLine() (line []byte, size int, ok bool, err error) {
	if !s.started {
		s.started = true
		s.refill()
		s.stripBOM()
	}
	s.acc = s.acc[:0]
	for {
		i := bytes.IndexAny(s.pend, "\r\n")
		if i < 0 {
			// конца строки в окне нет: хвост строки уходит в накопитель
			size += len(s.pend)
			s.take(len(s.pend))
			s.pend = s.pend[:0]
			if !s.refill() {
				if s.err != nil {
					return nil, 0, false, s.err
				}
				break // EOF: строка закончилась вместе с файлом
			}
			continue
		}
		size += i
		s.take(i)
		s.pend = s.pend[i:]
		// CR последним байтом окна: дочитываем, чтобы не разделить пару
		// CRLF (заодно не потерять его при EOF).
		for len(s.pend) == 1 && s.pend[0] == '\r' && s.refill() {
		}
		if s.pend[0] == '\r' {
			if len(s.pend) >= 2 && s.pend[1] == '\n' {
				s.pend = s.pend[2:]
			} else {
				s.pend = s.pend[1:]
			}
		} else {
			s.pend = s.pend[1:]
		}
		return s.acc, size, true, nil
	}
	if size == 0 {
		return nil, 0, false, nil
	}
	return s.acc, size, true, nil // последняя строка без конца строки
}

// take дописывает в накопитель первые n байт окна, не превосходя лимит
// MaxLineLen: перебор фиксируется счётчиком size (readLine), а не
// памятью — длинный мусор не буферизуется (ТЗ 3.5).
func (s *scanner) take(n int) {
	if room := MaxLineLen - len(s.acc); n > room {
		n = room
	}
	s.acc = append(s.acc, s.pend[:n]...)
}
