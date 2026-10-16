// Юнит-тесты потокового парсера входных файлов (ТЗ 5.1, 11.1): UTF-8 и
// BOM, концы строк CRLF/CR, пустые строки и комментарии, лимит длины
// строки, нумерация строк и счётчики, ошибки чтения и колбэков, границы
// окна чтения (white-box) и постоянство памяти на большом потоке (ТЗ 3.5).
package ingest

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
)

// gotLine / gotReject — что увидел потребитель Parse.
type gotLine struct {
	num  int
	text string
}

type gotReject struct {
	num    int
	reason Reason
	text   string
	bytes  int
}

// parseString прогоняет Parse по строке и собирает всё, что он отдал.
func parseString(input string) (Stats, []gotLine, []gotReject, error) {
	return parse(io.Reader(strings.NewReader(input)))
}

func parse(r io.Reader) (Stats, []gotLine, []gotReject, error) {
	var st Stats
	var lines []gotLine
	var rejects []gotReject
	st, err := Parse(r, Options{
		OnLine: func(l Line) error {
			lines = append(lines, gotLine{l.Num, l.Text})
			return nil
		},
		OnReject: func(rj Reject) error {
			rejects = append(rejects, gotReject{rj.Num, rj.Reason, rj.Text, rj.Bytes})
			return nil
		},
	})
	return st, lines, rejects, err
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		stats   Stats
		lines   []gotLine
		rejects []gotReject
	}{
		// ТЗ 11.1: пустой файл.
		{
			name:  "пустой файл",
			in:    "",
			stats: Stats{},
		},
		// ТЗ 11.1: только комментарии.
		{
			name:  "только комментарии",
			in:    "# список доменов\n  # отступ-комментарий\r\n#\n",
			stats: Stats{Total: 3, Skipped: 3},
		},
		{
			name:  "файл из одних пустых строк",
			in:    "\n\r\n \t \n",
			stats: Stats{Total: 3, Skipped: 3},
		},
		// ТЗ 11.1: CRLF (и одиночный CR — тоже конец строки).
		{
			name:  "CRLF",
			in:    "example.com\r\nпример.рф\r\n",
			stats: Stats{Total: 2, Data: 2},
			lines: []gotLine{
				{1, "example.com"},
				{2, "пример.рф"},
			},
		},
		{
			name:  "одиночный CR",
			in:    "a.com\rb.com\r",
			stats: Stats{Total: 2, Data: 2},
			lines: []gotLine{
				{1, "a.com"},
				{2, "b.com"},
			},
		},
		// ТЗ 11.1: BOM — снимается только в начале файла.
		{
			name:  "BOM в начале",
			in:    "\ufeffa.com\nb.com\n",
			stats: Stats{Total: 2, Data: 2},
			lines: []gotLine{
				{1, "a.com"},
				{2, "b.com"},
			},
		},
		{
			name:  "файл из одного BOM",
			in:    "\ufeff",
			stats: Stats{},
		},
		{
			name:  "BOM в середине файла не снимается",
			in:    "a.com\n\ufeffb.com\n",
			stats: Stats{Total: 2, Data: 2},
			lines: []gotLine{
				{1, "a.com"},
				{2, "\ufeffb.com"}, // уберёт нормализация (UTS-46), не парсер
			},
		},
		// ТЗ 11.1: пустые строки между данными.
		{
			name:  "пустые строки между данными",
			in:    "\na.com\n\n\nb.com\n",
			stats: Stats{Total: 5, Skipped: 3, Data: 2},
			lines: []gotLine{
				{2, "a.com"},
				{5, "b.com"},
			},
		},
		// Пробелы: крайние снимаются, внутренние остаются (ТЗ 5.2 п. 1 —
		// внутренние уберёт нормализация, не парсер).
		{
			name:  "крайние пробелы снимаются",
			in:    "  \texample.com \t \n",
			stats: Stats{Total: 1, Data: 1},
			lines: []gotLine{{1, "example.com"}},
		},
		{
			name:  "внутренние пробелы остаются",
			in:    "при мер.рф\n",
			stats: Stats{Total: 1, Data: 1},
			lines: []gotLine{{1, "при мер.рф"}},
		},
		// Комментарий — только первый значимый символ «#».
		{
			name:  "решётка не в начале — не комментарий",
			in:    "a.com#x\n",
			stats: Stats{Total: 1, Data: 1},
			lines: []gotLine{{1, "a.com#x"}},
		},
		// Файл без завершающего перевода строки.
		{
			name:  "без завершающего перевода строки",
			in:    "a.com\nb.com",
			stats: Stats{Total: 2, Data: 2},
			lines: []gotLine{
				{1, "a.com"},
				{2, "b.com"},
			},
		},
		// Слишком длинная строка: отклонение, разбор продолжается
		// (лимит — MaxLineLen, обоснование там же).
		{
			name:  "строка ровно MaxLineLen — ещё валидна",
			in:    strings.Repeat("a", MaxLineLen),
			stats: Stats{Total: 1, Data: 1},
			lines: []gotLine{{1, strings.Repeat("a", MaxLineLen)}},
		},
		{
			name:  "строка MaxLineLen+1 — отклонение",
			in:    strings.Repeat("a", MaxLineLen+1) + "\nb.com\n",
			stats: Stats{Total: 2, Rejected: 1, Data: 1},
			rejects: []gotReject{
				{1, ReasonLong, strings.Repeat("a", maxRejectText), MaxLineLen + 1},
			},
			lines: []gotLine{{2, "b.com"}},
		},
		// Невалидный UTF-8: отклонение строки, файл не падает.
		{
			name:  "невалидный UTF-8",
			in:    "ok\xff.com\nb.com\n",
			stats: Stats{Total: 2, Rejected: 1, Data: 1},
			rejects: []gotReject{
				{1, ReasonUTF8, "ok\xff.com", len("ok\xff.com")},
			},
			lines: []gotLine{{2, "b.com"}},
		},
		// Всё вперемешку: нумерация физических строк сквозная.
		{
			name: "смешанный файл",
			in: "\ufeff# заголовок\r\n" +
				"example.com\r\n" +
				"\n" +
				"бит\xffок\n" +
				"   \n" +
				strings.Repeat("x", MaxLineLen+2) + "\n" +
				"последняя.строка",
			stats: Stats{Total: 7, Skipped: 3, Rejected: 2, Data: 2},
			lines: []gotLine{
				{2, "example.com"},
				{7, "последняя.строка"},
			},
			rejects: []gotReject{
				{4, ReasonUTF8, "бит\xffок", len("бит\xffок")},
				{6, ReasonLong, strings.Repeat("x", maxRejectText), MaxLineLen + 2},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, lines, rejects, err := parseString(tc.in)
			if err != nil {
				t.Fatalf("Parse = %v, want без ошибки", err)
			}
			if st != tc.stats {
				t.Errorf("Stats = %+v, want %+v", st, tc.stats)
			}
			if st.Total != st.Skipped+st.Rejected+st.Data {
				t.Errorf("инвариант Total != Skipped+Rejected+Data: %+v", st)
			}
			compareLines(t, lines, tc.lines)
			compareRejects(t, rejects, tc.rejects)
		})
	}
}

func compareLines(t *testing.T, got, want []gotLine) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("строки = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("строка[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func compareRejects(t *testing.T, got, want []gotReject) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("отклонения = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("отклонение[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestParseNilCallbacks — колбэки необязательны: Parse годится как
// счётчик строк файла (предпроверка, отчёт T-021).
func TestParseNilCallbacks(t *testing.T) {
	st, err := Parse(strings.NewReader("a.com\n# комментарий\n\n"), Options{})
	if err != nil {
		t.Fatalf("Parse = %v, want без ошибки", err)
	}
	want := Stats{Total: 3, Skipped: 2, Data: 1}
	if st != want {
		t.Errorf("Stats = %+v, want %+v", st, want)
	}
}

// errStop — сентинел ошибок колбэка.
var errStop = errors.New("остановка потребителем")

// TestParseCallbackError — ошибка колбэка останавливает разбор и
// возвращается обёрнутой; отданное остаётся у потребителя.
func TestParseCallbackError(t *testing.T) {
	var st Stats
	var got []gotLine
	st, err := Parse(strings.NewReader("a.com\nstop.com\nb.com\n"), Options{
		OnLine: func(l Line) error {
			got = append(got, gotLine{l.Num, l.Text})
			if l.Text == "stop.com" {
				return errStop
			}
			return nil
		},
	})
	if !errors.Is(err, errStop) {
		t.Fatalf("Parse = %v, want обёрнутый errStop", err)
	}
	if !strings.Contains(err.Error(), "строка 2") {
		t.Errorf("ошибка без номера строки: %v", err)
	}
	if st != (Stats{Total: 2, Data: 2}) {
		t.Errorf("Stats = %+v, want Total=2 Data=2 (до остановки)", st)
	}
	compareLines(t, got, []gotLine{{1, "a.com"}, {2, "stop.com"}})

	// То же для OnReject.
	st, err = Parse(strings.NewReader(strings.Repeat("a", MaxLineLen+1)+"\n"), Options{
		OnReject: func(Reject) error { return errStop },
	})
	if !errors.Is(err, errStop) {
		t.Fatalf("Parse (OnReject) = %v, want обёрнутый errStop", err)
	}
	if st != (Stats{Total: 1, Rejected: 1}) {
		t.Errorf("Stats (OnReject) = %+v, want Total=1 Rejected=1", st)
	}
}

// failReader — первые n байт источника, затем ошибка fail (сбой чтения
// посреди файла).
type failReader struct {
	r    io.Reader
	n    int
	fail error
}

func (f *failReader) Read(p []byte) (int, error) {
	if f.n == 0 {
		return 0, f.fail
	}
	if len(p) > f.n {
		p = p[:f.n]
	}
	k, err := f.r.Read(p)
	f.n -= k
	return k, err
}

// TestParseReadError — сбой чтения: готовые строки отданы, Parse
// возвращает ошибку, недочитанный хвост не выдан.
func TestParseReadError(t *testing.T) {
	errDisk := errors.New("диск отвалился")
	var got []gotLine
	st, err := Parse(
		&failReader{r: strings.NewReader("a.com\nb.com\n"), n: len("a.com\nb."), fail: errDisk},
		Options{OnLine: func(l Line) error {
			got = append(got, gotLine{l.Num, l.Text})
			return nil
		}},
	)
	if !errors.Is(err, errDisk) {
		t.Fatalf("Parse = %v, want обёрнутая ошибка чтения", err)
	}
	if st != (Stats{Total: 1, Data: 1}) {
		t.Errorf("Stats = %+v, want Total=1 Data=1 (полная строка до сбоя)", st)
	}
	compareLines(t, got, []gotLine{{1, "a.com"}})
}

// shortReader — не больше max байт за Read: по контракту io.Reader
// короткие выдачи допустимы; refill обязан доливать окно (в том числе не
// резать BOM).
type shortReader struct {
	r   io.Reader
	max int
}

func (s shortReader) Read(p []byte) (int, error) {
	if len(p) > s.max {
		p = p[:s.max]
	}
	return s.r.Read(p)
}

// TestParseShortReads — результат коротких чтений совпадает с цельным.
func TestParseShortReads(t *testing.T) {
	in := "\ufeff# комментарий\r\na.com\r\nb.com\rc.com\n#\n"
	wantStats, wantLines, wantRejects, err := parseString(in)
	if err != nil {
		t.Fatalf("эталон: %v", err)
	}
	st, lines, rejects, err := parse(shortReader{r: strings.NewReader(in), max: 3})
	if err != nil {
		t.Fatalf("Parse = %v, want без ошибки", err)
	}
	if st != wantStats {
		t.Errorf("Stats = %+v, want %+v", st, wantStats)
	}
	compareLines(t, lines, wantLines)
	compareRejects(t, rejects, wantRejects)
}

// TestScannerWindowBoundaries — сканер с прижатым окном: пары CRLF и
// одиночные CR на краю окна, строки, пересекающие окно, BOM в окне 3.
// Память парсера — окно + накопитель; окно может быть любым ≥3 байт.
func TestScannerWindowBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		lines []string
	}{
		{"CRLF пересекает край окна", "xxx\r\nyyy", []string{"xxx", "yyy"}},
		{"CR пересекает край окна", "xxx\ryyy", []string{"xxx", "yyy"}},
		{"строка пересекает окно", "abcdefg\r\nh", []string{"abcdefg", "h"}},
		{"BOM и CRLF в окне 3", "\ufeffab\r\nc", []string{"ab", "c"}},
		{"CRLF в конце файла", "ab\r\n", []string{"ab"}},
		{"одиночный CR в конце файла", "ab\r", []string{"ab"}},
		{"только концы строк", "\r\n\r\nx", []string{"", "", "x"}},
		{"файл из одного BOM", "\ufeff", nil},
		{"пустой файл", "", nil},
	}
	for _, tc := range tests {
		for _, window := range []int{3, 4, 5, 7, 16, 64} {
			t.Run(fmt.Sprintf("%s/окно_%d", tc.name, window), func(t *testing.T) {
				sc := newScannerWindow(strings.NewReader(tc.in), window)
				var got []string
				for {
					line, size, ok, err := sc.readLine()
					if err != nil {
						t.Fatalf("readLine: %v", err)
					}
					if !ok {
						break
					}
					if size != len(line) {
						t.Fatalf("size = %d, len(line) = %d (короткая строка: %q)", size, len(line), line)
					}
					got = append(got, string(line))
				}
				if len(got) != len(tc.lines) {
					t.Fatalf("строки = %q, want %q", got, tc.lines)
				}
				for i := range tc.lines {
					if got[i] != tc.lines[i] {
						t.Errorf("строка[%d] = %q, want %q", i, got[i], tc.lines[i])
					}
				}
			})
		}
	}
}

// TestScannerOverlongLine — строка длиннее MaxLineLen, пересекающая окно
// многократно: накопитель не растёт сверх лимита, размер строки честный,
// следующая строка читается нормально (ТЗ 3.5).
func TestScannerOverlongLine(t *testing.T) {
	sc := newScannerWindow(
		strings.NewReader(strings.Repeat("a", MaxLineLen+5)+"\r\nok"), 7)

	line, size, ok, err := sc.readLine()
	if err != nil || !ok {
		t.Fatalf("readLine = (%q, %d, %v, %v), want первая строка", line, size, ok, err)
	}
	if size != MaxLineLen+5 {
		t.Errorf("size = %d, want %d", size, MaxLineLen+5)
	}
	if len(line) != MaxLineLen {
		t.Errorf("накопитель = %d байт, want не больше MaxLineLen", len(line))
	}

	line, size, ok, err = sc.readLine()
	if err != nil || !ok || string(line) != "ok" || size != 2 {
		t.Fatalf("readLine = (%q, %d, %v, %v), want (\"ok\", 2, true, nil)", line, size, ok, err)
	}
	if _, _, ok, err := sc.readLine(); ok || err != nil {
		t.Errorf("readLine после конца = (%v, %v), want (false, nil)", ok, err)
	}

	// То же без конца строки — файл кончился вместе с мусором.
	sc = newScannerWindow(strings.NewReader(strings.Repeat("x", MaxLineLen+2)), 5)
	_, size, ok, err = sc.readLine()
	if err != nil || !ok || size != MaxLineLen+2 {
		t.Fatalf("readLine = (%d, %v, %v), want (%d, true, nil)", size, ok, err, MaxLineLen+2)
	}
}

// lineGen — io.Reader, синтезирующий lines строк «d%08d.crawl.test\n»
// (~21 байт на строку) без файла на диске: поток в десятки МБ для теста
// памяти (ТЗ 3.5, 11.1).
type lineGen struct {
	lines int
	i     int
	buf   []byte
}

func (g *lineGen) Read(p []byte) (int, error) {
	if len(g.buf) == 0 {
		if g.i >= g.lines {
			return 0, io.EOF
		}
		g.buf = fmt.Appendf(g.buf[:0], "d%08d.crawl.test\n", g.i)
		g.i++
	}
	n := copy(p, g.buf)
	g.buf = g.buf[n:]
	return n, nil
}

// heapLive — живая куча после принудительной сборки мусора: выборки в тесте
// памяти меряют именно удерживаемое, а не накопленный мусор.
func heapLive(t *testing.T) uint64 {
	t.Helper()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestParseMemoryConstant — память разбора не зависит от размера файла
// (ТЗ 3.5): поток ≈40 МиБ (на два порядка больше окна чтения), потребитель
// ничего не удерживает — живая куча во время и после разбора обязана
// оставаться в константе. Буферизация файла целиком дала бы живые ~40 МиБ.
func TestParseMemoryConstant(t *testing.T) {
	const lines = 2_000_000 // ~40 МиБ потока
	const limit = 24 << 20  // запас на шум GC; поток — больше

	base := heapLive(t)
	var maxLive, afterLive uint64
	var count int
	var first, last string
	st, err := Parse(&lineGen{lines: lines}, Options{
		OnLine: func(l Line) error {
			count++
			if count == 1 {
				first = l.Text
			}
			last = l.Text
			if l.Num != count {
				t.Errorf("Num = %d, want %d", l.Num, count)
			}
			if count%250_000 == 0 {
				if h := heapLive(t); h > maxLive {
					maxLive = h
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Parse = %v, want без ошибки", err)
	}
	if st != (Stats{Total: lines, Data: lines}) {
		t.Errorf("Stats = %+v, want Total=Data=%d", st, lines)
	}
	if count != lines {
		t.Errorf("отдано %d строк, want %d", count, lines)
	}
	if want := "d00000000.crawl.test"; first != want {
		t.Errorf("первая строка = %q, want %q", first, want)
	}
	if want := fmt.Sprintf("d%08d.crawl.test", lines-1); last != want {
		t.Errorf("последняя строка = %q, want %q", last, want)
	}

	afterLive = heapLive(t)
	if maxLive > base+limit || afterLive > base+limit {
		t.Errorf("живая куча: во время %d, после %d, база %d — файл буферизуется?",
			maxLive, afterLive, base)
	}
}
