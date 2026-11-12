// Unit tests of the streaming parser of input files (spec 5.1, 11.1): UTF-8
// and BOM, CRLF/CR line endings, empty lines and comments, the line length
// limit, line numbering and counters, read and callback errors, the read
// window boundaries (white-box), and constant memory on a large stream
// (spec 3.5).
package ingest

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
)

// gotLine / gotReject — what the Parse consumer saw.
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

// parseString runs Parse over a string and collects everything it yielded.
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
		// Spec 11.1: an empty file.
		{
			name:  "пустой файл",
			in:    "",
			stats: Stats{},
		},
		// Spec 11.1: comments only.
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
		// Spec 11.1: CRLF (and a lone CR is a line ending too).
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
		// Spec 11.1: BOM — stripped only at the start of the file.
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
				{2, "\ufeffb.com"}, // normalization (UTS-46) will remove it, not the parser
			},
		},
		// Spec 11.1: empty lines between data lines.
		{
			name:  "пустые строки между данными",
			in:    "\na.com\n\n\nb.com\n",
			stats: Stats{Total: 5, Skipped: 3, Data: 2},
			lines: []gotLine{
				{2, "a.com"},
				{5, "b.com"},
			},
		},
		// Spaces: leading/trailing ones are stripped, inner ones stay in
		// Line.Text — the parser does not know the record format; further
		// down they are parsed by extraction (T-132, the field delimiter) or
		// by normalization of a single name.
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
		// A comment — "#" as the first significant character only.
		{
			name:  "решётка не в начале — не комментарий",
			in:    "a.com#x\n",
			stats: Stats{Total: 1, Data: 1},
			lines: []gotLine{{1, "a.com#x"}},
		},
		// A file without the trailing newline.
		{
			name:  "без завершающего перевода строки",
			in:    "a.com\nb.com",
			stats: Stats{Total: 2, Data: 2},
			lines: []gotLine{
				{1, "a.com"},
				{2, "b.com"},
			},
		},
		// A too long line: rejection, parsing continues
		// (the limit is MaxLineLen; the rationale is there too).
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
		// Invalid UTF-8: the line is rejected, the file does not fail.
		{
			name:  "невалидный UTF-8",
			in:    "ok\xff.com\nb.com\n",
			stats: Stats{Total: 2, Rejected: 1, Data: 1},
			rejects: []gotReject{
				{1, ReasonUTF8, "ok\xff.com", len("ok\xff.com")},
			},
			lines: []gotLine{{2, "b.com"}},
		},
		// Everything mixed: physical line numbering is continuous.
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

// TestParseNilCallbacks — the callbacks are optional: Parse works as a
// line counter of the file (precheck, the T-021 report).
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

// errStop — the sentinel of callback errors.
var errStop = errors.New("остановка потребителем")

// TestParseCallbackError — a callback error stops parsing and is returned
// wrapped; what was yielded stays with the consumer.
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

	// The same for OnReject.
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

// failReader — the first n bytes of the source, then the fail error (a read
// failure in the middle of the file).
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

// TestParseReadError — a read failure: the complete lines have been yielded,
// Parse returns an error, the unread tail is not emitted.
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

// shortReader — no more than max bytes per Read: by the io.Reader contract
// short reads are allowed; refill must top up the window (including not
// cutting the BOM).
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

// TestParseShortReads — the result of short reads matches the whole-read one.
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

// TestScannerWindowBoundaries — a scanner with a squeezed window: CRLF pairs
// and lone CRs at the window edge, lines crossing the window, a BOM in a
// window of 3. Parser memory — a window + an accumulator; the window may be
// any size ≥3 bytes.
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

// TestScannerOverlongLine — a line longer than MaxLineLen crossing the
// window many times: the accumulator does not grow beyond the limit, the
// line size is honest, the next line is read normally (spec 3.5).
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

	// The same without a line ending — the file ended together with the junk.
	sc = newScannerWindow(strings.NewReader(strings.Repeat("x", MaxLineLen+2)), 5)
	_, size, ok, err = sc.readLine()
	if err != nil || !ok || size != MaxLineLen+2 {
		t.Fatalf("readLine = (%d, %v, %v), want (%d, true, nil)", size, ok, err, MaxLineLen+2)
	}
}

// lineGen — an io.Reader synthesizing lines lines of "d%08d.crawl.test\n"
// (~21 bytes per line) without a file on disk: a stream of tens of MB for
// the memory test (spec 3.5, 11.1).
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

// heapLive — the live heap after a forced garbage collection: the samples in
// the memory test measure exactly what is retained, not accumulated garbage.
func heapLive(t *testing.T) uint64 {
	t.Helper()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestParseMemoryConstant — parse memory does not depend on the file size
// (spec 3.5): a stream of ≈40 MiB (two orders of magnitude more than the
// read window), the consumer retains nothing — the live heap during and
// after parsing must stay within a constant. Buffering the file as a whole
// would give a live ~40 MiB.
func TestParseMemoryConstant(t *testing.T) {
	const lines = 2_000_000 // ~40 MiB of stream
	const limit = 24 << 20  // margin for GC noise; the stream is bigger

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
