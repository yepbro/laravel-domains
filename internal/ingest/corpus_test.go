// Сквозной корпус ingest — задача T-022 (ТЗ 11.1: «парсинг файлов:
// пустые строки, комментарии, CRLF, BOM, дубликаты, строки не из зоны»).
// В отличие от тестов T-015 (парсер сам по себе) и T-016 (валидатор сам
// по себе) корпус гонит файл через оба этапа так, как их соединит
// потребитель T-017: Parse отдаёт значимые строки и построчные
// отклонения (невалидный UTF-8, лимит длины), Validator превращает
// строки в Entry зоны пути либо в причину отклонения. Лог rejected
// пишется в io.Discard: формат записей закреплён тестами T-016, корпус
// проверяет конвейер целиком.
//
// Контракты корпуса: дубликаты строк (и unicode/punycode формы одного
// IDN-домена) дают по записи на каждое вхождение — дедуп лежит на БД
// (T-018, INSERT … ON CONFLICT DO NOTHING); каждая значимая строка
// парсера доходит до валидатора (Stats.Data = ValidatorStats.Total),
// построчные отклонения парсера — нет.
package ingest

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// corpusEntry — ожидаемая валидная запись: номер физической строки и
// Entry для загрузки (T-017).
type corpusEntry struct {
	num   int
	entry Entry
}

// corpusReject — ожидаемое отклонение валидатора: номер физической
// строки и причина.
type corpusReject struct {
	num    int
	reason Reason
}

// corpusCase — сквозной кейс «файл → строки → записи/отклонения»:
// содержимое файла in, зона пути, порог доли ошибок (0 — дефолт),
// счётчики обоих этапов, вердикт файла, записи, отклонения валидатора
// и построчные отклонения парсера (в порядке строк).
type corpusCase struct {
	name    string
	zone    string
	in      string
	ratio   float64
	stats   Stats
	vstats  ValidatorStats
	failed  bool
	entries []corpusEntry
	rejects []corpusReject
	parsed  []gotReject
}

// corpusLongName — имя из 254 октет (63+63+63+62 и три точки): длиннее
// лимита 253 октет в ASCII-форме (ТЗ 5.2 п. 4) — ReasonNameLen.
var corpusLongName = strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
	strings.Repeat("c", 63) + "." + strings.Repeat("d", 62)

// corpusResult — итог сквозного прогона файла по конвейеру.
type corpusResult struct {
	stats   Stats
	res     FileResult
	entries []corpusEntry
	rejects []corpusReject
	parsed  []gotReject
}

// corpusRun — сквозной прогон кейса: парсер T-015 + валидатор T-016,
// соединённые как в T-017; rejected-логгер пишет в io.Discard.
func corpusRun(t *testing.T, tc corpusCase) corpusResult {
	t.Helper()
	v, err := NewValidator(ValidatorOptions{
		File:          "incoming/" + tc.zone + "/add/list.txt",
		Zone:          tc.zone,
		MaxErrorRatio: tc.ratio,
		RejectLog:     NewRejectedLogger(io.Discard, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	var out corpusResult
	out.stats, err = Parse(strings.NewReader(tc.in), Options{
		OnLine: func(l Line) error {
			entry, reason, ok := v.Line(l.Num, l.Text)
			if ok {
				out.entries = append(out.entries, corpusEntry{l.Num, entry})
			} else {
				out.rejects = append(out.rejects, corpusReject{l.Num, reason})
			}
			return nil
		},
		OnReject: func(rj Reject) error {
			out.parsed = append(out.parsed, gotReject{rj.Num, rj.Reason, rj.Text, rj.Bytes})
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out.res = v.Finish()
	return out
}

// TestCorpusFile — корпус сквозных кейсов файла (ТЗ 11.1).
func TestCorpusFile(t *testing.T) {
	tests := []corpusCase{
		// ТЗ 11.1: пустой файл.
		{
			name: "пустой файл",
			zone: "com",
		},
		// ТЗ 11.1: файл только из комментариев (в том числе с ведущими
		// пробелами — первый значимый символ «#»).
		{
			name:  "файл только из комментариев",
			zone:  "com",
			in:    "# домены зоны com\r\n  # комментарий с ведущими пробелами\n#\n",
			stats: Stats{Total: 3, Skipped: 3},
		},
		// ТЗ 11.1: пустые строки.
		{
			name:  "файл из пустых строк",
			zone:  "com",
			in:    "\n\r\n \t \n",
			stats: Stats{Total: 3, Skipped: 3},
		},
		// ТЗ 11.1: CRLF-файл.
		{
			name:   "CRLF-файл",
			zone:   "com",
			in:     "# зона com\r\nexample.com\r\nshop.com\r\n",
			stats:  Stats{Total: 3, Skipped: 1, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{2, Entry{"com", "example", false}},
				{3, Entry{"com", "shop", false}},
			},
		},
		// ТЗ 11.1: BOM-файл — BOM снимается в начале файла.
		{
			name:   "BOM-файл",
			zone:   "com",
			in:     "\ufeffexample.com\nshop.com\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "example", false}},
				{2, Entry{"com", "shop", false}},
			},
		},
		// BOM в середине файла остаётся в данных (парсер не трогает) и
		// убирается нормализацией UTS-46 — разборка ролей этапов.
		{
			name:   "BOM в середине файла — в данных, убирает нормализация",
			zone:   "com",
			in:     "a.com\n\ufeffb.com\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "a", false}},
				{2, Entry{"com", "b", false}},
			},
		},
		// ТЗ 11.1: дубликаты строк. Контракт: каждое вхождение валидно и
		// даёт свою запись (нормализация сводит формы к одной метке),
		// дедуп — задача БД (T-018), конвейер не отбрасывает повтор.
		{
			name:   "дубликаты строк — дедуп на БД (T-018), не на конвейере",
			zone:   "com",
			in:     "example.com\nexample.com\nEXAMPLE.com.\n",
			stats:  Stats{Total: 3, Data: 3},
			vstats: ValidatorStats{Total: 3, Accepted: 3},
			entries: []corpusEntry{
				{1, Entry{"com", "example", false}},
				{2, Entry{"com", "example", false}},
				{3, Entry{"com", "example", false}},
			},
		},
		// ТЗ 11.1: строки не из зоны пути. Доля отказов ~1 (3 из 4)
		// превышает порог по умолчанию 0.1 — файл ошибочен целиком
		// (сигнал failed/ для T-021), но строки разобраны и посчитаны.
		{
			name:   "файл не из зоны пути — доля ошибок роняет файл",
			zone:   "com",
			in:     "example.com\nexample.org\nsub.example.net\nпример.рф\n",
			stats:  Stats{Total: 4, Data: 4},
			vstats: ValidatorStats{Total: 4, Accepted: 1, Rejected: 3},
			failed: true,
			entries: []corpusEntry{
				{1, Entry{"com", "example", false}},
			},
			rejects: []corpusReject{
				{2, ReasonWrongZone},
				{3, ReasonWrongZone},
				{4, ReasonWrongZone},
			},
		},
		// 2LD-регистрация в TLD-зоне пути (ТЗТ §5.3): example.co.uk
		// в каталоге uk — бывший wrong_zone, теперь accept с составной
		// меткой example.co (зона имени — TLD uk).
		{
			name:   "2LD-регистрация в TLD-зоне пути: example.co.uk в зоне uk",
			zone:   "uk",
			in:     "good.uk\nexample.co.uk\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"uk", "good", false}},
				{2, Entry{"uk", "example.co", false}},
			},
		},
		// Составная зона пути и составная метка (контракт idn, T-010).
		{
			name:   "составная зона пути co.uk",
			zone:   "co.uk",
			ratio:  1,
			in:     "example.co.uk\nмагазин.рф.co.uk\nexample.com.au\n",
			stats:  Stats{Total: 3, Data: 3},
			vstats: ValidatorStats{Total: 3, Accepted: 2, Rejected: 1},
			entries: []corpusEntry{
				{1, Entry{"co.uk", "example", false}},
				{2, Entry{"co.uk", "xn--80aairftm.xn--p1ai", true}},
			},
			rejects: []corpusReject{
				{3, ReasonWrongZone},
			},
		},
		// ТЗ 11.1 (нормализация + парсинг): IDN-домен одной строкой в
		// unicode и в punycode — нормализация даёт одну и ту же запись;
		// unicode-зона пути каталога. Как и дубликаты: по записи на
		// вхождение, схлопывание форм — на БД (T-018).
		{
			name:   "IDN: unicode и punycode формы одного домена — одна запись",
			zone:   "рф",
			in:     "пример.рф\nxn--e1afmkfd.xn--p1ai\nПРИМЕР.РФ.\n",
			stats:  Stats{Total: 3, Data: 3},
			vstats: ValidatorStats{Total: 3, Accepted: 3},
			entries: []corpusEntry{
				{1, Entry{"xn--p1ai", "xn--e1afmkfd", true}},
				{2, Entry{"xn--p1ai", "xn--e1afmkfd", true}},
				{3, Entry{"xn--p1ai", "xn--e1afmkfd", true}},
			},
		},
		// Крайние пробелы снимает парсер; внутренние остаются в Line —
		// дальше их разбирает извлечение T-132 (разделитель полей) либо
		// нормализация одиночного имени (ТЗ 5.2 п. 1).
		{
			name:   "пробелы вокруг строки",
			zone:   "com",
			in:     "  \texample.com \t \nshop.com\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "example", false}},
				{2, Entry{"com", "shop", false}},
			},
		},
		// T-132 (смена поведения): внутренний пробел — разделитель полей
		// выгрузки, помарки больше не склеиваются нормализацией: из
		// «при мер.рф» извлекается токен с точкой (мер.рф), а не склейка
		// «пример.рф». Крайние пробелы по-прежнему снимает парсер,
		// внутренние остаются в Line (разборка ролей этапов не менялась).
		{
			name:   "внутренние пробелы — извлечение токена с точкой (T-132)",
			zone:   "рф",
			in:     "при мер.рф\n",
			stats:  Stats{Total: 1, Data: 1},
			vstats: ValidatorStats{Total: 1, Accepted: 1},
			entries: []corpusEntry{
				{1, Entry{"xn--p1ai", "xn--e1ann", true}},
			},
		},
		// T-132: смешанные строки реальных выгрузок — посторонние данные
		// в строке (дата, регистратор, URL, CSV-обрамление) снимаются
		// извлечением; строка без точкосодержащих токенов — no_domain.
		// Порог 1: проверяем разбор, не вердикт (2 отклонения из 6 при
		// дефолте роняли бы файл — no_domain считается в долю ошибок).
		{
			name:  "смешанные строки: извлечение домена (T-132)",
			zone:  "com",
			ratio: 1,
			in: "xxx.com 2029-11-11 TIMEWEB\n" + // пример заказчика
				"https://shop.com/page\n" + // URL с путём
				"\"quote.com\",\"2029-11-11\",\"TIMEWEB\"\n" + // CSV в кавычках
				"yyy.org xxx.com\n" + // первый кандидат — чужая зона
				"нет домена тут\n" + // кандидатов нет
				"2029-11-11 TIMEWEB\n", // даты/слова — не кандидаты
			stats:  Stats{Total: 6, Data: 6},
			vstats: ValidatorStats{Total: 6, Accepted: 4, Rejected: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "xxx", false}},
				{2, Entry{"com", "shop", false}},
				{3, Entry{"com", "quote", false}},
				{4, Entry{"com", "xxx", false}},
			},
			rejects: []corpusReject{
				{5, ReasonNoDomain},
				{6, ReasonNoDomain},
			},
		},
		// T-132: IDN в смешанной строке и в URL — полный конвейер ТЗ 5.2
		// (пунникод, метка, бит is_idn) на извлечённом токене.
		{
			name:  "IDN в смешанной строке и URL (T-132)",
			zone:  "рф",
			ratio: 1,
			in: "пример.рф 2029-11-11 TIMEWEB\n" +
				"https://ПРИМЕР.РФ/page\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"xn--p1ai", "xn--e1afmkfd", true}},
				{2, Entry{"xn--p1ai", "xn--e1afmkfd", true}},
			},
		},
		// ТЗ 5.2 п. 3: битый punycode отклоняется как невалидное имя.
		{
			name:   "битый punycode",
			zone:   "com",
			ratio:  1,
			in:     "xn--bcher--kva.com\nxn--zzzzzz.com\nok.com\n",
			stats:  Stats{Total: 3, Data: 3},
			vstats: ValidatorStats{Total: 3, Accepted: 1, Rejected: 2},
			entries: []corpusEntry{
				{3, Entry{"com", "ok", false}},
			},
			rejects: []corpusReject{
				{1, ReasonInvalid},
				{2, ReasonInvalid},
			},
		},
		// Длинные имена (ТЗ 5.2 п. 4): метка 64 октета и имя 254 октета.
		{
			name:   "длинные имена: метка длиннее 63 и имя длиннее 253",
			zone:   "com",
			ratio:  1,
			in:     "ok.com\n" + strings.Repeat("a", 64) + ".com\n" + corpusLongName + "\n",
			stats:  Stats{Total: 3, Data: 3},
			vstats: ValidatorStats{Total: 3, Accepted: 1, Rejected: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "ok", false}},
			},
			rejects: []corpusReject{
				{2, ReasonLabelLen},
				{3, ReasonNameLen},
			},
		},
		// Всё вперемешку: BOM, CRLF, комментарии, пустые строки, мусор
		// валидатора (невалидное имя, чужие зоны), отклонения парсера
		// (битый UTF-8, сверхдлинная строка), пробелы, дубликат без
		// завершающего перевода строки. Доля отказов валидатора 3 из 6
		// — ровно порог 0.5: файл не роняется (строгое сравнение).
		{
			name:  "смешанный файл: BOM, CRLF, мусор, дубликаты, порог на грани",
			zone:  "com",
			ratio: 0.5,
			in: "\ufeff# зона com: смешанный файл\r\n" +
				"example.com\r\n" +
				"\n" +
				"  # отступ-комментарий\n" +
				"под_чёркивание.com\n" +
				"example.org\n" +
				"пример.рф\r\n" +
				"бит\xffок.com\n" +
				strings.Repeat("a", MaxLineLen+76) + "\n" +
				"   shop.com  \n" +
				"example.com",
			stats:  Stats{Total: 11, Skipped: 3, Rejected: 2, Data: 6},
			vstats: ValidatorStats{Total: 6, Accepted: 3, Rejected: 3},
			entries: []corpusEntry{
				{2, Entry{"com", "example", false}},
				{10, Entry{"com", "shop", false}},
				{11, Entry{"com", "example", false}},
			},
			rejects: []corpusReject{
				{5, ReasonInvalid},
				{6, ReasonWrongZone},
				{7, ReasonWrongZone},
			},
			parsed: []gotReject{
				{8, ReasonUTF8, "бит\xffок.com", len("бит\xffок.com")},
				{9, ReasonLong, strings.Repeat("a", maxRejectText), MaxLineLen + 76},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := corpusRun(t, tc)

			// Счётчики парсера и его инвариант (T-015).
			if got.stats != tc.stats {
				t.Errorf("Stats = %+v, want %+v", got.stats, tc.stats)
			}
			if got.stats.Total != got.stats.Skipped+got.stats.Rejected+got.stats.Data {
				t.Errorf("инвариант Total != Skipped+Rejected+Data: %+v", got.stats)
			}
			// Каждая значимая строка парсера доходит до валидатора,
			// отклонённые парсером — нет: оба счётчика сцеплены.
			if got.res.Total != got.stats.Data {
				t.Errorf("ValidatorStats.Total = %d, want %d (Stats.Data)", got.res.Total, got.stats.Data)
			}
			// Счётчики файла (ТЗ 5.1, отчёт T-021); Reasons — карта,
			// сверяется ниже по производной отклонений.
			if got.res.Total != tc.vstats.Total ||
				got.res.Accepted != tc.vstats.Accepted ||
				got.res.Rejected != tc.vstats.Rejected {
				t.Errorf("ValidatorStats = %+v, want %+v", got.res.ValidatorStats, tc.vstats)
			}
			// Вердикт порога доли ошибок.
			if got.res.Failed != tc.failed {
				t.Errorf("Failed = %v, want %v", got.res.Failed, tc.failed)
			}
			// Причины — производная ожидаемых отклонений.
			wantReasons := make(map[Reason]int, len(tc.rejects))
			for _, rj := range tc.rejects {
				wantReasons[rj.reason]++
			}
			if len(got.res.Reasons) != len(wantReasons) {
				t.Fatalf("Reasons = %+v, want %+v", got.res.Reasons, wantReasons)
			}
			for r, n := range wantReasons {
				if got.res.Reasons[r] != n {
					t.Errorf("Reasons[%s] = %d, want %d", r, got.res.Reasons[r], n)
				}
			}

			corpusCompareEntries(t, got.entries, tc.entries)
			corpusCompareRejects(t, got.rejects, tc.rejects)
			compareRejects(t, got.parsed, tc.parsed) // хелпер parser_test.go
		})
	}
}

// TestCorpusErrorRatio — порог доли ошибок ingest.max_error_ratio на
// грани, сквозью через файл (ТЗ 5.1: единичный мусор файлу не вредит,
// файл не в той зоне роняет его; доля, равная порогу, — не роняет).
// Вердикт Failed — сигнал для T-021 (перемещение в failed/): валидные
// строки ошибочного файла потребитель не грузит, хотя конвейер их
// разобрал и посчитал.
func TestCorpusErrorRatio(t *testing.T) {
	// corpusRatioFile — файл из good доменов зоны com и bad чужих
	// (wrong_zone).
	corpusRatioFile := func(good, bad int) string {
		var b strings.Builder
		for i := 0; i < good; i++ {
			fmt.Fprintf(&b, "good%d.com\n", i)
		}
		for i := 0; i < bad; i++ {
			fmt.Fprintf(&b, "bad%d.org\n", i)
		}
		return b.String()
	}
	tests := []struct {
		name      string
		ratio     float64
		good, bad int
		failed    bool
	}{
		{"доля равна порогу 0.5 — файл не ошибочен", 0.5, 2, 2, false},
		{"доля выше порога 0.5 — файл ошибочен", 0.5, 1, 3, true},
		{"1 из 10 при пороге по умолчанию 0.1 — файл не ошибочен", 0, 9, 1, false},
		{"2 из 10 при пороге по умолчанию 0.1 — файл ошибочен", 0, 8, 2, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := corpusRun(t, corpusCase{
				zone:  "com",
				in:    corpusRatioFile(tc.good, tc.bad),
				ratio: tc.ratio,
			})
			if got.stats != (Stats{Total: tc.good + tc.bad, Data: tc.good + tc.bad}) {
				t.Errorf("Stats = %+v, want Total=Data=%d", got.stats, tc.good+tc.bad)
			}
			if len(got.parsed) != 0 {
				t.Errorf("отклонения парсера = %+v, want нет", got.parsed)
			}
			want := ValidatorStats{Total: tc.good + tc.bad, Accepted: tc.good, Rejected: tc.bad}
			if got.res.Total != want.Total || got.res.Accepted != want.Accepted ||
				got.res.Rejected != want.Rejected {
				t.Errorf("ValidatorStats = %+v, want %+v (без Reasons)", got.res.ValidatorStats, want)
			}
			if len(got.entries) != tc.good || len(got.rejects) != tc.bad {
				t.Errorf("записей %d (want %d), отклонений %d (want %d)",
					len(got.entries), tc.good, len(got.rejects), tc.bad)
			}
			if got.res.Failed != tc.failed {
				t.Errorf("Failed = %v, want %v", got.res.Failed, tc.failed)
			}
		})
	}
}

// corpusCompareEntries — сравнение ожидаемых записей (в порядке строк).
func corpusCompareEntries(t *testing.T, got, want []corpusEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("записи = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("запись[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// corpusCompareRejects — сравнение ожидаемых отклонений валидатора.
func corpusCompareRejects(t *testing.T, got, want []corpusReject) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("отклонения валидатора = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("отклонение[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}
