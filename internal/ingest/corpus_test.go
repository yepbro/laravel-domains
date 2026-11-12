// The end-to-end ingest corpus — task T-022 (spec 11.1: "parsing files:
// empty rows, comments, CRLF, BOM, duplicates, rows not of the zone").
// Unlike the T-015 tests (the parser on its own) and the T-016 tests
// (the validator on its own), the corpus runs a file through both stages
// the way the T-017 consumer connects them: Parse yields the meaningful
// rows and the per-row rejections (invalid UTF-8, the length limit),
// Validator turns the rows into an Entry of the path's zone or into a
// rejection reason. The rejected log goes to io.Discard: the record
// format is pinned by the T-016 tests, the corpus checks the pipeline as
// a whole.
//
// The corpus contracts: duplicate rows (and the unicode/punycode forms
// of one IDN domain) yield a record per occurrence — the dedup is the
// DB's job (T-018, INSERT … ON CONFLICT DO NOTHING); every meaningful
// parser row reaches the validator (Stats.Data = ValidatorStats.Total),
// the per-row parser rejections do not.
package ingest

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// corpusEntry — an expected valid record: the physical row number and
// the Entry to load (T-017).
type corpusEntry struct {
	num   int
	entry Entry
}

// corpusReject — an expected validator rejection: the physical row
// number and the reason.
type corpusReject struct {
	num    int
	reason Reason
}

// corpusCase — an end-to-end case "file → rows → records/rejections":
// the file content in, the path's zone, the error-share threshold
// (0 — default), the counters of both stages, the file verdict, the
// records, the validator rejections and the per-row parser rejections
// (in row order).
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

// corpusLongName — a name of 254 octets (63+63+63+62 and three dots):
// longer than the 253-octet limit in the ASCII form (spec 5.2 item 4) —
// ReasonNameLen.
var corpusLongName = strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
	strings.Repeat("c", 63) + "." + strings.Repeat("d", 62)

// corpusResult — the result of an end-to-end file run through the
// pipeline.
type corpusResult struct {
	stats   Stats
	res     FileResult
	entries []corpusEntry
	rejects []corpusReject
	parsed  []gotReject
}

// corpusRun — an end-to-end run of the case: the T-015 parser + the
// T-016 validator, connected as in T-017; the rejected logger writes to
// io.Discard. The multi-level directory zone — in the allowlist
// (the tld-only-zones spec §5.1): the corpus checks the per-row
// semantics, the gate — TestNewValidator.
func corpusRun(t *testing.T, tc corpusCase) corpusResult {
	t.Helper()
	var allowed []string
	if strings.Contains(tc.zone, ".") {
		allowed = []string{tc.zone}
	}
	v, err := NewValidator(ValidatorOptions{
		File:             "incoming/" + tc.zone + "/add/list.txt",
		Zone:             tc.zone,
		AllowedZonePaths: allowed,
		MaxErrorRatio:    tc.ratio,
		RejectLog:        NewRejectedLogger(io.Discard, slog.LevelInfo),
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

// TestCorpusFile — the corpus of the file's end-to-end cases
// (spec 11.1).
func TestCorpusFile(t *testing.T) {
	tests := []corpusCase{
		// Spec 11.1: an empty file.
		{
			name: "an empty file",
			zone: "com",
		},
		// Spec 11.1: a file of only comments (including ones with leading
		// spaces — the first meaningful character "#").
		{
			name:  "a file of comments only",
			zone:  "com",
			in:    "# domains of the com zone\r\n  # a comment with leading spaces\n#\n",
			stats: Stats{Total: 3, Skipped: 3},
		},
		// Spec 11.1: empty rows.
		{
			name:  "a file of blank lines",
			zone:  "com",
			in:    "\n\r\n \t \n",
			stats: Stats{Total: 3, Skipped: 3},
		},
		// Spec 11.1: a CRLF file.
		{
			name:   "a CRLF file",
			zone:   "com",
			in:     "# the com zone\r\nexample.com\r\nshop.com\r\n",
			stats:  Stats{Total: 3, Skipped: 1, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{2, Entry{"com", "example", false}},
				{3, Entry{"com", "shop", false}},
			},
		},
		// Spec 11.1: a BOM file — the BOM is stripped at the file start.
		{
			name:   "a BOM file",
			zone:   "com",
			in:     "\ufeffexample.com\nshop.com\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "example", false}},
				{2, Entry{"com", "shop", false}},
			},
		},
		// A BOM in the middle of the file stays in the data (the parser
		// does not touch it) and is removed by the UTS-46 normalization —
		// separating the stages' roles.
		{
			name:   "a BOM mid-file — in the data, normalization strips it",
			zone:   "com",
			in:     "a.com\n\ufeffb.com\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "a", false}},
				{2, Entry{"com", "b", false}},
			},
		},
		// Spec 11.1: duplicate rows. The contract: each occurrence is
		// valid and yields its record (normalization reduces the forms to
		// one label); the dedup is the DB's job (T-018), the pipeline does
		// not drop the repeat.
		{
			name:   "duplicate lines — the DB dedup (T-018), not the pipeline's",
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
		// Spec 11.1: rows not of the path's zone. The rejection share ~1
		// (3 of 4) exceeds the default threshold 0.1 — the file is bad as
		// a whole (the failed/ signal for T-021), but the rows are parsed
		// and counted.
		{
			name:   "a file not from the path zone — the error ratio fails the file",
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
		// 2LD registration in the path's TLD zone (the tld-only-zones spec
		// §5.3): example.co.uk in the uk directory — a former wrong_zone,
		// now an accept with the composite label example.co (the name's
		// zone — the TLD uk).
		{
			name:   "a 2LD registration in the path's TLD zone: example.co.uk in the uk zone",
			zone:   "uk",
			in:     "good.uk\nexample.co.uk\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"uk", "good", false}},
				{2, Entry{"uk", "example.co", false}},
			},
		},
		// A composite path zone and a composite label (the idn contract,
		// T-010).
		{
			name:   "the composite path zone co.uk",
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
		// Spec 11.1 (normalization + parsing): an IDN domain in one line
		// in unicode and in punycode — normalization yields the same
		// record; the unicode zone of the directory's path. Like the
		// duplicates: a record per occurrence, folding the forms is the
		// DB's job (T-018).
		{
			name:   "IDN: the unicode and punycode forms of one domain — one record",
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
		// Edge spaces are stripped by the parser; inner ones stay in the
		// Line — the T-132 extraction (a field separator) or the single
		// name normalization (spec 5.2 item 1) deals with them next.
		{
			name:   "spaces around a line",
			zone:   "com",
			in:     "  \texample.com \t \nshop.com\n",
			stats:  Stats{Total: 2, Data: 2},
			vstats: ValidatorStats{Total: 2, Accepted: 2},
			entries: []corpusEntry{
				{1, Entry{"com", "example", false}},
				{2, Entry{"com", "shop", false}},
			},
		},
		// T-132 (a behavior change): an inner space is a field separator
		// of the export, typos are not glued by the normalization anymore:
		// from "при мер.рф" the token with a dot (мер.рф) is extracted,
		// not the gluing "пример.рф". Edge spaces are still stripped by
		// the parser, inner ones stay in the Line (the stages' role
		// separation unchanged).
		{
			name:   "inner spaces — the dotted token extraction (T-132)",
			zone:   "рф",
			in:     "при мер.рф\n",
			stats:  Stats{Total: 1, Data: 1},
			vstats: ValidatorStats{Total: 1, Accepted: 1},
			entries: []corpusEntry{
				{1, Entry{"xn--p1ai", "xn--e1ann", true}},
			},
		},
		// T-132: mixed lines of real exports — the foreign data in a row
		// (a date, a registrar, a URL, CSV framing) is removed by the
		// extraction; a line without dot-bearing tokens — no_domain.
		// Threshold 1: we check the parsing, not the verdict (2 rejections
		// of 6 at the default would sink the file — no_domain counts into
		// the error share).
		{
			name:  "mixed lines: the domain extraction (T-132)",
			zone:  "com",
			ratio: 1,
			in: "xxx.com 2029-11-11 TIMEWEB\n" + // the customer's example
				"https://shop.com/page\n" + // a URL with a path
				"\"quote.com\",\"2029-11-11\",\"TIMEWEB\"\n" + // CSV in quotes
				"yyy.org xxx.com\n" + // the first candidate — a foreign zone
				"нет домена тут\n" + // no candidates
				"2029-11-11 TIMEWEB\n", // dates/words — not candidates
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
		// T-132: IDN in a mixed line and in a URL — the full pipeline of
		// spec 5.2 (punycode, label, the is_idn bit) on the extracted
		// token.
		{
			name:  "an IDN in a mixed line and a URL (T-132)",
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
		// Spec 5.2 item 3: broken punycode is rejected as an invalid name.
		{
			name:   "broken punycode",
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
		// Long names (spec 5.2 item 4): a 64-octet label and a 254-octet
		// name.
		{
			name:   "long names: a label longer than 63 and a name longer than 253",
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
		// Everything mixed: a BOM, CRLF, comments, empty rows, validator
		// junk (an invalid name, foreign zones), parser rejections
		// (broken UTF-8, an overlong row), spaces, a duplicate without
		// the trailing newline. The validator's rejection share 3 of 6 —
		// exactly the 0.5 threshold: the file is not sunk (a strict
		// comparison).
		{
			name:  "a mixed file: BOM, CRLF, junk, duplicates, the threshold on the edge",
			zone:  "com",
			ratio: 0.5,
			in: "\ufeff# the com zone: a mixed file\r\n" +
				"example.com\r\n" +
				"\n" +
				"  # an indented comment\n" +
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

			// The parser's counters and its invariant (T-015).
			if got.stats != tc.stats {
				t.Errorf("Stats = %+v, want %+v", got.stats, tc.stats)
			}
			if got.stats.Total != got.stats.Skipped+got.stats.Rejected+got.stats.Data {
				t.Errorf("the invariant Total != Skipped+Rejected+Data: %+v", got.stats)
			}
			// Every meaningful parser row reaches the validator, the ones
			// rejected by the parser do not: both counters are chained.
			if got.res.Total != got.stats.Data {
				t.Errorf("ValidatorStats.Total = %d, want %d (Stats.Data)", got.res.Total, got.stats.Data)
			}
			// The file's counters (spec 5.1, the T-021 report); Reasons is
			// a map, checked below against the rejections' derivative.
			if got.res.Total != tc.vstats.Total ||
				got.res.Accepted != tc.vstats.Accepted ||
				got.res.Rejected != tc.vstats.Rejected {
				t.Errorf("ValidatorStats = %+v, want %+v", got.res.ValidatorStats, tc.vstats)
			}
			// The error-share threshold verdict.
			if got.res.Failed != tc.failed {
				t.Errorf("Failed = %v, want %v", got.res.Failed, tc.failed)
			}
			// The reasons — the derivative of the expected rejections.
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
			compareRejects(t, got.parsed, tc.parsed) // a parser_test.go helper
		})
	}
}

// TestCorpusErrorRatio — the ingest.max_error_ratio error-share
// threshold at the boundary, end-to-end through a file (spec 5.1: a
// single junk row does not hurt a file, a file not of the zone sinks
// it; a share equal to the threshold does not sink). The Failed verdict
// — the signal for T-021 (moving to failed/): the consumer does not
// load a bad file's valid rows, although the pipeline parsed and counted
// them.
func TestCorpusErrorRatio(t *testing.T) {
	// corpusRatioFile — a file of good domains of the com zone and bad
	// foreign ones (wrong_zone).
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
		{"the share equals the 0.5 threshold — the file is not bad", 0.5, 2, 2, false},
		{"the share is above the 0.5 threshold — the file is bad", 0.5, 1, 3, true},
		{"1 of 10 at the default 0.1 threshold — the file is not bad", 0, 9, 1, false},
		{"2 of 10 at the default 0.1 threshold — the file is bad", 0, 8, 2, true},
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
				t.Errorf("the parser rejections = %+v, want none", got.parsed)
			}
			want := ValidatorStats{Total: tc.good + tc.bad, Accepted: tc.good, Rejected: tc.bad}
			if got.res.Total != want.Total || got.res.Accepted != want.Accepted ||
				got.res.Rejected != want.Rejected {
				t.Errorf("ValidatorStats = %+v, want %+v (without Reasons)", got.res.ValidatorStats, want)
			}
			if len(got.entries) != tc.good || len(got.rejects) != tc.bad {
				t.Errorf("records %d (want %d), rejections %d (want %d)",
					len(got.entries), tc.good, len(got.rejects), tc.bad)
			}
			if got.res.Failed != tc.failed {
				t.Errorf("Failed = %v, want %v", got.res.Failed, tc.failed)
			}
		})
	}
}

// corpusCompareEntries — comparing the expected records (in row order).
func corpusCompareEntries(t *testing.T, got, want []corpusEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("the records = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// corpusCompareRejects — comparing the expected validator rejections.
func corpusCompareRejects(t *testing.T, got, want []corpusReject) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("the validator rejections = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("rejection[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}
