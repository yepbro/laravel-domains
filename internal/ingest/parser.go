// Package ingest — processing of input domain files (spec 5.1).
//
// A streaming line-by-line parser — the stage between directory watching
// (internal/watch, T-013) and name validation (T-016): it reads an
// io.Reader and yields meaningful lines with their numbers; the numbers
// let the rejected log (T-016) and the file report (T-021) point at the
// real line. Memory does not depend on the file size (spec 3.5): a fixed
// read window and an accumulator for a single line; the file is never
// assembled in memory as a whole.
//
// File format (spec 5.1): UTF-8, one record per line. The parser is
// responsible for the format only and knows nothing about domains: a
// meaningful line is yielded as is; matching a full domain name is
// checked by the consumer (T-016, internal/idn). Parsing rules:
//
//   - a line ending is \n, \r or the pair \r\n (CRLF is not split into
//     two endings); normalization is removing the line ending;
//   - the BOM (U+FEFF) is removed only at the beginning of the file; in
//     the middle of a line it stays in the data and is removed further
//     down the pipeline by the UTS-46 mapping (idn.Normalize, spec 5.2);
//   - empty lines and comment lines (first meaningful character "#")
//     are skipped (spec 5.1);
//   - outer spaces are removed (unicode.IsSpace — the same predicate as
//     in idn.cleanup); inner spaces remain: in a multi-word line they are
//     the field separators for extraction (T-132), a single name is
//     normalized as is (spec 5.2 item 1);
//   - a line longer than MaxLineLen bytes or with invalid UTF-8 is
//     rejected with its number and reason, parsing continues — the file
//     is not considered erroneous because of single bad lines (spec 5.1);
//     rejections are yielded to the consumer for the rejected log (T-016).
package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

// MaxLineLen — the limit for a raw file line (without the line ending),
// in bytes. A valid line (a "full domain name", spec 5.1) is not longer
// than 253 octets in ASCII form after normalization (spec 5.2 item 4),
// and its original form is not longer with a margin: each octet of the
// punycode form corresponds to at most one code point of the input
// (≤4 bytes of UTF-8), i.e. ≤1012 bytes plus outer spaces and the
// trailing dot. Longer lines are junk: they are rejected line by line,
// without buffering beyond the limit (spec 3.5), and do not interfere
// with parsing the rest of the file.
const MaxLineLen = 1024

// maxRejectText — how many bytes of the raw line to carry into
// Reject.Text: the input is untrusted, the rejected log (T-016) does not
// need long junk.
const maxRejectText = 255

// Reason — a stable reason for line rejection in the ingest package: the
// parser's (invalid UTF-8, long line) and the validator's (the
// idn.ErrorKind classes, wrong_zone, no_domain — see validate.go). Goes
// into the rejected log and the file report.
type Reason string

const (
	// ReasonUTF8 — the line is not valid UTF-8: the file format is
	// UTF-8 (spec 5.1).
	ReasonUTF8 Reason = "invalid_utf8"
	// ReasonLong — the raw line is longer than MaxLineLen bytes: junk,
	// not a name (the limit rationale — MaxLineLen).
	ReasonLong Reason = "line_len"
)

// Line — a meaningful file line (not empty, not a comment).
type Line struct {
	// Num — the physical line number in the file, 1-based. Empty lines,
	// comments and rejected lines are numbered too: the rejected log
	// (T-016) and the report (T-021) reference the real file line.
	Num int
	// Text — the line content: without the line ending, without the BOM
	// of the first line and without outer spaces. Inner spaces remain: in
	// a multi-word line they are the field separators for extraction
	// (T-132); for a single name normalization removes them (spec 5.2
	// item 1).
	Text string
}

// Reject — a line rejected by the parser (ReasonUTF8, ReasonLong).
// Domain name validation is not here: the parser is responsible for the
// format, the consumer checks the line semantics (T-016).
type Reject struct {
	// Num — the physical line number in the file, 1-based.
	Num int
	// Reason — the rejection reason.
	Reason Reason
	// Text — the beginning of the raw line without the line ending (up to
	// maxRejectText bytes). With ReasonUTF8 it may contain invalid bytes
	// as is — these are the original line data.
	Text string
	// Bytes — the full size of the raw line in bytes (without the line ending).
	Bytes int
}

// Stats — line counters of a processed file; the basis of the spec 5.1
// report (the number of lines, … rejected) — task T-021. Invariant:
// Total = Skipped + Rejected + Data.
type Stats struct {
	Total    int // total physical lines of the file
	Skipped  int // skipped: empty lines and "#" comments
	Rejected int // rejected by the parser: not UTF-8, longer than MaxLineLen
	Data     int // meaningful lines yielded (OnLine)
}

// Options — parsing parameters. Both callbacks are optional: nil means
// lines of the corresponding kind are only counted.
type Options struct {
	// OnLine is called for each meaningful line. The returned error stops
	// parsing and is returned from Parse wrapped with the line number —
	// the way to stop at the consumer's discretion (for example, a DB
	// write error, cancellation).
	OnLine func(Line) error
	// OnReject is called for each rejected line — the raw material of the
	// T-016 rejected log. An error stops parsing like OnLine.
	OnReject func(Reject) error
}

// Parse reads r line by line: meaningful lines go to opts.OnLine,
// rejected ones — to opts.OnReject, empty ones and comments are silently
// skipped. Returns the counters. An error is either an r read failure or
// a callback error; lines yielded before the error remain with the
// consumer (parsing does not "roll back"), the counters match what was
// yielded.
func Parse(r io.Reader, opts Options) (Stats, error) {
	var st Stats
	sc := newScanner(r)
	for {
		raw, size, ok, err := sc.readLine()
		if err != nil {
			return st, fmt.Errorf("ingest: reading the input: %w", err)
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
					return st, fmt.Errorf("ingest: line %d: %w", st.Total, err)
				}
			}
		}
	}
}

// emitReject passes a rejection to the consumer with common error handling.
func (o Options) emitReject(rj Reject) error {
	if o.OnReject == nil {
		return nil
	}
	if err := o.OnReject(rj); err != nil {
		return fmt.Errorf("ingest: line %d (rejected): %w", rj.Num, err)
	}
	return nil
}

// truncateText cuts the raw line to maxRejectText bytes for Reject.Text.
func truncateText(raw []byte) string {
	if len(raw) > maxRejectText {
		raw = raw[:maxRejectText]
	}
	return string(raw)
}

// scanWindow — the default read window, 64 KiB: lines almost always fit
// in it entirely (the line limit is MaxLineLen), and refill handles the
// rare edge crossings.
const scanWindow = 64 << 10

// scanner — a byte stream line scanner with fixed memory (a window plus
// the accumulator of one line, spec 3.5). Line endings are \n, \r and
// \r\n; the BOM is removed once, at the beginning of the file.
type scanner struct {
	r       io.Reader
	buf     []byte // the read window buffer
	pend    []byte // unconsumed bytes (a prefix of buf)
	acc     []byte // accumulator for a line crossing the window edge (cap = MaxLineLen)
	started bool   // the BOM is checked once, before the first line
	eof     bool
	err     error // read error: yielded when the window runs out
}

func newScanner(r io.Reader) *scanner { return newScannerWindow(r, scanWindow) }

// newScannerWindow — a scanner with a window of the given size: tests
// squeeze the window to check the boundaries (a CR at the window edge,
// CRLF across the edge, the BOM). The minimum is 3 bytes — the BOM
// length.
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

// refill tops up the window, moving unconsumed bytes to its beginning
// (the window is not fully exhausted when a line crossed the edge or a
// CR happened to be the last byte). Read may return little — the window
// is topped up entirely so that the BOM at the beginning of the file is
// not split between windows. Returns true if there is data in the
// window; EOF and a read error are remembered: the error is yielded by
// the next readLine after the window is exhausted — already read lines
// are not lost.
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
			// Read must return data or an error; otherwise — a guard
			// against looping (io.ErrNoProgress).
			s.err = io.ErrNoProgress
			break
		}
	}
	s.pend = s.buf[:n]
	return n > 0
}

// stripBOM removes the UTF-8 BOM (U+FEFF) at the beginning of the file — and only there.
func (s *scanner) stripBOM() {
	if len(s.pend) >= 3 && bytes.Equal(s.pend[:3], []byte{0xEF, 0xBB, 0xBF}) {
		s.pend = s.pend[3:]
	}
}

// readLine returns the next physical line of the file. line is the bytes
// without the line ending (for a line longer than MaxLineLen — the first
// MaxLineLen bytes), size is the full line size in bytes; ok = false
// means there are no more lines. A read error is returned when the
// window data is exhausted.
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
			// no line ending in the window: the line tail goes into the accumulator
			size += len(s.pend)
			s.take(len(s.pend))
			s.pend = s.pend[:0]
			if !s.refill() {
				if s.err != nil {
					return nil, 0, false, s.err
				}
				break // EOF: the line ends together with the file
			}
			continue
		}
		size += i
		s.take(i)
		s.pend = s.pend[i:]
		// CR as the last byte of the window: read on so as not to split a
		// CRLF pair (and not lose it at EOF).
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
	return s.acc, size, true, nil // the last line without a line ending
}

// take appends the first n bytes of the window to the accumulator without
// exceeding the MaxLineLen limit: the overflow is recorded by the size
// counter (readLine), not by memory — long junk is not buffered
// (spec 3.5).
func (s *scanner) take(n int) {
	if room := MaxLineLen - len(s.acc); n > room {
		n = room
	}
	s.acc = append(s.acc, s.pend[:n]...)
}
