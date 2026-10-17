// Свидетельство (evidence) срабатывания правила — краткий фрагмент «что
// именно найдено» для jsonb laravel_findings.signals (миграция 000001:
// [{rule, weight, evidence}, …]). Требования: краткость (обрезка до N байт),
// читаемость одной строкой и безопасность хранения — контент недоверенный
// (ТЗ §9), свидетельство не должно тащить за собой сырые громоздкие куски,
// управляющие символы и невалидный UTF-8.
package rules

import (
	"strings"
	"unicode/utf8"
)

// evidenceNameBytes — лимит имени cookie/заголовка внутри свидетельства:
// чтобы враждебно длинное имя не вытеснило значение из окна обрезки.
const evidenceNameBytes = 32

// evidence-форматы по целям (стабильный контракт, T-070+ пишет их в jsonb):
//
//	cookie_name   → "cookie XSRF-TOKEN"
//	cookie_value  → "cookie laravel_session=eyJpdiI6…"
//	header_name   → "header X-Powered-By"
//	header_value  → "header X-Powered-By: PHP/8.2"
//	html          → matched-фрагмент регулярного выражения
//	meta          → весь meta-тег (обрезается лимитом свидетельства)
//
// Свидетельство одно на срабатывание правила — первое совпадение по
// детерминированному порядку обхода (cookies в порядке записи; имена
// заголовков сортируются — обход map в Go недетерминирован, а результат
// переанализа обязан быть воспроизводимым).
func evidenceCookieName(name string) string {
	return "cookie " + name
}

func evidenceCookieValue(name, value string) string {
	return "cookie " + capName(name) + "=" + value
}

func evidenceHeaderName(name string) string {
	return "header " + name
}

func evidenceHeaderValue(name, value string) string {
	return "header " + capName(name) + ": " + value
}

// capName компактно ограничивает имя внутри свидетельства (без многоточия —
// итоговую обрезку делает finalizeEvidence один раз для всей строки).
func capName(name string) string {
	if len(name) <= evidenceNameBytes {
		return name
	}
	return truncateUTF8(name, evidenceNameBytes)
}

// finalizeEvidence доводит сырое свидетельство до хранимого вида: невалидный
// UTF-8 → U+FFFD (json.Marshal делал бы то же на выходе — фиксируем здесь,
// чтобы свидетельство было валидно уже в памяти), управляющие символы → «·»
// (одна строка без терминальных escape-последовательностей), обрезка до
// лимита по границе руны с многоточием.
func finalizeEvidence(s string, lim Limits) string {
	s = cleanEvidence(s)
	max := lim.snippetCap()
	if len(s) <= max {
		return s
	}
	return truncateUTF8(s, max) + "…"
}

// cleanEvidence sanitирует недоверенный текст: валидный UTF-8, без
// управляющих символов (C0, DEL).
func cleanEvidence(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	if !strings.ContainsAny(s, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"+
		"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f") {
		return s // быстрый путь обычных страниц
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			r = '·'
		}
		b.WriteRune(r)
	}
	return b.String()
}

// truncateUTF8 обрезает строку не длиннее max байт, не рвая руну пополам:
// хвостовые байты незавершённой руны откатываются (leading-байт без
// продолжения так же бит, как и оборванное продолжение).
func truncateUTF8(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}
	s = s[:max]
	for len(s) > 0 {
		r, size := utf8.DecodeLastRuneInString(s)
		if r == utf8.RuneError && size == 1 {
			s = s[:len(s)-1] // битая/незавершённая руна в хвосте
			continue
		}
		break
	}
	return s
}
