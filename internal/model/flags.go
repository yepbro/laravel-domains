package model

import (
	"fmt"
	"strings"
)

// Flags — битовые признаки домена, колонка domains.flags (smallint,
// ТЗ 6.1). Занятые биты:
//
//	бит 0 — is_idn: метка домена содержит xn--, то есть домен задан
//	         в unicode-нотации (ТЗ 5.2 п. 9; внутри системы хранится
//	         punycode, бит — единственный след исходной нотации);
//	бит 1 — render_pending: домен ждёт рендера — очередь q:render
//	         переполнилась, домен дорабатывается позже и не задерживает
//	         остальные этапы (ТЗ 5.6);
//	бит 2 — http_published: задача q:http текущего DNS-эпизода
//	         опубликована, а результат эпизода ещё не записан —
//	         staging-пометка публикации (T-108): повторная доставка пачки
//	         q:dns не дублирует задачу, а доисходит до записи.
//
// Остальные биты свободны (в ТЗ 6.1 «…») — новые флаги добавляются
// константами-степенями двойки.
type Flags int16

// Биты Flags.
const (
	// FlagIDN — бит 0: is_idn (ТЗ 5.2 п. 9). Сдвиг на ноль в обе стороны
	// равен 1 (1<<0 == 1>>0) — битовый мутант строки эквивалентен,
	// значение бита пинит TestFlagsBitsMatchSchema.
	// mutator-disable-next-line arithmetic/bitwise
	FlagIDN Flags = 1 << 0
	// FlagRenderPending — бит 1: render_pending (ТЗ 5.6).
	FlagRenderPending Flags = 1 << 1
	// FlagHTTPPublished — бит 2: http_published — staging-пометка
	// «задача q:http этого эпизода опубликована» (T-108, DNS-04).
	// Жизненный цикл: dns-worker ставит бит сразу после успешной
	// публикации q:http (до записи результата эпизода); снимает запись
	// результата (done) и постановка планировщика (pending → queued —
	// каждый новый эпизод начинается с чистого бита). Пока бит стоит,
	// повторная доставка пачки пропускает публикацию — дубль q:http
	// (полная повторная HTTP-проверка) не создаётся.
	FlagHTTPPublished Flags = 1 << 2
)

// Set включает указанные биты.
func (f *Flags) Set(flags Flags) { *f |= flags }

// Clear выключает указанные биты.
func (f *Flags) Clear(flags Flags) { *f &^= flags }

// Test сообщает, установлен ли хотя бы один из указанных битов.
func (f Flags) Test(flags Flags) bool { return f&flags != 0 }

// IsIDN — установлен ли бит is_idn.
func (f Flags) IsIDN() bool { return f.Test(FlagIDN) }

// IsRenderPending — установлен ли бит render_pending.
func (f Flags) IsRenderPending() bool { return f.Test(FlagRenderPending) }

// IsHTTPPublished — установлен ли бит http_published (T-108).
func (f Flags) IsHTTPPublished() bool { return f.Test(FlagHTTPPublished) }

// String возвращает человекочитаемый вид: "is_idn",
// "is_idn|render_pending|http_published". Пустая маска — "0"; неизвестные
// биты — шестнадцатеричным остатком "0x…".
func (f Flags) String() string {
	if f == 0 {
		return "0"
	}
	var parts []string
	if f.Test(FlagIDN) {
		parts = append(parts, "is_idn")
	}
	if f.Test(FlagRenderPending) {
		parts = append(parts, "render_pending")
	}
	if f.Test(FlagHTTPPublished) {
		parts = append(parts, "http_published")
	}
	if rest := f &^ (FlagIDN | FlagRenderPending | FlagHTTPPublished); rest != 0 {
		parts = append(parts, fmt.Sprintf("0x%x", int16(rest)))
	}
	return strings.Join(parts, "|")
}
