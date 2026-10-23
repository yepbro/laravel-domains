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
//	         q:dns не дублирует задачу, а доисходит до записи;
//	бит 3 — dead_host_cutoff: последний HTTP-эпизод завершён по мёртвой
//	         сигнатуре apex+www (T-164) — признак для контрольной выборки
//	         ложных cutoff-ов (запись результата ставит/снимает бит).
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
	// FlagDeadHostCutoff — бит 3: dead_host_cutoff — последний
	// HTTP-эпизод домена завершён по мёртвой сигнатуре (T-164): apex
	// и www упали на connect-фазе (connection refused / connect-таймаут,
	// оба протокола), остальные субдомены не проверялись — записи по §8
	// («нет ответа»). Бит ставится и снимается записью результата
	// HTTP-эпизода (флаг последнего эпизода): по нему строится
	// контрольная выборка ложных cutoff-ов — повторная полная проверка
	// cutoff-доменов, критерий T-164 «доля ложных < 1%».
	FlagDeadHostCutoff Flags = 1 << 3
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

// IsDeadHostCutoff — установлен ли бит dead_host_cutoff (T-164).
func (f Flags) IsDeadHostCutoff() bool { return f.Test(FlagDeadHostCutoff) }

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
	if f.Test(FlagDeadHostCutoff) {
		parts = append(parts, "dead_host_cutoff")
	}
	if rest := f &^ (FlagIDN | FlagRenderPending | FlagHTTPPublished | FlagDeadHostCutoff); rest != 0 {
		parts = append(parts, fmt.Sprintf("0x%x", int16(rest)))
	}
	return strings.Join(parts, "|")
}
