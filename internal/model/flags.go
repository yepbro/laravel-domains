package model

import (
	"fmt"
	"strings"
)

// Flags — битовые признаки домена, колонка domains.flags (smallint,
// ТЗ 6.1). Занятые биты (миграция 000001):
//
//	бит 0 — is_idn: метка домена содержит xn--, то есть домен задан
//	         в unicode-нотации (ТЗ 5.2 п. 9; внутри системы хранится
//	         punycode, бит — единственный след исходной нотации);
//	бит 1 — render_pending: домен ждёт рендера — очередь q:render
//	         переполнилась, домен дорабатывается позже и не задерживает
//	         остальные этапы (ТЗ 5.6).
//
// Остальные биты свободны (в ТЗ 6.1 «…») — новые флаги добавляются
// константами-степенями двойки.
type Flags int16

// Биты Flags.
const (
	// FlagIDN — бит 0: is_idn (ТЗ 5.2 п. 9).
	FlagIDN Flags = 1 << 0
	// FlagRenderPending — бит 1: render_pending (ТЗ 5.6).
	FlagRenderPending Flags = 1 << 1
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

// String возвращает человекочитаемый вид: "is_idn", "is_idn|render_pending".
// Пустая маска — "0"; неизвестные биты — шестнадцатеричным остатком "0x…".
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
	if rest := f &^ (FlagIDN | FlagRenderPending); rest != 0 {
		parts = append(parts, fmt.Sprintf("0x%x", int16(rest)))
	}
	return strings.Join(parts, "|")
}
