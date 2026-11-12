package httpworker

import "testing"

// TestWorkerReaderDecide — the mode automaton's transition table of the q:http
// reader (T-262, TZ-fsm spec §3 FR-8): the pass's observations → a mode. The full
// mode table (browser/rate) — renderworker/readerstate.go; here —
// its q:http projection: there are no browser and rate sensors, the B/R modes
// are structurally unreachable (browser=false, rate=false) — the rows fix
// this, and the last two confirm the totality of the function on all
// five modes of the enum (the workers' automaton parity).
func TestWorkerReaderDecide(t *testing.T) {
	const batch = 4
	for _, c := range []struct {
		name string
		obs  readerObs
		want readerMode
	}{
		{"П5: штатное чтение",
			readerObs{failed: 0, batch: batch}, modeConsume},
		{"П2: инфра-пауза перекрывает предохранитель",
			readerObs{infra: true, writable: false, failed: 100, batch: batch}, modePauseInfra},
		{"флаг инфра-сбоя горит, проба записи прошла — ниже предохранитель",
			readerObs{infra: true, writable: true, failed: batch, batch: batch}, modePauseFail},
		{"флаг не горит — writable не наблюдается, работает предохранитель",
			readerObs{infra: false, writable: false, failed: batch, batch: batch}, modePauseFail},
		{"П3: предохранитель на пороге пачки",
			readerObs{failed: batch, batch: batch}, modePauseFail},
		{"неудачи ниже пачки чтения — предохранитель молчит",
			readerObs{failed: batch - 1, batch: batch}, modeConsume},
		{"нулевой счётчик — не пауза (ветка сброса серии)",
			readerObs{failed: 0, batch: batch}, modeConsume},
		{"нулевой счётчик при вырожденной пачке — не пауза",
			readerObs{failed: 0, batch: 0}, modeConsume},
		{"недостижимая пауза-браузер: датчика в q:http нет",
			readerObs{browser: false, alive: false, failed: 0, batch: batch}, modeConsume},
		{"недостижимая пауза-темп: датчика в q:http нет",
			readerObs{rate: false, allowance: 0, failed: 0, batch: batch}, modeConsume},
		{"тотальность enum'а: смонтированный датчик браузера выбрал бы Б",
			readerObs{browser: true, alive: false, failed: 0, batch: batch}, modePauseBrowser},
		{"тотальность enum'а: смонтированный датчик темпа выбрал бы Т",
			readerObs{failed: 0, batch: batch, rate: true, allowance: 0}, modePauseRate},
	} {
		if got := readerDecide(c.obs); got != c.want {
			t.Errorf("%s: режим %s, хочу %s (наблюдения: %+v)", c.name, got, c.want, c.obs)
		}
	}
}
