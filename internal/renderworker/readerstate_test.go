package renderworker

import "testing"

// TestRenderWorker_ReaderDecide — the transition table of the reader mode
// FSM (T-262, TZ-fsm §3 FR-8): the full set of pass observations →
// a mode. The rows follow the T1→T4 priority order of the
// readerstate.go specification: each row assumes the observations of the rows
// above did not hold; an unmounted sensor is encoded by the zero
// values of its fields (the decision does not read the value) — these rows
// also fix the unreachability of pauses without a sensor.
func TestRenderWorker_ReaderDecide(t *testing.T) {
	const batch = 4
	for _, c := range []struct {
		name string
		obs  readerObs
		want readerMode
	}{
		{"штатное чтение: всё живо",
			readerObs{browser: true, alive: true, failed: 0, batch: batch, rate: true, allowance: batch}, modeConsume},
		{"П1: мёртвый браузер перекрывает все паузы",
			readerObs{browser: true, alive: false, infra: true, writable: false, failed: 100, batch: batch, rate: true, allowance: 0}, modePauseBrowser},
		{"нет датчика браузера — alive не наблюдается",
			readerObs{browser: false, alive: false, failed: 0, batch: batch}, modeConsume},
		{"П2: инфра-пауза перекрывает предохранитель и темп",
			readerObs{browser: true, alive: true, infra: true, writable: false, failed: 100, batch: batch, rate: true, allowance: 0}, modePauseInfra},
		{"флаг инфра-сбоя горит, проба записи прошла",
			readerObs{browser: true, alive: true, infra: true, writable: true, failed: 0, batch: batch}, modeConsume},
		{"флаг не горит — writable не наблюдается (failed ниже порога)",
			readerObs{infra: false, writable: false, failed: 3, batch: batch}, modeConsume},
		{"П3: предохранитель перекрывает темп",
			readerObs{alive: true, infra: false, failed: batch, batch: batch, rate: true, allowance: 0}, modePauseFail},
		{"неудачи ниже пачки чтения — предохранитель молчит",
			readerObs{failed: batch - 1, batch: batch}, modeConsume},
		{"нулевой счётчик — не пауза (ветка сброса серии)",
			readerObs{failed: 0, batch: batch}, modeConsume},
		{"нулевой счётчик при вырожденной пачке — не пауза",
			readerObs{failed: 0, batch: 0}, modeConsume},
		{"П4: окно темпа исчерпано",
			readerObs{failed: 0, batch: batch, rate: true, allowance: 0}, modePauseRate},
		{"П4: окно темпа в долге (allowance < 0)",
			readerObs{failed: 0, batch: batch, rate: true, allowance: -2}, modePauseRate},
		{"нет датчика темпа — allowance не наблюдается",
			readerObs{failed: 0, batch: batch, rate: false, allowance: 0}, modeConsume},
		{"свободные слоты темпа урезают пачку — это чтение, не пауза",
			readerObs{failed: 0, batch: batch, rate: true, allowance: 2}, modeConsume},
	} {
		if got := readerDecide(c.obs); got != c.want {
			t.Errorf("%s: режим %s, хочу %s (наблюдения: %+v)", c.name, got, c.want, c.obs)
		}
	}
}
