package httpworker

import "testing"

// TestWorkerReaderDecide — таблица переходов автомата режимов читателя
// q:http (T-262, ТЗ-fsm §3 ФТ-8): наблюдения прохода → режим. Полная
// таблица режимов (браузер/темп) — renderworker/readerstate.go; здесь
// её q:http-проекция: датчиков браузера и темпа нет, режимы Б/Т
// структурно недостижимы (browser=false, rate=false) — строки это
// фиксируют, а две последние подтверждают тотальность функции на всех
// пяти режимах enum'а (паритет автоматов воркеров).
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
