// Тесты автомата алертов (T-264, ТЗ tasks/TZ-fsm.md §3 ФТ-10): драйвер
// перехода (единственная точка записи состояния) и табличные тесты
// машин «последовательность наблюдений → последовательность статусов».
// Последовательностные тесты остальных машин мигрированы на месте —
// alerts_test.go / badhashes_test.go / fallbackalerts_test.go (ожидания
// прежние); здесь — общие конструкторы наблюдений и покрытие машин без
// собственного последовательностного теста (unit_oom). Источники
// наблюдений (observe*-функции) и полный стек прохода покрываются
// Pass-тестами тех же файлов.
package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// --- конструкторы наблюдений и прогон последовательностей ---------------------

// obs — измерение (источник жив, наблюдение валидно).
func obs[V any](v V) observation[V] {
	return observation[V]{value: v}
}

// obsErr — ошибка источника (Warn каждого прохода).
func obsErr[V any](err error) observation[V] {
	return observation[V]{err: err}
}

// obsReason — тихая деградация «не измеряется» с причиной (без Warn).
func obsReason[V any](reason string) observation[V] {
	return observation[V]{reason: reason}
}

// runSequence прогоняет последовательность наблюдений через машину и
// возвращает последовательность статусов — формат табличных тестов ФТ-10.
func runSequence[V any](a *Alerts, m *alertMachine[V], list ...observation[V]) []string {
	out := make([]string, 0, len(list))
	for _, o := range list {
		out = append(out, m.transition(a, context.Background(), o).Status)
	}
	return out
}

// wantSequence сверяет последовательность статусов с ожидаемой.
func wantSequence(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: статусов %d (%v), want %d (%v)", name, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: шаг %d: статус = %s, want %s (последовательность: %v)",
				name, i, got[i], want[i], got)
		}
	}
}

// --- драйвер перехода ---------------------------------------------------------

// TestAlertMachineTransition — единственная точка записи состояния
// (инварианты И1/И2/И3 спецификации): ошибка источника — Warn и «не
// измеряется» с причиной в detail, track-память не трогается (серия
// переживает сбой); тихая причина — то же без Warn; отмена ctx — без
// Warn (часть остановки); статус пишется только transition-ом и виден
// следующему проходу (гистерезис/удержание читают его как prev).
func TestAlertMachineTransition(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newDeadMachine() // серия: память видна в детали («N/3»)

	// Ошибка источника: «не измеряется» с Warn; серия не сброшена —
	// следующее живое наблюдение продолжает счёт (не «первое»).
	s := m.transition(env.Alerts, ctx, obs(int64(5)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "1/3") {
		t.Fatalf("первое наблюдение: %s (%s), want ok с серией 1/3", s.Status, s.Detail)
	}
	before := env.buf.String()
	s = m.transition(env.Alerts, ctx, obsErr[int64](errors.New("connection refused")))
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "connection refused") {
		t.Fatalf("ошибка источника: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String()[len(before):], "level=WARN") ||
		!strings.Contains(env.buf.String()[len(before):], "connection refused") {
		t.Errorf("ошибка источника без Warn:\n%s", env.buf.String()[len(before):])
	}
	s = m.transition(env.Alerts, ctx, obs(int64(5)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "2/3") {
		t.Fatalf("после сбоя: %s (%s), want ok с серией 2/3 (память пережила деградацию)", s.Status, s.Detail)
	}

	// Тихая причина — «не измеряется» без Warn; серия, как и при
	// ошибке, не сбрасывается (деградация память не трогает).
	env2 := newAlertsTestEnv(t, nil)
	m2 := env2.newDeadMachine()
	_ = m2.transition(env2.Alerts, ctx, obs(int64(5))) // серия 1/3
	before = env2.buf.String()
	s = m2.transition(env2.Alerts, ctx, obsReason[int64]("Redis не задан"))
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "Redis не задан") {
		t.Fatalf("тихая причина: %s (%s), want not_measured с причиной", s.Status, s.Detail)
	}
	if strings.Contains(env2.buf.String()[len(before):], "level=WARN") {
		t.Errorf("тихая причина дала Warn:\n%s", env2.buf.String()[len(before):])
	}
	s = m2.transition(env2.Alerts, ctx, obs(int64(5)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "2/3") {
		t.Fatalf("после тихой причины: %s (%s), want ok с серией 2/3 (память пережила)", s.Status, s.Detail)
	}

	// Отмена ctx во время перехода — Warn подавлен (часть остановки).
	env3 := newAlertsTestEnv(t, nil)
	m3 := env3.newDeadMachine()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	before = env3.buf.String()
	_ = m3.transition(env3.Alerts, cancelled, obsErr[int64](errors.New("бд недоступна")))
	if strings.Contains(env3.buf.String()[len(before):], "level=WARN") {
		t.Errorf("отмена ctx дала Warn:\n%s", env3.buf.String()[len(before):])
	}

	// Статус пишется переходом: prev следующего прохода — прошлый
	// статус машины (удержание серии по prev = firing).
	env4 := newAlertsTestEnv(t, nil)
	m4 := env4.newDeadMachine()
	got := runSequence(env4.Alerts, m4,
		obs(int64(5)), obs(int64(5)), obs(int64(5)), obs(int64(1)), obs(int64(0)))
	wantSequence(t, "эпизод по prev", got, []string{
		AlertStatusOK, AlertStatusOK, AlertStatusFiring, AlertStatusFiring, AlertStatusOK,
	})
}

// TestAlertMachineUnitOOM — машина unit_oom (семья порога уровня, без
// памяти): каждый вид события отдельно включает алерт, сработавший
// держится до сброса счётчиков юнита, все нули — ok.
func TestAlertMachineUnitOOM(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newUnitOOMMachine()

	events := []struct {
		name  string
		units []UnitEvents
		want  string
	}{
		{"oom", []UnitEvents{{Unit: "crawler-http-worker.service", OOM: 3}}, AlertStatusFiring},
		{"oom_kill", []UnitEvents{{Unit: "crawler-render-worker.service", OOMKill: 1}}, AlertStatusFiring},
		{"рестарты", []UnitEvents{{Unit: "crawler-chromium.service", NRestarts: 2}}, AlertStatusFiring},
		{"два горячих + здоровый", []UnitEvents{
			{Unit: "crawler-http-worker.service", OOMKill: 1},
			{Unit: "crawler-monitor.service"},
			{Unit: "crawler-chromium.service", NRestarts: 4},
		}, AlertStatusFiring},
		{"все нули", []UnitEvents{
			{Unit: "crawler-http-worker.service"},
			{Unit: "crawler-monitor.service"},
		}, AlertStatusOK},
		{"сброс состояния юнита", []UnitEvents{{Unit: "crawler-http-worker.service"}}, AlertStatusOK},
	}
	for _, c := range events {
		s := m.transition(env.Alerts, context.Background(), obs(c.units))
		if s.Status != c.want {
			t.Errorf("%s: статус = %s (%s), want %s", c.name, s.Status, s.Detail, c.want)
		}
	}
	// Деталь сработавшего называет каждый горячий юнит и его счётчики,
	// здоровый не упомянут; деталь ok — число юнитов.
	env2 := newAlertsTestEnv(t, nil)
	m2 := env2.newUnitOOMMachine()
	s := m2.transition(env2.Alerts, context.Background(), obs(events[3].units))
	for _, want := range []string{"crawler-http-worker.service", "oom_kill=1",
		"crawler-chromium.service", "рестартов=4", "эпизод до перезапуска"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("detail %q не содержит %q", s.Detail, want)
		}
	}
	if strings.Contains(s.Detail, "crawler-monitor.service:") {
		t.Errorf("detail %q называет здоровый юнит", s.Detail)
	}
	s = m2.transition(env2.Alerts, context.Background(), obs(events[4].units))
	if !strings.Contains(s.Detail, "2 юнитов") {
		t.Errorf("detail ok = %q, want число юнитов", s.Detail)
	}
}

// TestAlertMachineClockUsesDriver — окна времени (psi/freeze) берут
// «сейчас» из часов драйвера: между проходами двигаются только часы —
// окно закрывается; часы стоят — удержание не растёт (проверка
// прокидывания now из transition в decide).
func TestAlertMachineClockUsesDriver(t *testing.T) {
	base := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	over := obs(PSIStat{Avg10: 95, Avg60: 95, Avg300: 95})

	// Часы драйвера двигаются на 31 мин между проходами — окно закрыто.
	now := base
	moved := NewAlerts(AlertsDeps{}, AlertsOptions{Now: func() time.Time { return now }})
	mMoved := moved.newPSIMachine()
	_ = mMoved.transition(moved, context.Background(), over) // база окна в t0
	now = base.Add(31 * time.Minute)
	if s := mMoved.transition(moved, context.Background(), over); s.Status != AlertStatusFiring {
		t.Errorf("часы +31м: %s (%s), want firing (окно закрыто по часам драйвера)", s.Status, s.Detail)
	}

	// Часы стоят — окно не тикает, удержание не растёт.
	still := base
	fixed := NewAlerts(AlertsDeps{}, AlertsOptions{Now: func() time.Time { return still }})
	mFixed := fixed.newPSIMachine()
	_ = mFixed.transition(fixed, context.Background(), over)
	if s := mFixed.transition(fixed, context.Background(), over); s.Status != AlertStatusOK {
		t.Errorf("часы стоят: %s (%s), want ok (удержание 0 — now из часов драйвера)", s.Status, s.Detail)
	}
}
