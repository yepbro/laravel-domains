// Тесты триажа мутантов CI-прогона 09.11.2026 (половина A): добивки
// alerts.go/alertstate.go — пины дефолтов и границ withDefaults (0 →
// дефолт, 1 — рабочее значение), границы машин алертов (деталь глубины
// ровно у watermark/порога удержания, порог ≥ ёмкости тома, процент
// свободного точно, наблюдение глубин всех стримов, ceil-граница
// защёлок рендера, серии new_done_stale по меткам снапшота, окно PSI на
// границе процента, кольцо дельт, q:dead глубина 1, удаление исчезнувших
// юнитов памяти) и прямые границы track-памятей.
package monitor

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"crawler/internal/queue"
)

// TestAlertsDefaultsPins — пины констант-дефолтов (значения — контракт
// конфигурации оператора).
func TestAlertsDefaultsPins(t *testing.T) {
	if DefaultAlertsInterval != 5*time.Minute {
		t.Errorf("DefaultAlertsInterval = %v, want 5m", DefaultAlertsInterval)
	}
	if DefaultQueueFreezeWindow != 15*time.Minute {
		t.Errorf("DefaultQueueFreezeWindow = %v, want 15m", DefaultQueueFreezeWindow)
	}
	if DefaultPSICPUPercent != 90.0 {
		t.Errorf("DefaultPSICPUPercent = %v, want 90", DefaultPSICPUPercent)
	}
	if DefaultPSIWindow != 30*time.Minute {
		t.Errorf("DefaultPSIWindow = %v, want 30m", DefaultPSIWindow)
	}
	if DefaultMemHighMinPasses != 2 {
		t.Errorf("DefaultMemHighMinPasses = %d, want 2", DefaultMemHighMinPasses)
	}
	if DefaultRenderDebtDomains != 50000 {
		t.Errorf("DefaultRenderDebtDomains = %d, want 50000", DefaultRenderDebtDomains)
	}
	if DefaultRenderDebtPasses != 2 {
		t.Errorf("DefaultRenderDebtPasses = %d, want 2", DefaultRenderDebtPasses)
	}
}

// TestAlertsWithDefaultsBoundaries — нули и отрицательные заменяются
// дефолтами, единицы — рабочие значения (границы «≤ 0»).
func TestAlertsWithDefaultsBoundaries(t *testing.T) {
	o := AlertsOptions{}.withDefaults()
	if o.QueueFreeze != DefaultQueueFreezeWindow || o.PSICPUPercent != DefaultPSICPUPercent ||
		o.RenderDebtPasses != DefaultRenderDebtPasses || o.MemHighMinPasses != DefaultMemHighMinPasses ||
		o.RenderDebtDomains != DefaultRenderDebtDomains || o.PSIWindow != DefaultPSIWindow ||
		o.Interval != DefaultAlertsInterval || o.ZoneStatsInterval != DefaultZoneStatsInterval ||
		o.DiskFreeMinBytes != DefaultDiskFreeMinBytes || o.DiskFreeCritBytes != DefaultDiskFreeCritBytes {
		t.Errorf("нулевые опции не дефолты: %+v", o)
	}
	// Единицы — рабочие значения, не дефолты (1нс > 0 — граница «≤ 0»
	// различима в обе стороны).
	o = AlertsOptions{
		QueueFreeze: time.Nanosecond, PSICPUPercent: 1, RenderDebtPasses: 1,
		MemHighMinPasses: 1, RenderDebtDomains: 1, PSIWindow: time.Nanosecond,
		Interval: time.Nanosecond, ZoneStatsInterval: time.Nanosecond,
		DiskFreeMinBytes: 1, DiskFreeCritBytes: 1,
	}.withDefaults()
	if o.QueueFreeze != time.Nanosecond || o.PSICPUPercent != 1 || o.RenderDebtPasses != 1 ||
		o.MemHighMinPasses != 1 || o.RenderDebtDomains != 1 || o.PSIWindow != time.Nanosecond ||
		o.Interval != time.Nanosecond || o.ZoneStatsInterval != time.Nanosecond ||
		o.DiskFreeMinBytes != 1 || o.DiskFreeCritBytes != 1 {
		t.Errorf("единичные значения затёрты дефолтами: %+v", o)
	}
}

// TestAlertsQueueHotDetailBoundary — сработавшая деталь: стрим ровно у
// watermark — горячий (деталь называет его, а не коридор удержания);
// стрим ровно у порога удержания — коридор, деталь при отсутствии
// горячих называет его.
func TestAlertsQueueHotDetailBoundary(t *testing.T) {
	// Горячий A ровно у watermark, B — в коридоре: деталь называет
	// только A (мутант «>» отправил бы A в коридор, деталь — оба).
	env := newAlertsTestEnv(t, nil)
	a, b := alertStreams[0], alertStreams[1]
	wmA := queue.StreamWatermark(env.opts.QueueWatermark, env.opts.QueueMaxLen, a)
	wmB := queue.StreamWatermark(env.opts.QueueWatermark, env.opts.QueueMaxLen, b)
	holdB := int64(float64(wmB) * AlertQueueRecoverFactor)
	if holdB >= wmB {
		t.Skip("коридор удержания совпадает с watermark — граница неотличима")
	}
	m := env.newQueuesMachine()
	env.lener.lens = map[string]int64{a: wmA, b: holdB}
	s := m.transition(env.Alerts, context.Background(), obs(env.lener.lens))
	if s.Status != AlertStatusFiring {
		t.Fatalf("глубина у watermark: %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, a+"="+strconv.FormatInt(wmA, 10)+" (порог") {
		t.Errorf("горячий стрим не назван: %s", s.Detail)
	}
	if strings.Contains(s.Detail, b+"=") {
		t.Errorf("коридорный стрим затопил горячий: %s", s.Detail)
	}

	// Горячих нет, B ровно у порога удержания — деталь называет B
	// (мутант «>» у holdAt потерял бы его).
	env2 := newAlertsTestEnv(t, nil)
	m2 := env2.newQueuesMachine()
	// Сначала срабатывание (firing), затем удержание ровно в коридоре.
	env2.lener.lens = map[string]int64{b: wmB}
	_ = m2.transition(env2.Alerts, context.Background(), obs(env2.lener.lens))
	env2.lener.lens = map[string]int64{b: holdB}
	s2 := m2.transition(env2.Alerts, context.Background(), obs(env2.lener.lens))
	if s2.Status != AlertStatusFiring {
		t.Fatalf("удержание в коридоре: %s (%s), want firing", s2.Status, s2.Detail)
	}
	if !strings.Contains(s2.Detail, b+"="+strconv.FormatInt(holdB, 10)+" (порог") {
		t.Errorf("коридорный стрим не назван: %s", s2.Detail)
	}
}

// TestAlertsDiskBytesNABoundary — порог абсолютного минимума ровно
// ёмкости тома — неприменимость, на единицу меньше — применим.
func TestAlertsDiskBytesNABoundary(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	total := int64(200 << 30)
	env.space.stat = SpaceStat{Avail: 100 << 30, Total: uint64(total)}
	mm := env.newDiskBytesMachine(AlertDiskMin, total)
	s := mm.transition(env.Alerts, context.Background(), obs(env.space.stat))
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "неприменим") {
		t.Errorf("порог = ёмкости: %s (%s), want not_measured/неприменим", s.Status, s.Detail)
	}
	mm2 := env.newDiskBytesMachine(AlertDiskMin, total-1)
	s2 := mm2.transition(env.Alerts, context.Background(), obs(env.space.stat))
	if s2.Status == AlertStatusNotMeasured {
		t.Errorf("порог < ёмкости: %s (%s), want измеряется", s2.Status, s2.Detail)
	}
}

// TestAlertsDiskPercentDetail — точный процент свободного в деталях
// (100 × avail / total).
func TestAlertsDiskPercentDetail(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newDiskMachine()
	env.space.stat = SpaceStat{Avail: 50 << 30, Total: 200 << 30} // ровно 25 %
	s := m.transition(env.Alerts, context.Background(), obs(env.space.stat))
	if !strings.Contains(s.Detail, "свободно 25.0%") {
		t.Errorf("деталь процента = %q, want «свободно 25.0%%»", s.Detail)
	}
}

// TestAlertsQueueLensObserved — наблюдение очередей возвращает ВСЕ
// стримы с глубинами (мутант вырезания записи в карту глотал стримы).
func TestAlertsQueueLensObserved(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	lens := map[string]int64{}
	for i, s := range alertStreams {
		lens[s] = int64(i + 1)
	}
	env.lener.lens = lens
	o := env.observeQueues(context.Background())
	if o.err != nil || o.reason != "" {
		t.Fatalf("наблюдение: %+v", o)
	}
	for _, s := range alertStreams {
		if o.value[s] != lens[s] {
			t.Errorf("lens[%s] = %d, want %d", s, o.value[s], lens[s])
		}
	}
}

// TestAlertsRedisMemoryMaxBytesBoundary — maxmemory = 1 — валидное
// наблюдение (потолок есть); = 0 — тихая неприменимость.
func TestAlertsRedisMemoryMaxBytesBoundary(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	env.redisMem.mem = RedisMemory{UsedBytes: 1, MaxBytes: 1, Policy: "noeviction"}
	o := env.observeRedisMemory(context.Background())
	if o.err != nil || o.reason != "" {
		t.Errorf("maxmemory=1: %+v, want измерение", o)
	}
	env.redisMem.mem = RedisMemory{UsedBytes: 1, MaxBytes: 0, Policy: "noeviction"}
	o = env.observeRedisMemory(context.Background())
	if o.reason == "" {
		t.Errorf("maxmemory=0: %+v, want неприменимость", o)
	}
}

// TestAlertsRenderFbDepthWatermarkBoundary — QueueWatermark=1 — рабочее
// значение (наблюдение глубины живо, не «порог не задан»).
func TestAlertsRenderFbDepthWatermarkBoundary(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.QueueWatermark = 1 })
	o := env.observeRenderFallbackDepth(context.Background())
	if o.err != nil || o.reason != "" {
		t.Errorf("QueueWatermark=1: %+v, want измерение", o)
	}
}

// TestAlertsDiskPercentEnabledBoundary — DiskFreePercent=1 — контроль
// включён (измерение, не «выключен»).
func TestAlertsDiskPercentEnabledBoundary(t *testing.T) {
	env := newAlertsTestEnv(t, func(o *AlertsOptions) { o.DiskFreePercent = 1 })
	o := env.observeDisk(context.Background())
	if o.err != nil || o.reason != "" {
		t.Errorf("DiskFreePercent=1: %+v, want измерение", o)
	}
}

// TestDeadStreakNOne — глубина 1 — полноценное событие серии (граница
// n > 0), удержание сработавшего любой ненулевой глубиной.
func TestDeadStreakNOne(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newDeadMachine()
	got := runSequence(env.Alerts, m, obs(int64(1)), obs(int64(1)), obs(int64(1)))
	wantSequence(t, "серия единиц", got, []string{AlertStatusOK, AlertStatusOK, AlertStatusFiring})
	got = runSequence(env.Alerts, m, obs(int64(1)), obs(int64(0)))
	wantSequence(t, "удержание единицей", got, []string{AlertStatusFiring, AlertStatusOK})
}

// TestMemHighStreakGrewAndEvict — рост High наращивает серию с деталью
// «+N за проход» (точная дельта); стагнация — снятие; исчезнувший юнит
// покидает память, возвращение начинает серию заново.
func TestMemHighStreakGrewAndEvict(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newMemHighMachine()
	_ = m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 100}}))
	_ = m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 130}}))
	s3 := m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 170}}))
	if s3.Status != AlertStatusFiring || !strings.Contains(s3.Detail, "+40 за проход") {
		t.Errorf("серия роста: %s (%s), want firing с «+40 за проход»", s3.Status, s3.Detail)
	}
	s4 := m.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 170}}))
	if s4.Status != AlertStatusOK {
		t.Errorf("стагнация: %s, want ok", s4.Status)
	}
	// Юнит исчез и вернулся с ростом — свежая база: серии нет (мутант
	// невычищения памяти продолжил бы серию и ложно срабатывал).
	m2 := env.newMemHighMachine()
	_ = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 100}}))
	_ = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 150}})) // серия 1
	_ = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{}))                       // юнит исчез
	s := m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 160}}))
	if s.Status != AlertStatusOK {
		t.Errorf("после исчезновения: %s (%s), want ok (свежая база, серия с нуля)", s.Status, s.Detail)
	}
	// Серия после возвращения считается заново: два роста — firing.
	s = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 200}}))
	s = m2.transition(env.Alerts, context.Background(), obs([]UnitEvents{{Unit: "u", High: 260}}))
	if s.Status != AlertStatusFiring {
		t.Errorf("новая серия после исчезновения не сработала: %s (%s)", s.Status, s.Detail)
	}
}

// TestRenderDebtTrackBoundaries — порог срабатывания строго больше,
// удержание от защёлки на пороге снятия (ceil), снятие ниже; стагнация
// без роста не срабатывает.
func TestRenderDebtTrackBoundaries(t *testing.T) {
	d := func(total int64) RenderDebt { return RenderDebt{Flags: total} }
	// Порог 10, снятие < 9 (ceil): уровень 9 удерживает, 8 — снимает.
	tr := &renderDebtTrack{domains: 10, passes: 2}
	if s := tr.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("база: %s", s.status)
	}
	if s := tr.decide("", time.Time{}, d(9)); s.status != AlertStatusOK {
		t.Errorf("рост до 9: %s, want ok (9 < 10)", s.status)
	}
	if s := tr.decide("", time.Time{}, d(12)); s.status != AlertStatusFiring {
		t.Errorf("рост выше порога: %s, want firing", s.status)
	}
	if s := tr.decide("", time.Time{}, d(9)); s.status != AlertStatusFiring {
		t.Errorf("удержание на 9: %s (%s), want firing", s.status, s.detail)
	}
	if s := tr.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("снятие на 8: %s, want ok", s.status)
	}
	// Стагнация выше порога без роста — ok.
	tr2 := &renderDebtTrack{domains: 10, passes: 2}
	_ = tr2.decide("", time.Time{}, d(5))
	if s := tr2.decide("", time.Time{}, d(20)); s.status != AlertStatusOK {
		t.Errorf("первый проход выше порога: %s, want ok (серия 1)", s.status)
	}
	if s := tr2.decide("", time.Time{}, d(20)); s.status != AlertStatusOK {
		t.Errorf("стагнация: %s, want ok (нет роста)", s.status)
	}
}

// TestRenderFbDepthBoundaries — глубина ровно у watermark — hot-серия,
// срабатывание стационарной глубиной; удержание ceil-порога снятия.
func TestRenderFbDepthBoundaries(t *testing.T) {
	tr := &renderFbDepthTrack{wm: 100, passes: 2}
	if s := tr.decide("", time.Time{}, 100); s.status != AlertStatusOK {
		t.Errorf("первый у порога: %s, want ok", s.status)
	}
	s := tr.decide("", time.Time{}, 100)
	if s.status != AlertStatusFiring || !strings.Contains(s.detail, "стационарно у порога") {
		t.Errorf("стационарная глубина: %s (%s), want firing «стационарно»", s.status, s.detail)
	}
	if s := tr.decide("", time.Time{}, 90); s.status != AlertStatusFiring {
		t.Errorf("удержание на 90: %s (%s), want firing", s.status, s.detail)
	}
	if s := tr.decide("", time.Time{}, 89); s.status != AlertStatusOK {
		t.Errorf("снятие на 89: %s, want ok", s.status)
	}
	// 99 — не «у порога»: hot не копится.
	tr2 := &renderFbDepthTrack{wm: 100, passes: 2}
	_ = tr2.decide("", time.Time{}, 99)
	if s := tr2.decide("", time.Time{}, 99); s.status != AlertStatusOK {
		t.Errorf("99 два прохода: %s (%s), want ok", s.status, s.detail)
	}
}

// TestNewDoneStaleSnapshotBoundaries — серия растёт только по новым
// снапшотам (смена метки), повтор строки — нет; невалидная метка —
// рост; нулевой снапшот обнуляет; сработавший удерживается единицей.
func TestNewDoneStaleSnapshotBoundaries(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	row := func(v int64, at time.Time) staleRow {
		return staleRow{value: v, snap: sql.NullTime{Time: at, Valid: true}}
	}
	tr := &newDoneStaleTrack{minPasses: 2}
	if s := tr.decide("", time.Time{}, row(5, t0)); !strings.Contains(s.detail, "1/2") {
		t.Errorf("первый снапшот: %s", s.detail)
	}
	if s := tr.decide("", time.Time{}, row(5, t0)); !strings.Contains(s.detail, "1/2") {
		t.Errorf("повтор снапшота нарастил серию: %s", s.detail)
	}
	if s := tr.decide("", time.Time{}, row(5, t0.Add(time.Minute))); s.status != AlertStatusFiring {
		t.Errorf("новый снапшот: %s (%s), want firing", s.status, s.detail)
	}
	// Невалидная метка — рост серии (первый проход такой строки).
	tr2 := &newDoneStaleTrack{minPasses: 2}
	_ = tr2.decide("", time.Time{}, row(5, t0))
	if s := tr2.decide("", time.Time{}, staleRow{value: 5}); !strings.Contains(s.detail, "2/2") {
		t.Errorf("невалидная метка не нарастила серию: %s", s.detail)
	}
	// Сработавший удерживается единицей; нулевой снапшот снимает.
	tr3 := &newDoneStaleTrack{minPasses: 2}
	_ = tr3.decide("", time.Time{}, row(5, t0))
	_ = tr3.decide("", time.Time{}, row(5, t0.Add(time.Minute)))
	if s := tr3.decide(AlertStatusFiring, time.Time{}, row(1, t0.Add(2*time.Minute))); s.status != AlertStatusFiring {
		t.Errorf("удержание единицей: %s, want firing", s.status)
	}
	if s := tr3.decide(AlertStatusFiring, time.Time{}, row(0, t0.Add(2*time.Minute))); s.status != AlertStatusOK {
		t.Errorf("нулевой снапшот: %s, want ok", s.status)
	}
}

// TestPsiWindowBoundary — avg300 ровно на пороге — не «выше» (строгое
// >); чуть выше — окно копится и закрывается firing.
func TestPsiWindowBoundary(t *testing.T) {
	base := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tr := &psiWindowTrack{percent: 90, window: 30 * time.Minute}
	s := tr.decide("", base, PSIStat{Avg10: 90, Avg60: 90, Avg300: 90})
	if s.status != AlertStatusOK || strings.Contains(s.detail, "выше порога") {
		t.Errorf("ровно на пороге: %s (%s), want ok без накопления", s.status, s.detail)
	}
	tr2 := &psiWindowTrack{percent: 90, window: 30 * time.Minute}
	_ = tr2.decide("", base, PSIStat{Avg300: 90.01})
	s = tr2.decide("", base.Add(31*time.Minute), PSIStat{Avg300: 90.01})
	if s.status != AlertStatusFiring {
		t.Errorf("окно закрылось: %s (%s), want firing", s.status, s.detail)
	}
}

// TestDeltaRingSizeOneAndValues — кольцо размера 1 — окно одного
// прохода; сумма — точная дельта, рестарт — нулевая дельта.
func TestDeltaRingSizeOneAndValues(t *testing.T) {
	w := &deltaRing{size: 1}
	if got := w.push(100); got != 0 {
		t.Errorf("база: сумма = %d, want 0", got)
	}
	if got := w.push(130); got != 30 {
		t.Errorf("окно 1: сумма = %d, want 30", got)
	}
	if got := w.push(10); got != 0 {
		t.Errorf("рестарт: сумма = %d, want 0", got)
	}
	if got := w.push(15); got != 5 {
		t.Errorf("после рестарта: сумма = %d, want 5", got)
	}
}

// TestQueueFreezeDepthZero — нулевая глубина не копит неподвижность:
// сколько проходов — ок; глубина 1 — полноценная работа окна (граница
// «> 0»); деталь ок называет счётчик движимых/пустых групп.
func TestQueueFreezeDepthZero(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newQueueFreezeMachine()
	zero := []StreamGroupState{{Stream: "q:dns", Group: "g", Depth: 0, LastDeliveredID: "1-1"}}
	for i := 0; i < 6; i++ {
		s := m.transition(env.Alerts, context.Background(), obs(zero))
		if s.Status != AlertStatusOK {
			t.Errorf("глубина 0, проход %d: %s (%s), want ok", i, s.Status, s.Detail)
		}
		if !strings.Contains(s.Detail, "1 из 1") {
			t.Errorf("деталь окна: %q, want счётчик «1 из 1» (пустая группа — движимая)", s.Detail)
		}
	}

	// Глубина 1 неподвижна окно — firing (минимальная живая глубина).
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.QueueFreeze = 15 * time.Minute
		o.Now = func() time.Time { return now }
	})
	m2 := env2.newQueueFreezeMachine()
	one := []StreamGroupState{{Stream: "q:dns", Group: "g", Depth: 1, LastDeliveredID: "1-1"}}
	if s := m2.transition(env2.Alerts, context.Background(), obs(one)); s.Status != AlertStatusOK {
		t.Fatalf("глубина 1, база: %s", s.Status)
	}
	now = now.Add(16 * time.Minute)
	s := m2.transition(env2.Alerts, context.Background(), obs(one))
	if s.Status != AlertStatusFiring {
		t.Errorf("глубина 1 без движения окно: %s (%s), want firing", s.Status, s.Detail)
	}
	if !strings.Contains(s.Detail, "глубине 1") {
		t.Errorf("деталь заморозки: %q, want «глубине 1»", s.Detail)
	}
}
