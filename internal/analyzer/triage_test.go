// Тесты триажа мутантов CI-прогона 09.11.2026 (половина A): добивки
// analyzer — ошибки счётчиков агрегации (зона/кумулятив истории, включая
// RowsAffected 0), путь findings=0 без перехода (upsert не зовётся),
// границы дренажа unfound (retention=1, метрики события/строк/сбоя),
// границы метрики UnfoundDrain (rows=1), ошибки выборки тика и корзин
// переанализа (нулевые значения на ошибках), якорь lookback и проход
// цикла без загруженных правил (без паники на nil-наборе).
package analyzer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"crawler/internal/content"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// seedUnfoundFixture — фикстура снятия находки переанализом: строка
// laravel_domains есть, доставка разбирает страницу без признаков.
func seedUnfoundFixture(t *testing.T, opts Options) (*anFixture, *anDrainRecorder) {
	t.Helper()
	f := newANFixture(t, anSet(1001), opts)
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.domains[anDomainKey{anKey.zone, anKey.domain}] = &anDomain{
		score: 0.9, foundOnMask: mustMask(t, 1), rulesVersion: 999, updatedAt: time.Now(),
	}
	rec := &anDrainRecorder{j: f.pool.j, stats: content.RetentionStats{Rows: 3, Refs: 2}}
	f.w.opt.DrainDomain = rec.drain
	return f, rec
}

// TestUnfoundDrain_MetricsAndRetentionBoundaries — событие unfound
// бьёт метрику, дренаж с retention=1 выполняется (граница «≤ 0»),
// строки дренажа суммируются метрикой.
func TestUnfoundDrain_MetricsAndRetentionBoundaries(t *testing.T) {
	reg := metrics.New("analyzer")
	f, rec := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: 1})
	f.w.met = NewMetrics(reg)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if rec.count() != 1 {
		t.Fatalf("дренаж с retention=1: вызовов = %d, want 1 (минимальное рабочее окно)", rec.count())
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_laravel_unfound_total", nil); got != 1 {
		t.Errorf("unfound_total = %v, want 1", got)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drained_rows_total", nil); got != 3 {
		t.Errorf("drained_rows = %v, want 3", got)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drain_errors_total", nil); got != 0 {
		t.Errorf("drain_errors = %v, want 0", got)
	}
}

// TestUnfoundDrain_ErrorMetrics — сбой дренажа бьёт метрику ошибки,
// событие и снятие строки зафиксированы.
func TestUnfoundDrain_ErrorMetrics(t *testing.T) {
	reg := metrics.New("analyzer")
	f, _ := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	f.w.met = NewMetrics(reg)
	rec := &anDrainRecorder{j: f.pool.j, err: errors.New("drain boom")}
	f.w.opt.DrainDomain = rec.drain
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v (сбой дренажа не роняет доставку)", err)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drain_errors_total", nil); got != 1 {
		t.Errorf("drain_errors = %v, want 1", got)
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_laravel_unfound_total", nil); got != 1 {
		t.Errorf("unfound_total = %v, want 1", got)
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Error("строка laravel_domains не снята")
	}
}

// TestUnfoundDrain_NotUnfoundNoMetric — без события unfound метрики не
// трогаются (вырезание w.met.Unfound() наблюдаемо).
func TestUnfoundDrain_NotUnfoundNoMetric(t *testing.T) {
	reg := metrics.New("analyzer")
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6, Retention: time.Hour})
	f.w.met = NewMetrics(reg)
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), nil)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"crawler_analyzer_laravel_unfound_total",
		"crawler_analyzer_unfound_drained_rows_total",
		"crawler_analyzer_unfound_drain_errors_total",
	} {
		if got := anMetricValue(t, reg, name, nil); got > 0 {
			t.Errorf("%s = %v, want 0/нет серии (без события)", name, got)
		}
	}
}

// TestMetricsUnfoundDrainRowOne — строка дренажа 1 — валидный инкремент
// (граница rows ≤ 0); ноль на нетронутом реестре НЕ создаёт серию.
func TestMetricsUnfoundDrainRowOne(t *testing.T) {
	reg := metrics.New("analyzer")
	m := NewMetrics(reg)
	m.UnfoundDrain(1)
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drained_rows_total", nil); got != 1 {
		t.Errorf("rows=1: %v, want 1", got)
	}
	m.UnfoundDrain(0)
	m.UnfoundDrain(-3)
	if got := anMetricValue(t, reg, "crawler_analyzer_unfound_drained_rows_total", nil); got != 1 {
		t.Errorf("нулевые/отрицательные меняли значение: %v, want 1", got)
	}
}

// TestAggregateZoneCounterErrors — сбои дельты зоны и кумулятива
// истории (включая RowsAffected 0) роняют доставку с контекстом и
// исходной причиной.
func TestAggregateZoneCounterErrors(t *testing.T) {
	hash := anTestHash(7)
	// Сбой дельты зоны на появлении строки (исходная причина в ошибке —
	// мутант вырезания return даёт только хвост «затронуто 0 строк»).
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f.pool.zoneLaravelErr = errors.New("зона: бум")
	_, err := f.w.ProcessResult(context.Background(), anTask)
	if err == nil || !strings.Contains(err.Error(), "счётчик Laravel-зоны") ||
		!strings.Contains(err.Error(), "зона: бум") {
		t.Errorf("сбой дельты зоны = %v, want контекст счётчика зоны и исходную", err)
	}

	// Строка зоны отсутствует (UPDATE 0) — ошибка обязательности.
	f2 := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f2.store.blocks[hash] = anPageHTML()
	f2.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f2.pool.zoneNoRow = true
	if _, err := f2.w.ProcessResult(context.Background(), anTask); err == nil ||
		!strings.Contains(err.Error(), "затронуто 0 строк") {
		t.Errorf("UPDATE 0 зоны = %v, want «затронуто 0 строк»", err)
	}

	// Сбой кумулятива истории (appeared: свежая уверенная находка).
	f3 := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f3.store.blocks[hash] = anPageHTML()
	f3.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f3.pool.counterDeltaErr = errors.New("кумулятив: бум")
	task := anTask
	task.Fresh = true
	if _, err := f3.w.ProcessResult(context.Background(), task); err == nil ||
		!strings.Contains(err.Error(), "счётчик истории") ||
		!strings.Contains(err.Error(), "кумулятив: бум") {
		t.Errorf("сбой кумулятива = %v, want контекст счётчика истории и исходную", err)
	}

	// Кумулятив без строки (INSERT 0 0) — ошибка обязательности.
	f4 := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f4.store.blocks[hash] = anPageHTML()
	f4.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	f4.pool.counterNoRow = true
	if _, err := f4.w.ProcessResult(context.Background(), task); err == nil ||
		!strings.Contains(err.Error(), "счётчик истории") {
		t.Errorf("кумулятив UPDATE 0 = %v, want ошибка обязательности", err)
	}
}

// TestAggregateLostPathCounterErrors — сбои дельты зоны и кумулятива на
// пути ПОТЕРИ находки (переход без fresh): ошибки роняют доставку.
func TestAggregateLostPathCounterErrors(t *testing.T) {
	// Сбой дельты зоны (−1) при снятии находки.
	f, _ := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	f.pool.zoneLaravelErr = errors.New("зона: бум")
	if _, err := f.w.ProcessResult(context.Background(), anTask); err == nil ||
		!strings.Contains(err.Error(), "зона: бум") {
		t.Errorf("потеря: сбой дельты зоны = %v, want исходную причину", err)
	}
	// Сбой кумулятива lost (свежая потеря находки пишет lost-историю).
	f2, _ := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	f2.pool.counterDeltaErr = errors.New("кумулятив: бум")
	task := anTask
	task.Fresh = true
	if _, err := f2.w.ProcessResult(context.Background(), task); err == nil ||
		!strings.Contains(err.Error(), "кумулятив: бум") {
		t.Errorf("потеря: сбой кумулятива = %v, want исходную причину", err)
	}
}

// TestAggregateRepeatDeliveryNoShift — повторная уверенная доставка при
// существующей строке (смена версии правил — строка переведена в
// устаревшие и доставлена заново): перехода нет — счётчик зоны и
// история не трогаются (мутант вырезания return звал бы дельту).
func TestAggregateRepeatDeliveryNoShift(t *testing.T) {
	hash := anTestHash(7)
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.store.blocks[hash] = anPageHTML()
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "XSRF-TOKEN", "laravel_session"), hash[:])
	// Первая доставка — строка появилась.
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("первая доставка: %v", err)
	}
	if got := f.pool.zoneLaravel[anKey.zone]; got != 1 {
		t.Fatalf("после первой доставки счётчик зоны = %d, want 1", got)
	}
	// Смена версии правил: строка устарела, повторная уверенная доставка
	// доходит до агрегации с existed=true — без дельт и истории.
	f.sets.swap(anSet(1002))
	historyBefore := len(f.pool.history)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("повторная доставка: %v", err)
	}
	if got := f.pool.zoneLaravel[anKey.zone]; got != 1 {
		t.Errorf("повторная доставка сдвинула счётчик зоны: %d, want 1", got)
	}
	if len(f.pool.history) != historyBefore {
		t.Errorf("повторная доставка записала историю: %d → %d", historyBefore, len(f.pool.history))
	}
}

// TestAnalyzeConsumeTailRowsAffected — декремент корзины хвостов с
// нестандартным RowsAffected — ошибка обязательности.
func TestAnalyzeConsumeTailRowsAffected(t *testing.T) {
	f := newANFixture(t, anSet(1001), Options{MinScore: 0.6})
	f.pool.seedResult(anKey, anHeaders(t), anCookies(t, "PHPSESSID"), nil)
	f.pool.consumeRowsAff = 2
	if _, err := f.w.ProcessResult(context.Background(), anTask); err == nil ||
		!strings.Contains(err.Error(), "корзина хвостов переанализа: затронуто 2") {
		t.Errorf("RowsAffected корзины = %v, want «затронуто 2 строк»", err)
	}
}

// TestUnfoundDrain_RetentionZeroNoCall — retention=0 при заданном шве:
// дренаж ВООБЩЕ не вызывается (контент хранится всегда), событие
// отмечается метрикой.
func TestUnfoundDrain_RetentionZeroNoCall(t *testing.T) {
	reg := metrics.New("analyzer")
	f, rec := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: 0})
	f.w.met = NewMetrics(reg)
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if rec.count() != 0 {
		t.Errorf("вызовов дренажа при retention=0 = %d, want 0 (хранение всегда)", rec.count())
	}
	if _, ok := anDomainSnapshot(f.pool, anKey.zone, anKey.domain); ok {
		t.Error("строка laravel_domains не снята")
	}
	if got := anMetricValue(t, reg, "crawler_analyzer_laravel_unfound_total", nil); got != 1 {
		t.Errorf("unfound_total = %v, want 1 (событие зафиксировано)", got)
	}
}

// TestAggregateNoFindingsNoUpsert — findings=0 без перехода (строки не
// было): upsert агрегата не выполняется (мутант вырезания return звал
// бы запись с пустым агрегатом).
func TestAggregateNoFindingsNoUpsert(t *testing.T) {
	f, rec := seedUnfoundFixture(t, Options{MinScore: 0.6, Retention: time.Hour})
	// Перехода нет: строку laravel_domains убираем — DELETE 0.
	delete(f.pool.domains, anDomainKey{anKey.zone, anKey.domain})
	if _, err := f.w.ProcessResult(context.Background(), anTask); err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if rec.count() != 0 {
		t.Errorf("дренаж без перехода = %d, want 0", rec.count())
	}
	for _, ev := range f.pool.j.snapshot() {
		if ev == anEvAggUpsert {
			t.Error("upsert laravel_domains при нулевых findings без перехода")
		}
	}
}

// TestReanalyzeMatureTailsErrors — ошибки guard-запроса: Query/Scan/Err
// и пустой ответ — ошибки с нулевыми значениями (пин нулей на каждой
// ветке); обрыв соединения до первой строки — ошибка итерации, не
// «пустой ответ».
func TestReanalyzeMatureTailsErrors(t *testing.T) {
	// Query-ошибка.
	pool := newReanPool()
	pool.setQueryErr(errors.New("бд недоступна"))
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool); err == nil ||
		tails != 0 || !hour.IsZero() {
		t.Errorf("Query-сбой: (%v/%v, %v), want нули и ошибка", tails, hour, err)
	}
	// Пустой ответ — обрыв протокола, значения нулевые.
	pool2 := newReanPool()
	pool2.guardEmpty = true
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool2); err == nil ||
		tails != 0 || !hour.IsZero() ||
		!strings.Contains(err.Error(), "пустой ответ") {
		t.Errorf("пустой ответ: (%v/%v, %v), want нули и «пустой ответ»", tails, hour, err)
	}
	// Обрыв до первой строки (rows.Err при пустом наборе) — ошибка
	// итерации, НЕ «пустой ответ агрегата».
	pool3 := newReanPool()
	pool3.guardEmpty = true
	pool3.guardRowsErr = errors.New("обрыв соединения")
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool3); err == nil ||
		tails != 0 || !hour.IsZero() || !strings.Contains(err.Error(), "обрыв соединения") {
		t.Errorf("обрыв до строки: (%v/%v, %v), want нули и исходную", tails, hour, err)
	}
	// Scan-ошибка — контекст «чтение», значения нулевые.
	pool4 := newReanPool()
	pool4.guardScanErr = errors.New("скан-бум")
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool4); err == nil ||
		tails != 0 || !hour.IsZero() ||
		!strings.Contains(err.Error(), "чтение") {
		t.Errorf("Scan-сбой: (%v/%v, %v), want нули и контекст чтения", tails, hour, err)
	}
	// rows.Err после строки — исходная ошибка, значения нулевые.
	pool5 := newReanPool()
	pool5.guardRowsErr = errors.New("обрыв")
	if tails, hour, err := reanalyzeMatureTails(context.Background(), pool5); err == nil ||
		tails != 0 || !hour.IsZero() || !strings.Contains(err.Error(), "обрыв") {
		t.Errorf("rows.Err после строки: (%v/%v, %v), want нули и исходную", tails, hour, err)
	}
}

// TestReanalyzeTickSelectErrors — ошибка выборки тика (guard прошёл):
// контекст «выборка тика», Scan/Err строк — с контекстами.
func TestReanalyzeTickSelectErrors(t *testing.T) {
	hour := time.Now().Truncate(time.Hour)
	seed := func() *reanPool {
		pool := newReanPool()
		pool.guardTails = 1000 // guard открыт — выборка выполняется
		pool.guardHour = hour
		pool.seedZone(1, "test")
		pool.seedDomain(1, 1, "a", 1) // статус new — не deleted
		pool.seedRowAt(1, 1, 0, 0, nil, hour.Add(-2*time.Hour))
		return pool
	}
	// Query-ошибка выборки тика.
	pool := seed()
	pool.tickQueryErr = errors.New("бд недоступна")
	if _, err := ReanalyzeTick(context.Background(), pool, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "выборка тика") {
		t.Errorf("Query тика = %v, want контекст «выборка тика»", err)
	}
	// Scan-ошибка строки.
	pool2 := seed()
	pool2.tickScanErr = errors.New("скан-бум")
	if _, err := ReanalyzeTick(context.Background(), pool2, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "чтение") {
		t.Errorf("Scan тика = %v, want контекст чтения", err)
	}
	// rows.Err.
	pool3 := seed()
	pool3.tickRowsErr = errors.New("обрыв")
	if _, err := ReanalyzeTick(context.Background(), pool3, &reanQueue{}, 1, &ReanalyzeCursor{}, ReanalyzeOptions{}); err == nil ||
		!strings.Contains(err.Error(), "обрыв") {
		t.Errorf("rows.Err тика = %v, want исходная", err)
	}
}

// cancelQueue — очередь, отменяющая ctx при первой публикации (отмена
// посреди пачки тика).
type cancelQueue struct {
	reanQueue
	cancel context.CancelFunc
	done   bool
}

func (q *cancelQueue) Publish(ctx context.Context, stream string, msg queue.Message) (string, error) {
	id, err := q.reanQueue.Publish(ctx, stream, msg) // публикация живым ctx
	if !q.done {
		q.done = true
		q.cancel() // отмена ПОСЛЕ успешной публикации — следом цикл
	}
	return id, err
}

// TestReanalyzeTickCancelMidBatch — отмена между пачками: точная
// обёртка «analyzer: переанализ: context canceled» (мутант вырезания
// re-check давал бы «выборка тика: …»), итог — пачка до отмены.
func TestReanalyzeTickCancelMidBatch(t *testing.T) {
	pool := newReanPool()
	pool.guardTails = 1000
	pool.guardHour = time.Now().Truncate(time.Hour)
	pool.seedZone(1, "test")
	base := time.Now().Add(-2 * time.Hour)
	for i := int64(1); i <= 3; i++ {
		pool.seedDomain(1, i, fmt.Sprintf("d%d", i), model.StatusAlive)
		pool.seedRowAt(1, i, 0, model.VariantMain, nil, base.Add(time.Duration(i)*time.Minute))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := &cancelQueue{cancel: cancel}
	res, err := ReanalyzeTick(ctx, pool, q, 1, &ReanalyzeCursor{}, ReanalyzeOptions{BatchSize: 1})
	if err == nil || err.Error() != "analyzer: переанализ: context canceled" {
		t.Fatalf("ошибка = %v, want точная обёртка «analyzer: переанализ: context canceled»", err)
	}
	if res.Selected == 0 || res.Published == 0 {
		t.Errorf("итог = %+v, want хотя бы одна пачка до отмены", res)
	}
}

// TestReanalyzeLookbackPin — якорь окна — сутки.
func TestReanalyzeLookbackPin(t *testing.T) {
	if DefaultReanalyzeLookback != 24*time.Hour {
		t.Errorf("DefaultReanalyzeLookback = %v, want 24h", DefaultReanalyzeLookback)
	}
}

// TestReanalyzeLoopNilRulesNoPanic — цикл без загруженного набора правил
// пропускает стартовый проход без паники (мутант вырезания
// Warn+return разыменовал бы nil-набор) и выходит по отмене; после
// загрузки набора проходы идут.
func TestReanalyzeLoopNilRulesNoPanic(t *testing.T) {
	pool := newReanPool()
	pool.guardTails = 0 // guard закрыт — тяжёлой выборки нет
	q := &reanQueue{}
	sets := &reanSource{set: nil} // набор не загружен
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- ReanalyzeLoop(ctx, pool, q, sets, ReanalyzeOptions{Interval: time.Nanosecond})
	}()
	// Стартовый drain И тики тикера обязаны выполниться с nil-набором
	// без паники (мутант вырезания Warn+return в tick разыменовал бы
	// nil-набор); ждём несколько тиков и отменяем.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReanalyzeLoop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не вышел по отмене")
	}
	if n := q.count(); n != 0 {
		t.Errorf("публикаций без набора правил = %d, want 0", n)
	}
}
