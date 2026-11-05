// Юнит-тесты crawlerctl fixtures report (T-244): диф детекции по
// протоколу T-213 (санити «два прогона одной конфигурации — диф ≈0» и
// «искусственно испорченный прогон ловится»), успешность с вычетом
// движко-независимых потерь, темп и время страницы (гистограмма),
// структурные метрики DOM, вердикты против порогов §5.1 п. 4 и честные
// деградации отсутствующих источников. Снапшоты — синтетические
// структуры (формат v1), инфраструктура не нужна.
package main

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

// snapRows — снапшот на n одинаковых строках: rendered+контент и
// Laravel-вердикт по умолчанию (протокол T-213: сравнимые ключи —
// контент записан прогоном и проштампован анализатором этого прогона).
func snapRows(n int, mut func(i int, r *fixturesSnapshotRow)) []fixturesSnapshotRow {
	rules := int32(7)
	out := make([]fixturesSnapshotRow, 0, n)
	for i := 0; i < n; i++ {
		r := fixturesSnapshotRow{
			ZoneID: 1, DomainID: int64(i + 1), SubdomainID: 0, Variant: 0,
			Rendered: true, HasContent: true,
			ContentHash: "aabb", DOMLen: 1000, DOMParsed: true,
			DOMNodes: 100, DOMScripts: 10,
			RulesVersion:        &rules, // штамп анализатора по этому DOM
			LaravelFound:        true,
			IsLaravel:           i%10 == 0,
			Score:               0.7,
			FindingRulesVersion: &rules,
		}
		if mut != nil {
			mut(i, &r)
		}
		out = append(out, r)
	}
	return out
}

// snapOf — снапшот с базовой идентичностью прогонов (label/engine
// различаются — это разные прогоны одной выборки).
func snapOf(label, engine string, rows []fixturesSnapshotRow) *fixturesSnapshot {
	return &fixturesSnapshot{
		FormatVersion: fixturesSnapshotVersion,
		Label:         label,
		Engine:        engine,
		CreatedAt:     time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		Dump:          fixturesSnapshotDump{Seed: 242, Keys: len(rows)},
		Rows:          rows,
	}
}

// verdictOf — вердикт по имени из отчёта (нет — nil).
func verdictOf(t *testing.T, r *fixturesReport, namePart string) *fixturesReportVerdict {
	t.Helper()
	for i := range r.Verdicts {
		if strings.Contains(r.Verdicts[i].Name, namePart) {
			return &r.Verdicts[i]
		}
	}
	return nil
}

// TestFixturesReportDetectionSanity — санити-критерий задачи: два прогона
// одной конфигурации → диф детекции 0, вердикт ПРОЙДЕН.
func TestFixturesReportDetectionSanity(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(300, nil))
	c := snapOf("chromium-2", "chromium", snapRows(300, nil))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if r.DetectionComparable != 300 || r.DetectionDiff != 0 {
		t.Fatalf("детекция: сравнимо %d, расхождений %d — хочу 300/0", r.DetectionComparable, r.DetectionDiff)
	}
	if r.DetectionRatePct != 0 {
		t.Errorf("доля расхождений = %v, хочу 0", r.DetectionRatePct)
	}
	v := verdictOf(t, r, "детекция")
	if v == nil || v.Status != verdictPass {
		t.Fatalf("вердикт детекции = %+v, хочу ПРОЙДЕН", v)
	}
}

// TestFixturesReportDetectionCorrupted — искусственно испорченный прогон:
// вердикты анализатора перевернуты у >1% ключей — отчёт ловит
// расхождение (НЕ ПРОЙДЕН, доля видна в тексте).
func TestFixturesReportDetectionCorrupted(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(300, nil))
	c := snapOf("moli-1", "moli", snapRows(300, func(i int, r *fixturesSnapshotRow) {
		if i < 6 { // 2% сравнимых ключей с противоположным вердиктом
			r.IsLaravel = !r.IsLaravel
		}
	}))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if r.DetectionDiff != 6 || r.DetectionRatePct != 2 {
		t.Fatalf("расхождения = %d (%.2f%%), хочу 6 (2.00%%)", r.DetectionDiff, r.DetectionRatePct)
	}
	v := verdictOf(t, r, "детекция")
	if v == nil || v.Status != verdictFail {
		t.Fatalf("вердикт детекции = %+v, хочу НЕ ПРОЙДЕН", v)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	for _, want := range []string{"2.00%", "6", "НЕ ПРОЙДЕН"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("текст отчёта без %q:\n%s", want, buf.String())
		}
	}
}

// TestFixturesReportDetectionDegradations — деградации дифа детекции:
// анализатор не доработал (строки без штампа), правила менялись;
// и «потеря всех признаков» ловится как расхождение (P1-1).
func TestFixturesReportDetectionDegradations(t *testing.T) {
	t.Run("анализатор не доработал — строка без штампа", func(t *testing.T) {
		b := snapOf("chromium-1", "chromium", snapRows(10, nil))
		c := snapOf("moli-1", "moli", snapRows(10, func(i int, r *fixturesSnapshotRow) {
			r.RulesVersion = nil // рендер прошёл, анализатор по этому DOM не прошёл
		}))
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if r.DetectionComparable != 0 {
			t.Fatalf("сравнимых %d, хочу 0 (без штампа анализатора)", r.DetectionComparable)
		}
		if v := verdictOf(t, r, "детекция"); v == nil || v.Status != verdictNone {
			t.Fatalf("вердикт детекции = %+v, хочу НЕТ ДАННЫХ", v)
		}
	})
	t.Run("finding_rules_version разошёлся", func(t *testing.T) {
		other := int32(9)
		b := snapOf("chromium-1", "chromium", snapRows(10, nil))
		c := snapOf("moli-1", "moli", snapRows(10, func(i int, r *fixturesSnapshotRow) {
			r.FindingRulesVersion = &other
		}))
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "детекция"); v == nil || v.Status != verdictNone {
			t.Fatalf("вердикт детекции = %+v, хочу НЕТ ДАННЫХ (правила менялись)", v)
		}
		if len(r.Warnings) == 0 {
			t.Errorf("нет предупреждения о версиях правил")
		}
	})
	t.Run("потеря всех признаков — расхождение, а не пропуск", func(t *testing.T) {
		b := snapOf("chromium-1", "chromium", snapRows(100, nil))
		c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
			if i%10 == 0 { // все baseline-Laravel строки
				r.LaravelFound = false // анализатор удалил findings: ни одного срабатывания
				r.IsLaravel = false
				r.FindingRulesVersion = nil
			}
		}))
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if r.DetectionComparable != 100 || r.DetectionDiff != 10 {
			t.Fatalf("сравнимых %d, расхождений %d — хочу 100/10 (пропадание сигналов — расхождение)", r.DetectionComparable, r.DetectionDiff)
		}
		if r.DetectionFoundB != 10 || r.DetectionFoundC != 0 {
			t.Fatalf("Laravel-вердиктов %d/%d, хочу 10/0", r.DetectionFoundB, r.DetectionFoundC)
		}
		if v := verdictOf(t, r, "детекция"); v == nil || v.Status != verdictFail {
			t.Fatalf("вердикт детекции = %+v, хочу НЕ ПРОЙДЕН (10%%)", v)
		}
	})
}

// TestFixturesReportSuccess — успешность: доля строк с DOM прогона;
// вердикт с вычетом движко-независимых потерь (DNS/cert из метрик)
// и строгое сравнение без метрик.
func TestFixturesReportSuccess(t *testing.T) {
	base := snapOf("chromium-1", "chromium", snapRows(100, nil))
	// Кандидат: 90 успешных, 10 без DOM.
	cand := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		if i >= 90 {
			r.Rendered, r.HasContent, r.ContentHash, r.DOMLen = false, false, "", 0
		}
	}))

	t.Run("без метрик — строгое сравнение", func(t *testing.T) {
		r, err := fixturesReportBuild(base, cand, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if r.SuccessPctB != 100 || r.SuccessPctC != 90 {
			t.Fatalf("успешность = %v/%v, хочу 100/90", r.SuccessPctB, r.SuccessPctC)
		}
		v := verdictOf(t, r, "успешность")
		if v == nil || v.Status != verdictFail {
			t.Fatalf("вердикт успешности = %+v, хочу НЕ ПРОЙДЕН (строгое сравнение)", v)
		}
	})

	t.Run("с метриками — вычет DNS/cert потерь", func(t *testing.T) {
		b := *base // копия: метрики под-теста не текут в другие
		b.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 1000, AnalyzeLastMs: 21000}
		b.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 1, Tasks: 100, SecondsCount: 100, SecondsSum: 300}
		c := *cand
		c.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 1000, AnalyzeLastMs: 21000}
		c.Metrics = &fixturesSnapshotMetrics{
			ProcessStartSeconds: 2, Tasks: 100, SecondsCount: 100, SecondsSum: 350,
			DNSFinalized: 5, CertFinalized: 6, // 11% движко-независимых потерь
		}
		r, err := fixturesReportBuild(&b, &c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if r.IndepLossPctC == nil || *r.IndepLossPctC != 11 {
			t.Fatalf("потери = %v, хочу 11%%", r.IndepLossPctC)
		}
		v := verdictOf(t, r, "успешность")
		if v == nil || v.Status != verdictPass {
			t.Fatalf("вердикт успешности = %+v, хочу ПРОЙДЕН (90 ≥ 100 − 11)", v)
		}
		if !strings.Contains(v.Detail, "11.00%") {
			t.Errorf("детали вердикта без доли потерь: %s", v.Detail)
		}
	})
}

// TestFixturesReportPace — темп по границам q:analyze и вердикт «не хуже
// базового»; пустые стримы — НЕТ ДАННЫХ.
func TestFixturesReportPace(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(50, nil))
	b.Streams = &fixturesSnapshotStreams{AnalyzeLen: 51, AnalyzeFirstMs: 1000, AnalyzeLastMs: 11000} // 5 стр/с
	c := snapOf("moli-1", "moli", snapRows(50, nil))

	t.Run("кандидат быстрее", func(t *testing.T) {
		c2 := *c                                                                                         // копия структуры, а не указателя: мутации не текут в соседние под-тесты
		c2.Streams = &fixturesSnapshotStreams{AnalyzeLen: 51, AnalyzeFirstMs: 2000, AnalyzeLastMs: 8200} // 8.06 стр/с
		r, err := fixturesReportBuild(b, &c2, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if r.PaceB == nil || *r.PaceB != 5 {
			t.Fatalf("темп baseline = %v, хочу 5", r.PaceB)
		}
		v := verdictOf(t, r, "темп")
		if v == nil || v.Status != verdictPass {
			t.Fatalf("вердикт темпа = %+v, хочу ПРОЙДЕН", v)
		}
	})
	t.Run("кандидат медленнее", func(t *testing.T) {
		c2 := *c
		c2.Streams = &fixturesSnapshotStreams{AnalyzeLen: 51, AnalyzeFirstMs: 1000, AnalyzeLastMs: 26000} // 2 стр/с
		r, err := fixturesReportBuild(b, &c2, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "темп"); v == nil || v.Status != verdictFail {
			t.Fatalf("вердикт темпа = %+v, хочу НЕ ПРОЙДЕН", v)
		}
	})
	t.Run("стримы не сняты", func(t *testing.T) {
		r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "темп"); v == nil || v.Status != verdictNone {
			t.Fatalf("вердикт темпа = %+v, хочу НЕТ ДАННЫХ", v)
		}
	})
}

// TestFixturesHistQuantile — квантиль по корзинам гистограммы:
// интерполяция внутри корзины, плоский участок, отсутствие данных.
func TestFixturesHistQuantile(t *testing.T) {
	buckets := []fixturesHistBucket{
		{0.5, 10}, {1, 20}, {2.5, 50}, {5, 90}, {10, 100},
	}
	if got := fixturesHistQuantile(buckets, 100, 0.5); got == nil || *got != 2.5 {
		t.Errorf("p50 = %v, хочу 2.5", got)
	}
	// p95: ранг 95 — корзина [5,10) с 90..100 → 5 + (95-90)/10*5 = 7.5.
	if got := fixturesHistQuantile(buckets, 100, 0.95); got == nil || *got != 7.5 {
		t.Errorf("p95 = %v, хочу 7.5", got)
	}
	// p10 — ровно на границе первой корзины.
	if got := fixturesHistQuantile(buckets, 100, 0.10); got == nil || *got != 0.5 {
		t.Errorf("p10 = %v, хочу 0.5", got)
	}
	if got := fixturesHistQuantile(nil, 100, 0.95); got != nil {
		t.Errorf("пустая гистограмма = %v, хочу nil", got)
	}
	if got := fixturesHistQuantile(buckets, 0, 0.95); got != nil {
		t.Errorf("нулевая счётность = %v, хочу nil", got)
	}
}

// TestFixturesReportDOM — структурные метрики DOM: медианы отношений,
// доля идентичного DOM, аномальные пары.
func TestFixturesReportDOM(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(100, nil))
	c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		switch {
		case i < 90: // стабильное ядро — тот же DOM
		case i < 95: // чуть другой (отношение в допуске)
			r.ContentHash, r.DOMLen, r.DOMNodes, r.DOMScripts = "ccdd", 1200, 110, 10
		default: // аномальные — втрое короче
			r.ContentHash, r.DOMLen, r.DOMNodes, r.DOMScripts = "eeff", 300, 90, 10
		}
	}))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if r.DOMPairs != 100 || r.DOMIdentical != 90 {
		t.Fatalf("пары/идентичные = %d/%d, хочу 100/90", r.DOMPairs, r.DOMIdentical)
	}
	if r.DOMAnomalousPairs != 5 || r.DOMAnomalousPct != 5 {
		t.Fatalf("аномальные = %d (%.2f%%), хочу 5 (5%%)", r.DOMAnomalousPairs, r.DOMAnomalousPct)
	}
	if r.DOMMedLenRatio == nil || *r.DOMMedLenRatio != 1 {
		t.Fatalf("медиана длины = %v, хочу 1", r.DOMMedLenRatio)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	for _, want := range []string{"×1.00", "90.00%", "идентичный DOM"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("текст отчёта без %q:\n%s", want, buf.String())
		}
	}
}

// TestFixturesReportMemoryAndCrashes — пороги памяти (⅓ бюджета) и
// crashes: операторский вход, прокси browser-класса, деградация.
func TestFixturesReportMemoryAndCrashes(t *testing.T) {
	newSnap := func(label string, rss *int64, crashes *int64, metrics *fixturesSnapshotMetrics) *fixturesSnapshot {
		s := snapOf(label, "moli", snapRows(10, nil))
		s.EngineRSSMax, s.EngineCrashes, s.Metrics = rss, crashes, metrics
		return s
	}
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))

	t.Run("память в допуске", func(t *testing.T) {
		rss := int64(300 << 20)
		r, err := fixturesReportBuild(b, newSnap("moli-1", &rss, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "память"); v == nil || v.Status != verdictPass {
			t.Fatalf("вердикт памяти = %+v, хочу ПРОЙДЕН", v)
		}
	})
	t.Run("память за допуском", func(t *testing.T) {
		rss := int64(800 << 20)
		r, err := fixturesReportBuild(b, newSnap("moli-1", &rss, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "память"); v == nil || v.Status != verdictFail {
			t.Fatalf("вердикт памяти = %+v, хочу НЕ ПРОЙДЕН (800 MiB ≥ ⅓×2 GiB)", v)
		}
	})
	t.Run("память не задана", func(t *testing.T) {
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "память"); v == nil || v.Status != verdictNone {
			t.Fatalf("вердикт памяти = %+v, хочу НЕТ ДАННЫХ", v)
		}
	})
	t.Run("crashes оператором", func(t *testing.T) {
		two := int64(2)
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, &two, nil), 2<<30)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictFail {
			t.Fatalf("вердикт crashes = %+v, хочу НЕ ПРОЙДЕН", v)
		}
	})
	t.Run("crashes по browser-классу", func(t *testing.T) {
		m := &fixturesSnapshotMetrics{ProcessStartSeconds: 5, Tasks: 10, SecondsCount: 10, Errors: map[string]float64{"browser": 3}}
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, nil, m), 2<<30)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictFail {
			t.Fatalf("вердикт crashes = %+v, хочу НЕ ПРОЙДЕН (browser=3)", v)
		}
	})
	t.Run("crashes без источников", func(t *testing.T) {
		r, err := fixturesReportBuild(b, newSnap("moli-1", nil, nil, nil), 2<<30)
		if err != nil {
			t.Fatalf("отчёт: %v", err)
		}
		if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictNone {
			t.Fatalf("вердикт crashes = %+v, хочу НЕТ ДАННЫХ", v)
		}
	})
}

// TestFixturesReportCumulative — воркер не перезапускался между
// снапшотами: счётчики накопительные — предупреждение и деградация
// вердиктов, завязанных на метрики candidate (browser-класс).
func TestFixturesReportCumulative(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 42, Tasks: 10}
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 42, Tasks: 15, Errors: map[string]float64{"browser": 7}}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if !r.Cumulative {
		t.Fatalf("счётчики не распознаны накопительными")
	}
	if len(r.Warnings) == 0 {
		t.Errorf("нет предупреждения о накопительных счётчиках")
	}
	if r.IndepLossPctC != nil {
		t.Errorf("потери по накопительным счётчикам = %v, хочу nil", r.IndepLossPctC)
	}
	if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictNone {
		t.Fatalf("вердикт crashes = %+v, хочу НЕТ ДАННЫХ (накопительные метрики)", v)
	}
}

// TestFixturesReportKeysMismatch — снапшоты по разным выборкам: отчёт
// отказывается сравнивать (ошибка), а не сравнивает яблоки с грушами.
func TestFixturesReportKeysMismatch(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	c := snapOf("moli-1", "moli", snapRows(10, func(i int, r *fixturesSnapshotRow) {
		r.DomainID += 100 // другой дамп
	}))
	if _, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget); err == nil {
		t.Fatalf("отчёт по разным выборкам не отказался")
	}
}

// TestFixturesReportPendingWork — незавершённый прогон: остатки
// q:render/q:render-fallback видны предупреждениями.
func TestFixturesReportPendingWork(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Streams = &fixturesSnapshotStreams{RenderLen: 4, FallbackLen: 2}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	joined := strings.Join(r.Warnings, "\n")
	for _, want := range []string{"q:render-fallback: 2", "q:render"} {
		if !strings.Contains(joined, want) {
			t.Errorf("предупреждения без %q: %s", want, joined)
		}
	}
}

// TestFixturesReportEmptyMetrics — пустые метрики candidate (чужой/
// перезапущенный после прогона воркер): вердикт crashes деградирует,
// предупреждение есть (ревью T-244 P2-1).
func TestFixturesReportEmptyMetrics(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 1, Tasks: 5, SecondsCount: 5, SecondsSum: 10}
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Metrics = &fixturesSnapshotMetrics{ProcessStartSeconds: 2} // Tasks=0, SecondsCount=0
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if !r.MetricsEmptyC || r.MetricsValidC {
		t.Fatalf("пустые метрики не распознаны: empty=%v valid=%v", r.MetricsEmptyC, r.MetricsValidC)
	}
	if r.IndepLossPctC != nil {
		t.Errorf("потери по пустым метрикам = %v, хочу nil", r.IndepLossPctC)
	}
	if v := verdictOf(t, r, "crashes"); v == nil || v.Status != verdictNone {
		t.Fatalf("вердикт crashes = %+v, хочу НЕТ ДАННЫХ (метрики без событий)", v)
	}
	if !strings.Contains(strings.Join(r.Warnings, "\n"), "без событий") {
		t.Errorf("нет предупреждения о пустых метриках")
	}
}

// TestFixturesReportPaceWithoutStreams — время страницы (ср./p95) из
// гистограммы печатается и без темпа: метрики снимались, Redis — нет
// (ревью T-244 P2-2).
func TestFixturesReportPaceWithoutStreams(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 1, Tasks: 10, SecondsSum: 40, SecondsCount: 10,
		Buckets: []fixturesHistBucket{{LE: 2.5, Count: 5}, {LE: 5, Count: 10}},
	}
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	c.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 2, Tasks: 10, SecondsSum: 30, SecondsCount: 10,
		Buckets: []fixturesHistBucket{{LE: 2.5, Count: 8}, {LE: 5, Count: 10}},
	}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if r.PaceB != nil || r.PaceC != nil {
		t.Fatalf("темп без стримов = %v/%v, хочу nil/nil", r.PaceB, r.PaceC)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	text := buf.String()
	if !strings.Contains(text, "темп нет данных") {
		t.Errorf("текст без честной деградации темпа:\n%s", text)
	}
	for _, want := range []string{"ср. 4.00 с", "p95 4.75 с", "ср. 3.00 с"} {
		if !strings.Contains(text, want) {
			t.Errorf("текст без %q (время страницы потерялось без темпа):\n%s", want, text)
		}
	}
}

// TestFixturesReportDOMScriptZero — пары, где один из прогонов не выполнил
// ни одного <script>: в медианы не входят, но видны отдельной метрикой
// (ревью T-244 P2-3).
func TestFixturesReportDOMScriptZero(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(100, nil))
	c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		if i < 3 {
			r.DOMScripts = 0 // кандидат не выполнил ни одного скрипта
		}
	}))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	if r.DOMScriptZeroPairs != 3 {
		t.Fatalf("пар с нулём скриптов %d, хочу 3", r.DOMScriptZeroPairs)
	}
	if r.DOMMedScriptsRatio == nil || *r.DOMMedScriptsRatio != 1 {
		t.Fatalf("медиана скриптов = %v, хочу 1 (нули в медиану не входят)", r.DOMMedScriptsRatio)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	if !strings.Contains(buf.String(), "ни одного <script>: 3") {
		t.Errorf("текст без метрики потерянных скриптов:\n%s", buf.String())
	}
}

// TestFixturesReportTruncatedKeys — снапшот с усеченным набором строк
// (строки дампа исчезли из БД): предупреждение, а не молчаливый
// знаменатель (ревью T-244 P2-4).
func TestFixturesReportTruncatedKeys(t *testing.T) {
	b := snapOf("chromium-1", "chromium", snapRows(10, nil))
	b.Dump.Keys = 12 // дамп насчитывал 12, в БД осталось 10
	c := snapOf("moli-1", "moli", snapRows(10, nil))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	joined := strings.Join(r.Warnings, "\n")
	if !strings.Contains(joined, "10 строк при 12 ключах") {
		t.Errorf("нет предупреждения об усечении выборки: %s", joined)
	}
	if r.Keys != 10 {
		t.Errorf("знаменатель = %d, хочу 10", r.Keys)
	}
}

// TestFixturesReportPairSanity — саниты пары: одинаковые метки и
// baseline не-chromium — предупреждения (ревью T-244 P3-4).
func TestFixturesReportPairSanity(t *testing.T) {
	b := snapOf("run", "moli", snapRows(10, nil))
	c := snapOf("run", "moli", snapRows(10, nil))
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	joined := strings.Join(r.Warnings, "\n")
	if !strings.Contains(joined, "метки снапшотов совпадают") {
		t.Errorf("нет предупреждения о совпадении меток: %s", joined)
	}
	if !strings.Contains(joined, "baseline-chromium") {
		t.Errorf("нет предупреждения о baseline-движке: %s", joined)
	}
}

// TestFixturesHistQuantileSentinelTail — ранг выше кумуляты при
// сентинеле +Inf последней корзиной: последняя конечная граница, не
// MaxFloat64 (ревью T-244 P3-2).
func TestFixturesHistQuantileSentinelTail(t *testing.T) {
	buckets := []fixturesHistBucket{
		{0.5, 10}, {5, 60}, {math.MaxFloat64, 70},
	}
	got := fixturesHistQuantile(buckets, 70, 0.99)
	if got == nil || *got != 5 {
		t.Fatalf("p99 = %v, хочу 5 (не сентинел)", got)
	}
	if got := fixturesHistQuantile([]fixturesHistBucket{{math.MaxFloat64, 5}}, 5, 0.5); got != nil {
		t.Fatalf("единственная корзина-сентинел = %v, хочу nil", got)
	}
}

// TestFixturesReportFullText — готовый отчёт целиком: все разделы §5.1
// п. 3 и все вердикты п. 4 присутствуют в тексте.
func TestFixturesReportFullText(t *testing.T) {
	rssB, rssC := int64(768<<20), int64(96<<20)
	zero := int64(0)
	b := snapOf("chromium-1", "chromium", snapRows(100, nil))
	b.EngineRSSMax = &rssB
	b.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 1000, AnalyzeLastMs: 20000}
	b.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 1, Tasks: 100, SecondsSum: 400, SecondsCount: 100,
		Buckets:         []fixturesHistBucket{{LE: 2.5, Count: 50}, {LE: 5, Count: 95}, {LE: 30, Count: 100}},
		TarpitFinalized: 3,
		TaskFinalized:   map[string]float64{"net::ERR_CONNECTION_REFUSED": 2, "timeout": 1},
	}
	c := snapOf("moli-1", "moli", snapRows(100, func(i int, r *fixturesSnapshotRow) {
		if i >= 98 {
			r.Rendered, r.HasContent, r.ContentHash, r.DOMLen = false, false, "", 0
		}
	}))
	c.EngineRSSMax = &rssC
	c.EngineCrashes = &zero
	c.Streams = &fixturesSnapshotStreams{AnalyzeLen: 100, AnalyzeFirstMs: 500, AnalyzeLastMs: 20500}
	c.Metrics = &fixturesSnapshotMetrics{
		ProcessStartSeconds: 2, Tasks: 100, SecondsSum: 300, SecondsCount: 100,
		Buckets:      []fixturesHistBucket{{LE: 2.5, Count: 70}, {LE: 5, Count: 99}, {LE: 30, Count: 100}},
		DNSFinalized: 1, CertFinalized: 1,
		TaskFinalized:     map[string]float64{"timeout": 2},
		FallbackPublished: map[string]float64{"timeout": 2},
		Errors:            map[string]float64{"task": 4},
	}
	r, err := fixturesReportBuild(b, c, fixturesDefaultMemoryBudget)
	if err != nil {
		t.Fatalf("отчёт: %v", err)
	}
	var buf bytes.Buffer
	fixturesReportPrint(&buf, r)
	text := buf.String()
	for _, want := range []string{
		"Успешность рендеров",
		"Темп и время страницы",
		"Причины финализаций",
		"net::ERR_CONNECTION_REFUSED",
		"Диф детекции Laravel",
		"Структурные метрики DOM",
		"Память RSS движка",
		"Вердикты против порогов §5.1 п.4",
		"детекция Laravel < 1%",
		"успешность",
		"темп не хуже базового",
		"память < ⅓ бюджета chromium",
		"0 crashes движка",
		"публикаций в фолбэк",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("текст отчёта без %q:\n%s", want, text)
		}
	}
}

// TestFixturesReasonList — словарь причин строкой: сортировка по счёту,
// детерминизм при равенстве.
func TestFixturesReasonList(t *testing.T) {
	got := fixturesReasonList(map[string]float64{"timeout": 5, "net::ERR_CONNECTION_REFUSED": 7, "other": 5})
	if got != "net::ERR_CONNECTION_REFUSED: 7, other: 5, timeout: 5" {
		t.Errorf("reasonList = %q", got)
	}
	if got := fixturesReasonList(nil); got != "—" {
		t.Errorf("пустой словарь = %q", got)
	}
}

// TestFixturesFormatBytes — человекочитаемые байты для строк отчёта.
func TestFixturesFormatBytes(t *testing.T) {
	cases := map[int64]string{
		512:                         "512 B",
		2048:                        "2.0 KiB",
		5 << 20:                     "5.0 MiB",
		768 << 20:                   "768.0 MiB",
		fixturesDefaultMemoryBudget: "2.00 GiB",
	}
	for v, want := range cases {
		if got := fixturesFormatBytes(v); got != want {
			t.Errorf("formatBytes(%d) = %q, хочу %q", v, got, want)
		}
	}
}
