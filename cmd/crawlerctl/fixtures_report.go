// ---------------------------------------------------------------- отчёт
//
// crawlerctl fixtures report (T-244, ТЗ задач выбора движка рендера
// tasks/TZ_render_engines.md §5.1 п. 3–4, §6.1 п. 5): сравнение двух
// снапшотов прогонов (fixtures snapshot) — baseline-chromium и
// движка-кандидата — по метрикам §5.1 п. 3 с явными строками вердикта
// против порогов допуска к канарейке §5.1 п. 4:
//
//	доля успешных рендеров;      причины финализаций (словарь net::ERR_* из
//	                             taskFinalizeReason);
//	темп (стр/с, ср./p95);       память RSS движка (<⅓ бюджета chromium);
//	диф детекции Laravel (<1%, протокол T-213 — вердикты анализатора
//	кандидата против baseline-chromium);
//	структурные метрики DOM (длина, узлы, скрипты) — нормализованное
//	сравнение (медианы отношений), побайтовый диф не применяется: живой
//	DOM недетерминирован;
//	0 crashes ядра движка.
//
// Сравнение честно деградирует: источник, отсутствующий в снапшоте
// (метрики не снимались, analyzer не доработал хвост, RSS движка не
// задан оператором), даёт строку «НЕТ ДАННЫХ» вместо вердикта — отчёт
// не выдумывает значения и не скрывает пробелы. Счётчики метрик валидны
// для одного прогона, только если воркер перезапускался между прогонами
// (момент старта процесса в снапшоте): иначе они накопительные, и
// завязанные на них вердикты деградируют с предупреждением.

package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
)

// fixturesDefaultMemoryBudget — бюджет памяти chromium-воркера для
// порога §5.1 п. 4 «память <⅓ бюджета chromium»: прод-бюджет рендер-контура
// ≈2 ГБ (ТЗ 3.2; docs/deploy.md §2). Флаг --memory-budget переопределяет
// для стенда с другим профилем.
const fixturesDefaultMemoryBudget = 2 << 30

// Статусы строк вердикта.
const (
	verdictPass = "ПРОЙДЕН"
	verdictFail = "НЕ ПРОЙДЕН"
	verdictNone = "НЕТ ДАННЫХ"
)

// fixturesReportVerdict — одна строка вердикта против порога §5.1 п. 4.
type fixturesReportVerdict struct {
	Name   string // формулировка порога
	Status string // verdictPass | verdictFail | verdictNone
	Detail string // числа, по которым вынесен вердикт
}

// fixturesReport — расчёт сравнения двух снапшотов: все метрики §5.1
// п. 3 и вердикты п. 4. Структура (а не текст) — чтобы тесты проверяли
// суть; печать — fixturesReportPrint.
type fixturesReport struct {
	Keys            int
	Seed            int64
	BaselineLabel   string
	CandidateLabel  string
	BaselineEngine  string
	CandidateEngine string
	Warnings        []string

	SuccessRenderedB int // успешные рендеры (rendered + контент) / Tasks
	SuccessRenderedC int
	SuccessPctB      float64
	SuccessPctC      float64
	IndepLossPctC    *float64 // движко-независимые потери candidate: (dns+cert)/задачи, % (nil — без метрик)

	PaceB      *float64 // стр/с по границам q:analyze (nil — нет данных)
	PaceC      *float64
	PaceTasksB int64
	PaceTasksC int64
	PaceSecB   float64
	PaceSecC   float64
	AvgB       *float64 // ср. время страницы по гистограмме, с
	AvgC       *float64
	P95B       *float64 // p95 времени страницы, с
	P95C       *float64

	RSSB   *int64 // макс. RSS движка baseline, байты
	RSSC   *int64
	Budget int64 // бюджет chromium для порога ⅓

	CrashesC    *int64  // падения ядра движка candidate (оператор)
	BrowserErrC float64 // browser-класс ошибок candidate (движок недоступен, прокси)

	DetectionComparable int
	DetectionDiff       int
	DetectionRatePct    float64
	DetectionFoundB     int
	DetectionFoundC     int

	// Причины финализаций — словарь taskFinalizeReason (net::ERR_*),
	// тарпит/cert/DNS-финализации и публикации фолбэка (§5.1 п. 3).
	// nil — метрики прогона не снимались.
	FinalizeB *fixturesSnapshotMetrics
	FinalizeC *fixturesSnapshotMetrics

	// Метрики candidate валидны для одного прогона (воркер перезапускался
	// между снапшотами и события есть); false — не снимались,
	// накопительные или пустые (чужой/перезапущенный после прогона
	// воркер: Tasks и SecondsCount нулевые, ревью T-244 P2-1).
	MetricsValidC bool
	MetricsEmptyC bool
	Cumulative    bool

	DOMPairs           int
	DOMIdentical       int
	DOMIdenticalPct    float64
	DOMMedLenRatio     *float64 // медиана отношений candidate/baseline
	DOMMedNodesRatio   *float64
	DOMMedScriptsRatio *float64
	DOMAnomalousPairs  int
	DOMAnomalousPct    float64
	DOMScriptZeroPairs int // пары, где один из прогонов не выполнил ни одного <script>

	Verdicts []fixturesReportVerdict
	Fail     bool // есть хоть один НЕ ПРОЙДЕН
}

// ---------------------------------------------------------------- расчёты

// fixturesSuccessCount — успешные рендеры прогона: rendered + контентная
// ссылка (после наката T-243 ссылка появляется только у DOM, записанного
// прогоном).
func fixturesSuccessCount(snap *fixturesSnapshot) int {
	n := 0
	for _, r := range snap.Rows {
		if r.Rendered && r.HasContent {
			n++
		}
	}
	return n
}

// fixturesPace — темп прогона (стр/с) по стриму q:analyze: записи
// публикуются воркером по ходу обработки, (N−1) интервалов между первой
// и последней дают средний темп dorаботки потока. nil — данных нет
// (стрим не снимался или в нём меньше двух записей).
func fixturesPace(s *fixturesSnapshotStreams) (pace *float64, tasks int64, sec float64) {
	if s == nil || s.AnalyzeLen < 2 || s.AnalyzeLastMs <= s.AnalyzeFirstMs {
		return nil, 0, 0
	}
	sec = float64(s.AnalyzeLastMs-s.AnalyzeFirstMs) / 1000
	v := float64(s.AnalyzeLen-1) / sec
	return &v, s.AnalyzeLen, sec
}

// fixturesHistQuantile — квантиль по кумулятивным корзинам гистограммы
// (интерполяция как у Prometheus histogram_quantile). nil — гистограммы
// или квантиля нет. Замыкающая корзина (сентинел MaxFloat64 вместо +Inf):
// интерполяции в бесконечность нет — возвращается последняя конечная
// граница (нижняя оценка хвоста за timeout).
func fixturesHistQuantile(buckets []fixturesHistBucket, count float64, q float64) *float64 {
	if count <= 0 || len(buckets) == 0 {
		return nil
	}
	rank := q * count
	var prevLE, prevCount float64 // нижняя граница первой корзины — 0
	for i, b := range buckets {
		if b.Count < rank {
			prevLE, prevCount = b.LE, b.Count
			continue
		}
		if b.Count == prevCount {
			v := b.LE // плоский участок: все наблюдения на границе
			return &v
		}
		if b.LE >= math.MaxFloat64 {
			if prevCount == 0 {
				return nil // единственная корзина — сентинел: конечных границ нет
			}
			v := prevLE // ранг в замыкающей корзине: нижняя оценка хвоста
			return &v
		}
		v := prevLE + (rank-prevCount)/(b.Count-prevCount)*(b.LE-prevLE)
		if i == 0 {
			v = b.LE * rank / b.Count // от нуля до первой границы
		}
		return &v
	}
	last := buckets[len(buckets)-1].LE
	if last >= math.MaxFloat64 {
		// сентинел +Inf как последняя граница (ранг выше кумуляты битого
		// экспоза): честнее последняя конечная граница, чем 1.8e308 с
		// (ревью T-244 P3-2)
		if len(buckets) > 1 {
			last = buckets[len(buckets)-2].LE
		} else {
			return nil
		}
	}
	return &last
}

// fixturesHistAvg — среднее по sum/count гистограммы.
func fixturesHistAvg(sum, count float64) *float64 {
	if count <= 0 {
		return nil
	}
	v := sum / count
	return &v
}

// fixturesMedian — медиана среза (срез не мутирует: копия).
func fixturesMedian(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	var v float64
	if len(sorted)%2 == 1 {
		v = sorted[mid]
	} else {
		v = (sorted[mid-1] + sorted[mid]) / 2
	}
	return &v
}

// fixturesRowMap — строки снапшота по ключу.
func fixturesRowMap(snap *fixturesSnapshot) map[fixturesKey]fixturesSnapshotRow {
	m := make(map[fixturesKey]fixturesSnapshotRow, len(snap.Rows))
	for _, r := range snap.Rows {
		m[fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant}] = r
	}
	return m
}

// fixturesKeysUnionMismatch — несовпадение множеств ключей двух снапшотов
// (сняты по разным дампам): отчёт требует одну выборку — возвращаются
// чужие ключи baseline и candidate.
func fixturesKeysUnionMismatch(b, c map[fixturesKey]fixturesSnapshotRow) (onlyB, onlyC int) {
	for k := range b {
		if _, ok := c[k]; !ok {
			onlyB++
		}
	}
	for k := range c {
		if _, ok := b[k]; !ok {
			onlyC++
		}
	}
	return onlyB, onlyC
}

// fixturesDetection — диф детекции Laravel (протокол T-213): вердикты
// анализатора кандидата против baseline построчно. Сравнимы строки,
// записанные прогоном в обоих случаях (контент есть) и проштампованные
// анализатором этого прогона (rules_version строки NOT NULL: рендер
// обнуляет штамп, анализатор ставит — NULL означает «анализатор не
// доработал», findings в БД могли остаться от прошлого прогона, ревью
// T-244 P1-2). Вердикт строки — is_laravel при наличии findings и false
// при их отсутствии: анализатор УДАЛЯЕТ строку findings при нуле
// срабатываний (findingDeleteSQL) — отсутствие строки это штатное
// «признаков нет», а не «не доработал»; иначе полная потеря/появление
// сигналов выпадала бы из дифа (ревью T-244 P1-1). Версии правил
// находок возвращаются для сверки: разные наборы правил делают порог
// <1% несравнимым.
func fixturesDetection(b, c map[fixturesKey]fixturesSnapshotRow) (comparable, diff, foundB, foundC int, rulesB, rulesC map[int32]int) {
	rulesB, rulesC = map[int32]int{}, map[int32]int{}
	for k, rb := range b {
		rc, ok := c[k]
		if !ok {
			continue
		}
		if !rb.HasContent || !rc.HasContent || rb.RulesVersion == nil || rc.RulesVersion == nil {
			continue // не записана прогоном или не проанализирована в нём
		}
		comparable++
		if rb.FindingRulesVersion != nil {
			rulesB[*rb.FindingRulesVersion]++
		}
		if rc.FindingRulesVersion != nil {
			rulesC[*rc.FindingRulesVersion]++
		}
		if rb.LaravelFound && rb.IsLaravel {
			foundB++
		}
		if rc.LaravelFound && rc.IsLaravel {
			foundC++
		}
		if rb.LaravelFound != rc.LaravelFound || rb.IsLaravel != rc.IsLaravel {
			diff++
		}
	}
	return comparable, diff, foundB, foundC, rulesB, rulesC
}

// fixturesDOMCompare — структурные метрики DOM: пары ключей с контентом
// в обоих прогонах; идентичный DOM (content_hash — дедуп хранилища),
// медианы отношений candidate/baseline (длина/узлы/скрипты — считать
// структуру, не байты: живой DOM недетерминирован) и доля аномальных пар
// по длине (вне ×0.5…×2 — движок собрал принципиально другую страницу);
// плюс пары, где один из прогонов не выполнил НИ ОДНОГО скрипта
// (в медианы они не входят — отношение к нулю бессмысленно, но потеря
// всех <script> это самостоятельный сигнал, ревью T-244 P2-3).
func fixturesDOMCompare(b, c map[fixturesKey]fixturesSnapshotRow) (pairs, identical, anomalous, scriptZero int, medLen, medNodes, medScripts *float64) {
	var lenRatios, nodeRatios, scriptRatios []float64
	for k, rb := range b {
		rc, ok := c[k]
		if !ok || !rb.HasContent || !rc.HasContent {
			continue
		}
		pairs++
		if rb.ContentHash != "" && rb.ContentHash == rc.ContentHash {
			identical++
		}
		if rb.DOMLen > 0 && rc.DOMLen > 0 {
			lenRatios = append(lenRatios, float64(rc.DOMLen)/float64(rb.DOMLen))
			if r := float64(rc.DOMLen) / float64(rb.DOMLen); r < 0.5 || r > 2 {
				anomalous++
			}
		}
		if rb.DOMParsed && rc.DOMParsed {
			if rb.DOMNodes > 0 && rc.DOMNodes > 0 {
				nodeRatios = append(nodeRatios, float64(rc.DOMNodes)/float64(rb.DOMNodes))
			}
			switch {
			case rb.DOMScripts > 0 && rc.DOMScripts > 0:
				scriptRatios = append(scriptRatios, float64(rc.DOMScripts)/float64(rb.DOMScripts))
			case rb.DOMScripts == 0 && rc.DOMScripts == 0:
				// без скриптов в обоих — норма для статических страниц
			default:
				scriptZero++
			}
		}
	}
	return pairs, identical, anomalous, scriptZero,
		fixturesMedian(lenRatios), fixturesMedian(nodeRatios), fixturesMedian(scriptRatios)
}

// fixturesMetricsCumulative — накопительные ли счётчики пары снапшотов:
// метрики снимались в обоих, но процесс воркера один (старт не менялся) —
// значения candidate включают прогон baseline.
func fixturesMetricsCumulative(b, c *fixturesSnapshot) bool {
	return b.Metrics != nil && c.Metrics != nil &&
		b.Metrics.ProcessStartSeconds != 0 &&
		b.Metrics.ProcessStartSeconds == c.Metrics.ProcessStartSeconds
}

// fixturesFinalizedSum — суммарные финализации класса задачи (по словарю
// причин) — для строк причин.
func fixturesFinalizedSum(m map[string]float64) float64 {
	var sum float64
	for _, v := range m {
		sum += v
	}
	return sum
}

// fixturesReportBuild — расчёт сравнения: метрики §5.1 п. 3 + вердикты
// п. 4. Ошибка — снапшоты по разным выборкам (дальше сравнивать
// бессмысленно).
func fixturesReportBuild(b, c *fixturesSnapshot, budget int64) (*fixturesReport, error) {
	rowsB, rowsC := fixturesRowMap(b), fixturesRowMap(c)
	if onlyB, onlyC := fixturesKeysUnionMismatch(rowsB, rowsC); onlyB+onlyC > 0 {
		return nil, fmt.Errorf("снапшоты сняты по разным выборкам: %d ключей только в baseline, %d только в candidate (дампы/страты расходятся — сравнение недействительно)", onlyB, onlyC)
	}

	r := &fixturesReport{
		Keys:            len(b.Rows),
		Seed:            b.Dump.Seed,
		BaselineLabel:   b.Label,
		CandidateLabel:  c.Label,
		BaselineEngine:  b.Engine,
		CandidateEngine: c.Engine,
		Budget:          budget,
	}
	if b.Dump.Seed != c.Dump.Seed {
		r.Warnings = append(r.Warnings, fmt.Sprintf("seed дампов расходится: baseline %d, candidate %d — выборки могли собраться из разных состояний БД", b.Dump.Seed, c.Dump.Seed))
	}
	// Сверки выборки: снапшот с усечённым набором строк (строки дампа
	// исчезли из БД) занижал бы знаменатель успешности незаметно (P2-4).
	for _, s := range []struct {
		name string
		snap *fixturesSnapshot
	}{
		{"baseline", b}, {"candidate", c},
	} {
		if s.snap.Dump.Keys > 0 && s.snap.Dump.Keys != len(s.snap.Rows) {
			r.Warnings = append(r.Warnings, fmt.Sprintf("%s: в снапшоте %d строк при %d ключах дампа — знаменатель успешности усечён", s.name, len(s.snap.Rows), s.snap.Dump.Keys))
		}
	}
	if b.Label == c.Label {
		r.Warnings = append(r.Warnings, "метки снапшотов совпадают — убедитесь, что это разные прогоны, а не один файл")
	}
	if b.Engine != "chromium" {
		r.Warnings = append(r.Warnings, fmt.Sprintf("baseline-движок %q — ТЗ §5.1 требует baseline-chromium (свежий прогон chromium на той же выборке)", b.Engine))
	}
	cumulative := fixturesMetricsCumulative(b, c)
	if cumulative {
		r.Warnings = append(r.Warnings, "воркер не перезапускался между прогонами (process_start_time совпадает): счётчики candidate накопительные — причины финализаций, время страницы и browser-ошибки включают baseline-прогон, вердикты по ним деградированы; темп по стримам q:analyze и успешность по строкам БД валидны")
	}
	// Незавершённость прогонов видна по остаткам стримов; проверяются ОБА
	// прогона: неполный baseline ослаблял бы порог успешности для
	// кандидата (P2-5); q:dead молча уменьшает успешность любого прогона.
	for _, s := range []struct {
		name string
		snap *fixturesSnapshot
	}{
		{"baseline", b}, {"candidate", c},
	} {
		if s.snap.Streams == nil {
			continue
		}
		if s.snap.Streams.RenderLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("q:render %s не пуста (%d записей) — прогон не завершён (или Trim не дошёл), темп и успешность отражают недоработанный прогон", s.name, s.snap.Streams.RenderLen))
		}
		if s.snap.Streams.FallbackLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("фолбэк %s не доработан (q:render-fallback: %d записей) — успешность занижена, снимайте снапшот после доработки", s.name, s.snap.Streams.FallbackLen))
		}
		if s.snap.Streams.DeadLen > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("q:dead %s: %d записей — задачи исчерпали доставки, успешность и причины стоит разобрать", s.name, s.snap.Streams.DeadLen))
		}
	}

	// Успешность: строки с DOM прогона / задачи выборки.
	r.SuccessRenderedB = fixturesSuccessCount(b)
	r.SuccessRenderedC = fixturesSuccessCount(c)
	r.SuccessPctB = pct(r.SuccessRenderedB, r.Keys)
	r.SuccessPctC = pct(r.SuccessRenderedC, r.Keys)
	if c.Metrics != nil && !cumulative && c.Metrics.Tasks > 0 {
		v := (c.Metrics.DNSFinalized + c.Metrics.CertFinalized) / c.Metrics.Tasks * 100
		r.IndepLossPctC = &v
	}

	// Темп и время страницы.
	r.PaceB, r.PaceTasksB, r.PaceSecB = fixturesPace(b.Streams)
	r.PaceC, r.PaceTasksC, r.PaceSecC = fixturesPace(c.Streams)
	if b.Metrics != nil {
		r.AvgB = fixturesHistAvg(b.Metrics.SecondsSum, b.Metrics.SecondsCount)
		r.P95B = fixturesHistQuantile(b.Metrics.Buckets, b.Metrics.SecondsCount, 0.95)
	}
	if c.Metrics != nil {
		r.AvgC = fixturesHistAvg(c.Metrics.SecondsSum, c.Metrics.SecondsCount)
		r.P95C = fixturesHistQuantile(c.Metrics.Buckets, c.Metrics.SecondsCount, 0.95)
	}

	// Память и краши.
	r.RSSB, r.RSSC = b.EngineRSSMax, c.EngineRSSMax
	r.CrashesC = c.EngineCrashes
	r.FinalizeB, r.FinalizeC = b.Metrics, c.Metrics
	r.Cumulative = cumulative
	r.MetricsEmptyC = c.Metrics != nil && !cumulative &&
		c.Metrics.Tasks == 0 && c.Metrics.SecondsCount == 0
	r.MetricsValidC = c.Metrics != nil && !cumulative && !r.MetricsEmptyC
	if r.MetricsEmptyC {
		r.Warnings = append(r.Warnings, "метрики candidate без событий (задач и наблюдений времени 0): --metrics указывает на пустой/чужой воркер или воркер перезапущен после прогона — вердикты по метрикам деградированы")
	}
	if r.MetricsValidC {
		r.BrowserErrC = c.Metrics.Errors["browser"]
		if fb := fixturesFinalizedSum(c.Metrics.FallbackPublished); fb > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("публикаций в фолбэк за прогон candidate: %.0f — при доработанном воркером B они уже входят в успешность (снимайте снапшот после дренажа q:render-fallback); без B вердикт успешности — «движок без фолбэка», строже порога ТЗ §5.1 п. 4", fb))
		}
	}

	// Диф детекции.
	comparable, diff, foundB, foundC, rulesB, rulesC := fixturesDetection(rowsB, rowsC)
	r.DetectionComparable = comparable
	r.DetectionDiff = diff
	r.DetectionFoundB = foundB
	r.DetectionFoundC = foundC
	r.DetectionRatePct = pct(diff, comparable)
	rulesDiffer := len(rulesB) != len(rulesC)
	if !rulesDiffer {
		for v := range rulesB {
			if _, ok := rulesC[v]; !ok {
				rulesDiffer = true
				break
			}
		}
	}
	if rulesDiffer {
		r.Warnings = append(r.Warnings, "версии правил анализатора в прогонах расходятся (rules_version) — порог детекции <1% сравнивает разные наборы правил, вердикт деградирован")
	}

	// Структурные метрики DOM.
	r.DOMPairs, r.DOMIdentical, r.DOMAnomalousPairs, r.DOMScriptZeroPairs, r.DOMMedLenRatio, r.DOMMedNodesRatio, r.DOMMedScriptsRatio =
		fixturesDOMCompare(rowsB, rowsC)
	r.DOMIdenticalPct = pct(r.DOMIdentical, r.DOMPairs)
	r.DOMAnomalousPct = pct(r.DOMAnomalousPairs, r.DOMPairs)

	r.Verdicts = fixturesReportVerdicts(r, rulesDiffer)
	for _, v := range r.Verdicts {
		if v.Status == verdictFail {
			r.Fail = true
		}
	}
	return r, nil
}

// fixturesReportVerdicts — строки вердикта против порогов §5.1 п. 4.
// Порядок — по тексту ТЗ: детекция, успешность, темп, память, crashes.
func fixturesReportVerdicts(r *fixturesReport, rulesDiffer bool) []fixturesReportVerdict {
	var out []fixturesReportVerdict

	// Детекция Laravel < 1% (T-213).
	switch {
	case r.DetectionComparable == 0:
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictNone, Detail: "нет сравнимых строк (прогон не записал DOM или анализатор не проштамповал results.rules_version — дождитесь доработки analyzer)"})
	case rulesDiffer:
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictNone, Detail: fmt.Sprintf("правила анализатора менялись между прогонами; расхождений %s (%d из %d)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	case r.DetectionRatePct < 1:
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictPass, Detail: fmt.Sprintf("%s расхождений (%d из %d сравнимых)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	default:
		out = append(out, fixturesReportVerdict{Name: "детекция Laravel < 1%",
			Status: verdictFail, Detail: fmt.Sprintf("%s расхождений (%d из %d сравнимых)", pctf(r.DetectionRatePct), r.DetectionDiff, r.DetectionComparable)})
	}

	// Успешность: candidate ≥ baseline − движко-независимые потери (DNS/cert).
	switch {
	case r.IndepLossPctC != nil && r.SuccessPctC >= r.SuccessPctB-*r.IndepLossPctC:
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline − движко-независимые потери",
			Status: verdictPass, Detail: fmt.Sprintf("%s ≥ %s − %s", pctf(r.SuccessPctC), pctf(r.SuccessPctB), pctf(*r.IndepLossPctC))})
	case r.IndepLossPctC != nil:
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline − движко-независимые потери",
			Status: verdictFail, Detail: fmt.Sprintf("%s < %s − %s (DNS/cert)", pctf(r.SuccessPctC), pctf(r.SuccessPctB), pctf(*r.IndepLossPctC))})
	case r.SuccessPctC >= r.SuccessPctB:
		// Потери неизвестны (метрик нет) — строгое сравнение: завышает
		// требования к кандидату, но не пропускает регресс.
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline (строго: потери DNS/cert неизвестны)",
			Status: verdictPass, Detail: fmt.Sprintf("%s ≥ %s", pctf(r.SuccessPctC), pctf(r.SuccessPctB))})
	default:
		out = append(out, fixturesReportVerdict{Name: "успешность ≥ baseline (строго: потери DNS/cert неизвестны)",
			Status: verdictFail, Detail: fmt.Sprintf("%s < %s; снимите --metrics прогона для вычета движко-независимых потерь", pctf(r.SuccessPctC), pctf(r.SuccessPctB))})
	}

	// Темп не хуже базового.
	switch {
	case r.PaceB == nil || r.PaceC == nil:
		out = append(out, fixturesReportVerdict{Name: "темп не хуже базового",
			Status: verdictNone, Detail: "темп не снят (Redis недоступен при снапшоте или q:analyze < 2 записей)"})
	case *r.PaceC >= *r.PaceB:
		out = append(out, fixturesReportVerdict{Name: "темп не хуже базового",
			Status: verdictPass, Detail: fmt.Sprintf("%.2f ≥ %.2f стр/с", *r.PaceC, *r.PaceB)})
	default:
		out = append(out, fixturesReportVerdict{Name: "темп не хуже базового",
			Status: verdictFail, Detail: fmt.Sprintf("%.2f < %.2f стр/с", *r.PaceC, *r.PaceB)})
	}

	// Память < ⅓ бюджета chromium.
	switch {
	case r.RSSC == nil:
		out = append(out, fixturesReportVerdict{Name: "память < ⅓ бюджета chromium",
			Status: verdictNone, Detail: "RSS движка не задан (--engine-rss-max при снапшоте кандидата)"})
	case *r.RSSC*3 < r.Budget:
		out = append(out, fixturesReportVerdict{Name: "память < ⅓ бюджета chromium",
			Status: verdictPass, Detail: fmt.Sprintf("%s < ⅓ от %s", fixturesFormatBytes(*r.RSSC), fixturesFormatBytes(r.Budget))})
	default:
		out = append(out, fixturesReportVerdict{Name: "память < ⅓ бюджета chromium",
			Status: verdictFail, Detail: fmt.Sprintf("%s ≥ ⅓ от %s", fixturesFormatBytes(*r.RSSC), fixturesFormatBytes(r.Budget))})
	}

	// 0 crashes ядра движка.
	switch {
	case r.CrashesC != nil:
		status, word := verdictPass, "0 падений ядра движка"
		if *r.CrashesC > 0 {
			status, word = verdictFail, fmt.Sprintf("%d падений ядра движка", *r.CrashesC)
		}
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: status, Detail: word + " (операторский вход --engine-crashes)"})
	case r.BrowserErrC > 0:
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: verdictFail, Detail: fmt.Sprintf("%.0f событий browser-класса (движок недоступен) — прокси падений/недоступности ядра", r.BrowserErrC)})
	case r.MetricsValidC:
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: verdictPass, Detail: "0 событий browser-класса по метрикам этапа (прокси; точный вход --engine-crashes)"})
	default:
		out = append(out, fixturesReportVerdict{Name: "0 crashes движка",
			Status: verdictNone, Detail: "нет ни операторского входа (--engine-crashes), ни валидных метрик этапа"})
	}
	return out
}

// ---------------------------------------------------------------- печать

// pctf — проценты строкой (2 знака).
func pctf(v float64) string { return fmt.Sprintf("%.2f%%", v) }

// fixturesFormatBytes — байты человекочитаемо (KiB/MiB/GiB).
func fixturesFormatBytes(v int64) string {
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(v)/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(v)/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(v)/(1<<10))
	default:
		return fmt.Sprintf("%d B", v)
	}
}

// ratioOrNone — отношение строкой «×N.NN» или прочерк.
func ratioOrNone(v *float64) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("×%.2f", *v)
}

// fixturesReportPrint — текст отчёта: метрики §5.1 п. 3 и вердикты п. 4
// (возвращается тем же текстом в --out).
func fixturesReportPrint(w io.Writer, r *fixturesReport) {
	out(w, "Отчёт сравнения движков рендера (ТЗ рендера §5.1; T-244)\n")
	out(w, "Выборка: %d задач фикстуры (дамп seed %d)\n", r.Keys, r.Seed)
	out(w, "baseline:  %s (движок %s)\n", r.BaselineLabel, r.BaselineEngine)
	out(w, "candidate: %s (движок %s)\n", r.CandidateLabel, r.CandidateEngine)
	for _, warn := range r.Warnings {
		out(w, "предупреждение: %s\n", warn)
	}
	out(w, "\n")

	out(w, "Успешность рендеров\n")
	out(w, "  baseline:  %d/%d (%s)", r.SuccessRenderedB, r.Keys, pctf(r.SuccessPctB))
	if r.FinalizeB != nil && r.FinalizeB.RenderedRecords > 0 {
		out(w, " [по метрикам этапа: rendered-записей %.0f]", r.FinalizeB.RenderedRecords)
	}
	out(w, "\n")
	out(w, "  candidate: %d/%d (%s)", r.SuccessRenderedC, r.Keys, pctf(r.SuccessPctC))
	if r.FinalizeC != nil && r.FinalizeC.RenderedRecords > 0 {
		out(w, " [по метрикам этапа: rendered-записей %.0f]", r.FinalizeC.RenderedRecords)
	}
	if r.IndepLossPctC != nil {
		out(w, "; движко-независимые потери (DNS/cert): %s", pctf(*r.IndepLossPctC))
	}
	out(w, "\n\n")

	out(w, "Темп и время страницы\n")
	paceLine := func(name string, pace *float64, tasks int64, sec float64, avg, p95 *float64) {
		if pace != nil {
			out(w, "  %s: %.2f стр/с (%d записей q:analyze за %.0f с)", name, *pace, tasks, sec)
		} else {
			out(w, "  %s: темп нет данных (стримы q:analyze не сняты или < 2 записей)", name)
		}
		// Время страницы — самостоятельная метрика §5.1 п. 3 (гистограмма
		// этапа): печатается и без темпа, метрики могли сниматься без Redis
		// (ревью T-244 P2-2).
		if avg != nil {
			out(w, "; ср. %.2f с", *avg)
		}
		if p95 != nil {
			out(w, "; p95 %.2f с", *p95)
		}
		out(w, "\n")
	}
	paceLine("baseline", r.PaceB, r.PaceTasksB, r.PaceSecB, r.AvgB, r.P95B)
	paceLine("candidate", r.PaceC, r.PaceTasksC, r.PaceSecC, r.AvgC, r.P95C)
	out(w, "\n")

	out(w, "Причины финализаций без рендера (метрики этапа, словарь taskFinalizeReason)\n")
	finalizeLine := func(name string, m *fixturesSnapshotMetrics) {
		if m == nil {
			out(w, "  %s: нет данных (снапшот без --metrics)\n", name)
			return
		}
		out(w, "  %s: тарпит %.0f, cert %.0f, dns %.0f, класс задачи %.0f",
			name, m.TarpitFinalized, m.CertFinalized, m.DNSFinalized, fixturesFinalizedSum(m.TaskFinalized))
		if len(m.TaskFinalized) > 0 {
			out(w, " (%s)", fixturesReasonList(m.TaskFinalized))
		}
		if fb := fixturesFinalizedSum(m.FallbackPublished); fb > 0 {
			out(w, "; публикаций в фолбэк %.0f (%s)", fb, fixturesReasonList(m.FallbackPublished))
		}
		out(w, "\n")
	}
	finalizeLine("baseline", r.FinalizeB)
	finalizeLine("candidate", r.FinalizeC)
	if r.Cumulative {
		out(w, "  (воркер не перезапускался: счётчики candidate включают baseline-прогон)\n")
	}
	out(w, "\n")

	out(w, "Диф детекции Laravel (протокол T-213)\n")
	out(w, "  сравнимые вердикты: %d из %d; Laravel-вердиктов baseline %d, candidate %d\n",
		r.DetectionComparable, r.Keys, r.DetectionFoundB, r.DetectionFoundC)
	out(w, "  расхождения вердиктов: %d (%s)\n\n", r.DetectionDiff, pctf(r.DetectionRatePct))

	out(w, "Структурные метрики DOM (нормализованное сравнение)\n")
	out(w, "  пары с DOM обоих прогонов: %d; идентичный DOM (content_hash): %d (%s)\n",
		r.DOMPairs, r.DOMIdentical, pctf(r.DOMIdenticalPct))
	out(w, "  медианы отношений candidate/baseline: длина %s, узлы %s, скрипты %s\n",
		ratioOrNone(r.DOMMedLenRatio), ratioOrNone(r.DOMMedNodesRatio), ratioOrNone(r.DOMMedScriptsRatio))
	out(w, "  аномальные пары по длине (вне ×0.5…×2): %d (%s)", r.DOMAnomalousPairs, pctf(r.DOMAnomalousPct))
	if r.DOMScriptZeroPairs > 0 {
		out(w, "; пар, где один из прогонов не выполнил ни одного <script>: %d", r.DOMScriptZeroPairs)
	}
	out(w, "\n\n")

	out(w, "Память RSS движка\n")
	out(w, "  baseline: %s; candidate: %s; бюджет chromium %s (⅓ = %s)\n\n",
		rssOrNone(r.RSSB), rssOrNone(r.RSSC), fixturesFormatBytes(r.Budget), fixturesFormatBytes(r.Budget/3))

	out(w, "Вердикты против порогов §5.1 п.4\n")
	for _, v := range r.Verdicts {
		out(w, "  [%s] %s: %s\n", v.Status, v.Name, v.Detail)
	}
}

// rssOrNone — RSS строкой или «нет данных».
func rssOrNone(v *int64) string {
	if v == nil {
		return "нет данных"
	}
	return fixturesFormatBytes(*v)
}

// fixturesReasonList — словарь «причина → счёт» строкой (по убыванию
// счёта, причина = ключ): причины финализаций/фолбэка.
func fixturesReasonList(m map[string]float64) string {
	if len(m) == 0 {
		return "—"
	}
	type pair struct {
		reason string
		n      float64
	}
	pairs := make([]pair, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, pair{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].n != pairs[j].n {
			return pairs[i].n > pairs[j].n
		}
		return pairs[i].reason < pairs[j].reason
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = fmt.Sprintf("%s: %.0f", p.reason, p.n)
	}
	return strings.Join(parts, ", ")
}

// ---------------------------------------------------------------- команда

// fixturesReportCmd — crawlerctl fixtures report: чтение двух снапшотов,
// расчёт, печать (и запись --out тем же текстом). Код возврата не зависит
// от вердиктов (отчёт — не гейт); --fail-exit переводит любой НЕ ПРОЙДЕН
// в код 1 для скриптов цикла T-247.
func fixturesReportCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures report")
	baseline := fs.String("baseline", "", "снапшот baseline-chromium (обязателен)")
	candidate := fs.String("candidate", "", "снапшот движка-кандидата (обязателен)")
	outPath := fs.String("out", "", "файл отчёта (кроме stdout; для протокола T-247)")
	budget := fs.Int64("memory-budget", fixturesDefaultMemoryBudget, "бюджет памяти chromium для порога ⅓, байты")
	failExit := fs.Bool("fail-exit", false, "код 1 при любом вердикте НЕ ПРОЙДЕН (для скриптов)")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*baseline) == "" || strings.TrimSpace(*candidate) == "" {
		return usagef("укажите оба снапшота: crawlerctl fixtures report --baseline A.snapshot.json --candidate B.snapshot.json")
	}
	if *budget <= 0 {
		return usagef("fixtures report: --memory-budget: положительное значение, получено %d", *budget)
	}
	if fs.NArg() > 0 {
		return usagef("fixtures report: лишние аргументы %v (команда принимает только флаги)", fs.Args())
	}

	snapB, err := fixturesReadSnapshot(*baseline)
	if err != nil {
		return fmt.Errorf("fixtures report: %w", err)
	}
	snapC, err := fixturesReadSnapshot(*candidate)
	if err != nil {
		return fmt.Errorf("fixtures report: %w", err)
	}

	report, err := fixturesReportBuild(snapB, snapC, *budget)
	if err != nil {
		return fmt.Errorf("fixtures report: %w", err)
	}

	var buf strings.Builder
	fixturesReportPrint(&buf, report)
	text := buf.String()
	out(stdout, "%s", text)
	if *outPath != "" {
		// Атомарно (tmp + rename, как снапшот): прерванная запись не
		// оставляет полуфайла протокола T-247 (ревью T-244 P3-5).
		tmp := *outPath + ".tmp"
		if err := os.WriteFile(tmp, []byte(text), 0o644); err != nil {
			return fmt.Errorf("fixtures report: запись %s: %w", *outPath, err)
		}
		if err := os.Rename(tmp, *outPath); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("fixtures report: запись %s: %w", *outPath, err)
		}
		_, _ = fmt.Fprintf(stderr, "отчёт записан: %s\n", *outPath)
	}
	if *failExit && report.Fail {
		return fmt.Errorf("fixtures report: вердикт НЕ ПРОЙДЕН по %s", strings.Join(failedVerdictNames(report), ", "))
	}
	return nil
}

// failedVerdictNames — имена непройденных вердиктов (сообщение --fail-exit).
func failedVerdictNames(r *fixturesReport) []string {
	var names []string
	for _, v := range r.Verdicts {
		if v.Status == verdictFail {
			names = append(names, v.Name)
		}
	}
	return names
}
