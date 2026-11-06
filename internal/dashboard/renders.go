// Панель движков рендера и фолбэка (ТЗ рендера §4.6 п.4, T-246; полнота
// ТЗ — S-P3-2): темп, успешность и тарпит-рейт рендера по движкам
// (метки engine счётчиков этапа, T-235/S-P2-4), RSS воркеров движка,
// fallback-рейт — published/страницы этапа — и потребление
// (consumed/глубина q:render-fallback).
//
// Источники — HTTP-экспозиции /metrics рендер-воркеров
// (dashboard.render_metrics_urls: основная сборка — движок этапа,
// сборка воркера B — потребление фолбэка): те же адреса, что скрейпит
// Prometheus (прецедент — зонд ingest у monitor, T-099), без
// промежуточной БД — счётчики живут у владельца данных, атрибуция по
// движкам существует только там (схема БД движка не знает, ТЗ рендера
// §4.8). Глубина q:render-fallback — тот же общий расчёт глубины, что у
// секции очередей (c.lens, GroupDepth через гейт), не метрика сборки B:
// при остановленном B ряд отсутствует, а остаток очереди — именно то,
// что оператору нужно видеть.
//
// Темп (ренд./с, публикации/с) — дельта кумулятивов между проходами
// сборщика в памяти Collector'а: окно = период refresh (5 с на
// дефолте), фронт сглаживает историей опросов. Рестарт воркера
// (кумулятив упал) даёт нулевую дельту — серии не рвутся, всплеска нет.
//
// Деградация — как у секций снимка (§5.9): список адресов пуст —
// Render = nil («н/д», не ошибка — стиль Health без проб); ошибка
// любого воркера деградирует секцию целиком (причина — в errors):
// полупустая панель вводила бы в заблуждение (движок «пропал» из
// агрегата неотличим от нулевого потока).
package dashboard

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crawler/internal/queue"
)

// Имена приборов этапа рендера в экспозиции (internal/renderworker/
// metrics.go): фиксированный срез — полноценный парсер экспозиции
// (expfmt) не окупает зависимость (прецедент — monitor/badhashes.go).
const (
	renderRecordsMetric   = "crawler_render_records_total"            // {rendered,engine} (T-235)
	renderFinalizedMetric = "crawler_render_task_finalized_total"     // {reason,engine} (T-172/T-235)
	renderTarpitMetric    = "crawler_render_tarpit_finalized_total"   // {engine} (T-148/S-P2-4)
	fbPublishedMetric     = "crawler_render_fallback_published_total" // {reason} (T-238)
	fbConsumedMetric      = "crawler_render_fallback_consumed_total"  // {result} (T-245)
	stageTasksMetric      = "crawler_stage_tasks_total"               // {stage} — знаменатель fallback-рейта (S-P3-2)
	processRSSMetric      = "process_resident_memory_bytes"           // стандартный process-collector — RSS воркера (S-P3-2)
)

// renderBodyLimit — потолок чтения тела /metrics: реестр воркера несёт
// коллекторы Go/runtime и процесса (десятки КиБ), потолок — защита от
// неадекватного ответа, не оценка размера (стиль monitor.badHashesBodyLimit).
const renderBodyLimit = 4 << 20

// promSample — сэмпл exposition text format: имя, метки, значение.
type promSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// parsePromExposition — минимальный разбор exposition text format
// (сэмплы с метками и без; HELP/TYPE пропускаются). Свой парсер вместо
// expfmt: зависимость prometheus/common в графе модуля indirect, панели
// достаточно фиксированного среза приборов (подход — fixturesParseProm
// Metrics инструмента сравнения движков T-244, своя копия: пакет main не
// импортируется).
func parsePromExposition(r io.Reader) ([]promSample, error) {
	var out []promSample
	sc := bufio.NewScanner(io.LimitReader(r, renderBodyLimit))
	for n := 0; sc.Scan(); n++ {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" || line[0] == '#' {
			continue
		}
		name, labels := "", ""
		rest := line
		if i := strings.IndexByte(line, '{'); i >= 0 {
			end := promLabelsEnd(line, i)
			if end < 0 {
				return nil, fmt.Errorf("метрики: строка %d: метки без закрывающей скобки", n+1)
			}
			name = line[:i]
			labels = line[i+1 : end]
			rest = line[end+1:]
		} else {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			name = fields[0]
			rest = strings.TrimPrefix(strings.TrimSpace(line), name)
		}
		// Значение — первый токен после имени/меток; таймстамп и
		// exemplar-хвост OpenMetrics не наш случай, отрезаются.
		if i := strings.Index(rest, " #"); i >= 0 {
			rest = rest[:i]
		}
		valueFields := strings.Fields(rest)
		if len(valueFields) == 0 {
			return nil, fmt.Errorf("метрики: строка %d: нет значения", n+1)
		}
		v, err := strconv.ParseFloat(valueFields[0], 64)
		if err != nil {
			return nil, fmt.Errorf("метрики: строка %d: значение %q: %w", n+1, valueFields[0], err)
		}
		parsed, err := parsePromLabels(labels)
		if err != nil {
			return nil, fmt.Errorf("метрики: строка %d: %w", n+1, err)
		}
		out = append(out, promSample{Name: name, Labels: parsed, Value: v})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("чтение экспозиции: %w", err)
	}
	return out, nil
}

// promLabelsEnd — индекс '}' закрывающей список меток (start — индекс
// открывающей '{'); '}' и '{' внутри кавычек значений — часть значения.
func promLabelsEnd(s string, start int) int {
	inQuote := false
	for i := start + 1; i < len(s); i++ {
		switch {
		case s[i] == '\\' && inQuote && i+1 < len(s):
			i++
		case s[i] == '"':
			inQuote = !inQuote
		case s[i] == '}' && !inQuote:
			return i
		}
	}
	return -1
}

// parsePromLabels — список меток name="value" через запятую (пустая
// строка — без меток). Разделитель — запятая вне кавычек: запятая внутри
// значения легальна (text format).
func parsePromLabels(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	labels := map[string]string{}
	var b strings.Builder
	inQuote := false
	flush := func() error { return promLabelPair(b.String(), labels) }
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && inQuote && i+1 < len(s):
			b.WriteByte(s[i])
			b.WriteByte(s[i+1])
			i++
		case s[i] == '"':
			inQuote = !inQuote
			b.WriteByte(s[i])
		case s[i] == ',' && !inQuote:
			if err := flush(); err != nil {
				return nil, err
			}
			b.Reset()
		default:
			b.WriteByte(s[i])
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return labels, nil
}

// promLabelPair — одна пара name="value" в карту меток.
func promLabelPair(pair string, labels map[string]string) error {
	pair = strings.TrimSpace(pair)
	if pair == "" {
		return nil
	}
	k, v, found := strings.Cut(pair, "=")
	if !found || len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
		return fmt.Errorf("метка %q без значения в кавычках", pair)
	}
	labels[strings.TrimSpace(k)] = promUnescape(v[1 : len(v)-1])
	return nil
}

// promUnescape — экраны значений меток text format: \\, \" и \n;
// неизвестный экран сохраняется буквально (обратный слэш + символ).
func promUnescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case '\\', '"', 'n':
			b.WriteByte(s[i+1])
			i++
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// renderCounters — агрегированные кумулятивы панели со всех воркеров:
// суммирование по движкам/исходам (несколько сборок одного движка —
// один браузерный стек класса, панель показывает движок, не воркера).
type renderCounters struct {
	rendered  map[string]float64 // engine → rendered="true" (T-235)
	finalized map[string]float64 // engine → Σ task_finalized по причинам
	tarpits   map[string]float64 // engine → tarpit_finalized (T-148/S-P2-4)
	published float64            // Σ fallback_published по причинам (T-238)
	consumed  map[string]float64 // result → fallback_consumed (T-245)
	tasks     float64            // Σ stage_tasks{stage="render"} — страницы этапа (S-P3-2)
	rss       map[string]float64 // engine → Σ RSS воркеров движка (S-P3-2)
}

// collectRenderCounters — скрейп всех экспозиций параллельно (стиль
// probeAll: таймаут клиента probeTimeout, не сумма N запросов) и
// сведение сэмплов в агрегат.
func (c *Collector) collectRenderCounters(ctx context.Context) (renderCounters, error) {
	urls := c.opt.RenderMetrics
	if len(urls) == 0 {
		return renderCounters{}, nil
	}
	bodies := make([][]byte, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i, raw := range urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bodies[i], errs[i] = c.fetchRenderMetrics(ctx, raw)
		}()
	}
	wg.Wait()
	var failed []error
	for _, err := range errs {
		if err != nil {
			failed = append(failed, err)
		}
	}
	if len(failed) > 0 {
		return renderCounters{}, errors.Join(failed...)
	}
	agg := renderCounters{
		rendered:  map[string]float64{},
		finalized: map[string]float64{},
		tarpits:   map[string]float64{},
		consumed:  map[string]float64{},
		rss:       map[string]float64{},
	}
	for _, body := range bodies {
		samples, err := parsePromExposition(bytes.NewReader(body))
		if err != nil {
			return renderCounters{}, err
		}
		agg.add(samples)
	}
	return agg, nil
}

// fetchRenderMetrics — GET одной экспозиции (клиент проб: таймаут
// probeTimeout, без редиректов — исходящие соединения только на
// настроенные адреса, ТЗД §9.3).
func (c *Collector) fetchRenderMetrics(ctx context.Context, raw string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, fmt.Errorf("экспозиция %s: %w", raw, err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("экспозиция %s: %w", raw, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("экспозиция %s: статус %s", raw, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, renderBodyLimit))
	if err != nil {
		return nil, fmt.Errorf("экспозиция %s: чтение: %w", raw, err)
	}
	return body, nil
}

// add — сведение сэмплов одной экспозиции в агрегат: знакомые приборы
// по именам, ветки — по меткам (rendered="true"; engine у доли рендера,
// тарпитов и финализаций класса задачи; reason у публикаций не
// различается — публике нужен темп; result у потребления; stage="render"
// у страниц этапа). RSS экспозиции (process_resident_memory_bytes, без
// меток) атрибутируется движку сборки воркера — метке engine
// предсозданной серии rendered="true" (NewMetrics, T-235: серия в
// экспозиции с первого старта); сборка без метки (старее T-235) — RSS
// не атрибутируется, пропускается.
func (a *renderCounters) add(samples []promSample) {
	engine := ""
	for _, s := range samples {
		if s.Name == renderRecordsMetric && s.Labels["rendered"] == "true" {
			if e := s.Labels["engine"]; e != "" {
				engine = e
				break
			}
		}
	}
	for _, s := range samples {
		switch s.Name {
		case renderRecordsMetric:
			if s.Labels["rendered"] == "true" {
				if e := s.Labels["engine"]; e != "" {
					a.rendered[e] += s.Value
				}
			}
		case renderFinalizedMetric:
			if e := s.Labels["engine"]; e != "" {
				a.finalized[e] += s.Value
			}
		case renderTarpitMetric:
			if e := s.Labels["engine"]; e != "" {
				a.tarpits[e] += s.Value
			}
		case fbPublishedMetric:
			a.published += s.Value
		case fbConsumedMetric:
			if r := s.Labels["result"]; r != "" {
				a.consumed[r] += s.Value
			}
		case stageTasksMetric:
			if s.Labels["stage"] == "render" {
				a.tasks += s.Value
			}
		case processRSSMetric:
			if engine != "" {
				a.rss[engine] += s.Value
			}
		}
	}
}

// renderRateState — память темпов панели между проходами: прошлый
// агрегат и момент. Живёт в Collector (не в снимке), первый проход —
// база (темпы 0), рестарт воркера (кумулятив упал) — нулевая дельта.
type renderRateState struct {
	seen      bool
	at        time.Time
	rendered  map[string]float64
	published float64
	consumed  float64
}

// rateOf — дельта кумулятива за Δt; prev < cur — рост (делится на окно),
// иначе 0 (нет базы / рестарт / стагнация).
func rateOf(prev, cur float64, dt time.Duration) float64 {
	if dt <= 0 || cur <= prev {
		return 0
	}
	return (cur - prev) / dt.Seconds()
}

// observe — темпы прохода: дельты агрегата к прошлому проходу и
// обновление базы. Движок, отсутствовавший в базе, темпа не имеет
// (нулевой), но базируется на текущем значении.
func (s *renderRateState) observe(cur renderCounters, now time.Time) renderRates {
	dt := time.Duration(0)
	if s.seen {
		dt = now.Sub(s.at)
	}
	if dt < 0 {
		dt = 0
	}
	out := renderRates{
		rendered: make(map[string]float64, len(cur.rendered)),
	}
	for e, v := range cur.rendered {
		out.rendered[e] = rateOf(s.rendered[e], v, dt)
	}
	out.published = rateOf(s.published, cur.published, dt)
	var consumed float64
	for _, v := range cur.consumed {
		consumed += v
	}
	out.consumed = rateOf(s.consumed, consumed, dt)

	s.seen = true
	s.at = now
	s.rendered = make(map[string]float64, len(cur.rendered))
	for e, v := range cur.rendered {
		s.rendered[e] = v
	}
	s.published = cur.published
	s.consumed = consumed
	return out
}

// renderRates — темпы одного прохода (ренд./с по движкам, публикации/с,
// потребление/с).
type renderRates struct {
	rendered  map[string]float64
	published float64
	consumed  float64
}

// collectRender — секция панели движков: агрегат экспозиций + темпы +
// глубина q:render-fallback (тем же lens, что у секции очередей; ошибка
// глубины не роняет секцию — Depth = nil «н/д», причина видна в errors
// секции queues). nil-возврат без ошибки — адреса не заданы («н/д»).
func (c *Collector) collectRender(ctx context.Context) (*RenderJSON, error) {
	if len(c.opt.RenderMetrics) == 0 {
		return nil, nil
	}
	agg, err := c.collectRenderCounters(ctx)
	if err != nil {
		return nil, err
	}
	rates := c.rates.observe(agg, c.opt.Now())

	engines := make([]string, 0, len(agg.rendered)+len(agg.finalized)+len(agg.tarpits)+len(agg.rss))
	seenEngine := map[string]bool{}
	for _, m := range []map[string]float64{agg.rendered, agg.finalized, agg.tarpits, agg.rss} {
		for e := range m {
			if !seenEngine[e] {
				seenEngine[e] = true
				engines = append(engines, e)
			}
		}
	}
	sort.Strings(engines)

	out := &RenderJSON{
		Engines: make([]RenderEngineJSON, 0, len(engines)),
		Fallback: RenderFallbackJSON{
			PublishedTotal:   int64(agg.published),
			PublishedPerSec:  rates.published,
			ConsumedPerSec:   rates.consumed,
			ConsumedByResult: make(map[string]int64, len(agg.consumed)),
		},
	}
	for _, e := range engines {
		row := RenderEngineJSON{
			Engine:         e,
			RenderedTotal:  int64(agg.rendered[e]),
			FinalizedTotal: int64(agg.finalized[e]),
			TarpitTotal:    int64(agg.tarpits[e]),
			RSSBytes:       int64(agg.rss[e]),
			RenderedPerSec: rates.rendered[e],
		}
		// Успешность — знаменатель прежний (rendered+finalized,
		// решение оператора по S-P2-4): тарпит — отдельной колонкой.
		if denom := agg.rendered[e] + agg.finalized[e]; denom > 0 {
			pct := 100 * agg.rendered[e] / denom
			row.SuccessPct = &pct
		}
		// Тарпит-рейт — доля тарпит-финализаций среди всех завершений
		// движка с меткой engine (кумулятивы, как успешность: свежий
		// воркер без завершений — nil «—»).
		if denom := agg.rendered[e] + agg.finalized[e] + agg.tarpits[e]; denom > 0 {
			pct := 100 * agg.tarpits[e] / denom
			row.TarpitPct = &pct
		}
		out.Engines = append(out.Engines, row)
	}
	for r, v := range agg.consumed {
		out.Fallback.ConsumedByResult[r] = int64(v)
		out.Fallback.ConsumedTotal += int64(v)
	}
	// Fallback-рейт — доля публикаций от страниц этапа: числитель —
	// published воркера A, знаменатель — Σ stage_tasks{stage="render"}
	// всех экспозиций (воркеры A+B, те же единицы, что окно бюджета
	// T-239); nil — этап ещё не обработал ни одной страницы.
	if agg.tasks > 0 {
		pct := 100 * agg.published / agg.tasks
		out.Fallback.RatePct = &pct
	}

	// Глубина q:render-fallback — тем же lens и бюджетом, что секция
	// очередей: ошибка — Depth nil («н/д»), секция жива.
	if c.lens != nil {
		qctx, cancel := context.WithTimeout(ctx, queueFetchTimeout)
		depth, derr := c.lens.Len(qctx, queue.StreamRenderFallback)
		cancel()
		if derr == nil {
			out.Fallback.Depth = &depth
		}
	}
	return out, nil
}
