// Источник алерта превышения бюджета доли фолбэка (T-246, ТЗ рендера
// §4.6 п.3): счётчик событий приостановки
// crawler_render_fallback_suspended_total{reason="budget"} (T-239, ФТ-9б)
// читается из HTTP-экспозиции /metrics рендер-воркера — общий реестр на
// сервис (ТЗ 5.9, metrics.New), у monitor своей копии счётчика быть не
// может (процессы разные). Второй межсервисный зонд пакета по образцу
// IngestBadHashes (T-099): те же адреса, что скрейпит Prometheus, без
// промежуточной БД — счётчик живёт у владельца данных. Читается ветка
// reason="budget" (деградация основного движка по доле публикаций);
// события health-гейта (reason="gate") алертом не считаются — стрик
// инфра-ошибок класса браузера виден напрямую ошибками этапа
// crawler_stage_errors_total{stage="render",type="browser"}.
//
// Деградация: render-worker остановлен или экспозиция чужая (нет маркера
// рендер-этапа crawler_stage_tasks_total{stage="render"}) — ошибка
// источника, алерт «не измеряется» (Warn), не ложное срабатывание; смерть
// самого воркера видна по crawler_service_up каркаса. Серии
// suspended{reason="budget"} нет в экспозиции рендер-воркера — 0, не
// ошибка (S-P1-4, решение T-250 — предсоздание): серия предсоздана нулём
// с T-250, у более старых сборок ленивое семейство до первого события
// означает тот же ноль — алерт не глушится «не измеряется» и не теряет
// первое событие 0→1. Пустой monitor.render_metrics_url (не настроен) —
// источник не подключается вовсе, алерт «не измеряется» тихо (стиль
// ingest-зонда).
package monitor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// suspendedMetricName — имя метрики событий приостановки в экспозиции
	// рендер-воркера (T-239).
	suspendedMetricName = "crawler_render_fallback_suspended_total"

	// suspendedBudgetPair — точная пара метка-значение ветки бюджета доли
	// (словарь причин замкнут: gate|budget).
	suspendedBudgetPair = `reason="budget"`

	// renderStageTasksMetric / renderStagePair — маркер рендер-воркера:
	// серия crawler_stage_tasks_total{stage="render"} предсоздаётся в
	// NewMetrics воркера и отличает его экспозицию от чужой (http-воркер
	// экспонирует stage="http").
	renderStageTasksMetric = "crawler_stage_tasks_total"
	renderStagePair        = `stage="render"`

	// suspendedBodyLimit — потолок чтения тела /metrics: реестр воркера
	// несёт коллекторы Go/runtime и процесса (десятки КиБ), потолок —
	// защита от неадекватного ответа, не оценка размера (стиль
	// badHashesBodyLimit).
	suspendedBodyLimit = 4 << 20
)

// RenderSuspends — FallbackBudgetSource поверх GET /metrics
// рендер-воркера: адрес — monitor.render_metrics_url (dev:
// http://render-worker:9105/metrics, бой: loopback render-worker по
// METRICS_ADDR).
type RenderSuspends struct {
	// URL — адрес экспозиции /metrics рендер-воркера.
	URL string
	// Client — HTTP-клиент; nil — собственный с таймаутом ниже.
	Client *http.Client
}

// compile-time: RenderSuspends — FallbackBudgetSource.
var _ FallbackBudgetSource = RenderSuspends{}

// rendersProbeTimeout — бюджет одного чтения (паритет ingestProbeTimeout:
// цикл алертов живёт своим периодом, зонд не должен его задерживать
// отвалом сети). Настроечный бюджет, а не логика — герметичность
// юнит-тестов (docs/mutation-testing.md §4).
// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
const rendersProbeTimeout = 5 * time.Second

// FallbackBudget читает текущее значение счётчика событий бюджета доли
// из экспозиции рендер-воркера.
func (s RenderSuspends) FallbackBudget(ctx context.Context) (int64, error) {
	cl := s.Client
	if cl == nil {
		cl = &http.Client{Timeout: rendersProbeTimeout}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return 0, fmt.Errorf("monitor: зонд %s: %w", s.URL, err)
	}
	resp, err := cl.Do(req)
	if err != nil {
		return 0, fmt.Errorf("monitor: зонд %s: %w", s.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("monitor: зонд %s: статус %s", s.URL, resp.Status)
	}
	return parseSuspendExposition(resp.Body)
}

// parseSuspendExposition достаёт значение счётчика ветки reason="budget"
// из текстовой экспозиции Prometheus: строки «имя{метки} значение»,
// '#'-строки (HELP/TYPE) пропускаются. Выделен чистой функцией для
// табличного теста без HTTP (стиль parseBadHashesExposition).
//
// Серии нет — 0, а не ошибка, когда экспозиция принадлежит
// рендер-воркеру (маркер crawler_stage_tasks_total{stage="render"},
// предсоздаётся в NewMetrics): серия suspended{reason="budget"}
// предсоздана нулём с T-250 (S-P1-4), у сборок постарше семейство
// ленивое — до первого события это тот же честный ноль, алерт не
// переводится в «не измеряется» и не теряет первое событие 0→1.
// Ошибкой остаётся только чужая экспозиция (маркера нет — адрес
// настроен не на рендер-воркера).
func parseSuspendExposition(r io.Reader) (int64, error) {
	body, err := readExposition(r)
	if err != nil {
		return 0, err
	}
	v, found, err := findPromSeries(body, suspendedMetricName, suspendedBudgetPair)
	if err != nil {
		return 0, err
	}
	if found {
		return int64(v), nil
	}
	if renderExposition(body) {
		return 0, nil // рендер-воркер, серии ещё нет — 0 (S-P1-4)
	}
	return 0, fmt.Errorf("monitor: метрика %s{%s} отсутствует в экспозиции рендер-воркера",
		suspendedMetricName, suspendedBudgetPair)
}

// readExposition — тело экспозиции целиком в память (потолок
// suspendedBodyLimit): повторный поиск по строкам требует буфера,
// io.Reader однопроходен.
func readExposition(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, suspendedBodyLimit))
	if err != nil {
		return nil, fmt.Errorf("monitor: чтение экспозиции: %w", err)
	}
	return body, nil
}

// findPromSeries — значение первого сэмпла metric, содержащего все пары
// меток pairs (подмножество: лишние метки сэмпла не мешают — «семейство
// есть, нужной ветки нет» отличает следующий за именем фильтр).
// '#'-строки и пустые пропускаются; не-число/отрицательное значение —
// ошибка «не счётчик» (стиль parseBadHashesExposition).
func findPromSeries(body []byte, metric string, pairs ...string) (float64, bool, error) {
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		// Пропуск '#'-строк и пустых эквивалентен сравнению имени: Cut от
		// поля с '#' даёт имя с '#', не равный метрике (стиль
		// parseBadHashesExposition).
		// mutator-disable-next-line expression/remove
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		name, labels, _ := strings.Cut(fields[0], "{")
		if name != metric {
			continue
		}
		// Метки до '}': пары ищутся точным сравнением элемента списка
		// (словари замкнуты, значений с запятыми/кавычками в них нет).
		labelPart, _, _ := strings.Cut(labels, "}")
		if !promHasPairs(labelPart, pairs...) {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || v < 0 {
			return 0, false, fmt.Errorf("monitor: %s{%s}: значение %q не счётчик",
				metric, labelPart, fields[1])
		}
		return v, true, nil
	}
	if err := sc.Err(); err != nil {
		return 0, false, fmt.Errorf("monitor: чтение экспозиции: %w", err)
	}
	return 0, false, nil
}

// promHasPairs — все ли пары меток name="value" есть в списке через
// запятую.
func promHasPairs(labelPart string, pairs ...string) bool {
	for _, want := range pairs {
		found := false
		for _, pair := range strings.Split(labelPart, ",") {
			if pair == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// renderExposition — маркер рендер-воркера: серия
// crawler_stage_tasks_total{stage="render"} предсоздаётся в NewMetrics
// воркера (T-077) и живёт в экспозиции с первого старта — прочие сервисы
// экспонируют свой stage (http-воркер — stage="http") или не экспонируют
// семейство вовсе. Отсутствие маркера = чужая экспозиция.
func renderExposition(body []byte) bool {
	_, found, _ := findPromSeries(body, renderStageTasksMetric, renderStagePair)
	return found
}
