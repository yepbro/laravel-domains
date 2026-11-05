// ---------------------------------------------------------------- снапшот
//
// crawlerctl fixtures snapshot (T-244, ТЗ задач выбора движка рендера
// tasks/TZ_render_engines.md §5.1 п. 3, §6.1 п. 5): фиксация результатов
// одного прогона стенда в самодостаточный JSON-снимок — вход отчёта
// сравнения (fixtures report). Повторный накат фикстуры (T-243)
// возвращает строки к состоянию дампа и выметает контент прогона,
// поэтому состояние «после прогона» нельзя прочесть из БД задним числом:
// снапшот снимается сразу по завершении прогона (пустая q:render,
// фолбэк доработан) и до повторного import/publish следующего.
//
// Состав (метрики сравнения §5.1 п. 3):
//
//	БД (по ключам дампа фикстуры):
//	  - rendered/контентная ссылка — доля успешных рендеров прогона:
//	    после наката content_hash = NULL (T-243), ссылка появляется только
//	    у DOM, записанного прогоном, — успешность = строки с контентом /
//	    задачи дампа (прогон обязан либо отрендерить задачу, либо
//	    финализировать/не тронуть — всё, кроме записи DOM, честно уменьшает
//	    долю);
//	  - длина DOM (contents.raw_len) и content_hash — структурные метрики
//	    (живой DOM недетерминирован по байтам, побайтовый диф ТЗ не
//	    применяет; совпадение хэша — сильный сигнал «тот же DOM»);
//	  - узлы/скрипты DOM — чтение DOM из хранилища контента (PackStore
//	    ReadOnly, стиль crawlerctl domain --content) и разбор;
//	  - вердикты анализатора (laravel_findings.is_laravel) — материал дифа
//	    детекции против baseline-chromium (протокол T-213);
//	Redis (redis.addr конфигурации): границы стрима q:analyze по ID
//	записей (мс публикации) и длины стримов — темп прогона (стр/с) и
//	остатки очередей; стримы чистятся между прогонами (инструкция
//	T-243), поэтому снимок принадлежит ровно одному прогону;
//	метрики этапа (--metrics URL /metrics render-worker): счётчики
//	финализаций по словарю taskFinalizeReason (net::ERR_*), тарпит/cert/
//	DNS-финализации, публикации фолбэка, гистограмма времени страницы
//	(ср./p95), browser-класс ошибок (движок недоступен) и момент старта
//	процесса воркера — по нему отчёт отличает счётчики одного прогона
//	(воркер перезапущен между прогонами) от накопительных;
//	оператор: --engine-rss-max (макс. RSS контейнера движка за прогон —
//	в dev-стеке нет экспортёра памяти движков, ТЗ §4.6 п. 4 обещает его
//	только боевой телеметрии) и --engine-crashes (падения ядра движка).
//
// Поля, источник которых недоступен (analyzer не доработал хвост, контент
// выметен retention'ом), не запрещают снапшот: соответствующие строки
// отчёта честно деградируют до «нет данных».

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/html"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/queue"
)

// fixturesSnapshotVersion — версия формата снапшота (контракт
// snapshot → report; смена состава — новая версия).
const fixturesSnapshotVersion = 1

// fixturesSnapshotFileSuffix — суффикс имени файла снапшота по умолчанию
// (<каталог дампа>/<label>.snapshot.json — снапшоты одного стенда лежат
// рядом со своим дампом).
const fixturesSnapshotFileSuffix = ".snapshot.json"

// fixturesLabelRe — допустимая метка снапшота: без разделителей пути
// (метка — часть имени файла по умолчанию).
var fixturesLabelRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// fixturesSnapshotRow — строка фикстуры в снапшоте: состояние после
// прогона + вердикт анализатора. Ключи — оригинальные идентификаторы
// дампа (T-242); поля сжаты до необходимых отчёту.
type fixturesSnapshotRow struct {
	ZoneID      int16  `json:"z"`
	DomainID    int64  `json:"d"`
	SubdomainID int16  `json:"s"`
	Variant     int16  `json:"v"`
	Rendered    bool   `json:"rendered"`               // rendered строки (прогон ставит вместе с контентом)
	HasContent  bool   `json:"content"`                // content_hash NOT NULL — DOM записан прогоном
	ContentHash string `json:"content_hash,omitempty"` // hex идентификатор DOM (дедуп хранилища)
	DOMLen      int    `json:"dom_len"`                // contents.raw_len, байты (0 — контента нет)
	DOMParsed   bool   `json:"dom_parsed"`             // узлы/скрипты посчитаны по DOM хранилища
	DOMNodes    int    `json:"dom_nodes"`              // узлы дерева HTML (все, включая текст)
	DOMScripts  int    `json:"dom_scripts"`            // элементы <script>
	// RulesVersion — rules_version СТРОКИ subdomain_results: рендер ставит
	// NULL (renderUpdateSQL), анализатор штампует после прохода по этому
	// DOM; NULL у строки с контентом = анализатор не доработал её в этом
	// прогоне, findings в БД могли остаться от прошлого (ревью T-244 P1-2).
	RulesVersion *int32 `json:"row_rules_version,omitempty"`
	// LaravelFound — есть ли строка laravel_findings. Отсутствие строки —
	// ШТАТНЫЙ вердикт анализатора «ни одного срабатывания» (findingDeleteSQL
	// удаляет её), а не «не доработал»: диф детекции трактует его как
	// is_laravel=false (ревью T-244 P1-1). Актуальность гарантирует
	// RulesVersion строки.
	LaravelFound        bool    `json:"laravel_found"`
	IsLaravel           bool    `json:"is_laravel"`                      // laravel_findings.is_laravel (при found)
	Score               float32 `json:"score"`                           // score находок (при found)
	FindingRulesVersion *int32  `json:"finding_rules_version,omitempty"` // версия правил находок (при found)
}

// fixturesSnapshotDump — выжимка манифеста дампа для сверки пары
// снапшотов: отчёт требует, чтобы оба были сняты по одной выборке.
type fixturesSnapshotDump struct {
	Seed   int64                          `json:"seed"`
	Keys   int                            `json:"keys"`
	Strata map[string]fixturesStratumStat `json:"strata"`
}

// fixturesSnapshotStreams — снимок стримов Redis стенда после прогона:
// границы q:analyze по ID (мс публикации воркером) — темп; длины —
// остатки очередей и размер фолбэк-потока.
type fixturesSnapshotStreams struct {
	AnalyzeLen     int64 `json:"analyze_len"`      // XLEN q:analyze = задачи, законченные прогоном
	AnalyzeFirstMs int64 `json:"analyze_first_ms"` // мс первой записи (0 — стрим пуст)
	AnalyzeLastMs  int64 `json:"analyze_last_ms"`  // мс последней записи
	RenderLen      int64 `json:"render_len"`       // XLEN q:render (остаток недоработанного)
	FallbackLen    int64 `json:"fallback_len"`     // XLEN q:render-fallback
	DeadLen        int64 `json:"dead_len"`         // XLEN q:dead
}

// fixturesHistBucket — кумулятивная корзина гистограммы (le ≤ …);
// +Inf — замыкающая.
type fixturesHistBucket struct {
	LE    float64 `json:"le"`
	Count float64 `json:"count"`
}

// fixturesSnapshotMetrics — снимок приборов этапа рендера с /metrics
// render-worker (счётчики — с момента старта процесса; между прогонами
// воркер перезапускается, тогда значения = ровно прогон).
type fixturesSnapshotMetrics struct {
	ProcessStartSeconds float64              `json:"process_start_seconds"` // process_start_time_seconds (0 — прибор не экспортирован)
	Tasks               float64              `json:"stage_tasks_render"`    // crawler_stage_tasks_total{stage="render"}
	SecondsSum          float64              `json:"stage_seconds_sum"`     // crawler_stage_seconds_sum{stage="render"}
	SecondsCount        float64              `json:"stage_seconds_count"`   // crawler_stage_seconds_count{stage="render"}
	Buckets             []fixturesHistBucket `json:"buckets"`               // crawler_stage_seconds_bucket{stage="render"} (le по возрастанию)
	Errors              map[string]float64   `json:"errors"`                // crawler_stage_errors_total{stage="render"} по type
	RenderedRecords     float64              `json:"rendered_records_true"` // crawler_render_records_total{rendered="true"}
	TarpitFinalized     float64              `json:"tarpit_finalized_total"`
	CertFinalized       float64              `json:"cert_finalized_total"`
	DNSFinalized        float64              `json:"dns_finalized_total"`
	TaskFinalized       map[string]float64   `json:"task_finalized_total"`     // reason → счёт (словарь net::ERR_*, T-172)
	FallbackPublished   map[string]float64   `json:"fallback_published_total"` // reason → счёт (T-238)
}

// fixturesSnapshot — снапшот прогона (формат v1): JSON-файл, вход
// fixtures report. Rows — по ключам дампа; Streams/Metrics отсутствуют
// (null), если источник не снимался; Engine* — операторский вход
// (отсутствует — отчёт деградирует по соответствующей строке вердикта).
type fixturesSnapshot struct {
	FormatVersion int                      `json:"format_version"`
	Label         string                   `json:"label"`
	Engine        string                   `json:"engine"` // chromium|moli|lightpanda
	CreatedAt     time.Time                `json:"created_at"`
	Dump          fixturesSnapshotDump     `json:"dump"`
	Rows          []fixturesSnapshotRow    `json:"rows"`
	Streams       *fixturesSnapshotStreams `json:"streams,omitempty"`
	Metrics       *fixturesSnapshotMetrics `json:"metrics,omitempty"`
	EngineRSSMax  *int64                   `json:"engine_rss_max_bytes,omitempty"` // макс. RSS движка за прогон (docker stats)
	EngineCrashes *int64                   `json:"engine_crashes,omitempty"`       // падения ядра движка за прогон
}

// ---------------------------------------------------------------- SQL

// fixturesSnapshotRowsSQL — состояние строк фикстуры после прогона по
// ключам дампа (unnest-массивы — стиль пакетных операций internal/db):
// контентная ссылка и её длина (JOIN contents), вердикт анализатора
// (LEFT JOIN laravel_findings — анализатор мог не доработать хвост).
const fixturesSnapshotRowsSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
	"r.rendered, r.content_hash, c.raw_len, r.rules_version, " +
	"f.is_laravel, f.score, f.rules_version " +
	"FROM unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[]) " +
	"AS k(zone_id, domain_id, subdomain_id, variant) " +
	"JOIN subdomain_results AS r ON r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
	"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant " +
	"LEFT JOIN contents AS c ON c.hash = r.content_hash " +
	"LEFT JOIN laravel_findings AS f ON f.zone_id = r.zone_id AND f.domain_id = r.domain_id " +
	"AND f.subdomain_id = r.subdomain_id AND f.variant = r.variant"

// ---------------------------------------------------------------- БД

// fixturesSnapshotReadRows — SQL-часть снапшота: строки по ключам дампа.
func fixturesSnapshotReadRows(ctx context.Context, pool db.DB, keys []fixturesKey) ([]fixturesSnapshotRow, error) {
	zones := make([]int16, len(keys))
	domains := make([]int64, len(keys))
	subs := make([]int16, len(keys))
	variants := make([]int16, len(keys))
	for i, k := range keys {
		zones[i], domains[i], subs[i], variants[i] = k.zone, k.domain, k.subdomain, k.variant
	}
	rows, err := pool.Query(ctx, fixturesSnapshotRowsSQL, zones, domains, subs, variants)
	if err != nil {
		return nil, fmt.Errorf("снапшот: чтение строк фикстуры: %w", err)
	}
	defer rows.Close()
	out := make([]fixturesSnapshotRow, 0, len(keys))
	for rows.Next() {
		var r fixturesSnapshotRow
		var contentHash []byte
		var rawLen *int32
		var isLaravel *bool
		var score *float32
		if err := rows.Scan(&r.ZoneID, &r.DomainID, &r.SubdomainID, &r.Variant,
			&r.Rendered, &contentHash, &rawLen, &r.RulesVersion,
			&isLaravel, &score, &r.FindingRulesVersion); err != nil {
			return nil, fmt.Errorf("снапшот: чтение строк фикстуры: %w", err)
		}
		r.HasContent = contentHash != nil
		if r.HasContent {
			r.ContentHash = hex.EncodeToString(contentHash)
		}
		if rawLen != nil {
			r.DOMLen = int(*rawLen)
		}
		if isLaravel != nil {
			r.LaravelFound = true
			r.IsLaravel = *isLaravel
		}
		if score != nil {
			r.Score = *score
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("снапшот: чтение строк фикстуры: %w", err)
	}
	return out, nil
}

// fixturesSnapshotDOMStats — структурные метрики DOM (узлы/скрипты):
// живой DOM недетерминирован по байтам, но структура стабильна —
// нормализованное сравнение ТЗ §5.1 п. 3. Считаются все узлы дерева
// (включая текст) и элементы <script>.
func fixturesSnapshotDOMStats(dom []byte) (nodes, scripts int) {
	if len(dom) == 0 {
		return 0, 0 // пустой DOM не «скелет html.Parse» из 4 узлов — контента нет
	}
	tree, err := html.Parse(bytes.NewReader(dom))
	if err != nil {
		return 0, 0 // лучший-effорт парсер: ошибки не фатальны, но ничего не считаем
	}
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		nodes++
		if n.Type == html.ElementNode && n.Data == "script" {
			scripts++
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(tree)
	return nodes, scripts
}

// contentGetter — читатель контента (сужение content.ContentStore до
// нужного снапшоту Get; *content.PackStore удовлетворяет).
type contentGetter interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// fixturesSnapshotParseDOM — заполнение структурных метрик строк снапшота
// по DOM хранилища контента. Отсутствующий контент (retention/повторный
// накат) и ошибки чтения — деградация строки (DOMParsed=false), не сбой
// снапшота: счётчик пропусков возвращается для отчёта оператору.
func fixturesSnapshotParseDOM(ctx context.Context, store contentGetter, rows []fixturesSnapshotRow) (skipped int) {
	for i := range rows {
		if !rows[i].HasContent {
			continue
		}
		raw, err := hex.DecodeString(rows[i].ContentHash)
		if err != nil || len(raw) != content.HashLen {
			skipped++
			continue
		}
		var hash content.Hash
		copy(hash[:], raw)
		dom, err := store.Get(ctx, hash)
		if err != nil || len(dom) == 0 {
			skipped++
			continue
		}
		rows[i].DOMNodes, rows[i].DOMScripts = fixturesSnapshotDOMStats(dom)
		rows[i].DOMParsed = true
	}
	return skipped
}

// ---------------------------------------------------------------- Redis

// fixturesSnapshotTakeStreams — стримы стенда: XLEN q:render/
// q:render-fallback/q:analyze/q:dead и границы q:analyze по ID записей
// (мс). ID Redis Streams — «<мс>-<seq>» публикации; до прогона стримы
// чистятся (инструкция T-243), значит границы принадлежат прогону.
func fixturesSnapshotTakeStreams(ctx context.Context, rdb redisStreamClient) (*fixturesSnapshotStreams, error) {
	s := &fixturesSnapshotStreams{}
	for _, st := range []struct {
		stream string
		dst    *int64
	}{
		{queue.StreamRender, &s.RenderLen},
		{queue.StreamRenderFallback, &s.FallbackLen},
		{queue.StreamAnalyze, &s.AnalyzeLen},
		{queue.StreamDead, &s.DeadLen},
	} {
		n, err := rdb.XLen(ctx, st.stream).Result()
		if err != nil {
			return nil, fmt.Errorf("снапшот: XLEN %s: %w", st.stream, err)
		}
		*st.dst = n
	}
	if s.AnalyzeLen > 0 {
		first, err := rdb.XRangeN(ctx, queue.StreamAnalyze, "-", "+", 1).Result()
		if err != nil {
			return nil, fmt.Errorf("снапшот: XRANGE %s: %w", queue.StreamAnalyze, err)
		}
		last, err := rdb.XRevRangeN(ctx, queue.StreamAnalyze, "+", "-", 1).Result()
		if err != nil {
			return nil, fmt.Errorf("снапшот: XREVRANGE %s: %w", queue.StreamAnalyze, err)
		}
		if len(first) > 0 && len(last) > 0 {
			var ok1, ok2 bool
			s.AnalyzeFirstMs, ok1 = fixturesStreamIDMs(first[0].ID)
			s.AnalyzeLastMs, ok2 = fixturesStreamIDMs(last[0].ID)
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("снапшот: ID %s/%s без миллисекунд — темп не снимается", first[0].ID, last[0].ID)
			}
		}
	}
	return s, nil
}

// redisStreamClient — сужение *redis.Client до нужного снапшоту
// (XLen/XRangeN/XRevRangeN; шов юнит-тестов).
type redisStreamClient interface {
	XLen(ctx context.Context, key string) *redis.IntCmd
	XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd
	XRevRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd
}

// fixturesStreamIDMs — миллисекунды из ID записи Redis Stream
// («<мс>-<seq>»); false — нестандартный ID.
func fixturesStreamIDMs(id string) (int64, bool) {
	ms, _, found := strings.Cut(id, "-")
	if !found {
		return 0, false
	}
	v, err := strconv.ParseInt(ms, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

// ---------------------------------------------------------------- метрики

// fixturesPromSample — сэмпл exposition text format (значение и метки).
type fixturesPromSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// fixturesParsePromMetrics — минимальный разбор exposition text format
// (нужный срез: сэмплы с метками и без, HELP/TYPE пропускаются). Свой
// парсер вместо expfmt: зависимость prometheus/common в графе модуля
// indirect, а инструменту достаточно фиксированного среза приборов.
func fixturesParsePromMetrics(data []byte) ([]fixturesPromSample, error) {
	var out []fixturesPromSample
	for n, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || line[0] == '#' {
			continue
		}
		name := ""
		labels := map[string]string{}
		rest := ""
		if i := strings.IndexByte(line, '{'); i >= 0 {
			end := fixturesLabelsEnd(line, i)
			if end < 0 {
				return nil, fmt.Errorf("метрики: строка %d: метки без закрывающей скобки", n+1)
			}
			name = line[:i]
			parsed, err := fixturesParsePromLabels(line[i+1 : end])
			if err != nil {
				return nil, fmt.Errorf("метрики: строка %d: %w", n+1, err)
			}
			labels = parsed
			rest = line[end+1:]
		} else {
			name = strings.Fields(line)[0] // сэмпл без меток: имя — первый токен строки
			rest = strings.TrimPrefix(strings.TrimSpace(line), name)
		}
		// Значение — первый токен после имени/меток; за ним может стоять
		// таймстамп exposition-формата и exemplar-хвост OpenMetrics
		// («# {…} …») — не наш случай, отрезается.
		if i := strings.Index(rest, " #"); i >= 0 {
			rest = rest[:i]
		}
		restFields := strings.Fields(rest)
		if len(restFields) == 0 {
			return nil, fmt.Errorf("метрики: строка %d: нет значения", n+1)
		}
		v, err := strconv.ParseFloat(restFields[0], 64)
		if err != nil {
			return nil, fmt.Errorf("метрики: строка %d: значение %q: %w", n+1, restFields[0], err)
		}
		out = append(out, fixturesPromSample{Name: name, Labels: labels, Value: v})
	}
	return out, nil
}

// fixturesLabelsEnd — индекс '}' закрывающей список меток (start — индекс
// открывающей '{'); '}' и '{' внутри кавычек значений — часть значения.
// -1 — скобка не закрыта.
func fixturesLabelsEnd(s string, start int) int {
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

// fixturesParsePromLabels — список меток name="value" через запятую.
// Разделитель — запятая ВНЕ кавычек: запятая внутри значения легальна
// (text format); exemplar-хвост ('# …' после значения сэмпла) отсекается
// вызовом fixturesStripExemplar до разбора меток.
func fixturesParsePromLabels(s string) (map[string]string, error) {
	labels := map[string]string{}
	for _, pair := range fixturesSplitLabelPairs(s) {
		if pair == "" {
			continue
		}
		k, v, found := strings.Cut(pair, "=")
		if !found || len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
			return nil, fmt.Errorf("метка %q без значения в кавычках", pair)
		}
		labels[strings.TrimSpace(k)] = fixturesUnescapeProm(v[1 : len(v)-1])
	}
	return labels, nil
}

// fixturesSplitLabelPairs — деление списка меток по запятым вне кавычек
// (внутри кавычек запятая — часть значения).
func fixturesSplitLabelPairs(s string) []string {
	var parts []string
	var b strings.Builder
	inQuote := false
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
			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteByte(s[i])
		}
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	return parts
}

// fixturesUnescapeProm — экраны значений меток text format: \\, \" и \n;
// неизвестный экран сохраняется буквально (обратный слэш + символ) —
// пути Windows в метках не должны молча терять слэши.
func fixturesUnescapeProm(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case '\\':
			b.WriteByte('\\')
		case '"':
			b.WriteByte('"')
		case 'n':
			b.WriteByte('\n')
		default:
			b.WriteByte('\\')
			b.WriteByte(s[i+1])
		}
		i++
	}
	return b.String()
}

// fixturesSnapshotMetricsURL — GET /metrics и сбор приборов этапа
// рендера (таймаут 10 с — зависший эндпоинт не вешает снапшот, который
// нужно снять до повторного наката). Отсутствие отдельных приборов
// (сборка без них) — нули, не ошибка: отчёт отличает «нет прибора» от
// «прогон без событий» по задачам этапа (Tasks == 0 ⇒ метрики чужого/
// пустого воркера — вердикты по ним деградируют, ревью T-244 P2-1).
func fixturesSnapshotMetricsURL(ctx context.Context, url string) (*fixturesSnapshotMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("метрики %s: %w", url, err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("метрики %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("метрики %s: статус %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("метрики %s: чтение: %w", url, err)
	}
	samples, err := fixturesParsePromMetrics(body)
	if err != nil {
		return nil, fmt.Errorf("метрики %s: %w", url, err)
	}
	return fixturesMetricsFromSamples(samples), nil
}

// fixturesMetricsFromSamples — сбор приборов этапа рендера из сэмплов
// /metrics: точечные счётчики по меткам, гистограмма — корзинами.
func fixturesMetricsFromSamples(samples []fixturesPromSample) *fixturesSnapshotMetrics {
	m := &fixturesSnapshotMetrics{
		Errors:            map[string]float64{},
		TaskFinalized:     map[string]float64{},
		FallbackPublished: map[string]float64{},
	}
	// value — первый подходящий сэмпл (повторов имён в корректном
	// экспозе нет; после рестарта процесса с shared-registry их тоже
	// не возникает — приборы одни).
	one := func(name string, match func(labels map[string]string) bool) (float64, bool) {
		for _, s := range samples {
			if s.Name == name && match(s.Labels) {
				return s.Value, true
			}
		}
		return 0, false
	}
	all := func(name string, each func(labels map[string]string, v float64)) {
		for _, s := range samples {
			if s.Name == name {
				each(s.Labels, s.Value)
			}
		}
	}
	anyLabel := func(map[string]string) bool { return true }
	stageRender := func(l map[string]string) bool { return l["stage"] == "render" }

	if v, ok := one("process_start_time_seconds", anyLabel); ok {
		m.ProcessStartSeconds = v
	}
	if v, ok := one("crawler_stage_tasks_total", stageRender); ok {
		m.Tasks = v
	}
	if v, ok := one("crawler_stage_seconds_sum", stageRender); ok {
		m.SecondsSum = v
	}
	if v, ok := one("crawler_stage_seconds_count", stageRender); ok {
		m.SecondsCount = v
	}
	if v, ok := one("crawler_render_records_total", func(l map[string]string) bool { return l["rendered"] == "true" }); ok {
		m.RenderedRecords = v
	}
	if v, ok := one("crawler_render_tarpit_finalized_total", anyLabel); ok {
		m.TarpitFinalized = v
	}
	if v, ok := one("crawler_render_cert_finalized_total", anyLabel); ok {
		m.CertFinalized = v
	}
	if v, ok := one("crawler_render_dns_finalized_total", anyLabel); ok {
		m.DNSFinalized = v
	}
	all("crawler_stage_errors_total", func(l map[string]string, v float64) {
		if l["stage"] == "render" {
			m.Errors[l["type"]] += v
		}
	})
	all("crawler_render_task_finalized_total", func(l map[string]string, v float64) {
		if reason := l["reason"]; reason != "" {
			m.TaskFinalized[reason] += v
		}
	})
	all("crawler_render_fallback_published_total", func(l map[string]string, v float64) {
		if reason := l["reason"]; reason != "" {
			m.FallbackPublished[reason] += v
		}
	})
	for _, s := range samples {
		if s.Name != "crawler_stage_seconds_bucket" || !stageRender(s.Labels) {
			continue
		}
		le, err := strconv.ParseFloat(s.Labels["le"], 64)
		if err != nil {
			continue // чужая/битая метка — корзину пропускаем, отчёт увидит неполную гистограмму
		}
		if math.IsInf(le, 1) {
			// +Inf не кодируется JSON (снапшот): замыкающая корзина хранится
			// сентинелом MaxFloat64 — кумулята сохранена, quantile в неё не
			// интерполирует (см. fixturesHistQuantile).
			le = math.MaxFloat64
		}
		m.Buckets = append(m.Buckets, fixturesHistBucket{LE: le, Count: s.Value})
	}
	sort.Slice(m.Buckets, func(i, j int) bool { return m.Buckets[i].LE < m.Buckets[j].LE })
	return m
}

// ---------------------------------------------------------------- файлы

// fixturesWriteSnapshotFile — атомарная запись снапшота (tmp + rename,
// стиль дампа T-242): прерванная запись не оставляет полуфайла.
func fixturesWriteSnapshotFile(path string, snap *fixturesSnapshot) error {
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("снапшот %s: кодирование: %w", path, err)
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("снапшот %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("снапшот %s: переименование: %w", path, err)
	}
	return nil
}

// fixturesReadSnapshot — чтение снапшота (вход отчёта): формат и версия
// проверяются до расчётов.
func fixturesReadSnapshot(path string) (*fixturesSnapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("снапшот %s: %w", path, err)
	}
	var snap fixturesSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, fmt.Errorf("снапшот %s: %w", path, err)
	}
	if snap.FormatVersion != fixturesSnapshotVersion {
		return nil, fmt.Errorf("снапшот %s: формат v%d не поддерживается (инструмент понимает v%d)",
			path, snap.FormatVersion, fixturesSnapshotVersion)
	}
	if len(snap.Rows) == 0 {
		return nil, fmt.Errorf("снапшот %s: без строк — пустой дамп или чужой файл", path)
	}
	return &snap, nil
}

// ---------------------------------------------------------------- команда

// fixturesSnapshotCmd — crawlerctl fixtures snapshot: разбор флагов,
// чтение дампа, соединения (БД всегда; Redis и /metrics — источники
// снапшота), сборка и запись файла.
func fixturesSnapshotCmd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("fixtures snapshot")
	dir := fs.String("dir", "", "каталог дампа фикстуры (ключи прогона; обязателен)")
	label := fs.String("label", "", "метка снапшота/прогона (обязательна; файл — <dir>/<label>.snapshot.json)")
	engine := fs.String("engine", "", "движок прогона: chromium|moli|lightpanda (обязателен)")
	metricsURL := fs.String("metrics", "", "URL /metrics render-worker стенда (пусто — без метрик этапа)")
	rssMax := fs.Int64("engine-rss-max", 0, "макс. RSS движка за прогон, байты (docker stats; 0 — не задан)")
	crashes := fs.Int64("engine-crashes", -1, "падения ядра движка за прогон (>= 0 — задано)")
	outPath := fs.String("out", "", "файл снапшота (по умолчанию <dir>/<label>.snapshot.json)")
	noDOM := fs.Bool("no-dom", false, "не читать DOM хранилища (без узлов/скриптов)")
	if handled, err := parseFlags(fs, fixturesHelp, args, stdout); handled {
		return err
	}
	if strings.TrimSpace(*dir) == "" {
		return usagef("укажите каталог дампа: crawlerctl fixtures snapshot --dir /path/to/fixtures")
	}
	if *label == "" || !fixturesLabelRe.MatchString(*label) {
		return usagef("укажите метку снапшота --label ИМЯ (буквы/цифры/._-, без разделителей пути)")
	}
	switch *engine {
	case config.EngineChromium, config.EngineMoli, config.EngineLightpanda:
	default:
		return usagef("fixtures snapshot: --engine: ожидается %s|%s|%s, задано %q",
			config.EngineChromium, config.EngineMoli, config.EngineLightpanda, *engine)
	}
	if *rssMax < 0 {
		return usagef("fixtures snapshot: --engine-rss-max: отрицательное значение %d", *rssMax)
	}
	if fs.NArg() > 0 {
		return usagef("fixtures snapshot: лишние аргументы %v (команда принимает только флаги)", fs.Args())
	}

	ctx, stop := signalCtx()
	defer stop()

	dump, err := fixturesReadDump(*dir)
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // бюджет соединений ТЗ 6.3 — у сервисов
	poolOpts.AppName = "crawlerctl" // метка в pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}
	defer pool.Close()

	keys := make([]fixturesKey, 0, len(dump.Results))
	for _, r := range dump.Results {
		keys = append(keys, fixturesKey{r.ZoneID, r.DomainID, r.SubdomainID, r.Variant})
	}
	rows, err := fixturesSnapshotReadRows(ctx, pool, keys)
	if err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}
	if len(rows) != len(keys) {
		// Строки дампа, отсутствующие в БД (удалены между выгрузкой и
		// прогоном), молча уменьшали бы знаменатель успешности — предупреждение
		// вместо незаметной усечки (ревью T-244 P2-4).
		out(stdout, "снапшот: предупреждение: в БД %d строк из %d ключей дампа — знаменатель успешности усечён\n",
			len(rows), len(keys))
	}

	snap := &fixturesSnapshot{
		FormatVersion: fixturesSnapshotVersion,
		Label:         *label,
		Engine:        *engine,
		CreatedAt:     time.Now().UTC(),
		Dump: fixturesSnapshotDump{
			Seed:   dump.Manifest.Seed,
			Keys:   len(dump.Results),
			Strata: dump.Manifest.Strata,
		},
		Rows: rows,
	}
	if *rssMax > 0 {
		v := *rssMax
		snap.EngineRSSMax = &v
	}
	if *crashes >= 0 {
		v := *crashes
		snap.EngineCrashes = &v
	}

	// Структурные метрики DOM: чтение хранилища тем же пулом (PackStore
	// ReadOnly — стиль crawlerctl domain --content). Недоступное
	// хранилище/контент — деградация (DOMParsed=false), не сбой снапшота.
	if !*noDOM {
		logger := slog.New(slog.NewTextHandler(stderr, nil))
		if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
			out(stdout, "снапшот: хранилище контента недоступно (%v) — без узлов/скриптов DOM\n", err)
		} else if store, err := content.OpenPackStore(pool, content.PackOptions{Dir: cfg.Content.Dir, ReadOnly: true}); err != nil {
			out(stdout, "снапшот: хранилище контента недоступно (%v) — без узлов/скриптов DOM\n", err)
		} else {
			content.WarnDictMismatch(store.Dir(), logger)
			if skipped := fixturesSnapshotParseDOM(ctx, store, snap.Rows); skipped > 0 {
				out(stdout, "снапшот: DOM не прочитан у %d строк (контент выметен/отсутствует) — без узлов/скриптов\n", skipped)
			}
			_ = store.Close()
		}
	}

	// Стримы Redis: границы q:analyze — темп; недоступность — деградация.
	rdb := queue.NewClient(cfg.Redis.Addr)
	if streams, err := fixturesSnapshotTakeStreams(ctx, rdb); err != nil {
		out(stdout, "снапшот: Redis недоступен (%v) — без темпа и остатков очередей\n", err)
	} else {
		snap.Streams = streams
	}
	if err := rdb.Close(); err != nil {
		out(stdout, "снапшот: закрытие Redis: %v\n", err)
	}

	// Метрики этапа: URL задан явно — недоступность это ошибка оператора.
	if *metricsURL != "" {
		m, err := fixturesSnapshotMetricsURL(ctx, *metricsURL)
		if err != nil {
			return fmt.Errorf("fixtures snapshot: %w", err)
		}
		snap.Metrics = m
	}

	path := *outPath
	if path == "" {
		path = filepath.Join(*dir, *label+fixturesSnapshotFileSuffix)
	}
	if err := fixturesWriteSnapshotFile(path, snap); err != nil {
		return fmt.Errorf("fixtures snapshot: %w", err)
	}

	rendered := 0
	unanalyzed := 0
	for _, r := range snap.Rows {
		if r.Rendered && r.HasContent {
			rendered++
			if r.RulesVersion == nil {
				unanalyzed++ // анализатор не прошёл по DOM прогона
			}
		}
	}
	out(stdout, "снапшот %s: движок %s, задач %d, успешных рендеров %d (%.2f%%)\n",
		*label, *engine, len(snap.Rows), rendered, pct(rendered, len(snap.Rows)))
	if unanalyzed > 0 {
		out(stdout, "  предупреждение: %d успешных рендеров без штампа анализатора (rules_version NULL) — вердикты по ним от прошлого анализа, диф детекции по ним деградирует\n", unanalyzed)
	}
	if snap.Streams != nil {
		out(stdout, "  q:analyze %d записей, q:render остаток %d, q:render-fallback %d, q:dead %d\n",
			snap.Streams.AnalyzeLen, snap.Streams.RenderLen, snap.Streams.FallbackLen, snap.Streams.DeadLen)
	}
	if snap.Metrics != nil {
		out(stdout, "  метрики: задач этапа %.0f, причин финализаций %d, тарпит/cert/dns %.0f/%.0f/%.0f\n",
			snap.Metrics.Tasks, len(snap.Metrics.TaskFinalized),
			snap.Metrics.TarpitFinalized, snap.Metrics.CertFinalized, snap.Metrics.DNSFinalized)
	}
	out(stdout, "снапшот записан: %s\n", path)
	return nil
}

// pct — доля в процентах (0 при делении на ноль).
func pct(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}
