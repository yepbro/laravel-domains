// Приёмочные тесты по критериям ТЗ §11.4 (T-094): каждый пункт
// спецификации «Критерии приёмки» — отдельный Test-кейс против живого
// dev-стека (PostgreSQL, Redis, unbound/coredns, testweb, mockserver,
// chromium). Suite собирает критерии в явную карту приёмки и не дублирует
// соседние файлы: их сценарии — источник покрытия деталей, здесь на
// каждый пункт §11.4 указана формулировка (комментарием над кейсом) и
// минимальный сценарий, закрывающий пробел:
//
//	п. 1  TestAccept01 — add/delete + IDN-зоны без дублей (детали: ingest_test)
//	п. 2  TestAccept02 — субдомены независимо, варианты/алиасы §5.5 (http_test)
//	п. 3  TestAccept03 — SPA: DOM и cookies; HTML; дедуп/сжатие; чтение
//	                     обратно (content_test, render live-тесты)
//	п. 4  TestAccept04 — анализ версий/score + reanalyze без скачивания
//	п. 5  TestAccept05 — выборки из laravel_domains/laravel_findings (e2e)
//	п. 6  TestAccept06 — прогресс зон обновляется автоматически + метрики
//	п. 7  — целевой сервер под нагрузкой 24 ч: вне dev-стенда, задача
//	         T-093 (заблокирована до развёртывания боевого железа)
//	п. 8  TestAccept08 — интервал перепроверки (по умолчанию 6 месяцев,
//	                     меняется конфигом)
//	п. 9  TestAccept09 — остановка сервиса: lease возвращает «застрявшие»
//	                     домены (e2e — перезапуск PostgreSQL, resilience —
//	                     потеря Redis и смерть воркера)
//	п. 10 TestAccept10 — поиск по имени в любой форме записи (schema_test —
//	                     контракт domain_info; crawlerctl — SQL поверх него)
//	п. 11 TestAccept11 — реальные образцы заказчика через конвейер
//
// Уровень — как у e2e_test.go (T-091): библиотечные вызовы компонентов
// всех этапов без запуска сервисов-процессов; каждая стадия гоняется тем
// же кодом, что и сборки cmd/*. Изоляция — изолированная схема
// (migratedSchema), Redis БД 15 (resilienceRedisEnv) с очисткой q:*,
// каталоги — t.TempDir.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/idn"
	"crawler/internal/ingest"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
	"crawler/internal/rules"
	"crawler/internal/scheduler"
)

// Группы потребителей стримов уникальны для suite (стиль e2e-констант):
// БД Redis 15 вычищается до/после, имена не пересекаются с соседями.
const (
	accDNSGroup     = "acc-dns"
	accHTTPGroup    = "acc-http"
	accAnalyzeGroup = "acc-an"
	accRenderGroup  = "acc-render"
	accZone         = "crawl.test"
	// accIDNZone/accIDNLabel — punycode-формы сайт.проверка.test
	// (зона и домен testbed, сценарий «проверка составных зон и IDN»).
	accIDNZone    = "xn--80adjurfhd.test"
	accIDNZoneUni = "проверка.test"
	accIDNLabel   = "xn--80aswg"
	accIDNLabelU  = "сайт"
	// accScoreTol — допуск сверки score с meta.yaml образцов (float4
	// в БД + округление ожидания до 3 знаков; как e2e/корпус T-075).
	accScoreTol = 0.001
	// accSamplesDir — корпус образцов заказчика относительно каталога
	// теста (в tools контейнер монтирует /testdata, а не переменную
	// окружения — путь от каталога, стиль rulesDir в e2e).
	accSamplesDir = "../../testdata/laravel-samples"
)

// accEnv — конвейер этапов приёмки на живом стеке (как e2eEnv, без
// обрыва PostgreSQL: устойчивость к перезапуску БД — территория e2e).
type accEnv struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client
	q        *queue.RedisQueue
	store    *content.PackStore
	saver    *httpworker.ContentSaver
	recorder *httpworker.Recorder
	hworker  *httpworker.Worker
	analyzer *analyzer.Worker
	rules    *rules.Set
}

// newAccEnv собирает окружение: схема с миграциями, очередь, хранилище
// контента и компоненты DNS/HTTP/analyze-этапов (связки — как в сборках
// cmd/*). Рендер-конвейер подключается точечно (TestAccept03).
func newAccEnv(t *testing.T) *accEnv {
	t.Helper()
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})

	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	saver, err := httpworker.NewContentSaver(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := httpworker.NewRecorder(pool, q, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Транспорт — как в сборке cmd/http-worker: резолв хостов редиректов
	// тем же резолвером DNS-этапа (ТЗ 5.5 п. 2).
	resolve := dnsEnv(t)
	fetcher := httpTestFetcher(t, func(o *httpfetch.Options) {
		o.Resolver = httpfetch.NewCachingResolver(
			httpworker.NewDNSLookup(resolve, false), httpfetch.CacheOptions{})
	})
	hworker, err := httpworker.New(httpworker.ChainFetch(fetcher), httpworker.Options{
		MaxRedirects: httpfetch.DefaultMaxRedirects,
	})
	if err != nil {
		t.Fatal(err)
	}

	loader, err := rules.NewReloader(accRulesDir(t), nil)
	if err != nil {
		t.Fatalf("набор правил: %v", err)
	}
	an, err := analyzer.New(pool, q, store, loader, analyzer.Options{
		MinScore: rules.DefaultMinScore,
	})
	if err != nil {
		t.Fatal(err)
	}

	return &accEnv{
		pool: pool, rdb: rdb, q: q, store: store,
		saver: saver, recorder: recorder,
		hworker: hworker, analyzer: an, rules: loader.Set(),
	}
}

// accResolver — клиент DNS к живому unbound стенда (dnsEnv); кэшируется
// на время теста, конвейер этапа берёт резолвер один раз.
func accResolver(t *testing.T) dnsclient.Resolver {
	t.Helper()
	return dnsEnv(t)
}

// accRulesDir — рабочий rules/: APP_RULES_DIR, если каталог существует
// (контейнеры сервисов), иначе путь от каталога теста к корню репозитория
// (в tools переменная унаследована, а сам каталог не смонтирован — урок
// T-091).
func accRulesDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("APP_RULES_DIR"); dir != "" {
		if _, err := os.Stat(dir); err == nil {
			return dir
		}
	}
	if _, err := os.Stat("../../rules"); err != nil {
		t.Fatalf("набор правил недоступен: %v", err)
	}
	return "../../rules"
}

// accModifiedRulesDir — копия рабочего набора с правилом-пробой,
// срабатывающей ровно на одном образце конвейера (symfony-negative):
// канонический хэш (Version) меняется, а reanalyze обязан переприменить
// правила — у образца-мишени появляется срабатывание пробы и растёт
// score, у остальных выводы прежние (TestAccept04).
const (
	// accProbeRule — id правила-пробы в копии набора.
	accProbeRule = "t094_reanalyze_probe"
	// accProbeSample — образец-мишень пробы: устойчивая подстрока его
	// HTML не встречается в остальных образцах и правилах корпуса.
	accProbeSample = "symfony-negative"
	// accProbeWeight — вес пробы: заметен в score (noisy-OR), но не
	// переводит слабый негатив через порог is_laravel 0.6.
	accProbeWeight = 0.05
)

func accModifiedRulesDir(t *testing.T) string {
	t.Helper()
	src := accRulesDir(t)
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("чтение %s: %v", src, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatalf("копия правила %s: %v", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), raw, 0o644); err != nil {
			t.Fatalf("копия правила %s: %v", e.Name(), err)
		}
	}
	probe := "rules:\n" +
		"  - id: " + accProbeRule + "\n" +
		"    description: приёмочная проба T-094 — срабатывает на образце " + accProbeSample + ", доказывая переприменение правил после reanalyze\n" +
		"    target: html\n" +
		"    regex: 'Hello Symfony!'\n" +
		"    weight: " + fmt.Sprintf("%g", accProbeWeight) + "\n"
	if err := os.WriteFile(filepath.Join(dst, "t094_probe.yaml"), []byte(probe), 0o644); err != nil {
		t.Fatalf("правило-проба: %v", err)
	}
	return dst
}

// ---------------------------------------------------------------- вставки

// accInsertZone — зона (unicode-имя для отображения, пустая строка → NULL).
func accInsertZone(t *testing.T, pool *pgxpool.Pool, name, unicode string) int16 {
	t.Helper()
	var id int16
	err := pool.QueryRow(context.Background(),
		"INSERT INTO zones (name, name_unicode) VALUES ($1, NULLIF($2, '')) RETURNING id",
		name, unicode,
	).Scan(&id)
	if err != nil {
		t.Fatalf("вставка зоны %s: %v", name, err)
	}
	return id
}

// accInsertDomains — pending-домены зоны (полный конвейер ingest —
// TestAccept01), возвращает id в порядке labels.
func accInsertDomains(t *testing.T, pool *pgxpool.Pool, zoneID int16, labels []string) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		"INSERT INTO domains (zone_id, label) SELECT $1, unnest($2::text[]) RETURNING id",
		zoneID, labels,
	)
	if err != nil {
		t.Fatalf("вставка доменов зоны %d: %v", zoneID, err)
	}
	defer rows.Close()
	ids := make([]int64, 0, len(labels))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("чтение id домена: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("вставка доменов зоны %d: %v", zoneID, err)
	}
	if len(ids) != len(labels) {
		t.Fatalf("вставлено доменов: %d, хочу %d", len(ids), len(labels))
	}
	return ids
}

// ---------------------------------------------------------------- конвейер

// accPipeline — домены зоны через диспетчер и DNS/HTTP-этапы (как
// соответствующие стадии e2e): pending → queued + q:dns → эпизоды DNS
// (маска субдоменов, q:http) → HTTP-эпизоды (subdomain_results,
// q:analyze). Возвращает карту «метка → id».
func accPipeline(ctx context.Context, t *testing.T, env *accEnv, zone string, labels []string) map[string]int64 {
	t.Helper()
	zoneID := accInsertZone(t, env.pool, zone, "")
	return accPipelineZone(ctx, t, env, zoneID, zone, labels)
}

// accPipelineZone — accPipeline для заранее созданной зоны (unicode-имя
// для отображения вставил вызывающий — поиск TestAccept10).
func accPipelineZone(ctx context.Context, t *testing.T, env *accEnv, zoneID int16, zone string, labels []string) map[string]int64 {
	t.Helper()
	ids := accInsertDomains(t, env.pool, zoneID, labels)

	batch, err := scheduler.DispatchBatch(ctx, env.pool, scheduler.DispatchQuery{Limit: len(labels)})
	if err != nil {
		t.Fatalf("выборка пачки: %v", err)
	}
	if len(batch) != len(labels) {
		t.Fatalf("выбрано доменов: %d, хочу %d", len(batch), len(labels))
	}
	if _, err := scheduler.EnqueueBatch(ctx, env.pool, env.q, batch); err != nil {
		t.Fatalf("постановка пачки: %v", err)
	}

	// DNS-этап — как processBatch воркера (T-039): XACK после записи.
	deliveries := consumeTasks(t, env.q, queue.StreamDNS, accDNSGroup, "acc", 5)
	if len(deliveries) != 1 {
		t.Fatalf("доставок q:dns: %d, хочу 1 (одна пачка)", len(deliveries))
	}
	err = queue.Processing(ctx, env.q, accDNSGroup, deliveries[0],
		func(c context.Context, d queue.Delivery) error {
			b, err := queue.Decode[queue.DNSBatch](d)
			if err != nil {
				return err
			}
			if len(b.Domains) != len(labels) {
				return fmt.Errorf("в пачке %d доменов, хочу %d", len(b.Domains), len(labels))
			}
			subs, err := dnsworker.LoadSubdomains(c, env.pool)
			if err != nil {
				return err
			}
			pending, err := dnsworker.PendingDomains(c, env.pool, b.Domains)
			if err != nil {
				return err
			}
			if len(pending) != len(labels) {
				return fmt.Errorf("актуальных доменов: %d, хочу %d", len(pending), len(labels))
			}
			resolve := accResolver(t)
			for _, dom := range pending {
				if err := dnsworker.ProcessDomain(c, env.pool, env.q, resolve, dom, subs, true, nil, nil); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("пачка q:dns: %v", err)
	}

	// HTTP-этап — как processDelivery воркера (T-053): эпизод → контент →
	// запись → XACK.
	tasks := consumeTasks(t, env.q, queue.StreamHTTP, accHTTPGroup, "acc", len(labels))
	if len(tasks) != len(labels) {
		t.Fatalf("задач q:http: %d, хочу %d", len(tasks), len(labels))
	}
	for _, d := range tasks {
		err := queue.Processing(ctx, env.q, accHTTPGroup, d,
			func(c context.Context, del queue.Delivery) error {
				task, err := queue.Decode[queue.HTTPTask](del)
				if err != nil {
					return err
				}
				res, err := env.hworker.ProcessDomain(c, task)
				if err != nil {
					return err
				}
				if err := env.saver.Save(c, res); err != nil {
					return err
				}
				recorded, err := env.recorder.RecordAndPublish(c, res)
				if err != nil {
					return err
				}
				if !recorded {
					return fmt.Errorf("домен %s: результат не записан (охрана записи, T-058)", task.Name)
				}
				return nil
			})
		if err != nil {
			t.Fatalf("HTTP-эпизод: %v", err)
		}
	}

	out := make(map[string]int64, len(labels))
	for i, l := range labels {
		out[l] = ids[i]
	}
	return out
}

// accAnalyze — стадия анализатора: n доставок q:analyze через воркер
// (как processDelivery воркера T-071). Анализатор передаётся явно:
// переанализ TestAccept04 работает вторым набором правил.
func accAnalyze(ctx context.Context, t *testing.T, env *accEnv, an *analyzer.Worker, n int) {
	t.Helper()
	msgs := consumeTasks(t, env.q, queue.StreamAnalyze, accAnalyzeGroup, "acc", n)
	if len(msgs) != n {
		t.Fatalf("доставок q:analyze: %d, хочу %d", len(msgs), n)
	}
	for _, d := range msgs {
		err := queue.Processing(ctx, env.q, accAnalyzeGroup, d,
			func(c context.Context, del queue.Delivery) error {
				res, err := queue.Decode[queue.Result](del)
				if err != nil {
					return err
				}
				processed, err := an.ProcessResult(c, res)
				if err != nil {
					return err
				}
				if !processed {
					return fmt.Errorf("строка %d/%d/%d/%d не найдена или перезаписана",
						res.ZoneID, res.DomainID, res.SubdomainID, int16(res.Variant))
				}
				return nil
			})
		if err != nil {
			t.Fatalf("анализ: %v", err)
		}
	}
}

// ---------------------------------------------------------------- читатели

// accResultRow — строка subdomain_results для проверок (порядок
// субдомен/вариант детерминирован).
type accResultRow struct {
	subdomainID int16
	variant     int16
	scheme      int16
	httpStatus  *int16
	finalURL    *string
	aliasKind   int16
	aliasSub    *int16
	rendered    bool
	contentHash []byte
	cookies     *string
	errCode     *int16
}

func accRows(t *testing.T, pool *pgxpool.Pool, domainID int64) []accResultRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT subdomain_id, variant, scheme, http_status, final_url, alias_kind,
		       alias_subdomain_id, rendered, content_hash, cookies::text, error
		FROM subdomain_results WHERE domain_id = $1
		ORDER BY subdomain_id, variant`, domainID)
	if err != nil {
		t.Fatalf("чтение subdomain_results: %v", err)
	}
	defer rows.Close()
	var out []accResultRow
	for rows.Next() {
		var r accResultRow
		if err := rows.Scan(&r.subdomainID, &r.variant, &r.scheme, &r.httpStatus, &r.finalURL,
			&r.aliasKind, &r.aliasSub, &r.rendered, &r.contentHash, &r.cookies, &r.errCode); err != nil {
			t.Fatalf("строка subdomain_results: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация subdomain_results: %v", err)
	}
	return out
}

// accFinding — строка laravel_findings.
type accFinding struct {
	sub          int16
	variant      int16
	score        float32
	verMin       *string
	verMax       *string
	isLaravel    bool
	signals      string
	rulesVersion int32
}

func accFindings(t *testing.T, pool *pgxpool.Pool, zoneID int16, domainID int64) []accFinding {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT subdomain_id, variant, score, ver_min, ver_max, is_laravel, signals::text, rules_version
		FROM laravel_findings WHERE zone_id = $1 AND domain_id = $2`, zoneID, domainID)
	if err != nil {
		t.Fatalf("чтение laravel_findings: %v", err)
	}
	defer rows.Close()
	var out []accFinding
	for rows.Next() {
		var f accFinding
		if err := rows.Scan(&f.sub, &f.variant, &f.score, &f.verMin, &f.verMax,
			&f.isLaravel, &f.signals, &f.rulesVersion); err != nil {
			t.Fatalf("строка laravel_findings: %v", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация laravel_findings: %v", err)
	}
	return out
}

// accAggregate — строка laravel_domains домена.
type accAggregate struct {
	score  float32
	verMin *string
	verMax *string
	mask   int64
	rulesV int32
}

func accLaravelDomain(t *testing.T, pool *pgxpool.Pool, zoneID int16, domainID int64) (accAggregate, bool) {
	t.Helper()
	var a accAggregate
	err := pool.QueryRow(context.Background(), `
		SELECT score, ver_min, ver_max, found_on_mask, rules_version
		FROM laravel_domains WHERE zone_id = $1 AND domain_id = $2`, zoneID, domainID,
	).Scan(&a.score, &a.verMin, &a.verMax, &a.mask, &a.rulesV)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, false
	}
	if err != nil {
		t.Fatalf("чтение laravel_domains: %v", err)
	}
	return a, true
}

// accStr — строка или NULL → "".
func accStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// accHash — content.Hash из слайса, прочитанного из БД.
func accHash(b []byte) content.Hash {
	var h content.Hash
	copy(h[:], b)
	return h
}

// accSignal — запись signals jsonb (T-071).
type accSignal struct {
	Rule string `json:"rule"`
}

// accFiredRules — множество id сработавших правил из signals jsonb.
func accFiredRules(t *testing.T, signals string) map[string]bool {
	t.Helper()
	var sigs []accSignal
	if err := json.Unmarshal([]byte(signals), &sigs); err != nil {
		t.Fatalf("signals jsonb: %v", err)
	}
	m := make(map[string]bool, len(sigs))
	for _, s := range sigs {
		m[s.Rule] = true
	}
	return m
}

// accWaitFor — poll до готовности или дедлайна (автоматические обновления
// TestAccept06).
func accWaitFor(t *testing.T, what string, deadline time.Duration, ready func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !ready() {
		if time.Now().After(end) {
			t.Fatalf("%s: не наступило за %s", what, deadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- п. 1

// TestAccept01_IngestAddDeleteIDN — §11.4 п. 1: «Файлы add/delete из
// наблюдаемой папки корректно применяются, в том числе для IDN-зон и
// доменов (без дублей зон в unicode/punycode формах)». Полный критерий
// одним сценарием через реальный конвейер ingest: add-файл в unicode-
// каталоге IDN-зоны, второй add в punycode-каталоге той же зоны (дубля
// зоны нет), delete-файл и возврат add-файлом тем же списком, undo
// первого add-файла тем же списком из delete/ (контракт дедупа T-113 —
// совпадение zone+sha256 при другом действии не дубль; детали —
// ingest_test).
func TestAccept01_IngestAddDeleteIDN(t *testing.T) {
	skipUnlessIntegration(t)
	env := newIngestEnv(t)

	// Первый add-файл сценария: его список в конце отменяется тем же
	// списком из delete/ (undo, T-113). Содержимое поделено переменной,
	// а не копией литерала — «тот же список» байт-точен по построению.
	firstAdd := fileContent(
		"# приёмка §11.4 п. 1",
		accIDNLabelU+"."+accIDNZoneUni, // сайт.проверка.test
		"магазин."+accIDNZoneUni,       // второй IDN-домен
	)

	t.Run("add в unicode-каталоге IDN-зоны", func(t *testing.T) {
		out := env.process(t, filepath.Join(accIDNZoneUni, "add", "unicode-form.txt"), firstAdd)
		if out.Kind != ingest.OutcomeApplied {
			t.Fatalf("итог файла = %q, хочу applied", out.Kind)
		}
		if out.Report.Added != 2 || out.Report.Existed != 0 {
			t.Errorf("счётчики: added=%d existed=%d, хочу 2/0", out.Report.Added, out.Report.Existed)
		}

		// Зона — одна, в punycode, с unicode-именем для отображения.
		z, ok := env.zone(t, accIDNZone)
		if !ok {
			t.Fatalf("зона %s не создана", accIDNZone)
		}
		if z.unicode != accIDNZoneUni || !z.isActive {
			t.Errorf("зона: unicode=%q active=%v, хочу %q/true", z.unicode, z.isActive, accIDNZoneUni)
		}

		// Домены — punycode-метки с битом is_idn (ТЗ 5.2 п. 9).
		for _, label := range []string{"xn--80aswg", "xn--80aairftm"} {
			d, ok := env.domain(t, accIDNZone, label)
			if !ok {
				t.Fatalf("домен %s.%s не создан", label, accIDNZone)
			}
			if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
				t.Errorf("%s: status/check_state = %d/%d, хочу new/pending", label, d.status, d.checkState)
			}
			if d.flags&int16(model.FlagIDN) == 0 {
				t.Errorf("%s: бит is_idn не выставлен", label)
			}
		}
		if !existsOnDisk(t, filepath.Join(env.dir, "processed", accIDNZoneUni, "add", "unicode-form.txt")) {
			t.Error("файл не перемещён в processed/")
		}
	})

	t.Run("punycode-каталог той же зоны — без дубля", func(t *testing.T) {
		out := env.process(t, filepath.Join(accIDNZone, "add", "punycode-form.txt"), fileContent(
			"XN--80ASWG.XN--80ADJURFHD.TEST.", // punycode, верхний регистр, точка — тот же домен
			accIDNLabel+"."+accIDNZone,        // каноничная punycode-форма
			"новый."+accIDNZoneUni,            // новый IDN-домен
		))
		if out.Kind != ingest.OutcomeApplied {
			t.Fatalf("итог файла = %q, хочу applied", out.Kind)
		}
		if out.Report.Added != 1 || out.Report.Existed != 1 {
			t.Errorf("счётчики: added=%d existed=%d, хочу 1/1 (новый + существующий)", out.Report.Added, out.Report.Existed)
		}

		var zones int
		if err := env.pool.QueryRow(context.Background(),
			"SELECT count(*) FROM zones WHERE name = $1", accIDNZone).Scan(&zones); err != nil {
			t.Fatal(err)
		}
		if zones != 1 {
			t.Errorf("строк зоны %s: %d, хочу 1 (unicode/punycode — одна зона)", accIDNZone, zones)
		}
		var total int
		if err := env.pool.QueryRow(context.Background(), "SELECT count(*) FROM domains").Scan(&total); err != nil {
			t.Fatal(err)
		}
		if total != 3 {
			t.Errorf("доменов в БД: %d, хочу 3", total)
		}
	})

	t.Run("delete и возврат add-файлом тем же списком", func(t *testing.T) {
		gone := fileContent(
			accIDNLabelU + "." + accIDNZoneUni, // сайт.проверка.test
		)
		out := env.process(t, filepath.Join(accIDNZoneUni, "delete", "gone.txt"), gone)
		if out.Kind != ingest.OutcomeApplied {
			t.Fatalf("итог delete = %q, хочу applied", out.Kind)
		}
		if out.Report.Deleted != 1 {
			t.Errorf("deleted = %d, хочу 1", out.Report.Deleted)
		}
		d, ok := env.domain(t, accIDNZone, accIDNLabel)
		if !ok {
			t.Fatal("удалённый домен обязан остаться в БД (§13.1)")
		}
		if d.status != int16(model.StatusDeleted) {
			t.Errorf("статус после delete = %d, хочу deleted", d.status)
		}

		// Возврат тем же списком байт-в-байт (тот же sha256, что у
		// delete-файла): действие другое — точной повторной доставки
		// нет, файл применяется (дедуп (zone_id, action, sha256),
		// T-113). До T-113 глобальный ключ по одному sha256 вынуждал
		// revive-файл уникальное содержимое (63da59c) — пиннинг снят,
		// то же содержимое другим действием легитимно.
		revive := env.process(t, filepath.Join(accIDNZoneUni, "add", "revive.txt"), gone)
		if revive.Kind != ingest.OutcomeApplied {
			t.Fatalf("итог возврата = %q, хочу applied (T-113: дубль sha256 delete-файла — не дубль доставки)", revive.Kind)
		}
		if revive.Report.Revived != 1 || revive.Report.Added != 1 {
			t.Errorf("возврат: added=%d revived=%d, хочу 1/1 (Added включает Revived — контракт T-021)", revive.Report.Added, revive.Report.Revived)
		}
		d, _ = env.domain(t, accIDNZone, accIDNLabel)
		if d.status != int16(model.StatusNew) || d.checkState != int16(model.CheckStatePending) {
			t.Errorf("после возврата: status/check_state = %d/%d, хочу new/pending", d.status, d.checkState)
		}
		if d.queuedAt != nil {
			t.Errorf("после возврата queued_at = %v, хочу NULL (снова в цикле проверок)", *d.queuedAt)
		}
	})

	t.Run("undo тем же списком из delete/ — отменяет первый add-файл", func(t *testing.T) {
		// Тот же список байт-в-байт, что и первый add-файл сценария
		// (unicode-form.txt): действие другое — точной повторной
		// доставки нет, файл ОБРАБАТЫВАЕТСЯ и удаляет домены списка
		// (контракт T-113; глобальный дедуп по одному sha256 skip-нул
		// бы его как дубль — семантика до T-113).
		undo := env.process(t, filepath.Join(accIDNZoneUni, "delete", "undo.txt"), firstAdd)
		if undo.Kind != ingest.OutcomeApplied {
			t.Fatalf("тот же список из delete/: итог = %q, хочу applied (T-113: другое действие — не дубль)", undo.Kind)
		}
		if undo.Report.Deleted != 2 {
			t.Errorf("undo: deleted=%d, хочу 2 (оба домена первого add-файла)", undo.Report.Deleted)
		}
		for _, label := range []string{accIDNLabel, "xn--80aairftm"} {
			if d, ok := env.domain(t, accIDNZone, label); !ok || d.status != int16(model.StatusDeleted) {
				t.Errorf("после undo: домен %s = %+v, хочу deleted (остался в БД, §13.1)", label, d)
			}
		}

		// Журнал: каждая доставка — своя строка; два содержимого
		// сценария доставлены дважды разными действиями (gone/revive,
		// unicode-form/undo) плюс одиночный punycode-файл — 5 строк,
		// 3 содержимых: уникальной остаётся тройка
		// (zone_id, action, sha256), не sha256 (T-113).
		var total, contents int
		if err := env.pool.QueryRow(context.Background(),
			"SELECT count(*), count(DISTINCT sha256) FROM ingest_files").Scan(&total, &contents); err != nil {
			t.Fatal(err)
		}
		if total != 5 || contents != 3 {
			t.Errorf("ingest_files: строк %d, содержим %d, хочу 5/3 (строка на доставку, не на содержимое; дублей точной доставки в сценарии нет)", total, contents)
		}
	})
}

// ---------------------------------------------------------------- п. 2

// TestAccept02_SubdomainsVariantsAliases — §11.4 п. 2: «Проверка по
// каждому субдомену выполняется независимо; результаты соответствуют
// правилам вариантов и алиасов (раздел 5.5)». Четыре сценария стенда
// через реальные DNS- и HTTP-этапы: same-site (апекс и www проверяются
// каждый сам по себе), different-sites (два варианта протоколов),
// equal-pair (алиас «идентичен»), www-to-bare (алиас «редирект»).
func TestAccept02_SubdomainsVariantsAliases(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{"same-site", "different-sites", "equal-pair", "www-to-bare"}
	ids := accPipeline(ctx, t, env, accZone, labels)

	// Всего записей: same-site 2 (апекс https + www http), different-sites 2
	// (варианты 0/1), equal-pair 2, www-to-bare 2 — по записи на каждую
	// проверку (§5.5 п. 7); столько же сообщений q:analyze.
	var totalRows int
	for _, l := range labels {
		n := len(accRows(t, env.pool, ids[l]))
		t.Logf("домен %s: записей %d", l, n)
		totalRows += n
	}
	if totalRows != 8 {
		t.Fatalf("записей subdomain_results по 4 доменам: %d, хочу 8", totalRows)
	}
	if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != int64(totalRows) {
		t.Errorf("сообщений q:analyze = %d, хочу %d (по записи на проверку)", n, totalRows)
	}

	t.Run("каждый субдомен — своя проверка (same-site)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["same-site"])
		if len(rows) != 2 {
			t.Fatalf("записей: %d, хочу 2 (апекс и www независимо)", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.subdomainID != 0 || www.subdomainID != 1 {
			t.Fatalf("порядок: subdomain_id = %d, %d, хочу 0, 1", apex.subdomainID, www.subdomainID)
		}
		// Апекс: протоколы сходятся на один сайт — единственная запись
		// variant 0, канонический https (ТЗ 5.5 п. 5).
		if apex.variant != int16(model.VariantMain) || apex.scheme != int16(model.SchemeHTTPS) {
			t.Errorf("апекс: variant/scheme = %d/%d, хочу 0/https", apex.variant, apex.scheme)
		}
		if accStr(apex.finalURL) != "https://same-site.crawl.test/" || apex.aliasKind != int16(model.AliasNone) {
			t.Errorf("апекс: url=%q alias=%d, хочу https://same-site…/ без алиаса", accStr(apex.finalURL), apex.aliasKind)
		}
		// www не настроен в Caddy: https отклоняется по SNI, http отвечает —
		// запись схемы своей проверки, а не «копия» апекса.
		if www.variant != int16(model.VariantMain) || www.scheme != int16(model.SchemeHTTP) {
			t.Errorf("www: variant/scheme = %d/%d, хочу 0/http (независимая проверка)", www.variant, www.scheme)
		}
		if accStr(www.finalURL) != "http://www.same-site.crawl.test/" {
			t.Errorf("www: итоговый URL = %q", accStr(www.finalURL))
		}
		// DNS-маска несёт оба субдомена (§5.4 п. 4).
		var mask int64
		if err := env.pool.QueryRow(ctx,
			"SELECT dns_mask FROM domains WHERE id = $1", ids["same-site"],
		).Scan(&mask); err != nil {
			t.Fatal(err)
		}
		if mask != 0b11 {
			t.Errorf("dns_mask = %b, хочу биты 0 и 1 (апекс и www живы)", mask)
		}
	})

	t.Run("варианты протоколов (different-sites, §5.5 п. 5)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["different-sites"])
		if len(rows) != 2 {
			t.Fatalf("записей: %d, хочу 2 (http и https — разные сайты)", len(rows))
		}
		if rows[0].subdomainID != 0 || rows[1].subdomainID != 0 {
			t.Fatalf("записи не апекс: %d, %d", rows[0].subdomainID, rows[1].subdomainID)
		}
		main, alt := rows[0], rows[1]
		if main.variant != int16(model.VariantMain) || main.scheme != int16(model.SchemeHTTPS) {
			t.Errorf("variant 0: scheme = %d, хочу https (канонический)", main.scheme)
		}
		if alt.variant != int16(model.VariantHTTP) || alt.scheme != int16(model.SchemeHTTP) {
			t.Errorf("variant 1: scheme = %d, хочу http", alt.scheme)
		}
		if accStr(main.finalURL) == accStr(alt.finalURL) {
			t.Errorf("итоговые URL совпали: %q — сайты обязаны быть разными", accStr(main.finalURL))
		}
		if got := accStr(alt.finalURL); got != "http://different-sites-alt.crawl.test/" {
			t.Errorf("итоговый URL http-варианта = %q, хочу http://different-sites-alt.crawl.test/ (редирект на хост-спутник)", got)
		}
		for i, r := range rows {
			if r.aliasKind != int16(model.AliasNone) || r.aliasSub != nil {
				t.Errorf("строка %d: алиас = %d/%v, хочу нет", i, r.aliasKind, r.aliasSub)
			}
		}
	})

	t.Run("алиас «идентичен» (equal-pair, §5.5 п. 6)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["equal-pair"])
		if len(rows) != 2 {
			t.Fatalf("записей: %d, хочу 2", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.subdomainID != 0 || www.subdomainID != 1 {
			t.Fatalf("порядок: %d, %d, хочу апекс, www", apex.subdomainID, www.subdomainID)
		}
		if apex.aliasKind != int16(model.AliasNone) {
			t.Errorf("апекс (оригинал): alias_kind = %d, хочу 0", apex.aliasKind)
		}
		if len(apex.contentHash) != 16 {
			t.Errorf("апекс: контент не сохранён (content_hash = %d байт)", len(apex.contentHash))
		}
		if www.aliasKind != int16(model.AliasIdentical) {
			t.Errorf("www: alias_kind = %d, хочу 1 (идентичен)", www.aliasKind)
		}
		if www.aliasSub == nil || *www.aliasSub != 0 {
			t.Errorf("www: alias_subdomain_id = %v, хочу 0 (оригинал — апекс)", www.aliasSub)
		}
		if www.contentHash != nil {
			t.Error("www-алиас хранит собственный контент — контент обязан быть один (у оригинала)")
		}
	})

	t.Run("алиас «редирект» (www-to-bare, §5.5 п. 6)", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["www-to-bare"])
		if len(rows) != 2 {
			t.Fatalf("записей: %d, хочу 2", len(rows))
		}
		apex, www := rows[0], rows[1]
		if apex.aliasKind != int16(model.AliasNone) || len(apex.contentHash) != 16 {
			t.Errorf("апекс: alias=%d контент=%d байт, хочу оригинал с контентом", apex.aliasKind, len(apex.contentHash))
		}
		if www.aliasKind != int16(model.AliasRedirect) {
			t.Errorf("www: alias_kind = %d, хочу 2 (редирект на оригинал)", www.aliasKind)
		}
		if www.aliasSub == nil || *www.aliasSub != 0 {
			t.Errorf("www: alias_subdomain_id = %v, хочу 0 (апекс)", www.aliasSub)
		}
		// Запись-алиас сходится на адрес оригинала (ТЗ 5.5 п. 6).
		if want := "https://www-to-bare.crawl.test/"; accStr(www.finalURL) != want {
			t.Errorf("www: итоговый URL = %q, хочу %q", accStr(www.finalURL), want)
		}
		if www.contentHash != nil {
			t.Error("www-алиас хранит собственный контент")
		}
	})

	// Итог эпизодов — alive/done (§7).
	for _, l := range labels {
		st := httpDomainState(t, env.pool, ids[l])
		if st.status != model.StatusAlive || st.checkState != int16(model.CheckStateDone) {
			t.Errorf("%s: status/check_state = %s/%d, хочу alive/done", l, st.status, st.checkState)
		}
	}
}

func accZoneID(t *testing.T, pool *pgxpool.Pool, zone string) int16 {
	t.Helper()
	var id int16
	if err := pool.QueryRow(context.Background(),
		"SELECT id FROM zones WHERE name = $1", zone).Scan(&id); err != nil {
		t.Fatalf("зона %s: %v", zone, err)
	}
	return id
}

// ---------------------------------------------------------------- п. 3

// accHTTPRenderer — адаптер рендера для testbed: навигация по http вместо
// https. Внутренний CA Caddy не доверен Chromium dev-стека (прод-режим
// рендерит сайты с публичными сертификатами), поэтому конвейер
// renderworker реален целиком (чтение цели → рендер → Put DOM → замена
// ссылки → публикация q:analyze), подменяется только схема навигации —
// та же страница, тот же JS (живые тесты рендера ходят так же,
// internal/render/*_live_test.go).
type accHTTPRenderer struct {
	inner renderworker.Renderer
}

func (r accHTTPRenderer) Run(ctx context.Context, task render.RenderTask) (render.RenderOutput, error) {
	if u, err := url.Parse(task.URL); err == nil && u.Scheme == "https" {
		u.Scheme = "http"
		task.URL = u.String()
	}
	return r.inner.Run(ctx, task)
}

// accChromiumWS — адрес CDP внешнего Chromium (как live-тесты рендера:
// CRAWLER_RENDER_REMOTE_URL → CHROMIUM_WS_URL → адрес стека).
func accChromiumWS() string {
	for _, key := range []string{"CRAWLER_RENDER_REMOTE_URL", "CHROMIUM_WS_URL"} {
		if u := os.Getenv(key); u != "" {
			return u
		}
	}
	return "ws://172.28.100.14:9222"
}

// accMonitorMetricsURL — адрес /metrics реального сервиса monitor: имя
// compose-сервиса через docker DNS (его скрейбит Prometheus dev-стенда,
// deploy/docker/prometheus/prometheus.yml), переопределяется окружением.
func accMonitorMetricsURL() string {
	if u := os.Getenv("CRAWLER_MONITOR_METRICS_URL"); u != "" {
		return u
	}
	return "http://monitor:9100/metrics"
}

// TestAccept03_SPADomCookiesContent — §11.4 п. 3: «Для SPA-страниц
// сохраняется итоговый DOM и cookies; для остальных — HTML; дедупликация
// и сжатие работают; контент можно прочитать обратно». SPA-сценарий
// стенда проходит HTTP-этап с рендер-конвейером (q:render) и
// render-worker на живом Chromium; парковки проверяют дедупликацию,
// сохранённые блоки — сжатие (len < raw_len), чтение — roundtrip.
func TestAccept03_SPADomCookiesContent(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Рендер-конвейер записи (T-066): рекордер с очередью рендера.
	gate := queue.NewBackpressureGate(env.rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(env.q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := httpworker.NewRenderRecorder(env.pool, env.q, renderQ, nil)
	if err != nil {
		t.Fatal(err)
	}
	env.recorder = rec

	ids := accPipeline(ctx, t, env, accZone, []string{"spa-shell", "parked-1", "parked-2"})

	t.Run("не-SPA: HTML сохраняется, дедуп и сжатие", func(t *testing.T) {
		p1 := accRows(t, env.pool, ids["parked-1"])
		p2 := accRows(t, env.pool, ids["parked-2"])
		if len(p1) != 1 || len(p2) != 1 {
			t.Fatalf("записей парковок: %d/%d, хочу 1/1", len(p1), len(p2))
		}
		h1, h2 := accHash(p1[0].contentHash), accHash(p2[0].contentHash)
		if h1 != h2 {
			t.Fatal("идентичные парковки дали разные content_hash — дедупликация не работает")
		}
		// Одна ссылка у каждой записи — refcount = 2 на общий блок.
		row, ok := contentRowByHash(t, env.pool, h1)
		if !ok {
			t.Fatal("контент парковок не записан в contents")
		}
		if row.refc != 2 {
			t.Errorf("refcount общего блока = %d, хочу 2 (две записи, один блок)", row.refc)
		}
		// Сжатие: блок в паке меньше исходных байт (zstd, T-041).
		if row.clen >= row.rawLen {
			t.Errorf("сжатие не работает: len=%d >= raw_len=%d", row.clen, row.rawLen)
		}
		// Контент читается обратно (roundtrip).
		body, err := env.store.Get(ctx, h1)
		if err != nil {
			t.Fatalf("чтение контента: %v", err)
		}
		if !strings.Contains(string(body), "Future home of something quite cool") {
			t.Errorf("контент не похож на парковку: %.80q", body)
		}
		if p1[0].rendered {
			t.Error("не-SPA запись обязана остаться rendered = false (HTML, не DOM)")
		}
	})

	t.Run("SPA: HTTP-этап публикует q:render", func(t *testing.T) {
		rows := accRows(t, env.pool, ids["spa-shell"])
		if len(rows) != 1 {
			t.Fatalf("записей spa-shell: %d, хочу 1", len(rows))
		}
		if rows[0].rendered {
			t.Error("до рендера rendered = false (сырой HTML)")
		}
		if n := streamLen(t, env.rdb, queue.StreamRender); n != 1 {
			t.Fatalf("сообщений q:render = %d, хочу 1 (SPA-вердикт T-057)", n)
		}
		// Флаг render_pending не выставлен: ёмкость очереди была.
		var flags int16
		if err := env.pool.QueryRow(ctx,
			"SELECT flags FROM domains WHERE id = $1", ids["spa-shell"],
		).Scan(&flags); err != nil {
			t.Fatal(err)
		}
		if flags&int16(model.FlagRenderPending) != 0 {
			t.Error("бит render_pending выставлен вопреки свободной очереди рендера")
		}
	})

	t.Run("render-worker: итоговый DOM и cookies", func(t *testing.T) {
		before := accRows(t, env.pool, ids["spa-shell"])[0]
		rawHash := accHash(before.contentHash)
		analyzeBefore := streamLen(t, env.rdb, queue.StreamAnalyze)

		browser, err := render.StartBrowser(context.Background(), render.Options{
			RemoteURL:         accChromiumWS(),
			Contexts:          1,
			Timeout:           30 * time.Second,
			RestartEveryPages: 1000,
		})
		if err != nil {
			t.Fatalf("подключение к Chromium: %v", err)
		}
		t.Cleanup(browser.Close)
		pool := render.NewPool(browser)
		t.Cleanup(pool.Close)
		rworker, err := renderworker.New(env.pool, env.q, env.store, accHTTPRenderer{pool}, renderworker.Options{})
		if err != nil {
			t.Fatal(err)
		}

		deliveries := consumeTasks(t, env.q, queue.StreamRender, accRenderGroup, "acc", 1)
		if len(deliveries) != 1 {
			t.Fatalf("доставок q:render: %d, хочу 1", len(deliveries))
		}
		err = queue.Processing(ctx, env.q, accRenderGroup, deliveries[0],
			func(c context.Context, d queue.Delivery) error {
				res, err := queue.Decode[queue.Result](d)
				if err != nil {
					return err
				}
				processed, err := rworker.ProcessResult(c, res)
				if err != nil {
					return err
				}
				if !processed {
					return errors.New("рендер не записан (строка ушла из-под задачи)")
				}
				return nil
			})
		if err != nil {
			t.Fatalf("рендер spa-shell: %v", err)
		}

		after := accRows(t, env.pool, ids["spa-shell"])[0]
		if !after.rendered {
			t.Error("rendered не выставлен — DOM не заменил HTML")
		}
		newHash := accHash(after.contentHash)
		if newHash == rawHash {
			t.Error("content_hash не изменился — записан сырой HTML вместо DOM")
		}
		// Итоговый DOM: контент после JS, которого в сыром HTML нет.
		dom, err := env.store.Get(ctx, newHash)
		if err != nil {
			t.Fatalf("чтение DOM из хранилища: %v", err)
		}
		if !strings.Contains(string(dom), "SPA-контент после рендера") {
			t.Errorf("DOM без контента, нарисованного JS: %.120q", dom)
		}
		// Cookies: JS выставил куку — CDP снимает всё хранилище (ТЗ 5.6).
		if after.cookies == nil || !strings.Contains(*after.cookies, `"js_session"`) {
			t.Errorf("cookies рендера без js_session: %v", after.cookies)
		}
		// Сырой HTML терял ссылку: блок мёртв (T-044), Get его не отдаёт.
		if _, err := env.store.Get(ctx, rawHash); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("чтение старого HTML после рендера: %v, хочу ErrNotFound (ссылка снята)", err)
		}
		// Публикация q:analyze после замены контента (§7).
		if n := streamLen(t, env.rdb, queue.StreamAnalyze); n != analyzeBefore+1 {
			t.Errorf("q:analyze после рендера: %d, хочу %d (+1)", n, analyzeBefore+1)
		}
	})
}

// ---------------------------------------------------------------- п. 4

// TestAccept04_ReanalyzeNoRefetch — §11.4 п. 4: «…изменение правил +
// reanalyze обновляет результаты без повторного скачивания». Анализ
// рабочих образцов первым набором → правила меняются (добавляется проба,
// срабатывающая на одном из образцов) → ReanalyzeOnce публикует
// q:analyze → воркер со вторым набором перезаписывает findings: у
// образца-мишени появляются срабатывание пробы и новый score (правила
// реально переприменены, а не перештампована rules_version), у остальных
// выводы прежние. Следы повторного скачивания проверяются отсутствием:
// эпизоды HTTP не запускаются, строки результатов не меняются
// (checked_at, content_hash), хранилище не растёт.
func TestAccept04_ReanalyzeNoRefetch(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{"laravel-11-vite", "symfony-negative"}
	ids := accPipeline(ctx, t, env, accZone, labels)
	zoneID := accZoneID(t, env.pool, accZone)
	accAnalyze(ctx, t, env, env.analyzer, len(labels))

	v1 := env.rules.Version
	if v1 == 0 {
		t.Fatal("версия рабочего набора не должна быть нулевой")
	}

	// Снимок «до»: строки результатов и хранилище.
	type rowSnap struct {
		checked time.Time
		hash    []byte
		version int32
	}
	before := map[int64][]rowSnap{}
	for _, l := range labels {
		rows, err := env.pool.Query(ctx, `
			SELECT checked_at, content_hash, rules_version FROM subdomain_results
			WHERE domain_id = $1 ORDER BY subdomain_id, variant`, ids[l])
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var s rowSnap
			if err := rows.Scan(&s.checked, &s.hash, &s.version); err != nil {
				t.Fatal(err)
			}
			before[ids[l]] = append(before[ids[l]], s)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	var contentsBefore int
	if err := env.pool.QueryRow(ctx, "SELECT count(*) FROM contents").Scan(&contentsBefore); err != nil {
		t.Fatal(err)
	}
	dnsBefore := streamLen(t, env.rdb, queue.StreamDNS)
	httpBefore := streamLen(t, env.rdb, queue.StreamHTTP)
	renderBefore := streamLen(t, env.rdb, queue.StreamRender)

	// Правила изменились: копия набора с пробой, срабатывающей на
	// symfony-negative, — канонический хэш (Version) другой, у мишени
	// должен появиться новый сигнал и вырасти score.
	loader2, err := rules.NewReloader(accModifiedRulesDir(t), nil)
	if err != nil {
		t.Fatalf("изменённый набор правил: %v", err)
	}
	v2 := loader2.Set().Version
	if v2 == v1 {
		t.Fatal("версия набора не изменилась — reanalyze не найдёт устаревших строк")
	}
	an2, err := analyzer.New(env.pool, env.q, env.store, loader2, analyzer.Options{
		MinScore: rules.DefaultMinScore,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := analyzer.ReanalyzeOnce(ctx, env.pool, env.q, v2, nil, analyzer.ReanalyzeOptions{})
	if err != nil {
		t.Fatalf("ReanalyzeOnce: %v", err)
	}
	// Выборка — устаревшие строки (rules_version IS DISTINCT FROM v2).
	if res.Selected != len(labels) || res.Published != len(labels) {
		t.Fatalf("переанализ: selected=%d published=%d, хочу %d/%d", res.Selected, res.Published, len(labels), len(labels))
	}

	// Воркер второго набора перепотребляет q:analyze.
	accAnalyze(ctx, t, env, an2, len(labels))

	// findings перезаписаны новой версией; правила реально переприменены:
	// у образца-мишени пробы срабатывание появилось и score пересчитан
	// (noisy-OR с весом пробы), у остальных выводы прежние. Заглушка
	// «обновили только штамп rules_version, переиспользовав signals/score»
	// здесь падает.
	for _, l := range labels {
		fs := accFindings(t, env.pool, zoneID, ids[l])
		sample := accSampleByID(t, l)
		// Строка findings — одна на (домен, субдомен, вариант): срабатывания
		// в signals jsonb, отдельной строки на правило нет (T-071, §6.1).
		if len(fs) != 1 {
			t.Errorf("%s: findings после переанализа = %d строк, хочу 1", l, len(fs))
			continue
		}
		fired := accFiredRules(t, fs[0].signals)
		wantScore := *sample.Expect.Score
		wantFired := len(sample.Expect.Rules)
		if l == accProbeSample {
			wantFired++
			wantScore = 1 - (1-*sample.Expect.Score)*(1-accProbeWeight)
			if !fired[accProbeRule] {
				t.Errorf("%s: в signals нет пробы %s — правила не переприменены (перештамповка rules_version?)", l, accProbeRule)
			}
		} else if fired[accProbeRule] {
			t.Errorf("%s: проба %s сработала вне образца-мишени %s", l, accProbeRule, accProbeSample)
		}
		if got := len(fired); got != wantFired {
			t.Errorf("%s: срабатываний в signals = %d (%v), хочу %d", l, got, fired, wantFired)
		}
		for _, f := range fs {
			if f.rulesVersion != v2 {
				t.Errorf("%s: rules_version срабатывания = %d, хочу %d (новый набор)", l, f.rulesVersion, v2)
			}
			if math.Abs(float64(f.score)-wantScore) > accScoreTol {
				t.Errorf("%s: score после переанализа = %.4f, хочу ≈%.3f", l, f.score, wantScore)
			}
		}
	}

	// Без повторного скачивания: строки не менялись вовсе, кроме штампа.
	for _, l := range labels {
		rows, err := env.pool.Query(ctx, `
			SELECT checked_at, content_hash, rules_version FROM subdomain_results
			WHERE domain_id = $1 ORDER BY subdomain_id, variant`, ids[l])
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for rows.Next() {
			var s rowSnap
			if err := rows.Scan(&s.checked, &s.hash, &s.version); err != nil {
				t.Fatal(err)
			}
			if i >= len(before[ids[l]]) {
				t.Fatalf("%s: строк после переанализа больше, чем до", l)
			}
			want := before[ids[l]][i]
			if !s.checked.Equal(want.checked) {
				t.Errorf("%s: checked_at изменился (%v → %v) — повторная проверка?", l, want.checked, s.checked)
			}
			if string(s.hash) != string(want.hash) {
				t.Errorf("%s: content_hash изменился — контент перекачан", l)
			}
			if s.version != v2 {
				t.Errorf("%s: rules_version строки = %d, хочу %d", l, s.version, v2)
			}
			i++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if i != len(before[ids[l]]) {
			t.Errorf("%s: строк %d, хочу %d", l, i, len(before[ids[l]]))
		}
	}
	if n := streamLen(t, env.rdb, queue.StreamDNS); n != dnsBefore {
		t.Errorf("q:dns вырос: %d → %d — DNS-этап запускался", dnsBefore, n)
	}
	if n := streamLen(t, env.rdb, queue.StreamHTTP); n != httpBefore {
		t.Errorf("q:http вырос: %d → %d — скачивание повторялось", httpBefore, n)
	}
	if n := streamLen(t, env.rdb, queue.StreamRender); n != renderBefore {
		t.Errorf("q:render изменился: %d → %d", renderBefore, n)
	}
	var contentsAfter int
	if err := env.pool.QueryRow(ctx, "SELECT count(*) FROM contents").Scan(&contentsAfter); err != nil {
		t.Fatal(err)
	}
	if contentsAfter != contentsBefore {
		t.Errorf("хранилище выросло: %d → %d блоков — контент перекачан", contentsBefore, contentsAfter)
	}
}

// ---------------------------------------------------------------- п. 5

// TestAccept05_LaravelTablesQueries — §11.4 п. 5: «Таблица laravel_domains
// / laravel_findings позволяет получить: список доменов с Laravel,
// субдомены, версию (диапазон), признаки». Выборки SQL из таблиц после
// конвейера: уверенный домен, слабый негатив (findings без aggregate),
// чистый негатив (без findings вовсе).
func TestAccept05_LaravelTablesQueries(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labels := []string{"laravel-8-mix", "rails-negative", "django-negative"}
	ids := accPipeline(ctx, t, env, accZone, labels)
	zoneID := accZoneID(t, env.pool, accZone)
	accAnalyze(ctx, t, env, env.analyzer, len(labels))

	t.Run("список доменов с Laravel", func(t *testing.T) {
		rows, err := env.pool.Query(ctx, `
			SELECT d.label FROM laravel_domains AS ld
			JOIN domains AS d ON d.zone_id = ld.zone_id AND d.id = ld.domain_id
			WHERE d.zone_id = $1 ORDER BY d.label`, zoneID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var label string
			if err := rows.Scan(&label); err != nil {
				t.Fatal(err)
			}
			got = append(got, label)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		// Порог §13.1: уверенный образец даёт строку, слабые — нет.
		if len(got) != 1 || got[0] != "laravel-8-mix" {
			t.Errorf("домены с Laravel = %v, хочу [laravel-8-mix]", got)
		}
	})

	t.Run("субдомены с признаками (found_on_mask ↔ справочник)", func(t *testing.T) {
		// Расшифровка found_on_mask битами справочника subdomains —
		// субдомены, на которых найден Laravel (ТЗ 5.8).
		var labelsOut []string
		rows, err := env.pool.Query(ctx, `
			SELECT s.label FROM laravel_domains AS ld
			JOIN subdomains AS s ON (ld.found_on_mask & (1 << s.id)) <> 0
			WHERE ld.zone_id = $1 AND ld.domain_id = $2
			ORDER BY s.id`, zoneID, ids["laravel-8-mix"])
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var l string
			if err := rows.Scan(&l); err != nil {
				t.Fatal(err)
			}
			labelsOut = append(labelsOut, l)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(labelsOut) != 1 || labelsOut[0] != "" {
			t.Errorf("субдомены с признаками = %q, хочу пустую метку (апекс, бит 0)", labelsOut)
		}
	})

	t.Run("версия (диапазон) и признаки уверенного домена", func(t *testing.T) {
		agg, ok := accLaravelDomain(t, env.pool, zoneID, ids["laravel-8-mix"])
		if !ok {
			t.Fatal("laravel_domains без строки уверенного домена")
		}
		// Диапазон образца: Mix — до 9.x (meta.yaml, границы — строки правил).
		if got := accStr(agg.verMin); got != "" {
			t.Errorf("ver_min = %q, хочу NULL (нижней границы нет)", got)
		}
		if got := accStr(agg.verMax); got != "9.x" {
			t.Errorf("ver_max = %q, хочу 9.x", got)
		}
		fs := accFindings(t, env.pool, zoneID, ids["laravel-8-mix"])
		if len(fs) != 1 {
			t.Fatalf("findings = %d строк, хочу 1 (апекс, variant 0)", len(fs))
		}
		fired := accFiredRules(t, fs[0].signals)
		want := []string{
			"cookie_xsrf_token", "cookie_laravel_session", "cookie_session_suffix",
			"cookie_encrypted_payload", "meta_csrf_token", "mix_assets",
		}
		for _, r := range want {
			if !fired[r] {
				t.Errorf("признак %s отсутствует (сработали %v)", r, fired)
			}
		}
		if len(fired) != len(want) {
			t.Errorf("сработавших признаков %d, хочу %d (%v)", len(fired), len(want), fired)
		}
		if !fs[0].isLaravel {
			t.Error("is_laravel уверенного срабатывания = false, хочу true (порог 0.6)")
		}
	})

	t.Run("слабые срабатывания — только findings", func(t *testing.T) {
		// rails: csrf-meta + *_session ниже порога — все срабатывания в
		// laravel_findings (§13.1), строки laravel_domains нет.
		fs := accFindings(t, env.pool, zoneID, ids["rails-negative"])
		if len(fs) != 1 {
			t.Fatalf("findings rails = %d строк, хочу 1", len(fs))
		}
		if fs[0].isLaravel {
			t.Error("слабое срабатывание помечено is_laravel = true")
		}
		fired := accFiredRules(t, fs[0].signals)
		if !fired["meta_csrf_token"] || !fired["cookie_session_suffix"] {
			t.Errorf("слабые признаки rails = %v, хочу csrf-meta и *_session", fired)
		}
		if _, ok := accLaravelDomain(t, env.pool, zoneID, ids["rails-negative"]); ok {
			t.Error("слабый домен попал в laravel_domains — порог 0.6 нарушен")
		}

		// django: ни одного срабатывания — ни findings, ни aggregate.
		if fs := accFindings(t, env.pool, zoneID, ids["django-negative"]); len(fs) != 0 {
			t.Errorf("findings чистого негатива = %d строк, хочу 0", len(fs))
		}
		if _, ok := accLaravelDomain(t, env.pool, zoneID, ids["django-negative"]); ok {
			t.Error("чистый негатив попал в laravel_domains")
		}
	})
}

// ---------------------------------------------------------------- п. 6

// TestAccept06_ZoneProgressAutomatic — §11.4 п. 6: «Прогресс по зонам
// (всего / проверено) отображается и обновляется автоматически; метрики
// доступны в Prometheus». Счётчики зон пересчитывает monitor
// (ZoneStatsOnce/ZoneStatsLoop — тот же код, что сервис); отображение —
// кэш zones.domains_total/domains_checked, который читает crawlerctl
// status/zones; метрики — экспозиция /metrics реестра сервиса (то, что
// скрейбит Prometheus dev-стека).
func TestAccept06_ZoneProgressAutomatic(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Данные: зона с 4 живыми (2 done, 1 pending, 1 queued) и 1 удалённым;
	// пустая зона — её счётчики обнуляются, а не застывают.
	zoneA := accInsertZone(t, pool, "acc6.test", "")
	zoneB := accInsertZone(t, pool, "acc6-empty.test", "")
	done := accInsertDomains(t, pool, zoneA, []string{"d1", "d2"})
	accInsertDomains(t, pool, zoneA, []string{"p1"})
	accInsertDomains(t, pool, zoneA, []string{"q1"})
	gone := accInsertDomains(t, pool, zoneA, []string{"g1"})
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET check_state = %d WHERE id = ANY($1)`, int16(model.CheckStateDone)), done,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET check_state = %d, queued_at = now() WHERE zone_id = $1 AND label = 'q1'`,
		int16(model.CheckStateQueued)), zoneA,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET status = %d, check_state = %d WHERE id = $1`,
		int16(model.StatusDeleted), int16(model.CheckStateDone)), gone[0],
	); err != nil {
		t.Fatal(err)
	}

	res, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res.Zones != 2 {
		t.Errorf("пересчитано зон: %d, хочу 2", res.Zones)
	}
	accZoneCounters := func(zone int16) (total, checked int32, updated bool) {
		t.Helper()
		var u *time.Time
		if err := pool.QueryRow(ctx,
			"SELECT domains_total, domains_checked, updated_at FROM zones WHERE id = $1", zone,
		).Scan(&total, &checked, &u); err != nil {
			t.Fatal(err)
		}
		return total, checked, u != nil
	}
	total, checked, updated := accZoneCounters(zoneA)
	if total != 4 || checked != 2 {
		t.Errorf("зона A: total/checked = %d/%d, хочу 4/2 (deleted вне знаменателя, queued не проверен)", total, checked)
	}
	if !updated {
		t.Error("updated_at не проставлен — алерт «остановка прогресса» (T-080) ослепнет")
	}
	if total, checked, _ := accZoneCounters(zoneB); total != 0 || checked != 0 {
		t.Errorf("пустая зона: total/checked = %d/%d, хочу 0/0", total, checked)
	}

	// Отображение: кэш сходится с прямым агрегатом (запрос crawlerctl
	// status по живым доменам — «всего/проверено» из одних данных).
	var liveTotal, liveChecked int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT count(*), count(*) FILTER (WHERE check_state = %d)
		FROM domains WHERE zone_id = $1 AND status <> %d`,
		int16(model.CheckStateDone), int16(model.StatusDeleted)), zoneA,
	).Scan(&liveTotal, &liveChecked); err != nil {
		t.Fatal(err)
	}
	if int32(liveTotal) != total || int32(liveChecked) != checked {
		t.Errorf("кэш %d/%d расходится с агрегатом %d/%d", total, checked, liveTotal, liveChecked)
	}

	// Обновляется автоматически: цикл монитор-сервиса с коротким
	// интервалом сам подхватывает изменение состояния (первый проход —
	// сразу, дальше по тику; тот же ZoneStatsLoop, что в cmd/monitor).
	loopCtx, stop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- monitor.ZoneStatsLoop(loopCtx, pool, monitor.ZoneStatsOptions{Interval: 100 * time.Millisecond})
	}()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET check_state = %d WHERE zone_id = $1 AND label = 'p1'`,
		int16(model.CheckStateDone)), zoneA,
	); err != nil {
		t.Fatal(err)
	}
	accWaitFor(t, "автоматическое обновление checked 2 → 3", 5*time.Second, func() bool {
		_, checked, _ := accZoneCounters(zoneA)
		return checked == 3
	})
	stop()
	if err := <-loopDone; err != nil {
		t.Errorf("ZoneStatsLoop завершился с ошибкой: %v", err)
	}

	// Метрики: реальная экспозиция работающего сервиса monitor — то, что
	// скрейбит Prometheus dev-стенда (docker DNS monitor:9100, как в
	// deploy/docker/prometheus/prometheus.yml). HTTP-запрос к сервису,
	// а не локальный реестр: забытый маршрут, неверный порт или молчащий
	// хендлер здесь падают по вине продового кода. Если сервис приложения
	// не поднят (make test-integration запускает только инфраструктуру
	// и testbed) — честный пропуск, а не тавтология «свой реестр → свой
	// хендлер».
	url := accMonitorMetricsURL()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Skipf("сервис monitor недоступен (%s: %v) — поднимите профиль app (make up): реальная экспозиция /metrics требует работающего сервиса", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("чтение %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics сервиса monitor: код %d, хочу 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), `crawler_service_up{service="monitor"} 1`) {
		t.Errorf("экспозиция monitor без crawler_service_up{service=\"monitor\"} 1:\n%s", string(body))
	}
}

// ---------------------------------------------------------------- п. 8

// TestAccept08_RecheckInterval — §11.4 п. 8: «Повторная проверка домена
// происходит не чаще заданного интервала (по умолчанию 6 месяцев);
// значение меняется конфигом». Домены разного возраста против живой БД:
// перевод done → pending берёт только созревших, повторный проход пуст
// (не чаще), уменьшение интервала конфигом подхватывает более свежие.
func TestAccept08_RecheckInterval(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	zone := accInsertZone(t, pool, "acc8.test", "")
	accInsertDomains(t, pool, zone, []string{"old", "fresh", "never", "pending", "gone"})
	set := func(label string, q string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("домен %s: %v", label, err)
		}
	}
	alive, done, del := int16(model.StatusAlive), int16(model.CheckStateDone), int16(model.StatusDeleted)
	set("old", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2025-04-01' WHERE zone_id = $1 AND label = 'old'`, alive, done), zone)
	set("fresh", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2026-10-01' WHERE zone_id = $1 AND label = 'fresh'`, alive, done), zone)
	set("never", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d WHERE zone_id = $1 AND label = 'never'`, alive, done), zone)
	set("pending", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2025-04-01' WHERE zone_id = $1 AND label = 'pending'`, alive, int16(model.CheckStatePending)), zone)
	set("gone", fmt.Sprintf(`UPDATE domains SET status = %d, check_state = %d, last_checked_at = DATE '2025-04-01' WHERE zone_id = $1 AND label = 'gone'`, del, done), zone)

	state := func(label string) (int16, int16) {
		t.Helper()
		var status, check int16
		if err := pool.QueryRow(ctx,
			"SELECT status, check_state FROM domains WHERE zone_id = $1 AND label = $2", zone, label,
		).Scan(&status, &check); err != nil {
			t.Fatal(err)
		}
		return status, check
	}

	// Дефолт — 6 месяцев (§3.3): конфиг и константа планировщика —
	// одно значение.
	if got := config.Default().Scheduler.RecheckInterval; got != config.Duration(scheduler.DefaultRecheckInterval) {
		t.Errorf("дефолт конфига = %s, хочу %s (6 месяцев)", got, scheduler.DefaultRecheckInterval)
	}
	// Значение меняется конфигом: переменная окружения перекрывает.
	envCfg, err := config.LoadEnv(func(key string) string {
		if key == "CRAWLER_SCHEDULER_RECHECK_INTERVAL" {
			return "170h"
		}
		return os.Getenv(key) // остальное окружение (в т.ч. APP_PG_DSN) — как есть
	})
	if err != nil {
		t.Fatalf("конфиг с переопределением: %v", err)
	}
	if got := envCfg.Scheduler.RecheckInterval; got != config.Duration(170*time.Hour) {
		t.Errorf("переопределение окружением = %s, хочу 170h", got)
	}

	// Фиксированное «сейчас»: возраст доменов не зависит от даты прогона.
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	res, err := scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if res.Domains != 1 {
		t.Fatalf("переведено доменов: %d, хочу 1 (только созревший old)", res.Domains)
	}
	if _, check := state("old"); check != int16(model.CheckStatePending) {
		t.Errorf("old: check_state = %d, хочу pending", check)
	}
	// Статус результата и чужие состояния не тронуты.
	if s, _ := state("old"); s != int16(model.StatusAlive) {
		t.Errorf("old: статус результата изменён (%d) — перепроверка меняет только цикл", s)
	}
	for _, label := range []string{"fresh", "never", "pending", "gone"} {
		if _, check := state(label); check == int16(model.CheckStatePending) && label != "pending" {
			t.Errorf("%s: переведён в pending вне интервала", label)
		}
	}

	// Не чаще интервала: повторный проход не находит работы.
	res, err = scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if res.Domains != 0 {
		t.Errorf("повторный проход перевёл ещё %d — интервал не соблюдается", res.Domains)
	}

	// Интервал меняется конфигом: 7 дней (короче возраста fresh ~17 дней,
	// но длиннее возраста «сегодня проверенных») — fresh тоже созрел, never
	// (никогда не проверялся) и pending/deleted по-прежнему нет. Созревание
	// — возраст > интервала (§11.4 п. 8 «не чаще»), длиннее — только старше.
	res, err = scheduler.RecheckOnce(ctx, pool, scheduler.RecheckOptions{
		Interval: 7 * 24 * time.Hour,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Domains != 1 {
		t.Fatalf("после смены интервала переведено %d, хочу 1 (fresh)", res.Domains)
	}
	if _, check := state("fresh"); check != int16(model.CheckStatePending) {
		t.Errorf("fresh: check_state = %d, хочу pending", check)
	}
	for _, label := range []string{"never", "gone"} {
		if _, check := state(label); check != int16(model.CheckStateDone) {
			t.Errorf("%s: check_state = %d, хочу done (возраста/статуса нет)", label, check)
		}
	}
}

// ---------------------------------------------------------------- п. 9

// TestAccept09_RestartNoStuckDomains — §11.4 п. 9: «Остановка/перезапуск
// любого сервиса не приводит к потере данных и "застрявшим" доменам (в
// течение времени lease)». Грань, не покрытая соседями (e2e — обрыв
// PostgreSQL, resilience — потеря Redis и смерть воркера посреди пачки):
// остановленный сервис оставляет домены queued с арендой; живой lease не
// отбирается, истёкший — LeaseOnce возвращает в pending, диспетчер
// перевыбирает и переиздаёт пачку; данные не теряются.
func TestAccept09_RestartNoStuckDomains(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	labels := []string{"a1", "a2", "a3"}
	ids := insertPendingDomains(t, pool, "acc9.test", labels)
	batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: len(labels)})
	if err != nil {
		t.Fatalf("выборка пачки: %v", err)
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
		t.Fatalf("постановка пачки: %v", err)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != len(labels) {
		t.Fatalf("queued: %d, хочу %d", n, len(labels))
	}

	// Живой lease не отдаётся: «сервис работает» — домены остаются в
	// работе (Now без сдвига, аренда только что выдана).
	res, err := scheduler.LeaseOnce(ctx, pool, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res.Domains != 0 {
		t.Fatalf("возврат при живом lease: %d, хочу 0", res.Domains)
	}

	// Остановка сервиса: воркер умер, домены зависли в queued. Сдвиг
	// «сейчас» за срок аренды — взгляд lease-прохода после простоя.
	shifted := func() time.Time { return time.Now().Add(2 * scheduler.DefaultLeaseTimeout) }
	res, err = scheduler.LeaseOnce(ctx, pool, scheduler.LeaseOptions{Now: shifted})
	if err != nil {
		t.Fatal(err)
	}
	if res.Domains != int64(len(labels)) {
		t.Fatalf("возврат истёкших: %d, хочу %d", res.Domains, len(labels))
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStatePending)); n != len(labels) {
		t.Fatalf("pending после возврата: %d, хочу %d (не застряли в queued)", n, len(labels))
	}

	// Диспетчер перевыбирает и переиздаёт: домены снова в работе, ни один
	// не потерялся (множество id сохранено).
	batch2, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: len(labels)})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch2) != len(labels) {
		t.Fatalf("перевыбрано: %d, хочу %d", len(batch2), len(labels))
	}
	got := idSet(func() []int64 {
		out := make([]int64, 0, len(batch2))
		for _, d := range batch2 {
			out = append(out, d.DomainID)
		}
		return out
	}())
	want := idSet(ids)
	if len(got) != len(want) {
		t.Fatalf("множество доменов изменилось: %v против %v — потеря данных", got, want)
	}
	// Состав, а не только размер: перевыбор чужих/дублей при том же
	// размере — тоже потеря данных.
	for id := range want {
		if !got[id] {
			t.Fatalf("домен %d потерян при переиздании: %v против %v", id, got, want)
		}
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch2); err != nil {
		t.Fatal(err)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 2 {
		t.Errorf("пачек q:dns: %d, хочу 2 (первая + переизданная)", n)
	}

	// Удалённый домен вне цикла: lease-проход и диспетчер его не трогают.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE domains SET status = %d WHERE id = $1`, int16(model.StatusDeleted)), ids[0]); err != nil {
		t.Fatal(err)
	}
	res, err = scheduler.LeaseOnce(ctx, pool, scheduler.LeaseOptions{Now: shifted})
	if err != nil {
		t.Fatal(err)
	}
	// Охрана §13.1: удалённый не возвращается в цикл — только живые.
	if res.Domains != int64(len(labels)-1) {
		t.Fatalf("lease после повторного зависания: %d, хочу %d (deleted вне цикла)", res.Domains, len(labels)-1)
	}
	batch3, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch3) != len(labels)-1 {
		t.Errorf("перевыбрано после удаления: %d, хочу %d", len(batch3), len(labels)-1)
	}
	for _, d := range batch3 {
		if d.DomainID == ids[0] {
			t.Error("удалённый домен выбран диспетчером (§13.1)")
		}
	}
}

// ---------------------------------------------------------------- п. 10

// TestAccept10_DomainSearchByName — §11.4 п. 10: «Поиск по имени
// (crawlerctl domain) для домена в любой форме записи (unicode/punycode)
// возвращает статус, результаты по субдоменам и признаки Laravel (если
// есть); для неизвестного домена выводится понятное сообщение». Уровень
// SQL-функции domain_info (единая точка правды CLI и psql, T-082) +
// нормализация форм internal/idn — то, что делает crawlerctl до запроса.
func TestAccept10_DomainSearchByName(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Два домена: IDN в IDN-зоне (unicode-имя зоны для отображения) и
	// Laravel-образец с признаками.
	idnZone := accInsertZone(t, env.pool, accIDNZone, accIDNZoneUni)
	accPipelineZone(ctx, t, env, idnZone, accIDNZone, []string{accIDNLabel})
	accPipeline(ctx, t, env, accZone, []string{"laravel-8-mix"})
	accAnalyze(ctx, t, env, env.analyzer, 2)

	info := func(name string) map[string]any {
		t.Helper()
		var raw []byte
		if err := env.pool.QueryRow(ctx, "SELECT domain_info($1)", name).Scan(&raw); err != nil {
			t.Fatalf("domain_info(%q): %v", name, err)
		}
		if raw == nil {
			return nil
		}
		return dinfoDecode(t, raw)
	}

	t.Run("IDN-домен в любой форме записи", func(t *testing.T) {
		// Формы входа: unicode (регистр, точка), punycode — нормализация
		// internal/idn до обращения к БД, как crawlerctl domain (T-081).
		forms := []string{
			accIDNLabelU + "." + accIDNZoneUni + ".",     // сайт.Проверка.TEST.
			"  " + accIDNLabel + "." + accIDNZone + "  ", // punycode с краевыми пробелами
		}
		var canonical map[string]any
		for _, form := range forms {
			n, err := idn.Normalize(form)
			if err != nil {
				t.Fatalf("нормализация %q: %v", form, err)
			}
			if n.ASCII != accIDNLabel+"."+accIDNZone {
				t.Fatalf("нормализованная форма %q → %q, хочу %s", form, n.ASCII, accIDNLabel+"."+accIDNZone)
			}
			got := info(n.ASCII)
			if got == nil {
				t.Fatalf("domain_info(%q из формы %q) = NULL", n.ASCII, form)
			}
			if canonical == nil {
				canonical = got
				continue
			}
			if fmt.Sprint(got["domain"]) != fmt.Sprint(canonical["domain"]) {
				t.Errorf("формы записи дали разные ответы: %v против %v", got["domain"], canonical["domain"])
			}
		}

		// Статус, результаты по субдоменам, зона с unicode-именем.
		dom := dinfoMap(t, canonical["domain"], "domain")
		if got := dinfoStr(t, dom["status_name"], "domain.status_name"); got != "alive" {
			t.Errorf("статус = %q, хочу alive", got)
		}
		if got := dinfoStr(t, dom["name"], "domain.name"); got != accIDNLabel+"."+accIDNZone {
			t.Errorf("имя = %q, хочу punycode-форму", got)
		}
		if alive := dinfoSlice(t, canonical["dns_alive"], "dns_alive"); len(alive) != 1 {
			t.Errorf("dns_alive = %v, хочу один живой субдомен (апекс)", alive)
		}
		results := dinfoSlice(t, canonical["results"], "results")
		if len(results) != 1 {
			t.Fatalf("results = %d записей, хочу 1", len(results))
		}
		rec := dinfoMap(t, results[0], "results[0]")
		if got := dinfoStr(t, rec["final_url"], "results[0].final_url"); got != "https://"+accIDNLabel+"."+accIDNZone+"/" {
			t.Errorf("итоговый URL = %q", got)
		}
		zone := dinfoMap(t, canonical["zone"], "zone")
		if got := dinfoStr(t, zone["name_unicode"], "zone.name_unicode"); got != accIDNZoneUni {
			t.Errorf("unicode-имя зоны = %q, хочу %q (отображение, ТЗ 5.2 п. 7)", got, accIDNZoneUni)
		}
		if canonical["laravel"] != nil {
			t.Errorf("laravel у не-Laravel домена = %v, хочу null", canonical["laravel"])
		}
	})

	t.Run("признаки Laravel в ответе", func(t *testing.T) {
		got := info("laravel-8-mix." + accZone)
		if got == nil {
			t.Fatal("domain_info(laravel-8-mix) = NULL")
		}
		lar := dinfoMap(t, got["laravel"], "laravel")
		if got := dinfoStr(t, lar["ver_max"], "laravel.ver_max"); got != "9.x" {
			t.Errorf("laravel.ver_max = %q, хочу 9.x", got)
		}
		findings := dinfoSlice(t, lar["findings"], "laravel.findings")
		if len(findings) != 1 {
			t.Fatalf("findings = %d, хочу 1", len(findings))
		}
		first := dinfoMap(t, findings[0], "findings[0]")
		if v, ok := first["is_laravel"].(bool); !ok || !v {
			t.Errorf("уверенное срабатывание: is_laravel = %v (present=%v), хочу true", first["is_laravel"], ok)
		}
	})

	t.Run("неизвестный домен — понятный ответ", func(t *testing.T) {
		// Домена нет, зона есть: NULL — CLI различает «в зоне нет домена»
		// (второй лёгкий запрос к zones, T-081) от «зона отсутствует».
		// Вход — как у crawlerctl: нормализованная punycode-форма.
		n, err := idn.Normalize("Нет-Такого." + accIDNZoneUni)
		if err != nil {
			t.Fatalf("нормализация неизвестного имени: %v", err)
		}
		if got := info(n.ASCII); got != nil {
			t.Errorf("domain_info несуществующего домена = %v, хочу NULL", got)
		}
		var zoneExists bool
		if err := env.pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM zones WHERE name = $1)", accIDNZone,
		).Scan(&zoneExists); err != nil {
			t.Fatal(err)
		}
		if !zoneExists {
			t.Error("зона исчезла — «нет домена» неотличимо от «нет зоны»")
		}
		// Незнакомая зона: тоже NULL без ошибки (вход валиден, данных нет).
		if got := info("site.unknown-zone.test"); got != nil {
			t.Errorf("domain_info в незнакомой зоне = %v, хочу NULL", got)
		}
	})
}

// ---------------------------------------------------------------- п. 11

// accSampleExpect — точные ожидания образца обязательны (контракт корпуса
// T-075): отсутствие — внятная ошибка, а не паника разыменования в кейсах.
func accSampleExpect(t *testing.T, s rules.Sample) rules.SampleExpect {
	t.Helper()
	if s.Expect.IsLaravel == nil || s.Expect.Score == nil ||
		s.Expect.CalcVerMin == nil || s.Expect.CalcVerMax == nil {
		t.Fatalf("samples/%s/meta.yaml: нужны ожидания is_laravel/score/calc_ver_min/calc_ver_max", s.ID)
	}
	return s.Expect
}

// accSampleByID — образец корпуса заказчика по имени каталога.
func accSampleByID(t *testing.T, id string) rules.Sample {
	t.Helper()
	if _, err := os.Stat(accSamplesDir); err != nil {
		t.Fatalf("корпус образцов недоступен: %v", err)
	}
	samples, err := rules.LoadSamples(accSamplesDir)
	if err != nil {
		t.Fatalf("корпус не загружается: %v", err)
	}
	for _, s := range samples {
		if s.ID == id {
			accSampleExpect(t, s)
			return s
		}
	}
	t.Fatalf("образец %s отсутствует в корпусе", id)
	return rules.Sample{}
}

// accSamples — весь корпус образцов (testbed подключает их доменами
// <id>.crawl.test, генератор T-090/T-073).
func accSamples(t *testing.T) []rules.Sample {
	t.Helper()
	if _, err := os.Stat(accSamplesDir); err != nil {
		t.Fatalf("корпус образцов недоступен: %v", err)
	}
	samples, err := rules.LoadSamples(accSamplesDir)
	if err != nil {
		t.Fatalf("корпус не загружается: %v", err)
	}
	if len(samples) < 13 {
		t.Fatalf("корпус усох: %d образцов, хочу не меньше 13", len(samples))
	}
	for _, s := range samples {
		accSampleExpect(t, s)
	}
	return samples
}

// TestAccept11_CustomerSamplesPipeline — §11.4 п. 11: «Анализатор на
// реальных образцах Laravel, предоставленных заказчиком, даёт ожидаемые
// срабатывания и диапазоны версий; в laravel_findings попадают все
// срабатывания, в laravel_domains — только со score не ниже порога».
// Весь корпус (юнит-движок гоняет его T-075; здесь — конвейер): файл
// образцов через DNS/HTTP/analyze на живом testbed, ожидания — meta.yaml
// каждого образца; в laravel_findings — все срабатывания включая слабые
// у негативных, в laravel_domains — только уверенные (порог 0.6, §13.1).
func TestAccept11_CustomerSamplesPipeline(t *testing.T) {
	skipUnlessIntegration(t)
	env := newAccEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	samples := accSamples(t)
	labels := make([]string, 0, len(samples))
	for _, s := range samples {
		labels = append(labels, s.ID)
	}
	ids := accPipeline(ctx, t, env, accZone, labels)
	zoneID := accZoneID(t, env.pool, accZone)
	accAnalyze(ctx, t, env, env.analyzer, len(labels))

	var positives, negatives, weakNegatives int
	for _, s := range samples {
		sample, id := s, ids[s.ID]
		name := sample.ID + "." + accZone

		// Домен проверен: alive/done (§7) — образцы отвечают по обоим
		// протоколам.
		st := httpDomainState(t, env.pool, id)
		if st.status != model.StatusAlive || st.checkState != int16(model.CheckStateDone) {
			t.Errorf("%s: status/check_state = %s/%d, хочу alive/done", name, st.status, st.checkState)
		}

		// laravel_findings: ВСЕ срабатывания (§13.1) — состав, score,
		// вычисленный диапазон версий, версия набора.
		fs := accFindings(t, env.pool, zoneID, id)
		switch {
		case len(sample.Expect.Rules) == 0:
			if len(fs) != 0 {
				t.Errorf("%s: чистый негатив, findings = %d строк", name, len(fs))
			}
		case len(fs) != 1:
			t.Errorf("%s: findings = %d строк, хочу 1 (апекс, variant 0)", name, len(fs))
		default:
			f := fs[0]
			if f.sub != 0 || f.variant != int16(model.VariantMain) {
				t.Errorf("%s: срабатывание субдомена/варианта = %d/%d, хочу 0/0", name, f.sub, f.variant)
			}
			if f.rulesVersion != env.rules.Version {
				t.Errorf("%s: rules_version = %d, хочу %d (версия набора)", name, f.rulesVersion, env.rules.Version)
			}
			if math.Abs(float64(f.score)-*sample.Expect.Score) > accScoreTol {
				t.Errorf("%s: score = %.4f, хочу ≈%.3f", name, f.score, *sample.Expect.Score)
			}
			if got, want := accStr(f.verMin), accStr(sample.Expect.CalcVerMin); got != want {
				t.Errorf("%s: ver_min = %q, хочу %q", name, got, want)
			}
			if got, want := accStr(f.verMax), accStr(sample.Expect.CalcVerMax); got != want {
				t.Errorf("%s: ver_max = %q, хочу %q", name, got, want)
			}
			if f.isLaravel != *sample.Expect.IsLaravel {
				t.Errorf("%s: is_laravel = %v, хочу %v (порог 0.6)", name, f.isLaravel, *sample.Expect.IsLaravel)
			}
			fired := accFiredRules(t, f.signals)
			if len(fired) != len(sample.Expect.Rules) {
				t.Errorf("%s: сработавших признаков %d, хочу %d (%v)", name, len(fired), len(sample.Expect.Rules), fired)
			}
			for _, r := range sample.Expect.Rules {
				if !fired[r] {
					t.Errorf("%s: признак %s отсутствует (сработали %v)", name, r, fired)
				}
			}
		}

		// laravel_domains: только уверенные (score ≥ порога); агрегат
		// домена с единственной записью совпадает со срабатыванием,
		// found_on_mask — бит субдомена (§5.8). Счётчики ниже — состав
		// корпуса по meta.yaml (ожидания), расхождения с БД уже отмечены
		// ошибками выше.
		agg, ok := accLaravelDomain(t, env.pool, zoneID, id)
		if !*sample.Expect.IsLaravel {
			if ok {
				t.Errorf("%s: негативный образец в laravel_domains (score %.3f)", name, agg.score)
			}
			negatives++
			if len(sample.Expect.Rules) > 0 {
				weakNegatives++ // слабые срабатывания остались в findings
			}
			continue
		}
		positives++
		if !ok {
			t.Errorf("%s: уверенный образец без строки laravel_domains", name)
			continue
		}
		if math.Abs(float64(agg.score)-*sample.Expect.Score) > accScoreTol {
			t.Errorf("%s: агрегат score = %.4f, хочу ≈%.3f", name, agg.score, *sample.Expect.Score)
		}
		if got, want := accStr(agg.verMin), accStr(sample.Expect.CalcVerMin); got != want {
			t.Errorf("%s: агрегат ver_min = %q, хочу %q", name, got, want)
		}
		if got, want := accStr(agg.verMax), accStr(sample.Expect.CalcVerMax); got != want {
			t.Errorf("%s: агрегат ver_max = %q, хочу %q", name, got, want)
		}
		if agg.mask != 0b1 {
			t.Errorf("%s: found_on_mask = %b, хочу бит 0 (апекс)", name, agg.mask)
		}
		if agg.rulesV != env.rules.Version {
			t.Errorf("%s: агрегат rules_version = %d, хочу %d", name, agg.rulesV, env.rules.Version)
		}
	}

	// Целостность корпуса на приёмку: позитивы и негативы с «слабыми»
	// (контроль ложноположительных) обязаны быть (§13.3, корпус T-075).
	if positives < 8 {
		t.Errorf("позитивных образцов %d, хочу не меньше 8 (мажорные версии Laravel)", positives)
	}
	if negatives < 4 || weakNegatives < 2 {
		t.Errorf("негативных %d (слабых %d), хочу не меньше 4/2", negatives, weakNegatives)
	}
}
