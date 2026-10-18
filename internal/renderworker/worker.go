// Package renderworker — этап рендера (T-067; ТЗ 5.6 «итоговый DOM заменяет
// сырой HTML в хранилище; в результате выставляется rendered = true», §5.7,
// §7 «render-worker → DOM + cookies → обновление результата → q:analyze»).
//
// Воркер потребляет q:render (сообщение Result — ключ строки
// subdomain_results: домен, субдомен, вариант; формат T-024, публикуют
// HTTP-этап T-066 при SPA-вердикте T-057 и проход доработки renderq.go).
// Сам результат остаётся в БД, сообщение только указывает на запись —
// итоговый URL для рендера читается из строки (сообщение несёт только
// идентификаторы, ТЗ 4.2). Обработка одной доставки:
//
//	SELECT строки (final_url, content_hash — цель рендера и старая ссылка)
//	→ render.Pool.Run (T-061…T-064: изолированный контекст, блокировка
//	  ресурсов, ожидание готовности, DOM + cookies + заголовки)
//	→ ContentStore.Put(DOM) — новая ссылка +1 (T-056/T-042, дедуп по хэшу)
//	→ ОДНА транзакция: UPDATE строки (rendered = true, новый content_hash,
//	  final_url/cookies/headers рендера, rules_version = NULL) +
//	  −1 refcount старого хэша (SwapRefTx, T-044)
//	→ после фиксации — публикация q:analyze (Result, тот же ключ; §7)
//	→ XACK (Processing, T-025: подтверждение только после записи в БД).
//
// Идемпотентность повторной доставки (ТЗ 3.4): перезапись без охраны
// rendered — повторный рендер перезаписывает контент и refcount (баланс
// в духе T-058: повторный Put даёт +1, UPDATE снимает старую ссылку).
// От гонки со свежей перепроверкой домена (recheck T-030 перезаписывает
// строки DELETE+INSERT, T-058) строка защищена оптимистической охраной
// UPDATE по content_hash: изменившаяся строка не перезаписывается (0
// строк — ссылка Put снимается, доставка подтверждается: задача была про
// прошлый эпизод, у нового свои публикация и рендер).
//
// Флаг render_pending (T-066) render-worker не трогает: владелец флага —
// HTTP-этап, его проход доработки снимает бит после публикации q:render;
// рендер меняет только строку результата (§7 — домен уже done, рендер
// асинхронное дополнение).
package renderworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// SQL этапа. rendered = true — литерал ТЗ 5.6; rules_version = NULL —
// контент сменился, прошлое заключение анализатора (rules_version ставит
// T-068+ при записи findings) новому контенту не соответствует: повторная
// публикация q:analyze ниже переанализирует запись и проставит свежую
// версию. Ключи — литералами типов (стиль record.go T-058).
var (
	// renderSelectSQL — цель рендера: итоговый URL строки результата и её
	// текущая ссылка на контент (оспариваемая оптимистической охраной
	// renderUpdateSQL). Ведущие колонки PK — партиционирование LIST по
	// zone_id и субпартиционирование HASH (T-005) режут поиск до листа.
	renderSelectSQL = "SELECT final_url, content_hash FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"

	// renderUpdateSQL — замена сырого HTML на итоговый DOM (ТЗ 5.6, §5.7):
	// rendered = true, content_hash — ссылка на DOM (Put выше уже дал ей
	// +1, здесь строка переключается на неё), final_url/cookies/headers —
	// данные рендера (JS-редиректи виден только рендеру — T-061; cookies —
	// все, включая httpOnly и выставленные JS, ТЗ 5.6/§11.4 п. 3 — единый
	// jsonb-контракт с T-058; заголовки — ответ итоговой навигации, ТЗ 5.8).
	// Охрана content_hash IS NOT DISTINCT FROM $9 — оптимистическая:
	// строка перезаписывается, только пока её контент тот же, что прочитан
	// (NULL сравнивается корректно — «контента нет» легально, T-056).
	renderUpdateSQL = "UPDATE subdomain_results AS r " +
		"SET rendered = true, final_url = $5::text, content_hash = $6::bytea, " +
		"cookies = $7::jsonb, headers = $8::jsonb, rules_version = NULL " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.content_hash IS NOT DISTINCT FROM $9::bytea"
)

// RenderDB — пул для чтения цели и записи результата: db.DB с
// транзакциями (стиль httpworker.RecordDB, content.RefDB). Замена строки
// и своп ссылок идут одной транзакцией (ТЗ 3.4); *pgxpool.Pool
// удовлетворяет интерфейсу.
type RenderDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ContentStore — шов хранилища контента (ТЗ 5.7): запись DOM и снятие
// ссылки, когда строка ушла из-под задачи. Узкое подмножество контракта
// content.ContentStore (Get этапу не нужен — DOM не читается обратно);
// удовлетворяется *content.PackStore (T-042).
type ContentStore interface {
	Put(ctx context.Context, data []byte) (content.PutResult, error)
	Delete(ctx context.Context, hash content.Hash) error
}

// Renderer — шов рендера: удовлетворяется *render.Manager (T-065 — боевая
// сборка cmd/render-worker: живое поколение «браузер + пул», перезапуски
// по страницам/памяти, реконнект и повтор задачи) и *render.Pool напрямую
// (T-061 — интеграционные тесты). Одна задача — одна страница в свежем
// изолированном контексте; параллелизм ограничен слотами самого пула
// (render.contexts, ТЗ 5.6).
type Renderer interface {
	Run(ctx context.Context, task render.RenderTask) (render.RenderOutput, error)
}

// Дефолты воркера.
const (
	// DefaultGroup — группа потребителей q:render (ТЗ 4.2: consumer groups
	// масштабируют воркеров горизонтально).
	DefaultGroup = "render-worker"

	// DefaultConcurrency — доставок q:render в работе одновременно.
	// Выше render.contexts не ускоряет сам рендер (слоты пула — узкое
	// горло этапа, ТЗ 5.6), но перекрывает его БД-часть (Put, транзакция,
	// публикация) — берём по верхней границе рекомендованных контекстов.
	DefaultConcurrency = 4

	// DefaultConsume — записей q:render за одно чтение: окно чтения не
	// глубже запаса обработки (стиль httpworker).
	DefaultConsume = 16

	// DefaultErrorWait — пауза после ошибок Redis и при недоступном
	// браузере (стиль httpworker.DefaultErrorWait): холостые круги не
	// греют логи, неподтверждённые доставки вернёт XAUTOCLAIM (T-025).
	DefaultErrorWait = 30 * time.Second
)

// Options — параметры воркера; нулевые значения заменяются дефолтами
// (withDefaults, стиль httpworker.Options).
type Options struct {
	// Concurrency — доставок q:render в работе одновременно; 0 —
	// DefaultConcurrency.
	Concurrency int
	// Consume — записей q:render за одно чтение; 0 — DefaultConsume.
	Consume int
	// Group — группа потребителей q:render; "" — DefaultGroup.
	Group string
	// Consumer — имя потребителя группы; "" — "render-worker-<pid>".
	Consumer string
	// Alive — зонд браузера: false — очередь не читается (доставки
	// остаются в стриме непрочитанными, а не в PEL без XACK — без
	// клеймов запись в PEL зависает надолго). Ставится сборкой сервиса
	// (render.Manager.Alive, T-061/T-065: мёртвое поколение зонд
	// перезапускает сам — реконнект); nil — зонд отключён.
	Alive func() bool
	// Recover — слой возврата зависших доставок (Reliab, T-025): Run
	// поднимает фоновый RecoverLoop (XAUTOCLAIM → та же обработка →
	// XACK) с теми же Group/Consumer/Batch; nil — восстановления нет
	// (изолированные сборки и тесты без Redis-специфики).
	Recover *queue.Reliab
	// Log — логгер; nil — записи подавлены (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — реестр Prometheus сервиса (T-077, ТЗ 5.9): из него New
	// строит приборы этапа (NewMetrics); nil — без метрик (изолированные
	// сборки и тесты).
	Metrics *metrics.Registry
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("render-worker-%d", os.Getpid())
	}
	return o
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return o.Log
}

// Worker — рендер-воркер: обрабатывает задачи q:render (ключи строк
// subdomain_results) через швы рендера и хранилища. Потокобезопасен:
// состояние неизменно после New.
type Worker struct {
	db       RenderDB
	q        queue.Queue
	contents ContentStore
	renderer Renderer
	opt      Options
	met      *Metrics // приборы этапа (T-077); nil — no-op
}

// New строит воркер поверх пула БД, очереди (публикация q:analyze — под
// гейтом backpressure в сборке, T-026), хранилища контента и пула
// рендера. nil-зависимости — ошибка: тихая потеря результата/публикации
// незаметна для оператора (стиль T-058).
func New(db RenderDB, q queue.Queue, contents ContentStore, renderer Renderer, opts Options) (*Worker, error) {
	if db == nil {
		return nil, errors.New("renderworker: пул БД не задан")
	}
	if q == nil {
		return nil, errors.New("renderworker: очередь не задана")
	}
	if contents == nil {
		return nil, errors.New("renderworker: хранилище контента не задано")
	}
	if renderer == nil {
		return nil, errors.New("renderworker: пул рендера не задан")
	}
	return &Worker{db: db, q: q, contents: contents, renderer: renderer, opt: opts.withDefaults(), met: NewMetrics(opts.Metrics)}, nil
}

// renderTarget — прочитанная строка результата: цель рендера и старая
// ссылка на контент.
type renderTarget struct {
	finalURL string
	hash     []byte // nil — контента нет (NULL content_hash, T-056)
}

// ProcessResult обрабатывает одну задачу q:render (§5.6, §7): рендер
// итогового URL строки, DOM в хранилище, атомарная замена ссылки и
// rendered = true, публикация q:analyze после фиксации. Возвращает false
// без ошибки, когда задачи делать нечего: строки нет, у неё нет итогового
// URL или она ушла из-под задачи (перепроверка перезаписала — охрана
// UPDATE) — это успех, публикация не нужна, доставка подтверждается.
// Ошибка — сбой любого шага: доставка q:render не подтверждается и
// вернётся повторной доставкой (Processing, T-025); повторная обработка
// идемпотентна (перезапись, см. комментарий пакета).
func (w *Worker) ProcessResult(ctx context.Context, task queue.Result) (bool, error) {
	target, found, err := w.selectTarget(ctx, task)
	if err != nil {
		return false, err
	}
	if !found || target.finalURL == "" {
		// Строки нет (домен удалён/перепроверен без этого ключа) либо
		// итогового URL нет — рендерить нечего; штатный отказ без ошибки.
		w.opt.log().Debug("рендер не требуется — строки нет или без итогового URL",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return false, nil
	}
	name := renderLogName(task, target.finalURL)

	out, err := w.renderer.Run(ctx, render.RenderTask{URL: target.finalURL})
	if err != nil {
		// Ошибка страницы (таймаут, net::ERR_*) или браузера — повторная
		// доставка уровнем очереди (класс task) / ожидание или будущий
		// перезапуск (класс browser, T-065); частичного результата по ТЗ §8
		// не бывает (T-063).
		return false, fmt.Errorf("renderworker: %s: рендер: %w", name, err)
	}
	if out.DOM == "" || out.FinalURL == "" {
		// Страховка контракта T-064: живая страница всегда имеет
		// documentElement и location; пустые значения — ненормальный
		// результат, строку не трогаем.
		return false, fmt.Errorf("renderworker: %s: пустой результат рендера (DOM %d байт, final_url %q)",
			name, len(out.DOM), out.FinalURL)
	}
	// DOM — одной копией байтов (R-02): хэш и Put из одного среза, сам
	// итоговый DOM уже ограничен рендером (render.Options.MaxDOMBytes);
	// кратные полные копии строки не нужны.
	dom := []byte(out.DOM)
	newHash := content.HashOf(dom)

	var oldHash content.Hash
	hasOld := len(target.hash) == content.HashLen
	if hasOld {
		copy(oldHash[:], target.hash)
	}

	// Put до транзакции (порядок T-056 → T-058: новая ссылка обязана
	// существовать до свопа, SwapRefTx требует живой newHash). Контент не
	// сменился (DOM побайтово равен сохранённому) — Put пропускается:
	// ссылка у строки уже есть, лишний +1 не нужен; охрана UPDATE ниже
	// сравнивает с тем же хэшем и проходит.
	// Ошибки: ErrStoreFull — временное (повтор доставки дождётся места),
	// ErrTooLarge — постоянное свойство страницы (DOM раздут JS): попытки
	// доставки исчерпаются и задача уйдёт в q:dead на ручной разбор
	// (T-025) — оператору сигнал поднять content.max_store_bytes.
	if !hasOld || newHash != oldHash {
		if _, err := w.contents.Put(ctx, dom); err != nil {
			return false, fmt.Errorf("renderworker: %s: сохранение DOM: %w", name, err)
		}
	}
	cookies, err := renderCookiesJSON(out.Cookies)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: cookies: %w", name, err)
	}
	headers, err := renderHeadersJSON(out.ResponseHeaders)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: заголовки: %w", name, err)
	}

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: замена контента: транзакция: %w", name, err)
	}
	// Откат безусловно: после Commit — no-op (стиль T-044/T-058).
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, renderUpdateSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant),
		out.FinalURL, newHash[:], cookies, headers, target.hash)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: замена контента: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		// Оптимистическая охрана: контент строки сменился после чтения
		// (перепроверка домена перезаписала результат T-058, конкурентная
		// доставка уже записала свой DOM) — строка не наша, транзакция
		// откатывается отложенным Rollback. Ссылка Put снимается: без
		// строки она повисла бы утечкой места (контент-то жив пока есть
		// ссылки; Delete снимает ровно добавленную Put). Ошибка снятия —
		// не ошибка задачи: Warn, доставка подтверждается.
		_ = tx.Rollback(ctx)
		if !hasOld || newHash != oldHash {
			if err := w.contents.Delete(ctx, newHash); err != nil && !errors.Is(err, content.ErrNotFound) {
				w.opt.log().Warn("рендер: ссылка контента не снята (строка ушла из-под задачи)",
					"domain", name, "hash", newHash.String(), "err", err)
			}
		}
		w.opt.log().Debug("рендер не записан — строка перезаписана другим эпизодом",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return false, nil
	}

	// −1 старому контенту (ТЗ 5.7 «при перезаписи результата счётчик
	// ссылок старого контента уменьшается»): SwapRefTx(old, нулевой) —
	// примитив «только −старому», новый уже имеет +1 от Put (T-058).
	// Алиасные строки (hash NULL, T-055) попадают в hasOld=false — снимать
	// нечего; DOM алиаса — его собственная ссылка.
	if hasOld && newHash != oldHash {
		if err := content.SwapRefTx(ctx, tx, oldHash, content.Hash{}); err != nil {
			return false, fmt.Errorf("renderworker: %s: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		// Откат вернёт строку; +1 от Put останется утечкой места —
		// безопасная сторона (T-044), повторная доставка перезапишет.
		return false, fmt.Errorf("renderworker: %s: замена контента: фиксация: %w", name, err)
	}

	// Публикация q:analyze после записи (§7): Result указывает на строку —
	// она обязана существовать к моменту потребления анализатором в новом
	// контенте. Сбой публикации → доставка q:render без XACK: повторная
	// доставка перезапишет результат идемпотентно и опубликует заново
	// (at-least-once, ТЗ 3.4; дубликаты ограничены попытками доставки).
	if _, err := w.q.Publish(ctx, queue.StreamAnalyze, task); err != nil {
		return true, fmt.Errorf("renderworker: %s: публикация q:analyze: %w", name, err)
	}
	w.opt.log().Debug("DOM записан",
		"domain", name, "final_url", out.FinalURL, "dom_bytes", len(out.DOM),
		"cookies", len(out.Cookies))
	return true, nil
}

// selectTarget читает цель рендера по ключу задачи. SELECT вне транзакции:
// конкурентная перезапись строки между чтением и UPDATE ловится охраной
// renderUpdateSQL, повторное чтение даёт повторную попытку с актуальным
// URL (идемпотентность доставки).
func (w *Worker) selectTarget(ctx context.Context, task queue.Result) (renderTarget, bool, error) {
	rows, err := w.db.Query(ctx, renderSelectSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant))
	if err != nil {
		return renderTarget{}, false, fmt.Errorf("renderworker: чтение строки результата: %w", err)
	}
	defer rows.Close()

	var t renderTarget
	if rows.Next() {
		var finalURL *string
		if err := rows.Scan(&finalURL, &t.hash); err != nil {
			return renderTarget{}, false, fmt.Errorf("renderworker: чтение строки результата: %w", err)
		}
		if finalURL != nil {
			t.finalURL = *finalURL
		}
		return t, true, nil // PK — не больше одной строки
	}
	if err := rows.Err(); err != nil {
		return renderTarget{}, false, fmt.Errorf("renderworker: чтение строки результата: %w", err)
	}
	return renderTarget{}, false, nil
}

// renderLogName — имя для логов: имя домена из сообщения, иначе ключ
// строки с целевым URL (Name опционально, T-024).
func renderLogName(task queue.Result, finalURL string) string {
	if task.Name != "" {
		return task.Name
	}
	return fmt.Sprintf("%d/%d/%d/%d → %s", task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant), finalURL)
}

// renderCookiesJSON сериализует cookies рендера в jsonb (единый контракт
// subdomain_results.cookies: ExtractCookie повторяет запись HTTP-этапа,
// T-064/T-058); пустой набор — NULL.
func renderCookiesJSON(cookies []render.ExtractCookie) (*string, error) {
	if len(cookies) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(cookies)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// renderHeadersJSON сериализует заголовки итоговой навигации в jsonb
// (канонические имена http.Header, как у T-058); пустой набор — NULL.
func renderHeadersJSON(h http.Header) (*string, error) {
	if len(h) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// --- рабочий цикл ------------------------------------------------------------

// Run — рабочий цикл render-worker (ТЗ 4.2, §5.6, §7): читает задачи
// q:render группой потребителей окном Consume и обрабатывает доставки
// параллельно до Concurrency, каждую — паттерном Processing (T-025):
// рендер → запись в БД → публикация q:analyze → XACK. Живость браузера
// (Options.Alive) проверяется перед каждым чтением: недоступный Chromium —
// пауза потребления, доставки остаются в стриме непрочитанными (не в PEL),
// перезапуск браузера — T-065. Работает до отмены ctx и возвращает nil —
// контракт OnRun каркаса (T-012; начатые задачи дорабатывают или
// возвращаются очереди повторной доставкой, ТЗ 3.4). Ошибка чтения или
// обработки не останавливает цикл: логируется (Warn), неподтверждённая
// доставка вернётся XAUTOCLAIM (ТЗ 4.2).
func Run(ctx context.Context, w *Worker, opts Options) error {
	if w == nil {
		return errors.New("renderworker: цикл воркера: воркер не задан")
	}
	o := opts.withDefaults()
	log := o.log()
	log.Info("цикл render-worker",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume, "alive", o.Alive != nil)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	// Возврат зависших доставок (T-025): фоновый RecoverLoop от имени
	// того же потребителя — клейм XAUTOCLAIM и та же обработка задач.
	// Живёт до отмены ctx; Run дожидается его выхода (начатая задача
	// дорабатывает или остаётся неподтверждённой, ТЗ 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoop(ctx, o.Recover, w.processDelivery, queue.RecoverOptions{
				Stream:   queue.StreamRender,
				Group:    o.Group,
				Consumer: o.Consumer,
				Batch:    o.Consume,
				Log:      log,
			})
		}()
		defer func() { <-recDone }()
	}

	sem := make(chan struct{}, o.Concurrency)
	for {
		if o.Alive != nil && !o.Alive() {
			log.Warn("Chromium недоступен — q:render не читается (перезапуск — менеджер, T-065)")
			wait(DefaultErrorWait)
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			continue
		}

		deliveries, err := w.q.Consume(ctx, queue.StreamRender, o.Group, o.Consumer, o.Consume)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			log.Warn("цикл render-worker: чтение q:render", "err", err)
			wait(DefaultErrorWait)
			continue
		}

		var wg sync.WaitGroup
		for _, d := range deliveries {
			if ctx.Err() != nil {
				break // не начатые доставки окна не подтверждены
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(d queue.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				// Паника доставки не должна ронять процесс без лога и
				// очистки — тот же инвариант, что у горутины OnRun
				// (internal/service): panic → Error-лог, доставка без
				// XACK вернётся повторной доставкой (XAUTOCLAIM, T-025);
				// in-flight соседей и graceful shutdown сохраняются.
				defer func() {
					if r := recover(); r != nil {
						log.Error("паника обработки доставки q:render",
							"entry", d.ID, "panic", r, "stack", string(debug.Stack()))
					}
				}()
				err := queue.Processing(ctx, w.q, o.Group, d, w.processDelivery)
				if err != nil && ctx.Err() == nil {
					log.Warn("задача q:render не подтверждена — вернётся повторной доставкой",
						"entry", d.ID, "err", err)
				}
			}(d)
		}
		wg.Wait()
		if ctx.Err() != nil {
			return nil // остановка: окно доработано или возвращено очереди
		}
	}
}

// processDelivery обрабатывает одну доставку q:render: декодирование
// Result-ключа и замена контента. Подтверждение (XACK) выполняет
// Processing после nil-возврата — «XACK только после записи результата в
// БД» (ТЗ 4.2); ядовитая запись (вне формата T-024) возвращается ошибкой —
// очередь исчерпает попытки и унесёт её в q:dead (T-025). Успешная
// обработка идёт в приборы этапа (T-077): счётчик задач и длительность,
// rendered=true при записанном DOM; ошибка — в счётчик классов рендера
// (доставка вернётся повторной доставкой, ТЗ 3.4).
func (w *Worker) processDelivery(ctx context.Context, del queue.Delivery) error {
	task, err := queue.Decode[queue.Result](del)
	if err != nil {
		return fmt.Errorf("renderworker: запись %s стрима %s: %w", del.ID, del.Stream, err)
	}
	start := time.Now()
	rendered, err := w.ProcessResult(ctx, task)
	if err != nil {
		w.met.Error(render.ClassOf(err))
		return fmt.Errorf("renderworker: домен %s: замена контента: %w", task.Name, err)
	}
	w.met.Task(time.Since(start))
	if rendered {
		w.met.Rendered()
	}
	return nil
}
